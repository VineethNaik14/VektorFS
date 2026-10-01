"""End-to-end behaviour of tracker + nodes + client over real TCP sockets."""

import asyncio
import hashlib

import pytest

from client.client import DownloadError
from common.hashing import sha256_file
from tests.cluster import Cluster

CHUNK = 64 * 1024


def chunk_ids(cluster, name):
    return [c.chunk_id for c in cluster.tracker.state.files[name].chunks]


def fully_replicated(cluster, name, want=3):
    """Ground truth from disk AND tracker view, only counting live nodes."""
    ids = chunk_ids(cluster, name)
    on_disk = all(len(cluster.holders(c)) >= want for c in ids)
    in_tracker = all(len(cluster.tracker.state.chunk_index[c].replicas) >= want for c in ids)
    return on_disk and in_tracker


@pytest.mark.asyncio
async def test_upload_replicates_each_chunk_3_times_and_download_verifies(cluster, sample_file, tmp_path):
    client = cluster.client()
    up = await client.upload(sample_file, "sample.bin", chunk_size=CHUNK)
    assert up.chunks == 6 and up.replicas_per_chunk == [3] * 6
    assert up.sha256 == sha256_file(str(sample_file))

    for cid in chunk_ids(cluster, "sample.bin"):
        assert len(cluster.holders(cid)) == 3          # really on 3 disks

    out = tmp_path / "out.bin"
    down = await client.download("sample.bin", out)
    assert sha256_file(str(out)) == up.sha256 == down.sha256
    assert out.read_bytes() == sample_file.read_bytes()
    assert not out.with_name("out.bin.part").exists()


@pytest.mark.asyncio
async def test_empty_and_exact_multiple_files(cluster, tmp_path):
    client = cluster.client()
    empty = tmp_path / "empty"; empty.write_bytes(b"")
    exact = tmp_path / "exact"; exact.write_bytes(b"x" * (CHUNK * 2))
    for f in (empty, exact):
        await client.upload(f, f.name, chunk_size=CHUNK)
        out = tmp_path / ("dl_" + f.name)
        await client.download(f.name, out)
        assert out.read_bytes() == f.read_bytes()


@pytest.mark.asyncio
async def test_FULL_WORKFLOW_kill_node_detect_rereplicate_download_verify(cluster, sample_file, tmp_path):
    """Upload -> Chunk -> Replicate -> Kill Node -> Detect -> Re-replicate -> Download -> SHA-256."""
    client = cluster.client()
    up = await client.upload(sample_file, "wf.bin", chunk_size=CHUNK)          # chunk+replicate
    ids = chunk_ids(cluster, "wf.bin")

    # Kill a node that actually holds data.
    victim = next(n for n in cluster.nodes if n.storage.list_chunks())
    held = set(victim.storage.list_chunks())
    assert held
    await victim.kill()

    # Failure detection via TTL.
    await cluster.wait_for(lambda: not cluster.tracker.state.nodes[victim.node_id].alive)
    # Automatic re-replication back to RF=3 on the 3 survivors.
    await cluster.wait_for(lambda: fully_replicated(cluster, "wf.bin"))
    for cid in ids:
        assert victim.node_id not in cluster.tracker.state.chunk_index[cid].replicas
        assert len(cluster.holders(cid)) == 3

    out = tmp_path / "wf_out.bin"
    down = await client.download("wf.bin", out)
    assert down.sha256 == up.sha256 == hashlib.sha256(sample_file.read_bytes()).hexdigest()
    assert out.read_bytes() == sample_file.read_bytes()


@pytest.mark.asyncio
async def test_download_works_immediately_after_kill_before_detection(tmp_path, sample_file):
    """Fault-tolerant read: tracker still lists the dead node; client must skip it."""
    c = await Cluster(tmp_path / "c", n_nodes=4, ttl=30.0).start()   # detector won't fire
    try:
        client = c.client()
        up = await client.upload(sample_file, "f", chunk_size=CHUNK)
        await c.nodes[0].kill()
        await c.nodes[1].kill()          # two of three replicas may be gone
        down = await client.download("f", tmp_path / "o")
        assert down.sha256 == up.sha256
    finally:
        await c.stop()


@pytest.mark.asyncio
async def test_corrupted_replica_is_detected_skipped_reported_and_repaired(
    cluster, sample_file, tmp_path, monkeypatch
):
    # The client shuffles replica order to spread load. Pin it (tracker lists
    # replicas sorted by node_id) so the corrupt copies are tried first.
    monkeypatch.setattr("client.client.random.sample", lambda pop, k: list(pop))
    client = cluster.client(concurrency=1)
    up = await client.upload(sample_file, "c.bin", chunk_size=CHUNK)
    cid = chunk_ids(cluster, "c.bin")[0]
    good_sha = cluster.tracker.state.chunk_index[cid].sha256

    # Flip bytes on ALL of this node's copies' siblings except one: corrupt 2 of 3.
    holders = sorted(cluster.holders(cid), key=lambda n: n.node_id)
    for n in holders[:2]:
        p = n.storage._get_chunk_path(cid)
        p.write_bytes(b"BITROT" + p.read_bytes()[6:])

    out = tmp_path / "c_out.bin"
    down = await client.download("c.bin", out)            # still succeeds via 3rd replica
    assert down.sha256 == up.sha256
    assert down.corrupt_replicas_skipped == 2

    # Self-healing: every replica on disk ends up with the correct hash again.
    await cluster.wait_for(lambda: len(cluster.holders(cid)) >= 3 and all(
        n.storage.sha256(cid) == good_sha for n in cluster.holders(cid)))


@pytest.mark.asyncio
async def test_all_replicas_corrupt_raises_instead_of_returning_bad_data(cluster, sample_file, tmp_path):
    client = cluster.client()
    await client.upload(sample_file, "d.bin", chunk_size=CHUNK)
    cid = chunk_ids(cluster, "d.bin")[0]
    for n in cluster.holders(cid):
        n.storage._get_chunk_path(cid).write_bytes(b"garbage")
    out = tmp_path / "d_out.bin"
    with pytest.raises(DownloadError):
        await client.download("d.bin", out)
    assert not out.exists() and not out.with_name("d_out.bin.part").exists()


@pytest.mark.asyncio
async def test_node_restart_reregisters_and_keeps_its_chunks(cluster, sample_file):
    client = cluster.client()
    await client.upload(sample_file, "r.bin", chunk_size=CHUNK)
    victim = next(n for n in cluster.nodes if n.storage.list_chunks())
    held = set(victim.storage.list_chunks())
    await victim.kill()
    await cluster.wait_for(lambda: not cluster.tracker.state.nodes[victim.node_id].alive)

    await victim.start()                                   # new port, same id + disk
    await cluster.wait_for(lambda: cluster.tracker.state.nodes[victim.node_id].alive)
    node = cluster.tracker.state.nodes[victim.node_id]
    assert node.port == victim.server.address[1]
    assert held <= node.chunks                             # chunk report restored locations


@pytest.mark.asyncio
async def test_file_deleted_while_node_down_is_garbage_collected_on_return(cluster, sample_file):
    client = cluster.client()
    await client.upload(sample_file, "g.bin", chunk_size=CHUNK)
    victim = next(n for n in cluster.nodes if n.storage.list_chunks())
    await victim.kill()
    await cluster.wait_for(lambda: not cluster.tracker.state.nodes[victim.node_id].alive)

    await client.delete("g.bin")
    await victim.start()
    await cluster.wait_for(lambda: victim.storage.list_chunks() == [])   # orphans removed
    for n in cluster.nodes:
        await cluster.wait_for(lambda n=n: n.storage.list_chunks() == [])


@pytest.mark.asyncio
async def test_tracker_restart_recovers_metadata_and_nodes_reregister(tmp_path, sample_file):
    c = await Cluster(tmp_path / "c", n_nodes=4, metadata=True).start()
    try:
        client = c.client()
        up = await client.upload(sample_file, "p.bin", chunk_size=CHUNK)
        await c.restart_tracker()
        # Metadata came from disk; replica locations come back via heartbeat->re-register.
        await c.wait_for(lambda: len(c.tracker.state.alive_nodes()) == 4)
        await c.wait_for(lambda: fully_replicated(c, "p.bin"))
        down = await c.client().download("p.bin", tmp_path / "p_out")
        assert down.sha256 == up.sha256
    finally:
        await c.stop()


@pytest.mark.asyncio
async def test_second_failure_still_recoverable_after_first_was_healed(cluster, sample_file, tmp_path):
    client = cluster.client()
    up = await client.upload(sample_file, "s.bin", chunk_size=CHUNK)
    await cluster.nodes[0].kill()
    await cluster.wait_for(lambda: len(cluster.tracker.state.alive_nodes()) == 3)
    await cluster.wait_for(lambda: fully_replicated(cluster, "s.bin"))
    await cluster.nodes[1].kill()                          # now only 2 nodes remain
    await cluster.wait_for(lambda: len(cluster.tracker.state.alive_nodes()) == 2)
    down = await client.download("s.bin", tmp_path / "s_out")
    assert down.sha256 == up.sha256


@pytest.mark.asyncio
async def test_duplicate_name_rejected_and_delete_then_reupload_ok(cluster, sample_file):
    client = cluster.client()
    await client.upload(sample_file, "dup", chunk_size=CHUNK)
    with pytest.raises(Exception, match="FILE_EXISTS"):
        await client.upload(sample_file, "dup", chunk_size=CHUNK)
    await client.delete("dup")
    await client.upload(sample_file, "dup", chunk_size=CHUNK)


@pytest.mark.asyncio
async def test_upload_with_fewer_nodes_than_rf_still_succeeds_then_heals(tmp_path, sample_file):
    c = await Cluster(tmp_path / "c", n_nodes=2).start()
    try:
        client = c.client()
        up = await client.upload(sample_file, "few", chunk_size=CHUNK)
        assert up.replicas_per_chunk == [2] * 6            # best effort: min(RF, live)
        n3 = __import__("tests.cluster", fromlist=["TestNode"]).TestNode(
            "node3", tmp_path / "c" / "node3", c.tracker_addr)
        await n3.start(); c.nodes.append(n3)
        await c.wait_for(lambda: fully_replicated(c, "few"))   # 3rd copy appears on its own
    finally:
        await c.stop()
