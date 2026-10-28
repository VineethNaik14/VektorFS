"""Regression tests for bugs found in the audit + the scrubber and ABORT paths."""

import asyncio
import os

import pytest

from client.client import UploadError
from common.messages import RemoteError
from common.rpc import call
from common.tracker_messages import build_abort
from tests.cluster import Cluster, TestNode
from tracker.server import TrackerServer
from tracker.store import JsonFileStore

CHUNK = 64 * 1024


def _chunk_ids(file_meta):
    return [c["chunk_id"] for c in file_meta["chunks"]]


# ---------------------------------------------------------------- metadata dir

@pytest.mark.asyncio
async def test_metadata_file_in_missing_directory_is_created(tmp_path, sample_file):
    """README flow: --metadata-file .\\data\\meta.json before data\\ exists."""
    meta = tmp_path / "does" / "not" / "exist" / "meta.json"
    c = Cluster(tmp_path / "c", n_nodes=2, rf=2)
    c.metadata_path = meta
    await c.start()
    try:
        await c.client().upload(sample_file, chunk_size=CHUNK)   # used to be INTERNAL_ERROR
        assert meta.is_file()
    finally:
        await c.stop()


def test_unwritable_metadata_path_fails_at_startup(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x")
    with pytest.raises(OSError):
        JsonFileStore(blocker / "meta.json")      # parent is a file: fail fast, not on first upload


def test_json_store_keeps_old_state_if_write_fails(tmp_path, monkeypatch):
    store = JsonFileStore(tmp_path / "m.json")
    rec = {"name": "a", "file_id": "f" * 32, "size": 0, "chunk_size": 1, "sha256": "0" * 64, "chunks": []}
    store.put_file(rec)
    monkeypatch.setattr("os.fsync", lambda fd: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        store.put_file({**rec, "name": "b"})
    assert [f["name"] for f in store.load()] == ["a"]


@pytest.mark.asyncio
async def test_commit_is_rolled_back_when_store_fails(tmp_path, sample_file):
    c = await Cluster(tmp_path / "c", n_nodes=3, metadata=True).start()
    try:
        def boom(_):
            raise OSError("disk full")
        c.tracker.store.put_file = boom
        with pytest.raises(RemoteError) as e:
            await c.client().upload(sample_file, chunk_size=CHUNK)
        assert e.value.code == "INTERNAL_ERROR"
        assert await c.client().list_files() == []          # not visible if not durable
        await c.wait_for(lambda: all(not n.storage.list_chunks() for n in c.nodes))   # GC'd
    finally:
        await c.stop()


@pytest.mark.asyncio
async def test_delete_survives_tracker_restart(tmp_path, sample_file):
    c = await Cluster(tmp_path / "c", n_nodes=3, metadata=True).start()
    try:
        client = c.client()
        await client.upload(sample_file, chunk_size=CHUNK)
        await client.delete(sample_file.name)
        await c.restart_tracker()
        assert await c.client().list_files() == []
    finally:
        await c.stop()


# ------------------------------------------------------- failed-upload cleanup

async def _two_node_rf1(tmp_path):
    tr = TrackerServer("127.0.0.1", 0, replication_factor=1, heartbeat_ttl=30)
    await tr.start()
    nodes = [TestNode(f"n{i}", tmp_path / f"n{i}", tr.address) for i in range(2)]
    for n in nodes:
        await n.start()
    while len(tr.state.alive_nodes()) < 2:
        await asyncio.sleep(0.02)
    return tr, nodes


@pytest.mark.asyncio
async def test_failed_upload_leaves_no_orphans_and_no_reservation(tmp_path, sample_file):
    tr, nodes = await _two_node_rf1(tmp_path)
    try:
        await nodes[1].kill()          # tracker still believes it is alive
        from client.client import VektorClient
        client = VektorClient(*tr.address, timeout=2, store_attempts=1)
        with pytest.raises(UploadError):
            await client.upload(sample_file, chunk_size=CHUNK)
        assert nodes[0].storage.list_chunks() == []      # copies that landed were removed
        assert tr.state.pending == {}                    # reservation released
    finally:
        await nodes[0].kill()
        await tr.stop()


@pytest.mark.asyncio
async def test_abort_command_is_idempotent(tmp_path):
    tr = TrackerServer("127.0.0.1", 0)
    await tr.start()
    try:
        r = await call(*tr.address, build_abort("a" * 32))
        assert r["aborted"] is False
        with pytest.raises(RemoteError):
            await call(*tr.address, {"command": "ABORT", "file_id": "../x"})
    finally:
        await tr.stop()


# ------------------------------------------------------------------- scrubber

@pytest.mark.asyncio
async def test_scrub_detects_and_heals_silent_corruption(cluster, sample_file):
    client = cluster.client()
    await client.upload(sample_file, chunk_size=CHUNK)
    meta = (await call(*cluster.tracker_addr, {"command": "GET_FILE", "name": sample_file.name}))["file"]
    victim = cluster.holders(_chunk_ids(meta)[0])[0]
    path = victim.storage._get_chunk_path(_chunk_ids(meta)[0])
    raw = bytearray(path.read_bytes()); raw[0] ^= 0xFF; path.write_bytes(bytes(raw))   # bit rot

    result = await client.scrub()
    assert result["corrupt"] == 1 and result["missing"] == 0
    cid = _chunk_ids(meta)[0]
    # Healed back to RF=3 with only good copies.
    await cluster.wait_for(lambda: len(cluster.tracker.state.chunk_index[cid].replicas) == 3)
    assert all(h.storage.sha256(cid) == meta["chunks"][0]["sha256"] for h in cluster.holders(cid))
    assert (await client.scrub())["corrupt"] == 0
    assert (await client.status())["scrub"]["corrupt"] == 1


@pytest.mark.asyncio
async def test_scrub_detects_deleted_chunk_file(cluster, sample_file):
    client = cluster.client()
    await client.upload(sample_file, chunk_size=CHUNK)
    meta = (await call(*cluster.tracker_addr, {"command": "GET_FILE", "name": sample_file.name}))["file"]
    cid = _chunk_ids(meta)[1]
    cluster.holders(cid)[0].storage.delete(cid)
    assert (await client.scrub())["missing"] == 1
    await cluster.wait_for(lambda: len(cluster.holders(cid)) == 3)


@pytest.mark.asyncio
async def test_background_scrub_loop_runs(tmp_path, sample_file):
    c = Cluster(tmp_path / "c", n_nodes=3)
    await c.start()
    try:
        c.tracker.scrub_interval = 0.1
        c.tracker._spawn(c.tracker._scrub_loop())
        await c.client().upload(sample_file, chunk_size=CHUNK)
        await c.wait_for(lambda: c.tracker.scrub_stats["checked"] > 0)
    finally:
        await c.stop()
