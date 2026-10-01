import pytest

from common.messages import ErrorCode
from tracker.state import ClusterState, StateError

SHA = "a" * 64


class Clock:
    def __init__(self): self.t = 0.0
    def __call__(self): return self.t


def make(n_nodes=4, rf=3, ttl=10.0):
    clock = Clock()
    s = ClusterState(rf, ttl, clock=clock)
    for i in range(n_nodes):
        s.register_node(f"n{i}", "h", 1000 + i, [])
    return s, clock


def upload(s, name="f", chunks=2, size=None):
    size = size if size is not None else chunks * 10
    p = s.allocate(name, size, 10, chunks)
    items = [{"index": i, "size": 10, "sha256": SHA, "replicas": p.assigned[cid]}
             for i, cid in enumerate(p.chunk_ids)]
    return s.commit(p.file_id, SHA, items)


def test_allocate_assigns_rf_distinct_nodes_and_balances_load():
    s, _ = make(4)
    p = s.allocate("f", 400, 10, 40)
    assert all(len(set(v)) == 3 for v in p.assigned.values())
    counts = {}
    for v in p.assigned.values():
        for n in v: counts[n] = counts.get(n, 0) + 1
    assert max(counts.values()) - min(counts.values()) <= 1


def test_allocate_caps_replicas_at_live_nodes_and_needs_one_node():
    s, _ = make(2)
    assert all(len(v) == 2 for v in s.allocate("f", 10, 10, 1).assigned.values())
    with pytest.raises(StateError) as e:
        ClusterState().allocate("f", 10, 10, 1)
    assert e.value.code is ErrorCode.NO_NODES_AVAILABLE


def test_allocate_validates_chunk_count_and_duplicate_name():
    s, _ = make()
    with pytest.raises(StateError):
        s.allocate("f", 100, 10, 3)
    upload(s)
    with pytest.raises(StateError) as e:
        s.allocate("f", 10, 10, 1)
    assert e.value.code is ErrorCode.FILE_EXISTS


def test_ttl_failure_detector_with_fake_clock():
    s, clock = make(3, ttl=10)
    upload(s)
    clock.t = 5; s.heartbeat("n0"); s.heartbeat("n1")
    clock.t = 11
    assert s.expire_dead_nodes() == ["n2"]
    assert not s.nodes["n2"].alive
    assert all("n2" not in c.replicas for c in s.chunk_index.values())
    assert s.heartbeat("n2") is False          # must re-register
    assert s.expire_dead_nodes() == []         # not reported twice


def test_plan_replication_restores_rf_from_surviving_source():
    s, clock = make(4)
    upload(s, chunks=3)
    victim = next(iter(s.chunk_index.values())).replicas.copy().pop()
    clock.t = 11
    for n in s.nodes:
        if n != victim: s.heartbeat(n)
    assert s.expire_dead_nodes() == [victim]
    tasks = s.plan_replication(set())
    assert tasks and all(t.target.node_id != victim and t.source.node_id != victim for t in tasks)
    for t in tasks:
        assert t.target.node_id not in s.chunk_index[t.chunk_id].replicas
        s.add_replica(t.chunk_id, t.target.node_id)
    assert s.plan_replication(set()) == []
    assert all(len(c.replicas) == 3 for c in s.chunk_index.values())


def test_plan_replication_respects_inflight_and_skips_lost_chunks():
    s, clock = make(4, rf=3)
    upload(s, chunks=1)
    cid = next(iter(s.chunk_index))
    s.remove_replica(cid, sorted(s.chunk_index[cid].replicas)[0])
    first = s.plan_replication(set())
    assert len(first) == 1
    assert s.plan_replication({(cid, first[0].target.node_id)}) == []   # already scheduled
    for n in list(s.chunk_index[cid].replicas):
        s.remove_replica(cid, n)
    assert s.plan_replication(set()) == [] and s.chunk_health()["lost"] == 1


def test_register_is_authoritative_and_reports_orphans():
    s, _ = make(3)
    rec = upload(s, chunks=1)
    cid = rec.chunks[0].chunk_id
    holder = sorted(rec.chunks[0].replicas)[0]
    assert s.register_node(holder, "h", 1, [cid, "ghost_chunk"]) == ["ghost_chunk"]
    assert holder in s.chunk_index[cid].replicas
    s.register_node(holder, "h", 1, [])                 # wiped disk
    assert holder not in s.chunk_index[cid].replicas


def test_pending_upload_chunks_are_not_orphans():
    s, _ = make(3)
    p = s.allocate("f", 10, 10, 1)
    assert s.register_node("n0", "h", 1, [p.chunk_ids[0]]) == []


def test_commit_rejects_bad_input_without_side_effects():
    s, _ = make(3)
    p = s.allocate("f", 20, 10, 2)
    good = [{"index": i, "size": 10, "sha256": SHA, "replicas": ["n0"]} for i in range(2)]
    for bad in (good[:1], [good[0], dict(good[0])],
                [dict(g, replicas=["nobody"]) for g in good],
                [dict(g, size=11) for g in good]):
        with pytest.raises(StateError):
            s.commit(p.file_id, SHA, bad)
    assert "f" not in s.files and not s.chunk_index
    s.commit(p.file_id, SHA, good)
    with pytest.raises(StateError):
        s.commit(p.file_id, SHA, good)                  # second commit: upload consumed


def test_delete_file_returns_gc_targets():
    s, _ = make(3)
    upload(s, chunks=2)
    todo = s.delete_file("f")
    assert len(todo) == 6 and not s.files and not s.chunk_index
    assert all(not n.chunks for n in s.nodes.values())


def test_persistence_roundtrip_keeps_metadata_but_not_locations(tmp_path):
    s, _ = make(3)
    upload(s, chunks=2)
    s.save(tmp_path / "m.json")
    s2 = ClusterState(); s2.load(tmp_path / "m.json")
    assert s2.files["f"].chunks[1].sha256 == SHA
    assert all(not c.replicas for c in s2.chunk_index.values())
