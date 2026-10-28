"""PostgreSQL metadata store. Skipped unless VEKTORFS_TEST_PG_DSN is set, e.g.
  $env:VEKTORFS_TEST_PG_DSN = "postgresql://postgres:vektor@localhost:5432/postgres"
"""

import os
import uuid

import pytest

DSN = os.environ.get("VEKTORFS_TEST_PG_DSN")
pytestmark = [pytest.mark.postgres,
              pytest.mark.skipif(not DSN, reason="VEKTORFS_TEST_PG_DSN not set")]

from tests.cluster import Cluster          # noqa: E402


def _record(name="a.bin", n=3):
    return {"name": name, "file_id": uuid.uuid4().hex, "size": 30, "chunk_size": 10,
            "sha256": "ab" * 32,
            "chunks": [{"chunk_id": f"{name}_{i}", "index": i, "size": 10, "sha256": "cd" * 32}
                       for i in range(n)]}


@pytest.fixture
def store():
    from tracker.store import PostgresStore
    s = PostgresStore(DSN)
    s._with_conn(lambda c: c.execute("TRUNCATE files CASCADE"))
    yield s
    s.close()


def test_roundtrip_preserves_order_and_hashes(store):
    rec = _record()
    store.put_file(rec)
    loaded = store.load()
    assert loaded == [rec]


def test_put_replaces_and_delete_cascades(store):
    store.put_file(_record("a", 3))
    store.put_file({**_record("a", 1)})
    assert len(store.load()[0]["chunks"]) == 1
    store.delete_file("a")
    assert store.load() == []
    n = store._with_conn(lambda c: c.execute("SELECT count(*) FROM chunks").fetchone()[0])
    assert n == 0


def test_reconnects_after_connection_drop(store):
    store.put_file(_record("a"))
    store._conn.close()                      # simulate a dropped link
    assert len(store.load()) == 1


@pytest.mark.asyncio
async def test_tracker_restart_with_postgres(tmp_path, sample_file):
    from tracker.store import PostgresStore
    PostgresStore(DSN)._with_conn(lambda c: c.execute("TRUNCATE files CASCADE"))
    c = Cluster(tmp_path / "c", n_nodes=3)
    c.store_factory = lambda: PostgresStore(DSN)
    await c.start()
    try:
        await c.client().upload(sample_file, chunk_size=65536)
        await c.restart_tracker()
        await c.wait_for(lambda: len(c.tracker.state.alive_nodes()) == 3)
        out = tmp_path / "o.bin"
        await c.client().download(sample_file.name, out)
        assert out.read_bytes() == sample_file.read_bytes()
    finally:
        await c.stop()
