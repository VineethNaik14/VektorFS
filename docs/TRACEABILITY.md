# Proposal -> implementation traceability

Maps every commitment in the approved mini-project proposal (Team AFK / 13) to
the code that implements it and the test or experiment that demonstrates it.

| Proposal commitment | Implementation | Evidence |
|---|---|---|
| Divide files into chunks, distribute across nodes | `common/chunking.py`, `tracker/state.py::allocate`, `client/client.py::upload` | `test_chunking`, `test_cluster_integration`, `benchmarks distribution` |
| SHA-256 integrity of stored and transferred chunks | `common/hashing.py`; checks in `node/server.py`, `client/client.py`; scrubber | `test_node_integrity`, `test_hardening::test_scrub_*`, `benchmarks integrity` |
| Metadata management | `tracker/state.py`, `tracker/store.py` | `test_tracker_state`, `test_hardening`, `test_postgres_store` |
| File reassembly | `client/client.py::download` (`.part` + whole-file hash + atomic rename) | `test_cluster_integration`, `test_e2e_processes` |
| Central tracker: active nodes + chunk locations | `tracker/server.py`, `node/agent.py` | `test_tracker_server`, `test_tracker_state` |
| Heartbeat mechanism / failure detection | `node/agent.py`, `ClusterState.expire_dead_nodes`, `TrackerServer._failure_detector` | `test_cluster_integration`, `benchmarks recovery` |
| Peer-to-peer chunk transfer | `REPLICATE` handler in `node/server.py` (node pulls from node) | `test_cluster_integration` |
| Chunk replication (RF) | `ClusterState.plan_replication`, `TrackerServer._replication_loop` | `test_cluster_integration`, `benchmarks recovery` |
| Parallel multi-peer transfer | `asyncio.gather` + semaphore in upload/download; replica chosen at random per chunk | `benchmarks throughput` |
| Automatic retry & recovery from node/chunk failures | `_fetch_chunk` retry + replica refresh; `_store_on`; tracker re-replication; node re-registration | `test_cluster_integration`, `test_e2e_processes` |
| asyncio + TCP sockets backend, CLI frontend | whole project; `client/cli.py` | `test_cli` |
| PostgreSQL | `tracker/store.py::PostgresStore`, `docker-compose.yml` | `test_postgres_store` (4 tests, run against PostgreSQL 16) |
| pytest, pytest-asyncio | `tests/`, `pytest.ini` | 131 tests |
| pandas, matplotlib | `benchmarks/plot.py` | `benchmarks/results/*.png` |
| Docker Compose multi-node environment | `Dockerfile`, `docker-compose.yml` (Postgres + tracker + 4 nodes) | see README "Docker" |
| Evaluation: transfer performance, storage distribution, integrity, failure/recovery | `benchmarks/bench.py` | `docs/BENCHMARKS.md` |
| Documentation, architecture diagrams | `docs/` | this folder |
