# Testing

```powershell
python -m pytest                       # everything (PostgreSQL tests skip without a DSN)
python -m pytest -m "not e2e"          # skip the real-process test (~faster)
$env:VEKTORFS_TEST_PG_DSN = "postgresql://postgres:vektor@localhost:5432/postgres"
python -m pytest -m postgres           # PostgreSQL store only
```

Three levels, from cheap to realistic:

| Level | Files | What it proves |
|---|---|---|
| Unit (no network) | `test_storage`, `test_chunking`, `test_hashing`, `test_messages`, `test_protocol`, `test_tracker_state` | path-traversal safety, atomic writes, framing limits, placement/repair logic with a fake clock |
| Component over real TCP | `test_server`, `test_tracker_server`, `test_node_integrity`, `test_cli` | timeouts, malformed frames, oversized messages, error mapping, CLI |
| Cluster integration | `test_cluster_integration`, `test_hardening`, `test_postgres_store` | in-process tracker + 4 nodes: node death, re-replication, corruption, tracker restart, failed-upload cleanup, scrubber, store failure rollback |
| End to end | `test_e2e_processes` | real OS processes + `SIGKILL`, restart, on-disk corruption |

`tests/cluster.py` provides the in-process cluster (`kill()` stops heartbeats and
sockets without any cleanup, simulating a crash). Fault-injection experiments with
real processes live in `benchmarks/` (see BENCHMARKS.md).

## Regression tests added after the audit

| Bug found | Test |
|---|---|
| `--metadata-file` in a missing directory made the first upload fail with `INTERNAL_ERROR` *after* mutating tracker memory | `test_metadata_file_in_missing_directory_is_created`, `test_unwritable_metadata_path_fails_at_startup` |
| Failed upload left orphan chunks on nodes and a phantom reservation skewing placement | `test_failed_upload_leaves_no_orphans_and_no_reservation`, `test_abort_command_is_idempotent` |
| Commit visible but not durable if the metadata write failed | `test_commit_is_rolled_back_when_store_fails`, `test_json_store_keeps_old_state_if_write_fails` |
| Silent disk corruption only found if a client happened to read it | `test_scrub_*` |
