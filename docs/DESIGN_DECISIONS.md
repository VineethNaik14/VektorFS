# Design decisions, trade-offs and known limitations

## Why these choices

| Decision | Why | Cost / alternative |
|---|---|---|
| **Tracker holds metadata, data flows client <-> nodes directly** | The tracker never carries bulk data, so it is not a bandwidth bottleneck, and chunk transfers parallelise across nodes. | Tracker is a single point of failure for *metadata operations* (see limitations). |
| **Length-prefixed JSON frames** | Trivial to parse safely (bounded read, no delimiter scanning), debuggable with a hex dump. | base64 inflates chunk payloads ~33%; a binary body would be faster. |
| **One connection per RPC** (`common/rpc.py`) | Any exception means "this attempt failed"; no half-broken pooled sockets to reason about. | Connect cost per call; a pool is the first optimisation if benchmarks demand it. |
| **`ClusterState` is synchronous and never awaits** | Each method is atomic on the event loop, so the tracker needs **no locks** and the logic is unit-testable with a fake clock. | Large scans (e.g. `plan_replication`) block the loop briefly. |
| **Replica locations are not persisted** | Nodes know what is on their disks; a stored location list could only be stale. Rebuilt from chunk reports on registration. | After a tracker restart, reads wait until nodes re-register (~one heartbeat). |
| **TTL heartbeat failure detector** (nodes beat every TTL/4) | Simple, tolerates losing 3 consecutive beats. | A slow-but-alive node can be declared dead; it simply re-registers. |
| **Commit with >=1 replica, repair afterwards** | Uploads survive a node dying mid-upload. | Briefly under-replicated data. |
| **Verify SHA-256 before storing a replicated chunk** | Prevents spreading silent corruption from a bad source. | One extra hash per replication. |
| **Atomic chunk writes** (tmp + fsync + rename) | A crash never leaves a truncated file that looks valid. | fsync latency per chunk. |
| **Persist-then-apply for deletes, apply-then-persist-with-rollback for commits** | A failed DB write can neither resurrect a deleted file nor expose a non-durable one. | More code than a naive "save after". |
| **PostgreSQL optional, JSON default** | Core stays dependency-free and trivial to run; Postgres adds transactional durability for the Docker deployment. | Two code paths (both covered by the same tests). |
| **Least-loaded placement with random tie-break** | Even distribution without coordination (measured in `BENCHMARKS.md`). | Ignores disk capacity and rack/zone topology. |

## Security model

What the code defends against:

* **Path traversal**: chunk ids are validated against a strict regex *and* the
  resolved path must stay under the storage directory (defence in depth).
* **Resource exhaustion**: 16 MiB frame cap, per-field limits, read/write
  timeouts, bounded replication and client concurrency.
* **Malformed input**: every message is parsed by a validating function;
  internal errors are never echoed to the peer.
* **Data tampering/corruption**: SHA-256 at four layers (see ARCHITECTURE).

## Known limitations (deliberate scope boundaries)

1. **No authentication or TLS.** Any host that can reach the ports can read,
   write or delete chunks and can register as a node. A node's `REPLICATE`
   will connect to any `host:port` it is told to (SSRF-style). Run on a trusted
   network / Docker network only. *Mitigation path:* a shared cluster token in
   every request and TLS on the sockets.
2. **Single tracker.** Chunk reads/writes in flight survive a tracker outage
   only until the client needs the tracker again. Metadata is durable (Postgres
   or JSON) and a restarted tracker recovers fully, but there is no hot standby.
   *Mitigation path:* tracker replication (e.g. Raft) is out of scope.
3. **Over-replication is not trimmed.** If a dead node returns after its chunks
   were re-replicated elsewhere, those chunks have RF+1 copies until deleted.
4. **Replication is by whole chunk, no erasure coding**; storage overhead = RF.
5. **No rebalancing** of existing chunks when a *new* node joins (new uploads
   do favour it because placement is least-loaded).
6. **Flat namespace**: file names are opaque strings, no directories/permissions.
7. **Chunk transfer is in-memory per chunk** (<= 8 MiB x concurrency), not streamed.
8. **A tracker-side 20 000-chunk/file cap** bounds the `GET_FILE` reply (64 GiB at 4 MiB chunks).
