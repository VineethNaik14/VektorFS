# VektorFS Wire Protocol

## Framing

```
+----------------------+---------------------------+
| 4 bytes, big-endian  | UTF-8 JSON object         |
| payload length N     | exactly N bytes           |
+----------------------+---------------------------+
```

* `N` must be <= **16 MiB** (`MAX_MESSAGE_SIZE`). A larger declared length gets
  `INVALID_REQUEST` and the connection is closed (the stream can no longer be
  trusted). Bad JSON gets `INVALID_REQUEST` but the connection stays usable.
* Chunk bytes travel base64-encoded in the `data` field. Chunks are <= 8 MiB raw
  (default 4 MiB), which stays below the frame cap after the ~33% base64 growth.
* Idle/stalled peers are dropped after 30 s on reads and writes.
* Every request is a JSON object with a `"command"`; every response has
  `"status": "OK"` or `"status": "ERROR"` with `"error"` (code) and `"message"`.

## Storage node commands

| Command | Request fields | OK response |
|---|---|---|
| `STORE` | `chunk_id`, `data` (base64), optional `sha256` | `{}` |
| `GET` | `chunk_id` | `data` (base64) |
| `DELETE` | `chunk_id` | `{}` |
| `EXISTS` | `chunk_id` | `exists` (bool) |
| `INFO` | `chunk_id` | `chunk_id`, `size`, `sha256` (of bytes on disk) |
| `REPLICATE` | `chunk_id`, `source_host`, `source_port`, `sha256` | `{}` after pulling from the peer and verifying |

`chunk_id` must match `[A-Za-z0-9][A-Za-z0-9._-]{0,199}` (no separators, no
leading dot), and the resolved path must stay inside the storage directory.

## Tracker commands

| Command | Sender | Fields | OK response |
|---|---|---|---|
| `REGISTER` | node | `node_id`, `host` (blank = use peer IP), `port`, `chunks[]` | `heartbeat_interval`, `orphans[]` |
| `HEARTBEAT` | node | `node_id` | `{}` (`UNKNOWN_NODE` => re-register) |
| `ALLOCATE` | client | `name`, `size`, `chunk_size`, `num_chunks` | `file_id`, `chunks[{index, chunk_id, targets[{node_id,host,port}]}]` |
| `COMMIT` | client | `file_id`, `sha256`, `chunks[{index,size,sha256,replicas[]}]` | `name`, `chunks` |
| `ABORT` | client | `file_id` | `aborted` (bool) |
| `GET_FILE` | client | `name` | `file{..., chunks[{..., replicas[]}]}` |
| `DELETE_FILE` | client | `name` | `{}` |
| `LIST_FILES` | client | - | `files[]` |
| `REPORT_CORRUPT` | client | `chunk_id`, `node_id` | `removed` (bool) |
| `SCRUB` | client | - | `checked`, `corrupt`, `missing` |
| `STATUS` | client | - | `replication_factor`, `files`, `chunks{total,healthy,under_replicated,lost}`, `scrub{...}`, `nodes[]` |

Limits (`common/tracker_messages.py`): names <= 255 chars without control
characters, <= 20 000 chunks/file, chunk size <= 8 MiB, <= 200 000 chunks in a
registration report, <= 16 replicas per chunk. Every field is type- and
range-checked before use (booleans are rejected where integers are expected).

## Error codes

`INVALID_REQUEST`, `INVALID_CHUNK_ID`, `CHUNK_NOT_FOUND`, `CHECKSUM_MISMATCH`,
`SOURCE_UNAVAILABLE`, `FILE_NOT_FOUND`, `FILE_EXISTS`, `NO_NODES_AVAILABLE`,
`UNKNOWN_NODE`, `UPLOAD_NOT_FOUND`, `INSUFFICIENT_REPLICAS`, `INTERNAL_ERROR`.
Internal exceptions are logged server-side and never leaked to the peer.
