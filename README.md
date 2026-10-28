# VektorFS

A fault-tolerant distributed file system in Python: files are split into
chunks, replicated across storage nodes, verified with SHA-256 end to end, and
automatically repaired when nodes fail.

`Client -> Tracker -> Storage Nodes`, built on `asyncio` and a custom framed TCP
protocol (4-byte length + JSON). The runtime has **no third-party dependencies**;
PostgreSQL metadata storage is optional.

**Features:** chunking and parallel transfer - configurable replication factor -
heartbeat failure detection - automatic re-replication - peer-to-peer repair -
per-chunk and whole-file SHA-256 - background integrity scrubber - atomic writes -
path-traversal protection and input limits - JSON or PostgreSQL metadata - Docker Compose cluster.

Docs: [Architecture](docs/ARCHITECTURE.md) - [Protocol](docs/PROTOCOL.md) -
[Design decisions & limitations](docs/DESIGN_DECISIONS.md) - [Testing](docs/TESTING.md) -
[Benchmarks](docs/BENCHMARKS.md) - [Proposal traceability](docs/TRACEABILITY.md)

## Quick start (PowerShell)

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m pytest                       # run the test suite

python scripts/demo.py                 # 1-minute guided fault-tolerance demo
```

### Run a cluster by hand

```powershell
python -m tracker.server --port 9100 --ttl 10 --metadata-file .\data\meta.json --scrub-interval 60
1..4 | % { Start-Process python "-m node.server --port $(9000+$_) --storage-dir .\data\node$_ --node-id node$_ --tracker 127.0.0.1:9100 --advertise-host 127.0.0.1" }

python -m client.cli upload .\file.bin
python -m client.cli ls
python -m client.cli status            # nodes, health, chunk counts  (--json for raw)
python -m client.cli download file.bin .\out.bin
python -m client.cli scrub             # audit every replica against its SHA-256 now
python -m client.cli rm file.bin
```

### Docker (PostgreSQL + tracker + 4 nodes)

```powershell
docker compose up -d --build
mkdir transfer; copy file.bin transfer\
docker compose run --rm client upload /transfer/file.bin
docker compose kill node2                         # crash a node
docker compose run --rm client status             # after ~10s: node2 DEAD, chunks re-replicated
docker compose run --rm client download file.bin /transfer/out.bin
docker compose start node2                        # re-registers, keeps its volume
docker compose restart tracker                    # metadata survives (PostgreSQL)
```

## Configuration

| Option | Where | Meaning |
|---|---|---|
| `--replication-factor N` | tracker | copies per chunk (default 3) |
| `--ttl S` | tracker | seconds without heartbeat before a node is DEAD (default 10) |
| `--metadata-file PATH` | tracker | JSON metadata store (parent folders are created) |
| `--postgres-dsn DSN` / `$VEKTORFS_PG_DSN` | tracker | PostgreSQL metadata store (needs `pip install -r requirements-postgres.txt`) |
| `--scrub-interval S` | tracker | background integrity audit period, 0 = off |
| `--storage-dir`, `--node-id`, `--tracker`, `--advertise-host` | node | see `python -m node.server --help` |

## Evaluation

```powershell
pip install -r requirements-bench.txt
python -m benchmarks.bench all         # throughput, distribution, recovery, integrity (real processes)
python -m benchmarks.plot              # charts -> benchmarks/results/*.png
```

## Layout

```
common/   protocol, message contracts, chunking, hashing, RPC helper
node/     StorageManager (disk), TCP server, tracker agent
tracker/  ClusterState (pure logic), TCP server, metadata stores
client/   VektorClient + CLI
tests/    unit, component, cluster-integration, real-process e2e
benchmarks/  harness, experiments, plots      scripts/  demo
docs/     architecture, protocol, decisions, testing, results
```
