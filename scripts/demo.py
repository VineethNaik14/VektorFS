"""Guided live demo of VektorFS fault tolerance (real processes, ~1 minute).

    python scripts/demo.py            # from the repository root

Shows: upload -> chunk placement -> kill a node -> reads still work ->
automatic re-replication -> silent corruption -> scrub heals it -> delete.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from benchmarks.harness import ProcessCluster          # noqa: E402
from client.cli import format_status                    # noqa: E402
from common.hashing import sha256_file                  # noqa: E402

MIB = 1024 * 1024


def step(text: str) -> None:
    print(f"\n\033[1;36m==> {text}\033[0m")


async def main() -> None:
    work = Path(tempfile.mkdtemp(prefix="vektorfs-demo-"))
    src = work / "report.bin"
    src.write_bytes(os.urandom(12 * MIB))
    original = sha256_file(str(src))

    step("Starting 1 tracker + 4 storage-node processes (RF=3, heartbeat TTL=3s)")
    cluster = await ProcessCluster(n_nodes=4, rf=3, ttl=3.0, workdir=work / "cluster").start()
    client = cluster.client(concurrency=4, timeout=30)
    try:
        step("Uploading a 12 MiB file as 1 MiB chunks, each stored on 3 nodes")
        r = await client.upload(src, "report.bin", chunk_size=MIB)
        print(f"  {r.chunks} chunks, replicas per chunk: {r.replicas_per_chunk}")
        print(format_status(await client.status()))

        step("CRASH: SIGKILL node2 (no clean shutdown)")
        cluster.kill_node("node2")
        step("Reading the file immediately - before the tracker even notices")
        d = await client.download("report.bin", work / "during.bin")
        print(f"  download OK, SHA-256 verified: {d.sha256 == original}")

        step("Waiting for the failure detector + automatic re-replication")
        async def healed() -> bool:
            st = await client.status()
            dead = any(n["node_id"] == "node2" and not n["alive"] for n in st["nodes"])
            return dead and st["chunks"]["under_replicated"] == 0
        secs = await cluster.wait_for(healed, 60, "recovery")
        print(f"  node2 declared DEAD and every chunk is back to 3 copies after {secs:.1f}s")
        print(format_status(await client.status()))

        step("SILENT CORRUPTION: flip a byte in a stored chunk on node1")
        victim = next(p for p in cluster.node_dir("node1").iterdir() if not p.name.startswith("."))
        raw = bytearray(victim.read_bytes()); raw[100] = (raw[100] + 1) % 256; victim.write_bytes(bytes(raw))
        found = await client.scrub()
        print(f"  scrub result: {found['corrupt']} corrupt replica(s) detected and dropped")
        await cluster.wait_for(
            lambda: _all_healthy(client), 30, "heal after scrub")
        d = await client.download("report.bin", work / "after.bin")
        print(f"  download OK after healing, SHA-256 verified: {d.sha256 == original}")

        step("Restarting node2: it re-registers and reports the chunks on its disk")
        cluster.start_node("node2")
        await cluster.wait_for(cluster.alive_count(4), 30, "node2 back")
        print(format_status(await client.status()))

        step("Deleting the file: metadata removed, chunks garbage-collected")
        await client.delete("report.bin")
        print("  ls ->", await client.list_files())
        print("\n\033[1;32mDemo complete.\033[0m")
    finally:
        await cluster.stop()


async def _all_healthy(client) -> bool:
    c = (await client.status())["chunks"]
    return c["under_replicated"] == 0 and c["lost"] == 0


if __name__ == "__main__":
    asyncio.run(main())
