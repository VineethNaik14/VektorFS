"""VektorFS benchmarks (real processes over loopback TCP).

  python -m benchmarks.bench throughput|distribution|recovery|integrity|all [--quick]

Writes CSVs to benchmarks/results/. Charts: python -m benchmarks.plot
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import statistics
import tempfile
import time
from pathlib import Path

from benchmarks.harness import ProcessCluster
from client.client import DownloadError
from common.hashing import sha256_file

MIB = 1024 * 1024
RESULTS = Path(__file__).parent / "results"


def write_csv(name: str, rows: list[dict]) -> None:
    RESULTS.mkdir(exist_ok=True)
    with (RESULTS / f"{name}.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"  -> benchmarks/results/{name}.csv ({len(rows)} rows)")


def make_file(path: Path, size: int) -> Path:
    with path.open("wb") as fh:
        for _ in range(size // MIB):
            fh.write(os.urandom(MIB))
        fh.write(os.urandom(size % MIB))
    return path


async def healthy(cluster: ProcessCluster) -> bool:
    c = (await cluster.client(timeout=3).status())["chunks"]
    return c["total"] > 0 and c["under_replicated"] == 0 and c["lost"] == 0


# --------------------------------------------------------------- 1. throughput
async def throughput(quick: bool) -> None:
    size = (16 if quick else 64) * MIB
    reps = 2 if quick else 3
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        src = make_file(Path(tmp) / "src.bin", size)
        for rf in (1, 3):
            cluster = await ProcessCluster(n_nodes=4, rf=rf).start()
            try:
                for chunk_mib in (1, 4):
                    for conc in (1, 2, 4, 8):
                        for rep in range(reps):
                            client = cluster.client(concurrency=conc, timeout=60)
                            name = f"f-{rf}-{chunk_mib}-{conc}-{rep}"
                            t0 = time.perf_counter()
                            await client.upload(src, name, chunk_size=chunk_mib * MIB)
                            up = time.perf_counter() - t0
                            out = Path(tmp) / "out.bin"
                            t0 = time.perf_counter()
                            await client.download(name, out)      # verifies SHA-256 inside
                            down = time.perf_counter() - t0
                            await client.delete(name)
                            rows.append(dict(rf=rf, chunk_mib=chunk_mib, concurrency=conc, rep=rep,
                                             size_mib=size // MIB, upload_s=round(up, 3),
                                             download_s=round(down, 3),
                                             upload_mib_s=round(size / MIB / up, 1),
                                             download_mib_s=round(size / MIB / down, 1)))
                            print(f"  rf={rf} chunk={chunk_mib}MiB conc={conc}: "
                                  f"up {rows[-1]['upload_mib_s']} MiB/s, down {rows[-1]['download_mib_s']} MiB/s")
            finally:
                await cluster.stop()
    write_csv("throughput", rows)


# ------------------------------------------------------------- 2. distribution
async def distribution(quick: bool) -> None:
    rows = []
    n_files = 12 if quick else 40
    with tempfile.TemporaryDirectory() as tmp:
        for n_nodes, rf in ((4, 3), (6, 2)):
            cluster = await ProcessCluster(n_nodes=n_nodes, rf=rf).start()
            try:
                client = cluster.client(concurrency=8, timeout=60)
                for i in range(n_files):
                    src = make_file(Path(tmp) / "s.bin", (1 + i % 6) * MIB)
                    await client.upload(src, f"file{i}", chunk_size=MIB)
                st = await client.status()
                counts = [n["chunks"] for n in st["nodes"]]
                mean = statistics.mean(counts)
                cv = statistics.pstdev(counts) / mean if mean else 0.0
                for n in sorted(st["nodes"], key=lambda n: n["node_id"]):
                    rows.append(dict(config=f"{n_nodes} nodes, RF={rf}", node=n["node_id"],
                                     chunks=n["chunks"], cv=round(cv, 4)))
                print(f"  {n_nodes} nodes RF={rf}: chunks/node={counts} CV={cv:.3f}")
            finally:
                await cluster.stop()
    write_csv("distribution", rows)


# ------------------------------------------------------------------ 3. recovery
async def recovery(quick: bool) -> None:
    rows, ttl = [], 3.0
    trials = 2 if quick else 4
    with tempfile.TemporaryDirectory() as tmp:
        src = make_file(Path(tmp) / "src.bin", (16 if quick else 64) * MIB)
        want = sha256_file(str(src))
        for trial in range(trials):
            cluster = await ProcessCluster(n_nodes=4, rf=3, ttl=ttl).start()
            try:
                client = cluster.client(concurrency=8, timeout=60)
                await client.upload(src, "big", chunk_size=MIB)
                victim = f"node{trial % 4 + 1}"
                watcher_client = cluster.client(timeout=3)
                t_kill = time.perf_counter()
                cluster.kill_node(victim)

                async def watch() -> tuple[float, float]:
                    """Timestamps (s after the kill) when the node is declared DEAD and when
                    every chunk is back to full replication. Runs CONCURRENTLY with the
                    download below, so slow reads cannot distort the recovery timings."""
                    t_dead = None
                    while True:
                        st = await watcher_client.status()
                        now = time.perf_counter() - t_kill
                        dead = any(n["node_id"] == victim and not n["alive"] for n in st["nodes"])
                        if dead and t_dead is None:
                            t_dead = now
                        c = st["chunks"]
                        if dead and c["under_replicated"] == 0 and c["lost"] == 0:
                            return t_dead, now
                        await asyncio.sleep(0.05)

                watcher = asyncio.create_task(watch())
                # Availability DURING the outage: read straight away, before detection.
                out = Path(tmp) / "during.bin"
                t0 = time.perf_counter()
                res = await client.download("big", out)
                during_s, ok = time.perf_counter() - t0, res.sha256 == want
                t_detect, t_heal = await asyncio.wait_for(watcher, 120)
                rows.append(dict(trial=trial + 1, victim=victim, ttl_s=ttl,
                                 download_during_outage_ok=ok, download_during_outage_s=round(during_s, 2),
                                 detect_s=round(t_detect, 2), fully_rereplicated_s=round(t_heal, 2)))
                print(f"  trial {trial + 1}: killed {victim}; read ok={ok}; detected {t_detect:.1f}s; "
                      f"back to RF=3 after {t_heal:.1f}s")
            finally:
                await cluster.stop()
    write_csv("recovery", rows)


# ------------------------------------------------------------------ 4. integrity
async def integrity(quick: bool) -> None:
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        src = make_file(Path(tmp) / "src.bin", 16 * MIB)
        want = sha256_file(str(src))
        cluster = await ProcessCluster(n_nodes=4, rf=3).start()
        try:
            client = cluster.client(concurrency=4, timeout=30)
            await client.upload(src, "f", chunk_size=MIB)
            meta = (await client.status())
            chunk_files = {}                       # chunk_id -> [paths of replicas on disk]
            for node_id in cluster.node_ports:
                for p in cluster.node_dir(node_id).iterdir():
                    if not p.name.startswith("."):
                        chunk_files.setdefault(p.name, []).append(p)

            def corrupt(path: Path) -> None:
                raw = bytearray(path.read_bytes()); raw[len(raw) // 2] = (raw[len(raw) // 2] + 1) % 256   # never self-reverts
                path.write_bytes(bytes(raw))

            # A) SCRUB path: silent bit-rot, no client involved. The tracker audits
            #    replicas against recorded SHA-256s and re-replicates good copies.
            for paths in chunk_files.values():
                corrupt(paths[0])
            t0 = time.perf_counter()
            found = await client.scrub()
            await cluster.wait_for(lambda: healthy(cluster), 60, "heal after scrub")
            rows.append(dict(scenario="scrubber: 1 corrupt replica in every chunk",
                             corrupt_replicas=found["corrupt"], download_ok=True, corrupt_skipped=0,
                             detail=f"all detected; back to RF=3 in {time.perf_counter() - t0:.1f}s"))
            # B) READ path: corruption nobody has scrubbed yet. The client verifies
            #    every chunk, skips the bad replica and reports it.
            for paths in chunk_files.values():
                corrupt(next(q for q in paths if q.exists()))
            res = await client.download("f", Path(tmp) / "a.out")
            rows.append(dict(scenario="client read: 1 corrupt replica in every chunk",
                             corrupt_replicas=len(chunk_files), download_ok=res.sha256 == want,
                             corrupt_skipped=res.corrupt_replicas_skipped,
                             detail="served from good replicas; bad ones reported"))
            await cluster.wait_for(lambda: healthy(cluster), 60, "heal after reported corruption")
            # C) ALL replicas of one chunk corrupt -> must FAIL LOUDLY, never return bad data.
            victim_chunk, paths = next(iter(chunk_files.items()))
            for node_id in cluster.node_ports:
                pth = cluster.node_dir(node_id) / victim_chunk
                if pth.exists():
                    corrupt(pth)
            try:
                res = await client.download("f", Path(tmp) / "c.out")
                ok, detail = False, "BAD DATA RETURNED"
            except DownloadError:
                ok, detail = True, "refused: no valid replica (download failed, no output file)"
            rows.append(dict(scenario="all replicas of one chunk corrupt", corrupt_replicas=len(paths),
                             download_ok=ok, corrupt_skipped=0, detail=detail))
            for r in rows:
                print(f"  {r['scenario']}: ok={r['download_ok']} ({r['detail']})")
        finally:
            await cluster.stop()
    write_csv("integrity", rows)


EXPERIMENTS = {"throughput": throughput, "distribution": distribution,
               "recovery": recovery, "integrity": integrity}


async def main() -> None:
    import logging
    logging.getLogger("vektorfs").setLevel(logging.CRITICAL)   # keep benchmark output readable
    ap = argparse.ArgumentParser()
    ap.add_argument("experiment", choices=[*EXPERIMENTS, "all"])
    ap.add_argument("--quick", action="store_true", help="smaller sizes / fewer repetitions")
    args = ap.parse_args()
    for name in (EXPERIMENTS if args.experiment == "all" else [args.experiment]):
        print(f"== {name} ==")
        await EXPERIMENTS[name](args.quick)


if __name__ == "__main__":
    asyncio.run(main())
