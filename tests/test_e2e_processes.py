"""Real multi-process test: tracker + 4 storage-node OS processes + CLI client.

Upload -> Chunk -> Replicate -> SIGKILL node -> Detect -> Re-replicate
       -> Download -> Verify SHA-256 -> restart node -> corrupt chunk -> recover
"""

import asyncio
import hashlib
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from client.client import VektorClient

pytestmark = pytest.mark.e2e
ROOT = Path(__file__).resolve().parent.parent
CHUNK = 256 * 1024
TTL = 2.0


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Procs:
    def __init__(self, tmp: Path):
        self.tmp, self.procs, self.logs = tmp, {}, []
        self.env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONUNBUFFERED": "1"}

    def spawn(self, name: str, *args: str) -> None:
        log = open(self.tmp / f"{name}.log", "ab")
        self.logs.append(log)
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-m", *args], cwd=ROOT, env=self.env, stdout=log, stderr=log)

    def kill(self, name: str) -> None:           # SIGKILL / TerminateProcess: no cleanup runs
        self.procs[name].kill()
        self.procs[name].wait(timeout=10)

    def stop_all(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.kill(); p.wait(timeout=10)
        for log in self.logs:
            log.close()


async def wait_for(pred, timeout=30.0, what="condition"):
    end = time.monotonic() + timeout
    while True:
        try:
            if await pred():
                return
        except Exception:
            pass
        if time.monotonic() > end:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.2)


@pytest.mark.asyncio
async def test_real_process_cluster_fault_tolerance_workflow(tmp_path, monkeypatch):
    tport = free_port()
    ports = {f"node{i}": free_port() for i in range(1, 5)}
    procs = Procs(tmp_path)

    def start_node(name):
        procs.spawn(name, "node.server", "--host", "127.0.0.1", "--port", str(ports[name]),
                    "--storage-dir", str(tmp_path / name), "--node-id", name,
                    "--tracker", f"127.0.0.1:{tport}", "--advertise-host", "127.0.0.1")

    try:
        procs.spawn("tracker", "tracker.server", "--port", str(tport), "--ttl", str(TTL),
                    "--metadata-file", str(tmp_path / "meta.json"), "--replication-interval", "0.3")
        for n in ports:
            start_node(n)
        client = VektorClient("127.0.0.1", tport, timeout=10)

        async def alive(n):
            return sum(x["alive"] for x in (await client.status())["nodes"]) == n
        await wait_for(lambda: alive(4), what="4 nodes registered")

        # ---- upload (3 MiB random, 256 KiB chunks => 12 chunks) -------------
        src = tmp_path / "big.bin"
        src.write_bytes(os.urandom(3 * 1024 * 1024))
        want = hashlib.sha256(src.read_bytes()).hexdigest()
        up = await client.upload(src, "big.bin", chunk_size=CHUNK)
        assert up.chunks == 12 and up.replicas_per_chunk == [3] * 12 and up.sha256 == want

        # ---- kill a node that holds chunks ---------------------------------
        victim = next(n for n in ports if any((tmp_path / n).iterdir()))
        procs.kill(victim)

        async def detected():
            nodes = {x["node_id"]: x for x in (await client.status())["nodes"]}
            return not nodes[victim]["alive"]
        await wait_for(detected, what="failure detection")

        # ---- download straight away (maybe mid-repair) must already work ---
        mid = await client.download("big.bin", tmp_path / "mid.bin")
        assert mid.sha256 == want

        # ---- automatic re-replication back to RF=3 on survivors ------------
        async def healed():
            st = await client.status()
            return st["chunks"]["healthy"] == 12 and st["chunks"]["under_replicated"] == 0
        await wait_for(healed, what="re-replication")
        meta = (await client._tracker({"command": "GET_FILE", "name": "big.bin"}))["file"]
        for c in meta["chunks"]:
            reps = {r["node_id"] for r in c["replicas"]}
            assert len(reps) == 3 and victim not in reps
            for r in reps:                                  # ground truth on disk
                assert hashlib.sha256((tmp_path / r / c["chunk_id"]).read_bytes()).hexdigest() == c["sha256"]

        # ---- final download via the real CLI + independent SHA-256 ---------
        out = tmp_path / "cli_out.bin"
        res = subprocess.run(
            [sys.executable, "-m", "client.cli", "--tracker", f"127.0.0.1:{tport}",
             "download", "big.bin", str(out)],
            cwd=ROOT, env=procs.env, capture_output=True, text=True, timeout=60)
        assert res.returncode == 0, res.stderr
        assert hashlib.sha256(out.read_bytes()).hexdigest() == want
        assert want in res.stdout

        # ---- restart the killed node: same id + disk -> re-registers -------
        start_node(victim)
        await wait_for(lambda: alive(4), what="node re-registration")

        # ---- bit-rot one replica on disk: download must still verify -------
        # Fresh metadata: the restarted node is back in the replica lists now.
        fresh = (await client._tracker({"command": "GET_FILE", "name": "big.bin"}))["file"]
        c0 = fresh["chunks"][0]
        rotten = tmp_path / c0["replicas"][0]["node_id"] / c0["chunk_id"]
        rotten.write_bytes(b"\x00" * 100)
        # Client shuffles replicas to spread load; pin the order so the rotten
        # copy (first in the tracker's sorted list) is guaranteed to be tried.
        monkeypatch.setattr("client.client.random.sample", lambda pop, k: list(pop))
        final = await client.download("big.bin", tmp_path / "final.bin")
        assert final.sha256 == want
        assert final.corrupt_replicas_skipped == 1          # detected, not silently used
        async def repaired():
            # Bad bytes must be gone from disk: deleted (RF already satisfied by
            # the other copies) or overwritten with a verified good copy.
            bad_gone = (not rotten.exists()
                        or hashlib.sha256(rotten.read_bytes()).hexdigest() == c0["sha256"])
            return bad_gone and await healed()
        await wait_for(repaired, what="rotten replica repaired on disk")
    finally:
        procs.stop_all()
