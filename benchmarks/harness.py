"""Launch a REAL multi-process VektorFS cluster (1 tracker + N node processes).

Used by the benchmarks and the demo. Separate OS processes (not in-process
tasks) so nodes genuinely run in parallel and `kill()` is a true SIGKILL.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from client.client import VektorClient

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ProcessCluster:
    def __init__(self, n_nodes: int = 4, rf: int = 3, ttl: float = 3.0,
                 workdir: Path | None = None, scrub_interval: float = 0.0):
        self.n_nodes, self.rf, self.ttl, self.scrub_interval = n_nodes, rf, ttl, scrub_interval
        self.dir = Path(workdir or tempfile.mkdtemp(prefix="vektorfs-"))
        self.tracker_port = free_port()
        self.node_ports = {f"node{i + 1}": free_port() for i in range(n_nodes)}
        self.procs: dict[str, subprocess.Popen] = {}
        self._logs = []
        self.env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONUNBUFFERED": "1"}

    # -- process control -----------------------------------------------------
    def _spawn(self, name: str, *args: str) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        log = open(self.dir / f"{name}.log", "ab")
        self._logs.append(log)
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-m", *args], cwd=ROOT, env=self.env, stdout=log, stderr=log)

    def node_dir(self, node_id: str) -> Path:
        return self.dir / node_id

    def start_node(self, node_id: str) -> None:
        self._spawn(node_id, "node.server", "--port", str(self.node_ports[node_id]),
                    "--storage-dir", str(self.node_dir(node_id)), "--node-id", node_id,
                    "--tracker", f"127.0.0.1:{self.tracker_port}", "--advertise-host", "127.0.0.1")

    def kill_node(self, node_id: str) -> None:
        """SIGKILL / TerminateProcess: no shutdown code runs, like a real crash."""
        p = self.procs[node_id]
        p.kill()
        p.wait(timeout=10)

    def client(self, **kw) -> VektorClient:
        return VektorClient("127.0.0.1", self.tracker_port, **kw)

    async def start(self) -> "ProcessCluster":
        args = ["tracker.server", "--port", str(self.tracker_port),
                "--replication-factor", str(self.rf), "--ttl", str(self.ttl),
                "--metadata-file", str(self.dir / "meta.json"), "--replication-interval", "0.3"]
        if self.scrub_interval:
            args += ["--scrub-interval", str(self.scrub_interval)]
        self._spawn("tracker", *args)
        await self._wait_port(self.tracker_port)
        for node_id in self.node_ports:
            self.start_node(node_id)
        await self.wait_for(self.alive_count(self.n_nodes), 30, "all nodes registered")
        return self

    async def _wait_port(self, port: int, timeout: float = 15.0) -> None:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                socket.create_connection(("127.0.0.1", port), 0.5).close()
                return
            except OSError:
                await asyncio.sleep(0.1)
        raise TimeoutError(f"port {port} never opened; see logs in {self.dir}")

    def alive_count(self, n: int):
        async def check() -> bool:
            try:
                st = await self.client(timeout=2).status()
            except Exception:       # noqa: BLE001 - not up yet
                return False
            return sum(x["alive"] for x in st["nodes"]) == n
        return check

    async def wait_for(self, pred, timeout: float = 30.0, what: str = "condition") -> float:
        """Poll an async predicate; returns seconds waited."""
        t0 = time.monotonic()
        while True:
            if await pred():
                return time.monotonic() - t0
            if time.monotonic() - t0 > timeout:
                raise TimeoutError(f"timed out waiting for: {what}")
            await asyncio.sleep(0.1)

    async def stop(self, keep_data: bool = False) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
                p.wait(timeout=10)
        for log in self._logs:
            log.close()
        if not keep_data:
            shutil.rmtree(self.dir, ignore_errors=True)
