"""In-process test cluster: 1 tracker + N storage nodes over real TCP sockets."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from client.client import VektorClient
from node.agent import TrackerAgent
from node.server import StorageNodeServer
from node.storage import StorageManager
from tracker.server import TrackerServer


class TestNode:
    __test__ = False  # not a pytest class

    def __init__(self, node_id: str, directory: Path, tracker_addr):
        self.node_id, self.dir, self.tracker_addr = node_id, directory, tracker_addr
        self.storage = StorageManager(directory)
        self.server: StorageNodeServer | None = None
        self.agent_task: asyncio.Task | None = None

    async def start(self) -> None:
        self.storage = StorageManager(self.dir)
        self.server = StorageNodeServer(self.storage, "127.0.0.1", 0)
        await self.server.start()
        agent = TrackerAgent(self.storage, self.node_id, self.tracker_addr,
                             "127.0.0.1", self.server.address[1])
        self.agent_task = asyncio.create_task(agent.run())

    async def kill(self) -> None:
        """Simulate a crash: stop heartbeating and stop answering."""
        if self.agent_task:
            self.agent_task.cancel()
            await asyncio.gather(self.agent_task, return_exceptions=True)
            self.agent_task = None
        if self.server:
            await self.server.stop()
            self.server = None

    @property
    def alive(self) -> bool:
        return self.server is not None


class Cluster:
    def __init__(self, tmp: Path, n_nodes: int = 4, ttl: float = 0.8, rf: int = 3,
                 metadata: bool = False):
        self.tmp, self.n_nodes, self.ttl, self.rf = tmp, n_nodes, ttl, rf
        self.metadata_path = tmp / "tracker.json" if metadata else None
        self.tracker: TrackerServer | None = None
        self.nodes: list[TestNode] = []

    def _new_tracker(self, port: int = 0) -> TrackerServer:
        return TrackerServer("127.0.0.1", port, replication_factor=self.rf,
                             heartbeat_ttl=self.ttl, metadata_path=self.metadata_path,
                             replication_interval=0.1)

    async def start(self) -> "Cluster":
        self.tracker = self._new_tracker()
        await self.tracker.start()
        for i in range(self.n_nodes):
            node = TestNode(f"node{i + 1}", self.tmp / f"node{i + 1}", self.tracker_addr)
            await node.start()
            self.nodes.append(node)
        await self.wait_for(lambda: len(self.tracker.state.alive_nodes()) == self.n_nodes)
        return self

    @property
    def tracker_addr(self) -> tuple[str, int]:
        return self.tracker.address if self.tracker else self._addr

    def client(self, **kw) -> VektorClient:
        return VektorClient(*self.tracker_addr, timeout=5.0, **kw)

    async def restart_tracker(self) -> None:
        host, port = self.tracker.address
        await self.tracker.stop()
        self._addr = (host, port)
        self.tracker = self._new_tracker(port)   # same port: nodes keep their config
        await self.tracker.start()

    async def wait_for(self, predicate, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                raise AssertionError("condition not reached in time")
            await asyncio.sleep(0.05)

    def holders(self, chunk_id: str) -> list[TestNode]:
        """Alive nodes that really have the chunk on disk (ground truth)."""
        return [n for n in self.nodes if n.alive and n.storage.exists(chunk_id)]

    async def stop(self) -> None:
        for n in self.nodes:
            await n.kill()
        if self.tracker:
            await self.tracker.stop()
