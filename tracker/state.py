"""Tracker metadata + cluster membership. Pure logic: no sockets, no sleeps.

Every method is synchronous and never awaits, so when called from the
asyncio event loop each one is atomic with respect to other requests. That
is what lets the tracker avoid locks entirely.

Source of truth split (a deliberate design decision):
  * FILES -> chunks (id, index, size, sha256) are persisted to disk.
  * REPLICA LOCATIONS are NOT persisted. They are rebuilt from the chunk
    report each node sends when it registers. Nodes know what is actually on
    their disks; a persisted location list could only ever be stale.
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from common.chunking import chunk_count
from common.messages import ErrorCode


class StateError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code


@dataclass
class NodeInfo:
    node_id: str
    host: str
    port: int
    last_seen: float
    alive: bool = True
    chunks: set[str] = field(default_factory=set)


@dataclass
class ChunkRecord:
    chunk_id: str
    index: int
    size: int
    sha256: str
    replicas: set[str] = field(default_factory=set)  # node_ids believed alive+holding


@dataclass
class FileRecord:
    name: str
    file_id: str
    size: int
    chunk_size: int
    sha256: str
    chunks: list[ChunkRecord]


@dataclass
class PendingUpload:
    file_id: str
    name: str
    size: int
    chunk_size: int
    chunk_ids: list[str]
    assigned: dict[str, list[str]]  # chunk_id -> node_ids chosen by ALLOCATE
    expires_at: float


@dataclass(frozen=True)
class ReplicationTask:
    chunk_id: str
    sha256: str
    source: NodeInfo
    target: NodeInfo


class ClusterState:
    def __init__(
        self,
        replication_factor: int = 3,
        heartbeat_ttl: float = 10.0,
        pending_ttl: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.rf = replication_factor
        self.ttl = heartbeat_ttl
        self.pending_ttl = pending_ttl
        self.clock = clock
        self.nodes: dict[str, NodeInfo] = {}
        self.files: dict[str, FileRecord] = {}
        self.chunk_index: dict[str, ChunkRecord] = {}
        self.pending: dict[str, PendingUpload] = {}

    # ------------------------------------------------------------ membership

    def alive_nodes(self) -> list[NodeInfo]:
        return [n for n in self.nodes.values() if n.alive]

    def register_node(
        self, node_id: str, host: str, port: int, chunk_ids: list[str]
    ) -> list[str]:
        """(Re-)register a node. Returns orphan chunk ids it should delete.

        The report is authoritative: first forget everything we thought the
        node held, then re-add only what it says it has. That correctly
        handles a restart, a wiped disk, and a node returning after being
        declared dead.
        """
        self._expire_pending()
        node = self.nodes.get(node_id)
        if node is None:
            node = NodeInfo(node_id, host, port, self.clock())
            self.nodes[node_id] = node
        else:
            self._drop_node_replicas(node)
            node.host, node.port = host, port
        node.alive = True
        node.last_seen = self.clock()

        pending_ids = {c for p in self.pending.values() for c in p.chunk_ids}
        orphans: list[str] = []
        for chunk_id in chunk_ids:
            record = self.chunk_index.get(chunk_id)
            if record is not None:
                record.replicas.add(node_id)
                node.chunks.add(chunk_id)
            elif chunk_id not in pending_ids:
                # Deleted while the node was away (or an abandoned upload).
                orphans.append(chunk_id)
        return orphans

    def heartbeat(self, node_id: str) -> bool:
        """False => node unknown or already declared dead: it must re-register."""
        node = self.nodes.get(node_id)
        if node is None or not node.alive:
            return False
        node.last_seen = self.clock()
        return True

    def expire_dead_nodes(self) -> list[str]:
        """TTL failure detector: mark silent nodes dead, drop their replicas."""
        now = self.clock()
        dead = []
        for node in self.nodes.values():
            if node.alive and now - node.last_seen > self.ttl:
                node.alive = False
                self._drop_node_replicas(node)
                dead.append(node.node_id)
        return dead

    def _drop_node_replicas(self, node: NodeInfo) -> None:
        for chunk_id in node.chunks:
            record = self.chunk_index.get(chunk_id)
            if record:
                record.replicas.discard(node.node_id)
        node.chunks.clear()

    def _expire_pending(self) -> None:
        now = self.clock()
        for fid in [f for f, p in self.pending.items() if p.expires_at < now]:
            del self.pending[fid]

    # ------------------------------------------------------------- placement

    def _load_map(self) -> Counter:
        load: Counter = Counter()
        for node in self.alive_nodes():
            load[node.node_id] = len(node.chunks)
        for p in self.pending.values():
            for node_ids in p.assigned.values():
                load.update(node_ids)
        return load

    # --------------------------------------------------------------- uploads

    def allocate(
        self, name: str, size: int, chunk_size: int, num_chunks: int
    ) -> PendingUpload:
        self._expire_pending()
        if name in self.files:
            raise StateError(ErrorCode.FILE_EXISTS, f"File already exists: {name}")
        if num_chunks != chunk_count(size, chunk_size):
            raise StateError(
                ErrorCode.INVALID_REQUEST, "num_chunks inconsistent with size/chunk_size"
            )
        alive = self.alive_nodes()
        if not alive:
            raise StateError(ErrorCode.NO_NODES_AVAILABLE, "No live storage nodes")

        want = min(self.rf, len(alive))
        load = self._load_map()
        file_id = uuid.uuid4().hex
        chunk_ids, assigned = [], {}
        for index in range(num_chunks):
            chunk_id = f"{file_id}_{index:06d}"
            # Least-loaded first; random tie-break spreads equal nodes evenly.
            ranked = sorted(alive, key=lambda n: (load[n.node_id], random.random()))
            chosen = [n.node_id for n in ranked[:want]]
            load.update(chosen)
            chunk_ids.append(chunk_id)
            assigned[chunk_id] = chosen
        pending = PendingUpload(
            file_id, name, size, chunk_size, chunk_ids, assigned,
            self.clock() + self.pending_ttl,
        )
        self.pending[file_id] = pending
        return pending

    def commit(self, file_id: str, sha256: str, chunks: list[dict]) -> FileRecord:
        self._expire_pending()
        pending = self.pending.get(file_id)
        if pending is None:
            raise StateError(ErrorCode.UPLOAD_NOT_FOUND, "Unknown or expired upload")
        if pending.name in self.files:
            raise StateError(ErrorCode.FILE_EXISTS, f"File already exists: {pending.name}")
        if sorted(c["index"] for c in chunks) != list(range(len(pending.chunk_ids))):
            raise StateError(ErrorCode.INVALID_REQUEST, "chunk list incomplete/duplicated")
        if sum(c["size"] for c in chunks) != pending.size or any(
            c["size"] > pending.chunk_size for c in chunks
        ):
            raise StateError(ErrorCode.INVALID_REQUEST, "chunk sizes inconsistent")

        records = []
        for item in sorted(chunks, key=lambda c: c["index"]):
            live = [
                r for r in dict.fromkeys(item["replicas"])
                if r in self.nodes and self.nodes[r].alive
            ]
            if not live:
                raise StateError(
                    ErrorCode.INSUFFICIENT_REPLICAS,
                    f"chunk {item['index']} has no live replica",
                )
            records.append(ChunkRecord(
                pending.chunk_ids[item["index"]], item["index"], item["size"],
                item["sha256"], set(live),
            ))
        # Validation done: only now mutate, so a rejected commit leaves no trace.
        record = FileRecord(pending.name, file_id, pending.size, pending.chunk_size,
                            sha256, records)
        self.files[pending.name] = record
        for c in records:
            self.chunk_index[c.chunk_id] = c
            for node_id in c.replicas:
                self.nodes[node_id].chunks.add(c.chunk_id)
        del self.pending[file_id]
        return record

    def get_file(self, name: str) -> FileRecord:
        record = self.files.get(name)
        if record is None:
            raise StateError(ErrorCode.FILE_NOT_FOUND, f"No such file: {name}")
        return record

    def delete_file(self, name: str) -> list[tuple[NodeInfo, str]]:
        """Remove metadata; return (node, chunk_id) pairs to garbage-collect."""
        record = self.get_file(name)
        todo = []
        for c in record.chunks:
            for node_id in c.replicas:
                node = self.nodes[node_id]
                node.chunks.discard(c.chunk_id)
                todo.append((node, c.chunk_id))
            del self.chunk_index[c.chunk_id]
        del self.files[name]
        return todo

    # ------------------------------------------------------ repair / healing

    def remove_replica(self, chunk_id: str, node_id: str) -> bool:
        """Forget a replica (reported corrupt). Re-replication will heal it."""
        record = self.chunk_index.get(chunk_id)
        if record is None or node_id not in record.replicas:
            return False
        record.replicas.discard(node_id)
        if node_id in self.nodes:
            self.nodes[node_id].chunks.discard(chunk_id)
        return True

    def add_replica(self, chunk_id: str, node_id: str) -> None:
        record, node = self.chunk_index.get(chunk_id), self.nodes.get(node_id)
        if record and node and node.alive:  # chunk may be deleted / node died meanwhile
            record.replicas.add(node_id)
            node.chunks.add(chunk_id)

    def plan_replication(
        self, inflight: set[tuple[str, str]], max_tasks: int = 16
    ) -> list[ReplicationTask]:
        """Copy under-replicated chunks, most endangered first."""
        alive = {n.node_id: n for n in self.alive_nodes()}
        if not alive:
            return []
        want = min(self.rf, len(alive))
        load = self._load_map()
        tasks: list[ReplicationTask] = []
        # 0 replicas = unrecoverable until a holder returns: skip, nothing to copy from.
        candidates = sorted(
            (c for c in self.chunk_index.values() if 0 < len(c.replicas) < want),
            key=lambda c: len(c.replicas),
        )
        for c in candidates:
            in_flight_targets = {t for cid, t in inflight if cid == c.chunk_id}
            need = want - len(c.replicas) - len(in_flight_targets)
            if need <= 0:
                continue
            sources = [alive[r] for r in c.replicas if r in alive]
            if not sources:
                continue
            targets = sorted(
                (n for n in alive.values()
                 if n.node_id not in c.replicas and n.node_id not in in_flight_targets),
                key=lambda n: (load[n.node_id], random.random()),
            )
            for target in targets[:need]:
                tasks.append(ReplicationTask(
                    c.chunk_id, c.sha256, random.choice(sources), target))
                load[target.node_id] += 1
            if len(tasks) >= max_tasks:
                break
        return tasks

    def chunk_health(self) -> dict[str, int]:
        want = min(self.rf, max(1, len(self.alive_nodes())))
        counts = {"total": 0, "healthy": 0, "under_replicated": 0, "lost": 0}
        for c in self.chunk_index.values():
            counts["total"] += 1
            n = len(c.replicas)
            counts["lost" if n == 0 else "healthy" if n >= want else "under_replicated"] += 1
        return counts

    # ----------------------------------------------------------- persistence

    def to_dict(self) -> dict:
        return {"files": [
            {"name": f.name, "file_id": f.file_id, "size": f.size,
             "chunk_size": f.chunk_size, "sha256": f.sha256,
             "chunks": [{"chunk_id": c.chunk_id, "index": c.index,
                         "size": c.size, "sha256": c.sha256} for c in f.chunks]}
            for f in self.files.values()]}

    def load_dict(self, data: dict) -> None:
        for f in data.get("files", []):
            chunks = [ChunkRecord(c["chunk_id"], c["index"], c["size"], c["sha256"])
                      for c in f["chunks"]]
            self.files[f["name"]] = FileRecord(
                f["name"], f["file_id"], f["size"], f["chunk_size"], f["sha256"], chunks)
            for c in chunks:
                self.chunk_index[c.chunk_id] = c

    def save(self, path: Path) -> None:
        """Atomic snapshot: a crash mid-save keeps the previous good file."""
        tmp = path.with_name(path.name + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self.to_dict(), fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)

    def load(self, path: Path) -> None:
        if path.is_file():
            self.load_dict(json.loads(path.read_text(encoding="utf-8")))
