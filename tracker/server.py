"""VektorFS Tracker: metadata, membership, failure detection, re-replication.

Layering mirrors the storage node: this file owns sockets, timers and
background tasks; tracker/state.py owns all decisions.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any

from common.messages import (
    ErrorCode,
    RemoteError,
    build_delete_request,
    build_error_response,
    build_ok_response,
    build_replicate_request,
)
from common.protocol import MessageTooLargeError, ProtocolError, read_message, write_message
from common.rpc import call
from common.tracker_messages import TrackerCommand, parse_tracker_request
from tracker.state import ClusterState, StateError

logger = logging.getLogger("vektorfs.tracker")

READ_TIMEOUT_SECONDS = 30.0
WRITE_TIMEOUT_SECONDS = 30.0


class TrackerServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9100,
        *,
        replication_factor: int = 3,
        heartbeat_ttl: float = 10.0,
        metadata_path: Path | None = None,
        replication_interval: float = 1.0,
        max_parallel_replications: int = 4,
        node_rpc_timeout: float = 10.0,
        replicate_timeout: float = 60.0,
    ):
        self.host, self.port = host, port
        self.state = ClusterState(replication_factor, heartbeat_ttl)
        # Nodes are told to heartbeat 4x per TTL: tolerates losing 3 in a row.
        self.heartbeat_interval = heartbeat_ttl / 4
        self.metadata_path = Path(metadata_path) if metadata_path else None
        self.replication_interval = replication_interval
        self.node_rpc_timeout = node_rpc_timeout
        self.replicate_timeout = replicate_timeout
        self._replication_slots = asyncio.Semaphore(max_parallel_replications)
        self._inflight: set[tuple[str, str]] = set()   # (chunk_id, target_node)
        self._wake = asyncio.Event()
        self._tasks: set[asyncio.Task] = set()
        self._server: asyncio.AbstractServer | None = None
        if self.metadata_path:
            self.state.load(self.metadata_path)
            logger.info("loaded %d files from %s", len(self.state.files), self.metadata_path)

    @property
    def address(self) -> tuple[str, int] | None:
        if self._server is None or not self._server.sockets:
            return None
        return self._server.sockets[0].getsockname()[:2]

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_connection, self.host, self.port)
        self._spawn(self._failure_detector())
        self._spawn(self._replication_loop())

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _save(self) -> None:
        if self.metadata_path:
            self.state.save(self.metadata_path)

    # ---------------------------------------------------- background loops

    async def _failure_detector(self) -> None:
        interval = max(0.05, min(1.0, self.state.ttl / 4))
        while True:
            await asyncio.sleep(interval)
            dead = self.state.expire_dead_nodes()
            if dead:
                logger.warning("nodes declared DEAD (no heartbeat > %.1fs): %s",
                               self.state.ttl, dead)
                self._wake.set()          # start re-replicating right away

    async def _replication_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), self.replication_interval)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            for task in self.state.plan_replication(self._inflight):
                key = (task.chunk_id, task.target.node_id)
                self._inflight.add(key)
                self._spawn(self._run_replication(task, key))

    async def _run_replication(self, task, key) -> None:
        try:
            async with self._replication_slots:
                await call(
                    task.target.host, task.target.port,
                    build_replicate_request(task.chunk_id, task.source.host,
                                            task.source.port, task.sha256),
                    timeout=self.replicate_timeout,
                )
            if task.chunk_id not in self.state.chunk_index:
                # File was deleted while this copy was in flight: the chunk we
                # just wrote is now an untracked orphan. Remove it right away.
                logger.info("file deleted mid-replication; discarding %s on %s",
                            task.chunk_id, task.target.node_id)
                self._spawn(self._garbage_collect([(task.target, task.chunk_id)]))
                return
            self.state.add_replica(task.chunk_id, task.target.node_id)
            logger.info("re-replicated %s: %s -> %s", task.chunk_id,
                        task.source.node_id, task.target.node_id)
            self._wake.set()              # maybe more copies are needed: chain on
        except RemoteError as exc:
            if exc.code == ErrorCode.CHECKSUM_MISMATCH.value:
                # The *source* handed over bytes that fail the recorded hash.
                logger.error("source %s holds a corrupt %s; dropping that replica",
                             task.source.node_id, task.chunk_id)
                if self.state.remove_replica(task.chunk_id, task.source.node_id):
                    self._spawn(self._discard_corrupt(task.source.node_id, task.chunk_id))
            else:
                logger.warning("replicate %s -> %s refused: %s",
                               task.chunk_id, task.target.node_id, exc)
        except (asyncio.TimeoutError, OSError, ProtocolError) as exc:
            logger.warning("replicate %s -> %s failed: %r; will retry",
                           task.chunk_id, task.target.node_id, exc)
        finally:
            self._inflight.discard(key)

    async def _discard_corrupt(self, node_id: str, chunk_id: str) -> None:
        """Delete a replica known to be corrupt so it can't be resurrected by a
        later chunk report. While the DELETE is in flight the pair is marked
        busy: otherwise the planner could pick this very node as a repair
        target and our late DELETE would destroy the freshly written good copy.
        """
        node = self.state.nodes.get(node_id)
        if node is None or not node.alive:
            return
        key = (chunk_id, node_id)
        self._inflight.add(key)
        try:
            await call(node.host, node.port, build_delete_request(chunk_id),
                       timeout=self.node_rpc_timeout)
        except Exception as exc:   # noqa: BLE001 - best effort
            logger.info("could not delete corrupt %s on %s: %r", chunk_id, node_id, exc)
        finally:
            self._inflight.discard(key)
            self._wake.set()       # now the node is a legal repair target again

    async def _garbage_collect(self, todo) -> None:
        """Best effort: nodes that miss this get the chunk removed as an
        orphan when they next register."""
        for node, chunk_id in todo:
            try:
                await call(node.host, node.port, build_delete_request(chunk_id),
                           timeout=self.node_rpc_timeout)
            except Exception as exc:   # noqa: BLE001 - GC must never crash the tracker
                logger.info("GC of %s on %s skipped: %r", chunk_id, node.node_id, exc)

    # ------------------------------------------------------------- handlers

    def _replica_list(self, record) -> list[dict]:
        return [{"node_id": n, "host": self.state.nodes[n].host,
                 "port": self.state.nodes[n].port}
                for n in sorted(record.replicas) if self.state.nodes[n].alive]

    def _handle(self, peer_ip: str, command: TrackerCommand, f: dict) -> dict[str, Any]:
        s = self.state
        if command is TrackerCommand.REGISTER:
            host = f["host"] if f["host"] not in ("", "0.0.0.0") else peer_ip
            orphans = s.register_node(f["node_id"], host, f["port"], f["chunks"])
            logger.info("node %s registered at %s:%s (%d chunks, %d orphans)",
                        f["node_id"], host, f["port"], len(f["chunks"]), len(orphans))
            self._wake.set()
            return build_ok_response(heartbeat_interval=self.heartbeat_interval,
                                     orphans=orphans)
        if command is TrackerCommand.HEARTBEAT:
            if not s.heartbeat(f["node_id"]):
                return build_error_response(ErrorCode.UNKNOWN_NODE, "register first")
            return build_ok_response()
        if command is TrackerCommand.ALLOCATE:
            p = s.allocate(f["name"], f["size"], f["chunk_size"], f["num_chunks"])
            return build_ok_response(
                file_id=p.file_id,
                chunks=[{"index": i, "chunk_id": cid,
                         "targets": [{"node_id": n, "host": s.nodes[n].host,
                                      "port": s.nodes[n].port} for n in p.assigned[cid]]}
                        for i, cid in enumerate(p.chunk_ids)])
        if command is TrackerCommand.COMMIT:
            record = s.commit(f["file_id"], f["sha256"], f["chunks"])
            self._save()
            self._wake.set()              # heal chunks stored below RF right away
            return build_ok_response(name=record.name, chunks=len(record.chunks))
        if command is TrackerCommand.GET_FILE:
            r = s.get_file(f["name"])
            return build_ok_response(file={
                "name": r.name, "file_id": r.file_id, "size": r.size,
                "chunk_size": r.chunk_size, "sha256": r.sha256,
                "chunks": [{"index": c.index, "chunk_id": c.chunk_id, "size": c.size,
                            "sha256": c.sha256, "replicas": self._replica_list(c)}
                           for c in r.chunks]})
        if command is TrackerCommand.DELETE_FILE:
            todo = s.delete_file(f["name"])
            self._save()
            self._spawn(self._garbage_collect(todo))
            return build_ok_response()
        if command is TrackerCommand.LIST_FILES:
            return build_ok_response(files=[
                {"name": r.name, "size": r.size, "chunks": len(r.chunks), "sha256": r.sha256}
                for r in s.files.values()])
        if command is TrackerCommand.REPORT_CORRUPT:
            removed = s.remove_replica(f["chunk_id"], f["node_id"])
            if removed:
                logger.error("corrupt replica reported: %s on %s", f["chunk_id"], f["node_id"])
                self._spawn(self._discard_corrupt(f["node_id"], f["chunk_id"]))
            return build_ok_response(removed=removed)
        if command is TrackerCommand.STATUS:
            now = s.clock()
            return build_ok_response(
                replication_factor=s.rf, files=len(s.files), chunks=s.chunk_health(),
                nodes=[{"node_id": n.node_id, "host": n.host, "port": n.port,
                        "alive": n.alive, "chunks": len(n.chunks),
                        "last_seen_age": round(now - n.last_seen, 2)}
                       for n in s.nodes.values()])
        raise ProtocolError(f"Unhandled command {command}")

    # ---------------------------------------------------------- connections

    async def _handle_connection(self, reader, writer) -> None:
        peer = writer.get_extra_info("peername")
        peer_ip = peer[0] if peer else ""
        try:
            while True:
                try:
                    payload = await asyncio.wait_for(read_message(reader), READ_TIMEOUT_SECONDS)
                except (asyncio.TimeoutError, ConnectionError):
                    return
                except MessageTooLargeError as exc:
                    await self._send(writer, build_error_response(ErrorCode.INVALID_REQUEST, str(exc)))
                    return
                except ProtocolError as exc:
                    await self._send(writer, build_error_response(ErrorCode.INVALID_REQUEST, str(exc)))
                    continue
                await self._send(writer, self._dispatch(peer_ip, payload))
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    def _dispatch(self, peer_ip: str, payload: dict) -> dict[str, Any]:
        try:
            command, fields = parse_tracker_request(payload)
            return self._handle(peer_ip, command, fields)
        except ProtocolError as exc:
            return build_error_response(ErrorCode.INVALID_REQUEST, str(exc))
        except StateError as exc:
            return build_error_response(exc.code, str(exc))
        except Exception:
            logger.exception("unhandled error")
            return build_error_response(ErrorCode.INTERNAL_ERROR, "Internal server error")

    async def _send(self, writer, response: dict) -> None:
        try:
            await asyncio.wait_for(write_message(writer, response), WRITE_TIMEOUT_SECONDS)
        except (ConnectionError, OSError, asyncio.TimeoutError):
            pass


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run the VektorFS tracker.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9100)
    p.add_argument("--replication-factor", type=int, default=3)
    p.add_argument("--ttl", type=float, default=10.0, help="Seconds without heartbeat before a node is dead.")
    p.add_argument("--metadata-file", default=None, help="JSON file for persisted file metadata.")
    p.add_argument("--replication-interval", type=float, default=1.0)
    return p


async def _run(args) -> None:
    tracker = TrackerServer(
        args.host, args.port, replication_factor=args.replication_factor,
        heartbeat_ttl=args.ttl, metadata_path=args.metadata_file,
        replication_interval=args.replication_interval)
    await tracker.start()
    logger.info("VektorFS tracker listening on %s:%s (RF=%d, TTL=%.1fs)",
                *tracker.address, args.replication_factor, args.ttl)
    await tracker.serve_forever()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(_run(_build_arg_parser().parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
