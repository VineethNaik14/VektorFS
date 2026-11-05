from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable

from common.hashing import ChecksumMismatchError, sha256_bytes
from common.messages import (
    ErrorCode,
    build_error_response,
    build_get_request,
    build_ok_response,
    parse_request,
)
from common.protocol import (
    Command,
    MessageTooLargeError,
    ProtocolError,
    decode_chunk_data,
    encode_chunk_data,
    read_message,
    write_message,
)
from common.rpc import call
from node.agent import TrackerAgent
from node.storage import StorageManager

logger = logging.getLogger("vektorfs.node.server")

READ_TIMEOUT_SECONDS = 30.0

WRITE_TIMEOUT_SECONDS = 30.0

REPLICATE_FETCH_TIMEOUT_SECONDS = 30.0


class SourceUnavailableError(Exception):
    """A REPLICATE could not fetch the chunk from the given source node."""

Handler = Callable[[StorageManager, dict[str, Any]], Awaitable[dict[str, Any]]]



async def _handle_store(
    storage: StorageManager, fields: dict[str, Any]
) -> dict[str, Any]:
    expected = fields.get("sha256")
    if expected is not None and sha256_bytes(fields["data"]) != expected:
        raise ChecksumMismatchError("data does not match the supplied sha256")
    await asyncio.to_thread(storage.store, fields["chunk_id"], fields["data"])
    return build_ok_response()


async def _handle_get(
    storage: StorageManager, fields: dict[str, Any]
) -> dict[str, Any]:
    data = await asyncio.to_thread(storage.get, fields["chunk_id"])
    return build_ok_response(data=encode_chunk_data(data))


async def _handle_delete(
    storage: StorageManager, fields: dict[str, Any]
) -> dict[str, Any]:
    await asyncio.to_thread(storage.delete, fields["chunk_id"])
    return build_ok_response()


async def _handle_exists(
    storage: StorageManager, fields: dict[str, Any]
) -> dict[str, Any]:
    exists = await asyncio.to_thread(storage.exists, fields["chunk_id"])
    return build_ok_response(exists=exists)


async def _handle_info(
    storage: StorageManager, fields: dict[str, Any]
) -> dict[str, Any]:
    info = await asyncio.to_thread(storage.info, fields["chunk_id"])
    # Hash of what is really on disk right now - lets the tracker/tests audit
    # a replica without downloading it.
    info["sha256"] = await asyncio.to_thread(storage.sha256, fields["chunk_id"])
    return build_ok_response(**info)


async def _handle_replicate(storage: StorageManager, fields: dict[str, Any]) -> dict[str, Any]:
    try:
        response = await call(
            fields["source_host"],
            fields["source_port"],
            build_get_request(fields["chunk_id"]),
            timeout=REPLICATE_FETCH_TIMEOUT_SECONDS,
        )
        data = decode_chunk_data(response["data"])
    except (asyncio.TimeoutError, OSError, ProtocolError, KeyError) as exc:
        raise SourceUnavailableError(
            f"cannot fetch {fields['chunk_id']} from "
            f"{fields['source_host']}:{fields['source_port']}: {exc!r}"
        ) from exc

    if sha256_bytes(data) != fields["sha256"]:
        raise ChecksumMismatchError("source replica failed SHA-256 verification")
    await asyncio.to_thread(storage.store, fields["chunk_id"], data)
    return build_ok_response()


_HANDLERS: dict[Command, Handler] = {
    Command.STORE: _handle_store,
    Command.GET: _handle_get,
    Command.DELETE: _handle_delete,
    Command.EXISTS: _handle_exists,
    Command.INFO: _handle_info,
    Command.REPLICATE: _handle_replicate,
}


class StorageNodeServer:
    def __init__(
        self, storage: StorageManager, host: str = "127.0.0.1", port: int = 9000
    ):
        self.storage = storage
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None

    @property
    def address(self) -> tuple[str, int] | None:
        if self._server is None or not self._server.sockets:
            return None
        return self._server.sockets[0].getsockname()[:2]

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        logger.info("connection opened: %s", peer)
        try:
            while True:
                try:
                    payload = await asyncio.wait_for(
                        read_message(reader), timeout=READ_TIMEOUT_SECONDS
                    )
                except asyncio.TimeoutError:
                    logger.info("connection %s idle timeout, closing", peer)
                    return
                except ConnectionError:
                    return
                except MessageTooLargeError as exc:
                    await self._try_send_error(
                        writer, ErrorCode.INVALID_REQUEST, str(exc)
                    )
                    return
                except ProtocolError as exc:
                    await self._try_send_error(
                        writer, ErrorCode.INVALID_REQUEST, str(exc)
                    )
                    continue

                await self._dispatch(writer, payload)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            logger.info("connection closed: %s", peer)

    async def _dispatch(
        self, writer: asyncio.StreamWriter, payload: dict[str, Any]
    ) -> None:
        try:
            command, fields = parse_request(payload)
        except ProtocolError as exc:
            await self._try_send_error(writer, ErrorCode.INVALID_REQUEST, str(exc))
            return

        handler = _HANDLERS[command]
        try:
            response = await handler(self.storage, fields)
        except FileNotFoundError:
            response = build_error_response(
                ErrorCode.CHUNK_NOT_FOUND, f"Chunk not found: {fields['chunk_id']}"
            )
        except ChecksumMismatchError as exc:
            response = build_error_response(ErrorCode.CHECKSUM_MISMATCH, str(exc))
        except SourceUnavailableError as exc:
            response = build_error_response(ErrorCode.SOURCE_UNAVAILABLE, str(exc))
        except ValueError as exc:
            response = build_error_response(ErrorCode.INVALID_CHUNK_ID, str(exc))
        except Exception:
            logger.exception("unhandled error processing %s", command)
            response = build_error_response(
                ErrorCode.INTERNAL_ERROR, "Internal server error"
            )

        try:
            await asyncio.wait_for(
                write_message(writer, response), timeout=WRITE_TIMEOUT_SECONDS
            )
        except (ConnectionError, OSError, asyncio.TimeoutError):
            logger.info("client gone/stalled; response not delivered")

    async def _try_send_error(
        self, writer: asyncio.StreamWriter, code: ErrorCode, message: str
    ) -> None:
        try:
            await write_message(writer, build_error_response(code, message))
        except (ConnectionError, OSError):
            pass


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a VektorFS storage node.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address.")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--storage-dir",
        default="./storage_data",
        help="Directory this node stores chunks in.",
    )
    parser.add_argument("--node-id", default=None, help="Stable id (needed with --tracker).")
    parser.add_argument("--tracker", default=None, help="Tracker address host:port.")
    parser.add_argument(
        "--advertise-host",
        default="",
        help="Host other machines use to reach this node (e.g. its Docker service name).",
    )
    return parser


async def _run(args: argparse.Namespace) -> None:
    storage = StorageManager(Path(args.storage_dir))
    server = StorageNodeServer(storage, host=args.host, port=args.port)
    await server.start()
    bound_host, bound_port = server.address
    logger.info("VektorFS storage node listening on %s:%s", bound_host, bound_port)

    agent_task = None
    if args.tracker:
        if not args.node_id:
            raise SystemExit("--node-id is required when --tracker is set")
        t_host, _, t_port = args.tracker.rpartition(":")
        agent = TrackerAgent(
            storage,
            node_id=args.node_id,
            tracker=(t_host, int(t_port)),
            advertise_host=args.advertise_host,
            advertise_port=bound_port,
        )
        agent_task = asyncio.create_task(agent.run())
    try:
        await server.serve_forever()
    finally:
        if agent_task:
            agent_task.cancel()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = _build_arg_parser().parse_args()
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
