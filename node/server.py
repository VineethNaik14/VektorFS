"""VektorFS Storage Node - TCP server.

This module is intentionally "thin": it owns none of the wire format
(that's common.protocol / common.messages) and none of the disk logic
(that's StorageManager). Its only job is to sit between a socket and a
StorageManager instance, one connection at a time, and translate:

    bytes on the wire <-> validated Python objects <-> StorageManager calls

Keeping this separation means every layer can be unit-tested in isolation
(as the existing test_protocol.py / test_messages.py / test_storage.py
already do), and this module's own tests only need to worry about
connection-level concerns: framing, timeouts, concurrency, and mapping
StorageManager exceptions to the right error response.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable

from common.messages import (
    ErrorCode,
    build_error_response,
    build_ok_response,
    parse_request,
)
from common.protocol import (
    Command,
    MessageTooLargeError,
    ProtocolError,
    encode_chunk_data,
    read_message,
    write_message,
)
from node.storage import StorageManager

logger = logging.getLogger("vektorfs.node.server")

# A client that opens a connection and then sends nothing (or sends a
# partial frame and stalls) would otherwise hold a task + file descriptor
# open forever. 30s is generous for a chunk-sized message on a slow link,
# but still bounds the damage a single stuck/hostile peer can do.
READ_TIMEOUT_SECONDS = 30.0

Handler = Callable[[StorageManager, dict[str, Any]], Awaitable[dict[str, Any]]]


# ---------------------------------------------------------------------------
# Command handlers: each one only knows how to turn validated request
# fields into a StorageManager call and an OK response. Exception -> error
# response mapping is centralized in _dispatch below so handlers stay tiny.
# ---------------------------------------------------------------------------


async def _handle_store(
    storage: StorageManager, fields: dict[str, Any]
) -> dict[str, Any]:
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
    return build_ok_response(**info)


_HANDLERS: dict[Command, Handler] = {
    Command.STORE: _handle_store,
    Command.GET: _handle_get,
    Command.DELETE: _handle_delete,
    Command.EXISTS: _handle_exists,
    Command.INFO: _handle_info,
}


class StorageNodeServer:
    """Async TCP server exposing a StorageManager over the VektorFS protocol."""

    def __init__(
        self, storage: StorageManager, host: str = "127.0.0.1", port: int = 9000
    ):
        self.storage = storage
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None

    @property
    def address(self) -> tuple[str, int] | None:
        """Actual bound (host, port), useful when port=0 asks the OS to pick one."""
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

    # -- connection handling -------------------------------------------------

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
                    # Peer closed the socket (cleanly, or mid-frame). Either
                    # way there's no one left to write a response to.
                    return
                except MessageTooLargeError as exc:
                    await self._try_send_error(
                        writer, ErrorCode.INVALID_REQUEST, str(exc)
                    )
                    # The declared length was bogus/oversized, so we can no
                    # longer trust our place in the byte stream. Don't try
                    # to keep reading framed messages from a desynced socket.
                    return
                except ProtocolError as exc:
                    # Framing was fine, only the JSON body was bad. The
                    # stream is still in a known state, so the connection
                    # can stay open for the client's next request.
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
        except ValueError as exc:
            # Raised by StorageManager._get_chunk_path for path-traversal /
            # otherwise invalid chunk ids. Deliberately not INTERNAL_ERROR:
            # this is a client mistake (or attack), not a server fault.
            response = build_error_response(ErrorCode.INVALID_CHUNK_ID, str(exc))
        except Exception:
            # Never leak internal exception details to the network - log
            # the real cause locally, tell the client only that we failed.
            logger.exception("unhandled error processing %s", command)
            response = build_error_response(
                ErrorCode.INTERNAL_ERROR, "Internal server error"
            )

        try:
            await write_message(writer, response)
        except (ConnectionError, OSError):
            logger.info("client disconnected before response could be sent")

    async def _try_send_error(
        self, writer: asyncio.StreamWriter, code: ErrorCode, message: str
    ) -> None:
        try:
            await write_message(writer, build_error_response(code, message))
        except (ConnectionError, OSError):
            pass


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a VektorFS storage node.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--storage-dir",
        default="./storage_data",
        help="Directory this node stores chunks in.",
    )
    return parser


async def _run(host: str, port: int, storage_dir: str) -> None:
    storage = StorageManager(Path(storage_dir))
    server = StorageNodeServer(storage, host=host, port=port)
    await server.start()
    bound_host, bound_port = server.address
    logger.info("VektorFS storage node listening on %s:%s", bound_host, bound_port)
    await server.serve_forever()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = _build_arg_parser().parse_args()
    try:
        asyncio.run(_run(args.host, args.port, args.storage_dir))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
