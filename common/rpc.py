"""One-shot request/response helper over the framed protocol.

Used by the client (-> tracker, -> nodes), the tracker (-> nodes) and the
nodes (-> tracker, -> peer nodes). One connection per call keeps failure
handling trivial: any exception means "this attempt failed", and there is no
half-broken pooled socket to reason about. (A connection pool is a later
optimisation once benchmarking shows connect cost matters.)
"""

from __future__ import annotations

import asyncio
from typing import Any

from common.messages import parse_response
from common.protocol import read_message, write_message

DEFAULT_TIMEOUT = 10.0


async def call(
    host: str,
    port: int,
    payload: dict[str, Any],
    *,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Send one request, return the OK response.

    The single `timeout` bounds connect + send + receive together, so a
    black-holed or frozen peer can never hang the caller.

    Raises:
        asyncio.TimeoutError, OSError/ConnectionError: peer unreachable/slow.
        RemoteError (a ProtocolError): peer replied status=ERROR.
        ProtocolError: garbage reply.
    """

    async def _exchange() -> dict[str, Any]:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            await write_message(writer, payload)
            return parse_response(await read_message(reader))
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    return await asyncio.wait_for(_exchange(), timeout)
