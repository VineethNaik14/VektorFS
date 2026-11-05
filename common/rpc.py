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
