"""Framed TCP protocol for VektorFS.

Wire format: [4-byte big-endian length][JSON payload, UTF-8]
"""

from __future__ import annotations

import asyncio
import base64
import json
from enum import Enum
from typing import Any

HEADER_SIZE = 4
MAX_MESSAGE_SIZE = (
    16 * 1024 * 1024
)  # 16 MiB: covers a 4MB chunk after base64 (+~33%) plus metadata


class Command(str, Enum):
    STORE = "STORE"
    GET = "GET"
    DELETE = "DELETE"
    EXISTS = "EXISTS"
    INFO = "INFO"


class ProtocolError(Exception):
    """Malformed message (bad JSON, missing fields, etc.)."""


class MessageTooLargeError(ProtocolError):
    """Declared payload size exceeds MAX_MESSAGE_SIZE."""


def encode_message(payload: dict[str, Any]) -> bytes:
    """Serialize a payload dict into a length-prefixed frame."""
    try:
        body = json.dumps(payload).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"Payload not JSON-serializable: {exc}") from exc

    if len(body) > MAX_MESSAGE_SIZE:
        raise MessageTooLargeError(
            f"Payload size {len(body)} exceeds limit {MAX_MESSAGE_SIZE}"
        )

    header = len(body).to_bytes(HEADER_SIZE, byteorder="big")
    return header + body


async def read_message(reader: asyncio.StreamReader) -> dict[str, Any]:
    """Read one length-prefixed JSON message from an asyncio stream.

    Raises:
        ConnectionError: peer closed the connection mid-message.
        MessageTooLargeError: declared length exceeds MAX_MESSAGE_SIZE.
        ProtocolError: body is not valid JSON / not a dict.
    """
    try:
        header = await reader.readexactly(HEADER_SIZE)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionError("Connection closed while reading header") from exc

    length = int.from_bytes(header, byteorder="big")

    if length > MAX_MESSAGE_SIZE:
        raise MessageTooLargeError(
            f"Declared message size {length} exceeds limit {MAX_MESSAGE_SIZE}"
        )

    try:
        body = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionError("Connection closed while reading body") from exc

    try:
        payload = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ProtocolError(f"Invalid JSON payload: {exc}") from exc

    if not isinstance(payload, dict):
        raise ProtocolError("Payload must be a JSON object")

    return payload


async def write_message(writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
    """Encode and send a message, then flush the socket buffer."""
    writer.write(encode_message(payload))
    await writer.drain()


def encode_chunk_data(data: bytes) -> str:
    """Encode raw chunk bytes for embedding in a JSON payload."""
    return base64.b64encode(data).decode("ascii")


def decode_chunk_data(encoded: str) -> bytes:
    """Decode chunk bytes previously produced by encode_chunk_data."""
    try:
        return base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError) as exc:
        raise ProtocolError(f"Invalid base64 chunk data: {exc}") from exc
