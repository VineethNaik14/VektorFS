"""Request/response message contract for VektorFS storage-node commands.

This is the single source of truth for wire shapes. Client code and the
Storage Node handler must both import from here instead of hand-building
dicts, so the two sides can never silently drift apart.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any

from common.protocol import Command, ProtocolError, decode_chunk_data, encode_chunk_data


class Status(str, Enum):
    OK = "OK"
    ERROR = "ERROR"


class ErrorCode(str, Enum):
    INVALID_REQUEST = "INVALID_REQUEST"
    INVALID_CHUNK_ID = "INVALID_CHUNK_ID"
    CHUNK_NOT_FOUND = "CHUNK_NOT_FOUND"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    # storage-node errors added for integrity / replication
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    # tracker errors
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    FILE_EXISTS = "FILE_EXISTS"
    NO_NODES_AVAILABLE = "NO_NODES_AVAILABLE"
    UNKNOWN_NODE = "UNKNOWN_NODE"
    UPLOAD_NOT_FOUND = "UPLOAD_NOT_FOUND"
    INSUFFICIENT_REPLICAS = "INSUFFICIENT_REPLICAS"


class RemoteError(ProtocolError):
    """The peer answered with status=ERROR. `.code` holds the ErrorCode string.

    Subclasses ProtocolError so existing `except ProtocolError` callers keep
    working, while new callers can branch on the machine-readable code
    instead of string-matching the message.
    """

    def __init__(self, code: str, message: str):
        super().__init__(f"Server error [{code}]: {message}")
        self.code = code


# ---------------------------------------------------------------------------
# Request builders (Client -> Storage Node)
# ---------------------------------------------------------------------------


def build_store_request(
    chunk_id: str, data: bytes, sha256: str | None = None
) -> dict[str, Any]:
    """`sha256` (optional) lets the node verify the bytes it received."""
    request: dict[str, Any] = {
        "command": Command.STORE.value,
        "chunk_id": chunk_id,
        "data": encode_chunk_data(data),
    }
    if sha256 is not None:
        request["sha256"] = sha256
    return request


def build_get_request(chunk_id: str) -> dict[str, Any]:
    return {"command": Command.GET.value, "chunk_id": chunk_id}


def build_delete_request(chunk_id: str) -> dict[str, Any]:
    return {"command": Command.DELETE.value, "chunk_id": chunk_id}


def build_exists_request(chunk_id: str) -> dict[str, Any]:
    return {"command": Command.EXISTS.value, "chunk_id": chunk_id}


def build_info_request(chunk_id: str) -> dict[str, Any]:
    return {"command": Command.INFO.value, "chunk_id": chunk_id}


def build_replicate_request(
    chunk_id: str, source_host: str, source_port: int, sha256: str
) -> dict[str, Any]:
    """Tracker -> node: fetch `chunk_id` from a peer, verify it, store it."""
    return {
        "command": Command.REPLICATE.value,
        "chunk_id": chunk_id,
        "source_host": source_host,
        "source_port": source_port,
        "sha256": sha256,
    }


# ---------------------------------------------------------------------------
# Response builders (Storage Node -> Client)
# ---------------------------------------------------------------------------


def build_ok_response(**fields: Any) -> dict[str, Any]:
    return {"status": Status.OK.value, **fields}


def build_error_response(code: ErrorCode, message: str) -> dict[str, Any]:
    return {"status": Status.ERROR.value, "error": code.value, "message": message}


# ---------------------------------------------------------------------------
# Request parsing/validation (Storage Node side)
# ---------------------------------------------------------------------------

REQUIRED_FIELDS: dict[Command, tuple[str, ...]] = {
    Command.STORE: ("chunk_id", "data"),
    Command.GET: ("chunk_id",),
    Command.DELETE: ("chunk_id",),
    Command.EXISTS: ("chunk_id",),
    Command.INFO: ("chunk_id",),
    Command.REPLICATE: ("chunk_id", "source_host", "source_port", "sha256"),
}

_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def is_sha256_hex(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def parse_request(payload: dict[str, Any]) -> tuple[Command, dict[str, Any]]:
    """Validate an incoming request payload.

    Returns:
        (command, fields) where fields contains only the validated,
        command-specific keys (e.g. chunk_id, and decoded raw bytes for STORE).

    Raises:
        ProtocolError: unknown command, missing fields, or wrong field types.
    """
    raw_command = payload.get("command")
    try:
        command = Command(raw_command)
    except ValueError as exc:
        raise ProtocolError(f"Unknown command: {raw_command!r}") from exc

    required = REQUIRED_FIELDS[command]
    missing = [field for field in required if field not in payload]
    if missing:
        raise ProtocolError(f"{command.value} request missing fields: {missing}")

    chunk_id = payload["chunk_id"]
    if not isinstance(chunk_id, str) or not chunk_id:
        raise ProtocolError("chunk_id must be a non-empty string")

    fields: dict[str, Any] = {"chunk_id": chunk_id}

    if command is Command.STORE:
        raw_data = payload["data"]
        if not isinstance(raw_data, str):
            raise ProtocolError("data must be a base64-encoded string")
        fields["data"] = decode_chunk_data(raw_data)
        if "sha256" in payload:
            if not is_sha256_hex(payload["sha256"]):
                raise ProtocolError("sha256 must be 64 lowercase hex characters")
            fields["sha256"] = payload["sha256"]

    if command is Command.REPLICATE:
        host, port = payload["source_host"], payload["source_port"]
        if not isinstance(host, str) or not host or len(host) > 255:
            raise ProtocolError("source_host must be a non-empty string")
        # bool is an int subclass; reject it explicitly.
        if not isinstance(port, int) or isinstance(port, bool) or not 0 < port < 65536:
            raise ProtocolError("source_port must be an integer in 1..65535")
        if not is_sha256_hex(payload["sha256"]):
            raise ProtocolError("sha256 must be 64 lowercase hex characters")
        fields.update(source_host=host, source_port=port, sha256=payload["sha256"])

    return command, fields


# ---------------------------------------------------------------------------
# Response parsing (Client side)
# ---------------------------------------------------------------------------


def parse_response(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate an incoming response payload and normalize it.

    Raises:
        ProtocolError: missing/invalid 'status', or an ERROR response
            (raised as ProtocolError with the server's message so callers
            get a single exception type to handle).
    """
    status = payload.get("status")

    if status == Status.OK.value:
        return payload

    if status == Status.ERROR.value:
        code = payload.get("error", ErrorCode.INTERNAL_ERROR.value)
        message = payload.get("message", "Unknown error")
        raise RemoteError(str(code), str(message))

    raise ProtocolError(f"Response missing valid 'status' field: {status!r}")
