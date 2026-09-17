"""Request/response message contract for VektorFS storage-node commands.

This is the single source of truth for wire shapes. Client code and the
Storage Node handler must both import from here instead of hand-building
dicts, so the two sides can never silently drift apart.
"""

from __future__ import annotations

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


# ---------------------------------------------------------------------------
# Request builders (Client -> Storage Node)
# ---------------------------------------------------------------------------


def build_store_request(chunk_id: str, data: bytes) -> dict[str, Any]:
    return {
        "command": Command.STORE.value,
        "chunk_id": chunk_id,
        "data": encode_chunk_data(data),
    }


def build_get_request(chunk_id: str) -> dict[str, Any]:
    return {"command": Command.GET.value, "chunk_id": chunk_id}


def build_delete_request(chunk_id: str) -> dict[str, Any]:
    return {"command": Command.DELETE.value, "chunk_id": chunk_id}


def build_exists_request(chunk_id: str) -> dict[str, Any]:
    return {"command": Command.EXISTS.value, "chunk_id": chunk_id}


def build_info_request(chunk_id: str) -> dict[str, Any]:
    return {"command": Command.INFO.value, "chunk_id": chunk_id}


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
}


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
        raise ProtocolError(f"Server error [{code}]: {message}")

    raise ProtocolError(f"Response missing valid 'status' field: {status!r}")
