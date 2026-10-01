"""Message contract for the Tracker (clients and storage nodes -> tracker).

Mirrors common/messages.py: builders for senders, one validating parser for
the receiver. Every limit here exists because the tracker parses JSON from
the network and must not trust its size or shape.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any

from common.messages import is_sha256_hex
from common.protocol import ProtocolError

MAX_NAME_LEN = 255
MAX_CHUNKS_PER_FILE = 20_000          # bounds GET_FILE reply size (< 16 MiB frame)
MAX_CHUNK_SIZE = 8 * 1024 * 1024      # base64(8 MiB) still fits in one frame
MAX_REPORTED_CHUNKS = 200_000
MAX_REPLICAS_PER_CHUNK = 16

_NODE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_FILE_ID_RE = re.compile(r"[0-9a-f]{32}")
_CHUNK_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}")
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")


class TrackerCommand(str, Enum):
    REGISTER = "REGISTER"
    HEARTBEAT = "HEARTBEAT"
    ALLOCATE = "ALLOCATE"
    COMMIT = "COMMIT"
    GET_FILE = "GET_FILE"
    DELETE_FILE = "DELETE_FILE"
    LIST_FILES = "LIST_FILES"
    REPORT_CORRUPT = "REPORT_CORRUPT"
    STATUS = "STATUS"


# ---------------------------------------------------------------- builders


def build_register(node_id: str, host: str, port: int, chunks: list[str]) -> dict:
    return {"command": "REGISTER", "node_id": node_id, "host": host,
            "port": port, "chunks": chunks}


def build_heartbeat(node_id: str) -> dict:
    return {"command": "HEARTBEAT", "node_id": node_id}


def build_allocate(name: str, size: int, chunk_size: int, num_chunks: int) -> dict:
    return {"command": "ALLOCATE", "name": name, "size": size,
            "chunk_size": chunk_size, "num_chunks": num_chunks}


def build_commit(file_id: str, sha256: str, chunks: list[dict]) -> dict:
    return {"command": "COMMIT", "file_id": file_id, "sha256": sha256, "chunks": chunks}


def build_get_file(name: str) -> dict:
    return {"command": "GET_FILE", "name": name}


def build_delete_file(name: str) -> dict:
    return {"command": "DELETE_FILE", "name": name}


def build_list_files() -> dict:
    return {"command": "LIST_FILES"}


def build_report_corrupt(chunk_id: str, node_id: str) -> dict:
    return {"command": "REPORT_CORRUPT", "chunk_id": chunk_id, "node_id": node_id}


def build_status() -> dict:
    return {"command": "STATUS"}


# ---------------------------------------------------------------- validators


def _pattern(payload: dict, key: str, regex: re.Pattern) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not regex.fullmatch(value):
        raise ProtocolError(f"invalid or missing {key}")
    return value


def _int(payload: dict, key: str, lo: int, hi: int) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or not lo <= value <= hi:
        raise ProtocolError(f"{key} must be an integer in {lo}..{hi}")
    return value


def _name(payload: dict) -> str:
    value = payload.get("name")
    if (
        not isinstance(value, str)
        or not 0 < len(value) <= MAX_NAME_LEN
        or _CONTROL_CHARS_RE.search(value)
    ):
        raise ProtocolError("invalid or missing name")
    return value


def parse_tracker_request(payload: dict[str, Any]) -> tuple[TrackerCommand, dict]:
    """Validate a tracker request; return (command, cleaned fields)."""
    try:
        command = TrackerCommand(payload.get("command"))
    except ValueError as exc:
        raise ProtocolError(f"Unknown command: {payload.get('command')!r}") from exc

    f: dict[str, Any] = {}

    if command is TrackerCommand.REGISTER:
        f["node_id"] = _pattern(payload, "node_id", _NODE_ID_RE)
        host = payload.get("host", "")
        if not isinstance(host, str) or len(host) > 255:
            raise ProtocolError("invalid host")
        f["host"] = host
        f["port"] = _int(payload, "port", 1, 65535)
        chunks = payload.get("chunks", [])
        if not isinstance(chunks, list) or len(chunks) > MAX_REPORTED_CHUNKS:
            raise ProtocolError("invalid chunks report")
        if not all(isinstance(c, str) and _CHUNK_ID_RE.fullmatch(c) for c in chunks):
            raise ProtocolError("chunks report contains an invalid chunk id")
        f["chunks"] = chunks

    elif command is TrackerCommand.HEARTBEAT:
        f["node_id"] = _pattern(payload, "node_id", _NODE_ID_RE)

    elif command is TrackerCommand.ALLOCATE:
        f["name"] = _name(payload)
        f["size"] = _int(payload, "size", 0, 2**50)
        f["chunk_size"] = _int(payload, "chunk_size", 1, MAX_CHUNK_SIZE)
        f["num_chunks"] = _int(payload, "num_chunks", 0, MAX_CHUNKS_PER_FILE)

    elif command is TrackerCommand.COMMIT:
        f["file_id"] = _pattern(payload, "file_id", _FILE_ID_RE)
        if not is_sha256_hex(payload.get("sha256")):
            raise ProtocolError("invalid file sha256")
        f["sha256"] = payload["sha256"]
        raw = payload.get("chunks")
        if not isinstance(raw, list) or len(raw) > MAX_CHUNKS_PER_FILE:
            raise ProtocolError("invalid chunks list")
        cleaned = []
        for item in raw:
            if not isinstance(item, dict):
                raise ProtocolError("chunk entry must be an object")
            reps = item.get("replicas")
            if (
                not isinstance(reps, list)
                or len(reps) > MAX_REPLICAS_PER_CHUNK
                or not all(isinstance(r, str) and _NODE_ID_RE.fullmatch(r) for r in reps)
            ):
                raise ProtocolError("invalid replicas list")
            if not is_sha256_hex(item.get("sha256")):
                raise ProtocolError("invalid chunk sha256")
            cleaned.append({
                "index": _int(item, "index", 0, MAX_CHUNKS_PER_FILE),
                "size": _int(item, "size", 0, MAX_CHUNK_SIZE),
                "sha256": item["sha256"],
                "replicas": reps,
            })
        f["chunks"] = cleaned

    elif command in (TrackerCommand.GET_FILE, TrackerCommand.DELETE_FILE):
        f["name"] = _name(payload)

    elif command is TrackerCommand.REPORT_CORRUPT:
        f["chunk_id"] = _pattern(payload, "chunk_id", _CHUNK_ID_RE)
        f["node_id"] = _pattern(payload, "node_id", _NODE_ID_RE)

    return command, f
