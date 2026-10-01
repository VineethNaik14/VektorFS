"""VektorFS client: chunk, hash, upload, download, verify.

Integrity is end-to-end: the client computes SHA-256s from the source file
and checks every downloaded chunk (and the reassembled file) against the
hashes the tracker recorded. It never trusts a storage node's word.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.chunking import CHUNK_SIZE, chunk_count, read_chunk, scan_file
from common.hashing import sha256_bytes, sha256_file
from common.messages import RemoteError, build_get_request, build_store_request
from common.protocol import ProtocolError, decode_chunk_data
from common.rpc import call
from common.tracker_messages import (
    build_allocate, build_commit, build_delete_file, build_get_file,
    build_list_files, build_report_corrupt, build_status,
)

logger = logging.getLogger("vektorfs.client")

_NET_ERRORS = (asyncio.TimeoutError, OSError, ProtocolError)


class VektorError(Exception):
    pass


class UploadError(VektorError):
    pass


class DownloadError(VektorError):
    pass


class IntegrityError(DownloadError):
    pass


@dataclass
class UploadResult:
    name: str
    file_id: str
    size: int
    sha256: str
    chunks: int
    replicas_per_chunk: list[int]


@dataclass
class DownloadResult:
    path: Path
    size: int
    sha256: str
    chunks: int
    corrupt_replicas_skipped: int


class VektorClient:
    def __init__(self, tracker_host: str, tracker_port: int, *, concurrency: int = 4,
                 timeout: float = 15.0, store_attempts: int = 2):
        self.tracker = (tracker_host, tracker_port)
        self.concurrency = concurrency
        self.timeout = timeout
        self.store_attempts = store_attempts

    async def _tracker(self, payload: dict) -> dict[str, Any]:
        return await call(*self.tracker, payload, timeout=self.timeout)

    # --------------------------------------------------------------- upload

    async def upload(self, path: str | Path, name: str | None = None,
                     chunk_size: int = CHUNK_SIZE) -> UploadResult:
        path = Path(path)
        name = name or path.name
        # Pass 1: whole-file + per-chunk hashes (bounded memory, off the loop).
        scan = await asyncio.to_thread(scan_file, str(path), chunk_size)
        alloc = await self._tracker(
            build_allocate(name, scan.size, chunk_size, len(scan.chunks)))
        file_id, plan = alloc["file_id"], alloc["chunks"]

        sem = asyncio.Semaphore(self.concurrency)

        async def upload_chunk(info, entry) -> dict:
            async with sem:   # also bounds how many chunk buffers live in RAM
                data = await asyncio.to_thread(read_chunk, str(path), info.index, chunk_size)
                if sha256_bytes(data) != info.sha256:
                    raise UploadError(f"{path} changed while uploading (chunk {info.index})")
                results = await asyncio.gather(*(
                    self._store_on(t, entry["chunk_id"], data, info.sha256)
                    for t in entry["targets"]))
                stored = [t["node_id"] for t, ok in zip(entry["targets"], results) if ok]
                if not stored:
                    raise UploadError(f"chunk {info.index}: no replica could be stored")
                return {"index": info.index, "size": info.size,
                        "sha256": info.sha256, "replicas": stored}

        committed = await asyncio.gather(*(
            upload_chunk(i, e) for i, e in zip(scan.chunks, plan)))
        await self._tracker(build_commit(file_id, scan.sha256, list(committed)))
        return UploadResult(name, file_id, scan.size, scan.sha256, len(scan.chunks),
                            [len(c["replicas"]) for c in committed])

    async def _store_on(self, target: dict, chunk_id: str, data: bytes, sha: str) -> bool:
        for attempt in range(1, self.store_attempts + 1):
            try:
                await call(target["host"], target["port"],
                           build_store_request(chunk_id, data, sha), timeout=self.timeout)
                return True
            except _NET_ERRORS as exc:
                logger.warning("store %s on %s failed (attempt %d): %r",
                               chunk_id, target["node_id"], attempt, exc)
        return False   # tracker's re-replication will make up the missing copy

    # ------------------------------------------------------------- download

    async def download(self, name: str, dest: str | Path) -> DownloadResult:
        dest = Path(dest)
        meta = (await self._tracker(build_get_file(name)))["file"]
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        skipped = [0]
        sem = asyncio.Semaphore(self.concurrency)
        try:
            with part.open("wb") as out:
                async def fetch(chunk: dict) -> None:
                    async with sem:
                        data = await self._fetch_chunk(name, chunk, skipped)
                        out.seek(chunk["index"] * meta["chunk_size"])
                        out.write(data)   # no await between seek and write: atomic
                await asyncio.gather(*(fetch(c) for c in meta["chunks"]))
                out.flush()
                os.fsync(out.fileno())
            if part.stat().st_size != meta["size"]:
                raise IntegrityError("reassembled size mismatch")
            digest = await asyncio.to_thread(sha256_file, str(part))
            if digest != meta["sha256"]:
                raise IntegrityError(
                    f"whole-file SHA-256 mismatch: expected {meta['sha256']}, got {digest}")
            os.replace(part, dest)
        except BaseException:
            part.unlink(missing_ok=True)   # never leave a half-written file behind
            raise
        return DownloadResult(dest, meta["size"], digest, len(meta["chunks"]), skipped[0])

    async def _fetch_chunk(self, name: str, chunk: dict, skipped: list[int]) -> bytes:
        """Try every replica; on a stale replica list, ask the tracker again."""
        replicas = chunk["replicas"]
        for round_no in range(3):
            candidates = random.sample(replicas, len(replicas))  # spread read load
            for node in candidates:
                try:
                    resp = await call(node["host"], node["port"],
                                      build_get_request(chunk["chunk_id"]),
                                      timeout=self.timeout)
                    data = decode_chunk_data(resp["data"])
                except _NET_ERRORS as exc:
                    logger.warning("replica %s unusable for chunk %d: %r",
                                   node["node_id"], chunk["index"], exc)
                    continue
                if len(data) == chunk["size"] and sha256_bytes(data) == chunk["sha256"]:
                    return data
                skipped[0] += 1
                logger.error("CORRUPT chunk %d on %s; trying next replica",
                             chunk["index"], node["node_id"])
                try:   # best effort: lets the tracker schedule a repair
                    await self._tracker(build_report_corrupt(chunk["chunk_id"], node["node_id"]))
                except _NET_ERRORS:
                    pass
            if round_no < 2:
                # All known replicas failed. The list may be stale (a node just
                # died / a repair just finished): refresh it and retry.
                await asyncio.sleep(0.5 * (round_no + 1))
                fresh = (await self._tracker(build_get_file(name)))["file"]["chunks"]
                replicas = fresh[chunk["index"]]["replicas"]
        raise DownloadError(f"chunk {chunk['index']} unavailable on every replica")

    # ----------------------------------------------------------- management

    async def delete(self, name: str) -> None:
        await self._tracker(build_delete_file(name))

    async def list_files(self) -> list[dict]:
        return (await self._tracker(build_list_files()))["files"]

    async def status(self) -> dict:
        return await self._tracker(build_status())
