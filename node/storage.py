import hashlib
import os
import re
import uuid
from pathlib import Path

# Chunk ids are flat file names: no separators, no leading dot (so they can
# never collide with our ".tmp-*" files), bounded length. This is the first
# line of defence; the resolve() check below stays as defence in depth.
_CHUNK_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}")
_TMP_PREFIX = ".tmp-"


class StorageManager:

    def __init__(self, storage_path: Path):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        # A crash between "write temp" and "rename" leaves a temp file behind.
        # It was never a valid chunk, so it is safe to discard on startup.
        for leftover in self.storage_path.glob(f"{_TMP_PREFIX}*"):
            leftover.unlink(missing_ok=True)

    def _get_chunk_path(self, chunk_id: str) -> Path:
        if not isinstance(chunk_id, str) or not _CHUNK_ID_RE.fullmatch(chunk_id):
            raise ValueError("Invalid chunk ID")

        chunk_path = (self.storage_path / chunk_id).resolve()
        storage_path = self.storage_path.resolve()

        if storage_path not in chunk_path.parents:
            raise ValueError("Invalid chunk ID")

        return chunk_path

    def store(self, chunk_id: str, data: bytes) -> None:
        """Atomically write a chunk.

        Write to a temp file, fsync, then os.replace(). Readers (and a
        restart after a crash) see either the complete old chunk or the
        complete new one, never a truncated file that would look valid.
        """
        chunk_path = self._get_chunk_path(chunk_id)
        tmp_path = self.storage_path / f"{_TMP_PREFIX}{uuid.uuid4().hex}"
        try:
            with tmp_path.open("wb") as file:
                file.write(data)
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp_path, chunk_path)
        finally:
            tmp_path.unlink(missing_ok=True)

    def get(self, chunk_id: str) -> bytes:
        chunk_path = self._get_chunk_path(chunk_id)

        if not chunk_path.is_file():
            raise FileNotFoundError(f"Chunk not found: {chunk_id}")

        return chunk_path.read_bytes()

    def exists(self, chunk_id: str) -> bool:
        chunk_path = self._get_chunk_path(chunk_id)
        return chunk_path.is_file()

    def delete(self, chunk_id: str) -> None:
        chunk_path = self._get_chunk_path(chunk_id)

        if not chunk_path.is_file():
            raise FileNotFoundError(f"Chunk not found: {chunk_id}")

        chunk_path.unlink()

    def info(self, chunk_id: str) -> dict:
        chunk_path = self._get_chunk_path(chunk_id)

        if not chunk_path.is_file():
            raise FileNotFoundError(f"Chunk not found: {chunk_id}")

        stat = chunk_path.stat()
        return {"chunk_id": chunk_id, "size": stat.st_size}

    def sha256(self, chunk_id: str) -> str:
        """SHA-256 of the bytes currently on disk (streamed, not loaded whole)."""
        chunk_path = self._get_chunk_path(chunk_id)
        if not chunk_path.is_file():
            raise FileNotFoundError(f"Chunk not found: {chunk_id}")
        digest = hashlib.sha256()
        with chunk_path.open("rb") as file:
            while block := file.read(1024 * 1024):
                digest.update(block)
        return digest.hexdigest()

    def list_chunks(self) -> list[str]:
        """Ids of every chunk on disk; used for the registration chunk report."""
        return sorted(
            p.name
            for p in self.storage_path.iterdir()
            if p.is_file() and _CHUNK_ID_RE.fullmatch(p.name)
        )
