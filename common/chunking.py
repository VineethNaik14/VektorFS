from pathlib import Path

CHUNK_SIZE = 4 * 1024 * 1024


def chunk_file(file_path: str, output_dir: str) -> list[Path]:
    
    source = Path(file_path)
    destination = Path(output_dir)

    if not source.is_file():
        raise FileNotFoundError(f"File not found: {source}")

    destination.mkdir(parents=True, exist_ok=True)

    chunks = []

    with source.open("rb") as file:
        index = 0

        while True:
            data = file.read(CHUNK_SIZE)

            if not data:
                break

            chunk_path = destination / f"chunk_{index:06d}"
            chunk_path.write_bytes(data)

            chunks.append(chunk_path)
            index += 1

    return chunks


def reassemble_file(chunk_paths: list[str], output_file: str,) -> Path:

    output = Path(output_file)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("wb") as destination:
        for chunk_path in chunk_paths:
            chunk = Path(chunk_path)

            if not chunk.is_file():
                raise FileNotFoundError(f"Chunk not found: {chunk}")

            with chunk.open("rb") as source:
                while data := source.read(1024 * 1024):
                    destination.write(data)

    return output

# ---------------------------------------------------------------------------
# Streaming helpers used by the client. chunk_file() above writes every chunk
# to disk first (doubling disk usage); for uploads we instead scan once for
# hashes and later read one chunk at a time, so memory stays bounded.
# ---------------------------------------------------------------------------

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class ChunkInfo:
    index: int
    size: int
    sha256: str


@dataclass(frozen=True)
class FileScan:
    size: int
    sha256: str
    chunks: list[ChunkInfo]


def chunk_count(size: int, chunk_size: int = CHUNK_SIZE) -> int:
    """Number of chunks for a file of `size` bytes (0 for an empty file)."""
    return -(-size // chunk_size)


def read_chunk(file_path: str, index: int, chunk_size: int = CHUNK_SIZE) -> bytes:
    """Read exactly one chunk by index without loading the rest of the file."""
    with open(file_path, "rb") as file:
        file.seek(index * chunk_size)
        return file.read(chunk_size)


def scan_file(file_path: str, chunk_size: int = CHUNK_SIZE) -> FileScan:
    """One pass over the file: whole-file SHA-256 plus per-chunk size/SHA-256."""
    source = Path(file_path)
    if not source.is_file():
        raise FileNotFoundError(f"File not found: {source}")

    whole = hashlib.sha256()
    chunks: list[ChunkInfo] = []
    total = 0
    with source.open("rb") as file:
        while data := file.read(chunk_size):
            whole.update(data)
            chunks.append(
                ChunkInfo(len(chunks), len(data), hashlib.sha256(data).hexdigest())
            )
            total += len(data)
    return FileScan(total, whole.hexdigest(), chunks)
