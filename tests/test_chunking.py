from pathlib import Path
import pytest
from common.chunking import chunk_file, reassemble_file

def test_chunk_and_reassemble(tmp_path: Path):
    original = tmp_path / "original.txt"
    chunks_dir = tmp_path / "chunks"
    reconstructed = tmp_path / "reconstructed.txt"

    data = b"Hello VektorFS! " * 1000
    original.write_bytes(data)

    chunks = chunk_file(
        str(original),
        str(chunks_dir),
    )

    assert len(chunks) > 0

    reassemble_file(
        [str(chunk) for chunk in chunks],
        str(reconstructed),
    )

    assert reconstructed.read_bytes() == data

def test_empty_file(tmp_path: Path):
    original = tmp_path / "empty.txt"
    chunks_dir = tmp_path / "chunks"

    original.write_bytes(b"")

    chunks = chunk_file(
        str(original),
        str(chunks_dir),
    )

    assert chunks == []

def test_small_file_creates_one_chunk(tmp_path: Path):
    original = tmp_path / "small.txt"
    chunks_dir = tmp_path / "chunks"

    data = b"A" * 100
    original.write_bytes(data)

    chunks = chunk_file(
        str(original),
        str(chunks_dir),
    )

    assert len(chunks) == 1
    assert chunks[0].read_bytes() == data

def test_missing_file_raises_error(tmp_path: Path):
    missing_file = tmp_path / "does_not_exist.txt"
    chunks_dir = tmp_path / "chunks"

    with pytest.raises(FileNotFoundError):
        chunk_file(
            str(missing_file),
            str(chunks_dir),
        )

def test_missing_chunk_raises_error(tmp_path: Path):
    output = tmp_path / "reconstructed.txt"
    missing_chunk = tmp_path / "missing_chunk"

    with pytest.raises(FileNotFoundError):
        reassemble_file(
            [str(missing_chunk)],
            str(output),
        )