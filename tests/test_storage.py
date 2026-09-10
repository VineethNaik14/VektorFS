from pathlib import Path

import pytest

from node.storage import StorageManager


def test_store_and_get_chunk(tmp_path: Path):
    storage = StorageManager(tmp_path)

    chunk_id = "chunk123"
    data = b"Hello VektorFS!"

    storage.store(chunk_id, data)

    assert storage.get(chunk_id) == data

def test_exists_returns_true_for_stored_chunk(tmp_path: Path):
    storage = StorageManager(tmp_path)

    storage.store("chunk123", b"Hello VektorFS!")

    assert storage.exists("chunk123") is True

def test_exists_returns_false_for_missing_chunk(tmp_path: Path):
    storage = StorageManager(tmp_path)

    assert storage.exists("missing_chunk") is False

def test_delete_chunk(tmp_path: Path):
    storage = StorageManager(tmp_path)

    storage.store("chunk123", b"Hello VektorFS!")

    storage.delete("chunk123")

    assert storage.exists("chunk123") is False

def test_get_missing_chunk_raises_error(tmp_path: Path):
    storage = StorageManager(tmp_path)

    with pytest.raises(FileNotFoundError):
        storage.get("missing_chunk")

def test_delete_missing_chunk_raises_error(tmp_path: Path):
    storage = StorageManager(tmp_path)

    with pytest.raises(FileNotFoundError):
        storage.delete("missing_chunk")

def test_multiple_chunks_are_stored_independently(tmp_path: Path):
    storage = StorageManager(tmp_path)

    storage.store("chunk1", b"First chunk")
    storage.store("chunk2", b"Second chunk")

    assert storage.get("chunk1") == b"First chunk"
    assert storage.get("chunk2") == b"Second chunk"

def test_storage_directory_is_created(tmp_path: Path):
    storage_path = tmp_path / "chunks"

    StorageManager(storage_path)

    assert storage_path.exists()
    assert storage_path.is_dir()

def test_chunk_id_cannot_escape_storage_directory(tmp_path: Path):
    storage = StorageManager(tmp_path / "chunks")

    with pytest.raises(ValueError):
        storage.store("../outside", b"malicious data")


def test_store_and_get_binary_data(tmp_path: Path):
    storage = StorageManager(tmp_path)

    data = bytes(range(256))

    storage.store("binary_chunk", data)

    assert storage.get("binary_chunk") == data