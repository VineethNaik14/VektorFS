from pathlib import Path
import pytest
from common.hashing import sha256_file

def test_sha256_is_deterministic(tmp_path: Path):
    file_path = tmp_path / "test.txt"

    file_path.write_bytes(b"hello VektorFS")

    hash1 = sha256_file(str(file_path))
    hash2 = sha256_file(str(file_path))

    assert hash1 == hash2
    assert len(hash1) == 64

def test_different_content_produces_different_hash(tmp_path: Path):
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"

    file1.write_bytes(b"hello")
    file2.write_bytes(b"world")

    assert sha256_file(str(file1)) != sha256_file(str(file2))

def test_missing_file_raises_error(tmp_path: Path):
    missing_file = tmp_path / "does_not_exist.txt"

    with pytest.raises(FileNotFoundError):
        sha256_file(str(missing_file))