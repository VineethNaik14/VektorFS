import hashlib
from pathlib import Path


def sha256_file(file_path: str) -> str:
    
    path = Path(file_path)

    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    digest = hashlib.sha256()

    with path.open("rb") as file:
        while data := file.read(1024 * 1024):
            digest.update(data)

    return digest.hexdigest()
