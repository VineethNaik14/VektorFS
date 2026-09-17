from pathlib import Path


class StorageManager:

    def __init__(self, storage_path: Path):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

    def _get_chunk_path(self, chunk_id: str) -> Path:
        chunk_path = (self.storage_path / chunk_id).resolve()
        storage_path = self.storage_path.resolve()

        if storage_path not in chunk_path.parents:
            raise ValueError("Invalid chunk ID")

        return chunk_path

    def store(self, chunk_id: str, data: bytes) -> None:
        chunk_path = self._get_chunk_path(chunk_id)
        chunk_path.write_bytes(data)

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
