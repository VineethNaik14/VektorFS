"""Durable storage for the tracker's FILE metadata.

Only file -> chunk(id, index, size, sha256) metadata is persisted. Replica
*locations* are deliberately not stored: nodes re-report what is on their
disks when they register, and a persisted location list could only be stale.

Two interchangeable back ends share one tiny interface:

  * JsonFileStore  - zero dependencies, atomic whole-file snapshots (default).
  * PostgresStore  - transactional, row-per-file/chunk (needs `psycopg`).

Both are synchronous and are called from worker threads by the tracker
(asyncio.to_thread) under a single lock, so they never run concurrently and
never block the event loop.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger("vektorfs.tracker.store")


class MetadataStore(Protocol):
    def load(self) -> list[dict[str, Any]]: ...
    def put_file(self, record: dict[str, Any]) -> None: ...
    def delete_file(self, name: str) -> None: ...
    def close(self) -> None: ...


class JsonFileStore:
    """Whole-file JSON snapshot, written atomically (tmp + fsync + rename).

    The on-disk format ({"files": [...]}) is the same one ClusterState.save()
    has always produced, so existing metadata files keep working.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        # Creating the directory here (and writing once below) means a bad
        # --metadata-file path fails at startup, not on the first upload.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._files: dict[str, dict[str, Any]] = {}
        if self.path.is_file():
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self._files = {f["name"]: f for f in data.get("files", [])}
        else:
            self._flush(self._files)

    def load(self) -> list[dict[str, Any]]:
        return list(self._files.values())

    def put_file(self, record: dict[str, Any]) -> None:
        updated = {**self._files, record["name"]: record}
        self._flush(updated)            # only adopt the new state once it is on disk
        self._files = updated

    def delete_file(self, name: str) -> None:
        updated = {k: v for k, v in self._files.items() if k != name}
        self._flush(updated)
        self._files = updated

    def close(self) -> None:
        pass

    def _flush(self, files: dict[str, dict[str, Any]]) -> None:
        tmp = self.path.with_name(self.path.name + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump({"files": list(files.values())}, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    name       TEXT PRIMARY KEY,
    file_id    TEXT NOT NULL UNIQUE,
    size       BIGINT NOT NULL,
    chunk_size INTEGER NOT NULL,
    sha256     CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id  TEXT PRIMARY KEY,
    file_name TEXT NOT NULL REFERENCES files(name) ON DELETE CASCADE,
    idx       INTEGER NOT NULL,
    size      INTEGER NOT NULL,
    sha256    CHAR(64) NOT NULL,
    UNIQUE (file_name, idx)
);
"""


class PostgresStore:
    """PostgreSQL back end. `psycopg` (v3) is imported lazily so the rest of
    VektorFS stays dependency-free unless this store is actually selected."""

    def __init__(self, dsn: str):
        try:
            import psycopg
        except ImportError as exc:      # pragma: no cover - environment specific
            raise RuntimeError(
                "PostgreSQL support needs psycopg: pip install 'psycopg[binary]'"
            ) from exc
        self._psycopg = psycopg
        self.dsn = dsn
        self._conn = None
        self._with_conn(lambda conn: conn.execute(_SCHEMA))

    def _connect(self):
        if self._conn is None or self._conn.closed:
            self._conn = self._psycopg.connect(self.dsn, connect_timeout=5)
        return self._conn

    def _with_conn(self, work):
        """Run `work(conn)` in one transaction; reconnect once if the link dropped."""
        for attempt in (1, 2):
            conn = self._connect()
            try:
                with conn.transaction():
                    return work(conn)
            except self._psycopg.OperationalError:
                self._drop_connection()
                if attempt == 2:
                    raise
            except Exception:
                raise

    def _drop_connection(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:           # noqa: BLE001 - already broken
                pass
            self._conn = None

    def load(self) -> list[dict[str, Any]]:
        def work(conn):
            files = {
                name: {"name": name, "file_id": fid, "size": size,
                       "chunk_size": csz, "sha256": sha.strip(), "chunks": []}
                for name, fid, size, csz, sha in conn.execute(
                    "SELECT name, file_id, size, chunk_size, sha256 FROM files")
            }
            for cid, fname, idx, size, sha in conn.execute(
                    "SELECT chunk_id, file_name, idx, size, sha256 FROM chunks ORDER BY idx"):
                files[fname]["chunks"].append(
                    {"chunk_id": cid, "index": idx, "size": size, "sha256": sha.strip()})
            return list(files.values())
        return self._with_conn(work)

    def put_file(self, record: dict[str, Any]) -> None:
        def work(conn):
            conn.execute(
                "INSERT INTO files (name, file_id, size, chunk_size, sha256) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (name) DO UPDATE SET "
                "file_id = EXCLUDED.file_id, size = EXCLUDED.size, "
                "chunk_size = EXCLUDED.chunk_size, sha256 = EXCLUDED.sha256",
                (record["name"], record["file_id"], record["size"],
                 record["chunk_size"], record["sha256"]))
            conn.execute("DELETE FROM chunks WHERE file_name = %s", (record["name"],))
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO chunks (chunk_id, file_name, idx, size, sha256) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    [(c["chunk_id"], record["name"], c["index"], c["size"], c["sha256"])
                     for c in record["chunks"]])
        self._with_conn(work)

    def delete_file(self, name: str) -> None:
        self._with_conn(lambda conn: conn.execute("DELETE FROM files WHERE name = %s", (name,)))

    def close(self) -> None:
        self._drop_connection()


def open_store(metadata_file: str | None = None, postgres_dsn: str | None = None):
    """Pick a back end from CLI/env settings. Postgres wins if both are given."""
    if postgres_dsn:
        logger.info("using PostgreSQL metadata store")
        return PostgresStore(postgres_dsn)
    if metadata_file:
        logger.info("using JSON metadata store at %s", metadata_file)
        return JsonFileStore(metadata_file)
    return None
