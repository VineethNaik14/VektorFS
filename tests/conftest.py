import os

import pytest
import pytest_asyncio

from tests.cluster import Cluster


@pytest.fixture
def sample_file(tmp_path):
    """~350 KB of random bytes; with a 64 KiB chunk size that is 6 chunks."""
    path = tmp_path / "sample.bin"
    path.write_bytes(os.urandom(350_000))
    return path


@pytest_asyncio.fixture
async def cluster(tmp_path):
    c = await Cluster(tmp_path / "cluster", n_nodes=4).start()
    try:
        yield c
    finally:
        await c.stop()
