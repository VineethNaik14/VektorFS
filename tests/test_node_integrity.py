"""STORE checksum verification and node-to-node REPLICATE."""

import hashlib
from pathlib import Path

import pytest
import pytest_asyncio

from common.messages import RemoteError, build_get_request, build_info_request, build_replicate_request, build_store_request, parse_request
from common.protocol import ProtocolError, decode_chunk_data
from common.rpc import call
from node.server import StorageNodeServer
from node.storage import StorageManager


def sha(b): return hashlib.sha256(b).hexdigest()


@pytest_asyncio.fixture
async def two_nodes(tmp_path: Path):
    nodes = []
    for name in ("a", "b"):
        n = StorageNodeServer(StorageManager(tmp_path / name), "127.0.0.1", 0)
        await n.start(); nodes.append(n)
    yield nodes
    for n in nodes: await n.stop()


@pytest.mark.asyncio
async def test_store_with_wrong_sha256_is_refused_and_not_persisted(two_nodes):
    a, _ = two_nodes
    with pytest.raises(RemoteError) as e:
        await call(*a.address, build_store_request("c1", b"data", sha(b"other")))
    assert e.value.code == "CHECKSUM_MISMATCH" and not a.storage.exists("c1")


@pytest.mark.asyncio
async def test_store_with_correct_sha256_and_info_reports_it(two_nodes):
    a, _ = two_nodes
    await call(*a.address, build_store_request("c1", b"data", sha(b"data")))
    info = await call(*a.address, build_info_request("c1"))
    assert info["sha256"] == sha(b"data") and info["size"] == 4


@pytest.mark.asyncio
async def test_replicate_pulls_verified_copy_from_peer(two_nodes):
    a, b = two_nodes
    a.storage.store("c1", b"payload")
    await call(*b.address, build_replicate_request("c1", *a.address, sha(b"payload")))
    assert b.storage.get("c1") == b"payload"


@pytest.mark.asyncio
async def test_replicate_refuses_corrupt_source_and_stores_nothing(two_nodes):
    a, b = two_nodes
    a.storage.store("c1", b"bitrotted")
    with pytest.raises(RemoteError) as e:
        await call(*b.address, build_replicate_request("c1", *a.address, sha(b"payload")))
    assert e.value.code == "CHECKSUM_MISMATCH" and not b.storage.exists("c1")


@pytest.mark.asyncio
async def test_replicate_reports_unreachable_or_missing_source(two_nodes):
    a, b = two_nodes
    with pytest.raises(RemoteError) as e:                       # chunk absent on source
        await call(*b.address, build_replicate_request("nope", *a.address, sha(b"x")))
    assert e.value.code == "SOURCE_UNAVAILABLE"
    await a.stop()
    with pytest.raises(RemoteError) as e:                       # source down
        await call(*b.address, build_replicate_request("c1", "127.0.0.1", 1, sha(b"x")), timeout=10)
    assert e.value.code == "SOURCE_UNAVAILABLE"


@pytest.mark.parametrize("patch", [{"source_port": 0}, {"source_port": "9"}, {"source_port": True},
                                   {"source_host": ""}, {"sha256": "XYZ"}])
def test_replicate_request_validation(patch):
    req = {**build_replicate_request("c1", "h", 5, "a" * 64), **patch}
    with pytest.raises(ProtocolError):
        parse_request(req)


def test_store_request_rejects_malformed_sha256():
    with pytest.raises(ProtocolError):
        parse_request({**build_store_request("c", b"x"), "sha256": "nothex"})
