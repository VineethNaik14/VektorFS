"""Tracker wire-level validation and limits."""

import asyncio

import pytest
import pytest_asyncio

from common.messages import RemoteError
from common.protocol import read_message, write_message
from common.rpc import call
from common.tracker_messages import build_heartbeat, build_register, build_status
from tracker.server import TrackerServer


@pytest_asyncio.fixture
async def tracker():
    t = TrackerServer("127.0.0.1", 0, heartbeat_ttl=5)
    await t.start()
    yield t
    await t.stop()


@pytest.mark.asyncio
async def test_register_heartbeat_and_unknown_node(tracker):
    r = await call(*tracker.address, build_register("n1", "", 9000, []))
    assert r["heartbeat_interval"] > 0 and r["orphans"] == []
    await call(*tracker.address, build_heartbeat("n1"))
    with pytest.raises(RemoteError) as e:
        await call(*tracker.address, build_heartbeat("stranger"))
    assert e.value.code == "UNKNOWN_NODE"
    status = await call(*tracker.address, build_status())
    assert status["nodes"][0]["host"] == "127.0.0.1"      # inferred from the peer address


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [
    {"command": "NOPE"}, {"command": "REGISTER"},
    {"command": "REGISTER", "node_id": "../x", "port": 1},
    {"command": "REGISTER", "node_id": "n", "port": 70000},
    {"command": "ALLOCATE", "name": "f", "size": 1, "chunk_size": 10**9, "num_chunks": 1},
    {"command": "ALLOCATE", "name": "bad\x00name", "size": 1, "chunk_size": 1, "num_chunks": 1},
    {"command": "GET_FILE", "name": ""},
    {"command": "COMMIT", "file_id": "zz", "sha256": "a" * 64, "chunks": []},
])
async def test_malformed_requests_get_invalid_request(tracker, payload):
    with pytest.raises(RemoteError) as e:
        await call(*tracker.address, payload)
    assert e.value.code == "INVALID_REQUEST"


@pytest.mark.asyncio
async def test_oversized_frame_rejected_and_connection_closed(tracker):
    reader, writer = await asyncio.open_connection(*tracker.address)
    writer.write((20 * 1024 * 1024).to_bytes(4, "big")); await writer.drain()
    assert (await read_message(reader))["error"] == "INVALID_REQUEST"
    assert await reader.read() == b""
    writer.close()


@pytest.mark.asyncio
async def test_tracker_survives_garbage_and_keeps_serving(tracker):
    reader, writer = await asyncio.open_connection(*tracker.address)
    writer.write((8).to_bytes(4, "big") + b"not json"); await writer.drain()
    assert (await read_message(reader))["status"] == "ERROR"
    await write_message(writer, build_status())
    assert (await read_message(reader))["status"] == "OK"
    writer.close()
