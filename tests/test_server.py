import asyncio
from pathlib import Path

import pytest
import pytest_asyncio

from common.messages import (
    build_delete_request,
    build_exists_request,
    build_get_request,
    build_info_request,
    build_store_request,
    parse_response,
)
from common.protocol import ProtocolError, read_message, write_message
from node import server as server_module
from node.server import StorageNodeServer
from node.storage import StorageManager


@pytest_asyncio.fixture
async def running_server(tmp_path: Path):
    storage = StorageManager(tmp_path)
    # port=0 asks the OS for a free ephemeral port so tests never collide
    # with each other or with anything already listening on the machine.
    node = StorageNodeServer(storage, host="127.0.0.1", port=0)
    await node.start()
    try:
        yield node
    finally:
        await node.stop()


async def open_client(
    node: StorageNodeServer,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    host, port = node.address
    return await asyncio.open_connection(host, port)


async def close_client(writer: asyncio.StreamWriter) -> None:
    writer.close()
    await writer.wait_closed()


# ---------------------------------------------------------------------------
# Happy path: one client, full command lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_store_get_exists_info_delete_round_trip(running_server):
    reader, writer = await open_client(running_server)
    try:
        chunk_id = "chunk_abc"
        data = b"hello vektorfs"

        await write_message(writer, build_store_request(chunk_id, data))
        assert parse_response(await read_message(reader))["status"] == "OK"

        await write_message(writer, build_exists_request(chunk_id))
        assert parse_response(await read_message(reader))["exists"] is True

        await write_message(writer, build_info_request(chunk_id))
        info = parse_response(await read_message(reader))
        assert info["chunk_id"] == chunk_id
        assert info["size"] == len(data)

        await write_message(writer, build_get_request(chunk_id))
        got = parse_response(await read_message(reader))
        from common.protocol import decode_chunk_data

        assert decode_chunk_data(got["data"]) == data

        await write_message(writer, build_delete_request(chunk_id))
        assert parse_response(await read_message(reader))["status"] == "OK"

        await write_message(writer, build_exists_request(chunk_id))
        assert parse_response(await read_message(reader))["exists"] is False
    finally:
        await close_client(writer)


@pytest.mark.asyncio
async def test_connection_stays_open_across_multiple_requests(running_server):
    """One TCP connection should be reusable for many requests, not one-shot."""
    reader, writer = await open_client(running_server)
    try:
        for i in range(5):
            chunk_id = f"chunk_{i}"
            await write_message(writer, build_store_request(chunk_id, str(i).encode()))
            assert parse_response(await read_message(reader))["status"] == "OK"
    finally:
        await close_client(writer)


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_missing_chunk_returns_chunk_not_found(running_server):
    reader, writer = await open_client(running_server)
    try:
        await write_message(writer, build_get_request("does_not_exist"))
        response = await read_message(reader)
        with pytest.raises(ProtocolError, match="CHUNK_NOT_FOUND"):
            parse_response(response)
    finally:
        await close_client(writer)


@pytest.mark.asyncio
async def test_path_traversal_chunk_id_returns_invalid_chunk_id(running_server):
    reader, writer = await open_client(running_server)
    try:
        await write_message(writer, build_get_request("../../etc/passwd"))
        response = await read_message(reader)
        with pytest.raises(ProtocolError, match="INVALID_CHUNK_ID"):
            parse_response(response)
    finally:
        await close_client(writer)


@pytest.mark.asyncio
async def test_malformed_json_gets_error_but_connection_survives(running_server):
    reader, writer = await open_client(running_server)
    try:
        # Send a frame whose body isn't valid JSON.
        body = b"not json"
        header = len(body).to_bytes(4, "big")
        writer.write(header + body)
        await writer.drain()

        response = await read_message(reader)
        with pytest.raises(ProtocolError, match="INVALID_REQUEST"):
            parse_response(response)

        # Connection should still be usable afterwards.
        await write_message(writer, build_exists_request("whatever"))
        assert parse_response(await read_message(reader))["exists"] is False
    finally:
        await close_client(writer)


@pytest.mark.asyncio
async def test_unknown_command_returns_invalid_request(running_server):
    reader, writer = await open_client(running_server)
    try:
        await write_message(writer, {"command": "DROP_TABLE", "chunk_id": "x"})
        response = await read_message(reader)
        with pytest.raises(ProtocolError, match="INVALID_REQUEST"):
            parse_response(response)
    finally:
        await close_client(writer)


@pytest.mark.asyncio
async def test_oversized_declared_length_closes_connection(running_server):
    reader, writer = await open_client(running_server)
    try:
        header = (20 * 1024 * 1024).to_bytes(4, "big")
        writer.write(header)
        await writer.drain()

        # Server tells us why, then closes rather than trying to keep
        # reading from a stream it can no longer trust the framing of.
        response = await read_message(reader)
        with pytest.raises(ProtocolError, match="INVALID_REQUEST"):
            parse_response(response)

        assert await reader.read() == b""
    finally:
        await close_client(writer)


# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_idle_connection_is_closed_after_timeout(running_server, monkeypatch):
    monkeypatch.setattr(server_module, "READ_TIMEOUT_SECONDS", 0.05)
    reader, writer = await open_client(running_server)
    try:
        data = await asyncio.wait_for(reader.read(), timeout=2)
        assert data == b""
    finally:
        await close_client(writer)


# ---------------------------------------------------------------------------
# Concurrency: two clients, two chunk ids, interleaved
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_clients_do_not_interfere(running_server):
    async def store_and_verify(index: int) -> None:
        reader, writer = await open_client(running_server)
        try:
            chunk_id = f"concurrent_{index}"
            payload = f"payload-{index}".encode()

            await write_message(writer, build_store_request(chunk_id, payload))
            assert parse_response(await read_message(reader))["status"] == "OK"

            await write_message(writer, build_get_request(chunk_id))
            from common.protocol import decode_chunk_data

            got = parse_response(await read_message(reader))
            assert decode_chunk_data(got["data"]) == payload
        finally:
            await close_client(writer)

    await asyncio.gather(*(store_and_verify(i) for i in range(10)))
