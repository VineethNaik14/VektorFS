import asyncio
import json

import pytest

from common.protocol import (
    Command,
    MessageTooLargeError,
    ProtocolError,
    decode_chunk_data,
    encode_chunk_data,
    encode_message,
    read_message,
)


def make_reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


def test_encode_message_header_matches_body_length():
    frame = encode_message({"command": Command.STORE.value})
    body = json.dumps({"command": Command.STORE.value}).encode("utf-8")

    header = frame[:4]
    assert int.from_bytes(header, "big") == len(body)
    assert frame[4:] == body


def test_encode_message_rejects_non_serializable_payload():
    with pytest.raises(ProtocolError):
        encode_message({"bad": object()})


def test_encode_message_rejects_oversized_payload():
    huge = {"data": "x" * (17 * 1024 * 1024)}
    with pytest.raises(MessageTooLargeError):
        encode_message(huge)


@pytest.mark.asyncio
async def test_read_message_round_trip():
    payload = {"command": Command.GET.value, "chunk_id": "abc123"}
    frame = encode_message(payload)
    reader = make_reader(frame)

    result = await read_message(reader)
    assert result == payload


@pytest.mark.asyncio
async def test_read_message_raises_on_truncated_header():
    reader = make_reader(b"\x00\x01")

    with pytest.raises(ConnectionError):
        await read_message(reader)


@pytest.mark.asyncio
async def test_read_message_raises_on_truncated_body():
    header = (10).to_bytes(4, "big")
    reader = make_reader(header + b"short")

    with pytest.raises(ConnectionError):
        await read_message(reader)


@pytest.mark.asyncio
async def test_read_message_rejects_oversized_declared_length():
    header = (20 * 1024 * 1024).to_bytes(4, "big")
    reader = make_reader(header)

    with pytest.raises(MessageTooLargeError):
        await read_message(reader)


@pytest.mark.asyncio
async def test_read_message_rejects_invalid_json():
    body = b"not json"
    header = len(body).to_bytes(4, "big")
    reader = make_reader(header + body)

    with pytest.raises(ProtocolError):
        await read_message(reader)


@pytest.mark.asyncio
async def test_read_message_rejects_non_dict_json():
    body = json.dumps([1, 2, 3]).encode("utf-8")
    header = len(body).to_bytes(4, "big")
    reader = make_reader(header + body)

    with pytest.raises(ProtocolError):
        await read_message(reader)


def test_chunk_data_encode_decode_round_trip():
    data = bytes(range(256))
    encoded = encode_chunk_data(data)

    assert isinstance(encoded, str)
    assert decode_chunk_data(encoded) == data


def test_decode_chunk_data_rejects_invalid_base64():
    with pytest.raises(ProtocolError):
        decode_chunk_data("not-valid-base64!!!")
