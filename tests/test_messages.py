import pytest

from common.messages import (
    ErrorCode,
    Status,
    build_delete_request,
    build_error_response,
    build_exists_request,
    build_get_request,
    build_info_request,
    build_ok_response,
    build_store_request,
    parse_request,
    parse_response,
)
from common.protocol import Command, ProtocolError

# ---------------------------------------------------------------------------
# Request builders + parse_request round trips
# ---------------------------------------------------------------------------


def test_store_request_round_trip():
    data = b"Hello VektorFS!"
    request = build_store_request("chunk1", data)

    command, fields = parse_request(request)

    assert command is Command.STORE
    assert fields["chunk_id"] == "chunk1"
    assert fields["data"] == data


def test_get_request_round_trip():
    request = build_get_request("chunk1")
    command, fields = parse_request(request)

    assert command is Command.GET
    assert fields == {"chunk_id": "chunk1"}


def test_delete_request_round_trip():
    request = build_delete_request("chunk1")
    command, fields = parse_request(request)

    assert command is Command.DELETE
    assert fields == {"chunk_id": "chunk1"}


def test_exists_request_round_trip():
    request = build_exists_request("chunk1")
    command, fields = parse_request(request)

    assert command is Command.EXISTS
    assert fields == {"chunk_id": "chunk1"}


def test_info_request_round_trip():
    request = build_info_request("chunk1")
    command, fields = parse_request(request)

    assert command is Command.INFO
    assert fields == {"chunk_id": "chunk1"}


# ---------------------------------------------------------------------------
# parse_request validation
# ---------------------------------------------------------------------------


def test_parse_request_rejects_unknown_command():
    with pytest.raises(ProtocolError):
        parse_request({"command": "DROP_TABLE", "chunk_id": "x"})


def test_parse_request_rejects_missing_command():
    with pytest.raises(ProtocolError):
        parse_request({"chunk_id": "x"})


def test_parse_request_rejects_missing_chunk_id():
    with pytest.raises(ProtocolError):
        parse_request({"command": "GET"})


def test_parse_request_rejects_empty_chunk_id():
    with pytest.raises(ProtocolError):
        parse_request({"command": "GET", "chunk_id": ""})


def test_parse_request_rejects_non_string_chunk_id():
    with pytest.raises(ProtocolError):
        parse_request({"command": "GET", "chunk_id": 123})


def test_parse_request_store_rejects_missing_data():
    with pytest.raises(ProtocolError):
        parse_request({"command": "STORE", "chunk_id": "chunk1"})


def test_parse_request_store_rejects_non_string_data():
    with pytest.raises(ProtocolError):
        parse_request({"command": "STORE", "chunk_id": "chunk1", "data": 12345})


def test_parse_request_store_rejects_invalid_base64():
    with pytest.raises(ProtocolError):
        parse_request(
            {"command": "STORE", "chunk_id": "chunk1", "data": "!!!not-base64!!!"}
        )


# ---------------------------------------------------------------------------
# Response builders + parse_response
# ---------------------------------------------------------------------------


def test_ok_response_with_extra_fields():
    response = build_ok_response(exists=True)

    assert response["status"] == Status.OK.value
    assert response["exists"] is True

    parsed = parse_response(response)
    assert parsed == response


def test_error_response_raises_on_parse():
    response = build_error_response(ErrorCode.CHUNK_NOT_FOUND, "chunk1 not found")

    assert response["status"] == Status.ERROR.value

    with pytest.raises(ProtocolError, match="CHUNK_NOT_FOUND"):
        parse_response(response)


def test_parse_response_rejects_missing_status():
    with pytest.raises(ProtocolError):
        parse_response({"data": "..."})


def test_parse_response_rejects_invalid_status():
    with pytest.raises(ProtocolError):
        parse_response({"status": "MAYBE"})
        