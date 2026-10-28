import json

import pytest

from client import cli
from client.cli import format_status


@pytest.mark.asyncio
async def test_cli_roundtrip(cluster, sample_file, tmp_path, capsys):
    t = "%s:%d" % cluster.tracker_addr
    async def run(*argv):
        return await cli._main(cli._parser().parse_args(["--tracker", t, *argv]))
    out = tmp_path / "out.bin"
    assert await run("upload", str(sample_file), "--chunk-size", "65536") == 0
    assert await run("ls") == 0
    assert await run("status") == 0
    assert await run("scrub") == 0
    assert await run("download", sample_file.name, str(out)) == 0
    assert out.read_bytes() == sample_file.read_bytes()
    assert await run("rm", sample_file.name) == 0
    printed = capsys.readouterr().out
    assert "ALIVE" in printed and "scrub: checked=" in printed and "(verified)" in printed


@pytest.mark.asyncio
async def test_cli_status_json_is_valid(cluster, capsys):
    t = "%s:%d" % cluster.tracker_addr
    await cli._main(cli._parser().parse_args(["--tracker", t, "status", "--json"]))
    assert json.loads(capsys.readouterr().out)["replication_factor"] == 3


def test_format_status_marks_dead_nodes():
    text = format_status({"replication_factor": 3, "files": 1,
        "chunks": {"total": 2, "healthy": 1, "under_replicated": 1, "lost": 0},
        "nodes": [{"node_id": "n1", "host": "h", "port": 1, "alive": False, "chunks": 0, "last_seen_age": 9.0}]})
    assert "DEAD" in text and "under-replicated=1" in text
