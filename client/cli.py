"""Command-line front end:  python -m client.cli --tracker HOST:PORT <command>"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from client.client import VektorClient, VektorError
from common.chunking import CHUNK_SIZE
from common.protocol import ProtocolError


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="vektorfs")
    p.add_argument("--tracker", default="127.0.0.1:9100", help="host:port")
    sub = p.add_subparsers(dest="cmd", required=True)
    up = sub.add_parser("upload"); up.add_argument("path"); up.add_argument("--name")
    up.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    dl = sub.add_parser("download"); dl.add_argument("name"); dl.add_argument("dest")
    rm = sub.add_parser("rm"); rm.add_argument("name")
    sub.add_parser("ls")
    sub.add_parser("status")
    return p


async def _main(args) -> int:
    host, _, port = args.tracker.rpartition(":")
    client = VektorClient(host, int(port))
    if args.cmd == "upload":
        r = await client.upload(args.path, args.name, args.chunk_size)
        print(f"uploaded {r.name}: {r.size} bytes, {r.chunks} chunks, "
              f"replicas/chunk={r.replicas_per_chunk}\nsha256 {r.sha256}")
    elif args.cmd == "download":
        r = await client.download(args.name, args.dest)
        print(f"downloaded to {r.path}: {r.size} bytes, {r.chunks} chunks, "
              f"corrupt replicas skipped={r.corrupt_replicas_skipped}\n"
              f"sha256 {r.sha256}  (verified)")
    elif args.cmd == "rm":
        await client.delete(args.name); print("deleted")
    elif args.cmd == "ls":
        for f in await client.list_files():
            print(f"{f['size']:>12}  {f['chunks']:>4} chunks  {f['name']}")
    elif args.cmd == "status":
        print(json.dumps(await client.status(), indent=2))
    return 0


def main() -> None:
    try:
        sys.exit(asyncio.run(_main(_parser().parse_args())))
    except (VektorError, ProtocolError, OSError, asyncio.TimeoutError) as exc:
        print(f"error: {exc!r}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
