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
    st = sub.add_parser("status"); st.add_argument("--json", action="store_true", help="raw JSON")
    sub.add_parser("scrub")
    return p


def format_status(st: dict) -> str:
    c = st["chunks"]
    lines = [f"VektorFS cluster  RF={st['replication_factor']}  files={st['files']}  "
             f"chunks: total={c['total']} healthy={c['healthy']} "
             f"under-replicated={c['under_replicated']} lost={c['lost']}",
             f"{'NODE':<12}{'ADDRESS':<22}{'STATE':<7}{'CHUNKS':>7}  LAST SEEN"]
    for n in sorted(st["nodes"], key=lambda n: n["node_id"]):
        lines.append(f"{n['node_id']:<12}{n['host'] + ':' + str(n['port']):<22}"
                     f"{'ALIVE' if n['alive'] else 'DEAD':<7}{n['chunks']:>7}  {n['last_seen_age']}s ago")
    return "\n".join(lines)


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
        st = await client.status()
        print(json.dumps(st, indent=2) if args.json else format_status(st))
    elif args.cmd == "scrub":
        r = await client.scrub()
        print(f"scrub: checked={r['checked']} corrupt={r['corrupt']} missing={r['missing']}")
    return 0


def main() -> None:
    try:
        sys.exit(asyncio.run(_main(_parser().parse_args())))
    except (VektorError, ProtocolError, OSError, asyncio.TimeoutError) as exc:
        print(f"error: {exc!r}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
