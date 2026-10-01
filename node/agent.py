"""Storage-node side of the tracker relationship: register, then heartbeat.

Kept separate from server.py on purpose: server.py answers requests, this
module *initiates* them. A node with no tracker (unit tests, standalone use)
simply never starts an agent.
"""

from __future__ import annotations

import asyncio
import logging

from common.messages import RemoteError
from common.rpc import call
from common.tracker_messages import build_heartbeat, build_register
from node.storage import StorageManager

logger = logging.getLogger("vektorfs.node.agent")

RPC_TIMEOUT_SECONDS = 5.0
RETRY_DELAY_SECONDS = 1.0


class TrackerAgent:
    def __init__(
        self,
        storage: StorageManager,
        node_id: str,
        tracker: tuple[str, int],
        advertise_host: str,
        advertise_port: int,
    ):
        self.storage = storage
        self.node_id = node_id
        self.tracker = tracker
        self.advertise_host = advertise_host
        self.advertise_port = advertise_port
        self.heartbeat_interval = 2.0  # replaced by the tracker's value
        self.registered = False

    async def register(self) -> None:
        # The chunk report is how the tracker learns what this node holds
        # after a restart - and how it spots chunks that were deleted while
        # we were away (returned as "orphans").
        chunks = await asyncio.to_thread(self.storage.list_chunks)
        response = await call(
            *self.tracker,
            build_register(
                self.node_id, self.advertise_host, self.advertise_port, chunks
            ),
            timeout=RPC_TIMEOUT_SECONDS,
        )
        self.heartbeat_interval = float(response["heartbeat_interval"])
        self.registered = True
        for chunk_id in response.get("orphans", []):
            try:
                await asyncio.to_thread(self.storage.delete, chunk_id)
            except (FileNotFoundError, ValueError):
                pass
        logger.info(
            "registered as %s (%d chunks reported, %d orphans removed)",
            self.node_id, len(chunks), len(response.get("orphans", [])),
        )

    async def run(self) -> None:
        """Forever: (re)register when needed, otherwise heartbeat."""
        while True:
            was_registered = self.registered
            try:
                if not was_registered:
                    await self.register()
                else:
                    await call(
                        *self.tracker,
                        build_heartbeat(self.node_id),
                        timeout=RPC_TIMEOUT_SECONDS,
                    )
            except RemoteError as exc:
                # Tracker restarted or declared us dead: our identity is
                # gone, so introduce ourselves (and our chunks) again.
                logger.warning("tracker rejected us (%s); re-registering", exc.code)
                self.registered = False
                if not was_registered:
                    # register() itself was refused: don't spin on it.
                    await asyncio.sleep(RETRY_DELAY_SECONDS)
                continue
            except (asyncio.TimeoutError, OSError, ConnectionError) as exc:
                logger.warning("tracker unreachable (%r); will retry", exc)
                await asyncio.sleep(RETRY_DELAY_SECONDS)
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("agent error; will retry")
                await asyncio.sleep(RETRY_DELAY_SECONDS)
                continue
            await asyncio.sleep(self.heartbeat_interval)
