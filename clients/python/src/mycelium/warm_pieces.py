"""Main-process helper for a fixed warm-pieces attempt."""

from __future__ import annotations

import array
import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WarmItem:
    key: str
    args: tuple[str, ...]
    inputs: dict[str, Path]
    output_dir: Path
    checkpoint_dir: Path | None


class WarmPieces:
    """Open in main before starting subprocesses. Settle each item before next."""

    def __init__(self):
        fd = int(os.environ.pop("MYCE_WARM_PIECES_FD"))
        os.set_inheritable(fd, False)
        self._channel = socket.socket(fileno=fd)
        self._pid = os.getpid()
        self._active = False
        self._ended = False
        os.register_at_fork(after_in_child=self._channel.close)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._channel.close()

    def _call(self, operation):
        if os.getpid() != self._pid:
            raise RuntimeError("warm pieces are available only in main")
        local, worker = socket.socketpair()
        with local, worker:
            rights = array.array("i", [worker.fileno()])
            self._channel.sendmsg([operation], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)])
            worker.close()
            with local.makefile("rb") as reply:
                line = reply.readline()
        if not line:
            raise RuntimeError("warm piece channel closed before acknowledgement")
        response = json.loads(line)
        if "error" in response:
            raise RuntimeError(response["error"])
        return response

    def next(self) -> WarmItem | None:
        if self._active:
            raise RuntimeError("settle the current warm item before requesting another")
        if self._ended:
            return None
        item = self._call(b"N").get("item")
        if item is None:
            self._ended = True
            return None
        self._active = True
        checkpoint = item.get("checkpoint_dir")
        return WarmItem(
            key=item["key"],
            args=tuple(item["args"] or []),
            inputs={name: Path(path) for name, path in item["inputs"].items()},
            output_dir=Path(item["output_dir"]),
            checkpoint_dir=Path(checkpoint) if checkpoint else None,
        )

    def checkpoint(self):
        """Publish a complete ordinary checkpoint for the active item."""
        if not self._active:
            raise RuntimeError("warm piece checkpoint requires an active item")
        self._call(b"C")

    def success(self):
        """Wait for verification and durable acceptance of declared outputs."""
        self._settle(b"S")

    def failure(self):
        """Settle an application error and allow the remaining items to run."""
        self._settle(b"F")

    def _settle(self, operation):
        if not self._active:
            raise RuntimeError("warm piece report requires an active item")
        self._call(operation)
        self._active = False
