"""Atomic JSON checkpoints and cooperative stop signals for plain commands."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import tempfile
from types import FrameType
from typing import Any


class Checkpoint:
    """Use MYCE_CHECKPOINT_DIR, which already contains prior attempt saves.

    Saves are self-contained JSON files. I/O errors propagate so the job can
    report and retry NAS failures; corrupted complete saves fail loudly.
    """

    def __init__(self, directory: str | Path | None = None):
        if directory is None:
            directory = os.environ["MYCE_CHECKPOINT_DIR"]
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def save(self, value: Any, name: str = "checkpoint.json") -> Path:
        """Flush/fsync a same-directory temp, replace, then fsync directory."""
        if Path(name).name != name or name in {"", ".", ".."} or not name.endswith(".json"):
            raise ValueError("checkpoint name must be a plain .json filename")
        data = json.dumps(value, allow_nan=False).encode("utf-8")
        path = self.directory / name
        # Failed temporary writes remain ignored and recoverable for inspection.
        with tempfile.NamedTemporaryFile(dir=self.directory, suffix=".tmp", delete=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
        fd = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return path

    def load(self, default: Any = None) -> Any:
        """Load newest complete JSON save by mtime; ignore temporary files."""
        paths = [p for p in self.directory.glob("*.json") if p.is_file()]
        if not paths:
            return default
        newest = max(paths, key=lambda p: (p.stat().st_mtime_ns, p.name))
        return json.loads(newest.read_text(encoding="utf-8"))


class StopSignal:
    """Context manager setting requested on SIGTERM/SIGINT, restoring handlers.

    Use on the main thread. Check requested between work units, save explicitly,
    then exit. Saving inside a signal handler is unsafe; this only sets a flag.
    """

    def __init__(self) -> None:
        self.requested = False
        self._previous: dict[int, Any] = {}

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        self.requested = True

    def __enter__(self) -> StopSignal:
        if self._previous:
            raise RuntimeError("StopSignal context is already active")
        for signum in (signal.SIGTERM, signal.SIGINT):
            self._previous[signum] = signal.signal(signum, self._handle)
        return self

    def __exit__(self, *exc: Any) -> None:
        for signum, previous in self._previous.items():
            signal.signal(signum, previous)
        self._previous.clear()
