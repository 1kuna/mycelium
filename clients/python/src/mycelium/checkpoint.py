"""Atomic JSON checkpoints and cooperative stop signals for plain commands."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sys
import tempfile
from types import FrameType
from typing import Any


def _is_linux_cifs(directory: Path) -> bool:
    # CIFS directory fsync is a no-op in Linux; unknown mounts retain fsync.
    if sys.platform != "linux":
        return False
    try:
        target = directory.resolve()
        mounts = Path("/proc/self/mountinfo").read_text(encoding="utf-8")
        best = (0, "")
        for line in mounts.splitlines():
            fields = line.split()
            separator = fields.index("-")
            mount = fields[4]
            for escaped, literal in ((r"\040", " "), (r"\011", "\t"), (r"\012", "\n"), (r"\134", "\\")):
                mount = mount.replace(escaped, literal)
            path = Path(mount)
            if target == path or path in target.parents:
                if len(mount) > best[0]:
                    best = (len(mount), fields[separator + 1])
                elif len(mount) == best[0] and fields[separator + 1] != best[1]:
                    best = (best[0], "unknown")
        return best[1] == "cifs"
    except (OSError, ValueError, IndexError):
        return False


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
        """Fsync temp, atomically replace, then sync directory except Linux CIFS."""
        if Path(name).name != name or name in {"", ".", ".."} or not name.endswith(".json"):
            raise ValueError("checkpoint name must be a plain .json filename")
        generation = max((g for g, _, _ in self._saves()), default=0) + 1
        if generation >= 2**63:
            raise OverflowError("checkpoint generation exhausted")
        data = json.dumps({"myce_checkpoint": {"generation": generation}, "value": value}, allow_nan=False).encode("utf-8")
        path = self.directory / name
        # Failed temporary writes remain ignored and recoverable for inspection.
        with tempfile.NamedTemporaryFile(dir=self.directory, suffix=".tmp", delete=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
        if not _is_linux_cifs(self.directory):
            fd = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        return path

    def _saves(self):
        for path in self.directory.glob("*.json"):
            if path.is_file():
                value = json.loads(path.read_text(encoding="utf-8"))
                generation = 0
                metadata = value.get("myce_checkpoint") if isinstance(value, dict) else None
                if isinstance(metadata, dict) and "generation" in metadata:
                    generation = metadata["generation"]
                    if type(generation) is not int or not 1 <= generation < 2**63:
                        raise ValueError("invalid checkpoint generation")
                    value = value["value"]
                yield generation, path.name, value

    def load(self, default: Any = None) -> Any:
        """Highest generation wins; legacy/tied saves use greatest filename."""
        saves = list(self._saves())
        if not saves:
            return default
        return max(saves, key=lambda save: save[:2])[2]


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
