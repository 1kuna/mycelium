"""Atomic JSON checkpoints and cooperative stop signals for plain commands."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
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


class CheckpointError(RuntimeError):
    """A typed cooperative checkpoint refusal; unique local bytes are retained."""
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


_PROTOCOL = "checkpoint-local-first-v1"


def _atomic_local(path: Path, data: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
        temporary = Path(stream.name)
    temporary.replace(path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


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
        self.protocol = os.environ.get("MYCE_CHECKPOINT_PROTOCOL", "")
        if self.protocol and self.protocol != _PROTOCOL:
            raise CheckpointError("checkpoint_protocol_unsupported", "unknown checkpoint protocol")

    def save(self, value: Any, name: str = "checkpoint.json") -> Path:
        """Fsync temp, atomically replace, then sync directory except Linux CIFS."""
        if Path(name).name != name or name in {"", ".", ".."} or not name.endswith(".json"):
            raise ValueError("checkpoint name must be a plain .json filename")
        if self.protocol:
            return self._save_local(value, name)
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

    def _generation(self):
        marker = self.directory / "generation.json"
        if not marker.exists():
            return None
        data = marker.read_bytes()
        saved = json.loads(data)
        meta = saved.get("myce_checkpoint", {})
        generation = meta.get("generation")
        files = meta.get("files", {})
        if saved.get("protocol") != _PROTOCOL or type(generation) is not int or not 0 < generation < 2**63 or list(files) != [f"generation-{generation}.json"]:
            raise CheckpointError("checkpoint_generation_invalid", "invalid generation manifest")
        name, ref = next(iter(files.items()))
        payload = self.directory / name
        if payload.is_symlink() or marker.is_symlink():
            raise CheckpointError("checkpoint_generation_invalid", "checkpoint symlink refused")
        content = payload.read_bytes()
        if len(content) != ref["size_bytes"] or hashlib.sha256(content).hexdigest() != ref["sha256"]:
            raise CheckpointError("checkpoint_generation_invalid", "checkpoint content mismatch")
        value = json.loads(content)
        if value.get("myce_checkpoint", {}).get("generation") != generation or "value" not in value:
            raise CheckpointError("checkpoint_generation_invalid", "payload generation mismatch")
        return generation, name, value["value"], data

    def _wait_accepted(self, generation: int, marker: bytes) -> None:
        expected = {"sha256": hashlib.sha256(marker).hexdigest(), "size_bytes": len(marker)}
        while True:
            try:
                ack = json.loads((self.directory / ".myce-accepted.json").read_bytes())
                if ack.get("protocol") == _PROTOCOL and ack.get("generation") == generation and ack.get("marker") == expected:
                    return
            except FileNotFoundError:
                pass
            # Payload-side polling only: a short cooperative wait avoids a busy
            # spin. Worker cancellation/stop grace remains independent.
            time.sleep(0.05)

    def _save_local(self, value: Any, name: str) -> Path:
        import fcntl  # Worker platforms: Linux and macOS; leave other clients importable.
        with (self.directory / ".myce-writer.lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise CheckpointError("checkpoint_protocol_unsupported", "concurrent checkpoint writers are unsupported") from error
            return self._save_local_locked(value, name)

    def _save_local_locked(self, value: Any, name: str) -> Path:
        if name != "checkpoint.json":
            raise CheckpointError("checkpoint_protocol_unsupported", "local_first v1 supports the default self-contained JSON save only")
        prior = self._generation()
        if prior:
            self._wait_accepted(prior[0], prior[3])
        generation = (prior[0] if prior else 0) + 1
        if generation >= 2**63:
            raise OverflowError("checkpoint generation exhausted")
        data = json.dumps({"myce_checkpoint": {"generation": generation}, "value": value}, allow_nan=False).encode("utf-8")
        if len(data) > 1 << 20:
            raise CheckpointError("checkpoint_protocol_unsupported", "local_first v1 JSON saves are at most 1 MiB")
        stat = os.statvfs(self.directory)
        free = stat.f_bavail * stat.f_frsize
        # Existing disk headroom floor: 1% of the actual volume, at least
        # 256 MiB. Charge payload, immutable transfer copy and metadata blocks.
        floor = max(stat.f_blocks * stat.f_frsize // 100, 256 << 20)
        needed = 2 * len(data) + 4 * stat.f_frsize
        if free < floor + needed:
            raise CheckpointError("checkpoint_local_capacity", f"checkpoint needs {needed} bytes above measured reserve; {max(0, free-floor)} available")
        path = self.directory / f"generation-{generation}.json"
        if path.exists():
            raise CheckpointError("checkpoint_generation_conflict", "unpublished generation exists; preserve it for recovery")
        _atomic_local(path, data)
        marker = json.dumps({"protocol": _PROTOCOL, "myce_checkpoint": {"generation": generation, "files": {path.name: {"sha256": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}}}}, sort_keys=True).encode("utf-8")
        _atomic_local(self.directory / "generation.json", marker)
        self._wait_accepted(generation, marker)
        # Previous bytes are queue-accepted and no publisher can still need
        # them once this generation's exact ack exists. Preserve other files.
        if prior:
            (self.directory / prior[1]).unlink()
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
        if self.protocol:
            saved = self._generation()
            return saved[2] if saved else default
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
