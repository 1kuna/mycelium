#!/usr/bin/env python3
"""Opt-in, private macOS SMB incident capture; never retries payload I/O."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import errno
import json
import math
import os
from pathlib import Path
import selectors
import signal
import subprocess
import threading
import time
from typing import Callable, TypeVar

T = TypeVar("T")
OPERATIONS = ("open", "read", "replace", "stat", "create", "remove", "fsync")
PATH_CLASSES = ("nas_input", "checkpoint", "checkpoint_root", "scratch")
# Each command is session metadata, not a payload dump. A 4 MiB ceiling retains
# thousands of kernel events without allowing a noisy log to fill local disk.
MAX_CAPTURE_BYTES = 4 * 1024 * 1024


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def capture_command(argv: list[str], destination: Path, seconds: float) -> dict:
    """Bound one diagnostic process group. Never retry a refused/stuck command."""
    started = time.monotonic()
    state = {"started_at": utc_now(), "status": "ok"}
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as output:
        try:
            process = subprocess.Popen(
                argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, start_new_session=True,
                # These OS inspection tools need no job or fleet environment.
                env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C", "TZ": "UTC"},
            )
        except OSError as exc:
            return {**state, "status": "unavailable", "errno": exc.errno}
        assert process.stdout is not None
        state["pid"] = process.pid

        def stop() -> None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        total = 0
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while selector.get_map():
                    remaining = seconds - (time.monotonic() - started)
                    if remaining <= 0:
                        state["status"] = "timeout"
                        break
                    if not selector.select(remaining):
                        continue
                    chunk = os.read(process.stdout.fileno(), min(65536, MAX_CAPTURE_BYTES - total + 1))
                    if not chunk:
                        break
                    output.write(chunk[:MAX_CAPTURE_BYTES - total])
                    total += len(chunk)
                    if total > MAX_CAPTURE_BYTES:
                        state["status"] = "output_limit"
                        break
            if state["status"] != "ok":
                stop()
            try:
                process.wait(timeout=max(0.001, seconds - (time.monotonic() - started)))
            except subprocess.TimeoutExpired:
                state["status"] = "timeout"
                stop()
                # One second observes exit after KILL, not useful I/O duration.
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    state["status"] = "cleanup_unconfirmed"
            state["returncode"] = process.returncode
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                pass
            else:
                state["status"] = "cleanup_unconfirmed"
        finally:
            process.stdout.close()
    state.update(finished_at=utc_now(), elapsed_seconds=time.monotonic() - started)
    return state


class Capture:
    """One private directory, optional baseline, and one failure-time snapshot.

    The owner supplies an existing SMB mount and a NEW local output directory.
    No filesystem operation is performed on the supplied mount by this helper;
    smbutil only inspects its existing session. Callables retain their exact
    return values/exceptions. Diagnostics may delay exception delivery, bounded
    by command_seconds per command plus KILL verification. No background tasks.
    """

    def __init__(self, output: Path, mount: str, *, command_seconds: float = 10):
        if not Path(mount).is_absolute():
            raise ValueError("mount must be absolute")
        if not math.isfinite(command_seconds) or command_seconds <= 0:
            raise ValueError("command_seconds must be positive and finite")
        output.mkdir(mode=0o700)  # Exclusive: never reuse an existing directory.
        self.output = output
        self.mount = mount
        self.command_seconds = command_seconds
        self.last_error: str | None = None
        self._lock = threading.RLock()
        self._phases: set[str] = set()
        self._stopped = False

    def _write(self, name: str, value: dict) -> None:
        fd = os.open(self.output / name, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as output:
            output.write(json.dumps(value, allow_nan=False) + "\n")
            output.flush()

    def baseline(self) -> None:
        """Call before the workload to preserve pre-failure session counters."""
        self._snapshot("before")

    def _snapshot(self, phase: str) -> None:
        with self._lock:
            if phase in self._phases or self._stopped:
                return
            self._phases.add(phase)
            commands = [
                ("shares", ["/usr/bin/smbutil", "statshares", "-m", self.mount]),
                ("channels", ["/usr/bin/smbutil", "multichannel", "-m", self.mount]),
            ]
            if phase == "incident":
                # A five-minute lookback includes the observed 5–9 second
                # reconnect/reopen sequences and is explicitly incomplete for
                # older faults. No info/debug logging is enabled or reconfigured.
                commands.append(("kernel", [
                    "/usr/bin/log", "show", "--last", "5m", "--style", "ndjson",
                    "--predicate", 'process == "kernel" AND (eventMessage CONTAINS[c] "smb" OR eventMessage CONTAINS[c] "netsmb")',
                ]))
            for label, argv in commands:
                result = capture_command(argv, self.output / f"{phase}-{label}.txt", self.command_seconds)
                self._write("capture.jsonl", {"phase": phase, "command": label, **result})
                if result["status"] != "ok" or result.get("returncode") != 0:
                    self._stopped = True  # Stop on refusal/uncertain custody.
                    self.last_error = result["status"] if result["status"] != "ok" else "command_refused"
                    break

    def incident(self, operation: str, path_class: str, *, started_at: str,
                 elapsed_seconds: float, error_number: int | None = None,
                 requested_bytes: int | None = None, returned_bytes: int | None = None) -> None:
        """Record an actual failed operation, without its path, message or bytes.

        Byte counts may be unknown. A short read is an incident only when the
        caller supplies an exact expected byte count; normal EOF is not inferred
        to be an SMB error. Raw snapshots remain private even though this event
        deliberately omits identities and content.
        """
        if operation not in OPERATIONS or path_class not in PATH_CLASSES:
            raise ValueError("unknown operation/path class")
        timestamp = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        if timestamp.utcoffset() is None:
            raise ValueError("started_at needs a UTC offset")
        if not math.isfinite(elapsed_seconds) or elapsed_seconds < 0:
            raise ValueError("elapsed_seconds must be nonnegative and finite")
        for count in (error_number, requested_bytes, returned_bytes):
            if count is not None and (type(count) is not int or count < 0):
                raise ValueError("errno and byte counts must be nonnegative integers")
        with self._lock:
            self._write("events.jsonl", {
                "operation": operation, "path_class": path_class,
                "started_at": timestamp.astimezone(timezone.utc).isoformat(),
                "finished_at": (timestamp + timedelta(seconds=elapsed_seconds)).astimezone(timezone.utc).isoformat(),
                "recorded_at": utc_now(), "elapsed_seconds": elapsed_seconds,
                "errno": error_number, "errno_name": errno.errorcode.get(error_number),
                "requested_bytes": requested_bytes, "returned_bytes": returned_bytes,
            })
            self._snapshot("incident")

    def observe(self, operation: str, path_class: str, call: Callable[[], T], *,
                expected_bytes: int | None = None) -> T:
        """Invoke once; preserve EAGAIN/EBUSY and incomplete byte results."""
        if operation not in OPERATIONS or path_class not in PATH_CLASSES:
            raise ValueError("unknown operation/path class")
        if expected_bytes is not None and (type(expected_bytes) is not int or expected_bytes < 0):
            raise ValueError("expected_bytes must be a nonnegative integer")
        started_at, started = utc_now(), time.monotonic()

        def record(number: int | None, returned: int | None) -> None:
            try:
                self.incident(operation, path_class, started_at=started_at,
                              elapsed_seconds=time.monotonic() - started,
                              error_number=number, requested_bytes=expected_bytes, returned_bytes=returned)
            except Exception as exc:
                # Diagnostics must never replace the payload's exception/result.
                # No exception text: OSError filenames may expose private paths.
                self.last_error = type(exc).__name__

        try:
            value = call()
        except OSError as exc:
            record(exc.errno, None)
            raise
        if expected_bytes is not None and isinstance(value, (bytes, bytearray, memoryview)) and len(value) < expected_bytes:
            record(None, len(value))
        return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="NEW private LOCAL directory; raw snapshots contain identities")
    parser.add_argument("--mount", required=True, help="existing SMB mount; no mounts/settings are changed")
    parser.add_argument("--operation", choices=OPERATIONS, required=True)
    parser.add_argument("--path-class", choices=PATH_CLASSES, required=True)
    parser.add_argument("--started-at", required=True, help="actual operation ISO timestamp with UTC offset")
    parser.add_argument("--elapsed-seconds", type=float, required=True)
    parser.add_argument("--errno", type=int)
    parser.add_argument("--requested-bytes", type=int)
    parser.add_argument("--returned-bytes", type=int)
    parser.add_argument("--command-seconds", type=float, default=10)
    args = parser.parse_args()
    capture = Capture(args.out, args.mount, command_seconds=args.command_seconds)
    capture.incident(args.operation, args.path_class, started_at=args.started_at,
                     elapsed_seconds=args.elapsed_seconds, error_number=args.errno,
                     requested_bytes=args.requested_bytes, returned_bytes=args.returned_bytes)
    if capture.last_error:
        parser.exit(1, f"capture incomplete: {capture.last_error}; retain the private directory\n")


if __name__ == "__main__":
    main()
