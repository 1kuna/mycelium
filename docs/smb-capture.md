# Capture an SMB failure without changing storage behavior

The underlying causes of the macOS read EAGAIN/partial reads and checkpoint
replace EBUSY in [#894](https://github.com/1kuna/mycelium/issues/894) and
[#346](https://github.com/1kuna/mycelium/issues/346) remain **unknown**.
Client logs establish reconnects, failed durable-handle reopens and subsequent
invalid-handle read failures. They do not identify the initiating network/server
fault, the precise consumer errno, or the rename response. Directory size and
an overlapping checkpoint reader alone do not establish those causes.

`scripts/smb_capture.py` is an opt-in diagnostic for a consumer/runtime owner to
wire around the actual failing operation during a separately authorized run.
It is not installed, enabled, or injected into existing jobs. It does not read
payloads or consumer logs, retry operations, change mounts, install tracing,
or change checkpoint/input policy. It needs only Python 3.9+ and macOS's
existing `smbutil` and `log` commands.

Choose an existing SMB mount and a **new private local directory**, outside
source control and the share. The helper creates it with mode 0700 and writes
files with mode 0600. Raw SMB/kernel snapshots contain deployment identities,
paths and potentially filenames: never attach them to a public issue or PR.
Review even the sanitized event before publishing it.

For Python, import the script from its `scripts` directory. The owner supplies
the real mount/output variables and existing operation; no new credential is
needed. Do not put the fleet token in the payload environment.

```python
from pathlib import Path
from smb_capture import Capture

capture = Capture(Path(private_local_output), existing_mount)
capture.baseline()  # Session counters before the workload, not per read.

# Surround the existing operation. Pass no path to the event serializer.
data = capture.observe("read", "nas_input", lambda: stream.read(expected_size),
                       expected_bytes=expected_size)
capture.observe("replace", "checkpoint", lambda: temporary.replace(destination))

# Preserve/report this field if collection was refused, timed out, or failed.
capture_incomplete = capture.last_error
```

Only supply `expected_bytes` where the caller already requires that exact
result length. An ordinary short POSIX read/EOF is not evidence of corruption.
The helper preserves the returned object or original exception and invokes the
operation exactly once. An exception's byte count is unknown, not inferred as
zero. It never serializes exception text, paths, arguments or payload bytes.
The event includes operation, fixed path class, UTC start/end/recording times,
elapsed monotonic seconds, numeric/symbolic errno and known byte counts.

Alternatively, a non-Python owner can call `python3 scripts/smb_capture.py
--help` and pass actual operation timing/errno/byte counts to the one-shot CLI
immediately on failure. That mode has no pre-failure session snapshot. It does
not reconstruct timing from the time a human noticed an error.

The first incident collects `smbutil statshares -m`, `smbutil multichannel -m`
and the preceding five minutes of kernel SMB logs, sequentially. A baseline
collects just the two session snapshots. No polling, detached process or
scheduled work is left behind. Further incidents record events without starting
more collection. A five-minute history includes the observed seconds-long
reconnect sequences; older faults may be absent. Empty/partial logs do not prove
the absence of a fault. Snapshot times are recorded separately from syscall
times; they are not simultaneous.

Each diagnostic command gets ten seconds by default, configurable through
`command_seconds`/`--command-seconds`. This bounds incident-reporting delay
(up to thirty seconds for three commands, plus exit verification), rather than
assuming an SMB operation must complete in ten seconds. Each output is capped
at 4 MiB to prevent noisy kernel logs filling the disk; truncation is explicit.
On refusal, timeout, truncation or uncertain child exit, collection stops with
an incomplete result and no alternate privilege/tool path. Its process group
receives KILL on timeout; one second verifies exit. A kernel-blocked process can
survive KILL: `cleanup_unconfirmed` is a hold for the owner, not cleanup proof.
Retain the capture directory and report the PID/custody separately; do not
reboot/remount or start another workload to clear it.

To settle attribution, align the event with client SMB READ/CREATE/SET_INFO/
delete/CLOSE status and handle/lease/reconnect state. If unprivileged logs lack
those details, the storage owner must authorize a trace and obtain matching
server records, including failed durable reconnect status and competing handles.
This helper cannot inspect server administration or prove a handle's owner.
Use server/network evidence to explain why a session broke; a reconnect counter
alone is not a root cause. No NAS/client tuning or local-first default follows
from an unknown cause. Existing verified immutable shared-input staging can be
qualified by its owner as a mitigation, with cold-copy failures still visible.

Laptop regression: `python3 scripts/test-smb-capture.py`. It covers EAGAIN,
EBUSY, exact-length partial reads, original outcome preservation, private files,
refusal/timeout cleanup and diagnostic credential exclusion. Fake failures on a
local filesystem do not reproduce SMB or qualify production behavior.
