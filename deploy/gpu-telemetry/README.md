# Read-only xe GPU telemetry broker

This optional Linux root system service implements the Unix-stream broker
contract of PR #422 (`myce gpu-telemetry-client`). It needs systemd and OS
Python 3.10+, with no third-party packages. It does not contain a second client:
install the built-in client from a release containing #422, or the exact reviewed
PR source when separately qualifying that client before release.

The broker owns a fixed PCI inventory supplied in its root-owned unit. Clients
send zero bytes followed by write-half EOF; any request byte is rejected. Only
then does it timestamp and freshly read each configured
`/sys/kernel/debug/dri/<PCI>/gtt_mm`. It validates `use_type: 1`, `use_tt: 1` and
unsigned-byte `usage`, assembles one complete JSON object of at most 64 KiB,
sends it, and closes. There is no snapshot file or cache and no GPU write, command
execution, caller-selected path, network endpoint or fleet credential.

Systemd owns the socket. The persistent broker uses Python's standard-library
`socketserver.ForkingMixIn` so no interpreter starts per request. Each child gets
one connection, a 150 ms total deadline from accept (request, reads and reply),
and a SIGALRM deadline for ordinary blocking debugfs reads. Socket sends also
use the remaining budget. Even if a kernel read is uninterruptible, the client
retains its independent 200 ms deadline and cannot accept incomplete output.
The standard-library 16-child limit prevents an accumulation of blocked readers;
its overload cost is conservative client failure. Typical two-card 5 Hz collection
needs fewer than two concurrent readers. No history or per-request files remain.
Systemd retries a crashed broker after one second, avoiding a busy failure loop.
The service filesystem and explicit debugfs sources are read-only, network
families are limited to Unix, and namespace creation, writable executable memory
and foreign syscall architectures are disabled.

## Install (administrator)

Check load and service ownership first. Back up every existing destination and
record socket/service enabled and active states before replacing anything.
Check `/run/mycelium` is not used by another service before changing its metadata.
Replace `REPLACE_PCI_1 REPLACE_PCI_2` in the service with every full lowercase PCI
address attempts can use in both `ExecStart` and `ReadOnlyPaths`; add/remove
entries in both directives for other card counts. Example:
`0000:01:00.0 0000:02:00.0`. Confirm the exact debugfs paths exist. Never broaden
debugfs permissions or let clients choose the inventory.

Render **`REPLACE_CLIENT_GROUP`** in both the socket unit's `SocketGroup` and
its tmpfiles rule with the same existing group. Prefer the actual worker process's
primary group: read `Uid`, `Gid`, and `Groups` from `/proc/WORKER_PID/status`, then
resolve its effective GID with `getent group GID`. This grants access immediately
without account mutation, worker group changes, or a user-manager restart.
Choose a group whose members may read the fixed GPU counters. An existing
supplementary group already held by the actual worker PID is also usable.
For example, render both files before installing:

```sh
rendered=$(mktemp -d)
client_group=$(getent group WORKER_EFFECTIVE_GID | cut -d: -f1)
test -n "$client_group"
sed "s/REPLACE_CLIENT_GROUP/$client_group/g" myce-gpu-telemetry.socket > "$rendered/myce-gpu-telemetry.socket"
sed "s/REPLACE_CLIENT_GROUP/$client_group/g" myce-gpu-telemetry.conf > "$rendered/myce-gpu-telemetry.conf"
```

Use these rendered files in the install commands below, after replacing
`WORKER_EFFECTIVE_GID` with the effective GID read from the running worker.

Install the binary
with the built-in client at `/usr/local/libexec/mycelium-telemetry/myce`, and the
provided no-argument wrapper at `/usr/local/libexec/myce-gpu-telemetry-helper`.
Both and the broker must be root-owned 0755, and their parent directories must
be root-owned and unwritable by worker/job accounts. The wrapper contains real
newlines and execs the absolute binary path with `gpu-telemetry-client`, clearing
the environment to only `PATH=/usr/bin:/bin`, `LANG=C`, and `LC_ALL=C`. It forwards
no caller arguments.

From this directory, using the rendered service and chosen client binary:

```sh
sudo install -d -o root -g root -m 0755 /usr/local/libexec/mycelium-telemetry
sudo install -o root -g root -m 0755 /path/to/built/myce /usr/local/libexec/mycelium-telemetry/myce
sudo install -o root -g root -m 0755 myce-gpu-telemetry-broker /usr/local/libexec/myce-gpu-telemetry-broker
sudo install -o root -g root -m 0755 myce-gpu-telemetry-helper /usr/local/libexec/myce-gpu-telemetry-helper
sudo install -o root -g root -m 0644 "$rendered/myce-gpu-telemetry.conf" /etc/tmpfiles.d/myce-gpu-telemetry.conf
sudo install -o root -g root -m 0644 "$rendered/myce-gpu-telemetry.socket" /etc/systemd/system/myce-gpu-telemetry.socket
sudo install -o root -g root -m 0644 myce-gpu-telemetry.service /etc/systemd/system/myce-gpu-telemetry.service
sudo systemd-tmpfiles --create /etc/tmpfiles.d/myce-gpu-telemetry.conf
sudo systemd-analyze verify /etc/systemd/system/myce-gpu-telemetry.socket /etc/systemd/system/myce-gpu-telemetry.service
sudo systemctl daemon-reload
sudo systemctl enable --now myce-gpu-telemetry.socket myce-gpu-telemetry.service
rm -r "$rendered"
```

The tmpfiles rule recreates `/run/mycelium` as `root:<client-group>` 0750 on boot;
its socket is `root:<client-group>` 0660. Both use the rendered client group.
Group write permits connecting, while the
parent's lack of group write prevents replacing the socket. Check ownership and
modes after restart. Enabling the persistent service as well as the socket warms
its interpreter before helper use; cold activation must also fit the client
budget. Nothing installs or modifies a Mycelium worker unit/configuration.

Verify `/proc/WORKER_PID/status` again, then make a fresh two-card client call
with those exact numeric Uid/Gid/Groups (for example, administrator-run `setpriv
--reuid UID --regid GID --groups GID1,GID2 ...` with every group from that PID).
Do not substitute a fresh login shell's group list. Match the worker's service
restrictions and preserve its effective `NoNewPrivileges` policy; additionally
qualify `yes` if it currently uses `no`. Neither installing these templates nor
changing their client group needs a user-manager restart.
Use only `PATH=/usr/bin:/bin`, `LANG=C`, `LC_ALL=C` in helper calls. Repeatedly
check each timestamp falls between invocation start/completion, both/all PCI keys
and manager counters are valid, output is at most 64 KiB, and calls take less
than 200 ms. Check request rejection and stalled connections, then inspect
`CPUUsageNSec` over a measured sampling interval and the service journal.

Only with a compatible worker release and separate activation authorization set:

```text
--gpu-telemetry-helper /usr/local/libexec/myce-gpu-telemetry-helper
```

Broker installation does not set the flag, change worker groups or restart the
worker. Native pressure rejection and two-card workload/output acceptance remain
separate. To retire the broker, first clear the helper through the normal
authorized worker configuration path, then disable the socket and service.
Preserve operator backups.

## Existing primary-group override (#444)

Release #421 applied this arrangement on b70: the socket drop-in
`/etc/systemd/system/myce-gpu-telemetry.socket.d/60-worker-primary-group.conf`
sets `SocketGroup` to the worker's existing primary group and `SocketMode=0660`;
`/etc/tmpfiles.d/myce-gpu-telemetry.conf` sets the same group on the root-owned
0750 parent. Fresh complete two-card access under the actual worker PID's
credentials was recorded in #444; the worker's effective `NoNewPrivileges=no`
was preserved. This is helper connectivity proof, not native workload acceptance.
Preserve that installed override until a future reviewed deployment replaces it;
a drop-in overrides a newly rendered base unit. When changing an installation,
stop the broker service and socket together, apply tmpfiles, daemon-reload, then
start both and recheck ownership, modes and exact-PID credential access. Keep the
same group in both files so reboot and socket recreation preserve permissions.

## Laptop checks

```sh
python3 -B -m unittest discover -s deploy/gpu-telemetry -v
./scripts/gate.sh quick main
```

Temporary two-card fixtures and real Unix socket pairs verify post-request fresh
reads, exact raw text, request framing, the shared 150 ms request/read deadline,
invalid/incomplete/oversized sources and canonical inventory. No GPU or root is
needed. Native installed-client verification remains separate.
