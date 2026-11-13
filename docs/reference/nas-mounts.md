# NAS mounts

Operator boot integration for Linux CIFS mounts and how workers detect, hold and
recover from NAS mount faults. NAS selection at install is in
[onboarding storage](onboarding.md#onboarding-storage-and-tools).

## Linux CIFS boot recovery

The host operator owns the system CIFS mount and its credentials; the worker
installer creates a user service and does not configure system mounts. For an
fstab-managed share use `_netdev,nofail,x-systemd.automount` and a bounded
`x-systemd.mount-timeout=30s`, together with the site's existing CIFS and
credential options. Enable the network manager's appropriate wait-online service
and verify that it actually waits for the interface needed by the share;
`network-online.target` ordering alone does not establish connectivity.

The mount can still fail while the network is down. Add the provided
`deploy/checkpoint-mount-start-limit.conf` as a systemd **mount unit** drop-in
(`systemctl edit MOUNT.mount`), then reload systemd and qualify it at the next
authorized boot. `StartLimitIntervalSec=0` disables the mount's start-rate
limit: otherwise repeated failed accesses can put the automount in
`mount-start-limit-hit` and remove it permanently. This permits another mount
attempt on subsequent worker probes, at the cost of repeated connection attempts
while the share is down. The bounded background probes and checkpoint admission
fences remain in effect. Do not put this setting on the worker user service or
the automount unit. Confirm the effective mount-unit setting and automount state
with `systemctl show` before qualification.

If an automount is already failed, after the network is restored an authorized
operator can inspect the exact mount and automount units, reset their failed
state and start that automount only. Do not restart the worker or queue; check
that active work is unaffected. After deployment, qualify on a Linux test host:
boot with its network unavailable, observe checkpoint-dependent work held and
heartbeats continuing, restore the network, and verify the share mounts and the
held work is admitted without restarting either service. This native boot check
is separate from the laptop fixture and requires an authorized test window.

## Worker mount recovery (Linux and macOS)

Workers reuse the host-hygiene mount probe for each configured checkpoint/NAS
root. Checkpoint health reuses the existing admission access check: stat the
root, rewrite one fixed per-host `.myce-probe-<host>-worker` file in place,
close it and read it back. Probes never delete files: a NAS recycle bin keeps
every SMB delete, and a create-then-remove probe filled one bin folder until
listing it stalled the share (#1545). The queue's probe works the same way and
reuses a completed result for one measured worker check-in period; install
probes also rename their fixed file and back. Leftover randomly named
`.myce-probe-*` or `.myce-install-probe-*` files from older versions can be
removed from the live share normally. It does not enumerate checkpoint entries
or retain a directory stream.
NAS-root health stats the root and reads one directory entry, preserving
read-only access checks. On macOS SMB, opening a directory stream can enumerate
the directory even when the caller requests one entry. Avoiding that work
removes unnecessary load and long directory-handle lifetimes; it does not make
slow checkpoint write operations fast. Their measured latency still
uses the unchanged host-relative advisory thresholds. Access
re-triggers an existing Linux systemd/autofs or macOS autofs mount through the
OS; no separate repair daemon or privileged command is installed. Cached mount
inventory is checked again after access, so an empty local mountpoint cannot
pretend to be a recovered NAS. Independent local checkpoint roots remain local.

A learned whole-sequence latency outlier is advisory while filesystem
operations still progress. The existing hygiene band and confirmation count
remain unchanged; a slow success retains its evidence and alert without
holding admission. A pending request is stalled only when no individual
filesystem or inventory operation completes within the existing #234
per-operation allowance. That unchanged 30-second backstop retains its measured
basis in successful contended metadata tails (see checkpoint availability); it
is not a total sequence deadline. Cold/unverified probes and explicit failures
still withhold dependent work. Heartbeats never wait for these calls, and a
stuck kernel request retains its one slot until it returns. No timeout creates
another request. A separate `host_mount_unavailable` wake prevents an earlier
advisory episode from hiding a new stall or failure.

Completed successful probes train through hygiene warmup before outlier
classification; adverse samples cannot widen the healthy baseline.
Probe-format upgrades invalidate only incompatible baseline evidence, preserving
other health evidence and historical diagnostics.
Returned failures retry with exponential backoff starting at the larger of the
sampling interval and measured operation cost. The ceiling is the larger of the
existing signal-freshness allowance and that measured cost: this limits repeated
connection pressure while keeping recovery discovery within the host's evidence
budget. Three newly completed successful probes clear the hold automatically, including
slow successes; an unfinished probe cannot confirm recovery.
NAS-dependent work waits with a typed root reason; local/cached work continues
when its own dependencies are available. Existing operator drains and attempt
custody are unchanged.

From the owned-NAS fault follow-up, errors in Mycelium-owned shared-input
staging/hash reads, ordinary input staging, checkpoint preparation/progress and
existing probes latch the same dependency-scoped mount health failure. Only
exact filesystem errors attributed to a configured network backing qualify;
missing files, permissions, cancellation, a bad declared digest and local cache
errors do not quarantine a mount. This is automatic worker behavior, with no
opt-in switch and no consumer syscall or kernel-log sensor.

Batch start checks use the live latch through the existing dependency resolver,
including before and after the final queue authorization. A fault between
heartbeats therefore fences a retry or an older healthy queue reply. Model
launches ask the same worker resolver in their existing pre-authorization engine
gate. The health owner is initialized before the runtime captures its Worker,
so subsequent faults remain live in that copy. Faults
propagate only to roles with the same known backing; a separately mounted
nested root stays independent. Checkpoint preparation uses explicit userspace
read/write streaming so Linux fast-copy errors cannot mislabel source data
faults as destination metadata errors. This preserves syscall provenance at the
cost of userspace copying; live NAS throughput for this change is unmeasured.

For a data fault the existing single outstanding probe lane replaces metadata
checks with a bounded read/reopen proof: two fresh open/stat/read/close cycles,
at the same offset within that completion, verify nonempty bytes and stable
size/mode/mtime. The offset rotates between completions; each read is at most
one OS page. Darwin requests `F_NOCACHE` on each descriptor; Linux requests
`POSIX_FADV_DONTNEED` for the page-rounded window before each read. These OS
requests bypass client data caching where supported; success does not attest
server cache behavior or the producer's complete declared digest. Full staging
still verifies that digest. Unsupported/refused bypass records
`cache_bypass: unavailable` in private evidence and keeps the data-fault hold.
One in-memory record per backing holds its latest generation, recovery counter
and one recovery owner; sibling roles project that shared state through
`HostMountHealth`, `UnavailableRoots` and the existing dependency resolver.
Owned, probe and recovery faults enter the same transition. Storage events
never recount a cached host-pressure sensor reading.
No sentinel write is required, including on read-only layouts. The failed
source must remain readable; a removed or empty source cannot prove recovery.

Three newly completed proofs, each started after the latest fault, clear the
hold. Every newer fault resets the candidate and generation; pre-fault requests,
metadata-only success and cached results cannot count. Existing per-operation
stall accounting and failure retry backoff apply; there is no recovery cooldown.
Healthy operation adds no recovery reads. During a data fault one recovery
owner per backing performs at most two page reads per completion, plus opens,
stats and closes. An unreturned syscall retains that one lane.

Known limit: a worker restart forgets this in-memory fault latch. The first
NAS-dependent attempt after restart may encounter the fault again and re-latch
it. There is no new local-state writer or crash-recovery protocol. Durability is
parked on #894; revisit on an observed post-restart repeat failure.

An incident owner's temporary whole-host exclusion remains independent. Retire
it only after deploying the reviewed release and verifying live that the fault
withholds NAS-dependent work, independent local work stays eligible, and fresh
cache-bypass read/reopen proof recovers the affected roots. The incident owner
removes that exclusion; merge, elapsed time or metadata success alone is not
sufficient. A natural data fault unseen by owned I/O is the trigger to revisit
a kernel-log sensor; it is not part of this implementation.

On **Linux**, a surviving automount is retried by access, including a late
network after boot. Apply the operator-owned boot integration above to prevent
`mount-start-limit-hit`. When the automount has disappeared, the worker reports
`mount_linux_operator_required`: an authorized operator must restore the exact
mount/automount through the existing system configuration. The worker never
runs root mount helpers, resets system units, or changes credentials.

On **macOS**, access retries an existing autofs path and lets the OS reconnect
an existing SMB session. If no mount/automount remains, or access is denied,
`mount_macos_approval_required` asks the owner to restore the configured mount
and approve Keychain access if macOS requests it. This code identifies the need
for owner action, not the exact cause of a Keychain refusal. It never launches
Finder, displays a password dialog, reads Keychain, or reconstructs an SMB URL.
A dead SMB request that never returns stays `mount_probe_stalled`; inspect the
OS session through the storage owner rather than force-unmounting active data.

Status exposes these codes in `host_health.history[].mounts[].reason` and
`unavailable_roots`. The existing #200 wake channel emits a deduplicated
`host_mount_action_required` event, including escalation after an earlier
`host_mount_degraded` event. Diagnoses contain root roles and fixed codes only,
never paths, mount sources, syscall text or credentials. Native late-network
boot, Wi-Fi/DNS loss and dead-SMB qualification require a separately authorized
window; laptop fake-filesystem tests do not establish native remount success.
