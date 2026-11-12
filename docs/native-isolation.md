# Native job isolation

The [approved amendment](design-proposal.md#amendment-minimum-native-job-isolation-177-219-248)
defines the #177 minimum: job-scoped work/temp, no ambient credentials, and no
worker control access. [#248](https://github.com/1kuna/mycelium/issues/248#issuecomment-6447873336)
makes that minimum the v1 default; the [declared and legacy stage exceptions](design-proposal.md#amendment-minimum-native-job-isolation-177-219-248)
supersede its former blanket requirement. Wider executor hardening is separate.
These are implementation contracts and limits, **not fleet qualification or
deployment evidence**.

## macOS sampled memory guard (#770)

Native CPU attempts use the worker's birth-fenced memory observer for both plain
and sandboxed commands and sidecar groups. Explicit host-memory declarations keep
their resolved threshold (including configured slack). GPU jobs, preparation and
model servers retain their existing behavior.

Learned and bounded exploratory reservations are estimates, not authored hard limits. From the next
release, a queue and worker that both support adaptive native CPU memory let a
learned or bounded exploratory `memory: auto` attempt grow its charge from live measurement. Linux's
parent `memory.max` is bounded by measured physical RAM and the configured host
allowance, rather than the smaller estimate. Cold physical probes keep their initial
execution ceiling and existing probe-charge forecast. Explicit member cgroups stay
in place. Darwin uses sampled RSS and has no kernel memory ceiling. An older
queue or worker retains the previous reservation-sized ceiling/guard contract.

Above an estimated reservation, sustained measured contention stops the exact
attempt: OS warning/critical pressure, new swap/OOM/stall activity, or live
availability insufficient for the configured owner reserve plus measured growth
over the sampling horizon. The configured host allowance remains a hard bound.
Missing pressure evidence cannot establish contention. Unknown current-memory
telemetry retains the full execution ceiling's admission charge until fresh
measurements return; it cannot free capacity for another job. Explicit limits do
not depend on pressure evidence.

The 2026-11-06 incident recorded 4.4857635498046875 GiB RSS against a
3.9251251220703125 GiB learned threshold. Stopping solely at that estimate was the
wrong rule for a healthy job on a host with room. The retained receipt cannot
establish contemporaneous host pressure or exclusive physical demand, so it
cannot establish whether that particular execution was healthy or under physical
pressure. Fake-clock replays exercise both states and retain explicit limits.
The #389 incident separately recorded 64.07 GiB summed RSS and 2,004 Python
processes on 16 GiB RAM with critical pressure and about 44.83 GiB swap-out.
Its timing was not retained. A synthetic fake-clock replay of an accelerating
fork shape must stop while pressure is still normal and before swap starts;
its shared-RSS/stable-headroom counterpart retains the learned growth contract.
This is a stop-decision regression, not historical timing or native stress proof.
A later saved #850 incident stopped at 3.3578033447265625 GiB against a
3.2467041015625 GiB bounded exploratory estimate despite normal pressure and
8.378646850585938 GiB minimum available. The fake-clock regression permits this
growth, re-reserves through the common forecast, and lets the clean completion
teach an accepted peak. The pressure counterpart remains an OOM-class guard stop
and cannot train an accepted peak. Growth of an estimate alone is not an OOM.
The #850 recovery separately retains the measured 0.560638427734375 GiB overshoot
as uncertainty without doubling demand beyond every eligible allowance.

The process observer samples once per second and includes main, helpers and
attributable descendants within the cleanup session boundary, excluding the
control anchor, worker and sibling attempts. The tree parser counts each PID
once even when its parent is also owned; each PID's RSS is its own resident set,
not a recursive child sum. Shared physical pages can appear in multiple PIDs'
RSS. Apple's [ps implementation](https://github.com/apple-oss-distributions/adv_cmds/blob/main/ps/print.c)
prints task `resident_size`; XNU exposes [resident size, compressed bytes and
physical footprint separately](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/mach/task_info.h).
RSS is not a unique physical footprint and can omit compressed memory. Summed
RSS greater than physical RAM alone therefore does not establish exhaustion.
Darwin receives no anonymous-resident admission credit from this measurement.

Three valid overage/contention observations spanning two seconds normally stop
through exact-attempt cleanup. A rapidly growing owned process tree shortens
that grace when consecutive valid observations show both RSS/member-count growth
and actual host headroom loss: if the measured depletion rate would consume the
remaining room within one observed sampling interval plus the existing grace,
the guard stops an already-over-limit attempt immediately. This applies to both
learned and declared thresholds. It uses neither a process-count cap nor summed
RSS as physical usage; stable host headroom does not earn this early stop. A lower or pressure-free sample, missing inventory,
changed birth/session identity or gap over two sample periods resets the window.
The ordinary grace filters a transient spike without another polling loop;
physical Linux and explicit member ceilings remain kernel-enforced between reads.

The cadence preserves the observer's measured cost budget: 30 original RSS plus
pressure cycles took 27.0 ms median, 32.1 ms maximum; 20 complete birth-fenced
session/RSS plus pressure cycles, including member birth checks, took 65.4 ms
median, 74.0 ms p95, 77.9 ms maximum
on one development Mac. One new cycle per second uses about 65 ms of serialized probe wall time versus
54 ms for two old cycles, while retaining a roughly three-second ordinary reaction window. The growth
forecast above avoids spending that grace during a measured fork/growth burst.
A two-second cadence would halve probe work but double that ordinary reaction window.
Two further observations filter brief
spikes without a second sampler or an arbitrary memory multiplier. These are
bounded local overhead measurements, not fleet load qualification.

Job GET/status retains `execution_cause.class: rss_limit_exceeded` with
`rss_limit` evidence (`observed_bytes`, `limit_bytes`, `quality:
owned_session_rss` on Darwin; `cgroup_current` for Linux pressure stops). The
fact is persisted before stopping and survives worker recovery. It is a memory failure, not infrastructure loss: `memory: auto` uses the
existing bounded OOM escalation; plain declared-limit failures are terminal, and
extension retry remains bounded by the existing OOM policy. Failed/constrained
readings cannot become accepted clean memory peaks. Partial diagnostics and
cleanup charges retain ordinary custody until all owned processes are gone.

Darwin has a reactive RSS guard, **not a kernel ceiling or aggregate RAM containment**.
It can miss fast spikes, compression and deliberately escaped sessions; shared
pages can count more than once. A sufficiently fast allocation can still exhaust
RAM before detection. A bounded native runner check with a memory-using child
verified stop, whole-session release, retained logging and a successful below-limit
command; it does not qualify representative owner interference (#389).

**Reaction time and overshoot.** With cgo (every release build), the guard reads
the process table and libproc in-process (about 1 ms per sample, against about
70 ms for the earlier `ps`/`vm_stat` probes), so it samples every 200 ms, the
cgroup sampler's cadence. Three over-limit samples across two periods still
filter a fork's transient shared pages. A member that exits or is replaced
mid-sample is dropped from that sample, not charged and not allowed to void it.
A late sample no longer restarts the window. Overshoot is therefore about the
growth rate × 0.2–0.8 s. Measured on one M-series Mac
(`TestDarwinRSSGuardGrowthOvershoot`, 256 MiB limit):

| Synthetic growth | Before: reaction, overshoot | Now: reaction, overshoot |
|---|---|---|
| one process, 256 MiB/s | 2.2 s, 560 MiB | 0.6 s, 160 MiB |
| one process, 1 GiB/s | 2.8 s, ≥2.8 GiB (harness cap) | 0.8 s, 816 MiB |
| spawned 32 MiB processes, 320 MiB/s | 2.4 s, 736 MiB | 0.2 s, 64 MiB |
| 256 MiB/s plus short-lived helper churn | never stopped | 0.5 s, 128 MiB |

These are single-host measurements, not per-host qualification. A
non-cgo Darwin build keeps the `ps` probe and a 1 s period.

**Metal jobs (#1044).** Metal buffers are charged to the task's
`phys_footprint`, not its RSS. For a Metal job with a declared size, the guard
reads `proc_pid_rusage` (footprint and RSS in one call) for each owned process
at the same 200 ms cadence and stops the attempt once the summed footprint
stays above device plus host plus slack for the same three samples. A process
it cannot inspect makes the sample unavailable rather than falling back to RSS.
Non-cgo builds and auto-sized Metal work keep the old behaviour (no guard).
Measured on an M4 Mac mini worker (16 GiB) through an experiment-tier job
(`TestDarwinMetalFootprintGuardOvershoot`, 512 MiB limit; a Swift child
allocates private `MTLBuffer`s, fills each with a GPU blit and logs its own
footprint):

| Synthetic Metal growth | Reaction (last write) | Footprint overshoot | RSS at stop |
|---|---|---|---|
| 256 MiB/s | 0.42 s | 141 MiB | 12 MiB |
| 1 GiB/s | 0.75 s | 797 MiB | 12 MiB |

The RSS guard would never have acted: RSS stayed at 12 MiB with 1.2 GiB of
buffers held.

## Boundary and guarantees

Sandboxed native batch commands and their sidecars, including queued extension run/check
payloads, inherit kernel restrictions across fork/exec. Main and helpers in
one attempt trust each other and share its resources. `0700` is useful hygiene,
but provides no boundary between jobs running as the same user.

| Platform/mode | Directory and files | Temp | Environment | Control access | Network |
| --- | --- | --- | --- | --- | --- |
| Declared unsandboxed / legacy registration / plain native default | Ordinary worker-user file access, except the worker credentials below (#1309); managed cwd and private staged source remain. | Managed work/temp/scratch paths; other host temp remains accessible. | Same #219 allowlist and token scrub. | No #177 kernel control/process/IPC protection. Device binding, cgroups, launch gates, fencing and release custody remain. | Ordinary host network access; no native sandbox/network capability required. |
| Linux namespaces | User/mount/PID/IPC isolation; read-only host-root view plus Landlock content grants. Sibling contents denied; host metadata can remain visible. | Managed `{work}/.tmp` and shared `.scratch`; private `/tmp` and `/dev/shm` shared within the attempt. | #219 allowlist; worker overrides temp and attempt/device controls; no ambient agent/session or fleet credentials. | Private PID/network namespaces plus seccomp/Landlock deny host process, control-file and socket access. | Private network namespace, shared by the attempt's sidecars; reserved TCP loopback only. No qualified outbound transport; no `native_network`. |
| Linux Landlock/seccomp fallback | Content reads/execution limited to runtime/cache/attempt grants; writes limited to attempt/checkpoint/usage grants. **Metadata is not isolated**, including permission changes. No private mount/PID view. | Managed `{work}/.tmp` and shared `.scratch`; sibling contents denied. Host `/tmp` is not replaced. | Same allowlist and overrides. | Denies other processes' sensitive procfs contents, tracing/memory access and external signals; denies Unix stream/datagram endpoints and non-reserved TCP ports. **same-UID pathname SEQPACKET endpoints reachable (partial)**. Foreign abstract endpoints remain scoped. | **Partial network isolation**: exact reserved sidecar TCP ports permit connect/bind on any address, including off-host. All other TCP ports denied; no general outbound capability. |
| macOS Seatbelt | Targeted denial of same-spool sibling contents/writes, configured worker storage and standard credential roots. Prepared caches allow reads/execution, not writes. Metadata and unrelated host files can remain visible. | Managed `{work}/.tmp` and shared `.scratch` protected with the spool; no private system `/tmp` namespace. | Same allowlist and overrides. | Denies Unix endpoint connections, protected credential contents, cross-job process access and unwanted Mach services; retains Metal services. | Default outbound: reserved sidecar TCP loopback only. With `needs.network: true`: general outbound except other loopback endpoints and the queue's TCP port at all addresses. |
| `isolation: ci` (#1298), either platform | Everything in that platform's sandboxed row, plus: HOME is the attempt-private `{work}/.home`; the worker's shared model cache, model root and operator roots on a network filesystem or below the NAS root aren't granted. macOS also denies reading the worker user's home, `/Users`, `/Volumes`, every non-local mount and the configured NAS root, except this attempt's own grants. | Same as sandboxed. | Same, with a worker-owned HOME that authored `run.env` can't override. | Same as sandboxed. | Same as sandboxed. |
| Missing/unsupported confinement for a sandboxed stage | Required behavior: sandboxed native launch fails closed. | No implicit unconfined fallback. | An allowlist alone is insufficient. | No implied protection. | No inferred online capability. |

### Worker credentials in unsandboxed jobs (#1309)

A 2026-11-11 probe found that plain native jobs on one Linux arm64, one Linux
amd64 and one macOS arm64 worker could read the installed worker (and
co-installed queue) configuration, the fleet environment or joining file that
holds the token, and the worker user's SSH private keys. Both Linux workers'
GitHub CLI token (`~/.config/gh/hosts.yml`) and two workers' Docker client
configuration were readable too. On Linux they could also
read the token from the worker service's `/proc/PID/environ`, because systemd's
`EnvironmentFile` puts it in the initial environment.

Plain, declared-unsandboxed and legacy native payloads now launch without read
access to: the Mycelium install root (`~/.local/share/mycelium` and the
configured one), every installed role's `config.json`, credentials, environment
and joining files, the worker's secret root, and the worker user's credential
stores: `~/.ssh`, `~/.config/gh`, `~/.git-credentials`, `~/.netrc`,
`~/.docker/config.json`, `~/.aws`, `~/.config/gcloud`, `~/.kube` and the macOS
login keychain files (`~/Library/Keychains/login.keychain-db`, `login.keychain`).
Each lets its holder act as that user elsewhere, so the list stays this short;
`nativeCredentialStores` holds it in code. Inside the install root
they keep this worker's state (their own attempt files), caches, `tools` and
installed executables. Everything else is unchanged. Tools that read those stores
directly therefore lose them in these jobs: `gh`, git HTTPS credentials from the
store or `.netrc`, registry logins in Docker's client configuration (the CLI
warns and continues), the AWS, gcloud and kubectl CLIs. On macOS, `securityd`
rather than the job reads the keychain: `security find-identity` still listed
signing identities with the login keychain file denied (one local check; signing
itself wasn't tested).

- **macOS:** an allow-default Seatbelt profile denies reads and writes of those
  paths. Seatbelt refuses setuid programs and, under any non-trivial profile,
  a nested sandbox. `ps`, `top`, `traceroute` and `/usr/bin/sandbox-exec`
  (SwiftPM manifests and plugins) therefore run outside the profile as before.
  `crontab`, `at`, `sudo`, `su`, `authopen` and the other setuid tools are
  refused, as is a program that calls `sandbox_init` itself.
- **Linux:** the `_native-exec` helper applies a Landlock ruleset handling only
  file reads and cross-directory renames (ABI 2+), granting both on every entry
  beside the path to each denied one. No seccomp or network rule applies.
  Landlock also implies `no_new_privs` (setuid programs such as `sudo` don't
  elevate), forbids mounts, and prevents ptrace and `/proc/PID/environ` reads of
  processes outside the job. A file or directory created during the job directly
  in one of those ancestor directories (`/`, home, `~/.config`, ...), or an
  existing one replaced there by rename (`git config --global` rewriting
  `~/.gitconfig`), is not readable by that job; cross-directory renames into
  them fail with `EXDEV`. Writes are not restricted. A kernel without Landlock
  ABI 2 runs the job unrestricted and the worker logs a warning.
- **Linux services:** installed services mark themselves non-dumpable, so a
  same-user process can't read their environment or attach to them.

This removes direct reads only. An unsandboxed job can still start processes
outside its restriction through the user's service manager, cron, login
files or (on macOS) `sandbox-exec`; `isolation: sandboxed` or `ci` is the boundary for untrusted code.

Choose an exception at the extension top level or adapter entry with
`isolation: unsandboxed` plus a required nonempty `reason`. Main, its sidecars
and queued check payloads follow that stage choice. Status/job GET expose
`unsandboxed: <reason>` across the lifecycle; workers warn once per attempt.
The reason is intentionally public and must not contain secrets/private paths.
New registrations receive server-owned `sandbox_default: true` metadata;
persisted registrations and frozen jobs without it retain legacy unsandboxed
behavior if no author mode is present. Identical re-registration preserves the
marker. Author a new version/digest with `isolation: sandboxed` for explicit
legacy opt-in; old queued/frozen jobs do not change. These metadata never alter
the immutable bundle hash. Queue and workers need the policy-capable release.
Plain native `run.command` jobs stay unsandboxed by default in v1 for consumer
compatibility (dependency preparation and shared home caches). Their status and
job GET explain this default; workers warn once per attempt. Use the one-line
`isolation: sandboxed` to opt in to the kernel boundary and offline default.
New extension registrations still default sandboxed.
The remaining kernel guarantees on this page apply to sandboxed stages.

### CI mode (#1298)

Use `isolation: ci` for a build or test of repository code, or for any job that
mustn't see shared data. It needs no queue-owner approval (MY-44). The mode is
`sandboxed` plus three things: an attempt-private HOME, no shared model library,
and on macOS no worker home, `/Users`, `/Volumes`, network mounts or NAS root.
The job still sees its declared inputs and model snapshots, its own checkpoint
directory, prepared environments and extension caches, and system toolchains
(`/usr`, `/opt`, `/Library`, `/Applications` on macOS). Tools that keep config or
caches under HOME (git, Go, npm, pip, cargo) write to `{work}/.home`, which
follows the attempt's retention and cleanup. Unless the job sets its own,
`GIT_CEILING_DIRECTORIES` stops git's repository discovery at the attempt
directory, so a hidden parent repository can't cause a fatal read. Toolchains installed under the
worker user's home aren't visible: declare them through a prepared environment,
or install them system-wide. Linux reads only granted files, so `/etc/gitconfig`
and similar system files can be unreadable; set `GIT_CONFIG_NOSYSTEM=1` if git
fails on them.

Workers advertise `native_isolation_ci`. The queue places ci jobs only on workers
that advertise it, so an older worker never runs one under the weaker sandboxed
profile. Network access follows the ordinary declaration below: offline by
default. Linux doesn't yet qualify native egress, so a Linux ci job that
downloads dependencies needs them prepared or vendored. On macOS,
`needs.network: true` grants outbound access. Metadata such as directory names
can still be visible. Host `/tmp` stays shared on macOS, as for sandboxed.

The boundary protects against confined jobs. Same-user **unsandboxed host
processes** can still inspect or interfere with work and listeners and are
outside the threat model, as are root, kernel/driver exploits and malicious
operator runtime files. This is not a hostile multitenant security boundary.
Shared runtime/cache contents are readable by eligible jobs; keep secrets out of
them. macOS's targeted profile does not make the entire home directory private.

Landlock does not mediate all filesystem operations. In particular `stat`,
`chmod`, `chown`, `utime`, `setxattr` and related metadata operations retain
ordinary host permission checks. A same-UID confined job may therefore observe
metadata or change permissions of otherwise content-protected files. Even the
namespace mode uses a read-only host-root view plus selective content grants,
not a promise that every host pathname is hidden. In fallback mode, read-only
caches mean denied content writes, not immutable cache metadata. See the
[kernel's Landlock limitations and TCP rules](https://www.kernel.org/doc/html/v6.12/userspace-api/landlock.html).

## Work, environment and devices

Native workdirs must resolve within the worker-managed `{work}`. Stage scripts
and data as job inputs, or use a frozen extension whose source is copied to
`{work}/.myce-extension`; do not depend on a host checkout being writable.
Prepared environments and shared extension caches are available for reads and
execution. The operator's `--model-cache` (including its default beneath the
worker state root) and `--model-root` are also available for reads, not payload
content writes. Keep these shared model roots free of secrets; authored
`run.env` paths do not create new filesystem grants. Linux applies the same
protected-storage overlap validation to these roots as other runtime grants.
Use prepared model-file paths and libraries' offline/read-only cache options;
place mutable indexes, lock files and generated caches in attempt scratch.
The current job's checkpoint and usage paths receive specific write
grants. Work, temp and scratch follow existing attempt retention and cleanup custody.

### Declared model snapshots (#553)

`model_snapshots` resolves an exact snapshot and its blobs into a verified
worker cache entry. Concurrent attempts receive views of that same entry, with
no per-job copy. The initial cache fill may copy or clone hub bytes; it never
hard-links writable hub blobs. Restart pins, cleanup custody and eviction rules
are unchanged. `MYCE_MODEL_<NAME>` names the acquired data directory, not the hub.

- **Sandboxed Linux:** acquired model paths receive Landlock content-read grants
  without write or direct-execute rights. The namespace mode also exposes them
  through bubblewrap's read-only mount view (restoring exact paths hidden by
  private `/tmp`). Conflicting writable/executable ancestor grants refuse launch.
  In Landlock fallback `chmod` can still succeed, but cannot authorize a content
  write; metadata is not immutable.
- **macOS Seatbelt:** the acquired paths are readable and explicitly deny
  `file-write*` and direct execution, including caches outside the default spool
  or home. A conflicting write/execute grant refuses launch.
- **Plain native default (MY-25), or declared unsandboxed:** cache files and
  directories have write bits removed. Ordinary writes/creates/unlinks fail for
  the unprivileged worker user. **This is cooperative read-only access, not a
  security boundary:** a same-user job can chmod files/directories back to
  writable, replace entries through writable parents, or use another writable
  alias; root can bypass file modes. It can mutate shared cache bytes. Choose
  `isolation: sandboxed` when payload write denial is required.
  Unsandboxed processes on the host remain outside the sandbox threat model.

Interpreters can read model Python sources and loaders can map model libraries;
read-only data grants do not imply a model engine or safe custom code. Put
indexes, downloads and generated caches in `MYCE_SCRATCH_DIR`, and use offline
loader flags. These guarantees do not introduce verify-in-place access to a
mutable hub.

`TestModelSnapshotPlainReadOnlyViews`, `TestModelSnapshotLinuxReadOnlyViews` and
`TestModelSnapshotSeatbeltReadOnlyViews` use two live payloads holding concurrent acquisitions of the same cache inode,
and attempt writes against synthetic model bytes. Linux exercises Landlock and,
when available, namespaces separately. Unavailable platform checks are skipped
and must not be reported as native qualification.

Operators can declare additional local roots with repeatable worker/install
flags, for example:

```sh
--native-exec-root "$HOME/local-runtime/llama/bin" \
--native-read-root "$HOME/local-models"
```

`--native-read-root` permits content reads without writes or direct program
execution; `--native-exec-root` also permits execution. Interpreters can read
model-tree Python sources; libraries can be mapped by an allowed runtime.
These are trusted operator settings, saved in worker configuration, not
permissions granted by authored `run.env`. Declare literal absolute paths;
recursive directory grants need no glob. Filesystem, home, NAS and installed
Mycelium roots are refused, as are private-storage overlaps and conflicting
read-only versus executable/writable grants. A narrow model subtree on mounted
storage is permitted. Shared roots must contain no secrets or host controls.

Linux grants `/proc/self/cgroup`, `/proc/meminfo`, and exact
`memory.peak`, `memory.max`, `memory.events`, `memory.current`, `memory.stat`
files for the process's current cgroup and its trusted attempt members. It does
not grant ancestor/sibling cgroup contents or cgroup control writes. Without
per-member ceilings, members can inherit the same outer cgroup; these readings
then describe that inherited group, not independent member usage. GPU runtime
sysfs discovery remains separate from cgroup access.

NAS access uses the existing declarations: `inputs` names relative to
`--job-file-root` are copied into `{work}` before execution, and declared
outputs are published by the worker after execution. The payload can read and
write its exact `MYCE_CHECKPOINT_DIR` on the configured checkpoint mount. These
file operations need no payload outbound-network opt-in; the host owns the
mounted filesystem connection. They do not grant access to the entire NAS or
arbitrary host paths. An application that previously used container volume
mappings must use these declarations and attempt paths for native execution.

Headless office tools must put their writable user profile and scratch beneath
`{work}` (for example LibreOffice's `-env:UserInstallation=file://...` option).
Their own Unix-socket requirements still need qualification against the control
boundary; being offline does not authorize host Unix endpoints. No claim of
LibreOffice or GPU compatibility follows solely from the filesystem grants.

After `run.env` and environment activation, the worker sets `TMPDIR`, `TMP` and
`TEMP` to `{work}/.tmp` (through a short spool alias that resolves there, so
socket paths fit) and `MYCE_SCRATCH_DIR` to `{work}/.scratch` for main and
helpers. Both directories have identical absolute paths within an attempt, so a
main process can pass a scratch filename to its own sidecar. Other attempts
cannot read their contents. Authored values cannot redirect either directory to
a sibling or host directory. They do not claim that programs cannot discover
other host metadata or that macOS's unrelated system temp contents are hidden.

Keep [#219's allowlist](https://github.com/1kuna/mycelium/issues/219): documented
base/locale and GPU runtime variables, credential-free proxy/CA settings, then
explicit recipe/job values and worker-owned attempt/device variables.
`SSH_AUTH_SOCK`, session buses, `MYCE_QUEUE`, `MYCE_JOINING_FILE` and
`MYCE_TOKEN` are not inherited; the fleet token is also removed after authored
values. Inherited proxy/uv settings containing any `@` are dropped. Explicit
inputs, job secrets and `run.env` are intentional access for that job, not
protection from the job itself. `HOME` may still name the host home; filtering
an environment does not itself restrict filesystem access.

Worker-owned gate shells and namespace utilities use a trusted launcher
base environment. Authored loader and shell-startup variables enter only after
confinement; they cannot run code before the boundary. Check-output publication
opens a regular, bounded file beneath the trusted attempt root with
escape-resistant traversal, so a payload symlink/FIFO cannot turn the worker
into a credential reader or indefinitely block publication.

Linux refuses layouts where a runtime read grant overlaps private content trees
or contains a protected storage ancestor or configured credential. macOS applies
the same validation to its added model read exceptions. Descendants and exact
files within sibling work/temp, checkpoints, secret roots and credential trees
remain private; normal cache/model subtrees beneath storage ancestors remain
permitted. Validation resolves symlinked installation paths.
The default `/opt` grant is limited to the oneAPI runtime; other operator
runtime roots must be narrow and keep state and secrets elsewhere.

Linux retains the selected accelerator's verified device nodes, required shared
driver nodes and runtime roots, rather than exposing all device nodes or
guessing a kernel minor from an ordinal. Missing device mapping refuses launch.
Linux permits writes to existing files in the launching payload process's own
`/proc/self/task` tree so driver libraries can name their threads; other process
trees remain denied. This grant is inherited for that process, not newly created
child process identities. macOS retains Metal/compiler services. CPU select-none
qualification and
macOS's cooperative Metal limitation remain unchanged. Existing aggregate and
member cgroups, memory limits, authorization/Started gates, process identity
records and cleanup verification still apply. The unsandboxed supervisor
retains control, observation and signaling; a sandbox exit alone proves no
resource release.

## Linux mode selection and sidecars

The running worker probes its **service context**; success in an interactive
shell is insufficient. The current stack requires Landlock ABI >= 6, seccomp,
`bwrap` and `nsenter`. ABI 6 supplies signal and abstract-Unix scoping, but does
not by itself deny pathname Unix-socket connections. Seccomp supplies the
additional endpoint/syscall restrictions. Bubblewrap always creates a mount
namespace; Mycelium requests rootless user, PID, IPC and network namespaces too,
drops capabilities and installs the file/device policy. See
[bubblewrap's namespace and security model](https://github.com/containers/bubblewrap#sandboxing).

A native group creates one per-attempt namespace anchor after the existing
control/cgroup gate. Main and all sidecars join its user, mount, PID, IPC and
network namespaces; readiness probes enter its network namespace. A private
network namespace per member would prevent the declared helpers communicating.

Only a namespace creation **policy refusal** selects Landlock/seccomp fallback.
Missing tools, insufficient ABI, failed confinement probe, timeout or other
setup failures are refusal reasons, not permission to run unconfined or
silently downgrade. The current detector recognizes permission-denied errors;
operators must inspect the service policy when diagnosing them.

Fallback MUST permit connect and bind on the exact worker-reserved TCP ports
for this attempt's sidecars. With no reserved ports, it permits no new IP
endpoints. It denies all other TCP connects/binds, UDP, raw/packet sockets,
SCTP, MPTCP and AF_UNIX stream/datagram endpoint creation. The approved v1
exception permits AF_UNIX `SOCK_SEQPACKET` creation with protocol zero and only
the normal CLOEXEC/NONBLOCK flags, including jobs without TCP sidecars.
Private Unix stream socketpairs remain available for process wakeups;
datagram/seqpacket pairs remain denied. Authored environment
values cannot add a permitted port, and a
reserved port conflicting with the queue control port refuses launch.

**This is partial network isolation.** Landlock filters TCP port numbers, not
destination or bind addresses. A permitted destination port can be reached on
an off-host address, and a permitted local port can bind a wildcard or LAN
address. Readiness checks that the actual listener belongs to the attempt and
is bound to `127.0.0.1`, before accepting its response. That observation is not
a kernel guarantee against a later rebind or another connection on that port.
Use namespace mode when an enforced private network boundary is required.

### CUDA compatibility boundary

Some CUDA drivers initialize an abstract Unix seqpacket listener even for a
tiny device allocation. The [v1 ruling](https://github.com/1kuna/mycelium/issues/177#issuecomment-6453575275)
accepts a type-only exception in Landlock fallback, retaining abstract-socket
and signal scoping and every other denial. Namespace mode still denies new
AF_UNIX endpoints. Permission to create this socket does not itself qualify
CUDA execution or cleanup.

**same-UID pathname SEQPACKET endpoints reachable (partial)**. This is the
explicit accepted residual: classic seccomp cannot inspect `sockaddr_un`, and
Landlock before ABI 9 cannot mediate pathname Unix connections. The fallback
does not enforce abstract-only creation or connection. Ordinary Unix socket
permissions still apply; a root-owned mode-0600 socket remains inaccessible to
the unprivileged job. Mycelium/Docker/systemd stream endpoints remain denied,
but a same-UID pathname seqpacket control endpoint present now or added later
would be reachable. A current inventory cannot establish its future absence.

At fallback-worker startup, one `/proc/net/unix` snapshot inventories listening
seqpacket endpoints in the current network namespace. Root-owned pathname
sockets and the observed abstract `cuda-uvmfd-<namespace>-<pid>` naming convention
are excluded from warnings; every other endpoint, including unknown ownership,
is logged visibly. The CUDA name is a convention, not authenticated process
identity. Inventory failure also warns. `native_isolation_warning` exposes a
compact startup summary in status; endpoint details remain in worker logs.
This advisory runs once at startup, not continuously, does not inspect other
network namespaces and grants no authority or additional isolation.

The full-isolation upgrade remains minimal-root namespaces plus the separately
approved per-host AppArmor userns exception, pending the operator and
non-blocking for the accepted v1 fallback. The userns exception below only
permits namespace creation. The current
read-only host-root mount and socket policy would still need changes and real
qualification before supporting that CUDA path. The preferred candidate is a
minimal bubblewrap filesystem containing socket-free trusted runtime/cache
trees, exact attempt/checkpoint mounts, private runtime/temp/proc filesystems
and assigned devices, with Unix creation enabled only inside that boundary.
Read-only mounts alone do not prevent connections to sockets they expose.
An alternative enforcing AppArmor payload profile needs attempt-specific file
write grants, protected profile lifecycle and a verified enforce-mode transition:
pathname Unix connections use file permissions, so a socket-only profile with
broad writable paths is insufficient. Neither option is implemented or
qualified by this document; either retains its separate administrator approval
and hardware qualification requirements.

## Declaring outbound access

```yaml
needs:
  network: true
```

Declare payload-time downloads or external APIs in the job/profile or extension
variant's `needs`. Omitted/false means offline by default, subject to the
fallback exception above. Sidecar loopback requires no general outbound
declaration. Worker-owned dependency/environment and input preparation remains
network-capable and launch-gated; it is outside this payload boundary. Trust
preparation recipes accordingly. Explicit local setup/qualification hooks and
standalone model servers are not covered by this payload isolation contract.

Both Linux modes currently withhold `native_network`: namespace egress transport
is unqualified, and fallback reserved-port access is not an online capability.
Protected namespace egress is a [post-v1 follow-up (#259)](https://github.com/1kuna/mycelium/issues/259),
revisited earlier only for a consumer that requires it.
Failed offline attempts include the outbound declaration remedy in their
diagnostic. An online declaration routes only to a host advertising protected native
outbound execution. If none qualifies, submission is refused immediately with
HTTP 409 `no_qualified_target`; it must not sit in an impossible queue forever.
Normal temporary capacity/maintenance waits on a capable host retain their
existing semantics. Every native dispatch requires a recognized isolation capability; older
workers with no report and unavailable workers cannot receive offline jobs
either.

macOS may advertise `native_network` when `sandbox-exec` is available. Its online
profile continues to deny non-sidecar loopback, Unix control endpoints and the
queue TCP port on every destination address, covering queue aliases/DNS changes.
This deliberately also denies unrelated external services using the same port;
`needs.network: true` is not unrestricted access. A missing launcher or rejected
profile fails the attempt. Seatbelt's deprecated interface is a compatibility
and qualification risk, not justification for an unconfined retry.

## Optional administrator action: per-host AppArmor user namespaces

**OPTION ONLY — not applied by this amendment.** On a host using AppArmor's
`kernel.apparmor_restrict_unprivileged_userns=1`, an administrator may authorize
user namespaces for the installed `myce` executable while keeping the global
restriction enabled. This is a privileged, per-host policy change requiring
separate authorization. Do not disable the global sysctl.

For AppArmor with ABI 4.0/userns support, the profile syntax is:

```text
# /etc/apparmor.d/myce-userns
abi <abi/4.0>,
include <tunables/global>

profile myce-userns "/ABSOLUTE/INSTALL/PATH/myce" flags=(unconfined) {
  userns,
}
```

Replace the attachment placeholder with the actual resolved installed binary
path, not a shell variable, wildcard, source checkout or symlink alias. Review
existing attached profiles first; do not replace a stricter profile with this
permissive one. With the administrator-reviewed file installed, load it and
restart the worker so it executes under the new attachment:

```sh
sudo apparmor_parser --replace /etc/apparmor.d/myce-userns
systemctl --user restart MYCE_WORKER_UNIT
```

Use the actual installed user unit, after the normal drain/cleanup procedure.
The parser loads kernel policy; merely saving the file is insufficient. A
running worker will not acquire the new executable attachment retroactively,
and its capability detection is cached for its process lifetime. Verify the
restarted service's profile label and namespace probe, including inherited
policy/any separate attachment for `bwrap` and `nsenter`; report failure rather
than widening the global policy. The option does not supply missing tools,
Landlock/seccomp, GPU mapping or an outbound transport.

**Risk:** `flags=(unconfined)` is intentionally permissive: it adds no general
AppArmor file, network or process confinement. `userns,` exempts this executable
and descendants retaining its profile from the user-namespace restriction.
Unprivileged user namespaces expose additional kernel code to those processes,
including namespaced privileged operations, increasing the kernel exploit
attack surface. The attachment trusts any executable bytes installed at that
path; a user who can replace that binary can use the exception for replacement
code. The payload's independent namespace/Landlock/seccomp restrictions remain
necessary. This option is narrower than disabling the restriction globally,
but is not a claim that enabling user namespaces has no security cost.

Syntax and scope: [AppArmor profile modes and userns rules](https://manpages.ubuntu.com/manpages/noble/en/man5/apparmor.d.5.html),
[parser load/replace behavior](https://manpages.ubuntu.com/manpages/noble/en/man8/apparmor_parser.8.html).
No profile was loaded or service restarted for this documentation change.
