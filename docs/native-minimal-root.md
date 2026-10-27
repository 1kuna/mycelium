# Optional Linux minimal root (#262)

This is the source contract for the optional upgrade after #177 / PR #257.
It is **off by default and unqualified**. Source tests establish no Linux,
CUDA, XPU, Office or mounted-NAS acceptance. This mode's Linux qualification
waits for the operator's per-host userns-policy approval. Existing Option B qualification
has its separate gate, idle and preemption requirements and does not depend on
that decision. No AppArmor manager, package installation, sysctl or service
policy mutation is implemented.

## Operator contract

`Worker.NativeMinimalRoot`, worker/install `--native-minimal-root`, and persisted
`native_minimal_root` request the mode. No job, extension or environment setting
enables it. Omitted/false preserves the existing host-root namespace and fallback
selection. True never falls back, even where the old Landlock mode works.

The empty root supplies no ambient `/usr`, `/bin`, `/sbin`, `/lib` or oneAPI
installation. Existing `--native-exec-root` declarations must include the
installed runtime, `/bin/sh`, `/bin/true`, ELF loader/libraries and commands.
Exact declared aliases are materialized from their validated canonical sources.
`--native-read-root` retains read-only/no direct execution; executable/writable
inode aliases are refused. Prepared environments, caches, narrow device discovery
and the small existing configuration/proc allowlist keep their roles. Unsafe
symlinks or unrelated service artifacts require a narrower runtime declaration.
No broad home, NAS, spool or cgroup grant is added.

This mode requires the existing bubblewrap tool's `--bind-fd` and
`--ro-bind-fd` features (0.10 or later). Older tools refuse the mode; no upgrade
or installation is performed. Ordinary `/proc/self/fd` bind source spelling is
insufficient: bubblewrap resolves it back to a pathname. Its FD-bind option
compares the resulting mounted inode before executing any runtime/helper code
([upstream FD-bind implementation](https://github.com/containers/bubblewrap/blob/v0.10.0/bubblewrap.c)).
The final helper checks the same inode identities again.

The actual worker service must construct an anchor, join its user/mount/PID/IPC/
network namespaces with `nsenter`, verify metrics, apply Landlock ABI >= 6 and
seccomp, and execute `/bin/true`. Startup refuses failure; only success advertises
`native_isolation_minimal_root`. Missing tools, unsafe layout, missing metrics,
setup failure or policy refusal never selects a substitute executor. The probe
does no GPU computation. Evidence belongs to this service process/configuration;
every real launch constructs and verifies a new root.

## Construction and trust

Read-only directory binds do **not** prevent pathname Unix connections. A scan
followed by such a bind cannot enforce socket absence. The trusted launcher uses
standard bubblewrap and Linux FDs instead:

1. Construct a fresh tmpfs root, private `/run`, `/tmp`, `/dev/shm`, synthetic
   `/dev` and procfs in the new PID namespace.
2. Recreate mutable artifact read-tree directories. Open children relative to pinned parents
   with `O_PATH|O_NOFOLLOW`; bind individual regular files read-only with bubblewrap
   FD-bind comparison before executable startup. Never bind a mutable artifact read-tree directory. Refuse sockets, FIFOs and other special
   files. Recreate only links contained in the declared read layout without
   aliases to writable trees. Imports beneath private `/tmp` use separate empty
   tmpfs skeleton mounts, remounted read-only after construction and verified by
   the final helper; remounting `/` alone cannot protect that separate filesystem.
   Generic private `/tmp` and `/run` Landlock grants omit execution. Declared RX
   roots and exact writable work (including its managed `TMPDIR`) retain execution;
   imported RO content receives no direct execution grant. An import of all `/tmp`
   is refused so ordinary private temporary file/directory creation remains usable.
3. Pin and bind exact attempt work/checkpoint/usage objects. Inspect existing
   content through the pinned directory; refuse special files, symlinks and
   runtime inode aliases. Existing exclusive staging, verified previous-attempt
   cleanup and launch gates own these objects until launch. Main and its own
   sidecars intentionally trust each other afterward. Logs use their existing
   exact stdout/stderr descriptors, never a mounted log directory.
4. The fixed GPU-discovery roots under sysfs are a separate kernel-generated
   case: verify `SYSFS_MAGIC`, pin their directory FDs, reject every nested mount
   alias, and verify the resulting mounts read-only. Sysfs permits no user-created
   socket inode or arbitrary symlink; its kobject links are kernel-owned. This
   exception never covers a runtime/cache directory, `/sys`, or cgroupfs. Trusted
   privileged host mount changes remain outside the threat model.
5. Bind verified assigned character-device nodes and remount the synthetic root
   read-only. Retain Landlock content/network/abstract/signal scopes and seccomp.

Replacing a regular source with a live socket cannot pass the pinned-inode
comparison; a surviving rename can preserve the original inode, otherwise setup
refuses before executing the helper.
Later source-directory insertion is invisible because the directory is absent.
This protects shared caches against socket insertion/replacement by previous or
sibling confined attempts without claiming a pathname scan is atomic. Read-only
constructed mounts protect runtime/cache contents and directory entries even
beneath the generic writable private-temp Landlock grants. Writable inode
aliases are refused. Fallback's permitted host metadata changes cannot turn a
pinned regular inode into a socket.

The worker, bubblewrap, kernel/drivers and operator-selected artifacts/producers
remain trusted. Runtime **contents** are not a cryptographically authenticated
snapshot. Trusted updates must preserve artifact layout and keep controls and
credentials out of it; concurrent removal can refuse launch. Root, malicious
operator artifacts and unconfined same-UID host processes remain outside #177's
threat model. No payload-writable shared producer or inherited host directory/
socket handle is accepted as authority.

No model/runtime bytes are copied; the receipt is the only copied metadata. Cost is traversal, directory metadata and one
mount/open descriptor per regular artifact file, plus a tmpfs mount per outer
imported directory tree beneath `/tmp`; the fixed sysfs directories do
not require expansion. `bwrap --args` consumes NUL-separated
arguments from a sealed memfd to avoid `ARG_MAX`. Descriptor/mount exhaustion
refuses launch. Large layouts and startup cost require measurement; no automatic
directory bind or host-limit adjustment is allowed. The capability canary has a
ten-second process budget; the existing two-second namespace handshake bounds
a broken launcher. Neither is a workload deadline or a fallback authorization.

## Metrics, permission and custody

After attachment, the trusted launcher resolves host `/proc/self/cgroup` and
mountinfo and binds existing regular `memory.peak`, `memory.max`, `memory.events`,
`memory.current`, `memory.stat` for the current direct member or trusted main/
sidecar leaves. A group anchor excludes its aggregate/control leaf. Without
exact payload leaves a group refuses the mode. No metrics at all is an error;
individual older-kernel optional-file absence retains its existing meaning.

A sealed metadata memfd supplies bubblewrap `--ro-bind-data`, which copies only
the receipt into an exact read-only regular mount. It records mounted inodes,
host cgroup mapping and namespace identities. The final helper checks the root, private namespaces/
proc, mount topology, read-only regular inodes and its current kernel membership.
No cgroup2 directory mount, ancestor/sibling/control grant, environment-selected
path or silent metric disable is used. `/proc/meminfo` and `/proc/self/cgroup`
remain readable.

Only after verification may the final helper permit AF_UNIX `SOCK_SEQPACKET`,
protocol zero with normal CLOEXEC/NONBLOCK. The old host-root namespace still
denies creation. The approved fallback exception stays unchanged:
**same-UID pathname SEQPACKET endpoints reachable (partial)**. TCP queue-port
denials, reserved sidecar ports, stream-only socketpairs, blocked syscalls and
Landlock scopes remain. All Linux modes remain offline-only, without
`native_network`; egress is #259 linked to #248.

Members share the anchor, mount paths, scratch, IPC and network. Sidecar
environments are prepared before root construction. Strict-mode preparation is
registered once with the existing process controller before acquiring sidecar
cache/build locks and links cancellation to the caller's context during synchronous
startup only. Running groups retain controller-owned TERM/checkpoint grace.
Cancellation/preemption can
close that gate before an anchor exists. A startup refusal unregisters the
preparing process and releases already-acquired sidecar cache holds; the caller
retains ownership of main-environment and memory cleanup on startup failure.
After successful startup the existing group goroutine owns those resources.
Existing launch/start-by
receipt pipes 3/4, memory attachment, TERM/checkpoint grace, pidfd custody and
group cleanup remain. Setup FDs close at exec; stdio/receipt slots reject host
sockets/directories. Source handles do not survive into payloads or the
production anchor shell. Bubblewrap selects an explicit in-root cwd; the final
helper verifies its mount identity, and joined members use `--wdns`.

## Verification handoff

Parent owns compilation, execution and review. Suggested selectors:

- Portable: `TestNativeMinimalRootLinkLayout`,
  `TestNativeMinimalRootOperatorConfig`,
  `TestNativeMinimalRootCancellationPreparing` (real held build lock; user cancel
  and priority preemption plus startup-context cancellation, no namespace or payload
  launch), `TestInstallSettingsTableComplete`,
  `TestInstallSettingsOverlayMatrix`, `TestNativeNetworkPlacement`.
- Linux planning/refusal: `TestNativeMinimalRootPinnedConstruction`,
  `TestNativeMinimalRootCapabilityRefusal`; retain
  `TestNativeIsolationLinuxNamespaceSocketPolicy` and existing fallback attacks.
- After policy approval: `MYCE_TEST_NATIVE_MINIMAL_ROOT=1` and
  `TestNativeMinimalRootLinuxAttacks`, in a static (`CGO_ENABLED=0`) test binary.
  Unavailable namespaces explicitly skip/refuse; no unconfined substitute counts.

The attack covers live same-UID/type baselines, a read-only-mounted socket,
sibling and foreign abstract endpoints, link escape, replacement after FD
pinning, own abstract IPC, loopback/scratch sharing, runtime read/execute, model
no-write/no direct execution, creation/metadata denial in imported RO/RX root and
nested directories beneath `/tmp`, and current/main/sidecar metrics with other/control
denials. Synthetic member metric fixtures are distinct from current membership
proof. The attack also joins a separately constructed anchor through `nsenter`; its
sidecar is a descendant in the real constructed root; production
group attachment/custody, actual runtime layouts, checkpoints, GPU/NAS/Office and
TERM/cleanup still need the parent's integration and approved Linux evidence.

Current state: source pending parent compilation/tests and independent review;
the first source review's private-temp grant and pre-anchor cancellation findings
have candidate fixes and regressions, with no build/test execution by this worker.
No Linux execution, policy exception or rollout claimed. Next owner: parent for
gates and stack-base integration; the maintainer for host policy. Tracking:
[#262](https://github.com/1kuna/mycelium/issues/262).
