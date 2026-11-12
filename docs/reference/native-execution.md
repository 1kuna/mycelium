# Native execution

The native payload sandbox default and its exceptions. See
[native isolation](../native-isolation.md) for the platform guarantee table.

## Native payload isolation

New native entries and extension registrations default to a kernel boundary,
not same-UID `0700` alone.
See [native isolation](../native-isolation.md) for the platform guarantee table and
limits. This #177 minimum is a v1 requirement; documentation does not establish
fleet qualification or rollout.

For an intentional stage exception, put these fields at the top level of
`extension.yaml` or inside the selected adapter entry:

```yaml
isolation: unsandboxed
reason: The native helper requires host IPC unavailable in the sandbox.
```

The reason is required, nonblank and public. Main, queued check/qualification
payloads and all its sidecars share this choice. Invalid modes are refused;
`isolation: sandboxed` explicitly selects the default. There is no global
switch or per-sidecar choice. A submitted extension job cannot override its
registration's isolation. Unsandboxed stages need no sandbox/network capability
report and have ordinary worker-user host and network access, including access
to sibling files and host controls. Keep the queue and workers on the same
policy-capable release; older workers do not implement this exception.

The queue's server-owned `sandbox_default` marker is registration metadata,
outside the digest. Missing markers on persisted pre-policy registrations and
frozen jobs preserve legacy unsandboxed behavior. Re-registering identical
bytes preserves that choice, including after restart. To sandbox a legacy
registration, publish a new authored version/digest with `isolation: sandboxed`.
Already queued/frozen jobs keep their original policy. Do not edit database
markers or bundle identities as an upgrade step.

`myce status`, the status page, `GET /api/status` and job GET show
`unsandboxed: <reason>` even for queued or terminal jobs; legacy registrations
use a fixed public reason. The worker logs one warning per attempt rather than
one per member/retry, with the warning receipt retained in its existing spool.
Work/input staging, managed cwd/temp/scratch, token/environment filtering,
device selection/binding, cgroups, launch authorization, Started, fencing and
release checks still apply. They do not provide a kernel boundary or make
shared caches read-only for unsandboxed code.

The kernel restrictions below describe sandboxed stages.
Linux fallback explicitly reports **same-UID pathname SEQPACKET endpoints
reachable (partial)**. It permits the v1 CUDA socket-type exception while
retaining stream/datagram control denial and Landlock abstract-socket scoping.
Check `native_isolation_warning` and worker startup logs for unexpected
non-root/non-CUDA seqpacket listeners or inventory failure. The startup scan
is advisory and cannot prove future endpoint absence. Minimal-root namespaces
with the per-host AppArmor userns exception remain an unapplied operator
upgrade; the exception alone does not qualify CUDA or the mount policy.

Stage source/data into the managed `{work}` through inputs or a frozen
extension. Native workdirs must resolve beneath `{work}`. Extension source gets
a private copy; prepared environment/extension caches permit reads/execution,
not content writes. The worker overrides `TMPDIR`, `TMP` and `TEMP` to
`{work}/.tmp` (through a short alias, below) and `MYCE_SCRATCH_DIR` to
`{work}/.scratch` after authored env/activation. Main and its own sidecars share both paths. Repeatable
`--native-read-root` and `--native-exec-root` worker/install settings add narrow
read-only model roots or read/execute runtime roots; filesystem, home, NAS and
installed Mycelium roots and protected-content overlaps are refused. #219's environment allowlist and
credential filtering remain in effect. Linux fallback does not protect all
metadata operations, including `chmod`; macOS protects targeted spool and
credential contents while unrelated host metadata/files remain visible.

### Job environment

Every native command and sidecar, on every host, is told what it was given;
jobs need not set these themselves:

- `MYCE_CPUS`: the job's CPU allotment, the `needs.cpu_cores` the host ledger
  charged it (at least 1). The worker always sets it.
- `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `OPENBLAS_NUM_THREADS`,
  `VECLIB_MAXIMUM_THREADS`, `NUMEXPR_NUM_THREADS`, `RAYON_NUM_THREADS` and
  `GOMAXPROCS` default to `MYCE_CPUS` (OpenMP and PyTorch, MKL, OpenBLAS, Apple
  Accelerate, NumExpr, Rayon, Go). A value the job sets in `run.env`, a sidecar's
  `env` or its environment activation wins. On a shared interactive host only, a
  value above the allotment is lowered to it. The worker's own settings never
  pass through.
- `TMPDIR`, `TMP` and `TEMP`: the attempt's `{work}/.tmp`, and
  `MYCE_SCRATCH_DIR`: its `{work}/.scratch`. The worker owns both and they
  override authored values; retention and cleanup follow the attempt.
  `TMPDIR` is a short alias, `STATE/t/<12 hex>`, a link that resolves to
  `{work}/.tmp`. A Unix socket path must fit 108 bytes on Linux and 104 on
  Darwin, and the attempt tree alone can use most of that: servers that bind
  sockets in `TMPDIR`, such as vLLM's engine IPC, failed to start. With a
  default install's state path the alias leaves room for a UUID-named socket.
  Where links are unsupported the real path is used.

Docker jobs get their own `MYCE_*` paths (see [files](../files.md)) and are not
covered by the thread defaults above.

Payloads are offline by default. Add `needs.network: true` for payload-time
downloads or API calls. Worker-owned environment/input preparation keeps its
network access. Placement requires protected outbound capability; submission
returns `no_qualified_target` immediately when none qualifies. Both Linux modes
currently withhold `native_network`, because namespace egress is unqualified.
macOS online jobs still cannot reach other loopback endpoints or the queue's
TCP port at any address, including unrelated services using that port.

Linux probes `bwrap`, `nsenter`, Landlock ABI >= 6 and seccomp in the running
service's context. Namespace mode shares one attempt anchor across sidecars.
Only namespace policy refusal permits Landlock/seccomp fallback; missing tools
or broken setup fail closed. Fallback allows only exact reserved sidecar TCP
ports, **including off-host connections and any bind address on those ports**;
this is partial network isolation. Readiness checks actual listener ownership
and `127.0.0.1` binding, not a kernel restriction on future binds. Other TCP
ports, UDP/raw/packet/SCTP/MPTCP and Unix endpoints are denied. macOS uses
deprecated `sandbox-exec`/Seatbelt and refuses launch if the launcher/profile
fails. Existing cgroup, GPU, launch and cleanup gates remain required.

An optional [per-host AppArmor profile](../native-isolation.md#optional-administrator-action-per-host-apparmor-user-namespaces)
can authorize user namespaces for the installed binary while keeping
`kernel.apparmor_restrict_unprivileged_userns=1`. It is a separate privileged
action, not applied here: review the intentionally permissive profile and
kernel attack-surface cost, load with `apparmor_parser`, then restart and verify
the actual worker service. Never disable the global restriction as a shortcut.
