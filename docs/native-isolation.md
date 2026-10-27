# Native job isolation

The [approved amendment](design-proposal.md#amendment-minimum-native-job-isolation-177-219-248)
defines the #177 minimum: job-scoped work/temp, no ambient credentials, and no
worker control access. [#248](https://github.com/1kuna/mycelium/issues/248#issuecomment-6447873336)
makes that minimum a v1 requirement; wider executor hardening is separate.
These are implementation contracts and limits, **not fleet qualification or
deployment evidence**.

## Boundary and guarantees

Native batch commands and their sidecars, including queued extension run/check
payloads, inherit kernel restrictions across fork/exec. Main and helpers in
one attempt trust each other and share its resources. `0700` is useful hygiene,
but provides no boundary between jobs running as the same user.

| Platform/mode | Directory and files | Temp | Environment | Control access | Network |
| --- | --- | --- | --- | --- | --- |
| Linux namespaces | User/mount/PID/IPC isolation; read-only host-root view plus Landlock content grants. Sibling contents denied; host metadata can remain visible. | Managed `{work}/.tmp`; private `/tmp` and `/dev/shm` shared within the attempt. | #219 allowlist; worker overrides temp and attempt/device controls; no ambient agent/session or fleet credentials. | Private PID/network namespaces plus seccomp/Landlock deny host process, control-file and socket access. | Private network namespace, shared by the attempt's sidecars; reserved TCP loopback only. No qualified outbound transport; no `native_network`. |
| Linux Landlock/seccomp fallback | Content reads/execution limited to runtime/cache/attempt grants; writes limited to attempt/checkpoint/usage grants. **Metadata is not isolated**, including permission changes. No private mount/PID view. | Managed `{work}/.tmp`; sibling temp contents denied. Host `/tmp` is not replaced. | Same allowlist and overrides. | Denies other processes' sensitive procfs contents, tracing/memory access and external signals; denies Unix endpoints and non-reserved TCP ports. | **Partial network isolation**: exact reserved sidecar TCP ports permit connect/bind on any address, including off-host. All other TCP ports denied; no general outbound capability. |
| macOS Seatbelt | Targeted denial of same-spool sibling contents/writes, configured worker storage and standard credential roots. Prepared caches allow reads/execution, not writes. Metadata and unrelated host files can remain visible. | Managed `{work}/.tmp` protected with the spool; no private system `/tmp` namespace. | Same allowlist and overrides. | Denies Unix endpoint connections, protected credential contents, cross-job process access and unwanted Mach services; retains Metal services. | Default outbound: reserved sidecar TCP loopback only. With `needs.network: true`: general outbound except other loopback endpoints and the queue's TCP port at all addresses. |
| Missing/unsupported confinement | Required behavior: native launch fails closed. | No unconfined payload launch permitted. | An allowlist alone is insufficient. | No implied protection. | No inferred online capability. |

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
grants. Work and temp follow existing attempt retention and cleanup custody.

NAS access uses the existing declarations: `inputs` names relative to
`--job-file-root` are copied into `{work}` before execution, and declared
outputs are published by the worker after execution. The payload can read and
write its exact `MYCE_CHECKPOINT_DIR` on the configured checkpoint mount. These
file operations need no payload outbound-network opt-in; the host owns the
mounted filesystem connection. They do not grant access to the entire NAS or
arbitrary host paths. An application that previously used Docker volume
mappings must use these declarations and attempt paths for native execution.

Headless office tools must put their writable user profile and scratch beneath
`{work}` (for example LibreOffice's `-env:UserInstallation=file://...` option).
Their own Unix-socket requirements still need qualification against the control
boundary; being offline does not authorize host Unix endpoints. No claim of
LibreOffice or GPU compatibility follows solely from the filesystem grants.

After `run.env` and environment activation, the worker sets `TMPDIR`, `TMP` and
`TEMP` to `{work}/.tmp` for main and helpers. These values cannot redirect temp to
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

Linux refuses layouts where a runtime read grant contains protected worker
storage or configured credentials, including symlinked installation paths.
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
for this attempt's sidecars. With no reserved ports, it permits no new network
endpoints. It denies all other TCP connects/binds, UDP, raw/packet sockets,
SCTP, MPTCP and new AF_UNIX endpoints. Private Unix stream socketpairs remain
available for process wakeups; datagram/seqpacket pairs are denied because a
datagram pair could reconnect to a protected pathname socket. Authored environment
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
tiny device allocation. The current seccomp policy denies that creation.
Landlock before ABI 9 cannot mediate pathname Unix connections; enabling the
same socket type would also permit attempts against host pathname controls.
An offline job declaration does not resolve this compatibility boundary.
Do not enable Unix sockets as an unprotected fallback.

The userns exception below only permits namespace creation. The current
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
Failed offline attempts include the outbound declaration remedy in their
diagnostic. An online declaration routes only to a host advertising protected native
outbound execution. If none qualifies, submission is refused immediately with
HTTP 409 `no_qualified_target`; it must not sit in an impossible queue forever.
Normal temporary capacity/maintenance waits on a capable host retain their
existing semantics. Every native dispatch requires a recognized isolation capability; older
workers with no report and unavailable workers cannot receive offline jobs
either. Managed Docker retains its separate contract.

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
