# Onboarding and joining

How invitations, joining, NAS selection, discovery and capability profiles work.
The step-by-step setup is in the [runbook](../operations.md#0-ask-the-user); NAS
mount recovery is in [NAS mounts](nas-mounts.md).

## Automatic joining

Run `myce invite` on an installed, authorized hypha and paste its output on the
new machine. The earlier [setup steps](../operations.md#4-add-each-other-machine) cover the first
invitation; [signed release publishing](../operations.md#publish-and-approve-an-onboarding-release)
explains artifact approval. `--out FILE` saves the line mode 0600 without
overwriting an existing file. Installed authority is the default; an explicit
`MYCE_JOINING_FILE`/`--joining-file` or queue-local `--identity-dir` selects another.
The bootstrap needs curl, a SHA-256 tool and OS shell utilities, not Go/Git.
It verifies approved artifact bytes before execution and redeems a pinned-TLS
one-time enrollment ticket, never the fleet token.

Enrollment binds the receipt to the current service PID and release, then waits
for the queue to observe the exact local session, compatible
protocols, every expected device, CPU/RAM and acknowledged budgets, a small runner
smoke, and five seconds of stable service-manager health. The final line contains
the hypha name, OS/arch, components, allowances, participation and enforcement.
`ready`, `polite_waiting`, `disabled` and `drained` are separate successful states.
A queue outage, HTTP 426, unavailable component or restarting service leaves
enrollment incomplete. A smoke proves the command runner; project extensions
still need their own native checks.

Keep the printed command to resume an interrupted install or repeat a health
check. It uses private saved authority after installation, even when the original
ticket is spent or expired. A same-release rerun preserves files and service PID.
A command for a newer approved release drains only this hypha, waits for custody
to settle, upgrades through the existing installer and rechecks readiness. Spools,
identity, settings and operator-disabled policy persist. Failure leaves custody
and maintenance in place for repair. Printed commands freeze a release; a newer
release needs a newly printed line, whose ticket an installed host does not use.

NAS mode is required by [#109](https://github.com/1kuna/mycelium/issues/109).
Hardware preflight, NAS/tool setup and saved participation/service policy use the
normal installer options; those slices are integrated separately under #108.
Automatic legacy conversion, installed extension qualification and native MacBook
sleep/travel acceptance remain separate slices. No onboarding command grants
permission to operate other hyphae or bypass storage/device prerequisites.

## Private file compatibility and foreground join

For explicit credential-file transfer, use:

```sh
myce invite --private-file --out invitation.json
myce install worker --joining-file invitation.json
```

This older mode requires a separately delivered binary. Its mode-0600 file
contains the shared credential: transfer privately and keep it out of Git,
messages, argv, history and logs. The installer copies it into its private
installation directory. Foreground operation and status use the same pinned client:

```sh
myce join --joining-file invitation.json
myce status --joining-file invitation.json
```

`MYCE_JOINING_FILE` also selects that file. Queue host maintenance remains effective
across joining and rejoin. Preserve the queue's identity directory with its SQLite
state; replacing the identity invalidates existing certificate pins.

## Onboarding storage and tools

The NAS selection is saved configuration, `--nas PATH`, or the default
`/mnt/nas` (Linux) / `/Volumes/NAS` (macOS). The installer never selects an
unrelated mounted share. When the default is absent, `nas_not_configured` lists
candidate mounts with an explicit selection hint before either install plan.
Dry-run resolves the same selection without creating roots or running write
probes. Real installation requires a visible network/automount and validates
all NAS filesystem operations in a single 250 ms I/O budget; `nas_unresponsive`
means the mount did not answer within that budget (#63). A kernel call may
remain blocked, but retries cannot duplicate the outstanding validation.
NFS/SMB, autofs (including autofs-managed NFS) and the known network FUSE types
`fuse.sshfs`, `fuse.rclone`, `fuse.smbnetfs` are eligible; generic/local FUSE is
not. Mycelium does not mount a share or fall back to local storage.

### Accelerator runtimes and build tools

On Linux, missing-runtime refusal applies to NVIDIA display controllers and
Intel discrete PCIe display endpoints (Intel vendor ID and positive
[`current_link_width`](https://github.com/torvalds/linux/blob/master/Documentation/ABI/testing/sysfs-bus-pci)
from the PCI sysfs ABI). Integrated root-complex graphics have no discrete
PCIe link. This device-class rule avoids a model list; actual runtime inventory
still qualifies the accelerator. Other displays, including BMC, VM and AMD
controllers, are ignored with one informational PCI identity line. Apple Silicon
without Metal returns a typed runtime failure.

Onboarding does not download uv/pixi or require them for an idle command worker.
A native environment build resolves its required backend from the service's
saved PATH first. A suitable tool identifies itself with a numeric version via
`--version`; existing installs retain precedence on upgrades. If unavailable,
only that backend's pinned vendor archive is fetched, checksum-verified and
installed under the user's Mycelium tool directory, then invoked by absolute
path. uv archives also install uvx when present. Download failure fails that
requested environment build with a typed reason. It does not retroactively fail
onboarding. Probe commands strip `MYCE_TOKEN` and never print raw stderr.

## Address and network behavior

The queue keeps the existing HTTP API and adds a TLS endpoint for joining.
It advertises `_mycelium._tcp.local.` with an opaque instance name and protocol
version only. Neither the token, a personal name nor the capability profile is
advertised. The TLS endpoint defaults to the same listening interface as the
HTTP queue, with an automatically selected port. A loopback-only queue remains
local: configure the queue's `--listen` for its intended interface when other
hyphae need to reach it. `--discovery-listen ADDRESS:PORT` selects a fixed TLS
port; `--discovery=false` disables both advertising and the joining endpoint.
These queue flags are available for foreground queue runs; the existing
installer uses the default discovery settings.

Joining first browses for two seconds, authenticates candidates against the
privately provisioned pin, then tries the saved HTTPS address. The whole CLI
resolution has a bounded deadline. A wrong certificate receives no credential;
a wrong credential cannot join; redirects are never followed. Several discovered
mycelia are safe to encounter: only the one matching the invitation can admit
the hypha. Failure never creates another mycelium or changes the pin. Errors identify a
failed mDNS browse or zero compatible addresses, connection failure, identity
verification rejection, or credential rejection without exposing candidate
addresses or the credential. A started advertisement does not prove that
other machines can see it.

For a network that blocks multicast, supply a reachable HTTPS queue origin
with `invite --queue HTTPS_ORIGIN`, or `join --queue HTTPS_ORIGIN` together with
the invitation. A supplied address bypasses discovery but still checks the pin.
Use a fixed TLS port for a saved address that must survive queue restarts.
The hypha retries its selected endpoint while running; rediscovery happens at
its next join/start, rather than migrating an active worker between URLs.

macOS uses the system mDNSResponder through `/usr/bin/dns-sd` for proxy
registration, browsing and address resolution. Linux uses the running Avahi
daemon through `avahi-browse`, `avahi-publish-service` and
`avahi-publish-address`. These client tools must be available when Avahi is
running; a missing tool or daemon failure reports discovery unavailable rather
than starting a competing responder. Only Linux without a running Avahi daemon
(and other platforms) uses the existing pure-Go fallback. Mycelium does not
install packages, start daemons or alter their configuration.

Native publication uses opaque proxy host/service names and `version=1`, with
one service per usable listener address; no machine name, token or profile is
published. Registration waits for the daemon's acknowledgement. Browse commands
have the same two-second bound; advertisement commands live with the queue and
are stopped and reaped when it exits. The daemon owns multicast sockets.

mDNS needs local-link UDP 5353 multicast; joining also needs the queue's selected
TCP port. Segmented networks, guest isolation and firewalls can block either
separately. Do not disable firewalls to fix discovery.

[Apple TN3179](https://developer.apple.com/documentation/technotes/tn3179-understanding-local-network-privacy)
says macOS automatically allows local-network access for command-line tools
started in Terminal or over SSH, including their children. SSH alone therefore
does not require a Local Network permission grant. App-launched helpers and
launch agents can instead require user approval for the responsible app in
System Settings > Privacy & Security > Local Network. Allow Mycelium in its normal
consent alert or that panel. The signed agent stays alive and retries blocked
routes after approval; it does not grant or reset the permission itself.
The foreground installer can reach its readiness deadline while consent is
pending; rerun the same command to wait again without reinstalling or spending
another ticket. It prints enrollment success only after readiness passes.
A successful foreground/SSH check does not establish launch-agent,
login/reboot or app-launch behavior.

WSL2 default NAT does not reliably carry mDNS. Use a saved/supplied address, or
mirrored networking with appropriate private-network and Hyper-V firewall rules.
Mycelium does not change WSL networking or firewall configuration. A WSL2
worker reports Windows owner-presence, power and heat as unknown. Joining runs no benchmark.

Explicit `MYCE_QUEUE` plus `MYCE_TOKEN` joining continues to work as before,
including the existing HTTP transport. The pinned transport applies to the
invitation path; it does not convert existing model callback transports to TLS.
This is still a shared credential, without per-hypha revocation or approval.
Treat an invitation leak as a shared-credential incident; individual keys and
revocation remain work for #32.

## Capability profile v0

Each hypha records hardware facts at startup and sends them on check-in.
Opted-in benchmarks save `capability-profile.json` in the private worker spool;
default joins leave that cache untouched. `myce status` shows:

- OS, architecture and logical CPU count;
- accelerator type/name and reachable memory size;
- one unified memory pool for Apple silicon and DGX Spark, or separate system
  memory and accelerator memory for discrete GPUs;
- total and available bytes on the spool filesystem;
- unavailable facts, which remain unknown instead of becoming guessed budgets.

One machine worker reports all configured devices and the shared CPU/RAM
capacity. Fake resources remain explicitly fake. The queue
stores the profile with the normal SQLite machine state. These facts are distinct
from the live free-memory admission readings and operator caps.

The profile reports `hardware-facts-only; throughput pending` (the wording is
kept for the adjacent release). Facts refresh when the hypha starts again.
Joining runs no benchmark: placement uses declared resources, hardware profiles
and each kind of work's last accepted duration on each target (MY-49). The
onboarding benchmark, `myce benchmark` and reference curves are retired;
`--onboarding-benchmark`, `--curve-max-concurrency` and the installed
`"onboarding_benchmark"` setting are accepted and ignored so existing service
units keep starting.

Discovery, pinned authority and profile checks are covered by laptop fixtures.
Native cross-machine evidence and outstanding boundaries are owned by
[#48](https://github.com/1kuna/mycelium/issues/48); WSL2, installed-service,
login/reboot and native backend checks remain separate. See
[development](../development.md#native-acceptance-checklist).

## Invite and upgrade machines

An inviter can explicitly override the configured distribution with
`--release-manifest FILE --release-key PUBLIC_PEM --release-url HTTPS_DIRECTORY`.
For that local override, `--release-ca PUBLIC_CERTIFICATE_PEM` is also supported.
That verifies the signature locally with the independently provisioned key.
The output embeds approved hashes instead of fetching and running a remote script.
The bootstrap resumes private hash-addressed `.partial` files, rejects redirect
responses and checks exact size/SHA-256 before running even `myce version`.
The version command then checks version, OS/arch, both protocols and Mac sensing
before install or ticket redemption. A corrupt full-size partial, a redirect body
or a checksum mismatch is discarded with a typed reason so a rerun can recover.
The ticket is supplied to the installer through stdin and is absent from its
argv/environment. The temporary private invitation is deleted once the installed
joining file is durable.

Each line binds one approved release. Repeat it after an interrupted download or
installation; print a line for a newer release to upgrade. An already-installed
hypha reuses its saved authority and does not spend a new ticket. A new host
cannot reuse a spent ticket. If redemption succeeds but the response is lost
before private authority is saved, issue a fresh ticket; replay stays refused.

See [joining](#automatic-joining) for the user flow and successful enrollment states.

An older invitation line cannot silently downgrade an installed binary. The target
compares the signed release version with the installed executable's version and
refuses an older version unless `--allow-downgrade` is explicitly appended.
An equal version with different bytes also requires that flag; `+build` metadata
counts as equal. Identical-byte same-version reruns remain idempotent.

Existing fleet installations may report `development` or another non-semver
version. To replace one, use a currently approved semver release's printed line
with `--allow-downgrade` appended explicitly. This overrides the unordered installed
version, while retaining checksum, protocol, saved-authority, custody and readiness
checks. It does not convert an incompatible legacy installation; use the explicit
conversion path where required.
Same-version reruns explain that installer flags are not reapplied; use the normal
`myce install worker --config CONFIG` path to change those settings.

Hardware, NAS, policy, state-root, manager-session and candidate service-environment
preflights finish before onboarding drains or stops the installed worker.
Upgrade preflight also refuses busy hosts before changing policy. If work races with
the drain, a wait timeout restores the exact self-made drain through CAS, with a
separate cleanup budget. A mode-0600 intent receipt is written before the drain
POST, carrying a unique owner ID that the queue stores atomically with the drain.
A rerun requires that queue-side owner to match before recording a resume receipt.
Resume checks both owner and timestamp in the queue transaction. Independent
maintenance edits revoke ownership, even at the same clock timestamp. Old receipts
or queues without ownership proof cannot authorize resume: maintenance stays in
place and the error names the recorded drain owner and prints the exact
`myce host resume HOST` command for the operator. A drained same-release rerun
prints that recovery information even if its local intent receipt was lost.

Installer failure restores the existing install transaction and resumes only the
self-made drain. If newly installed bytes never become ready, that same transaction
restores the previous binary/config/service and verifies its enrolled readiness
before resuming. The error states the
recovery outcome. Failed rollback or conditional resume reports why the host could
not be resumed and asks for a rerun. A process crash loses the in-memory install
snapshot: if already-installed bytes fail readiness on rerun, the command explicitly
reports that rollback is unavailable and preserves maintenance for recovery.
Drain waiting, installation and readiness have separate timeout budgets.
