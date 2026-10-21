# Automatic joining

One mycelium is the cluster; a hypha is a machine that contributes its devices,
CPU and RAM. An installed, authorized hypha prints an invitation with:

```sh
myce invite
```

Paste the single output line into a terminal on the new hypha. It selects Linux
amd64/arm64 or native macOS arm64, resumes interrupted downloads, checks the
approved release's SHA-256 before execution, redeems a short-lived ticket over
pinned TLS, and uses the existing worker installer. The new machine needs curl,
a SHA-256 tool and the OS shell utilities; it needs neither Go nor Git.
The fleet token never appears in that command, shell history or logs. The line
contains public release metadata, the queue certificate pin, and an enrollment-only
one-time ticket (15 minutes by default, `--expires` up to one hour).

`--out FILE` optionally saves the same line mode 0600 and still prints it.
Existing files are never overwritten. `MYCE_JOINING_FILE` or `--joining-file FILE`
selects an existing private invitation when the inviter is not using its default
installed config. A queue operator may use `--identity-dir queue.db.joining`
with its token supplied privately and `--queue HTTPS_ORIGIN`.
The queue must already advertise a reachable joining endpoint and approve a
signed release: see [release setup](guide-onboarding-releases.md).

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
System Settings > Privacy & Security > Local Network. If denied or prompted,
stop and have the user review that permission; Mycelium does not grant or reset
it. A successful foreground/SSH check does not establish launch-agent,
login/reboot or app-launch behavior.

WSL2 default NAT does not reliably carry mDNS. Use a saved/supplied address, or
mirrored networking with appropriate private-network and Hyper-V firewall rules.
Mycelium does not change WSL networking or firewall configuration. Windows host owner-presence and power signals use the
[host bridge](guide-wsl-host-bridge.md). Joining runs no throughput benchmark
unless explicitly requested; see the benchmark section below.

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

Without a benchmark, the profile reports `hardware-facts-only; throughput pending`.
Facts refresh when the hypha starts again. Joining does not run or wait for
throughput measurement by default; placement still uses declared resources and
hardware profiles with the existing fallback rules.

For throughput measurement before admission, pass `myce join --onboarding-benchmark`
(or add it to the join argv after `service ... --`). Installed worker configs can
set `"onboarding_benchmark": true`. For personal or polite hyphae, also pass
`--daily-use` and the appropriate `--laptop` or `--desktop` declaration. Operators
following an invitation can instead run `myce benchmark` explicitly with their
private queue credentials and prepared extensions. See the
[benchmark guide](guide-benchmark-v3.md) for its bounds and cache behavior.

## Verification

The normal suite includes fake discovery (no results, multiple candidates,
untrusted candidates and multicast failure), pinned transport, private file
handling, installer custody, profile/check-in/status and fake hardware fixtures
for Apple silicon, DGX Spark and discrete GPUs. Run:

```sh
go build ./...
go vet ./...
go test ./... -race
```

The CLI dev end-to-end tests exercise the real queue and command runner with
fake GPU inventory. A short isolated macOS check additionally exercised a queue
and joining hypha as separate processes, real mDNS with no supplied address,
actual Apple hardware facts, a short command without `MYCE_TOKEN`, and explicit
saved-origin joining. Native cross-machine results and remaining verification limits are recorded in
[#48](https://github.com/1kuna/mycelium/issues/48). WSL2 and installed-service
discovery require separate verification.
