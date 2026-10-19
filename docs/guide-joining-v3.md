# Automatic joining

A new hypha uses Bonjour/mDNS to find the mycelium. Discovery is only an
address finder: a private invitation supplies the queue's certificate pin and
joining credential before any connection is trusted. This is the provisional
invitation choice in [#32](https://github.com/1kuna/mycelium/issues/32).
It does not add a peer network, another queue or extension qualification.

## Make an invitation

On the queue, with its token already supplied privately through `MYCE_TOKEN`:

```sh
myce invite --identity-dir queue.db.joining --out invitation.json
```

`queue.db.joining` is the identity directory for `queue --db queue.db`.
`queue --identity-dir DIR` overrides it. The queue creates its identity when it
starts, and reuses it on restart. Preserve that directory with the queue's
private state; replacing it invalidates existing invitations.

The command writes a mode-0600 invitation and prints only:

```sh
myce install worker --joining-file 'invitation.json'
```

Transfer the binary and invitation privately to the new hypha. An invitation
contains the shared credential: keep it out of Git, messages, command arguments,
shell history and logs. The printed command references the file rather than
printing its contents. `invite` refuses to overwrite an existing file.
An existing hypha can invite another using its installed invitation:

```sh
myce invite --joining-file joining.json --out invitation.json
```

Joining uses the installer, including its GPU, service, software and
mounted-storage prerequisites. Add the usual `--nas` option when that mount is
not at the installer's default location. Automatic joining supplies the address
and trust; it does not mount storage or install GPU drivers. The installer
copies the invitation into its private installation directory, and the service
uses that copy on future starts. CLOSED host intent takes
precedence over joining. Foreground joining is also available:

```sh
myce join --joining-file invitation.json
myce status --joining-file invitation.json
```

Foreground join saves the authenticated address back into the private invitation.
`MYCE_JOINING_FILE` can select it for these commands. Separate commands with
independent parsers, such as `wait` and `model`, retain their existing explicit
URL/token configuration.

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

Each per-card worker reports the accelerator it owns, following the existing
one-card-per-worker contract. Fake resources remain explicitly fake. The queue
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
