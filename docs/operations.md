# Operate Mycelium

This is the current runbook: set up, install and upgrade, maintain, back up and
recover. Explanations, field semantics and tuning live in
[reference pages](reference/README.md); finished one-time migrations live in the
[migration archive](reference/migrations.md).

The automated install/onboarding route requires a verified NAS mount. Standalone
`myce dev` and file-store execution work without a NAS; see [start here](START-HERE.md)
and [files](files.md). No installer mounts storage, changes drivers or opens firewalls.
Agent skill/harness installation lives [beside the skill](../skills/mycelium/setup-project.md).

| Task | Procedure | Reference |
| --- | --- | --- |
| Set up a new fleet | [steps 0–7](#0-ask-the-user) | [onboarding](reference/onboarding.md), [NAS mounts](reference/nas-mounts.md) |
| Install, reinstall or upgrade one service | [install and upgrade](#install-and-upgrade) | [installer transactions](reference/install.md) |
| Replace pre-v3 state | [archive old state](#archive-old-state-and-install-fresh) | [migration archive](reference/migrations.md) |
| Build, sign and approve a release | [publish](#publish-and-approve-an-onboarding-release), [advance the default](#advance-the-default-after-a-signed-update) | [invite and upgrade machines](reference/onboarding.md#invite-and-upgrade-machines) |
| Upgrade the whole fleet | [rolling fleet upgrades](#rolling-fleet-upgrades), [preserve curves](#preserve-learned-curves-across-a-release) | [fleet upgrade internals and failure codes](reference/fleet-upgrades.md) |
| Drain, disable, exclude or budget a host or device | [queue maintenance](#queue-maintenance) | [maintenance semantics](reference/queue-maintenance.md), [host capacity](reference/host-capacity.md) |
| Qualify an extension or reserve hardware | [extension check](#check-an-installed-workers-extension), [reservations](#reserve-a-host-or-its-devices-for-an-existing-validation-job) | [reservation semantics](reference/queue-maintenance.md#validation-reservation-semantics) |
| Change project or host policy | [live project policy](#change-live-project-policy) | [scheduling](scheduling.md) |
| Back up, move or compact the queue | [backup and relocation](#queue-backup-restore-and-relocation), [history retention](#queue-history-retention) | [queue disk I/O](reference/queue-io.md) |
| Inspect health and recover | [monitoring](#monitoring-and-failures), [cleanup custody](#native-cleanup-custody), [other recovery](#other-recovery-procedures) | [status fields](status.md) |
| Files, caches and retention | — | [file store](reference/file-store.md), [worker caches](reference/worker-caches.md) |
| Models and measurement | — | [models](reference/models.md), [services](services.md) |
| Daily-use, shared and Windows/WSL2 hosts | — | [shared and polite hosts](reference/shared-and-polite-hosts.md) |
| Native sandbox and isolation | — | [native execution](reference/native-execution.md), [isolation limits](native-isolation.md) |

[Native qualification](development.md#native-acceptance-checklist) and
[isolation limits](native-isolation.md) distinguish readiness from enforcement.

You are a coding agent setting up Mycelium on a user's own computers. Follow
these steps in order. Commands are written for a POSIX shell. Text in CAPITALS
is a placeholder that you fill in. If a flag here is rejected, run
`myce COMMAND --help`, use the closest current option, and tell the user what
differed. Use the supported client version for these commands.

## Rules

- Run every `myce` command as the user's ordinary account. Never run `myce` with
  `sudo` or as root, because it refuses to start that way.
- The fleet token is a password for the whole cluster. Never print, echo, log,
  paste into chat or commit it, and never put it on a command line. Keep the
  files that hold it at mode `0600`.
- Run every `install` with `--dry-run` first, show the user the plan, then run
  it for real.
- Don't install drivers, mount drives, change firewalls or touch machines the
  user didn't name without asking first.
- Only the user's own trusted commands should run on the cluster. Plain native
  jobs retain the v1 unsandboxed default; new adapters/extensions default to the
  native sandbox. See [isolation](native-isolation.md) for exceptions and limits.

## 0. Ask the user

1. Which computer is the **hub**? Pick one that stays on, running Linux or an
   Apple Silicon Mac. It keeps the job list and can also run jobs.
2. Which other computers should join, and how can you reach each one (a terminal
   on it, SSH, or the user pasting commands)?
3. Where is the shared network drive (NAS) mounted on each machine?
4. Which machines does someone use every day, and which are laptops?

## Shared orchestration hosts

A machine running agents or interactive tools can also be a worker. Measure its
external-memory history and owner bursts, then use the [shared-host profile](reference/shared-and-polite-hosts.md#shared-interactive-hosts)
with positive CPU/RAM allowances. Keep ordinary fitting machines first; permit
light native CPU work inside the shared host's limits. Do not clear an existing
drain to make a setup appear ready. Hard CPU shares and pressure yielding require
the separate qualification described there.

## 1. Check prerequisites on every machine

- **OS:** Linux on x86-64 or ARM64, or macOS on Apple Silicon.
- **Network:** the same local network, with the hub reachable from the others.
- **Shared drive:** the same NAS share (NFS, SMB or autofs) mounted on every
  machine. The defaults are `/mnt/nas` on Linux and `/Volumes/NAS` on macOS;
  otherwise pass `--nas PATH`. Mycelium never mounts a drive or falls back to
  local disk, so a missing mount stops the install with `nas_not_configured`.
  The dry run does not check the mount. Run `ls NAS_PATH` yourself and confirm
  it shows the shared drive's contents before installing.
- **GPU drivers already working:** for Nvidia, `nvidia-smi` lists the card. For
  Intel GPUs on Linux, the Level Zero/oneAPI runtime is installed. Apple GPUs
  need nothing. CPU-only machines are fine.
- **Hub only:** Git and Go 1.25.6 or newer (`go version`) to build `myce`.

## 2. Install the hub

On the hub, from a checkout of this repository:

```sh
go build -o myce ./cmd/myce
```

Find the hub's local network address (HUB_IP, for example from `ip -4 addr` or
`ipconfig getifaddr en0`). Then create the private fleet credential without
displaying it:

```sh
mkdir -p ~/.config/myce && umask 077
printf 'MYCE_TOKEN=%s\nMYCE_QUEUE=http://HUB_IP:7331\n' "$(openssl rand -hex 32)" \
  > ~/.config/myce/fleet.env
chmod 600 ~/.config/myce/fleet.env
```

Install the hub service, which listens on the local network, so other machines can reach it:

```sh
./myce install queue --listen HUB_IP:7331 --queue http://HUB_IP:7331 --nas NAS_PATH --dry-run
./myce install queue --listen HUB_IP:7331 --queue http://HUB_IP:7331 --nas NAS_PATH
```

Then run `./myce doctor` to check prerequisites. On Linux, if the installer
reports `lingering_permission_required`, ask the user to run
`sudo loginctl enable-linger USER` once, then repeat the install.

## 3. Create the first invitation

Load the credential into the environment without printing it, then write a
private invitation file. This bootstraps the first worker without requiring a
signed release; later workers can use the one-time invitations in step 4:

```sh
set -a; . ~/.config/myce/fleet.env; set +a
./myce invite --identity-dir ~/.local/share/mycelium/queue/state/myce.db.joining \
  --private-file --out invitation.json
```

The identity directory is the hub database path (shown in the
`install queue` plan) plus `.joining`. `invitation.json` contains the fleet
token: keep it mode `0600`, move it only by private means such as `scp`, and
delete every copy once installs are done.

Install a worker on the hub with it, so the hub also runs jobs. Add
`--daily-use` if someone uses this computer, so jobs wait until they step away.

```sh
./myce install worker --joining-file invitation.json --nas NAS_PATH --dry-run
./myce install worker --joining-file invitation.json --nas NAS_PATH
```

The new worker finds the hub on the local network by itself. If it can't (for
example, across subnets), see [address and network behavior](reference/onboarding.md#address-and-network-behavior).

## 4. Add each other machine

**Option A: one pasted line.** On the hub, or any machine that has joined, run:

```sh
./myce invite
```

It prints one line. Run that exact line in a terminal on the new machine. It
downloads and verifies `myce`, installs the worker and waits until the hub sees
it. The line has a one-time ticket that expires after 15 minutes
(`--expires` allows up to 1h). It needs no Go or Git on the new machine, and it
never contains the fleet token. If it fails with `release_unavailable`, the hub
has no approved signed release. Either set one up with the user
([publish and approve a release](#publish-and-approve-an-onboarding-release)), or use option B.

**Option B: copy the invitation file.** This option needs no release. Copy
`invitation.json` and a `myce` binary for the new machine privately. Build the
binary on that machine, or cross-build it on the hub, for example
`GOOS=linux GOARCH=amd64 go build -o myce ./cmd/myce`. Build a Mac binary on a
Mac. On the new machine:

```sh
./myce install worker --joining-file invitation.json --nas NAS_PATH --dry-run
./myce install worker --joining-file invitation.json --nas NAS_PATH
rm invitation.json   # the installer keeps its own private copy
```

With either option, add `--daily-use` for computers someone uses, and
`--laptop` for laptops. With `--laptop`, the machine only works while plugged in.
with option A, append these flags to the end of the pasted line only
if `myce install worker --help` on that machine lists them.

## 5. Verify

On the hub, with the credential loaded as in step 3:

```sh
./myce status
```

Every machine should appear with its GPUs, CPU and memory. A machine reporting
`ready` takes work now. `polite_waiting` means it's waiting for its user to step
away, which is also a success. `disabled` or `drained` means someone paused it
(`myce host resume HOST` turns it back on). See [status fields](status.md).

## 6. Run a first job

```sh
./myce run -- echo hello
./myce run --gpu cuda --mem 4G -- nvidia-smi
```

The first command runs on any free machine and prints `hello`. The second needs
an Nvidia GPU. Use `--gpu xpu` for Intel or `--gpu metal` for Apple, with a
command that exists there. `run` waits and streams the output. Add `--detach` to
get a job ID instead, then use `myce wait ID` and `myce logs ID`.

Use a supported client with plain `myce run`; installed older clients need an
explicit upgrade through the normal installer. See [jobs](jobs.md) for submit YAML.

## 7. Report back to the user

List each machine, its hardware and its state from `myce status`, plus the
output of the test jobs. Say where the credential file lives, but not its
contents. Mention anything you skipped or had to change. For upkeep (pausing a
machine, upgrades, uninstalling), point them to [operations](operations.md).

## Install and upgrade

Build the single binary and inspect a plan before installation:

```sh
go build -o myce ./cmd/myce
./myce install queue --dry-run
./myce install worker --dry-run
```

For an existing current installation, select its exact saved config:

```sh
./myce install worker --config WORKER_CONFIG --dry-run
./myce install queue --config QUEUE_CONFIG --dry-run
```

Omitted flags preserve saved values, including false/zero/empty settings,
queue/NAS addresses, Docker opt-in, GPU runtime environment, catalogs and secret
references. Explicit flags overlay only their setting. Identity cannot be moved
over an existing installation. Fresh Linux installs use one `myce-worker.service` and spool per machine;
`--device` restricts its inventory. Split installations are below the [supported release floor](DECISIONS.md#my-42); archive their state and install a current machine worker.
Docker is optional (`--docker-host` opts in); ambient `DOCKER_HOST` is not adopted.

The queue saves its selected TLS joining bind as `discovery_listen` in the queue
install config on first start and reuses it across restarts and reinstalls.
Older configs select and save a port on their next start. Foreground queues save
the same setting in `listen.json` beside their durable joining identity (default
`DB.joining`). Keep that directory with the database when restarting or moving it.
To set or change the installed bind, use:

```sh
myce install queue --config QUEUE_CONFIG --discovery-listen 0.0.0.0:7443
```

An empty setting or port `0` selects and saves an automatic port. If a saved port
is busy, the queue warns, selects and saves another automatic port, and continues;
saved HTTPS addresses may then need repair. A runtime `myce queue
--discovery-listen ADDRESS` overrides the saved setting and saves the selected
bind; an explicit busy address fails instead of falling back. Queue services read
this setting from the config at startup so generated service arguments cannot
retain an old automatic port.

If an enrolled worker has a stale HTTPS queue address and discovery is unavailable,
repair it through the supported reinstall path:

```sh
myce install worker --config WORKER_CONFIG --queue https://queue.example:7443
```

This verifies the new endpoint against the installed joining authority's existing
certificate pin, then publishes the authority's saved `address` before the worker
config's `queue`. On its next start the service reads `--joining-file`, so an
interruption between those writes leaves it able to use the verified new address
even while the config still shows the old one. Re-run the same repair command to reconcile
the config and finish installation. This also repairs older interrupted installs
that wrote only the config. The pin and credentials are preserved exactly.
A pin mismatch refuses the repair without changing installed files; this
path cannot replace the authority or rotate its pin. Changing an address requires
the new queue endpoint to be reachable. Preview with `--dry-run` to inspect the
config change without verification or writes.

Drain/disable admission before installing an upgrade. Matching queue and workers
must use the current protocol. The installer updates only the selected service,
enables/starts it and restarts an existing Linux service. It installs no local
start condition or timer. Linux units retain GPU EnvironmentFile,
`Restart=on-failure`, `Delegate=memory cpu`, `KillMode=mixed` and bounded stop.
TERM goes only to the worker, which records and delivers attempt stops before
reporting infrastructure interruptions. Installed shutdown also stops
non-resumable CPU attempts; an interactive `join` still lets those finish.
After 45 seconds systemd retains a final group-wide KILL, including delegated
attempt cgroups. Payload failures observed before worker stop remain failures.
From the next release (#1081), installed TERM cancels GPU inventory, host
observations and pending queue I/O immediately. After local attempt cleanup is
durable, exit does not require a queue reply or a remote model plan; unacknowledged
results retain their exact attempt identity for startup replay. Interactive leave
continues to use the existing graceful queue handshake.
macOS uses the generated launchd service. Configuration and secret files stay
private. Never run the ordinary binary as root.

The installer's slot reconciliation (`fleet_slot_unapproved`), rollback snapshot
and service-manager stop/start timeouts are described in
[installer transaction details](reference/install.md).

For a signed-release deployment, installing the queue/worker bytes alone leaves
the default onboarding approval unchanged. After verifying the updated queue,
run `myce release approve --config QUEUE_CONFIG --release-manifest RELEASE_JSON
--release-key PUBLIC_PEM --release-url HTTPS_PUBLISHER_DIRECTORY` (plus
`--release-ca PUBLIC_CERTIFICATE_PEM` for a private publisher). This authenticated,
verified update persists the new default without changing catalogs or service
settings. Its public key must match the publisher key stored by queue-host startup;
API callers cannot provision or rotate trust. An existing queue without a stored
key refuses until its local command is restarted with the existing `--release-*`
flags (and usual catalog/service settings) to provision it.
Identical reruns are idempotent; older or changed equal-version API approvals
require `--allow-downgrade`. Do this before fresh invites or one-command onboarding;
see the [signed-release deployment step](#advance-the-default-after-a-signed-update).

Queue startup verifies retained `--release-*` flags, then logs and skips manifests
older than or equal to the persisted default. This preserves a newer API approval
across restarts while still provisioning the local publisher key. An explicit
startup `--allow-downgrade` applies the flag manifest instead; remove it after an
intentional downgrade so future restarts do not keep reverting the default.
Invalid signatures never qualify for skipping or forcing. Publisher-key rotation
remains a local queue-host restart with an independently provisioned key and a
manifest signed by it. Approval storage failures return HTTP 500
`release_approval_failed`; invalid input and downgrade refusals remain 4xx.

Saved configs accept the retired local-control fields during overlay and omit
them when rewritten; runtime options and secret references remain intact. Old state is never converted or
silently cleared. See [fresh install](#archive-old-state-and-install-fresh).

### Legacy EnvironmentFile workers

A compatible single-service worker with `environment_file` in its saved config
uses the ordinary in-place installer. The printed signed upgrade line identifies
this authority mode and gives the exact verified candidate/config command;
it does not require split-worker conversion or new credentials. The verified
candidate stays in the bootstrap cache. Use that candidate as `NEW_MYCE` below.

First inspect `myce host status HOST`. Preserve any existing operator drain or
disable; otherwise run `myce host drain HOST --wait` and record the drain you own.
Preview and install the same signed candidate using the exact saved config:

```sh
NEW_MYCE install worker --config WORKER_CONFIG --dry-run
NEW_MYCE install worker --config WORKER_CONFIG
```

The preview explicitly reports preservation of the EnvironmentFile credential
reference and lists any configuration changes. Omit authority flags to keep the
existing private file; never copy its token into output, argv or another config.
The installer preserves identity, service, spool/state, catalog/cache/runtime
settings and credential references, and restores the existing installation on
activation failure. Inspect preview changes before proceeding. This path does
not change queue maintenance or restart the queue. Verify the running release,
service health and fresh worker report afterward, then resume only the drain you
created. Existing operator maintenance remains in force. Platform mismatch is a
separate refusal requiring the correct signed OS/architecture artifact.

## Foreground and service control

`myce join` runs a worker against the configured queue. `myce service --config
CONFIG` uses the installed settings. Stop through the owning service manager or
foreground interrupt, preserving spool/custody. An unavailable queue cannot
authorize a new payload. `myce detect --config CONFIG` reports GPU inventory.

## Uninstall without deleting data

Drain or disable and verify exact release before an authorized uninstall. Inspect
`myce uninstall --config CONFIG --dry-run` first. The selected service is stopped
and disabled and role files move to recoverable Trash; preserve active spools,
checkpoints, archives and data needed by sibling workers. No fleet action is
implied by these instructions.

## Archive old state and install fresh

Old formats are refused; there is no converter. Every queue/worker/cache root
has `.myce-format` containing `v3` and a newline. Startup validates every root
before writing. Nonempty unmarked roots fail with `legacy_state_present`;
contents, checkpoints and unique evidence are preserved for operator handling.

Stop old services and prevent their restart before archiving whole data roots
outside active lookup paths. Inventory saved database, worker state and cache
paths, including custom roots. Preserve old configuration, credentials and GPU
environment. An old shared host-control directory and reconcile timer are retired
assets: preserve the directory. A worker installer upgrade stops, disables and
retires the obsolete user reconcile service/timer and `_host-start-check`
drop-ins, retaining backups and restoring their prior state if the upgrade fails. Do not start old binaries against new data or fabricate format markers.

Default data lives under `~/.local/share/mycelium/queue/state/` and
`~/.local/share/mycelium/worker[-INSTANCE]/state/`. Keep checkpoints and archives;
a fresh queue cannot adopt old attempts. Render configuration in the current
schema, retaining runtime options and secret references and removing retired
local-control settings. Run `myce install ROLE --config CONFIG --dry-run`, inspect
the plan, then perform the authorized install for matching queue and workers.

Queue maintenance controls admission from the first report. To stage a host,
set `myce host disable HOST` on the queue before starting its workers; the policy
persists across their first registration and rejoin. Configure its queue allowance
while disabled, inspect `myce host status HOST`, then explicitly resume. A new
host without prior policy is enabled by default. No local close/open operation,
root copy or timer is installed. Native activation and qualification remain a
separate operator task.

## Publish and approve an onboarding release

> **Release checklist: migrate preservation collectors before queue cutover.**
> With bounded status (#439), preservation and curve-provenance checks must use
> `/api/status?detail=full` or complete `jobs=all` pagination, including private
> release-script copies. Default status can omit old completed jobs even though
> curves remain present. Existing complete `/api/inspection` collectors remain
> supported. See [the upgrade requirement](#rolling-fleet-upgrades).

The queue administrator provisions a trusted publisher HTTPS directory and an
Ed25519 public key independently of the release manifest. The signing key stays
with the publisher in a mode-0600 PKCS8 PEM file. Never distribute it with a release.
The repo-owned fleet build path uses the committed `release-build` Mycelium
profile for Linux and the local signing Mac for Darwin. Commit the release
source first. On the native Apple Silicon Mac holding the provisioned Developer
ID Application identity, with Go, Python 3 and Xcode command-line tools:

```sh
python3 scripts/release-build.py fleet v3.1.0 "$(git rev-parse HEAD)" .reports/release-v3.1.0
```

Set `MYCE_DEVELOPER_ID` through the Mac's private environment before running it.
Never put the identity, a signing key or credentials in a profile or logs. The
command refuses a missing identity, dirty checkout, or source commit mismatch;
resolves the committed profile, submits the Linux job with literal version and
commit arguments, waits and downloads hash-verified results; then builds and
verifies Darwin locally and assembles `.reports/release-v3.1.0/release/`.
The installed Mycelium CLI supplies authority internally. For agent use, bind
`MYCE_NOTIFY` to the submitting thread as described in the consumer skill.
No profile generation or textual replacement is involved, so `VERSION` cannot
corrupt tool names such as `GOVERSION`. The command does not publish or deploy.
Use a fresh output directory for each attempt; on a failure, retain its job
receipt and artifacts and resume with the explicit steps below.

The profile uses the existing `mycelium-gate` managed Go/Python environment,
two CPU cores and automatic learned memory, with no host pin. Both Linux targets
are CGO-disabled. Darwin is CGO-enabled and must report native owner sensing.
The shared `myce release build-targets` matrix selects the artifacts.
The Darwin executable embeds the stable identifier, Local Network usage
description and Bonjour service declaration. A timestamped hardened-runtime
Developer ID signature is applied and verified before hashes are recorded.
Ad-hoc signatures fail the Developer ID requirement. Tests use fake tools and
never touch a real keychain or signing key.

The same committed steps can be run separately, including recovery after a
completed Linux job (use its saved `job_id`, without resubmitting):

```sh
# From the clean checkout at COMMIT; arguments append literally to the profile.
myce spec check --resolve .mycelium/profiles/release-build.yaml
myce run --profile release-build -- VERSION COMMIT
myce wait JOB_ID
myce result JOB_ID --out .reports/linux-result

# On the native signing Mac, at the same COMMIT and with MYCE_DEVELOPER_ID set:
python3 scripts/release-build.py darwin VERSION COMMIT .reports/darwin-build
python3 scripts/release-build.py assemble VERSION COMMIT .reports/linux-result/release-linux .reports/darwin-build .reports/assembled
```

Each build writes a JSON receipt with its version, commit, artifact sizes and
SHA-256 hashes. Assembly checks both receipts, each artifact's embedded Go build ID
(version and commit) and target/CGO metadata, the Darwin runtime version/protocol/sensing
and its Developer ID signature, then rechecks copied hashes. A mismatch refuses
assembly before manifest signing. Output includes `manifest.unsigned.json`,
`build-provenance.json`, `build-provenance.sha256`, `SHA256SUMS` and the three
executables. The receipt records the checked Darwin runtime/protocol/native
sensing and Developer ID verification, binding them to the artifact hashes,
source commit and unsigned manifest bytes. Retain the receipt SHA-256 through an
independent trusted channel from this native assembly step. The unsigned
manifest is not an approved release.

If the Ed25519 publisher key is on this same Mac, append
`--signing-key PRIVATE_SIGNING_KEY` to `fleet` or `assemble` to generate the
signed `release.json` through the existing `myce release manifest` command.
Otherwise transfer the verified assembled directory unchanged to the provisioned
key host, including Linux, and use a host-native `myce` with these options.
The signing-only publisher needs no Go toolchain:

```sh
myce release manifest --dir ASSEMBLED_DIR --version VERSION --commit COMMIT \
  --assembly-receipt ASSEMBLED_DIR/build-provenance.json \
  --assembly-sha256 TRUSTED_ASSEMBLY_RECEIPT_SHA256 --signing-key PRIVATE_SIGNING_KEY
```

Use the receipt digest retained independently from native assembly; computing it
only from the transported receipt (or trusting its adjacent checksum file) does
not authenticate the recorded native checks. The publisher rechecks manifest,
artifact and build-receipt hashes, source/version and target/CGO metadata without
executing any candidate artifact. Exact build-ID version/commit proof comes from
the authenticated native assembly receipt, whose artifact hashes must match.
Native Darwin qualification still
happens on the Mac during assembly. Missing proof or a mismatch refuses signing.
The signer parses the newline-terminated unsigned manifest and signs exactly the
serialized JSON value it embeds in `release.json`; never sign the unsigned file
with an external wrapper.

Before immutable publication, use the host-native parser with the independently
provisioned public key and publisher HTTPS origin:

```sh
myce release approve --verify-only --release-manifest ASSEMBLED_DIR/release.json \
  --release-key PUBLIC_PEM --release-url HTTPS_PUBLISHER_DIRECTORY
```

Add `--release-ca PUBLIC_CA_PEM` when the publisher uses a privately provisioned
CA. Verify-only checks signature, manifest matrix/protocols and public trust
inputs through the ordinary approval validator, then exits without loading queue
authority, contacting the queue or writing approval, invitation or service state.
It does not fetch publisher artifacts, test HTTPS reachability or compare against
the queue's existing approval/trust anchor; ordinary approval still enforces
queue-side trust and upgrade policy.

Never copy a key to a build worker for convenience. The existing
`scripts/release.sh VERSION OUTPUT_DIR PRIVATE_SIGNING_KEY` remains a local
all-target legacy path; fleet release workers use the committed profile path
above. Versions must be full v-prefixed semver values and normally increase;
`+build` metadata does not change upgrade precedence.
Publish the assembled artifacts and signed `release.json` unchanged at an HTTPS
directory that serves them directly without redirects. Mac distribution signing,
Gatekeeper/notarization and actual launchd login/network behavior need separate
native acceptance; laptop tests do not establish those properties.

Add these options to the existing queue command with its usual private credential,
database, checkpoint and joining-listener configuration:

```sh
myce queue --release-manifest RELEASE_JSON --release-key PUBLIC_PEM \
  --release-url HTTPS_PUBLISHER_DIRECTORY
```

The queue refuses invalid signatures, incomplete target matrices, incompatible
protocols or a Mac artifact lacking native sensing. It persists both approval and
the locally provisioned publisher public key in SQLite. Only queue-host startup
can provision or rotate that key; fleet-token API callers cannot establish trust.
On restart, a verified startup manifest older than or equal to the stored approval
is skipped and logged, preserving a default advanced through `release approve`.
The local publisher key is still provisioned when the flag manifest is skipped,
so existing queues can acquire the stored trust without reverting their default.
Invalid startup manifests still fail verification, even when old enough to skip.
To deliberately apply an older or changed equal-version approval, append
`--allow-downgrade` to the queue command. Remove that override after the intended
change: leaving it with stale service flags explicitly reapplies their old release
on future restarts. Signature and manifest validation still apply. A pinned authenticated
`myce invite` fetches the approved public metadata; a queue without release
configuration returns `release_unavailable` and cannot mint a bootstrap command.
The publisher certificate trust is ordinary HTTPS. The LAN queue's full
certificate-DER pin is checked separately by the existing joining client;
it is not a curl SPKI `--pinnedpubkey` value.

For a private LAN HTTPS directory, the administrator may additionally pass
`--release-ca PUBLIC_CERTIFICATE_PEM` to the queue command. This explicitly
provisions the publisher's public CA certificate (or self-signed server
certificate); never pass its private key. The queue persists this certificate
with the approved release and supplies it to authenticated inviters. The inviter
must run a version supporting `--release-ca`. Its pinned queue connection
authenticates this trust material, which the bootstrap supplies to curl through
a private temporary `--cacert` file. TLS certificate validity and the publisher
URL's hostname/IP SAN are still verified; redirects remain refused. The temporary
certificate is removed before the executable runs and on download failure.
No system trust-store changes or TLS-verification bypass is used. Without this
option, ordinary HTTPS trust is unchanged. Existing joining certificates have no
hostname SAN and cannot serve as ordinary HTTPS publisher certificates; provision
a separate publisher certificate with the correct SAN. A static HTTPS directory
can serve the signed release without changing the queue's joining identity.

## Advance the default after a signed update

Installing signed runtime bytes on the queue or workers does **not** change the
queue's persisted default onboarding release. After publishing the artifacts and
installing/verifying the new queue runtime, advance that default as a deployment
step, before issuing fresh invites or using one-command onboarding:

```sh
myce release approve --config QUEUE_CONFIG \
  --release-manifest RELEASE_JSON --release-key PUBLIC_PEM \
  --release-url HTTPS_PUBLISHER_DIRECTORY
```

Run as the ordinary queue administrator. `--config` reads the existing saved
queue authority; no credential extraction or service reconfiguration is needed.
Installed hypha authority is also supported when `--config` is omitted. The queue
must already run a version supporting this command's approval endpoint.
Supply the independently provisioned publisher public key, never a key obtained
from the manifest. It must exactly match the key already persisted by the queue
host. The command verifies the signed manifest locally; the authenticated queue
verifies it again against its own stored key using the existing signature,
protocol, target-matrix and native-sensing checks before committing to SQLite.
The API cannot provision or rotate publisher keys, even with `--allow-downgrade`.
If the queue has no stored key (including an existing approval created by an older
version), the command refuses with `release_publisher_key_unavailable`. On the
queue host, restart the existing queue command with `--release-manifest`,
`--release-key` and `--release-url` as above to provision it. Retain the usual
catalog, database and service flags. Key rotation uses that same local procedure
with a valid manifest signed by the replacement key.
This approves the manifest; it does not download or install artifacts.

For a private publisher, add `--release-ca PUBLIC_CERTIFICATE_PEM` on each approval;
omitting it selects ordinary HTTPS trust. The command updates only the default
release, publisher URL and public CA. It preserves the loaded environment/model
catalogs, saved service settings, queue database path and joining identity, and
does not restart the queue or reinstall workers. Success prints the approved
version and publisher URL. Identical reruns are safe after a lost response;
older versions or changed metadata at equal version precedence require explicit
`--allow-downgrade`, which never bypasses signature or manifest validation.
Invalid approval input returns a 4xx code; queue storage/commit failures return
HTTP 500 `release_approval_failed` and leave the previous default and key intact.

After success, `GET /api/invites/release` over the authenticated queue connection
returns the new default. Run ordinary `myce invite` without release overrides for
fresh machines; one-command onboarding uses the same default. Previously printed
invitation lines remain bound to their original release.

To upgrade one machine with a printed invitation line, or to replace a
`development` build, see [invite and upgrade machines](reference/onboarding.md#invite-and-upgrade-machines).

## Rolling fleet upgrades

`myce fleet upgrade` is the one way a release reaches the fleet (MY-21, MY-35,
MY-49). It does what the per-release rollout scripts did, host by host: drain,
wait for running work, swap in the verified binary, restart, check health,
resume, advance. No SSH, and nothing changes on a host except Mycelium's own
binary, restarted under its existing service unit.

Tested fixes deploy without a separate task, bundled one release at a time; roll
back if the queue is not healthy. Critical updates may deploy during a production
consumer's run (MY-39); patch time is weighed and is being made quick, but it is
not a precondition. Ordinary updates wait during a successful run. Fixing a run
failing because of Mycelium is allowed and expected. Orchestrator application:
announce rollouts to consumer orchestrators. Keep the fleet on a healthy, tested
release before each scheduled production window. There is no pre-production
freeze, consumer canary deploy gate, or need to ask about deploy timing
(MY-33, amended by MY-39).

**Before.** Publish the signed release and approve it with `myce release approve`
([publish](#publish-and-approve-an-onboarding-release)). Save the queue's
complete history for the after-check: `myce status --history --json > before.json`.

**Run.**

```sh
myce fleet upgrade                      # the approved release, normal urgency
myce fleet upgrade --release vX.Y.Z     # name the approved release explicitly
myce fleet upgrade --urgency critical   # MY-39: stop running work at a checkpoint
```

The command returns the operation and exits; the queue runs it. `myce status`
shows each host's `phase`, `wait` and `failure`. For each host in turn the queue:

1. **Picks the host.** Hosts idle now go first, hosts running production after,
   and the queue's own host last. A normal update leaves a host enabled while
   draining it would strand open production work: a
   [production lease](#production-leases) or a queued production stage,
   including one still waiting on `after:` (#1387). Its wait names that work;
   the host goes once the work starts, ends, or the lease closes or expires.
2. **Drains it**, with the operation as drain owner. A host an operator already
   drained or disabled keeps the operator's policy: it is updated, never resumed.
   While the operation's drain holds, a production job whose only eligible hosts
   it drained waits for them instead of failing.
3. **Waits for its running work**, model calls and cleanup to finish on their
   own, with no deadline. With `--urgency critical` it stops that work at once
   through the planned-maintenance stop instead: resumable work checkpoints
   within the stop grace and is requeued without a retry charge, and each
   host's `stop_requests` lists what was stopped (attempt, whether resumable,
   accepted progress). Exact release of every attempt is still required; an
   elapsed grace never proves it. On the queue's host, new model calls wait
   while it is updated, because the queue restart ends every model stream.
4. **Updates each Mycelium service on the host**, the worker first and then, on
   the queue's host, the queue. The service fetches the release through the
   queue's authenticated endpoint and checks it against its locally pinned
   publisher key, the signed size and SHA-256, the reported version and
   protocols, and on macOS its code-signing identity. It asks the queue once
   more, replaces the installed binary atomically and restarts its existing
   service. Settings, units and state are untouched.
5. **Checks health**: a fresh runtime proof from a new process running the
   release's exact bytes, with a healthy service manager and runner smoke,
   stable for 30 seconds.
6. **Resumes the drain it took** and moves to the next host.

Services already on the release are not drained or restarted. A drained or
disabled host without a usable runtime proof is `skipped` and named; rerun once
it is back. Rerunning while the operation runs returns it unchanged; a different
release or urgency is refused until it finishes or stops.

**When a host is unhealthy**, the operation stops (`state: stopped`), that host
keeps its drain, and later hosts are untouched. The host's `failure` says why:
a code from the service's updater, a new process running other bytes
(`worker_running_hash_mismatch`, `queue_running_hash_mismatch`), no healthy new
process within 15 minutes (`worker_update_not_verified`,
`queue_update_not_verified`), or a maintenance change by someone else
(`maintenance_changed`, keeping their policy). Look the code up in the
[failure code table](reference/fleet-upgrades.md#failure-codes-and-repairs).
Nothing rolls back. Fix forward, then rerun `myce fleet upgrade` (with
`--release NEWER` once a fixed release is approved). A rerun is a new operation:
it takes over the drains the stopped operation left, verifies those hosts and
resumes them, and skips services already on the release. If the queue itself is
unhealthy, roll it back as in
[queue-only rollback](#queue-only-rollback-to-the-previous-release).

**After.** `myce status` shows the operation `complete`, every host `complete`
or a named `skipped`. Check `myce status --history --json` against `before.json`:
every earlier job ID is present, running attempts kept their attempt number, and
the priority policy is unchanged. Confirm `myce status` lists each worker's
release and that the default onboarding approval is the new release
([advance the default](#advance-the-default-after-a-signed-update)).

**First use.** Each service must run as an installed service on a release with
this path. `myce fleet upgrade` refuses and names every service without a fresh
runtime proof. To bring a service onto such a release, do the same steps by
hand: `myce host drain HOST --wait` (it returns once the host is quiescent),
run the verified new binary's `myce install ROLE --config CONFIG`,
check `myce status` shows the new release and a new session, then
`myce host resume HOST`, one host at a time with the queue's host last.

Queue shutdown fences new requests, cancels open HTTP observations and long
polls, and lets an already committing SQLite transaction finish; batch attempts
on other hosts reconnect unchanged. The journal reports `event=queue_shutdown`
phases `fenced`, `api_draining`, `api_closed`, `storage_wait` and
`storage_closed`. The ten-second HTTP stop budget closes sockets on expiry; it
does not prove storage closure.

How hosts are chosen, how services verify a release, and every failure code are
in [fleet upgrade internals](reference/fleet-upgrades.md).

## Production leases

Queued production jobs are protected without a lease, including stages
submitted with `after:` that still wait on their upstream (#1374); that
protection ends when the stage runs, is cancelled or its upstream fails. A
project opens a lease only for production stages it cannot submit ahead, so
maintenance never breaks an in-flight pipeline (#1009):

```sh
myce fleet lease open --project P --key RUN --for 4h --platform cuda --memory-gb 24
myce fleet lease close --project P --key RUN
myce fleet lease list
```

Python: `client.open_production_lease(project, key, "4h", stages)`,
`client.close_production_lease(project, key)`, `client.production_leases()`.
HTTP: `POST /api/production-leases` (`project`, `key`, `ttl`, optional `stages`),
`DELETE /api/production-leases/PROJECT/KEY`, `GET /api/production-leases`.

Each stage carries the placement fields of a job (`needs`, `allowed_machines`,
`excluded_machines`); without stages the lease covers any host the project may
use. Reopening the same key renews it with a new expiry. The lease always ends
on its own; the project picks the duration. It grants no priority and reserves
no capacity. While it is open:

- `myce host drain` of an enabled host refuses with HTTP 409
  `production_work_open` when that host is the last enabled one able to start a
  lease stage or a queued production job (a stage waiting on `after:`
  included). `myce host status HOST` shows the
  same reason as `production_blocker`. `myce host disable` remains the explicit
  out-of-service action and is not refused.
- `myce fleet upgrade` leaves that host pending with admission open, as above.

## Preserve learned curves across a release

Use the versioned `scripts/verify-curve-preservation.sh` guard (or
`myce release verify-curves`) instead of private release-script equality checks:

```sh
MYCE_BIN=/path/to/myce scripts/verify-curve-preservation.sh \
  --before before-status.json --after after-status.json \
  --ledgers learning-ledger-pages.json > curve-diff.json 2> curve-guard.stderr
rc=$? # retain this status, stderr and the diff receipt, including on failure
```

Capture authenticated **`/api/status?detail=full`** snapshots before and after
cutover, using the canonical helper's read-only capture mode:

```sh
MYCE_BIN=/path/to/myce scripts/verify-curve-preservation.sh capture \
  --config CONFIG > before-status.json 2> before-capture.stderr
# Perform the separately authorized cutover, then capture the after snapshot:
MYCE_BIN=/path/to/myce scripts/verify-curve-preservation.sh capture \
  --config CONFIG > after-status.json 2> after-capture.stderr
# Require exit 0 for each capture before verifying.
```

The default `/api/status` view is bounded: it can omit older terminal jobs and
work curves. Never use that view to collect either side of preservation proof.
Capture requests full detail for **both jobs and curves**, and preserves the
server completeness metadata. Capture and verification reject bounded or
incomplete job pages and missing jobs/curves fields; a queue predating page
metadata can still return its original full snapshot. Collect accepted-learning
ledger pages from the complete job inventory, not the default recent-job list.
The **before response's server `now`** is the learning boundary, never the local
save time or installer launch. The ledger file is a JSON array of pages from
`GET /api/jobs/{id}/ledger`; collect pages covering the interval for jobs that
could have learned, including pagination. Use the after snapshot's `now` as the
upper boundary; later ledger entries are ignored.

The guard preserves every key, sample count and throughput/memory evidence
clock. It ignores only derived `stale_reason` labels. Changed values and new
keys require a complete chain of accepted sample records, with exact job,
attempt, key and queue-time provenance. Each record binds before/after digests
of the actual learning mutation in the same SQLite transaction as completion;
the guard shares the runtime's measurement digest and does not reimplement
EWMA or memory merges. Memory-only samples may advance their sample count and
memory clock while preserving the old throughput and its provenance.

A failed OOM or ceiling-bound auto attempt invalidates comparable memory on
every host's key and may keep its constrained peak there as a diagnostic. Each
key it rewrites gets a `memory_invalidated` record on that failing job's ledger,
binding the same before/after digests. The guard accepts it as a link that adds
no sample and may reset the memory clock, so collect ledger pages for failed
jobs too.

Removed keys, resets, backward clocks, changed measurements without new
samples or invalidation records, missing/ambiguous provenance and unexplained
final values fail closed. Older jobs have no such learning ledger proof: an
unchanged curve still passes, but an unproven change stops the release for
investigation. Reference sweep changes, and invalidations recorded before this
ledger record existed, also stop this guard.
Ledger records follow existing job history retention; collect evidence before
it expires. No release, install or fleet operation is performed by the guard.

## Queue maintenance

All commands use the authenticated queue and run as an ordinary account.
On an enrolled machine they read installed authority and keep its certificate
pin, like `myce status` and `myce cancel`, so no operator token handling is needed.
For owner writes, use the queue-host `--config <installed-queue-config>` route in
[live project policy](#change-live-project-policy). Never source env files or set `MYCE_TOKEN` by hand. Explicit
`--queue`/`--token`, `MYCE_QUEUE`/`MYCE_TOKEN`, `--config` and `--joining-file`
still select the endpoint or authority.
Use the physical host ID shared by all device workers:

```sh
myce host drain HOST --wait --timeout 30s
myce host disable HOST --stop --wait --timeout 30s
myce host status HOST
myce host budget HOST --memory-budget-gb GiB
myce host placement HOST --last-resort=true
myce host placement HOST --last-resort=false
myce host role HOST --role node|services|interactive
myce host resume HOST
```

The role (MY-45) selects how host RAM capacity is measured; see
[host roles](reference/host-capacity.md#host-ram-admission-and-ceilings). It
defaults to `node`, applies while the host stays enabled, and keeps any drain.

### Exclude components with a selector

```sh
myce exclude add 'host=compute-a;devices=0' --reason 'driver investigation'
myce exclude add 'host=compute-a;devices=0,1' --reason 'reserve two cards'
myce exclude add 'host=compute-a;kind=gpu' --reason 'keep CPU capacity available'
myce exclude add 'platform=xpu' --reason 'platform maintenance'
myce exclude add 'platform=cuda' --reason 'reserve CUDA fleet capacity'
myce exclude add 'kind=gpu' --reason 'reserve all accelerators'
myce exclude add 'host=compute-a' --reason 'host maintenance'
myce exclude list
myce exclude remove 'platform=xpu'
```

Quote selectors containing `;`, and remove an exclusion with the durable
selector that `exclude add` or `exclude list` prints. Selector forms, how
exclusions combine with drains and budgets, attribution and the API are in
[exclusion semantics](reference/queue-maintenance.md#exclusion-semantics).

### Set aside one accelerator

```sh
myce host device disable HOST DEVICE --reason "recurring driver faults"
myce host device enable HOST DEVICE
myce host status HOST
myce status
```

`DEVICE` is the displayed inventory ID or its stable `identity` from
`myce host status HOST`; prefer the identity when ordinals may have changed.
Disabling a device preserves started work and needs no worker restart; enabling
it does not resume a drained host. `myce host placement HOST --last-resort=true`
ranks the host after ordinary hosts. See
[device maintenance, placement and drain semantics](reference/queue-maintenance.md#device-maintenance-placement-and-drain-semantics),
including what `--wait` requires and how drains treat preparations.

## Check an installed worker's extension

Run locally on the enrolled worker with its queue running:

```sh
myce extension check --gpu xpu --device 0 --digest SHA256 example@1
```

Runtime checks read authority (including the certificate pin), `host_id`,
`state_dir/extensions` and `environment_cache` from the installed worker config.
Use `--config FILE` for a custom installation. Explicit `--host`, `--root` and
`--environment-cache` override the local defaults; authority flags work as for
other installed commands. `--gpu` and, for GPUs, `--device` remain explicit:
they select what to qualify. For CPU checks use `--gpu cpu` without a device.
A different `--root` warns that the live worker is not qualified by that check.

Without installed worker settings, supply host/cache explicitly or use
`myce dev` for automatic local defaults. A successful check is evidence only
for the selected extension digest, target and hardware/driver fingerprint.
See [extension authoring](adapter.md#install-and-qualify) and the
[check guidance](adapter.md) for preparation and replay criteria.

Add `--reserve` to withhold unrelated new work on the selected physical device
while the check drains, runs and cleans up:

```sh
myce extension check --reserve --owner validation-agent --reason 'driver qualification' --gpu xpu --device 0 example@1
```

The owner defaults to the local OS username; the reason defaults to `extension
qualification`. Each work-type check acquires its own reservation in the same
transaction as its keyed job submission. A CPU check reserves the whole host.
Existing Started work finishes first. Reservations do not promote checks above
production, grant qualification, bypass independent maintenance or cancel work.
The queue owns release even if the CLI disconnects. Inspect `myce reserve list`
after a caller restart; do not clear the hold merely because the caller exited.
`--max-lifetime` defaults to `2h` and accepts at most `24h`; see [expiry](reference/queue-maintenance.md#validation-reservation-semantics).

### Reserve a host or its devices for an existing validation job

```sh
myce reserve add 'host=compute-a;devices=0' --id driver-check --job JOB_ID --owner validation-agent --reason 'driver qualification' --max-lifetime 2h
myce reserve add 'host=compute-b' --whole-host --id host-check --job OTHER_JOB_ID --owner validation-agent --reason 'host validation requires host-wide isolation'
myce reserve list
myce reserve release driver-check --if-revision 1 --reason 'validation abandoned; inspect retained execution custody separately'
```

`reserve add` binds one existing single batch job/check before enabling admission
policy. For a GPU extension check, the CLI narrows a host-only selector to the
check's explicitly selected device. An unselected check requires an explicit
device selector. Use `--whole-host` with a host-only selector only when the check
needs host-wide isolation, and explain why in `--reason`. CPU checks and ordinary
validation jobs retain their authored scope. This CLI default does not edit
existing reservations or change the API's explicit selector contract.
It resolves the same stable physical identities as `exclude`, accepting
one host, its explicit devices, `host=HOST;kind=gpu`, or
`host=HOST;platform=PLATFORM`. The bound job must fit a selected target and is
then restricted to that target. Unknown jobs, dependencies/fan-outs, incompatible
targets, overlapping active reservations, and another reservation for the same
job are rejected. Already queued production takeover demand on that host must
settle before acquiring a competing reservation, avoiding a self-blocking hold.
The reservation admits only the bound job through its own fence; all ordinary
priority, memory, isolation, qualification, maintenance and cleanup gates remain.
Production work already running is never displaced by a validation reservation.

Binding rules, release conditions, `expired` versus released, and the
reservation API are in
[validation reservation semantics](reference/queue-maintenance.md#validation-reservation-semantics).

## Change live project policy

`queue --project-config` (and `dev --project-config`) bootstraps project settings
only before the first policy is stored. After first boot, the persisted SQLite
policy is authoritative. Editing the config and restarting or reinstalling the
queue no longer changes live policy or advances its revision.

`myce policy get` resolves installed worker authority, then queue authority.
From the release containing #1079, policy and model writes use the same
installed-authority resolver and preserve its certificate pin, including when
legacy endpoint/credential environment variables are present.

The supported owner-write route is on the queue host, using the installed queue
configuration. Run as the account allowed to read that configuration and its
credential references. Export the current policy, edit its `projects` mapping,
and import it using the revision printed by the export:

```sh
myce policy get --yaml --config <installed-queue-config> > policy.yaml
# Edit policy.yaml, then replace N with the exported revision.
myce policy set policy.yaml --if-revision N --config <installed-queue-config>
```

The CLI reads the credential reference internally; the operator handles no
credentials. The configured queue endpoint is retained (including a queue's
installed HTTP endpoint); an installed joining authority retains its HTTPS pin.
An HTTPS owner write without provisioned trust refuses with
`owner_authority_unpinned`; missing or unreadable authority refuses with
`owner_authority_unavailable`. Ask the installation owner to restore authority.
Never source env files or set `MYCE_TOKEN` by hand, and never extract a joining
token into environment variables. These are not supported operator workarounds.

Other owner-scoped writes use the same `--config <installed-queue-config>` route,
for example `myce model sync MODEL --to all --config <installed-queue-config>`
and `myce host drain HOST --config <installed-queue-config>`. Enrollment still
uses provisional shared authority; this CLI route does not introduce separate
server-side roles or grant authority to an unenrolled machine.

The same file carries the optional `models` mapping (permitted repository
patterns, per-snapshot `max_bytes` and `free_floor_bytes`) for
[declared model snapshots](files.md#declared-model-snapshots-553). Keep the
deployment's values there, not in Git.

Learned memory (#585) is on by default: a job with no memory figure is sized
from accepted peaks, with a bounded first run. To turn it off, add
`learned_memory: false` at the top level of the exported file and import it.
Omitted figures are then rejected and cold `memory: auto` runs explore
host-wide, as before. Set `learned_memory: true` to turn it back on. A file
without the field keeps the saved setting.

If the revision is stale, export again and reconcile the newer policy before
retrying. See [owner priority policy](scheduling.md) for the full
import/export contract, including replacing or clearing project settings.

Host production holds use the same export and import. The export's `hosts`
mapping is the live host policy. For example, to move one host from the
exclusive host-wide hold to the device-scoped hold
([hold scope](jobs.md#hold-scope)), add one line under that host and import:

```yaml
hosts:
  example-host:
    production_hold: true
    production_hold_scope: device
```

Set `production_hold_scope: host`, or delete the line, to restore the
exclusive hold. Change one host at a time. The change takes effect on the next
planning decision. Work that a host-scope hold already stopped still finishes
stopping, and its exact release is still required. `queue --host-config` only
seeds host policy. A restart with the last accepted startup file keeps the
live policy; a different startup file is refused, so edit host policy with
`myce policy set`, not the file.

## Queue history retention

Maintenance retires terminal job rows after 168 hours (or longer configured
result/log retention), once all attempts are released, files have expired and log cleanup is
complete. Pending file cleanup transfers to durable per-hash ownership. Each tick removes at most 64 jobs. Keyed replay answers remain for
30 days after removal, followed by permanent expired-key digests. Learned
work-curve, memory and storage observations stay until every one of their
timestamps is older than 30 days, the age past which no reader uses them
(#597); each tick then deletes at most 64 of each. Points forbidden after an
OOM and constrained memory peaks stay at any age, because sweeps and first-run
bounds still read them. Expired jobs disappear from listing and ID lookup. See the
[retention contract](design-proposal.md#submission-identity-and-cancellation-80).

Deleted SQLite pages are reused. Retention bounds full history by age, not total
bytes: permanent key digests and unresolved custody still accumulate. To reclaim
physical disk space, stop the queue through normal maintenance, take a verified
local backup, and compact the stopped local database using SQLite `VACUUM`.
Never open a NAS copy with SQLite or compact the running queue. Check free space
first: compaction needs room for a second database. Preserve the verified backup
until the compacted database passes `PRAGMA integrity_check` and reopens normally.

## Queue backup, restore and relocation

Mycelium has one SQLite writer. Queue loss stops submissions, new starts,
acknowledged cancellation and queue-mediated routing. Running commands may
continue, subject to their inputs and external dependencies. Moving the queue
changes its failure domain; it does not provide automatic failover or replicated
availability. Keep the database on a local filesystem, not a network share.

Use the installer, service manager, a verified stopped-state backup and SQLite
integrity checking. There is no `myce backup` or `myce restore` command.
Rehearse on an isolated queue fixture before an authorized cutover. Measure disk
headroom, transfer and verification time, and restore/rejoin time for the actual
roots; derive downtime and recovery expectations from that rehearsal.

### Queue-only rollback to the previous release

Under [MY-42](DECISIONS.md#my-42) only the previous release can read the
current queue database. The current release and r6.10 both persist
`myce-normalized-state-v3`, so rolling the queue back needs no conversion:

1. Stop the queue service and prevent its supervisor from restarting it. Verify
   the queue process is gone. Preserve the complete stopped generation described
   below. Keep worker spools, joining identity, file store and configured roots
   in place; workers can reconnect with their existing sessions and custody.
2. Start the exact previous queue binary with the preserved config and roots,
   using the existing installation's release-replacement procedure. Verify the
   expected job IDs, pending custody and worker reconnection before resuming
   submissions. This is an operator-directed queue rollback; it does not
   downgrade or restart workers. Normal restart grace and attempt fencing apply.

Starting the current release again resumes on the same database. The adjacent
release test (`scripts/upgrade-compat.py`) pins the previous release's source and
exercises both directions. Releases older than the previous one are refused at
startup with `unsupported queue persistence format`; their converters, including
the r6.9-to-r6.8 `myce queue downgrade`, live only in source history (r6.10,
`302620cacaace8a16e7ed424e1183dbb10f46414`). Never restore only an old DB while
leaving a mismatched generation of referenced files.

### Preserve one complete generation

Inventory the active configuration and every referenced root. Preserve privately:

- SQLite DB and any WAL/SHM companions, including job/key/attempt/custody history,
  learned curves, live policy, maintenance and release approvals. Paused jobs,
  their reason/expiry/revision and waiting qualification jobs are live state;
  preserve them with their original submission keys, inputs and queue age.
- Queue file store (`DB.files` by default), implicit `DB.uploads`, any configured
  upload spool, retained logs and other referenced roots. `QueueStateRoots` in
  `internal/myce/queue_roots.go` enumerates startup roots.
- Joining identity (`DB.joining` by default), certificate/private key, enrollment
  authority and existing credential. An identity change invalidates existing pins.
- Saved config, catalogs, policy/bootstrap files, publisher trust, credential
  references and service metadata. Record effective limits, retention and timeouts;
  persisted live policy must not be replaced with stale bootstrap YAML.

Shared checkpoints/models stay on their existing storage with relative
assignments. Configure destination-local mount roots and preserve worker spools
and identities in place. Across operating systems, preserve configuration meaning
and permissions; generate native service metadata rather than copying units.

### Consistent copy and restore

1. Pause producers and drain workers to exact settlement, including preparation,
   engines, pending releases, buffered results and uploads. Record maintenance
   created by this operation so unrelated drains/disables remain authoritative.
2. A pre-copy of large immutable payloads with rsync can reduce downtime. An
   online filesystem copy of the DB is not a SQLite-consistent backup. Python's
   backup sleep argument is not a bandwidth limit; see [I/O controls](reference/queue-io.md#standard-systemd-policy).
3. Record config/root hashes and authenticated history, policy/revision, curve
   and authority baselines. Stop and disable the source queue service; verify its
   process and listeners are gone and it cannot auto-restart. Never serve two
   queues with the same identity. If source fencing is uncertain, do not activate
   the destination.
4. Copy the stopped DB and companions together with the complete root set. Final
   sync must reconcile removals in an owned destination mirror, so pre-copy cannot
   resurrect reclaimed uploads. Keep an independent backup outside that mirror;
   never apply a destructive sync to the backup. Compare SHA-256 manifests,
   including non-default roots, and verify permissions/modes separately.
5. Run SQLite `PRAGMA integrity_check` on the local restored DB before activation.
   Do not vacuum as part of relocation. Restore all state before starting a fresh
   installation; never expose an empty DB or newly generated identity. Use the
   approved native release and the installer's dry-run with every preserved
   non-default setting. Confirm that the release can express custom roots and
   trust settings; resolve any gap before activation.

A SQLite online backup alone does not freeze referenced files or authority. A
complete online recovery scheme needs a consistent referenced-file snapshot too;
plain rsync of a live DB is insufficient.

### Preserve endpoint authority and verify cutover

Inventory direct endpoint configurations, environment overrides, explicit client
URLs and pinned joining files. Queue HTTP origin and joining TLS origin are
distinct; `--queue` is not a TLS listener flag. Preserve the certificate identity:
the pin is SHA-256 of certificate DER, not an SPKI pin. Discovery supplies
candidate addresses; the existing pin and credential authenticate them.

Direct-endpoint workers can use the supported saved-config overlay:

```sh
myce install worker --config WORKER_CONFIG --queue NEW_QUEUE_ORIGIN --dry-run
myce install worker --config WORKER_CONFIG --queue NEW_QUEUE_ORIGIN
```

For pinned workers, changing saved `queue` alone is insufficient: their service
uses `--joining-file`. On restart, discovery can locate the preserved identity
and refresh the authenticated address in that file. Rehearse with discovery
unavailable too: export a mode-0600 private invitation from the preserved identity
using `myce invite --private-file --identity-dir IDENTITY_DIR --queue NEW_TLS_ORIGIN
--out NEW_AUTHORITY`, transfer it privately to a persistent worker-local path,
and verify before installation:

```sh
myce status --joining-file NEW_AUTHORITY --queue NEW_TLS_ORIGIN
myce install worker --config WORKER_CONFIG --joining-file NEW_AUTHORITY --queue NEW_TLS_ORIGIN --dry-run
myce install worker --config WORKER_CONFIG --joining-file NEW_AUTHORITY --queue NEW_TLS_ORIGIN
```

Keep the original authority until recovery is verified. Refresh saved CLI,
environment and consumer endpoints too; discovery does not retarget an already
running client on every check-in. Do not disable pin checking, hand-edit authority
JSON or reuse a spent bootstrap ticket to force migration.

Before reopening admission, verify the release, local/shared storage, listener,
TLS fingerprint and authenticated API. Compare original history/key samples,
curves, live policy/revision, release approvals and publisher trust. Account for
expected age-based retention. Verify each expected worker identity, fresh session,
devices, service health and exact cleanup custody, then release only maintenance
created for the move. Compare output count/quality and control-call latency under
matched workloads. Keep admission closed and repair forward if preservation fails.

Restart grace is not an endpoint migration or proof of release. Restoring an older
backup can lose accepted jobs, cancellations, keys, policy and custody; reconcile
exact attempts before resuming. Keep the verified recovery generation privately
under the recovery policy and remove only verified rebuildable staging. Keep the
source service disabled. A cold restore still requires positive source fencing;
backup age defines possible data loss, not a promise of seamless failover.

## Monitoring and failures

`myce status`, the HTML page and `GET /api/status` show queue projections with
redacted job specifications. Use [typed status](status.md) for holds,
eligibility and last-reported allowance. Inspect exact jobs/logs separately when
authorized. Status and logs require the privately configured queue credential.
Never put that token in URLs or copy private payloads into reports.

Worker scratch, failed-attempt folders and installer `.Trash` copies follow
[worker scratch retention](reference/worker-caches.md#worker-scratch-retention):
superseded released attempts are collected on the next sweep; recover a failed
or cancelled job's partial outputs from its current attempt's
`job-files/JOB/ATTEMPT` before the configured file-retention deadline (default
`72h` after the job finishes; set `--file-retention`, `0` keeps files until
release) or explicit release. The former fixed 24-hour scratch promise is retired; unknown/unreleased
data stays protected. For a slow queue, capture
[live queue profiles](reference/queue-maintenance.md#opt-in-live-queue-profiling)
and read the [queue timing log lines](reference/queue-maintenance.md#outages-and-control-timing).

## Native cleanup custody

If an installed worker retains a finished native attempt after a transient process
inspection failure, its custody can prevent upgrading to the cleanup retry fix.
During an authorized maintenance window, stop its existing worker service through
the owning service manager. As the ordinary worker account, run a staged new binary
without replacing the installed binary:

```sh
NEW_MYCE resolve-native --state-dir WORKER_SPOOL --attempt-id ATTEMPT_ID
```

Use the exact local execution identity from the attempt custody record, not the job
ID. The command exclusively locks the offline spool and requires a matching saved
finished result. It independently checks every recorded process group, PID,
native sidecar and cgroup, then checks open handles under the exact
`job-files/JOB/ATTEMPT` scratch folder using a descriptor inventory, including
unlinked files, from `lsof` (included on macOS; install it separately on Linux).
A live or reused PID, unknown identity, missing custody,
failed inspection, open handle or unavailable inspector retains custody. It never
signals processes or accepts an operator assertion. The default 30-second
inspection budget bounds a stuck inspector; expiry retains custody. Increase
`--timeout` for a busy account after inspecting the failure. An incomplete
inventory, including warnings about unavailable mounts, must be repaired before
retrying; it cannot serve as proof of release.

Success only clears the exact result's pending release locally, preserving its
failure outcome, scratch, process evidence and other attempts. It does not contact
the queue, change maintenance or rerun the payload. Restart the unchanged installed
worker and require fresh queue maintenance status confirming release before
rerunning the normal upgrade. Preserve existing drain/disable ownership throughout.
No manual spool or database edits are needed.

## Docker cleanup custody

Managed Docker batches use exact immutable attempt labels and a durable create/start
journal. Inventory every configured endpoint, including sidecars and late accepted
operations. Only Mycelium-owned labeled objects may be stopped. Native cgroup
release alone does not settle daemon custody. Paused/restarting containers are
live; an unavailable endpoint or unknown create/start disposition stays UNKNOWN
and charged. Preserve journals and stopped evidence containers.

Use `myce resolve-docker --state-dir WORKER_SPOOL --attempt-id ATTEMPT_ID`
(as the ordinary worker account) to reconcile exact offline
custody. This uses the same worker executor and inspection path; it does not
accept an operator assertion or launch work. Reconnect workers and require fresh queue maintenance status and exact release
acknowledgements afterward. See [managed Docker](jobs.md#managed-docker-batch-commands)
for supported command/network/sidecar forms. Unmanaged containers remain outside
this containment contract.

Stopped-container retention defaults to 24 hours **after the queue result is
acknowledged**, rather than after container exit. The normal collector covers
the current worker's spool and identity. Retired conversion tooling is available
only in source history for installations below the supported release floor.

Each managed attempt also retains a private `ATTEMPT_ID.docker-events.json`
in its worker spool, including with `logs: none`. A separate `docker events`
subscription retains the last 256 lifecycle events (container ID, daemon
timestamp, action, numeric signal and exit code), plus dropped/rejected counts
and stream start/end status. Labels, images, command output and daemon stderr
are excluded. The file survives stopped-container retention and is mode 0600.
Recovery preserves prior events and increments the stream count; subscription
interruptions and Docker's finite replay history can leave gaps. These are
diagnostic observations, not proof that Mycelium delivered a signal, and cannot
change attempt outcomes or custody. Capture failures do not stop jobs.

## Other recovery procedures

- **NAS mount lost or slow:** [worker mount recovery](reference/nas-mounts.md#worker-mount-recovery-linux-and-macos);
  Linux boot ordering and `mount-start-limit-hit`: [CIFS boot recovery](reference/nas-mounts.md#linux-cifs-boot-recovery).
- **Accelerator quarantined after driver faults:** inspect and clear with
  `myce device clear-quarantine` per [driver fault quarantine](reference/accelerator-quarantine.md).
- **Completion notifications stopped or a legacy observer is stuck:**
  [completion observer recovery](reference/completion-observer.md).
- **Fleet upgrade stopped:** [failure codes and repairs](reference/fleet-upgrades.md#failure-codes-and-repairs),
  then [rerun](#rolling-fleet-upgrades).
- **Queue check-ins slow under disk load:** [queue disk I/O](reference/queue-io.md).
- **Shared host under owner pressure:** [pressure response](reference/shared-and-polite-hosts.md#pressure-response).
- **File store full:** [file store reference](reference/file-store.md), including
  [early file release](reference/file-store.md#project-owner-early-file-release-465).

## Moved sections

Older links to these anchors land here; each entry points to the section's current home.

- <a id="opt-in-live-queue-profiling"></a>[Opt-in live queue profiling](reference/queue-maintenance.md#opt-in-live-queue-profiling)
- <a id="stored-job-file-retention"></a>[Stored job file retention](reference/file-store.md#stored-job-file-retention)
- <a id="production-file-store-reserve"></a>[Production file-store reserve](reference/file-store.md#production-file-store-reserve)
- <a id="per-project-file-store-quotas-464"></a>[Per-project file-store quotas (#464)](reference/file-store.md#per-project-file-store-quotas-464)
- <a id="parked-native-storage-integration-564"></a>[Parked native storage integration (#564)](reference/file-store.md#parked-native-storage-integration-564)
- <a id="project-owner-early-file-release-465"></a>[Project-owner early file release (#465)](reference/file-store.md#project-owner-early-file-release-465)
- <a id="worker-scratch-retention"></a>[Worker scratch retention](reference/worker-caches.md#worker-scratch-retention)
- <a id="model-master-folder-worker-cache-and-production-reservations"></a>[Model master folder, worker cache, and production reservations](reference/models.md#model-master-folder-worker-cache-and-production-reservations)
- <a id="general-model-servers-and-joint-measurement"></a>[General model servers and joint measurement](reference/models.md#general-model-servers-and-joint-measurement)
- <a id="host-ram-admission-and-ceilings"></a>[Host RAM admission and ceilings](reference/host-capacity.md#host-ram-admission-and-ceilings)
- <a id="optional-privileged-gpu-telemetry-source"></a>[Optional privileged GPU telemetry source](reference/host-capacity.md#optional-privileged-gpu-telemetry-source)
- <a id="root-owned-broker-and-unprivileged-client"></a>[Root-owned broker and unprivileged client](reference/host-capacity.md#root-owned-broker-and-unprivileged-client)
- <a id="admission-and-enforcement"></a>[Admission and enforcement](reference/host-capacity.md#admission-and-enforcement)
- <a id="native-payload-isolation"></a>[Native payload isolation](reference/native-execution.md#native-payload-isolation)
- <a id="cpu-capacity-and-allowances-r1"></a>[CPU capacity and allowances (R1)](reference/host-capacity.md#cpu-capacity-and-allowances-r1)
- <a id="file-and-shared-input-cache-capacity"></a>[File and shared-input cache capacity](reference/worker-caches.md#file-and-shared-input-cache-capacity)
- <a id="environment-cache-capacity"></a>[Environment cache capacity](reference/worker-caches.md#environment-cache-capacity)
- <a id="accelerator-driver-fault-quarantine"></a>[Accelerator driver fault quarantine](reference/accelerator-quarantine.md#accelerator-driver-fault-quarantine)
- <a id="verify-the-loaded-environment-catalog"></a>[Verify the loaded environment catalog](reference/worker-caches.md#verify-the-loaded-environment-catalog)
- <a id="migrate-storage-guards-to-automatic-admission"></a>[Migrate storage guards to automatic admission](reference/migrations.md#migrate-storage-guards-to-automatic-admission)
- <a id="automatic-joining"></a>[Automatic joining](reference/onboarding.md#automatic-joining)
- <a id="private-file-compatibility-and-foreground-join"></a>[Private file compatibility and foreground join](reference/onboarding.md#private-file-compatibility-and-foreground-join)
- <a id="onboarding-storage-and-tools"></a>[Onboarding storage and tools](reference/onboarding.md#onboarding-storage-and-tools)
- <a id="linux-cifs-boot-recovery"></a>[Linux CIFS boot recovery](reference/nas-mounts.md#linux-cifs-boot-recovery)
- <a id="worker-mount-recovery-linux-and-macos"></a>[Worker mount recovery (Linux and macOS)](reference/nas-mounts.md#worker-mount-recovery-linux-and-macos)
- <a id="address-and-network-behavior"></a>[Address and network behavior](reference/onboarding.md#address-and-network-behavior)
- <a id="capability-profile-v0"></a>[Capability profile v0](reference/onboarding.md#capability-profile-v0)
- <a id="invite-and-upgrade-machines"></a>[Invite and upgrade machines](reference/onboarding.md#invite-and-upgrade-machines)
- <a id="polite-part-time-hyphae"></a>[Polite part-time hyphae](reference/shared-and-polite-hosts.md#polite-part-time-hyphae)
- <a id="os-signals-and-limits"></a>[OS signals and limits](reference/shared-and-polite-hosts.md#os-signals-and-limits)
- <a id="shared-interactive-hosts"></a>[Shared interactive hosts](reference/shared-and-polite-hosts.md#shared-interactive-hosts)
- <a id="supported-work"></a>[Supported work](reference/shared-and-polite-hosts.md#supported-work)
- <a id="measure-the-memory-reserve"></a>[Measure the memory reserve](reference/shared-and-polite-hosts.md#measure-the-memory-reserve)
- <a id="configure-deliberately"></a>[Configure deliberately](reference/shared-and-polite-hosts.md#configure-deliberately)
- <a id="pressure-response"></a>[Pressure response](reference/shared-and-polite-hosts.md#pressure-response)
- <a id="qualification-and-laptop-checks"></a>[Qualification and laptop checks](reference/shared-and-polite-hosts.md#qualification-and-laptop-checks)
- <a id="queue-disk-contention-527"></a>[Queue disk contention (#527)](reference/queue-io.md#queue-disk-contention-527)
- <a id="sqlite-backup-and-commit-limits"></a>[SQLite backup and commit limits](reference/queue-io.md#sqlite-backup-and-commit-limits)
- <a id="standard-systemd-policy"></a>[Standard systemd policy](reference/queue-io.md#standard-systemd-policy)
- <a id="laptop-experiment-and-native-acceptance"></a>[Laptop experiment and native acceptance](reference/queue-io.md#laptop-experiment-and-native-acceptance)
- <a id="durable-model-engine-interval-history-663"></a>[Durable model-engine interval history (#663)](reference/models.md#durable-model-engine-interval-history-663)
- <a id="completion-observer-upgrade-and-recovery-next-release"></a>[Completion observer upgrade and recovery (next release)](reference/completion-observer.md#completion-observer-upgrade-and-recovery-next-release)
- <a id="completion-and-health-delivery-acceptance"></a>[Completion and health delivery acceptance](reference/completion-observer.md#completion-and-health-delivery-acceptance)
