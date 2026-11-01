# Operate Mycelium

Start with [setup prerequisites](#1-check-prerequisites-on-every-machine),
[hub installation](#2-install-the-hub), then [add machines](#4-add-each-other-machine).
The automated install/onboarding route requires a verified NAS mount. Standalone
`myce dev` and file-store execution work without a NAS; see [start here](START-HERE.md)
and [files](files.md). No installer mounts storage, changes drivers or opens firewalls.

For an existing installation use [maintenance](#queue-maintenance),
[install/upgrade](#install-and-upgrade), [backup/recovery](#queue-backup-restore-and-relocation),
[signed releases](#publish-and-approve-an-onboarding-release),
[polite participation](#polite-part-time-hyphae) or [shared-host sizing](#shared-interactive-hosts).
Detailed [status fields](status.md), [native qualification](development.md#native-acceptance-checklist)
and [isolation limits](native-isolation.md) distinguish readiness from enforcement.
Agent skill/harness installation lives [beside the skill](../skills/mycelium-offload/setup-project.md).

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
external-memory history and owner bursts, then use the [shared-host profile](operations.md)
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
example, across subnets), see [address and network behavior](operations.md#address-and-network-behavior).

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
([publish and approve a release](operations.md)), or use option B.

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

### Preserve one complete generation

Inventory the active configuration and every referenced root. Preserve privately:

- SQLite DB and any WAL/SHM companions, including job/key/attempt/custody history,
  learned curves, live policy, maintenance and release approvals.
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
   backup sleep argument is not a bandwidth limit; see [I/O controls](operations.md).
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
`--max-lifetime` defaults to `2h` and accepts at most `24h`; see below for expiry.

### Reserve a host or its devices for an existing validation job

```sh
myce reserve add 'host=compute-a;devices=0' --id driver-check --job JOB_ID --owner validation-agent --reason 'driver qualification' --max-lifetime 2h
myce reserve add 'host=compute-b' --id host-check --job OTHER_JOB_ID --owner validation-agent --reason 'host validation'
myce reserve list
myce reserve release driver-check --if-revision 1 --reason 'validation abandoned; inspect retained execution custody separately'
```

`reserve add` binds one existing single batch job/check before enabling admission
policy. It resolves the same stable physical identities as `exclude`, accepting
one host, its explicit devices, `host=HOST;kind=gpu`, or
`host=HOST;platform=PLATFORM`. The bound job must fit a selected target and is
then restricted to that target. Unknown jobs, dependencies/fan-outs, incompatible
targets, overlapping active reservations, and another reservation for the same
job are rejected. Already queued production takeover demand on that host must
settle before acquiring a competing reservation, avoiding a self-blocking hold.
The reservation admits only the bound job through its own fence; all ordinary
priority, memory, isolation, qualification, maintenance and cleanup gates remain.
Production work already running is never displaced by a validation reservation.

Acquisition is serialized with placement, launch and Started. Cached unstarted
offers are withdrawn using planned maintenance cleanup without spending an
infrastructure retry. Started commands/calls and started preparations continue.
Device reservations retain sibling-device and CPU capacity. They do not change
host drains, device maintenance, exclusions, budgets or production-hold policy;
automatic or conditional release removes only the reservation's own fence.

The authenticated API is `POST /api/reservations` with
`{id, selector, job, owner, reason, max_lifetime}`, `GET /api/reservations`, and
`POST /api/reservations/{id}/release` with `{if_revision, reason}`. Keep the
returned ID, canonical selector and revision. Retrying the same ID and binding
does not renew or resurrect it; a different binding is stale. The owner is
attribution under existing operator authority, not another authentication role.
Runtime checks may include the same options under `extension_check.reserve`
on `POST /api/jobs`; the job and reservation commit or roll back together.

Queue SQLite persists the binding, stable target, original expiry and release
receipt. Success, failure and cancellation release it automatically only after
**every known attempt** and its Started preparations have confirmed physical
cleanup. A terminal result, elapsed stop grace, missing worker or queue restart
never proves cleanup. Status
distinguishes `draining`, `ready`, `in_use`, `waiting_for_release` and `released`,
and shows `reserved for OWNER: REASON` plus pending work/evidence.

The maximum lifetime is a lost-binding backstop, not a deadline for live work.
Its cost is retaining reserved capacity beyond the lifetime while a queued,
preparing or running bound job exists, or any exact cleanup is unconfirmed.
Status then reports `reserved past max lifetime` with the blocking reason;
expiry never cancels work or confirms physical release. Only an absent job
binding with no retained execution custody can expire with the visible release
reason `maximum lifetime elapsed`. A terminal bound job releases after all
attempts physically release, whether before or after the lifetime. Independent
drains and exclusions still apply. Conditional operator
release has the same admission-only meaning and requires an observed revision
and reason. Use indefinite `exclude`/host maintenance for an operator-controlled
hardware hold instead.

Laptop proof uses the real API and command runner with fake inventory. Start
`myce dev --workers 1 --devices 2` and use its printed queue settings; add a local
example such as `examples/extensions/http-stage`, then run
`myce extension check --reserve http-stage@1` and inspect `myce reserve list`.
No GPU is needed. `TestReservationDev` additionally queues ordinary work on the
reserved fake device and proves it runs after the bound validation releases;
`TestExtensionAgentDevEndToEnd` exercises the CLI flag with the real check runner.

## Change live project policy

`queue --project-config` (and `dev --project-config`) bootstraps project settings
only before the first policy is stored. After first boot, the persisted SQLite
policy is authoritative. Editing the config and restarting or reinstalling the
queue no longer changes live policy or advances its revision.

For a deliberate change, export the current policy, edit its `projects` mapping,
and import it using the revision printed by the export:

```sh
myce policy get --yaml > policy.yaml
# Edit policy.yaml, then replace N with the exported revision.
myce policy set policy.yaml --if-revision N
```

If the revision is stale, export again and reconcile the newer policy before
retrying. See [owner priority policy](scheduling.md) for the full
import/export contract, including replacing or clearing project settings.

## Queue maintenance

All commands use the authenticated queue and run as an ordinary account.
On an enrolled machine they read installed authority and keep its certificate
pin, like `myce status` and `myce cancel`, so no token is needed. Explicit
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
myce host resume HOST
```

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

Selectors are literal: `host=HOST`, `kind=gpu` (all accelerators, including
CUDA, XPU, Metal and dev fake devices), `platform=PLATFORM`, or
`host=HOST;devices=DEVICE,DEVICE`. A host can qualify either kind or platform,
for example `host=compute-a;platform=cuda`. Quote selectors containing `;` so
the shell passes them as one argument. Device lists accept displayed IDs or
stable identities, resolve them to physical identities, sort and deduplicate
them, and print the durable selector. Use that printed selector for removal,
especially after a worker's ordinals change or devices leave inventory.
Kind/platform settings remain in force for future matching devices and hosts;
they are not expanded into a snapshot of today's inventory.

These queue-owned settings persist in SQLite across restarts. They reuse the
per-device admission fence below: new placements and unstarted launches avoid
excluded components, while Started attempts and active model calls finish.
They require no worker restart or configuration edit. A host's GPU exclusion
leaves its CPU work and RAM allowances available. Whole-host exclusion calls
the existing host drain transaction, covering CPU, RAM and accelerators together.
Adding a whole-host exclusion to an already drained/disabled host is refused;
manage that existing maintenance with `myce host` instead. Remove the host
exclusion to resume its drain. If another operator has since drained or disabled
the host independently, removal leaves that maintenance intact. `host resume`
cannot bypass an active whole-host exclusion.

`myce status` (CLI, JSON and web) and `myce exclude list` show each active
selector, reason, queue timestamp and `set_by`. `--by OPERATOR` supplies the
attribution; it defaults to the current OS username. This is an operator-supplied
label under the existing shared fleet-token authority, not a separately verified
login. The commands reuse installed pinned operator authority. The CLI resolves
the selector and reads its revision before editing; a competing edit is rejected
as stale. API clients use `GET /api/exclusions/resolve?selector=...`,
`GET /api/exclusions`, and `POST /api/exclusions` with `action` (`add`/`remove`),
the resolved `selector`, `set_by`, `if_revision`, and optional add `reason`.
The API retains inactive revision records; `exclude list` displays active ones.

Exclusions combine additively with each other, per-device disables and host
maintenance. Removing one restores only capacity not withheld by another fence.
Use the CPU/RAM allowances under [CPU capacity and allowances](#cpu-capacity-and-allowances-r1)
to reserve a chunk of those resources; there is no new CPU/RAM slicing protocol.
`--reserve-cpu-cores` and `--reserve-host-memory-gb` provide initial owner reserves;
`myce host budget HOST --cpu-budget-cores N --memory-budget-gb GiB` durably changes
the queue's allowances while the host is drained. Resume after the change and
allow worker views to acknowledge the policy. Zero CPU budget means automatic
capacity minus reserve, not zero available cores; RAM budgets must be positive.

The `exclude add|remove|list` shape keeps host, fleet and list selectors in one
small command family. The single-device controls below remain available as the
original independent per-device setting.

### Set aside one accelerator

```sh
myce host device disable HOST DEVICE --reason "recurring driver faults"
myce host device enable HOST DEVICE
myce host status HOST
myce status
```

`DEVICE` is the displayed inventory ID (for example `0`) or its stable
`identity` from `myce host status HOST`. The CLI resolves it to the physical
identity before submitting a conditional edit. The queue persists the policy
by host, platform and identity in SQLite, independently of ordinals, worker
sessions and health reports. Queue and worker restarts preserve it; an index
reused by a different physical device does not inherit the old disable.
If an inventory identity is missing or ambiguous, inspect host status and use
the stable identity rather than guessing an index. Workers without a stable
inventory cannot use this control.

Disabling admission preserves Started batch attempts, running preparations,
engines and active calls. It withdraws unstarted batch offers and refuses new
batch placements, calls, engine launches and device-specific preparation
launches on that device. Model-cache copies use shared host storage and remain
allowed. Healthy siblings retain their budgets and CPU work
remains eligible. New whole-host onboarding benchmarks are deferred while any
inventory device is disabled; ordinary worker startup continues. A later join
after enabling the device can refresh whole-host throughput. Already authorized
benchmarks continue as existing work. Existing work can still finish or stop under ordinary
cancellation, failure and priority rules. This command does not edit the
worker installation/configuration and needs no restart. Host drain/disable
remains an independent fence; enabling a device does not resume its host.

Host status returns `device_maintenance` records with identity, reason,
`updated_at` (injected queue time) and revision. CLI/web status lists disabled
devices, including retained policies for absent devices. Enable clears the
reason but keeps a revision record. Mutations use the same installed operator
authority as host drain. The CLI reads the current per-device revision and
sends `if_revision`; a concurrent edit is rejected as stale, never retried
blindly. API clients post `action`, `platform`, `identity`, optional `reason`
and required `if_revision` to `/api/hosts/HOST/devices/maintenance`; revision
zero means no prior record. Device edits preserve the host drain owner's
receipt and timestamp.

`host placement` changes the queue-owned policy for a physical host, including
all its worker/device views. It can be changed while enabled, persists across
queue restarts and worker sessions, and cannot be overridden by check-ins.
The default is `last_resort: false`. When true, ordinary admissible hosts rank
first, followed by last-resort hosts; MY-18 value ordering remains unchanged
within each group. For model calls, a ready or loading engine on a last-resort
host beats a cold ordinary host (readiness before value). If no ordinary host can admit the work in the current plan,
fallback is immediate, with no waiting or reservation. Fairness, priority,
preemption, eligibility and cleanup custody retain their existing rules.

`host status HOST` reports `policy.last_resort`; status API machine rows expose
`last_resort` when true, and CLI/web status labels the host's placement policy.
Queued job `reason` and the assigned attempt's `timing.placement_reason` explain
last-resort fallback and retain the value-ranking explanation. These reasons are
diagnostic text. See the paired [simulator fixture](../examples/sim/last-resort-hosts/README.md)
for synthetic placement and immediate overflow behavior.

Drain preserves Started work. Disable with `--stop` requests bounded stops of
exact current attempts, preparations and engines. Issued preparation plans count
as work before delivery. Every launch rechecks queue authorization; drain cancels
unstarted plans, and started preparations retain custody until exact completion.
Upgrade queue and workers together for the current launch-gate protocol. Planned maintenance does not
consume infrastructure retries; cancellation and independently observed stage
failures still take precedence. A successful mutation is durable policy
acceptance. `--wait` requires drained/disabled state, fresh consistent known
workers and zero work, preparations, engines or pending cleanup. Timeout and
missing reports leave custody unresolved. Resume does not discharge old work.

Opt-in onboarding benchmarks register before measuring and count as preparations
until their exact attempt finishes and process cleanup is confirmed. They share
the queue launch check with jobs, builds and engines, including the final shell
gate for qualified reference commands. Heartbeats continue during measurement;
drain/disable preserve a running command, and disable-stop cancels it. If
maintenance or existing host work withholds onboarding, the worker continues
checking in without benchmarking; a later join can retry it.

During queue outage, no maintenance mutation or new Started authorization can
succeed. Inspect owned work before using separately authorized service/container
tools. Preserve results and custody for reconnect; there is no offline release
guarantee. Consumer cancellation and application health remain independent.

Launch authorization and the separate Started hand-off each use the normal
ten-second control-request budget. Measured queue-lock waits reached 3.54 seconds,
exceeding the former three- and two-second deadlines. The standard budget admits
these bursts without retrying or opening either gate before a positive reply.
An unreachable queue still fails within ten seconds per request; the longer wait
costs at most seven/eight extra seconds at the respective boundaries. Exact
attempt, session, cancellation and placement checks remain authoritative.
The local exec-receipt budget starts afresh after Started approval. Authorization
failure remains an infrastructure interruption under the bounded retry policy.

A check-in from the current accepted worker session counts as contact before
waiting for the queue lock, for at most 30 seconds from its arrival. Request
cancellation retains only that original bounded credit, so a client deadline
during queue contention does not turn queue delay into worker silence (#343).
It neither refreshes `LastSeen` nor extends the arrival deadline. A noncancelled
rejection removes pending credit. An accepted check-in refreshes contact when
processing finishes. A report rejected by later validation
can therefore delay loss detection by at most 30 seconds, without refreshing
`LastSeen`; an unauthenticated or mismatched-session report receives no pending
credit. Queue capacity limits still require separate correction; start requests
that exceed their ten-second budget remain bounded failures.

Status capture batches retained fan-out cancellation membership once per capture,
including removed waves and consumers. It reuses that read-only view for scope
and release checks instead of repeatedly scanning all retained jobs/submissions
under the queue lock. The view is discarded after capture; exact execution
acknowledgements still determine release, and elapsed time never proves cleanup.

The queue emits one `event=slow_launch_authorization` line for an authorization
taking at least 500 ms. `duration_ms` covers the authorization, including the
checkpoint probe, queue-lock wait and persistence; `mutex_wait_ms` isolates waiting
for `q.mu`, and `sqlite_commit_ms` measures the SQLite commit itself (excluding
statement execution and state serialization). `ok` reports authorization success.
The line contains no worker identity, payload, filesystem path or transport error text;
it is written after releasing the queue lock. These timings are diagnostic wall
time, independent of the injected scheduling clock.

`event=slow_queue_mutex_hold` identifies sampled holds of at least 250 ms in
queue updates (including Tick, check-ins and scheduling), status projection and
batch-log retention. `operation` names the code operation, `duration_ms` measures
time holding the lock (excluding acquisition), and `sample_every=16` means one
in sixteen holds at each boundary is measured. Fast holds stay silent; unsampled
holds do no clock reads. Logging and operation-name resolution happen after
unlocking, without request identifiers. Sampling can miss an isolated slow hold;
absence of a line does not rule out contention. These measurements also use
diagnostic wall time, never the scheduling clock.

### Stored job file retention

Frozen extension bundle bytes and their snapshotted environment files live in
the queue file store once per content digest. Jobs, retained keyed answers and
the extension registry hold small references; control transactions do not copy
the bundle bytes. Worker delivery and digest downloads verify and load the bytes
outside the queue mutex. Keep the file store with the queue database when moving
or backing up queue state.

On startup, embedded bundles are migrated before serving traffic: verify their
digests, durably publish the blobs, then atomically commit the references in
SQLite. An interrupted migration repeats safely on the next startup. A digest
mismatch or missing referenced blob stops startup rather than discarding data.
Use the same `--file-store` root on restart (default `DATABASE.files`). Embedded
Go callers select a custom root with `WithFileStore` in `Open`, before migration;
changing roots after bundle references exist is refused.

Queue maintenance reclaims stored job inputs and results after **7 days in a
terminal state**. Set a positive `myce queue --file-retention DURATION`; installed
queues accept `myce install queue --config QUEUE_CONFIG --file-retention DURATION`
and preserve the setting across upgrades. The default is `168h`. This is separate
from log retention, and applies in NAS and file-store checkpoint modes.

Job records and keyed answers stay, including their file identities and
`files_expired: true`. A keyed retry returns the original answer and never reruns
expired work. Result helpers return HTTP 410 `result_expired`; raw input/result
fetches return that typed error once their bytes are reclaimed. Open downloads
can finish; callers returning after the window must accept expiry. Increasing
retention does not restore expired data. Historical terminal jobs without a
recorded finish time receive a full window from first observation.

Reclamation preserves blobs needed by non-terminal or unexpired jobs, any
checkpoint generation, catalog models, extensions and environments. It runs from
`Queue.Tick`, with filesystem work outside the queue lock. There is no job-record
pruning, result acknowledgment or early release command. Each pass checks at most
64 unprotected candidate blobs and records completed reclamation on the existing
job, so later passes do not revisit those hashes. Publication and successful blob
HEAD verification refresh a durable mtime; collection waits six hours after that
refresh. This covers the gap from the first file through later multi-GB uploads,
slow NAS, or a queue outage/deploy before submit/complete. Only refreshed blobs
receive the grace; a new committed reference then keeps the bytes.
This publication grace does not extend an expired job's result-recovery window.

Storage removal/stat failures are logged and retried on the next pass; they do
not stop queue maintenance. A busy publication/verification fence logs
`file retention deferred` once per consecutive streak of skipped passes.
Only uploads positively marked as job inputs by the CLI/profile, Go `UploadJobInput`
or Python `upload_file` are eligible for orphan reclamation. Their eligibility and
publish time survive restart in the existing queue SQLite state. They must have no
job, keyed-answer, checkpoint, catalog/model, extension, environment or pending
reclamation reference, and both publish time and mtime must be older than the
configured retention window, outside the six-hour publication grace. A committed
job reference clears the marker; terminal-job retention then applies. Unmarked
imports (`myce file put`, Go `UploadFile`, models and older clients) are never
orphan-swept, even if no model catalog is loaded or an entry is removed. Reusing an
unmarked import as a job input cannot mark it; a generic import clears any marker.
Marked interrupted tus uploads are reclaimed after an idle retention window and
the publication grace. POST/HEAD/PATCH refresh their durable idle timestamp using
the queue Clock; active requests and all protected references survive collection.
Unmarked imports and unknown files remain for explicit inspection.

`myce status` and `/api/status.file_store` report the queue-file-store's payload
`used_bytes`, admission `free_bytes`, and currently `reclaimable_bytes`. Free space
is the smaller of the configured store allowance and disk space remaining after
existing tus lengths and the shared disk floor (1%, minimum 256 MiB). The JSON
also exposes `limit_bytes`, `disk_free_bytes`, `floor_bytes`, and an explicit error
when the reading is unavailable. Status never deletes files.

**Deploy order:** deploy the queue before new CLIs. Older queues return HTTP 404
for `/api/files/preflight`, causing every profile submission to exit 69.

Profiles preflight the complete input manifest through `POST /api/files/preflight`
before the first upload. Preflight runs ordinary retention maintenance; upload creation repeats capacity
admission. Capacity refusals use HTTP 507 with code `file_store_capacity`, the
public store name, needed/free/shortfall byte counts and recovery instructions;
profile receipts preserve the reason and exit 75 for retry. A failed capacity
reading returns HTTP 503 `file_store_unavailable`. Preflight makes no reservation:
other writers can consume space afterward, and unknown output sizes cannot be
predicted. Protected data is never evicted to admit work; increase the configured
store allowance or free disk space when retention cannot help. Worker file caches
already evict reproducible copies under pressure. See [files](files.md).

## Production file-store reserve

`myce queue --file-store-production-reserve BYTES` (also supported by `myce dev`)
protects part of the existing `--file-store-bytes` allowance for production-tier
work. Zero, the default, preserves existing admission. The reserve must be
nonnegative and no larger than the store limit. It adds no disk space and changes
neither retention nor deletion behavior. Existing blobs and admitted resumable
uploads remain usable.

For an installed queue, set both values through the supported installer (byte
counts, not GiB suffixes). For example, a 200 GiB limit with a 16 GiB reserve:

```sh
myce install queue --config QUEUE_CONFIG \
  --file-store-bytes 214748364800 --file-store-production-reserve 17179869184 --dry-run
myce install queue --config QUEUE_CONFIG \
  --file-store-bytes 214748364800 --file-store-production-reserve 17179869184
```

These flags also work on a fresh queue install. They are saved as
`file_store_bytes` and `file_store_production_reserve` and included in the
generated service. Omitted flags preserve saved values across reinstalls and
upgrades. Fresh installs and older configs without these fields keep the
existing defaults: 100 GiB capacity and zero reserve. Set
`--file-store-production-reserve 0` explicitly to disable the reserve.

To retire an existing systemd `ExecStart` drop-in during an authorized upgrade:

1. Inspect `systemctl --user cat myce-queue.service` and record the effective
   capacity, reserve and any other custom arguments. Pass the existing byte
   values explicitly to the installer; it does not import drop-in arguments.
2. Preview the install with those flags and the exact saved config. Drain/disable
   admission using the normal upgrade procedure. Preserve a private copy of
   the drop-in outside the unit's `.d` directory. **Keep the override in place**
   and run the install with those flags to save both replacement settings. The
   installer warns that the override still takes precedence; any restart during
   installation continues to use that override.
3. Check the saved config's `file_store_bytes` and
   `file_store_production_reserve`, and inspect the generated base unit at the
   saved `service` path. Verify that its `ExecStart` includes both flags with the
   intended values. If installation or verification fails, leave the override
   in place and fix the problem before proceeding.
4. Remove only the overriding `ExecStart` directives (including the empty reset),
   or the whole file if it contains nothing else. Preserve unrelated settings;
   migrate any other custom arguments before removing their override. Run
   `systemctl --user daemon-reload`, then
   `systemctl --user restart myce-queue.service`.
5. Verify the effective unit with `systemctl --user cat myce-queue.service` and
   `systemctl --user show myce-queue.service --property=ExecStart`. Confirm both
   values in the running command and `myce status --json` file-store fields
   (`limit_bytes`, `production_reserve_bytes`) before resuming admission.
   If verification fails after removal, put the old drop-in back at its original
   path, run `systemctl --user daemon-reload`, restart the queue, and verify the
   restored values before resuming admission.

A full `ExecStart` override hides new arguments in the generated base unit;
future capacity/reserve changes should use install flags instead.

New identified non-production allocations that would consume the reserve receive
HTTP 507 `file_store_capacity`, naming the reserve. Preflight, upload creation,
internal registry bundles and declared async output reservations use this check.
Production output commitments back the reserve rather than being charged twice.
Production still needs actual store/disk space above the physical floor and
existing commitments.

**Unidentified writes can use the production reserve.** Deployed input clients
and old native workers send the shared token without a project or attempt
identity; the queue cannot distinguish their production bytes from experimental
bytes. The compatibility exception favors production admission and also permits
unidentified experiments/imports to consume protected capacity. A configured
reserve therefore does not guarantee isolation from those legacy writers.
Input-client identity is follow-up work under #464; quotas remain a separate PR.

New workers send `X-Myce-File-Attempt` containing the job ID, attempt, machine and
session on native/async result and checkpoint uploads. The authenticated queue
checks that identity against the current execution and derives the tier from the
job, ignoring project/class claims for those outputs. Replaced/lost/cancelled
attempts cannot allocate or resume uploads; an exactly accepted attempt can reuse
its blobs after a lost completion acknowledgement. Existing async workers' exact
pending-result identities are also classified from their owning jobs. Known
experiment writes and internal registry bundles cannot use the exception.

For inputs, Go callers may wrap preflight/upload contexts with
`myce.WithFileStoreSubmission(ctx, spec)`; the profile CLI supplies these hints.
HTTP callers can send `X-Myce-Project` and optionally `X-Myce-Class`. The queue uses
the project's `default_call_priority` (experiment when absent) and effective
policy tier. Class hints can narrow that decision, never promote it; claiming
`production` cannot upgrade a server-known experimental project. These are
shared-token accounting hints, not per-project authorization credentials.

`myce status` and `file_store` JSON report `production_reserve_bytes`,
`unidentified_writes_may_use_reserve`, and `unidentified_reserve_admitted_bytes`.
The last counter is the cumulative portion of successful unidentified tus create
admissions crossing the protected floor **since queue start**. It counts declared
allocation, including incomplete uploads, not bytes transferred or current usage.
Preflight, resumable replay and deduplicated reuse do not increment it. It resets
on queue restart and decreases neither on completion nor on ordinary expiry.

### Worker-local task delivery (#508)

The versioned `output_destination` contract keeps unique outputs on a charged
worker spool through verified NAS or submitter delivery. Collection is lazy:
verified local copies remain until a qualified queued task needs space on that
same worker backing, or the configured retention window expires. Eligible copies
are collected in MY-18 order (least valuable owner tier first), preserving #479's
expiry ledger, six-hour publication grace and keeping-reference fences.
`until_delivered` grants expiry authority after delivery; it does not trigger
eager local collection. Independent production backing does not create pressure
on retained experiment outputs.

Each collection operation takes its intent, freshly hashes the destination
outside `q.mu`, rechecks the exact execution/verification-round/reference fences,
then deletes. Saved intents and historical receipts never supply reusable proof.
Failed verification or a changed fence retains the worker copy, marks redelivery
and raises an operator/project alert. An intent can finish interrupted deletion
and acknowledgement even if its original capacity demand has disappeared.

The accepted residual is this one operation's own verify-to-delete span. Once
delivery is verified, the destination is authoritative; its NAS, backups and other
durability mechanisms are responsible for subsequent preservation. No
verify-then-delete operation across independent filesystems can eliminate the
possibility of destination loss a moment after local deletion. Lazy collection
reduces exposure; it does not promise continued destination durability.

Submitter `deliver_outputs` can restore a missing destination from retained
worker bytes after ordinary result expiry, until actual local collection.
Ordinary expired-result downloads still return `result_expired`. See the
[task output guide](files.md) for destination identity configuration,
client commands, truthful local availability and native qualification limits.

### Project-owner early file release (#465)

Use explicit job IDs to release terminal input/result files before their normal
retention deadline. **Ownership means project attribution under the shared fleet
credential, not an authorization boundary.** Any holder of that credential can
act for a project; this does not add individual owner authentication. Global
retention and capacity admission remain unchanged.

First close the application operation: all downstream consumers and controller
reconciliation must have finished using these files. Include every owning
terminal reference you want to expire, including fan-out parents, children and
keyed answers' job IDs. Other retained owners continue to protect shared content.
A successful stage alone does not establish that application completion boundary.
For ordinary tasks, accepting expiry explicitly gives up queue recovery of the
selected data, including unsuccessful tasks you select. Preserve any required
source/result copies with verified identities before doing so. Unselected work
keeps its ordinary retention. For tasks using `output_delivery: async_nas`, the
queue additionally requires its existing verified delivery receipt to match the
accepted execution and manifest; pending/failed or mismatched delivery blocks
release. An unavailable NAS is handled by the existing asynchronous delivery
retries; release never tries to copy or wait on the NAS.

```sh
# Read-only: save and inspect the returned JSON before applying it.
myce file release plan --project example JOB_ID_1 JOB_ID_2 > release-plan.json
# Irreversible expiry acceptance for exactly the reviewed project/jobs.
myce file release apply --accept-expiry release-plan.json
```

Both commands accept the usual installed authority options (`--config`,
`--joining-file`, `--queue`). Keep the saved plan/receipt private when it contains
private project or file names. The API equivalents are
`POST /api/files/release/plan` with `{project, jobs}` and
`POST /api/files/release/apply` with
`{selection: {project, jobs}, plan_id, accept_expiry: true}`. A plan is a
read-only snapshot, not a reservation. Its digest identifies the reviewed state;
it is not a signature or an authorization token.

The plan lists each job's state, release/retention state, original expiry deadline,
input/result/shared-input/dependency/diagnostic/fan-out manifests, and delivery
evidence where applicable. Each content identity appears once in the byte total,
using its current stored size. `reclaimable_bytes` counts only bytes removable
now after selection: retained job references, checkpoints/resume generations,
model/catalog assets, extension/environment assets and bundles, pending output
and unresolved custody remain protected. The plan lists the references that
block each identity and flags publication grace or bundle retention separately.
Absent blobs contribute zero. Recently published/HEAD-refreshed blobs retain the
ordinary six-hour publication grace. An eligible plan can reclaim zero bytes
while still expiring the selected identities.

`collection_eligible` uses the collector's pending reclamation and keeping
references. Importing identical bytes after completed reclamation does not
re-enroll them through the old job's manifests, so a new plan counts zero bytes
for that content unless another pending reclamation owns it.

Apply requires explicit acceptance and recomputes the reviewed plan, then fences
custody and reference metadata again in the expiry transaction. HTTP 409
`file_release_stale` means regenerate and review a new plan; nothing was expired.
HTTP 409 `file_release_protected` means a selected job is live, has unresolved
physical custody or lacks required verified delivery. Include only eligible jobs
in the next plan. Other projects' retained references can block physical removal
without blocking expiry of your selected job references.

A successful apply returns `collection: scheduled`: it durably sets the existing
job file-expiry/reclamation state, preserving identities, manifests and a small
`file_expiry_accepted` ledger receipt (plan digest in `reason`, acceptance time
in `at`) in the job/keyed answer.
The existing bounded maintenance collector rechecks references and publication
grace before unlinking, retries failures, and resumes after restart. This is
asynchronous reclamation, not a promise that capacity is immediately available.
No new deletion daemon or global retention change is involved. Repeating the
same apply after a lost response returns the original receipt while the selected
job/keyed answers remain retained, even after the blobs are gone or a restart.
Ordinary history/key expiry still applies afterward; unknown/expired selections
are refused without creating jobs. Retrieval retains typed HTTP 410
`result_expired` behavior for expired, unshared content. Save the plan and apply
response for the application's audit record.

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
`--device` restricts its inventory. Existing split installations require the M4
conversion below before installing this version.
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
`Restart=on-failure`, `Delegate=memory cpu`, `KillMode=control-group` and bounded stop.
macOS uses the generated launchd service. Configuration and secret files stay
private. Never run the ordinary binary as root.

The transaction snapshots binary, config, credentials, GPU environment and
service files/modes plus actual manager registration/activity. Failure restores
that snapshot and verifies it independently. Incomplete rollback stops affected
services and reports differences; preserve the snapshot for recovery. Effective
drop-ins that override execution, credentials or containment block activation,
except queue `ExecStart` overrides: these are preserved with a warning so their
settings can be migrated before removal. Consumer files are preserved. Check
health and exact cleanup before resuming.

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
see the [signed-release deployment step](operations.md#advance-the-default-after-a-signed-update).

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
silently cleared. See [fresh install](operations.md).

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

## Convert split workers to one machine worker (M4)

Follow the [accepted maintenance gates](operations.md#convert-split-workers-to-one-machine-worker-m4)
and [native qualification](development.md#machine-worker-native-qualification).
Conversion is an operator-run maintenance action on the queue host and every
converted worker host, including the single-card worker in the same
window. Commands below use placeholders for approved installation paths and
accounts. Load authentication through the existing private secret mechanism;
never print a token or pass it on the command line.

Pause ordinary submissions and inspect the source queue's maintenance controls.
An older source may not have queue-owned host maintenance: use its confirmed
host-close/discharge route or the [verified-empty alternative](#verified-empty-conversion-and-publication).
On the source lineage supporting `host close --local --wait-released`, exit 1
with `release_not_confirmed` means re-check status; it is not permission to
release custody. Require confirmed settlement and zero custody, or independently
prove the verified-empty alternative's conditions. Record source identities,
sessions, unit enablement/activity, config and binary digests, device identities
and spool evidence. Stop the exact units and snapshot binaries (including any
root-owned binary separately), configs, units, registries and database. Upgrade
the stopped queue first; only
then use the new queue's host-maintenance commands below. Its one-time import
preserves closed `host_intents` as disabled maintenance; disagreeing per-worker
host memory allowances refuse migration with `machine_migration_conflict`.

Disable all affected physical hosts through the upgraded queue. Keep the
current binaries and a queue/database/config backup as well as the installer's
rollback journals. Require fresh reports and no attempts, preparations, engines,
buffered results, releases or queued jobs resolved to retiring names:

```sh
myce host disable HOST --stop --wait --timeout 5m
myce host disable SINGLE_CARD_HOST --stop --wait --timeout 5m
myce host status HOST
myce host status SINGLE_CARD_HOST
```

Stop the exact recorded queue unit before offline conversion. Use the new binary
from a staging location; do not copy it into old split worker installations.
Write an explicit ownership map in private storage, for example:

```json
[
  {"from":"HOST-gpu0","to":"HOST","device":"0"},
  {"from":"HOST-gpu1","to":"HOST","device":"1"},
  {"from":"SINGLE_CARD_HOST","to":"SINGLE_CARD_HOST","device":"0"}
]
```

Preview every before/after project, pin, server and host-policy change, then
commit it while the queue remains stopped:

```sh
systemctl --user stop QUEUE_UNIT
NEW_MYCE install queue --config QUEUE_CONFIG --machine-map PRIVATE_MAP --dry-run
NEW_MYCE install queue --config QUEUE_CONFIG --machine-map PRIVATE_MAP
NEW_MYCE install queue --config QUEUE_CONFIG --dry-run
NEW_MYCE install queue --config QUEUE_CONFIG
```

The last command installs the compatible queue binary and restarts its recorded
unit. Verify durable disables, job/key/retry/checkpoint/release state and the single-card worker's
legacy reconnect before changing workers. `machine_migration_queued_jobs` lists
jobs to cancel/resubmit through clients; accepted specs are never rewritten.
Partial-card project fences and unrepresentable intersections refuse with
`machine_migration_conflict`. Reconcile those policies explicitly before retry.
A partial file publication blocks queue startup: rerun the identical offline
conversion or its queue rollback command. Never clear the database or journal.

On each worker host, pass a fresh destination config and **all** source configs:

```sh
NEW_MYCE install worker --config MACHINE_CONFIG \
  --from-worker-config GPU0_CONFIG --from-worker-config GPU1_CONFIG --dry-run
NEW_MYCE install worker --config MACHINE_CONFIG \
  --from-worker-config GPU0_CONFIG --from-worker-config GPU1_CONFIG
```

On the single-card host use one source, retaining its UUID selector:

```sh
NEW_MYCE install worker --config SINGLE_MACHINE_CONFIG \
  --from-worker-config SINGLE_SOURCE_CONFIG --dry-run
NEW_MYCE install worker --config SINGLE_MACHINE_CONFIG \
  --from-worker-config SINGLE_SOURCE_CONFIG
```

A single source with automatic device selection is resolved on that host using
the normal accelerator inventory. The preview shows the detected UUIDs and keeps
the source's memory settings for each device in its configured accelerator
family. Explicit source selectors are preserved without probing. An automatic
source cannot be combined with other source workers. Preview does not require
queue registration; installation still requires the source in the maintained
queue with the existing identity and settlement checks. For a source absent from
the queue, use the ordinary in-place install path and the
[fresh-state procedure](operations.md) when required; a successful
conversion preview does not authorize bypassing those checks.

The source manager stop must complete the same-name worker's normal leave.
Failure restores the previous install and keeps queue maintenance active.
Worker-local catalog placement fences are rewritten from explicit queue retirement
records, preserving local environment/secret values and original bytes for rollback.
The exact legacy single-card session may reconnect with its old catalog hash only while
disabled with no obligations; its model endpoint is withheld. New machine workers
still require the current catalog hash.
Source definitions are retired recoverably and persistent wants links removed;
spools, binaries and all caches remain intact. The gpu0 model/environment caches
and limits remain the sole machine caches unless the explicit migration below is
selected. Per-device selectors, fixed capacities,
reserves, shared roots, secret references and the host allowance are retained.
For a new conversion, add `--machine-cache-root` to move generated per-device
cache settings to `MACHINE_STATE/model-cache` and
`MACHINE_STATE/environment-cache`. For an already converted worker, use
`myce install worker --config MACHINE_CONFIG --machine-cache-root --dry-run`,
then repeat without `--dry-run` during an authorized maintenance window.
The saved conversion journal identifies generated source defaults; custom paths
are preserved unless selected with `--model-cache` or `--environment-cache`.
The preview shows both paths, retained allocated bytes and available disk space.
Installation requires a disabled or drained, idle, Healthy host. It uses the
normal saved-config overlay and install rollback, including same-binary
reconfiguration. Limits stay unchanged, old directories remain intact, and
environments at the new root will be prepared again. An ordinary upgrade keeps
saved cache paths; this option does not copy or delete cache contents. Keep the
journal and old directories for rollback; cleanup needs separate authorization.

Shared-field conflicts require a named explicit installer flag; budgets are
never added. Conversion reads a legacy source's persisted
`host_state_dir/host-state.json` allowance before comparing shared settings;
that effective value wins over stale installer flags. Missing/invalid allowance
in a local-control source refuses with `machine_memory_allowance_unavailable`;
use an explicit `--host-memory-budget-gb` only after establishing the intended
allowance. Sources without retired local-control configuration retain their
saved queue bootstrap value. Existing queue-owned policy remains authoritative
and is never replaced by later worker reports. The private
`MACHINE_CONFIG.m4-rollback.json` journal contains exact
original file bytes/modes and unit registration/activity. Preserve it securely.
Interrupted worker conversion requires its rollback command before retry.

Inspect effective manager configuration, exactly one worker unit/process, device
inventory and maintenance state. During the agreed test phases explicitly resume
hosts, run the entire native checklist, then disable/stop/wait before further
maintenance. Installer success does not qualify hardware. An ordinary `install
worker --config MACHINE_CONFIG` redeploy preserves the converted device settings;
repeating the completed conversion is idempotent. An old split config or an
unselected split unit fails the deploy gate with `machine_worker_conflict` or
`machine_migration_incomplete`.

### Verified-empty conversion and publication

This is an explicit alternative to persisted source-session close/discharge
receipts for supported split installations, never a normal admission shortcut.
Freeze producers, stop source user units, disable mapped hosts, and stop the
queue before its offline exclusive-lock conversion. Supply every mapped source
config, including a single-worker host:

```sh
myce install queue --config QUEUE_CONFIG --machine-map PRIVATE_MAP \
  --drain-evidence=verified-empty \
  --from-worker-config SOURCE_0 --from-worker-config SOURCE_1 --dry-run
```

After reviewing the plan, repeat without `--dry-run`. The local systemd user
manager and source spools provide the evidence; copied remote attestations do
not authorize conversion. The supported locked database must be readable and
contain zero queued/running/stopping/preparing jobs, including retained submission
records, plus zero mapped-host releases/obligations. Age never proves discharge.

For each exact source/session, require the loaded unit identity/fragment,
`ActiveState=inactive`, `MainPID=0`, no pending unit job and empty cgroup-v2
`cgroup.procs` throughout its subtree. If systemd cleared `ControlGroup`, derive
the normal path from its reported containing slice and unit name; an absent
subtree requires a readable containing slice. Hold the existing `worker.lock`
through conversion/publication. Require a marked v3 spool and exact saved session.
Use the read-only supported sparse-spool inspector; no cleanup or inferred ack.

Retained results/released Docker records are settled history only when the locked
queue confirms the exact job/attempt/worker/session release (or that superseded
copy's own release). A retention ack, newer attempt or terminal state alone is
insufficient. At each configured Docker endpoint, including the explicit local
default when Docker was not configured, successful `docker ps` must show zero
running containers with the source `myce.worker` label and zero with any
`myce.attempt` label. Disabling future Docker use does not waive this check.
Unknown/unreadable evidence refuses. Blocking spool files retain individual reasons.

Typed refusals distinguish `machine_drain_queue_not_empty`,
`machine_drain_obligations_pending`, `machine_drain_unit_active`,
`machine_drain_unit_unreadable`, `machine_drain_cgroup_populated`,
`machine_drain_cgroup_unreadable`, `machine_drain_spool_unsettled`,
`machine_drain_evidence_missing`, `machine_drain_queue_unreadable`,
`machine_drain_docker_inaccessible` and `machine_drain_container_running`.
Preview/refusal persists no receipt. Successful retirement atomically records
`verified_empty_drains`: injected-Clock time, exact machine/host/session, inactive
unit/substate/PID, cgroup evidence, sorted length-delimited top-level spool/format
SHA-256, Docker observations and six zero queue/custody counts. These receipts
are installation history, never reusable health/admission authority. They can
retire a same-name source to Gone without inventing a previous-session chain.

Queue retirement commits a pending-publication fence; startup refuses partial
policy publication. Identical-command retry finishes files or the journal
restores them, never a database snapshot. Unknown policy edits or mode/content
changes block with `machine_migration_conflict`. Project policies cannot express
partial-card allowlists and must refuse widening; a pin/single-host server can
retain its card restriction. Queue conversion records only model/placement
identities and file hashes, never model secret/environment values. Private
mode-0600 journals retain exact bytes, modes and manager state; referenced secrets
remain references. Retire only matching selected source-unit/config/spool/host/
queue enable records, including supported `worker-*.json` records. Unknown or
unselected split units block ordinary redeploy as well as conversion. Write-ahead
receipts cover each publication and preserve original modes.

### Additional rollback fences

Worker rollback inspects both source and reused destination spools, exact
process/cgroup/session custody, buffered results and Docker records before any
manager mutation. Dry rollback is read-only; real restoration holds the exclusive
spool lock after stop. Unknown changed file/mode blocks restoration; unchanged
shared catalogs are not rewritten. Interrupted plans use journaled source identity;
a missing destination unit is valid rollback input. CLI cancellation invokes
bounded restoration independently of the cancelled operation. A reused machine
spool survives forward/rollback/forward with its historical custody intact.

Queue rollback refuses any queued/failed/cancelled machine-era allowlist intent
that would disappear, including resumable work; cancellation alone cannot clear
that fence. Succeeded history remains exact. Representable pin additions/removals
and preemption counters survive; unrepresentable host-wide pins require an
explicit card/removal decision. Retired source sessions may report exact late
custody but cannot revive themselves. Fresh evidence is required for a new
forward conversion after rollback; committed publication retry uses its existing
conversion receipt.

A full production-snapshot abort is stricter than worker/topology rollback:
require no accepted new business intent, stop all new writers, archive synthetic
probe evidence, restore the entire pre-window DB/config/registry/enablement and
each separately hashed binary. Never open mutated state with an old binary or
overwrite newer checkpoints. Otherwise remain disabled and repair forward.
Verify actual legacy reconnect/refusal, not merely wire compatibility.
`RestartPreventExitStatus=78` contains only an actual exit 78, not a retrying HTTP
refusal. Preserve checkpoint IDs, attempts and highest complete generations;
missing accepted progress holds resumable work rather than restarting it empty.

### Roll back without losing accepted intent

Pause ordinary submissions and disable/stop/wait on both hosts. Require all new
attempts, group members, engines, results and releases settled. Restore worker
installations first while maintenance remains active, then restore queue fences
and legacy identities offline. This ordering allows the upgraded queue to verify
exact cleanup before the worker stop. Early legacy reconnects remain refused
until queue rollback finishes; no ordinary admission is enabled.

```sh
NEW_MYCE install worker --config MACHINE_CONFIG --rollback-machine --dry-run
NEW_MYCE install worker --config MACHINE_CONFIG --rollback-machine
NEW_MYCE install worker --config SINGLE_MACHINE_CONFIG --rollback-machine
systemctl --user stop QUEUE_UNIT
NEW_MYCE install queue --config QUEUE_CONFIG --rollback-machines --dry-run
NEW_MYCE install queue --config QUEUE_CONFIG --rollback-machines
systemctl --user start QUEUE_UNIT
```

Keep the upgraded queue binary/database, all new jobs/keys/counters and accepted
checkpoint generations. The journal restores source configs, units, enablement
and verified prior activity; never merge spools. Current pin additions/removals
and counters survive when their card restriction is representable. A new
host-wide pin or an unknown policy edit blocks restoration until explicitly
reconciled, preserving that intent. Unknown cleanup/restoration keeps admission
disabled. Verify both source workers and the single-card worker reconnect, device isolation,
secret/root custody and consumer health before the operator accepts rollback.
Forward conversion can be repeated under the same maintenance gates.

## Monitoring and failures

`myce status`, the HTML page and `GET /api/status` show queue projections with
redacted job specifications. Use [typed status](status.md) for holds,
eligibility and last-reported allowance. Inspect exact jobs/logs separately when
authorized. Status and logs require the privately configured queue credential.
Never put that token in URLs or copy private payloads into reports.

### Worker scratch retention

Each worker cleans its own state directory once a minute. Finished worker logs
go after the worker's `--log-retention` (default 7 days). Cleared per-attempt
state and associated process/result/diagnostic files keep that same window after
custody clears. Live attempts, pending result replay, indexed actors and Docker
custody awaiting the stopped-container collector are protected. Private Docker
evidence survives container removal until this log window expires. Unknown
orphan records are retained for recovery. Per-attempt input and
work folders (`job-files/JOB/ATTEMPT`, including inputs, unpacked source and a
Python job's `.venv`) go immediately through the existing collector once the
worker confirms that the exact attempt succeeded with accepted, published
outputs and confirmed release. The confirmation is saved with cleared custody,
so a restart before collection does not restart a grace period. Small logs and
process/result records keep the log-retention window. Failed attempts and
cancelled attempts and unknown orphan folders keep **24 hours** after the worker first finds them
without custody, timed by the worker clock. Cancellation does not collect partial
outputs into `output_refs`: recover them from the assigned worker's
`job-files/JOB/ATTEMPT` directory within that day, using the job's machine and
attempt from its status or ledger. This includes only files actually present on
that worker; logs and process state follow the separate log-retention window.
The grace bounds retention time, not bytes: scratch may be as large as the job's
work and inputs, and remains on the worker rather than consuming queue file-store
capacity. A worker outage or disk loss can prevent recovery. Collect failed test logs or partial
outputs within that day; opt-in published attempt diagnostics have their own
file retention. A held attempt keeps its folder however long it runs. The pass refuses
to run when `job-files` is not a real directory, for example a symlink. A folder
it cannot remove, such as one with root-owned Docker output, gets a fresh day
and is retried; other folders are still cleaned, and a repeated error is logged
once rather than every minute.

Successful installs keep one previous copy of each replaced artifact in the
role's `.Trash`; older installer-owned copies are pruned after activation. The
current install and the immediately replaced binary/config/service are preserved.
Failed transactions do not prune. Cleanup failures are reported and retried by
the next successful install, without undoing a healthy activation.

## Model master folder, worker cache, and production reservations

Models use immutable NAS masters, verified file-store imports or explicit local
source approvals. Loading never downloads weights. Catalog hashes and byte counts
identify content; cache verification precedes publication. Protect loaded/pinned
files from eviction and keep recoverable copies. Use `model sync` for pre-copy;
partial transfers and pins persist through restart. See [models](services.md)
and [files](files.md) for transfer, retention and cache limits.

## General model servers and joint measurement

General HTTP servers, llama.cpp and vLLM share the queue's engine lifecycle,
placement, pins and release custody. See [servers](services.md) for routing,
local secrets, approved folders, timeout/retry behavior and log policy.
`myce measure` loads declared models together, optionally overlaps warm-ups,
records observations, and unloads. Use explicit runtime/device/worker identity
and an authorized maintenance window. Measured peaks and headroom, not model-file
sizes or a guessed budget, qualify native admission. The [native test plan](development.md)
keeps this proof separate from laptop fixtures.

## Host RAM admission and ceilings

The separately installed [root broker templates and helper wrapper](../deploy/gpu-telemetry/README.md)
implement the Unix-socket contract in follow-up PR #422, using fresh fixed-inventory
reads after each request. They do not set the worker flag or restart the worker.

### Optional privileged GPU telemetry source

Discrete xe/XPU host-envelope learning requires both per-process DRM fdinfo and
runtime xe GTT-manager accounting. By default the worker reads debugfs with its
existing permissions. If those reads are unavailable, sizing stays conservative.
An administrator may instead provide an existing read-only telemetry executable:

```sh
myce join --gpu-telemetry-helper /usr/local/libexec/gpu-telemetry-client ...
```

The same optional flag is saved by worker installation as `gpu_telemetry_helper`
and preserved by upgrades. An explicit empty value disables the helper. The
path must be absolute. The worker executes that exact path directly, without
shell expansion, arguments or stdin, from `/`. Its environment contains only
`PATH=/usr/bin:/bin`, `LANG=C`, and `LC_ALL=C`; it receives no fleet token or job
environment. The executable is operator authority, never part of a job spec.

#### Root-owned broker and unprivileged client

The privileged-source arrangement works with `NoNewPrivileges=yes` on the
worker; it does not require changing an existing `NoNewPrivileges=no` setting.
Inspect the actual service policy rather than assuming the installer default.
A separate administrator-owned broker reads debugfs; the worker's helper
is an unprivileged client. The binary provides `myce gpu-telemetry-client`, which
connects to the Unix stream socket **`/run/mycelium/gpu-telemetry.sock`**. An
operator-owned wrapper at the configured helper path runs:

```sh
#!/bin/sh
exec /usr/bin/env -i PATH=/usr/bin:/bin LANG=C LC_ALL=C /absolute/path/to/installed/myce gpu-telemetry-client
```

The absolute binary path is chosen at installation. `--socket PATH` on the client
command supports an explicitly administered alternative or a laptop test socket;
it is never derived from a job. The helper receives no arguments from the worker.
Neither process invokes sudo, setuid or a privileged executable. This client
needs no queue connection or credentials. Mycelium installs neither the wrapper
nor the broker, and changes no service privilege policy.
The separately installed [reference broker](../deploy/gpu-telemetry/README.md)
implements this contract with fixed administrator-configured debugfs sources.

The broker contract is:

1. Listen on `AF_UNIX/SOCK_STREAM` at `/run/mycelium/gpu-telemetry.sock`. The client
   connects and immediately closes its write half. **Zero request bytes followed
   by EOF** is the entire request. Reject nonempty requests. There are no requested
   paths, PIDs, commands, identities or device arguments.
2. After accepting that request, take a UTC timestamp immediately **before the
   first debugfs read**. Freshly read `/sys/kernel/debug/dri/<PCI>/gtt_mm` for all
   configured xe cards. The broker owns this fixed source inventory; clients
   cannot alter it. `<PCI>` is a full lowercase domain address, for example
   `0000:01:00.0`; resolve numeric debugfs directories to their PCI identities.
   Return raw text containing `use_type: 1`, `use_tt: 1`, and
   `usage: <unsigned integer bytes>`. No net delta or VRAM substitution.
3. Assemble the entire JSON reply before sending. On any read failure, unsupported
   card or incomplete counter set, close without a snapshot. On success send
   exactly one JSON object, at most 64 KiB, and close the connection. No banner,
   status envelope or trailing stream. Use a 150 ms total connection deadline
   from accept, including receipt of request EOF and sending the reply, leaving
   time for client startup and delivery inside the worker's existing 200 ms
   collection window. Do not serve a cached or periodically refreshed
   snapshot and do not timestamp old data as fresh.

The exact JSON schema is:

```json
{
  "sampled_at": "2026-11-02T12:00:00.123456Z",
  "gtt_mm": {
    "0000:01:00.0": "use_type: 1\nuse_tt: 1\nusage: 167231488\n",
    "0000:02:00.0": "use_type: 1\nuse_tt: 1\nusage: 87408640\n"
  }
}
```

`sampled_at` is a UTC RFC3339 timestamp with subsecond precision, on the same
host clock as the worker. `gtt_mm` maps each full lowercase PCI address to its
unmodified debugfs text. Include every card an attempt can use: the worker
requires its selected card and every other card present in its DRM clients.
The helper forwards the broker's timestamp and text unchanged. A timestamp
before the current helper invocation or after it finishes is rejected, so a
cached snapshot is not accepted even when recent. A malformed, absent or stale
reply, missing card, nonzero helper exit, oversized response or deadline expiry
makes the attempt's GPU telemetry incomplete. A configured failing helper does
not fall back to another source. The client emits no stdout until broker EOF
and the size/deadline checks succeed, even for a complete-looking JSON prefix.

Install the broker's binary, configuration and service unit as root-owned and
unwritable by worker/job accounts. Use a root-owned `/run/mycelium` directory
with the configured client group and mode **0750**, and a socket owned by
`root:<client-group>` with mode **0660**. Render `REPLACE_CLIENT_GROUP` identically
in the broker socket and tmpfiles templates. Prefer the worker's existing primary
group, avoiding account mutation and user-manager credential refresh. The
group's socket write permission permits connecting, not changing the broker's
sources. The parent directory must have no worker/group write permission, so
clients cannot replace the socket. Keep the wrapper and executable chain
root-owned, mode **0755**, in directories unwritable by workers/jobs. The broker
uses no queue authority or fleet token and provides no filesystem-write or
administration operation to clients. Verify these owners/modes after every
broker restart. These are operator-enforced installation requirements: the
client does not check filesystem ownership/modes or authenticate the peer. It
only bounds and forwards the reply; the worker parses the JSON and validates
freshness, participating cards and counters before using it as evidence.

Activate only after separately installing and qualifying a conforming broker:

1. Read `Uid`, `Gid`, and `Groups` from the actual worker's `/proc/PID/status`.
   Select its existing primary group (or an existing supplementary group already
   held by that PID), and resolve the numeric GID with `getent group`. Render
   `REPLACE_CLIENT_GROUP` in both broker templates with that group. Create the
   root-owned directory and socket with the matching group and modes above;
   verify them after boot/socket recreation. No account change is required.
2. Verify access with one fresh complete two-card client call under the actual
   worker PID's exact numeric Uid/Gid/Groups and service restrictions. A fresh
   login shell alone is insufficient. A worker-only restart does not refresh a
   persistent user manager's supplementary groups; using its existing primary
   group avoids any user-manager restart. Existing installations may use
   a `60-worker-primary-group.conf` socket drop-in and matching tmpfiles override.
   Preserve applicable overrides until deliberately replaced; see the [template instructions](../deploy/gpu-telemetry/README.md#existing-primary-group-override-444).
3. Qualify repeated wrapper calls under the service's actual user, groups and
   restrictions, and additionally under `NoNewPrivileges=yes` if the service
   currently uses `no`. Preserve the existing service policy; no sudoers grant
   or privilege relaxation is needed. Check fresh complete per-card replies,
   failure behavior and broker resource use before enabling the helper.
4. With a compatible release installed, use the normal drained in-place worker
   installation procedure, selecting the saved `--config WORKER_CONFIG` and
   adding `--gpu-telemetry-helper /absolute/wrapper`. Inspect `--dry-run` before
   applying it, preserving saved settings. Inspect the saved config and running
   service, then qualify real attempt telemetry before claiming reduced sizing.

The client has its own 200 ms connection/read deadline and 64 KiB response limit;
the worker separately enforces its full driver-collection coverage window.
Resident/pressure sampling remains independent. The same manager parser and
conservative absolute manager-plus-client accounting apply, including overlaps.
Missing evidence is unknown, never zero. Complete fresh repeated local samples,
pressure rejection, retained floors and native concurrency/output proof remain
required. Clearing the helper on a host without readable debugfs restores
conservative sizing for new evidence; it does not revoke already learned history.

### Admission and enforcement

Declare batch `needs.host_memory_gb` or catalog `host_memory_gb` in GiB for
additional CPU-side memory, including the measured load and overlapping
activation peak. Qualify the actual wrapper and workload. Unset/zero retains
GPU-only admission on uncapped hosts. A configured host ceiling budget rejects
zero host declarations, including on sibling workers. Host requests require
valid live RAM telemetry.

All per-card workers on one physical machine must share a stable `join
--host-id` (default hostname); use unique host IDs across different machines.
Linux uses `MemAvailable`; macOS uses free plus inactive pages from `vm_stat`.
`--reserve-host-memory-gb` (default 4 GiB) withholds OS/other-workload headroom.
The queue uses its durable host memory policy, refuses inconsistent device reports,
and subtracts all outstanding declared allocations, including loading models
and stopping/draining work. This is intentionally conservative: resident memory
already reduces the live OS allowance, and its declared reservation is retained
to cover growth rather than guessing process usage. A slower/stale sibling
report can temporarily reduce admission until its next check-in. This is
aggregate admission; external allocations can still race it. Workers repeat
live refusal after preparation and before batch/model starts.

On dedicated GPUs the host pool charges only `host_memory_gb`. On unified-memory
machines, GPU and CPU allocations share one host pool:
charge `memory_gb + host_memory_gb` once per allocation, not two independent
RAM budgets. `host_memory_gb` means additional CPU-side memory, never the
combined GPU-plus-CPU footprint. Existing GPU-only allocations also reserve
their GPU portion against that pool when placing a RAM-requesting neighbor.
GPU admission remains a cap; passing both checks does not charge a second
GPU allocation. The OS reserve is the larger GPU/host reserve, applied once
to the shared allowance. Auto CUDA detects unsupported NVIDIA memory fields;
explicit `--memory-mode unified` and Metal always use this rule.


Linux cgroup v2 workers installed as systemd user services enforce declared
ceilings. The generated and manual worker units use `Delegate=memory cpu` and retain
`KillMode=control-group`. At startup the worker moves itself into the service's
`worker` leaf, then enables the delegated memory controller in the service
parent. Each native batch attempt and model-server execution gets a unique
`attempt-<execution-id>` or `engine-<execution-id>` child of that same parent.
The worker writes and reads back `memory.max`, `memory.swap.max` and
`memory.oom.group=1`, then confirms the gated process's `cgroup.procs` membership
before releasing the command. Model stop helpers share the server's ceiling.
No attempt is moved into a separate systemd scope. Ordinary service stop contains
the worker and every attempt child. Queue maintenance waits for exact cleanup,
including child cgroups; an elapsed stop deadline cannot release memory.

`join` and `install worker` accept:

- `--host-memory-budget-gb`: initial queue allowance input, default `0` for
  uncapped admission-only operation. The first report bootstraps queue policy;
  subsequent worker reports cannot change it. Configure a positive allowance
  with `host budget HOST --memory-budget-gb GiB` while drained/disabled.
  Both cards receive it through checkin and must agree before new starts.
  Retain all attempt/model charges, slack and unconfirmed releases.

- `--memory-ceiling-slack-gb`: extra memory per declared allocation; default
  `0.125` GiB (128 MiB). Slack counts against the aggregate budget. Unified
  machines enforce `memory_gb + host_memory_gb + slack` once; dedicated cards
  enforce `host_memory_gb + slack`.
- `--memory-swap-max-bytes`: swap allowance per attempt; default `0`. Swap is
  separate from the RAM budget; leave it zero for the no-swap envelope.

The kernel can OOM-kill an undersized attempt. The worker reads
`memory.events oom_kill` and reports `exceeded its <actual limit> GiB memory
ceiling`, kills remaining descendants, confirms release and removes the empty
kernel cgroup. This is a normal command failure: jobs become **failed**, with
no infrastructure loss and no automatic retry. It preserves existing checkpoint
files. A pinned model retains its pin and follows the existing bounded load
retry backoff; an unpinned failed load is removed. Ready model-server OOMs are
also detected at heartbeat and reported as failed after cleanup. Cleanup failure
retains RAM custody instead of granting a new allocation.

macOS, cgroup v1, missing delegation and foreground `myce join` retain
admission-only execution. Startup logs and `myce status` say exactly
`memory ceiling not enforced on this host (reason)`. Installer and doctor inspect
the live service's delegation and controller; startup can still be in progress,
so refresh worker status before claiming enforcement. Doctor reports this as an
advisory capability, not a required laptop prerequisite. If a qualified worker
cannot install/confirm an attempt's limit or membership, it refuses that payload;
it does not silently run it without a ceiling.

Managed `run.docker: true` jobs are limited by the Docker daemon, since container
processes are outside the worker service. The adapter supplies `--memory`;
optional per-container `memory_swap` supplies `--memory-swap` (total RAM plus
swap GiB), otherwise Docker defaults apply. It injects
`MYCE_MEMORY_LIMIT_BYTES`, and verifies the labeled container's HostConfig before
start and during reconciliation. Missing or changed limits refuse start or kill
the owned container; `State.OOMKilled` reports the same ordinary failure. An
unmanaged shell wrapper **must** pass the provided limit itself, for example:

```sh
docker run --memory "$MYCE_MEMORY_LIMIT_BYTES" \
  --memory-swap "$MYCE_MEMORY_LIMIT_BYTES" IMAGE COMMAND
```

Unmanaged/nested containers and Docker-backed catalog servers cannot be
certified by the native cgroup or inspected automatically; use the managed batch
adapter and its custody/label contract for qualified container work.

Preserve the conservative existing value as UNQUALIFIED input. Change queue
allowance only during drained/disabled maintenance after checking retained work.
Native capacity requires separately authorized simultaneous-envelope and
headroom measurement; laptop fixture values do not qualify native admission.

The budget bounds admitted attempt/server RAM ceilings, not every byte used by
the worker, environment builds, copies, Docker daemon or unrelated software.
Reserve measured overhead separately. Admission-only hosts do not provide a
kernel bound. GPU/device allocations that the driver does not charge to memcg
still require native qualification, especially on unified-memory hardware.
No configuration prevents unrelated programs from allocating their own RAM.

Optional CPU-only native check: on Linux with an already delegated cgroup v2
parent and the test runner in its `worker` leaf, set
`MYCE_TEST_MEMORY_CGROUP=/sys/fs/cgroup/.../OWNED_PARENT` and run
`go test ./internal/myce -run '^TestMemoryCeilingLinuxIntegration$' -v`.
It creates an owned 32-MiB child, attempts a 128-MiB allocation, checks kernel
OOM evidence and removes that empty cgroup. It skips without explicit delegation;
no fleet or GPU access is needed.

Production can stop lower-priority commands or drain unpinned models on another
card of the same host when that releases RAM; GPU reclaim remains card-local.
Pinned models and failed unloads remain protected. Reservations follow the
existing attempt, loss and restart reconciliation rules. Status shows host RAM
headroom/probe errors and a plain RAM waiting reason.

For several warm audio models, use `measure --warmup WARMUPS.json
--warmup-parallel N` with representative requests, stopped local join and the
exact worker spool. Loads stay serial; N bounds requests after all models load.
`--load-timeout 10m` can qualify slow startup (default 5m). Reports separate
load and warm-up seconds and record the combined sampled peak. Brief spikes
can be missed; real CUDA/XPU qualification remains necessary.

## Native payload isolation

New native entries and extension registrations default to a kernel boundary,
not same-UID `0700` alone.
See [native isolation](native-isolation.md) for the platform guarantee table and
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
`{work}/.tmp` and `MYCE_SCRATCH_DIR` to `{work}/.scratch` after authored
env/activation. Main and its own sidecars share both paths. Repeatable
`--native-read-root` and `--native-exec-root` worker/install settings add narrow
read-only model roots or read/execute runtime roots; filesystem, home, NAS and
installed Mycelium roots and protected-content overlaps are refused. #219's environment allowlist and
credential filtering remain in effect. Linux fallback does not protect all
metadata operations, including `chmod`; macOS protects targeted spool and
credential contents while unrelated host metadata/files remain visible.

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

An optional [per-host AppArmor profile](native-isolation.md#optional-administrator-action-per-host-apparmor-user-namespaces)
can authorize user namespaces for the installed binary while keeping
`kernel.apparmor_restrict_unprivileged_userns=1`. It is a separate privileged
action, not applied here: review the intentionally permissive profile and
kernel attack-surface cost, load with `apparmor_parser`, then restart and verify
the actual worker service. Never disable the global restriction as a shortcut.

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
the current worker's spool and identity. A completed conversion from per-device
workers leaves their historical spools intact, so their retained containers
need a one-time reconciliation:

```sh
myce reconcile-docker-retention --config /absolute/path/to/MACHINE_CONFIG
```

Run as the ordinary worker account. This command removes containers; it does not
stop or restart workers or containers. It verifies the completed
`MACHINE_CONFIG.m4-rollback.json` journal against the successor config, inventories
only its explicitly retired worker identities on the configured Docker endpoint,
and exclusively locks each retired spool. The successor may remain running.
Conversions that kept the same worker name are refused by design; clean up those
containers by hand with verified ownership and release custody, or leave them
as they are.
Removal uses the normal collector's exact attempt/worker label checks, matching
acknowledgement, elapsed retention, confirmed released custody, stopped state,
no-restart policy and private exit evidence. Removal never uses force.
Containers without matching `myce.attempt` and `myce.worker` ownership labels
are never removed; container names do not determine ownership.

The JSON report lists every inventoried container, whether removal was confirmed,
and a reason for each hold. Missing or invalid custody/acknowledgements, active
containers and incomplete evidence stay held. Earlier archived spools are not
searched or adopted; containers without reachable custody remain held and
reported. After successful removal, the normal collector also deletes that
attempt's retired-spool custody record, acknowledgement, result and cidfiles.
Private evidence files survive removal. If a multi-container removal partly
fails, the report confirms each ID Docker reported as removed and retains the
attempt's custody for retry. An operational/verification error returns nonzero
after collecting the other results; inspect the report
before retrying. The command never loads fleet credentials or contacts the queue,
and adds no cross-identity scan to the steady-state collector.

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


## CPU capacity and allowances (R1)

`myce join --gpu cpu` enrolls a GPU-less Linux/macOS worker without GPU probes.
Bare join/install selects CPU when supported GPU detection finds no accelerator;
a detected GPU with incomplete/ambiguous capacity still needs an explicit fix.
The CPU target reports logical CPUs and measured system RAM, with no fabricated
accelerator. `myce dev --gpu cpu --workers 1` exercises the real API/runner.

Set `--reserve-cpu-cores 1` (default) to leave logical cores for the owner.
`--cpu-budget-cores N` caps the remainder; zero uses detected cores minus
reserve. These options are also durable install settings. The existing
`--host-memory-budget-gb` and `--reserve-host-memory-gb` govern RAM.

The queue adopts the initial CPU allowance, then owns it alongside RAM:

```sh
myce host drain HOST --wait
myce host budget HOST --cpu-budget-cores 4 --memory-budget-gb 8
myce host resume HOST
```

Allow each worker view to acknowledge the policy; a mismatch withholds new
admission. Status shows free/admitted logical cores and unavailable-capacity
reasons. CPU/GPU views sharing `host_id` share both ledgers. Pending releases
retain cores/RAM until exact cleanup is acknowledged. One attempt per worker
remains the R1 execution limit. Core counts reserve capacity, with no CPU-time
kernel enforcement in R1. Existing Linux memory ceilings and macOS admission-only
semantics apply unchanged. Native Windows/WSL CPU qualification is later work.

## Environment cache capacity

Omitting `--environment-cache-bytes` auto-sizes from available bytes on the local
cache filesystem (the nearest existing ancestor for a fresh path): one quarter
of free space, bounded to 4–80 GiB and capped to retain 20 GiB free. Below 24 GiB
free, automatic sizing refuses; choose an explicit byte limit after checking
headroom. The limit is a ceiling, not a preallocation. Explicit CLI byte values
win. Saved-config upgrades migrate only the exact former 20 GiB default;
other saved limits stay unchanged. A historical explicitly selected 20 GiB
cannot be distinguished from that former default; pass it explicitly again to
retain it during an upgrade.

After a cache-capacity refusal, reconfigure the idle worker with the supported
`myce install worker --config CONFIG --environment-cache-bytes BYTES` path.
The worker reconsiders saved capacity failures when its allowance changes,
including failures written by older releases. Other preparation failures remain
cached. No state-file edit or cache deletion is needed. Preparation may be
admitted on the next heartbeat; successful preparation and qualification are
separate results.

One-shot NAS install/reconfigure validation allows 30 seconds per filesystem
operation. Successful operations advance validation without a shared total
deadline. Contended deletes completed in 8–15 seconds; the stall backstop allows
two 15-second intervals (waiting behind contended I/O, then completing the call).
An indefinitely blocked CIFS call therefore refuses after 30 seconds, while
completed operations taking at least 10 seconds report `NAS slow`, their operation
and elapsed seconds. These incident samples are not a supported-load percentile
or latency qualification. Mount inventory retains its
5-second bound, and installer subprocesses retain a 15-second bound per command.
Cancellation remains active throughout setup. Logs report operation and root
progress with elapsed time, without paths or mount credentials. A stalled syscall
stays owned until it returns, so retries cannot start overlapping NAS sequences;
refusal occurs before installed files or services change.

Background checkpoint availability returns within the 750 ms control budget.
After a successful observation, a new pending probe retains availability for
up to the existing 10-second network allowance, accommodating the measured slow
share without delaying heartbeats. Cold observations, completed failures and
probes still pending after that allowance fence checkpoint-dependent work.

Workers retain prepared environments for at least 24 hours after preparation or
last use (including qualification checks and completion of long-running jobs).
Active attempt/build locks also protect them. If protected entries fill the
configured limit, preparation refuses capacity rather than evicting them.
Evictions record the environment key and cache-limit reason in the worker log.

To reclaim unused prepared environments without admitting a new preparation,
run locally on the worker:

```sh
myce worker env prune
myce worker env prune --apply
```

The default is a dry-run. Each ready prefix is listed with its age since
preparation/last use, allocated size in bytes, queue reference state, use/build
lock state and recent-use protection. `--apply` uses the same eviction path and
24-hour minimum retention as pressure eviction. Locked, recently used, and
queue-referenced environments are preserved, including queued work, sidecars,
extension variants and attempts still awaiting physical release. Reference
state is queried again under the candidate's local locks before retirement;
an unavailable queue refuses deletion. The queue must support this command's
reference API. Shared downloads and unrelated retired prefixes are preserved.
Pruning does not auto-size preparation capacity or require its 24 GiB free-space
floor; both dry-run and apply remain available below that floor.

`--config FILE` selects an installed worker; `--environment-cache DIR` overrides
its cache directory. Without installed settings, supply that directory and queue
authority using `--queue`, `--joining-file` or `MYCE_TOKEN` as appropriate.
The summary reports `reclaimed_bytes`, measured as the decrease in allocated
blocks across prepared/retired environments and shared downloads, counting
hardlinked inodes once. Candidate sizes can therefore exceed reclaimed bytes.
Apply appends synced retirement intents and a final timestamped report to
`prune-audit.jsonl` in the cache root, including partial progress on failure.

Package downloads use persistent worker-wide `.downloads/uv` and
`.downloads/pixi` caches under the configured environment-cache root. These bytes
are excluded from the prepared-environment budget and have a separate 16 GiB
combined high-water limit. Before and after builds, excess downloads are pruned
with `uv cache prune --ci` and `pixi clean cache --yes`, while no Mycelium
installer uses the shared cache. If standard pruning cannot bring the cache
under budget, preparation fails with the remaining byte count. Active builds
can temporarily exceed this waterline; the existing free-disk guard still
applies. Pruning can require a future wheel download; installed environments
are preserved. uv retains its default clone/hardlink mode (an explicit
`UV_LINK_MODE` remains available); avoid symlink mode because pruning would break
installed environments. Existing per-environment caches are not migrated or
removed on upgrade.

uv builds set `UV_HTTP_TIMEOUT=60` and `UV_HTTP_RETRIES=3`, supported by the
pinned uv release, and log verbose download/cache activity. The build watchdog
samples log and shared download-cache growth every ten seconds. Five minutes
without growth cancels the exact recorded build process group, waits for cleanup
and retries once with the persistent downloads. A second stall fails preparation
with a diagnostic in the build log; cancellation never starts a retry. This
watchdog also covers pixi and custom build steps. It does not impose a total
build deadline on a build that continues making progress.

### Accelerator driver fault quarantine

Linux machine workers inspect the current boot's kernel journal on startup and
check for new records every five seconds using the injected worker clock.
`journalctl -k -b` runs as the worker account, without sudo; a readable `/dev/kmsg`
is the fallback. The account needs ordinary kernel-journal read permission (for
example, membership in `systemd-journal` where configured). If neither source is
readable, the worker logs `accelerator fault monitor unavailable`; monitoring
cannot detect faults in that state. No cheap qualified Apple source is currently
available, so Apple monitoring is skipped.

An xe/i915 fault or NVIDIA Xid matched to the device's PCI BDF sets its health
reason to `device_fault_xe`, `device_fault_i915`, or `device_fault_nvidia_xid` and
withholds its placement capacity. Other devices remain usable. The queue moves
affected running jobs directly to `queued` using the existing bounded
infrastructure-loss budget; their old exact attempts receive stop/kill directives
and retain custody until cleanup is acknowledged. Reassignment creates a new
attempt under the same job ID. This transition does not expose `stopping` or a
terminal job state when a retry remains available. Infrastructure exhaustion
still follows the existing terminal policy. Stale results cannot publish.

Quarantine persists in the local worker spool across worker restarts. A changed
Linux boot ID clears old quarantine; faults in the new boot can quarantine again.
After repairing a device, an operator can request a clear without stopping the
worker (replace the example directory with the configured worker state directory):

```sh
myce device clear-quarantine --state-dir ./worker-state --device 0
```

The command prints `clear_requested: true`; this confirms the local request,
not healthy capacity. The worker applies it on the next successful log scan,
provided no newer fault has replaced the acknowledged fault. Verify the device
reason is empty and capacity is available in `myce status` before relying on it.
Historical fault records are acknowledged so clearing does not immediately
replay the same fault. Raw kernel text remains local and is neither published
in status nor retained in quarantine state. Fault sources and family matchers
are injectable for laptop verification.


## Rolling fleet upgrades

> **Before deploying bounded status (#439), migrate preservation checks.**
> Default `GET /api/status` includes active/cleanup-pending jobs and only the
> newest 100 other terminal jobs. Before/after preservation, protected-job,
> history and curve-provenance checks must request `/api/status?detail=full`
> (Python: `client.status(full=True)`, CLI: `myce status --history --json`) or
> collect every `jobs=all` page through `jobs_page.next_before`. Existing complete
> `/api/inspection` collectors remain supported. Migrate private release-script
> copies before queue cutover; a passing pre-upgrade read does not prove the
> post-upgrade collector is complete. Curves remain included by default, but
> missing historical jobs must never count as preservation or confirmed release.

After publishing a signed release and approving it with `myce release approve`,
run `myce fleet upgrade` (or `myce fleet upgrade --release vX.Y.Z`). The command
uses the same saved installed authority as `myce status` and returns a durable
operation ID. It can exit immediately: the queue owns progress. Watch phases,
wait reasons and failures with `myce status`. An approval from before signed
envelope retention needs identical verified reapproval first.

For a queue hotfix, use
`myce fleet upgrade --release vX.Y.Z --queue-only --wait-production-idle`.
`--queue-only` selects only the queue service, including when workers share its
host or installed binary. It still drains that host and waits for fresh exact
custody; worker services are not restarted or required to support self-update.
Other hosts retain their maintenance policies and versions.

`--wait-production-idle` also works for a full upgrade. It pauses new production
batch assignments until queue running-image health is verified, and waits for
all assigned production attempts, retained release custody and production calls
across the fleet before queue restart. Existing work finishes naturally without
a deadline. All model calls/streams still settle at queue cutover. A stopped
operation keeps the admission fence until explicit recovery. Queued production
keeps its ordinary start/give-up deadlines; choose the maintenance window with
that in mind. Status includes both policy flags and the current wait reason.

Repeat the same flags when resuming: scope and idle policy cannot change during
an unfinished operation, including when selecting a newer fix-forward release.
After queue-only completion, a full same-release invocation creates a separate
operation for the workers without restarting the already-current queue.
These options require a queue version that implements them; they cannot change
restart behavior inside an older installed queue or helper.

Hosts update sequentially, with production hosts last. Started commands finish
naturally before their host restarts. At queue cutover new model calls pause and
existing calls/streams finish; batch attempts on other hosts reconnect unchanged.
The queue precedes its colocated worker. This implementation accepts the current
worker protocols only, so incompatible protocol migrations need a separate plan.

Each enrolled service fetches approved bytes through the authenticated queue,
checks its locally pinned publisher key, signature, size and SHA-256, preserves
its installed path/settings and restarts its existing service. Completion needs
a new PID/session, running-image hash, manager and enrollment health. Publisher
trust is pinned once during authenticated service enrollment in the private state
root; rotation is a local provisioning operation, never an update instruction.
A foreground queue or worker without saved service settings/runtime proof cannot
participate; install the capable release normally before the first fleet update.

A storage preflight failure is reported as `storage_preflight_failed`, including
when recovered from the durable helper receipt; arbitrary helper error text is
never exposed. A failed step stops the operation and preserves maintenance; later hosts remain
untouched. Repair the cause and explicitly rerun `myce fleet upgrade` to reconcile
that same operation. A rerun advances its recovery revision and rechecks the exact
old runtime and maintenance receipt before recovering an interrupted helper.
An active helper continues to report its result across reruns. Services already
running the approved bytes receive the same stable health checks without a restart.
It never blindly repeats a restart or resumes an operator's drain. For a stopped
operation, `--release` may explicitly select a newly approved newer release to
fix forward. Approval changes alone never retarget a running operation.

If an operator has replaced a stopped operation's drain, an identical rerun
keeps the stale receipt as a visible `maintenance_changed` conflict. Explicitly
selecting an approved newer release uses the current drained/disabled policy,
including its budgets and placement, without acquiring permission to resume it.
Exact release custody and stable health are still required; already-current
services are verified without restarting. If admission was resumed instead,
the newer operation is refused until the operator explicitly drains or disables
the host. No receipt or database edits are needed.

No automatic rollback, downgrade, forced job stop, SSH, driver, package or OS
changes occur. Offline or unknown custody stays a visible wait; elapsed time
never proves release. Native service/signing qualification and the first live
rollout remain separate from the laptop acceptance tests.

## Queue history retention

Maintenance retires terminal job rows after 168 hours (or longer configured
result/log retention), once all attempts are released, files have expired and log cleanup is
complete. Pending file cleanup transfers to durable per-hash ownership. Each tick removes at most 64 jobs. Keyed replay answers remain for
30 days after removal, followed by permanent expired-key digests; learned work
curves remain. Expired jobs disappear from listing and ID lookup. See the
[retention contract](design-proposal.md#submission-identity-and-cancellation-80).

Deleted SQLite pages are reused. Retention bounds full history by age, not total
bytes: permanent key digests and unresolved custody still accumulate. To reclaim
physical disk space, stop the queue through normal maintenance, take a verified
local backup, and compact the stopped local database using SQLite `VACUUM`.
Never open a NAS copy with SQLite or compact the running queue. Check free space
first: compaction needs room for a second database. Preserve the verified backup
until the compacted database passes `PRAGMA integrity_check` and reopens normally.

### Verify the loaded environment catalog

`myce env list` returns a JSON array; `myce env get NAME` returns one entry
(or `environment_unknown` when absent). Both use installed client authority;
`--config`, `--joining-file`, `--queue` and `--token` precede NAME when supplied.
The authenticated GET endpoints are `/api/environments` and
`/api/environments/{name}`. They do not prepare environments or submit work.

Each entry has its name and sorted platform recipes with effective backend
(`uv`, `pixi`, or `binary`), immutable recipe `key`, and `input_digests`: SHA-256
of loaded lock/manifest bytes under canonical labels. Compare these with the
expected inputs to verify registration, even after the original files change.
Raw recipes, commands, file contents and local paths are omitted. Binary recipes
report the digest of the `binary.sha256` identity input. See the
[entry schema](schemas/environment-v1.schema.json).

### Preserve learned curves across a release

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

Removed keys, resets, backward clocks, changed measurements without new
samples, missing/ambiguous provenance and unexplained final values fail closed.
Older jobs have no such learning ledger proof: an unchanged curve still passes,
but an unproven change stops the release for investigation. Reference sweep or
OOM invalidation changes are not accepted job samples and also stop this guard.
Ledger records follow existing job history retention; collect evidence before
it expires. No release, install or fleet operation is performed by the guard.

## Migrate storage guards to automatic admission

This is an explicit queue operation, separate from saving installer settings,
retiring an `ExecStart` override, upgrading clients, or enabling storage protection.
The default remains shadow. There is currently **no production native
quota/confinement backend**: installation alone cannot satisfy the qualification
blocker. Native setup, denial/isolation qualification and fleet activation require
a separately authorized operation. Laptop fake backends never qualify a host.

During that operation:

1. Keep the effective cap and reserve unchanged while qualifying independent
   worker custody, production staging, complete write confinement and measured,
   enforced SQLite/WAL/log/sync/checkpoint/metadata backing. Inspect actual
   ext4/XFS quota/features or fully allocated bounded filesystems on Linux;
   inspect actual APFS quota/reserve behavior on macOS. A filesystem name,
   directory quota, sparse maximum, sampled kill or unreachable host is not
   qualification. Reconstruct exact allocations/custody after a queue and worker
   restart; restart requires fresh backend verification. Upgrade destination
   workers and clients, including Python, to the versioned custody/result contract.
2. Start the supported observer using installed queue client authority:

   ```sh
   myce file admission observe --config QUEUE_CONFIG
   myce file admission status --config QUEUE_CONFIG
   ```

   The first observation saves the **effective running** `file-store-bytes` and
   `file-store-production-reserve` values in the queue's existing SQLite state.
   Thus it preserves saved installer arguments or the overriding drop-in's
   arguments without parsing/removing the drop-in or rewriting the config. Compare
   `saved` with the effective unit/argv. Until activation those values remain active.
   The installer/drop-in retirement procedure above still applies independently.
3. Existing queue maintenance now samples shadow decisions automatically; `observe`
   can also request a sample. It does not collect/delete data. Status reports
   comparisons, disagreements, streak timestamps, `consistent_seconds`,
   `required_seconds`, `period_job`, completed cycles and precise blockers under
   `file_store.admission.migration` or the dedicated command. Reading status does
   not earn evidence. The required period is the longest measured
   queued-to-verified-delivery-to-authorized-collection lifecycle in retained
   evidence, including actual waiting/retention, rather than a chosen number of
   hours. At least one whole lifecycle must have its queued decision compared in
   the current consistent streak. Idle time cannot qualify it. A disagreement,
   restart, clock reversal or changed installed backing/control/worker boundary
   requires new evidence; free-space changes on the same boundary do not.
4. Preserve legacy shared experiment outputs, then release them only through the
   existing owner release/retention/collector authority. Wait for actual collection,
   not just a successful destination probe or an expired job flag. Incompatible
   live writers and remaining shared experiment backlog report migration incomplete.
   Frozen/keyed policies, six-hour publication grace, 168-hour legacy retention,
   checkpoints, registry/shared references, unknown imports and unique data keep
   their existing protection. Do not edit stored policies or delete unique bytes
   to clear a blocker. Inspect configured operator and production-project alert
   routes; unconfigured notifications remain visible events, not sent messages.
5. When status reports `ready: true`, explicitly activate:

   ```sh
   myce file admission activate --config QUEUE_CONFIG
   myce file admission status --config QUEUE_CONFIG
   myce status --json --config QUEUE_CONFIG
   ```

   A single SQLite transaction sets the existing admission latch and marks the
   saved cap/reserve inactive together. Uploads/submissions serialize with cutover.
   Active `limit_bytes`, `production_reserve_bytes` and the historical percentage
   `floor_bytes` are zero; `free_bytes` comes from fresh physical capacity minus
   remaining accepted upload lengths, unused backed reserves and unwritten output
   commitments. Zero limit here means no artificial ceiling, not unlimited disk.
   The measured enforced control boundary replaces the old 1%/256 MiB floor.
   Per-attempt byte/inode boundaries still deny growth between observations.
   Retained experiment spools remain charged to independent worker capacity;
   neither a healthy destination nor stopping compute permits a shared spill.
6. Retire external daily writer agreements only when
   `manual_writer_limits_retirable` is true, backed by actual verified delivery
   and collection progress. Keep the previous agreement record for rollback;
   these human agreements were never queue settings and the command does not
   edit them. Keep attribution and owner tiers. Production/input exhaustion can
   still hold with an exact deficit and operator/project alerts; cap removal does
   not make protected data reclaimable or provide unlimited production space.

Rollback is explicit and does not delete data or undo delivery:

```sh
myce file admission rollback --config QUEUE_CONFIG
myce file admission status --config QUEUE_CONFIG
myce status --json --config QUEUE_CONFIG
```

Verify `saved_settings_active: true` and the **exact** saved cap/reserve in file
store status, then reinstate the unchanged external writer agreements if retired.
Already stored bytes may exceed the restored cap; new allocations hold until they
fit, while existing data stays protected. Saved settings and active/inactive mode
survive reinstall/restart even when service arguments contain different defaults.
Backend verification is never renewed by restart, and rollback cannot make an
unverified retained boundary safe. Preserve/repair its backing before resuming work.
Do not remove the SQLite migration record to change settings. A new activation
following rollback requires a new measured observation streak.

The evidence replacing fixed 100/150/185 GiB ceilings is fresh physical capacity
and persisted enforced commitments; structural worker custody replaces the flat
16 GiB reserve; observed delivery/collection and tier placement replace daily byte
agreements. Largest comparable accepted peaks plus observed range/prediction error
replace mandatory output guesses (an optional cold output prior remains). Operators
still set roots/destination identities, preservation policy, owner tiers and alert
routes. Backend provisioning supplies measured limits and bounded exploration;
agents do not choose per-job production byte budgets.

Laptop acceptance uses injected clocks, HTTP/SQLite, CPU-generated small files
and fake boundaries for the original 100/95/10 and 4/96/outage/6 schedules, the
October 31 zero-reclaim incident, burst denial and production surge. The opt-in
`TestStorageIncidentReplayNativeBurstAPFS` creates/detaches one disposable fully
allocated APFS image and proves real bounded-volume ENOSPC with independent
publication/WAL progress. It does **not** qualify per-attempt aggregate quotas,
inode denial, arbitrary sidecar confinement, APFS native reserves, Linux backends
or fleet hosts. Full integrated and native backend gates remain deployment work.


## Automatic joining

Run `myce invite` on an installed, authorized hypha and paste its output on the
new machine. The earlier [setup steps](#4-add-each-other-machine) cover the first
invitation; [signed release publishing](#publish-and-approve-an-onboarding-release)
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
Mycelium does not change WSL networking or firewall configuration. Windows host owner-presence and power signals use the
[host bridge](operations.md). Joining runs no throughput benchmark
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
[benchmark guide](development.md) for its bounds and cache behavior.

Discovery, pinned authority and profile checks are covered by laptop fixtures.
Native cross-machine evidence and outstanding boundaries are owned by
[#48](https://github.com/1kuna/mycelium/issues/48); WSL2, installed-service,
login/reboot and native backend checks remain separate. See
[development](development.md#native-acceptance-checklist).

## Publish and approve an onboarding release

> **Release checklist: migrate preservation collectors before queue cutover.**
> With bounded status (#439), preservation and curve-provenance checks must use
> `/api/status?detail=full` or complete `jobs=all` pagination, including private
> release-script copies. Default status can omit old completed jobs even though
> curves remain present. Existing complete `/api/inspection` collectors remain
> supported. See [the upgrade requirement](operations.md#rolling-fleet-upgrades).

The queue administrator provisions a trusted publisher HTTPS directory and an
Ed25519 public key independently of the release manifest. The signing key stays
with the publisher in a mode-0600 PKCS8 PEM file. Never distribute it with a release.
No release tooling existed before this flow; the script uses Go and standard
Ed25519/SHA-256 rather than adding a distribution service.

On an Apple Silicon macOS build host with Go and Xcode command-line tools:

```sh
scripts/release.sh v3.1.0 OUTPUT_DIR PRIVATE_SIGNING_KEY
```

For native LaunchAgent distribution, set `MYCE_DEVELOPER_ID` to the provisioned
Developer ID Application identity. The build embeds Mycelium's stable identifier,
Local Network usage description and Bonjour service declaration in the single
Darwin executable. The script applies and verifies a timestamped hardened-runtime
signature before manifest generation, so Ed25519 authenticates the codesigned
bytes. Linux artifacts use the same CGO-disabled builds and are not codesigned.
Tests replace the signing tool and never access a real keychain.

When the Ed25519 key lives on a different publisher host, use
`sh scripts/build-release.sh VERSION OUTPUT_DIR` on the Mac, then generate and
sign the manifest on the key's host without modifying the artifacts. Never copy
the private key to the build host just to run the combined script.

Versions must be semver values, monotonically increasing for normal upgrades.
Build metadata (`+build`) does not change version precedence.
The shared `ReleaseTargets` data drives validation, manifest creation and
`myce release build-targets`, which the build script consumes.
This builds `myce-linux-amd64`, `myce-linux-arm64` (CGO disabled), and
`myce-darwin-arm64` (CGO enabled with the Apple SDK). The native Mac binary must
report real owner-sensing support. `myce release manifest` checks executable
headers, the Mac release version/protocol/sensing, and writes a signed
`release.json` plus `SHA256SUMS`. Publish the artifacts unchanged at an HTTPS
directory that serves them directly without redirects. Mac distribution signing,
Gatekeeper/notarization and actual launchd login/network behavior need their own
native release acceptance; never disable those controls to make onboarding pass.

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

See [joining](operations.md) for the user flow and successful enrollment states.

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

## Polite part-time hyphae

Dedicated hyphae keep the existing default: offer resources whenever awake.
For a daily-use machine, add `--daily-use` to its usual `myce join` command.
No GPU, power, fan, sleep or screen settings are changed. No input is recorded.

```sh
myce join --daily-use --max-jobs 1
```

Daily-use hyphae take work after **five idle minutes**, on confirmed external
power when battery-equipped, with a known safe heat signal. Batteryless desktops
need a confirmed batteryless power report. Plugging in never overrides owner
activity. An unknown required signal means waiting with a reason in CLI/HTML
status and `capability_profile.availability` in the status API. Aggregate idle
seconds, power source and thermal state are reported; input contents aren't.

A detected laptop needs external power even in dedicated mode. `--laptop`
explicitly enforces that rule when automatic battery detection is unavailable.
Use dedicated mode for batteryless servers; daily-use mode for personal machines.
The battery-equipped classification is retained throughout a worker session
if a subsequent power read fails. Daily-use flags are available on foreground `join`; installed services can
pass them through `service ... -- join`. Installation alone is not an opt-in.

Signals refresh every five seconds independently of queue connectivity.
Required readings older than 15 seconds withhold work. A sampling gap, including
sleep, starts a fresh five-minute awake window; the worker never prevents sleep.
Owner return, battery use, unsafe heat or unknown required signals stop admission
locally and through the queue. Resumable commands get SIGTERM and up to ten
seconds to checkpoint, then are killed if needed. Non-resumable commands stop
immediately and later restart from scratch. These planned stops don't spend the
infrastructure-loss retry budget. Checkpoints use explicit NAS roots or the
[verified file transport](files.md) when no shared storage is configured.

Model loads and calls also stop when availability is withdrawn. Pins never
override owner availability. Engines stay reserved until cleanup confirms
release; queued callers may retry another hypha. Withholding remains temporary:
fresh eligible signals allow new work without rejoining.

`--max-jobs N` is the **one optional per-machine limit** (default zero: unlimited).
It counts simultaneous batch jobs plus in-flight model calls across all workers
with the same `--host-id`. A stopping batch continues to count until acknowledged.
Ready idle model servers aren't jobs. If workers report different positive
limits, the smallest is used; configure the same limit on every worker of a
physical machine. Lowering a limit blocks new work rather than killing existing
jobs. Memory capacity remains an independent admission constraint. This limits
concurrency, not watts, GPU percentage, fan speed or noise.

## OS signals and limits

- **macOS:** the binary links CoreGraphics' combined-session aggregate event age,
  IOKit's power-source snapshot, and Foundation's process thermal state. It needs
  no event tap, keylogging, screen capture or sensor permission. Daily work
  withholds at fair/serious/critical; nominal must persist for 60 seconds after
  a heat hold. Building on macOS needs cgo and the usual Xcode command-line SDK.
  A macOS binary built without cgo reports unknown signals and waits in daily mode.
  The aggregate query covers the worker's login session; fast user switching
  and coverage of other logged-in users remain unverified.
- **Linux:** logind, queried through `loginctl`, covers every listed user session.
  Work waits five minutes after every user session's idle hint, which is more
  conservative than claiming it is a last-input timer. Hint age uses the same
  OS monotonic clock as logind, rather than boot uptime that includes sleep. No user session, an
  uncovered session, denied access or missing desktop idle hints means waiting.
  Power comes from read-only kernel `power_supply` attributes. Heat comes from
  recognized CPU/GPU hwmon sensors with valid max/critical limits and no faults.
  All discovered relevant sensors must be usable. A max alarm/limit holds work;
  a critical-only sensor uses critical minus 10°C. Recovery needs readings at
  least 5°C below the guard (or a more conservative supplied hysteresis) for
  60 seconds. Unknown heat holds work. The recognized sensor list is intentionally
  small; unsensed accelerators and these margins are not hardware-qualified.
- **WSL2:** owner activity, power and physical heat must come from the **Windows
  host**, never the Linux guest. The [per-user Windows host bridge](operations.md)
  supplies authenticated host idle/power over mirrored-network IPv4 loopback,
  with verified single-console-session coverage or explicit unknowns. Missing,
  stopped or unauthenticated helpers never fall back to guest signals. Windows
  CPU thermal coverage remains **unknown**. An externally powered desktop
  explicitly joined with `--daily-use --desktop` may use nominal measured GPU
  heat, with status **CPU thermal coverage waived for desktop**. A detected
  battery vetoes the waiver; without `--desktop`, CPU coverage stays required.
  Use `--daily-use --laptop` for a laptop; the flags are mutually exclusive.
  Native session, logon, networking and install behavior require the separate acceptance plan.

Thresholds follow #25's research where compatible with #43. Unlike the research
note's proposed missing-heat exception, missing required heat **withholds** daily
work, with the explicit-desktop CPU waiver described above. These OS observations
aren't a
promise that a machine is silent, that a motionless owner is absent, or that all
physical components have thermal coverage. No quietness claim is made.

Laptop fixtures prove signal policy and fake-clock decisions. Actual OS owner,
power and thermal signals require native qualification; see
[development](development.md#native-acceptance-checklist).

## Shared interactive hosts


These hosts can run fitting light work through Mycelium. Use the opt-in
[shared-host profile](operations.md) to combine measured interactive RAM
reserves, #353 external peaks, native command nice=10, positive CPU/RAM budgets
and last-resort placement. `myce host profile HOST --shared=true
--interactive-reserve-gb GiB` requires a drained/disabled, fresh quiescent host;
`--shared=false` removes the profile without resuming admission. Measured OS
memory pressure or foreground scheduling latency stops work through planned
checkpoint/requeue with hysteresis. Linux execution cgroups cap aggregate CPU
time. macOS applies `setpriority(PRIO_DARWIN_PROCESS, pid, PRIO_DARWIN_BG)`
(equivalent to `taskpolicy -b`) to gated main payloads and native sidecars.
Children inherit background CPU/I/O scheduling and lower timer/network priority;
worker control stays outside background mode. Hosts without the profile retain
their existing policy. macOS also caps aggregate declared cores at the CPU allowance, and clamps common runtime thread
counts to each command's declared cores. Payloads can ignore those thread hints:
aggregate CPU time and RAM ceilings remain unenforced on macOS. See the
shared-host guide for thresholds and qualification limits. To compare nice-only
and background scheduling through an isolated CPU dev worker, build the CLI and
run `python3 scripts/measure-shared-host.py --myce ./myce`. The canary uses two
CPU children and one second of foreground samples per mode; each payload
self-expires after three seconds to bound interference if the probe fails.
It measures a light workload, not saturation or a latency guarantee. Existing
exclusions, drain ownership and storage fences apply.


An orchestration machine, developer workstation or interactive server is usable
compute capacity. Opt it into the shared-host profile when protecting its owner
matters more than throughput. Any number of hosts can use it. Agents submit
portable light work normally; the queue chooses an ordinary fitting host first
and uses a shared host immediately when ordinary hosts cannot admit it.
Existing project restrictions, priorities, exclusions and maintenance still win.

## Supported work

The queue stores `policy.shared_host.interactive_reserve_gb` per physical host.
Enabling it requires positive CPU/RAM allowances and fresh quiescent custody
while drained or disabled. This avoids applying a new execution-priority policy
to already running processes. It sets the existing `last_resort` policy; that
flag cannot be cleared while the profile is active. Profile removal leaves
placement, budgets, independent exclusions and drain/disable in place.

The profile admits native CPU commands with already prepared environments,
including CI/tests and small builds. It withholds accelerator work through the
existing `kind=gpu` exclusion matching path, including future devices, without
creating or editing an operator's selector. Explicit selector exclusions remain
independent. Docker, extension execution, new environment/extension preparation,
model loading/copying and onboarding benchmarks are withheld on this profile
until their execution budgets and priority are covered. A job's own shell can
still compile code: that work inherits its command's priority.

Commands and native sidecars receive nice >= 10 before their execution gates
open. macOS additionally applies
`setpriority(PRIO_DARWIN_PROCESS, pid, PRIO_DARWIN_BG)` (equivalent to
`taskpolicy -b`) to every gated supervisor and member. Because a successful external
request does not always leave the payload at its requested priority, each
profiled payload runs through the system `/usr/bin/nice -n 10` and
`/usr/sbin/taskpolicy -b` after its gate opens, establishing nice >= 10 and its
own inheritable background policy before executing the command. The nice
increment can further lower inherited priority; it never raises it. This covers ordinary
commands, native main commands and sidecars, including their descendants.
It lowers CPU, I/O, timer and network priority. Worker heartbeats and queue
control retain their original priority. A failed external background-policy
application logs a warning; the payload still establishes its own policy.
If the system helper cannot establish policy or execute the command, that
execution fails rather than launching the command without its policy.
The existing physical-host CPU ledger caps aggregate
**declared cores** at `cpu_budget_cores`. Common Go, OpenMP, BLAS and Rayon runtime
thread-count variables are clamped to each command's declared cores, including
sidecars. A payload can ignore these variables or start more processes.

Linux installed workers use delegated cgroup v2 CPU controls. Generated/manual
units delegate `memory cpu`; the worker remains in its existing `worker` leaf.
Only attempts enter the sibling `shared-execution` subtree, with `cpu.weight=10`
and `cpu.max=CPU_BUDGET_CORES*100000 100000`. The quota aggregates main commands,
sidecars and descendants across attempts. Existing per-attempt/member memory
limits and exact cleanup custody remain underneath it. Controls are read back
before admission and rechecked in the bounded worker observation lane. Missing
CPU delegation withholds production admission. Aggregate containment requires
one machine worker per physical host; legacy multiple workers are withheld.
The dev harness explicitly reports admission-only CPU simulation on Linux.

macOS has no qualified aggregate CPU-time containment backend here. Status says
`background task policy; declared-core cap; CPU time unenforced`. Background QoS,
runtime concurrency hints and declared reservations protect fitting cooperative
work; they are not a hard share or stall-proof guarantee. macOS RAM ceilings
also remain unenforced. No profile is enabled by installation or upgrade.

## Measure the memory reserve

Use [external-memory history](status.md#non-mycelium-ram-history)
from #353: physical-minus-reclaimable RAM minus simultaneous worker-tree RSS.
Observe representative overlapping agents, interactive apps and external work;
check sample coverage and bucket times. A short window is partial evidence,
not a measured week or a future bound. Do not add another OS/queue baseline:
those processes are already in the residual. RSS can overcount shared pages,
miss compressed memory and miss spikes, so this is admission evidence rather
than hard isolation.

Measure the **additional** RAM needed by representative owner activity beyond
the retained external peak, using the same observation method before/during that
activity. Supply that positive increment as `--interactive-reserve-gb`. If the
activity was already observed, retain an explicit allowance for another such
burst. Record the workload and coverage with the sizing decision. There is no
universal fixed reserve or machine-name rule. Until that measurement exists,
leave the host drained rather than inventing capacity.

For each admission the protected reserve is that measured additional burst.
The historical allowance is physical RAM minus the seven-day external maximum
minus the protected reserve, clamped at zero and rounded down to 0.25 GiB.
The peak already includes observed external growth; scaling that entire baseline
by another percentage does not measure the next interactive burst. The generic
#353 dry-run suggestion keeps its 25% growth guard for sizing without a measured
burst; it is separate from this opt-in profile's measured reserve.
Full reservations consume the historical allowance. It can only
reduce the operator's configured RAM budget, never raise it. Live raw available
RAM minus the protected reserve and running attempts' remaining growth is also
an upper bound. Existing after-reserve live availability remains another upper
bound, so the old reserve is not subtracted twice. Missing live/history evidence
or a worker policy mismatch withholds new work. History ages with the injected
queue clock; no bulk probe or file work occurs under the queue mutex.

Immediately before execution the worker rechecks raw live RAM minus the same
configured interactive reserve, bounded by its existing after-reserve availability.
It cannot credit sampled RSS as available memory.
The queue still enforces the historical bound. Shared-host status reports the
configured reserve, acknowledgement and admission reason. A full reserve can
leave zero admitted RAM; the existing drain remains authoritative regardless.

## Configure deliberately

Upgrade the queue and workers together before using the additive check-in fields.
On an authorized host, inspect `myce host status HOST` and the history first.
The following placeholders are measured values, not defaults:

```sh
myce host budget HOST --dry-run # Generic sizing advice; retains the 25% guard.
myce host drain HOST --wait
myce host budget HOST --cpu-budget-cores MEASURED_LIGHT_CORES --memory-budget-gb MEASURED_ALLOWANCE_GIB
myce host profile HOST --shared=true --interactive-reserve-gb MEASURED_BURST_GIB
myce host status HOST
# Resume only when authorized to release this drain.
myce host resume HOST
```

`myce host profile HOST --shared=false` removes the profile while drained or
disabled. It never resumes admission or clears last-resort placement. API clients
POST `action: profile` and `shared_host: {interactive_reserve_gb: N}` to the
existing maintenance endpoint; zero explicitly removes the profile.
Workers acknowledge the exact profile through check-in before new admission.
`myce dev --gpu cpu --workers 1` supports these same API/CLI operations and runs
real commands; no GPU, production service or fleet edits are required.

A checkpoint-unavailable warning remains a separate storage constraint.
Resumable/checkpoint-dependent work waits under the existing contract. A profile
does not repair storage or authorize clearing maintenance.

## Pressure response

Only shared-profile workers collect pressure samples. A bounded background lane
reads `kern.memorystatus_vm_pressure_level` (the macOS memory-pressure notification
level) or Linux `/proc/pressure/memory` `some avg10`, then measures twenty normal-
priority 5 ms timer wakes. The maximum lateness measures scheduling interference,
including competing owner/agent work, without recording apps or owner input.
It cannot attribute contention to a particular process. Probes never run under
the queue mutex or synchronously block heartbeat delivery for their full duration.

Thresholds and recovery bands:

| Signal | Yield | Recover |
| --- | --- | --- |
| macOS memory level | warning (2) or critical (4) | normal (1) |
| Linux memory PSI `some avg10` | >= 1% | <= 0.1% |
| Foreground-class wake lateness | >= 20 ms | <= 5 ms |

macOS uses the OS's pressure warning rather than inventing a free-RAM percentage.
The PSI bands represent 100 ms / 10 ms of stalled time per ten seconds. They are
conservative latency budgets; native Linux interference calibration remains
unverified. The wake bands use a 5 ms recovery allowance and a 20 ms yield threshold;
20 ms exceeds a 60 Hz interaction frame. A short local canary observed no
foreground starvation, but cannot establish a universal latency guarantee.
Native pressure-stress calibration and representative owner qualification remain
outstanding; repeat the probe for the intended workload before enablement.

Samples refresh at the existing 5-second owner-signal cadence and expire at its
15-second freshness bound. Worker-local age is translated to queue receipt time;
worker wall timestamps cannot renew evidence. Missing, malformed, failed or
stale observations remain **unknown** and withhold new work. Unknown alone does
not stop running work or clear a pressure latch. A fresh low sample for every
signal clears the latch; middling samples retain it. There is no extra recovery
sleep. This amplitude hysteresis avoids stopping/restarting at one threshold.

On measured pressure, the queue withholds new work on the physical host and
attaches `shared_host_pressure` planned-stop provenance to each exact running
attempt. The existing preemption path grants resumable work its configured stop
grace (normally 10 seconds); nonresumable CPU work stops immediately. Existing
checkpoint preparation resumes the same logical job and submission key, without
infrastructure retry/loss charges. Independent command/OOM failure, cancellation,
stale results and exact cleanup acknowledgement retain their existing precedence.
Elapsed grace never proves release. Status exposes the sample, latch reason,
admission diagnostic and actual CPU enforcement mode.

The response can cost one sampling interval plus checkpoint grace, and pressure
can stop useful progress. PSI's averaged recovery may withhold capacity after
stalls end. Larger RAM reserves and a smaller CPU allowance reduce interference
but admit less work; a Linux hard quota also costs throughput while the owner is
idle. Keep existing operator drains authoritative. Docker, extension execution,
new environment/extension preparation and models remain withheld; this change
does not broaden execution eligibility or qualify their pressure handling.

## Qualification and laptop checks

`python3 scripts/measure-shared-host.py` compares a normal-priority foreground
5 ms wake probe and fixed CPU burst at baseline and beside two CPU-bound
background commands. It runs for three seconds per phase and reaps its own
commands. It neither installs services nor enables a profile. This bounded
canary does not qualify all-core saturation, hostile payloads, memory pressure
or Linux containment. Check local headroom before running it; use representative
owner work for native qualification. [#389](https://github.com/1kuna/mycelium/issues/389)
tracks that acceptance separately.

Laptop regressions use fake-clock yield/recovery/stale samples and aggregate CPU
admission/cgroup files. `TestSharedHostDevPressureCheckpointRequeueAndCancel`
starts the real `myce dev` API/worker/command runner with injected OS pressure and
real local checkpoint copying. It verifies checkpoint, planned requeue, resume on
attempt 2 and cancellation/release. `TestSharedHostDevCommandPriority` verifies
real main/sidecar nice values, concurrency hints, token filtering and unchanged
worker-control priority. No GPU, Docker or fleet changes are involved.

## Windows host signals for WSL2

The `myce-host-bridge` companion runs as the Windows user at interactive logon.
The Linux `myce` worker stays in WSL2. The companion reads aggregate idle time
and actual system power state; it never reads input contents, raw input devices,
window contents, process/application names, screenshots, or user names. Session
IDs and connection states are checked locally and are never sent to WSL or the
hub. There is no service, driver, administrator requirement, or firewall change.
Elevated invocation is refused.

**Thermal coverage remains conservative.** The helper queries the driver-installed
Windows system-directory `nvidia-smi.exe` (never PATH/current-directory lookup):
`--query-gpu=temperature.gpu,temperature.gpu.tlimit --format=csv,noheader`.
All GPU rows must parse; missing/unsupported values remain unknown. NVIDIA
T.Limit is signed **remaining headroom**, not an absolute ceiling: at/below zero
means serious, below five degrees means fair, otherwise nominal. This follows
the Linux probe's five-degree recovery margin; the existing 60-second cooling
dwell still applies. See [NVIDIA temperature semantics](https://docs.nvidia.com/deploy/nvidia-smi/index.html#temperature).
The query is hidden, bounded by a 750 ms deadline and invoked only when sampled.

GPU heat appears in `gpu_heat`, with `heat_coverage=gpu-only`. It cannot certify
CPU/host thermal coverage; host `heat` remains `unknown`. On an externally
powered desktop explicitly declared with `--desktop` (and no detected battery),
daily admission uses GPU heat and reports **CPU thermal coverage waived for desktop**. Laptops
still wait with **host CPU thermal coverage unavailable**. Unknown power,
unknown owner activity, and unknown or hot GPU heat continue to withhold work.
Generic ACPI thermal zones do not establish CPU coverage; no nominal CPU
reading is inferred from an unidentified zone. No driver, service, third-party
sensor install or guest fallback is required for the desktop exemption.

## Build on a Mac or Linux laptop

```sh
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build \
  -ldflags='-H=windowsgui' -o myce-host-bridge.exe ./cmd/myce-host-bridge
go build -o myce ./cmd/myce
```

This small Go executable intentionally imports only `internal/hostbridge` and
Windows syscall wrappers. The queue and command runner use Unix process APIs;
the Windows companion does not cross-compile that whole runtime or run jobs.
The GUI subsystem prevents a terminal opening at each Windows logon. It has no
window, UI, input hooks, or access log. Use `Start-Process -Wait -PassThru` to
check install/uninstall exit codes with a GUI binary; errors do not have a
console. A console build without `-ldflags` is useful for troubleshooting.

## One-step install and uninstall

Stage `scripts/windows-host-bridge/*` and the Windows executable together in
`%USERPROFILE%\Downloads\MyceliumHostBridge-Setup`. From the regular Windows
desktop double-click **Install.cmd**; double-click **Uninstall.cmd** to remove
the installed helper. Setup checks each binary exit code, starts the installed
helper immediately, and leaves a failed window open. Do not run as administrator.
The original downloaded executable must remain outside the installed directory.

PowerShell one-line alternative:

```powershell
& "$env:USERPROFILE\Downloads\MyceliumHostBridge-Setup\Setup.ps1" -Action Install
```

For an authorized remote operator using the same account as the logged-on owner:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "$env:USERPROFILE\Downloads\MyceliumHostBridge-Setup\Remote-Setup.ps1" -Action Install
```

This uses the standard Windows Task Scheduler COM API: InteractiveToken (no
password) and LUA/limited run level. The one-shot task runs the **same Setup.ps1**
in the existing desktop session and is deleted after completion. Aggregate
receipt fields verify exit code, session and elevation; no credential is logged.
The exact task name is `MyceliumHostBridge-Setup`. Setup deletes it as its
last step on success or failure, even after a launcher timeout. A retry first
removes an inactive leftover with that exact name and refuses a running or queued
setup; it never kills an active installer. Replace Install with Uninstall for
either command. Downloads files and aggregate receipts are intentionally retained after uninstall.
No logoff, service, persistent task, administrator install or settings change
is required. The helper also refuses installation outside the covered console.

Installation creates only that previously absent directory with the executable
and `bridge.json`, plus the single `MyceliumHostBridge` value in
`HKCU\Software\Microsoft\Windows\CurrentVersion\Run`. It refuses an existing
directory or startup entry. Before writing the random 256-bit credential, the
directory gets a protected DACL granting only this user's SID access; children
inherit it. No credential is put in the Run value, arguments, output, or fleet
token. Installation does not start a session-0 helper, change WSL, or alter any
other startup values. Windows Startup policy can disable/delay Run entries;
natural logon operation needs native verification.

Keep `bridge.json` private. Do not print/copy it into a report, issue, shell
history, job environment, or repository. Programs with the same Windows user
rights (including that user's WSL/root processes) can read it: this is a local
user trust boundary, not isolation from hostile code running as the owner.

## Connect from WSL2

The bridge listens **only** on IPv4 `127.0.0.1:47353`. WSL2 **mirrored** networking
is required for WSL-to-Windows IPv4 loopback. NAT fallback, a LAN listener,
portproxy, firewall exceptions and administrator changes are not provided.
If loopback is unavailable, work waits.

Set `MYCE_HOST_BRIDGE_CONFIG` to the WSL path of the installed Windows config
(using the actual local profile path privately):

```sh
export MYCE_HOST_BRIDGE_CONFIG='/mnt/c/Users/<owner>/AppData/Local/MyceliumHostBridge/bridge.json'
./myce join --daily-use --laptop --max-jobs 1 --joining-file '<private invitation path>'
```

For a desktop, use `--desktop`; it is mutually exclusive with `--laptop`.
Without `--desktop`, CPU coverage is required. Detected battery hardware still
requires CPU thermal coverage and confirmed AC.

The path is explicit to avoid guessing Windows user/session identity. WSL reads
the file through its existing Windows filesystem access. No credential is
passed in argv or HTTP. Each request has a random nonce and HMAC-SHA256 request
authentication; the response signs the nonce and entire body with a separate
direction tag. A replayed response, impersonator, modified response or wrong credential
fails closed. Requests are read-only; request nonces are not an access-audit or
server-side replay ledger. The loopback transport relies on OS isolation from
packet capture by other users; same-user code and administrators are trusted. Proxies, redirects, DNS hostnames and non-loopback config addresses
are rejected. Requests have a 1.5-second deadline and 4 KiB response limit.
Only `GET /v1/signals` is exposed. No remote control/stop endpoint exists.

Each authenticated request samples fresh signals, never a cached observation.
Windows `ProcessIdToSessionId`, `WTSEnumerateSessions` and
`WTSGetActiveConsoleSessionId` must establish one active console session matching
the helper, both before and after the `GetLastInputInfo` query. Session 0, RDP,
additional/disconnected sessions, denied enumeration and transitions are unknown.
This deliberately conservative first version does not aggregate helpers across
multiple sessions. Idle arithmetic handles tick wrap and rejects future tick
anomalies. It counts at most the continuously observed awake interval, resets
on changed last-input ticks, and resets after gaps over 15 seconds/clock rollback.
Startup/reconnect therefore requires five newly observed idle minutes. This is
aggregate input inactivity, not proof that a motionless owner or controller user
is absent; no claim of covering every input device is made.

`GetSystemPowerStatus` supplies actual AC/DC and battery classification. Failed,
unknown or invalid values stay unknown. Battery saver vetoes work through the
existing battery hold. A desktop is not declared externally powered merely
because it has no battery. `--laptop` conservatively requires confirmed AC even
when battery detection fails. Native hardware/API behavior remains to be tested.

Missing config, stopped helper, read/auth/transport failure or uncovered session
never falls back to guest readings. Existing five-second sampling, local job
withdrawal, 15-second staleness, cooling dwell and checkpoint behavior apply.
The normal Linux probes are unchanged on non-WSL kernels. Dedicated mode retains
its existing policy; use `--daily-use` for a personal machine.

## Uninstall and undo

Double-click **Uninstall.cmd**, or use Setup.ps1/Remote-Setup.ps1 with
`-Action Uninstall`. Both use the original downloaded executable.

Uninstall checks ownership of the Run command, removes that one value, revokes
`bridge.json`, waits up to five seconds for the executable to become removable,
and removes only its two known files and the empty installation directory. The
running helper notices config revocation within a second and closes its server.
If the binary stays locked or unrelated files exist, it reports failure and
preserves them; inspect, then retry. It never kills an unrelated PID or removes
a folder recursively. Reinstall creates a fresh key. Remove the WSL environment
setting (`unset MYCE_HOST_BRIDGE_CONFIG`) and any explicit shell configuration
you added. No system startup/service/driver setting needs undoing.

## Native verification

Use an isolated Mac-hosted hub with saved-address joining and the real WSL
worker. Discovery is a separate concern; do not change firewall/networking
settings to test this helper. Verify artifact hashes before installation.
Check limited interactive install, protected directory/config ACL, Run value,
loopback listener, real authenticated signals, continuous five-minute idle
sampling, fail-closed CPU coverage, easy uninstall residue checks and reinstall.
Leave the helper installed when requested and stop the owned worker, hub,
forwarding processes and test distro. Never synthesize owner input or treat a
GPU-only nominal reading as host coverage. Native sleep, battery unplug, RDP,
fast user switching and natural logon require separate checks.

Laptop bridge fixtures cannot qualify Windows session, power or firewall behavior.
Use the native verification above before claiming a WSL daily-use host ready.

## Queue disk contention (#527)

Keep bulk copies, heavy tests and maintenance off the queue's backing device
while qualifying I/O protection. Another directory or partition on the same
device provides no physical isolation. Native protection remains unqualified;
[#527](https://github.com/1kuna/mycelium/issues/527) owns measured acceptance.

## SQLite backup and commit limits

Python `Connection.backup(pages=256, sleep=0.05)` does not throttle successful
steps: CPython sleeps only on BUSY/LOCKED. Concurrent source changes can restart
the copy and increase writes beyond the database size. See
[CPython's implementation](https://github.com/python/cpython/blob/v3.14.0/Modules/_sqlite/connection.c)
and [SQLite backup semantics](https://sqlite.org/c3ref/backup_finish.html).
Use a progress callback or external I/O controls when bounding an online copy;
use the [stopped-state backup procedure](operations.md#queue-backup-restore-and-relocation)
for a complete queue recovery generation.

Queue mutations commit under the queue mutex with WAL and `synchronous=FULL`.
Synchronization and automatic checkpoints can delay other control calls.
Unlocking before commit would break durable publication/rollback semantics.
Mutable state and WAL checkpointing can cause sustained write amplification
without database growth. Process counters do not distinguish database, WAL,
logs and payload writes; whole-disk averages do not establish a safe I/O ceiling.

## Standard systemd policy

The installer accepts `--io-weight=1..10000` for Linux queue and worker services,
emitting `IOAccounting=yes` and `IOWeight=VALUE` on the existing service. Zero
leaves system defaults, emits neither directive and clears an explicit setting.
Omitted settings survive upgrades; existing installations default to zero.
macOS rejects nonzero values rather than silently claiming enforcement.

A worker weight of 10 versus the queue's system default 100 is an initial
relative-priority candidate, **not a measured latency guarantee**. Weighting
costs competing jobs throughput under contention; measure the ratio natively.
The aggregate service policy covers preparation, output collection and existing
attempt descendants, without adding more share per additional job. It also
lowers worker control-file write priority; qualify contact and those writes too.
Configure every worker service instance sharing the device through the installer.

Weights require an available/enabled cgroup-v2 I/O controller and an effective
weighting implementation such as BFQ or configured io.cost. `none`/`mq-deadline`
alone do not supply proportional weighting. Check the common-ancestor hierarchy:
weights compare siblings and do not prioritize a service over an uncontrolled
competing branch. A `systemctl show` property is not enforcement evidence.
An SSH backup outside the worker subtree is not covered by its policy.

If effective weights miss the existing deadline, use standard `io.max`/systemd
`IOWriteBandwidthMax` and `IOReadBandwidthMax` at the aggregate worker subtree
on the actual backing device. Derive caps from a measured latency sweep under
representative read/write bursts and fsync, rather than advertised sequential
bandwidth. Caps cost stage time: measure that too. Put bulk operator tools in
controlled scopes or keep them off that disk. No unmeasured cap is proposed.
For relocation, use the [queue recovery procedure](operations.md#queue-backup-restore-and-relocation).

## Laptop experiment and native acceptance

Check free space, memory and active work before opting in:

```sh
MYCE_TEST_QUEUE_DISK_IO=1 GOMAXPROCS=4 go test ./internal/myce \
  -run '^TestQueueDiskIOReproduction$' -count=1 -v
```

The real dev HTTP API, SQLite queue and CPU command runner share one filesystem.
Four finite writers fsync every 4 MiB, write 2 GiB total and occupy at most
256 MiB of payload space. Test-owned scratch is removed after worker shutdown.
The two-minute harness context bounds a broken local experiment's resource
cost; it does not change ordinary fleet-job timeouts. Plain native execution
uses existing unsandboxed dev behavior. A passing test proves execution/cleanup,
not reproduction of the incident or native I/O protection.

Run an authorized Linux experiment through Mycelium with platform
targets and no host pins, with a disposable queue on the job's filesystem. Record
device/controller/scheduler readback, actual job I/O rates, commit duration,
check-in p99/max and losses before/after policy. Include an isolated, bounded
live-backup reproduction and verify competing writers' scope membership.
The unchanged ten-second check-in deadline must hold under representative
production bursts, without false quiet losses. Collect every workload failure,
retain a compact receipt and remove disposable payloads after process release.
Native Linux proof and #527's measured-protection acceptance remain open.
