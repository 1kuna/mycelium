# Operating Mycelium v3

## Add a hypha with one command

On an authorized installed hypha, run `myce invite` and paste its single output
line on the new machine. `--out FILE` optionally saves the same command. The queue
must approve a [signed release](guide-onboarding-releases.md) first. The bootstrap
needs no Go or Git and never prints the fleet token. See
[joining](guide-joining-v3.md) for prerequisites, ticket expiry and recovery.

Fresh invitations carry a credential-free server/share fingerprint and a
share-relative checkpoint location. The joining machine automatically selects
exactly one matching OS mount; missing or duplicate matches require mounting
the share or choosing `--nas PATH`. Different server aliases or storage protocols
require an explicit selection. Saved paths and explicit storage flags are preserved.

Completion checks exact session/protocol/components, CPU/RAM budgets, runner smoke
and stable manager health. `polite_waiting`, `disabled` and `drained` do not mean
`ready`. Repeat a release's line for an interrupted install or health check;
use a line for a newer release to upgrade while preserving spool, identity and
maintenance. Existing legacy conversion remains the explicit M4 procedure below.

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
See [extension authoring](write-an-extension.md#install-and-qualify) and the
[check guidance](extensions/AGENT-GUIDE.md) for preparation and replay criteria.

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
retrying. See [owner priority policy](guide-priority-policy.md) for the full
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
already evict reproducible copies under pressure. See [files](guide-files-v3.md).

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
drop-ins that override execution, credentials or containment block activation;
consumer files are preserved. Check health and exact cleanup before resuming.

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
see the [signed-release deployment step](guide-onboarding-releases.md#advance-the-default-after-a-signed-update).

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
silently cleared. See [fresh install](guide-fresh-install.md).

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

Follow the [accepted maintenance gates](design-proposal.md#installation-checkpoints-and-rollback-m4)
and [native qualification](design-proposal.md#native-maintenance-window-qualification).
Conversion is an operator-run maintenance action on the queue host and every
converted worker host, including the single-card Spark worker in the same
window. Commands below use placeholders for approved installation paths and
accounts. Load authentication through the existing private secret mechanism;
never print a token or pass it on the command line.

Pause ordinary submissions and disable both physical hosts through the current
queue. Record source identities/sessions, actual unit enablement/activity,
config/binary digests, device identities and settled spool evidence. Keep the
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
unit. Verify durable disables, job/key/retry/checkpoint/release state and Spark's
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
[fresh-state procedure](guide-fresh-install.md) when required; a successful
conversion preview does not authorize bypassing those checks.

The source manager stop must complete the same-name worker's normal leave.
Failure restores the previous install and keeps queue maintenance active.
Worker-local catalog placement fences are rewritten from explicit queue retirement
records, preserving local environment/secret values and original bytes for rollback.
The exact legacy Spark session may reconnect with its old catalog hash only while
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
disabled. Verify both source workers and Spark reconnect, device isolation,
secret/root custody and consumer health before the operator accepts rollback.
Forward conversion can be repeated under the same maintenance gates.

## Monitoring and failures

`myce status`, the HTML page and `GET /api/status` show queue projections with
redacted job specifications. Use [typed status](guide-status-v3.md) for holds,
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
partial transfers and pins persist through restart. See [models](guide-model-calls.md)
and [files](guide-files-v3.md) for transfer, retention and cache limits.

## General model servers and joint measurement

General HTTP servers, llama.cpp and vLLM share the queue's engine lifecycle,
placement, pins and release custody. See [servers](guide-servers.md) for routing,
local secrets, approved folders, timeout/retry behavior and log policy.
`myce measure` loads declared models together, optionally overlaps warm-ups,
records observations, and unloads. Use explicit runtime/device/worker identity
and an authorized maintenance window. Measured peaks and headroom, not model-file
sizes or a guessed budget, qualify native admission. The [native test plan](production-test-plan.md)
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
with group `myce-telemetry` and mode **0750**, and a socket owned by
`root:myce-telemetry` with mode **0660**. Only designated worker accounts join
that group; their actual service must receive that supplementary group. The
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

1. Create the dedicated `myce-telemetry` system group if absent (for example,
   `getent group myce-telemetry || groupadd --system myce-telemetry`, as root).
   Add only the designated worker account to it, then create the broker's runtime
   directory/socket with the owners and modes above. Ensure the broker recreates
   them correctly after reboot.
2. Give the actual worker process the supplementary group. A system service can
   use an administrator-owned `SupplementaryGroups=myce-telemetry` drop-in. For
   a user service, refresh the user manager's group credentials through an
   operator-controlled session/manager restart; restarting only the worker may
   retain old groups. Verify the running worker's groups in `/proc/PID/status`;
   a successful client call from a new login shell alone is insufficient.
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

On dedicated GPUs the host pool charges only `host_memory_gb`. On unified
Metal/Spark/integrated machines, GPU and CPU allocations share one host pool:
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
acknowledgements afterward. See [managed Docker](job-reference.md#managed-docker-batch-commands)
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


## Shared orchestration and interactive hosts

These hosts can run fitting light work through Mycelium. Use the opt-in
[shared-host profile](shared-hosts.md) to combine measured interactive RAM
reserves, #353 external peaks, native command nice=10, positive CPU/RAM budgets
and last-resort placement. `myce host profile HOST --shared=true
--interactive-reserve-gb GiB` requires a drained/disabled, fresh quiescent host;
`--shared=false` removes the profile without resuming admission. Measured OS memory pressure or foreground scheduling latency now stops work
through planned checkpoint/requeue with hysteresis. Linux execution cgroups cap
aggregate CPU time; macOS adds background taskpolicy and reports CPU time
unenforced. See the shared-host guide for thresholds and qualification limits. Existing exclusions, drain ownership and storage fences apply.

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
