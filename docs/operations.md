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
cancellation or rejection removes that pending credit. An accepted check-in
refreshes contact when processing finishes. A report rejected by later validation
can therefore delay loss detection by at most 30 seconds, without refreshing
`LastSeen`; an unauthenticated or mismatched-session report receives no pending
credit. Queue capacity limits still require separate correction; start requests
that exceed their ten-second budget remain bounded failures.

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
`Restart=on-failure`, `Delegate=memory`, `KillMode=control-group` and bounded stop.
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

Follow the [accepted maintenance and native qualification gates](design-proposal.md#installation-checkpoints-and-rollback-m4).
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
go after the worker's `--log-retention` (default 7 days). Per-attempt input and
work folders (`job-files/JOB/ATTEMPT`, including inputs, unpacked source and a
Python job's `.venv`) go **24 hours** after the worker first finds them without
attempt custody, timed by the worker clock. That is after the queue accepts the
result, or after a crash orphans the folder. A failed run's work files are never
uploaded, so collect its test logs or partial outputs from that folder within
the day. A held attempt keeps its folder however long it runs. The pass refuses
to run when `job-files` is not a real directory, for example a symlink. A folder
it cannot remove, such as one with root-owned Docker output, gets a fresh day
and is retried; other folders are still cleaned, and a repeated error is logged
once rather than every minute.

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
ceilings. The generated and manual worker units use `Delegate=memory` and retain
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
`verso-*` and `docker-render-1` containers are always protected.

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

One-shot NAS install/reconfigure validation allows 10 seconds per filesystem
operation within the existing 15-second total deadline, to accommodate SMB
metadata/fsync latency. Heartbeat filesystem observation keeps its shorter
budget; a stalled installation remains bounded and preserves installed state.

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
