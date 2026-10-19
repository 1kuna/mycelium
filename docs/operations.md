# Operating Mycelium v3

This is the canonical runbook for installation, host close/open, reconciliation
and recovery. The [approved design](design-proposal.md) is the product spec;
[status](guide-status-v3.md) defines machine-readable fields and verdicts.
These instructions describe the implementation. Laptop checks do not qualify
native GPU, systemd, Docker, boot or consumer behavior.

## Queue maintenance

For ordinary maintenance use `myce host drain HOST`, `disable HOST [--stop]`,
`resume HOST` and `status HOST`. Set `MYCE_QUEUE` and the private `MYCE_TOKEN`.
These commands are unprivileged, host-wide and durable across rejoin. Use
`--wait --timeout 30s` with drain/disable/status to wait for fresh workers and
zero known execution/cleanup custody; nonzero exit leaves all custody intact.
Drain finishes active work; disable only stops it when `--stop` is supplied.
Unstarted assignments are withdrawn through the existing stop protocol.

Use `myce host budget HOST --memory-budget-gb GiB` while drained or disabled.
The queue owns the allowance after its first worker report. Worker report
agreement, aggregate RAM/debt and per-attempt main/sidecar limits still apply.
Planned queue maintenance stops do not spend production infrastructure retries;
independent stage failures still fail immediately.

See the [maintenance contract](design-proposal.md#queue-owned-host-maintenance-s5)
for JSON fields, deadlines and bootstrap behavior. S5 still enforces the local
fence below: queue resume cannot override a local close. Local close/open and
`host-memory-budget configure` are transitional controls pending S6; the latter
cannot change an established queue allowance. Queue quiescence is not local
exclusion or an offline release guarantee.

## Paths and prerequisites

Use one queue, one local SQLite database and one worker per selected GPU.
Installed workers on a physical host share one host ID and host-control directory.
Run the installer as the worker account. It never installs packages, changes
hardware owners or grants privileges. Supply GPU drivers, the required runtime,
software recipes and storage explicitly. `myce doctor --config CONFIG` checks
prerequisites; Linux boot without login also needs administrator-managed linger.

| Item | Default / selection |
| --- | --- |
| Installed role | `~/.local/share/mycelium/queue/`, `worker/` or `worker-INSTANCE/` |
| Role files | `myce`, `config.json`, private credentials and `state/` inside that role |
| Shared host control | `~/.local/share/myce/`; saved `host_state_dir` selects a custom location |
| Linux user units | `myce-queue.service`, `myce-worker.service`, `myce-worker@INSTANCE.service` |
| Reconciliation | `myce-host-reconcile.service` and `.timer` in the worker user manager |
| macOS installer labels | `local.mycelium.queue`, `local.mycelium.worker` |
| Protected root control | Separate `/usr/local/libexec/myce/myce`, mode 0755 |

Saved config is authoritative for actual paths. Manual [deploy templates](../deploy/README.md)
require substituted binary/config/state paths; their macOS labels are distinct.
Do not run two service definitions against the same spool or database.

NAS mode uses each machine's local mount of the same shared root. Queue
`--checkpoints` and worker `--checkpoint-root` may differ locally; assignments
carry relative paths. Verify mount identity independently before creating the
checkpoint root: a writable directory alone can be an unmounted fallback.
For jobs without a NAS, use [verified file transport](guide-files-v3.md).
Never publish private paths, invitations, credentials or operational snapshots.

## Install binary and private configuration

Build a trusted artifact with Go 1.25.6 or newer. Provision `MYCE_TOKEN` through
private environment/secret files, never argv or job YAML. An existing installation
retains its credential or EnvironmentFile reference. Jobs do not inherit the token.
The invitation alternative is in [joining](guide-joining-v3.md).

```sh
go build -o myce ./cmd/myce
./myce install queue --nas NAS_ROOT --listen QUEUE_BIND_ADDRESS:7331 --dry-run
./myce install worker --nas NAS_ROOT --queue QUEUE_URL --dry-run
```

Replace uppercase placeholders before use. Inspect dry-run output, then perform
an authorized install without `--dry-run`. For an existing role use its exact
config instead of new defaults:

```sh
./myce install worker --config WORKER_CONFIG --dry-run
./myce install queue --config QUEUE_CONFIG --dry-run
```

### Installer overlay and rollback

The installer reads saved configuration **before** detection/defaults. Omitted
flags preserve queue address, NAS, worker name, Docker endpoint, credentials,
EnvironmentFile, GPU runtime file, catalogs, memory policy and custom settings.
Only explicit flags change configurable values; even explicit false/zero/empty
values are deliberate overlays. `--force` is deprecated and unnecessary.
Installed role, account, host, device, unit, binary and state identity cannot be
relocated over an existing installation. Preserve custody and install separately.
Docker is disabled on fresh installs and foreground `join`; ambient
`DOCKER_HOST` is not adopted. Native jobs need no Docker CLI, daemon or socket.
To enable image jobs on Linux, pass `--docker-host unix:///var/run/docker.sock`
or save a nonempty `docker_endpoint` in the worker config. Saved endpoints survive
upgrades. An old config missing `docker_endpoint` retains its empty value unless
`--docker-host` explicitly selects an endpoint. An explicit `--docker-host=`
disables new Docker execution and background container retention, preserving
custody records. Re-enable the runtime to recover an outstanding Docker execution.
Image jobs wait for an enabled Linux worker; native placement is unaffected.

Host close/reconcile/release applicability on native-only hosts is separate
follow-up work; the existing exclusion requirements still apply. Disabling
Docker does not retire historical daemon scope or prove release.

Worker installation requires CLOSED intent. It preserves the revision and never
opens the host. A genuinely empty installation starts revision 1 CLOSED with
confirmed empty previous custody; surviving worker data prevents that shortcut.
Workers are enabled but not started. All card instances must be upgraded before
opening. Linux units keep `_host-start-check`, `Restart=on-failure`,
`KillMode=control-group`, bounded stop timeouts and `Delegate=memory`. The service
entry repeats the revision gate. Conflicting effective drop-ins block activation;
consumer-owned drop-ins are preserved for resolution.

Publication snapshots prior artifact bytes/modes and manager state, atomically
replaces files and keeps recoverable `.Trash` backups. Failures restore the
binary, config, credentials/runtime files, worker/reconcile units and prior
service/timer enablement/activity. Rollback uses a separate bounded recovery
context, including after cancellation. It never opens or renews host intent.
A rollback failure is reported as **rollback incomplete** and leaves convergence
stopped/fenced; inspect files and actual manager state before retrying. An install
error followed by a manual restart is not successful rollback qualification.

Linux validates GPU access through a transient user service using the installed
runtime environment and selected device. Unknown devices, denied access or a
conflicting startup gate refuse acceptance. `--allow-deviceless` is an explicit
laptop test option, not hardware qualification. `gpu.env` contains only recognized
GPU runtime controls, never the whole shell environment or fleet token.

## Inventory and foreground start/stop

`myce detect` reports the GPU inventory used by `join` and `install worker`.
Bare `join` auto-detects; explicit `--gpu` and `--device` override it. Select one
CUDA card by stable UUID (numeric selection resolves to that UUID); use distinct
worker names and one shared physical `--host-id` across cards. Multi-card live
CUDA inventory without a selector refuses rather than summing cards.

CUDA dedicated memory uses live `nvidia-smi` readings. Unified CUDA uses Linux
`MemAvailable`; Metal uses free plus inactive pages. Reserve is subtracted after
an optional cap. XPU/fake use configured budgets; `--memory-mode fixed` needs a
positive explicit budget and cannot see external allocations. Invalid telemetry
blocks admission visibly. A GPU selector is not an OS isolation boundary.

Foreground `join` runs ordinary commands as the worker user. Ctrl-C or `leave`
prevents new work and lets resumable commands save; `leave --now` stops all work
with checkpoint grace. Leave does not fence automatic service restart. Use host
close for durable whole-host ownership changes. Preserve worker spools through
outages: logs/results replay only for their exact session and attempt.

## Close, reconcile and reopen a host

Host control is **whole physical host**, including every card, engine, native
descendant, preparation and managed Docker attempt. There is no per-device enable.
`disable`/`enable`, local-disable receipts and manual receipt-based reopening are
retired. Local intent, fresh physical evidence and queue acknowledgement have
separate meanings; see [the envelope reference](guide-status-v3.md#host-control-envelope).

On Linux, privileged control must use the separate protected copy described
below. As the installed worker user, these commands use the same controller but
cannot prove privileged GPU-holder absence:

```sh
myce host close --local --config WORKER_CONFIG --wait-released --timeout 30s --json
myce host reconcile --local --config WORKER_CONFIG --timeout 30s
myce host status --local --config WORKER_CONFIG --expect closed --verdict --timeout 10s --json
```

Close durably writes CLOSED before discovery/network calls and fences all launch
paths. Saved stop intent and its original deadline survive restart; TERM/KILL or
retry does not renew grace. Charges remain until exact release. Without
`--wait-released`, exit 0 means durable acceptance. With it, exit 0 means local
exclusion PASS, which can occur while queue settlement is pending. Rest PASS
also needs current queue acknowledgement and settlement. A timeout leaves
CLOSED intent and unconfirmed obligations intact.

Close can use `--expect-revision REVISION` for compare-and-set. Without that
selector, close accepts a new closing revision even during another operation.
A rejected selector returns exit 3, `revision_mismatch`, unchanged desired state
and `accepted_revision: null`. If an accepted close is overtaken, it keeps its
accepted revision and reports `revision_superseded` with the current revision.
Without a release wait its acceptance still exits 0; with the wait it fails
nonzero, never as a CAS rejection. Follow the newer revision.

Reconcile retries the same revision using durable custody. The installed timer
runs it even when workers are stopped; a co-located queue also reconciles before
admission. Every worker session is registered durably before its first check-in.
Current-boot local exclusion discharges only the exact registered sessions and
covered attempts at the queue. Unregistered or unknown custody remains charged.
A queue ahead of local intent reports `host_revision_behind_queue`: inspect the
mismatch, then issue a new close; it allocates above the observed queue revision.
Never edit SQLite or delete state to declare release.

Before reopening, the consumer must persist completion of its **own** cancellations,
fence late publication and verify the other owner's release. Mycelium settlement
covers Mycelium only. Read the current status, require `host_settlement: confirmed`
and use **`desired.revision`**, since read-only status has null accepted revision:

```sh
myce host status --local --config WORKER_CONFIG --json
myce host open --local --config WORKER_CONFIG --expect-revision REVISION --timeout 30s --json
myce host status --local --config WORKER_CONFIG --expect ready --wait --timeout 30s --json
```

Open requires the current confirmed close revision, allocates a new revision,
and preserves `last_close` as history. `previous_close_unsettled` refuses pending
or unknown prior settlement. Acceptance restores eligibility; it is not readiness.
Ready needs every installed required worker fresh and healthy on this boot at
the open revision, queue acknowledgement and no unresolved release. It does not
certify model/application health. No idle timer proves either rest or readiness.

### Protected root-owned copy

A trusted operator deploys and hashes a separate `/usr/local/libexec/myce/myce`
copy, mode 0755, with root-owned, non-group/other-writable ancestors. The worker
installer never writes it. Use its **absolute path** in root commands and sudoers;
never execute the worker-owned binary as root. The precise install/hash/ACL and
native user-manager checks are in the [identity checklist](probe-s11-worker-identity.md).

```sh
sudo /usr/local/libexec/myce/myce host close --local --worker-user WORKER_USER --wait-released --timeout 30s --json
sudo /usr/local/libexec/myce/myce host status --local --worker-user WORKER_USER --expect closed --verdict --timeout 10s --json
```

Root startup checks its executing inode, ancestors and Linux access ACLs and
restricts commands to local host control. Root resolves the OS worker account,
then drops all worker config/state reads and mutations into a scrubbed same-binary
child with that account's groups, HOME and runtime directory. No caller HOME,
user bus or inherited fleet token chooses identity. Root performs read-only
cgroup/Docker/GPU observations. Unsafe ownership, symlinks or ACLs produce typed
errors. Non-Linux root control is unsupported; unprivileged GPU-holder evidence
is UNKNOWN. Do not bypass these checks to obtain PASS.

## Rest and recovery

Use `host status --local --expect closed|ready --verdict --json` for the product
verdict, not English hold text or an empty process list. FAIL means fresh contrary
evidence; UNKNOWN means missing/stale/ambiguous evidence. FAIL wins while all
causes are retained. Null counts mean unknown; empty arrays prove a complete
empty inventory only. Evidence is evaluated after collection at one instant,
bound to current boot/revision, with the default 10-second freshness window;
future timestamps are absent. Collection/fence failures yield UNKNOWN.

Closed PASS requires launch fencing, empty owned cgroups, complete terminal
Docker operations/inventory, no owned GPU holder, safe restart policy and fresh
queue settlement/placement evidence. Fenced queued work is counted and allowed;
positively foreign-only work is outside scope; unknown placement stays unresolved.
Ready PASS requires current open revision and all required workers, not empty
cgroups. With `--verdict`, zero exit means PASS only.

During a queue outage, close can prove local exclusion while settlement remains
pending. Another owner may use that local release under its own recovery rules;
full rest and consumer coverage remain unresolved. Reconnect/reboot reconciles
before admission. Old observations do not survive a boot change. Undelivered
results stay as diagnostic Holds, superseded attempts cannot publish, and eligible
experiments can resume without a loss charge after close. Keep outputs safe to repeat.

If the binary or a daemon is broken, preserve intent, spools and pending operation
journals. Stop only independently identified Mycelium-owned units/containers;
a stopped unit, dead Docker client or elapsed deadline alone never proves release.
Restore a trusted matching binary and rerun close/reconcile. Do not remove intent,
create a format marker by hand, reset counters or assert unknown operations away.

## Upgrade

Close first and preserve config, secret references, state and manager snapshots.
Upgrade queue and all workers together; [current worker protocol](current-worker-protocol.md)
rejects old or missing protocol with HTTP 426. Reinstall by exact `--config` to
apply the overlay above. Check rollback outcome on any failure; do not open until
settlement and the consumer's obligations are complete. Then explicitly open
against the fresh desired revision and verify ready. Root control is deployed
separately from worker installation.

### Old formats: archive plus fresh install

Every queue, worker, cache and shared host-control state root needs `.myce-format`
containing exactly `v3` and a newline. Startup checks all roots before writes.
Absent/empty roots get a fsynced marker first. Any nonempty root without the exact
marker refuses with `legacy_state_present`; even pre-marker development state is
old. There is no migration/converter or old-format fallback.

Stop old services/controllers and prevent their restart, archive **whole** data
roots outside active lookup paths, retain config and secret references, then
install matching current binaries/units fresh. Inventory saved `state_dir`, `db`,
`host_state_dir`, `model_cache` and `environment_cache` for custom roots. Preserve
checkpoints, archives and unique evidence; fresh queues do not adopt old attempts.
See [the archive and fresh-install procedure](guide-fresh-install.md) for default
paths, per-card units and the first CLOSED reconciliation/open sequence.

## Uninstall without deleting data

Close and verify release before uninstalling a worker. Inspect
`myce uninstall --config CONFIG --dry-run`, then run the authorized uninstall.
It stops/disables the selected boot service and moves its role files to recoverable
Trash. NAS data stays intact. Preserve shared host-control state, archives and
spools needed by other instances; do not remove shared state to uninstall one card.
Restore only from a verified snapshot, then reconcile before any reopen.

## Monitoring and failures

`myce status`, the HTML page and `GET /api/status` show queue projections with
redacted job specifications. Use [typed status](guide-status-v3.md) for holds,
eligibility and last-reported allowance. Inspect exact jobs/logs separately when
authorized. Status and logs require the privately configured queue credential.
Never put that token in URLs or copy private payloads into reports.

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
The queue uses the lowest current allowance reported by workers on that host
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
No attempt is moved into a separate systemd scope. `myce host close --local` therefore
still stops the worker **and every attempt child**, and its parent
`cgroup.events populated` check includes those descendants.

`join` and `install worker` accept:

- `--host-memory-budget-gb`: UNQUALIFIED migration input; default `0` keeps
  uncapped admission-only operation. A positive install-time input requires
  explicit `host-memory-budget configure` while CLOSED before first-report
  bootstrap, or configure the queue with `host disable HOST --memory-budget-gb
  GiB` before starting the worker. Established queue policy is changed with
  `host budget`, not the local configurator.
  All cards read the same allowance from the host intent; differing worker
  reports fence admission. Retain aggregate attempt/model charges, including
  slack and unconfirmed releases, until exact release proof. Do not reduce
  declarations to fit a guessed allowance.
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

Preserve the conservative existing value as UNQUALIFIED input during explicit
CLOSED configuration. Do not raise or lower it during an owner switch.
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
(with installed identity/config options where required) to reconcile exact offline
custody. This uses the same worker executor and inspection path; it does not
accept an operator assertion or launch work. Repeat host close/reconcile afterward
and require fresh rest evidence. See [managed Docker](job-reference.md#managed-docker-batch-commands)
for supported command/network/sidecar forms. Unmanaged containers remain outside
this containment contract.
