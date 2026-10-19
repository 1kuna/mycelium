# Operating Mycelium v3

## Queue maintenance

All commands use the authenticated queue and run as an ordinary account.
`MYCE_QUEUE` supplies its URL and `MYCE_TOKEN` supplies the private fleet token.
Use the physical host ID shared by all device workers:

```sh
myce host drain HOST --wait --timeout 30s
myce host disable HOST --stop --wait --timeout 30s
myce host status HOST
myce host budget HOST --memory-budget-gb GiB
myce host resume HOST
```

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

During queue outage, no maintenance mutation or new Started authorization can
succeed. Inspect owned work before using separately authorized service/container
tools. Preserve results and custody for reconnect; there is no offline release
guarantee. Consumer cancellation and application health remain independent.

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
over an existing installation. Each Linux GPU instance retains its own config,
spool and `myce-worker@INSTANCE.service`; the physical `--host-id` is shared.
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
