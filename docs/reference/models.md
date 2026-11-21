# Models and engine history

Model storage, general model servers, joint measurement and the durable
model-engine interval history. See [services](../services.md) for the model and
server contract.

## Model master folder, worker cache, and production reservations

Models use immutable NAS masters, verified file-store imports or explicit local
source approvals. Loading never downloads weights. Catalog hashes and byte counts
identify content; cache verification precedes publication. Protect loaded/pinned
files from eviction and keep recoverable copies. Use `model sync` for pre-copy;
partial transfers and pins persist through restart. See [models](../services.md)
and [files](../files.md) for transfer, retention and cache limits.

### Local acquisition root (#1248, next release)

The supported installer accepts a selected local model source/acquisition root:

```sh
myce install worker --config CONFIG --model-root /local/model-acquisition --dry-run
myce install worker --config CONFIG --model-root /local/model-acquisition
```

Use a real local directory with measured headroom above the filesystem floor.
Outside the selected NAS, installer preflight verifies a local filesystem,
directory ancestry, and write/sync/rename access. Checkpoint and environment
source roots still require their selected mounted NAS and normal health probes.
Saved configuration, identity, active-attempt checks and installer rollback stay
on the supported reconfiguration path; no settings-file edit or mount bypass is
needed. Dry-run and real installation perform the same read-only local-volume
and headroom selection checks. Real preflight additionally probes write access
before changing saved settings or services.

The acquisition root is the source of immutable hub snapshots; the verified
read-only execution copies live in the worker's model-snapshot cache and share
its filesystem budget with other caches. This distinction allows acquisition
to avoid charged network-filesystem dirty writeback. Catalog masters on NAS
remain untouched. Moving the setting does not copy or delete old masters,
download weights, authorize deleting a last unique source, or migrate a live
attempt. Populating this root is optional: a worker fetches a declared
snapshot it lacks from the model hub into its own snapshot cache
([model snapshots](../files.md#declared-model-snapshots-553)).

## General model servers and joint measurement

General HTTP servers, llama.cpp and vLLM share the queue's engine lifecycle,
placement, pins and release custody. See [servers](../services.md) for routing,
local secrets, approved folders, timeout/retry behavior and log policy.
Accepted jobs retain their last duration and eligible memory peaks. Memory
admission uses those observations with measured uncertainty; wall-time limits
come only from the job's explicit contract. Native model qualification still
needs representative load and request peaks, runtime overhead and OS headroom;
laptop fixtures do not qualify real model capacity. Use the engine-attributed
peaks below together with worker telemetry and the calling job’s attempt facts.

### Resident engine memory peaks

Read `loaded_models[].memory_peaks` in `myce status --jobs recent --json` or
authenticated `GET /api/status?jobs=recent`. Use the model, machine, device and engine attempt to select
the right record. `load` covers engine startup through readiness, `steady`
accumulates samples taken while ready with no routed requests active, and
`request` is the latest completed activation. Requests include the upstream
response body and streaming lifetime. An overlapping request window measures
the whole engine during that call and sets `overlaps_other_requests`.

Each window reports `host` and `device` measurements in GiB, with
`peak_gb`, `source`, the latest `sampled_at`, `samples`, `complete` and
`constrained`. `complete` requires at least two readings, no rejected reads,
and first, last and inter-sample gaps within the existing missed-cycle
tolerance. Busy time is excluded from idle coverage. Sampling can miss a
transient spike, including an entire short request. An absent peak carries an
`unavailable` reason. `pressure_unavailable` marks missing pressure evidence,
and `constrained` makes the peak a lower bound on unconstrained demand.
Cgroup events and host pressure can carry forward conservatively within the
engine attempt.

Linux cgroup host samples cover anonymous, kernel and shared memory. Native
process RSS is available where the worker supports owned-process observations.
CUDA and XPU use the available per-process device counters. Metal can expose
the sampled process footprint beyond RSS, labeled with that source. The
numbers describe the engine's absolute footprint in the window. On unified
memory, consider the shared pool and the source before deriving a capacity
budget. Placement continues to enforce the configured resident model budget
and its existing live headroom checks. Use representative, complete,
unconstrained samples to qualify a catalog budget.

Successful routed responses expose `X-Myce-Model-Request-ID`. Match that ID to
`memory_peaks.request.request_id` on `memory_request` interval events. The
queue records a request event when it receives the exact worker receipt.
Interrupted transports can lose the individual receipt; an accepted worker
report can still carry the latest completed window. Older workers can omit
`memory_peaks`.

## Durable model-engine interval history (#663)

Authenticated `GET /api/models/intervals?limit=100` returns immutable transition
`entries` with `sequence`, `at`, `kind`, model/attempt, machine/session, physical
host, device ordinal/stable identity/platform, project/class/priority and the
resolved `tier` (name, index, selector and policy revision). Events retain the
interval's known `assigned_at`, `load_started_at`, `stopped_at`, `released_at`.
Assignment records queue custody; load start records the first accepted engine
launch authorization, not a process-start acknowledgement. Stop records queue
stop intent, not signal delivery. Exact matching `failed` cleanup or accepted
`unloaded` receipts record release. A fresh replacement check-in also records
release for slots in its exact attested `previous_session`, before old slots are
discarded. Stale receipts and unverified worker loss cannot establish release.
A direct cleanup receipt without prior stop intent sets stop at receipt time.
Class/tier changes use the scheduler's effective identity, including active
production pins, and produce another event at their transition time, so a model
promoted or demoted in place remains visible to overlap proofs.
`memory_load` records the first finished load window. `memory_request` records
the specific received request window. Accepted release events retain the last
reported load, steady and request peaks after unload. The [generated intervals
schema](../schemas/model-intervals-v1.schema.json) covers these optional fields.

Begin with `after=0` and omit `high_water`. Follow `next` as `after`, retaining
the first page's `high_water`, until `has_more=false`. `limit` is 1..256. Start
a later snapshot at the previous high water to collect new arrivals. Even an
empty snapshot's high water is a valid fixed bound. Events persist in SQLite
with their queue transitions and survive slot removal and queue restarts; they
are retained without automatic pruning. Reads fetch only a bounded indexed page
outside the queue lock. Back up the entire queue database as usual.

`coverage_since` starts the current uninterrupted ledger-enabled period. If an
older binary rewrites state during rollback, re-upgrade recovers sequence
allocation from retained SQLite events and starts a new coverage period; events
from before that gap remain readable but do not establish continuous coverage
through it. Existing slots
are recorded as `observed` with unknown earlier times; there is no historical
backfill. Missing assignment, load-start or release evidence cannot pass a
complete interval proof. Join events by attempt/machine/session/device and use
class-change event times to split tier occupancy. For batch copies, continue to
use job GET times and the paginated job ledger described in [status](../status.md).
