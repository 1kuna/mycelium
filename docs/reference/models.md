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
`myce measure` loads declared models together, optionally overlaps warm-ups,
records observations, and unloads. Use explicit runtime/device/worker identity
and an authorized maintenance window. Measured peaks and headroom, not model-file
sizes or a guessed budget, qualify native admission. The [native test plan](../development.md)
keeps this proof separate from laptop fixtures.
The installed queue must support the fresh `/api/measure/safety` read used by
the measurement binary. This small production-work/call/pin check avoids the
full inspection snapshot. Transient read timeouts back off within the existing
load-timeout budget and any earlier maintenance-context deadline. Keep the
external maintenance deadline in force: the safety wait consumes window time,
and no timeout, missing route or failed check certifies production safety.
See the [measurement contract](../services.md#model-catalog-and-worker-setup).
The command uses installed queue credentials and TLS authority by default;
`--config` selects the installation without exporting or copying its token.

On Linux unified CUDA hosts whose selected device reports unsupported used and
free memory counters, measurement uses `MemTotal - MemAvailable`, matching
admission's unified-memory detection. Reports label these readings
`basis: unified/host`: device and host figures are the same shared RAM usage and
must not be added. Baseline-to-peak deltas cover load and warm-up; absolute peaks
still determine whether the supplied total usage budget fits. These are system
observations, including other processes, rather than isolated GPU allocations.
Malformed counters, a failed probe, unsupported used memory on a device with a
numeric free-memory counter, or unavailable Linux host counters still refuse
measurement. A change of memory basis during a run also fails the measurement.

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
