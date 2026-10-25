# Scheduling simulator

`myce sim` replays synthetic GPU work through the **real** queue, batch dispatch,
model-call placement, memory admission, priority reclamation, and decaying
fair-share accounting. In-process workers run timers instead of processes or
models. A fake clock jumps between arrivals, finishes, stop acknowledgements,
model readiness/idle expiry, availability changes and worker-loss deadlines.
No GPU, network connection, token, SQLite file, model download or real sleep is
needed. Model loads admitted during a check-in settle at that same fake-clock
instant, including after command cleanup; an unrelated future event must not
delay their startup timer or create an artificial load timeout. The simulator skips persistence and assumes a reachable pretend NAS.
It aborts on queue errors. Finished jobs are removed after their metrics are
collected; production does not use that removal path.

```sh
go build -o /tmp/myce ./cmd/myce
/tmp/myce sim --fleet examples/sim/fleet.yaml --workload examples/sim/workload.yaml \
  --days 7 --seed 1 --json /tmp/baseline.json
/tmp/myce sim --days 7 --seed 1 --what-if split-batch --json /tmp/comparison.json
```

The defaults resolve relative to your working directory. JSON defaults to
`sim-results.json`; use `--json` to choose the output. YAML parsing rejects unknown
fields, multiple documents and invalid values. Runs allow at most 90 days and
20,000 jobs. The supplied week runs in seconds on a laptop.

## Inputs and comparisons

Fleet workers are individual cards with names, platforms, usable `memory_gb`,
shared physical `host`, usable `host_memory_gb`, `unified`, and relative `speed`.
The two CUDA workers on `host-b` share one host RAM budget; the Metal worker
on `host-c` shares GPU/CPU RAM. Budgets are **GiB after reserves**, not apparent
model-file sizes.
No availability windows means always present. Windows use Monday=0 through
Sunday=6, with start-inclusive/end-exclusive hours in a repeating synthetic
UTC week. `departure: planned` (default) saves resumable work and restarts
non-resumable work. `departure: loss` cuts execution without a final checkpoint;
the real queue's priority loss timeouts and majority-silence guard determine
when batch work becomes available again. Interrupted model calls fail visibly
on abrupt loss; this simulator does not emulate HTTP response replay.

A workload may include `policy:` with the same version/tiers/projects mapping as
`myce policy set`; the simulator installs it through the real queue policy API.
Legacy top-level `projects:` still supplies weights/caps; do not also specify
`policy.projects`. Batch streams accept `class:` or legacy `priority:`, normalized just as at
submission. Model calls still use
legacy priorities. Shared-tier dispatch, backfill and status use the same queue
planner; the simulator adds no fairness ordering of its own.

Models specify
identity, file bytes (when known), GPU/host memory, platforms and local startup
time. Streams specify project, priority, model, batch versus `call: true`,
platform/memory for batches, duration, checkpoint interval, repeating arrival
interval in hours, first arrival hour, count and optional deadline seconds from
submission. Durations mean base-speed compute seconds. Per-job seeded jitter is
±20%; `duration_jitter: 0` makes hand-checkable scenarios exact. Startup time and
compute time divide by the worker's speed. All model files are assumed already
cached on compatible workers: startup times exclude NAS copy/verification.

A batch process loads its native model on each attempt; `needs.model` remains
only the real scheduler's cache hint. The simulator does **not** invent persistent
batch residency. Model calls use real model slots and load/unload acknowledgements,
with one in-flight call per model slot, allowing real co-residency admission.
Resumable processes save current progress within a modeled one-second SIGTERM
response; abrupt loss retains only complete periodic checkpoints. Checkpoint
intervals are wall seconds of compute and progress carries between worker speeds.

`batch_files: 12` marks a serial transcription batch. `--what-if split-batch`
submits the same seeded file work as 12 separate jobs instead of one. It changes
no scheduler policy. The comparison uses only matching batches finished in both
runs; JSON also retains every batch's finish time or null. Negative “seconds
sooner” means splitting was slower. Completion counts and unfinished batches
expose censoring. Additional comparisons can simply use edited fleet/workload
YAML files; there is no plugin system.

## Reading the numbers

| Metric | Meaning |
|---|---|
| Production wait median / p95 / max | Submission to first command start, or to first ready-model call assignment. Batch native model initialization happens after command start and is reported as loading. Nearest-rank percentiles; unstarted work is counted separately, never assigned a zero wait. |
| Deadline misses | Due jobs not successfully finished by their deadline, including unfinished/failed jobs whose deadline passed within the horizon. |
| Experiment throughput | Completed queue jobs per simulated day, per project; split-file jobs change this unit. |
| Experiment mean finish time | Submission-to-success seconds, for completed experiments only; unfinished count must also be read. |
| Project GPU hours / share / weight | Active device time divided equally among simultaneous commands/calls. Includes batch initialization and stop grace. Experiment-only GPU hours/share are also in JSON. Weight percent is a global comparison, not an entitlement when projects cannot compete on the same hardware. |
| Idle with fitting work waiting | Online device-hours with no command, call or model load while the real queue's fit/cap checks admit waiting work there. Memory-incompatible or capped work does not count. Empty or incompatible idle time appears through low utilization. |
| Machine utilization | Command/call device-hours divided by online device-hours, summed per physical GPU worker. Loading-only intervals appear separately and are not called compute utilization. |
| Loads / reloads / loading seconds | Every batch initialization or real slot load; subsequent loads of the same model on the same worker are reloads. Loading seconds count occupied device time once per device even if loads overlap. |
| Preemptions / lost work | Real queue's preemption records; compute seconds discarded by forced non-resumable stops. Resumable graceful stops save progress; loss since the last checkpoint is separately reported for abrupt departures. |
| Never placeable | Submitted jobs that cannot fit any configured fleet worker even when empty, regardless of temporary availability. This differs from unfinished backlog. |
| Batch completion mean | Time until every file in each completed marked batch finishes. Per-batch timestamps and unfinished counts are in JSON. |

## Limits

This is a measuring tool for the scheduler under stated assumptions, **not** a
fleet benchmark or hardware qualification. All example host/model/project
identities, topology, speeds, memory budgets,
file sizes, startup times and arrival/duration values are invented for illustration.
They contain no fleet measurements or consumer traces. File sizes do not establish
native peak memory. Streams arrive independently rather than forming a dependency
graph, and cross-platform portability is hypothetical.

The workers have continuous healthy heartbeat coverage and instant pull/stop
notification outside the modeled stop response. Transport polling, HTTP overhead,
real checkpoint I/O, NAS transfers, environment builds, external GPU owners,
thermal effects, concurrent inference slowdown and partial response custody are
not simulated. Thus the run gives an optimistic scheduling response bound.
Model-call `max_wait` is a transport/API timeout and is not simulated. This loop
uses the real scheduler entry points but does not test HTTP, process control,
SQLite durability or native engines. Whole-fleet silence follows the real
majority guard; work may remain pending until reconnect instead of being retried.

The queue's scan implementation has equivalent performance shortcuts for large
backlogs: one ordinary candidate per project, cached fair scores within a scan,
priority-specific reclaimable memory, and no reclamation scan for experiments.
Within a read-only device scan it also reuses identical resource projections
when no preparation is active and sorts each project's candidates once for
exclusion enumeration. These snapshots are discarded on placement mutations.
They do not add placement or fairness rules; regression tests compare decisions
against the original scans. Fake-clock tests cover preemption, graceful leave,
sudden loss/checkpoints, exact weighted allocation, fitting-idle accounting,
model loading/reuse, impossible work, deadlines, batching and determinism.

Production release planning counts commands already being stopped. After stop
completion, it retains release credit until the next worker memory report or
the original stop grace expires.
Each waiting batch needs a separate card; release credit never admits a command
before a free-memory report. The one-production/two-experiment fixture therefore
interrupts exactly one experiment and starts production after its one-second save.

## Per-piece pipeline comparison

```sh
myce sim --pipeline-scenario separate-stage --seed 1 --json pipeline.json
python3 scripts/sim-per-piece.py --binary ./myce --out .reports/per-piece
```

The harness uses slice 4's fast runtime (`GOMAXPROCS=1 GOGC=400`) with three
parallel seeds. Fixed scenarios cover a homogeneous joint-acceptance control,
heterogeneous waves and a short tail, separate stage capacity, shared stage
capacity, and the original seven-day mixed-production workload with an identical
predeclared downstream stage in each mode. `--what-if per-piece` also accepts a
custom workload: add `downstream` to a stream with `batch_files` and curves:

```yaml
downstream:
  duration_seconds: 60
  platforms: [xpu, cuda]
  memory_gb: 1
  host_memory_gb: 1
  publication_seconds: 2
  transfer_seconds: 1
  preparation_seconds: 1
  intermediate_bytes: 1048576
```

These are explicit synthetic costs, not native measurements. Producer publication
and verification cost is charged per piece after compute, before acceptance;
input transfer and preparation precede downstream command start. Both modes
predeclare the same jobs and use the real queue planner and dependency binding.
Only the baseline's simulator-only eligibility barrier waits for producer gather.
There is no downstream event handler making placement decisions.

JSON records keyed coverage, first downstream start, acceptance-to-ready/start,
full pipeline makespan, producer gather, production p95 and unstarted count,
failed/cancelled/unfinished work, allocation utilization, and intermediate bytes.
Retained intermediate peak includes consumed outputs; pending intermediate peak
counts accepted pieces still waiting for their consumer. Report gather slowdown
and every scenario's regression alongside the mean gain. The overlap gate
requires complete output coverage, an earlier downstream start, lower makespan,
exactly unchanged production p95 and no extra failures/cancellations/unfinished
work. Failed gates still write JSON and exit nonzero. The homogeneous control
expects equality because all pieces accept jointly; it supplies no speed claim.
The lean simulator regression separately exercises controlled producer loss/retry
and parent abort, outside the speed comparison.

## Scarce-machine preservation (#193, slice 6)

Use the same binary interface at baseline `e060a9b9` and with value placement:

```sh
myce sim --fleet examples/sim/value-placement/fleet.yaml \
  --workload examples/sim/value-placement/workload.yaml \
  --days 0.01 --seed 1 --json preservation.json
myce sim --days 1 --seed 1 --json mixed.json
```

The finite fixture uses equal synthetic speeds and two differently sized cards.
Ordinary work fits either; the 32 GiB job fits only the large card. Every stream
is a marked batch, so the largest `batches.*.finished_seconds` is whole-workload
makespan. The fake-clock CPU/component tests separately prove that CPU commands
prefer the small CPU host and GPU commands retain their accelerator.

Paired laptop runs on 2026-10-25 (same inputs, arrivals, policy and seed):

| Workload / metric | Baseline | Value placement |
|---|---:|---:|
| Finite preservation: whole makespan | 300 s | 200 s |
| Finite preservation: large-card job finishes | 300 s | 101 s |
| Finite preservation: production wait p95 | 0 s | 0 s |
| Finite preservation: failed / unfinished | 0 / 0 | 0 / 0 |
| Original one-day mixed: marked batch makespan | 2,691.46 s | 2,691.46 s |
| Original one-day mixed: production wait p95 | 1 s | 1 s |
| Original one-day mixed: completed / failed / unfinished | 194 / 1 / 98 | 194 / 1 / 98 |

The mixed metric is the marked batch's submission-to-accepted-finish interval;
its remaining backlog prevents a whole-workload makespan claim. The existing
simulated failure is unchanged. These runs demonstrate preservation and no
regression on the paired examples, not native performance or a general speedup.
The explicit speed/deadline tradeoff remains a later slice.
