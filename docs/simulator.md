# Scheduling simulator

`myce sim` replays synthetic GPU work through the **real** queue, batch dispatch,
model-call placement, memory admission, priority reclamation, and decaying
fair-share accounting. In-process workers run timers instead of processes or
models. A fake clock jumps between arrivals, finishes, stop acknowledgements,
model readiness/idle expiry, availability changes and worker-loss deadlines.
No GPU, network connection, token, SQLite file, model download or real sleep is
needed. The simulator skips persistence and assumes a reachable pretend NAS.
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
Illustrative workers may share one host RAM budget or unified GPU/CPU RAM. Budgets are **GiB after reserves**, not apparent model-file sizes.
No availability windows means always present. Windows use Monday=0 through
Sunday=6, with start-inclusive/end-exclusive hours in a repeating synthetic
UTC week. `departure: planned` (default) saves resumable work and restarts
non-resumable work. `departure: loss` cuts execution without a final checkpoint;
the real queue's priority loss timeouts and majority-silence guard determine
when batch work becomes available again. Interrupted model calls fail visibly
on abrupt loss; this simulator does not emulate HTTP response replay.

Workload projects accept ordinary queue weight/cap policies. Models specify
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
fleet benchmark or hardware qualification. All supplied identities, topology, sizes, speeds, memory budgets, arrival patterns and durations are fictional examples. File sizes do not establish native peak memory. Portability is hypothetical.

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
