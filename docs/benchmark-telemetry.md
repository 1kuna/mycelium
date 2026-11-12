# Benchmark telemetry receipts

`mycelium-host-telemetry-v1` is the versioned, private JSONL artifact for a
benchmark's time-aligned host observations. Validate **each line** against
[the host telemetry schema](schemas/host-telemetry-v1.schema.json). The local
`myce telemetry capture` sampler is available in candidate builds containing
this change; it is not implied by an earlier installed release. It neither
contacts the queue nor reads credentials, the database, worker state or spool.
It does not change services, settings or admission policy.

## Attach to each baseline and candidate run

Keep `host-telemetry.jsonl`, the sampler's exit status, binary version/commit,
and the benchmark's existing terminal-job JSON exports beside the run's timing
and parity receipt. Record each artifact's SHA-256 and byte size in that receipt.
Keep operational artifacts private. Use opaque host/card aliases, and retain
the private alias-to-machine/device mapping with the run. Do not publish process
identities, sessions, host names, addresses, PCI mappings or local paths.

Run the sampler **locally on the observed host**, using known daemon PIDs and
physical PCI identities, for a bounded window spanning the replay. For example:

```sh
myce telemetry capture --run-id baseline-r1 --host-ref host-a \
  --queue-pid "$QUEUE_PID" --worker-pid "$WORKER_PID" \
  --duration 30m --interval 5s \
  --card "card-a=$CARD_A_PCI" --card "card-b=$CARD_B_PCI" \
  --native-worker-only > host-telemetry.jsonl
```

The duration is an explicit capture bound, not a benchmark timeout. Choose it
to cover the actual run, with separate idle-before/after windows when useful.
The example cadence matches the existing five-second device-memory cadence;
it is not a qualified peak-detection rate. Use the same cadence and sampler in
baseline/candidate runs. Record read-window durations and sampler overhead;
short spikes between samples remain unknown. No polling loop on the queue is
required. Do not infer permission to run a replay or change an installed worker
from the availability of this command.

Cancellation or a sensor slot deadline emits the partial sample with unavailable
readings marked missing, then exits with an error. Retain that exit status; a
partial receipt is not a completed capture. Sensor cancellation kills the owned
process group; inherited output pipes have an additional one-second drain bound.

Only assert `--native-worker-only` when the worker has no managed attempts
outside its process tree. Otherwise omit it: external attribution is
explicitly missing, while independent RAM/RSS/card readings remain available.
The sampler does not inspect container state to invent this assertion.

## Host record and readings

Every row has `schema`, `run_id`, `host_ref`, random `capture_id`, zero-based
`sequence`, requested `capture_window`, `interval_seconds`, actual collection
`window`, and monotonic `elapsed_seconds` since capture initialization. UTC
RFC3339 timestamps use the observed host's clock. Sequence gaps, truncated
artifacts and uncovered requested time are missing coverage, never zero use.
The requested window is not a claim that every instant was observed.

Each numeric reading has `unit`, `source`, its own `window.start/end`, and either:

```json
{"value": 0, "unit": "percent", "source": "Xe PMU maximum across all enumerated hardware engines", "sampled_at": "2026-11-06T21:00:00.200Z", "window": {"start": "2026-11-06T21:00:00Z", "end": "2026-11-06T21:00:00.200Z"}}
```

or an explicit missing reason, with **no value or sampled_at**:

```json
{"missing": "card_activity_unavailable", "unit": "percent", "source": "Xe PMU maximum across all enumerated hardware engines", "window": {"start": "2026-11-06T21:00:00Z", "end": "2026-11-06T21:00:00.200Z"}}
```

For gauges, the window bounds the read's timestamp uncertainty; it is not a
continuous maximum. For activity it bounds the active/total counter interval.
`sampled_at` is the end of a successful read. The missing reading's window only
describes the attempted observation. Sensors run sequentially: their individual
windows, rather than the row timestamp, establish how closely they align. Do
not assume synchronized clocks across hosts or with the queue. Retain known
clock-offset/error evidence; if absent, cross-clock alignment is unknown.

| Field | Unit and meaning |
| --- | --- |
| `queue.rss`, `worker.rss` | Bytes of each daemon process's current RSS, excluding descendants. `pid` plus hashed `birth_id` fence the process lifetime. PID reuse emits missing RSS and stops capture. |
| `worker_tree_rss` | Bytes of worker plus descendant RSS from the same process inventory. Includes native attempts/model processes; shared pages may be counted repeatedly. |
| `host_used` | Bytes: Linux `MemTotal - MemAvailable`, without admission reserve/clamp. Reclaimable cache is excluded. |
| `external_memory` | Bytes: `host_used - worker_tree_rss`, the existing external-memory residual for native process-tree attribution. **Includes a separate queue process** and other memory outside the worker tree, including the sampler if launched separately. Do not add queue RSS to this residual. Missing attribution or an inconsistent residual stays missing. |
| `cards[].memory_total/used/free` | Raw physical bytes from the supported PCI-identified `xpu-smi --query-gpu=pci.bus_id,memory.total,memory.used,memory.free --format=csv,noheader,nounits` query. No budget/reserve subtraction. `card_ref` identifies each card. |
| `cards[].activity` | Percent: maximum paired Xe PMU active/total ratio across every enumerated hardware engine. Uses the same read-only Xe reader as device telemetry. Missing, incomplete, nonadvancing or multiplexed counters are unavailable, not idle. Activity is not compute occupancy or a throughput prediction. |

RSS/residual estimates are not a disjoint physical-page accounting. Do not add
worker root RSS to its tree RSS. Compute run-window p95/maximum separately for
each metric using valid samples, reporting sample count, first/last sample,
missing count and largest observed gap. Compare simultaneous readings, not a
sum of independently timed component maxima. Sampled maxima are lower bounds
on true transient peaks; the residual can also over/underestimate attributed RAM.

## Reuse retained attempt and host facts

Keep the existing job GET `memory_attempts` and #858 `resource_attempts` verbatim;
their shapes are already described by [job-v1](schemas/job-v1.schema.json) and
[status telemetry](status.md). Bind each row by
**outer job ID + attempt + machine + session**, never job ID alone. Keep the
`*_attempts_dropped` counts and unavailable/lower-bound/source flags. A fan-out
parent is not an additional execution; export the producing children.

For `resources`, CPU time/throttling is in seconds (periods are counts); block
I/O is bytes/operations, PSI averages are percent and totals are microseconds.
Preserve `sampled_at`, `received_at`, `age_seconds`, and `elapsed_seconds`.
`sampled_at - elapsed_seconds` bounds the observer lifetime, including
preparation/teardown; it does not prove uninterrupted counter coverage.
Memory `*_gb` fields use GiB (`2^30` bytes). Attempt memory peaks have no peak
timestamp: bind their coverage to the attempt lifetime when known, mark peak
time unknown, and never fabricate a host snapshot from them. Attempt start/end
times come from the exact execution/ledger; missing historical boundaries stay
unknown. These per-attempt facts are distinct from whole-card memory/activity.

If an already retained status/maintenance artifact includes `external_memory`,
attach it with capture/freshness timestamps and its original `24h`/`7d` windows.
These summaries use fifteen-minute buckets and approximate histogram p95 (up
to 20% bin rounding), plus exact observed max/time and sample counts. They can
include fifteen extra minutes and offline gaps. They are **not** replay-window
samples and cannot reconstruct queue/worker RSS or simultaneous card telemetry.
Do not poll aggregate status or read the live database/spool to fill gaps.
Already retained `memory_observation` and device `telemetry` can supplement
capture only with their original source, timestamp, age and coverage limits;
never renew them by assigning the receipt's current time.
Device telemetry's `free_gb` is in GiB after the worker's device reserve; the
sampler's card `memory_free` is raw physical bytes. Retain that distinction.
