# Adaptive runtime readiness (#808, next release)

Readiness observes the worker's own latency and forward progress. It has no
automatic whole-load wall deadline. An advancing load can continue indefinitely.
Successful comparable local loads teach a silence envelope; a learned load with
no advancing signal can fail through exact-attempt cleanup. Neither an observation
timeout nor elapsed time supplies release proof.

A cold load has no inferred silence deadline, including after its first progress
signal. Valid silent initialization and an unknown stall are indistinguishable
without guessing. This is an **orchestrator default under MY-34**, not a direct
maintainer ruling. Explicit caller deadlines and authored readiness limits bind.
CPU time, read bytes attributable to the owned model process tree, and observed
host/device memory growth count as activity. CPU/I/O use #858's shared per-attempt
resource fields and observer; no duplicate CPU/I/O sampler is introduced. Missing
OS counters remain unknown. Learned silence detection allows at least two existing
slowest enabled shared-sensor periods, including device memory (one observation
interval plus scheduling uncertainty), so
it cannot expire before a new CPU/I/O sample can arrive. This observability floor
does not create a cold-load deadline. Physical read bytes can exclude cached model reads,
and process reads can include runtime dependencies; they establish activity,
not completed tensor loading. Process existence alone is not activity.

If a cold load has no activity between resource observations while eligible,
prepared demand waits for its resources, status says that custody is retained and
owner attention is needed. The existing adaptive health path raises one
`model_load_needs_attention` alert per episode, with reason
`cold_load_inactive_contended`. Activity or contention clearing rearms it.
No alert kills the load. Observation cadence determines visibility latency;
it does not become a cold-load deadline.

Model load history lives in the worker's local state directory, bound to model
configuration, immutable environment recipe, OS/architecture and resolved device
and platform. Another host, recipe or target starts cold. Successful readiness
updates load mean/deviation and the longest quiet gap. Concurrent completions
merge the latest state and publish through unique temporary files. Cache copy
reports and verification reads renew progress. Environment preparation retains
its separate builder protocol; a queued builder is not a stalled model process.

Custom servers with captured logs can report progress on stdout/stderr.
With `logs: none`, output descriptors remain null devices, so progress comes
from health transitions and shared OS activity instead of stdout records:

```text
MYCE_LOAD_PROGRESS {"stage":1,"completed":12,"total":100}
MYCE_LOAD_PROGRESS {"stage":1,"completed":13,"total":100}
MYCE_LOAD_PROGRESS {"stage":2,"completed":0,"total":20}
```

Stage indices must increase, and completed counters must increase within a
stage. Totals must be positive and completed must stay within the total.
Repeated counters and ordinary log traffic do not renew progress. The built-in
percentage parser also recognizes loading/tensor percentage lines from model
servers. Servers with long silent initialization phases should report their
actual loading progress. Process existence, heartbeat arrival and repeated HTTP
503 responses are not loading progress.

## Retired assumptions and retained boundaries

| Previous policy | Replacement, failure prevented and cost |
| --- | --- |
| Readiness probes 100ms, timeout 5s, three failures over 15s | Serial probes use their own measured cost for cadence and mean plus deviation for I/O uncertainty. Completed failures confirm continuous failure beyond that envelope; one failure cannot withdraw readiness. Timed-out responses widen the next probe allowance and retain degraded custody so slower healthy replies remain observable. HTTP/container health transitions renew load observation. Explicit sidecar timeouts still win; omission opts into progress-based readiness. |
| Model idle 10m | Per-model, per-worker request spacing and successful reload cost determine how long retaining idle residency is useful. Assigned or eligible local demand and pins protect the matching residency; competing work can still reclaim residency through the scheduler. Explicit `idle_timeout` remains authoritative. |
| Model wait 5m, queue load slack 5s | Omitted `max_wait` waits for caller cancellation or a real load failure. A positive authored value bounds that caller's queued wait, not a progressing worker load. The worker owns stall detection; the queue's worker-loss and exact-release protocol remain independent. Legacy `load_until` values are cleared. |
| Measurement load 5m | Measurement uses the same progress supervisor and local history. A positive `--load-timeout` is an explicit wall deadline. Safety observations retry only transport timeouts, using measured request cost; an actual safety refusal stops immediately. |
| Measurement warm-up parallelism 1 | Retained for individual incremental-memory attribution: concurrent warm-ups confound per-model peaks. Explicit parallelism instead measures a joint peak. This is a measurement-method boundary, not a host capacity guess. |

The latency estimator uses RFC 6298 mean/deviation smoothing and a one-second
initial **transport observation** allowance. Completed observations replace that
protocol seed; timeouts expand the next allowance. It never bounds cold process
start or sidecar initialization. Learned silence permits one additional
observed quiet gap as measurement uncertainty. Duration arithmetic saturates at
the representation limit to prevent overflow. The existing diagnostic line-size
bound protects stream parser memory against unterminated untrusted lines;
oversized lines cannot provide progress. These safeguards cost observation latency
or lost malformed evidence, and none supplies release proof.

This change deliberately preserves the lifecycle/retry owner's stop grace,
crash retry/reset policy and worker-loss protocol (#832), and the client-wait
owner's SDK transport defaults (#819). The #800 inventory's extension check wall
deadline is also an execution-lifecycle policy, outside the model
readiness paths. Those existing tracked policies are not claimed to have a
new measured basis here. Inference `request_timeout` is separate from loading.

## Proof and limits

Fake-clock tests exercise slow advancing model loads, fast local latency, repeated/non-progressing signals, a true silent
stall, identity-bound load history, and burst demand followed by idle on one and
200 synthetic workers with different CPU/RAM inventories. Real subprocess and
loopback HTTP tests check supervision and exact-attempt custody without GPUs.

GPU initialization and native device/boot qualification remain separate checks. They were not run
for this implementation; no fleet service, worker configuration or production
state was changed.
