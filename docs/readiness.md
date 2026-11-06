# Adaptive runtime readiness (#808, next release)

Readiness observes the worker's own latency and forward progress. It has no
automatic whole-load wall deadline. A slow load can continue indefinitely while
its verified byte counts, load counters or health state advance. A silent load
fails through the existing exact-attempt cleanup path; neither a failed probe
nor elapsed time releases memory or Docker custody.

Model load history lives in the worker's local state directory, keyed by the
complete model configuration (weights, engine, arguments and environment).
Another worker or changed model starts cold. Successful readiness updates the
load mean/deviation and longest observed quiet gap. Cold local weights are
calibrated by reading at most one OS page and projecting its measured latency
over the declared weight bytes. This is an uncertainty allowance, not a GPU
throughput prediction. Model cache copy reports and verification reads renew
progress. Environment preparation retains its separate builder protocol; a
queued builder is not evidence of a stalled model process.

Custom servers can report progress on stdout/stderr, including with `logs: none`:

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
| Docker observation 2s and start 30s | An operation-local response envelope learns completed RPC latency. A timed-out read increases its next uncertainty allowance. Streaming forward progress renews silence without extending any explicit caller deadline. This catches an unresponsive CLI/daemon without imposing a whole-operation load clock; cold observations have less evidence than later ones. |
| Docker retry 100ms..5s and warning after 2m | Each failed observation leaves the daemon an idle interval equal to the I/O cost it just consumed. The first unavailable observation records one warning. Recovery can be delayed by the preceding failed request, rather than a deployment-specific cooldown. |
| Docker reconcile 100ms, shutdown checks 10ms, post-stop 5s | Serial reconciliation leaves an interval equal to its measured cost; shutdown uses the observed inspect envelope after the existing stop deadline. Unknown disposition stays pending, including after shutdown. |
| Readiness probes 100ms, timeout 5s, three failures over 15s | Serial probes use their own measured cost for cadence and mean plus deviation for I/O uncertainty. A second failed observation confirms continuous failure beyond that envelope; one failure cannot withdraw readiness. HTTP/container health transitions renew load observation. Explicit sidecar timeouts still win; omission opts into progress-based readiness. |
| Model idle 10m | Per-model, per-worker request spacing and successful reload cost determine how long retaining idle residency is useful. In-flight/queued demand and pins retain their existing protection; competing work can still reclaim residency through the scheduler. Explicit `idle_timeout` remains authoritative. |
| Model wait 5m, queue load slack 5s | Omitted `max_wait` waits for caller cancellation or a real load failure. A positive authored value bounds that caller's queued wait, not a progressing worker load. The worker owns stall detection; the queue's worker-loss and exact-release protocol remain independent. Legacy `load_until` values are cleared. |
| Measurement load 5m | Measurement uses the same progress supervisor and local history. A positive `--load-timeout` is an explicit wall deadline. Safety observations retry only transport timeouts, using measured request cost; an actual safety refusal stops immediately. |
| Measurement warm-up parallelism 1 | Retained for individual incremental-memory attribution: concurrent warm-ups confound per-model peaks. Explicit parallelism instead measures a joint peak. This is a measurement-method boundary, not a host capacity guess. |

The latency estimator uses RFC 6298's mean/deviation smoothing and initial
one-second unmeasured transport allowance. Completed local observations replace
that seed; it is not a universal measured load time. An unmeasured custom server
without accessible local weights has no inferred cold-load deadline until it
provides a first progress/health signal; use an explicit `ready_timeout` when
that unknown initialization must have a wall bound. Real model weights supply
the local I/O calibration instead. Quiet-gap uncertainty
allows one additional observed gap, so a single delayed progress report is not
enough to establish a stall. Duration arithmetic saturates at the representation
limit to prevent overflow. The existing diagnostic line-size bound protects the
stream parser from an unterminated untrusted line; oversized lines cannot provide
progress. These safeguards cost observation latency or lost malformed evidence,
and none supplies release proof.

Managed Docker continues to require already-local digest-pinned images and
`--pull=never`. The pull/extract decoder observes per-layer forward byte counts
and phase transitions if a Docker command supplies such a stream; it does not
add a registry download, authorize network access or change image preparation.
Native pulls are not part of this managed-launch contract.

This change deliberately preserves the lifecycle/retry owner's stop grace,
crash retry/reset policy and worker-loss protocol (#832), and the client-wait
owner's SDK transport defaults (#819). The #800 inventory's extension check wall
deadline is also an execution-lifecycle policy, outside the Docker/model
readiness paths. Those existing tracked policies are not claimed to have a
new measured basis here. Inference `request_timeout` is separate from loading.

## Proof and limits

Fake-clock tests exercise slow advancing model loads and image pull/extract
streams, fast local latency, repeated/non-progressing signals, a true silent
stall, identity-bound load history, and burst demand followed by idle on one and
200 synthetic workers with different CPU/RAM inventories. Real subprocess,
loopback HTTP and fake Docker tests check supervision and exact-attempt custody
without GPUs or a Docker daemon.

Native Docker daemon recovery, real registry pull/extraction, GPU initialization
and native device/boot qualification remain separate checks. They were not run
for this implementation; no fleet service, worker configuration or production
state was changed.
