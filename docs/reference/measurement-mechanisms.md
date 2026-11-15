# Measurement mechanisms

Part of the [approved design](../design-proposal.md), moved verbatim apart
from relative links. Product contracts and maintainer rulings are unchanged.

### Comparable observations
<a id="comparable-observations-slice-1-107-approved-2026-10-22"></a>

Observations change no admission, qualification or concurrency limit.

A variant may declare `references`, keyed by its existing work type. Each entry
has `input_shape` (an opaque compatibility bucket), `unit` (a generic work
unit), and positive `units` (the amount in that work type's bundled native
baseline). It requires that variant's check command and baseline. This is a
serial reference contract: one independent item, concurrency 1, vector batch
size 1. Threads, backend/cache settings and correctness semantics belong in the
frozen recipe; changing the reference or recipe changes the extension digest
and requires qualification again. Sweeps run only as [safe sparse
curves](../design-proposal.md#safe-sparse-curves) allow.

An ordinary extension job opts into comparable timing with
`workload: {input_shape: small-v1, units: 100}`. The author promises the same
serial execution mode and work unit as the matching reference. Absent metadata,
legacy profiles, checks (which have not yet qualified), resumptions and unmatched
shapes remain unknown. Successful accepted attempts alone update a per-key mean;
failed, interrupted, stopped, stale and duplicate results cannot contribute.

Keys bind work type, extension digest, exact target, reference, input shape,
physical host/component, hardware/backend/environment fingerprints, host/device
allowances, and the attempt's declared resources. Queue Clock timestamps measure
Started acknowledgement through accepted completion: preparation is excluded,
while cleanup/result handoff is included. These are observed elapsed times, not
isolated kernel timings; contention is reported and mixed/unknown occupancy is
excluded from comparable estimates. No rates are inferred from old scalar
onboarding benchmarks. Evidence persists as per-record SQLite rows alongside jobs.

Status exposes the frozen prediction, actual accepted elapsed time, sample
count and unknown reason. Predictions use only matching prior evidence, and
placement uses them only through [completion-aware
placement](../design-proposal.md#completion-aware-placement). Status remains a read-only projection:
no mutation, persistence or history-wide reserialization is added to status
reads.

### Safe sparse curves
<a id="safe-sparse-curves-slice-2-107-approved-2026-10-24"></a>

> **Removed by [MY-49](../DECISIONS.md#my-49).**

Sweeps change no placement or ordinary admission/concurrency limit.

A variant may declare `sweeps`, keyed by a declared reference work type, with a
`command`, sorted supported power-of-two `counts` starting at 1, and positive
`max_concurrency` (at most 64). One command implements concurrency internally;
Mycelium does not launch concurrent GPU attempts. `--curve-max-concurrency N`
on `join`, together with `--onboarding-benchmark`, explicitly enables sweeps up
to the smaller operator/recipe bound (default 0, disabled). There is no periodic
load generation. Prepared exact variants and native baselines must qualify first.

Reuse host-wide benchmark preparation custody, command supervision and cleanup.
Busy/recovering hosts cannot start; components run serially. Daily-use hosts need
fresh locally measured idle recovery, external-power and safe-heat evidence. Owner return or
unsafe/unknown signals cancels the command, retaining custody until cleanup.
A sweep has a 60-second Clock budget, and each command a ten-second bound.
Stop on non-increasing aggregate throughput, memory exhaustion or failure.
The declared per-item CPU/RAM/device floors bound counts conservatively; unified
memory is charged once against the one host allowance. Unknown memory permits
no point. No resources are granted by learned measurements.

The command gets `MYCE_BENCHMARK_CONCURRENCY`, normal check input/output/work,
and `MYCE_BENCHMARK_OUTPUT`. It replays the bundled reference with unchanged
quality, writes each item’s native output to `MYCE_CHECK_OUTPUT_DIR/0`, `/1`,
and so on (all are compared), and reports aggregate `units`, measured service `seconds`, mean
`latency_seconds`, baseline and peak host/device GiB, plus `out_of_memory`.
Correct output is required for successful samples. Incremental memory is peak
minus baseline; high-water memory never decreases. OOM permanently forbids that
exact point, even if a later successful sample arrives.

Store points as separate rows in the existing observation store. Clean accepted
ordinary attempts refine their exact point using `0.8 * old + 0.2 * sample`;
serial-only ordinary timing stays serial. Failed, mixed, interrupted, duplicate
and stale attempts never refine rates. Fingerprint, hardware, recipe/reference,
environment or allowance changes retain evidence with a stale reason and prevent
rate reuse; 30-day-old points also become stale. JSON, CLI and HTML status expose
rates, latency, incremental/high-water memory, samples and stale/forbidden reasons.
Native mixed-load curves and fleet acceptance require separate authorization.

### Completion-aware placement
<a id="completion-aware-placement-slice-3-107-approved-2026-10-24"></a>

> **Reduced by [MY-49](../DECISIONS.md#my-49)** to the last accepted duration on each target.

Rank qualified extension commands using a common completion estimate: declared
workload units divided by the exact comparable serial measured rate, plus cold
environment setup, model copy and input-file transfer costs. Reuse the existing
setup/copy estimates; input transfers require measured network throughput.
Missing, stale, forbidden, resumed or incompatible evidence remains unknown.
Unknown estimates retain the existing placement decision exactly, including
legacy scalar benchmark ordering. Comparable measured candidates precede
unknown ones; distinct reference groups and equal estimates have deterministic
ordering.

Eligibility precedes ranking. Existing resource and learned memory floors,
platform preferences, owner policy, tiers, fairness, project caps, holds and
checkpoint/cleanup fences remain authoritative. Reuse the existing victim
selection and minimize stop count/lost-work cost before completion ranking.
Only currently admissible alternatives can defer dispatch; estimates create no
reservations and never wait for a busy fast host. Ranking changes no
concurrency: attempts share a card only under the [shared GPU
rules](../design-proposal.md#inventory-and-resource-ownership) (MY-30). Freeze the chosen measured
estimate's reason for JSON/CLI/HTML status. All time comparisons use the
injected Clock; ranking does no I/O.

Model calls use the shared estimate seam but remain unknown without the
explicit model/reference binding defined under [declared fan-out](../design-proposal.md#declared-fan-out-and-batch-planning).
Their routing, configured limits and victim selection remain as configured.
Extension command sweep concurrency does not qualify a model server for
concurrent calls. The quality-tolerance filter belongs at the existing
qualified-variant eligibility boundary ([variant eligibility](../design-proposal.md#variant-eligibility-and-production-approval)),
before completion ranking; ranking adds no quality policy.

### Repeated plain-command measurements
<a id="repeated-plain-command-measurements-878"></a>

Under MY-31/MY-34, native plain commands without workload units can teach seconds
per whole job through the same accepted-attempt timing path and observation store.
The speed key adds a digest of the exact run, frozen source archive and input
manifests, output contract, environment recipes and isolation. It retains the
existing target, physical host/component, runtime fingerprint, resources,
allowance and power-mode identity. This exact-byte identity is deliberately
narrower than the memory work identity; changing source/input size cannot reuse
a prior speed, even when memory learning remains comparable. Unversioned legacy
inputs, dynamic upstream inputs, external work directories/runtimes and missing
runtime fingerprints remain unknown. Undeclared external state is outside this
comparison contract; these observations do not qualify software or prove parity.

Only successful accepted serial attempts with exact custody and complete clean
contention evidence contribute. Resumptions, failed/interrupted/stale/duplicate
results and intervals with another host obligation are excluded. Existing memory
pressure classification must be eligible, and complete CPU scheduling-stall,
quota-throttling and I/O-stall counters must measure zero. Linux `cpu.pressure`
is read in the existing resource sampler; quota throttling alone cannot detect
an unrelated process delaying execution. An independently negotiated optional
CPU-pressure field preserves the prior telemetry contract with older queues.
This has no new tuned pressure threshold: its cost is that internal parallelism,
intrinsic I/O stalls and platforms without complete counters cannot teach a clean
comparison. No second collector or benchmark runs.

Each comparable key retains the incremental mean seconds/job, a clean sample
count and observed minimum/maximum. One sample explicitly leaves variability
unknown; the observed range is not a confidence interval or a deadline. Existing
staleness/invalidation rules apply. Each speed mutation records its exact
before/after digest in the job's accepted-sample ledger transaction, preserving
release-verification evidence even when the usable timing window restarts.
Status and inspection expose this evidence
through work curves and attempt timing. Completion ranking adds existing setup
and measured transfer costs and remains after admission, tiers, fairness and
least-valuable fit. Estimates neither reserve capacity nor wait for a busy faster
machine. Prediction-error calibration is separate work (#879).

