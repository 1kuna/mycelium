# Measurement mechanisms

Part of the [approved design](../design-proposal.md), covering comparable accepted work.

### Comparable observations
<a id="comparable-observations-slice-1-107-approved-2026-10-22"></a>

Observations change no admission, qualification or concurrency limit.

A variant can declare `references` for an existing work type with
`input_shape`, `unit` and the positive number of baseline `units`. An
ordinary job opts in with matching `workload` metadata. Check output
establishes qualification. A comparable, successful accepted execution can
teach duration and peak memory. The comparison binds the work, frozen recipe
and reference, target, device, environment and relevant resource identity.
Queue elapsed time runs from Started through accepted completion. See
[status](../status.md#execution-elapsed-time-last-accepted-duration-and-application-progress)
for unknown and incompatible observations.

<a id="safe-sparse-curves"></a>
<a id="safe-sparse-curves-slice-2-107-approved-2026-10-24"></a>
<a id="completion-aware-placement"></a>
<a id="completion-aware-placement-slice-3-107-approved-2026-10-24"></a>

[MY-49](../DECISIONS.md#my-49) retired sparse sweeps and calibrated completion
forecasts. The [approved design](../design-proposal.md#measured-work-and-completion-aware-placement)
describes current placement measurements.

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

