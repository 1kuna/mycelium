# Memory admission

Mycelium estimates how much memory a job will use. Submit CPU RAM or GPU
memory in `needs.memory_gb` and additional GPU host RAM in
`needs.host_memory_gb`. Omit memory for automatic sizing. Estimates guide
placement and concurrency. Set `advanced.memory_limit_gb` to opt into a
host RAM limit. [Job fields](../jobs.md)

## Pools and estimates

Each physical host has a RAM pool. Each discrete card has a separate pool.
Unified-memory accelerator allocations share the host's RAM pool.

For each pool, Mycelium estimates typical use and a cautious admission charge.
It learns from comparable runs on the same platform family and falls back to
broader evidence or the supplied hint. A new discrete-card workload gets
exclusive card access until it has measured device evidence. Accelerator jobs
can share host RAM from the first run. With sparse accelerator RAM history,
Mycelium uses available platform peaks, then same-OS CPU peaks.
When a run has been measured but there is too little evidence to calibrate
its charge, an existing hint remains a floor alongside the observed peak.

The available RAM reading already accounts for occupied memory. Mycelium
subtracts the host reserve and the amount running work can still grow into.
Each attempt holds the larger of its charge and its observed peak. For
discrete cards, placement also respects the device's reported free memory.
Cards configured with a fixed budget use that budget less known reservations
when the worker has no live free-memory counter.
If the cautious charge exceeds a target's entire allowance, the queue can
admit using the typical estimate, subject to any measured retry floor. The
worker checks typical use against fresh room before releasing the payload.
Relative start budgets pause during `memory_wait`.

The highest-ranked blocked job on a host reserves enough pool capacity for
its charge. Later jobs can use the surplus the head job will leave. Production
can reclaim RAM from lower tiers and receives new work after exact release
confirmation. [Admission and backfill design](../design-memory-admission.md#24-admission)

## Containment and pressure

Linux uses an accounting cgroup per attempt and the shared worker slice's
ceiling. A submitter's explicit RAM limit or an authored member cap adds a
cgroup limit. During a slice OOM, inherited tier-relative OOM scores favor
keeping higher-priority work.

On Darwin, Mycelium samples the owned process tree's physical footprint and
reacts to the OS warning level or falling availability. The host forecast uses
the lower loss rate from two consecutive sampling intervals for the stop
grace. A single drop can still trigger a stop if the reserve is breached or
projected to run out before the next sample. A pressure stop
follows tier priority and measured overage. Workers without usable native
containment or sampled protection report a typed readiness hold.

A stop or an OOM provides a lower bound for future estimates. Save
checkpointable progress so that a pressure stop can resume the work. A
non-resumable job starts again from scratch after a stop for higher-priority
work. [Checkpoint contract](../files.md#resumable-jobs)

## Adjacent release

Upgrade workers before the queue. New workers interpret r6.35 offers as
estimates. New queue offers require the pooled memory capability.
Attempts already running retain their exact custody and containment.

`memory` on a job reports per-pool estimates, current held and observed
peak values, and an explicit limit if present. `memory_envelope` is kept
readable for the adjacent release. See the migration and rollback details
in the [design](../design-memory-admission.md#4-migration).
