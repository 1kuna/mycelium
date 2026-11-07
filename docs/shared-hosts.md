# Shared-host pressure freshness

Shared interactive hosts protect their owner with measured reserves, polite CPU
execution and pressure-triggered yield. The canonical admission, thresholds and
native qualification guidance is in [operations](operations.md#pressure-response).

## Freshness during slow check-ins

New workers negotiate `host-probe-v2`. Pressure sensors refresh independently
of check-ins, on a cadence learned from this host's probe cost, CPU capacity and
signal changes. The queue requires age below that observation's frozen
`probe.valid_seconds`; it derives observation time from its injected Clock minus
worker-local age. Worker wall timestamps cannot renew evidence.

Matching-session in-flight check-in contact can discount queue delay within the
existing contact budget. Total age is still bounded by the observation's frozen
validity plus that budget. A failed probe removes its observation immediately;
a pending probe retains the old age and never extends its horizon. Missing/stale
pressure withholds new work without becoming measured pressure or stopping
running work. Direct launch and exact release checks remain authoritative.

Older workers retain the legacy 5-second cadence, 15-second sample age and
30-second contact allowance until upgraded. These are compatibility stopgaps,
not learned public defaults. See [adaptive host observations](host-probes.md)
for the replacement calculation, owner recovery and remaining inventory.

## Health agreement

Host maintenance health includes pressure freshness for shared-profile hosts and uses the
same bounded session contact as worker-loss detection. Fleet status copies those
contact facts into its detached observation and uses the same host classifier.
Expected owner-pressure yield is temporary occupancy and does not itself emit
an infrastructure-failure alert or mark the host unhealthy. Missing/stale sensor
evidence still does, including when a previous pressure latch remains set.
This fixes a host-status response being healthy despite unknown/stale pressure,
and a fleet snapshot losing in-flight contact and interpreting queue delay as
silence. The ordinary one-second public status cache remains an older snapshot;
compare snapshot times when checking separately issued commands. See
[status fields](status.md#shared-interactive-profile).

The implementation is queue-side and worker-side. Upgrading only the queue
fixes health/contact interpretation but does not make older workers refresh
pressure independently of slow requests. Native pressure calibration remains
separate from the fake-clock incident regressions.

## Measured CPU and cooperative yield (next release)

Shared-host admission charges the larger of declared cores and the logical
job's measured peak CPU need. The existing CPU-time telemetry supplies adjacent
sample rates; macOS sampled process-tree readings remain lower bounds. Whole
core accounting rounds up. Missing telemetry keeps the declaration, and later
quiet samples never erase demonstrated overuse. The learned need survives
checkpoint/requeue; it does not predict another submission's concurrency.

Memory warning levels, host-relative Linux memory/CPU PSI and owner scheduling
latency stop new admission. Existing host-health swap-growth/latency evidence
also yields resumable lower-tier jobs through exact-attempt checkpoint, stop and
release custody. Production tier and non-resumable work stay running with a
reason naming the pressure and retained work. Recovery permits the same queued
job to resume, subject to the existing owner-latency recovery hold.

`host-probe-v2` negotiates the additive CPU PSI fields; older queues receive the
legacy pressure shape, without mutating local measurements. macOS still has no
hard aggregate CPU-time or memory containment. This policy reduces new admission
and cooperatively yields work; it cannot guarantee recovery from an OS freeze.

The fake-clock fork-growth replay uses a retained incident's 16 GiB physical
host, 2 GiB declaration (2.125 GiB resolved), 2,004 members, 64.07 GiB RSS-sum
and 44.83 GiB swap-out activity. Intermediate values and timing are synthetic;
the retained record does not establish that chronology. The first warning
triggers checkpoint yield before the critical observation. Repeated pressure
keeps the original stop deadline. An explicit RSS-guard failure before or racing
that yield remains a resource failure, with one attempt/release accounting path;
owner-pressure provenance cannot turn it into a successful checkpoint or an
infrastructure retry. Summed RSS/compressed footprint is not exclusive physical
RAM, and swap-out activity is not swap allocation.
