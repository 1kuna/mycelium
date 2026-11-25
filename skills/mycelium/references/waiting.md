# Waiting

Detail for "Classes and tiers" and "When a job fails or waits" in
[the guide](../SKILL.md). Read the waiting reason once, then fix your authored needs, input or recipe, or name the next
owner and action on the existing work record. Unknown telemetry, unconfirmed
cleanup, build offers and prepared environments are different states; only fresh
reports prove progress. Don't erase custody, raise shared budgets, repeat
submissions or run heavy work locally to bypass a hold. A field your queue
doesn't return is unknown.

## One hold

Resolve, status and wait responses carry one `hold` record: `kind` (stable
code), `owner`, `reason` (one sentence), `clears`, optional `expires`, `next`
(one action) and `detail` (evidence). Act on `kind` and `owner`, not the text.
Host and device fences use the same record in status `holds`; `owner: operator`
means only an operator clears it. An offer is not a started build; an unknown
observation is not measured zero capacity. Keep typed reasons and original intent, and fix the cause rather than
retrying an unchanged deterministic failure. See [diagnostics](https://github.com/1kuna/mycelium/blob/main/docs/status.md).

Single-job text prints the hold's reason and `next` action. `--history` adds
evidence and variants; priority advice appears only for deadline-class jobs.
The queue schedules native checks and environment preparation ahead of useful
work. If no known host can perform that prerequisite, the job fails with a
`dead_end_wait` finding and one action. Busy hosts and unknown/offline inventory
remain waits. A deterministic build failure excludes its exact worker/target/hash;
another compatible builder can proceed. If all compatible builders have failed,
repair a builder or the recipe and resubmit with a new key. Terminal jobs do not
reopen when a builder is repaired.

## Coverage versus capacity

Placement coverage reasons differ from occupied capacity or environment
preparation: `missing_extension` needs a registered implementation;
`execution_coverage` needs a compatible variant; `native_qualification` waits
for the exact native check the queue runs itself. Before submission, the reason
can name the accelerator attempt that must release before extension preparation.
After submission it names the check and its hold. A failed check fails the job
with its action. When a recorded priority stop, maintenance, or validation
reservation interrupts a check before main-command authorization, the queue
keeps qualification pending and retries after exact release and restored
eligibility. Keep the original submission key. An explicit cancellation ends
the check and requires an explicit check to resume qualification.
`production_parity` or `production_parity_kind` (formerly
`production_parity_hosts`) needs reference-output evidence on a machine of a
kind that can run it, then `myce parity add`. Detailed status also explains a
single freshly observed compatible host and the project's approved kinds. These
are coverage limits, not instructions to add a pin. Stale or missing inventory
is unknown, not proof that hardware is unique.

When authored `targets` exclude an observed CPU alternative, the hold can name
one candidate and the evidence behind its next action. Selected immutable
member locks establish dependency coverage. Verify the complete workload before
widening targets, and obtain accepted output parity before production use.
Unknown coverage calls for target-native environment verification. A missing
member lock target requires authoring that coverage. Use `myce portability`
for the full per-target report. Hard requirements such as Darwin developer
tools stay binding.

## Admission evidence

Production job status includes `admission_wait`: first and latest blockers,
candidate targets, exact capacity holders, memory forecasts and sample ages, and
actual scheduler preemption decisions. Check `device_fault` before blaming RAM
pressure on tests; a quarantined device cannot serve work after CPU holders
stop. `preemption.source` separates an actual planner decision from a diagnostic
projection. The target field `release_commitment` reports retained releases
still pending and clears once those releases are acknowledged, even if another
blocker remains. `last_planner_decision` keeps its original timestamp and wave
vector. Read `latest.closed_at`/`latest.closed_reason` for ended waits and
`capture_gaps`/`deferred_at` for capture deferred to protect control service.
Missing values and dropped history stay unknown. See
[wait evidence](https://github.com/1kuna/mycelium/blob/main/docs/status.md#retained-production-admission-waits-868).

Overdue waits in every tier can include
`admission_wait.latest.targets[].admission_decision`. `outcome`, `stage` and
`predicate` give the actual check-in's first failure; `at` and
`checkin_machine`/`checkin_session` identify the observation. `not_considered`
means selection was skipped, not a rejected candidate; `not_selected` means an
eligible candidate lost selection order. The threshold is one advertised or
measured check-in opportunity, not a start promise. Unchanged evidence keeps its
timestamp; sibling `predicates` are diagnostic projections. See
[actual decisions](https://github.com/1kuna/mycelium/blob/main/docs/status.md#actual-all-tier-admission-decisions-1158).

Leave cold accelerator work queued while it waits for measured host RAM.
The offer leaves headroom for measured idle variation and retained launch
shortfalls. A live RAM refusal before main can retry under the same key after
capacity or the offer changes. Read `execution_cause.admission` for required
and available RAM on that attempt. A terminal plain CPU launch OOM requires
a larger explicit `needs.memory_gb` or a smaller workload and a new key.

Per-job JSON keeps `last_memory_admission` for a blocked RAM comparison:
envelope, drift, signed headroom and charge/growth owners. It survives startup
failure and restart. Its timestamp is historical evidence, not live usage or the
job's only blocker; a retry starts a new episode.

Launch, resource-limit and memory-capacity refusals keep safe numeric evidence and wait on the same logical job without
spending infrastructure retries. A helper that ran does not prove main ran.
`execution_cause.admission` includes aggregate `live_host_growth` contributors and
sample ages; compare those terms before attributing the total to the launching
job. Exact cleanup, release and
absolute deadlines still bind. See [launch capacity waits](https://github.com/1kuna/mycelium/blob/main/docs/status.md).

## Device faults

`device_fault_last_healthy` holds a job that lost an attempt to a driver fault
before it can consume that host's last healthy accelerator of the same backend.
Keep the job ID and key: another host, spare healthy capacity or a health-proven
recovery can make it eligible. The hold pauses the remaining relative
start/recovery budget (status: `paused for device recovery`), and the saved
amount persists through later capacity waiting and restart. Explicit absolute
`start_by` and queued-only `give_up_at` still bind. A queue start-fence refusal before your command ran gives back its
assignment-to-refusal time to an explicit `start_by`. A started infrastructure
interruption renews the original first-start window; unstarted losses never do.
If recovery never becomes eligible, the hold ends one maximum start window (one
hour) after the recorded fault loss with `diagnostic_code:
device_fault_recovery_timeout`; keep that terminal result, since a late clear
cannot revive it. Fresh inventory showing both cards healthy releases the guard
after an operator clear or automatic recovery.

Automatic quarantine clearance needs the supporting worker and its vendor health
tool; unknown health keeps quarantine. `devices[].quarantine_recovery` records the
fault event, healthy interval, observation window and clear evidence. With
`device-health-v1`, `devices[].quarantine_recovery_blocked` explains the last
rejected proof. `unsupported_on_device` lists a component excluded only after a
fresh healthy same-model sibling reports it undeterminable in the same batch;
every other required component must be OK. Unknown without that comparison
(including single-card hosts), stale comparison or a monitoring gap keeps
quarantine; command exit alone is not proof. Repeated-fault escalation needs the
maintenance owner. Consumers never clear real devices or resubmit to bypass the
hold. See [device quarantine](https://github.com/1kuna/mycelium/blob/main/docs/reference/accelerator-quarantine.md#accelerator-driver-fault-quarantine).

When status includes device or job `fault_evidence`, or terminal
`attempt_history`, it holds bounded attempt/PID/memory/page snapshots for observed
driver faults; fatal native signals add `signal_evidence` (fault address,
loaded-library mappings, core pointers). Compare kernel, capture and sample
timestamps in one `myce status JOB_ID`; delayed samples, ambiguous CUDA
attribution and missing cores do not establish a cause. Report `unknown` until
the causal chain is proved. Enabling cores is a separately authorized operator
task; agents never change limits, core patterns or drivers. See
[fault evidence](https://github.com/1kuna/mycelium/blob/main/docs/status.md#fault-evidence).

## Storage and disk holds

`disk_headroom` means no eligible host currently has the free space the job
writes: inputs, outputs, work dir and `TMPDIR` scratch (learned or
`disk.scratch_bytes`), checkpoints, and an environment build or declared model
snapshot the host has not cached. The reason names the volume, `disk: N GiB needed, M GiB free on HOST`.
Keep the same job; it starts when a host frees space or a larger one joins.
Lowering the hint below a measured peak changes nothing, because the measured peak wins.

Mycelium's own staging, hashing and checkpoint I/O failures withhold work that
depends on the affected network root, through the host-health reason and alerts;
independent local work stays eligible. Data faults clear only after three fresh
cache-bypass read/reopen proofs; metadata or cached success cannot clear them,
and read-only inputs need no sentinel write. Unsupported cache bypass keeps the
hold for the storage owner. Sibling roles on one backing share one latch. A
worker restart forgets the latch, so the next dependent attempt may fail and
re-latch it. Operator exclusions are independent. The latch covers Mycelium's
own I/O; it does not observe arbitrary consumer syscalls. Keep the same job while the
root recovers; pinning a host or changing mounts does not help.

Filesystem pressure reclaims inactive rebuildable caches across cache classes.
Keep the same job while pending demand triggers cleanup; active readers, queued
references, exact-attempt custody and last unique sources stay protected.
Machine `hygiene_collection.disk_budget` reports physical headroom, reservations
and `foreign_disk_growth`; `scratch.retained_bytes_by_reason` explains preserved
attempt storage. Foreign or external compiler caches are warned about, never
deleted. Installation owners can select verified local snapshot acquisition with
`myce install worker --config CONFIG --model-root PATH`; NAS checkpoints,
environment sources and immutable masters keep their existing contracts. See
[worker budgets](https://github.com/1kuna/mycelium/blob/main/docs/reference/worker-caches.md#shared-filesystem-budget-811-next-release)
and [local acquisition](https://github.com/1kuna/mycelium/blob/main/docs/reference/models.md#local-acquisition-root-1248-next-release).
Host health drains (memory pressure, mounts, disk) name a typed cause in
machine `host_health` and clear after measured recovery. `myce host status HOST`
lists what outside Mycelium holds a host's memory: each foreign process's
`user`, `age_seconds` and `gpu_bytes`, and `host_health.foreign_services` totals
per service or container. They are context for the host's owner, never yours to
stop. `host_disk_exhaustion_forecast` wakes the owner when something outside
Mycelium has filled a volume for as long as its remaining space would last; it
is context, not a drain. A normal fleet
update keeps busy hosts available until a natural idle moment, so keep using the
queue; don't cancel experiments to finish a drain. See
[host health](https://github.com/1kuna/mycelium/blob/main/docs/status.md) and [operations](https://github.com/1kuna/mycelium/blob/main/docs/operations.md).

## Shared interactive host

The agents' host carries the shared/interactive profile
([MY-45](https://github.com/1kuna/mycelium/blob/main/docs/DECISIONS.md#my-45)). There a job's CPU charge is the
larger of its declared cores and its measured peak, carried across
checkpoint/requeue and shown in the CPU entry of status `resources`. Owner
pressure stops new admission and yields every running job on that host,
production included: memory warnings or PSI, Linux CPU PSI, owner scheduling
latency, or swap growth with latency. Resumable jobs checkpoint and requeue;
non-resumable jobs stop and requeue; neither is charged an infrastructure retry.
Keep waiting on the same job and key, with no manual resume and no pressure
polling. Dedicated hosts are unaffected. See
[shared hosts](https://github.com/1kuna/mycelium/blob/main/docs/shared-hosts.md#measured-cpu-and-owner-yield).

## Classes and preemption

Owner policy orders project/class selectors. The default tiers are
`production`, `deadline`, and the `experiment` catchall. The planner tries
free capacity first. A priority stop waits for exact release and respects
eligibility and qualification. Usage balancing does not interrupt equal-tier
work. [Scarce-host yield](https://github.com/1kuna/mycelium/blob/main/docs/design-proposal.md#scarce-host-yield)
can move eligible finite work for a hard requirement with only one capable host.

Read `copies[].priority_stop.cause` (or the retained attempt's `stop_cause`)
to distinguish scheduling from memory safety. `priority_preemption` names a
waiting higher-tier demand. `shared_gpu_pressure` names a device safety stop;
`memory_overcommit_yield` names lent host RAM. The safety stops use the planned
checkpoint/requeue path and can stop equal tiers. After a lending yield,
placement and worker launch checks reserve the surviving lenders' measured
growth through stop grace and sensor freshness. Keep the original job/key;
it waits until measured headroom returns or those attempts release, rather
than repeatedly restarting into the same pressure. A completion-time memory
sample cannot reconstruct the stop-time forecast.
`waiting for lent host memory` retains the last started copy's yield cause
across requeue and queue restart. Readmission on that host needs room for the
running attempts' unconsumed ceilings and the resumed job's limit, so a flat
sample cannot restart the lending loop. Another compatible host can still run
the job. Stale or unknown physical telemetry blocks admission and does not
authorize a memory-safety yield.

- **Production** is for work a person or customer is waiting on, as the
  project defines it. A priority stop requeues the displaced job without a
  retry charge. Accelerator jobs and resumable CPU jobs get ten seconds by
  default (`--preemption-stop-grace`); non-resumable CPU work stops immediately. A production job must start within its
  start window (`start_window`, default 150 s, at most 1 h; `start_by` for an
  absolute time). A stuck start fails
  visibly. Read the [start-window contract](https://github.com/1kuna/mycelium/blob/main/docs/jobs.md#production-start-deadline) for which waits pause the relative budget. Infrastructure interruptions requeue it at most twice; its own
  command failures and OOMs fail without retry. Production needs explicit
  targets and runs only on machine kinds the project approved for that work.
- **Deadline** needs a real owner-set date (`--class deadline --deadline
  RFC3339`). It stops only resumable lower-tier work, with two minutes to save.
  The date orders work; it is not a completion guarantee.
- **Experiment** can be stopped by either tier and requeued. A stop is not
  charged as a loss, but a non-resumable job restarts from scratch, so make long
  experiments resumable. Experiments keep a three-loss limit for real losses.

Ready production can reclaim useful lower-tier host RAM even when existing
charges exceed a reduced allowance. Signed debt is repaid before admission,
not hidden by clamping free RAM to zero. Backfill cannot deepen production's
host RAM/CPU deficit, including on another card; compatible other hosts remain
available. Exact cleanup and physical-memory checks still gate replacement.

Within a tier, jobs order by readiness (environment and model), earliest
deadline, then waiting age, and competing projects share by recent use. There
is no in-tier ordering hint. To order your own work, use `after:` stages, and
the class the work actually deserves; pauses, local locks and holding back
submissions only hide work from the queue. A critical Mycelium update may
checkpoint and requeue even production (MY-39); work stopped that way, or by
host maintenance, resumes ahead of its tier's other queued work while its host
drains toward it. Owner pressure on a shared interactive host yields every tier
(below). See
[scheduling](https://github.com/1kuna/mycelium/blob/main/docs/scheduling.md) and
[start deadlines](https://github.com/1kuna/mycelium/blob/main/docs/jobs.md#production-start-deadline).

## Reservations and pausing

Use job-bound validation reservations for isolated qualification, not an
invented project hold or higher class. `myce extension check --reserve` binds
one; `myce reserve add SELECTOR --job JOB_ID --owner OWNER --reason TEXT` binds
an existing validation job. Device checks default to their selected device;
whole-host isolation needs its actual reason. The reservation can withdraw an
assigned check during sidecar startup and waits for its exact release. After an
unconfirmed write, inspect `myce reserve list` before retrying the same explicit `--id`; an empty response
does not confirm a reservation. See
[validation reservations](https://github.com/1kuna/mycelium/blob/main/docs/operations.md#reserve-a-host-or-its-devices-for-an-existing-validation-job).

A queued pause keeps the job's key and queue age. A running yield pause uses the
planned maintenance stop and waits for exact release. It keeps saved progress
and spends no infrastructure loss. Use the CLI commands in [Pause, resume and
cancel](../SKILL.md#pause-resume-and-cancel). For the HTTP attempt and pause
revision fences, Python arguments, and fan-out behavior see
[Pause queued work](https://github.com/1kuna/mycelium/blob/main/docs/jobs.md#pause-queued-work).
Shared policy belongs to the queue owner.

## Production leases

Submit later stages with `after:` first: the queue then sees them, starts each
when its inputs are accepted and cancels them with their producer. A planned
drain or fleet upgrade keeps at least one enabled host able to start each queued
production job, including a stage still waiting on `after:`; that protection
ends when the stage runs, is cancelled or its upstream fails. You don't need a
lease for those stages. Open a lease only for production stages you can't
submit ahead (for example, ones whose spec depends on earlier results), so a
drain can't strand them:

```sh
myce fleet lease open --project P --key RUN --for 4h --platform cuda --memory-gb 24
myce fleet lease close --project P --key RUN
```

Python: `client.open_production_lease(project, run_key, "4h", [{"needs": {...}}])`
and `client.close_production_lease(project, run_key)`. Each stage takes a job's
placement fields (`needs`, `allowed_machines`, `excluded_machines`); without
stages the lease covers any host your project may use. Pick a duration that
covers the pipeline, reopen the same key to renew, and close it when the last
undeclared stage is submitted. It expires on its own, grants no priority and reserves no
capacity. A critical Mycelium update may still checkpoint and requeue production
work (MY-39); requeued or newly submitted stages wait for the host to return.
See [production leases](https://github.com/1kuna/mycelium/blob/main/docs/operations.md#production-leases).

Estimates need comparable complete work and stay uncertain; they are not
deadlines. Distinguish queue wait, preparation, execution, output acceptance and
release. See [status contracts](https://github.com/1kuna/mycelium/blob/main/docs/status.md).
