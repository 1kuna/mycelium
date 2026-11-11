# Waiting

Detail for "When a job waits" in [the guide](../SKILL.md). Read the waiting
reason once, then fix your authored needs, input or recipe, or name the next
owner and action on the existing work record. Unknown telemetry, unconfirmed
cleanup, build offers and prepared environments are different states; only fresh
reports prove progress. Don't erase custody, raise shared budgets, repeat
submissions or run heavy work locally to bypass a hold. A field your queue
doesn't return is unknown.

## One blocker

Resolve, status and wait responses name a blocking dependency, owner and action.
An offer is not a started build; an unknown observation is not measured zero
capacity. Keep typed reasons and original intent, and fix the cause rather than
retrying an unchanged deterministic failure. See [diagnostics](../../../docs/status.md).

## Coverage versus capacity

Placement coverage reasons differ from occupied capacity or environment
preparation: `missing_extension` needs a registered implementation;
`execution_coverage` needs a compatible variant; `native_qualification` needs its
exact native check; `production_parity` or `production_parity_hosts` needs
reference-output evidence and the stated owner. Detailed status also explains a
single freshly observed compatible host and an owner's parity list size. These
are coverage limits, not instructions to add a pin. Stale or missing inventory
is unknown, not proof that hardware is unique.

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
[wait evidence](../../../docs/status.md#retained-production-admission-waits-next-release-868).

Overdue waits in every tier can include
`admission_wait.latest.targets[].admission_decision`. `outcome`, `stage` and
`predicate` give the actual check-in's first failure; `at` and
`checkin_machine`/`checkin_session` identify the observation. `not_considered`
means selection was skipped, not a rejected candidate; `not_selected` means an
eligible candidate lost selection order. The threshold is one advertised or
measured check-in opportunity, not a start promise. Unchanged evidence keeps its
timestamp; sibling `predicates` are diagnostic projections. See
[actual decisions](../../../docs/status.md#actual-all-tier-admission-decisions-next-release-1158).

Per-job JSON keeps `last_memory_admission` for a blocked RAM comparison:
envelope, drift, signed headroom and charge/growth owners. It survives startup
failure and restart. Its timestamp is historical evidence, not live usage or the
job's only blocker; a retry starts a new episode.

On matching queue/worker builds, launch, resource-limit and memory-capacity
refusals keep safe numeric evidence and wait on the same logical job without
spending infrastructure retries. A helper that ran does not prove main ran.
`execution_cause.admission` includes aggregate `live_host_growth` contributors and
sample ages; compare those terms before attributing the total to the launching
job. Positive `main: not_started` capacity evidence also protects an older
worker's `launch/io_error` refusal from loss charges. Exact cleanup, release and
absolute deadlines still bind. See [launch capacity waits](../../../docs/status.md).

## Device faults

`device_fault_last_healthy` holds a job that lost an attempt to a driver fault
before it can consume that host's last healthy accelerator of the same backend.
Keep the job ID and key: another host, spare healthy capacity or a health-proven
recovery can make it eligible. The hold pauses the remaining relative
start/recovery budget (status: `paused for device recovery`), and the saved
amount persists through later capacity waiting and restart. Explicit absolute
`start_by` and queued-only `give_up_at` still bind. A started infrastructure
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
hold. See [device quarantine](../../../docs/reference/accelerator-quarantine.md#accelerator-driver-fault-quarantine).

When status includes device or job `fault_evidence`, or terminal
`attempt_history`, it holds bounded attempt/PID/memory/page snapshots for observed
driver faults; fatal native signals add `signal_evidence` (fault address,
loaded-library mappings, core pointers). Compare kernel, capture and sample
timestamps in one `myce status JOB_ID`; delayed samples, ambiguous CUDA
attribution and missing cores do not establish a cause. Report `unknown` until
the causal chain is proved. Enabling cores is a separately authorized operator
task; agents never change limits, core patterns or drivers. See
[fault evidence](../../../docs/status.md#fault-evidence-next-release).

## Storage and disk holds

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
[worker budgets](../../../docs/reference/worker-caches.md#shared-filesystem-budget-811-next-release)
and [local acquisition](../../../docs/reference/models.md#local-acquisition-root-1248-next-release).
Host health, mounts and collection detail: [release behavior](release-behavior.md#automatic-host-hygiene-source-audited-verify-installed-support).

## Urgency, reservations and pausing

Class expresses urgency: `production` only for work the project's rulings call
production, `deadline` only with a real owner-set date, `experiment` otherwise.
Production can preempt lower tiers; eligibility, qualification, capacity and
exact release still apply. Equal-tier work normally waits, and messages asking
other consumers to stop do not change queue order.

Use job-bound validation reservations for isolated qualification, not an
invented project hold or higher class. `myce extension check --reserve` binds
one; `myce reserve add SELECTOR --job JOB_ID --owner OWNER --reason TEXT` binds
an existing validation job. Device checks default to their selected device;
whole-host isolation needs its actual reason. After an unconfirmed write, inspect
`myce reserve list` before retrying the same explicit `--id`; an empty response
does not confirm a reservation. See
[validation reservations](../../../docs/operations.md#reserve-a-host-or-its-devices-for-an-existing-validation-job).

Pause only your own queued job: `myce pause JOB_ID --reason TEXT`, optionally
`--until RFC3339`; `myce resume JOB_ID` keeps its key and queue age. Assigned or
running attempts cannot be paused; cancelling and resubmitting does not improve
their place. Shared policy belongs to the queue owner.

Estimates need comparable complete work and stay uncertain; they are not
deadlines. Distinguish queue wait, preparation, execution, output acceptance and
release. See [status contracts](../../../docs/status.md).
