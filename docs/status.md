# Seeing your mycelium

`myce status` shows current work, machines, holds and health conditions.
`--json` returns the compact view (`CompactStatus`). Use `--sections jobs,machines`
for full job and machine diagnostics, `--jobs all` for a history page
(`StatusJobsPage`), or `--history` for retained state (`Snapshot`).
Use `--sections machines` for full machine telemetry and current job summaries.
The job summaries use committed reasons. Add `jobs` to the sections to
calculate current placement diagnostics.
For one job use `myce status JOB_ID`, `myce wait JOB_ID` and `myce logs JOB_ID`.

## Queue restart grace

Compact and full fleet JSON include `restart_grace`. `active` is true while
the restart-recovery budget remains. `remaining_seconds` gives the smaller
remaining budget from idle control opportunity and the window established by
successful worker check-ins. Before the first successful check-in, only idle
control opportunity counts. `accrued_seconds` gives the configured grace minus
that remaining budget, capped at the configured grace. Both are zero if no
restart window was configured. A cached response retains the value at the
response's `now`. See [how the queue measures the window](reference/scheduling-mechanisms.md#queue-restart-recovery).

The restart warning concerns only unreported workers outside planned host
maintenance. Grace can be active after the warning disappears. Maintenance
can mark a silent worker gone at expiry, including on a drained or disabled
host. In-flight reports, their bounded reconnect delay after cancellation,
and exact pending worker handovers can still protect the affected session.
Older queues and idle-only responses omit the field.

## Idle machine-hours while runnable work waited

Run `myce status --sections idle --json` for the last seven days as measured
by the queue clock. Set `--idle-since RFC3339` to choose the start of the
window. The `idle` section includes total machine-hours, the breakdown by
refusal reason, and the same reason breakdown and state totals for each
machine class. Classes use OS, architecture and the host's accelerator
backends. Worker identities and cards on the same physical host count once.

Each elapsed span contributes to one state.

- `working` covers active queue work and its in-flight assignments and
  preparations. Release custody is recorded separately in the interval.
- `idle_no_compatible_ready_demand` means the observed host had no
  dependency-ready work with a compatible, qualified target.
- `idle_compatible_demand_refused` means the idle host had such work and
  the queue observed an actual refusal. If several jobs waited, the earliest
  observed refusal accounts for the host's time once.
- `intentional_hold` covers a drain, maintenance, a departing worker, or
  compatible demand paused by its consumer. In-flight work is counted as
  working while a drain is in progress.
- `unknown` covers missing observations, expired admission evidence,
  skipped captures and history outside measured retention. Its hours are
  separate from the idle-while-waiting total.

For the underlying evidence, add `--idle-intervals` and use a narrower
`--idle-since` to inspect a specific period. Each interval includes the host's
observed online state, maintenance, in-flight work, release custody, resource
readings, dependency-ready job count, target compatibility and qualification,
and the actual first admission predicate or selection when observed. Reasons
come from real check-ins and can span at most the worker's advertised or
measured opportunity. A change to matching inputs can invalidate a reason
sooner. The newest undurable span is reported as unknown until a tick
publishes it. Older queues omit `idle` or reject the section.

The queue keeps interval history in its local `.idle` companion database within
the measured evidence headroom. Its writer is independent of the queue database.
A requested window can be longer than the retained coverage. Include `usage`
in `--sections idle,usage --json` to read `usage.cards[]` in the same response
for the separate accelerator metric. A card can be idle while the physical
host is working.

Job rows include the project and the optional submission key. Compact JSON
places them at `jobs[].project` and `jobs[].submission_key`; detailed views
and single-job JSON put them under `spec`. The compact view groups paused
jobs. Use a detailed view or select a paused job by ID to see its key.
Submission keys identify work within a project. Keep credentials out of keys.

To verify a model catalog rollout, check `hyphas[].model_config_hash` in
`myce status --json` or `machines[].model_config_hash` in
`myce status --sections machines --json`. This is the last hash reported by
that worker. Check its availability and freshness before treating the hash
as current. An omitted hash means no hash was reported.

When a worker cannot report managed runtime proof, `myce status` displays the
stage when known. Compact JSON reports `hyphas[].fleet_proof_issue` and machine
detail reports `machines[].fleet_proof_issue`. The worker log also records
changes to failure codes, including while the queue is still on an older release.

For a refused worker start, use `myce status --history JOB_ID` or read the
exact attempt's `execution_cause.launch_refusal` in `myce status --json JOB_ID`.
The worker journal records the same boundary and class as
`event=worker_launch_refused`, keyed by job ID and attempt.

| `launch_refusal` | Observed boundary |
| --- | --- |
| `custodian_startup_failed` | The detached custodian exited before startup. |
| `assignment_withdrawn` | The worker received a stop directive before starting the assignment. |
| `launch_rpc_stale` | The queue rejected the exact launch request's attempt custody with HTTP 409. |
| `queue_launch_refused` | The queue recognized the current assignment and refused its launch gate with HTTP 409. The ledger may also have a `start_refused` entry naming the fence. |
| `launch_rpc_conflict` | HTTP 409 arrived without a boundary, as with an older queue. |
| `launch_rpc_failed` | The launch RPC supplied no usable authorization. Read `execution_cause.class` for the available transport or response class. |
| `local_launch_refused` | An unclassified local refusal occurred before execution. |

The `launch_authorization` predicate stays on the receipt for release and
retry compatibility. A missing `launch_refusal` leaves the specific boundary
unknown. An obsolete result may settle its own release, while the queue
continues to reject its outcome. In that case only the worker journal carries
the new boundary for that result.

### Attempt launch timings

Use `myce status --history JOB_ID` for launch phases in seconds and add
`--json` for timestamps and full precision. `launch_timings` on a job belongs
to its current attempt. In `attempt_history[]`, it belongs to that row's
attempt, machine and session. Older worker receipts can omit the field.

`assignment_received_at` records when the worker received the check-in reply.
For delay before receipt, compare it with the same attempt's `assigned` entry
in `myce ledger JOB_ID`. The queue and worker timestamp difference can include
clock skew. `inputs_ready_at` marks when the launch owner continued past staging.
The existing `inputs_staged_at` and `staging_timings.completed_at` report file
copy completion, which can precede consumption when a copy finishes
asynchronously. `preparation_ready_at` marks the first pass through input,
checkpoint and command-environment preparation and command resolution.

The entries in `phases` partition elapsed worker time up to `observed_at`.
The recorder accumulates repeated visits to the same phase without double
counting nested work. A phase that was never entered is omitted. Durations use
the worker's monotonic clock within a process. The coordinator-to-custodian
handoff uses a same-host wall-clock difference when crossing processes.

| Phase | Work included |
| --- | --- |
| `assignment_dispatch`, `worker_binding` | Check-in reply processing, local admission and durable attempt dispatch, then binding and joining any still-owned checkpoint preparation. |
| `custodian_setup`, `custodian_initialization` | Preparing detached launch custody and reaching the helper's entry, then initializing the detached worker and its local storage, isolation and control. |
| `input_staging`, `post_staging_checks` | Input preparation and consumption, then linkage, private work setup, and capacity checks before checkpoint preparation. `staging_timings` gives finer staging detail. |
| `checkpoint_preparation`, `command_environment` | Checkpoint path creation or restore, then command-environment acquisition, environment binding and command resolution. |
| `command_setup`, `sandbox_setup`, `supervisor_setup` | Local command and resource setup, kernel sandbox configuration or activation, and gated process creation with local custody records. |
| `launch_authorization`, `local_launch_admission` | Launch checks and queue round trips, and the worker-local launch gate, including coordinator admission for a detached custodian. |
| `custody_reservation`, `custody_started` | Sending and acknowledging native custody records, including local process identity collection for the started report. |
| `sidecar_environment`, `sidecar_startup` | Acquiring helper environments and starting helpers through readiness when the command uses sidecars. |
| `started_rpc`, `started_checks`, `payload_handshake` | Queue Started round trip, local checks around it, and the native gate or exec receipt for the main payload. |
| `preparation_retry` | Time between preparation attempts or draining a native group before a retry. |

`current_phase` identifies an open observation. When it is absent, the
snapshot stopped at a launch boundary or a prelaunch failure. A refusal or
failure before the payload handoff can have timings without a
`command_start_at`. `command_start_boundary` is `exec_confirmed` when the
worker received the gated exec receipt, or `payload_gate_released` on paths
where the worker observes the successful gate write. Neither boundary is an
application readiness or first-instruction measurement. The queue's
`started_at` records authorization and can precede either boundary.

Use `myce cancel JOB_ID --wait --timeout 30s` to bound the cancellation call
and release wait. A deadline after acknowledgement returns the last
acknowledgement and exits 75 if release still needs confirmation. A deadline
before acknowledgement leaves the outcome uncertain. Retry with the same job
ID or project and key.

### Attempt publication timings

`myce status JOB_ID --history --json` reports `publication_timings` under
the exact `attempt_history` entry. The terminal job also shows its final
attempt's record. The queue measures `verification_seconds` for manifest
verification and `completion_queue_seconds` from handler entry through queue
processing before the commit. The latter includes verification and queue
waits. `release_to_accept_seconds` uses the queue's exact release and result
timestamps. An absent field means that boundary was not observed.

The optional `worker` section measures result-ready through the release-only
acknowledgement, output manifest construction and hashing, and upload creation
and PATCH requests. Request counts and summed service times include retries
and can overlap. The upload window runs from first request to last reply.
`published_bytes` counts distinct verified blobs, including deduplication.
The worker section covers the supervisor's current lifetime. Restarting a
worker can leave earlier phases unobserved. Older workers can provide
queue-only timings. The queue's `file_upload` log includes body read, file
write, streaming hash, fsync, input claim and observed mutex intervals for
upload requests. `data_fsync_ms` breaks out the upload data barrier from
`fsync_ms`. `upload_verify_ms` measures a fallback read of the upload and
`publication_commit_ms` covers custody and publication bookkeeping, including
any queue lock waits during that phase.

### Maintenance holds

When every eligible host for a queued job is in maintenance, `myce status JOB_ID`
and full fleet status report a `host_maintenance` job hold. Its reason names
the affected hosts and their operations. The hold's `owner` and `next` identify
who can restore admission. The fleet overview also lists each host's maintenance
hold. An eligible route with missing or stale observations outside maintenance
keeps its own diagnosis.

Operators can record context with `myce host drain HOST --reason TEXT
--expected-return RFC3339`. Both flags are optional and also work with
`myce host disable`. The host policy exposes them as `reason` and
`expected_return`. Job and fleet holds show an expected return only for hosts
with a recorded time. Admission resumes through the operation's normal resume
action, even if the expected return time has already passed.

The inventory below comes from the Go types. Behavior, freshness, redaction,
units, compatibility and interpretation live in the [status behavior reference](reference/status-behavior.md).
Declared fields are not a promise that every view exposes their values.

Worker and compact hypha status expose `memory_containment` when reported:
`state: unavailable` with `reason` and `next` means the installed worker unit is
outside its configured slice. Batch and model assignments continue; only the
joint slice ceiling is skipped. Migrate the worker unit into its configured
slice to enable it (see [#1771](https://github.com/1kuna/mycelium/issues/1771)).
`state: available` means the joint slice controls were confirmed. An omitted
field is unknown. Per-attempt cgroup memory/swap limits and group OOM handling,
RSS guards where applicable, and tier OOM scores retain their existing behavior.
`myce status` includes the condition and next step in text as well as JSON.

For a pooled-memory assignment, job GET includes `memory.ram` and, on a
discrete accelerator, `memory.card`. Each pool shows the typical estimate,
admission charge, evidence level and run count, held memory and measured peak.
On XPU, card evidence `size_band` means the job's decoded duration falls
within comparable clean measurements.
`memory.limit_gb` appears when the submitter set an explicit RAM limit.
The queue can use the typical estimate when the cautious charge exceeds the
target pool's allowance. A temporary shortage uses `memory_wait` with the RAM
or card comparison. An occupied card reports queue-owned `capacity` and names
the holder when known. If every compatible pool is too small even when empty,
`memory_capacity` identifies the limiting estimates and allowances.

Attempt history distinguishes OOM/resource-limit stops from entry coverage
findings. `myce status JOB_ID` prints each stopped attempt's retained memory
bound in GiB, available cgroup/non-reclaimable peaks and kernel event counts.
Attempt-owned limits never mark an unproven machine type incompatible; auto
memory recovery and authored ceilings keep their existing policies.
`myce logs JOB_ID` labels every chunk by attempt, including the appended
`[attempt 0: environment failure]` block. Sequence cursors are unchanged.

## Memory v2 shadow

Status shows the worker's observed footprint, holds, signed slack (S), and
reach beside current admission capacity. JSON includes the individual attempt
readings. Missing attempt counters or physical free memory leave S and reach
unavailable. The current assignment's typical RAM estimate supplies E for
this stage, and cards with no shadow learning history use their whole capacity.

`myce efficiency` shows the same reports and the queue's recent shadow R4
forecasts against retained `memory_pressure` outcomes. Shadow counts and
GiB-hours cover observed samples from the current queue process. The weekly
window is bucketed by UTC hour. A forecast counts once until the selected
attempt leaves the observed pool. The queue integrates estimated raises and
owners' holds over observed time.

## Generated field inventory

<!-- BEGIN GENERATED STATUS FIELDS -->

Generated from `CompactStatus`, `Snapshot` and `StatusJobsPage`, including
all reachable objects. Run `go generate ./internal/statusdoc` after changing
the types. Quick checks reject drift. `?` marks `omitempty`/`omitzero`, not
guaranteed presence: status projection can redact or omit values. Arrays and
maps link to their element types; timestamps are JSON time strings.

| Go object | Declared JSON fields and value types |
| --- | --- |
| <a id="fields-acceleratorprofile"></a>**AcceleratorProfile** | `type`: string; `name?`: string; `memory_bytes?`: number |
| <a id="fields-admissiondecision"></a>**AdmissionDecision** | `at`: timestamp; `checkin_machine`: string; `checkin_session`: string; `device?`: string; `outcome`: string; `stage`: string; `predicate`: string; `hold?`: [Hold](#fields-hold); `opportunity_seconds`: number |
| <a id="fields-admissiondetail"></a>**AdmissionDetail** | `subcheck`: string; `unit`: string; `required`: number; `available?`: number; `unavailable?`: boolean; `keep_gb?`: number; `held_gb?`: number; `startup_gb?`: number; `envelope_source?`: string; `envelope_host_gb`: number; `envelope_device_gb`: number; `contributors?`: array of [HostGrowthContributor](#fields-hostgrowthcontributor); `memory_room?`: [MemoryRoomDecision](#fields-memoryroomdecision) |
| <a id="fields-admissionwaitevidence"></a>**AdmissionWaitEvidence** | `first?`: [AdmissionWaitSnapshot](#fields-admissionwaitsnapshot); `latest?`: [AdmissionWaitSnapshot](#fields-admissionwaitsnapshot); `transitions?`: array of [AdmissionWaitSnapshot](#fields-admissionwaitsnapshot); `dropped?`: number; `history_budget_bytes`: number; `history_bytes`: number; `capture_gaps?`: number; `deferred_at?`: string; `budget_unknown?`: boolean |
| <a id="fields-admissionwaitholder"></a>**AdmissionWaitHolder** | `key`: string; `kind`: string; `job?`: string; `machine`: string; `session`: string; `attempt?`: number; `engine_attempt?`: string; `devices?`: array of string; `tier?`: [PriorityMatch](#fields-prioritymatch); `cores`: number; `declared_host_gb?`: number; `admission_host_gb?`: number; `charged_host_gb?`: number; `remaining_growth_gb?`: number; `forecast_host_gb?`: number; `sample_age_seconds?`: number; `sample_fresh`: boolean; `enforced_host_gb?`: number; `enforcement_basis`: string; `preemptible`: boolean; `preemption_reason`: string |
| <a id="fields-admissionwaitplan"></a>**AdmissionWaitPlan** | `at`: timestamp; `preemption`: [WaitPreemption](#fields-waitpreemption); `concurrency?`: number; `required_cores`: number; `required_host_gb`: number; `required_device_gb`: number |
| <a id="fields-admissionwaitsnapshot"></a>**AdmissionWaitSnapshot** | `at`: timestamp; `attempt`: number; `next_piece?`: number; `predicate`: string; `targets?`: array of [AdmissionWaitTarget](#fields-admissionwaittarget); `closed_at?`: string; `closed_reason?`: string |
| <a id="fields-admissionwaittarget"></a>**AdmissionWaitTarget** | `admission_decision?`: [AdmissionDecision](#fields-admissiondecision); `host`: string; `machine`: string; `session`: string; `device?`: string; `predicates?`: array of string; `device_fault?`: string; `resolution`: string; `concurrency?`: number; `required_cores`: number; `available_cores`: number; `required_host_gb`: number; `available_host_gb`: number; `host_memory_known`: boolean; `host_memory_ledger?`: [HostMemoryLedger](#fields-hostmemoryledger); `host_debt_gb`: number; `required_device_gb`: number; `available_device_gb`: number; `device_memory_known`: boolean; `preemption`: [WaitPreemption](#fields-waitpreemption); `release_commitment?`: boolean; `last_planner_decision?`: [AdmissionWaitPlan](#fields-admissionwaitplan); `holders?`: array of [AdmissionWaitHolder](#fields-admissionwaitholder) |
| <a id="fields-advanced"></a>**Advanced** | `warm_pieces?`: string; `resident?`: [retiredField](#fields-retiredfield); `checkpoint?`: string; `worker_data?`: string; `allow_cpu_fallback?`: boolean; `exclusive_device?`: boolean; `resumable?`: boolean; `resume_compatibility?`: [ResumeCompatibility](#fields-resumecompatibility); `preemptible?`: boolean; `isolation?`: string; `reason?`: string; `wall_time?`: string; `give_up_at?`: string; `expected_minutes?`: number; `start_by?`: string; `start_window?`: string; `disk?`: [DiskNeeds](#fields-diskneeds); `size_hint?`: [SizeHint](#fields-sizehint); `memory_limit_gb?`: number; `allowed_machines?`: array of string; `excluded_machines?`: array of string; `requirement_reasons?`: map of string; `portable?`: boolean; `require_target?`: boolean; `output_delivery?`: string; `quality_tolerance?`: map of number; `logs?`: string; `keep_on_failure?`: array of [KeepFile](#fields-keepfile); `diagnostics?`: [DiagnosticSpec](#fields-diagnosticspec) |
| <a id="fields-alertrecovery"></a>**AlertRecovery** | `episode`: string; `action`: string; `state`: string; `cause`: string; `summary`: string; `next_step?`: string; `machine`: string; `session?`: string; `job_id`: string; `attempt`: number; `admission?`: [AdmissionDetail](#fields-admissiondetail); `at`: timestamp |
| <a id="fields-applicationprogress"></a>**ApplicationProgress** | `cadence_seconds?`: number; `completed`: number; `total`: number; `unit`: string; `attempt`: number; `machine`: string; `session`: string; `received_at`: timestamp; `updated_at?`: timestamp; `age_seconds`: number; `stale`: boolean; `stage_index?`: number; `stage_count?`: number; `total_work_known`: boolean |
| <a id="fields-attemptcputime"></a>**AttemptCPUTime** | `pressure?`: [IOPressure](#fields-iopressure); `usage_seconds?`: number; `user_seconds?`: number; `system_seconds?`: number; `periods?`: number; `throttled_periods?`: number; `throttled_seconds?`: number; `source?`: string; `unavailable?`: string; `lower_bound?`: boolean |
| <a id="fields-attemptdiagnostics"></a>**AttemptDiagnostics** | `files`: map of [FileRef](#fields-fileref); `summary?`: [FailureSummary](#fields-failuresummary) |
| <a id="fields-attemptfilescollection"></a>**AttemptFilesCollection** | `retained_bytes`: number; `reclaimed_bytes`: number; `retained_bytes_by_reason`: map of number |
| <a id="fields-attempthostmemorycharge"></a>**AttemptHostMemoryCharge** | `progress?`: number; `id`: string; `attempt`: number; `session`: string; `gib`: number; `observed_gb?`: number; `sample_id?`: number |
| <a id="fields-attemptio"></a>**AttemptIO** | `read_bytes?`: number; `write_bytes?`: number; `read_ops?`: number; `write_ops?`: number; `pressure?`: [IOPressure](#fields-iopressure); `unavailable?`: string |
| <a id="fields-attemptliveness"></a>**AttemptLiveness** | `cpu`: [LivenessSignal](#fields-livenesssignal); `work_dir`: [LivenessSignal](#fields-livenesssignal); `attempt`: number; `machine`: string; `session`: string; `received_at`: timestamp; `state`: string; `idle_seconds?`: number; `reason?`: string |
| <a id="fields-attemptmemoryfacts"></a>**AttemptMemoryFacts** | `attempt`: number; `machine`: string; `session`: string; `memory_peak?`: [AttemptMemoryPeak](#fields-attemptmemorypeak); `memory_envelope?`: [JobMemoryEnvelope](#fields-jobmemoryenvelope); `learning_reason?`: string |
| <a id="fields-attemptmemorypeak"></a>**AttemptMemoryPeak** | `pool_peaks?`: [memoryRunPeaks](#fields-memoryrunpeaks); `device_growth_gb?`: number; `host_sampling?`: [HostMemorySampling](#fields-hostmemorysampling); `device_sampling?`: [DeviceMemorySampling](#fields-devicememorysampling); `growth_gb?`: number; `combined_host_gb?`: number; `gpu_host?`: [GPUHostMemory](#fields-gpuhostmemory); `nonreclaimable_gb?`: number; `host_gb?`: number; `cgroup_gb?`: number; `device_gb?`: number; `device_unavailable?`: string; `device_source?`: string; `swap_gb?`: number; `source?`: string; `events?`: [MemoryEventCounts](#fields-memoryeventcounts); `constrained?`: boolean; `pressure_unavailable?`: boolean; `pressure?`: [MemoryPressureEvidence](#fields-memorypressureevidence) |
| <a id="fields-attemptoutcome"></a>**AttemptOutcome** | `worker_data_cleanup?`: [WorkerDataCleanup](#fields-workerdatacleanup); `fault_evidence?`: array of [FaultEvidence](#fields-faultevidence); `signal_evidence?`: [SignalEvidence](#fields-signalevidence); `device_fault?`: [DeviceFaultLoss](#fields-devicefaultloss); `execution_evidence?`: [ExecutionEvidence](#fields-executionevidence); `stop_observation?`: [AttemptStopObservation](#fields-attemptstopobservation); `staging_timings?`: [StagingTimings](#fields-stagingtimings); `launch_timings?`: [LaunchTimings](#fields-launchtimings); `publication_timings?`: [PublicationTimings](#fields-publicationtimings); `placement?`: [PlacementDecision](#fields-placementdecision); `diagnostics?`: [AttemptDiagnostics](#fields-attemptdiagnostics); `attempt`: number; `machine`: string; `session`: string; `ended_at`: timestamp; `state`: string; `reason?`: string; `execution_cause?`: [RuntimeCause](#fields-runtimecause); `cleanup_cause?`: [RuntimeCause](#fields-runtimecause); `forced?`: boolean; `interrupted?`: boolean; `admission_requeue?`: boolean; `out_of_memory?`: boolean; `polite_stop?`: boolean; `operator_stop?`: boolean; `priority_stop?`: [PriorityPreemption](#fields-prioritypreemption) |
| <a id="fields-attemptpeaksummary"></a>**AttemptPeakSummary** | `attempts`: number; `median_gb`: number; `max_gb`: number |
| <a id="fields-attemptresourcefacts"></a>**AttemptResourceFacts** | `attempt`: number; `machine`: string; `session`: string; `resources`: [AttemptResources](#fields-attemptresources) |
| <a id="fields-attemptresources"></a>**AttemptResources** | `cpu`: [AttemptCPUTime](#fields-attemptcputime); `io`: [AttemptIO](#fields-attemptio); `sampled_at`: timestamp; `age_seconds`: number; `elapsed_seconds`: number; `received_at?`: timestamp |
| <a id="fields-attemptstopobservation"></a>**AttemptStopObservation** | `io_pressure`: [IOPressure](#fields-iopressure); `at`: timestamp; `host_sample_age_seconds`: number; `headroom_gb`: number; `physical_used_gb`: number; `memory_pressure`: number; `pressure_unit`: string; `pressure_age_seconds`: number; `swap_growth_bytes_per_second`: number |
| <a id="fields-attemptstoragefacts"></a>**AttemptStorageFacts** | `execution`: [Execution](#fields-execution); `machine`: string; `session`: string; `peak?`: [AttemptStoragePeak](#fields-attemptstoragepeak); `envelope?`: [JobStorageEnvelope](#fields-jobstorageenvelope); `learning_reason`: string; `output_learned?`: boolean |
| <a id="fields-attemptstoragepeak"></a>**AttemptStoragePeak** | `scratch_bytes`: number; `spool_bytes`: number; `checkpoint_bytes`: number; `metadata_bytes`: number; `managed_bytes`: number; `files`: number; `staged_input_files`: number; `staged_input_bytes`: number; `allocation_unit`: number; `samples`: number; `complete`: boolean; `constrained?`: boolean; `source`: string |
| <a id="fields-availability"></a>**Availability** | `age_seconds?`: number; `probe?`: [ProbeObservation](#fields-probeobservation); `mode`: string; `available`: boolean; `reason?`: string; `checked_at`: timestamp; `signals`: [OwnerSignals](#fields-ownersignals) |
| <a id="fields-batchwork"></a>**BatchWork** | `parent`: string; `pieces`: array of [FanOutPiece](#fields-fanoutpiece); `concurrency`: number; `share_xpu?`: boolean |
| <a id="fields-cachecollection"></a>**CacheCollection** | `bytes`: number; `limit`: number; `reclaimed`: number; `busy?`: boolean; `protected?`: boolean; `pending?`: boolean; `error?`: string |
| <a id="fields-cancellation"></a>**Cancellation** | `job_id`: string; `project`: string; `submission_key`: string; `state`: string; `will_not_run`: boolean; `release`: string; `acknowledged_at`: string |
| <a id="fields-cancellationmember"></a>**CancellationMember** | `execution_evidence?`: [ExecutionEvidence](#fields-executionevidence); `job_id`: string; `state`: string; `will_not_run`: boolean; `release`: string |
| <a id="fields-cancellationpiece"></a>**CancellationPiece** | `key`: string; `materialized`: boolean; `accepted`: boolean; `wave_id?`: string; `will_not_run`: boolean; `release`: string; `dependents`: array of string |
| <a id="fields-cancellationscope"></a>**CancellationScope** | `pieces`: array of [CancellationPiece](#fields-cancellationpiece); `members`: array of [CancellationMember](#fields-cancellationmember) |
| <a id="fields-capabilityprofile"></a>**CapabilityProfile** | `benchmark?`: [retiredField](#fields-retiredfield); `availability?`: [Availability](#fields-availability); `version`: number; `onboarding`: string; `recorded_at`: timestamp; `os`: string; `arch`: string; `logical_cpus`: number; `memory_layout`: string; `system_memory_bytes?`: number; `accelerators`: array of [AcceleratorProfile](#fields-acceleratorprofile); `disk_total_bytes?`: number; `disk_free_bytes?`: number; `unavailable?`: array of string |
| <a id="fields-cardusage"></a>**CardUsage** | `machine`: string; `device`: string; `name`: string; `gpu_hours`: number; `busy_percent`: number; `idle_hours`: number; `pinned_hours`: number |
| <a id="fields-cardutilization"></a>**CardUtilization** | `host`: string; `machine`: string; `device`: string; `platform`: string; `current_state`: string; `state_hours`: map of number; `idle_while_waiting_hours_by_reason?`: map of number; `busy_while_waiting_hours_by_reason?`: map of number |
| <a id="fields-checkpointexpirystatus"></a>**CheckpointExpiryStatus** | `waiting`: number; `undated`: number |
| <a id="fields-checkpointfloormissing"></a>**CheckpointFloorMissing** | `required`: number; `observed`: number |
| <a id="fields-checkpointinputexpiry"></a>**CheckpointInputExpiry** | `field`: string; `deadline?`: timestamp; `expired_at?`: timestamp |
| <a id="fields-checkpointmirror"></a>**CheckpointMirror** | `checkpoint`: [FileCheckpoint](#fields-filecheckpoint); `delivery`: [OutputDelivery](#fields-outputdelivery); `prefix`: string |
| <a id="fields-checkpointorigin"></a>**CheckpointOrigin** | `target`: [ExecutionTarget](#fields-executiontarget); `generation`: number; `attempt`: number |
| <a id="fields-checkpointstorageevidence"></a>**CheckpointStorageEvidence** | `at`: timestamp; `reason`: string; `operation_age_seconds?`: number; `operation?`: string; `errno?`: number |
| <a id="fields-commanddiscovery"></a>**CommandDiscovery** | `targets`: array of [ExecutionTarget](#fields-executiontarget) |
| <a id="fields-compacthypha"></a>**CompactHypha** | `memory_shadow?`: [MemoryShadowReport](#fields-memoryshadowreport); `model_config_hash?`: string; `memory_containment?`: [MemoryContainment](#fields-memorycontainment); `fleet_proof_issue?`: string; `id`: string; `gpu`: string; `free_memory_gb`: number; `availability`: string; `host_ram_for_admission_gb?`: number; `free_cpu_cores?`: number; `cpu_cores?`: number; `running?`: number; `release?`: string; `preparing?`: array of string; `copying?`: array of string; `result_publications?`: array of [ResultPublication](#fields-resultpublication) |
| <a id="fields-compactjob"></a>**CompactJob** | `id`: string; `name`: string; `project`: string; `submission_key?`: string; `class?`: string; `state`: string; `attempt?`: number; `machine?`: string; `since?`: string; `reason?`: string; `next?`: string; `delivery?`: string; `command_started`: boolean; `finding?`: [JobFinding](#fields-jobfinding) |
| <a id="fields-compactpaused"></a>**CompactPaused** | `project`: string; `reason`: string; `next?`: string; `count`: number; `ids`: array of string |
| <a id="fields-compactstatus"></a>**CompactStatus** | `now`: timestamp; `queue_release`: [RunningRelease](#fields-runningrelease); `restart_grace?`: [RestartGraceStatus](#fields-restartgracestatus); `retained_jobs`: number; `counts`: map of number; `alert_groups?`: array of [StatusAlertGroup](#fields-statusalertgroup); `production_leases?`: array of [StatusLease](#fields-statuslease); `not_quiescent?`: array of [StatusQuiescence](#fields-statusquiescence); `health_alert_delivery?`: [HealthAlertDelivery](#fields-healthalertdelivery); `fleet_upgrade?`: array of string; `host_states?`: map of string; `holds?`: array of [Hold](#fields-hold); `warning?`: string; `hyphas`: array of [CompactHypha](#fields-compacthypha); `models?`: array of string; `jobs?`: array of [CompactJob](#fields-compactjob); `paused?`: array of [CompactPaused](#fields-compactpaused); `detail`: string; `freshness?`: [SnapshotFreshness](#fields-snapshotfreshness); `queue_capabilities?`: array of string |
| <a id="fields-completionstate"></a>**CompletionState** | `collected_at?`: string; `superseded_by?`: string; `delivery_id?`: string; `delivered_at?`: string |
| <a id="fields-componentexclusion"></a>**ComponentExclusion** | `selector`: string; `reason`: string; `set_by`: string; `updated_at`: timestamp; `revision`: number; `active`: boolean; `host?`: string; `kind?`: string; `platform?`: string; `devices?`: array of [ExclusionDevice](#fields-exclusiondevice) |
| <a id="fields-controlstoragestatus"></a>**ControlStorageStatus** | `database_bytes`: number; `wal_bytes`: number; `shared_bytes`: number; `allocated_bytes`: number |
| <a id="fields-copy"></a>**Copy** | `memory?`: [MemoryPools](#fields-memorypools); `liveness?`: [AttemptLiveness](#fields-attemptliveness); `started_at?`: string; `stopped_at?`: string; `released_at?`: string; `process_id?`: string; `allocated_devices?`: array of string; `priority_stop?`: [PriorityPreemption](#fields-prioritypreemption); `stop_cause?`: string; `extension?`: [ExtensionSelection](#fields-extensionselection); `loss_charged?`: boolean; `recoverable`: boolean; `machine`: string; `session`: string; `attempt`: number; `progress`: number; `release_confirmed?`: boolean; `release_reason?`: string |
| <a id="fields-copyreport"></a>**CopyReport** | `attempt?`: string; `bytes_per_second?`: number; `model`: string; `state`: string; `bytes`: number; `total`: number; `error?`: string |
| <a id="fields-coveragefinding"></a>**CoverageFinding** | `target`: [ExecutionTarget](#fields-executiontarget); `job`: string; `attempt`: number; `at`: timestamp; `action`: string |
| <a id="fields-dailyusage"></a>**DailyUsage** | `date`: string; `project?`: string; `machine?`: string; `gpu_hours`: number; `busy_percent?`: number |
| <a id="fields-deadlineguidance"></a>**DeadlineGuidance** | `code`: string; `deadline_policy`: [PriorityMatch](#fields-prioritymatch); `precedence_change`: string; `blockers?`: array of string; `details_deferred?`: boolean; `eta`: string; `example`: string; `next_action`: string; `message`: string |
| <a id="fields-devicefaultloss"></a>**DeviceFaultLoss** | `host`: string; `platform`: string; `devices`: array of string; `at`: timestamp |
| <a id="fields-devicefaultstartwait"></a>**DeviceFaultStartWait** | `since`: timestamp; `remaining`: number |
| <a id="fields-devicehealthfailure"></a>**DeviceHealthFailure** | `code`: string; `component?`: string; `exit_status?`: number; `observed_at?`: timestamp |
| <a id="fields-devicemaintenance"></a>**DeviceMaintenance** | `host`: string; `platform`: string; `identity`: string; `disabled`: boolean; `reason?`: string; `updated_at`: timestamp; `revision`: number |
| <a id="fields-devicememorysampling"></a>**DeviceMemorySampling** | `samples`: number; `complete`: boolean; `min_gb`: number; `max_gb`: number; `last_gb`: number; `max_step_gb`: number; `last_step_gb`: number; `first_at_seconds`: number; `peak_at_seconds`: number; `elapsed_seconds`: number; `max_interval_seconds`: number |
| <a id="fields-devicepoolstop"></a>**DevicePoolStop** | `observed_at`: timestamp; `budget_gb`: number; `used_gb`: number; `free_gb`: number; `memory_measured`: boolean; `overage_gb`: number; `trigger`: string |
| <a id="fields-devicerecovery"></a>**DeviceRecovery** | `observed_at?`: timestamp; `fault_event`: string; `healthy_since`: timestamp; `cleared_at`: timestamp; `observation_seconds`: number; `probe`: string |
| <a id="fields-devicetelemetry"></a>**DeviceTelemetry** | `at`: timestamp; `age_seconds`: number; `free_gb?`: number; `active_percent?`: number; `source`: string |
| <a id="fields-diagnosticreport"></a>**DiagnosticReport** | `path`: string; `format`: string |
| <a id="fields-diagnosticspec"></a>**DiagnosticSpec** | `reports?`: array of [DiagnosticReport](#fields-diagnosticreport) |
| <a id="fields-diskbudgetvolume"></a>**DiskBudgetVolume** | `id`: string; `at`: timestamp; `roots`: array of string; `free_bytes`: number; `floor_bytes`: number; `reserved_bytes`: number; `owned_bytes`: number; `reclaimed_bytes`: number; `unexplained_growth_bytes`: number; `exhaustion_seconds?`: number; `declining_seconds?`: number; `decline_cause?`: string; `pressure?`: boolean; `warning?`: string; `control_growth_bytes`: number; `foreign_bytes?`: number; `foreign_peak_bytes?`: number; `foreign_peak_growth_bytes?`: number; `foreign_headroom_bytes?`: number |
| <a id="fields-diskneeds"></a>**DiskNeeds** | `input_bytes?`: number; `output_bytes?`: number; `checkpoint_bytes?`: number; `scratch_bytes?`: number |
| <a id="fields-diskvolume"></a>**DiskVolume** | `id?`: string; `roots`: array of string; `total_bytes`: number; `free_bytes`: number; `change_bytes_per_hour?`: number; `sampled_at`: timestamp; `sample_interval_seconds?`: number; `age_seconds?`: number; `retained?`: boolean; `probe_age_seconds?`: number; `error?`: string |
| <a id="fields-efficiencyallocation"></a>**EfficiencyAllocation** | `host`: string; `work`: string; `basis`: string; `reserved_covered_memory_gb_seconds`: number; `reserved_device_gb_seconds`: number; `measured_device_gb_seconds`: number; `device_covered_seconds`: number; `reserved_memory_gb_seconds`: number; `measured_memory_gb_seconds`: number; `memory_covered_seconds`: number; `reserved_cpu_seconds`: number; `measured_cpu_seconds`: number; `cpu_observed_elapsed_seconds`: number; `cpu_observed_attempts`: number; `cpu_lower_bound`: boolean; `read_bytes`: number; `write_bytes`: number; `read_ops`: number; `write_ops`: number; `io_observed_attempts`: number; `io_lower_bound`: boolean; `seconds`: number |
| <a id="fields-efficiencyhost"></a>**EfficiencyHost** | `identity`: string; `host`: string; `since`: timestamp; `at`: timestamp; `reserved_covered_memory_gb_seconds`: number; `reserved_device_gb_seconds`: number; `measured_device_gb_seconds`: number; `device_covered_seconds`: number; `reserved_memory_gb_seconds`: number; `measured_memory_gb_seconds`: number; `memory_covered_seconds`: number; `reserved_cpu_seconds`: number; `measured_cpu_seconds`: number; `cpu_observed_elapsed_seconds`: number; `cpu_observed_attempts`: number; `cpu_lower_bound`: boolean; `read_bytes`: number; `write_bytes`: number; `read_ops`: number; `write_ops`: number; `io_observed_attempts`: number; `io_lower_bound`: boolean; `seconds`: number; `current`: [efficiencyRate](#fields-efficiencyrate); `total_memory_gb`: number; `physical_used_memory_gb`: number; `io_status`: string; `io_pressure`: [IOPressure](#fields-iopressure); `measured_memory_unknown?`: string; `attempt_peaks?`: [AttemptPeakSummary](#fields-attemptpeaksummary) |
| <a id="fields-efficiencyreport"></a>**EfficiencyReport** | `memory_shadow?`: array of [memoryShadowHost](#fields-memoryshadowhost); `memory_shadow_week?`: [MemoryShadowWeek](#fields-memoryshadowweek); `at`: timestamp; `hosts`: array of [EfficiencyHost](#fields-efficiencyhost); `programs`: array of [EfficiencyAllocation](#fields-efficiencyallocation); `jobs`: array of [EfficiencyWait](#fields-efficiencywait); `projects`: array of [EfficiencyWait](#fields-efficiencywait); `stranded`: array of [StrandedDemand](#fields-strandeddemand); `restrictions`: array of [ProjectRestrictions](#fields-projectrestrictions); `predictions`: array of [PredictionError](#fields-predictionerror); `wasted_attempts`: array of [WastedAttempt](#fields-wastedattempt); `wasted_windows`: array of [WastedWindow](#fields-wastedwindow); `utilization?`: [UtilizationReport](#fields-utilizationreport); `signals`: array of [EfficiencySignal](#fields-efficiencysignal); `cpu_status`: string; `resource_attempts_dropped`: number; `coverage`: string |
| <a id="fields-efficiencysignal"></a>**EfficiencySignal** | `observed`: boolean; `subject`: string; `metric`: string; `value`: number; `baseline_samples`: number; `upper_bound`: number; `active`: boolean |
| <a id="fields-efficiencywait"></a>**EfficiencyWait** | `active`: boolean; `current_blocker?`: string; `job_id?`: string; `project`: string; `seconds`: map of number |
| <a id="fields-enrollmentproof"></a>**EnrollmentProof** | `version`: string; `start_gate`: string; `attempt_protocol`: string; `runner_smoke`: boolean |
| <a id="fields-entrycoverage"></a>**EntryCoverage** | `key`: string; `recipe`: string; `lock_targets?`: array of [ExecutionTarget](#fields-executiontarget); `findings?`: array of [CoverageFinding](#fields-coveragefinding) |
| <a id="fields-environment"></a>**Environment** | `backend?`: string; `name?`: string; `lock_file?`: string; `python?`: string; `platforms?`: array of string; `files?`: map of base64 string |
| <a id="fields-environmentlock"></a>**EnvironmentLock** | `backend?`: string; `sha256`: map of string |
| <a id="fields-environmentrecipe"></a>**EnvironmentRecipe** | `files_ref?`: [FileRef](#fields-fileref); `launch?`: [HostLaunch](#fields-hostlaunch); `preparation?`: [ExtensionSelection](#fields-extensionselection); `assets?`: map of [FileRef](#fields-fileref); `input_digest?`: string; `manifest?`: string; `environment?`: string; `backend?`: string; `target?`: [ExecutionTarget](#fields-executiontarget); `python?`: string; `lock_file?`: string; `binary?`: string; `binary_relative?`: string; `build?`: array of string; `files?`: map of base64 string; `key`: string; `platform`: string; `node_layer_targets?`: array of [ExecutionTarget](#fields-executiontarget) |
| <a id="fields-environmentreport"></a>**EnvironmentReport** | `target?`: [ExecutionTarget](#fields-executiontarget); `fingerprint?`: string; `log_reference?`: string; `failure_kind?`: string; `retry_at?`: string; `failures?`: number; `path_diagnostic?`: [PathDiagnostic](#fields-pathdiagnostic); `kind?`: string; `attempt?`: string; `key`: string; `state`: string; `error?`: string; `log?`: string; `build_seconds?`: number; `bytes?`: number |
| <a id="fields-evidenceretention"></a>**EvidenceRetention** | `at`: timestamp; `capacity`: [RetentionCapacity](#fields-retentioncapacity); `retained_bytes`: number; `budget_bytes`: number; `eligible_bytes`: number; `protected_bytes`: number; `reclaimed_bytes`: number; `drifted_rows`: number; `over_budget`: boolean; `unavailable`: boolean |
| <a id="fields-exclusiondevice"></a>**ExclusionDevice** | `platform`: string; `identity`: string |
| <a id="fields-execution"></a>**Execution** | `progress?`: number; `id`: string; `attempt`: number |
| <a id="fields-executionevidence"></a>**ExecutionEvidence** | `attempt`: number; `main`: string; `sidecars`: string; `environment_preparation`: string |
| <a id="fields-executionmember"></a>**ExecutionMember** | `command`: array of string; `environment?`: string; `workdir?`: string; `env?`: map of string; `memory_gb?`: number |
| <a id="fields-executionspec"></a>**ExecutionSpec** | `main`: [ExecutionMember](#fields-executionmember); `sidecars?`: array of [NativeSidecar](#fields-nativesidecar) |
| <a id="fields-executiontarget"></a>**ExecutionTarget** | `os`: string; `arch`: string; `platform`: string |
| <a id="fields-extension"></a>**Extension** | `resident?`: [retiredField](#fields-retiredfield); `requirement_reasons?`: map of string; `isolation?`: string; `reason?`: string; `sandbox_default?`: boolean; `outputs?`: array of string; `assets?`: map of [FileRef](#fields-fileref); `default_class?`: string; `variants?`: array of [ExtensionVariant](#fields-extensionvariant); `name`: string; `version`: string; `purpose`: string; `platforms`: array of string; `kinds?`: array of string; `program_types?`: array of string; `memory_gb`: number; `host_memory_gb?`: number; `resumable?`: boolean; `setup?`: array of string; `run`: array of string; `check?`: array of string; `baselines?`: map of [ExtensionBaseline](#fields-extensionbaseline); `platform_baselines?`: map of map of [ExtensionBaseline](#fields-extensionbaseline); `notes?`: string; `files?`: map of base64 string; `files_ref?`: [FileRef](#fields-fileref); `digest`: string; `production_parity_required?`: boolean |
| <a id="fields-extensionbaseline"></a>**ExtensionBaseline** | `input`: string; `output`: string; `measure`: string; `absolute?`: number; `relative?`: number; `native`: string; `variation`: string |
| <a id="fields-extensioncheckrequest"></a>**ExtensionCheckRequest** | `reserve?`: [ReservationOptions](#fields-reservationoptions); `work`: string; `directory`: string; `timeout?`: string; `automatic?`: [ExtensionReport](#fields-extensionreport) |
| <a id="fields-extensionreport"></a>**ExtensionReport** | `device?`: string; `host?`: string; `environment_identity?`: string; `check_digest?`: string; `check_id?`: string; `check_state?`: string; `target?`: [ExecutionTarget](#fields-executiontarget); `name`: string; `version`: string; `digest`: string; `platform`: string; `work`: string; `fingerprint`: string; `setup`: boolean; `qualified`: boolean; `reason?`: string |
| <a id="fields-extensionselection"></a>**ExtensionSelection** | `name`: string; `version`: string; `digest`: string |
| <a id="fields-extensionvariant"></a>**ExtensionVariant** | `quality?`: map of map of [QualityMeasurement](#fields-qualitymeasurement); `references?`: map of [WorkReference](#fields-workreference); `target`: [ExecutionTarget](#fields-executiontarget); `environments?`: map of [EnvironmentRecipe](#fields-environmentrecipe); `needs`: [ResourceNeeds](#fields-resourceneeds); `execution`: [ExecutionSpec](#fields-executionspec); `setup?`: array of string; `check?`: array of string; `baselines?`: map of [ExtensionBaseline](#fields-extensionbaseline) |
| <a id="fields-externalmemoryhistory"></a>**ExternalMemoryHistory** | `quiescent_v1?`: [ExternalMemoryHistory](#fields-externalmemoryhistory); `24h`: [ExternalMemoryWindow](#fields-externalmemorywindow); `7d`: [ExternalMemoryWindow](#fields-externalmemorywindow) |
| <a id="fields-externalmemorywindow"></a>**ExternalMemoryWindow** | `windows?`: number; `samples`: number; `buckets`: number; `p50_gb`: number; `p95_gb`: number; `max_gb`: number; `peak_at`: timestamp; `first_bucket_at`: timestamp; `last_bucket_at`: timestamp |
| <a id="fields-failedtest"></a>**FailedTest** | `id`: string; `classname?`: string; `file?`: string |
| <a id="fields-failuresummary"></a>**FailureSummary** | `progress?`: number; `id`: string; `attempt`: number; `machine`: string; `session`: string; `success`: boolean; `error?`: string; `execution_cause?`: [RuntimeCause](#fields-runtimecause); `cleanup_cause?`: [RuntimeCause](#fields-runtimecause); `failed_tests`: array of [FailedTest](#fields-failedtest); `report_errors?`: array of string |
| <a id="fields-fanout"></a>**FanOut** | `pieces`: array of [FanOutPiece](#fields-fanoutpiece); `max_concurrency`: number; `share_xpu?`: boolean |
| <a id="fields-fanoutpiece"></a>**FanOutPiece** | `key`: string; `args?`: array of string; `after?`: [PieceDependency](#fields-piecedependency) |
| <a id="fields-fanoutprogress"></a>**FanOutProgress** | `warm?`: [WarmPieceProgress](#fields-warmpieceprogress); `next`: number; `children?`: array of string; `results?`: map of [PieceResult](#fields-pieceresult) |
| <a id="fields-faultattempt"></a>**FaultAttempt** | `assigned_at?`: string; `progress?`: number; `id`: string; `attempt`: number; `session`: string; `pids`: array of number; `pid_unavailable?`: boolean; `host_gb?`: number; `host_at`: timestamp; `device_gb?`: number; `device_at`: timestamp; `device_source?`: string |
| <a id="fields-faultevidence"></a>**FaultEvidence** | `event`: string; `kind`: string; `device`: string; `kernel_at`: timestamp; `captured_at`: timestamp; `capture_ns`: number; `replaced?`: number; `device_attribution`: string; `attempts`: array of [FaultAttempt](#fields-faultattempt); `host`: map of string; `omitted_attempts?`: number; `unavailable?`: array of string |
| <a id="fields-faultlibrary"></a>**FaultLibrary** | `name`: string; `address`: string; `offset`: string; `device`: string; `inode`: string |
| <a id="fields-filecapacityevidence"></a>**FileCapacityEvidence** | `bound?`: string; `store_limit_bytes?`: number; `stored_bytes?`: number; `observed_at`: timestamp; `job_id?`: string; `attempt`: number; `request_id`: string; `production`: boolean; `identified?`: boolean; `need_bytes`: number; `growth_bytes`: number; `available_bytes`: number; `free_bytes`: number; `output_commitment_bytes`: number; `upload_commitment_bytes?`: number; `store_free_bytes?`: number; `volume_free_bytes?`: number; `disk_free_bytes?`: number; `disk_floor_bytes?`: number; `production_reserve_bytes`: number; `production_committed_bytes`: number; `production_withheld_bytes`: number; `project_withheld_bytes`: number |
| <a id="fields-filecheckpoint"></a>**FileCheckpoint** | `generation`: number; `files`: map of [FileRef](#fields-fileref) |
| <a id="fields-filecollectionstatus"></a>**FileCollectionStatus** | `passes`: number; `yielded_passes`: number; `deferred_passes`: number; `reclaimed_blobs`: number; `reclaimed_bytes`: number; `pending_candidates`: number; `last_reclaim_at?`: string |
| <a id="fields-filereclamation"></a>**FileReclamation** | `pending?`: array of string; `legacy_checkpoints_expired?`: boolean |
| <a id="fields-fileref"></a>**FileRef** | `sha256`: string; `size_bytes`: number |
| <a id="fields-filestorestatus"></a>**FileStoreStatus** | `physical?`: [PhysicalStorageStatus](#fields-physicalstoragestatus); `unidentified_writes_may_use_reserve`: boolean; `unidentified_reserve_admitted_bytes`: number; `production_reserve_bytes`: number; `reserved_output_bytes?`: number; `projects?`: array of [ProjectFileStoreUsage](#fields-projectfilestoreusage); `store`: string; `used_bytes`: number; `free_bytes`: number; `reclaimable_bytes`: number; `pending_collection_bytes`: number; `unreferenced_published_bytes`: number; `open_upload_bytes`: number; `held_published_bytes`: number; `collection?`: [FileCollectionStatus](#fields-filecollectionstatus); `limit_bytes`: number; `disk_free_bytes`: number; `floor_bytes`: number; `error?`: string |
| <a id="fields-finalizationdetail"></a>**FinalizationDetail** | `step`: string; `output?`: string; `destination?`: string; `error`: string; `http_status?`: number; `command_exit_code?`: number |
| <a id="fields-fleethoststatus"></a>**FleetHostStatus** | `authorization_cause?`: string; `outcome?`: string; `deferred_reason?`: string; `storage_warning?`: [StoragePreflightFailure](#fields-storagepreflightfailure); `planned_stops?`: array of [FleetPlannedStop](#fields-fleetplannedstop); `storage_failure?`: [StoragePreflightFailure](#fields-storagepreflightfailure); `stop_requests?`: array of [FleetStopRequest](#fields-fleetstoprequest); `host`: string; `phase`: string; `wait?`: string; `takeover_wait?`: string; `failure?`: string; `failure_code?`: string; `progress_at`: timestamp |
| <a id="fields-fleetplannedstop"></a>**FleetPlannedStop** | `obligation`: [HostObligation](#fields-hostobligation); `stop_after`: timestamp; `reason`: string |
| <a id="fields-fleetruntimeproof"></a>**FleetRuntimeProof** | `adoption?`: [WorkerAdoptionProof](#fields-workeradoptionproof); `installed_slot?`: [ReleaseSlotIdentity](#fields-releaseslotidentity); `slot_mismatch?`: boolean; `slot_error?`: string; `observation_age_seconds`: number; `failure?`: string; `version`: string; `sha256`: string; `session`: string; `pid`: number; `service_healthy`: boolean; `start_gate`: string; `attempt_protocol`: string; `runner_smoke`: boolean; `observed_at`: timestamp |
| <a id="fields-fleetstoprequest"></a>**FleetStopRequest** | `reason?`: string; `obligation`: [HostObligation](#fields-hostobligation); `requested_at`: timestamp; `resumable`: boolean; `progress_before`: number |
| <a id="fields-fleetupgradestatus"></a>**FleetUpgradeStatus** | `host?`: string; `urgency`: string; `operation`: string; `release`: string; `state`: string; `hosts`: array of [FleetHostStatus](#fields-fleethoststatus) |
| <a id="fields-foreignconsumer"></a>**ForeignConsumer** | `pid`: number; `executable`: string; `service?`: string; `cpu_percent`: number; `rss_bytes`: number; `user?`: string; `age_seconds?`: number; `gpu_bytes?`: number |
| <a id="fields-foreignservice"></a>**ForeignService** | `service`: string; `user?`: string; `processes`: number; `cpu_percent`: number; `rss_bytes`: number; `gpu_bytes?`: number; `age_seconds?`: number |
| <a id="fields-gpuhostmemory"></a>**GPUHostMemory** | `source?`: string; `pinned_gb?`: number; `physical_gb?`: number; `sibling_adjusted?`: boolean; `driver_gb`: number; `host_drop_gb`: number; `outside_gb`: number |
| <a id="fields-gpuselection"></a>**GPUSelection** | `devices?`: array of string; `count?`: number |
| <a id="fields-healthalert"></a>**HealthAlert** | `recovery?`: [AlertRecovery](#fields-alertrecovery); `machine?`: string; `client_kind?`: string; `project?`: string; `reason?`: string; `recipients?`: array of string; `delivery_id`: string; `kind`: string; `store?`: string; `host?`: string; `device?`: string; `reservation?`: string; `job_id?`: string; `attempt?`: number; `at`: timestamp; `delivered`: boolean; `mount?`: [HostMountHealth](#fields-hostmounthealth); `state?`: string; `first_at?`: string; `last_at?`: string; `closed_at?`: string; `count?`: number; `closed_delivered?`: boolean; `opening_attempted?`: boolean; `closing_attempted?`: boolean |
| <a id="fields-healthalertdelivery"></a>**HealthAlertDelivery** | `configured`: boolean; `pending`: number |
| <a id="fields-hold"></a>**Hold** | `kind`: string; `scope?`: string; `owner`: string; `reason`: string; `clears`: string; `expires?`: string; `next`: string; `detail?`: string; `runs_now_on?`: string |
| <a id="fields-hostadmissions"></a>**HostAdmissions** | `host`: string; `current?`: map of number; `recent`: map of number; `declared_with_evidence?`: number |
| <a id="fields-hostcallexecution"></a>**HostCallExecution** | `device?`: string; `machine`: string; `session`: string; `model`: string; `attempt`: string; `id`: string |
| <a id="fields-hostengineexecution"></a>**HostEngineExecution** | `device?`: string; `machine`: string; `session`: string; `model`: string; `attempt`: string |
| <a id="fields-hostgrowthcontributor"></a>**HostGrowthContributor** | `id?`: string; `attempt?`: number; `kind`: string; `launching?`: boolean; `charge_gb`: number; `helper_gb?`: number; `pending_gb`: number; `sample_age_seconds?`: number; `sample_fresh`: boolean |
| <a id="fields-hosthealth"></a>**HostHealth** | `pressure_evaluation?`: [HostPressureEvaluation](#fields-hostpressureevaluation); `probe?`: [ProbeObservation](#fields-probeobservation); `disk_recovery?`: map of number; `persistence_error?`: string; `history`: array of [HostHealthSample](#fields-hosthealthsample); `baseline`: array of [HostHealthSample](#fields-hosthealthsample); `drain_reason?`: string; `candidate?`: string; `consecutive?`: number; `foreign?`: array of [ForeignConsumer](#fields-foreignconsumer); `foreign_services?`: array of [ForeignService](#fields-foreignservice); `foreign_error?`: string; `foreign_at?`: timestamp; `age_seconds`: number |
| <a id="fields-hosthealthsample"></a>**HostHealthSample** | `load`: number; `pressure`: number; `swap_bytes`: number; `wake_seconds`: number; `at`: timestamp; `heartbeat_seconds`: number; `swap_growth_bytes_per_second`: number; `disk_free?`: map of number; `mounts?`: array of [HostMountHealth](#fields-hostmounthealth); `error?`: string |
| <a id="fields-hostidleinterval"></a>**HostIdleInterval** | `host`: string; `start`: timestamp; `end`: timestamp; `machine_class`: string; `state`: string; `refusal_reason?`: string; `online?`: boolean; `maintenance?`: string; `leaving?`: boolean; `in_flight?`: number; `release_custody?`: number; `dependency_ready_demand?`: number; `targets?`: array of [IdleTarget](#fields-idletarget) |
| <a id="fields-hostlaunch"></a>**HostLaunch** | `native_startup?`: string; `native_resident_gb?`: number; `cpu_cores?`: number; `device?`: string; `started_at?`: string; `spec?`: [Spec](#fields-spec); `demand?`: string; `priority_stop?`: [PriorityPreemption](#fields-prioritypreemption); `stop_at?`: string; `machine`: string; `session`: string; `kind`: string; `key`: string; `attempt`: string; `started?`: boolean |
| <a id="fields-hostmemoryderivation"></a>**HostMemoryDerivation** | `source`: string; `reason?`: string; `fallback_gb`: number; `total_gb?`: number; `baseline_max_gb?`: number; `dedicated?`: boolean; `role?`: string; `baseline_quantile_gb?`: number; `reserve_source?`: string; `reserve_gb?`: number; `allowance_gb`: number; `history_7d`: [ExternalMemoryWindow](#fields-externalmemorywindow) |
| <a id="fields-hostmemoryledger"></a>**HostMemoryLedger** | `captured_at`: timestamp; `observed_at`: timestamp; `available_known`: boolean; `available_gb`: number; `capacity_gb`: number; `allowance_gb`: number; `reserved_gb`: number; `growth_gb`: number; `signed_room_gb`: number; `debt_gb`: number; `free_gb`: number; `owners`: array of [HostMemoryOwner](#fields-hostmemoryowner) |
| <a id="fields-hostmemoryobservation"></a>**HostMemoryObservation** | `baseline_version?`: number; `quiescent_v1?`: boolean; `reserve_gb?`: number; `attribution_unknown?`: boolean; `total_gb`: number; `used_gb`: number; `mycelium_gb`: number |
| <a id="fields-hostmemoryowner"></a>**HostMemoryOwner** | `kind`: string; `execution?`: [Execution](#fields-execution); `machine`: string; `session`: string; `model?`: string; `model_attempt?`: string; `state?`: string; `charge_gb`: number; `growth_gb`: number |
| <a id="fields-hostmemorysampling"></a>**HostMemorySampling** | `complete`: boolean; `reason`: string; `samples`: number; `max_collection_seconds`: number; `max_interval_seconds`: number; `final_age_seconds`: number; `failed_pid?`: number; `failed_state?`: string; `failed_counter?`: string; `failed_counter_reason?`: string |
| <a id="fields-hostmemoryterms"></a>**HostMemoryTerms** | `capacity_gb`: number; `charged_gb`: number; `room_gb`: number |
| <a id="fields-hostmounthealth"></a>**HostMountHealth** | `adverse?`: boolean; `root`: string; `latency_seconds`: number; `pending_seconds`: number; `operation_age_seconds?`: number; `slow_threshold_seconds?`: number; `stall_allowance_seconds?`: number; `reason?`: string |
| <a id="fields-hostobligation"></a>**HostObligation** | `launch?`: [HostLaunch](#fields-hostlaunch); `call?`: [HostCallExecution](#fields-hostcallexecution); `engine?`: [HostEngineExecution](#fields-hostengineexecution); `preparation?`: boolean; `execution`: [Result](#fields-result) |
| <a id="fields-hostpolicy"></a>**HostPolicy** | `production_hold`: boolean; `production_hold_scope?`: string |
| <a id="fields-hostpressureevaluation"></a>**HostPressureEvaluation** | `pressure_limit`: number; `baseline_samples`: number; `reported_drain_reason?`: string; `corroboration`: array of string; `recovery_samples`: number |
| <a id="fields-hostramgate"></a>**HostRAMGate** | `raw_gb`: number; `held_gb`: number; `pending_gb`: number; `unavailable?`: boolean |
| <a id="fields-hostrelease"></a>**HostRelease** | `cpu_only?`: boolean; `cpu_cores?`: number; `host_charge_gb?`: number; `cleanup_failed?`: boolean; `device?`: string; `MemoryGB`: number; `HostMemoryGB`: number; `preparation?`: boolean; `Host`: string; `Job`: string; `Machine`: string; `Session`: string; `Attempt`: number; `Until`: timestamp; `devices?`: array of string |
| <a id="fields-iopressure"></a>**IOPressure** | `some?`: [PressureStall](#fields-pressurestall); `full?`: [PressureStall](#fields-pressurestall); `unavailable?`: string; `sampled_at`: timestamp; `age_seconds`: number; `received_at?`: timestamp |
| <a id="fields-idleclasshours"></a>**IdleClassHours** | `state_machine_hours`: map of number; `by_refusal_reason`: map of number |
| <a id="fields-idledecision"></a>**IdleDecision** | `at`: timestamp; `expires_at`: timestamp; `outcome`: string; `stage`: string; `predicate`: string; `hold_kind?`: string |
| <a id="fields-idledemand"></a>**IdleDemand** | `job`: string; `attempt`: number; `piece?`: number; `consumer_paused?`: boolean; `compatibility`: string; `qualification`: string; `needs_device?`: boolean; `decision?`: [IdleDecision](#fields-idledecision) |
| <a id="fields-idlereport"></a>**IdleReport** | `since`: timestamp; `through`: timestamp; `idle_machine_hours_while_runnable_work_waited`: number; `by_machine_class`: map of [IdleClassHours](#fields-idleclasshours); `by_refusal_reason`: map of number; `intervals?`: array of [HostIdleInterval](#fields-hostidleinterval) |
| <a id="fields-idletarget"></a>**IdleTarget** | `machine`: string; `session`: string; `device?`: string; `platform`: string; `free_cores?`: number; `free_host_gb?`: number; `free_device_gb?`: number; `demand?`: array of [IdleDemand](#fields-idledemand) |
| <a id="fields-inventoryobservation"></a>**InventoryObservation** | `scan_at?`: timestamp; `at`: timestamp; `age_seconds`: number; `probe?`: [ProbeObservation](#fields-probeobservation); `state`: string; `error_class?`: string; `operation?`: string; `pending_seconds?`: number |
| <a id="fields-job"></a>**Job** | `completion?`: [CompletionState](#fields-completionstate); `worker_data_cleanup?`: [WorkerDataCleanup](#fields-workerdatacleanup); `admission_wait?`: [AdmissionWaitEvidence](#fields-admissionwaitevidence); `device_fault_start_wait?`: [DeviceFaultStartWait](#fields-devicefaultstartwait); `start_capacity_waiting?`: boolean; `start_capacity_budget?`: number; `launch_capacity_wait?`: map of [LaunchCapacityWait](#fields-launchcapacitywait); `execution_evidence?`: [ExecutionEvidence](#fields-executionevidence); `health_recovery?`: [AlertRecovery](#fields-alertrecovery); `cpu_observation?`: [SharedCPUObservation](#fields-sharedcpuobservation); `resource_attempts?`: array of [AttemptResourceFacts](#fields-attemptresourcefacts); `resource_attempts_dropped?`: number; `efficiency?`: [JobEfficiency](#fields-jobefficiency); `hold?`: [Hold](#fields-hold); `finding?`: [JobFinding](#fields-jobfinding); `native_check_failures?`: array of [NativeCheckFailure](#fields-nativecheckfailure); `wall_time?`: [WallTimeStatus](#fields-walltimestatus); `yield_plan?`: [ScarceYieldProjection](#fields-scarceyieldprojection); `scarce_host_yield?`: [ScarceHostYield](#fields-scarcehostyield); `yield_generation?`: number; `checkpoint_target?`: [CheckpointOrigin](#fields-checkpointorigin); `fault_evidence?`: array of [FaultEvidence](#fields-faultevidence); `shared_host_backoff?`: map of [SharedHostBackoff](#fields-sharedhostbackoff); `pause?`: [JobPause](#fields-jobpause); `yield_pause?`: [YieldPause](#fields-yieldpause); `pause_revision?`: number; `control?`: [JobControl](#fields-jobcontrol); `execution_release?`: string; `local_outputs?`: [TaskOutputCustody](#fields-taskoutputcustody); `storage_allocation?`: [StorageAllocation](#fields-storageallocation); `storage_envelope?`: [JobStorageEnvelope](#fields-jobstorageenvelope); `storage_attempts?`: array of [AttemptStorageFacts](#fields-attemptstoragefacts); `storage_attempts_dropped?`: number; `requirement_reasons?`: array of [RequirementReasonStatus](#fields-requirementreasonstatus); `spec_projection?`: [SpecProjection](#fields-specprojection); `environment_lock?`: [EnvironmentLock](#fields-environmentlock); `recovered_results?`: array of [RecoveredResult](#fields-recoveredresult); `placement_intent?`: [PlacementIntent](#fields-placementintent); `variant_eligibility?`: array of [VariantEligibility](#fields-varianteligibility); `variant_advice?`: array of string; `placement?`: [PlacementDecision](#fields-placementdecision); `runtime?`: [JobRuntime](#fields-jobruntime); `application_progress?`: [ApplicationProgress](#fields-applicationprogress); `liveness?`: [AttemptLiveness](#fields-attemptliveness); `output_reservation_bytes?`: number; `output_admission_pending?`: boolean; `pending_output_refs?`: map of [FileRef](#fields-fileref); `delivery?`: [OutputDelivery](#fields-outputdelivery); `ledger?`: [JobLedgerInfo](#fields-jobledgerinfo); `ledger_entries?`: array of [JobLedgerEntry](#fields-jobledgerentry); `isolation?`: string; `advanced?`: [Advanced](#fields-advanced); `cpu_fallback_from?`: [ResourceNeeds](#fields-resourceneeds); `submission_reused?`: boolean; `last_health_alert_attempt?`: number; `attempt_history?`: array of [AttemptOutcome](#fields-attemptoutcome); `staging_storage_retry_after?`: number; `cancellation?`: [Cancellation](#fields-cancellation); `cancellation_scope?`: [CancellationScope](#fields-cancellationscope); `dependency?`: [PieceBinding](#fields-piecebinding); `fan_out?`: [FanOutProgress](#fields-fanoutprogress); `batch?`: [BatchWork](#fields-batchwork); `timing?`: [WorkTiming](#fields-worktiming); `diagnostic_code?`: string; `allocated_devices?`: array of string; `preemption_count?`: number; `production_backoff?`: map of [ProductionBackoff](#fields-productionbackoff); `priority_policy`: [PriorityMatch](#fields-prioritymatch); `model_limits?`: [ModelLimits](#fields-modellimits); `deadline_guidance?`: [DeadlineGuidance](#fields-deadlineguidance); `resources?`: array of [ResourceBasis](#fields-resourcebasis); `execution_cause?`: [RuntimeCause](#fields-runtimecause); `cleanup_cause?`: [RuntimeCause](#fields-runtimecause); `cancelled_at?`: string; `command_started?`: boolean; `eligible_hosts?`: array of string; `eligible_unknown_machines?`: array of string; `eligible_unrestricted?`: boolean; `model_environment_key?`: string; `model_environment_needs_source?`: boolean; `command_discovery?`: [CommandDiscovery](#fields-commanddiscovery); `entry_coverage?`: [EntryCoverage](#fields-entrycoverage); `sidecar_environment_recipes?`: map of map of [EnvironmentRecipe](#fields-environmentrecipe); `host_memory_charge?`: [AttemptHostMemoryCharge](#fields-attempthostmemorycharge); `file_transport?`: boolean; `local_checkpoint_generation?`: number; `checkpoint_mirrors?`: map of [CheckpointMirror](#fields-checkpointmirror); `file_checkpoints?`: map of [FileCheckpoint](#fields-filecheckpoint); `resume_files?`: [FileCheckpoint](#fields-filecheckpoint); `files_expired?`: boolean; `file_reclamation?`: [FileReclamation](#fields-filereclamation); `output_refs?`: map of [FileRef](#fields-fileref); `memory_envelope?`: [JobMemoryEnvelope](#fields-jobmemoryenvelope); `memory?`: [MemoryPools](#fields-memorypools); `last_memory_admission?`: [MemoryAdmissionSnapshot](#fields-memoryadmissionsnapshot); `memory_attempts?`: array of [AttemptMemoryFacts](#fields-attemptmemoryfacts); `memory_attempts_dropped?`: number; `memory_fallback?`: [SizeHint](#fields-sizehint); `host_memory_override_gb?`: number; `memory_oom_floor?`: [SizeHint](#fields-sizehint); `memory_censored_oom_growth_gb?`: number; `device_memory_floor_gb?`: number; `measured_needs?`: [SizeHint](#fields-sizehint); `oom_capacity?`: [SizeHint](#fields-sizehint); `oom_retries?`: number; `extensions?`: array of [Extension](#fields-extension); `extension?`: [ExtensionSelection](#fields-extensionselection); `output_files?`: map of string; `release_confirmed?`: boolean; `requeued_by_operator?`: boolean; `maintenance_resume?`: string; `last_stop_outcome?`: string; `preemption_placement?`: [PreemptionPlacement](#fields-preemptionplacement); `waiting_since?`: timestamp; `assigned_at?`: string; `staging_timings?`: [StagingTimings](#fields-stagingtimings); `launch_timings?`: [LaunchTimings](#fields-launchtimings); `publication_timings?`: [PublicationTimings](#fields-publicationtimings); `inputs_staged_at?`: string; `finished_at?`: string; `queue_order?`: number; `environment_failure_log?`: string; `environment_recipes?`: map of [EnvironmentRecipe](#fields-environmentrecipe); `copies?`: array of [Copy](#fields-copy); `start_window?`: number; `start_by_explicit?`: boolean; `started_at?`: string; `start_latency_seconds`: number; `starts`: number; `admission_requeues?`: number; `infrastructure_retries?`: number; `losses`: number; `progress`: number; `id`: string; `spec`: [Spec](#fields-spec); `state`: string; `attempt`: number; `machine?`: string; `session?`: string; `queued`: timestamp; `front`: number; `stop_at?`: string; `reason?`: string; `checkpoint`: string; `checkpoint_expired?`: string; `checkpoint_input_expiries?`: array of [CheckpointInputExpiry](#fields-checkpointinputexpiry) |
| <a id="fields-jobcontrol"></a>**JobControl** | `generation`: number; `action`: string; `revision`: number |
| <a id="fields-jobefficiency"></a>**JobEfficiency** | `retained_rates?`: array of [efficiencyRate](#fields-efficiencyrate); `since`: timestamp; `at`: timestamp; `work`: string; `blocker?`: string; `wait_seconds`: map of number; `allocations?`: array of [EfficiencyAllocation](#fields-efficiencyallocation); `rate`: [efficiencyRate](#fields-efficiencyrate); `stranded?`: boolean; `stranded_seconds?`: number |
| <a id="fields-jobfinding"></a>**JobFinding** | `code`: string; `owner`: string; `what`: string; `action`: string |
| <a id="fields-jobledgerentry"></a>**JobLedgerEntry** | `resize?`: [JobResizeAudit](#fields-jobresizeaudit); `placement?`: [PlacementDecision](#fields-placementdecision); `work_learning?`: [WorkLearningChange](#fields-worklearningchange); `sequence`: number; `at`: timestamp; `attempt`: number; `execution_id`: string; `process_id?`: string; `supervisor_pid?`: number; `supervisor_birth?`: string; `machine`: string; `session`: string; `kind`: string; `reason`: string; `state?`: string; `release`: string; `custody_id?`: string; `custody_start?`: string; `custody_end?`: string; `file_release_selection_sha256?`: string |
| <a id="fields-jobledgerinfo"></a>**JobLedgerInfo** | `complete`: boolean; `high_water`: number |
| <a id="fields-jobmemoryenvelope"></a>**JobMemoryEnvelope** | `probe_floor_gb?`: number; `launch_drift_gb?`: number; `available_host_gb?`: number; `source`: string; `refusal?`: string; `machine`: string; `device?`: string; `host_gb`: number; `device_gb`: number; `enforcement`: string; `basis?`: string; `evidence?`: number; `learned?`: [LearnedMemory](#fields-learnedmemory); `charge_host_gb?`: number; `limit_host_gb?`: number; `sizing_inputs?`: [MemorySizingInputs](#fields-memorysizinginputs) |
| <a id="fields-jobpause"></a>**JobPause** | `checkpoint_floor_missing?`: [CheckpointFloorMissing](#fields-checkpointfloormissing); `reason`: string; `until?`: string; `parent?`: string |
| <a id="fields-jobresizeaudit"></a>**JobResizeAudit** | `requested_by`: string; `old_host_memory_gb`: number; `new_host_memory_gb`: number; `old_limit_gb?`: number |
| <a id="fields-jobruntime"></a>**JobRuntime** | `elapsed_seconds?`: number; `last_accepted_seconds?`: number; `reason?`: string; `limitation`: string; `remaining_work_unknown`: boolean; `samples`: number |
| <a id="fields-jobstorageenvelope"></a>**JobStorageEnvelope** | `key?`: [StorageKey](#fields-storagekey); `evidence?`: [StorageObservation](#fields-storageobservation); `source`: string; `basis?`: string; `samples`: number; `output_bytes?`: number; `scratch_bytes?`: number; `spool_bytes?`: number; `checkpoint_bytes?`: number; `metadata_bytes?`: number; `managed_bytes?`: number; `files?`: number; `input_bytes`: number; `predicted_need?`: number; `margin`: number; `positive_error`: number; `multiplier`: number; `hard_limit?`: number; `enforcement`: string; `exploration?`: string |
| <a id="fields-keepfile"></a>**KeepFile** | `path`: string; `format?`: string |
| <a id="fields-launchcapacitywait"></a>**LaunchCapacityWait** | `machine`: string; `session`: string; `attempt`: number; `evidence`: string; `admission?`: [AdmissionDetail](#fields-admissiondetail); `live_growth_surplus_gb?`: number |
| <a id="fields-launchphasetiming"></a>**LaunchPhaseTiming** | `phase`: string; `seconds`: number |
| <a id="fields-launchtimings"></a>**LaunchTimings** | `assignment_received_at`: timestamp; `observed_at`: timestamp; `inputs_ready_at?`: string; `preparation_ready_at?`: string; `command_start_at?`: string; `command_start_boundary?`: string; `current_phase?`: string; `phases`: array of [LaunchPhaseTiming](#fields-launchphasetiming) |
| <a id="fields-learnedmemory"></a>**LearnedMemory** | `host_gb`: number; `device_gb?`: number; `evidence`: number; `source`: string; `peak_host_gb?`: number; `peak_device_gb?`: number |
| <a id="fields-livenesssignal"></a>**LivenessSignal** | `value?`: number; `age_seconds`: number; `compared`: boolean; `advancing`: boolean; `idle_seconds`: number; `unavailable?`: boolean |
| <a id="fields-macholinkage"></a>**MachOLinkage** | `binary`: string; `library`: string |
| <a id="fields-machine"></a>**Machine** | `memory_shadow?`: [MemoryShadowReport](#fields-memoryshadowreport); `inventory_probe?`: [InventoryObservation](#fields-inventoryobservation); `suspect?`: boolean; `io_pressure?`: [IOPressure](#fields-iopressure); `hygiene_collection?`: [WorkerHygieneCollection](#fields-workerhygienecollection); `host_health?`: [HostHealth](#fields-hosthealth); `tool_inventory?`: map of array of string; `output_destination_ids?`: array of string; `shared_host?`: [SharedHostProfile](#fields-sharedhostprofile); `shared_host_reason?`: string; `shared_host_pressure?`: [SharedHostPressure](#fields-sharedhostpressure); `shared_host_yield_reason?`: string; `shared_cpu_enforcement?`: string; `external_memory?`: [ExternalMemoryHistory](#fields-externalmemoryhistory); `memory_observation?`: [HostMemoryObservation](#fields-hostmemoryobservation); `release?`: [RunningRelease](#fields-runningrelease); `fleet?`: [FleetRuntimeProof](#fields-fleetruntimeproof); `fleet_proof_issue?`: string; `disk_received_at?`: timestamp; `disk_volumes?`: array of [DiskVolume](#fields-diskvolume); `enrollment?`: [EnrollmentProof](#fields-enrollmentproof); `recovering?`: boolean; `recovering_since?`: string; `recovery_needs_attention?`: boolean; `replacement?`: [MachineReplacement](#fields-machinereplacement); `devices`: array of [MachineDevice](#fields-machinedevice); `inventory_reason?`: string; `cpu_isolation?`: string; `cpu_reserve_cores?`: number; `cpu_budget_cores?`: number; `effective_cpu_cores`: number; `cpu_in_use_cores`: number; `free_cpu_cores`: number; `cpu_capacity_reason?`: string; `docker_cause?`: [retiredField](#fields-retiredfield); `benchmark_pending?`: [retiredField](#fields-retiredfield); `control_plane_cpu_cores?`: number; `cached_shared_inputs?`: array of string; `model_snapshots?`: array of string; `model_snapshot_bytes?`: map of number; `shared_input_evidence?`: [SharedInputEvidence](#fields-sharedinputevidence); `shared_input_cache?`: [SharedInputCapacity](#fields-sharedinputcapacity); `capabilities?`: array of string; `ephemeral_refusal?`: string; `native_isolation_warning?`: string; `max_jobs?`: number; `admission_reason?`: string; `file_transfer_error?`: string; `result_publications?`: array of [ResultPublication](#fields-resultpublication); `fault_monitor_error?`: string; `capability_profile?`: [CapabilityProfile](#fields-capabilityprofile); `cpu_extension_fingerprint?`: string; `extension_fingerprint?`: string; `extension_reports?`: array of [ExtensionReport](#fields-extensionreport); `host_memory?`: [HostMemoryTerms](#fields-hostmemoryterms); `memory_allowance_bytes?`: number; `memory_allowance_qualification?`: string; `host_memory_policy_mismatch?`: boolean; `host_memory_budget_measured?`: boolean; `host_memory_derivation?`: [HostMemoryDerivation](#fields-hostmemoryderivation); `effective_host_memory_budget_gb?`: number; `host_memory_in_use_gb?`: number; `memory_containment?`: [MemoryContainment](#fields-memorycontainment); `memory_enforcement?`: string; `host_memory_budget_gb?`: number; `memory_ceiling_slack_gb?`: number; `docker_cleanup?`: [retiredField](#fields-retiredfield); `disabled?`: string; `last_resort?`: boolean; `host_id?`: string; `host_available_memory_gb`: number; `free_host_memory_gb?`: number; `host_memory_measured?`: boolean; `host_memory_error?`: string; `unified_memory?`: boolean; `memory_used_gb?`: number; `total_memory_gb`: number; `reserved_memory_gb?`: number; `device?`: string; `environment_builder?`: boolean; `environment_reports?`: array of [EnvironmentReport](#fields-environmentreport); `environment_build_seconds?`: map of number; `environment_bytes?`: map of number; `model_copy_seconds?`: map of number; `copy_reports?`: array of [CopyReport](#fields-copyreport); `copy_bytes_per_second?`: number; `approved_models?`: [retiredField](#fields-retiredfield); `cached_models?`: array of string; `cache_free_bytes`: number; `cache_error?`: string; `memory_error?`: string; `memory_measured?`: boolean; `model_config_hash?`: string; `model_url?`: string; `unavailable_roots?`: array of [UnavailableRoot](#fields-unavailableroot); `checkpoint_unavailable?`: boolean; `id`: string; `session`: string; `gpu`: string; `free_memory_gb`: number; `models?`: array of string; `environments?`: array of string; `last_seen`: timestamp; `leaving?`: boolean; `gone?`: boolean; `model_approval_states?`: [retiredField](#fields-retiredfield); `host_ram_gate?`: [HostRAMGate](#fields-hostramgate) |
| <a id="fields-machinedevice"></a>**MachineDevice** | `inventory_probe_failed_scan?`: string; `inventory_probe_after?`: string; `fault_evidence?`: array of [FaultEvidence](#fields-faultevidence); `quarantine_recovery_blocked?`: [DeviceHealthFailure](#fields-devicehealthfailure); `unsupported_on_device?`: array of string; `quarantine_recovery?`: [DeviceRecovery](#fields-devicerecovery); `telemetry?`: [DeviceTelemetry](#fields-devicetelemetry); `attempts?`: array of [Execution](#fields-execution); `models?`: array of [ModelSlot](#fields-modelslot); `releases?`: array of [HostRelease](#fields-hostrelease); `extension_fingerprint?`: string; `extension_reports?`: array of [ExtensionReport](#fields-extensionreport); `id`: string; `platform`: string; `identity`: string; `fingerprint`: string; `selector`: string; `memory_mode`: string; `total_memory_gb`: number; `reserve_memory_gb`: number; `budget_gb`: number; `free_memory_gb`: number; `memory_measured?`: boolean; `unified_memory?`: boolean; `reason?`: string; `repeated_teardown_fault?`: boolean; `capability_profile?`: [CapabilityProfile](#fields-capabilityprofile) |
| <a id="fields-machinereplacement"></a>**MachineReplacement** | `machine`: string; `device`: string |
| <a id="fields-memoryadmissionreservation"></a>**MemoryAdmissionReservation** | `id`: string; `job`: string; `state`: string; `ready_at?`: string; `pending_known`: boolean; `pending_observed_at?`: string; `pending`: array of string |
| <a id="fields-memoryadmissionsnapshot"></a>**MemoryAdmissionSnapshot** | `captured_at`: timestamp; `waiting_since`: timestamp; `attempt`: number; `machine`: string; `session`: string; `host`: string; `device?`: string; `target`: [ExecutionTarget](#fields-executiontarget); `envelope`: [JobMemoryEnvelope](#fields-jobmemoryenvelope); `drift_reserve_gb`: number; `reason`: string; `ledger`: [HostMemoryLedger](#fields-hostmemoryledger); `reservations`: array of [MemoryAdmissionReservation](#fields-memoryadmissionreservation) |
| <a id="fields-memorycontainment"></a>**MemoryContainment** | `state`: string; `reason?`: string; `next?`: string |
| <a id="fields-memoryescalationprior"></a>**MemoryEscalationPrior** | `peak`: [AttemptMemoryPeak](#fields-attemptmemorypeak); `recorded_at`: timestamp |
| <a id="fields-memoryeventcounts"></a>**MemoryEventCounts** | `high`: number; `max`: number; `oom`: number; `oom_kill`: number; `oom_group_kill`: number |
| <a id="fields-memorypeakrange"></a>**MemoryPeakRange** | `samples`: number; `min_gb`: number; `max_gb`: number |
| <a id="fields-memorypoolstop"></a>**MemoryPoolStop** | `observed_at`: timestamp; `available_gb`: number; `reserve_gb`: number; `growth_gb_per_second`: number; `sustained_growth_gb_per_second`: number; `next_loss_gb`: number; `grace_loss_gb`: number; `sample_seconds`: number; `grace_seconds`: number; `host_pressure?`: number; `trigger`: string |
| <a id="fields-memorypools"></a>**MemoryPools** | `ram`: [memoryPoolEstimate](#fields-memorypoolestimate); `card?`: [memoryPoolEstimate](#fields-memorypoolestimate); `keep_gb`: number; `limit_gb?`: number |
| <a id="fields-memorypressureevidence"></a>**MemoryPressureEvidence** | `reason`: string; `elapsed_us`: number; `host_stall_us?`: number; `cgroup_stall_us?`: number; `host_avg10`: number; `cgroup_avg10`: number; `host_swap_growth_bytes`: number; `host_swap_out_bytes`: number; `resident_gb`: number; `min_available_gb?`: number; `reserve_gb`: number; `available_floor_gb`: number; `swap_budget_gb`: number; `host_oom_delta`: number; `host_pressure_level?`: number; `cgroup_refault_anon?`: number; `cgroup_swap_out_pages?`: number |
| <a id="fields-memoryroomdecision"></a>**MemoryRoomDecision** | `room_gb`: number; `floor_gb`: number; `allowance_gb`: number; `capacity_gb`: number; `capacity_source`: string; `charges_gb`: number; `pending_gb`: number; `available_gb?`: number; `reserve_gb`: number; `own_growth_gb`: number; `host_loss_gb`: number; `ledger_read_failed?`: boolean |
| <a id="fields-memoryshadowattempt"></a>**MemoryShadowAttempt** | `id`: string; `attempt`: number; `expected_gb`: number; `footprint_gb?`: number; `high_water_gb`: number; `hold_gb?`: number; `commitment_gb?`: number; `growth_gb_per_second`: number; `lent?`: boolean |
| <a id="fields-memoryshadowpool"></a>**MemoryShadowPool** | `pool`: string; `capacity_gb`: number; `keep_gb`: number; `free_gb?`: number; `footprint_gb`: number; `held_gb`: number; `commitment_gb`: number; `slack_gb?`: number; `reach_gb?`: number; `forecast_free_gb?`: number; `owners_gb`: number; `owners_forecast_gb`: number; `pressure_warning?`: boolean; `attempts?`: array of [MemoryShadowAttempt](#fields-memoryshadowattempt) |
| <a id="fields-memoryshadowreport"></a>**MemoryShadowReport** | `pools`: array of [MemoryShadowPool](#fields-memoryshadowpool); `fastest_growth_gb_per_second?`: map of number; `reaction_seconds`: number |
| <a id="fields-memoryshadowweek"></a>**MemoryShadowWeek** | `since`: timestamp; `through`: timestamp; `r4_over_e`: number; `r4_victim`: number; `repeat_by_cause`: map of number; `r4_attempt_hours`: number; `raises`: number; `raise_gib_hours`: number; `owners_holds`: number; `owners_hold_gib_hours`: number; `victims_after_lending`: number; `current_memory_pressure_stops`: number; `current_memory_pressure_attempt_hours`: number |
| <a id="fields-memorysizinginputs"></a>**MemorySizingInputs** | `ledger_remainder_gb`: number; `allowance_gb`: number; `idle_history_bound_gb?`: number; `slack_gb`: number; `launch_gap_gb`: number; `projected_remainder_gb?`: number; `drift_gb?`: number; `resident_engine_host_gb?`: number; `recovery_capacity_gb?`: number; `constrained_check_host_gb?`: number |
| <a id="fields-modelcustody"></a>**ModelCustody** | `protocol`: number; `id`: string; `worker_pid?`: number |
| <a id="fields-modellimits"></a>**ModelLimits** | `policy_revision`: number; `max_bytes?`: number; `free_floor_bytes?`: number |
| <a id="fields-modelmemorymeasurement"></a>**ModelMemoryMeasurement** | `peak_gb?`: number; `source?`: string; `sampled_at?`: string; `samples`: number; `complete`: boolean; `constrained`: boolean; `pressure_unavailable?`: boolean; `unavailable?`: string |
| <a id="fields-modelmemorypeaks"></a>**ModelMemoryPeaks** | `load?`: [ModelMemoryWindow](#fields-modelmemorywindow); `steady?`: [ModelMemoryWindow](#fields-modelmemorywindow); `request?`: [ModelMemoryWindow](#fields-modelmemorywindow) |
| <a id="fields-modelmemorywindow"></a>**ModelMemoryWindow** | `started_at`: timestamp; `ended_at?`: string; `request_id?`: string; `overlaps_other_requests?`: boolean; `host`: [ModelMemoryMeasurement](#fields-modelmemorymeasurement); `device`: [ModelMemoryMeasurement](#fields-modelmemorymeasurement) |
| <a id="fields-modelplacement"></a>**ModelPlacement** | `project?`: string; `allowed_machines?`: array of string; `project_machines?`: array of string |
| <a id="fields-modelpolicy"></a>**ModelPolicy** | `sources?`: array of string; `max_bytes?`: number; `free_floor_bytes?`: number |
| <a id="fields-modelslot"></a>**ModelSlot** | `memory_peaks?`: [ModelMemoryPeaks](#fields-modelmemorypeaks); `priority_policy?`: [PriorityMatch](#fields-prioritymatch); `custody?`: [ModelCustody](#fields-modelcustody); `url?`: string; `device?`: string; `preemption_count?`: number; `run_since?`: string; `class?`: string; `priority_stop?`: [PriorityPreemption](#fields-prioritypreemption); `project?`: string; `runtime_failed?`: boolean; `load_until?`: string; `load_timed_out?`: boolean; `failure_counted?`: boolean; `attempt?`: string; `crash_count?`: number; `healthy_since?`: string; `health_revision?`: number; `placement?`: [ModelPlacement](#fields-modelplacement); `retry_at?`: string; `retry_delay?`: number; `releasing_calls?`: boolean; `recovering?`: boolean; `calls_unconfirmed?`: boolean; `production_used?`: boolean; `stop_at?`: string; `environment_recipe?`: [EnvironmentRecipe](#fields-environmentrecipe); `model`: string; `machine`: string; `session`: string; `state`: string; `priority`: string; `last_used`: timestamp; `calls_in_flight`: number; `error?`: string; `idle_seconds`: number |
| <a id="fields-modelsnapshot"></a>**ModelSnapshot** | `repo`: string; `revision`: string; `files?`: array of string |
| <a id="fields-nativecheckfailure"></a>**NativeCheckFailure** | `job_id`: string; `attempt`: number; `code?`: string; `exception_type?`: string; `missing_module?`: string |
| <a id="fields-nativereadiness"></a>**NativeReadiness** | `path`: string; `timeout`: string |
| <a id="fields-nativesidecar"></a>**NativeSidecar** | `name`: string; `command`: array of string; `environment?`: string; `env?`: map of string; `memory_gb?`: number; `port?`: number; `readiness?`: [NativeReadiness](#fields-nativereadiness) |
| <a id="fields-observationkey"></a>**ObservationKey** | `repeated_work?`: string; `server_digest?`: string; `allowance?`: string; `work`: string; `extension_digest`: string; `target`: [ExecutionTarget](#fields-executiontarget); `reference`: [WorkReference](#fields-workreference); `host`: string; `component`: string; `hardware`: string; `fingerprint`: string; `environment`: string; `configuration`: string; `concurrency`: number; `batch_size`: number; `check?`: boolean |
| <a id="fields-outputdelivery"></a>**OutputDelivery** | `generation?`: number; `verification_round?`: number; `progress_operations?`: number; `protocol?`: string; `kind?`: string; `destination_id`: string; `progress?`: number; `id`: string; `attempt`: number; `machine`: string; `session`: string; `state`: string; `manifest_sha256`: string; `bytes`: number; `pending_at`: timestamp; `last_progress_at?`: string; `last_attempt_at?`: string; `next_attempt_at?`: timestamp; `verified_at?`: string; `attempts`: number; `cause?`: string |
| <a id="fields-outputdestination"></a>**OutputDestination** | `protocol`: string; `kind`: string; `id`: string |
| <a id="fields-ownersignals"></a>**OwnerSignals** | `gpu_heat?`: string; `heat_coverage?`: string; `idle_seconds`: number; `idle_known`: boolean; `laptop`: boolean; `power`: string; `heat`: string |
| <a id="fields-pathdiagnostic"></a>**PathDiagnostic** | `field`: string; `side`: string; `path`: string; `resolution_base`: string; `cause`: string; `companion?`: string |
| <a id="fields-physicalstoragestatus"></a>**PhysicalStorageStatus** | `mode`: string; `sampled_at`: timestamp; `disk_free_bytes`: number; `remaining_upload_bytes`: number; `reserved_output_bytes`: number; `uncommitted_bytes`: number; `protected_free_bytes`: number; `control`: [ControlStorageStatus](#fields-controlstoragestatus); `upload_metadata_bytes`: number; `qualification`: string; `error?`: string |
| <a id="fields-piecebinding"></a>**PieceBinding** | `job`: string; `piece`: string; `state`: string; `execution`: [Execution](#fields-execution); `executions?`: map of [Execution](#fields-execution); `ready_at?`: string; `files?`: map of [FileRef](#fields-fileref); `inputs?`: map of string |
| <a id="fields-piecedependency"></a>**PieceDependency** | `job`: string; `piece?`: string; `outputs`: map of string |
| <a id="fields-pieceresult"></a>**PieceResult** | `execution`: [Execution](#fields-execution); `output_refs?`: map of [FileRef](#fields-fileref); `output_files?`: map of string |
| <a id="fields-placementcoverage"></a>**PlacementCoverage** | `origin`: string; `state`: string; `platforms?`: array of string; `targets?`: array of [ExecutionTarget](#fields-executiontarget) |
| <a id="fields-placementdecision"></a>**PlacementDecision** | `origin`: string; `attempt`: number; `assigned_at`: timestamp; `host`: string; `devices?`: array of string; `host_choice`: string; `device_choice`: string; `reason`: string; `limited_by`: array of string |
| <a id="fields-placementintent"></a>**PlacementIntent** | `origin`: string; `scope`: string; `host_constraint`: string; `device_constraint`: string; `targets?`: array of [ExecutionTarget](#fields-executiontarget); `recipe_device_targets?`: array of [ExecutionTarget](#fields-executiontarget); `required_backends?`: array of string; `preferred_backends?`: array of string; `environment`: [PlacementCoverage](#fields-placementcoverage); `qualification`: string; `parity`: string |
| <a id="fields-platform"></a>**Platform** | `required?`: array of string; `preferred?`: array of string; `any?`: boolean |
| <a id="fields-predictionerror"></a>**PredictionError** | `job_id`: string; `attempt`: number; `work`: string; `basis`: string; `reserved_host_gb`: number; `accepted_host_peak_gb`: number; `host_error_gb`: number; `cgroup_peak_gb?`: number; `reserved_device_gb`: number; `accepted_device_peak_gb`: number; `device_error_gb`: number; `predicted_seconds`: number; `actual_seconds`: number; `duration_error_seconds`: number |
| <a id="fields-preemption"></a>**Preemption** | `at`: timestamp; `job`: string; `project`: string; `for`: string |
| <a id="fields-preemptionplacement"></a>**PreemptionPlacement** | `machine`: string; `session`: string; `device`: string; `releases?`: array of string |
| <a id="fields-pressurestall"></a>**PressureStall** | `avg10`: number; `avg60`: number; `avg300`: number; `total_usec`: number |
| <a id="fields-prioritymatch"></a>**PriorityMatch** | `tier`: string; `index`: number; `selector`: string; `revision`: number |
| <a id="fields-prioritypolicy"></a>**PriorityPolicy** | `version`: number; `tiers`: array of [PriorityTier](#fields-prioritytier); `projects`: map of [ProjectPolicy](#fields-projectpolicy); `models?`: [ModelPolicy](#fields-modelpolicy); `hosts?`: map of [HostPolicy](#fields-hostpolicy) |
| <a id="fields-prioritypolicyrecord"></a>**PriorityPolicyRecord** | `revision`: number; `policy`: [PriorityPolicy](#fields-prioritypolicy); `warnings?`: array of string |
| <a id="fields-prioritypreemption"></a>**PriorityPreemption** | `memory_sample?`: [memoryStopSample](#fields-memorystopsample); `memory_pool?`: [MemoryPoolStop](#fields-memorypoolstop); `device_pool?`: [DevicePoolStop](#fields-devicepoolstop); `cause`: string; `demand`: string; `victim`: string; `policy`: [PriorityMatch](#fields-prioritymatch) |
| <a id="fields-prioritytier"></a>**PriorityTier** | `name`: string; `selectors`: array of string |
| <a id="fields-probeobservation"></a>**ProbeObservation** | `period_seconds`: number; `cost_seconds`: number; `valid_seconds`: number |
| <a id="fields-productionbackoff"></a>**ProductionBackoff** | `last_at`: timestamp; `until?`: timestamp |
| <a id="fields-productionstart"></a>**ProductionStart** | `at`: timestamp; `job`: string; `seconds`: number |
| <a id="fields-program"></a>**Program** | `type`: string; `file`: string; `args?`: array of string |
| <a id="fields-projectfilestoreusage"></a>**ProjectFileStoreUsage** | `project`: string; `retained_bytes`: number; `added_bytes_24h`: number; `reserved_output_bytes?`: number; `max_retained_file_bytes?`: number; `max_added_file_bytes_24h?`: number; `reserved_file_bytes?`: number; `refusal?`: string |
| <a id="fields-projectpolicy"></a>**ProjectPolicy** | `default_call_priority?`: string; `allowed_machines?`: array of string; `production_parity_hosts?`: map of array of string; `max_concurrent?`: number; `weight`: number; `max_concurrent_experiments`: number; `max_memory_share`: number; `max_retained_file_bytes?`: number; `max_added_file_bytes_24h?`: number; `reserved_file_bytes?`: number; `file_retention?`: string |
| <a id="fields-projectrestrictions"></a>**ProjectRestrictions** | `project`: string; `jobs`: number; `host_restricted`: number; `host_wait_seconds`: number; `host_stranded_jobs`: number; `restricted`: number; `restricted_wait_seconds`: number; `stranded_jobs`: number; `targeted`: number; `non_production_targeted`: number; `stranded_seconds`: number; `stranded_platforms?`: array of string; `typed_memory`: number; `memory_compared`: number; `memory_over_peak_gb`: number; `typed_cores`: number; `cores_compared`: number; `cores_over_peak`: number; `advice?`: array of string |
| <a id="fields-projectusage"></a>**ProjectUsage** | `name`: string; `gpu_hours`: number; `pinned_hours`: number |
| <a id="fields-publicationtimings"></a>**PublicationTimings** | `worker?`: [workerPublicationTimings](#fields-workerpublicationtimings); `verification_seconds?`: number; `completion_queue_seconds`: number; `release_to_accept_seconds?`: number |
| <a id="fields-qualitymeasurement"></a>**QualityMeasurement** | `reference`: string; `value`: number; `reference_value`: number; `margin_of_error`: number |
| <a id="fields-rsslimitevidence"></a>**RSSLimitEvidence** | `observed_bytes`: number; `limit_bytes`: number; `quality`: string; `memory_room?`: [MemoryRoomDecision](#fields-memoryroomdecision) |
| <a id="fields-recoveredresult"></a>**RecoveredResult** | `progress?`: number; `id`: string; `attempt`: number; `machine`: string; `session`: string; `key`: string; `requested_by`: string; `source`: [RecoverySource](#fields-recoverysource); `reason`: string; `files`: map of [FileRef](#fields-fileref); `result_id`: string; `kind`: string; `recovered_by`: string; `recovered_at`: timestamp; `expires_at`: timestamp; `manifest_sha256`: string |
| <a id="fields-recoverysource"></a>**RecoverySource** | `kind`: string; `location`: string |
| <a id="fields-releaseslotidentity"></a>**ReleaseSlotIdentity** | `sha256`: string; `size`: number; `version?`: string |
| <a id="fields-requestlatency"></a>**RequestLatency** | `route`: string; `count`: number; `samples`: number; `p50_ms`: number; `p95_ms`: number; `p99_ms`: number |
| <a id="fields-requirementreasonstatus"></a>**RequirementReasonStatus** | `constraint`: string; `reason_source`: string; `source`: string |
| <a id="fields-reservation"></a>**Reservation** | `id`: string; `selector`: string; `owner`: string; `reason`: string; `max_lifetime`: string; `job`: string; `target`: [ComponentExclusion](#fields-componentexclusion); `revision`: number; `created_at`: timestamp; `expires_at`: timestamp; `ready_at?`: string; `expired_at?`: string; `released_at?`: string; `release_reason?`: string; `state`: string; `pending?`: array of string; `cleanup_pending?`: boolean; `text`: string |
| <a id="fields-reservationoptions"></a>**ReservationOptions** | `id`: string; `selector`: string; `owner`: string; `reason`: string; `max_lifetime`: string |
| <a id="fields-resourcebasis"></a>**ResourceBasis** | `dimension`: string; `declared?`: number; `learned?`: number; `evidence?`: number; `used?`: number; `basis`: string; `note?`: string |
| <a id="fields-resourceneeds"></a>**ResourceNeeds** | `tools?`: array of string; `network?`: boolean; `memory?`: string; `gpu?`: [GPUSelection](#fields-gpuselection); `cpu_cores?`: number; `memory_gb`: number; `host_memory_gb?`: number; `platform`: [Platform](#fields-platform); `model?`: string |
| <a id="fields-restartgracestatus"></a>**RestartGraceStatus** | `active`: boolean; `accrued_seconds`: number; `remaining_seconds`: number |
| <a id="fields-result"></a>**Result** | `checkpoint_floor_missing?`: [CheckpointFloorMissing](#fields-checkpointfloormissing); `worker_data_cleanup?`: [WorkerDataCleanup](#fields-workerdatacleanup); `execution_evidence?`: [ExecutionEvidence](#fields-executionevidence); `resources?`: [AttemptResources](#fields-attemptresources); `signal_evidence?`: [SignalEvidence](#fields-signalevidence); `final_checkpoint_generation?`: number; `storage_peak?`: [AttemptStoragePeak](#fields-attemptstoragepeak); `staging_timings?`: [StagingTimings](#fields-stagingtimings); `launch_timings?`: [LaunchTimings](#fields-launchtimings); `inputs_staged_at?`: string; `diagnostics?`: [AttemptDiagnostics](#fields-attemptdiagnostics); `priority_stop?`: [PriorityPreemption](#fields-prioritypreemption); `execution_cause?`: [RuntimeCause](#fields-runtimecause); `cleanup_cause?`: [RuntimeCause](#fields-runtimecause); `polite_stop?`: boolean; `output_refs?`: map of [FileRef](#fields-fileref); `memory_peak?`: [AttemptMemoryPeak](#fields-attemptmemorypeak); `usage?`: [SizeHint](#fields-sizehint); `out_of_memory?`: boolean; `extension_unavailable?`: boolean; `operator_stop?`: boolean; `release_only?`: boolean; `forced?`: boolean; `release_pending?`: boolean; `machine`: string; `session`: string; `progress?`: number; `id`: string; `attempt`: number; `interrupted?`: boolean; `success`: boolean; `error?`: string |
| <a id="fields-resultpublication"></a>**ResultPublication** | `progress?`: number; `id`: string; `attempt`: number; `published_bytes`: number; `blocking`: string; `waiting_seconds`: number; `last_error?`: string |
| <a id="fields-resumecompatibility"></a>**ResumeCompatibility** | `checkpoint_format`: string; `result_contract`: string; `targets`: array of [ResumeTarget](#fields-resumetarget) |
| <a id="fields-resumetarget"></a>**ResumeTarget** | `target`: [ExecutionTarget](#fields-executiontarget); `environment`: string; `result_check`: string |
| <a id="fields-retentioncapacity"></a>**RetentionCapacity** | `memory_available_bytes`: number; `disk_available_bytes`: number |
| <a id="fields-runspec"></a>**RunSpec** | `memory_gb?`: number; `command`: array of string; `workdir?`: string; `env?`: map of string; `sidecars?`: array of [NativeSidecar](#fields-nativesidecar); `docker?`: boolean; `docker_network?`: string; `docker_memory_gb?`: number; `docker_memory_swap?`: number; `docker_sidecars?`: array of JSON value |
| <a id="fields-runningrelease"></a>**RunningRelease** | `version`: string; `commit`: string |
| <a id="fields-runtimecause"></a>**RuntimeCause** | `file_capacity?`: [FileCapacityEvidence](#fields-filecapacityevidence); `checkpoint_storage?`: [CheckpointStorageEvidence](#fields-checkpointstorageevidence); `finalization?`: [FinalizationDetail](#fields-finalizationdetail); `linkage?`: [MachOLinkage](#fields-macholinkage); `rss_limit?`: [RSSLimitEvidence](#fields-rsslimitevidence); `missing_tools?`: array of string; `host?`: string; `staging?`: [StagingCause](#fields-stagingcause); `path_diagnostic?`: [PathDiagnostic](#fields-pathdiagnostic); `admission?`: [AdmissionDetail](#fields-admissiondetail); `prelaunch_refused?`: boolean; `launch_refusal?`: string; `operation`: string; `class`: string; `exit_code?`: number; `predicate?`: string; `reason?`: string |
| <a id="fields-scarcehostyield"></a>**ScarceHostYield** | `cause`: string; `demand`: string; `victim`: string; `attempt`: number; `machine`: string; `session`: string; `destination`: string; `destination_device`: string; `destination_target`: [ExecutionTarget](#fields-executiontarget); `generation`: number; `authorized`: boolean; `final_received`: boolean; `hold?`: string |
| <a id="fields-scarceyieldprojection"></a>**ScarceYieldProjection** | `host?`: string; `requirements?`: array of string; `victims?`: array of [ScarceHostYield](#fields-scarcehostyield); `reason`: string |
| <a id="fields-sharedcpuobservation"></a>**SharedCPUObservation** | `attempt`: number; `machine`: string; `session`: string; `elapsed_seconds`: number; `usage_seconds`: number; `peak_cores`: number |
| <a id="fields-sharedhostbackoff"></a>**SharedHostBackoff** | `recovery_seconds?`: number; `quiet_since?`: timestamp; `last_observation?`: timestamp |
| <a id="fields-sharedhostpressure"></a>**SharedHostPressure** | `cpu_known?`: boolean; `cpu_psi?`: number; `probe?`: [ProbeObservation](#fields-probeobservation); `learning?`: [sharedPressureLearning](#fields-sharedpressurelearning); `memory_known`: boolean; `wake_known`: boolean; `memory_level`: number; `memory_psi`: number; `wake_delay_ms`: number; `age_seconds`: number; `received_at`: timestamp; `delivery_seconds?`: number |
| <a id="fields-sharedhostprofile"></a>**SharedHostProfile** | `interactive_reserve_gb`: number |
| <a id="fields-sharedinput"></a>**SharedInput** | `nas?`: string; `sha256?`: string; `size_bytes?`: number; `archive?`: string; `unpacked_bytes?`: number; `manifest_file?`: string; `manifest?`: map of [FileRef](#fields-fileref) |
| <a id="fields-sharedinputcapacity"></a>**SharedInputCapacity** | `capacity_bytes`: number; `free_bytes`: number; `evictable_bytes`: number; `protected?`: map of number |
| <a id="fields-sharedinputevidence"></a>**SharedInputEvidence** | `hits`: number; `misses`: number; `nas_declared_bytes`: number |
| <a id="fields-signalevidence"></a>**SignalEvidence** | `started_at`: timestamp; `attribution`: string; `exit_status`: number; `kernel_core_dumped?`: boolean; `pid`: number; `signal`: string; `observed_at`: timestamp; `fault_address?`: string; `libraries?`: array of [FaultLibrary](#fields-faultlibrary); `core_capture`: string; `core_pointer?`: string; `core_bytes?`: number; `core_truncated?`: boolean; `unavailable?`: array of string; `capture_ns`: number |
| <a id="fields-sizehint"></a>**SizeHint** | `memory_gb?`: number; `host_memory_gb?`: number |
| <a id="fields-snapshot"></a>**Snapshot** | `idle?`: [IdleReport](#fields-idlereport); `summary?`: [StatusSummary](#fields-statussummary); `efficiency?`: [EfficiencyReport](#fields-efficiencyreport); `evidence_retention?`: [EvidenceRetention](#fields-evidenceretention); `freshness?`: [SnapshotFreshness](#fields-snapshotfreshness); `restart_grace?`: [RestartGraceStatus](#fields-restartgracestatus); `storage_observations?`: array of [StorageObservation](#fields-storageobservation); `jobs_page?`: [StatusJobsPage](#fields-statusjobspage); `request_latencies?`: array of [RequestLatency](#fields-requestlatency); `reservations?`: array of [Reservation](#fields-reservation); `component_exclusions?`: array of [ComponentExclusion](#fields-componentexclusion); `device_maintenance?`: array of [DeviceMaintenance](#fields-devicemaintenance); `health_alert_delivery?`: [HealthAlertDelivery](#fields-healthalertdelivery); `health_alerts?`: array of [HealthAlert](#fields-healthalert); `queue_release`: [RunningRelease](#fields-runningrelease); `queue_capabilities?`: array of string; `fleet_upgrade?`: [FleetUpgradeStatus](#fields-fleetupgradestatus); `file_store?`: [FileStoreStatus](#fields-filestorestatus); `checkpoint_expiry?`: [CheckpointExpiryStatus](#fields-checkpointexpirystatus); `host_states?`: map of string; `admissions?`: array of [HostAdmissions](#fields-hostadmissions); `work_curves`: array of [WorkObservation](#fields-workobservation); `priority_policy`: [PriorityPolicyRecord](#fields-prioritypolicyrecord); `holds`: array of [Hold](#fields-hold); `now`: timestamp; `usage`: [UsageReport](#fields-usagereport); `loaded_models`: array of [ModelSlot](#fields-modelslot); `warning?`: string; `jobs`: array of [Job](#fields-job); `machines`: array of [Machine](#fields-machine) |
| <a id="fields-snapshotfreshness"></a>**SnapshotFreshness** | `captured_at`: timestamp; `served_at`: timestamp; `age_seconds`: number; `cached`: boolean |
| <a id="fields-spec"></a>**Spec** | `warm_pieces?`: string; `resident?`: [retiredField](#fields-retiredfield); `supersedes?`: string; `checkpoint_policy?`: string; `authoring_origin?`: string; `wall_time?`: string; `requirement_reasons?`: map of string; `quality_tolerance?`: map of number; `output_destination?`: [OutputDestination](#fields-outputdestination); `output_retention?`: string; `storage_protocol?`: string; `portable?`: boolean; `output_delivery?`: string; `isolation?`: string; `reason?`: string; `shared_inputs?`: map of [SharedInput](#fields-sharedinput); `model_snapshots?`: map of [ModelSnapshot](#fields-modelsnapshot); `prepare_source?`: boolean; `profile_memory?`: string; `diagnostics?`: [DiagnosticSpec](#fields-diagnosticspec); `keep_on_failure?`: array of [KeepFile](#fields-keepfile); `disk?`: [DiskNeeds](#fields-diskneeds); `after?`: [PieceDependency](#fields-piecedependency); `targets?`: array of [ExecutionTarget](#fields-executiontarget); `require_target?`: boolean; `fan_out?`: [FanOut](#fields-fanout); `workload?`: [Workload](#fields-workload); `submission_key?`: string; `files?`: map of [FileRef](#fields-fileref); `kind?`: string; `program?`: [Program](#fields-program); `args?`: array of string; `size_hint?`: [SizeHint](#fields-sizehint); `memory_limit_gb?`: number; `inputs?`: map of string; `outputs?`: map of string; `name`: string; `project`: string; `priority`: string; `class?`: string; `logs?`: string; `deadline?`: string; `run`: [RunSpec](#fields-runspec); `needs`: [ResourceNeeds](#fields-resourceneeds); `execution?`: [ExecutionSpec](#fields-executionspec); `extension?`: string; `extension_digest?`: string; `extension_check?`: [ExtensionCheckRequest](#fields-extensioncheckrequest); `environment?`: [Environment](#fields-environment); `excluded_machines?`: array of string; `allowed_machines?`: array of string; `start_by?`: string; `start_window?`: string; `give_up_at?`: string; `expected_minutes?`: number; `resumable`: boolean; `resume_compatibility?`: [ResumeCompatibility](#fields-resumecompatibility); `preemptible?`: boolean; `worker_data?`: string; `allow_cpu_fallback?`: boolean; `exclusive_device?`: boolean; `advanced?`: [Advanced](#fields-advanced) |
| <a id="fields-specprojection"></a>**SpecProjection** | `projected`: boolean; `omitted`: array of string |
| <a id="fields-stagingcause"></a>**StagingCause** | `stage?`: string; `path_class?`: string; `bytes_copied?`: number; `expected_bytes?`: number; `errno?`: number; `required_bytes?`: number; `available_bytes?`: number; `limit_bytes?`: number; `capacity?`: string; `input?`: string; `blob?`: string; `operation`: string; `error`: string; `syscall?`: string; `status?`: number |
| <a id="fields-stagingphasetiming"></a>**StagingPhaseTiming** | `name`: string; `calls`: number; `seconds`: number; `failures`: number; `http_4xx?`: number; `http_5xx?`: number; `expired?`: number |
| <a id="fields-stagingtimings"></a>**StagingTimings** | `started_at`: timestamp; `completed_at?`: string; `elapsed_seconds`: number; `cache_hits`: number; `cache_misses`: number; `evicted`: number; `phases`: array of [StagingPhaseTiming](#fields-stagingphasetiming) |
| <a id="fields-statusalertgroup"></a>**StatusAlertGroup** | `kind`: string; `count`: number; `since`: timestamp; `scopes?`: map of number |
| <a id="fields-statusholder"></a>**StatusHolder** | `job_id`: string; `name?`: string; `attempt`: number; `state`: string; `reason?`: string |
| <a id="fields-statusjobspage"></a>**StatusJobsPage** | `mode`: string; `total`: number; `returned`: number; `next_before?`: string |
| <a id="fields-statuslease"></a>**StatusLease** | `project`: string; `key`: string; `expires_at`: timestamp; `hosts`: array of string |
| <a id="fields-statusquiescence"></a>**StatusQuiescence** | `host`: string; `mode`: string; `attempts?`: array of [StatusHolder](#fields-statusholder); `other?`: array of string |
| <a id="fields-statussummary"></a>**StatusSummary** | `retained_jobs`: number; `alert_groups`: array of [StatusAlertGroup](#fields-statusalertgroup); `production_leases?`: array of [StatusLease](#fields-statuslease); `not_quiescent?`: array of [StatusQuiescence](#fields-statusquiescence); `diagnostics`: string |
| <a id="fields-storageallocation"></a>**StorageAllocation** | `id`: string; `execution`: [Execution](#fields-execution); `machine`: string; `session`: string; `backing`: string; `bytes`: number; `files`: number; `worker_custody`: boolean; `at`: timestamp |
| <a id="fields-storagekey"></a>**StorageKey** | `program`: string; `recipe`: string; `source`: string; `variant`: [ExecutionTarget](#fields-executiontarget); `configuration`: string; `environment`: string; `shape`: string; `input_shape`: string |
| <a id="fields-storageobservation"></a>**StorageObservation** | `key`: [StorageKey](#fields-storagekey); `staging`: [StorageRange](#fields-storagerange); `output`: [StorageRange](#fields-storagerange); `scratch`: [StorageRange](#fields-storagerange); `spool`: [StorageRange](#fields-storagerange); `checkpoint`: [StorageRange](#fields-storagerange); `metadata`: [StorageRange](#fields-storagerange); `managed`: [StorageRange](#fields-storagerange); `files`: [StorageRange](#fields-storagerange); `allocation_unit`: number; `recorded_at`: timestamp; `output_execution`: [Execution](#fields-execution); `output_recorded_at`: timestamp; `execution`: [Execution](#fields-execution); `family?`: string; `lower_bound?`: [AttemptStoragePeak](#fields-attemptstoragepeak); `lower_bound_execution?`: [Execution](#fields-execution); `lower_bound_reason?`: string; `lower_bound_at?`: timestamp |
| <a id="fields-storagepreflightfailure"></a>**StoragePreflightFailure** | `root`: string; `operation`: string; `errno`: number; `elapsed_ns`: number; `timed_out?`: boolean |
| <a id="fields-storagerange"></a>**StorageRange** | `samples`: number; `min`: number; `peak`: number; `positive_error`: number |
| <a id="fields-strandeddemand"></a>**StrandedDemand** | `job_id`: string; `project`: string; `host`: string; `platform`: string; `excluding_fields`: array of string; `memory_gb`: number; `cpu_cores`: number |
| <a id="fields-taskoutputcustody"></a>**TaskOutputCustody** | `state`: string; `allocation`: string; `files`: map of [FileRef](#fields-fileref); `stored_at`: timestamp; `collected_at?`: string; `collection_requested_at?`: string |
| <a id="fields-unavailableroot"></a>**UnavailableRoot** | `root`: string; `reason`: string |
| <a id="fields-usagereport"></a>**UsageReport** | `since`: timestamp; `until`: timestamp; `projects`: array of [ProjectUsage](#fields-projectusage); `machines`: array of [UsageSummary](#fields-usagesummary); `cards`: array of [CardUsage](#fields-cardusage); `daily`: array of [DailyUsage](#fields-dailyusage); `preemptions`: array of [Preemption](#fields-preemption); `production_starts`: array of [ProductionStart](#fields-productionstart) |
| <a id="fields-usagesummary"></a>**UsageSummary** | `name`: string; `gpu_hours`: number; `busy_percent`: number; `idle_hours`: number; `pinned_hours`: number |
| <a id="fields-utilizationreport"></a>**UtilizationReport** | `since`: timestamp; `through`: timestamp; `idle_card_hours_while_work_waited`: number; `busy_card_hours_while_work_waited`: number; `unknown_card_hours`: number; `cards`: array of [CardUtilization](#fields-cardutilization); `would_have_fit_at_measured_size`: array of [WouldFitRow](#fields-wouldfitrow); `coverage`: string |
| <a id="fields-varianteligibility"></a>**VariantEligibility** | `extension`: string; `digest`: string; `variant`: [ExecutionTarget](#fields-executiontarget); `host`: string; `device?`: string; `target`: [ExecutionTarget](#fields-executiontarget); `freshness`: string; `qualified`: boolean; `eligible`: boolean; `reasons`: array of string |
| <a id="fields-waitpreemption"></a>**WaitPreemption** | `source`: string; `considered`: boolean; `planned`: boolean; `reason`: string |
| <a id="fields-walltimestatus"></a>**WallTimeStatus** | `limit`: string; `expires_at?`: string; `remaining_seconds?`: number |
| <a id="fields-warmpiecedelivery"></a>**WarmPieceDelivery** | `key`: string; `sequence`: number; `attempt`: number; `offered_at`: timestamp; `started_at?`: string |
| <a id="fields-warmpieceprogress"></a>**WarmPieceProgress** | `delivery?`: [WarmPieceDelivery](#fields-warmpiecedelivery); `failures?`: map of [Execution](#fields-execution); `dependencies?`: map of [PieceBinding](#fields-piecebinding); `last_settled_at?`: string; `idle_since?`: string; `startup_seconds?`: number; `loads?`: number; `load_attempt?`: number; `load_seconds?`: number; `item_seconds?`: number; `between_items_seconds?`: number |
| <a id="fields-wastedattempt"></a>**WastedAttempt** | `resources`: [AttemptResources](#fields-attemptresources); `stop_observation`: [AttemptStopObservation](#fields-attemptstopobservation); `job_id`: string; `attempt`: number; `host`: string; `kind`: string; `ended_at`: timestamp; `started`: string; `burned_seconds`: number; `suspect`: boolean; `pressure`: [MemoryPressureEvidence](#fields-memorypressureevidence); `headroom_gb`: number; `io_status`: string |
| <a id="fields-wastedwindow"></a>**WastedWindow** | `name`: string; `since`: timestamp; `through`: timestamp; `attempts_by_kind`: map of number; `burned_attempt_seconds`: number; `attempts_with_unknown_burn`: number |
| <a id="fields-worklearningchange"></a>**WorkLearningChange** | `key`: [ObservationKey](#fields-observationkey); `before`: string; `after`: string |
| <a id="fields-workobservation"></a>**WorkObservation** | `memory_escalation?`: [MemoryEscalationPrior](#fields-memoryescalationprior); `gpu_host_range?`: [MemoryPeakRange](#fields-memorypeakrange); `device_range?`: [MemoryPeakRange](#fields-memorypeakrange); `memory_range?`: [MemoryPeakRange](#fields-memorypeakrange); `memory_layout?`: string; `memory_configuration?`: string; `memory_family?`: string; `memory_samples?`: number; `memory_recorded_at?`: timestamp; `memory_peak?`: [AttemptMemoryPeak](#fields-attemptmemorypeak); `sample_id?`: string; `last_seconds?`: number; `forbidden?`: boolean; `stale_reason?`: string; `source?`: string; `key`: [ObservationKey](#fields-observationkey); `samples`: number; `recorded_at`: timestamp; `job`: string; `machine`: string; `session`: string; `attempt`: number |
| <a id="fields-workreference"></a>**WorkReference** | `input_shape`: string; `unit`: string; `units`: number |
| <a id="fields-worktiming"></a>**WorkTiming** | `history_reason?`: string; `placement_reason?`: string; `execution`: [Execution](#fields-execution); `machine`: string; `session`: string; `key?`: [ObservationKey](#fields-observationkey); `last_accepted_seconds?`: number; `predicted_seconds?`: number; `actual_seconds?`: number; `reason?`: string; `samples`: number |
| <a id="fields-workeradoptionproof"></a>**WorkerAdoptionProof** | `protocol`: number; `attempts`: array of [Execution](#fields-execution); `reason?`: string |
| <a id="fields-workerdatacleanup"></a>**WorkerDataCleanup** | `progress?`: number; `id`: string; `attempt`: number; `machine`: string; `session`: string; `state`: string; `result_accepted`: boolean; `checked_at`: timestamp; `paths?`: array of [WorkerDataRemoval](#fields-workerdataremoval) |
| <a id="fields-workerdataremoval"></a>**WorkerDataRemoval** | `path`: string; `bytes`: number; `removed`: boolean; `error?`: string |
| <a id="fields-workerhygienecollection"></a>**WorkerHygieneCollection** | `disk_budget?`: array of [DiskBudgetVolume](#fields-diskbudgetvolume); `scratch?`: [AttemptFilesCollection](#fields-attemptfilescollection); `compiler_warning?`: string; `unmanaged_compiler_bytes?`: number; `at`: timestamp; `next_at`: timestamp; `file_cache`: [CacheCollection](#fields-cachecollection); `environments`: [CacheCollection](#fields-cachecollection); `staging_error?`: string |
| <a id="fields-workload"></a>**Workload** | `input_shape`: string; `units`: number |
| <a id="fields-wouldfitrow"></a>**WouldFitRow** | `blocker`: string; `job_episodes`: number; `job_hours`: number |
| <a id="fields-yieldpause"></a>**YieldPause** | `progress?`: number; `id`: string; `attempt`: number; `pause`: [JobPause](#fields-jobpause) |
| <a id="fields-efficiencyrate"></a>**efficiencyRate** | `release?`: string; `reserved_device_gb`: number; `measured_device_gb`: number; `device_valid_until`: timestamp; `host?`: string; `basis?`: string; `reserved_memory_gb`: number; `reserved_cpu_cores`: number; `measured_memory_gb`: number; `valid_until`: timestamp |
| <a id="fields-memoryestimateevidence"></a>**memoryEstimateEvidence** | `level`: string; `runs`: number |
| <a id="fields-memorypoolestimate"></a>**memoryPoolEstimate** | `typical_gb`: number; `charge_gb`: number; `evidence`: [memoryEstimateEvidence](#fields-memoryestimateevidence); `alone?`: boolean; `held_gb?`: number; `peak_gb?`: number; `learned?`: boolean |
| <a id="fields-memoryrunpeaks"></a>**memoryRunPeaks** | `ram`: [memoryRunPool](#fields-memoryrunpool); `card`: [memoryRunPool](#fields-memoryrunpool) |
| <a id="fields-memoryrunpool"></a>**memoryRunPool** | `peak_gb`: number; `complete`: boolean |
| <a id="fields-memoryshadowhost"></a>**memoryShadowHost** | `worker`: string; `report`: [MemoryShadowReport](#fields-memoryshadowreport) |
| <a id="fields-memorystopsample"></a>**memoryStopSample** | `expected_gb`: number; `peak_gb`: number; `owners_gb`: number; `owners_peak_gb`: number |
| <a id="fields-probeestimate"></a>**probeEstimate** | `mean`: number; `deviation`: number |
| <a id="fields-retiredfield"></a>**retiredField** |  |
| <a id="fields-sharedpressurelearning"></a>**sharedPressureLearning** | `cpu_known?`: boolean; `cpu`: [probeEstimate](#fields-probeestimate); `wake`: [probeEstimate](#fields-probeestimate); `psi`: [probeEstimate](#fields-probeestimate); `at`: timestamp |
| <a id="fields-workerpublicationtimings"></a>**workerPublicationTimings** | `result_ready_to_release_ack_seconds?`: number; `manifest_seconds`: number; `upload_create_seconds`: number; `upload_patch_seconds`: number; `upload_window_seconds`: number; `upload_creates`: number; `upload_patches`: number; `published_bytes`: number |

<!-- END GENERATED STATUS FIELDS -->

## Behavior reference

The original section anchors remain here for existing links. Follow each link
for its explanation; add field names to the types, rather than this index.

<a id="reading-myce-status"></a>
- [Reading `myce status`](reference/status-behavior.md#reading-myce-status)

<a id="automatic-host-health-834"></a>
- [Automatic host health (#834)](reference/status-behavior.md#automatic-host-health-834)

<a id="retained-queue-measurement-1120"></a>
- [Retained-queue measurement (#1120)](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/reference/status-behavior.md#retained-queue-measurement-1120)

<a id="positive-execution-evidence-1008"></a>
- [Positive execution evidence (#1008)](reference/status-behavior.md#positive-execution-evidence-1008)

<a id="validation-reservation-status"></a>
- [Validation reservation status](reference/status-behavior.md#validation-reservation-status)

<a id="queue-host-maintenance-status"></a>
- [Queue host maintenance status](reference/status-behavior.md#queue-host-maintenance-status)

<a id="live-headroom-and-reservations"></a>
- [Live headroom and reservations](reference/status-behavior.md#live-headroom-and-reservations)

<a id="api-request-latency"></a>
- [API request latency](reference/status-behavior.md#api-request-latency)

<a id="non-mycelium-ram-history"></a>
- [Non-Mycelium RAM history](reference/status-behavior.md#non-mycelium-ram-history)

<a id="suggesting-a-host-allowance"></a>
- [Suggesting a host allowance](reference/status-behavior.md#suggesting-a-host-allowance)

<a id="shared-interactive-profile"></a>
- [Shared interactive profile](reference/status-behavior.md#shared-interactive-profile)

<a id="execution-elapsed-time-last-accepted-duration-and-application-progress"></a>
- [Execution elapsed time, last accepted duration, and application progress](reference/status-behavior.md#execution-elapsed-time-last-accepted-duration-and-application-progress)

<a id="authored-intent-and-retained-placement"></a>
- [Authored intent and retained placement](reference/status-behavior.md#authored-intent-and-retained-placement)

<a id="per-copy-execution-intervals"></a>
- [Per-copy execution intervals](reference/status-behavior.md#per-copy-execution-intervals)

<a id="learned-storage-evidence"></a>
- [Learned storage evidence](reference/status-behavior.md#learned-storage-evidence)

<a id="deadline-priority-guidance"></a>
- [Deadline priority guidance](reference/status-behavior.md#deadline-priority-guidance)

<a id="configured-nas-mount-availability"></a>
- [Configured NAS mount availability](reference/status-behavior.md#configured-nas-mount-availability)

<a id="incident-health-alerts"></a>
- [Incident health alerts](reference/status-behavior.md#incident-health-alerts)

<a id="health-notifier-hook-contract"></a>
- [Health notifier hook contract](reference/status-behavior.md#health-notifier-hook-contract)


<a id="local-completion-observer-diagnostics"></a>
- [Local completion observer diagnostics](reference/status-behavior.md#local-completion-observer-diagnostics)

<a id="batched-waits-and-consumer-flood-protection"></a>
- [Batched waits and consumer flood protection](reference/status-behavior.md#batched-waits-and-consumer-flood-protection)

<a id="job-get-request-attribution"></a>
- [Job GET request attribution](reference/status-behavior.md#job-get-request-attribution)

<a id="variant-eligibility-499"></a>
- [Variant eligibility (#499)](reference/status-behavior.md#variant-eligibility-499)

<a id="read-only-host-pin-audit-499"></a>
- [Read-only host-pin audit (#499)](reference/status-behavior.md#read-only-host-pin-audit-499)

<a id="authored-requirement-explanations-748"></a>
- [Authored requirement explanations (#748)](reference/status-behavior.md#authored-requirement-explanations-748)

<a id="one-hold-record"></a>
- [One hold record](reference/status-behavior.md#one-hold-record)

<a id="job-findings"></a>
- [Job findings](reference/status-behavior.md#job-findings)

<a id="read-only-consumer-project-doctor"></a>
- [Read-only consumer project doctor](reference/status-behavior.md#read-only-consumer-project-doctor)

<a id="worker-cleanup-834"></a>
- [Worker cleanup (#834)](reference/status-behavior.md#worker-cleanup-834)

<a id="installed-executable-versus-running-image-824"></a>
- [Installed executable versus running image (#824)](reference/status-behavior.md#installed-executable-versus-running-image-824)

<a id="adaptive-fair-share-history-813"></a>
- [Adaptive fair-share history (#813)](reference/status-behavior.md#adaptive-fair-share-history-813)

<a id="learned-native-cpu-memory-growth"></a>
- [Learned native CPU memory growth](reference/status-behavior.md#learned-native-cpu-memory-growth)

<a id="queue-efficiency-857"></a>
- [Queue efficiency (#857)](reference/status-behavior.md#queue-efficiency-857)

<a id="adaptive-evidence-budgets-811"></a>
- [Adaptive evidence budgets (#811)](reference/status-behavior.md#adaptive-evidence-budgets-811)

<a id="launch-capacity-waits-1011"></a>
- [Launch capacity waits (#1011)](reference/status-behavior.md#launch-capacity-waits-1011)

<a id="committed-job-reads-under-queue-load-1052"></a>
- [Committed job reads under queue load (#1052)](reference/status-behavior.md#committed-job-reads-under-queue-load-1052)

<a id="retained-production-admission-waits-868"></a>
- [Retained production admission waits (#868)](reference/status-behavior.md#retained-production-admission-waits-868)

<a id="actual-all-tier-admission-decisions-1158"></a>
- [Actual all-tier admission decisions (#1158)](reference/status-behavior.md#actual-all-tier-admission-decisions-1158)

<a id="accelerator-placement-history-1210"></a>
- [Accelerator placement history (#1210)](reference/status-behavior.md#accelerator-placement-history-1210)

<a id="fault-evidence"></a>
- [Fault evidence](reference/status-behavior.md#fault-evidence)

<a id="shared-owner-yield-and-measured-cpu-389"></a>
- [Shared owner yield and measured CPU (#389)](reference/status-behavior.md#shared-owner-yield-and-measured-cpu-389)

## File-store capacity refusals

HTTP 507 keeps the public `code: file_store_capacity`. Supporting queues add
`cause`, an ordinary `RuntimeCause` with `operation: outputs`, `class: http_error`,
a stable `predicate`, and `file_capacity` evidence.
Folder/glob preflight preserves its terminal `limit_exceeded` class while
retaining the same predicate, evidence and HTTP status. The same cause is journalled
as `event=file_store_capacity cause=...` and retained by worker finalization in
`execution_cause` (or `cleanup_cause` when execution already failed). Job JSON,
attempt diagnostics and `myce status` preserve it. Older queues/clients may omit
it; absence is unknown. A 507 alone does not establish whether production fits.

| Predicate | Refusal-time comparison |
| --- | --- |
| `file_store_backing` | `need_bytes > free_bytes`; `bound` is `store_allowance` or `physical_volume` when measured. |
| `file_store_production_reserve` | `need_bytes > available_bytes`, after subtracting the uncommitted production reserve from `free_bytes`. |
| `file_store_project_reservation` | `need_bytes > available_bytes`, after any non-production withholding and then aggregate unused project input reservations. Exact worker outputs remain exempt. |

Every byte value is an unsigned count. `growth_bytes` is the new requested
allocation after identity deduplication; `need_bytes` also includes
`output_commitment_bytes` for other missing outputs, deduplicated against the
request, existing uploads and each other. `free_bytes` is the effective backing
headroom before reserve withholding. `store_free_bytes` is the store allowance
minus allocated declared lengths; `volume_free_bytes` is `disk_free_bytes`
minus `disk_floor_bytes` and the outstanding unwritten
`upload_commitment_bytes`, saturating at zero. Backing headroom is the minimum
of those two bounds. `store_limit_bytes` and `stored_bytes` retain the allowance
and physically stored payload bytes. Optional measurements remain omitted when
unknown; measured zero is explicit.

`production_reserve_bytes` and `production_committed_bytes` explain the
uncommitted reserve. For non-production, `production_withheld_bytes` is their
saturating difference; for production it is zero. Reserve withholding is applied
only after the backing check passes. `project_withheld_bytes` is the aggregate
unused entitlement applied by the project arm. Each subtraction saturates at
zero, producing `available_bytes`. No per-project identities, limits or usage
breakdown enter this evidence or its public status projection.

`production` records the effective classification, including the legacy
unidentified-write exception; `identified` distinguishes that exception when
known. `observed_at` uses the queue's injected clock at refusal; `job_id` and
`attempt` identify an exact worker request when available. `request_id` correlates
the response and journal event (a sanitized supplied request ID or a generated
one). Input requests without an attempt omit `job_id`.

These are immutable historical operands, not a fresh capacity reading. Later
collection or free-space changes do not update them. A successful finalization
retry clears its pending cause through ordinary completion. Retry only when the
binding shortage has been relieved; changing the submission key does not free
storage or bypass protected reservations.
