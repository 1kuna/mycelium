# Seeing your mycelium

Run `myce status` using your usual queue address and private token. `--full`
shows full job IDs. Open the queue address in a browser for the same information;
sign in as `myce`, using the token as the password. The page is read-only and
refreshes every five seconds. Keep tokens in private configuration, not URLs.

The page and `GET /api/status` share a snapshot for up to one second. Job receipt
polls (`GET /api/jobs/{id}`) read the requested job afresh. Status snapshots do
not drive start authorization, cancellation or result publication.
Administrative tools use uncached `GET /api/inspection` for migration custody,
local extension targeting and measurement safety checks.

Each hypha shows its capability profile: CPU count, system memory, accelerator
memory, shared or separate memory, and local storage. Missing measurements say
unknown. Extension reports say which kind of work or program is qualified on
that hypha; having hardware alone does not make an extension qualified.

Worker `disk_volumes` in `GET /api/status` contains `free_bytes`, `total_bytes`,
worker-clock `sampled_at`, `sample_interval_seconds`, `age_seconds`, and signed
`change_bytes_per_hour` (positive means free space is growing). The rate uses
successive worker samples and is omitted until two readings exist. `roots`
identifies `data`, `environment`, `checkpoint`, `file-cache`, and `model-cache`;
roots on the same filesystem share an entry. An opaque `id` identifies that
host's volume; no paths or raw filesystem IDs are published.
The [disk-volume schema](schemas/disk-volume-v1.schema.json) describes each entry.
Old workers without samples retain startup storage figures.

Sampling uses at most five bounded observations per heartbeat, outside the queue.
Local probes use the existing 25 ms budget; the checkpoint/NAS probe uses the
existing 750 ms heartbeat filesystem budget. A timeout retains the last good
reading with its age and `retained: true`, and reports `error: disk reading
unavailable`. Retention preserves the baseline for later rate calculation. The
queue can use that reading for three observation cycles plus the 850 ms sampling
budget, since the worker timestamps before probing. At a one-second heartbeat,
the freshness window is 3.85 seconds. A cycle is the reported
heartbeat interval, with a minimum of 850 ms from four local probe budgets plus
one NAS probe budget. Faster control ticks cannot expire a healthy reading
during bounded observation and start handoff. Freshness is
worker-reported `age_seconds` plus queue time since `disk_received_at`, which the
queue stamps on check-in; worker and queue clocks need not agree. Older or
missing readings hold new work as `disk reading unavailable`, without inventing
zero free bytes. The CLI shows retained values and age. A status read also
updates displayed age without renewing freshness or writing queue state.

New work stays queued with `diagnostic_code: disk_headroom` when a volume cannot
retain its free floor plus the job's projected need. The diagnostic names the
host, roots, free bytes, floor and need. Admission uses observed free bytes and
does not subtract other jobs' projected writes: writes already made are included
in the observation. One machine worker admits work per physical host; its
component targets share the observations. Dispatch applies this check. The
launch and Started fences refuse only a current or retained reading below the
free floor; an expired reading does not refuse there, since need was admitted at
dispatch and staged inputs already appear in free space. Recovery of an
already-running copy bypasses admission-time disk checks.
Queued production work can move off a disk-held host to another fitting host,
and its pinned-host disk reason remains visible while it waits. Disk pressure
does not trigger preemption: stopping work does not free its files.

The floor is 1% of volume size with a 256 MiB minimum for filesystem metadata,
control state and other writers; there is no job-size cap. Submission preserves
reported total capacity separately from current free space. When every reporting
eligible target is too small even with all capacity free, submission refuses with
typed `disk_headroom`. Unknown capacity does not prove permanent impossibility;
a shortage of current free space remains a temporary hold.

Input manifests and measured attempt `output_refs` supply projected bytes.
Optional `spec.disk.input_bytes` / `output_bytes` are fallback hints when sizes
are absent or unknown. Nothing requires a declaration; without size evidence
admission uses the floor alone. Inputs and outputs charge the data volume; the
immutable input cache copy charges `file-cache`; NAS/path inputs bypass that cache.
Shared volumes sum those writes once per
role. Checkpoint writes charge only declared resumable/checkpoint work, using a
measured resume manifest or optional `spec.disk.checkpoint_bytes` hint. A separate
environment volume checks the floor when the job uses it. New engine/model loads
check the model-cache volume for the known model size plus its floor when files
need copying. Calls to an already-ready slot, and pinned-engine restarts whose
files are cached or read in place, do not run disk admission checks. The existing
cache copy/eviction checks still govern actual model downloads.

The NAS checkpoint volume is shared across hosts. Each host's observation can
admit concurrent checkpoint writes against the same free-space figure. A host
with multiple GPUs can also admit several jobs before their writes appear in a
sample. Both are accepted limits of admission from observations without disk
reservations. Estimates do not predict future writes by other jobs or programs,
or unreported scratch/archive expansion. Cross-job output-size learning remains
a follow-up; existing observation keys learn memory/throughput, not disk size.

Machine `cpu_isolation` reports `select-none` after runtime qualification,
`not-enforced` for cooperative CPU execution on macOS/Metal, or
`cpu_accelerator_isolation_unavailable` while qualification is unavailable.
Metal has no device-masking environment equivalent; a CPU request is a resource
and execution target, not a guarantee that its command cannot use Metal.
CUDA/XPU select-none controls remain required. CPU/RAM accounting is unchanged.
Unqualified CPU targets are withheld before placement, with the same typed job
`diagnostic_code`. If qualification changes before launch, the job is requeued
without consuming infrastructure retries and the host's CPU readiness is withdrawn.

Availability explains owner use, battery power, heat, missing readings,
disabling, leaving, and loss of contact. An available hypha can still be busy.
Running jobs show their hypha and attempt. Every waiting job has an explanation,
including missing qualification, memory or resource requirements, unavailable
hyphae, software preparation, and work ahead in the queue.

The page and CLI use the queue's decisions. Reading status does not choose where
to run work. The status API returns job summaries without submitted commands,
environment values, program arguments, extension files, or local checkpoint
paths. `GET /api/status`, the browser page, CLI `status` (including
`status JOB`), and Python `Client.list()` also omit `spec.submission_key`. This list redaction does not
apply to Job replies: `POST /api/jobs` and `GET /api/jobs/{id}` echo
`spec.project` and `spec.submission_key` exactly. Go `Submit`, `GetJob`, `Wait`
and Python `submit`, `get`, `wait` return these Job replies. GET replies keep
the status view's payload and diagnostic redaction, except for the key echo;
POST replies retain the submitted spec. Unkeyed jobs omit the optional key.
Use the existing job output and log commands when you need their results.

`myce status JOB` (full ID or an unambiguous prefix of at least eight characters)
shows that job and its `attempt_history`. Put `--full` before the job ID:
`myce status --full JOB`. The same optional array is included
in `GET /api/status`, the [job-v1 reply](schemas/job-v1.schema.json), and Python
`Client.get(job_id)`. Each entry has exact `attempt`, `machine`, `session`,
queue-clock `ended_at`, the resulting queue `state` (`queued` for a requeue),
and a diagnostic `reason`. Optional `execution_cause` and `cleanup_cause` use
the existing safe runtime operation/class and command exit code.

History preserves the existing outcome classification: `launch` causes include
`launch_refused`, `launch_failed` and `environment_failed`; `polite_stop` marks
availability withdrawal or a queue-recorded planned stop (leaving or making
room), `operator_stop` marks operator/host maintenance,
and `priority_stop` retains priority preemption provenance. `interrupted` without
those planned-stop flags or a launch cause records infrastructure interruption
or loss. `out_of_memory` distinguishes OOM, with `state: queued` when retried;
terminal command failures retain `command_exit` and `exit_code`. These are
diagnostics, not retry charges: two starts and zero losses can include a planned
requeue followed by one command failure. A launch refusal without an availability
flag is still charged a loss under the existing retry rules.

Launch-gate transport failures use `execution_cause.operation: start_authorization`;
the Started hand-off uses `start_handoff`. Both retain the underlying safe class
(for example `timeout`). An interrupted result
uses that cause in its reason, without implying a worker restart. Actual fresh
worker-session recovery retains its separate restart diagnostic. Authorization
failures still consume the existing infrastructure retry/loss budget, except
when the queue's start fence refused placement: that attempt requeues without a
loss or retry charge. The queue log line `event=launch_placement_rejected`
names the failed `predicate` (`disk_floor`, `machine_unavailable`,
`preparation_stopping`, `execution_target`, `cpu_fit` or `placement_policy`).

History is persisted with the job and retains the **latest four ended attempts**:
three infrastructure endings (two production retries, or three counted losses),
plus the existing single OOM retry. Repeated planned requeues can exceed that
window; older entries roll off without changing execution or charging limits.
Existing jobs start with no history; past causes cannot be reconstructed.
An end time records the queue's decision, which can precede physical cleanup.
History never proves release; use the existing exact release evidence.

`GET /api/jobs/{id}` also returns optional `memory_attempts`, available as Go
`GetJob(...).MemoryAttempts` and Python `Client.get(id)["memory_attempts"]`.
Each entry binds `attempt`, `machine`, and `session` to its observed
`memory_peak` and resolved `memory_envelope`. These facts persist with the job
independently of learning, including a constrained first run with no comparable
learned point. Retention is bounded to 32 rows: the first and the most recent
31. Optional `memory_attempts_dropped` counts evicted rows (omitted zero), so
missing intermediate facts are visible. Attempts that never launched and have
no peak do not create rows or increment the dropped count. The four-entry
`attempt_history` window is independent. Old jobs cannot reconstruct missing
telemetry. Launched attempts ending without a worker reading retain the
envelope and omit the peak. Release-only kill-time acknowledgements retain
their measured facts too.

Peak fields `host_gb`, `cgroup_gb`, and `device_gb` identify the measured scopes;
`swap_gb` is the Linux attempt parent's `memory.swap.peak`, when exposed.
All sizes are GiB. The parent covers native or managed Docker main and sidecars;
device memory is process-scoped. `events` holds hierarchical `memory.events`
counts `high`, `max`, `oom`, `oom_kill`, and `oom_group_kill`, retaining the
maximum per counter across the attempt's samples. Absent readings mean unknown;
explicit zeros mean read zeros. `constrained` is omitted when false. Darwin
process-group RSS has its unenforced `source` label and omits cgroup events and
swap peaks. A zero-OOM/swap/max-hit claim requires those readings to be present
and zero; an absent reading cannot support that claim. These observations do
not imply acceptance into learning or prove execution release.

Production job summaries and the [job-v1 reply](schemas/job-v1.schema.json) may
include `infrastructure_retries`, the durable number of infrastructure requeues
already used. It is an optional nonnegative integer, omitted at zero, with a
queue-configurable default limit of 2. Exhaustion reports
`infrastructure_retries_exhausted`; stage failures do not consume this budget.

For maintenance gates, every nonterminal job in redacted `GET /api/status`
retains `spec.allowed_machines` (machine names) and reports:

- `eligible_hosts`: sorted, unique host IDs of currently known machines allowed
  by both the job and project allow-lists. For running and stopping jobs, including
  in-flight model calls, this also includes the current machine's host and
  `production_host` when set, even if policy no longer allows them. A restricted
  job with no known eligible or occupied host reports an empty array.
- `eligible_unknown_machines`: sorted, unique machine names in the effective
  allow-list that are not currently known machines. The effective list is the intersection
  when both job and project lists are nonempty, or whichever list is nonempty.
  This field is omitted when there are no unknown names. A joining machine
  resolves its name into a host ID on subsequent status reads.
- `eligible_unrestricted: true`: neither allow-list restricts the job. Treat it
  as able to run anywhere, including future hosts. `eligible_hosts` is omitted
  unless the job currently occupies a host; listed hosts never limit this marker.

These fields cover queued, infra-requeued, running and stopping jobs. They
express placement policy and current occupancy, not dispatch readiness: busy,
disabled, offline or temporarily unavailable machines remain policy candidates.
Workers normally default `HostID` to the OS hostname. The queue uses a machine-ID
fallback only when a check-in has no host ID. Gates must match the exact `host_id`
strings workers report, rather than assuming host IDs equal machine names.

Refresh status before each decision. Count a job as the protected host's work if
any of these apply:

- `eligible_hosts` contains that host ID;
- `eligible_unrestricted` is true;
- `eligible_unknown_machines` is nonempty, unless the gate can map all those names
  to other hosts;
- `spec.allowed_machines` names a worker on that host.

Missing/invalid eligibility fields and unreadable status must fail closed. Also
check current assignments and release/hold state before maintenance: policy
eligibility alone proves no release of existing work. Loaded model servers
appear under `loaded_models`, not jobs, and must be checked separately. Terminal
jobs omit all three derived eligibility fields.

Host gates should read `holds_detail`, an array (empty when there are no holds)
whose entries have `host`, `kind`, `operator_action_required`, `text` and an
optional `clears_at` timestamp. Entries are sorted by host and kind; a host can
have several holds at once. Do not match English text to choose an action.

| `kind` | `operator_action_required` | `clears_at` | When emitted |
| --- | --- | --- | --- |
| `production_hold` | false | omitted | Active production demand, including work between stages. |
| `production_stopping` | false | omitted | Lower-priority work is within its stop grace. |
| `production_release_unknown` | conditional | omitted | Production reservation has an unconfirmed release. True only for a gone/missing worker, failed cleanup, or release still unknown at least two minutes after grace. Restart check-in grace suppresses the time-based flag. Draining calls, recovery and the short wait for a kill result are false. |
| `docker_transition` | false | omitted | Recognized Docker startup/stop operation or final release report in progress. |
| `docker_cleanup_waiting` | false | omitted | Retained Docker custody or a `cleanup unconfirmed:`-prefixed status awaits confirmation, before escalation. |
| `docker_cleanup_operator_action` | true | omitted | Docker custody reached the cleanup escalation bound (`myce resolve-docker`). |
| `startup_release` | conditional | omitted | Failed startup awaits exact execution release; true when any corresponding worker is gone/missing. |
| `host_disabled` | false | omitted | Queue host maintenance is drained/disabled. Does not clear by itself; not a fault. A machine's `disabled` field is a status projection of this queue policy. |
| `unclassified` | true | omitted | A production host blocker has no recognized explicit classification, or a worker reports an unrecognized Docker status (for example, `pull pending`). Never treated as self-clearing. |

There is no idle production cooldown. `clears_at` is optional; refresh before acting. A false
operator flag means no fault currently demands operator attention, not that
release is proven or that recovery has a deadline. In particular, deliberate
Queue admission stays drained/disabled until explicit resume. Cleanup acknowledgements may clear an
escalated hold without an operator override; custody remains fenced until proven.

Docker summaries choose deterministically by severity: escalation, unclassified
status, reported cleanup error, waiting release, then recognized transition.
Equal severity uses lexical text order. A transition cannot hide waiting custody.
Exact current executions that are healthy and starting or running do not accrue cleanup time;
becoming live again clears stale timers. Stopped attempts escalate two minutes
**after stop grace expires**. Lost/unconfirmed custody without a stop deadline
starts its two-minute window when first observed. All decisions use the queue's
injected clock. Once escalated, a later stop request preserves operator attention
and does not emit a second escalation event.

Known limits: a recovering model slot without `StopAt` has no time-based
escalation deadline while its worker remains present. A deliberate disabled-host
hold stays non-fault (`operator_action_required: false`) even if its disabled
worker later dies. Check worker liveness and model release evidence separately;
these holds alone do not prove release or eventual recovery.

For run monitoring, distinguish routine reservations and transitions from
operator holds using these fields, including the target host. For maintenance,
a hold's automatic recovery does not make the host safe: wait for relevant holds
to clear and verify assignments, loaded models and attempt release evidence.
Treat unreadable status, missing/invalid details, or unknown kinds as unresolved.
An empty hold list alone does not prove a specific attempt has released custody.

The compatibility field `host_holds` remains a list of the existing strings and
is **omitted when empty**. `text` equals the corresponding existing string; the
additional startup-release and disabled-host entries use the queue's existing
fence messages and appear only in `holds_detail`.

The CLI and page label the effective host allowance **last reported**, including
stopped workers, with `memory_allowance_bytes` and UNQUALIFIED qualification.
The durable queue policy supplies it; worker reports must agree before admission.
Worker memory availability and usage remain last-reported facts.
Read the queue-owned allowance with `myce host status HOST`. Configure it while
drained/disabled with `myce host budget HOST --memory-budget-gb GiB`.

## Queue host maintenance status

`myce host status HOST` prints authenticated `GET /api/hosts/HOST/maintenance`.
`POST` to the same endpoint accepts `action` (`drain`, `disable`, `resume`,
`budget`), optional `stop` (disable only) and `memory_budget_gb` (positive, only
while drained/disabled). Host identity is the physical `host_id`, never a device
or session selector. Job/project placement restrictions still apply.

The reply has `host`, durable `policy` (`mode`: enabled/drained/disabled,
`memory_budget`, `updated_at`), `local_fence`, `workers` (machine/device report
and `fresh`), exact `attempts`, `calls`, `preparations`, `sidecars`, `pending_releases`,
`pending_cleanup`, `memory_in_use_gb`, `memory_debt_gb`, `healthy` and `quiescent`.
Workers and releases retain missing/unreported members. `healthy` needs all known
workers fresh with consistent budget and usable memory and inventory reports.
`quiescent` needs fresh worker custody reports, drained/disabled policy and every
work/cleanup count zero. Degraded inventory or memory probes can leave `healthy`
false while `quiescent` is true, allowing `disable --stop --wait` to complete.
Sidecars are conservatively counted until their exact attempt is released.
Plain status exits nonzero when worker health is incomplete; mutations exit
zero on durable acceptance. Queue maintenance status has no privileged
local-exclusion verdict.

The optional wait reads this status until quiescent or the injected deadline.
Timeout and queue outage fail without changing policy or release custody.
Every host command uses queue evidence; there is no local envelope or rest verdict.

### Live headroom and reservations

`myce status --json` returns the status snapshot as JSON; with a job ID it
returns that job's status view. `host_available_memory_gb` is worker-observed
available RAM after its configured host reserve. `host_memory_in_use_gb` is
Mycelium's reservation ledger, not measured process RSS.
`free_host_memory_gb` is admitted headroom after reservations and any configured
allowance. Similarly, `cpu_in_use_cores` and `free_cpu_cores` are reservations,
not CPU utilization. Zero reserved cores does not establish an idle host.

A queued job whose other resource checks fit but whose host RAM does not fit
shows `diagnostic_code: host_memory_headroom`. Its reason names the machine,
required RAM including slack, live available RAM, reservations and admitted
headroom; unavailable telemetry is reported as unavailable. Dispatch and the
worker's immediate launch recheck compare headroom with the same declared RAM
need and slack. The worker rechecks live RAM because other programs can consume
it after dispatch. No additional RAM ceiling or pressure threshold is introduced.

The runtime does not currently report an OS pressure verdict (Linux PSI or
macOS memory pressure), live CPU utilization, or swap rate: treat those
measurements as **unknown**, never as healthy or zero. The controlled RAM
headroom regression proves admission/status agreement for the existing RAM
signal, not complete OS-pressure coverage. Native pressure qualification remains
separate; see [the native checklist](production-test-plan.md#pressure-and-status-agreement).
Status is diagnostic and never permission to launch work locally; submit it.
