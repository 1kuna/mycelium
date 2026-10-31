# Seeing your mycelium

Run `myce status` using your usual queue address and private token. `--full`
shows full job IDs. Open the queue address in a browser for the same information;
sign in as `myce`, using the token as the password. The page is read-only and
refreshes every five seconds. Keep tokens in private configuration, not URLs.

The page and `GET /api/status` share a snapshot for up to one second. Job receipt
polls (`GET /api/jobs/{id}`) read the requested job afresh. Status snapshots do
not drive start authorization, cancellation or result publication.

Default status keeps the existing JSON fields and job shape, but includes all
nonterminal jobs and terminal jobs with pending cleanup/delivery, plus the newest
100 other terminal jobs by submission time. Work curves remain included for
existing consumer audits and release-preservation guards.
This bounds retained-history detail without hiding control-plane custody.
`jobs_page` ([schema](schemas/status-jobs-page-v1.schema.json)) reports the
filtered `total`, `returned`, `mode`, and optional `next_before` job ID.

- `GET /api/status?jobs=active` returns all active/cleanup-pending jobs.
- `?jobs=all&limit=100` pages all history, newest submission first; append
  `&before=NEXT_BEFORE` from each reply until that field is absent. `limit` is
  1–1000 and defaults to 100. The normal status presentation order is retained
  within each page; use the returned cursor rather than the last displayed ID.
- `?project=example&since=2026-10-30T00:00:00Z` filters jobs by project and
  inclusive submission timestamp. Filters do not alter fleet health, eligibility
  or queue order. Keep filters and mode unchanged when paging. Cursors require
  their job to remain retained; an expired/unknown cursor returns HTTP 400
  `status_query_invalid`, so restart that listing.
- `?detail=full` restores all retained jobs and work curves without a job limit.
  CLI `myce status --history` and Python `client.status(full=True)` use this
  supported expensive view. `--full` still means full job IDs.

Python `client.status()` uses the bounded default; `client.list()` walks bounded
`jobs=all` pages and retains its all-jobs contract. Both also work with older
queues, which ignore the new query parameters and return their legacy snapshot.
Old clients can decode the new status shape, but their list/status views now
contain the bounded history. Upgrade the CLI for access to older historical IDs:
`myce status JOB` and `myce logs JOB --follow` read the job endpoint directly,
including prefix resolution, independently of aggregate status retention/cache.
Logs follow polls only the selected job and drains a final log page on completion.

Release-preservation tools should request `GET /api/status?detail=full` (or
Python `client.status(full=True)`) for both snapshots: curve provenance can
depend on completed jobs outside the default history window. Curves remain in
default status for existing callers, but that view is not a complete job ledger.

Only the unfiltered default shares the one-second cache. Filtered/full reads are
fresh. Snapshot capture keeps scheduling inputs and dependency state under the
queue lock; projection, usage queries, file inventory, encoding the reply and
compression run after release. No bulk file or network I/O runs under `q.mu`.

Jobs with explicitly recovered outputs expose an append-only `recovered_results`
list in status and job GET. Each record has `kind: recovered`, its own result ID,
exact failed execution, recovery-time verification identities and operator
provenance. CLI status lists these separately and keeps the original failed
state. Ordinary `output_refs`, attempt outcomes, cleanup causes and dependency
eligibility remain unchanged. See [recover failed-attempt outputs](guide-files-v3.md#recover-outputs-from-a-failed-terminal-attempt)
for the explicit recovery/download commands and independent byte retention.

Job GET and status job rows expose a privacy projection in `spec`, not the
submitted spec. They include `spec_projection: {"projected": true, "omitted": [...]}`.
`omitted` lists paths relative to `spec` that the projection strips, regardless
of whether those fields were submitted. In particular, absent `spec.files`
**does not mean no files were submitted**. The same metadata is available through
Python `Client.get()` and `Client.list()`.

The projection strips these entire fields: `after`, `args`, `diagnostics`,
`disk`, `environment`, `execution`, `expected_minutes`, `extension`,
`extension_digest`, `fan_out`, `files`, `give_up_at`, `inputs`, `isolation`,
`output_delivery`, `outputs`, `prepare_source`, `profile_memory`,
`reason`, `require_target`, `run`, `shared_inputs`, `size_hint`, and `targets`.
Status rows additionally strip `submission_key`; job GET retains it. Within `program`, it strips `args` and `file`, retaining only
`type`. Within `extension_check`, it strips `automatic`, `directory` and
`reserve`, retaining only `work` and `timeout`. Some stripped fields encode
empty or zero placeholders because of their JSON types; those placeholders do
not preserve submitted values. Other retained fields still follow normal JSON
`omitempty` rules. This metadata does not expose commands, environment values,
file references or credentials. There is no full-spec read mode.

Native stage exceptions appear in job `isolation` as
`unsandboxed: <reason>` in status JSON and job GET; CLI/page reasons include the
same text through queued, running and terminal outcomes. Legacy registrations
use a fixed public explanation. Only this reason survives payload redaction;
the observation never grants launch authority. See [native isolation](native-isolation.md).
Administrative tools use uncached `GET /api/inspection` for migration custody,
local extension targeting and measurement safety checks.

Job receipts (`Client.get()` / `GET /api/jobs/{id}`) include `inputs_staged_at`
when a worker reports completion of the current attempt's input staging. This
worker-clock timestamp is captured after shared-input acquisition and private
input copies finish, before launch-time RAM checks, checkpoint preparation and
the Started handoff. It is diagnostic only and never changes admission or start
deadlines. It survives worker/queue recovery, resets on a new attempt, and is
absent for older workers or jobs with no staging operation. An absent value is
unknown, not proof that copying is still running. Compare with `assigned_at`
and `started_at`, allowing for clock differences between worker and queue.
Upgrade the queue before workers that report this field; older queues reject
unknown request fields.

`myce status` and `GET /api/status` report `queue_release` (`version`, `commit`)
from the queue process. Each machine's `release` reports the worker process's
version and commit from its check-in. Missing worker releases mean unknown
(older workers); they are never inferred from the queue release. Release builds
embed the source commit even with Go VCS stamping disabled. Development builds
use Go's VCS revision when available, otherwise `unknown`.

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
existing 750 ms heartbeat filesystem budget. Samples are timestamped at operation
completion. Successful capacity readings for every volume remain usable until
30 seconds after completion, even without pending-probe metadata. This avoids
false local-disk holds when busy workers report later than the former 3.85-second
window. Freshness follows measured sampler progress rather than the nominal
heartbeat cadence. The cost is up to 30 seconds of older local capacity evidence;
the last good free bytes still enforce the free-space floor and projected need.
Repeated reports and long sample intervals cannot extend that age bound.

A pending checkpoint capacity operation retains its last good reading using the same 30-second
per-operation stall backstop as root reachability.
`retained: true` and `probe_age_seconds` distinguish the pending
operation's age from the last sample's `age_seconds`. Successful slow `Statfs`
calls have exceeded the old freshness window. The cost of retention is up to
30 seconds of older capacity evidence; a completed I/O error, cold root or stalled
operation still holds new checkpoint-dependent work. The last good values remain
visible for diagnosis and rate calculation, without inventing zero free space.

Freshness uses worker-reported ages plus queue time since `disk_received_at`;
worker and queue clocks need not agree. Status reads advance displayed ages
without renewing freshness or writing queue state. Upgrade the queue before
workers emitting the additional probe-age field.

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

Machine `capabilities` describes native payload confinement separately from
`cpu_isolation` and memory enforcement:

| Capability | Meaning |
| --- | --- |
| `native_isolation_namespace` | Linux service probe selected rootless bubblewrap namespaces plus Landlock/seccomp. Main and sidecars share an attempt namespace anchor. |
| `native_isolation_landlock` | Namespace policy refused; Landlock ABI >= 6 plus seccomp fallback selected. File contents are protected by grants; metadata is not isolated. Type-only Unix seqpacket creation is permitted: same-UID pathname SEQPACKET endpoints reachable (partial). |
| `native_network_partial` | Linux fallback permits exact sidecar TCP ports at any address. Off-host same-port connects and non-loopback binds are also permitted; this is not general protected outbound capability. |
| `native_isolation_seatbelt` | macOS launcher available for targeted spool/credential/control protection. Metadata remains visible; profile acceptance is checked at launch. |
| `native_isolation_unavailable` | No supported native confinement stack; native payload launch fails closed. |
| `native_sidecars` | Selected mode supports the attempt's reserved sidecar ports. It does not strengthen fallback's address guarantees. |
| `native_network` | Host offers protected native outbound execution for `needs.network: true`. Both Linux modes currently withhold this capability pending qualified namespace egress. |

Capabilities report implementation availability, not completed fleet/GPU
qualification. Linux detection is cached for the worker process; recheck after
a service restart when policy/tools change. macOS's launcher-availability report
does not prove every profile will load; a rejected profile fails the attempt.
A missing capability report from an older worker is not evidence of online support.

Sandboxed native payloads default offline; worker-owned environment/input preparation
remains network-capable. An online job is eligible only on `native_network`
hosts. If none qualifies, submission returns HTTP 409 `no_qualified_target`
with a protected-outbound capability reason, rather than creating an impossible
queued job. A launch-time Linux mismatch reports `native_network_unavailable`;
unsupported confinement reports `native_isolation_unavailable`. macOS online
protection also denies the queue TCP port on every address, so unrelated
services on that port remain unavailable. CLI machine rows label namespace
mode as `general egress unavailable` and fallback as `partial network isolation
(reserved sidecar TCP ports permit off-host traffic)` and
`same-UID pathname SEQPACKET endpoints reachable (partial)`; an absent report says
`native isolation unreported`. For the threat model and exact
directory/temp/control/network guarantees, see [native isolation](native-isolation.md).

`native_isolation_warning` is the worker's compact startup inventory warning,
shown as `WARNING` in the CLI machine row. Fallback workers scan listening Unix
seqpacket endpoints once at startup: unexpected non-root/non-CUDA listeners,
unknown ownership or inventory failure warn visibly. Root-owned pathname
sockets and the observed CUDA abstract naming convention are excluded; this
is advisory naming, not authenticated identity or continuous monitoring.
Inspect worker logs for endpoint details. Later or other-network-namespace
listeners remain outside that snapshot and do not strengthen the residual.

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

`myce wait` resolves full IDs and unique prefixes through authenticated
`GET /api/jobs/{id}`, so a recently submitted job is visible even while the
aggregate status cache still shows an older generation. Go `WaitSubmitted`
accepts the Job returned by `Submit` and retries initial `job_unknown` replies
once per second for a five-second visibility window, until the first successful
job read. `SubmitAndWait`, foreground `myce run`, and profile receipt observers
use this receipt policy. A caller deadline or authentication error ends the wait
sooner. Plain ID-only waits fail immediately for an unknown ID. Waiting never
resubmits or cancels a job.

`myce status JOB` (full ID or an unambiguous prefix of at least eight characters)
shows that job and its `attempt_history`. Put `--full` before the job ID:
`myce status --full JOB`. The same optional array is included
in `GET /api/status`, the [job-v1 reply](schemas/job-v1.schema.json), and Python
`Client.get(job_id)`. Each entry has exact `attempt`, `machine`, `session`,
queue-clock `ended_at`, the resulting queue `state` (`queued` for a requeue),
and a diagnostic `reason`. Optional `execution_cause` and `cleanup_cause` use
the existing safe runtime operation/class and command exit code.
Launch causes also carry optional `predicate` and `reason`: a fixed, allowlisted
failed launch rule and its safe explanation. For example,
`native_workdir_containment` reports `native isolation requires workdir beneath
{work}; stage source with inputs or a profile`. Device identity/probe, memory
capacity, environment preparation, native isolation and queue launch authorization
have their own predicates. Existing operation/class, retry charging and release
semantics are unchanged. Unknown worker predicates and worker-provided reason
text are discarded; messages never contain environment values, credentials or
host paths. `{work}` denotes the managed attempt workspace.

The same detail appears in job `execution_cause`, `attempt_history`, job GET,
status, CLI `myce status JOB`, and each attempt's ledger outcome reason. Queue
logs emit `event=launch_outcome` with the exact attempt, predicate and safe reason.
The worker persists the predicate with its launch facts, so acknowledgement and
queue reopen retain the diagnosis. Older attempts without recorded predicates
cannot recover it. Workdir containment remains a worker-side launch check because
it resolves the actual managed workspace and symlinks on that host.

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

Queue-driven cancellation and an assigned attempt's start-deadline expiry also
record exact-attempt endings, without inventing a worker execution cause or
physical-release evidence. Never-assigned jobs have no attempt entry. Late exact
operator refunds refine that attempt's operator flag and, when reviving its
failed decision, its state/reason; the original end time and other attempts stay
intact. Cleanup-only acknowledgements do not rewrite an ended attempt.

After cancellation, `cancellation` is the read-only aggregate acknowledgement.
Declared parents also expose `cancellation_scope` for every piece and registered
descendant, including never-dispatched work. See [abort and release](guide-fan-out.md#abort-and-wait-for-release).

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

CUDA/XPU cgroup attempts sample device use every five seconds, including declared
jobs, checks and resumed attempts that cannot feed learning. CUDA uses NVIDIA's
per-process `compute-apps` counters; GPU-wide `memory.used` can be unsupported
on unified-memory devices while these process counters work. `device_gb` is a
sampled lower bound, not a retained allocator maximum. When no usable sample is
retained, `device_unavailable` explains missing process samples, unsupported or
invalid accounting, unavailable process inventory/cgroups, or sampler shutdown
timeout. Older workers may omit both fields; historical peaks cannot be recovered.
These diagnostics grant no learning eligibility or accounting reduction. Shared
physical RAM alone does not establish cgroup coverage: GB10 CUDA allocations can
exceed the job's current cgroup charge. Unified admission retains conservative
host plus device reservations and existing slack until overlap is qualified.

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

Lower-tier job `reason` also explains host-specific admission delays independently
of `production_hold`. Two production preemptions of the same job on one physical
host within twice the larger of its measured cold preparation time and the
150-second production start window delay re-admission there for one such interval.
The job's `production_backoff` records the last preemption and retry time by host;
it survives queue restart and leaves other hosts eligible. Started environment
preparations count, while revoked unstarted offers do not.

When matching preparation history exists, new lower-tier setup is withheld if
it exceeds the free window before eligible queued production or an imminent
declared dependent production stage. Piece and whole-parent `after` edges use
current qualified measurements of already-running production waves. Unlaunched
pieces, unknown/stale measurements and overruns provide no forecast; only the
preemption back-off applies. Warm environments incur no preparation cost. These
admission checks do not enable a disabled production hold or stop Started work.

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

## Validation reservation status

`myce reserve list`, `myce status` (CLI/JSON/web), and
`GET /api/reservations` expose durable validation reservations. Snapshot JSON
uses `reservations`. Each record includes `id`, `job`, the canonical stable
`selector` and resolved `target`, `owner`, `reason`, `created_at`, `expires_at`,
`max_lifetime` and `revision`. `text` reads `reserved for OWNER: REASON`.

| State | Meaning |
| --- | --- |
| `draining` | Existing work, independent maintenance, production takeover, missing inventory or fresh worker/cleanup evidence still prevents validation. The reservation never cancels Started work. `pending` names the blockers. |
| `ready` | The reserved components have no unrelated physical obligations or reservation readiness blockers. Ordinary qualification, priority and capacity checks still apply; this is not a launch grant. |
| `in_use` | The bound job has Started and is running/stopping. |
| `waiting_for_release` | The job is terminal with exact cleanup still pending, or its binding is unavailable. A terminal state alone does not release the reservation. |
| `released` | Only this reservation's admission fence was removed. `released_at` and `release_reason` explain automatic terminal-plus-cleanup release, lost-binding expiry or conditional operator release. |

`cleanup_pending` independently reports retained bound-job execution custody;
it may remain true for a released reservation after conditional operator release.
Operator release never manufactures physical cleanup. All known attempts,
including lost or superseded ones, and the bound job's Started preparations
must settle before automatic completion release. Missing or stale reports
survive queue restarts. The maximum lifetime
defaults to two hours and is bounded at 24 hours; the original expiry survives
caller replays and queue restarts. A non-terminal bound job or unconfirmed
cleanup retains the fence past this lifetime; `text` reports `reserved past max
lifetime` and the reason. Expiry releases only an absent binding without retained
execution custody. The release reason remains in status.
Other reservations, device exclusions and newer operator drains remain intact.
See [operations](operations.md#reserve-a-host-or-its-devices-for-an-existing-validation-job)
for acquisition, atomic `extension check --reserve`, and conditional release.

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
`free_host_memory_gb` is admitted headroom: the smaller of live availability
minus running attempts' remaining growth room, and the configured allowance
minus full reservations (when configured). Remaining growth is
`max(0, reservation - current nonreclaimable use)` per attempt. The operator
reserve is already removed from live availability; it is not subtracted again.
Only fresh exact-attempt cgroup anonymous resident memory reduces growth;
reclaimable cache, historical peaks and process RSS do not. Missing or five-second-old
samples retain the full reservation, as do models and unconfirmed release custody.
Queue restarts discard current-use credit. macOS uses the conservative fallback
because its process-group RSS includes shared pages and is not a safe credit.
Similarly, `cpu_in_use_cores` and `free_cpu_cores` are reservations,
not CPU utilization. Zero reserved cores does not establish an idle host.

Capacity waits name the selected host's reservations and, when reported, live
RAM availability and admission headroom. An allocated batch slot is a concurrency
rule, not proof of GPU activity. CPU status says reserved/unreserved and reports
utilization as unknown. Usage charts show allocation occupancy, not compute load.

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

Plain native jobs without an isolation declaration remain unsandboxed in v1.
Their `isolation` status explains the compatibility default and the one-line
`isolation: sandboxed` opt-in. New extension registrations default sandboxed.

### API request latency

Authenticated status JSON includes `request_latencies`: one entry per HTTP route
pattern with lifetime `count`, retained `samples`, and nearest-rank `p50_ms`,
`p95_ms`, and `p99_ms` handler durations. Each route retains its latest 256
completed requests, with a hard cap of 128 routes. Samples reset on queue restart;
the ordinary one-second status cache also applies to this summary. These are
server handler times, not client transport or process-budget times.

The queue emits one `event=slow_request` line for each handler taking more than
one second. It records the route pattern, response status, `duration_ms`,
`mutex_wait_ms`, `mutex_hold_ms`, and `after_unlock_ms`. Wait and hold accumulate
across the request's measured queue acquisitions; after-unlock measures the tail
from the final measured unlock through response completion, including hydration
and response writes. Work before/between acquisitions is included in total time.

Mutex attribution follows request contexts through queue-backed routes, including
hydration recovery, file-store callbacks, and model forwarding. A
`mutex_measured=false` request reached no queue acquisition (for example a cache
hit or early rejection). Summary timings cover every route. Instrumentation uses monotonic wall time only for observation;
queue decisions still use the injected clock. Logging and summary publication
run after unlocking. Labels use registered route patterns or `unmatched`, never
request paths, query strings, tokens or bodies. Long-lived streams are recorded
when their handler ends.

### Non-Mycelium RAM history

Each host's `external_memory` status field (also returned by host maintenance
status) contains `24h` and `7d` summaries: `p50_gb`, `p95_gb`, exact observed
`max_gb`, queue-clock `peak_at`, sample count, occupied bucket count and first/last
bucket times. `myce status` prints these beside the host's current allowance.
No samples means unknown, never zero external use. Old workers without
`memory_observation` do not contribute. History survives queue restart.

The worker adds a current observation to its existing heartbeat on macOS and
Linux. Host used RAM is physical RAM minus reclaimable RAM, undoing the worker's
admission reserve. Raw reclaimable RAM is retained before the admission clamp,
including pressure below that reserve. Current worker and descendant RSS (including native jobs and
model processes) is subtracted; invalid observations are skipped. Custom sources lacking raw
RAM evidence cannot contribute while reserve-clamped.
Reservations and attempt high-water marks are never subtracted. This is a
sampled estimate: RSS may count shared pages more than once and miss compressed
pages, reparented processes, or short spikes. Docker daemon children and other
Mycelium processes outside the worker tree remain in the residual, conservatively
reducing suggested budgets. Device-only GPU RAM is not host RAM.

Storage is bounded to 673 fifteen-minute bucket slots per host. Each bucket has
a fixed 64-bin logarithmic histogram, stored sparsely, plus exact maximum and its
time. Percentiles weight heartbeat samples and use bin upper edges (up to 20%
rounding, minimum positive bin 0.125 GiB), capped at the exact maximum. The
boundary bucket is included, so a window can include up to fifteen extra minutes.
Offline gaps have no invented samples. These are observations, not proof of a
future peak or a complete week: inspect counts and first/last bucket times.

### Suggesting a host allowance

```sh
myce host budget HOST --dry-run
myce host budget HOST --dry-run --headroom-gb 4 --queue-overhead-gb 2
```

This uses a read-only GET and prints JSON; it never applies a budget, drains a
host, or changes admission. A fresh worker observation and nonempty seven-day
history are required. The suggested GiB allowance is:

```
max(0, physical RAM - seven-day external max - headroom - extra queue reserve)
```

It rounds down to 0.25 GiB. Headroom is the greater of `--headroom-gb` (default
4 GiB for interactive use/new workloads) and 25% of the external maximum (the
existing guard against external growth). This deliberate reserve costs admitted
capacity to leave room for unsampled or future external peaks. Max, rather than
p99, preserves observed bursts. No suggested value changes the configured budget.

A queue already running on the host is included in the residual: its overhead
is already subtracted once. `--queue-overhead-gb` defaults to zero and is an
**additional** planning reserve for a future queue relocation or growth not
represented by the observations; for example, 2 GiB is an explicit planning
assumption, not a measured queue peak. Set that reserve when sizing for a queue
that will run on the host. Inspect coverage and workload comparability before
using a suggestion to change policy through the separate explicit budget command.

Opted-in asynchronous NAS results expose a `delivery` object on job/status
receipts independently of compute `state`. `state` is `pending`, `publishing`,
`failed` or `verified`; it includes accepted `id`/`attempt`, `machine`, `session`,
`destination_id`, `manifest_sha256`, `bytes`, `pending_at`, `last_attempt_at`,
`last_progress_at`, `next_attempt_at`, `attempts`, `verified_at` and a sanitized `cause` as applicable.
`myce status` prints delivery state/bytes/age/attempts even for succeeded jobs.
`publishing` with a stall cause still owns its outstanding call; it is not a
signal to start a second publisher. Returned errors show failed and a retry time.
Only verified with the matching execution is a NAS receipt. `output_refs` is
available at compute completion. `pending_output_refs` is an unaccepted upload
reservation, never a successful result. File-store `reserved_output_bytes`
reports missing async output reservations; `free_bytes` excludes those bytes.
See [delivery policy](guide-files-v3.md#complete-from-queue-storage-deliver-to-nas-later-349).

### Shared interactive profile

Host maintenance `policy.shared_host.interactive_reserve_gb` is the configured
protected reserve. Machine `shared_host` is the worker's exact acknowledgement;
`shared_host_reason` explains missing history/live observations or acknowledgement.
Shared hosts retain `last_resort: true`. `free_host_memory_gb` additionally respects
the seven-day external peak and protected reserve, without increasing the static
allowance. No samples means withheld admission. Missing `shared_host` normally
means no profile; compare queue policy before interpreting an old worker's row.

Shared-profile hosts also report `shared_host_pressure` (OS memory level/PSI,
foreground wake lateness, local age and queue receipt time),
`shared_host_yield_reason` (the queue-owned hysteresis latch), and
`shared_cpu_enforcement`. A pressure reason withholds admission and marks exact
attempts with `shared_host_pressure` planned-stop provenance. Unknown/stale
observations block admission without becoming measured pressure. Linux reports
the actual delegated quota/weight; macOS explicitly reports CPU time unenforced.
See [shared-host thresholds and qualification](shared-hosts.md#pressure-response).


## Execution elapsed time, remaining estimates, and application progress

Running jobs expose `runtime` in Job GET, status JSON, CLI and the read-only
page. `elapsed_seconds` starts at the queue's Started acknowledgement, not
assignment or input staging. It is absent before Started. Queue wait,
preparation and delivery are separate lifecycle evidence, not predicted phases.
Accepted-history timing includes result handoff; no separate delivery ETA is
invented.

For comparable extension work, declare a variant reference, for example
`references: {"extension:example": {input_shape: small-v1, unit: frames, units: 10}}`,
with the variant's check and baseline. Submit
`workload: {input_shape: small-v1, units: 100}` in the same serial mode and unit.
The existing qualified exact-key work curve supplies the frozen total prediction.
`runtime.remaining_seconds` subtracts elapsed from that prediction, with
`source`, `unit`, `samples`, `history_age_seconds`, and a `limitation`.
It is an approximate runtime estimate, without a statistical confidence bound.
After the prediction is reached, remaining becomes unknown with reason
`historical estimate exceeded; remaining time unknown`; zero is never a promise
of completion. Missing, invalidated, older-than-30-day and noncomparable history
have explicit `runtime.reason` values. Plain commands still show elapsed time
and explain their missing workload/reference metadata. These observations do not
change placement or measurement qualification.

Native commands and extensions can atomically replace the local JSON file named
by `MYCE_PROGRESS_FILE` (also mounted for managed Docker commands):

```sh
printf '{"completed":25,"total":100,"unit":"frames"}\n' > "$MYCE_PROGRESS_FILE.tmp"
mv "$MYCE_PROGRESS_FILE.tmp" "$MYCE_PROGRESS_FILE"
```

No API address or fleet token is needed. This small file uses the existing
worker heartbeat and attempt-directory cleanup, so no new service or Python
client call is needed. Python jobs can use `os.environ["MYCE_PROGRESS_FILE"]`
with `json.dump` to a sibling temporary file and `os.replace` to publish it;
Python `Client.get()` and `Client.list()` return the same JSON evidence.

Both counts are required, finite and nonnegative, with `completed <= total`.
Zero/zero is allowed. Unit is a single generic name of at most 64 characters
using the extension-name syntax (lowercase letters, digits, `_`, `-`, `.`,
starting with a letter or digit).
The worker reads only regular files up to 4096 bytes. Invalid, partial, oversized
or missing reports retain the last valid report and do not refresh its age.
File modification age is measured by the worker; queue time since receipt adds
to it, so worker and queue clocks need not agree. `application_progress` contains
counts, unit, exact attempt/machine/session, receipt time, age and `stale`.
More than 30 seconds without a file update marks it stale; this is a display
threshold and never stops work. Rewrite even unchanged counts to indicate a
live reporter. Worker restart preserves file age. Queue restart preserves receipt
age. New attempts clear reports; old attempts/sessions cannot replace them.

Application progress is advisory. It is separate from durable checkpoint
`job.progress` generations and accepted `fan_out.results`. A report of 100% may
still be finalizing outputs: it never verifies results, releases custody, proves
portable recovery, qualifies an extension or contributes throughput history.
The CPU case of `TestDevStandaloneSmoke` exercises credential-free reporting
through `myce dev`, observes it in Job/status replies, then completes the command
through the normal result and release path.

### Authored intent and retained placement

Job GET, status and inspection expose `placement_intent`, frozen at submission
before recipe defaults or scheduler assignment change the executable spec:

- `scope`: `work_resources`, `platform_constraints`, or `host_device_restriction`,
  describing caller constraints; recipe metadata remains separate.
- `origin: authored`; `host_constraint` distinguishes no restriction, a single
  pin, and an allowlist. `device_constraint` identifies an explicit selector.
- Exact authored `targets` (OS/architecture/backend), required/preferred backends,
  and recipe targets with authored device selectors. These finite platform
  fields omit commands, local paths, environment contents and arbitrary reasons.
- `environment` records coverage and its catalog, inline submission, or extension snapshot origin.
  `targets` means exact target coverage; `platforms_only` means backend coverage
  with OS/architecture unknown; `unknown` and `not_required` are explicit.
  Main/sidecar catalog coverage is their intersection. Extension coverage is its
  frozen variant target list, not proof that a worker is ready now.
- `qualification` records whether qualified execution is required. Owner output
  `parity` stays `unknown`: neither a pin nor registration proves MY-19 parity.

`placement` records the historical scheduler assignment: attempt, assignment
queue time, host/devices, the existing scarcity/completion planner `reason`, and
`limited_by` constraint categories. `host_choice` distinguishes `authored_pin`,
`scheduler_with_authored_constraint`, and `scheduler`; `device_choice` similarly
identifies authored selectors (including recipe selectors) versus scheduler
selection. A selector/allowlist restricts alternatives; the scheduler still
chooses within the permitted set. Project policy and production binding appear
as separate limiting categories.

Each assigned ledger entry and retained attempt outcome carries that same
historical decision. Requeues retain it with its original attempt number; a new
assignment replaces the job's latest decision. The existing paginated job ledger
retains older assignments under normal history retention. `eligible_hosts` and
related fields remain a **current policy/eligibility projection**, not evidence
of what was eligible at a past assignment or a promise of current capacity.

`production_binding` reports the current queue binding with `origin: scheduler`;
it never establishes an authored host pin. Old jobs lacking submission/assignment
provenance explicitly report unknown intent and omit historical placement;
status does not reconstruct intent from `production_host` or current eligibility.
These fields are observational and do not change placement or qualification.
See [the job schema](schemas/job-v1.schema.json) and
[ledger schema](schemas/job-ledger-v1.schema.json).
