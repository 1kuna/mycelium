# Seeing your mycelium

Run `myce status` using your usual queue address and private token. `--full`
shows full job IDs. Open the queue address in a browser for the same information;
sign in as `myce`, using the token as the password. The page is read-only and
refreshes every five seconds. Keep tokens in private configuration, not URLs.

Each hypha shows its capability profile: CPU count, system memory, accelerator
memory, shared or separate memory, and local storage. Missing measurements say
unknown. Extension reports say which kind of work or program is qualified on
that hypha; having hardware alone does not make an extension qualified.

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
paths. Use the existing job output and log commands when you need their results.

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

Start-gate transport failures use `execution_cause.operation: start_authorization`
with the underlying safe class (for example `timeout`). An interrupted result
uses that cause in its reason, without implying a worker restart. Actual fresh
worker-session recovery retains its separate restart diagnostic. Authorization
failures still consume the existing infrastructure retry/loss budget.

History is persisted with the job and retains the **latest four ended attempts**:
three infrastructure endings (two production retries, or three counted losses),
plus the existing single OOM retry. Repeated planned requeues can exceed that
window; older entries roll off without changing execution or charging limits.
Existing jobs start with no history; past causes cannot be reconstructed.
An end time records the queue's decision, which can precede physical cleanup.
History never proves release; use the existing exact release evidence.

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
