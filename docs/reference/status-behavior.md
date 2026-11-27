# Seeing your mycelium

Released startup results replay independently of new compute admission.
`machines[].result_publications[]` reports exact `id`/`attempt`, unique
server-confirmed `published_bytes` observed since worker startup (including
dedup hits), `blocking` operation/request, `waiting_seconds`, and a fixed
`last_error` class. Default hypha summaries and worker logs show this progress.
An outstanding filesystem call retains its single publication owner; a queue
API call is watched against queue progress and measured request periods.
Unreleased startup executions still gate recovery; released publication does
not. The original session stays until its actors finish, preserving result
fences while new work uses the remaining capacity.

Accelerator discovery has independent freshness. `machines[].inventory_probe`
reports `state` (`pending`, `ready`, `partial`, `failed`, `stale`), original scan
`at`, send-time `age_seconds`, and measured `probe` validity. While a scan is
pending, `operation` and `pending_seconds` identify the current fixed operation
class; `error_class` distinguishes cancellation, deadline and driver/inventory
failure without publishing subprocess text. A restart or expired scan holds
new accelerator assignments with `waiting for fresh accelerator inventory`;
CPU work continues. A fresh partial scan keeps healthy device families usable.
A failed launch probe fences its affected card until a successful scan begun
after that refusal arrives. Contact and replaying cached healthy inventory do
not clear that fence. Queue reopen requires fresh discovery again.

Queue `event=file_upload` journal lines attribute upload mutations and server
errors to a digest and, when supplied, exact job/attempt. They include method,
status, requested/accepted offsets, declared length, publication operation and
safe error class. Offsets describe tus acceptance; only the verified blob and
accepted result establish delivery. An accepted end can still need publication.
Go uploads negotiate HTTP 102 reports from actual verification reads and
completed publication steps, so the final PATCH can verify and publish the
full blob without being mistaken for a dead socket.

## Reading `myce status`

`myce status` prints current work, machines, holds and active health conditions.
`--json` returns the same compact view. Add `--sections jobs,machines` for
placement diagnostics and machine telemetry, `--jobs all` for a history page,
or `--history` for the complete retained document. Retained means finished in
the last 14 days or still holding something (files, logs, custody, a checkpoint
folder, a dependent `after:` job, an extension check). Older records expire
and leave a one-line summary: `myce status JOB_ID` and `GET /api/jobs/ID`
then answer `410 job_expired` with it (see [retention](../retention.md#job-records)). `--full` means full job IDs. For one job, `myce wait JOB_ID` prints each state change
and `myce logs JOB_ID` prints its output, labelling each chunk `[attempt N]`.
The queue's appended environment-preparation failure is labelled
`[attempt 0: environment failure]`. Labels do not change `--after`/`--follow`
sequence cursors or the API's `attempt` field.

`queue restarted: waiting for workers to report` means at least one worker
outside planned host maintenance has yet to check in during restart recovery.
Workers that have checked in can take work when otherwise eligible.
Drained and disabled hosts still show their maintenance holds.

Attempt history reports memory-limit stops with their retained bound in GiB,
measured cgroup/non-reclaimable peaks and kernel event counts. A failed attempt
with `execution_cause.class: resource_limit`, an RSS guard stop, or kernel OOM
evidence at its enforced ceiling is not a coverage finding. Auto-memory work
uses ordinary OOM recovery; a declared ceiling is still the author's limit.
A missing tool or exec failure on an unproven machine type can still record a
coverage finding. Pressure (`high`/`max`) alone does not prove an OOM stop.

- **Machines:** each worker has a `hypha NAME:` line (a hypha is one machine)
  with its hardware and free memory. It says `available for work` when it can
  take jobs; otherwise it says why not, for example that its owner is using it,
  it is on battery, an operator disabled it, or it is offline.
- **Jobs:** each job line shows a short ID, its name, `[project/class]`, its
  state and the machine that ran it (`hypha=`). The main states are `queued`,
  `running`, `stopping`, `succeeded`, `failed`, `cancelled` and `paused`. A
  queued job carries a reason naming what it is waiting for and who needs to act.
- **Started:** every job GET reply and `myce status --json` job row carries
  `command_started: true|false`. It is true once any attempt's command may have
  begun (the queue's start acknowledgement on the job or a retained copy, or
  main `execution_evidence` of `may_have_run` or `unknown`). False means no
  attempt's command began, so a resubmission cannot repeat this job's work.

A command that exits zero can still fail output validation or publication.
`execution_cause.finalization` identifies the step, declared relative output or
destination, sanitized underlying error and `command_exit_code: 0` when known.
`myce status JOB_ID`, job GET and attempt history retain that distinction.
`myce result JOB_ID --attempt N --out DIR` can retrieve a metadata-only
`summary.json` even without opt-in log/report capture. No worker root or raw
transport error is included. Retryable publication errors remain pending and
retain local results; their typed cause clears after successful publication.

For a host memory-pool stop, `attempt_history[].priority_stop.memory_pool`
holds the queue's decision-time availability, reserve, physical growth rates,
forecast losses and trigger. For a discrete device pool stop, `device_pool`
in the same stop record contains the budget, measured usage, available memory,
victim overage and trigger. `stop_observation` is the later outcome reading.

## Automatic host health (#834)

Machine `host_health` contains a typed `drain_reason`, recent `history`, the
healthy `baseline`, and CPU/RSS-leading `foreign` processes and service identities.
Samples include OS memory pressure, swap/growth, load, timer wake latency,
heartbeat duration, local free disk, and configured NAS/checkpoint metadata-probe
latency or pending age. `age_seconds` is age at the worker's last report; worker
freshness remains independent. `host-health.json` in worker state survives a
restart. `persistence_error`, sample `error` and `foreign_error` are fixed codes,
never raw errors or local paths.

### Positive execution evidence (#1008)

Read `execution_evidence` with `execution_cause` when deciding whether retry
can repeat work. `not_started` proves the named component did not begin.
`may_have_run` and `unknown` do not justify a blind retry. A host refusal
can establish that main did not start even while other components still
hold custody. Check release independently.

## Validation reservation status

`myce reserve list`, `myce status` (CLI/JSON/web), and
`GET /api/reservations` expose durable validation reservations. Snapshot JSON
uses `reservations`. Each record includes `id`, `job`, the canonical stable
`selector` and resolved `target`, `owner`, `reason`, `created_at`, `expires_at`,
`max_lifetime` and `revision`. `text` reads `reserved for OWNER: REASON`.

`myce reserve list` requests `GET /api/reservations?view=unreleased`.
It shows every unreleased reservation, including draining reservations and
expired reservations awaiting explicit release. Use `myce reserve list --id ID`
to inspect one retained reservation, including its release record.
`myce reserve list --history` and unfiltered `GET /api/reservations` include
all retained reservations when the response fits the queue's request budget.
The exact-ID query (`?id=ID`) projects just that record. During a rolling
upgrade, older queues return the full array for any of these queries, and
the new CLI filters it locally if the request succeeds.

| State | Meaning |
| --- | --- |
| `draining` | Existing work, independent maintenance, missing inventory or fresh worker/cleanup evidence still prevents validation. The reservation never cancels Started work. `pending` names the blockers. |
| `expired` | Never ready before max lifetime, still queued and without retained custody. Other jobs regain admission; the bound job remains queued until explicit release. `expired_at` is admission expiry, not physical cleanup. |
| `ready` | The reserved components have no unrelated physical obligations or reservation readiness blockers. Ordinary qualification, priority and capacity checks still apply; this is not a launch grant. |
| `in_use` | The bound job has Started and is running/stopping. |
| `waiting_for_release` | The job is terminal with exact cleanup still pending, or its binding is unavailable. A terminal state alone does not release the reservation. |
| `released` | Only this reservation's admission fence was removed. `released_at` and `release_reason` explain automatic terminal-plus-cleanup release, lost-binding expiry or conditional operator release. |

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
Plain status prints the JSON on stdout in every case and exits by `state`
(#1187): 0 `healthy`; 3 `degraded` (all known workers fresh with consistent
memory and inventory evidence, while a device reason, unavailable root or host
hygiene reason withholds capacity), with a stderr line naming each worker,
component, reason and what clears it, such as `myce device clear-quarantine`
for a `device_fault_*` quarantine; 1 `unhealthy` (`host health incomplete:
missing, stale or inconsistent worker evidence`). Mutations exit zero on
durable acceptance. Queue maintenance status has no privileged
local-exclusion verdict.

### Live headroom and reservations

Use the `host_memory_ledger` attached to the same admission decision.
`capacity_gb - growth_gb` is remaining physical room and
`allowance_gb - reserved_gb` is remaining allowance room. Admission uses the
smaller amount and applies any shared-interactive cap. `available_gb` already
excludes the explicit reserve and owner charges include launch slack.
A later machine reading may describe different occupancy. Linux
`MemAvailable` includes reclaimable cache. Missing evidence stays unknown.

### API request latency

Authenticated status JSON includes `request_latencies`: finite registered route
patterns, lifetime counts and conservative histogram percentiles. Duration-bit
bins replace raw sample rings and route caps. Locally measured service cost and
adjacent variation select unusual `slow_request` lines; measured logging cost
gates further lines. No universal one-second threshold applies. Lines retain
route/status and mutex wait/hold/after-unlock durations.

### Non-Mycelium RAM history

For a replay-window artifact with queue/worker RSS and card observations, see
[benchmark telemetry receipts](../benchmark-telemetry.md). Terminal job facts and
the bucketed history below cannot reconstruct missing host snapshots.

Each host's `external_memory` status field (also returned by host maintenance
status) contains `24h` and `7d` summaries: `p50_gb`, `p95_gb`, exact observed
`max_gb`, queue-clock `peak_at`, sample count, occupied bucket count and first/last
bucket times. `myce status` prints these beside the host's current allowance.
No samples means unknown, never zero external use. Old workers without
`memory_observation` do not contribute. History survives queue restart.

### Suggesting a host allowance

From the measured-allowance release, `external_memory.quiescent_v1` separately
reports physical used RAM when both local and queue custody prove an idle host.
It subtracts no RSS. Its `24h`/`7d` sample and `windows` counts expose actual
learning opportunities; legacy history has no such provenance, so cannot tell
how many idle windows occurred. Missing qualified history remains unknown.
Machine `host_memory_derivation` names the opt-in allowance's evidence or authored
fallback. Preview that same derivation before a drain with the command below.
See [queue maintenance](../operations.md#queue-maintenance) for the output and
[measured host allowances](../reference/host-capacity.md#host-ram-admission-and-ceilings)
for the role-specific calculation.

```sh
myce host budget HOST --dry-run
```

### Shared interactive profile

Host maintenance `policy.shared_host.interactive_reserve_gb` is the configured
reserve. Machine `shared_host` is the worker's acknowledgement, and
`shared_host_reason` explains missing history, observations or acknowledgement.
Shared hosts report `last_resort: true`. `host_memory.room_gb` respects the
seven-day external peak and protected reserve. Missing samples withhold
admission. Compare queue policy before interpreting an older worker's row.
See [shared and polite hosts](shared-and-polite-hosts.md).

## Execution elapsed time, last accepted duration, and application progress

Running jobs expose `runtime` in Job GET, status JSON, CLI and the read-only
page. `elapsed_seconds` starts at the queue's Started acknowledgement, not
assignment or input staging. It is absent before Started. Queue wait,
preparation and delivery are separate lifecycle evidence, not predicted phases.

Mycelium keeps one record per kind of work (MY-49): its last accepted duration
(Started to accepted completion, including result handoff) and its accepted
memory peak. The kind of work is the exact observation key: the work or
command identity, extension digest and reference, host, component, runtime
fingerprint, environment and resource configuration. A fan-out piece is its own
kind of work, including its arguments. Qualification checks learn memory, not a
duration. `runtime.last_accepted_seconds` and `timing.last_accepted_seconds`
show that duration when the attempt began, or `runtime.reason` says why it is
unknown: no accepted matching run, an invalidated or OOM-forbidden record, or a
changed attempt or identity. It is history, never a forecast, remaining time or
deadline, and it has no age limit; the next accepted run replaces it. Status
shows `last accepted duration Ns` or `unknown`.

For compatibility, `timing.predicted_seconds` mirrors the last accepted
duration. `runtime.remaining_work_unknown` describes forecast coverage and
`runtime.samples` counts accepted history. Use `last_accepted_seconds` as
elapsed history.

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
  `targets` means exact target coverage. `platforms_only` means backend coverage
  with OS/architecture unknown. `unknown` and `not_required` are explicit.
  `selected_member_lock_targets` records the frozen intersection of the CPU job's
  selected locks for main and sidecars. It describes dependencies only.
  Extension coverage records its frozen variant target list.
- `qualification` records whether qualified execution is required. Owner output
  `parity` stays `unknown` (always present, required by the previous release's
  v1 reply): neither a pin nor registration proves MY-19 parity. Only command discovery records
  `not_accepted_for_automatic_production`.

## Per-copy execution intervals

Job GET `copies` retains `started_at`, `stopped_at` and `released_at` for each
exact `(job, attempt, machine, session)`, using the queue's injected clock.
`started_at` is the first accepted start acknowledgement. `stopped_at` records
the queue's stop transition, or the exact release receipt if no stop was
requested; it is not signal-delivery evidence. `released_at` is the first
accepted exact cleanup acknowledgement. Replays, elapsed grace, loss and an
omitted heartbeat attempt do not manufacture or move release times. Older
copies omit unknown timestamps. Legacy idle-heartbeat release inference keeps
`released_at` absent; a later exact receipt may fill it. Fresh-session cleanup
uses the existing verified release protocol. Copies survive retry and queue restart.

## Learned storage evidence

Status JSON includes `storage_observations`, also shown by CLI/HTML status as
Storage evidence. Each aggregate identifies the program/profile, frozen recipe
and source, qualified OS/architecture/platform variant, hashed output-affecting
configuration and input shape. There is no host in the comparison key; evidence
does not qualify another variant or change placement. Plain commands need a
frozen source manifest; unversioned external software stays unknown.

## Deadline priority guidance

Submission and job GET replies include `priority_policy` (effective tier,
selector and revision) and `deadline_guidance`. The latter contains advisory
`code`, the `deadline_policy` match for a new deadline-class submission,
`precedence_change` (`higher`, `same` or `lower` tier precedence), current
`blockers`, `eta`, an `example`, `next_action` and explanatory `message`.
These are live observations, separate from immutable submission intent and job
holds. They do not change the accepted class or submission key.

## Configured NAS mount availability

Worker `unavailable_roots` lists affected configured root roles and a fixed reason
when the configured NAS mount is absent or its mount inventory is unavailable.
Mountpoint directories and local fallback capacity do not establish mount health.
Host health is degraded while any configured root is affected.
Jobs requiring those roots wait, including non-resumable production inputs and
models. Prepared local environments and cached shared inputs remain eligible
when their own roots are healthy. Started attempts retain their exact custody.
Restored mounts clear the observation automatically; an explicit operator disable
still requires an explicit resume. Independently configured network roots are
remembered after being observed mounted; installed NAS is checked even at cold
startup. Root roles, never local paths or mount source addresses, cross the API.

## Incident health alerts

Default status leads with unhealthy hosts, holds and current work. Statistical
findings occupy one trailing count line. `myce status --sections alerts` (alias
`health`) lists all open/recently closed records; `myce efficiency` diagnoses
reservation gaps, placement restrictions, waits and wasted attempts.

`health_alerts` keeps one record per kind and subject (host, device, job, project,
store, reason, etc.). Repeated firings update `count` and `last_at`; `first_at`
bounds the history, `at` starts the current episode, and `state` is `open` or
`closed`. Conditions use the injected queue clock and clear on positive recovery;
point events such as `production_attempt_lost` close immediately. Closed records
expire after three days, except configured, unacknowledged host notifications.
No detector changes admission, retry, learned sizes or release custody.

### Health notifier hook contract

Configure the queue with `--health-notify EXECUTABLE` or `MYCE_HEALTH_NOTIFY`.
The adapter checks configuration with `--check` at startup and receives one JSON
record on stdin per delivery. Exit 0 acknowledges acceptance. Failures and
timeouts retry the same `delivery_id`, including across restarts, so deduplicate
durably. Closings append `/closed` to the opening ID. Unattempted closed history
stays in status. Only host problems reach the inbox. Advisory
`host_mount_degraded` stays in status unless it escalates.

`health_alert_delivery` reports whether delivery is configured and how many
records await acknowledgement, including closings waiting for steady recovery.
The hook receives no fleet token. Its acknowledgement proves receiver acceptance.
Job-completion delivery is independent.

## Local completion observer diagnostics

`myce wait --observer-status` reads the user registry's `observer.status` and
reports `owner_locked` from the lifetime lock. A record alone does not prove a
live owner; the lock alone does not prove progress. Compare `captured_at`,
`phase`, `last_read_at` and `last_delivery_at` with job completion timestamps.
Phases identify discovery, authority resolution, batch/terminal reads, notifier
execution, waiting, handoff and stopped state. Version, commit, PID, start time,
registration/delivery/failure counts and typed last-error cause support diagnosis.
The `readers` map retains each in-flight operation's phase, job count and start/capture times independently, so one responsive authority does not hide another stalled read. `next_read_at` and `next_delivery_at` identify recorded backoff/pacing. Stale metadata identifies the last recorded boundary, not the current Go frame.

## Batched waits and consumer flood protection

`POST /api/wait` takes `ids`, an optional `after` map of job IDs to revisions,
and `seconds`. The first request may omit `after` and returns immediately.
`jobs` contains ordered records with `id`, `state`, sanitized `reason` and
`revision`. Send those revisions with exact IDs on subsequent requests.
The request waits for a selected change, a terminal record or timeout.
Use `myce wait JOB_A JOB_B` for the supported client flow.

A 429 means observation capacity is limited. Respect the retry guidance,
reuse one batched wait, and fetch full terminal results independently.
See [request budgets](../request-budgets.md).

## Job GET request attribution

The API emits `event=job_get_totals totals=[...]` when cumulative traffic reaches a new binary magnitude,
subject to measured sink cost and one in-flight summary. Each row has `kind` (`cli`, `python` or `unknown`),
`version`, and cumulative `full` and `wait` counts since that API handler started.
`wait` counts requests with `?view=wait`, including terminal replies that return
full Jobs; other views count as `full`. Counts include failed job lookups.
Each row's optional `rate_limited` map also counts refused observations by
finite route class: `job_full`, `job_wait`, `batch_wait`, `status`, `inspection`,
`usage`, `portability` or `page`. Rejected reads do not increment admitted
`full`/`wait` counts. Rejections trigger the periodic summary even when no job
GET is admitted. Authentication failures remain excluded. Restarting resets totals;
compare consecutive summaries for rates. No traffic means no summary.

### Variant eligibility (#499)

Nonterminal native variant jobs expose
`variant_eligibility` in status, Job GET, the CLI and the status page. Rows
identify the frozen implementation, exact variant and observed target.
`freshness` is fresh, stale or unknown. Only fresh inventory establishes
observed eligibility, which describes compatibility and may include busy hosts.

### Read-only host-pin audit (#499)

Run with an OpenSSL-backed Python supported by the installed-authority client:

```sh
python3 scripts/audit-host-pins.py --since 2026-11-01T00:00:00Z --until 2026-11-04T00:00:00Z
```

The script reads paginated status through `Client.from_installed()`; it never
reads a database or changes jobs. The inclusive start/exclusive end window uses
job submission (`queued`) time. Deduplicate by job ID and count explicit
`spec.allowed_machines` or authored `placement_intent.host_constraint=pin`.
Group by project and production/non-production (all other classes). Scheduler
assignments, production bindings and backend/device selectors do not count as
host pins. The denominator is all retained submitted jobs in the window, the
same method as the 2026-10-30 audit (1,108/2,535 overall; 62.2% non-production).

### Authored requirement explanations (#748)

Adapter entries, legacy profiles and job specs retain
`requirement_reasons` in frozen submitted intent and private receipts. Existing
native registrations already retain their map in each frozen extension snapshot.
No scheduling authority, parity approval or pin policy comes from an explanation.

Status and job GET expose job-level `requirement_reasons` rows with `constraint`,
`reason_source` (`authored` or `missing`) and `source`. `source` locates the field
in the frozen spec or the extension identified by its digest; it is not a host
filesystem path. CLI/page status show the same metadata. Host/device constraints
with no nonblank recorded explanation show `missing`; for historical submissions
that means no explanation was retained, not that their requirement is invalid.
Native recipe device selectors are included. Registration metadata and accepted
jobs are never rewritten to fill in reasons.

## One hold record

The queue's `hold` for a queued or paused job includes `kind` (stable code),
`owner`, `reason`, `clears`, optional `expires`, `next` and `detail`. Status
and wait print the reason and next action. History, resolve and JSON expose
details. Follow `owner` and `next` for an unfamiliar hold.

- `capacity` can mean an allowed target fits but is occupied. Keep the job and
  wait. `no_eligible_host` can mean it exceeds every allowed target's rated
  capacity. Check authored needs and allowed machines.
- `unprepared` means no environment builder has started, `waiting_for_builder`
  means a builder offer or core is pending, and `building` requires a fresh
  builder report. `deterministically_failed` retains the recipe error.
  `unknown_observation` means the queue owner must restore evidence.
- `missing_extension` asks for implementation registration.
  `execution_coverage` means no compatible implementation is observed.
  `native_qualification` names a pending native check or an expected offer.
  `production_parity` and `production_parity_kind` name missing approvals.
- `checkpoint_root` means the queue's checkpoint root is unreachable,
  `checkpoint_target` means resume needs a compatible target, and
  `storage_root` identifies unavailable storage required by the job.
- `hold` on a target's `admission_decision` names the refusing check. The
  job's hold still carries the details.

Resolve reports the next compatibility action and its source. It cannot create
check demand. A fresh observation that only one physical host fits does not
establish durable uniqueness. `needs-staging` is normal during preflight,
and the subsequent run stages local source automatically. For per-target
advice use `myce portability`.

## Job findings

Failed jobs, and queued or running jobs that Mycelium requeued after an
infrastructure loss or a queue admission refusal, carry one additive `finding` with `code`, `owner`, `what`
and `action`. `owner` is `job` (the command or its declaration), `mycelium`
(host or queue infrastructure, not the job's fault) or `unknown` (no typed
cause was recorded). `what` is one sentence of measured evidence and what
Mycelium did; `action` is the single change to make. The finding appears in job
GET, compact and detailed status, batched wait replies for failed jobs, and the
`myce status`/`myce run` reason text. `myce wait` prints it on stderr after the
terminal `ID state` line, and `myce result` includes it when it refuses a job
with no accepted result.

Findings are derived from sanitized runtime causes, queue-owned reasons and
bounded qualification evidence. The finding itself is never persisted.
Covered classes include output-contract failures after exit 0 (`output_*`),
publication failures (`output_publication`), resource stops (`memory_limit`,
`out_of_memory`, `memory_exceeds_hosts`, `wall_time_limit`, `disk_limit`),
infrastructure requeues and exhaustion, queue admission requeues before the
command started (`queue_admission`, owner `mycelium`), launch failures (`library_missing`,
`tools_missing`, `input_unavailable`, `launch_*`), upstream dependencies,
nonzero exits and payload signals. A failure with no typed cause reports
`cause_unknown` rather than a guess. Succeeded and cancelled jobs carry none;
waiting explanations remain in `hold`.

A useful job that fails because of automatic native qualification keeps
`finding.code: dead_end_wait`. Its finding names the failed check job IDs and
attempts, with `code`, `exception_type` and `missing_module` when available from
the [qualification error contract](../adapter.md#report-a-qualification-error).
Job GET and detailed status also expose these records as `native_check_failures`.
The action links the evidence through `myce logs CHECK_ID` even when a check
emitted no structured error. Automatic retries of failed verdicts stay disabled.

## Read-only consumer project doctor

On a matching client/queue release, `myce doctor --project` diagnoses the selected
`.mycelium/adapter.yaml` entry without installation probes or changes. Use
`--adapter FILE`, `--entry NAME`, `--notify EXECUTABLE` and `--json` as needed.
Ordinary `myce doctor` retains its installation diagnosis.

Project mode checks installed authority internally, authenticated API/resolve
support, strict adapter validity, run's selected-source cleanliness contract,
local dependency readability, resolve findings, target coverage and completion
configuration. It leads with ready/blocked, the next owner and one action;
blocking checks exit 1, invalid options exit 2. Findings retain their evidence
and fixes. Queue-side unreadable locks remain queue-owner holds. Current resource
pressure, failed preparation, missing builders and unknown observations reuse
resolve's hold. Cold environments with an available builder can be submitted
normally; target eligibility still does not promise immediate capacity.

## Worker cleanup (#834)

The #811 worker/queue release adds `hygiene_collection.disk_budget`: per-volume
owned allocated bytes, free/floor/reserved bytes, measured control-file growth,
last-pass reclaimed free blocks and pressure. `foreign_disk_growth` means a
free-space loss unexplained by Mycelium's own allocation delta; it is not proof
of a particular foreign writer. A falling free-space trend adds
`exhaustion_seconds`; recovery clears the warning/forecast.
With `worker_disk_trend_protocol: worker-disk-trend-v1` (#834), the forecast uses
the current continuous decline's average rate instead of one collection step.
The decline starts when free space first falls and ends when free space returns
to where it started. `declining_seconds` is its age, and `decline_cause` is
`foreign_disk_growth` when bytes outside Mycelium's own allocation explain most
of the decline, otherwise `owned_disk_growth`. A foreign decline that has lasted
at least as long as its `exhaustion_seconds` raises one
`host_disk_exhaustion_forecast` health alert per volume (`store` is the opaque
volume ID). At a constant rate, that is when the decline has used half the space
above the floor that was free when it began, so the threshold depends on this
host's own measurements, not a fixed size. The alert clears when the decline
ends or the volume is no longer reported. Owned growth stays bounded by the
filesystem budget and never raises it. The alert is context for the host owner:
it causes no drain and no cleanup of foreign data. A worker restart starts a new
decline. Imminent exhaustion still drains through `host_disk_pressure`. These observations
can include concurrent writes and filesystem allocation changes.
`compiler_warning: external_compiler_cache_unmanaged` and
`unmanaged_compiler_bytes` identify known external Go cache storage without
publishing its path. `scratch` contains allocated retained/reclaimed bytes and
`retained_bytes_by_reason` from #1230's exact-attempt collector. Protected unique
data needs its owner's release/recovery; a capacity warning never authorizes
foreign deletion. See [filesystem budgets](../reference/worker-caches.md#shared-filesystem-budget-811-next-release).

### Installed executable versus running image (#824)

Worker `fleet` proof keeps the running `version`, `sha256`, PID and session
separate from `installed_slot` (`sha256`, `size`, and `version` when established).
`slot_mismatch: true` means saved bytes differ from the active process, including
publication followed by failed activation. `slot_error` means the slot could not
be observed; neither condition asserts activation or release approval. These
observations run outside queue locks and normal heartbeat work. Hashes are reused
only while local file identity, size and modification time agree; installation
rechecks the bytes independently. CLI status displays mismatches before an
install is attempted. Older workers omit these observations.

## Adaptive fair-share history (#813)

On a release containing #813, queued jobs in the dispatch projection explain
retained resource-seconds, weighted share, signed debt (positive ahead, negative
behind), the history horizon, weighted oldest project-wait round, observed active
service age, latest completed service quantum and the number of fitting competing projects. The horizon is learned
from the current tier/resource demand and service cycle, rather than a fixed
daily half-life. CPU and GPU ledgers stay separate. A zero completed quantum means initial learning: history is
preserved and new covered service is counted without decay until the first
allocation completes. Unknown reconnect/restart gaps never add service age. See [fair sharing](../scheduling.md#sharing-gpus-and-measuring-the-pool).

## Learned native CPU memory growth

At an exact native batch launch, fresh worker-local direct CUDA process memory
can establish already occupied physical RAM on a unified host. Admission credits
at most the largest process counter, avoiding duplicate shared allocations in a
process sum. Across the aggregate, total anonymous credit and this counter use
their maximum, protecting possible overlap even with another attempt's cgroup.
Only the launching attempt receives additional device credit. The full static
envelope remains reserved and measured device overshoot increases it. Missing,
stale, fallback/RSS and remotely supplied counters grant no new physical credit;
discrete device memory never reduces host growth. These counters cover helpers
and main in the same exact attempt subtree. Envelope calibration is separate.

## Queue efficiency (#857)

`myce efficiency` is the concise view. Its `--json` reads
`GET /api/efficiency`. Status and inspection also include `efficiency`.
`at` is the report's observation time. A new queue returns `null` until the
first maintenance observation. Diagnose a single project with
`myce efficiency --project NAME`.

- `utilization` answers "is work waiting while a card sits idle?" per card or
  CPU pool over the last seven UTC days (#2183). Each maintenance observation
  classifies every card as busy, busy while more compatible work waited, idle
  while compatible work waited, idle with no demand, held, or unknown, and
  holds until the next observation or until the card's inventory goes stale.
  `unknown_card_hours` is window time no observation covered; it is reported
  beside the idle number, never folded into it. Waiting reasons are the
  efficiency categories below, or `would_fit:BLOCKER` when a waiting job fit
  physically once one term is charged at its measurement:
  `holder_pending_growth` (running holders charged only to their measured
  peak), `own_charge` (the job at its own measured peak),
  `card_exclusive_unmeasured_device` (the card is refused only because a
  holder's device peak is unmeasured), `accumulation_fence` (room exists but
  the backfill fence withholds it) and `unknown_stale_inventory`.
  `would_have_fit_at_measured_size` totals job episodes and job-hours by
  blocker. Would-fit is a counterfactual, never an admission decision.
- `hosts` compares reserved, measured Mycelium and total physical RAM. Its
  integrals restart when the host's inventory identity changes; `since` is
  their window and the text view prints it. `measured_memory_unknown` says why
  measured Mycelium memory is unknown, and `attempt_peaks` gives retained
  accepted per-attempt host peaks, which exist where host-wide attribution
  does not.
  `programs` groups reservation and use integrals by host, work and sizing
  basis. Reservation spans custody through release. Measured coverage
  distinguishes missing readings from zero. Use
  `reserved_covered_memory_gb_seconds` for like-for-like comparisons.
- `wait_seconds` reports observed time by blocker, starting from the first
  observation. Earlier time stays unknown and pause time is attributed to holds.
  Every hold kind maps to a named category. Host-memory ledger, pool and
  headroom refusals are `memory_reservation`, including when some other target
  is refused for a different reason; an unmapped kind is `unknown`.
- `stranded` means the job fits current capacity if only the named authored
  or policy exclusions are removed. Qualification, preparation, resource and
  custody requirements still apply. Host-restricted and targeted jobs count
  separately. Observed wait alone is no causal estimate of the cost of a pin.
- `predictions` compares reservation with the accepted peak the scheduler
  learns from (host peak, or the combined GPU-host peak), and estimated
  duration with actual duration. Only accepted learning samples qualify for
  the memory comparison. `cgroup_peak_gb` is diagnostic: it counts reclaimable
  page cache.
- `wasted_attempts` lists every retained stopped attempt with its `kind`:
  `lost` (ran, then interrupted), `lost_start_unknown`, `never_started`,
  `cancelled`, `killed`, `guard_stopped` or `oom`, plus `started` and
  `burned_seconds` (command time, when known). `wasted_windows` counts them for
  the last 7 days and for the whole retained span, with its dates; the
  headline and regression signals count only kinds that may have burned run
  time. A `suspect` ending gives positive contrary evidence worth
  investigating, while missing evidence stays unknown.

`myce usage --by machine` reports GPU busy and idle hours per physical card.
A multi-card target charges its constituent cards. `busy_percent` is
allocation occupancy (GPU-hours assigned to started work over observed online
hours, excluding unknown outages), not device utilization. Project rows carry
GPU-hours only: a project has no observed-time denominator. CPU and I/O counters describe
observed attempt windows. Lower-bound indicators flag incomplete coverage,
which limits comparisons with reservation-time integrals.

## Adaptive evidence budgets (#811)

`evidence_retention` is an immutable maintenance summary of queue-host capacity,
soft budget, retained/protected bytes, reclamation and unavailable/over-budget
state. Status does no measurement or collection. See [retention rules and the
complete replacement inventory](../retention.md).

### Launch capacity waits (#1011)

A typed `launch/resource_limit/memory_capacity` refusal requeues the same logical
job without charging infrastructure retries or losses. `execution_cause.admission`
retains the failed subcheck and compared safe values. Component
`execution_evidence` remains independent: helper custody can exist while main is
`not_started`, including after Started authorization. Cleanup still requires the
exact attempt's release before the logical job can run elsewhere.

### Committed job reads under queue load (#1052)

Per-job reads reflect one committed observation. An in-flight response can finish
at that snapshot after newer work has committed. It does not promise the next
poll will see the same generation.

## Retained production admission waits (#868)

`admission_wait` reports first/latest production waits and distinct transitions.
Each target records its identity, failed predicates, resource demand, room and
holders with their tiers. Values and ages belong to the snapshot's `at`.
`preemption.source` distinguishes a planner decision from a diagnostic
projection. `preemption.planned` is new stop intent. `release_commitment`
reports prior releases still pending. `last_planner_decision` retains the last
real decision for that target and attempt or piece.

## Actual all-tier admission decisions (#1158)

After a queued job has waited one advertised or measured worker send interval,
a real check-in can add `admission_wait.latest.targets[].admission_decision`
for production, deadline and experiment jobs. Unknown cadence grants no invented
start deadline. This threshold is an observation opportunity, not a start SLA;
`opportunity_seconds` records its basis. Earlier production diagnostic evidence
remains available independently.

## Accelerator placement history (#1210)

`myce host status HOST` includes `device_use`: each used accelerator's stable
`platform` and `identity`, `last_assigned_at`, and cumulative batch `assignments`.
These queue-owned records survive queue reopen, worker sessions and inventory
ordinal changes. Among otherwise tied single-card targets on the same host,
placement prefers the least recently assigned card; assignment counts break ties
within one clock instant. Fitness, readiness, memory fit, quarantine and operator
maintenance remain authoritative. History records offers, including preparation,
rather than claiming measured runtime or successful execution.

## Fault evidence

`machines[].devices[].fault_evidence` retains a byte-bounded history of observed
NVRM allocation errors and recognized xe/i915/Xid faults. Each record contains
an event hash, typed signature, kernel wall timestamp, capture timestamp and
measured capture duration. Raw kernel messages are excluded. NVRM allocation
observations do not quarantine a device. A message without a device address
lists CUDA candidates and explicitly reports ambiguous device attribution.
Existing teardown/quarantine classification is unchanged.

## Shared owner yield and measured CPU (#389)

`cpu_in_use_cores` includes shared-host measured overuse, rounded up and never
below declared cores. Job `cpu_observation` retains the exact sample identity,
previous elapsed/usage counters and logical-job `peak_cores` lower bound. The
first reading establishes a counter origin; subsequent adjacent readings raise
the learned rate. The CPU `resources` entry exposes this learned need. It remains
valid through checkpoint/requeue, including missing or quieter later telemetry;
it does not predict a different submission.
