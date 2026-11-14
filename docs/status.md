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
Go uploads negotiate HTTP 102 reports from actual verification reads so the
final PATCH can verify the full blob without being mistaken for a dead socket.

## Reading `myce status`

`myce status` prints current work, machines, holds and active health conditions.
`--json` returns the same compact view. Add `--sections jobs,machines` for
placement diagnostics and machine telemetry, `--jobs all` for a history page,
or `--history` for the complete retained document. Retained means finished in
the last 14 days or still holding something (files, logs, custody, a checkpoint
folder, a dependent `after:` job, an extension check). Older records expire
and leave a one-line summary: `myce status JOB_ID` and `GET /api/jobs/ID`
then answer `410 job_expired` with it (see [retention](retention.md#job-records)). `--full` means full job IDs. For one job, `myce wait JOB_ID` prints each state change
and `myce logs JOB_ID` prints its output.

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

`myce status JOB_ID --history` prints `staging_timings` for the current attempt
and each retained outcome; `--json` carries the same fixed phase counters.
The worker's `started_at` is its first preparation call, `completed_at` is the
actual inputs-staged completion, even when a later retry consumes it. Compare
with `assigned_at` for dispatch/preparation delay, allowing for queue/worker clock
skew. Durations use the injected worker clock (monotonic in production).

Phases cover preflight/device checks, root metadata, extension/environment setup,
shared inputs, compiler acquisition and file preparation. Nested file phases
separate disk-budget admission/reclaim, cache and collection-lock waits, local
lookup (including hashing), eviction/space checks, victim presence checks,
blob HEAD requests, GET response headers (including server authorization,
queue wait and server verification), body download/local write, verification,
cache sync/publication, private work-directory placement, NAS inputs and final
work-directory sync/publication. Cache hits, misses and evicted copies are
counters; phase `calls`, `seconds` and `failures` include failed operations.
HTTP failures also count 4xx, 5xx and expired (410) responses.
They aggregate within an exact attempt, without file names or payload data.
A failed lookup can be an ordinary cache miss, not a staging failure.

Phases nest: verification is inside cache lookup, victim/HEAD checks inside
eviction, and all file phases inside `files`. Concurrent work can overlap.
Do not sum nested counters or call the whole interval network transfer time.
Unmeasured remainder includes dispatch, retries between preparation calls and
uninstrumented local setup; headers do not split queue-internal work. Environment
or checkpoint preparation after inputs-staged belongs to the later launch
interval. Legacy attempts have no phase evidence; timings never change scheduling. During
a rolling upgrade, workers retain timings locally and send them only after the
queue advertises staging phase support on its existing worker replies.

The rest of this page is a field-by-field reference.

With the worker/queue release containing #871's finalization correction, a
command that exits zero can still fail output validation or publication.
`execution_cause.finalization` identifies the step, declared relative output or
destination, sanitized underlying error and `command_exit_code: 0` when known.
`myce status JOB_ID`, job GET and attempt history retain that distinction.
`myce result JOB_ID --attempt N --out DIR` can retrieve a metadata-only
`summary.json` even without opt-in log/report capture. No worker root or raw
transport error is included. Retryable publication errors remain pending and
retain local results; their typed cause clears after successful publication.

Cold CPU probes report their initial reservation
in `memory_envelope.host_gb`, with known need and member/retry floors in
`probe_floor_gb`. A first run charged its floor (a non-production guarded CPU
first run on a worker advertising `first_run_floor_v1`; see
[scheduling](scheduling.md#first-runs-start-at-their-floor)) reserves only
that floor: its source ends `charged its floor, grows into measured room`,
`host_gb` equals `probe_floor_gb`, and it never waits on `cold_probe_margin`.
If it outgrows the room its worker's ledger leaves it, its
`execution_cause.rss_limit` limit is that room, and it requeues with
`memory_oom_floor` at its measured peak. A running probe's host admission charge can follow its fresh
complete cgroup footprint plus observed growth and launch drift instead of
reserving all initial headroom. Machine `host_memory.charged_gb` sums those
charges. A typed CPU job charged its learned figure (`charge_host_gb`) is
charged by use the same way, up to its typed limit; other declared and learned
allocations retain their full reservations.
Two timed samples establish a growth horizon; the measured rate extends that
forecast through the sample's age. Batch launches and model loads use the same
exact-attempt charge. Missing counters, old workers,
stale samples, restart and stopping custody restore the full execution ceiling charge.
This forecasts measured growth; it cannot predict unseen bursts. Kernel
ceilings, physical-memory rechecks and exact release remain independent.
RAM wait text labels `live available` as available RAM after operator reserve,
not memory in use. Linux reads `MemAvailable`, which includes reclaimable cache.

Unified CUDA attempt `memory_peak.gpu_host` can report
`source: CUDA proc physical upper bound`: `pinned_gb` retains exact-attempt
`VmPin`/`VmLck` plus outstanding whole-host GUP pins, and `physical_gb` retains
whole-host non-cache occupancy (with locked/dirty pages kept). `driver_gb`
keeps that absolute occupancy outside root-cgroup anon/shmem plus pins;
`outside_gb` also covers the sibling-adjusted host drop. These are conservative
upper bounds and can overlap device allocations, never evidence of a unique
physical union. The 200 ms collector must have complete, fresh cgroup/proc
coverage before `nonreclaimable_gb` and the GPU host range can calibrate an
envelope; missing fields retain total-cgroup sizing.
`memory_peak.device_sampling` records one exact attempt's driver sample count,
extrema, final reading, largest/last step, first/peak timing and maximum gap.
`complete: false` identifies missing, late or fallback coverage. These diagnostics
do not reduce `device_growth_gb`, relax fresh sharing guards or qualify coexistence.

`diagnostic_code: memory_evidence_recovering` (#850)
means declared requirements have an eligible target but the learned/exploratory
envelope does not. The durable queued job re-evaluates on evidence and capacity
changes. No attempt starts below its resolved reservation or retained retry
floor. A new clean comparable result may replace an invalidated class prior;
explicit impossible needs, incompatible targets and impossible per-job retry
floors still refuse. This wait grants no capacity or cleanup permission.

## Automatic host health (#834)

Machine `host_health` contains a typed `drain_reason`, recent `history`, the
healthy `baseline`, and CPU/RSS-leading `foreign` processes and service identities.
Samples include OS memory pressure, swap/growth, load, timer wake latency,
heartbeat duration, local free disk, and configured NAS/checkpoint metadata-probe
latency or pending age. `age_seconds` is age at the worker's last report; worker
freshness remains independent. `host-health.json` in worker state survives a
restart. `persistence_error`, sample `error` and `foreign_error` are fixed codes,
never raw errors or local paths.

The sampler reuses the five-second observation cadence and keeps 64 recent and
64 healthy samples to bound diagnostic memory and local writes. Eight healthy
samples bootstrap each host's prediction bands: quartiles plus three
interquartile ranges (Tukey's far-outlier fence), with observed adjacent changes
supplying a noise floor. These are statistical/history bounds, not fleet capacity
limits. Three consecutive adverse samples confirm degradation; three healthy
samples resume admission. Missing evidence breaks confirmation and cannot prove
recovery. Adverse samples never teach a new healthy baseline. Load requires
increased wake latency; heartbeat delay also requires local wake latency, so
ordinary useful CPU work or a slow queue alone does not drain a host. Memory
pressure, swap growth with wake latency, and low free disk use the host's bands.
On Linux, pressure is host-wide memory PSI `some avg10` (percent). From the
release containing #1110, PSI above its learned band alone does not drain:
it requires a local wake-latency outlier. Positive PSI with a swap-growth
outlier also drains, even within the learned PSI band. All outlier limits come
from this host's healthy baseline; there is no absolute PSI cutoff. A positive
idle baseline is not evidence that ordinary working PSI harms work. Without
corroboration, ordinary work can train the baseline and clear a retained drain
through the existing three-sample recovery rule. Queue-only heartbeat delay is
not local memory-pressure evidence. Darwin retains raw OS pressure levels
(`1` is normal) and its learned pressure-outlier rule; normal pressure plus swap
growth alone does not drain it. Persisted units are unchanged.

The queue also applies this correction to older Linux workers, including their
already-latched `host_memory_pressure` reports: three distinct, error-free
samples with swap growth and wake latency inside their learned bands clear the
effective drain while ordinary work continues. Missing calibration, short or
invalid history cannot establish recovery. Other drain reasons, stale-probe
holds, memory headroom/allowances and operator maintenance remain binding. No
worker restart is needed for this queue-side correction. The worker correction
also prevents the false local latch after a later worker upgrade.

Linux `host_health.pressure_evaluation` exposes `pressure_limit` (PSI percent),
`baseline_samples`, the original `reported_drain_reason`, `corroboration`
(`swap_growth` or `wake_latency` in the latest confirmation horizon), and
`recovery_samples` (up to three). `host_health.drain_reason` is the queue's
effective admission reason, used by host health and launch gates, including
production. Fewer than eight baseline samples means the limit is uncalibrated;
the reported drain is retained. These queue-owned fields are absent from worker
check-ins, preserving strict older-queue compatibility.
Disk pressure also requires exhaustion against measured consumption over the
confirmation horizon. `disk_recovery` retains that measured reserve until space
actually recovers; aging history alone cannot clear it. Pending adverse mount
samples (`adverse`) are excluded from healthy baselines before confirmation.
History serialization/fsync runs outside the mutex used by heartbeat snapshots.

Workers send the additive health fields only after the queue advertises
`host_hygiene_protocol: host-hygiene-v1`. The additive operation evidence also
requires `host_mount_progress_protocol: mount-progress-v1`; v1-only queues still
receive the original health and collection payloads. A strict older queue receives the
legacy shape; local evidence still persists and unknown health or a confirmed
local drain uses reversible availability withholding. After a queue rollback the worker retries the legacy shape once
after a strict-decoder rejection, preserving session and attempt custody.

Drains withhold new command, model and preparation launches without changing
operator maintenance, killing running work or discarding exact-attempt custody.
`host_degraded` alerts carry the typed cause; existing stale-worker alerts carry
`reason: host_silent`. Admission withholds at once, but `host_degraded` and
`host_unhealthy_or_stale` open only when the cause outlasts the host's own
measured evidence window (the worker's probe `valid_seconds`, which covers its
report period, cost and measured delivery variation; 15 s for evidence without
one, including the first window after a queue restart). A report a moment late
no longer opens an alert (live r6.10: 12,463 `host_probe_unknown` openings in
four days, median 3 s apart on one host whose evidence lasts 4.6 s, #1404). The
first clear observation closes it. Existing notifier episode/cooldown and delivery custody
apply. Operator drains still require their owner's resume.

Mount probes inspect configured root metadata without traversing or remounting
it. A blocked syscall remains the single owned outstanding operation for that
root, off the heartbeat path. Pending age and completion latency above the
host's learned band are advisory (`mount_probe_slow`), with a deduplicated alert
and measured evidence; successful slow operations do not withhold admission.
`operation_age_seconds` measures time since the last completed filesystem or
inventory operation. No progress for the existing measured per-operation
allowance (`stall_allowance_seconds`, #234) confirms a stall and withholds only
dependent work. Samples also retain `slow_threshold_seconds`. Independent local work stays eligible. Cold roots retain existing
checkpoint and mount-inventory availability rules. No filesystem I/O runs in a
queue transaction. Missing pre-hang evidence cannot establish an earlier hang's
cause; native OS/mount qualification is separate from fake-clock proof.

From the owned-NAS fault follow-up, staging and checkpoint I/O also feed this
same mount health lane. Exact read, reopen and filesystem errors on a configured
network backing latch `mount_probe_failed` for dependent roots. One record per
backing holds the generation, recovery counter and one owner on an existing
probe lane; sibling roles project the same state. Owned I/O, probe and recovery
faults reset that record. An older in-flight completion cannot clear it. Data faults
require three fresh read/reopen completions after the latest fault. Metadata,
cached probe results and incomplete requests cannot recover data access.
The worker dependency resolver reads the live latch before placement/start
checks. Batch starts also check at the final hand-off after the queue reply. An
earlier healthy heartbeat or successful preparation retry cannot bypass it.
Models use the same resolver at their existing pre-authorization engine gate;
their Worker captures the shared health owner before model runtime creation.
Separate learned backing mounts remain independent even when one mountpoint
is nested beneath another.
Typed operation/errno/time/generation evidence and recovery source custody stay
in worker memory; paths are never sent in status. **Known limit:** a worker
restart forgets the fault latch. The first NAS-dependent attempt after restart
may hit the fault again and re-latch it. An operator exclusion remains independent;
only its owner removes it after live verification. Durability is deferred until
an observed post-restart repeat failure (#894). Storage faults do not recount
cached host-pressure measurements. No consumer process or kernel log is inspected.

Foreign CPU/RSS leaders (up to five of each) refresh once per minute to keep
enumeration out of the heartbeat path. Only executable basenames and available
service identities are reported, without argv or environment values. The worker
tree and birth-verified recorded surviving attempt members are excluded. This
is owner context only; it causes no signals, service changes or cleanup.

With `host_foreign_protocol: host-foreign-v2` (#834), each foreign process also
carries `user` (account name), `age_seconds` (elapsed since start) and, on hosts
whose GPU driver attributes per-process memory, `gpu_bytes`. Memory leaders rank
by RSS plus `gpu_bytes`; unified-memory device allocations need not appear in
RSS, so the two are reported separately. `foreign_services` totals every
foreign process per service or container identity (systemd unit or scope,
Docker container, or the nearest launchd-labelled ancestor on macOS): process
count, CPU, RSS, GPU bytes, and the oldest member's user and age, up to five CPU
and five memory leaders. Read these in `myce host status HOST` to see why work
waits on a host whose memory is held outside Mycelium; the owner decides what to
do. The GPU query runs off the sampler lane, one at a time, and a failed query
reports no GPU bytes rather than a stale value. Older queues receive the
original foreign shape.

Run `myce status` using your usual queue address and private token. `--full`
shows full job IDs. Open the queue address in a browser for the same information;
sign in as `myce`, using the token as the password. The page is read-only and
refreshes every five seconds. Keep tokens in private configuration, not URLs.

Every status view (the page, default, `view=snapshot`, sections, history pages
and `detail=full`) and every `GET /api/jobs/{id}` is projected from the queue's
last committed publication. Each successful commit (every check-in, submit and
the one-second maintenance tick) publishes an immutable snapshot of its records
and observation context; accepted logs and startup configuration republish it
too. Readers never acquire the live queue mutex and never wait for a writer, so
a burst of status or job reads cannot delay submits, check-ins or scheduling.
Unknown or ambiguous job IDs are answered from the same publication. Because a
build owns no live queue work, default status and job receipts do not enter
control-pressure admission. Legacy scalar wait admission and the independent
batch-wait index retain their existing behavior. Newly published records are
decoded once in the background, off the queue lock and outside every request,
so concurrent reads do not each charge the same retained history. A history page
copies only its returned rows; other retained work contributes its shared
immutable summary. Response copies still consume the queue's measured memory
allowance: `detail=full` still returns (and charges) all retained history.

The page and `GET /api/status` share a cached response for a lease learned from
local build/hit costs and mutation intervals. Job receipts use the same
cost-based policy with their own generations. Cache metadata locks cover only
completed-response metadata, never a rebuild. Negotiated default-status reads
reuse a published page when its history rows fit the peer's page-planning
allowance. Status snapshots do not drive start authorization, cancellation or
result publication. Terminal job commits and their release acknowledgements
publish a new terminal fence and invalidate the shared response. If one is
published while status is projecting or waiting for usage/file-store I/O,
status projects the newer publication before returning, including uncached
filtered and full views. Discarded response allocations are released before
retrying rather than accumulating as if every discarded generation were still
in flight. Ordinary worker check-ins do not invalidate the cache. Snapshot `now`
is the injected queue time when the read started, rather than the response
completion time.

Every status and inspection response, and every job receipt, includes additive
`freshness` metadata: `captured_at` is when the committed state it projects was
published; `served_at` is the queue time when the response is assembled;
`age_seconds` is their nonnegative difference; and `cached` says whether this
request reused a previously completed status or receipt generation. The age
therefore covers time since that commit as well as projection and observation
I/O. With an active fleet it is normally about a second or less; a large age
means the queue has not committed recently, so interpret contact and progress
at `captured_at`. It excludes JSON encoding and network transfer after assembly.
Old CLIs continue to decode the existing fields.

From s072, machine `shared_input_cache` reports `capacity_bytes`, `free_bytes`
and `evictable_bytes`. Free space excludes cache allocation and outstanding
staging reservations; evictable space contains only inactive reproducible entries.
From #1061, automatic capacity follows current headroom during admission and the
existing worker retention sweep. Free bytes exclude unwritten reservation bytes,
without charging already written prefixes twice. Inactive resumable prefixes are
evictable; a detached writer keeps its build lock and remaining-byte reservation.
Explicit cache ceilings and the filesystem control-space floor still bind.
`protected` maps cache identities to allocated bytes held by readers/custody, so
placement can reuse an already held input without charging it twice. Check-ins
copy an in-memory snapshot; cache discovery does not run under the queue mutex.
Workers without the optional report retain staging-time admission. Temporary cache pressure appears
as job `diagnostic_code: shared_input_cache`, with byte comparisons in `reason`.
Resolve/submit return `shared_input_cache_capacity` only when all enabled workers
under consideration report capacity and none of the matching targets can hold
the job. With no matching worker yet, the job waits with
`diagnostic_code: shared_input_cache_unreported`. Reporting workers retain
queue-side checks during a rolling upgrade; older workers enforce actual cache
capacity when staging.

Machine `cpu_extension_fingerprint` identifies the CPU qualification identity;
device entries carry their own extension fingerprints and reports. The scalar
`extension_fingerprint` and reports retain a first-device compatibility view.

Extension report `reason` and status displays (#891)
say `not yet checked on this device; run the extension check` when no matching
work/device record exists for the installed version. Existing records distinguish
`hardware/driver fingerprint changed`, `extension fingerprint (digest) changed`,
and `hardware/driver fingerprint unavailable` (current or recorded identity is
missing); these require setup and check. `fingerprint` is the hardware/driver/OS
qualification hash; `digest` identifies the extension contents. The text is a
human diagnostic, not a machine code; `setup` and `qualified` remain the stable
boolean verdicts. No record does not establish a hardware change.

From the release containing #1120, default `GET /api/status`, `myce status`
and `myce status --json` are compact. They contain all nonterminal jobs and
terminal jobs with outstanding cleanup/delivery, current machine capacity and
availability (including the latest health sample and active preparation identities), host/device holds, current
fleet upgrade, and `summary.alert_groups`.
`summary.retained_jobs` counts all retained jobs, including calls.

From the release containing #1267, the default is a few
KB. Replaying the 2026-11-11 live fleet snapshot (five hosts, 67 current jobs,
49 active alert conditions) through it took plain `myce status` from 33,924
bytes / 267 lines to 3,668 bytes / 31 lines. From the release containing the
compact JSON change, default `myce status --json` encodes the same projection
(below): replaying a 68-job snapshot of the same fleet took it from 156,292 to
7,542 bytes. From the release containing the compact API change, plain
`GET /api/status` and Python `client.status()` return the same compact view.
The compatibility path `GET /api/status?view=snapshot` (Python
`client.status(snapshot=True)`) returns the summary snapshot with full current
job and machine objects; it shares the default's one-second cached capture.
`myce leave` uses it. Any of `sections`, `jobs`, `limit`,
`before` or `detail=full` also returns a snapshot, as before. Other `view`
values are refused with `status_query_invalid`. `project`/`since` filters alone
keep the compact view. Previous-release queues ignore `view` and return the
snapshot for every read; the CLI projects that snapshot itself.

- `summary.alert_groups` folds active conditions into one row per kind:
  `kind`, `count`, earliest `since`, and `scopes` (members per host or
  `project:NAME`). `efficiency/<subject>/<metric>` and `reports/<host>/<code>`
  are kinded by metric or code; other keys by their first segment. Hash-like
  subjects (work profiles, targets, recipe identities) are counted, not named.
  Members are named by `myce efficiency` (efficiency signals) and by
  `--sections machines,jobs` (recipe, report and target conditions). This
  replaces the flat `summary.alerts` list: 18 work profiles with one prediction
  regression were 18 rows.
- Replaced per-card worker identities (`replacement` set) are history: only
  `--history` (`detail=full`) lists them; the default and `--sections` omit them.
- Plain `myce status` prints one line per hypha, one line per running, waiting
  or cleanup job with its committed reason (bounded to 160 characters), and
  one line per paused project and reason with a count. Host states print only
  when not `healthy`, a completed fleet upgrade is omitted, and a worker release
  prints only when it differs from the queue's. Estimates that are unknown,
  capability profiles, caches and devices stay in `--sections jobs,machines`.
  `myce status JOB_ID` prints the job's one current reason and `next:` action
  on its line, plus attempt history; `myce status --history JOB_ID` adds the
  hold's evidence, the isolation note, per-host variant rows and the priority
  advisory.
- Default `myce status --json` is that compact view as one object. Text and
  JSON come from the same projection; JSON keeps reasons whole and full IDs:

  | Field | Content |
  |---|---|
  | `now`, `queue_release`, `retained_jobs` | Capture time, queue release, all retained jobs including calls |
  | `counts` | Current jobs by `running` (includes stopping), `waiting`, `paused`, `other` |
  | `alert_groups` | As `summary.alert_groups` |
  | `health_alert_delivery` | Present only while alerts await delivery |
  | `fleet_upgrade`, `host_states`, `holds`, `warning` | Unfinished upgrade lines, non-`healthy` hosts, one line per reservation, exclusion, disabled device or host hold |
  | `production_leases` | One row per open production lease: `project`, `key`, `expires_at`, `hosts` (hosts able to start a declared stage, which planned drains and fleet upgrades keep one of enabled). Text: `production lease PROJECT/KEY: expires T; holds HOSTS` |
  | `not_quiescent` | One row per host that a drain, disable or the active fleet upgrade step is waiting on (`quiescent: false` in `myce host status`): `host`, `mode`, `attempts[]` (`job_id`, `name`, `attempt`, `state`, `reason` = stop cause) and `other` (stale workers, cleanup, calls, sidecars, preparations, admission still enabled). Text: `not quiescent HOST (MODE): job ID NAME attempt N STATE; ...` |
  | `hyphas[]` | `id`, `gpu`, `free_memory_gb`, `availability` (the status sentence), `host_ram_for_admission_gb`, `free_cpu_cores`/`cpu_cores`, `running` (current jobs placed there), `release` (only when it differs or is unknown), `preparing`, `copying` |
  | `models` | One line per loaded model |
  | `jobs[]` | Each running, waiting or cleanup job: `id`, `name`, `project`, `class`, `state`, `attempt`, `machine`, `since` (waiting since or started), `reason`, `delivery` |
  | `paused[]` | One row per project and reason: `project`, `reason`, `count`, `ids` |
  | `detail` | Where the full objects are |

  Full job and machine objects need `view=snapshot`, `status JOB_ID --json`,
  `--sections jobs,machines --json`, `--jobs ...` or `--history`; those
  return the snapshot shape below.
`health_alert_delivery` retains the notifier configuration and pending count.
No installation-sized row or byte cap hides current work.

The default captures maintained current facts instead of materializing the full
retained generation, rebuilding evidence catalogs/placement plans, reading usage
SQL, or traversing retained file references. Job reasons are the last committed
reason; a generic capacity wait means no detailed reason was committed. Queue
order is unknown unless recorded. Use `status JOB_ID` or `sections=jobs` for a
fresh placement explanation. Runtime/progress ages and machine contact freshness
still use the captured clock. These observations grant no launch or release authority.

Retained diagnostic fields are absent or empty/null placeholders in the default
JSON, **not zero evidence**. `summary.diagnostics` identifies that projection.
Opt into comma-separated sections with `?sections=work,storage` or
`myce status --sections work,storage --json`:

| Section | Existing detail restored |
|---|---|
| `jobs` | Complete selected job status fields, attempts, admission/evidence, eligibility and fresh placement diagnostics |
| `machines` | Complete machine status, health history/baselines, inventories and preparation reports |
| `storage` | `storage_observations`, `file_store` (including retained-file accounting) and `checkpoint_expiry` (settled NAS checkpoint folders `waiting` for their grace period or a worker, and `undated` ones kept for lack of any timestamp; see [files](files.md#resumable-jobs)) |
| `work` | `work_curves`: one record per kind of work, with its last accepted duration and accepted peak |
| `health` | `health_alerts`: one record per open or recently closed problem (kind and subject), independently of current `summary.alert_groups` |
| `efficiency` | Complete `efficiency`, including retained per-job rows and predictions |
| `usage` | Complete `usage`, including daily rows, preemptions and production starts |
| `diagnostics` | `request_latencies`, `evidence_retention`, and retained `admissions` |

Current fleet metadata (release, policy, holds, model slots/pins and sync requests)
keeps its existing fields. The default omits released reservations and inactive
exclusions; an explicit diagnostic read retains their audit records. Section
reads may perform expensive diagnostic planning, even when returning a subset.
`?detail=full` restores **every existing field and every retained job**, including
all sections and audit metadata, with the existing public redaction contract.
CLI `--history` and Python `client.status(full=True)` use that preservation view.
Keep it explicit for release-preservation snapshots; the default is not a ledger.

Job selection remains independent of section selection:

- `?jobs=active` is the default: all current/cleanup-pending jobs.
- `?jobs=recent` adds a capacity-sized terminal history page to current work.
- `?jobs=all&limit=100` pages all retained jobs, newest submission first. Append
  `&before=NEXT_BEFORE` until `jobs_page.next_before` is absent. `limit` is positive
  and bounded by negotiated capacity; omission uses measured row costs, not a
  fixed installation-sized default. See [request budgets](request-budgets.md).
  `myce status --jobs all --limit 100 --before CURSOR --json` exposes the same
  cursor. History modes retain complete selected job status rows.
- `?project=example&since=2026-10-30T00:00:00Z` filters jobs by project and inclusive
  submission timestamp. Keep mode/filters unchanged when paging. An expired or
  unknown cursor returns HTTP 400 `status_query_invalid`; restart the listing.

`jobs_page` ([schema](schemas/status-jobs-page-v1.schema.json)) reports the
filtered selection's `total`, `returned`, `mode`, and optional `next_before`.
Absence from default status does not prove completion or loss: read known IDs or
use one scalar batch `POST /api/wait` / `myce wait` / Python `wait_many()`.
Completion observers continue to use that batch endpoint, not fleet polling.

Python `client.status(sections="work,storage")` selects detail;
`client.list()` still walks capacity-bounded `jobs=all` pages. Older queues ignore
unknown section parameters and return their older larger default. `full=True`
and `detail=full` continue to provide complete inspection across the supported
adjacent release pair.
The HTML status page also accepts explicit sections, for example
`/?sections=jobs,machines,usage`.
`myce host status` uses its separate host-maintenance endpoint and retains stdout
JSON even when inventory is degraded.

### Retained-queue measurement (#1120)

A synthetic laptop fixture has 8,000 retained jobs, eight running jobs, sixteen
workers, and 8,000 independent storage, work, health-event and efficiency rows.
`BenchmarkStatusRetained` measures warmed builds and cache hits over 30 iterations
with `GOMAXPROCS=2`. These are ordinary uncompressed JSON bytes and local shared-host
measurements, not production measurements, gzip transfer sizes or timing gates.
The before run was captured before the implementation; the after run uses the
compact projection. Sample percentiles describe this small run (p99 uses the
lower observed rank), not a service latency guarantee.

| View | JSON bytes | Mean ms | p50 ms | p99 ms | Allocated bytes/build | Allocations/build |
|---|---:|---:|---:|---:|---:|---:|
| Before: refresh | 19,593,648 | 252.16 | 251.27 | 267.19 | 316,525,999 | 306,129 |
| After: refresh | 28,166 | 4.77 | 4.75 | 5.23 | 552,573 | 1,148 |
| Before: cache hit | 19,593,647 | 6.17 | 5.67 | 8.36 | 19,651,332 | 79 |
| After: cache hit | 28,165 | 3.19 | 3.15 | 3.83 | 84,750 | 78 |

For process CPU, the same after run replays the legacy projection with all
sections and the old recent-job selection, reproducing its 19,593,648-byte
response: 287.21 ms CPU/build versus 2.94 ms for compact refresh (1.39 ms for a
compact cache hit). That replay is a CPU comparator, separate from the original
before measurement. The retention regression compares eight versus 8,000
retained rows with identical current work: wire size changes only with the
retained count's digit width, and construction allocations stay independent of
those retained diagnostic collections. Explicit section reads remain expensive.

Reproduce with:

```sh
GOMAXPROCS=2 go test ./internal/myce -run '^$' \
  -bench '^BenchmarkStatusRetained$' -benchtime=30x -count=1
```

Memory-only accepted samples increment `samples` and `memory_samples` and advance
`memory_recorded_at`; they leave the last accepted duration unchanged. A
failed ceiling-bound attempt can invalidate comparable memory on several hosts
without adding a sample. Each rewritten key has its own `memory_invalidated`
ledger link, including a replacement diagnostic peak with an already-zero memory
clock. The preservation guard requires that exact key's complete digest chain;
another host's accepted sample alone never explains its mutation (#620).

Only the unfiltered default shares the one-second cache. Filtered/full reads are
fresh. Status and job reads (#1052) take an immutable committed
input generation plus worker contact/private scalar facts under the queue lock.
The commit journal publishes changed encoded records through a persistent tree;
unchanged records and decoded inputs are shared across revisions. Traversal,
decoding, selection, file-reference/cancellation walks, capacity accounting,
recursive copies, projection, usage queries, file inventory, reply encoding and
compression run after releasing the live lock. Selected started/terminal receipts
decode only their own record and policy; queued receipts share live context,
without materializing cold execution payloads. Cold decode capacity is charged
before allocation. Each detached generation reuses one placement scan and
memory-evidence planning cache across ordering, startup and blocker diagnostics,
including a queued single-job read; derived capacity views keep their own scans. No bulk file or network I/O runs
under `q.mu`. No jobs, curves, custody or foreign-load fields are removed.

Jobs with explicitly recovered outputs expose an append-only `recovered_results`
list in status and job GET. Each record has `kind: recovered`, its own result ID,
exact failed execution, recovery-time verification identities and operator
provenance. CLI status lists these separately and keeps the original failed
state. Ordinary `output_refs`, attempt outcomes, cleanup causes and dependency
eligibility remain unchanged. See [recover failed-attempt outputs](files.md#recover-outputs-from-a-failed-terminal-attempt)
for the explicit recovery/download commands and independent byte retention.

Job GET and status job rows expose a privacy projection in `spec`, not the
submitted spec. They include `spec_projection: {"projected": true, "omitted": [...]}`.
`omitted` lists paths relative to `spec` that the projection strips, regardless
of whether those fields were submitted. In particular, absent `spec.files`
**does not mean no files were submitted**. The same metadata is available through
Python `Client.get()` and `Client.list()`.

On releases with #775, `spec.authoring_origin` records the reader used at first
acceptance: `profile`, `adapter`, `job_spec` (explicit YAML/JSON or Python YAML),
`command` (one-off CLI), `extension` (Go/Python extension run helper), `python`
(mappings built with the Python client), or `api` (otherwise unmarked new
API/SDK submissions). Generated qualification jobs are
`extension_check`; generated fan-out execution records are `fan_out_child`,
while their parent retains its reader label. Count those separately from
independent submissions. CLI status and the status page show the same label.

These fixed labels carry no YAML, argv, paths, inputs or keys. They are diagnostic
reader markers, not authenticated proof of a client's implementation. Origin is
immutable after acceptance, excluded from keyed submission fingerprints, and
survives restart. A replay through another reader preserves the original label.
Historical records without the field read as `unknown`; no backfill is inferred
from names or payloads. Older clients that do not send a reader marker appear as
`api` for new submissions, so retirement audits must also verify caller upgrades
and cutover. This field does not measure extension registration events.

The projection strips these entire fields: `after`, `args`, `diagnostics`,
`disk`, `environment`, `execution`, `expected_minutes`, `extension`,
`extension_digest`, `fan_out`, `files`, `give_up_at`, `inputs`, `isolation`,
`output_delivery`, `outputs`, `prepare_source`, `profile_memory`,
`reason`, `require_target`, `requirement_reasons`, `run`, `shared_inputs`, `size_hint`, and `targets`.
Status rows additionally strip `submission_key`; job GET retains it. Within `program`, it strips `args` and `file`, retaining only
`type`. Within `extension_check`, it strips `automatic`, `directory` and
`reserve`, retaining only `work` and `timeout`. Some stripped fields encode
empty or zero placeholders because of their JSON types; those placeholders do
not preserve submitted values. Other retained fields still follow normal JSON
`omitempty` rules. This metadata does not expose commands, environment values,
file references or credentials. There is no full-spec read mode.

Job `advanced` (MY-54) is the read-only view of the job's
[advanced settings](jobs.md#advanced-settings): it holds only the scalar
settings that differ from their defaults, such as `allow_cpu_fallback`,
`resumable`, `isolation`, `wall_time` or `start_window`, and is absent when
none do. Disk, size, resume-compatibility and failure-evidence payloads and raw
`requirement_reasons` text stay out of it and keep the redaction above. While
an opted-in GPU job's attempt runs on a CPU target, `spec.needs` shows the CPU
reservation and `cpu_fallback_from` keeps the authored GPU needs.

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

Running job reads and status rows include `liveness`, also retained on the exact
entry in `copies` (attempt, machine and session). **Available in the next fleet
release:** this worker observation requires no application reporter, log output
or checkpoint. `state` answers one of:

- `advancing`: a fresh live process tree has increased CPU time, or its managed
  work directory has grown while the process tree is still observed alive.
- `idle`: both signals have fresh, comparable, unchanged samples;
  `idle_seconds` is the shorter continuously observed inactive interval, through
  the latest samples. CLI/page wording is “alive but idle for N min”.
- `unknown`: old worker, no baseline pair, unavailable process tree, failed or
  incomplete directory measurement, counter reset or stale observation. Unknown
  never means idle. A fresh advancing CPU signal suffices when directory size is
  unknown; directory growth alone cannot establish a live process.

`cpu.value` is cumulative CPU seconds; `work_dir.value` is logical regular-file
bytes below the managed attempt directory. Each signal has `age_seconds`,
`compared`, `advancing`, `idle_seconds`, and optional `unavailable`. Absent `value`
means no successful measurement, so its zero age is not a fresh reading. Ages use
worker-measured elapsed time plus queue-clock time since `received_at`, without
synchronizing the two clocks. Cached check-ins do not renew samples. Evidence
expires after 30 seconds (six normal sample cycles); a failed measurement breaks
idle continuity immediately. Past/released attempts retain their last values
and ages with an unknown current state. New attempts reset job-level evidence;
stale reports/results cannot overwrite it.

The worker samples no more often than every five seconds, separately from its
control loop, and sends results on its existing check-in. Linux uses a populated
attempt cgroup's aggregate `cpu.stat` when available. Native macOS/Linux otherwise
use `ps` CPU-time counters for the attempt anchor's group and descendant tree,
with checks against the anchor birth identity recorded at launch, before and
after each inventory. A reused PID cannot supply activity after its owned
anchor exits. Membership changes or decreasing counters break
idle comparisons; positive CPU advance remains useful. Short-lived processes
between samples and detached descendants no longer connected to that tree can
be missed. Recovered processes without a live anchor report CPU unknown. No
worker process is substituted for payload CPU.

Directory measurement uses metadata only, never payload reads or symlink
traversal, and excludes writes outside the managed attempt directory (including
NAS checkpoints and shared caches). Each scan is limited to 4,096 entries,
64-entry directory-read batches, and a cooperative 25 ms deadline: at most 0.5%
of the five-second cycle spent walking locally. The entry bound also caps work
on fast, very large trees; partial totals are unknown, never unchanged zeros.
A blocked OS call can exceed the deadline, but the single background collector
retains its slot until it returns, cannot block check-in or launch overlapping
scans, and its last observation ages to unknown. CPU probing has a two-second
command deadline plus a one-second pipe-drain allowance. All collection runs on
the worker, outside `q.mu`; the queue only retains small fenced values and
projects status. These are activity signals, not proof of useful progress,
completion, a hang, or permission to kill/retry a job; no scheduling or retry
policy consumes them.

`myce status` and `GET /api/status` report `queue_release` (`version`, `commit`)
from the queue process. Each machine's `release` reports the worker process's
version and commit from its check-in. Missing worker releases mean unknown
(older workers); they are never inferred from the queue release. Release builds
embed the source commit even with Go VCS stamping disabled. Development builds
use Go's VCS revision when available, otherwise `unknown`.

Worker CPU and I/O telemetry appears on job GET and status in
`resource_attempts`: exact attempt, machine, session and `resources`. This retains
the first and latest 31 attempts like memory facts; `resource_attempts_dropped`
counts evictions. Running heartbeats and final results update the same row, so
lost attempts retain the last observation. Missing rows/counters mean unknown.
These observations do not change CPU or I/O admission.

`resources.cpu` reports cumulative `usage_seconds`, and Linux cgroup v2 also
reports `user_seconds`, `system_seconds`, `periods`, `throttled_periods` and
`throttled_seconds` where the controller exposes them. The parent includes all
member cgroups once. macOS uses custody-fenced process-tree CPU inventories,
retains exited observed lifetimes and supplements the root with exit rusage.
`source: sampled_process_tree` and `lower_bound: true` identify its limits:
short-lived unseen children and CPU between observations can be missed. Missing
user/system or throttling counters on this path remain unknown.

`resources.io` reports summed cgroup `read_bytes`, `write_bytes`, `read_ops`,
`write_ops`, and `pressure.some` / `pressure.full` PSI (`avg10`, `avg60`, `avg300`
percent and cumulative `total_usec`). Device identities are omitted. An empty
Linux `io.stat` is a measured zero; absent/incomplete files are unavailable.
Machine `io_pressure` reads host `/proc/pressure/io` on every heartbeat. No
comparable host/attempt PSI or cgroup block-I/O attribution is implemented on
macOS: it explicitly reports `unavailable: unsupported_os`. Other unavailable
reasons distinguish missing local cgroups, failed reads and invalid counters.

Worker `sampled_at`, worker `age_seconds`, and queue `received_at` distinguish
sample time from receipt without assuming synchronized clocks. Host status ages
the reading after receipt. Attempt `elapsed_seconds` spans its observer lifetime,
including teardown; its counters include wrapper/preparation work in the attempt
boundary. The final result is sampled before the cgroup disappears. An unavailable
final CPU read retains its last observed value as an explicit lower bound.

The `X-Myce-Resource-Telemetry: attempt-resources-v1` response header negotiates
support. New workers initially omit additive telemetry JSON; after negotiation
they send it on check-in and result. Strict old queues, including a downgrade,
receive the old shape, with local buffered results preserved. Old workers keep
working with new queues; their missing telemetry remains unknown.

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

Queue `file_store.projects` reports per-project retained and 24-hour added
bytes, limits and any quota refusal; see
[per-project quotas](reference/file-store.md#per-project-file-store-quotas-464).

Queue `file_store.collection` counts published-blob collection since the queue
process opened its store: `passes`, `yielded_passes` (stopped after a batch
because a control request was waiting on the queue), `deferred_passes` (an
upload or publication held the file fence), `reclaimed_blobs`,
`reclaimed_bytes`, `pending_candidates` left by the last pass, and
`last_reclaim_at` (queue clock). Every pass collects at least one batch before
it can yield, so rising `passes` with flat `reclaimed_bytes` and nonzero
`pending_candidates` means collection is stalled
([#1375](https://github.com/1kuna/mycelium/issues/1375)).

Queue `file_store.physical` is shadow accounting for #506. `disk_free_bytes`
comes from the physical probe; `remaining_upload_bytes` counts only unwritten
tus lengths, and `reserved_output_bytes` counts existing output commitments not
already covered by those lengths or stored blobs. `uncommitted_bytes` is their
saturating subtraction from physical free space. Existing payloads, control files
and allocated backing already reduce physical free space and are not deducted
again. This upper bound ignores unqualified expansion and is not admission credit.

`control` measures allocated database, WAL and shared-memory bytes, counting
hardlinks once. Job/environment logs live in SQLite. `upload_metadata_bytes`
measures existing tus metadata allocation. These are current working-set
observations, not measured future sync/checkpoint headroom or an enforced floor.
`sampled_at` uses the queue's injected clock at status capture. Errors are sanitized
and set the shadow uncommitted value to zero. No filesystem paths are published.

`mode` remains `shadow` and `protected_free_bytes` remains zero: native denial,
complete writable-path confinement, measured/enforced control expansion and
independent worker-custody backing remain unimplemented/unqualified. No setting
activates automatic admission here; the configured cap/reserve and legacy disk
floor still govern writes until #509's explicit migration.

The native protection/migration and shadow-admission proposal are parked under
[#564](https://github.com/1kuna/mycelium/issues/564). `file_store.admission` is no
longer emitted. Retained policy fields remain readable but do not activate
admission or disable the configured cap/reserve. Storage learning below remains
observational.

A typed quota denial is `disk_limit_exceeded`. It fails that exact attempt even
if a stop races it, without spending an infrastructure retry or classifying the
denial as memory OOM. Larger execution needs fresh storage admission. An ordinary
`ENOSPC` remains `storage_full`; stderr and exit codes alone do not prove a
contained quota denial. This diagnostic support does not qualify native quotas.

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
Work dir plus `TMPDIR` scratch also charges the data volume, as the larger of
outputs and scratch because outputs are written inside the work dir. Scratch is
the accepted managed-storage scratch peak plus its measured margin for the same
storage key (see learned storage below), or the optional `spec.disk.scratch_bytes`
hint before such a peak exists; a measured peak always replaces the hint.
Workers report each ready environment's allocated size at the end of its build
(`machines[].environment_bytes`). A job whose environment key a host has not
cached charges that host's environment volume with the largest size any host
reported for the key. Likewise workers report held model snapshots
(`machines[].model_snapshots`) and learned snapshot sizes
(`machines[].model_snapshot_bytes`); a declared snapshot a host does not hold
charges its `file-cache` volume with the largest learned size. A hold reads
`disk_headroom: host H ROOTS disk: N GiB needed, M GiB free on H; free=… floor=… need=… total=…`.
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
or unreported scratch/archive expansion. Learned scratch peaks are keyed by
storage identity, so another machine type or changed configuration falls back to
the hint until it has its own accepted run.

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
remains network-capable. A sandboxed online job is eligible only on `native_network`
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

Unsandboxed native commands (including plain commands under the MY-25 default)
do not require `native_network`: they have no kernel network confinement to
qualify. `needs.network: true` still declares outbound intent. This does not
grant protected egress or change a frozen extension's isolation policy.

Queued reasons explain network, environment preparation,
GPU card contention and RAM envelope blockers per compatible target. Card waits
say `device N shared with K attempts` or `card busy: waiting for GPU batch slot`,
and name the holding jobs and tiers (or retained custody awaiting exact release).

With `device-activity-v1` support, discrete XPU inventory includes optional
`devices[].telemetry`: `free_gb`, `active_percent`, `at`, `age_seconds`, and
`source`. Missing counters mean **unknown**, including `xpu-smi` N/A. Memory
comes from a PCI-identified `xpu-smi --query-gpu=pci.bus_id,memory.free` CSV
query. Xe activity is the busiest hardware engine's active/total PMU tick
ratio across the complete `DRM_XE_DEVICE_QUERY_ENGINES` inventory, sampled over
the existing memory-sensor interval. It includes foreign engine activity but
does not attribute that activity to jobs or predict a sharing speedup. Reads
use the worker's existing permissions; no settings, driver or privileges change.
An unsupported, multiplexed, incomplete or nonadvancing counter remains unknown.
Sensor ages are rebased onto queue time; cached inventory cannot refresh them.
Strict older queues receive the existing inventory shape after negotiation.

With `device-health-v1` support (#1185),
`devices[].quarantine_recovery_blocked` reports the last rejected health proof:
`code`, optional `component` or `exit_status`, and worker `observed_at`.
Only sanitized classifications are reported, never raw probe text or paths.
`health_probe_monitor_gap` rejects a query begun before a kernel-monitor gap;
`health_probe_comparison_stale` rejects target/sibling evidence exceeding the
comparison horizon. A sibling query never renews the target's `observed_at`.
A healthy reading clears this diagnostic but must still establish the recovery
window. `devices[].unsupported_on_device` lists canonical required components
excluded by a fresh same-batch healthy same-model sibling comparison (for
example `memory`). The list accompanies a durable automatic-clear receipt.
Unknown without that comparison, and every missing or failed required
component, retain quarantine. See the orchestrator default in
[DECISIONS.md](DECISIONS.md).

With `device-recovery-v1` support (#1142), `devices[].quarantine_recovery`
records an automatic clear's fault hash, healthy interval, observation window,
vendor probe and queue-owned transition receipt (`observed_at`). Older queues
receive inventory without this optional field. `attempt_history[].device_fault`
is queue-owned host/backend loss evidence, and `hold.kind:
device_fault_last_healthy` explains a held retry. See
[device quarantine](reference/accelerator-quarantine.md#accelerator-driver-fault-quarantine) for
recovery conditions and mixed-version behavior.

Fan-out execution may share a discrete XPU card only when every tenant opts in
with `fan_out.share_xpu: true`; omission or `false` preserves existing admission.
The declaration is frozen as `batch.share_xpu` on each executing child, including
retries, after its `spec.fan_out` is stripped. Sharing also needs fresh host/card
memory and activity evidence, unsaturated engines, and fresh exact-attempt host
and device samples for every started holder. Missing evidence keeps new work
exclusive. This adds no host allowance or numeric concurrency cap; declared or
accepted learned memory, same-tier isolation, physical headroom and exact
release still bind. The existing growth guard also projects fan-out device
growth through the observation horizon and stop grace. Native throughput and
output parity require separate matched measurement before production opt-in.

A RAM figure on another target cannot hide that card contention. A RAM wait gives the resolved
envelope and that target's free host memory; an unrelated target's reservation
cannot explain the wait. Shared-host preparation refusals explicitly say the
environment is not prepared and that the host never builds environments.

`host_capacity_accumulation` names a ready queued job whose host RAM or cores
would otherwise be repeatedly consumed by smaller backfill (#787). This
also covers CPU-only demand. New work behind the demand in
tier/fair-turn order waits on its selected host unless it fits beside the
demand. Existing attempts finish normally. The projection
selects one currently ready target per demand, prefers immediate capacity, and
otherwise uses ordinary placement value. It retains current device capacity,
environment, qualification, cache, maintenance and liveness requirements; cold,
impossible or card-blocked demand does not withhold CPU capacity. Cancellation
or loss of readiness clears the projection on the next planning pass. There is
no wait-age threshold, new resource allowance or persistent host reservation.

Owner pressure on a shared interactive host is temporary: an otherwise qualified
`require_target` job is accepted and waits with `shared host temporarily
unavailable: shared host memory pressure` (or the current pressure observation
reason). Resolve retains eligibility and portability calls the admission block
temporary. Dispatch and launch remain blocked until pressure hysteresis clears;
structural shared-host restrictions and impossible capacity still refuse.

`native_isolation_warning` is the worker's compact startup inventory warning,
shown as `WARNING` in the CLI machine row. Fallback workers scan listening Unix
seqpacket endpoints once at startup: unexpected non-root/non-CUDA listeners,
unknown ownership or inventory failure warn visibly. Root-owned pathname
sockets and the observed CUDA abstract naming convention are excluded; this
is advisory naming, not authenticated identity or continuous monitoring.
Inspect worker logs for endpoint details. Later or other-network-namespace
listeners remain outside that snapshot and do not strengthen the residual.

From the release containing #661's adaptive loss correction, worker freshness in
maintenance, fleet, inventory and placement follows the same session contact
calculation as loss detection, with a separate wall-time suspicion check. It measures missed expected check-ins while the
queue can serve control traffic; queue transaction time does not imply worker
silence. A current-session in-flight check-in protects only that worker, and so
does that worker's open stop-event poll for confirmed loss only, up to a bounded
check-in stall (#1450). A
cancelled call has a measured reconnect opportunity; rejection and stale sessions
do not refresh accepted contact. `last_seen` remains the actual accepted queue
timestamp, so an old timestamp alone does not establish a lost machine during a
queue stall. `suspect: true` means current-session arrivals exceeded the measured
wall-arrival allowance, including observed control delivery cost even when the
worker advertises a shorter send interval. Completed response cost provides
the initial delivery evidence before a second arrival. Confirmed loss also
requires enough available control service. The machine receives no new assignments and maintenance
reports `fresh: false`, while existing attempts and retry budgets remain intact.
Started preparations retain their exact custody and original start timestamp;
unstarted cached offers cannot launch. Pending native qualification demand is
retained so fresh contact can resume the same check without a poisoned result.
CLI/HTML availability says “suspect; waiting for worker contact; no new work”.
Health reports `worker_suspect` through the existing notification channel. This
bounds uncertainty even when continuous queue saturation prevents confirmed
loss; it does not prove that the worker died. Evaluate the view at
`freshness.captured_at`. Physical probe/sample
validity remains independent and can still withhold admission. See
[adaptive worker loss](design-proposal.md#adaptive-worker-loss-661-my-31my-34) for
normal/overloaded detection latency and retained operator minimums.

Availability explains owner use, battery power, heat, missing readings,
disabling, leaving, and loss of contact. An available hypha can still be busy.
Running jobs show their hypha and attempt. Every waiting job has an explanation,
including missing qualification, memory or resource requirements, unavailable
hyphae, software preparation, and work ahead in the queue.

From s070, work held behind another job's environment preparation names the
blocking job, environment identity and worker in status and job GET. The reason
distinguishes an offered build awaiting a builder report, a started build and
preparation awaiting exact stop/release acknowledgement. An unready deadline
winner permits ready backfill until existing priority rules take custody.
Production retains its preparation turn. Once priority preemption has
released its chosen target, that placement stays committed through preparation;
the just-requeued victim cannot repeatedly backfill ahead of the build.

From s072, one environment builder can run beside CPU commands when a separate
logical core fits the host allowance, including on accelerator workers. The
builder's core stays charged until its exact completion or stop receipt; CPU
commands retain their existing reservations. Preparation RAM uses the existing
owner reserve. CPU-scoped builds may also run beside device commands;
device-scoped builds wait for those commands to release. Active model work,
recovery and shared-host policy still withhold new builds. Peer preference uses
the same preparation eligibility, so a worker unable to start a builder cannot silently attract an
offer. Existing offered/building and CPU-capacity reasons describe these waits.

From s071, a deterministic environment build failure fails matching queued jobs
with `environment failed: ...; fix the recipe and resubmit with changed inputs`.
Job GET, `myce status ID`, and aggregate status include the build-log tail (up to
4 KiB); `myce logs ID` retains the uploaded full build log. `logs: none` omits the
excerpt and saved job log. CPU recipes are matched to the worker's CPU execution
target, including on accelerator hosts. A retained failed key is never offered
again just because a new launch attempt or submission key exists: fix the locked
recipe so its immutable environment identity changes.
While the full log is still uploading, the job stays queued with the same typed
failure reason and excerpt, excluded from preparation; terminal failure follows
the upload receipt so no trailing log chunks are lost.

Worker environment reports add `failure_kind` (`deterministic` or `transient`).
Network, disk/capacity, killed builders and unknown preparation infrastructure
errors remain queued. Their reason is `environment retry pending` with an excerpt
and the queue-clock `retry_at`; repeated failures back off for 30, 60, 120, 240,
then at most 300 seconds. Reports also expose the capped consecutive `failures`
counter. Repeated heartbeats do not move that retry time. Exact completion
receipts, including a finished build still uploading its log, release only their
own preparation attempt. Unstarted replacement offers are withdrawn; started
custody still requires its own receipt. Other environments can prepare during
backoff. Lost workers retain the existing session/loss and cleanup fences.

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

Input-staging failures carry `execution_cause.staging` on the job and exact
attempt history: `input`, `blob` (SHA-256), `operation`, `error`, optional
`syscall`, and optional HTTP `status`. From the next worker release, shared-input
failures also retain `stage` (for example read, write, sync, verify or headers),
`path_class` (source, cache or store), `bytes_copied`, `expected_bytes`, and
numeric OS `errno` when supplied. A timeout snapshots the currently active
operation even if its private writer finishes later. Byte counts describe the
active file's materialization, including a resumed prefix or successful clone;
verification does not add copied bytes. Missing errno means no OS errno was
observed, rather than EIO. Zero fields may be omitted; older workers omit these
fields entirely. Capacity failures also name `capacity`
and byte comparisons (`required_bytes`, `available_bytes`, `limit_bytes`;
zero values may be omitted). Names are bound to declared and resolved dependency inputs;
expanded worker paths and raw upstream text are excluded. `myce logs JOB_ID`
also includes the safe prelaunch cause unless `logs: none`. Infrastructure
staging failures follow existing retry accounting; missing/expired referenced
blobs use class `input_unavailable`, malformed inputs use `invalid_input`.
See [input staging diagnosis](files.md#diagnosing-input-staging).

Memory admission refusals additionally retain `execution_cause.admission` with
the worker's failed `subcheck`, `required`, `available` and `unit`, plus the
attempt's `envelope_source`, `envelope_host_gb` and `envelope_device_gb`.
Subchecks distinguish local device/job reservations, device budget, aggregate
CPU/host allowances, configured host allowance, live host RAM/growth, live GPU
memory and unavailable shared CPU control. An unavailable probe/configuration
sets `unavailable: true` and omits `available`; no raw probe errors are retained.
These values are captured on the worker outside the queue lock and survive
requeue, acknowledgement and replacement of the current envelope. Queue logs
include the same comparison in `detail`.

For `live_host_growth`, `required` is the worker's aggregate pending growth,
not the launching attempt's envelope. Supporting workers retain `contributors`:
attempt ID/number or model kind, static `charge_gb`, compared `pending_gb`, the
`launching` flag, and `sample_fresh`/`sample_age_seconds` (absent when no sample
exists). The terms include exact-helper credit and sum to the failed comparison.
A launch-capacity refusal with positive `main: not_started` evidence never spends
losses or infrastructure retries, including an older worker's `launch/io_error`
classification. Queue Started authorization alone is not main execution; existing
supervisor custody still requires exact release before retry.
If a predecessor queue rejects the additive contributors with `request_invalid`,
completion delivery retries once with contributors removed from a wire copy;
the durable local diagnostic and exact release evidence remain intact.

A modern worker also records `prelaunch_refused: true` only for a persisted
native refusal before custody or launch intent. An exact released copy with
that proof and no queue-recorded start/custody preserves ledger completeness,
including a release-only acknowledgement before the final outcome. Missing
process identity without this proof remains conservatively incomplete.

### Positive execution evidence (#1008)

A production start refusal because every eligible host is disabled has
`diagnostic_code: host_unavailable` and `execution_cause` with `operation: launch`,
`class: host_unavailable`, `predicate: no_eligible_host_enabled`. Existing reason
text is unchanged. It also has:

```json
"execution_evidence": {
  "attempt": 0,
  "main": "not_started",
  "sidecars": "not_started",
  "environment_preparation": "not_started"
}
```

The same optional object is on job GET/status, returned Jobs from result/run
clients, worker `Result` reports, exact `attempt_history` entries, and each
`cancellation_scope.members[]` entry. A member describes its own job; parent
proof is never inherited. Zero means rejection before assignment. Otherwise
`attempt` identifies one physical attempt, not the entire retry history.

Each component is `not_started` (positive evidence its boundary was never
entered), `may_have_run` (boundary entered/opened; successful exec is not proved),
or `unknown`. Missing objects, missing states and unrecognized values are unknown.
`main` includes the submitted extension stage wrapper. `sidecars` describes all
authored helpers collectively: `not_started` means none were released;
`may_have_run` means at least one was allowed to execute. Framework supervisor
and isolation/probe helpers are outside these authored-command components.
`environment_preparation` covers attempt-local main/helper environment and
extension-asset acquisition. It becomes `may_have_run` before acquisition, even
on a cache hit, because acquisition can run authored build/install commands.
Earlier independent preparation/qualification jobs are outside this evidence.

Native memory refusals retain their existing cause, including
`launch/resource_limit/memory_capacity` with `prelaunch_refused` where already
applicable, or `command/stage_failed/memory_capacity` for native groups. Neither
that predicate, `interrupted`, `starts`, nor absent `started_at` proves no user
code ran. Sidecars and environment preparation can run before main's Started
acknowledgement. Use the component evidence; a refused main can be `not_started`
after a helper is `may_have_run`.

For a no-user-code conclusion, require all three components `not_started` for
every attempt and separately for each materialized member. Current evidence is
cleared on assignment; history retains the prior attempt. Native live completion
observations are persisted with their completed result. Incomplete recovery,
older workers and unobserved daemon boundaries stay unknown. Workers negotiate
`X-Myce-Execution-Evidence: execution-evidence-v1`; strict older queues receive
no additive field while the durable local result retains it. These diagnostics
change no retry or admission decisions and do not prove cleanup/release or grant
overlap/deletion permission. Artifact-download APIs still require accepted
results; read the Job for refusal evidence.

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

When the queue's own start fence (launch authorization, start placement or
hand-off) refuses an attempt before its main command starts, the attempt is an
admission requeue: its history entry has `admission_requeue: true` and no
`interrupted`, the job's `starts` does not count it, and `admission_requeues`
does. The attempt number stays spent, so the retry gets the next number and
reports from the refused attempt are rejected as stale.

Queue-driven cancellation and an assigned attempt's start-deadline expiry also
record exact-attempt endings, without inventing a worker execution cause or
physical-release evidence. Never-assigned jobs have no attempt entry. Late exact
operator refunds refine that attempt's operator flag and, when reviving its
failed decision, its state/reason; the original end time and other attempts stay
intact. Cleanup-only acknowledgements do not rewrite an ended attempt.

After cancellation, `cancellation` is the read-only aggregate acknowledgement.
Declared parents also expose `cancellation_scope` for every piece and registered
descendant, including never-dispatched work. See [abort and release](jobs.md#abort-and-wait-for-release).

Launch-gate transport failures use `execution_cause.operation: start_authorization`;
the Started hand-off uses `start_handoff`. Both retain the underlying safe class
(for example `timeout`). An interrupted result
uses that cause in its reason, without implying a worker restart. Actual fresh
worker-session recovery retains its separate restart diagnostic. Authorization
failures still consume the existing infrastructure retry/loss budget, except
when the queue's start fence refused placement: that attempt requeues without a
loss or retry charge. After Started authorization, that exemption needs the
worker's `execution_evidence` for that exact attempt with `main: not_started`.
A refusal with missing, `unknown`, `may_have_run` or other-attempt evidence is
charged as an ordinary loss and infrastructure retry: the queue's refusal record
names the fence predicate, not whether it refused main's gate or a helper's
custody, and a helper gate can be refused after main started. Current workers
send that evidence; older workers' post-Started refusals are charged. The queue log line `event=launch_placement_rejected`
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
`memory_peak` and resolved `memory_envelope`. The envelope's optional `basis`
(`declared`, `learned`, `hint` or `exploratory`) and `evidence` (accepted
attempts behind a learned figure or similar-work bound) also appear in status
and waiting reasons (#585). At assignment the envelope's optional `learned`
(`host_gb`, `device_gb`, `evidence`, `source`) records what learned sizing
would reserve on that target, for explicit figures too, so a stale number
shows beside its evidence. A typed host figure on a CPU job is a limit, not a
reservation: once the work has accepted runs on that target (or its family two,
as below), the envelope's `basis` is `learned`, `charge_host_gb` is the charge
(accepted peak plus measured spread) and `limit_host_gb` the typed figure the
worker still enforces. Production, the shared interactive host, hosts without
a cgroup v2 or Darwin RSS guard, OOM retries and older workers keep the typed
figure as the charge. Job GET `resources` lists each dimension
(`host_memory_gb`, `device_memory_gb` for GPU jobs, `cpu_cores`,
`storage_bytes`) with `declared`, `learned`, `evidence`, `used` and `basis`;
a missing value means none, not zero. `used` host memory is the charged
figure, including ceiling slack and, on unified hosts, the device share. CPU
is not learned yet. Storage `basis` names what sized the job's work dir and
`TMPDIR` scratch: `exact` (accepted runs of this exact work), `family`
(accepted runs of the same entry, see below), `declared` (`disk.scratch_bytes`)
or `manifest` (sizes only). Status
`admissions` counts memory admissions per host by basis: `current` over
running attempts, `recent` over every retained assigned job, and
`declared_with_evidence` for explicit figures with a learned comparison.
These facts persist with the job
independently of learning, including a constrained first run with no comparable
learned point. Unique attempt facts are retained. Historical `memory_attempts_dropped`
counts remain readable; they describe earlier discarded rows. Attempts that never launched and have
no peak do not create rows or increment the dropped count. `attempt_history` also retains distinct attempt evidence. Old jobs cannot reconstruct missing
telemetry. Launched attempts ending without a worker reading retain the
envelope and omit the peak. Release-only kill-time acknowledgements retain
their measured facts too.

One eligible accepted peak of the same plain-command identity is enough to seed
the next run's learned envelope; the two-sample rule only enables the narrower
complete-component range estimator. Identity includes project, name, exact run,
environment and input references, so changing an argument or a job name can
leave later runs exploratory even when earlier attempts report `eligible`.
Resolve adds a non-blocking note pointing these auto
commands to the existing profile memory class. Use a stable adapter entry or
profile base command and append item arguments/pass `--input` files; the default
class shares evidence across items. `memory_class: per_item` isolates arguments
and item bytes when those change memory need. Arbitrary raw command arguments
are never automatically treated as comparable (#631).

New attempt rows include `learning_reason`: `eligible` means the queue's memory
learning predicates passed; other values name the failed predicate (for example
`constrained`, `pressure_unavailable`, `identity_changed`, or `attempt_failed`).
The same gate authorizes accepted work peaks, resident/GPU host ranges and
program needs/`measured_needs`. Each learner retains its own identity rules:
programs can learn without reference timing, while work records require a
matching timing key. Missing peaks cannot authorize learning. Duration
eligibility is independent. The optional `memory_peak.pressure`
records the worker's first failing pressure predicate, or `eligible`, together
with baselined host/cgroup `some` stall deltas and elapsed time in microseconds,
diagnostic avg10 maxima in percent, host swap growth/out in bytes, and resident,
minimum available RAM, reserve and comparison thresholds in GiB. Cumulative
stalls at least 1% of elapsed time reject learning; avg10 alone does not.
Nil stall deltas mean unavailable/reset coverage. Missing/reset counters and
invalid observation windows have explicit reasons. Raw cleanup counter reads
say `counter_snapshot_only`, rather than claiming complete eligibility. Cgroup
events and swap peaks remain in their existing fields; refault/swap-out counts
are retained in `pressure`. Old attempts omit these details. A successful
constrained attempt retains diagnostics but cannot alter accepted envelopes.

CPU memory estimates prefer accepted same-OS evidence. A peak measured on
another OS is a floor, not a bound: with no measured transfer error between
the two targets, the run is that OS's ordinary first run (`exploratory
(transfer error unavailable)`), offered the room and floored at the foreign
peak, any hint and any constrained lower bound (#1486); where first runs are
charged their floor, it is charged that floor instead of the room. Once works have run on
both targets, their measured transfer factor sizes it instead (`accepted
transfer + measured error`). If no qualified target-OS allowance fits that
floor, fresh comparable same-OS constrained evidence and a fitting hint may
permit one `bounded cross-OS exploration` on the largest qualified allowance.
The attempt retains normal slack, drift and launch guards. Assignment consumes
the exception durably across restart and resubmission; OOM has no retry, and a
constrained result cannot authorize another probe. Only clean evidence accepted
by the same learning gate supersedes the prior. Refusals name capacity remedies.

Peak fields `host_gb`, `cgroup_gb`, and `device_gb` identify the measured scopes;
`swap_gb` is the Linux attempt parent's `memory.swap.peak`, when exposed.
All sizes are GiB. The parent covers the native main command and sidecars;
device memory is process-scoped. `events` holds hierarchical `memory.events`
counts `high`, `max`, `oom`, `oom_kill`, and `oom_group_kill`, retaining the
maximum per counter across the attempt's samples. Absent readings mean unknown;
explicit zeros mean read zeros. `constrained` is omitted when false. Darwin
process-group RSS has its unenforced `source` label and omits cgroup events and
swap peaks. A zero-OOM/swap/max-hit claim requires those readings to be present
and zero; an absent reading cannot support that claim. These observations do
not imply acceptance into learning or prove execution release.

Discrete XPU `gpu_host.host_drop_gb` subtracts sibling Mycelium cgroup growth
from the host's available-memory drop; `outside_gb` subtracts the attempt's
synchronized nonreclaimable sample and retains the counted driver-object floor.
`gpu_host.sibling_adjusted` identifies corrected attribution. `combined_host_gb`
retains the maximum per-attempt resident-plus-outside figure across accepted
runs, so independent component maxima cannot invent a larger combined run.
The first corrected accepted sample replaces legacy XPU memory aggregates;
comparable uncorrected XPU rows no longer dominate sizing. This replacement and
its accepted-sample provenance survive queue reopen. Real OOM lower bounds and
pressure/custody gates still apply. Unexplained activity outside Mycelium remains
a conservative residual; the sample is not proof of exclusive ownership.

CUDA/XPU cgroup attempts sample device use every five seconds, including declared
jobs, checks and resumed attempts that cannot feed learning. CUDA uses NVIDIA's
per-process `compute-apps` counters; GPU-wide `memory.used` can be unsupported
on unified-memory devices while these process counters work. `device_gb` is a
sampled lower bound, not a retained allocator maximum. When no usable sample is
retained, `device_unavailable` explains missing process samples, unsupported or
invalid accounting, unavailable process inventory/cgroups, or sampler shutdown
timeout. Older workers may omit both fields; historical peaks cannot be recovered.
Live check-ins refresh each attempt's device counter from its sampler at send
time, even if the actor's cached host-memory report predates CUDA initialization
or is delayed by log/result I/O. Device age still starts at the original sensor
reading, and the host counter keeps its own older age. A sampler that stops
producing readings still becomes stale after ten seconds; refreshing the report
does not renew its evidence. Unified CUDA admission
requires a fresh sample from every existing tenant before adding another.

When CUDA lists attempt PIDs but reports `[N/A]`, a Linux worker also reads
those PIDs' RSS and the exact attempt cgroup's resident anonymous, kernel and
shared-memory pages. `device_source` labels their maximum as a **unified
RSS/cgroup upper estimate with host reconciliation**. CPU pages and repeated
shared RSS pages can overstate device demand; this is not a CUDA allocator
counter. The worker checks the signed `MemAvailable` drop against growth of the
entire delegated cgroup root and this attempt's RSS outside its cgroup charge.
An unexplained residual larger than one kernel page, a foreign GPU process,
missing baseline or incomplete RSS/cgroup/host evidence sets
`current_host_memory.device_exclusive`. A residual is charged conservatively
for live diagnostics, never divided proportionally among co-tenants. Such
samples do not feed device learning. The older exclusive host-delta completion
fallback remains available when no per-process figure can be retained.

Check-ins refresh the estimate's source, exclusivity and original timestamp
atomically alongside its value. Missing, stale or uncertain attribution prevents
new unified CUDA sharing. A newly started attempt may wait for its first sample
within the existing two-period sensor freshness window; that wait cannot admit
another tenant or excuse explicitly uncertain evidence. Once the window expires,
missing samples also require exclusive execution. Existing shared attempts yield in the guard's normal
lowest-tier, then greatest-over-reservation order until execution is exclusive.
The running reason names uncertain attribution. Dedicated-card missing-sample
behavior remains diagnostic only. Higher CUDA classes still stop every lower
co-tenant and await exact release before starting. Workers negotiate
`unified_memory_protocol: unified-attempt-memory-v1` before sending the new
sample fields. An older or downgraded queue receives the legacy shape and a
unified CUDA capacity withheld through existing device-unavailable fields while
a fallback estimate is live; no leave/stop is requested. Attempt custody and
local evidence remain intact.

Fresh, reconciled estimates enable the shared-card growth guard. For unified
CUDA it compares reservations with measured usage plus growth projected over the
observed sensor horizon and existing checkpoint/termination grace. It also
checks that projected growth fits current physical headroom above the configured
reserve/allowance floor. It requests a planned stop while headroom remains,
retaining exact-release custody and measured retry floors without spending a
failure budget. Graceful stop, kernel containment and sampling are not a hard
bound against an unseen allocation burst. Simultaneous unrelated frees can hide
host drops, and sampling can miss short peaks; native qualification remains
separate from fake-sensor tests.
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
retains `spec.allowed_machines` and `spec.excluded_machines` (machine names) and reports:

- `eligible_hosts`: sorted, unique host IDs of currently known machines allowed
  by both the job and project allow-lists, excluding `spec.excluded_machines`. For running and stopping jobs, including
  in-flight model calls, this also includes the current machine's host, even
  if policy no longer allows it. A restricted
  job with no known eligible or occupied host reports an empty array.
- `eligible_unknown_machines`: sorted, unique machine names in the effective
  allow-list that are not currently known machines. The effective list is the intersection
  when both job and project lists are nonempty, or whichever list is nonempty.
  This field is omitted when there are no unknown names. A joining machine
  resolves its name into a host ID on subsequent status reads. Excluded names
  are removed from this list.
- `eligible_unrestricted: true`: neither allow-list restricts the job. Treat it
  as able to run anywhere, including future hosts. `eligible_hosts` is omitted
  unless the job currently occupies a host; listed hosts never limit this marker.
  This is an open-ended, conservative maintenance marker: exclusion-only jobs
  retain it because future non-excluded workers remain eligible. The exact
  authored exclusions remain in `spec.excluded_machines`. Gates must continue
  counting this marker for every host, even an excluded machine's host; an
  exclusion is by machine ID and does not establish that every current or future
  worker on a physical host is excluded.

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

Lower-tier job `reason` also explains host-specific admission delays. Two production preemptions of the same job on one physical
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

Unified CUDA admission also protects memory for queued production whose sole
qualified physical destination is that host, including declared dependent stages
whose running producer piece has a last accepted duration. The co-tenant charge plus the
production envelope must fit the host ledger and live physical headroom. The
envelope retains declared host/device floors and accepted measured peaks with
the estimator's existing margins (at least 1.25), plus per-attempt slack. A bound
production job recognizes alternatives only under the project's exact-work
machine-kind approval (MY-51) and fresh, immediate admission proof. Smaller co-tenants can fit; cancellation
removes the demand. Envelopes that exceed the host's maximum physical admission
capacity do not withhold co-tenants indefinitely. A dependent stage's expected
arrival is the producer's last accepted duration less its elapsed time, and
expires when the producer overruns it. No prediction of future, unsubmitted jobs
is made.

The physical free-RAM floor is the existing unified-CUDA reserve: the OS/driver
reserve is at least 16 GiB, and a larger configured device reserve is retained.
An allowance, measured or authored, caps reservations and is never a floor:
live used RAM already contains the idle footprint it excludes, so physical RAM
minus the allowance would withhold that footprint twice. For example, 121.06 GiB
physical RAM, 4 GiB idle use, a 16 GiB reserve and a 96 GiB authored allowance
offer 96 GiB at idle (previously 92). Only a shared-interactive host keeps the
larger of its reserve and physical RAM minus its allowance. This is a
conservative admission policy, not a measured guarantee against driver
allocation failures; native coexistence/fragmentation qualification remains
separate.

The queue checks raw physical availability from the existing host-memory
observation, subtracting this floor and outstanding reservation growth; the
worker rechecks raw RAM before command/model launch. Missing or five-second-old
check-in observations withhold new admission until refreshed, bounding stale
credit at the cost of temporarily idle capacity. Full reservations still bound
the static allowance. Cgroup current memory can credit only the host portion of
a unified CUDA reservation: the device portion always remains charged in full,
including when device telemetry is missing. No CUDA/cgroup overlap is qualified.
The external-RSS history is not added to reservations; raw available RAM already
reflects both managed and unmanaged usage. Discrete CUDA and other platforms
retain their existing memory admission rules. Production priority rules are
unchanged.

Host gates should read `holds`, an array (empty when there are no holds) of
the same hold record jobs use: `kind`, `scope` (the host, device or selector),
`owner`, `reason`, `clears`, optional `expires`, `next` and `detail`. Entries are
sorted by scope and kind; a host can have several holds at once. Choose an
action from `kind` and `owner`, never from English text. `owner: operator` means
no automatic path clears it. Reservations, component exclusions and disabled
devices also appear here, beside their own records.

| `kind` | owner | When emitted |
| --- | --- | --- |
| `startup_release` | queue or operator | Failed startup awaits exact execution release; operator when any corresponding worker is gone/missing. |
| `host_disabled` | operator | Queue host maintenance is drained/disabled. Does not clear by itself; resume with `myce host resume HOST`. A machine's `disabled` field is a status projection of this queue policy. |
| `reservation_active` | reservation owner | A reservation holds components for one job until release or `expires`. |
| `component_exclusion` | operator | An active exclusion removes components from placement. |
| `device_maintenance` | operator | An operator disabled a device. |
| `unclassified` | operator | A production host blocker has no recognized explicit classification. Never treated as self-clearing. |

There is no idle production cooldown. `expires` is optional; refresh before acting. A queue
owner means no fault currently demands operator attention, not that release is
proven or that recovery has a deadline. In particular, deliberate
Queue admission stays drained/disabled until explicit resume. Cleanup acknowledgements may clear an
escalated hold without an operator override; custody remains fenced until proven.
Exact current executions that are healthy and starting or running do not accrue cleanup time;
becoming live again clears stale timers. Stopped attempts escalate two minutes
**after stop grace expires**. Lost/unconfirmed custody without a stop deadline
starts its two-minute window when first observed. All decisions use the queue's
injected clock. Once escalated, a later stop request preserves operator attention
and does not emit a second escalation event.

Known limits: a recovering model slot without `StopAt` has no time-based
escalation deadline while its worker remains present. A deliberate disabled-host
hold stays `host_disabled` even if its disabled worker later dies. Check worker liveness and model release evidence separately;
these holds alone do not prove release or eventual recovery.

For run monitoring, distinguish routine reservations and transitions from
operator holds using these fields, including the target host. For maintenance,
a hold's automatic recovery does not make the host safe: wait for relevant holds
to clear and verify assignments, loaded models and attempt release evidence.
Treat unreadable status, missing/invalid details, or unknown kinds as unresolved.
An empty hold list alone does not prove a specific attempt has released custody.

The effective host allowance is folded into `host_memory.capacity_gb`; the CLI
and page no longer print it per machine (`memory_allowance_bytes` is deprecated).
A worker's own ceiling budget still prints **last reported** when the queue has
no effective allowance, including for stopped workers.
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
| `draining` | Existing work, independent maintenance, missing inventory or fresh worker/cleanup evidence still prevents validation. The reservation never cancels Started work. `pending` names the blockers. |
| `expired` | Never ready before max lifetime, still queued and without retained custody. Other jobs regain admission; the bound job remains queued until explicit release. `expired_at` is admission expiry, not physical cleanup. |
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
caller replays and queue restarts. `ready_at` records first readiness. A never-ready
reservation with a queued bound job and no retained custody becomes `expired`,
yielding admission to others while keeping its bound job queued. Release it
explicitly before reserving again. Once ready, or while cleanup remains unknown,
the fence stays past its lifetime; `text` reports `reserved past max lifetime`
and the reason. An absent binding without retained custody can release. The
release reason remains in status.
Other reservations, device exclusions and newer operator drains remain intact.
Device reservations leave CPU placement eligible within the host's CPU/RAM
allowances. A machine worker's CPU environment preparation
also retains CPU scope, so its offer cannot withdraw the reserved GPU job at
launch. The bound job's own exact preparation remains excluded from `pending`;
it waits unassigned while its environment builds and launches after readiness.
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
Plain status prints the JSON on stdout in every case and exits by `state`
(#1187): 0 `healthy`; 3 `degraded` (all known workers fresh with consistent
memory and inventory evidence, while a device reason, unavailable root or host
hygiene reason withholds capacity), with a stderr line naming each worker,
component, reason and what clears it, such as `myce device clear-quarantine`
for a `device_fault_*` quarantine; 1 `unhealthy` (`host health incomplete:
missing, stale or inconsistent worker evidence`). Mutations exit zero on
durable acceptance. Queue maintenance status has no privileged
local-exclusion verdict.

The optional wait reads this status until quiescent or the injected deadline.
Timeout and queue outage fail without changing policy or release custody.
Every host command uses queue evidence; there is no local envelope or rest verdict.

### Live headroom and reservations

Per-job JSON retains `last_memory_admission`: the last
changed blocking host-memory comparison in its current wait episode. It is
retained through a start-deadline failure and queue restart; a retry starts a
new episode. It records a target comparison, not a claim that RAM is the only
blocker or that the target will be selected. Absence means no retained comparison,
not zero usage. Unchanged comparisons reuse their original capture timestamp.

The snapshot has `captured_at`, `waiting_since`, `attempt`, target machine/session,
host/device and execution target, `envelope` (including basis and source), the
`drift_reserve_gb` used by physical exploratory sizing, `reason`, `ledger` and
`reservations`. No exploratory sizing means no exploratory drift reserve.
The ledger has its own `captured_at`, oldest worker `observed_at`,
`available_known`, `available_gb`, `allowance_gb`,
`reserved_gb`, `growth_gb`, `signed_room_gb`, `debt_gb`, `free_gb` and exact
`owners`. `signed_room_gb` is admission headroom before clamping at zero;
`debt_gb` is `max(0, -signed_room_gb)`, recalculated rather than accumulated.
`available_known: false` means the available-memory value is unknown; zero
`allowance_gb` means no explicit static allowance, not zero physical capacity.
Each owner identifies an attempt, retained release or model execution, with
its actual `charge_gb` and remaining `growth_gb`. A live attempt and its nominal
release receipt appear once. Reservation entries preserve ID, bound job, ready
time and lifecycle state. Pending custody reuses existing reservation accounting,
with `pending_known` and `pending_observed_at`; if that pass did not evaluate it,
pending state is explicitly unknown. Capturing a comparison adds no reservation
or backlog scan. Reservations add no RAM charge and
do not exempt their bound jobs from physical capacity or cleanup requirements.

This is one replaceable diagnostic per job, retained with ordinary job history,
not an event log. It complements #857/#859's integrated wait attribution; it
does not change admission, containment, retry floors or the worker protocol.

`myce status --json` returns the status snapshot as JSON; with a job ID it
returns that job's status view.

A machine reports host RAM as the one memory rule's three terms (#990, #1366),
`host_memory`: `capacity_gb`, `charged_gb` (Σ charge: Mycelium's reservation
ledger, not measured process RSS) and `room_gb` = capacity − charged, signed
(negative is debt). Capacity is the smaller arm the rule admits within: the
live reading less its one margin plus Mycelium's own resident use, which that
reading already excludes; or the queue's enforced allowance (read and set it
with `myce host status HOST` / `myce host budget`). It is absent while the
host's RAM reading is unknown. Text: `host H RAM capacity C, charged X, room R
GiB`; the page shows a Host RAM row.

Deprecated in status (still emitted this release; removed next release, MY-42):
`free_host_memory_gb` (room, floored at zero), `host_memory_in_use_gb`
(charged), `host_available_memory_gb` (the worker's live reading, which stays
on check-in), `effective_host_memory_budget_gb` and `memory_allowance_bytes`
(the allowance inside capacity). Accelerator `memory_used_gb` is no longer in
status (it is `total_memory_gb` − `free_memory_gb`); `reserved_memory_gb` (pinned
model memory), `leaving`, `gone` and `checkpoint_unavailable` are omitted when
zero or false. A job's `checkpoint` (its NAS assignment, which status redacts)
is always present, as an empty string when unset: v1 reply consumers require
it (MY-42). Text prints non-Mycelium RAM history once, as `24h=7d`,
when both windows agree. The definitions below describe how the terms are
computed.

`host_available_memory_gb` is worker-observed
available RAM after its configured host reserve. `host_memory_in_use_gb` is
Mycelium's reservation ledger, not measured process RSS.
`free_host_memory_gb` is admitted headroom: the smaller of live availability
minus running attempts' remaining growth room, and the configured allowance
minus full reservations (when configured). Remaining
growth is `max(0, reservation - current nonreclaimable use, live growth forecast)`
per attempt. The forecast includes the measured growth rate through the sample's
age. The envelope and forecast cover the same future bytes; they are not added.
The static charge is at least the envelope and current anonymous residency plus
that forecast, so observed over-use still consumes the allowance. The operator
reserve is already removed from live availability; it is not subtracted again.
Only fresh exact-attempt cgroup anonymous resident memory reduces growth.
The unified-CUDA device-growth floor
remains: host cgroup memory cannot cancel the device reservation. Summed
per-process device counters may include shared allocations; neither their sum
nor RSS/host-delta estimates establish unique physical residency for credit.
Heartbeat device snapshots freeze before reading physical
headroom; aging them afterward never fetches newer allocations.
Native helper memory can be reported before main starts: occupancy does not
establish main execution. Launch takes fresh actor snapshots before its live
physical-RAM read and rechecks exact custody afterward. Confirmed local cleanup
removes an old actor's reservation even before its next heartbeat; stop intent
and pending cleanup do not.
Only this current nonreclaimable memory reduces growth;
reclaimable cache, historical peaks and process RSS do not. Missing or expired
samples retain the full reservation, as do models and unconfirmed release custody.
Host sample validity follows twice the longest observed
exact-attempt sample delivery interval (one missed delivery); the five-second
legacy window applies until cadence is measured. Only distinct negotiated
`sample_id` observations train cadence; cached, unidentified repeated and
out-of-order deliveries keep the original expiry. Missing samples grant no
resident credit.
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
separate; see [the native checklist](development.md#pressure-and-status-agreement).
Status is diagnostic and never permission to launch work locally; submit it.

Plain native jobs without an isolation declaration remain unsandboxed in v1.
Their `isolation` status explains the compatibility default and the one-line
`isolation: sandboxed` opt-in. New extension registrations default sandboxed.

### API request latency

Authenticated status JSON includes `request_latencies`: finite registered route
patterns, lifetime counts and conservative histogram percentiles. Duration-bit
bins replace raw sample rings and route caps. Locally measured service cost and
adjacent variation select unusual `slow_request` lines; measured logging cost
gates further lines. No universal one-second threshold applies. Lines retain
route/status and mutex wait/hold/after-unlock durations.

Mutex attribution follows request contexts through queue-backed routes, including
hydration recovery, file-store callbacks, and model forwarding. A
`mutex_measured=false` request reached no queue acquisition (for example a cache
hit or early rejection). Summary timings cover every route. Instrumentation uses monotonic wall time only for observation;
queue decisions still use the injected clock. Logging and summary publication
run after unlocking. Labels use registered route patterns or `unmatched`, never
request paths, query strings, tokens or bodies. Long-lived streams are recorded
when their handler ends.

### Non-Mycelium RAM history

For a replay-window artifact with queue/worker RSS and card observations, see
[benchmark telemetry receipts](benchmark-telemetry.md). Terminal job facts and
the bucketed history below cannot reconstruct missing host snapshots.

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
model processes) is subtracted.
`memory_observation.attribution_unknown: true` marks an attempt whose memory
sample is missing or stale; its `total_gb`
and `used_gb` still supply physical-memory admission, with full reservation
growth when the attempt sample is unavailable. Invalid observations
are skipped. Custom sources lacking raw
RAM evidence cannot contribute while reserve-clamped.
Reservations and attempt high-water marks are never subtracted. This is a
sampled estimate: RSS may count shared pages more than once and miss compressed
pages, reparented processes, or short spikes. Container daemon children and
Mycelium processes outside the worker tree remain in the residual, conservatively
reducing suggested budgets. Device-only GPU RAM is not host RAM.

Capacity and measured serialization cost determine aligned time subdivisions.
They retain counts, byte-bit distribution summaries and exact observed maxima.
The boundary subdivision can extend the requested window; first/last bucket and
peak times expose that coverage. Offline gaps create no samples. Histories use
separate durable records and expire only beyond every public reader window.
See [adaptive retention](retention.md) for budgets and dependency protection.

### Suggesting a host allowance

From the measured-allowance release, `external_memory.quiescent_v1` separately
reports physical used RAM when both local and queue custody prove an idle host.
It subtracts no RSS. Its `24h`/`7d` sample and `windows` counts expose actual
learning opportunities; legacy history has no such provenance, so cannot tell
how many idle windows occurred. Missing qualified history remains unknown.
Machine `host_memory_derivation` names the opt-in allowance's evidence or authored
fallback. See [measured host allowances](reference/host-capacity.md#host-ram-admission-and-ceilings).
The legacy suggestion below continues to be a diagnostic estimate; it does not
activate measured policy or convert residual peaks into qualified baselines.

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

There is no quarter-GiB rounding. Headroom is the greater of authored
`--headroom-gb` (omitted: no extra authored reserve) and observed peak-to-typical
variation. Unknown/single-sample variability cannot invent a reserve. This is a
dry-run observation, never a guarantee against unseen future peaks.

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
See [delivery policy](files.md#complete-from-queue-storage-deliver-to-nas-later-349).

Historical `output_destination`, `storage_allocation` and `local_outputs` fields
remain readable. Their native implementation is parked; new submissions cannot
request it. A retained protected task holds with an explicit parked-storage
reason, and historical local custody is never treated as ordinary `output_refs`
or permission to discard its files. An exact persisted collected acknowledgement
permits ordinary file release/retention and history expiry; unresolved custody
continues to hold. See [parked native storage](reference/file-store.md#parked-native-storage-integration-564).

### Shared interactive profile

Host maintenance `policy.shared_host.interactive_reserve_gb` is the configured
protected reserve. Machine `shared_host` is the worker's exact acknowledgement;
`shared_host_reason` explains missing history/live observations or acknowledgement.
Shared hosts retain `last_resort: true`. `host_memory.room_gb` additionally respects
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
On negotiated `host-probe-v1` workers, `shared_host_pressure.probe` reports
measured period/cost and frozen validity in seconds. Local sample age must remain
below that validity at each report, including time waiting behind queue
transactions. Between reports the sample stays fresh until the next report is
due: its processing time plus that validity plus `delivery_seconds`, the
session's measured check-in delivery prediction frozen at acceptance (#684).
`age_seconds` is the age at that report, not the current age.
Matching-session contact cannot renew a negotiated observation. `availability.probe` and
`availability.age_seconds` similarly preserve owner observation age across clock
skew. `host_health.probe` applies the same frozen validity to health evidence;
`host_probe_unknown` withholds admission when it expires or the source fails.
After a queue restart, persisted health evidence falls back to wall age, so each
host stays `host_probe_unknown` for admission until its first report. From the
release containing the restart fix, that state raises no `host_degraded` alert
while the worker is unreported and restart grace lasts. Earlier releases alerted
every host at each restart. A worker still silent when grace ends alerts as usual.
Older workers retain legacy age rules until upgraded. See
[adaptive host observations](host-probes.md).

Host maintenance `healthy`/`state` and fleet `host_states` use the same pressure
and freshness facts (legacy workers retain bounded contact credit). Normal owner-pressure yield remains temporary
occupancy, without an infrastructure-failure alert; missing/stale evidence is
unhealthy even with a yield latch set. Detached fleet snapshots freeze contact at snapshot
`now`; the ordinary one-second status cache can still show an older observation
than a subsequent host-status request. Missing or stale pressure makes a shared
host unhealthy and withholds new work; it does not itself stop running work.
See [pressure freshness](shared-hosts.md) and
[shared-host thresholds and qualification](reference/shared-and-polite-hosts.md#pressure-response).


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

Retired with the curves (MY-49), for the adjacent release only (MY-42):
`timing.predicted_seconds` mirrors `last_accepted_seconds` and is then removed.
Gone now: `runtime.remaining_seconds`, `remaining_work_unknown`, `source`,
`unit`, `samples`, `history_age_seconds`, and `timing.service_estimate`,
`samples`, `comparability`, `uncertainty`, `history_source`,
`history_recorded_at` and `repeated_key`. Consumers that read them see them
absent.

Placement uses the same record (MY-31): among already eligible targets, the one
whose last accepted run of this kind of work was shorter goes first, and the
placement reason says `last accepted duration Ns on this target`. It ranks only
eligible, currently admissible alternatives after qualification, parity, tiers,
fairness, disruption cost and scarcity. It grants no capacity, reserves no
future slot and does not move running work.

Native commands and extensions can atomically replace the local JSON file named
by `MYCE_PROGRESS_FILE`:

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
Optional `stage_index` and `stage_count` are positive,
one-based integers. A count requires an index, with index no greater than count;
an index without a count means the number of stages is unknown. Counts and units
describe the current stage, so they may reset/change when the stage advances.
`total_work_known` defaults to false: set it true only when the complete amount
of work across every stage is known to the application. Knowing a stage's total
does not establish that fact. Status shows these declarations separately.
Only a fresh, explicitly single-stage report with `total_work_known: true` can
also describe whole-job reported counts. Multi-stage counts never imply an
overall percentage, and no report implies successful job completion.
Missing later-stage timings stay unknown; stage counters are never extrapolated
into an ETA. The last accepted duration of the kind of work remains independent
and covers all stages.
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
Delayed older file evidence and a backwards stage index cannot replace newer
evidence within the same attempt either.
Worker-observed `updated_at` is the local file modification time, used only to
order reports within the same attempt/session; it is not a queue-clock time or
an application-authored timestamp. Repeated equal timestamps do not renew age.
Older workers without it retain the legacy age-based interpretation. Upgrade
the queue before workers that send the extended heartbeat fields.

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
  `parity` stays `unknown` (always present, required by the previous release's
  v1 reply): neither a pin nor registration proves MY-19 parity. Only command discovery records
  `not_accepted_for_automatic_production`.

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

Old jobs lacking submission/assignment provenance explicitly report unknown
intent and omit historical placement; status does not reconstruct intent from
current eligibility.
These fields are observational and do not change placement or qualification.
See [the job schema](schemas/job-v1.schema.json) and
[ledger schema](schemas/job-ledger-v1.schema.json).

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

Read `GET /api/jobs/ID` (Python `Client.get(ID)`) for copies, `allocated_devices`
and the submitted `spec.class`/`spec.priority`. Read assignment time from the
matching `assigned` ledger entry's `at` or `placement.assigned_at` through
`myce ledger ID --limit 256` / `GET /api/jobs/ID/ledger`. Follow `has_more` using
`next` as `after` and the first page's fixed `high_water`.

For an XPU overlap canary, save every relevant job and ledger page before normal
history expiry, plus the policy revision used to map class to tier. Compare
each lower-tier card's conservative `[assigned, released_at)` custody interval
with every production `[started_at, released_at)` interval on that same machine
and card. Require disjoint intervals; for a preempted lower-tier holder require
its `released_at <= production.started_at`. Missing assignment, start or exact
release evidence makes the affected proof incomplete, never a pass. Include
all attempts rather than only final jobs; keep fleet-specific evidence private.

## Learned storage evidence

Status JSON includes `storage_observations`, also shown by CLI/HTML status as
Storage evidence. Each aggregate identifies the program/profile, frozen recipe
and source, qualified OS/architecture/platform variant, hashed output-affecting
configuration and input shape. There is no host in the comparison key; evidence
does not qualify another variant or change placement. Plain commands need a
frozen source manifest; unversioned external software stays unknown.

An entry that runs once per item (new inputs, settings or source each time)
never repeats its exact key. Its runs share a family: the same project, entry
or profile name, program, workload shape, target and environment. When the
exact key has no accepted peaks and the family has at least two accepted runs
(the fewest that show spread between items), the family's peak plus that
spread (peak minus smallest, or the largest measured underprediction if
larger) sizes scratch and replaces `disk.scratch_bytes`. The envelope's
`basis` is then `family`, and its source says so. Input size is not scaled.
CPU memory does the same: with a stated hint (`memory: auto` plus a figure),
two accepted runs of the same project and name on that host replace the host
hint with the bounded family figure (`submitter hint superseded`, basis
`exploratory`, normal OOM retry).

Assigned jobs expose `storage_envelope`; job GET also retains `storage_attempts`
(first and latest 31, with `storage_attempts_dropped`). These facts carry exact
execution/machine/session, allocated-block peaks, the assignment's prediction,
and `learning_reason`. The existing memory-learning gate checks success,
exact custody, complete pressure coverage and clean memory before storage's
additional identity/verified-manifest checks. Verified output can teach its logical
size when scratch coverage is incomplete; `output_learned` records that subset.
Accepted scratch additionally requires complete managed-root sampling. Rejected facts
cannot overwrite clean ranges. The largest rejected managed-byte sample and its
exact execution/reason/time remain separate `lower_bound` evidence for fresh
admission; they do not grant an automatic larger retry.

Output is the **logical sum of verified manifest entries**, including repeated
content hashes. It is independent of the file store's physical deduplicated
increment. Scratch is allocated mutable workspace/output above the measured
staged-input baseline; spool, checkpoint/stop writes, metadata and file/inode
counts (including staged-input inodes) are retained separately. `managed_bytes`
comes directly from all allocated blocks in the attempt's workspace, checkpoint,
private publication root and exact-attempt metadata, rather than summing selected
categories or peaks observed at different times. Staging is charged additionally in `predicted_need`,
using known input bytes and the measured staging range. Hardlinks/aliased roots
count once within a scan; symlinks are not followed. Publication sampling includes
the simultaneously retained work files, immutable spool and manifest, original
checkpoint plus its immutable publication copy, and exact-attempt custody metadata
(including temporary replacements). Checkpoint copies live under a private
per-attempt publication root outside payload write grants. Live and final
publication take a synchronous root sample before deleting staging, retaining
byte/inode peaks even on upload failure. Final completion waits for any outstanding
live checkpoint publication to finish sampling and cleanup.

Each dimension predicts its largest accepted complete peak plus
`max(observed range, positive prediction error)`, rounding allocated bytes to the
observed filesystem allocation unit. `evidence` exposes samples/minimum/peak/error;
`margin` and `multiplier` explain the managed-byte prediction. A single complete
4-unit sample with no error predicts exactly 4 units. Evidence reuse depends on comparable identity, not elapsed age. `spec.disk.output_bytes`
is only a cold output prior: scratch, total need and file counts remain unknown
until measured. Output hints never seed scratch or silently become hard limits.

This slice is observation only: `hard_limit` is absent/unknown and `enforcement`
says `unenforced; observation only`. Unknown shapes require bounded exploration
and fresh storage admission from the subsequent accounting/admission slices.
Managed roots are sampled every five seconds, plus initial/final and explicit
publication-before-cleanup measurements. This cuts scheduled metadata traversals
25-fold from the former 200 ms cadence; transient peaks between samples can be
missed. End-only measurement would also miss longer-lived scratch removed before
exit. Neither cadence enforces a bound. Native metadata sampling can miss writes
deleted between scans. External writable
work/checkpoint roots remain incomplete, with no
accepted scratch estimate; native denial/confinement qualification is separate.
No store cap, reservation, delivery contract, retry or placement policy changes.

The unfinished native-backed protection, task-output delivery and migration
paths live on [`park/storage-protection`](https://github.com/1kuna/mycelium/tree/park/storage-protection).
They do not activate from retained flags on main. Shared async output admission
uses declared estimates and exact pending manifests; retry reservations reacquire
in legacy FIFO order. The configured cap, reserve and physical free-space floor
remain active. Accepted logical-output and managed-storage evidence remains
visible for diagnostics and future qualification; it does not change admission.

## Deadline priority guidance

Submission and job GET replies include `priority_policy` (effective tier,
selector and revision) and `deadline_guidance`. The latter contains advisory
`code`, the `deadline_policy` match for a new deadline-class submission,
`precedence_change` (`higher`, `same` or `lower` tier precedence), current
`blockers`, `eta`, an `example`, `next_action` and explanatory `message`.
These are live observations, separate from immutable submission intent and job
holds. They do not change the accepted class or submission key.

`myce submit` writes this advice to stderr and keeps the job ID on stdout.
`myce wait` prints one short line per state change: progress on stderr and
terminal states on stdout. `myce wait --json JOB` emits JSON lines on state or
reason changes, retaining full reasons and initial placement/deadline diagnostics;
`myce status --history JOB` shows the current advisory; default status leaves
it out so a job reads as one reason and one action.

For deadline work, author a **new submission** with, for example:

```yaml
class: deadline
deadline: 2030-01-02T15:04:05Z
```

Choose the desired RFC3339 deadline, replace legacy `priority` if present,
and choose a new `submission_key` for keyed work. An experiment carrying only a
`deadline` remains in its effective owner tier. The example's proposed match
uses the same owner policy matcher as scheduling: a project selector or catchall
can give deadline class equal or lower precedence. A deadline is an ordering
hint, not a completion guarantee or production `start_by`. Deadline demand does
not preempt non-resumable commands. Each blocker shows its kind of work's last
accepted duration, or `unknown`; it does not assure a queue start time.

## Configured NAS mount availability

Worker `unavailable_roots` lists affected configured root roles and a fixed reason
when the configured NAS mount is absent or its mount inventory is unavailable.
Mountpoint directories and local fallback capacity do not establish mount health.
The worker checks OS mount metadata before check-in and native/batch preparation;
placement and the launch fence consume this observation without filesystem I/O
under the queue lock. Host health is degraded while any root is affected.
Jobs requiring those roots wait, including non-resumable production inputs and
models. Prepared local environments and cached shared inputs remain eligible
when their own roots are healthy. Started attempts retain their exact custody. Batch assignments freeze the selected
model environment recipe key so queue placement and worker admission use the
same prepared-environment identity.
Restored mounts clear the observation automatically; an explicit operator disable
still requires an explicit resume. Independently configured network roots are
remembered after being observed mounted; installed NAS is checked even at cold
startup. Root roles, never local paths or mount source addresses, cross the API.

Job `shared_host_backoff` persists each host's measured `recovery_seconds`,
`quiet_since` and `last_observation`. The first owner-latency interruption holds
that job on that host until distinct fresh healthy observations span its measured
time to interference. Gaps/failures restart recovery; elapsed time alone cannot
clear it. Other eligible hosts remain available. Old timestamp/expiry records
remain readable for compatibility. See [owner recovery](host-probes.md).

## Incident health alerts

`health_alerts` holds one record per problem: a `kind` and its subject (host,
machine, device, project, client kind, store, reservation, job, reason and
recipients). A firing of an existing record updates it instead of adding a row:
`first_at` and `last_at` bound every firing, `count` totals them, `state` is
`open` or `closed`, and `closed_at` records the latest closing. `at` and
`delivery_id` identify the current episode; `attempt`, `recovery` and `mount`
are the latest firing's detail. On 2026-11-11 the live queue had saved 25,818
per-firing rows in four days (12,531 `host_degraded`), none delivered because
no notifier was configured; a replay of that mix now keeps 16 records (#1395).

Detectors run on queue mutations and ticks, using the queue clock; no polling
service or automatic issue filing is added. Each condition has a stable subject
key and fires once per episode; positive recovery rearms it without a wall-time
cooldown. A record stays open while any active condition keeps it open, and
closes when none does. Point events with no clearing condition
(`production_attempt_lost`) are recorded closed. Closed records expire
`HealthAlertRetention` (3 days, matching finished-job file retention, MY-47)
after closing, except that with a notifier configured an unacknowledged
closing is kept until delivered. Clearing changes no admission, retry or
release-custody decision.

The optional queue `--health-notify` executable receives openings and closings
of host-level problems only (MY-49): `host_degraded`,
`host_unhealthy_or_stale`, `worker_stale_flapping`, `worker_reports_missing`,
`host_mount_unavailable`, `host_mount_action_required`,
`host_disk_exhaustion_forecast`, `disk_below_guard`,
`checkpoint_store_unavailable`, `xpu_repeated_teardown_faults` and
`repeated_launch_refusals`. Every other kind (efficiency, recipe, job, project
and queue-load findings, and advisory `host_mount_degraded`, which
`host_mount_unavailable` escalates when real) stays in status only: it is not
counted `pending` and expires on retention with or without a hook. An opening uses the record's `delivery_id`; a closing uses
`delivery_id + "/closed"` with `state: closed`. A record that closed before its
opening was delivered sends one closing notification carrying its count. A
record that reopens before its closing was delivered continues the episode the
receiver already sees open and sends nothing; a reopen after a delivered
closing is a new episode with a new `delivery_id`. Delivery retries keep the
same identity. `delivered` acknowledges the opening and `closed_delivered` the
closing.

`health_alert_delivery` reports `configured` and `pending` in status/inspection
JSON; CLI and page status explain the same state. `pending` counts openings and
closings without a durable hook acknowledgement. No hook is a supported
diagnostic-only mode. It does not prove an attempt or distinguish hook failures
from queue acknowledgement failures. Restarting without a hook clears its
historical configuration flag. Successful hook exit does not itself prove
downstream thread execution. Job completion observers and their thread-bound
notifiers are independent of this queue-level channel.

### Health notifier hook contract

- Configured with `myce queue --health-notify EXECUTABLE` or
  `MYCE_HEALTH_NOTIFY`, on the queue host. The queue resolves it on `PATH`.
- Check: at startup the queue runs `EXECUTABLE --check` with no stdin. A
  nonzero exit refuses startup.
- Delivery: one run per notification, no arguments, one JSON object on stdin
  (the record's `health_alerts` fields): `kind`, `delivery_id`, `state`
  (`open` or `closed`), `at` (episode opening), `first_at`, `last_at`,
  `closed_at` (closings), `count`, `delivered`, and the subject identities that
  apply (`host`, `machine`, `device`, `store`, `reason`, `mount`, `recovery`).
  A closing's `delivery_id` ends in `/closed`. Fields are identities and fixed
  codes, never raw worker text or paths.
- Exit 0 acknowledges. Any other exit, or no exit within 30 seconds, retries
  the same `delivery_id` after a minute, so the receiver deduplicates by it.
  Deliveries are paced one per second, in record order.
- Environment: the queue's, without `MYCE_TOKEN`. Stdout and stderr are not
  logged. Keep receiver credentials in the hook's own private configuration.

Upgrade from the previous release folds its per-firing rows into records on
the first queue transaction, closed at their last firing, and rearms active
conditions so current problems reopen a record at once. Records stay dense
`health_events/N` rows whose JSON the previous release reads as events, so a
rollback keeps working; rows it appends fold in again on the next upgrade.

- `queue_overload`: sustained response-cost or mutex-wait regression against
  this installation's healthy history, separately for each normalized API route.
  Intentional worker-event and consumer-wait response duration is excluded;
  their real lock waits still count. Eight completed observation intervals
  establish a local baseline, and three consecutive adverse intervals confirm
  a regression. The robust band uses quartiles, adjacent observed variation and
  Tukey's far-outlier fence. The bounded 64-interval history replaces itself as
  healthy service cost changes; adverse samples cannot teach degradation as
  normal. Three healthy intervals, or an interval with no requests, clear it.
- `client_request_flood`: only while the queue is degraded, one existing client
  family supplies a strict majority of relative excess GET demand over its own
  healthy rate band. Relative growth preserves the normal request mix even when
  one family normally dominates. Healthy high volume, sole-family dominance
  without degradation and proportional growth of multiple families do not fire.
  Versions aggregate by kind; `client_kind` is only `cli`, `python` or `unknown`.
  This attributes a family, not a process, and copies no User-Agent or address.

The traffic detectors close intervals on maintenance ticks and normalize rates
by actual elapsed queue-clock time. They contain no request-per-minute, fixed
latency or fleet-size threshold. Statistical work and aggregation run outside
`q.mu`; ticks commit fixed booleans. A stopped queue cannot commit or deliver an
alert until it progresses. Restart requires fresh local traffic history; retained
delivery events persist. A cold installation has unknown latency, rather than
an assumed universal healthy speed. The initial observed interval contributes
cost evidence and its actual duration, never an assumed complete minute.

- `worker_reports_missing`: an enabled joined worker omitted CPU capacity,
  required host-memory telemetry, advertised device-memory telemetry or a tool
  inventory needed by live demand. Optional reports without demand and positive
  sensor errors already owned by host-health alerts are excluded. Reasons are
  finite `*_report_missing` codes; supplied reports clear the condition.
- `environment_recipe_failed`: a current worker environment report is failed
  and the existing classifier says deterministic. Transient download/storage
  failures and cold/preparing environments are excluded. Ready/replaced/removed
  reports clear it; raw recipe errors and logs never enter notifications.
- `pin_reason_missing`: live frozen host/device restrictions lack the corresponding
  authored requirement reason. Terminal/cancelled work clears it. Authored prose
  remains private, and reasons are never treated as qualification evidence.
- `job_target_unsatisfied`: a queued job has known enabled targets, but none
  passes resolve's permanent-capacity predicates. Busy or temporarily offline
  fitting targets exclude it. Missing inventory makes the conclusion unknown;
  it is owned by the missing-report detector. A fitting target or removal of the
  live demand clears it. No scheduling decision is changed.
- `infrastructure_loss_streak`: consecutive infrastructure endings on one worker,
  across distinct exact executions. Two is a repeated loss, not a timed fleet
  threshold. Final classification replaces provisional requeue evidence before detection.
  Replays cannot increment it; planned stops are excluded. Successful
  or workload-caused endings break the streak. Historical failures interrupted
  by successful work do not fire. It preserves all existing attempt fences.
  The episode and affected job retain an
  `owner_required` decision receipt (#881) in `health_alerts[].recovery` and
  `health_recovery`. It records the episode, exact execution/session, time,
  retained cause, sanitized admission comparison when available, plain-language
  summary and next owner action. `action: none` means no automatic repair was
  performed. No alert changes capacity, configured allowances, reservations,
  learned sizes, retry budgets, checkpoints or release obligations.
  Both direct-RAM and `live_host_growth` refusals need diagnosis. A growth
  comparison cannot distinguish stale availability from an inflated forecast
  (#868/#909); the maintenance owner needs forecast bounds and paired
  queue/worker memory snapshots. A subsequent successful start does not rewrite
  the receipt or claim a repair. Delivery, restart and repeated handling retain
  the original decision; newer executions cannot be changed by old evidence.
  No remediation timer or retry is added; normal scheduling and owner policy
  still apply. Plain CLI/page job reasons include the summary and next step.
  Automatic action remains deferred until a recorded incident establishes an
  existing supported reversible remedy and its fresh evidence prerequisites.
- `staging_cache_refused`: accepted worker transfer/cache/copy failure reports or
  an exact attempt's typed input-staging refusal. Cold caches, successful copies
  and planned stops do not fire. A repaired report or later successful staging
  clears its corresponding subject; missing staging evidence cannot clear it.
  Only fixed reason codes enter notifications.
- `update_failed`: the current update operation has a failed host or recorded
  failure. It uses #741's sanitized `failure_code` allow-list (including the
  explicit runner-smoke failure); successful retry/removal clears it. Helper,
  download, installer and service text is never copied.

#834 owns host degradation, silence, mount probes and collector diagnostics;
these detectors do not create another host-hygiene alert class. All remaining
checks use already accepted in-memory reports, intent and exact-attempt outcomes,
with no network, blob/manifest verification or filesystem probes under `q.mu`.
The older storage persistence delays and launch/flap history windows below are
unchanged by the adaptive traffic replacement; their broader #803 follow-up is
separate from the retired #768 thresholds.

- `checkpoint_store_unavailable`: a worker reports its `checkpoint` NAS root
  unavailable (or `checkpoint_unavailable`) continuously for five minutes by
  default. Other missing root roles and shorter outages do not trigger this
  storage alert. A restored checkpoint root clears it. Planned leave and host
  maintenance suppress it. The existing queue-store detector remains independent.
- `repeated_launch_refusals`: three distinct unstarted attempts on one physical
  host report `launch_authorization` refusals or start-fence admission refusals
  within ten minutes. Duplicate reports, successful starts, planned stops and
  transport errors without recorded fence evidence do not count. It clears
  once fewer than three refusals remain in that window.
- `worker_stale_flapping`: three fresh-to-stale transitions on one worker within
  an hour, using the same contact/suspicion calculation as maintenance freshness. A
  continuous outage counts once. Cold restart grace, leave and maintenance do
  not seed flaps. It clears as the transitions age out, or on planned maintenance.
- `xpu_repeated_teardown_faults`: the worker's existing quarantine classifier
  reports three distinct same-card teardown faults in an hour. Isolated teardown
  faults and unrelated quarantines do not trigger this alert. The optional
  device `repeated_teardown_fault` fact survives worker restart and clears with
  the exact quarantine clear or new boot. Alert `device` identifies the card;
  a sibling's recovery cannot clear it. Upgrade the queue before workers that
  can publish this new optional field.
- `reservation_never_ready`: a reservation remains unreleased past its maximum
  lifetime and has never recorded readiness. Normal draining before expiry,
  ready/in-use reservations and released records are excluded. Readiness or
  explicit release clears it; the alert itself does not release anything. The
  event includes the `reservation` identifier and bound `job_id`.
- `learned_envelope_exceeds_allowances`: current auto-memory demand has a learned
  envelope exceeding every otherwise eligible target's permanent allowance.
  Qualification, recipes, platform and other non-memory requirements still
  apply. Busy but sufficiently large peers, cold/exploratory estimates and
  oversized declarations are excluded. Refused submissions retain a private
  demand probe so new allowances or corrected/expired learning can clear the
  incident without resubmitting; it is discarded on recovery. Accepted queued
  demand clears when it fits or leaves the queue. Auto sizing inherited from
  variants is included; explicit member caps are excluded from the learned
  comparison. Healthy demand creates no condition history, and recovered
  envelope conditions are discarded on recovery. Events carry only project/job
  identities, never commands, mount paths or worker error text.

## Durable model interval API

`GET /api/models/intervals` is the supported paginated history for resident model
attempts, including engines assigned and removed between status reads. See the
[interval contract](reference/models.md#durable-model-engine-interval-history-663).
Events expose `assigned_at`, first-authorized `load_started_at`, stop-intent
`stopped_at`, exact-cleanup `released_at`, card identity and class/resolved tier
with policy revision. `class_changed` events timestamp in-place tier changes,
including effective production pins. `coverage_since` bounds the current
uninterrupted coverage period; a rollback through an older binary can create
a gap even though older events remain readable. Unknown legacy times remain absent.
A status snapshot's `loaded_models` alone cannot rule out transient arrivals.

## Local completion observer diagnostics

`myce wait --observer-status` reads the user registry's `observer.status` and
reports `owner_locked` from the lifetime lock. A record alone does not prove a
live owner; the lock alone does not prove progress. Compare `captured_at`,
`phase`, `last_read_at` and `last_delivery_at` with job completion timestamps.
Phases identify discovery, authority resolution, batch/terminal reads, notifier
execution, waiting, handoff and stopped state. Version, commit, PID, start time,
registration/delivery/failure counts and typed last-error cause support diagnosis.
The `readers` map retains each in-flight operation's phase, job count and start/capture times independently, so one responsive authority does not hide another stalled read. `next_read_at` and `next_delivery_at` identify recorded backoff/pacing. Stale metadata identifies the last recorded boundary, not the current Go frame.

The private registry retains `observer.log` and one rotated `.1` file, each at
most 1 MiB, mode 0600. Structured lifecycle, delivery and failure events exclude
authority, raw transport errors, notifier paths/targets and environment values.
The last-error timestamp distinguishes an old refusal from present progress.
Legacy observers have no diagnostic record; absence cannot establish a cause.

Interpret owner existence, observation and delivery separately. A current
successful read establishes that observation advanced at that time; a held
lock or a stale phase alone does not. A quiet long-poll can legitimately wait
without job changes. Use the recorded read/hold/retry evidence rather than an
invented fixed age cutoff; missing timing remains unknown. Queue/job status
and local completion-observer status are separate observations. Job execution
`liveness` does not establish completion-observer or receiver liveness.

The raw-output fallback is intended to retain only approved aggregate fields:
event time, `process_output` and byte count, never raw output/error bytes.
Structured lifecycle records have their separately documented safe fields.
Verification must include startup/error capture, malformed input and direct
stdout/stderr paths with synthetic sentinels; bounded log size alone does not
prove this privacy boundary. Keep expanded paths, authority and notifier
configuration in their authorized private custody, outside diagnostic output.
Status snapshots use atomic temporary-file replacement without durable syncs;
observer progress reconstructs them after an unclean host stop. Receipt, arm
registration and delivery acknowledgement saves still sync both the file and
its parent directory before returning success.
See [supported recovery](reference/completion-observer.md#completion-observer-upgrade-and-recovery-next-release).

## Batched waits and consumer flood protection

`POST /api/wait` accepts `{"ids":["JOB_A","JOB_B"],"after":{"JOB_A":1},"seconds":20}`.
The first request may omit `after` and returns immediately. Replies contain
`jobs`, an ordered array of scalar `id`, `state`, sanitized `reason` and
`revision` records. Send the returned revisions on the next request; it waits
until a selected record changes, becomes terminal, or the transport timeout
expires. Use exact IDs after initial prefix resolution. Fitting ID count follows current memory capacity and
wait duration follows caller intent and negotiated transport deadlines; see
[request budgets](request-budgets.md). Malformed requests receive `wait_invalid`;
capacity refusals name the fitting ID count. Unknown and
ambiguous IDs retain typed errors. Cancellation releases the transport wait.
The committed scalar index uses its own mutex; waiting never takes the queue
control mutex. Rollbacks cannot publish or wake consumers. HTTP job GET replies
advertise `X-Myce-Wait: batch-v1`; clients without this capability retain legacy
wait reads. Pending legacy wait reads also use the index; ordinary terminal
GETs retain the full public Job contract.

Consumer observation admission is adaptive. A healthy queue never refuses a
read because of its rate, address or number of clients. The queue measures every
control-mutex acquisition with its injected clock. Its mean uncontended occupied
interval supplies the local service baseline; a live lock waiter older than that
baseline establishes pressure. Overlapping readers form one occupied interval.
There is no fleet-derived rate, latency threshold, peer table or cooldown. A cold
queue without a positive baseline admits observations while it learns.

During pressure, admission compares incoming observations with the highest-value
observation already in flight. Higher scheduler tiers precede lower tiers;
selected-job scalar waits precede detailed job reads, then broad fleet projections.
An ignored `view=wait` query on a diagnostic subresource grants no scalar priority.
Within the same tier/scope, the running installation's mean completed healthy
request cost determines the order. Requests spanning contention cannot teach a
healthy cost, even if pressure clears before they complete. Unknown cost cannot
claim to be cheaper.
Only a higher-value read crosses that frontier; equal/lower-value reads receive
HTTP 429 `client_rate_limited` before projection work. Supporting queues add
`code_alias: observation_shed_control_pressure` while keeping the legacy code.
This is fleet-wide control pressure, not a per-client quota. If no observation remains
in flight, one request can measure recovery. As soon as live lock wait clears,
all reads are admitted again, even if previous observations are still active.
This is progress-based admission, not a fixed concurrency allowance.

Production-class job observation (including production policy tiers, prefixes,
HEAD and job subresources) is never shed. Class comes from committed scalar
intent and current immutable policy, never User-Agent or request headers; the
classification read does not take the control mutex. Keyed production ledgers
retain protection through history pruning and restart until their durable answer
expires. Prefix classification follows an index of precomputed priorities rather
than scanning retained jobs. Batched scalar waits are
also exempt because they use their independent committed index and do not delay
control. All worker protocol, launch/report, file transfer, instruction/catalog
and environment-reference dependencies remain exempt. Existing clients and the
429 code remain compatible. `Retry-After` rounds the measured live wait up to a
positive integer number of seconds, the HTTP header's granularity. It does not
promise the queue will have recovered by then.

The CLI's idempotent GET recovery already honors Retry-After with positive
jitter (#819). Retry bounds belong to caller cancellation or an explicit owner
deadline; no fixed retry count or fleet-tuned timeout is added. From #960,
interruption retains the last HTTP error and prints its Retry-After interval.
Follow the seconds-scale server signal rather than guessing minutes of backoff.
From the release containing #1072, generic Go and Python GET/HEAD retries,
downloads and replay-safe recovery use the current positive Retry-After plus
positive jitter, independent of earlier errors or unchanged reads. Without that
signal they use measured request service cost plus jitter, rather than accumulating
exponential error pauses. From the fast-failure correction to #1072, no-signal
recovery also permits at least the one-second initial/minimum TCP retransmission
period from RFC 6298 sections 2.1/2.4. Fast connection failures and immediate
503 replies cannot prove that the service has recovered at that RTT. The cost
is up to one recovery period plus jitter before probing a fast recovery; slower
measured service extends it. Positive Retry-After remains authoritative. This
also applies to shared-observer retries, not successful long-poll pacing.
The caller's deadline may end a pause before retry is
permitted; it stops recovery instead of starting an early request. No fixed cap
or retry count is introduced.
From #959, `myce logs JOB --after SEQ` resumes strictly after a log sequence.
An interrupted read prints the continuation command on stderr; stdout remains
ordered raw log text. The cursor advances after each successfully written chunk,
so resuming skips completed pages. Add `--follow` to continue following.

Measurements and active-request bookkeeping are process-local. A first concurrent
wave can arrive before pressure is measurable, and production observations remain
exempt even during overload. This protects control from continued nonproduction
read floods; it is not an absolute bound on all admitted work. No network or bulk
I/O is added under the queue mutex. Adaptive admission ships in the release that
includes #800; earlier deployed releases may still use the fixed-rate stopgap.

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

Go/CLI requests identify as `myce/BUILD_VERSION`; Python uses
`mycelium-python/PACKAGE_VERSION` (or `development` from an unpackaged source
checkout). Older clients with no first-party identity, including stock Go/Python
User-Agents, count as `unknown`. These are self-reported identities, not access
controls. Distinct sanitized versions share measured local diagnostic headroom plus per-kind
`other` overflow buckets; all requests remain counted. Raw User-Agents and job
IDs and peer addresses are never retained in these counters or summaries.
Counting, summary encoding and logging run outside the
queue mutex and do not write the database.

### Variant eligibility (#499)

Nonterminal native variant jobs expose
`variant_eligibility` in status and job GET, and in the CLI and status page:

- `extension`/`digest` identify the frozen implementation; `variant` is its exact
  declared target, while `host`/`device` and `target` identify observed capacity.
- `freshness` is fresh, stale or unknown, using queue time and the existing
  worker loss timeout. Stale inventory cannot establish eligibility.
- `qualified` requires the existing exact native report identity. `eligible`
  additionally requires current resources, policy, readiness and admission.
  `reasons` lists blockers; mismatched targets remain visible. It is not a
  selection, reservation, forecast or proof of reference-output parity.
- Assigned attempts say alternatives require requeue. Terminal jobs keep their
  historical placement receipt rather than projecting new alternatives.
- A newly registered multi-variant production implementation requires the
  candidate's kind of machine among the project's own approved kinds for that
  work (MY-51). The reason explains reference-output comparison and
  `myce parity add --evidence JOB_ID`. Checks/experiments can obtain evidence;
  historical registrations/replays and accepted jobs are kept.

An explicit work approval list also constrains historical extensions without
variants. When it excludes every qualified candidate, status reports
`production_parity_kind: held: OS/ARCH/PLATFORM is not a machine kind the project approved for production`
and the typed `hold.kind` is `production_parity_kind` (called
`production_parity_hosts` before MY-51), owned by the consumer (the project).
The action is reference-output comparison on a machine of a kind that can run
the work, then approving that kind for that exact work. A queued binding to a
host no longer of an approved kind is cleared and placed again; running
attempts retain their execution and release custody.

`variant_advice` is conditional porting guidance when missing coverage could
relieve current contention on a scarce host. It names the exact missing target,
backend/device selection, resources, frozen environment/lock, native check and
MY-19 approval steps. It does not infer support, change pins or relocate work.
Registration warnings reuse the live portability report without executing work.

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

Public output uses generic project labels. Optional `--private-project-map FILE`
creates a new mode-0600 label map for a private handoff; never publish it.
Retention can remove older records, so report coverage and window with every
number. Pages are read-only observations, not a transaction; a concurrent
submission or history collection may affect coverage. The script refuses an API
without pagination or a projection omitting host pins. A percentage change alone
is not evidence that a particular release changed consumer behavior.

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

The raw `spec.requirement_reasons` map is stripped and listed in
`spec_projection.omitted`. Native recipe maps remain stripped with extensions.
Only fixed host/device/backend constraint names survive; arbitrary keys coalesce
as `other` with a redacted-key locator. Free-form explanations stay in authored
source, private receipts and frozen intent, under the existing privacy default.
There is no raw-text exception or full-spec status mode.

## One hold record

Every reason Mycelium keeps runnable work waiting is one record: a queued or
paused job's `hold`, resolve's `hold`, and each entry of snapshot `holds`. It has
`kind` (the mechanism, a stable code), `owner` (who clears it), `reason` (one
plain sentence), `clears` (the condition), optional `expires`, `next` (one
action) and `detail` (the producing mechanism's evidence). Default and compact
status, `myce status JOB_ID`, `myce wait` and `myce run --wait` show `reason`
and its `next:` action; `myce status --history`, resolve and JSON add `detail`. The scheduler tags the kind where it decides the wait, so
no view infers the mechanism from reason text. The record changes no admission
decision.

Job kinds: `upstream`, `paused`, `checkpoint_root` (the queue's checkpoint
root is unreachable), `device_fault_last_healthy`, `launch_capacity`,
`helper_charge` (a launch refused with measured helper memory beyond the
reservation), `checkpoint_target` (a resume waits for a host of the target
that wrote its checkpoint), `scarce_host_yield`, `shared_host_backoff`, `storage_root`,
`production_parity_kind`, `production_parity`, `no_matching_device`,
`log_policy`, `project_cap`, `production_admission`, `unified_cuda_headroom`
(a unified-memory host's headroom kept for expected production),
`pinned_model`, `disk_headroom`,
`reservation`, `capacity` (mixed or card/CPU causes),
`memory`, `host_memory_headroom`, `cold_probe_margin` (a whole-room first run
needs host memory above its launch margin), `host_backfill`, `host_unavailable`,
`capability`, `shared_input`, `extension`, `no_eligible_host`,
`waiting_for_worker`, `model_engine` (a model call waits for its engine to
recover) and the preparation and coverage kinds below. A queued model call's
hold comes from the same scheduler wait as a job's. An unknown kind is
unresolved; read its `owner` and `next`.

A check-in's admission decision (`admission_wait` targets) carries the refusing
check's `hold` row (kind, owner, reason, clears, next) when that check names
one; its detail stays on the job's own hold.

`unprepared` means no builder has started; `waiting_for_builder` distinguishes
an unstarted offer or unavailable preparation core; `building` requires a fresh
builder report. The queue builds a missing plain environment on every
eligible host with a free builder core, for queued work that could use that
host once it has room, not only for the job admissible now; such a build runs
beside device commands. Extension setup and command discovery keep their
admission-bound offers. `deterministically_failed` retains the failed immutable recipe's
cause and log excerpt: the consumer must fix its inputs. Transient failures retain
the existing retry evidence. `unknown_observation` asks the queue owner to restore
missing/stale observations; it never claims preparation is progressing.
A ready alternative takes precedence over preparation on another target.

Queue builds with placement coverage diagnostics distinguish `missing_extension`
(register an implementation), `execution_coverage` (no compatible implementation
on observed targets), `native_qualification` (a declared implementation needs its
exact native check, which the queue runs itself), and `production_parity`
(reference-output parity required). A `native_qualification` reason names the
check job and what it waits on, or says the queue submits it at the host's next
check-in. The queue offers that check whenever the job could use the host once
it has room, not only while the host is free, so a busy host cannot starve it;
the check then waits in ordinary admission. A worker restart does not cancel a
pending check before the host reports its identity again, and a cancelled
check is resubmitted under the next key in its chain. A failed check, or no
compatible host, fails the job at once as a `dead_end_wait`, including a
fan-out parent before its first piece starts.
An explicit machine-kind approval hold is `production_parity_kind`. Each names
the next owner/action rather than presenting the missing software or approval as
ordinary capacity pressure. Extension coverage advice includes declared targets,
without recipes or private paths. Stale compatible inventory remains unknown.

A queued job with authored `targets` that waits while a freshly observed host of
another platform has room for it now gets `runs_now_on` on its hold: the
platforms (`os/arch/accelerator`, never hosts) whose hosts pass every current
check except the job's targets, and the same platforms in `next`. The check
reuses the targets the hold already evaluated and re-reads only those hosts'
current free memory, cores and host memory; a platform the job can already
use is omitted. The consumer acts by adding that platform to the job's targets
and environment (`myce portability` shows what each platform still needs), not
by naming a host. The advice is a snapshot, not a reservation.

Detailed queued observations also explain when only one freshly observed
physical host passes permanent requirements, deduplicating devices on a host.
This is current coverage, not proof of a permanent hardware monopoly or a reason
to pin a job. Authored host restrictions are not relabeled as observed uniqueness.
Configured production approvals report their machine kinds and that the
project approves another kind with `myce parity add`, even while an approved
target waits for capacity or preparation.
The existing small default status remains a lifecycle projection; use a job ID
or explicit detailed sections for these diagnostics.

CPU resolve omits accelerator and multi-card target views. Eligible means permanent
placement compatibility, not immediate capacity. `needs-staging` for a local source
placeholder is normal: the following run stages it automatically. A successful
wait/status omits deadline-policy tutorials and empty blockers. These changes
require the matching client and queue release; they are not rollout evidence.

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

Findings are derived on each read from sanitized runtime causes and fixed
queue-owned reasons; they are never persisted and never copy worker text.
Covered classes include output-contract failures after exit 0 (`output_*`),
publication failures (`output_publication`), resource stops (`memory_limit`,
`out_of_memory`, `memory_exceeds_hosts`, `wall_time_limit`, `disk_limit`),
infrastructure requeues and exhaustion, queue admission requeues before the
command started (`queue_admission`, owner `mycelium`), launch failures (`library_missing`,
`tools_missing`, `input_unavailable`, `launch_*`), upstream dependencies,
nonzero exits and payload signals. A failure with no typed cause reports
`cause_unknown` rather than a guess. Succeeded and cancelled jobs carry none;
waiting explanations remain in `hold`.

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

No notifier is a valid foreground `myce wait` workflow, explicitly distinguished
from a supplied missing/non-executable notifier. Doctor checks an executable's
presence only; it does not run notifier checks or prove delivery/thread ownership.
Result checks are reported without execution. Worker NAS readability is an
explicit storage-owner verification hold: doctor does not request NAS inspection.
Managed source/input placeholders are staged by the subsequent run.

Only authenticated ping and read-only resolve requests are made. No upload,
preparation, reservation, adapter/receipt write or service edit occurs. Known authority is redacted in every output field; raw YAML values
and transport errors are withheld. Notifier lookup uses the same PATH/absolute
executable resolution as run. Recognized cache-capacity refusals preserve the
required/capacity byte evidence and a dependency remedy. This is diagnosis, not execution or
release verification; submit revalidates and results still need acceptance checks.

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
foreign deletion. See [filesystem budgets](reference/worker-caches.md#shared-filesystem-budget-811-next-release).

These additions require `worker_disk_budget_protocol: worker-disk-budget-v1`.
Workers omit them for an older queue while preserving its supported collection
fields, including after a strict downgrade. Public output sanitizes roles,
reasons and warnings. The private worker-state `disk-inventory.json` retains
local role/path mappings for owner inspection.

Machine `hygiene_collection` reports the last automatic collection time, next
eligible sweep, file/environment cache bytes and effective limits, reclaimed
bytes, busy locks, protected entries and fixed diagnostic codes. File-cache
accounting uses logical copy sizes; environments use existing allocated/inode
accounting. Reclaimed counters are cache-accounting changes, not a promise of
equivalent physical free space on clone/compressed filesystems.
Unknown worker diagnostic strings are replaced by fixed codes at the public
status boundary. Collection fields require separately advertised
`hygiene_collection_protocol: worker-collection-v1`; health-only/older queues
receive their supported request shape.

The existing serialized retention lane applies the configured/automatically
sized budgets even when no new download or environment build arrives. Complete
file-cache copies evict oldest first under the same process and filesystem lock
as staging; each attempt has a private materialization. Partials, unknown data
and links remain protected. Environment collection reuses use/build/admission
locks, recent-use retention, ready receipts and authoritative queue reference
checks immediately before retirement. Unavailable reference state fails closed.
Inventory and reference requests do not hold admission; only final atomic
retirement tries that gate without waiting behind foreground work. Inventory
checks cancellation between metadata operations. Per-prefix saving estimates
can include shared inodes, so collection stops conservatively and recounts the
union once after a batch. `pending` means a residual budget needs another sweep.
In-use or referenced environments can leave a cache over budget; status explains
that protection instead of deleting custody to satisfy a target. Existing
shared-input/model caches keep their own separate policies and locks.

Inventory runs no faster than the existing minute retention cadence. A sweep
that takes longer backs off to ten times its measured duration, limiting hygiene
to a 10% duty budget instead of repeatedly scanning a large cache. It runs off
the heartbeat path, with no cache scan or network call under `q.mu`.

Attempt scratch/log/diagnostic retention continues through existing publication,
custody and grace checks. The installer already prunes its committed obsolete
backups while retaining the installed and immediately replaced image. Periodic
installer cleanup selects only the exact staging path owned by a failed durable
transaction receipt, with matching config, updater lock and proven helper absence.
Once the receipt names a different operation, earlier operations' staging
partials (exact `.myce-fleet-*.partial` regular files) are also removed under the
updater lock, but only when the binary's directory also holds this install's
config; a shared binary directory is never scanned (#834).
It never scans or deletes rollback binaries, bootstrap release archives, active
state, receipts or unknown staging. Foreign processes, services and shared
personal/project caches are outside this collector.

Worker mount recovery reuses `host_health.history[].mounts` and
`unavailable_roots`. `mount_probe_pending` is cold/unverified access;
`mount_probe_slow` is a host-relative advisory and does not hold admission;
`mount_probe_stalled` means no operation progress within the existing measured
#234 allowance;
`mount_probe_failed` is returned I/O failure. `mount_inventory_unavailable`
means the cached mount table could not establish backing storage, and
`mount_automount_pending` means access has not established the network mount.
`mount_linux_operator_required` and `mount_macos_approval_required` require
storage-owner action, with a deduplicated `host_mount_action_required` health
wake even after a prior generic degradation. The latter includes possible
Keychain approval; it does not claim to diagnose a specific credential failure.
Failures and confirmed stalls withhold only dependent work; three fresh
successful completions resume it, including slow successes. Pending observations
cannot prove recovery. `host_mount_degraded` alerts include typed `mount` evidence;
`host_mount_unavailable` separately wakes on a stall/failure after an advisory.
See [platform recovery](reference/nas-mounts.md#worker-mount-recovery-linux-and-macos).

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
allocation completes. Unknown reconnect/restart gaps never add service age. See [fair sharing](scheduling.md#sharing-gpus-and-measuring-the-pool).

These are conditional admission comparisons. Hard eligibility, quotas and tier
precedence remain binding; active same-tier work is not stopped to repay debt.


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

An acknowledged `adaptive native CPU memory guard` treats learned and
exploratory reservations as predictions. Fresh exact-attempt footprint and growth measurements use the
same forecast as cold CPU probes, including sample age and measured drift, with
the resolved estimate as a floor for learned jobs. Cold CPU probes keep their
known-need floor and use the same physical/allowance execution ceiling.
Bounded learned/exploratory estimates (#1048) keep the
resolved reservation plus measured growth and drift when telemetry is stale,
missing or stopping. Exact-attempt `host_memory_charge` retains grown charges
through smaller/missing reports, queue restarts and unconfirmed release custody;
observed peaks remain floors. A learned allocation that has not acknowledged
`Started` also keeps its bounded reservation without requiring telemetry.
Stale data grants no resident credit. Forecasts cannot exceed containment, but
actual observations and retained reservations are never lowered to that bound.
Cold probes still retain their complete ceiling without fresh complete evidence.
Darwin RSS can increase
the charge but grants no resident-memory credit because shared pages can appear
in multiple processes. The reported estimate and the execution ceiling therefore
serve different purposes. Explicit declarations and member limits keep
their existing containment contract. See [native isolation](native-isolation.md).

Declared native CPU attempts (#1064) with enforced cgroup
v2 containment reserve their complete declaration plus ceiling slack. Their live
growth forecast cannot add bytes beyond that containment, in either the static
charge or pending physical growth. Larger actual anonymous/footprint observations
remain floors; only fresh uniquely charged anonymous residency reduces pending
growth. Missing/stale observations retain the full reservation. This bound does
not apply to auto estimates, sampled RSS or accelerator memory outside
the cgroup; authored member limits remain binding.

Native CPU auto-memory and native accelerator-check host-OOM recovery retain a
durable lower bound from the previous reservation and killed peak, plus a
physical page to require progress. Growth/overshoot remains replaceable sizing
uncertainty, rather than a permanent demand floor.
`memory_envelope.source` names the physical-headroom recovery probe; an ended
attempt retains the envelope actually applied. Recovery does not fail merely
because a fixed number of exploratory attempts OOMed. A lower bound exceeding
all eligible physical/allowance capacities produces a capacity refusal. Linux
kernel ceilings and the pressure guard still bound a leak. Device-OOM retry contracts and accepted-family sizing are unchanged.
Physical probes also report `launch_drift_gb` and `available_host_gb`: the
exact job/worker-session launch-error reserve and ledger headroom at sizing.
Unrelated inventory declines and other jobs' old launch errors cannot starve a
new CPU probe or extension check. Exact launch refusals teach the same job's
next offer; strict worker rechecks remain authoritative.

## Queue efficiency (#857)

`myce efficiency` is the concise diagnosis; `myce efficiency --json` reads
`GET /api/efficiency`. Status and inspection JSON also include `efficiency`.
The dedicated read returns an immutable maintenance projection without planning,
job history scans or storage work. `at` is its observation time; a new queue
returns `null` until its first maintenance observation. It does not change
admission, placement, authored intent or retry decisions.

`myce efficiency` and `myce usage` resolve credentials like `myce status`:
`--config`, then the installed worker, then the installed queue configuration.
They no longer require `MYCE_TOKEN`, which consumers must not export.

`myce usage --by machine` reports GPU-hours and idle hours summed over physical
cards, followed by each card's figures. A containing multi-card target makes
its participating cards busy; it is never an extra card. Usage JSON exposes
these rows in `cards` (`machine`, `device`, and the summary fields). Busy
percent divides busy card-hours by observed card-hours, excluding unknown
outages. Historical synthetic group samples are normalized on read; retained
minute/hour bucket boundaries remain proportional estimates.

- `hosts` compares instantaneous reservations with measured Mycelium RAM and
  independent physical used/total RAM. `programs` groups reservation-time and
  measured-use integrals by host, opaque work identity and sizing basis. These
  include assigned/startup/stopping custody until release, not just execution.
  Current program reservations use the admission ledger's charge, including
  fresh measured CPU-probe growth credit; the fixed execution ceiling remains
  the separate prediction-envelope comparison.
  Retained rates keep the original host/work/basis after requeue clears the
  current assignment, and end only when exact release settles that obligation.
  Host attribution includes the worker and managed descendants. Per-program
  memory is the existing nonreclaimable attempt attribution, not historical peak
  RSS. Coverage seconds distinguish missing samples from zero use. Reservation
  integrals span custody; use integrals stop at the existing sensor freshness
  boundary. `reserved_covered_memory_gb_seconds` supports like-for-like gap
  comparisons over measured coverage. Worst program comparisons rank measured
  gaps; unknown coverage cannot establish wasted memory.
- Host integrals and the cold-probe launch headroom samples are kept in memory
  and written to SQLite only when the stored copy would stop supporting a
  restart (#1066). Host integrals are written when the host set, identity,
  reservation rates or reported measurement dimensions change, and when the
  stored reading passes its own freshness boundary (last contact plus the
  measured worker-loss allowance). Launch samples are written when the reserve
  they yield changes, and with any refusal or offer margin. A clean shutdown
  writes both. After a crash, seconds, reservation integrals and measured
  coverage up to the crash are exact. Readings since the last write count at
  the stored, still-fresh reading. The cold-probe reserve equals the live one.
  Coverage resumes at each host's first check-in, and a missing host identity
  during restart grace is not treated as a hardware change.
- Job `efficiency.wait_seconds` and report `jobs`/`projects` retain queue-clock
  time by sampled blocker: `memory_reservation`, `authored_platform_targets`,
  `environment`, `priority_fairness`, `preparation`, `capacity`, `holds`,
  `dependency`, or `unknown`. The existing waiting-reason planner supplies each
  category on maintenance. A category starts when observed, without assigning
  earlier unknown time to today's reason. Paused time is separately attributed
  to holds. Report jobs identify the current blocker and whether demand is live.
- `stranded` means the resource need fits **current** available capacity after
  removing only the named authored/policy exclusions (`targets`,
  `allowed_machines`, `excluded_machines`, or project policy allowed machines).
  Capability, software/qualification, parity, preparation, resource, reservation
  and custody checks still apply. It names every excluding field. This is the
  shared definition for resolve advice; it grants no portability or parity.
- `restrictions` has one row per project over retained jobs: jobs naming hosts
  (`host_restricted`), jobs with `targets` and how many of those are below the
  production tier, and `stranded_seconds`, the queue wait accrued while the
  maintenance `stranded` verdict above held for the job (job
  `efficiency.stranded_seconds`). `stranded_platforms` names the platforms
  (OS/architecture and accelerator kind, never hosts) with room now. Typed
  `host_memory_gb` and `cpu_cores` counts sit beside the mean typed-minus-learned
  peak where a learned figure exists. `advice` gives one action per item.
  `myce efficiency --project NAME` prints only that project's waits, steering
  cost and advice, without host rows (MY-31).
- `predictions` shows signed reservation-minus-accepted-peak and
  predicted-minus-actual-duration errors, with sizing basis and exact attempt.
  Memory comparisons require the existing accepted-learning classifier;
  constrained, failed and pressure-unknown peaks are excluded.
- `wasted_attempts` reports lost, killed, guard-stopped and OOM endings, excluding
  explicit operator and priority stops. The exact attempt's pressure window and
  minimum headroom accompany it. The first outcome also preserves fresh host
  headroom, pressure and swap observations across cleanup replays. `suspect: true`
  requires positive headroom with complete zero-stall/swap/OOM attempt evidence,
  or fresh first-outcome host evidence showing normal OS memory pressure and no
  swap growth. Adverse host evidence overrides an apparently healthy attempt
  window. Unavailable evidence is `null`, not healthy. Suspect is a diagnosis to investigate, not proof the
  stop was erroneous. Exact retained `resources` add CPU and I/O counters to each
  ending; the first stop observation also freezes host I/O pressure when available.
  Missing counters remain unknown; reservation cores are not utilization.

Host/program `measured_cpu_seconds`, `cpu_observed_elapsed_seconds` and
`cpu_observed_attempts` sum the collector's one cumulative record per exact
retained attempt, never successive heartbeat totals. `cpu_lower_bound` marks
sampled or incomplete/live counters. Nullable `read_bytes`, `write_bytes`,
`read_ops` and `write_ops` preserve measured zero versus unknown;
`io_observed_attempts` and `io_lower_bound` describe their coverage. The report's
`resource_attempts_dropped` names the owning history's first-plus-latest retention
loss. These full observed attempt windows can differ from the reservation
integrals' horizon; do not divide them to infer utilization over a common period.
Historical sizing basis is unknown if its matching memory record is gone.
Host `io_pressure` retains nullable PSI dimensions and source age; unavailable or
stale evidence cannot train a healthy zero. Fresh I/O pressure compares against
its own learned local baseline. No extra telemetry retention is introduced.

`efficiency_regression` uses the existing adaptive health notification path.
Reservation gaps, live blocker waits, stranded demand, accepted prediction errors
and wasted/suspect endings compare with their own local histories. It reuses the
host/request statistical model's bounded observation history, warmup, robust
quartile/observed-resolution band and consecutive confirmations. There are no
fleet-size, GiB, utilization-percent or hours-waiting alert thresholds. Adverse
samples cannot normalize a regression; healthy confirmations rearm delivery.
Cold or absent evidence is unknown. Alerts carry fixed metric codes and safe
identities through the existing episode/delivery-ID machinery.

Accounting is compact and stored with its owning job, so job removal also removes
its diagnostic contribution; there is no new age-based history policy or raw
sample log. Host aggregates follow inventory, and baseline subjects follow the
retained evidence. Bulk projection, sorting and baseline statistics run outside
`q.mu`; the existing maintenance commit applies monotonic observations and exact-job
fences. Unrelated heartbeats do not starve diagnostic publication.
Host identity uses sorted sibling inventory, so CPU/GPU heartbeat order cannot
reset accounting or alert warmup. Project wait history survives gaps between
queue episodes while its owning jobs remain retained; idle gaps resolve the
current condition without retraining that history with zeros. Job-only captures
omit the fleet report and its baseline maps.
Reads reuse the published generation. Sensor coverage and maintenance sampling
limits are evidence limits, not authorization to release custody or shrink needs.

## Adaptive evidence budgets (#811)

`evidence_retention` is an immutable maintenance summary of queue-host capacity,
soft budget, retained/protected bytes, reclamation and unavailable/over-budget
state. Status does no measurement or collection. See [retention rules and the
complete replacement inventory](retention.md).

### Launch capacity waits (#1011)

A typed `launch/resource_limit/memory_capacity` refusal requeues the same logical
job without charging infrastructure retries or losses. `execution_cause.admission`
retains the failed subcheck and compared safe values. Component
`execution_evidence` remains independent: helper custody can exist while main is
`not_started`, including after Started authorization. Cleanup still requires the
exact attempt's release before the logical job can run elsewhere.

`launch_capacity_wait` retains per-target admission evidence and an opaque capacity
fingerprint. A changed capacity measurement, allowance, competing reservation or
exact release permits reevaluation; heartbeat timestamps alone do not. Ordinary
lower-tier preemption can satisfy the measured deficit; equal-tier production
waits. `start_capacity_waiting` records any relative startup
window suspended by qualified resource contention, including explicit
`start_window` declarations. Typed uncharged launch refusals also suspend the
relative window. Status reasons show the original window's seconds left with
`paused for capacity`; assignment restores that full window for actual preparation.
An assigned start without an accepted acknowledgement continues counting down.

The separate `device_fault_start_wait` records queue `since` and the saved
`remaining` budget in nanoseconds for `device_fault_last_healthy`. Its relative
countdown says `paused for device recovery`; clearance resumes that saved amount.
`start_capacity_budget` retains that amount in nanoseconds through subsequent
capacity waits and assignment, separately from the original `start_window`.
A later started-attempt loss renews the original window and clears this override.
An unrecoverable hold fails with `diagnostic_code: device_fault_recovery_timeout`
one hour (the existing maximum start window) after queue-recorded fault loss.
Caller-owned absolute cutoffs remain authoritative.

Absolute `start_by`, give-up, ordering `deadline` and cancellation retain their
existing authority. No retry, reserve or timeout increase is involved.

### Committed job reads under queue load (#1052)

Point-in-time completion contract: orchestrator decision, 2026-11-09.

From the release containing this correction, per-job GETs capture committed
observation inputs from an atomically published generation. Cold reads, expired
responses and fan-out parents do not wait for the live queue transaction mutex.
An in-flight response completes at its captured generation; later commits
invalidate subsequent cache lookups instead of repeatedly restarting that read.
Terminal state and release confirmation come from the same captured generation;
the next read after an invalidating publication sees the committed update.
A read is a point-in-time observation, not a fence asserting that no commit
finished while the response was being built. Elapsed runtime is evaluated with
the injected read clock. Policy, custody and worker evidence remain from the
committed capture; mutation acknowledgements and accepted results retain their
separate authority. Allocation accounting still refuses work that cannot fit
measured memory headroom. Transport cancellation returns retryable 503
`status_unavailable`, rather than classifying an unavailable read as bad input.

From the next queue release, cold status captures copy only active-work and
worker-contact scalars under the queue mutex. Contact/loss derivation, retained
history traversal, decoding, diagnostics and response encoding run outside it.
Status still samples current log contact, pending check-ins and service grace;
this adds no eventual-consistency interval or response-field change. Existing
response-cache freshness and terminal-commit invalidation remain in force.
Slow-request `mutex_wait_ms` / `mutex_hold_ms` measure the live queue mutex;
response-cache refresh serialization is excluded. Earlier releases included
the entire cache refresh/build in those fields, so their status/job hold figures
alone cannot establish queue-mutex occupancy.

## Retained production admission waits (#868)

`admission_wait` on a production Job retains queue-clock first/latest waiting
snapshots and a capacity-sized history of distinct transitions. Snapshot construction
runs on immutable committed inputs outside the writer mutex. Unchanged waiting
sets and placement inputs reuse the previous explanation without building it;
sample expiry and minimum-runtime/release-credit boundaries force refresh. Only changed
blocker/holder/policy sets are published; scalar telemetry and ages describe
that snapshot's `at`, rather than every heartbeat. Publication rechecks the
committed generation; a competing mutation wins and records a capture gap. Each target
records its host/session/device, typed failed predicates, resource demand and
room, exact holders with current policy tiers, declaration/admission/charge/
forecast values and sample age. `preemption.considered` is captured from the
actual scheduler; diagnostic status reads never manufacture a decision.
`preemption.source` distinguishes `planner` and `diagnostic_projection`.
`preemption.planned` is the recorded decision's new-stop intent, not delivery
or release proof. The separate target field `release_commitment` reports
previously selected releases still pending; it never rewrites that decision.
It clears after the exact obligations are acknowledged, even when the job
remains blocked on resources and retains its selected placement.
`last_planner_decision` preserves the last actual decision's timestamp
and resource vector for this exact attempt/next piece/target, even when later
passes only wait for releases. It never labels a new projection as a past
planner decision. `resolution` separates `planner_candidate` from
`candidate_projection` and `serial_diagnostic_projection`. `device_fault` distinguishes known driver
quarantine from host RAM pressure. Several predicates may apply together.

`enforced_host_gb` with `enforcement_basis=started_worker_contract_not_direct_read`
comes from the successful Started native-cgroup contract, not a new controller
read. Other/older/prestart enforcement remains unknown; sampled RSS is
not kernel containment. Missing forecasts/ages mean unknown, while numeric zero
is measured zero. Interpret available memory with `host_memory_known` and
`device_memory_known`; unknown telemetry is not measured zero. Current reservation,
available RAM and physical use are separate
values and need not agree.

First/latest are retained as semantic endpoints. Additional transitions share
measured unused queue evidence headroom across waiting production jobs; unavailable
capacity grants no optional history. `dropped` and `budget_unknown` expose incomplete
coverage. `history_bytes` and `history_budget_bytes` report optional transition
cost and its grant. Under measured pressure the ordinary retention path can
shed optional history on closed jobs too. A capture that consumes one measured
ordinary-control service interval yields remaining snapshots; `capture_gaps`
and `deferred_at` expose that gap. Oldest observations are refreshed first.
No timer, sample-count cap or deployment-specific reserve is added.
`latest.closed_at` and `closed_reason` close the last wait on assignment, wave
admission, pause, cancellation or failure. Resume/retry can open another snapshot
while keeping the first endpoint. Parent evidence is not copied into a new
child's execution history. This is an observation-only record, including for parents before any
child exists. Retained samples survive assignment/retry and queue restart with
the job; existing job history retention still owns their lifetime.


## Actual all-tier admission decisions (#1158)

After a queued job has waited one advertised or measured worker send interval,
a real check-in can add `admission_wait.latest.targets[].admission_decision`
for production, deadline and experiment jobs. Unknown cadence grants no invented
start deadline. This threshold is an observation opportunity, not a start SLA;
`opportunity_seconds` records its basis. Earlier production diagnostic evidence
remains available independently.

The typed `outcome` is `rejected`, `not_selected`, or `not_considered`.
`stage` identifies `candidate_eligibility`, `placement`, or `checkin`.
`predicate` names the first actual failed branch (for example `tools`,
`checkpoint`, `maintenance`, `release_pending`, or `resources`). A fitting
candidate that loses ordering records `not_selected` / `selection_order`.
An early return records `not_considered`, with reasons such as `attempt_reports`,
`existing_assignment`, `recovering`, `leaving`, `benchmark`, or `cleanup`.
`selection_not_reached` means this target did not reach candidate evaluation;
`candidate_unavailable` means no variant emerged without a more specific
captured predicate. It does not imply a diagnosed underlying cause.

`at`, `checkin_machine`, `checkin_session`, and optional `device` identify the
actual observation. Unchanged outcomes reuse their original timestamp; absence
is unknown, never proof of consideration. Each target retains its most recent
distinct decision for the same attempt/piece and worker session, including
check-ins on other targets. Removed inventory expires that target's endpoint.
The sibling `predicates` remain diagnostic projections; they never claim this
check-in executed those branches. `resolution=admission_decision_only` means
only the real decision could be captured, without a concrete diagnostic vector.
Its zero resource fields are placeholders, not measured capacity.

Only compact enum facts are collected during real selection. Construction,
comparison and history encoding run outside the writer lock and use the same
committed-generation fence, capture gaps, measured adaptive history budget and
lifecycle closure as production evidence. No status read records a decision.
Per-target endpoints are bounded by current inventory; optional transitions
share measured evidence headroom across all observed waiting jobs. Missing or
dropped history cannot explain a past event retroactively.

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

Each snapshot names the currently running attempts on that device, their exact
job/attempt/session and PIDs, and their latest existing host/device samples with
the original sample timestamps and device source. Missing samples remain absent.
Readable Linux memory/page state includes `/proc/meminfo`, `vmstat`, `buddyinfo`
and memory PSI; missing or truncated sections are listed. Compressed-memory
counters are available only where the kernel exports them in these tables.
Capture follows kernel-log observation, not the actual allocation instruction:
compare `kernel_at`, `captured_at` and sample times before drawing conclusions.
Attempt eligibility is evaluated against each kernel timestamp even when burst
lines share a capture. Without a cgroup, birth-verified owned process groups
include payload descendants; missing ownership is reported explicitly. Exited
processes and allocations between samples can remain unknown.

Affected jobs expose `fault_evidence`; terminal `attempt_history` retains the
same exact-attempt records. Old sessions cannot attach evidence to a new attempt.
Histories evict oldest snapshots to a fault-evidence envelope per device/current
attempt: the worst-case escaped size of one four-table host capture (1.5 MiB), with `replaced` and `omitted_attempts` recording loss.
Attempt history follows the existing unique-attempt retention contract. Default
fleet status omits these snapshots; inspect a job or request full status. This is evidence, not
an admission, retry, release or root-cause signal.

Fatal native SIGSEGV/SIGBUS/SIGABRT outcomes include
`attempt_history[].signal_evidence`: signal, PID, exit observation, kernel fault
address when readable, and loaded executable/library mappings from matching
systemd-coredump metadata (basename, address/offset and filesystem device/inode).
These are mapped identities, not ELF build IDs. A shell-reported exit status
alone does not prove a signal; matching child-PID kernel/core evidence is needed,
and the signal otherwise remains `unknown`. Exits are published before optional
metadata I/O so teardown classification does not wait for capture. Core lookup
matches the current boot, retained payload PID and execution lifetime, with
second-resolution kernel crash timestamps. Systemd does not export a process
birth value for independent comparison. Queries
request complete selected journal fields within the capture byte budget. Native
namespace main members use their joined payload PID. Other wrapped commands use
exact-attempt cgroup core metadata where available; absent that identity, capture
reports unknown rather than recording the launcher as the payload.

`core_capture` distinguishes a disabled inherited soft limit, an existing pipe
handler/file pattern without a confirmed record, a recorded core, and unavailable
capture. A zero soft limit disables file-pattern dumps; Linux pipe handlers
ignore that limit, so their availability remains unconfirmed without a record.
`core_pointer`, size where readable and truncation metadata refer to an
existing core; Mycelium never reads, uploads, enables or deletes core bytes.
Core storage/expiry remains the operator's configured policy; only the bounded
pointer metadata follows Mycelium attempt retention. Core metadata can arrive
after the immediate exit lookup; a missing record does not prove no core exists.
Native stacks/registers and ELF identities must be recovered privately from that
operator-owned core to establish a cause. Hosts with a zero soft limit or an
unsuitable handler need a separately authorized operator task. No core limit,
core pattern, sysctl or driver setting is changed automatically.

Capture excludes environment values, argv and raw journal/core text. Older
queues negotiate without the additive fields; the worker keeps local receipts.
GPU-host/native capture qualification remains separate from synthetic laptop
proof. A recorded event is never itself a causal finding: report `unknown` until
the retained evidence establishes the actual chain.

## Shared owner yield and measured CPU (#389)

`cpu_in_use_cores` includes shared-host measured overuse, rounded up and never
below declared cores. Job `cpu_observation` retains the exact sample identity,
previous elapsed/usage counters and logical-job `peak_cores` lower bound. The
first reading establishes a counter origin; subsequent adjacent readings raise
the learned rate. The CPU `resources` entry exposes this learned need. It remains
valid through checkpoint/requeue, including missing or quieter later telemetry;
it does not predict a different submission.

`shared_host_pressure` additionally reports `cpu_known` and `cpu_psi` on Linux.
Memory/CPU PSI and owner latency use this host's learned healthy bands; macOS
memory warnings and existing host-relative swap-growth/latency health also
withhold admission. Every running job on the host, production included,
receives `shared_host_pressure` stop/requeue provenance (MY-45); resumable jobs
get checkpoint grace. Native containment and stress
qualification remain separate from these diagnostics and synthetic tests.
