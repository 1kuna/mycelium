# Queue file store

Retention, capacity, the production reserve, per-project quotas and early release
for the queue's file store. Keep the file store with the queue database when
[backing up or moving the queue](../operations.md#queue-backup-restore-and-relocation).

## Stored job file retention

Frozen extension bundle bytes and their snapshotted environment files live in
the queue file store once per content digest. Jobs, retained keyed answers and
the extension registry hold small references; control transactions do not copy
the bundle bytes. Worker delivery and digest downloads verify and load the bytes
outside the queue mutex. Keep the file store with the queue database when moving
or backing up queue state.

Startup verifies frozen bundle references before serving traffic. Missing or
corrupt blobs and retired embedded bundle formats stop startup. Keep the same
`--file-store` root on restart (default `DATABASE.files`). Go callers select a
custom root with `WithFileStore` in `Open`; changing roots after bundle references
exist is refused. Only current and previous release formats are supported.

Stored job inputs and results stay until they are released or a retention
window expires them. By default `myce queue --file-retention` is `72h`: a
finished job's files are kept for 3 days, then reclaimed ([MY-47](../DECISIONS.md#my-47)).
Pass another duration to change the window, or an explicit `0` to keep unique
bytes until [explicit release](#project-owner-early-file-release-465). Installed
queues accept `myce install queue --config QUEUE_CONFIG --file-retention DURATION`
and preserve the setting across upgrades; an installation keeps its saved window,
including a saved `0`, until its owner changes it. This is separate from log
retention, and applies in NAS and file-store checkpoint modes.

Code bundles held as job inputs follow the same release and project-retention
rules. Once only released or expired jobs reference them, collection keeps the
ordinary publication grace, without another queue-wide window from upload.
Live, queued and unexpired jobs, including another project's retained jobs,
still protect shared bytes. Frozen extension/environment references, catalog
assets, models and checkpoints retain their independent protection. Unclaimed
bundle uploads keep the queue-wide orphan window and publication grace.

### Per-project file retention (#1214)

A project policy can set its own window with `file_retention` (a Go duration,
for example `72h`), through `myce policy set` like other project settings:

```yaml
projects:
  example:
    weight: 1
    file_retention: 72h   # this project's finished jobs release files after 3 days
```

It applies to the project's terminal jobs, including jobs that finished before
the policy was set, and to their [worker scratch](worker-caches.md#worker-scratch-retention).
It can only shorten the queue-wide window: with the default `--file-retention 72h`,
a project `file_retention: 24h` expires after 24 hours, and `1000h` still expires
after 72 hours. Empty keeps the queue-wide policy. As with the limits below, a policy PUT
from an older CLI keeps the stored value; a current CLI's file can clear it.

Job records and keyed answers stay, including their file identities and
`files_expired: true`. A keyed retry returns the original answer and never reruns
expired work. Result helpers return HTTP 410 `result_expired`; raw input/result
fetches return that typed error once their bytes are reclaimed. Open downloads
can finish; callers returning after the window must accept expiry. Increasing
retention does not restore expired data. Historical terminal jobs without a
recorded finish time receive a full window from first observation.

Reclamation preserves blobs needed by non-terminal or unexpired jobs, any
checkpoint generation, catalog models, extensions and environments. It runs from
`Queue.Tick`, with filesystem work outside the queue lock. There is no job-record
pruning or result acknowledgment; owners release early with `myce file release`
(below). Collection takes one record at a time, yields to waiting control work,
and records completed reclamation on the existing job, so later passes do not
revisit those hashes. Publication and successful blob HEAD verification refresh a
durable mtime. Grace follows measured first settlements of input claims. A peak
stays in use while it is within the configured retention window or its own
measured duration. When existing observations age out, the latest measurement
supplies grace. Publications at or before the first new measurement after an
upgrade keep their prior protection. Ordinary expired job files cap grace at
the configured retention window. A committed reference keeps the bytes, and
the job's result-recovery deadline still applies.

Storage removal/stat failures are logged and retried on the next pass; they do
not stop queue maintenance. A busy publication/verification fence logs
`file retention deferred` once per consecutive streak of skipped passes.
Only uploads positively marked as job inputs by the CLI/profile, Go `UploadJobInput`
or Python `upload_file` are eligible for orphan reclamation. Their eligibility and
publish time survive restart in the existing queue SQLite state. They must have no
job, keyed-answer, checkpoint, catalog/model, extension, environment or pending
reclamation reference, and both publish time and mtime must be older than the
configured retention window and the publication grace. A committed
job reference clears the marker; terminal-job retention then applies. Unmarked
imports (`myce file put`, Go `UploadFile`, models and older clients) are never
orphan-swept, even if no model catalog is loaded or an entry is removed. Reusing an
unmarked import as a job input cannot mark it; a generic import clears any marker.
Marked interrupted tus uploads are reclaimed after an idle retention window and
the publication grace. POST/HEAD/PATCH refresh their durable idle timestamp using
the queue Clock; active requests and all protected references survive collection.
Unmarked imports and unknown files remain for explicit inspection.

`myce status` and `/api/status.file_store` report the queue-file-store's payload
`used_bytes`, admission `free_bytes`, and currently `reclaimable_bytes`. Free space
is the smaller of the configured store allowance and disk space remaining after
existing tus lengths and the shared disk floor (1%, minimum 256 MiB). The JSON
also exposes `limit_bytes`, `disk_free_bytes`, `floor_bytes`, and an explicit error
when the reading is unavailable. Status never deletes files.

**Deploy order:** deploy the queue before new CLIs. Older queues return HTTP 404
for `/api/files/preflight`, causing every profile submission to exit 69.

Profiles preflight the complete input manifest through `POST /api/files/preflight`
before the first upload. Preflight runs ordinary retention maintenance; upload creation repeats capacity
admission. **Before refusing for space, preflight reclaims what is already
authorized:** it expires jobs whose retention deadline has passed and collects
expired or owner-released files until the manifest's bytes are covered, without
waiting for a maintenance pass. It never deletes anything retention or a release
has not already authorized, and it stops once the request fits. Collection skips
files inside their publication grace and yields if an upload holds the store's
publication fence; maintenance collects those later.

Capacity refusals use HTTP 507 with code `file_store_capacity`, the
public store name, needed/free/shortfall byte counts and recovery instructions;
profile receipts preserve the reason and exit 75 for retry. The reason names the
bound that set free space: `bound=store_allowance` (the `--file-store-bytes`
limit) or `bound=volume` with the bytes other data uses on the queue volume. It
then gives the bytes finished jobs hold until release, the queue and project
retention in force, the requesting project's share, and the next step: release
finished work with `myce file release plan`/`apply`, set the project's
`file_retention`, or (operator) free volume space or raise `--file-store-bytes`. A failed capacity
reading returns HTTP 503 `file_store_unavailable`. Preflight makes no reservation:
other writers can consume space afterward, and unknown output sizes cannot be
predicted. Protected data is never evicted to admit work; increase the configured
store allowance or free disk space when retention and release cannot help. Worker file caches
already evict reproducible copies under pressure. See [files](../files.md).

## Production file-store reserve

`myce queue --file-store-production-reserve BYTES` (also supported by `myce dev`)
protects part of the existing `--file-store-bytes` allowance for production-tier
work. Zero, the default, preserves existing admission. The reserve must be
nonnegative and no larger than the store limit. It adds no disk space and changes
neither retention nor deletion behavior. Existing blobs and admitted resumable
uploads remain usable.

For an installed queue, set both values through the supported installer (byte
counts, not GiB suffixes). For example, a 200 GiB limit with a 16 GiB reserve:

```sh
myce install queue --config QUEUE_CONFIG \
  --file-store-bytes 214748364800 --file-store-production-reserve 17179869184 --dry-run
myce install queue --config QUEUE_CONFIG \
  --file-store-bytes 214748364800 --file-store-production-reserve 17179869184
```

These flags also work on a fresh queue install. They are saved as
`file_store_bytes` and `file_store_production_reserve` and included in the
generated service. Omitted flags preserve saved values across reinstalls and
upgrades. Fresh installs and older configs without these fields keep the
existing defaults: 100 GiB capacity and zero reserve. Set
`--file-store-production-reserve 0` explicitly to disable the reserve.

To retire an existing systemd `ExecStart` drop-in during an authorized upgrade:

1. Inspect `systemctl --user cat myce-queue.service` and record the effective
   capacity, reserve and any other custom arguments. Pass the existing byte
   values explicitly to the installer; it does not import drop-in arguments.
2. Preview the install with those flags and the exact saved config. Drain/disable
   admission using the normal upgrade procedure. Preserve a private copy of
   the drop-in outside the unit's `.d` directory. **Keep the override in place**
   and run the install with those flags plus `--save-settings-only` to save the
   replacement settings and generated base unit. For example:
   `myce install queue --config QUEUE_CONFIG --file-store-bytes CAPACITY_BYTES --file-store-production-reserve RESERVE_BYTES --save-settings-only`.
   Include every other custom setting you intend to keep, including loss timeouts
   and the discovery bind. This mode does not upgrade the binary, reload systemd
   or restart the queue. Exit zero means settings saved, **not activated**.
3. Check the saved config's `file_store_bytes` and
   `file_store_production_reserve`, and inspect the generated base unit at the
   saved `service` path. Verify that its `ExecStart` includes both flags with the
   intended values, and compare all other custom arguments too. The discovery bind
   is read from the saved config through `--service-config`. If installation or verification fails, leave the override
   in place and fix the problem before proceeding.
4. Remove only the overriding `ExecStart` directives (including the empty reset),
   or the whole file if it contains nothing else. Preserve unrelated settings;
   migrate any other custom arguments before removing their override. Run
   `systemctl --user daemon-reload`, then
   `systemctl --user restart myce-queue.service`.
5. Verify the effective unit with `systemctl --user cat myce-queue.service` and
   `systemctl --user show myce-queue.service --property=ExecStart`. Confirm both
   values in the running command and `myce status --json` file-store fields
   (`limit_bytes`, `production_reserve_bytes`) before resuming admission.
   If verification fails after removal, put the old drop-in back at its original
   path, run `systemctl --user daemon-reload`, restart the queue, and verify the
   restored values before resuming admission.

A normal install refuses before activation with `queue_execstart_override` when
an effective full `ExecStart` override hides or differs from the generated
arguments. The error names each differing flag without exposing values. Equivalent
arguments (including `60s`/`1m0s`, explicit default `--file-retention 72h`, and the
saved discovery bind) remain accepted. Use `--save-settings-only` only to prepare
retirement; it does not claim the running queue uses the saved settings. After
retirement, run a normal install if the binary also needs upgrading. Future
capacity/reserve changes should use install flags instead.

New identified non-production allocations that would consume the reserve receive
HTTP 507 `file_store_capacity`, naming the reserve, total free space and the bound
that set it. Preflight first reclaims already-authorized files as described above. Preflight, upload creation,
internal registry bundles and declared async output reservations use this check.
Production output commitments back the reserve rather than being charged twice.
Production still needs actual store/disk space above the physical floor and
existing commitments.

**Unidentified writes can use the production reserve.** Deployed input clients
and old native workers send the shared token without a project or attempt
identity; the queue cannot distinguish their production bytes from experimental
bytes. The compatibility exception favors production admission and also permits
unidentified experiments/imports to consume protected capacity. A configured
reserve therefore does not guarantee isolation from those legacy writers.
Input-client identity is follow-up work under #464; per-project quotas are below.

New workers send `X-Myce-File-Attempt` containing the job ID, attempt, machine and
session on native/async result and checkpoint uploads. The authenticated queue
checks that identity against the current execution and derives the tier from the
job, ignoring project/class claims for those outputs. Replaced/lost/cancelled
attempts cannot allocate or resume uploads; an exactly accepted attempt can reuse
its blobs after a lost completion acknowledgement. Existing async workers' exact
pending-result identities are also classified from their owning jobs. Known
experiment writes and internal registry bundles cannot use the exception.

For inputs, Go callers may wrap preflight/upload contexts with
`myce.WithFileStoreSubmission(ctx, spec)`; the profile CLI supplies these hints.
HTTP callers can send `X-Myce-Project` and optionally `X-Myce-Class`. The queue uses
the project's `default_call_priority` (experiment when absent) and effective
policy tier. Class hints can narrow that decision, never promote it; claiming
`production` cannot upgrade a server-known experimental project. These are
shared-token accounting hints, not per-project authorization credentials.

`myce status` and `file_store` JSON report `production_reserve_bytes`,
`unidentified_writes_may_use_reserve`, and `unidentified_reserve_admitted_bytes`.
The last counter is the cumulative portion of successful unidentified tus create
admissions crossing the protected floor **since queue start**. It counts declared
allocation, including incomplete uploads, not bytes transferred or current usage.
Preflight, resumable replay and deduplicated reuse do not increment it. It resets
on queue restart and decreases neither on completion nor on ordinary expiry.

### Per-project file-store quotas (#464)

`file_store.projects` in `myce status` lists each project holding store bytes or
configured with a limit. `retained_bytes` counts digests protected by the
project's unexpired jobs, keyed answers, checkpoints and pending outputs, each
digest once per project; a digest shared by two projects counts toward both but
once in `used_bytes`. `added_bytes_24h` counts digests first referenced by the
project in the last 24 hours on the queue clock: inputs at submission, results
at completion. Releasing or expiring files lowers `retained_bytes` at once but
does not refund `added_bytes_24h`. `reserved_output_bytes` is the project's share
of declared async output commitments. Sizes come from job manifests, so the walk
needs no store I/O.

Two optional project policy fields limit non-production admission; zero or
absent means no limit, which is the default:

```yaml
projects:
  example:
    weight: 1
    max_retained_file_bytes: 21474836480   # 20 GiB held at once
    max_added_file_bytes_24h: 2147483648   # 2 GiB added per rolling 24 h
```

Set them through `myce policy set` like other project settings. A policy PUT
without `?file_store_limits=1` (an older CLI, which cannot carry these fields)
keeps a project's existing limit wherever it sends zero; current CLIs send the
marker, so removing a limit from the file clears it. A non-production
submission whose new inputs plus declared `disk.output_bytes` (and the project's
other declared async outputs) would exceed either limit is refused with
`project_file_store_quota` and a reason naming the limit, the cleanup command
and the advice to keep large outputs in project storage. Input preflight and
upload creation that name the project (`X-Myce-Project`, as the profile CLI
does) return the same refusal before any bytes upload. Like the reserve, that
name is a shared-token hint: an upload without one is not checked, and its
submission is still refused. Accepted completions are never
refused or dropped: a result that overshoots stays, `refusal` appears on the
project's status entry, and the project's next non-production submissions are
refused until a release or retention lowers retained usage, or old additions
leave the 24-hour window. Production submissions are always admitted; the
production reserve and store capacity still apply to them. Status builds this
accounting from a list copied during its existing reference walk and totals it
after releasing the queue lock; submit and upload checks walk only the limited
project's jobs.

### Parked native storage integration (#564)

The unfinished native protection, automatic-admission migration and versioned
worker-local task-output implementation is preserved on [`park/storage-protection`](https://github.com/1kuna/mycelium/tree/park/storage-protection).
Main has no backend/activation command or worker-local delivery client.
Use ordinary accepted file-store outputs or `output_delivery: async_nas`.
Configured file-store caps, production reserve, declared-output commitments,
legacy disk floor, checkpoint transfer and custody fences remain active.

Retained database fields remain readable and preserved across updates/reopens.
A retained protected task is held rather than silently executed without its
boundary. A persisted, exact collected acknowledgement allows ordinary owner
file release, retention and history cleanup to drain settled records. Uncollected
or mismatched custody stays protected; reconcile it with the preserved parked
implementation before switching that worker to main. Startup reads protected
child state even if a crash preceded the root discovery/index write, and refuses
unresolved custody before changing the session or claiming the worker is empty.
Readiness/activation flags retained from the parked implementation cannot retire
the configured guards. Future native implementation/qualification is tracked in
[#504](https://github.com/1kuna/mycelium/issues/504).

### Project-owner early file release (#465)

Use explicit job IDs to release terminal input/result files before their normal
retention deadline. **Ownership means project attribution under the shared fleet
credential, not an authorization boundary.** Any holder of that credential can
act for a project; this does not add individual owner authentication. Global
retention and capacity admission remain unchanged.

First close the application operation: all downstream consumers and controller
reconciliation must have finished using these files. Include every owning
terminal reference you want to expire, including fan-out parents, children and
keyed answers' job IDs. Other retained owners continue to protect shared content.
A successful stage alone does not establish that application completion boundary.
For ordinary tasks, accepting expiry explicitly gives up queue recovery of the
selected data, including unsuccessful tasks you select. Preserve any required
source/result copies with verified identities before doing so. Unselected work
keeps its ordinary retention. For tasks using `output_delivery: async_nas`, the
queue additionally requires its existing verified delivery receipt to match the
accepted execution and manifest; pending/failed or mismatched delivery blocks
release. An unavailable NAS is handled by the existing asynchronous delivery
retries; release never tries to copy or wait on the NAS.

```sh
# Read-only: save and inspect the returned JSON before applying it.
myce file release plan --project example JOB_ID_1 JOB_ID_2 > release-plan.json
# Irreversible expiry acceptance for exactly the reviewed project/jobs.
myce file release apply --accept-expiry release-plan.json
```

Both commands accept the usual installed authority options (`--config`,
`--joining-file`, `--queue`). Keep the saved plan/receipt private when it contains
private project or file names. Supply each ID as a separate argument. For a shell
array, use `"${jobs[@]}"`. Duplicate IDs are accepted and counted once.

The API equivalents are
`POST /api/files/release/plan` with `{project, jobs}` and
`POST /api/files/release/apply` with
`{selection: {project, jobs}, plan_id, accept_expiry: true, collection_candidates}`.
Copy `selection`, the plan's `id` as `plan_id`, and any
`collection_candidates` from the reviewed response. The CLI does this from the
saved file. API callers that omit the candidate fingerprints can apply when the
live digest matches. A plan is a read-only snapshot and reserves no files. The
digest binds selected files and custody, presence and size for every selected
blob, and publication time and bundle tracking for collection-eligible blobs.
When publication grace elapses, a new plan can show more `reclaimable_bytes`
under the same digest. Regenerate unapplied plans saved before the queue upgrade.

The plan lists each job's state, release/retention state, original expiry deadline,
input/result/shared-input/dependency/diagnostic/fan-out manifests, and delivery
evidence where applicable. `physical_custody` includes `custody_blockers` naming
each blocking job's
`job_id`, `state`, and one stable relationship `path` from the selected job.
Each path step has a `relationship` (`after` or `fan_out`) and `job_id`; an
empty path means the selected job itself. For example, a released recovery job
can be blocked by `after` evaluation followed by `after` serving, even when
its own copies have `release_confirmed`; aggregate `release` remains `pending`.
Other projects' blockers and intermediate steps have `other_project: true`
with job identity and state omitted.
Each content identity appears once in the byte total,
using its current stored size. `reclaimable_bytes` counts only bytes removable
now after selection: retained job references, checkpoints/resume generations,
model/catalog assets, extension/environment assets and bundles, pending output
and unresolved custody remain protected. The plan lists the references that
block each identity and flags publication grace or bundle retention separately.
`bundle_tracked` records upload tracking; it does not by itself pin bytes.
Expired or selected releasable job files do not keep a separate bundle-retention
floor, and count in `reclaimable_bytes` once publication grace and all keeping
references have cleared. `bundle_protected` describes that separate floor;
retained owners remain listed under `references`.
Absent blobs contribute zero. Recently published or HEAD-verified blobs keep
their measured publication grace. An eligible plan can reclaim zero bytes while
still expiring the selected identities.

`collection_eligible` uses the collector's pending reclamation and keeping
references. Importing identical bytes after completed reclamation does not
re-enroll them through the old job's manifests, so a new plan counts zero bytes
for that content unless another pending reclamation owns it.

Apply requires explicit acceptance. It checks the reviewed job custody and
collection scope again in the expiry transaction. More keeping references leave
the plan valid even if a blob becomes ineligible for collection. Republishing or
HEAD-refreshing a blob that stays ineligible also leaves the plan valid.
A newly eligible blob, changed publication time for a blob that remains
eligible, or changed selected job custody invalidates the review. HTTP 409
`file_release_stale` means nothing was expired. Generate and review a new plan.
HTTP 409 `file_release_protected` means a selected job is live, has unresolved
physical custody or lacks required verified delivery. Include only eligible jobs
in the next plan. Other projects' retained references can block physical removal
without blocking expiry of your selected job references.
HTTP 400 `file_release_invalid` gives the failed check, such as an unavailable
selection, unconfigured store, or unreadable stored metadata. The response gives
the same selection error for an unknown job and a job outside the project.
An ID with embedded whitespace gets a separate error explaining how to pass
multiple IDs.

A successful apply returns `collection: scheduled`: it durably sets the existing
job file-expiry/reclamation state, preserving identities, manifests and a small
`file_expiry_accepted` ledger receipt (plan digest in `reason`, acceptance time
in `at`) in the job/keyed answer.
The existing bounded maintenance collector rechecks references and publication
grace before unlinking, retries failures, and resumes after restart. This is
asynchronous reclamation, not a promise that capacity is immediately available.
No new deletion daemon or global retention change is involved. Repeating the
same apply after a lost response returns the original receipt while the selected
job/keyed answers remain retained, even after the blobs are gone or a restart.
Ordinary history/key expiry still applies afterward; unknown/expired selections
are refused without creating jobs. Retrieval retains typed HTTP 410
`result_expired` behavior for expired, unshared content. Save the plan and apply
response for the application's audit record.
