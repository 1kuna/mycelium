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
It can only shorten the queue-wide window: with `--file-retention 168h`, a project
`file_retention: 24h` expires after 24 hours, and `1000h` still expires after 168
hours. Empty keeps the queue-wide policy. As with the limits below, a policy PUT
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
revisit those hashes. Publication and successful blob
HEAD verification refresh a durable mtime; collection waits six hours after that
refresh. This covers the gap from the first file through later multi-GB uploads,
slow NAS, or a queue outage/deploy before submit/complete. Only refreshed blobs
receive the grace; a new committed reference then keeps the bytes.
This publication grace does not extend an expired job's result-recovery window.

Storage removal/stat failures are logged and retried on the next pass; they do
not stop queue maintenance. A busy publication/verification fence logs
`file retention deferred` once per consecutive streak of skipped passes.
Only uploads positively marked as job inputs by the CLI/profile, Go `UploadJobInput`
or Python `upload_file` are eligible for orphan reclamation. Their eligibility and
publish time survive restart in the existing queue SQLite state. They must have no
job, keyed-answer, checkpoint, catalog/model, extension, environment or pending
reclamation reference, and both publish time and mtime must be older than the
configured retention window, outside the six-hour publication grace. A committed
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
   and run the install with those flags to save both replacement settings. The
   installer warns that the override still takes precedence; any restart during
   installation continues to use that override.
3. Check the saved config's `file_store_bytes` and
   `file_store_production_reserve`, and inspect the generated base unit at the
   saved `service` path. Verify that its `ExecStart` includes both flags with the
   intended values. If installation or verification fails, leave the override
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

A full `ExecStart` override hides new arguments in the generated base unit;
future capacity/reserve changes should use install flags instead.

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
private project or file names. The API equivalents are
`POST /api/files/release/plan` with `{project, jobs}` and
`POST /api/files/release/apply` with
`{selection: {project, jobs}, plan_id, accept_expiry: true}`. A plan is a
read-only snapshot, not a reservation. Its digest identifies the reviewed
custody, references and stored content; it is not a signature or an
authorization token. Content aging out of publication grace raises
`reclaimable_bytes` in a new plan but leaves the digest unchanged, so a reviewed
plan stays valid; a new publication or HEAD refresh of selected content does not.

The plan lists each job's state, release/retention state, original expiry deadline,
input/result/shared-input/dependency/diagnostic/fan-out manifests, and delivery
evidence where applicable. Each content identity appears once in the byte total,
using its current stored size. `reclaimable_bytes` counts only bytes removable
now after selection: retained job references, checkpoints/resume generations,
model/catalog assets, extension/environment assets and bundles, pending output
and unresolved custody remain protected. The plan lists the references that
block each identity and flags publication grace or bundle retention separately.
Absent blobs contribute zero. Recently published/HEAD-refreshed blobs retain the
ordinary six-hour publication grace. An eligible plan can reclaim zero bytes
while still expiring the selected identities.

`collection_eligible` uses the collector's pending reclamation and keeping
references. Importing identical bytes after completed reclamation does not
re-enroll them through the old job's manifests, so a new plan counts zero bytes
for that content unless another pending reclamation owns it.

Apply requires explicit acceptance and recomputes the reviewed plan, then fences
custody and reference metadata again in the expiry transaction. HTTP 409
`file_release_stale` means regenerate and review a new plan; nothing was expired.
For an active project, apply soon after planning: a new job sharing selected
content adds a reference and stales the plan.
HTTP 409 `file_release_protected` means a selected job is live, has unresolved
physical custody or lacks required verified delivery. Include only eligible jobs
in the next plan. Other projects' retained references can block physical removal
without blocking expiry of your selected job references.

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
