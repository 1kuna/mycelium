# Files, results and checkpoints

Use immutable `files` for private inputs, `shared_inputs` for verified read-only
large assets, and accepted `output_refs` for successful results. A NAS is optional
for standalone file transport; the automated installer still requires a verified
NAS mount. Failed-attempt diagnostics are separate from accepted outputs.

- [Submit and transfer files](#start-and-submit)
- [Failed-attempt recovery](#recover-outputs-from-a-failed-terminal-attempt)
- [Atomic checkpoints and resume](#resumable-jobs)
- [Asynchronous NAS delivery](#complete-from-queue-storage-deliver-to-nas-later-349)
- [Worker-local destination tasks](#worker-local-task-outputs-508)

The versioned worker-local destination route is parked. Ordinary accepted outputs
and asynchronous NAS publication remain active. See the
[pending storage proposal](design-learned-storage.md) and
[parked integration](development.md#backed-storage-admission-integration) for
future backend work.

The queue's always-on hypha stores immutable files. Every execution hypha has
its own bounded cache and private attempt folders. No shared mount is needed.
Stored job inputs and their results use this v3 file transport in either
checkpoint mode. Jobs without explicit NAS roots also use it for checkpoints.
One store is **not a backup**. Losing its disk can lose the only authoritative
copy; a cache is not a promised second copy.

## Start and submit

`myce queue` uses file-store checkpoints by default, with files beside the
SQLite database in `DB.files`. `--file-store DIR` chooses that local disk folder;
`--file-store-bytes` bounds complete files plus reserved interrupted uploads
(default 100 GiB). Keep the database and file store on durable local storage.
Existing queue `--checkpoints` and hypha `--checkpoint-root` NAS configurations
keep their original checkpoint behavior. Supplying queue `--checkpoints` selects
NAS checkpoints; uploaded job inputs and their verified results still use the
queue's local file store. Model files can use either source independently.
Keep the same checkpoint mode and storage folder across restarts.

Use the existing private invitation and pinned HTTPS joining endpoint for hyphae
on other machines. File APIs use the same authentication and TLS connection as
the queue API. Plain loopback HTTP remains available for laptop tests. Digests
are identities, never credentials. Keep tokens and invitation files private.

For a laptop trial, in a fresh folder:

```sh
myce dev --file-store ./store --workers 2
```

Prepare a job using ordinary relative input mappings, then upload just those
inputs before submission:

```yaml
name: example
project: demo
priority: experiment
program: {type: pytorch-script, file: train.py}
inputs: {train.py: train.py, data.json: data.json, weights.bin: weights.bin}
outputs: {answer.json: results}
resumable: true
```

```sh
myce submit --files-from ./source job.yaml
```

The extension still needs explicit setup and qualification on each hypha, as
explained in [write an extension](adapter.md). The CLI sends the
relative job filenames, byte counts and SHA-256 identities; it does not send
local source paths. Only explicitly mapped inputs are uploaded, never a whole
project or home folder. Inputs are copied into a private working folder before
running. Programs cannot mutate shared cached blobs. Plain commands can also
use `files` with relative command arguments and declared outputs; stored inputs
cannot be combined with a caller workdir or NAS input mappings.
A NAS-mode job declaring any stored `files` returns all its outputs through the
file store rather than the worker's `--job-file-root` (`JobFileRoot`).

For API/Python dict submissions, upload each file first with the file API or CLI
and replace `inputs` with `files`:

```yaml
files:
  train.py:
    sha256: REPLACE_WITH_THE_FILE_SHA256
    size_bytes: 1234
```

`myce file put FILE` prints this identity as JSON. `myce file put|get
--joining-file PRIVATE_INVITATION ...` uses pinned HTTPS. The accepted successful
job's `output_refs` maps output names to identities; retrieve an output with:

```sh
myce file get --size-bytes BYTE_COUNT SHA256 ./answer.json
```

### Folder and glob outputs (#1341)

An output name may be a file, a folder or a glob, so a job never needs to tar
its results:

```yaml
outputs:
  report.json: results   # one file
  plots/: results        # every regular file beneath plots/
  "logs/*.txt": results  # a path.Match glob; a matched folder contributes its files
```

`output_refs` stays a flat list of individual files named relative to the work
folder (`plots/a.png`, `logs/run.txt`), and each one is size- and SHA-256-checked
exactly like a single declared file; `myce result JOB_ID --out DIR` recreates the
subfolders. A folder must exist and a glob must match, each with at least one
regular file; a symlink or special file inside one fails the job rather than
being skipped. `*` stays within one path segment (there is no `**`). Patterns
skip declared input `files` and worker-owned `.myce-*` entries. The queue accepts
only files covered by a declaration, from the current attempt.

Before uploading, the worker preflights the whole expanded manifest against the
file store. If the store cannot admit it (byte allowance or project quota after
reclaiming released files, or a file list larger than the queue's measured
request capacity), the attempt fails once with `execution_cause.finalization`
`{step: output_preflight, error: limit_exceeded}` and nothing is uploaded. Narrow
the declaration or release finished results; the files stay in the attempt's
scratch for [recovery](#recover-outputs-from-a-failed-terminal-attempt). With
`output_delivery: async_nas` the existing output reservation decides admission
instead. Folder and glob outputs apply to plain commands that return results through the file
store, including `output_delivery: async_nas` (whose NAS copy keeps each file
under its declaration's prefix); `fan_out`, `after` stages, `output_destination`,
`storage_protocol` and extension jobs still name individual files.

### Keep files from a failed attempt (#1341)

`keep_on_failure` publishes named files for a failed attempt, so a command never
has to exit 0 just to return its logs:

```yaml
keep_on_failure:
  - logs/                                  # file, folder or glob, as for outputs
  - {path: junit.xml, format: pytest-junit} # or vitest-junit
```

A failed attempt (non-zero exit, or an output-contract failure after exit 0)
publishes each match as `kept/NAME` beside `summary.json` and `log-tail.txt` in
its diagnostics; the job stays failed and has no accepted outputs. An optional
`format` parses that JUnit report's failed test identifiers into
`summary.json`, as `diagnostics.reports` does. A missing or unreadable entry is
noted in `summary.json` `report_errors` without dropping the others. The kept set
is preflighted like outputs: if the store cannot admit it, none is kept and
`report_errors` says so. Retrieve with `myce result JOB_ID --attempt N --out DIR`.
`keep_on_failure` needs a plain command with stored input `files`, like
`diagnostics`.

## Ephemeral worker data

For payload that must leave the execution worker after processing and result
transfer, opt in per job or adapter entry:

```yaml
name: private-transform
project: example
run: {command: [sh, -c, 'cat input.txt > output.txt']}
files:
  input.txt: {sha256: INPUT_SHA256, size_bytes: 12}
outputs: {output.txt: results}
needs: {platform: cpu, cpu_cores: 1}
advanced: {worker_data: ephemeral}
```

Upload inputs normally; their authoritative queue copies keep their existing
retention. `files`, source archives and declared `shared_inputs` (stored or NAS,
including extracted trees) stage privately, bypassing both cold and warm
persistent payload caches. No cache entries or pins belonging to ordinary jobs
are removed. Processing uses the staged local paths. `TMPDIR`, scratch, private
`HOME` and non-resumable local checkpoints belong to the same attempt. No
short-path temp alias is created. Declared outputs return through the queue file
store even when checkpoint storage uses a NAS.

Supply payload through declared managed files. Command text, authored settings,
logical filenames, hashes and numeric/control evidence are retained metadata;
do not embed file payload in command text or environment values.

After successful processing, all successful result bytes stay local through
transfer outages and lost replies. The worker verifies exact attempt/manifest
acceptance in durable queue state, records it locally, then synchronously
unlinks the entire private data tree and the delivered stdout/stderr log file,
syncs their parent directories, and verifies the paths are absent before it
reports release. This includes input/extraction staging leftovers, outputs and
any spool below the attempt root. `worker_data_cleanup` in job status and
`attempt_history` separates `pending`, `transfer_pending`, `cleanup_pending`,
`failed` and `complete`. A receipt identifies the attempt, relative worker-state
roots, logical file bytes removed, verification time and per-root errors. Bytes
are not a free-space measurement. Result success with cleanup still pending is
not a satisfied ephemeral policy; inspect the receipt and `release_confirmed`.
`wait` still observes the compute outcome; its reason exposes cleanup state,
and its terminal job includes the full receipt. A terminal result alone is
not proof of removal, including when an older attempt still awaits cleanup.
If the queue has already abandoned the exact attempt through a terminal or
requeue decision, that durable disposition permits discarding its outputs;
it does not certify those outputs as accepted results.
A failed unlink, inventory or directory sync cannot produce `complete` or
release the attempt. Cleanup is retried idempotently, without a retention timer.

Failure, cancellation and requeue discard private payload after physical
process/writer cleanup; logs remain until delivered, including late logs for the
exact unreleased copy. Every retry starts with fresh private inputs. After a
worker crash, startup first kills/reaps its recorded process custody, then
cleans failed/interrupted attempt data and reconciles pending successful result
transfer before removing its bytes. An offline worker remains cleanup-unknown
or pending until it returns; queue timeout or a fresh session does not certify
removal. Failed ephemeral output copies are not retained for manual recovery.

Initial scope: offline non-resumable plain commands, installed tools, managed
`{work}` and declared outputs. The policy automatically selects `ci` isolation.
Submission refuses environments/builders, extensions/program jobs, model
servers/snapshots, sidecars/member limits, custom workdirs, resumability,
fan-out/dependencies, diagnostics/`keep_on_failure`, alternate output delivery,
and fleets with no supporting worker, with `job_worker_data_unsupported`.
Execution requires `worker_data_ephemeral_v1`: Linux PID/mount namespaces and
a readable file core route, or the qualified Mycelium core router, and an exec
helper that passed a core-limit probe.
macOS, unsupported workers and the Linux fallback boundary cannot silently run
these jobs. The worker rechecks core routing at launch and sets both soft and
hard `RLIMIT_CORE` to zero in the payload helper, without changing the worker
or host limits. **Linux piped core handlers ignore RLIMIT_CORE**. Mycelium's
qualified router instead matches the dumping PID's cgroup to exact attempt
custody and discards ephemeral cores unread, retaining only signal/time/attempt
metadata in the journal. Other piped handlers (including apport and
systemd-coredump) and older Mycelium routers remain refused. See the
[operator switch and rollback](operations.md#linux-core-routing-for-ephemeral-attempts).
Do not downgrade queue or workers while ephemeral attempts retain custody.

Removal means unlink, **not secure erasure on SSDs**. Full-disk encryption is the
real at-rest control. This guarantee covers managed worker-local filesystem
copies under the supported confinement boundary, not RAM/VRAM, swap, snapshots,
backups, copies retained by earlier ordinary jobs, or privileged/operator
processes. Queue- and NAS-side retention is unchanged and out of scope. The
three-day default still applies there; ephemeral local copies are an explicit
exception to the ordinary worker retention policy.

## Transfer behavior

### Project input attribution and capacity reservations (#464)

Before uploading, current profile, files-from and Go `Run` clients obtain a
queue-issued input claim bound to project, class, SHA-256 and size. Python
`run` does the same; direct `upload_file(path, project="demo", class_="experiment")`
and `myce file put --project demo FILE` provide explicit attribution. Go callers
wrap upload context with `WithFileStoreSubmission(ctx, spec)`. Claims survive
queue restart. Upload creation, resume and deduplicated HEAD reuse verify the
claim; changing a project header cannot transfer its quota charge.

API clients call `POST /api/files/input-claims` with `{project, class, file}`,
then send its `id` as `X-Myce-Input-Claim` on upload POST/HEAD/PATCH and input
blob HEAD. A claim commits the file's full declared length against the project's
quota before any bytes arrive. Concurrent claims are checked in one SQLite
transaction. Each digest counts once per project, including unfinished and
abandoned inputs, and once in global physical usage. Submission transfers the
claim into ordinary job attribution; a settled validation receipt remains valid
for the six-hour publication grace so concurrent deduplicated transfers finish.
The receipt adds no quota charge or retention; owner release works as before.
Unused, unallocated claims expire after the existing six-hour publication grace;
allocated inputs retain attribution until submission or ordinary file collection.
Claims without bytes are reconciled after that grace. No claim deletes bytes.

Local project policy may set `max_retained_file_bytes`,
`max_added_file_bytes_24h`, and `reserved_file_bytes` (all default zero/off).
The reservation protects that project's unused entitlement from other projects'
new input allocations and declared async output commitments, in addition to the
existing production reserve. Retained shared digests count toward each holder's
entitlement. Preflight is advisory; serialized upload creation checks again.
Status `file_store.projects` exposes attribution, limits, reservation and refusal
reasons. Quota refusal is `project_file_store_quota`; a protected pool reports
`file_store_capacity` naming `reserved_file_bytes`.

When any such local policy is enabled, unidentified input uploads and resumes
return `file_input_claim_required`; upgrade the client or obtain a claim. Exact
worker attempts and legacy declared async output identities keep their existing
output route. Failed-attempt diagnostics carry the exact execution identity.
Go/CLI and Python result recovery upload with the captured failed-execution fence
(`WithFileStoreRecovery` / `upload_file(..., recovery=plan)`); a changed attempt
is refused. Production is exempt from project quotas and project reservations;
completed output transfers receive no new refusal. Physical capacity still
applies. Existing retained bytes, registry assets and checkpoints are preserved;
this does not activate the parked storage backend or shorten retention.

The fleet authenticates one operator credential. A claim is server-verifiable
attribution to an operator-selected project, not a separately authenticated
project account. It cannot prevent that operator from choosing another project
or presenting an active worker attempt. Keep claims and operator authority out
of payloads.

`get` refuses an existing destination and retains a `.myce-partial` download
beside it after interruption. Rerunning resumes it. A bad range response, byte
count or hash is refused; a server ignoring Range causes a safe complete restart.
Upload creation/HEAD/PATCH follow tus 1.0. If an upload fails on an older
client, rerun `myce file put` on the unchanged file, or call Python `upload_file`
again. The digest identifies the same upload; the client queries tus HEAD and
seeks to the server's accepted offset. Profile/adapter retries retain their same
submission key and receipt. There is no CLI stall-timeout override.

From the release containing #1100, Go/CLI and Python reconcile an interrupted
PATCH automatically, including a lost final acknowledgement. They first check
for a published blob, then query the accepted offset before sending more bytes;
local socket writes never count as acknowledgement. Typed integrity, authority
and storage-policy refusals still return immediately. Retry pacing follows the
current server `Retry-After` or measured service cost with positive jitter.

From the release containing #1170, every `/api/files/uploads/` error uses the
standard JSON `{"code":"…","error":"…"}` envelope, including errors from the
resumable-upload handler. HEAD errors expose the code in `X-Myce-Error-Code`
because HTTP HEAD has no response body. Successful tus responses are unchanged.
A concurrent duplicate final PATCH or late HEAD can return 404
`file_upload_not_found` after publication has removed the upload session.
Go/CLI and Python check the content-addressed blob, whose HEAD verifies stored
bytes against SHA-256; clients also require the expected ETag and size. A
verified blob makes the upload successful. If the session and blob are both
absent, clients retry with existing pacing within the same Go context deadline
or Python `transfer_timeout`, recreating/resuming the same digest as needed.
Missing upload metadata alone never proves either completion or lost content.
Integrity and typed authority/storage refusals still stop recovery immediately.
Completion also requires the source identity to match: clients hash each
attempt's resumed prefix and the bytes read for PATCH, then verify the remaining
tail and total size. A changed source fails even when another uploader published
the original blob, including when the source is restored before its 404 arrives.
Keep input files unchanged until the upload finishes.

File transfers have no default whole-request deadline. The per-socket progress
window learns byte service time and gaps. Upload PATCH windows additionally use
the upload's observed request round-trip/service time (four service periods of
jitter allowance) and server `Retry-After` signals, with the one-second TCP
initial/minimum retransmission timer basis from RFC 6298 sections 2.1/2.4. This
prevents fast local copies from declaring a dead peer before retransmission;
slower service and pressure raise the window. Evidence is scoped to one upload.
A socket with no progress still times out; recovery reconciles rather than
blindly replaying. Before non-PATCH response headers, only the owner bounds queue
verification. Go callers use a context deadline; Python `transfer_timeout`
bounds the whole upload, including retries. API `timeout` is separate and does
not set this no-progress threshold. There is no default retry-count limit.
Default and installed pinned TLS clients use this progress policy. An injected
Go `DialTLSContext`, `DialTLS`, or custom `http.RoundTripper` retains its authority
and protocol handling and owns its I/O deadlines; custom dial setup is bounded.
A stalled result reports `result_download_failed`; Python raises
`TransferStalledError`. Size/SHA-256 verification and atomic publication still
apply.

From the worker release containing #1045, ordinary queue-stored job inputs
recover transient reads within the same preparation attempt. An idle expiry or
interrupted body retains the partial blob and resumes with Range, ETag and final
size/SHA-256 verification. Recovery uses measured, jittered pacing and honours
`Retry-After`, cancellation and the preparation owner's deadline. Worker status
reports `input transfer recovering; same preparation attempt retained`; this is
pending work, not a staging refusal or an infrastructure retry. A completed
failed dial returns `storage_unreachable` rather than waiting forever. Permanent
authentication, typed storage and integrity refusals still end preparation with
their retained cause. Payload execution remains fenced until staging succeeds.

For partial cache eviction, the worker reads the full blob size from
`GET /api/files/blobs/{hash}/metadata`, then performs a verified HEAD using that
size's budget. Metadata alone never authorizes eviction; an unavailable size
leaves the partial in place and reports the lookup error.

Final size/hash verification is mandatory. Workers retain local results until
publication is acknowledged. The queue accepts output references only from the
current successful attempt, after every referenced file is durable. A missing
required output fails visibly. Late attempts cannot replace accepted output.

## Recover outputs from a failed terminal attempt

`myce result recover` attaches retained bytes as a **separate recovered result**,
without rerunning the payload. The failed status, native outcome, cleanup and
execution causes, release evidence and ordinary `output_refs` stay unchanged.
Recovery verifies bytes **now**; it is not evidence of acceptance at execution
completion or of application quality. Recovered results never satisfy
dependencies or make `wait` succeed.

Run from a machine with the retained regular files, including a mounted NAS.
This uses installed queue authority; keep credentials out of argv. The directory
contains declared output names, including relative subfolders:

```sh
myce result recover JOB_ID --files-from ./retained-outputs \
  --key cleanup-recovery --by project-owner \
  --source nas:project/results/attempt \
  --reason 'Payload exited zero; cleanup inspection prevented output publication'
```

For different filenames or a subset of outputs, use repeated
`--file 'answer.json=/mounted/source/answer.json'` instead of `--files-from`.
Sources are read and uploaded, never changed. Symlinks and special files are
refused. Automatic selection stays beneath `--files-from`, including parent
directory resolution, and uploads retain the opened source file even if its
pathname changes. `--source upload:LABEL` is the default source kind; `nas:RELATIVE_PATH`
records an operator-asserted NAS origin. Source labels must be relative and omit
private host paths and credentials. The queue does not open caller-selected NAS
paths. Requester labels and reasons must be nonempty, without control characters.

The CLI captures the exact job/attempt/machine/session before uploading; optional
`--attempt N` checks an additional caller expectation. The queue checks that this
is still the current failed terminal attempt and every name is a declared output,
independently verifies SHA-256 and size in its store, then rechecks identity in
the SQLite transaction. Hashing and filesystem sync happen outside the queue
mutex. Ordinary upload capacity admission applies. Uploads abandoned before
attachment use normal job-input orphan retention.

The current API authenticates one fleet **operator** credential, without separate
owner/project credentials. Recovery requires that authority, like existing queue
mutations. `recovered_by: operator` is server assigned; `requested_by` (`--by`)
identifies the requester asserted by that operator, not a separately authenticated
account. Naming a project or requester grants no authority.

The command prints immutable JSON with `kind: recovered`, `result_id`, exact
execution identity, requester/source/reason, `recovered_at`, `expires_at`, file
identities and `manifest_sha256` (SHA-256 of compact Go JSON for `files`, with
sorted keys). Status and job GET expose `recovered_results` separately from
ordinary accepted outputs. Repeat the same key and identical evidence to recover
the same record, including after a lost response or byte expiry. Different
evidence under the same key returns `409 recovery_conflict`. A new key appends
evidence; it cannot replace or delete an earlier record.

Download by explicitly selecting its ID through the usual verified transport:

```sh
myce result JOB_ID --recovered RESULT_ID --out ./recovered
```

This labels the download as recovered. Unqualified `myce result JOB_ID` continues
to refuse the failed job. Bytes have a fresh `--file-retention` window (the queue's file retention, `72h` by default; protected until release when it is `0`) beginning at recovery, even if original job files expired. Each
`expires_at` is fixed at acceptance; changing retention does not revive expired
bytes. Expired downloads return `410 result_expired`. Metadata and original job
provenance remain indefinitely, exempt from history pruning. Keep unique source
bytes separately: the queue store is one copy, not a backup.

API:

- `GET /api/jobs/JOB_ID/recovery-plan` returns exact failed execution identity and
  declared output names, without worker paths or commands.
- Upload bytes with the existing file API, then
  `POST /api/jobs/JOB_ID/recovered-results` with `id`, `attempt`, `machine`,
  `session`, `key`, `requested_by`, `source: {kind, location}`, `reason`, and
  `files: {name: {sha256, size_bytes}}`. Caller hashes are expectations: the queue
  hashes every referenced blob before acceptance.
- `GET /api/jobs/JOB_ID/recovered-results/RESULT_ID` returns immutable metadata.
  `?require_files=true` additionally enforces the recovery retention deadline.
  Metadata lookup still works after expiry.

Go provides `Client.RecoverResult`, `GetRecoveredResult`, and
`DownloadRecoveredResult`. Python provides `Client.recover_result(job_id,
files={name: local_path}, key=..., requested_by=..., reason=...,
source={"kind": "nas", "location": "project/results/attempt"})`; download with
`download_result(job_id, name, destination, recovered_result_id=result_id)`.
Both helpers fence identity before uploads and allow size-based hashing time.

## Checkpoints and interrupted connections

Jobs still use `MYCE_CHECKPOINT_DIR` and the existing atomic generation contract
in [resumable jobs](files.md). In no-NAS mode that folder is local
to the attempt. Mycelium captures a consistent save, uploads its immutable files,
verifies SHA-256 and size at the store, syncs files and directories, then records
the generation in SQLite. It never publishes a manifest before its payloads.
The newest published generation is sent to the next hypha and fully staged
before the command starts. Generations, not timestamps, determine progress.

Status `job.progress` counts **published** progress. `file_checkpoints` holds the
last published manifest per attempt. `file_transfer_error` explains pending
transfers. A newer local save is not available elsewhere yet. During a connection
loss, commands keep running and saving locally; transfers and result delivery
retry after reconnect. A sudden loss can resume only a complete published save.
After the process group exits, physical release is acknowledged separately from
CPU file publication. Stop grace does not promise that any checkpoint size can
cross the network within ten seconds. Save often and size payloads accordingly.

Use immutable payload files plus one atomic JSON manifest, or a self-contained
atomic JSON save. Files/directories ending in `.tmp` are excluded. Symlinks,
unsafe paths, overlapping/case-colliding names and changed snapshots are refused.
The initial manifest limit is 1024 files; JSON checkpoint metadata is at most
1 MiB. Larger binary payloads can be referenced by that metadata.

## Local-first checkpoints (#1600)

Opt in explicitly on an individual resumable plain command:

```yaml
advanced:
  resumable: true
  checkpoint: local_first
```

This leaves default NAS and existing no-NAS file-store behavior unchanged.
`MYCE_CHECKPOINT_DIR` is managed local storage on a worker advertising
`checkpoint-local-first-v1`; `MYCE_CHECKPOINT_PROTOCOL` identifies that contract.
Use the updated Python `mycelium.Checkpoint` with the default `save(value)` name.
V1 accepts **one self-contained JSON payload, at most 1 MiB**, per generation.
Named/multi-file/binary saves refuse with `checkpoint_protocol_unsupported`;
non-resumable, extension, fan-out, dependency and ephemeral-policy combinations
refuse with `checkpoint_policy_unsupported`. A worker-state filesystem that cannot be verified local refuses launch with
`checkpoint_storage_unsupported`. No directory-snapshot fallback is used. A custom writer must implement the entire following cooperative protocol.

1. Write `generation-N.json` to a temporary file, flush/fsync/close, rename and
   sync its local directory. Its JSON contains
   `{"myce_checkpoint":{"generation":N},"value":...}`. N is a positive int64,
   strictly increases from the resumed generation, and published payloads stay
   immutable until acceptance. Only one writer may publish generations; the Python helper holds an exclusive local writer lock and refuses concurrent writers with `checkpoint_protocol_unsupported`.
2. Write/fsync/rename/directory-sync `generation.json` **last**. Its immutable
   manifest is `{"protocol":"checkpoint-local-first-v1","myce_checkpoint":
   {"generation":N,"files":{"generation-N.json":{"sha256":"...","size_bytes":123}}}}`.
   A temp, missing, torn, wrong-hash or conflicting generation cannot be committed.
   Unrelated working files never enter the captured set.
3. Keep the manifest and named payload unchanged until the worker atomically
   writes `.myce-accepted.json`. It contains the protocol, generation, and
   `marker` SHA-256/size identity of these exact `generation.json` bytes.
   Only that acknowledgement permits a save to return, an older acknowledged
   local payload to be removed, or the next generation to be published.
   The file is a local acknowledgement, not an author-supplied queue receipt.

The worker uploads through the existing resumable file transport, verifies size,
checksum and sync barriers, then commits the manifest under exact job, current
attempt, machine and session fences. Lost acknowledgements replay idempotently;
a restarted worker can replay the unchanged published source after an interrupted
upload. Stale attempts cannot advance portable progress. Another compatible
worker stages the greatest **accepted** complete generation before launch.
Existing `resume_compatibility` target restrictions still apply, including when
moving from macOS to Linux. A lost local disk loses unpublished work.

There are three distinct states: `local_checkpoint_generation` for published local progress; `file_checkpoints`
for queue-portable progress (and `job.progress`); and `checkpoint_mirrors` with
per-generation NAS delivery state/receipt identity. Queue acceptance atomically
creates mirror custody when a NAS checkpoint root is configured. The queue's
existing single publisher writes under that job's checkpoint folder, using
immutable generation/attempt paths and verified receipts. Payloads perform no
NAS checkpoint writes. A blocked syscall retains ownership; restart/lost-ack
replay verifies receipt and content. No NAS root means queue-portable storage
alone; a queue file store is not a backup.

`save()` waits cooperatively when queue commits lag. Its measured free-space
check charges payload plus transfer copy and metadata above the existing
volume-derived reserve; refusal is `CheckpointError.code ==
"checkpoint_local_capacity"`. Failed local sync/write raises the real I/O error.
Queue upload admission accounts for reserved/in-flight and pinned generations;
capacity refusal retains published local bytes and exposes pending transfer
status. Only one unacknowledged save is allowed; this bounds cooperative local
growth. Heartbeats, cancellation and independent work continue. Arbitrary writes
outside this protocol have **no enforced disk bound**. Pending/failed NAS mirrors
retain queue custody beyond ordinary expiry; they can eventually fill storage
and backpressure saves. Neither retained history nor in-flight bytes are silently
pruned. Settled, physically released checkpoints retain the existing 72-hour
checkpoint grace (and any longer configured queue file retention); early explicit
file release reports `checkpoint_retention`. They then use the
existing collection/expiry paths. Never downgrade with unresolved local-first
attempts or mirror custody.

Choose the portable recovery budget from acceptable lost work: work-unit time,
save interval, and measured local capture plus queue upload/acceptance tail.
Only successful `save()` return promises portability; a planned stop's grace
cannot promise an arbitrary upload will finish. NAS mirror lag is unbounded
through outage. Consumers requiring NAS durability must explicitly wait for
verified mirror state and stop further work cooperatively. Laptop fault tests
are not native SMB durability or sustained write/flush/close qualification.

## Diagnosing input staging

Before the command starts, staging failures appear on the exact attempt in
`myce status JOB_ID`, job GET `execution_cause.staging`, and `myce logs JOB_ID`
(unless `logs: none`). The detail names the declared input and SHA-256 blob,
operation (for example `cache_verify`, `cache_download`, `source_extract`,
`copy`, or `publish`), OS/storage error, syscall when available, and HTTP status
when applicable. Expanded worker paths and raw transport text stay in the local
worker journal. Attempt history retains this cause when the job retries.

Local I/O, permissions, capacity and transient storage/transport failures before
launch use the existing infrastructure retry policy, including its production
retry bound. `input_unavailable` with `blob_missing` or `blob_expired`, and
`invalid_input`, require consumer action rather than infrastructure retries.
Missing authored NAS inputs also require corrected inputs rather than retries.

From s072, resolve and submit refuse impossible shared-input jobs with HTTP
409 `shared_input_cache_capacity`: the reason gives required bytes and the
largest enabled matching worker cache capacity. Refusal requires known capacity
from every enabled worker under consideration and no fitting matching target.
During startup with no matching worker, submit accepts the job with the typed
waiting reason `shared_input_cache_unreported`. A worker without the optional
capacity report retains staging-time admission, so a queue upgrade does not
strand work on older workers. Its local cache still enforces its actual capacity
and reports required/available/limit bytes if staging cannot fit. Workers that
report capacity retain queue-side checks. Declaration sizes are a hard requirement: file bytes, manifest
file totals, or `unpacked_bytes` for a tree/archive, plus compressed archive
bytes for cold staging. Every distinct cache identity needed simultaneously must fit together; aliases
with identical verified content share one entry.
Mycelium chooses a fitting worker; do not pin a host to work around data size.
If a fitting worker exists but its space is occupied, the job waits with
`diagnostic_code: shared_input_cache` and required/free/evictable/capacity bytes.
Inactive reproducible entries are evictable; active/custody-pinned entries stay
protected. Queue claims also cover assigned inputs before the next check-in.

Worker-side admission remains authoritative against disk changes between samples.
A staging failure reports `resource_limit`, operation `shared_input_admission`,
error `cache_space`, and `capacity` (`shared_input_cache`, `file_cache`, or `disk_free_floor`),
with required/available/limit bytes. Inputs stage before source/plan blobs.
Omitting `--file-cache-bytes` now sizes both separate cache ceilings automatically
from disk headroom; see [cache policy](reference/worker-caches.md#file-and-shared-input-cache-capacity).
An explicit saved positive limit still wins. Split work whose simultaneous inputs
exceed all eligible cache capacities; re-uploading identical bytes cannot help.

Receipts identify bytes; they do not reserve those blobs forever. A new
submission verifies every referenced input outside the queue lock, while the
existing lifecycle fence protects verification through commit. A live accepted
job then protects those bytes under ordinary retention, even when an older job
referencing them expires. Missing bytes are refused with `input_blob_missing`,
corrupt bytes with `input_blob_corrupt` (HTTP 409), and storage read/durability
failures with `input_blob_unavailable` (HTTP 503). The refusal names the input,
blob and failed verification. Restore storage for a 503; upload the original
bytes again for missing/corrupt inputs, then retry the same unaccepted key.
Prefer normal profile/adapter upload over copying references from old receipts.

## Model files and caches

Import an approved model once with `myce file put`. In the existing model catalog,
set `file_store: true`, `size_bytes`, and `sha256`; `path` is unnecessary. For a
bundle retain the existing `files` manifest and its combined size/hash identity,
and import every listed file. Configuration and tokenizer files belong in that
manifest too. Engine loading never downloads from the internet. Existing NAS
catalog entries, `direct_nas`, pins, pre-copy, protected loaded models and model
cache limits retain their behavior. Extensions can receive models as job inputs.

`--file-cache` and `--file-cache-bytes` configure each hypha's reproducible cache
(automatic from disk headroom by default, under its state folder). It counts complete and partial bytes,
evicts least-recently-used entries, and refuses a file larger than its allowance.
A private materialization keeps active jobs independent of eviction. Model
loading/pre-copy uses the existing `--model-cache-bytes` admission and pin/loaded
protection. Stored model eviction verifies that authoritative files remain before
removing disposable copies. The original NAS cache Trash behavior is unchanged.

From the release containing #1365, omitted `--file-retention` is `72h`: finished
jobs' files are kept for 3 days, then reclaimed (MY-47). An explicit zero retains
unique bytes until explicit owner release or a verified authored delivery policy.
Saved operator durations, including a saved zero, still bind. An expired
result still returns `result_expired`; increasing a window cannot restore already
removed bytes. Logs follow the same omitted/explicit distinction. Neither policy
deletes intent, custody ledgers, wait/error facts or keyed replay answers.

Unsettled input claims protect exact bytes across upload/HEAD/submission requests.
Lifecycle locks and dependency revisions fence collection; measured claim-to-submit
delays supply publication protection. There is no six-hour or one-day default.
Only a claim's first settlement is measured; a reused claim's age is not a
publication delay. For an expired job's files that protection never outlasts the
file retention, so a recent HEAD cannot keep them past 3 days (#1375).
Open work, checkpoints, catalogs, recovered results without a deadline and retained
replay bundles keep their references.

Automatic mode does not sweep orphan/interrupted uploads, unknown imports or
unclaimed attempt directories. Positive authored retention applies to positively
identified disposable uploads/bundles after active claims/references are gone.
Collection releases control between objects and yields under measured contention.
Pressure never authorizes deleting unique data. See [adaptive retention](retention.md)
for every replacement and [owner release](reference/file-store.md#project-owner-early-file-release-465).

If assignment recipe or bundle bytes become unreadable after a check-in commits,
the queue sends an exact stop and retains attempt custody until release is
acknowledged. Other controls still arrive. Undelivered, unstarted environment
preparations are withdrawn and can be offered again after storage recovers;
unreadable model preparation bytes defer loading without dropping unload controls.

`POST /api/files/preflight` accepts a file manifest and checks the additional bytes
before upload; `Client.PreflightFileStore` exposes it in Go. Profiles use this for
the complete input set, counting shared hashes once and reusing existing blobs and
tus lengths. Upload creation repeats the check. This is advisory, with no reservation.
`myce status` and `/api/status.file_store` expose payload used bytes, effective free
bytes and reclaimable bytes. Effective free space respects `--file-store-bytes`,
existing tus lengths, and #158's physical disk floor (1%, minimum 256 MiB).

Capacity exhaustion returns HTTP 507 `file_store_capacity`, naming
`queue-file-store`, required/free/shortfall bytes and the recovery action. Profiles
retain that refusal in their receipt and exit 75. An unavailable reading returns
503 `file_store_unavailable`. Retention never evicts active data, checkpoints or
catalog files to admit work. Keep local sources until acknowledgement and retrieve
results before expiry.
One store is not a backup. Attempt folders and local spools remain for inspection,
separate from bounded reusable caches.

The implementation uses Go HTTP range serving, SHA-256 and embedded tusd file
storage/locking. Local failure tests cover interrupted transfers, offsets, corrupt
bytes, cache eviction, filesystem sync failure, stale attempts, restart and
resume on another hypha. Filesystem sync ordering is tested; storage hardware's
power-loss behavior needs its own qualification.

## Read-only inputs staged locally

The default for **every declared NAS input**, including small files, is local
staging before processing (MY-36). Read the local destination or
`MYCE_INPUT_NAME`, never the NAS original during processing. Legacy `inputs`
path mappings already copy to the local attempt folder; no command change is
needed when it reads the declared destination. Repeated/large immutable inputs
should use the shared cache below. Raw mounted paths hidden in shell commands,
programs, configuration or external workdirs cannot be safely inferred or
rewritten: declare the input and use its local path. Mutable checkpoint state
and output publication retain their independent synchronization contracts.

From #1061, NAS copies make one sequential read, resume completed transient
interruptions from the local prefix within the preparation attempt, and verify
the local checksum before execution. Declared shared-input prefixes also survive
owner cancellation/worker restart and remain evictable when inactive. Authored
hash/size provide immutable identity. Legacy path inputs instead freeze source
metadata for the transfer, hash copied bytes and verify the local digest; an
observed source change refuses preparation. This cannot establish an authored
immutable snapshot of an otherwise mutable path.

Deploy the queue before workers. Shared-input jobs require the advertised
`shared_inputs_v1` capability; older workers cannot receive or start them.
Extension jobs also require `extension_shared_inputs_v1`, so older workers
cannot acknowledge success under a different output publication contract.

Profiles, native extension jobs, and plain native jobs can use `shared_inputs` beside stored source
`files`. Names use letters, digits and underscores, unique ignoring case. Read
`MYCE_INPUT_<UPPERCASE_NAME>` in the command; it is the absolute path of a shared
read-only file or directory, with no attempt copy or unpack.

```yaml
shared_inputs:
  runtime:
    nas: runtimes/python.tar.gz
    sha256: REPLACE_WITH_ARCHIVE_SHA256
    size_bytes: 98000000
    archive: tar.gz
    unpacked_bytes: 350000000
  original:
    nas: media/original.mov
    sha256: REPLACE_WITH_ORIGINAL_SHA256
    size_bytes: 2000000000
```

NAS names stay relative to each worker's `--checkpoint-root`; the queue and
submitter need no NAS access. No NAS bytes pass through the queue store. Omit
`nas` for a blob already imported with `myce file put`. A profile can instead
set `local: path/to/archive` to import it, deriving SHA-256 and size; subsequent
profiles should reference that hash or its NAS location. Existing `inputs`
retain their private-copy behavior. Commands must explicitly use the environment
paths, for example `python "$MYCE_INPUT_RUNTIME/run.py" "$MYCE_INPUT_ORIGINAL"`.
Extension commands receive the same environment variables; their NAS output and
checkpoint contracts are unchanged. Extension execution must be native.

`spec check --resolve` evaluates target eligibility and labels worker NAS reads
as unverified. From the next release, optional `--inspect-nas` requests cached,
authenticated worker existence evidence; repeat after worker heartbeats.
`pending` or `unverified` remains unknown. `present` does not verify bytes,
and `missing` flags a wrong checkpoint-root-relative path before submission.
From the next release, samples expire with their worker session freshness fence
([request budgets](request-budgets.md)); upgraded workers are required. A path relative to the NAS mount can therefore
resolve successfully and still be missing beneath `--checkpoint-root`. Import
such a file with profile `local:` or `myce file put`, then omit `nas` and use its
verified hash and size. This needs no worker root change or cache clearing.

A missing NAS source still fails before launch with `input_staging: not_found`;
`execution_cause.path_diagnostic` identifies the shared-input field (for example
`shared_inputs.movie.nas`), the worker side, and logical base
`worker-checkpoint-root`. The expanded mount path and underlying syscall text
stay private. Present file-store inputs remain readable immediately after a
queue restart; a deferred retention pass does not hide them.

Workers report verified cache identities for placement. Their shared input
evidence counts successful cache hits and cold stages, plus the declared NAS
source sizes for completed cold stages; it is not a network traffic measurement.
These counters reset when the worker restarts. Assignment, staged and started
timestamps show the input phase, while declared input sizes remain in the job.
After restart an entry is reverified locally before it is advertised as ready.

For a large manifest already on the NAS, use `nas: runtimes/python` with
`manifest_file: manifests/python.json`, `sha256` and `size_bytes` identifying
that JSON file, and `unpacked_bytes` bounding the tree. The JSON is a file
manifest mapping relative names to SHA-256 and byte counts; it stays on the NAS
so a 12,000-file tree does not exceed the API/profile request limit.

For a small already unpacked NAS directory, declare `nas: datasets/frozen` with
`manifest: {relative/file: {sha256: HASH, size_bytes: N}, ...}`. Only listed
regular files are copied, and executable bits are preserved. Archives support
`tar` and `tar.gz`, regular files and directories only; links and special files
are refused. `unpacked_bytes` bounds expanded file bytes to stop an archive from
exhausting disk. Use a fresh hash/manifest when changing content.

Workers keep entries under `file-cache/shared-inputs`, separately bounded by
`--file-cache-bytes` (automatic by default from s072), in addition to the private-file cache's
allowance. From #1061, automatic allowances refresh during admission and the
existing worker retention sweep from current filesystem headroom plus occupied
cache blocks, leaving the existing disk control floor (1% of the volume, at
least 256 MiB) for metadata, logs and control state. There is no input-size cap
or startup quarter-free ceiling. Other caches/writers already reduce measured
free space; outstanding staging reserves only unwritten bytes. Explicit operator
limits remain authoritative. Physical disk headroom applies across both. LRU eviction removes
reproducible inactive copies; active attempts hold use locks through process
cleanup. Mode bits provide cooperative read-only access, not a sandbox against
the owning account changing permissions. First use after restart hashes every
file; subsequent uses check inode, change time, size and mode. Metadata changes
trigger full re-verification, allowing another cache instance's valid rebuild.
A damaged entry fails preparation visibly; stop its readers and remove the reproducible
entry before retrying. Stored shared inputs use the ordinary queue retention
window; NAS originals remain owned by their project.

Input downloads renew their socket deadlines on progress instead of timing out
a whole file after 30 seconds. From #810, each direction learns its no-progress window from observed
throughput and gaps. Queue verification before headers cannot be estimated
from network throughput and is bounded by the owner; there is no fixed
whole-transfer or default API deadline.

## Declared model snapshots (#553)

A plain native batch can declare an exact Hugging Face model
snapshot instead of asking for a host bind mount:

```yaml
model_snapshots:
  base:
    repo: OWNER/NAME
    revision: 0123456789abcdef0123456789abcdef01234567  # full commit, never a branch or tag
    files: [config.json, model.safetensors]           # optional allow-list
```

Read `MYCE_MODEL_<UPPERCASE_NAME>`: a read-only directory holding the declared
files at their snapshot-relative paths: the worker's verified local copy.

Declare the snapshot and submit; never pre-download it or aim a download job at
a host. Wherever Mycelium places the job, the worker makes the snapshot present
in its own model snapshot cache during preparation, before the command starts,
in this order:

1. **Held here:** an entry this host already holds for the same declaration is
   reused. Nothing is downloaded and the hub is not contacted.
2. **Local hub cache:** the standard hub-cache layout under the worker's
   optional `--model-root` (`hub/models--OWNER--NAME/snapshots/REVISION`).
3. **Model hub:** otherwise the worker fetches the exact revision from the
   Hugging Face hub (`HF_ENDPOINT` on the worker, else `https://huggingface.co`)
   straight into its cache. Large (LFS) files are verified against their
   SHA-256 while populating, and small files against their Git blob SHA-1. One
   lock per declaration lets concurrent jobs on a host share one download, and an
   interrupted file resumes. The hub endpoint must be HTTPS, except on loopback.

For gated or private repositories, provision the hub token on each worker as the
local secret `hf_token` in its `--secret-root` (mode-0600 file in a mode-0700
directory, see [local secrets](services.md#environment-and-local-named-secrets)).
Only the worker reads it, for hub requests during preparation; it is never put in
a job, a queue message or a log, and the job never sees it.

Placement accounts for the download. Workers report each snapshot they hold
(`machines[].model_snapshots`, by declaration identity) and each verified size
they have learned (`machines[].model_snapshot_bytes`). A host that does not hold
a declared snapshot has its `file-cache` volume charged with that size, so a host
without room is not chosen and the job waits with a `disk_headroom` reason
naming the GiB needed. A host that holds it is not charged and is preferred as a
tie-break. Before any host has learned a snapshot's size, the first worker's own
admission refuses a cache without room; that attempt requeues and the queue,
which now knows the size, places it on a host with room.

The local hub cache is read, never an authored path. Each snapshot file must be a link into that repository's `blobs/` folder
or a regular file. On SMB/CIFS volumes without native symlinks, the worker
recognizes fixed `XSym` (mfsymlink) files, checks their format checksum and
resolves their targets within the same repository's content-addressed blobs.
It stages the resolved blob bytes. Malformed or unsupported link encodings,
missing targets and changed blobs receive a typed refusal; the source hub is
never rewritten. Blobs named by SHA-256 (large files) are verified while
copying; blobs named by Git SHA-1 (small files) are hashed and checked against
their name. The verified closure is copied into the worker's model snapshot
cache (`model-snapshots` under the file cache folder), reused by identical
snapshots, protected by use locks and durable attempt pins until exact process
or container cleanup, and evicted only when
inactive. That cache has its own budget, `--model-cache-bytes` (default
100 GiB), plus the usual free-space floor; `--file-cache-bytes` does not apply.
When the cache folder and the hub share a copy-on-write volume (APFS, or Linux
Btrfs/XFS with reflinks), each file is cloned instead of copied, so it shares
blocks with the hub until either side writes. A later write to a hub blob cannot
change the clone. Hard links are never used, because a link is the same file:
writing or changing permissions on the hub blob would change the "verified"
copy. Other volumes, including a NAS, get a full copy. After verification the
worker records each file's size, mode, modification time, inode and change time
beside the entry; a restarted worker re-hashes the entry only when that record
no longer matches and no attempt still owns a durable pin. Pins survive worker
death independently of process-held locks; restart cleanup reads the saved
attempt receipt before releasing them. A changed pinned entry is refused without
rehashing or replacing the active snapshot. Copies are mode read-only, which is cooperative for
unsandboxed jobs, as for shared inputs.

Every worker with a snapshot cache advertises `model_snapshots_v1`; a model
root is optional. A job withheld before launch fails with one of these
path-free reasons and never starts its command:

- `model_snapshot_revision`: the hub has no such repository revision;
- `model_snapshot_access`: the hub refused access. The public hub answers this
  way for gated and private repositories without a token, and for repositories
  that do not exist;
- `model_snapshot_mismatch`: a declared file is missing, special, or its bytes do
  not match its blob identity;
- `model_snapshot_escape`: a snapshot link or hub file name points outside the
  repository;
- `model_snapshot_too_large`: the snapshot exceeds the policy's `max_bytes`.

A fault that belongs to the host, such as a disk without room above its floor,
a network failure or a hub server error, does not fail the job. The attempt is
requeued as an infrastructure interruption (`input_staging: storage_full`,
`connection_refused`, `timeout` or `io_error`). A read I/O error or no-progress
timeout does not establish a content mismatch. Before command start, these
interruptions spend neither losses nor infrastructure retries, even if storage
recovers before the result arrives. Status names the cause; placement prefers
another eligible host and storage/disk admission withholds unhealthy roots or
hosts without room. A completed read whose hash differs still fails with
`model_snapshot_mismatch` (`invalid_input`) and requires source or declaration
repair.

From the release implementing [MY-44](DECISIONS.md#my-44), every repository is
permitted by default. No `models` policy or `models.sources` entry is needed.
An owner may optionally restrict repositories with positive `sources` patterns,
or deny repositories with `!` patterns. With only deny patterns, everything else
is permitted; a matching deny always wins. An absent or empty `sources` list
permits everything. Explicit restrictions refuse a disallowed repository with
`model_snapshot_not_permitted`.

The admitting policy revision and byte limits are frozen into the job, including
when no custom policy exists. Workers enforce `max_bytes` before copying. An
absent or zero `free_floor_bytes` derives the cold-copy reserve from the worker's
actual cache volume: 1% of total capacity, at least 256 MiB, using the existing
disk headroom rule. This leaves room for filesystem metadata, control logs and
other writers without imposing one deployment's disk size on smaller systems.
A positive value requests an explicit reserve for that model copy's admission;
it does not raise the floor for unrelated staging or background collection.
The ordinary measured cache disk floor still applies. Cache capacity, verified identities and read-only staging remain
required.

```yaml
models:                                    # optional
  sources: ["OWNER/*", "!OWNER/EXCLUDED"]   # optional restriction and deny patterns
  max_bytes: 0                             # per snapshot; 0 means no extra limit
  free_floor_bytes: 0                      # omitted/0: derive from cache volume
```

Custom restrictions and limits can be set with `myce policy get --yaml`, then
`myce policy set FILE --if-revision N`. They are deployment settings stored in
the queue database. See the [execution gate audit](execution-gates.md) for the
remaining safety boundaries and consumer quality contracts.

## Complete from queue storage, deliver to NAS later (#349)

Tasks explicitly select `output_delivery: async_nas`. Other tasks retain their
existing completion and publication behavior. Configure the queue (or a laptop
`myce dev`) with `--output-delivery-root PATH`, an existing NAS destination root;
keep its SQLite database and file store on durable local disk. This setting is
independent of `--checkpoints` and does not change checkpoint policy. Laptop
fixtures can use a temporary local destination instead of a NAS.

```yaml
output_delivery: async_nas
outputs: {answer.json: results}
disk: {output_bytes: 10485760}
```

Declare the estimated total output bytes before admission. The queue durably
reserves that estimate for queued/running tasks; unrelated uploads cannot consume
it, including against extension/environment bundle writes. After process exit,
the worker copies outputs into an immutable local spool,
hashes/syncs it and publishes its manifest. It then declares the exact current-
attempt manifest through `POST /api/files/result` (the Result identity plus
`output_refs`). This exchanges the estimate for actual missing bytes. Larger
results need additional admission space; HTTP 507 retains the spool and retries.
Missing/invalid required outputs fail. The worker budgets both work files and the
spool and checks actual local free space before copying; physical exhaustion
retains unaccepted work rather than reporting success. This cannot bound disk
writes by an arbitrary command that exceeds its declared disk needs.

If an attempt is requeued, its retry must reacquire the estimated reservation
before dispatch. Until space is available, `output_admission_pending: true` and
the job's capacity waiting reason explain the hold; existing uploaded bytes still
count toward usage. Abandoned attempt uploads retain a durable cleanup owner and
become collectible after the publication grace when no job references them.
Reusing an unrelated unmarked import does not make it collectible.

Uploads use the existing resumable verified file API. One SQLite transaction
accepts the current result and pending delivery only after every blob is verified
and synced. Only that commit completes the task. The worker confirms exact queue
acceptance before ordinary worker retention reclaims its managed attempt
directory and spool. Spools live under the worker state directory at
`job-files/JOB_ID/ATTEMPT/.myce-result-spool`, independently of the command
workdir. The existing accepted-attempt record and deletion retry handle partial
cleanup without retaining the completion actor. Retrieve `output_refs` immediately with
`myce file get` or the client download helper; dependent jobs use file references.
A consumer requiring NAS paths must explicitly wait for `delivery.state` to be
`verified` on `GET /api/jobs/ID`, and match `delivery.id`/`attempt` to the accepted
execution. Job success by itself no longer promises a NAS path for these tasks.
Fan-out parent aggregation does not support this policy; submit individual native/extension tasks instead.

The queue process runs one background publisher per configured root, with the
existing two-file staging bound. It publishes complete immutable directories at
`PATH/PREFIX/JOB_ID/ATTEMPT/`, retaining declared local output names, plus
`.myce-delivery.json`. That reserved filename and `.myce-result-spool` cannot be
output names. The receipt contains execution/machine/session identity, destination
identity, policy revision `async_nas/1`, manifest digest and files with SHA-256 and
size. Each destination prefix is atomic; the queue reports verified only after
all prefixes are durable. There is no mutable latest alias or stale overwrite.

Replay after restart/lost acknowledgement verifies both the receipt and all
actual destination bytes. Conflicts refuse publication and remain failed with
source blobs pinned. Returned errors retry after one minute (limits retry load,
costing up to a minute after recovery). A pending syscall retains its slot;
after the existing storage stall budget without observed I/O progress, status explains the stall, without
launching another copy or extending job completion. Queue ticks start/observe
work and never wait for NAS I/O under the queue lock. Restoring destination
configuration resumes pending tasks; changing the configured path does not
silently redirect already accepted deliveries.

Pending/failed delivery pins outputs through ordinary file-retention expiry (`72h` by default) and
restart. Once verified, ordinary retrieval retention measured from compute
completion applies, so an already overdue result may expire on the next tick.
Verification never requests early removal. Status capacity includes pending
blobs, upload lengths and missing-output reservations (`reserved_output_bytes`);
no finite store permits unlimited computation during a NAS outage. Admission
refuses before consuming reserved space; heartbeats and unrelated jobs that need
no additional storage continue. Queue-store loss can still lose undelivered bytes;
this is one durable authority, not a backup guarantee. Native NAS power-loss and
concurrent-backup performance qualification remain separate from laptop tests.


## Resumable jobs

On v3, [files without a NAS](files.md) adds local saves transferred to
the queue hypha before publication. The atomic generation contract below is the
same; newer local-only saves cannot resume elsewhere. Explicit NAS configuration
keeps the path described below.

Stored job inputs (`files`, including a profile's frozen source archive) and
their verified results use the queue file store in both modes. Uploading inputs
does not switch NAS checkpoints to file-store generations; workers fetch and
verify those inputs through the queue API. See [file transport](files.md).

For scarce-host yield (#328), `resumable: true` also needs frozen tested
`resume_compatibility` for each exact target/environment and a verified positive
portable generation before planning. A cooperative final save must publish a
newer generation before resume; failed final save/transport or a forced stop
holds the job, preserving prior generations. Exact process/sidecar release
remains a separate requirement. `preemptible: true` instead opts only disposable,
non-resumable experiments into restarting without a checkpoint; it never grants
this to an existing class implicitly. See the [approved yield contract](design-proposal.md#amendment-scarce-host-yield-327-328-my-26).

**Resume target (#1437).** Generations are portable as files, but an
application's resume may depend on replay that holds only on the target that
wrote them. The queue records the target (OS/arch/platform, `cpu` for CPU work)
of the attempt that wrote the newest accepted generation as the job's
`checkpoint_target`. A copied generation never moves it; only a strictly newer
one does. Placement then admits a resume only on a host of that target while one
the job may use remains in the fleet, including hosts that are busy, disabled
for maintenance or lost; a host that has left the fleet does not count. The wait
appears as hold `checkpoint_target`. A job whose `resume_compatibility` lists
both the writing target and the destination may cross between them. The
attempt that is already running is never moved by this rule.

Set `resumable: true` only when the command can safely restart. Mycelium sends
SIGTERM to the command's process group, allows 10 seconds for production preemption (configurable), or two minutes for
deadline preemption and planned leave, to save and exit,
then sends SIGKILL if necessary. Do not daemonize or leave that group. Save
periodically too: a machine that loses power cannot save on demand.

Each attempt receives `MYCE_JOB_ID`, `MYCE_ATTEMPT`, and `MYCE_CHECKPOINT_DIR`.
Each worker sets `--checkpoint-root` to its own local mount of the same NAS
checkpoint directory, configured privately per host. For example, use a generic
`NAS_ROOT/checkpoints` under each machine's verified mount. Queue `--checkpoints`
uses the queue's local path. Jobs carry relative checkpoint paths; the worker
resolves them before setting `MYCE_CHECKPOINT_DIR`. Create the root on the
mounted NAS before starting the queue. Dev creates a local root.
The stable job folder holds `attempt-1`, `attempt-2`, etc.; the environment points
to the current attempt's folder. Before starting, the worker copies checkpoints
from the attempt with the highest durable checkpoint generation.
The worker checks exact-attempt launch authorization before materializing that
copy. If preparation outlives an observation budget, the same attempt waits for
its existing writer before repeating admission or device probes; it does not
start another copy. A refused attempt's copy is removed after exact release is
acknowledged and positive execution evidence proves neither main nor sidecars
ran. Pending writers retain release custody until they finish. Unknown or
possibly executed attempts keep their checkpoints under settled expiry below.

Temporary files and directories ending in `.tmp` never count as complete and are never copied.
Workers stage a complete attempt copy in an ignored temporary directory, then
rename it into place, so a failed copy cannot publish a manifest without its payloads.
Failed staging directories remain available for inspection; retries preserve an
already prepared attempt's saves. If a writer publishes a new generation during
copying, preparation retries rather than combining its manifest with an older payload list.
This isolates disconnected copies so they cannot overwrite each other's saves.
Old step-1 checkpoint files directly in the job folder are also preserved when
resuming.

**Layout (#894).** Thousands of job folders directly under one SMB root made
every listing and metadata operation there slow on every host. With
`myce queue` and `myce dev`, the queue first creates 256 shard folders `00`–`ff`
and a `.myce-checkpoint-shards-v1` marker in the root, outside its lock and in
the background, then gives new jobs `SHARD/JOB_ID`, where the shard is the first
two hex digits of the job ID. Until that succeeds, or while the root is missing,
new jobs keep `JOB_ID`; the queue never creates the root itself and retries
preparation at most once a minute. A job keeps the path recorded when it was
submitted, so existing flat folders stay readable and resume on every retry.
The previous release's workers need only an existing parent folder, which the
queue already created; current workers recreate a missing shard beneath an
existing root.

**Settled expiry (#894).** Checkpoints are not results: a NAS checkpoint is kept
for 72 h after the job ends; declare outputs for anything needed longer. The
queue setting `myce queue --checkpoint-expiry-grace` (default `72h`) is measured
from the job's recorded terminal time (`finished_at`). A folder also expires only
once nothing can resume or still write it: the job is terminal, any `async_nas`
delivery is verified, every attempt's release is confirmed and no storage custody
is parked. Terminal jobs never resume, and retries, fan-out waves and
resubmissions use new job IDs. Elapsed time alone never qualifies a folder.
Some historical terminal records never stamped `finished_at`. For those, the
grace period counts from the latest timestamp anywhere in the job's own record:
attempt ends, stops and releases, cancellation and ledger events, then starts,
assignment, waiting and submission. That never places the end before any recorded
activity. A record with no timestamp at all is kept, and `myce status`
counts it under `checkpoint_expiry.undated` in the storage section, beside
`waiting` (settled folders waiting for their grace period or a worker). The queue picks
due jobs from its in-memory records, never from directory scans, and gives a worker that advertises
`checkpoint-expiry-v1` up to 8 job folders at a time. The worker removes them in
the background, off the queue lock, and reports each removal. The queue records
`checkpoint_expired` on the job, which survives restart. A worker removes nothing
unless the root contains the queue's marker, so an empty local mountpoint never
counts as the share. It refuses any path other than the job's own flat or
sharded folder and never follows a symlinked shard. A failed or unfinished
removal is reissued after 30 minutes, or sooner if that worker restarts or
leaves. Jobs never assigned an attempt, and file-store checkpoints, which file retention
already governs, are not touched. The whole settled job folder is removed,
including orphaned `attempt-N` copies and unpublished preparation `.tmp` folders;
their own timestamps, generations and presence in attempt history do not extend
the 72-hour window.

**Every save must be atomic:** write to a temporary file in the same directory,
flush and fsync it, rename it into place, then fsync the directory (except the
Linux CIFS no-op described below). Use one
self-contained checkpoint file, or a single atomic manifest referencing immutable
files, so readers never combine pieces from different saves. The newest complete
save is determined by a positive monotonic `generation` recorded inside the
atomic JSON save, never by file times or wall clocks:

```json
{"myce_checkpoint": {"generation": 9}, "value": {"step": 42}}
```

Increment the highest retained generation on every save, including after resume.
The Python `Checkpoint` helper does this automatically and returns only `value`
when loading. Keep a single writer per attempt. For non-JSON payloads, atomically
publish one JSON manifest with the same `myce_checkpoint` metadata referencing
immutable payload files; publish the manifest only after its payload is durable.
Temporary `.tmp` files never count. Unsequenced legacy saves still load at
generation zero: Python chooses the lexicographically greatest JSON filename on
a tie; worker resume chooses the greatest numeric prior attempt (direct job-folder
saves lose a tie). Reconciliation retains the current execution on a tie. These
are deterministic compatibility rules, not estimates of legacy progress.
Invalid generation metadata fails visibly instead of using timestamps. A job must also make its output writes safe to retry.

On Linux CIFS/SMB mounts, the Go checkpoint-copy path and Python helper skip
only directory fsync: Linux `cifs_dir_fsync` is explicitly a dummy operation
because directory operations are synchronous. Go identifies CIFS on the opened
directory with `fstatfs`; Python uses the most specific resolved mount in
`/proc/self/mountinfo`. Unknown filesystems and other platforms retain directory
fsync. File fsync and same-directory atomic rename remain mandatory. A mount
using `nostrictsync` may omit the server flush for files; avoid that option for
durable checkpoints. Server acknowledgements do not independently prove power-loss
survival of NAS caches: qualify server/storage guarantees and power-loss behavior
separately. Neither helper returns early from a still-running file flush.

Resume copies now sync each copied file, then each staged directory once,
children before parents, before atomically publishing the entire attempt and
syncing its parent. This removes per-file directory barriers on local filesystems
without weakening publication ordering. This does not establish a fixed
latency bound for a large checkpoint.
File flushes, copying, generation scanning and remote I/O can still be slow or
block. A timeout after rename on a filesystem requiring directory fsync must not
be reported as a durable save; visibility alone does not prove durability.

Size checkpoint intervals from measured worst-case/p99 save duration and
acceptable lost work, including time spent doing the next bounded work unit.
Avoid overlapping saves. Measure resume-copy latency separately, including file
count and payload size. Stop grace must exceed the longest bounded work unit plus
a complete final save and process-exit margin; the 10-second production default
is not a NAS latency guarantee. Increase configured grace only within the agreed
production-start budget; otherwise reduce checkpoint payload/file count or save
more frequently so the previous complete generation is sufficient after forced
stop. The queue's 25 ms wait budget does not bound a command's save syscall.
Run the large ext4 fixture separately with a qualified storage-duration budget
and memory/cache accounting; preserve all file-count/hash/heap assertions.

For a small restartable command use [examples/resumable.py](../examples/resumable.py)
and the [Python checkpoint helper](../clients/python/README.md#resumable-commands).
The helper owns atomic generation publication, including the CIFS exception above.
Test stop/save/restart on a later attempt and verify complete output.

Non-resumable production (`priority: production`, `resumable: false`) starts
without NAS checkpoint access and receives a local scratch checkpoint directory
under the worker state directory. Declare `resumable: true` when production needs
NAS checkpoints. Experiments and resumable production still wait for NAS recovery.

The host-hygiene mount probe also checks configured checkpoint/NAS roots with a
stat and one directory-entry read, using this host's healthy latency history.
It retries existing OS automount access with backoff, holds only dependent work,
and resumes after fresh successful probes. A missing mount that requires Linux
operator privileges or macOS owner/Keychain approval is reported with a typed
human-action code and an existing health wake; the worker never prompts for or
stores credentials. See [worker mount recovery](reference/nas-mounts.md#worker-mount-recovery-linux-and-macos)
for platform ownership, cold-start deadlines and stuck-request behavior.

The worker heartbeat allows 750 ms for its checkpoint-root probe, including
creating and removing a temporary file; slower or blocked probes report the root
unavailable. One-shot onboarding allows ten seconds per filesystem operation
and has a cancellable 15-second total validation limit, including mount inventory.
Standalone mount selection retains its five-second limit, including dry runs.
Probe failures and recovery are logged on state changes. Worker
attempt preparation also allows 250 ms for network-filesystem metadata and cold
automount access. Queue I/O and checkpoint progress reads retain their 25 ms
waits; the queue performs them outside its lock. An outstanding filesystem
operation is reused on retry rather than duplicated. A blocked filesystem syscall
may continue in the background, with at most one outstanding call per operation;
commands never start from an incomplete preparation. Status reports the NAS as
unreachable while a probe fails or exceeds its budget.

Production preemption has a 10-second default grace; deadline preemption and
planned leave have two minutes. If the NAS stays down beyond that allowance,
the worker force-stops the displaced process group, reports it interrupted, and
production proceeds after physical release is reported. The next experiment
attempt resumes the previous complete save. Planned pauses do not exhaust
retries. All jobs fail loudly after three counted infrastructure losses;
a command's own failure fails immediately.

Workers keep commands running through network loss. Logs and results are spooled
on local disk, then delivered after reconnect. Acknowledged per-execution `.log` and
`.result.json` files remain in the local spool for inspection; monitor disk usage
and archive them according to your retention needs. The worker does not start another
command while disconnected. A restarted worker kills its recorded old process
group before checking in fresh. If two copies meet after a partition, the queue
keeps the one with the newest complete checkpoint and stops the other. If the NAS is unavailable,
conflict reconciliation and uploads wait until checkpoints can be compared. No system
can send a stop through a disconnected network: copies can overlap until they
reconnect. Commands must not rely on exactly-once external side effects.

For a laptop demonstration, use `myce dev --workers 1`, submit the script as an
experiment, then submit production requiring the same memory. Follow the logs to
see stop/save and resumption. See `deploy/README.md` for optional boot services.

## Worker-local task outputs (#508)

The unfinished versioned worker-local output path is preserved on [`park/storage-protection`](https://github.com/1kuna/mycelium/tree/park/storage-protection).
New `storage_protocol`, `output_destination` and `output_retention` requests are
rejected on main. Retained fields remain readable; historical custody remains
protected from collection. Use ordinary accepted file-store outputs or async NAS
publication above. See [operations](reference/file-store.md#parked-native-storage-integration-564)
and [#504](https://github.com/1kuna/mycelium/issues/504) for future backend work.

### Client recovery ownership

Profile/adapter input storage retries span preflight and reconciled uploads in
one caller-owned context. `Client.RetryTransient` uses the shared `ClientPace`
and server seconds/date `Retry-After`, disables nested retries, and reports each
transient cause before pausing. A retry rechecks content identity and the durable
upload offset; it never blindly replays a PATCH. Permanent authentication, TLS,
capacity, integrity and configuration errors return immediately. The context
deadline (including an explicitly configured Go HTTP timeout) bounds the whole
operation; there is no retry-count limit or new default timeout. Without an owner
deadline, the caller can interrupt and resume its retained intent later.

`myce result` uses the same helper for accepted-result retrieval, including the
CI submitter. Each attempt resolves accepted custody and verifies size/SHA-256
before publishing the local result. CI still requires each platform's exact
accepted gate marker; transient recovery adds neither submissions nor a second
backoff loop. These changes require the supporting client release.
