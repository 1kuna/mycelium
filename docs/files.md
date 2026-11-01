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
cannot be combined with a caller workdir or NAS input mappings. Managed Docker
batches support stored files with separate input/output roots, described below.
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

### Managed Docker files

A plain `run.docker: true` batch with `files` or `outputs` requires a Linux
worker advertising `docker_files_v1` as well as `docker`. Older workers cannot
receive it. The worker stages verified private copies at `/myce/inputs`,
read-only in main and every managed sidecar, preserving relative input names.
`/myce/work` is a separate private read-write output/scratch directory and the
container's working directory. Outputs are relative to this directory.
`MYCE_INPUT_DIR=/myce/inputs`, `MYCE_OUTPUT_DIR=/myce/work` and
`MYCE_SCRATCH_DIR=/myce/work` describe these roots. Inputs are not copied into
work. The durable result spool is outside all container mounts.

All bind mounts are worker-owned attempt directories, including the existing
checkpoint and progress mounts. Caller bind mounts, `run.workdir`, Docker
`--workdir`, `--user`, `--group-add`, all authored environment (`run.env`, sidecar
`env`, Docker `--env`), NAS `inputs`, `shared_inputs`, prepared source and native
environments are refused for this contract. Put configuration in hashed files
or payload arguments. Images must already be local and digest-pinned; image
defaults remain part of those immutable image bytes.

The worker sets the container user to its own numeric UID:GID, drops all Linux
capabilities and sets `no-new-privileges`. Private directories stay mode 0700;
inputs are private copies, never writable shared cache blobs. Images must
support this non-root numeric identity, readable application files and writable
state beneath work/checkpoint/progress. A fixed image `USER` is overridden.
Root-only entrypoints or incompatible user-namespace mappings fail visibly;
the worker never widens permissions or uses privileged helpers to repair
root-owned output. Existing device selection, limits, offline/private networks,
start authorization and exact-attempt custody still apply.

The daemon must access the worker's local paths. `--mount` refuses missing
source directories; inspection checks exact sources, destinations and read-only
mode before start and during observation. Images declaring `VOLUME` are refused
before creation, avoiding anonymous host volumes. Remote-daemon and Docker Desktop path sharing are not assumed. Check
the worker-local Docker refusal when a mount cannot be created.

After every writer is proven stopped, the worker validates declared regular
files, hashes and durably spools them, uploads through the normal verified file
store and submits an attempt-specific result. Missing, escaping, non-regular or
unreadable outputs fail the job. Upload/acknowledgement failures retain the
spool and custody for replay, including restart; stale attempts cannot publish.
Cleanup uses existing attempt-file and Docker retention after queue settlement.

For a CPU example, use a joined Linux worker with `--docker-host
unix:///var/run/docker.sock`. Prepare its image explicitly:

```sh
docker pull busybox:1.37.0
docker image inspect busybox:1.37.0 --format '{{index .RepoDigests 0}}'
printf 'hash-in hash-out\n' > data.txt
myce file put data.txt
```

Substitute the returned file identity and image digest in `docker-files.yaml`
(including the actual byte count printed by `myce file put`):

```yaml
name: docker-files
project: demo
priority: experiment
submission_key: docker-files-example-v1
files:
  data.txt: {sha256: REPLACE_WITH_FILE_SHA256, size_bytes: 17}
outputs: {answer.txt: results}
run:
  docker: true
  command: [docker, run, "busybox@sha256:REPLACE_WITH_IMAGE_DIGEST", sh, -ec,
            'cat /myce/inputs/data.txt > /myce/work/answer.txt']
needs: {cpu_cores: 1, memory_gb: 0.125, platform: [cpu]}
resumable: false
```

Run `myce submit docker-files.yaml`, wait for success, then use `myce result
JOB_ID --out ./download`. The accepted `output_refs.answer.txt` must match
`files.data.txt`. Reusing the same submission key and spec returns the same job;
use a new key for changed inputs or commands. This proves CPU file transport;
GPU runtime and application parity require their own qualification.

## Transfer behavior

`get` refuses an existing destination and retains a `.myce-partial` download
beside it after interruption. Rerunning resumes it. A bad range response, byte
count or hash is refused; a server ignoring Range causes a safe complete restart.
Upload creation/HEAD/PATCH follow tus 1.0; retries use its acknowledged offset.
File transfers use per-socket-operation deadlines, with no whole-request timeout.
The idle budget is the greater of 60 seconds and 30 seconds plus two full-blob
hash passes at 100 MiB/s. API calls retain their ordinary 10-second budget.
Default and installed pinned TLS clients use this progress policy. An injected
Go `DialTLSContext`, `DialTLS`, or custom `http.RoundTripper` retains its authority
and protocol handling and owns its I/O deadlines; custom dial setup is bounded.
A stalled result reports `result_download_failed`; Python raises
`TransferStalledError`. Size/SHA-256 verification and atomic publication still
apply. For partial cache eviction, the worker reads the full blob size from
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
to refuse the failed job. Bytes have a fresh `--file-retention` window (default
seven days) beginning at recovery, even if original job files expired. Each
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

## Model files and caches

Import an approved model once with `myce file put`. In the existing model catalog,
set `file_store: true`, `size_bytes`, and `sha256`; `path` is unnecessary. For a
bundle retain the existing `files` manifest and its combined size/hash identity,
and import every listed file. Configuration and tokenizer files belong in that
manifest too. Engine loading never downloads from the internet. Existing NAS
catalog entries, `direct_nas`, pins, pre-copy, protected loaded models and model
cache limits retain their behavior. Extensions can receive models as job inputs.

`--file-cache` and `--file-cache-bytes` configure each hypha's reproducible cache
(default 10 GiB, under its state folder). It counts complete and partial bytes,
evicts least-recently-used entries, and refuses a file larger than its allowance.
A private materialization keeps active jobs independent of eviction. Model
loading/pre-copy uses the existing `--model-cache-bytes` admission and pin/loaded
protection. Stored model eviction verifies that authoritative files remain before
removing disposable copies. The original NAS cache Trash behavior is unchanged.

The queue reclaims stored inputs and results **7 days after a job becomes
terminal**, configured with `queue --file-retention DURATION` (positive, default
`168h`). Queue maintenance uses the queue clock; no separate cleanup timer runs.
Job records, keyed answers and file identities remain, with `files_expired: true`.
Increasing the window cannot restore files that already expired.

Publishing a blob or successfully HEAD-verifying it refreshes its on-disk mtime.
Collection gives those bytes a six-hour publication grace, covering the gap from
the first file's refresh through later multi-GB uploads, slow NAS, or a queue
outage/deploy before submit/complete establishes a new reference.
That grace survives queue restarts; it does not extend an expired job's result
recovery contract. Expired reusable worker-cache copies remain evictable.

A blob remains while another job within its window or any non-terminal job needs
it, or any checkpoint generation, catalog model, extension or environment refers
to it. Checkpoints and catalog storage are outside this collector. Open downloads
finish through their existing file handles; disconnected callers must download
within the retention window. An expired result download returns typed HTTP 410
`result_expired`. Raw file fetches return that code when expired bytes have been
reclaimed, rather than reporting an unknown file. Accepted result helpers also
check the job's expiry marker, even if another reference keeps shared bytes.

Only uploads explicitly created for job inputs can be reclaimed before submission.
CLI/profile input uploads, Go `UploadJobInput` and Python `upload_file` send tus
metadata `purpose=job-input`; the queue persists an eligibility marker and publish
time in its existing SQLite state. Collection requires both the publish time and
blob's mtime to be older than `--file-retention` (default `168h`), the six-hour
publication grace to have elapsed, and no job (including retained expired records
and fan-out results), keyed answer, checkpoint, model, extension, environment or
pending file-reclamation list to reference the blob. All times use the queue Clock.
Publishing or successful HEAD verification refreshes the mtime. Job admission clears
the marker in the same transaction; normal terminal-job retention then governs it.
Model and environment catalog registration also clears and persists eligibility
immediately, without waiting for a maintenance tick.

Unmarked imports, including `myce file put`, Go `UploadFile`, models and existing
uploads from older clients, are never orphan-swept, even with an empty or edited
model catalog. Reusing an unmarked import as a job input does not mark it; a generic
import of marked bytes clears the marker. The collector shares its bounded per-tick
budget between terminal reclamation and eligible marked uploads, rechecking purpose,
custody and mtime before unlink. It does not scan the blob directory for orphans.

Internally created recipe and extension bundle blobs also have durable eligibility
recorded before creation, so rejected submissions and subsequently pruned history
do not leak bytes. The same bounded collector requires no remaining reference,
an eligibility time and blob mtime older than retention, and publication grace.
Retained jobs and keyed answers protect these blobs independently of result expiry.
A generic import clears this eligibility too. Existing unmarked bytes are not
retroactively classified by their contents or swept by scanning the blob directory.

Marked interrupted tus uploads are also collected after an idle retention window
and publication grace. Successful POST/HEAD/PATCH refresh their durable idle
timestamps; error replies do not. Collection excludes active requests and rechecks
all protected references. A crash after blob creation recovers the publication
marker from the blob's mtime and cleans up expired upload remnants. Unmarked
imports and unknown files remain for explicit operator inspection and recovery.

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

## Large read-only inputs

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
`--file-cache-bytes` (default 10 GiB), in addition to the private-file cache's
allowance. Physical disk headroom applies across both. LRU eviction removes
reproducible inactive copies; active attempts hold use locks through process
cleanup. Mode bits provide cooperative read-only access, not a sandbox against
the owning account changing permissions. First use after restart hashes every
file; subsequent uses check inode, change time, size and mode. Metadata changes
trigger full re-verification, allowing another cache instance's valid rebuild.
A damaged entry fails preparation visibly; stop its readers and remove the reproducible
entry before retrying. Stored shared inputs use the ordinary queue retention
window; NAS originals remain owned by their project.

Input downloads renew their socket deadlines on progress instead of timing out
a whole file after 30 seconds. The idle budget is at least 60 seconds and scales
with two hash passes at 100 MiB/s. API control calls keep their normal timeout.

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
Fan-out parent aggregation and explicit plain Docker submissions do not support
this policy; submit individual native/extension tasks instead.

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

Pending/failed delivery pins outputs through ordinary seven-day expiry and
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
Temporary files and directories ending in `.tmp` never count as complete and are never copied.
Workers stage a complete attempt copy in an ignored temporary directory, then
rename it into place, so a failed copy cannot publish a manifest without its payloads.
Failed staging directories remain available for inspection; retries preserve an
already prepared attempt's saves. If a writer publishes a new generation during
copying, preparation retries rather than combining its manifest with an older payload list.
This isolates disconnected copies so they cannot overwrite each other's saves.
Old step-1 checkpoint files directly in the job folder are also preserved when
resuming. Checkpoints remain after completion or failure.

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
publication above. See [operations](operations.md#parked-native-storage-integration-564)
and [#504](https://github.com/1kuna/mycelium/issues/504) for future backend work.
