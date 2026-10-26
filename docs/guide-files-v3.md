# Files with or without a NAS (v3)

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
explained in [write an extension](write-an-extension.md). The CLI sends the
relative job filenames, byte counts and SHA-256 identities; it does not send
local source paths. Only explicitly mapped inputs are uploaded, never a whole
project or home folder. Inputs are copied into a private working folder before
running. Programs cannot mutate shared cached blobs. Plain commands can also
use `files` with relative command arguments and declared outputs; stored inputs
cannot be combined with a workdir, Docker or NAS input mappings.
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

`get` refuses an existing destination and retains a `.myce-partial` download
beside it after interruption. Rerunning resumes it. A bad range response, byte
count or hash is refused; a server ignoring Range causes a safe complete restart.
Upload creation/HEAD/PATCH follow tus 1.0; retries use its acknowledged offset.
Final size/hash verification is mandatory. Workers retain local results until
publication is acknowledged. The queue accepts output references only from the
current successful attempt, after every referenced file is durable. A missing
required output fails visibly. Late attempts cannot replace accepted output.

## Checkpoints and interrupted connections

Jobs still use `MYCE_CHECKPOINT_DIR` and the existing atomic generation contract
in [resumable jobs](guide-resumable-jobs.md). In no-NAS mode that folder is local
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

Marked interrupted tus uploads are also collected after an idle retention window
and publication grace. POST/HEAD/PATCH refresh their durable idle timestamps;
collection excludes active requests and rechecks all protected references. Unmarked
imports and unknown files remain for explicit operator inspection and recovery.

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

Profiles and plain native jobs can use `shared_inputs` beside stored source
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
file; subsequent uses check inode, change time, size and mode. A changed entry
fails preparation visibly; stop its readers and remove the damaged reproducible
entry before retrying. Stored shared inputs use the ordinary queue retention
window; NAS originals remain owned by their project.

Input downloads renew their socket deadlines on progress instead of timing out
a whole file after 30 seconds. The idle budget is at least 60 seconds and scales
with two hash passes at 100 MiB/s. API control calls keep their normal timeout.
