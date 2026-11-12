# Results

Detail for "Files back" in [the guide](../SKILL.md).

## Accepted output

`myce result JOB_ID --out DIR` retrieves accepted output and verifies manifest
sizes and hashes; apply the project's own output acceptance too. Failed jobs have
no accepted outputs. `myce result JOB_ID --attempt N --out DIR` requests
diagnostic artifacts, which may be unavailable. A recovered result is a separately
identified artifact and does not change the original execution verdict.

## Output-contract failures

An exit-zero command can still fail its declared output contract. Status/GET
`execution_cause.finalization` gives the `step`, declared `output` or
`destination`, a fixed safe `error`, optional HTTP status and
`command_exit_code: 0` when observed. Finalization failures keep a metadata-only
`attempt_history[].diagnostics.summary`; `result --attempt N` writes it as
`summary.json` even without diagnostic capture. Optional logs and reports still
need capture and storage.

Declare outputs as files, folders (`plots/`) or globs (`"logs/*.txt"`), in an
entry or a job spec; do not tar them. `after` stages, `output_destination`,
`storage_protocol` and extension jobs still name individual files. Results list individual files and `--out DIR` recreates subfolders. An
empty folder, an unmatched glob, or a symlink or special file inside one fails
the output contract. A folder or glob the file store cannot admit fails once with
finalization `limit_exceeded` before any upload: narrow it or release finished
results. A pending upload or reservation keeps local results and retries; a
publication-pending job is not accepted success. Keep its identity and resolve
the reported storage or permission problem. Details:
[folder and glob outputs](https://github.com/1kuna/mycelium/blob/main/docs/files.md#folder-and-glob-outputs-1341).

## Keep files on failure

Do not exit 0 just to return logs. Declare `keep_on_failure: [logs/, {path:
junit.xml, format: pytest-junit}]` (files, folders or globs; `format` is optional
`pytest-junit` or `vitest-junit`). A failed attempt publishes them as `kept/NAME`
in its diagnostics, retrieved with `myce result JOB_ID --attempt N --out DIR`;
the job stays failed. See
[keep files from a failed attempt](https://github.com/1kuna/mycelium/blob/main/docs/files.md#keep-files-from-a-failed-attempt-1341).

## Large outputs and release

For large outputs, publish to the project's own storage inside the job and
return a small path/size/SHA-256 receipt. Verify downstream copies and finish
every use before releasing only your own terminal results:

```sh
myce file release plan --project P JOB_ID... > release-plan.json
# Review files, protections and all downstream custody before this irreversible apply.
myce file release apply --accept-expiry release-plan.json
```

`file_release_stale` calls for a new reviewed plan; protected, live or unresolved
custody cannot be forced. Collection is asynchronous and released references
return `result_expired`. Project attribution is not an authorization check. Keep
plan/apply receipts and verified copies. By default a finished job's files are
kept for 3 days (`72h`), then return `result_expired`: download what you need
within that window. An operator can set the queue's `--file-retention`, and `0`
keeps files until explicit release. A project policy `file_retention` (for example `72h`) expires its own finished jobs'
files sooner, and a `file_store_capacity` refusal names the limiting bound, bytes
held by finished jobs and the release or retention step to take. See
[result retention and release](https://github.com/1kuna/mycelium/blob/main/docs/files.md),
[per-project retention](https://github.com/1kuna/mycelium/blob/main/docs/reference/file-store.md#per-project-file-retention-1214)
and [operations](https://github.com/1kuna/mycelium/blob/main/docs/operations.md).

## Worker scratch

Accepted exact-attempt scratch is reclaimed automatically, and so is a released
attempt the job has moved past (interrupted, yielded, preempted or requeued):
recover partial files from the job's current attempt. Terminal failed or
cancelled scratch follows the queue's file-retention policy (`72h` by default) or an
explicit owner file release; there is no fixed 24-hour promise (MY-34). Unknown
data and pending upload, replay or custody stay protected. Recover needed partial
files before the configured deadline or release, then use the file-release
plan/apply flow above. See
[worker scratch retention](https://github.com/1kuna/mycelium/blob/main/docs/reference/worker-caches.md#worker-scratch-retention).
