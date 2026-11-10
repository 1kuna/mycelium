# Worker scratch, caches and environments

Worker scratch retention, the file and shared-input cache, the environment cache,
and how to verify the loaded environment catalog.

## Worker scratch retention

Each worker cleans its own state directory once a minute. Finished worker logs
go after the worker's `--log-retention` (default 7 days). Cleared per-attempt
state and associated process/result/diagnostic files keep that same window after
custody clears. Live attempts, pending result replay, indexed actors and Docker
custody awaiting the stopped-container collector are protected. Private Docker
evidence survives container removal until this log window expires. Unknown
orphan records are retained for recovery. Per-attempt input and
work folders (`job-files/JOB/ATTEMPT`, including inputs, unpacked source and a
Python job's `.venv`) go immediately through the existing collector once the
worker confirms that the exact attempt succeeded with accepted, published
outputs and confirmed release. The confirmation is saved with cleared custody,
so a restart before collection does not restart a grace period. Small logs and
process/result records keep the log-retention window. Failed attempts and
cancelled attempts and unknown orphan folders keep **24 hours** after the worker first finds them
without custody, timed by the worker clock. Cancellation does not collect partial
outputs into `output_refs`: recover them from the assigned worker's
`job-files/JOB/ATTEMPT` directory within that day, using the job's machine and
attempt from its status or ledger. This includes only files actually present on
that worker; logs and process state follow the separate log-retention window.
The grace bounds retention time, not bytes: scratch may be as large as the job's
work and inputs, and remains on the worker rather than consuming queue file-store
capacity. A worker outage or disk loss can prevent recovery. Collect failed test logs or partial
outputs within that day; opt-in published attempt diagnostics have their own
file retention. A held attempt keeps its folder however long it runs. The pass refuses
to run when `job-files` is not a real directory, for example a symlink. A folder
it cannot remove, such as one with root-owned Docker output, gets a fresh day
and is retried; other folders are still cleaned, and a repeated error is logged
once rather than every minute.

Successful installs keep one previous copy of each replaced artifact in the
role's `.Trash`; older installer-owned copies are pruned after activation. The
current install and the immediately replaced binary/config/service are preserved.
Failed transactions do not prune. Cleanup failures are reported and retried by
the next successful install, without undoing a healthy activation.

## File and shared-input cache capacity

From the release containing #1061, omitted `--file-cache-bytes` (or `0`)
selects automatic sizing from **current** local filesystem headroom. Admission
and the existing worker retention sweep refresh it without restarting the worker.
The effective allowance adds occupied cache blocks to measured usable free
space, so eviction exchanges occupied and free bytes without shrinking the
allowance. The existing floor leaves 1% of the filesystem, at least 256 MiB,
for filesystem metadata, logs and control state. Other caches, active work and
external writers already reduce measured free space; concurrent shared stages
reserve their unwritten bytes. Cache allocation is not a reservation against
unmanaged external writers or the native disk-isolation guarantee parked under
#504/#506. Staging rechecks current headroom and reports required/available bytes.

The startup-only policy in s072 through releases before #1061 used quarter-free,
4–80 GiB ceilings and a 20 GiB reserve. Its fixed ceiling could remain too small
after disk reclamation; #1060 records that incident. Input caches no longer use
those startup caps. Environment and model caches keep their separate policies.

`myce install worker --config CONFIG --file-cache-bytes 34359738368` retains an
explicit 32 GiB ceiling across reinstall and signed upgrade. Saved distinct positive
values remain authoritative. Like environment sizing, the exact retired 10 GiB
default in an older configuration migrates to automatic mode: older settings did
not record whether that value was explicit. New explicit settings persist that
intent, including an explicit 10 GiB. An omitted field selects automatic mode; reinstall with `--file-cache-bytes 0`
clears a saved override. No installer writes a machine-specific automatic size:
the worker refreshes it from its actual filesystem during admission and retention.

Workers report shared-input capacity, free and evictable bytes without cache
scans in check-in. Discovery and accounting run at startup and cache changes,
outside the queue mutex. Resolve and submit refuse impossible declared footprints
only once every enabled worker under consideration reports capacity. During
startup with no matching worker, shared-input work waits
with `shared_input_cache_unreported`. Workers
without the optional report retain staging-time admission; their local cache still
enforces actual capacity and reports required/available/limit bytes on failure.
Temporary occupancy waits, and mixed fleets route to a fitting worker. See
[large shared inputs](../files.md#diagnosing-input-staging) for typed reasons.

## Environment cache capacity

Omitting `--environment-cache-bytes` auto-sizes from available bytes on the local
cache filesystem (the nearest existing ancestor for a fresh path): one quarter
of free space, bounded to 4–80 GiB and capped to retain 20 GiB free. Below 24 GiB
free, automatic sizing refuses; choose an explicit byte limit after checking
headroom. The limit is a ceiling, not a preallocation. Explicit CLI byte values
win. Saved-config upgrades migrate only the exact former 20 GiB default;
other saved limits stay unchanged. A historical explicitly selected 20 GiB
cannot be distinguished from that former default; pass it explicitly again to
retain it during an upgrade.

After a cache-capacity refusal, reconfigure the idle worker with the supported
`myce install worker --config CONFIG --environment-cache-bytes BYTES` path.
The worker reconsiders saved capacity failures when its allowance changes,
including failures written by older releases. Other preparation failures remain
cached. No state-file edit or cache deletion is needed. Preparation may be
admitted on the next heartbeat; successful preparation and qualification are
separate results.

One-shot NAS install/reconfigure validation allows 30 seconds per filesystem
operation. Successful operations advance validation without a shared total
deadline. Contended deletes completed in 8–15 seconds; the stall backstop allows
two 15-second intervals (waiting behind contended I/O, then completing the call).
An indefinitely blocked CIFS call therefore refuses after 30 seconds, while
completed operations taking at least 10 seconds report `NAS slow`, their operation
and elapsed seconds. These incident samples are not a supported-load percentile
or latency qualification. Mount inventory retains its
5-second bound, and ordinary installer subprocesses retain a 15-second bound per
command; systemd lifecycle waits use the effective unit timeouts described in
[installer transaction details](install.md).
Cancellation remains active throughout setup. Logs report operation and root
progress with elapsed time, without paths or mount credentials. A stalled syscall
stays owned until it returns, so retries cannot start overlapping NAS sequences;
refusal occurs before installed files or services change.

Background checkpoint availability returns within the 750 ms control budget.
After a successful observation, a new pending probe retains availability for
up to the existing 10-second network allowance, accommodating the measured slow
share without delaying heartbeats. Cold observations, completed failures and
probes still pending after that allowance fence checkpoint-dependent work.

Workers retain prepared environments for at least 24 hours after preparation or
last use (including qualification checks and completion of long-running jobs).
Active attempt/build locks also protect them. If protected entries fill the
configured limit, preparation refuses capacity rather than evicting them.
Evictions record the environment key and cache-limit reason in the worker log.

To reclaim unused prepared environments without admitting a new preparation,
run locally on the worker:

```sh
myce worker env prune
myce worker env prune --apply
```

The default is a dry-run. Each ready prefix is listed with its age since
preparation/last use, allocated size in bytes, queue reference state, use/build
lock state and recent-use protection. `--apply` uses the same eviction path and
24-hour minimum retention as pressure eviction. Locked, recently used, and
queue-referenced environments are preserved, including queued work, sidecars,
extension variants and attempts still awaiting physical release. Reference
state is queried again under the candidate's local locks before retirement;
an unavailable queue refuses deletion. The queue must support this command's
reference API. Shared downloads and unrelated retired prefixes are preserved.
Pruning does not auto-size preparation capacity or require its 24 GiB free-space
floor; both dry-run and apply remain available below that floor.

`--config FILE` selects an installed worker; `--environment-cache DIR` overrides
its cache directory. Without installed settings, supply that directory and queue
authority using `--queue`, `--joining-file` or `MYCE_TOKEN` as appropriate.
The summary reports `reclaimed_bytes`, measured as the decrease in allocated
blocks across prepared/retired environments and shared downloads, counting
hardlinked inodes once. Candidate sizes can therefore exceed reclaimed bytes.
Apply appends synced retirement intents and a final timestamped report to
`prune-audit.jsonl` in the cache root, including partial progress on failure.

Package downloads use persistent worker-wide `.downloads/uv` and
`.downloads/pixi` caches under the configured environment-cache root. These bytes
are excluded from the prepared-environment budget and have a separate 16 GiB
combined high-water limit. Before and after builds, excess downloads are pruned
with `uv cache prune --ci` and `pixi clean cache --yes`, while no Mycelium
installer uses the shared cache. If standard pruning cannot bring the cache
under budget, preparation fails with the remaining byte count. Active builds
can temporarily exceed this waterline; the existing free-disk guard still
applies. Pruning can require a future wheel download; installed environments
are preserved. uv retains its default clone/hardlink mode (an explicit
`UV_LINK_MODE` remains available); avoid symlink mode because pruning would break
installed environments. Existing per-environment caches are not migrated or
removed on upgrade.

uv builds set `UV_HTTP_TIMEOUT=60` and `UV_HTTP_RETRIES=3`, supported by the
pinned uv release, and log verbose download/cache activity. The build watchdog
samples log and shared download-cache growth every ten seconds. Five minutes
without growth cancels the exact recorded build process group, waits for cleanup
and retries once with the persistent downloads. A second stall fails preparation
with a diagnostic in the build log; cancellation never starts a retry. This
watchdog also covers pixi and custom build steps. It does not impose a total
build deadline on a build that continues making progress.

## Verify the loaded environment catalog

`myce env list` returns a JSON array; `myce env get NAME` returns one entry
(or `environment_unknown` when absent). Both use installed client authority;
`--config`, `--joining-file`, `--queue` and `--token` precede NAME when supplied.
The authenticated GET endpoints are `/api/environments` and
`/api/environments/{name}`. They do not prepare environments or submit work.

Each entry has its name and sorted platform recipes with effective backend
(`uv`, `pixi`, or `binary`), immutable recipe `key`, and `input_digests`: SHA-256
of loaded lock/manifest bytes under canonical labels. Compare these with the
expected inputs to verify registration, even after the original files change.
Raw recipes, commands, file contents and local paths are omitted. Binary recipes
report the digest of the `binary.sha256` identity input. See the
[entry schema](../schemas/environment-v1.schema.json).
