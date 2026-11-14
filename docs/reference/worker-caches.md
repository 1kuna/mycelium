# Worker scratch, caches and environments

Worker scratch retention, the file and shared-input cache, the environment cache,
and how to verify the loaded environment catalog.

## Shared filesystem budget (#811, next release)

Each worker shares one physical-filesystem ledger across model copies and
snapshots, private-file/shared-input caches, prepared environments, shared
package downloads and its own Go build cache. Different configured directories
on the same filesystem cannot reserve the same free blocks. Existing explicit
cache ceilings still apply. Automatic shared-input feasibility uses physical
capacity; temporary occupation by another cache is a wait, not a permanent
size refusal.

Admission measures available blocks and reserves pending writes above the
existing disk floor (1% of the volume, at least 256 MiB), plus measured growth
of the worker's small control files, plus the volume's measured
[foreign headroom](#foreign-disk-headroom-1104). A model-copy policy can require a larger
floor for its own cold-copy admission. That explicit policy is not physical
headroom: unrelated writers and background collection use measured headroom,
without inheriting a model transfer's policy floor. Pending write bytes remain
shared across all writers on the volume.
Reservations remain charged through detached cancellation tails. To avoid
overcommit during concurrent writes, a transfer's full remaining reservation
stays charged until completion, even as statfs starts reflecting those writes.
This can temporarily withhold a transfer that would fit after another finishes.

Before refusing capacity, the ledger reclaims inactive entries across cache
classes, oldest successful use first. Private-file cache blobs and partials
are disposable copies: attempts have private materializations, and eviction
holds both the in-process mutex and cross-process collection/download lock.
They do not require a victim HEAD, so queue expiry or an unavailable victim
does not wedge admission. Unknown files remain protected.
Model/snapshot copies require a verified
recoverable source; source loss, unknown provenance, loaded models, queue pins,
queued snapshot/shared-input references, reader/build locks and durable
exact-attempt pins preserve them. New local recovery records contain source
paths and never enter public status. Older snapshots gain provenance on a
successful acquisition; unknown old copies remain protected. Failed reference
or source checks retain data. Catalog masters and arbitrary acquisition/model
folders are source data, never automatic deletion candidates.

Environment reclamation reuses supported prune and its live queue references,
use/build locks and existing recent-use protection. Shared downloads use uv's
`cache prune --ci` or Pixi's `clean cache --yes` with the exclusive download
lock; workers never unlink a package manager's live objects manually. Retained
shared/model partial stages count toward occupancy and are reclaimable only
with their build lock and source proof. Unknown partials remain protected.
Hardlink unions account allocated blocks, and new admission remeasures free
space after each eviction rather than assuming a removed hardlink freed bytes.

The existing retention lane also requests pending disk demand from the queue.
This lets a worker reclaim enough inactive cache for a job held before staging,
then advertise recovered free space at its next ordinary check-in. It adds no
polling daemon. Neither filesystem scans nor remote source/reference checks run
under the queue mutex or on the heartbeat path. Collection retains the existing
measured 10% duty budget and minimum minute cadence.

Default unsandboxed native jobs receive a worker-owned `GOCACHE`, shared between
jobs and protected until exact process cleanup, including durable restart pins.
Environment builders use a private compiler cache removed after their helper
custody clears. Authored `GOCACHE`, sandbox/container caches and historical Go
caches in the user's default cache directory remain external: report them and
leave them intact. Changing an arbitrary cache path inside payload code cannot
make it a managed cache.

`hygiene_collection.disk_budget` reports per-volume allocated Mycelium bytes,
free/floor/reserved bytes, measured control growth, last-pass physical free-space
recovery, pressure and unexplained growth. A positive unexplained delta raises
`foreign_disk_growth`; a falling free-space trend supplies an exhaustion
forecast. These are observations, not attribution to a particular application.
Recovery clears the warning. Foreign models, Docker data, other projects and
unique scratch are never victims. A private `disk-inventory.json` in worker state
maps the role names to local paths for its owner; public diagnostics contain
only roles and opaque volume IDs. `compiler_warning` and
`unmanaged_compiler_bytes` expose known external compiler storage without paths.
Scratch bytes/reasons come from the exact-attempt collector below.

### Foreign disk headroom (#1104)

On a volume other users also write to (builds, test caches, tool updates, the
queue's own file store), the worker keeps free space for them on top of the disk
floor. Each collection measures their use as volume size minus free space minus
Mycelium's allocated bytes there. The headroom is the larger of:

- the room for them to return to their highest use observed since the worker
  started (`foreign_peak_bytes - foreign_bytes`); and
- their fastest growth between two collections (`foreign_peak_growth_bytes`),
  which is how far they can grow before this worker's next chance to evict.

Collection evicts reproducible cache copies (oldest use first, with the existing
protections) so that free space stays at or above `floor_bytes`, which now
includes `foreign_headroom_bytes`. Automatic `--file-cache-bytes` and
shared-input allowances shrink by the same amount, and new reservations cannot
take it. On a host nobody else writes to, the headroom stays zero. It is not a
fixed size and resets when the worker restarts. A large permanent deletion by
another user reserves at most the space it freed. An explicit `--file-cache-bytes`
still overrides the automatic allowance, but not the headroom kept by eviction
and reservations.

`hygiene_collection.disk_budget[]` reports `foreign_bytes`, `foreign_peak_bytes`,
`foreign_peak_growth_bytes` and `foreign_headroom_bytes` to queues that
advertise `worker-disk-headroom-v1`; older queues receive the budget without
these fields. This only keeps what it measures: a writer that outgrows anything
seen so far can still find less space than it needs until the next collection.

These are admission and sampled-pressure protections, not a filesystem quota
on arbitrary payload/foreign writes. Active or unique data can exhaust a volume
and requires its owner's recovery/release; the worker cannot safely delete it.
The laptop fake-clock/race tests do not establish a deployed idle-worker
before/after allocated-byte receipt. That receipt belongs to the later rollout.

## Worker scratch retention

Each worker runs retention in its existing background lane, normally once a
minute. Finished worker logs follow `--log-retention`; omitted/zero retention
preserves them. Unique process/result/diagnostic records remain evidence even
after custody clears. Live attempts and pending replay keep their scratch for
however long custody lasts.

From the worker and queue release containing #1227, the collector reconciles
each unclaimed `job-files/JOB/ATTEMPT` folder with the queue by exact job ID,
attempt number and worker identity. It also honors the existing durable local
publication receipt, including after restart. Scratch goes automatically when:

- The queue accepted that exact attempt's successful result and confirmed its
  release, or the worker saved that publication confirmation before clearing custody.
- The queue authorized file expiry through `myce file release apply`, or the
  terminal job passed the queue's positive `--file-retention` duration (default
  `72h`, so scratch held for a finished job goes after 3 days; an explicit `0`
  keeps it until release). Physical
  release must be confirmed, and pending delivery or protected-storage custody
  still prevents collection.
- The queue confirmed release of that exact attempt and the job has since moved
  to a later attempt (interrupted, yielded, preempted or requeued). A superseded
  attempt can never be accepted, so its scratch goes on the next sweep, live or
  terminal job alike, instead of waiting for file retention. A copy the queue
  still marks recoverable is kept (`recoverable_attempt`). Checkpoints live in
  the checkpoint root or the queue file store, not scratch, and pending uploads,
  diagnostics and replay keep local custody until the queue has them. Partial
  outputs of a failed or cancelled job are recovered from its current attempt.

The old fixed **24-hour** scratch promise was retired under MY-34. A saved
24-hour file-retention policy still means 24 hours from the job's finish time;
other saved positive durations retain their own meaning. Omitted/zero file
retention does not expire unique failed/cancelled scratch. `--log-retention`
does not authorize deleting scratch. The `.myce-unclaimed` marker records first
observation for diagnosis; its age neither authorizes deletion nor prevents
collection of accepted/released data. Eligible folders and their markers are
removed together. Unknown jobs, unknown folder names, unmatched attempts,
unconfirmed custody and failed queue lookups remain protected. Absence from a
fleet status page never proves release. An older queue without this endpoint
keeps unconfirmed folders; existing local publication receipts still work.

Cancellation does not publish partial outputs into `output_refs`. Recover them
from the exact assigned attempt before its configured file-retention deadline
or explicit release. To release owned terminal scratch deliberately, use the
existing reference/custody-aware flow after preserving any required files:

```sh
myce file release plan --project P JOB_ID > release-plan.json
# Review the exact job/attempt, blockers, references and required downstream copies.
myce file release apply --accept-expiry release-plan.json
```

The release applies to the selected terminal job's files, including scratch;
it is not a force-delete for an unknown or live attempt. Retain the plan/apply
receipts. `file_release_stale` requires a fresh plan. Queue references still
protect shared blobs; scratch reclamation leaves the accepted queue manifest
and its referenced blobs intact. The authenticated worker endpoint
`POST /api/workers/attempt-files` takes `id`, `attempt`, and `machine` and returns
`reclaim`, a fixed `reason`, and an optional policy deadline `until`; it is a
read-only exact-attempt check, not an override or a paginated status lookup.
Each retention pass sends one `POST /api/workers/attempt-files/batch` with
`machine` and `attempts` (`id`, `attempt` pairs) and gets `dispositions` back in
the same order; the queue takes its lock for one lookup at a time. Against a
previous-release queue, which answers that route with a plain 404, the worker
falls back to one single-attempt request per folder.

Worker logs report `worker scratch retention` with allocated `retained_bytes`,
`reclaimed_bytes`, and `retained_bytes_by_reason` (for example `local_custody`,
`unique_unreleased`, `unknown_job`, `file_retention`, `recoverable_attempt`, or
`queue_unavailable`).
These are filesystem-block measurements taken in the retention lane, outside
queue locks and heartbeat work. Hardlinks across attempt folders can overstate
physical savings; a partially failed deletion is conservatively counted as
retained until the next pass. Measurement errors accompany the sweep error.
The pass refuses a symlinked `job-files` root and never follows attempt symlinks.
After collection authority and custody checks pass, the collector restores owner
write permission on directories inside the disposable attempt tree. It leaves
file permissions and external symlink targets unchanged, including hardlinked
input files. A failed removal is retried on the next pass without blocking other
folders.

Accepted scratch does not wait for the configured terminal-file retention
window: the next retention pass can collect it even under disk pressure. The
shared disk-budget pressure evictor introduced in #1250 accounts for scratch
but does not select it as an eviction candidate; crossing the disk floor does
not synchronously trigger scratch collection or override retention/custody.
The existing startup, minute and attempt-release sweeps own that collection.

This bounds disposable scratch by acceptance or the installation's authored
retention policy, not by a universal byte cap. Protected unique data can exceed
available capacity and requires recovery/release by its owner; disk loss or a
worker outage can prevent recovery. No live-worker reclamation measurement or
deployment is implied by the laptop regression tests.

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
for filesystem metadata, logs and control state, and the measured
[foreign headroom](#foreign-disk-headroom-1104) leaves room for the volume's
other users to swing and grow. Other caches, active work and
external writers' current use already reduce measured free space; concurrent shared stages
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

A `myce dev` session sizes from need instead: its cache starts at 1 GiB, or at
what it already holds, and grows to each environment's measured size while the
cache volume keeps its disk floor (1% of the volume, at least 256 MiB). A
laptop with a few GiB free runs dev and the quick start. When an environment
does not fit, the refusal names it, the bytes it is short, and the measured
free space and floor.

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
are excluded from the prepared-environment budget and have a separate combined
high-water limit measured on each worker: twice the largest growth one build
caused (`download-need.json` in the cache root), which holds the largest
environment's downloads plus one more build of that size, such as its next lock
revision. A build records its growth before its own post-install prune, so it
never prunes what it just fetched; before any measurement there is no separate
limit. Before and after builds, excess downloads are pruned with
`uv cache prune --ci` and `pixi clean cache --yes`, run from the download cache
itself (pixi reads a project manifest from its working directory), while no
Mycelium installer uses the shared cache. A failed prune is logged in the build
log and never fails a build. Real disk pressure still prunes downloads through
the worker's free-disk floor. Pruning can require a future wheel download; installed environments
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
