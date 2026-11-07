# Checkpoint-root lifecycle audit (#894)

Source baseline: main `87ec085defb56debe8b276bec3751b3e791a5a77`,
2026-11-07. This inventories supported code paths, not the ownership or age of
any live directory entry. No existing checkpoint entry was inspected or removed.
Runtime evidence and unknown SMB causes are tracked in
[#894](https://github.com/1kuna/mycelium/issues/894) and
[#346](https://github.com/1kuna/mycelium/issues/346).

Ordinary submission assigns `checkpoint-root/job-id` in
[queue.go:838](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/queue.go#L838).
Fan-out execution creates a distinct child job ID and checkpoint assignment in
[fan_out.go:312](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/fan_out.go#L312).
Assignment itself creates no filesystem entry. Replies make NAS assignments
relative to the configured queue root; the worker resolves them under its own
root ([queue.go:1610](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/queue.go#L1610),
[worker_attempts.go:1274](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/worker_attempts.go#L1274)).

The gate is broader than resumability: `needsCheckpoint` is true for **every
non-production job**, and for resumable production jobs
([checkpoint_io.go:154](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/checkpoint_io.go#L154)).
Consequently experiment/deadline commands can populate the NAS job root even
when they never save useful progress. The supported preparation path makes the
job directory, then atomically publishes an `attempt-N` directory
([worker.go:534](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/worker.go#L534),
[worker_recovery.go:688](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/worker_recovery.go#L688)).
Non-resumable production uses local worker-state `checkpoints/job-id/attempt-N`
instead; file-transport mode uses local `file-checkpoints` and verified queue
generations ([worker.go:545](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/worker.go#L545),
[worker_files.go:37](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/worker_files.go#L37)).
Those local paths do not explain NAS top-level growth.

The NAS lifecycle by entry class is:

- **Job directory and current attempt:** preparation creates them; terminal
  completion does not remove them. No NAS job-root collector was found in the
  audited retention paths. A growing count is structurally supported, including
  jobs without saved payloads; the actual live composition remains unverified.
- **Prior attempts:** preparation scans earlier `attempt-N` directories and
  copies the greatest complete generation, using the attempt number to break a
  generation tie. It preserves prior attempts and an already prepared current
  attempt containing progress
  ([worker_recovery.go:694](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/worker_recovery.go#L694)).
  Removing them based only on terminal state or mtime could destroy recovery.
- **Preparation staging:** `.myce-prepare-*.tmp` lives *inside* a job directory,
  then renames to `attempt-N`. Returned copy/sync/rename errors leave the staging
  directory: there is no deferred removal in this function
  ([worker_recovery.go:733](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/worker_recovery.go#L733)).
  Repeated returned failures can accumulate nested remnants; they do not add a
  separate checkpoint-root entry per attempt. The progress scanner skips `.tmp`
  subtrees. A syscall exceeding its caller's wait can still be active, so age
  alone does not prove abandoned ownership.
- **Worker/queue availability probes:** create top-level `.myce-probe-*` files,
  close and remove them in the same sequence. Worker probes use `.tmp`; queue
  probes additionally fsync the file. A crash or failed/blocked remove can leave
  a remnant ([worker_recovery.go:826](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/worker_recovery.go#L826),
  [queue.go:448](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/queue.go#L448)).
  At this baseline the separate host-hygiene probe reads a directory stream;
  the correction in #895 belongs to another owner.
- **Installer validation:** creates `.myce-install-probe-*`, writes/syncs/closes,
  renames with `.renamed`, then removes. Deferred cleanup uses the same cancelled
  context; comments explicitly retain a disposable probe when cancellation
  prevents cleanup ([onboarding_storage.go:321](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/onboarding_storage.go#L321)).
  Installation can create the configured root/ancestors, not ordinary job IDs.
- **Payload checkpoints and temporary saves:** the Python helper writes inside
  the exact attempt directory and deliberately preserves failed temporary
  writes for inspection
  ([checkpoint.py:68](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/clients/python/src/mycelium/checkpoint.py#L68)).
  User-managed legacy files/directories can also exist; their retention and
  ownership cannot be inferred from a familiar name.

Existing cleanup is narrower. Worker retention operates on local `job-files`,
logs and attempt-state records, with custody/publication checks
([log_retention.go:194](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/log_retention.go#L194)).
Cache hygiene collects local file/environment/installer staging; it is not a NAS
checkpoint collector
([worker_hygiene_collection.go:57](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/worker_hygiene_collection.go#L57)).
Queue file expiry explicitly protects checkpoint/resume generations
([file_retention.go:203](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/file_retention.go#L203)).
History retirement deletes queue records, not NAS paths
([history_retention.go:15](https://github.com/1kuna/mycelium/blob/87ec085defb56debe8b276bec3751b3e791a5a77/internal/myce/history_retention.go#L15)).

The supported probe owns and removes its own temporary entry, but that is no
authority for an investigator to delete a pre-existing similarly named file.
This investigation authorizes removal only of its own scratch. Existing data,
even apparently empty terminal job directories or preparation/probe remnants,
requires explicit ownership, quiescence, recovery/reference and retention
authority before collection. The retention/cache owners retain their scope.

Prefix sharding and an ownership-aware checkpoint lifecycle are proposals for
qualification, not proven fixes for read EAGAIN, rename EBUSY or metadata tails.
Sharding needs a large/small-directory scratch comparison on the same client
and session, plus old relative-path resume and exact-attempt fencing tests.
Retention needs a separate approved checkpoint policy; do not reuse result/cache
expiry as permission to remove unique progress. Neither change repairs a broken
SMB session by itself.
