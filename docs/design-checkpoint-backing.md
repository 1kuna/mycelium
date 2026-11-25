# Checkpoint backing: commands save locally, the worker copies to the NAS

Design for [#2100](https://github.com/1kuna/mycelium/issues/2100). **Draft. Do
not merge until the orchestrator approves.** The [file contract](files.md#resumable-jobs)
and the [shared-queue design](design-queue-beyond-one-host.md) stay in force.

## Recommendation

Stop giving a running command a path on the NAS mount. A resumable command
saves into a local directory under the worker's state volume. The worker
copies each finished save to the NAS through a directory handle it has
checked, verifies the copy, and only then reports the generation. The queue
accepts that report under the existing attempt fences and records which bytes
it accepted. Resume copies those exact bytes back to local disk.

This is the shared-queue design's file custody rule ("stage or upload
immutable bytes in attempt-specific storage, verify the durable NAS copy,
then commit the accepted reference under the attempt check"), applied now
with today's single queue. The work is done once and moves unchanged to the
shared authority at its stage 3.

The worker config stays the same. `--checkpoint-root` still names the
checkpoint folder on the mounted share. No new setting is needed.

## Why the earlier attempts failed

PRs #2085 and #2096 kept the command writing straight to the mount. They tried
to prove, from outside, which filesystem the path reached at each moment.
Three review rounds kept finding gaps:

- The command opens the path itself on every save. A mount can drop between
  any check and the next open. The Python helper creates missing folders
  (`mkdir(parents=True)`), so a save into an empty mountpoint succeeds and
  writes a notice. Nothing the worker checks beforehand can stop that.
- Device and inode numbers change on a healthy remount, so identity based on
  them suppressed real progress.
- Checking a notice meant reading the named save back from the NAS. That
  added NAS reads that #1670 removed. It also broke custom writers whose
  generation metadata sits past the first 1 MiB.
- An in-memory receipt was lost on queue restart. Attempts started by an
  older binary had no receipt at all.

The common cause is that the command does I/O on a mount nobody can pin.
Taking the mount out of the command's path removes that whole class of
failure.

## Approaches compared

**A. Keep direct writes and pin the backing.** Put an identity marker in the
root. Check it at preparation through a directory handle. At each notice,
re-read the named save on the checked root to confirm its generation.
Persist the binding per attempt in SQLite.

**B. Route saves through the queue file store.** Every resumable job uses
the existing no-NAS capture or `local_first` upload. The queue mirrors
accepted generations to the NAS, as `checkpoint_mirrors` already do.

**C. Local saves, verified worker copy to the NAS (recommended).** Described
below.

**D. Fail closed by folder absence.** The worker creates the attempt folder.
Helpers stop creating it, so a save into an empty mountpoint fails. This is
the cheapest option and is listed for completeness.

| Case | A. Pin the backing | B. Queue file store | C. Worker copy | D. Folder absence |
| --- | --- | --- | --- | --- |
| 1. Nested share down, parent up | Caught at preparation. A mid-run save into the parent is caught only if the re-read happens before the share returns. | Safe: the worker never writes to the NAS. The queue's mirror needs the same marker check. | Safe: marker missing, so no copy is made. The save stays local and pending. | Caught when the folder is missing. A custom writer that creates folders is not caught. |
| 2. Share replaced at the same mountpoint | Caught by the marker at the checks. The command writes into the replacement between checks. | Safe on the worker. The queue mirror needs the marker. | Safe: wrong or missing marker means no copy. | Not caught if the replacement has the same layout. |
| 3. Mount vanishes between check and use | The command's own opens can't be pinned. | Safe | Safe: every worker operation goes through one checked handle. A vanished mount returns an error, never the fallback. | Partly: an open after the folder is gone fails. |
| 4. Mid-run NAS loss, fallback save accepted, then blocks resume as the floor (#2101) | Needs the NAS read-back on every save (MY-43 cost, the 1 MiB issue). Races with remount. | Safe: acceptance needs the verified upload. | Safe: the local save can't be accepted until its verified NAS copy exists. The floor only moves on verified copies. | Not caught for writers that create folders. |
| 5. Queue restart while on a fallback | Needs the binding persisted in SQLite. | Safe: acceptance is durable. | Safe: acceptance is durable in SQLite. Pending local copies replay from worker state. | Not applicable |
| 6. Mixed-version rollout | Old workers keep today's behaviour until upgraded. | Same | Same. The queue records each attempt's protocol. Old-protocol attempts keep main's rules, including #2101, until they finish. | Same |
| 7. Healthy remount with new device or inode | Safe if identity is marker content, not device or inode. | Safe | Safe: identity is the marker's ID. | Safe |
| 8. Live takeover of an older binary's attempt (MY-55) | Has no binding for the adopted attempt. | The adopted attempt stays on its old path. | The adopted attempt stays on the legacy protocol until it ends. The next attempt starts on C and stages through the checked handle. | Same as today |
| 9. Missing root during competing-copy recovery | Needs "unknown, not zero" handling everywhere a scan runs. | Safe: compares accepted records. | Safe: compares accepted records in SQLite, with no NAS scan. An unreadable root on a legacy attempt stays unknown (#2101's rule). | Not addressed |
| 10. Custom writers with metadata past 1 MiB | Regresses unless the full save is streamed over the NAS. | Regresses: the capture limits are 1 MiB of JSON and 1024 files. | Safe: the generation comes from the local notice or a local scan. No NAS read-back of content. | Safe |
| 11. `join --checkpoint-root` cold start | Marker check | Safe | Safe: no local/NAS classification. Missing marker means unverified. | Not addressed |
| 12. Today's Linux layout, no extra config | Safe | Changes where bytes live. Bulk goes through the queue host. | Safe: same `--checkpoint-root`. The marker is found there. | Safe |
| Bulk and capacity | NAS only | Every save crosses the queue host's disk, which hit 97% under MY-47. Against MY-40. | One local copy plus one NAS copy. Local disk must hold about two saves. | NAS only |
| Fits the shared-queue stages | Rewritten at stage 3 | Rewritten at stage 3: shared mode accepts only after the NAS copy. | Is stage 3's custody rule | Not related |

C is the only option with no open case and no rework at the shared-queue
cutover. A leaves case 3 open by construction. B moves bulk the wrong way.
D depends on how writers behave.

## How C works

1. **Root identity.** The checkpoint root holds `.myce-checkpoint-root.json`
   with a random root ID. The queue writes it once, in its existing
   background shard preparation, and only into a root that already holds the
   shard marker (`.myce-checkpoint-shards-v1`). The ID is recorded in SQLite.
   Once an ID is recorded, the queue never writes a new one. A missing or
   different marker is an outage for the operator, not something the queue
   repairs. Workers learn the expected ID from the queue with the assignment.
   Identity is the marker's content, so a healthy remount keeps it and a
   replaced or nested-down share does not have it. The same check replaces
   the path-hash `deliveryRootID` used by the queue's own `async_nas` and
   checkpoint-mirror publisher.
2. **Checked handle.** Each NAS operation by the worker opens the root once
   with `os.OpenRoot` and reads the marker through that handle. If it
   matches, all copies, verification and cleanup in that operation go
   through the same handle. The `async_nas` publisher already works this way
   (`publishOutputDelivery`). A handle opened on the real share never
   follows a later fallback. A vanished mount returns an error. The NAS calls
   keep the existing single-flight, bounded `checkpointIO` runners outside
   `q.mu`.
3. **Local save.** `MYCE_CHECKPOINT_DIR` is a managed attempt folder on the
   worker's state volume. The atomic generation contract stays the same.
   Writers that already work keep working unchanged.
4. **Capture and copy.** A new notice, or the existing event scan when there
   is none, triggers capture. Capture takes a consistent local snapshot that
   excludes `.tmp` and unsafe names, as the no-NAS capture does today. The
   worker writes the payload files into
   `SHARD/JOB/generations/GEN-ATTEMPT/` and then a small receipt with sizes
   and SHA-256, as `.myce-delivery.json` is written today. Each file is
   synced. Directory renames on CIFS are avoided, as files.md advises. The
   worker verifies the receipt and the bytes before reporting. Folders are
   immutable and unique per attempt, so nothing is overwritten.
5. **Acceptance.** The check-in carries the generation, the receipt digest,
   the root ID and the relative folder. The queue accepts it only under the
   exact job, attempt, machine and session fences, and only above the
   current floor. One SQLite transaction records the accepted reference and
   raises the #2101 floor. This is the only way a C attempt's progress
   advances. A plain notice is shown as local progress
   (`local_checkpoint_generation`) and never accepted.
6. **Save return.** The Python helper waits for the worker's acceptance
   file, using the `local_first` acknowledgement protocol
   (`.myce-accepted.json`). A helper save that returns is therefore portable,
   as today. Custom writers that skip the acknowledgement become portable
   when the copy is accepted, a few seconds later. A power loss in that
   window loses only that save; the previous accepted one remains.
7. **Resume.** Preparation copies the accepted folder, checked against its
   receipt, from the checked handle into the new local attempt folder. If
   the accepted bytes are missing, the result is the existing
   `checkpoint_floor_missing` hold. If the root is unverified, the result is
   the existing `storage_root` hold. C jobs don't scan `attempt-N` folders.
8. **Retention.** After a newer generation is accepted, the publishing
   worker removes older generation folders of that job through the checked
   handle, keeping the accepted one and any pending one. Settled expiry and
   purge are unchanged and operate on the whole job folder.
9. **Disk.** Admission charges `disk.checkpoint_bytes` to the local state
   volume as twice the declared size: one save plus the capture being copied.
   It still charges the NAS root as today. A worker with too little local
   space waits, as other disk holds do.

The trade-off: each save is written twice, once locally and once to the NAS.
In return, preemption saves finish at local-disk speed, and a stalled share
stops only the copy, not the command.

## Staged plan

**Stage 1: root identity and checked handle for today's reads (one release).**
Add the marker, the recorded ID and the checked handle. Route the existing
worker NAS operations through it: resume preparation, generation scans,
recovery and finalization reads, and expiry. Also route the queue's
`async_nas` and mirror publisher. An unverified root gives `storage_root`,
or an unknown generation where a scan was asked. It is never zero. Merge
the two conflicting `checkpointRootUnavailable` definitions into this
check. Commands still write directly in this stage.

- Laptop tests: preparation refuses a root folder swapped for an empty
  folder (nested-down), a different marker (replaced share) and a rename
  after the handle opened (vanished mount), with zero fallback writes. A
  remount with the same marker in a new folder passes. A cold start with
  `join --checkpoint-root` on an unmarked folder holds. A missing root
  during competing-copy recovery is unknown. The queue never rewrites a
  recorded marker. `TestInstalledWorkerLegacyLinuxNASCheckpoint` keeps
  passing.
- Fleet check: one Linux worker on its existing config, upgraded with a NAS
  checkpoint canary running. It resumes the canary from its accepted
  generation after a planned restart. Status shows the recorded root ID.
- Rollback: the previous release ignores the extra marker file in the root.

**Stage 2a: read side (one release).** Workers and the queue understand
accepted generation folders. They can stage, verify and expire them, and
they advertise `checkpoint-verified-copy-v1`. Nothing writes them yet, so
rolling back to the previous release loses nothing (MY-42).

- Laptop tests: staging a hand-made accepted folder, refusing a receipt
  mismatch, and `checkpoint_floor_missing` when the folder is gone.

**Stage 2b: write side (next release).** New attempts on capable workers use
C. The queue records each attempt's protocol. MY-55-adopted and old-binary
attempts stay on the legacy path, with main's rules and #2101, until they
end. The first C attempt of an existing job stages its legacy floor through
the checked handle (stage 1). From then on, its folders are generation
folders.

- Laptop tests (fake clock, `myce dev`): a mid-run root swap during a save
  leaves the generation local, the floor unchanged and the job resumable
  from the real save once the root returns. This is the #2101 interaction.
  The other tests cover: a queue restart with a pending copy replaying
  idempotently; a stale attempt's copy rejected; a custom writer with
  metadata past 1 MiB, accepted; a writer without notices, accepted at a
  stop event; an adopted legacy attempt that keeps reporting and hands over
  to a C attempt; helper `save()` blocking until acceptance; and local disk
  admission.
- Fleet check: a NAS canary on one Linux worker crosses the upgrade with an
  adopted legacy attempt. The next attempt runs on C. A preemption, then a
  resume on a second worker, restores the accepted generation by digest.
  On a non-production worker, with operator authorization, unmounting the
  share mid-run leaves saves local and the floor unchanged, and the copies
  land after remount.
- Rollback: back to 2a. C jobs still resume, because 2a reads their
  folders. Pending local copies stay in worker state, and 2b replays them
  when it returns.

**Stage 3: remove the legacy path.** This happens after a release in which no
legacy attempt is running and every worker is on 2b. Check that with status
before deleting (MY-42). The deletions are direct command writes to the
NAS, notice-only acceptance, `attempt-N` scanning for new jobs, and the
read-back helpers from #2096. Settled legacy job folders expire as today.

- Laptop: the quick gate, after removing the tests for removed paths and
  naming each one (MY-53).
- Rollback: back to 2b, which still has the legacy code.

## How this fits the shared-queue stages

- The accept transaction in step 5 is a normal queue write. At shared-queue
  stage 3 it moves to the PostgreSQL authority with the same fences. The
  handover number becomes one more fence.
- The worker already holds the bytes and the NAS copy, as the shared design
  requires. The queue moves no checkpoint bulk at any stage.
- `local_first` converges on the same copier. The shared design says that in
  shared mode, durable acceptance needs the NAS copy. Its queue file-store
  path stays only for fleets with no NAS root, where the queue remains the
  single queue (shared-design question 3).
- The marker gives shared-queue stage 2 the root identity it needs to
  qualify NAS file custody. A broken Mac mount then holds only that worker's
  copies and does not touch queue writes.
- None of this work is redone at cutover. Without C, B or A would each be
  rewritten there.

## Questions for the maintainer

1. When a worker's NAS is down, should resumable jobs still start there,
   saving locally until the NAS returns? Progress would become portable only
   then, and losing the machine first would lose that work. **Recommended:
   not yet.** Keep today's hold until stage 2b is proven. Then this becomes
   a small change that would help the Macs (#2083).
2. Commands that don't use Mycelium's save helper: is it fine that a save
   becomes safe on the NAS a few seconds after the command writes it,
   instead of at that moment? A power cut in those seconds loses only that
   save. Helper users keep today's guarantee. **Recommended: yes.**
