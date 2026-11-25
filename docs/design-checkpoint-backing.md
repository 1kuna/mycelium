# Checkpoint backing: commands save locally, the worker copies to the NAS

Design for [#2100](https://github.com/1kuna/mycelium/issues/2100). **Draft,
revision 2. Do not merge until the orchestrator approves.** The
[file contract](files.md#resumable-jobs) and the
[shared-queue design](design-queue-beyond-one-host.md) stay in force.

Revision 2 answers the independent review of the first draft. The
disposition of each finding is on the PR. The main changes:

- The root identity is enrolled once, from evidence, with a crash-safe
  order. A witness file catches stale clones and restored shares.
- The NAS copy has one durable commit order, and replay tells the
  incomplete, unaccepted and accepted cases apart.
- Competing-copy recovery compares accepted custody only. Unknown stays
  unknown.
- The C runtime ships in 2a, before anything starts on it, so a rollback
  from 2b keeps serving running C attempts.
- The receipt binds to the generation parsed from the captured save. The
  Python helper gets a C mode that keeps the NAS save format.
- Output delivery is out of this rollout.
- A stage-0 native harness must pass before stage 1.

## Recommendation

Stop giving a running command a path on the NAS mount. A resumable command
saves into a local attempt folder on the worker. The worker captures each
finished save and copies it to the NAS through a directory handle it has
checked. It commits the copy with a receipt written last, verifies it, and
only then reports it. The queue accepts that report under the existing
attempt fences and records which bytes it accepted. Only accepted copies
raise the #2101 floor, certify a yield or win a recovery. Resume copies
those exact bytes back to local disk.

This is the shared-queue design's file custody rule ("stage or upload
immutable bytes in attempt-specific storage, verify the durable NAS copy,
then commit the accepted reference under the attempt check"), applied now
with today's single queue. It moves unchanged to the shared authority at
its stage 3.

`--checkpoint-root` still names the checkpoint folder on the mounted share.
Workers need no new setting.

## Decided defaults

The orchestrator recorded these on 2026-11-25 as technical defaults, not
product choices:

- **NAS-down starts.** New NAS-backed starts keep the existing
  `storage_root` hold. Explicit queue-backed `local_first` jobs stay
  eligible during an outage.
- **Custom saves.** Periodic saves from writers that don't wait for
  acceptance may be copied asynchronously. Status shows them as pending,
  with the measured lag. Final-save and yield guarantees stay as they are
  today.

Two things would be later product decisions for the maintainer, and this
design does neither: starting default NAS-backed jobs without portable
custody, and weakening the final-save guarantee.

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
failure. What remains is the worker's own NAS I/O. The worker controls that
I/O, can pin it to a handle, and can order it.

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
| 1. Nested share down, parent up | Caught at preparation. A mid-run save into the parent is caught only if the re-read happens before the share returns. | Safe: the worker never writes to the NAS. The queue's mirror needs the same marker check. | Safe: the marker is missing, so no copy is made. The save stays local and pending. | Caught when the folder is missing. A custom writer that creates folders is not caught. |
| 2. Share replaced at the same mountpoint | Caught by the marker at the checks. The command writes into the replacement between checks. | Safe on the worker. The queue mirror needs the marker. | Safe: wrong or missing marker means no copy. A copied marker is case 14 below. | Not caught if the replacement has the same layout. |
| 3. Mount vanishes between check and use | The command's own opens can't be pinned. | Safe | Safe: every worker operation goes through one checked handle. Stage 0 measures what a vanished mount returns on each platform. | Partly: an open after the folder is gone fails. |
| 4. Mid-run NAS loss, fallback save accepted, then blocks resume as the floor (#2101) | Needs the NAS read-back on every save (MY-43 cost, the 1 MiB issue). Races with remount. | Safe: acceptance needs the verified upload. | Safe for C attempts: only verified copies move the floor. Legacy attempts keep the hazard until the last one ends (case 24). | Not caught for writers that create folders. |
| 5. Queue restart while on a fallback | Needs the binding persisted in SQLite. | Safe: acceptance is durable. | Safe: the binding and acceptance are durable in SQLite. Pending copies replay from the worker's custody journal. | Not applicable |
| 6. Mixed-version rollout | Old workers keep today's behaviour until upgraded. | Same | The protocol is recorded per launch and in worker custody. A job with a C reference is placed only on capable workers. See the staged plan. | Same |
| 7. Healthy remount with new device or inode | Safe if identity is marker content, not device or inode. | Safe | Safe: identity is the marker's ID plus the witness. Device numbers are compared only within one open handle. | Safe |
| 8. Live takeover of an older binary's attempt (MY-55) | Has no binding for the adopted attempt. | The adopted attempt stays on its old path. | A legacy attempt stays legacy until it ends. Its floor is not inherited unverified (case 24). An adopted C attempt keeps its custody journal. | Same as today |
| 9. Missing root during competing-copy recovery | Needs "unknown, not zero" handling everywhere a scan runs. | Safe: compares accepted records. | C compares accepted records in SQLite, with no NAS scan. For legacy attempts, stage 1 stops turning a missing root into zero. Today `queue.go` maps `os.ErrNotExist` to durable 0, and #2101 did not change that. | Not addressed |
| 10. Custom writers with metadata past 1 MiB | Regresses unless the full save is streamed over the NAS. | Regresses: the capture limits are 1 MiB of JSON and 1,024 files. | Safe: the generation is parsed by streaming the captured local save, with no size cap. | Safe |
| 11. `join --checkpoint-root` cold start | Marker check | Safe | Safe: an unmarked or unenrolled root is unverified. | Not addressed |
| 12. Today's Linux layout, no extra config | Safe | Changes where bytes live. Bulk goes through the queue host. | Safe: same `--checkpoint-root`. | Safe |
| Bulk and capacity | NAS only | Every save crosses the queue host's disk, which hit 97% under MY-47. Against MY-40. | Local disk holds up to 3× a save per running attempt. The NAS holds the old and the incoming copy. See capacity. | NAS only |
| Fits the shared-queue stages | Rewritten at stage 3 | Rewritten at stage 3: shared mode accepts only after the NAS copy. | Is stage 3's custody rule | Not related |

C is the only option with no open case and no rework at the shared-queue
cutover. A leaves case 3 open by construction. B moves bulk the wrong way.
D depends on how writers behave.

## How C works

### 1. Root binding

The checkpoint root holds `.myce-checkpoint-root.json` with a random root
ID. The queue's SQLite records the same ID. That record is the binding.

**Enrollment happens once, from evidence.** The existing shard preparation
(`prepareCheckpointShards`) will create its folders and shard marker in any
existing directory. That includes a writable fallback under a missing nested
share. The shard marker is therefore not evidence, and shard preparation
never writes the root marker. Instead:

- **Automatic enrollment.** The queue opens the root with `os.OpenRoot`. It
  lists the jobs whose recorded folder should exist: not expired, and with
  positive accepted NAS progress. It enrolls only if that list is non-empty
  and every one of those folders exists through the handle. A fallback has
  none or a few of them. The real share has all of them. A floor that a
  legacy attempt published from a fallback can make this check fail. That
  falls back to operator enrollment, never to a wrong enrollment.
- **Operator enrollment** handles everything else: a new fleet, or a share
  missing folders for a known reason. `myce queue checkpoint-root enroll`
  prints the filesystem type, the mount source, and the expected and found
  job folders. It refuses a shortfall unless `--expect-missing N` names it
  exactly. The operations runbook gains this step. `myce dev` enrolls the
  local root it creates.
- Until enrollment, stage 1 behaves as today. Enrollment never blocks work
  that already runs without it.

**Crash-safe order.** Enrollment is resumable at every step:

1. Commit `{id, state: enrolling}` in SQLite.
2. Through the handle, create the marker with `O_CREATE|O_EXCL`, write it
   and sync it. Then sync the root directory.
3. Read the marker back through a fresh handle and compare it.
4. Commit `state: bound`.

After a crash in the `enrolling` state, the next step depends on what the
marker holds:

- **Missing, empty, or a prefix of the expected bytes.** It is the queue's
  own partial write. The queue rewrites it.
- **A different complete ID.** The queue refuses and reports to the
  operator.

Once the binding is `bound`, the queue never writes a marker again. A
missing or different marker is an outage (`storage_root`), never a repair.

**Witness.** A marker alone can't tell the live share from a clone or a
restore that carries the same marker. The root also holds
`.myce-checkpoint-witness.json` with `{root_id, seq}`. The queue advances
`seq` about once a minute, in the same background lane as shard
preparation and outside `q.mu`. The interval bounds how long a fresh clone
can pass, and it costs one small write a minute:

1. Write a temporary file, sync it, rename it into place (a file rename is
   safe on CIFS), and read it back.
2. Then commit `seq` in SQLite.

The next value is always `max(file, SQLite) + 1`, so a crash between the
two steps is harmless. Workers receive the committed `seq` in assignments
and check-in replies. They accept a handle only when the witness is at
least that value.

- A restored or cloned share made before the latest advance fails.
- A clone made after the latest advance passes until the next advance,
  about one minute.
- If the queue's own mount is broken, the witness stops advancing. Workers
  keep the last value, so a stalled witness never causes false refusals.

This defends against operator and mount mistakes. A deliberate clone that
keeps advancing in step is out of scope, and the fleet token is the
security boundary.

**Restored or cloned share.** Every worker refuses it as
`checkpoint_root_stale`, with the found and expected witness values, so
NAS-backed work holds with `storage_root`. If the restore is now the real
backing, the operator runs `myce queue checkpoint-root adopt-restored`
after inspecting it. That command:

- writes a new root ID and resets the witness;
- keeps every accepted reference as it is;
- leaves jobs whose accepted folder is missing to the existing
  `checkpoint_floor_missing` hold at their next preparation;
- leaves pending worker copies to land under the new ID.

**Protection of the namespace.**

- **No NAS path for C commands.** Under C a command has no NAS path at all.
  The only NAS writers are the worker's copier, the queue's binding and
  witness writes, and legacy attempts in their own `attempt-N` folders.
- **Isolation.** Where native isolation is enforced, a legacy command can
  write only its own attempt folder: Landlock on Linux and the sandbox
  profile on macOS both grant `MYCE_CHECKPOINT_DIR`.
- **Marker damage fails closed.** Without isolation, a legacy command could
  still overwrite the marker or the witness. That makes the root unverified
  and blocks copies. It never causes a false acceptance.
- **Symlinks are refused.** The marker and witness must be regular files,
  checked with `Lstat` through the handle.
- **Replaced folders are caught.** Generation folders carry a worker nonce
  in their name. Restore verifies them against the receipt digest recorded
  in SQLite, not against the receipt on the share. A replaced or forged
  folder fails verification and holds the job. It is never used silently.

### 2. Checked handle

Each worker NAS operation opens the root once with `os.OpenRoot`. Through
that handle it reads the marker and the witness. If both match, all copies,
verification, restores and cleanup in that operation go through the same
handle. The `async_nas` publisher already works this way
(`publishOutputDelivery`).

- **No fallback.** A handle opened on the real share never follows a later
  fallback.
- **No child mounts.** `os.Root` confines path resolution but still crosses
  into a child mount. So every directory the operation opens or creates
  must have the same device number as the root handle. That comparison is
  valid within one handle's lifetime. The remount problem only arises
  across handles. A child mount under the root (for example, a share
  mounted at a shard) is refused.
- **Vanished mounts.** Stage 0 measures whether the handle returns an error
  or stalls. A stall must stay contained to its copy lane (section 13).

### 3. Local save, protocol and custody

- **Protocol per launch.** The queue records each launch's protocol
  (`legacy` or `c1`) on the attempt.
- **Custody journal.** The worker writes the protocol, the root ID and every
  pending copy to a durable custody journal in its state volume. The
  journal survives a worker restart and MY-55 binary takeover. An adopted
  attempt is served under its recorded protocol, never under the new
  binary's default.
- **Local folder.** For `c1`, `MYCE_CHECKPOINT_DIR` is a managed attempt
  folder on the worker's state volume. The queue restores the accepted
  bytes there before launch. The atomic generation contract and the save
  format stay as they are, so working writers keep working.
- **Out-of-folder signals.** The notice file (`MYCE_CHECKPOINT_NOTIFY`) and
  the new acceptance file (`MYCE_CHECKPOINT_ACK`) live in the private work
  directory, not in the checkpoint folder. They never mix with saves.

### 4. Capture

A notice, the event scan at stop or exit, or a periodic local scan for
writers without notices triggers capture.

1. Capture walks the local attempt folder. It skips `.tmp` files and
   folders and refuses unsafe names and symlinks, as no-NAS capture does
   today.
2. It hard-links each file into a worker-owned capture folder on the same
   volume, or copies it where hard links are unavailable. A link keeps the
   old inode when the writer later replaces that save.
3. It parses the generation from the captured top-level JSON save or
   manifest. It streams the JSON tokens with bounded memory and no size
   cap. The generation is never taken from the notice. A writer that
   replaces the save between its notice and capture is therefore bound
   correctly, and metadata past 1 MiB is fine.
4. It checks stability as `captureCheckpoint` does: the generation before
   and after the walk must match, and the captured bytes must hash to
   what was walked.
5. A capture lower than or equal to the job's accepted generation is
   dropped.
6. If a newer save appears while an older one is still being copied, the
   older copy finishes or is abandoned. Only the newest capture is
   queued. At most one capture per attempt is in flight and one waits.

C capture does not use the file store's 1,024-file limit (`captureCheckpoint`
in `file_checkpoint.go`) or its 1 MiB JSON limit. The receipt lists every
file, with the existing manifest name rules.

### 5. Durable commit sequence on the NAS

The worker writes each capture under the job's recorded folder as
`SHARD/JOB/generations/G-A-N/`. G is the generation, A the attempt, and N
a random copy nonce. The nonce is written to the custody journal before
any NAS write.

1. **Exclusive staging ownership.** `Mkdir` of `G-A-N` through the handle.
   The nonce makes the name unique to this worker's custody. No other
   process creates it.
2. **Payload.** Each file is written with `O_CREATE|O_EXCL`, hashed while
   it is written, and synced (`writeDeliveryFile`).
3. **Receipt last.** The receipt holds the root ID, the job, attempt and
   session, the generation, the nonce, and each file's size and SHA-256.
   The worker writes it to `.receipt-N.tmp` and syncs it, then renames that
   file to `receipt.json` in the same folder. That publication is atomic. A
   file rename avoids the CIFS directory-rename problem in files.md.
4. **Directory and ancestor syncs.** Sync `G-A-N`, `generations`, `JOB`
   and `SHARD` (`syncDeliveryAncestors`). The Linux CIFS directory-fsync
   no-op applies as today.
5. **Verification.** Through the same handle, read `receipt.json` back and
   compare its identity hash. Then re-read and re-hash every payload file,
   refusing symlinks (`verifyDeliveryDirectory`). Stage 0 records whether
   that re-read reaches the server or the client cache on each platform.
   files.md already says server acknowledgements don't prove the NAS
   survives power loss.
6. Mark `verified` in the custody journal, then report.

This reuses the output-delivery helpers and its replay rule: an existing
destination is verified and never assumed (`output_delivery.go`, the
`Lstat` then `verifyDeliveryDirectory` branch). It uses receipt-last
instead of the directory rename that output delivery uses.

### 6. Acceptance and replay

The report carries a stable copy identity: job, attempt, session,
generation, nonce, receipt digest and root ID. It carries no path. The
queue derives the folder from the job's recorded checkpoint path. In one
SQLite transaction it checks:

- the root ID equals the binding;
- the exact attempt, machine and session fences. The attempt must be
  current, or it must be the ended attempt whose final save this is
  (section 8);
- that the generation is above the accepted one.

Then it records the accepted reference `{generation, attempt, nonce,
receipt digest, file list}` and raises the #2101 floor in the same
transaction. This is the only way a `c1` attempt's progress advances.
Plain notices show as local progress (`local_checkpoint_generation`) and
are never accepted.

**Replay** after a worker or queue crash, or after a lost reply:

| Worker finds | Meaning | Action |
| --- | --- | --- |
| Journal says staging; folder missing | Nothing durable yet | `Mkdir` again with the same nonce and continue. |
| Folder present, no `receipt.json` | Incomplete staging, owned by this nonce | Remove its contents through the handle, refusing symlinks, and rewrite. Only this custody writes that folder. |
| `receipt.json` present and equal to the journal's receipt | Complete, not yet known to be accepted | Verify the bytes, then send the report. |
| `receipt.json` present but different | Damage in an owned folder | Refuse. Report `checkpoint_copy_conflict`. Abandon the nonce, which settled expiry reclaims later. |

| Queue finds | Action |
| --- | --- |
| The same copy identity is already the accepted reference | Return the original acknowledgement. Nothing changes. This covers a lost reply. |
| The same generation is accepted with a different receipt digest | Reject with `checkpoint_generation_conflict` (the existing code). |
| A lower generation, or a failed fence | Reject as stale. The worker drops the capture from custody. |

After a positive answer, the worker records the acknowledgement in its
journal first. Then it writes the exact acceptance file for the command:
`{protocol, generation, name, sha256, size_bytes}` of the captured save,
written with a temporary file and rename, as `writeLocalFirstAck` does
today. A refusal writes `{state: refused, code}` for that generation.

### 7. Python helper and custom writers

The queue sets `MYCE_CHECKPOINT_ACK` for `c1` launches. It does not set
`MYCE_CHECKPOINT_PROTOCOL`, because the released helper rejects unknown
protocol values. An older helper therefore runs in its plain NAS mode
against the local folder and is handled as a custom writer. That makes it
asynchronous and visible, as decided.

The current helper sees `MYCE_CHECKPOINT_ACK` and uses C mode. C mode keeps
the NAS save format and naming:

- any plain `.json` name;
- `{"myce_checkpoint": {"generation": N}, "value": ...}`;
- the same atomic write and sync;
- no 1 MiB limit and no `generation.json` manifest.

These belong to `local_first` v1 only, and `local_first` stays unchanged
for queue-backed jobs.

`save()` writes locally, publishes the notice, and then waits for the exact
acceptance of that generation and those bytes. The wait ends in one of
three ways:

- **Accepted.** `save()` returns. The save is portable, which is today's
  helper guarantee on a healthy share.
- **Refused.** `save()` raises `CheckpointError` with the worker's code. A
  refused copy will never be accepted, so waiting would never end.
- **Stop requested.** `StopSignal` received TERM. `save()` returns with
  `pending_generation` set, and the command goes on to its final save.
  Waiting would only burn stop grace. Section 8 covers the final save.

`save(wait=False)` is the asynchronous periodic form. It returns once the
save is locally durable and sets `pending_generation`. `accepted_generation`
reports the last exact acknowledgement.

The wait has no timer of its own. The review flagged the deadline-free wait
in `local_first`. The C wait is the same as today's blocking direct write.
Its two real failure modes now end it: a refusal, and a stop that would eat
grace. While a copy is blocked, for example because the root is unverified,
status shows `checkpoint_copy_pending` with the cause and the lag. The job
stays visible and preemptible like any running job. Large saves are custom
writers in practice, so they take the asynchronous path. The Python helper
saves JSON values.

Custom writers keep the existing notice format or write none. Their saves
are captured from the notice or the periodic local scan, and copied
asynchronously.

### 8. Final save, release and yield

Final-save acceptance is separate from process release:

1. **Physical release.** When the process group has exited, the worker
   reports release at once. Devices, memory and CPU are free for
   higher-priority work. The pending copy holds only local disk, which is
   charged as custody (section 12).
2. **Final capture.** After exit, the worker scans the local folder once
   and captures the newest save. The final save is copied by the same
   pipeline, and acceptance is allowed for that ended attempt under its
   exact fences.
3. **Yield certification.** A cooperative yield, and the
   `FinalCheckpointGeneration` check, is certified only by an accepted
   receipt at or above the reported final generation. A pending copy never
   certifies a yield. Scarce-host yield victim selection uses the accepted
   generation, not `YieldGeneration` from local reports.
4. **Next attempt.** Until the final copy resolves, the next attempt of the
   job waits with hold `checkpoint_copy_pending`, naming the worker and the
   cause. This matches today's rule that a failed final save holds the job
   and preserves prior generations. The wait ends in one of three ways:
   - the copy is accepted;
   - the worker is lost under the existing loss rules, and its custody goes
     with it;
   - the owner runs `myce resume JOB_ID`, which abandons the pending copy
     and resumes from the accepted generation.

   There is no timer.

### 9. Resume

Preparation opens the checked handle and copies the accepted folder into
the new local attempt folder. It verifies each file against the file list
recorded in SQLite.

- **Bytes missing or different.** The existing `checkpoint_floor_missing`
  hold applies.
- **Root unverified.** The existing `storage_root` hold applies.

`c1` jobs don't scan `attempt-N` folders. The first `c1` attempt of a job
with legacy progress is a special case. It scans the legacy attempts once
through the checked handle, takes the best complete generation, and then
commits it through the normal copy and acceptance path as generation G
from attempt 0. After that, the job's progress lives only in generation
folders. If that scan finds less than the legacy floor, the result is
`checkpoint_floor_missing` (case 24).

### 10. Competing-copy recovery

Today a recoverable older execution reports its own local generation. The
queue compares it with a NAS scan of the current attempt and maps
`os.ErrNotExist` to durable 0 (`queue.go`, `processCheckinContext`). So a
missing root ranks the current execution at zero. #2101 filters reports
known to be below the floor, and nothing else.

For `c1`, the contract changes explicitly to favour accepted custody:

- **The candidate.** A recoverable execution's worker copies its newest
  save through the normal pipeline and reports it as a candidate. Its
  exact fences are checked against the recorded recoverable copy.
- **One decision transaction.** The queue accepts or rejects the candidate
  and decides recovery in the same SQLite transaction. The candidate wins
  only if its verified generation is above the current execution's
  accepted generation. A tie keeps the current execution, as today.
- **Directives after commit.** Stop directives for the loser come from the
  committed state, after the commit, as replies do today.
- **No local progress.** Nothing local or pending takes part in the
  decision. The loser's later copies are rejected as stale.
- **Unknown stays unknown.** If the candidate's root is unverified, or its
  copy is still in flight, the recoverable copy gets `Hold` (the existing
  reply field). No decision and no stop is made until it resolves.

Legacy attempts keep the scan until stage 3, but stage 1 changes how it is
read. A missing attempt folder under a verified handle is a known "no save".
An unverified root, or a missing root, is unknown and returns `Hold` through
the existing error path. It is never 0.

### 11. Retention and protected bytes

- **Superseded generations.** After a newer accepted reference commits,
  the queue instructs a worker with a verified root to move older
  generation folders of that job into the root's purge directory, as
  checkpoint expiry does. The accepted reference, any candidate under
  decision, and any folder named in a pending custody journal are never
  instructed.
- **Folders in use.** Restore only reads the accepted reference. A new
  reference can't commit during a restore, because the next attempt starts
  only after earlier copies resolve (section 8).
- **Orphans.** Abandoned nonces, stale rejects and conflicts are reclaimed
  by settled-job expiry, the initial cleanup policy. Status counts their
  bytes.
- **Settled expiry.** It is unchanged and moves the whole job folder.

### 12. Capacity and timing

Let S be the job's declared `disk.checkpoint_bytes`.

- **Local disk, per running `c1` attempt: 3 × S.** During an atomic
  replacement the old save and the writer's new temporary file coexist.
  A capture of an earlier generation may also still be linked while it is
  copied. The restored accepted bytes are the first "old save", so they
  are not charged again.
- **Local disk, per ended attempt with a pending copy: 1 × S.** This lasts
  until that copy resolves.
- **Admission.** It charges the worker's state volume with the sum of both.
  A worker short of space doesn't take new `c1` starts and waits like other
  disk holds. Pending custody is never evicted to make room.
- **NAS: 2 × S per job.** That covers the accepted copy plus the incoming
  copy until the old one is pruned. Orphans are extra and shown in status.

Timing, using 100 MiB/s as an example for a local disk and a NAS link (not
a measurement):

| Step | 30 GiB save | Notes |
| --- | --- | --- |
| Command writes locally | about 5 min | Local disk speed. Today the command writes over the NAS link. |
| Worker copies to the NAS | about 5 min | Off the command's path. The command keeps computing. |
| Worker re-reads to verify | about 5 min | Unless stage 0 finds the re-read is served from the client cache. |
| Save to acceptance | about 15 min | About 10 min after the local save completes. The previous accepted generation is the resume point until then. |

Size checkpoint intervals from this measured lag, as files.md already asks
for save duration. Status reports the lag per job.

### 13. Copy lane containment

Each checkpoint root has one copy lane: one goroutine at a time, no locks
shared with heartbeat, launch, stop or queue-reporting paths. Its progress
counters feed a stall cause, as output delivery's `LastProgressAt` does.

- **Single-flight.** While an operation is stuck, the lane starts no new
  NAS operation, so blocked threads can't pile up.
- **What keeps running.** Heartbeats, cancels, other jobs and queue writes
  continue.
- **Stage 0 decides.** If it shows macOS stalls instead of erroring, the
  guarantee becomes "the stall is contained" in place of "the error is
  prompt". If containment can't be shown, macOS stays on the legacy path
  until it can.

## Failure table for the review's cases

These add to cases 1–12 above. "Stage" is where each one is closed.

| # | Case | What C does | Stage |
| --- | --- | --- | --- |
| 13 | Writable fallback under a missing nested share at first enrollment | The automatic path needs every known job folder. The fallback lacks them, so enrollment waits for the operator. Shard preparation never writes the root marker. | 1 |
| 14 | Replacement or clone carrying a copied marker | The witness is below the committed `seq`, giving `checkpoint_root_stale`. A clone made after the last advance passes for at most one advance interval. | 1 |
| 15 | Restored share | It is refused as stale. Operator `adopt-restored` re-binds it. Missing accepted folders get `checkpoint_floor_missing` per job. | 1 |
| 16 | Child mount under the root | A device mismatch within the handle is refused. | 1 |
| 17 | Crash during enrollment | The `enrolling` state resumes. The queue's own partial marker is rewritten. A foreign marker is refused. | 1 |
| 18 | Crash part-way through a copy | Replay table: an owned incomplete folder is rewritten, a complete one is verified and reported. | 2a |
| 19 | Acceptance committed, reply lost | The same copy identity returns the original acknowledgement. | 2a |
| 20 | Same generation, different bytes | `checkpoint_generation_conflict`. The accepted reference is unchanged. | 2a |
| 21 | Cleanup removes referenced bytes | Pruning only follows a newer commit and skips accepted, candidate and journalled folders. Orphans wait for settled expiry. | 2a |
| 22 | Missing root during legacy recovery | Unknown, so `Hold`, never 0. | 1 |
| 23 | Recoverable copy with only local progress | It must report a verified candidate. The decision and the acceptance are one transaction. Pending progress never wins. | 2a |
| 24 | MY-55-adopted legacy attempt publishes a fallback generation, then a C successor follows | It is still an open hazard while legacy attempts run. The C successor doesn't inherit the floor unverified. It scans legacy bytes through the handle and holds with `checkpoint_floor_missing` if they fall short. The hold is visible and owner-recoverable, the #2101 trade-off the orchestrator accepted. Closed when the last legacy attempt ends. | 2b–3 |
| 25 | Rollback from 2b to 2a while a C helper waits | 2a has the full copier, acceptance and acknowledgement, so the adopted attempt keeps being served. | 2a |
| 26 | Rollback below 2a | Allowed only when status shows no `c1` attempt in custody and no non-terminal job with a C reference. In 2a such a job can run its next attempt as legacy: preparation restores the accepted bytes into the legacy attempt folder. | 2a |
| 27 | Custom writer replaces its save between notice and capture | The receipt binds to the generation parsed from the captured bytes. | 2a |
| 28 | Metadata past 1 MiB, or more than 1,024 files | Streamed parse. C capture has no file-count limit. | 2a |
| 29 | Released Python helper, or `local_first` limits | An older helper runs as a custom writer. The current helper's C mode keeps the NAS format. `local_first` is unchanged. | 2a/2b |
| 30 | Final save still copying at stop | Release is reported at exit. The yield is certified only when accepted. The next attempt waits with a visible `checkpoint_copy_pending`. | 2a |
| 31 | `async_nas` output root with no shard marker and path-hash destination IDs | Untouched. Output delivery is outside this rollout. | — |
| 32 | Local disk full | 3 × S per running attempt plus S per pending copy, charged at admission. Custody is never evicted. | 2a |
| 33 | Shared-queue handover mid-acceptance | The stable copy identity deduplicates the retry. The accept transaction checks the coordinator turn. | Shared stage 3 |
| 34 | macOS mount stalls instead of erroring | The lane is contained. If stage 0 can't show containment, macOS stays legacy. | 0 |
| 35 | A job reads another job's checkpoint folder as a NAS input | It sees accepted, pending and orphan folders. The docs point it to the accepted reference in status, or to declared outputs. | 2b docs |

## Staged plan

Each stage is one release, under the adjacent-release rule (MY-35, MY-42).

### Stage 0: native verification gate (before stage 1)

This is a standalone harness, with no queue and no worker changes. It runs
on one non-production Linux CIFS worker and one non-production macOS SMB
worker, against disposable shares that the storage owner provisions. It
opens a root through `os.OpenRoot`, checks a marker and witness, and runs
copy, sync, rename and verify operations. Cases:

1. Healthy remount: the marker and bytes match even though the device or
   inode changed.
2. A different, unmarked export at the same mountpoint.
3. A stale clone carrying the same marker and an old witness.
4. Loss between check and use. On Linux, `umount -l`. On macOS, a busy
   unmount, then a forced unmount of the disposable mount.
5. A connection interrupted while I/O is in flight.
6. Whether a verification re-read reaches the server.
7. Whether the device number stays the same for every directory within one
   open handle, and whether a child mount under the root changes it.

For every operation, record the errno and the timing, with a watchdog that
records any stall. If macOS stalls instead of erroring, run case 4 again
with the lane design. Heartbeats, cancels, other work and SQLite writes in
the same process must keep their measured cadence while the lane is stuck.
If they can't, macOS waits for stage 1.

### Stage 1: root binding and checked handle for today's reads

- **Binding.** Add enrollment (automatic and operator), the crash-safe
  binding, the witness, and status for the binding and witness. Routing
  happens only once the binding is `bound`. Before that, behaviour is
  today's.
- **Routing.** Route the worker's existing NAS reads through the checked
  handle: resume preparation, generation scans, recovery and finalization
  reads, and expiry. An unverified root gives `storage_root`, or unknown
  where a scan was asked. It is never 0.
- **Recovery.** Change the competing-copy scan so `os.ErrNotExist` from an
  unverified or missing root is unknown.
- **Unchanged.** Commands still write directly. Output delivery and
  `local_first` mirrors are untouched.
- **Laptop tests.** Enrollment refuses a fallback folder that lacks the
  known job folders. Operator enrollment with `--expect-missing`. Crash at
  each enrollment step. The queue never rewrites a bound marker. A stale
  witness is refused. Remount with the same marker in a new folder passes.
  A child mount at a shard is refused. Preparation refuses a root swapped
  for an empty folder, a different marker, or a rename after the handle
  opened, with zero fallback writes. A cold start with
  `join --checkpoint-root` on an unenrolled folder holds. A missing root
  during competing-copy recovery gives `Hold`.
  `TestInstalledWorkerLegacyLinuxNASCheckpoint` keeps passing.
- **Fleet check.** One Linux worker on its existing config, upgraded with a
  NAS checkpoint canary running. It resumes the canary after a planned
  restart. Status shows the binding and the witness advancing.
- **Rollback.** The previous release ignores the marker, the witness and
  the binding record.

### Stage 2a: full C runtime, no new C starts

- **Workers.** They gain capture, the custody journal, the copy lane, the
  commit sequence, verification, replay, the acknowledgement file,
  candidate reports, `c1` restore and pruning. They advertise
  `checkpoint-verified-copy-v1`.
- **Queue.** It gains acceptance with replay, accepted references, the
  per-launch protocol, the recovery transaction, yield certification from
  receipts, `checkpoint_copy_pending`, local disk admission and prune
  instructions. A job with a C reference is placed only on capable workers.
- **Python helper.** It ships its C mode.
- **No `c1` launches.** The queue launches nothing as `c1`. A job that
  already has a C reference, from a 2b that was rolled back, runs its next
  attempt as legacy. Preparation restores the accepted bytes into the
  legacy attempt folder, and the floor stays.
- **Serving rolled-back work.** It serves any adopted `c1` attempt to
  completion, including its final copy. That is the supported rollback for
  active C publishers.
- **Laptop tests** (fake clock, `myce dev`):
  - staging a hand-made accepted folder;
  - a receipt mismatch refused;
  - every row of the two replay tables, with a crash injected before each
    step;
  - a lost reply returning the original acknowledgement;
  - a conflict refused;
  - recovery with a verified candidate, with a pending candidate (`Hold`),
    and a tie;
  - a final copy pending after release, with devices freed, the yield not
    certified and the next attempt held;
  - a prune that skips referenced folders;
  - local disk admission at 3 × S;
  - an adopted `c1` attempt served after a downgrade from 2b;
  - a helper wait that ends on refusal and on stop;
  - a released helper treated as a custom writer.
- **Fleet check.** Upgrade with a running NAS canary and confirm nothing
  changes for legacy attempts.
- **Rollback to 1.** Only when status shows no `c1` custody and no
  non-terminal job with a C reference.

### Stage 2b: new starts on C

- **New starts.** New NAS-backed resumable attempts on capable workers
  launch as `c1`. MY-55-adopted and old-binary attempts stay legacy until
  they end. The first `c1` attempt of a job with legacy bytes converts
  them (section 9).
- **Consumer docs ship in this release** (below).
- **Laptop tests** (fake clock, `myce dev`):
  - a mid-run root swap during a save leaves the generation local, the
    floor unchanged and the job resumable from the real save after the
    root returns (the #2101 interaction);
  - a stale attempt's copy rejected;
  - a custom writer with metadata past 1 MiB, accepted;
  - a writer without notices, accepted at exit;
  - an adopted legacy attempt with a false floor, handed to `c1`, which
    holds with `checkpoint_floor_missing`;
  - helper `save()` blocking until acceptance.
- **Fleet check.**
  - A NAS canary on one Linux worker crosses the upgrade with an adopted
    legacy attempt, and the next attempt runs as `c1`.
  - A preemption, then a resume on a second worker, restores the accepted
    generation by digest.
  - On a non-production worker, with operator authorization, unmounting the
    share mid-run leaves saves local and the floor unchanged. The copies
    land after remount.
  - macOS joins only if stage 0 passed for macOS.
- **Rollback to 2a.** Running `c1` attempts keep being served. New starts
  go back to legacy, and C-referenced jobs run legacy with a restore.

### Stage 3: remove the legacy path

The preconditions, checked with status before deleting anything (MY-42):

- every worker is on 2b or later;
- no legacy attempt is running or in custody;
- no non-terminal job, including paused and queued ones, depends on
  legacy bytes without a C reference.

Before the release, the queue converts the remaining paused and queued
legacy-only jobs through the section 9 path. Jobs whose conversion fails
keep the legacy reader until an owner resolves their hold, and stage 3
waits for them.

The deletions:

- direct command writes to the NAS;
- notice-only acceptance;
- `attempt-N` scanning;
- the legacy competing scan.

Settled legacy job folders expire as today.

- **Laptop.** The quick gate, after removing the tests for removed paths
  and naming each one (MY-53).
- **Rollback.** Back to 2b, which still has the legacy code.

## Shared-queue fit

- **Replicated records.** The binding (root ID, state, witness `seq`), the
  accepted references, the per-launch protocol and the candidates are
  authoritative queue records. They travel in the changed-row stream and
  the NAS snapshot like other job state. A new coordinator applies them
  before it serves decisions.
- **Fenced writes.** The accept transaction, the recovery transaction and
  witness advances are ordinary queue writes. At shared stage 3 they also
  check the coordinator turn and handover number in the same transaction.
  The witness is advanced by whichever machine holds the turn.
- **Stable report identity.** The copy identity (job, attempt, session,
  generation, nonce, receipt digest) is the stable operation key. A report
  retried across a handover is looked up, not applied twice. Worker custody
  journals don't depend on which machine coordinates.
- **No queue bulk.** The queue moves no checkpoint bulk at any stage. The
  shared design says shared-mode acceptance needs the NAS copy. This is
  that rule.
- **Contained mounts.** A broken worker mount holds only that worker's
  copies. It doesn't touch queue writes, which shared stage 2 must verify.

## Consumer docs in 2b

These ship in the 2b change, as AGENTS.md requires for consumer-visible
changes:

- **`skills/mycelium/SKILL.md`.**
  - Local versus accepted progress, and that only accepted progress
    resumes elsewhere or certifies a yield.
  - The hold instruction changes from "restore a complete NAS checkpoint at
    or above the generation" to "the exact accepted bytes, or a supported
    repair". Supported repairs are `myce resume JOB_ID` from the last
    accepted generation, or operator `adopt-restored`.
  - `checkpoint_copy_pending` and what to do about it.
  - Do not read checkpoint folders as NAS inputs: use declared outputs, or
    the accepted reference path in `myce status --json`.
- **`docs/files.md`.**
  - The local attempt folder, capture rules and acceptance, the generation
    folder layout and receipts.
  - Disk sizing: local 3 × S plus S per pending copy, NAS 2 × S.
  - Copy lag and checkpoint intervals.
  - Stop grace: the final save must be locally durable within grace, and
    its copy happens after release.
  - Helper compatibility, and NAS inputs that point into checkpoint folders.
- **`clients/python/README.md`.**
  - C mode, and `save(wait=False)`.
  - `pending_generation` and `accepted_generation`.
  - Refusal errors.
  - That released helper versions keep working as asynchronous writers.

## Out of scope for this rollout

- **`async_nas` output delivery.** It uses its own `output-delivery-root`,
  which has no shard marker. Its pending deliveries carry a path-hash
  `DestinationID` (`deliveryRootID`). Changing that would fail existing
  pending deliveries as "destination configuration changed". It stays as
  it is.
- **`local_first` checkpoint mirrors.** They publish from the queue to its
  own root path with the same path-hash ID. The same fallback hazard
  applies on the queue host. A follow-up issue can add the binding check
  as an extra precondition there, with no change to the ID, after stage 1
  proves the binding. Converging `local_first` onto the worker copier
  waits for shared stage 3, where queue-file-store custody stays only for
  fleets without a NAS.
