# Worker-local task outputs (#508)

New, individually submitted destination tasks can opt into the versioned result
contract below. This requires the protected storage protocol from #506/#516 and
a worker advertising `task_outputs_v1`. Native isolation and activation remain
separate qualification work; configuring a destination does not qualify storage.
Legacy `output_delivery: async_nas`, frozen jobs and keyed replays retain their
existing contract and recovery window.

```yaml
storage_protocol: worker-custody/1
output_destination:
  protocol: task-output/1
  kind: nas
  id: archive
outputs:
  result.txt: results
# Default for this new contract; retained preserves the configured recovery window.
output_retention: until_delivered
```

Unique outputs stay in the producing worker's immutable, synced attempt spool
even when the NAS is healthy. No health probe, full local budget or destination
failure permits shared-store staging. The installed attempt boundary funds work,
spool duplication, checkpoints and metadata together. A full worker budget keeps
new writers from starting there; compute completion alone never frees it.

The queue records the exact execution, machine, session, allocation and manifest
in SQLite. Compute can become `succeeded` after confirmed process cleanup while
`local_outputs.state` is `worker_local_pending_delivery`. Ordinary `output_refs`
are absent. Inspect `delivery` independently: it identifies the destination,
manifest, pending/failed/publishing/verified state, bytes, attempts, progress and
retry times. Workers retain custody across restart and uncertain acknowledgements.
A lost worker remains the recorded custodian and raises the existing health
alert `worker_output_custodian_unavailable`; its output is not reported durable.

## NAS destination

The stable destination ID maps to an operator-owned root and its native volume
identity on each worker. With the intended destination mounted, generate a
private local configuration:

```sh
myce file destination-identity --destination-id archive /srv/archive > destinations.json
myce join --output-destinations-config destinations.json # plus normal join flags
```

Keep the configuration private: it contains that worker's local path. Jobs carry
only the stable ID. Copy opens the configured root and checks the actual opened
filesystem identity before publication, so a missing mount cannot silently use
the worker's underlying disk. A NAS task also requires its destination ID in the
worker's advertised `output_destination_ids`.

The worker stages, verifies and syncs each complete prefix tree before publishing
`ROOT/PREFIX/JOB/ATTEMPT`, with `.myce-delivery.json`. Partial publication remains
pending. Retry verifies both existing receipts and every file before accepting
the whole task. One worker publication/verification runs at a time; destination
I/O never holds the queue lock. Failed publication retains custody and uses the
existing retry delay, resuming when the destination returns.

## Submitter destination

Use `kind: submitter` and a stable submitter-owned destination ID. Pull from the
submitter machine with either supported client:

```sh
myce file deliver JOB /srv/results
```

```python
path = client.deliver_outputs(job_id, "/srv/results")
```

The explicit local path stays on the submitter. Queue HTTP relays a single
backpressured stream at a time, without disk staging. The client verifies hashes,
syncs the complete tree and receipt, atomically exposes `ROOT/JOB/ATTEMPT`, then
acknowledges delivery. Retry the same root after a lost acknowledgement: an
existing tree must verify exactly. Individual `download_result` calls and partial
pulls never acknowledge whole-task delivery. After worker collection, further
streams return typed `result_expired`; keep the delivered submitter copy.

When collection is due, repeat `deliver` / `deliver_outputs` from the same root.
The client verifies the tree again and streams its current bytes back over a
bounded HTTP connection. The worker hashes that live stream immediately before
collection. This extra verification transfer is necessary because the worker
cannot reach a private submitter path. An absent submitter or interrupted stream
retains charged worker custody; a saved receipt cannot authorize deletion.

## Retention and collection

`until_delivered` enrolls verified outputs in #479's durable expiry/ledger and
collector automatically. Collection of delivered local copies is lazy: it starts
only when a qualified queued task needs the same worker backing, or at the
configured retention deadline (168 hours by default). Copies are selected in
MY-18 order, least valuable owner tier first. `retained` also requires the
configured recovery window or explicit owner release before becoming legally
reclaimable; release alone does not trigger eager collection. Pending or failed
delivery cannot expire unique results.

Verification alone does not authorize immediate deletion: collection rechecks
exact attempt/destination/manifest fences, confirmed compute release, six-hour
local publication grace, downstream/shared references and checkpoint/recovery
pins. Registry/environment assets, unknown imports and required diagnostics keep
their existing protections. Expired results cannot acquire new references via
the expired task; acquiring an independent protected reference still uses normal
capacity admission.

Every collection invocation, including saved-intent replay and SQLite restart,
re-verifies current destination content outside the queue lock, then rechecks
the live attempt and reference fences before unlinking. The accepted residual is
that single operation's own verify-to-delete span: after verified delivery, the
destination is authoritative and its NAS/backups own subsequent durability. No
verify-then-delete across independent filesystems can prevent destination loss
immediately afterward. See [operations](operations.md#worker-local-task-delivery-508).
Missing, corrupt or unreachable destination data, or a changed collection fence,
clears verified delivery, retains worker custody,
and raises `output_destination_preservation_lost` to operator/project owners.
Delivery retry/reconciliation follows the existing path; conflicting destination
trees remain protected from overwrite until repaired. A historical receipt is
never a substitute for the payload.
A missing submitter destination can be restored with `deliver_outputs` from
retained worker bytes even after ordinary result expiry, until actual collection.
Ordinary expired-result downloads still return `result_expired`.
Failed verification advances a durable verification round, so replaying an old
success receipt cannot restore verified state or clear that alert after restart.

After a worker restart, successful compute with reconciled queue custody no
longer gates compute recovery/admission. Delivery actors retain their original
session, saved spool, allocation charge and bounded I/O slot independently;
production using independent backing can be assigned during destination outage,
publication grace or retained recovery.

Worker collection persists an intent before deleting its authorized attempt
work/spool and calls the storage backend's idempotent
`StorageAttemptCollector.Collect` to retire that exact bound. The backend must
preserve active control state and collection intent through acknowledgement and
independently fund required diagnostics. Missing support or interrupted retirement
keeps the allocation charged. Only the exact collection acknowledgement removes
the queue's allocation/custody charge; the delivery and collection ledger survives.
Shared copies use #479's publication fences, reference graph and bounded batches.

Laptop tests use fake clocks, small files, real HTTP and SQLite; they do not
qualify native mounts, isolated backing or aggregate filesystem denial. Placement
requires the delivery capability and matching NAS destination ID before assigning
an attempt. Delivery state and sanitized failures feed #507's production storage
alerts; collector ordering recognizes worker-local manifests and respects the
existing shadow/activation latch. #509 owns integrated
migration and activation. No cap/reserve retirement is implied by this contract.
