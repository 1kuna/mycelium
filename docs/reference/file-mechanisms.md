# File mechanisms

Part of the [approved design](../design-proposal.md), moved verbatim apart
from relative links. Product contracts and maintainer rulings are unchanged.

### Durable asynchronous NAS output delivery
<a id="opt-in-durable-asynchronous-nas-output-delivery-349-2026-10-29"></a>

Tasks may explicitly select `output_delivery: async_nas` with declared outputs
and a positive `disk.output_bytes` estimate. Existing tasks retain their result
semantics. The queue's `--output-delivery-root` selects the NAS destination
independently of checkpoint mode; SQLite and the queue file store stay on durable
local storage. This selects output delivery only; local-first checkpoints require the separate explicit checkpoint opt-in above.

After payload exit, the worker retains an immutable synced local output spool
until exact durable queue acceptance. Queue admission reserves estimated output
bytes, exchanges them for the current attempt's exact missing manifest, and
preserves physical-space/quota backpressure. Queue success atomically records
verified file-store outputs and a pending delivery. `output_refs` retrieval and
compute completion never wait for NAS publication. Consumers needing NAS files
explicitly wait for verified delivery and the accepted execution identity.

One queue-owned background publisher stages complete immutable job/attempt
paths, verifies bytes and an identity/policy/manifest receipt, syncs and renames.
A pending NAS syscall retains its slot. Returned failures retry durably using the
injected Clock. Restart/lost-ack replay compares receipts and content; conflicts
and stale executions cannot claim verified delivery. Pending/failed delivery pins
blobs beyond ordinary expiry, and status exposes state, bytes, age, attempts and
cause. Verified delivery preserves ordinary retrieval retention; no early deletion,
rollback, new storage service or publisher election is introduced. Details and
supported forms: [file guide](../files.md#complete-from-queue-storage-deliver-to-nas-later-349).

