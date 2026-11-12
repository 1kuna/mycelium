# Current and previous release

[MY-42](../DECISIONS.md#my-42) limits release interoperation to the current
release and its immediate predecessor. For candidate r6.11 that pair is
r6.10 ↔ r6.11; the live predecessor is pinned at
`302620cacaace8a16e7ed424e1183dbb10f46414`.
On 2026-11-11 read-only fleet status showed the queue and every active worker on
r6.10, and r6.10 writes only the normalized-v3 database marker. The r6.8 rollback
converter and the normalized-v2 reader were removed on that evidence.

## Retained contracts

Rows that still name r6.8 describe first-send protocol behaviour that this
cleanup kept but did not re-verify against r6.10; prove each is unused by r6.10
before removing it.

| Contract | Why the supported pair still needs it |
| --- | --- |
| `keyed-attempt-v1`, exact job/attempt/session fences, explicit release acknowledgements | Both releases use these for rolling updates, process recovery and custody. Omission never releases an attempt. |
| Reply protocol advertisements and HTTP capability headers | r6.8 negotiates its first send. It initially omits additive health, probe, collection, memory and resource fields, then sends them after the current queue advertises support. New workers send the shared r6.8 shape directly. |
| `worker-disk-budget-v1`/`worker-disk-headroom-v1` advertisements and narrow projection/retry | Disk headroom fields were added after r6.10. New workers omit them until the queue advertises support, and after a strict rollback refusal retry once without the additive fields; baseline collection and local evidence stay intact. |
| Receive-side missing-probe timing behavior | r6.8's first check-in deliberately strips probe validity. Removing this bootstrap receive behavior would break the adjacent pair even though both binaries implement adaptive probes. |
| Full tool-inventory bootstrap, hash/version/session acknowledgement | An r6.8 worker and a restarted same-session worker must learn the queue's durable inventory fence. This is an ongoing synchronization protocol. |
| Per-machine `myce-normalized-state-v3`, compact records, field rows and pages | r6.10 and the candidate share this layout, so upgrade and rollback are plain restarts. `testdata/r6.10-queue.sql`, written by exact r6.10 source, guards it. Normalized-v2, normalized-v1 and monolithic input is refused. |
| Fair-usage version 1, frozen bundle/file references, recipe references | Retained state carried into r6.10 holds these current representations. Startup verifies references and refuses embedded predecessor formats instead of importing them. Submission still freezes new authored inputs. |
| `Machine.Replacement` and historical-worker filtering | Retained state carried into r6.10 still contains superseded worker identities. Keeping their decode/filter representation prevents retired identities from reentering capacity. The conversion machinery is gone. |
| Previously adopted `legacy-state.json` attempt children, parked storage records, process identity and checkpoint generations | Retained state carried into r6.10 can still own exact process, result or last-copy custody in retained state. Reading and refusing unsafe reuse remains necessary; adoption of the old single-attempt root spool is removed. |
| Frozen extension isolation policy, priority aliases, optional preinstalled environments and absent hardware facts | These remain supported authored input or retained data carried into r6.10, rather than release migration paths. Missing telemetry remains unknown. |
| Current install transactions and preservation | Interrupted current upgrades, address repair, verified binaries, service identity and operator settings still require these transactions. |
| Batch-v1 waits, scalar deadline metadata, terminal payload fetches and pressure recovery | Both releases implement batch waits. Clients no longer poll per job when an obsolete queue lacks the endpoint. Diagnostic reads and terminal retrieval retain their current roles. |

## Removed runtime paths

The pre-keyed attempt protocol, omission-as-release behavior, below-floor request-shape
stripping retries and queue capability negotiation, normalized-v2, normalized-v1 and
monolithic persistence readers, the r6.9-to-r6.8 `myce queue downgrade` converter, startup fair-usage/cancellation/bundle importers,
split-worker conversion and rollback, retired service-config overlays/default
rewrites, and root single-attempt spool adoption are removed with their migration
fixtures and procedures. Tagged source history retains earlier implementations.
Older state is refused without importing or discarding its custody.

The upgrade harness covers only this adjacent pair: old worker/new queue,
new worker/old queue, current retained-state/configuration round trips and
candidate v3 → exact previous reopen → candidate reopen.
Rollback preserves all retained IDs, decisions, inventory and pending custody.
See the [queue-only rollback procedure](../operations.md#queue-only-rollback-to-the-previous-release).
The pair advances together at the next release; do not add older baselines back.
