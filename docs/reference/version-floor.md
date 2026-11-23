# Current and previous release

[MY-42](../DECISIONS.md#my-42) limits release interoperation to the current
release and its immediate predecessor. Select the actual installed predecessor
and candidate by version and verified bytes. Use the
[release compatibility checks](../development.md#release-upgrade-compatibility).
The normalized-v3 database is current. The older readers and migration
procedures remain in historical revisions.

## Retained contracts

This inventory records the compatibility paths in source. Prove the current adjacent pair before removing a path.

| Contract | Why the supported pair still needs it |
| --- | --- |
| `keyed-attempt-v1`, exact job/attempt/session fences, explicit release acknowledgements | Both peers use these for rolling updates, process recovery and custody. Omission never releases an attempt. |
| Reply protocol advertisements and HTTP capability headers | Workers negotiating their first send omit additive health, probe, collection, memory and resource fields, then sends them after the current queue advertises support. Use the shared baseline shape until support is known. |
| `worker-disk-budget-v1`, `worker-disk-headroom-v1`, `worker-disk-trend-v1` | The r6.31, r6.33 and r6.34 queue tags advertise all three together. Current workers bootstrap with the baseline report and send the full disk report once all three are advertised. The shared retry after a strict refusal still sends the baseline report. The two partial disk report shapes were removed. |
| Receive-side missing-probe timing behavior | A legacy first check-in can omit probe validity. Removing this bootstrap receive behavior would break the adjacent pair even though both binaries implement adaptive probes. |
| Full tool-inventory bootstrap, hash/version/session acknowledgement | A newly connected worker and a restarted same-session worker must learn the queue's durable inventory fence. This is an ongoing synchronization protocol. |
| Per-machine `myce-normalized-state-v3`, compact records, field rows and pages | The normalized-v3 layout supports compatible reopen and rollback. Verify the exact pair. Normalized-v2, normalized-v1 and monolithic input is refused. |
| Fair-usage version 1, frozen bundle/file references, recipe references | Retained state holds these current representations. Startup verifies references and refuses embedded predecessor formats instead of importing them. Submission still freezes new authored inputs. |
| `Machine.Replacement` and historical-worker filtering | Retained state still contains superseded worker identities. Keeping their decode/filter representation prevents retired identities from reentering capacity. The conversion machinery is gone. |
| Previously adopted `legacy-state.json` attempt children, parked storage records, process identity and checkpoint generations | Retained state can still own exact process, result or last-copy custody. Reading and refusing unsafe reuse remains necessary; adoption of the old single-attempt root spool is removed. |
| Frozen extension isolation policy, priority aliases, optional preinstalled environments and absent hardware facts | These remain supported authored input or retained data. Missing telemetry remains unknown. |
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
Advance the checked pair with each release.
