# Current and previous release

[MY-42](../DECISIONS.md#my-42) limits release interoperation to the current
release and its immediate predecessor. For r6.8 that pair is r6.7 ↔ r6.8;
r6.7 is pinned at `f010f15abc5483c527f4226cbf298966a35f2b33`.
The operator confirmed all active installations at that predecessor, unified
machine workers everywhere, and the current production database marker before
runtime deletion. That confirmation uses retained release verification plus the
operator's confirmation that no installation followed it. No deployment is part
of this cleanup.

## Retained contracts

| Contract | Why the supported pair still needs it |
| --- | --- |
| `keyed-attempt-v1`, exact job/attempt/session fences, explicit release acknowledgements | Both releases use these for rolling updates, process recovery and custody. Omission never releases an attempt. |
| Reply protocol advertisements and HTTP capability headers | r6.7 negotiates its first send. It initially omits additive health, probe, collection, memory and resource fields, then sends them after the current queue advertises support. New workers send the shared r6.7 shape directly. |
| `worker-disk-budget-v1` advertisement and narrow projection/retry | Disk-budget telemetry was added after r6.7. New workers omit only these fields until advertised and retry once without them after a strict downgrade refusal; baseline collection and local evidence stay intact. |
| Receive-side missing-probe timing behavior | r6.7's first check-in deliberately strips probe validity. Removing this bootstrap receive behavior would break the adjacent pair even though both binaries implement adaptive probes. |
| Full tool-inventory bootstrap, hash/version/session acknowledgement | An r6.7 worker and a restarted same-session worker must learn the queue's durable inventory fence. This is an ongoing synchronization protocol. |
| `myce-normalized-state-v2`, per-machine `myce-normalized-state-v3`, compact records, field rows and pages | r6.7 persists v2. Current persistence work writes machines individually in v3, so its atomic v2-to-v3 transition remains within the supported floor. Monolithic and normalized-v1 input is refused. |
| Fair-usage version 1, frozen bundle/file references, recipe references | r6.7 persists these current representations. Startup verifies references and refuses embedded predecessor formats instead of importing them. Submission still freezes new authored inputs. |
| `Machine.Replacement` and historical-worker filtering | r6.7 retained state contains superseded worker identities. Keeping their decode/filter representation prevents retired identities from reentering capacity. The conversion machinery is gone. |
| Previously adopted `legacy-state.json` attempt children, parked storage records, Docker/process identity and checkpoint generations | r6.7 can still own exact process, result or last-copy custody in retained state. Reading and refusing unsafe reuse remains necessary; adoption of the old single-attempt root spool is removed. |
| Frozen extension isolation policy, priority aliases, optional preinstalled environments and absent hardware facts | These remain supported authored input or retained r6.7 data, rather than release migration paths. Missing telemetry remains unknown. |
| Current install transactions and preservation | Interrupted current upgrades, address repair, verified binaries, service identity and operator settings still require these transactions. |
| Batch-v1 waits, scalar deadline metadata, terminal payload fetches and pressure recovery | Both releases implement batch waits. Clients no longer poll per job when an obsolete queue lacks the endpoint. Diagnostic reads and terminal retrieval retain their current roles. |

## Removed runtime paths

The pre-keyed attempt protocol, omission-as-release behavior, below-floor request-shape
stripping retries and queue capability negotiation, normalized-v1 and
monolithic persistence readers, startup fair-usage/cancellation/bundle importers,
split-worker conversion and rollback, retired service-config overlays/default
rewrites, and root single-attempt spool adoption are removed with their migration
fixtures and procedures. Tagged source history retains earlier implementations.
Older state is refused without importing or discarding its custody.

The upgrade harness covers only this adjacent pair: old worker/new queue,
new worker/old queue and current retained-state/configuration round trips.
The pair advances together at the next release; do not add older baselines back.
