# Adapter mechanisms

Part of the [approved design](../design-proposal.md), moved verbatim apart
from relative links. Product contracts and maintainer rulings are unchanged.

### Migration: one-shot converter, fix forward

Choose a **one-shot source converter**, not runtime compatibility readers for a
deprecation window. MY-03 favors one current runtime; two permanent readers
retain competing concepts and validation drift. Cost: one reviewed source/caller
change per maintained consumer. Queued jobs, keys, frozen plans, output refs,
checkpoints and exact custody remain unchanged. Retire old source-authoring
entry points, not the internal decoder needed by saved bundles. Today preparation
calls `extensionFromFiles` and execution calls `ReadExtension` on frozen
`extension.yaml`; keep those paths (including the exact legacy/composed schemas,
bytes and digest verification) for persisted job/registry snapshots. New adapter
entries lower into the same execution representation. Internal snapshot decoding
cannot accept a mutable author path or register a new old-format file. It remains
while any retained snapshot needs it; consumer conversion alone cannot justify
its deletion. This is decoding durable v3 intent, not an old runtime fallback.
No accepted identity or digest is rewritten and no rollback layer is added.

`myce adapter convert` reads explicitly selected profiles/extensions and emits
one adapter plus field/caller-change report without overwriting its sources. An
explicit apply publishes the reviewed output. Original sources stay in Git
history. Refuse conflicting names, missing bytes and unrepresentable fields;
never guess targets, references, outputs or locks. Preserve catalog names;
copying their exact recipe bytes into a project requires the project-supplied
environment option. Never invent a lock for installed tools.

Profiles retain source, argv appending, env, project/class, targets/allow-lists,
resources, memory comparability, common/item input identity, outputs/diagnostics
and notifier recovery. Their `check` becomes `checks.result`. Extensions retain
complete variants, setup/assets, named environments, sidecar caps/readiness,
work types, baselines, references/sweeps and resumability; their `check` becomes
`checks.qualification`. Changed frozen digests require fresh native qualification,
not a relabelled old pass. Observations remain historical and reusable only when
their complete comparability identity still matches.

Stage converter/new reader first, then convert maintained callers and remove old
YAML authoring readers/init paths in the cutover batch. This is implementation
staging, not a supported dual-format window or automatic fallback. Retirement
requires verified consumer conversion, not an arbitrary date. Old new-submission
commands return typed conversion guidance; accepted-key replay, cancellation,
receipt recovery and serialized job/registry snapshots keep working through
the retained internal decoder. Retirement additionally requires a laptop restart
proof: accept a native job before cutover, restart queue/worker with empty local
extension/environment caches, fetch/decode its original snapshot, qualify that
exact identity and execute the job, then verify accepted output and keyed replay.
The original bundle, digest and submission fingerprint must remain unchanged.
Incomplete conversion or failed cold-recovery proof leaves the retirement issue
open and source-authoring entry points intact until cutover.

Consumer migration preserves each existing seam: exact source/runtime and frozen
inputs, target/resource/placement restrictions, sidecars, checks and artifacts,
submission/key/cancellation behavior, endpoints and existing holds. A conversion
must not substitute an environment, infer native qualification or launch held
work. Runtime or platform changes require separate parity and native proof.
Keep private paths, host IDs, data and receipts in consumer repositories and
use sanitized fixtures in Mycelium.

| Work kind | Preserve and prove at its existing seam |
| --- | --- |
| Offline suites and native naming | Two offline-suite entries retain Pixi CPU tools, wrapper, exact target and JUnit check. Its native naming extension retains CUDA variant, sidecar, baseline/result names and production submit/key/cancel seam. Model endpoints remain unchanged. |
| Application backend/frontend/tooling | Backend/frontend/tooling entries retain targeted/full distinctions, macOS-only suites, placement restrictions, Python/Node prerequisites and markers/archive. Copy exact temporary queue-side lock bytes only under the approved environment option; do not infer Linux qualification from a list. |
| Python/browser tests | Pytest/browser-e2e entries retain explicit Git bundle input and stale-bundle check (source omits `.git`), resources/thread limits, installed Python/Node/browser prerequisites and success markers. Conversion does not substitute a new environment. |
| Rendering | Render/canary and per-item catalog entries retain frozen scene/binary inputs, snapshot identities, large frame archives, semantic checks, private placement and existing holds. Conversion must not launch another render. |
| Generation/validation/evaluation | Generation/validation/evaluation entries retain the exact native CPU runtime/source closure, checksum checks, seed/candidate coverage, artifacts and holds. Historical loader alternatives stay historical; new uv/Pixi runtimes or GPU ports need separate parity/native proof. |

Convert Mycelium's gate/go-test profiles and starter extensions first. Prove a
small CPU fixture and fake native variant in `myce dev`, then compare sanitized
consumer conversions through the same lowering. Consumer owners apply changes,
update harness/CI callers and record one small authorized accepted-result proof
per active entry type, requalifying changed native digests. A held consumer stays
held with owner/next action and blocks removal of any reader it still uses.
Retire redundant docs/examples and update the offload skill with cutover.

### Remaining work
<a id="implementation-plan-and-acceptance"></a>

The unified file and lowering (#250) and the converter with internal dogfood
(#253) are complete; their acceptance lists are in the
[text before consolidation](https://github.com/1kuna/mycelium/blob/e0488c0c5034b5a57827ceebfa69f770c060bb8f/docs/design-proposal.md#L2352-L2384). One slice remains open.

**[Consumer cutover and reader retirement (#254)](https://github.com/1kuna/mycelium/issues/254).**
Acceptance: reviewed maintained-consumer files/callers with evidence or explicit
holds; no active caller requires a removed reader; changed native digests
requalify; old new-submission calls give conversion guidance while keyed
recovery works. Require the pre-cutover native job's cold-cache queue/worker
restart proof described under migration; retain internal snapshot decoders while
referenced, even after source-authoring readers retire. Update
guides/examples/skill without weakening checks, widening targets or consuming
unaccepted results.

#192 and #248 remain open until live acceptance; design text alone removes
no consumer support.

