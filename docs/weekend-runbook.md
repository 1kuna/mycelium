# Weekend checklist

Use this checklist in order; linked runbooks supply the commands and detailed
acceptance tests. Stop at a failed gate. Estimates are planning allowances, not
measured promises; large copies/builds and source recovery can exceed a weekend.
Nothing here authorizes fleet changes. Keep evidence and old releases recoverable.

## Zach must decide or provide first

- Spark owner's admission, fleet cutover authority, fault-test windows and console
  access; separate B70 GPU0/GPU1 maintenance windows. Keep the CPU queue up.
- Exact deployed Verso release/source and naming context/template; the missing
  Atlas `atlas_wiki` deployed source, immutable revision, dependencies and CLI.
- Approved complete NAS model bundles, trusted per-file sizes/SHA-256 and aggregate
  identities; official model/generation contracts; exact environment build inputs,
  native artifacts/patches and installation paths. Example catalogs are invalid
  templates. Identify unknown Atlas face/Chandra/PII/VLM models before their use.
- Verified existing NAS mounts/roots, private credentials, spool/cache paths and
  capacities, audio transfer roots/allowed paths, retention policy, immutable
  representative warmup inputs, output baselines and rollback owners/unit snapshots.
- Conservative device budgets, host/cgroup headroom, required concurrency and
  latency and observation window; choose a capacity/product response if the exact models cannot fit.
  No implicit model/context reduction, GPU1 move or CPU fallback.

References: [prep report](../.reports/migration-prep-2-2026-10-09.md),
[Atlas migration](../../worktrees/atlas-mycelium-v2-migration/docs/mycelium-migration.md),
[Verso migration](../../worktrees/verso-mycelium-v2-migration/docs/mycelium-migration.md).
The project runbook links refer to the adjacent local worktrees; preserve their
reviewed revisions before weekend execution.

## Ordered stages

1. **Mycelium real-machine test — 2–4 hours.** Follow the
   [Spark acceptance sequence](production-test-plan.md#1-qualify-prerequisites-and-build-locally)
   through final acceptance; B70 hosts the CPU queue, Spark is the only worker.
   Prepare its two smoke models as that sequence requires. **Go:** all real
   command/model, recovery, NAS and clean-release gates pass with saved hashes and
   reports. **Blocks:** mounts, runtime/linger, ownership, credentials, any failed
   fault/canary. **Rollback:** use its drain/leave/service restoration steps;
   preserve queue state and unrelated apps. Never reinstall v1.
2. **Register migration models on NAS and qualify environments — 2–6 hours,
   longer for downloads/builds.** Follow Atlas steps 1–3 and Verso steps 1–3,
   [catalogs](guide-model-calls.md), [multi-file identity](guide-servers.md#immutable-multi-file-weights)
   and [environment recipes](environments.md).
   Freeze complete offline bundles/checksums, golden inference contracts and exact
   native builds; run off/on laptop gates. Register reviewed catalogs with clearly
   provisional reservations only for measurement. **Go:** no placeholders, offline
   identity and environment reproduction pass; old source/runtime/output baselines
   are preserved. **Blocks:** unknown manifests/builds, Verso source or naming
   contract; Atlas source gap for deployed-loop qualification (see below).
   **Rollback:** retain NAS masters and old catalogs/envs; keep switches false.
3. **Pre-copy — 30 minutes–4 hours, driven by bytes and measured NAS speed.** Use
   [model sync](guide-model-calls.md) to the intended Spark/B70 cache, then follow
   each project's sync checks. Only join under approved ownership; copying is not
   permission to load or pin production. Pre-copy the exact measurement cache if
   a joined worker must leave. **Go:** complete verified local copies, enough cache
   space for every simultaneous model, no copy errors/partials counted as ready.
   **Blocks:** NAS, capacity, hashes or unavailable authorized worker/cache path.
   **Rollback:** cancel sync requests, keep resumable partials and verified copies;
   drain/leave any preparation worker and prove release.
4. **B70 physical GPU0 measurement and capacity decision — 1–3 hours.** Follow
   [measurement safety/cleanup](production-test-plan.md#b70-gpu0-weekend-joint-residency-measurement)
   and **Verso step 4's current six-model command**: `verso-asr`,
   `verso-diarization`, `verso-alignment`, `verso-short-turn`, `verso-nemotron`,
   `qwen3.8-27b`. The older test-plan example's five-model/`verso-naming` list
   must not replace that exact migration inventory. Measure audio-first,
   naming-first and representative concurrent peaks in separate reports.
   **Go/no-go decision:** every exact model/warmup passes, `fits_device_budget`
   is true, native counters/headroom/concurrency are safe and cleanup releases
   GPU0. Set final total reservations from peaks plus reviewed margin and repeat
   admission. Zach explicitly accepts the evidence or stops for a capacity/product
   choice. **Blocks:** ownership, unsupported counters, fit, parity, pressure or
   cleanup. **Rollback:** unload, prove native release, restore the exact Verso
   owner and full original canary before reopening arrivals; CPU queue stays up.
5. **Atlas judge, then OCR — 2–4 hours.** Follow **Atlas steps 4–7**: Spark joint
   measurement in both orders and at concurrent peaks, measured final budgets,
   coordinated guards/single ownership, then judge sync/pin/isolated canary and GLM
   OCR canary. **Go:** golden context/tools/reasoning, certification/provenance,
   warm latency and complete OCR pixel/output parity pass. **Blocks:** fit/runtime,
   guard coordination or identity; missing source blocks deployed wiki/loop callers
   and their final acceptance, not isolated in-repo judge/OCR tests. **Rollback:**
   freeze routed ingress, drain/unpin/leave, prove release, switch false, restore
   exact old engines/units/guards and certify judge/OCR before reopening.
6. **Atlas GPU batches — 1–3 hours per qualified family.** Follow **Atlas step 7**
   for known local-model commands with exact environment and Spark allow-list.
   **Go:** admission/start, terminal success, completeness, fingerprints and loud
   failure pass on isolated outputs; no nested queue calls/token in commands.
   **Blocks:** unknown model/API/budget or checkpoint contract. The source gap
   blocks loop-triggered batches and search/loop integration, not independent
   known GPU batches. CPU search and remote-API-only work stay outside the queue.
   **Rollback:** stop submissions, cancel/drain IDs, preserve outputs/checkpoints,
   restore original worker only after Mycelium releases ownership.
7. **Verso experiments — 1–3 hours per representative family.** Follow **Verso
   steps 5 and 7** for a separately qualified physical GPU1 environment/owner;
   defer GPU0 production sync/pins. **Go:** actual bounded-unit jobs start and
   finish; checkpoint timing, attempt isolation, hashes and preemption/resume pass.
   **Blocks:** deployed source mismatch, unmeasured GPU1 runtime/budget or
   checkpoint failure. Optional accuracy needs its own expanded fit/parity gate;
   it is not implied by mandatory GPU0 measurement. **Rollback:** stop submissions,
   cancel/drain attempts, keep evidence, leave/prove release, restore GPU1 owner.
8. **Verso production last — 2–4 hours plus observation window Zach chooses.**
   Follow **Verso steps 5–6 and 8** for GPU0 transfer, all mandatory ready pins,
   isolated full-pipeline canary and then arrivals. **Go:** exact constructor,
   streaming reset/output parity, real warm residency, first-inference latency,
   concurrent pressure and no competing GPU owner pass. **Blocks:** any previous
   GPU0 decision gate, source/context mismatch, missing pin or output/latency
   failure. **Rollback:** freeze ingress → drain → cancel experiments → unpin →
   leave/prove release → switch false → restore exact units/config/guards/release →
   full original pipeline canary → reopen. Preserve DBs/caches/checkpoints/reports.

## Atlas source boundary

The prep report identifies missing `atlas_wiki` CLI/package, query/passage
encoders, full/incremental rebuild callers, ingest/query/lint loop, deployed
model constructors/list checks and exact revision/dependency/entrypoint contracts.
This blocks stage 2's **deployed Atlas source qualification**, stage 5's
**wiki/loop caller cutover and certification**, stage 6's **loop-triggered batches,
CPU-search parity and loop integration**, and **full deployed Atlas acceptance**.
It does not block stage 1, independent NAS/runtime preparation or pre-copy,
B70 measurement, isolated known in-repo judge/OCR/batches, or Verso stages.
Keep the existing deployed loop/search untouched until source reconciliation and
Atlas step 8 pass; isolated successes do not clear that blocker.
