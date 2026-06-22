# FABLE-FINDINGS — SSOT Conformance & Bug Audit

**Date:** 2026-06-09
**Scope:** Implementation vs SSOT docs **01-project-spec.md**, **02-testing-architecture.md**, **03-development-guide.md** (04 excluded per instruction).
**Method:** All three SSOT docs + DECISIONS.md/BLOCKERS.md read in full; eight parallel subsystem audits covering the entire `internal/`, `cmd/`, `pkg/`, `test/`, `tools/` tree (contracts/test-infra, scheduler/lease/estimate, node/backends/telemetry, gateway/translate, catalog/membership/discovery, federation/optimizer, CLI/CI/wiring, cross-cutting end-to-end flows); gate commands (`gofmt`, `go build`, `go vet`, `go test ./... -race`, covergate) run live on HEAD (`1a3b667`). Every High finding was independently re-verified against the source before inclusion.

**Top-line:** the owner-authority core (fence/commit serialization, no double-booking) and the fail-loud translation layer are genuinely sound — the two things the spec is most paranoid about hold. The serious problems cluster in (a) the canonical gate being red on main, (b) lifecycle seams (queue/rescue never re-executing work, preemption destroying idle victims, registry merge ordering preventing convergence), and (c) production wiring quietly disabling spec'd behavior (request-control headers, CI).

**Conventions:** Severity = Critical / High / Medium / Low. Category = `drift` (diverges from SSOT), `incomplete` (spec'd but partial/missing), `bug` (breaks in typical use or a realistic edge), `brittle` (works today, fragile by construction). "Logged" means a DECISIONS.md entry covers it (a logged *your-call* within hard requirements is not drift; a logged `PROPOSED SPEC CHANGE` still is, by Doc 3's own rule: "keep building to spec — don't silently diverge").

---

## 0. Gate status at HEAD — the canonical gate is RED

Doc 3: "Gates are runnable… Never advance on a red gate." `make ci` = fmt + build + vet + test + coverage. Run live on this tree:

| Check | Result |
|---|---|
| `gofmt -l .` | ✅ clean |
| `go build ./...` | ✅ clean |
| `go vet ./...` | ❌ **FAIL** (G1) |
| `go test ./... -race` | ✅ all green |
| covergate | ❌ **FAIL** (G2, G3) |

### G1 — `go vet ./...` fails (every phase gate requires it clean) — HIGH / bug
`internal/gateway/router_test.go:546` and `:600`: table-driven tests embed `gateway.Router` **by value**; `Router` gained `jobPrefixOnce sync.Once` (`internal/gateway/router.go:115`), so `range` copies a lock (`copylocks`). Introduced by `7a345ba` ("Fix gateway project telemetry lifecycle"), already on main. Every phase gate in Doc 3 lists `go vet ./...  # clean`.

### G2 — `internal/scheduler` coverage 99.9% vs the hard 100% bar — HIGH / incomplete
`releaseErrAllowsOwnerFallback` (`internal/scheduler/service.go:513`) is at 75% (the `err == nil` branch is untested). Doc 2 §10 / Doc 3 Phase 0 gate: "internal/scheduler AND internal/lease == 100.0%". Verified on both a fresh profile and the repo's committed `all.out`. `internal/lease` is at 100% ✅. Bonus: that function classifies errors by **substring match** ("not claimed by this coordinator", "has no committed lease") instead of typed `domain.Err*` / wire codes — the exact brittleness DECISIONS 2026-05-29 ("stable codes… `errors.Is` instead of parsing strings") was written to avoid.

### G3 — `internal/backends/llamacpp` at 84.0% vs the 85% per-package floor — HIGH / incomplete
`stopStoredProcess` 37%, `expectedStopSignalExit` 64.7% — the recently hardened stop/reap paths (commit `6ede47e`) are the least-tested code in the package. `make coverage` fails. Note the committed `all.out` and a fresh run disagree on which backends package is under the floor (84.0% llamacpp fresh vs 83.8% processadapter committed) — coverage in backends tests is timing-dependent, itself a smell.

### G4 — GitHub Actions CI is disabled — HIGH / drift (unlogged)
Doc 2 §9 specifies `.github/workflows/ci.yml` (fast job on push/PR; smoke job on main on a self-hosted GPU runner). The file exists with the right content but is parked at `.github/disabled-workflows/ci.yml`, so nothing triggers. DECISIONS logs "CI proof is centralized in `make ci`" — it does **not** log disabling the workflow. G1–G3 sitting red on main is the concrete demonstration of the cost.

---

## 1. Critical-path drift: spec-defining behaviors that don't hold

### D1 — Displaced **idle** instances are destroyed, not re-placed/re-queued; the Phase 0 §3.6 gate test passes vacuously — HIGH / drift + incomplete
**Where:** `internal/scheduler/preempt.go:84-86`; `internal/scheduler/service.go:683-699`; `test/e2e/phase0_worked_example_test.go`.
**Spec:** Doc 1 §3.6: "displaced work re-places elsewhere if it fits, else re-queues at its own priority." Doc 3 Phase 0 hard requirement repeats it with no idle exception, and the named gate requires "(b) re-places that 27B onto a `Make4090Node()` if it fits there else re-queues it at background priority."
**Code (verified):** the replacement loop skips any victim with `InFlight <= 0` (`if selectedVictim.InFlight <= 0 { continue }`). Idle victims appear in neither `Replacements` nor `Requeued` — they are unloaded and gone. `fixtures.MakeInstance` defaults `InFlight: 0` and the worked-example e2e builds the background 27B that way, so the gate's canonical scenario destroys the 27B instead of migrating it; the test asserts `Requeued` empty (vacuously true) and the presence of a `replace` trace step (satisfied by the "idle victims evicted" note), never `decision.Replacements`. The victim's job record is left `running` forever (never set `preempted`/`queued`). Not logged in DECISIONS (the logged victim decision covers tie-breaks only).
**Impact:** the project's defining behavior — the §3.6 worked example — does not do what the spec and its own gate say. A background analysis instance idle between batches is exactly the realistic victim; it gets killed, its job orphaned.

### D2 — Queued and rescued jobs never deliver results: drain/rescue load the model, grant a lease, and discard the work — HIGH / incomplete
**Where:** `internal/gateway/router.go:661-663` (ActionQueued → error to client → HTTP 502); `internal/scheduler/service.go:355-381` (`Drain` → `SubmitWithPayload`); `cmd/mycelium/peer.go:2062` (`_, err := runtime.Drain(...)` — results discarded; verified); `cmd/mycelium/peer.go` `rescueRecoveredJob` (same `SubmitWithPayload` path).
**Spec:** Doc 1 §3.4/§3.6: with soft defaults a job "waits for a slot" / "queue"; D16/§3.12: the registry holds the payload "so a job can be **truly rescued (re-run)**, not merely flagged as lost."
**Code (verified):** when placement queues, the client gets a 502 while the job stays durably enqueued with its payload. The background drainer later dequeues it, places, cold-loads, commits an owner lease, marks the job `running` — and **never issues the upstream inference request**. Dead-peer rescue follows the identical path: the rescue envelope's ingress body is decoded but never replayed to a backend. The job sits `running` until the manufactured 30-min lease expiry (D12 below).
**Impact:** the documented default behavior (queue-and-wait) is observed by clients as hard failure; "rescue" warms a model and pins capacity but re-runs nothing. Ten queued requests during a saturation blip become ten ghost cold-loads each holding a lease for 30 minutes. D16's payload-for-rescue promise is structurally unfulfilled.

### D3 — Per-request intent is dead in production; `X-Myc-Handling: private` is silently ignored instead of rejected — HIGH / drift + incomplete
**Where:** `cmd/mycelium/peer.go:348` (`TrustControlHeaders: false`, hardcoded; no config knob); `internal/gateway/server.go:91-93`.
**Spec:** Doc 1 §7 step 2: project defaults merge "with any per-request overrides"; §3.8 Job carries per-request priority/speed_pref/context_request/preemption. DECISIONS 2026-06-04: "`X-Myc-Handling: private` is rejected at ingress."
**Code:** the full header-mapping with strict validation exists and is tested (invalid values → 400), but production hardcodes trust off, so `X-Myc-Priority`, `X-Myc-Speed-Pref`, `X-Myc-Context-Cap`, `X-Myc-Preemption`, `X-Myc-Project`, `X-Myc-Submitter`, `X-Myc-Handling` are all silently dropped. The loud-reject path for `private` only executes when headers are trusted — i.e., never in the shipped daemon. No DECISIONS entry logs "control headers disabled in production."
**Impact:** "apps submit intent" — the product's core contract — is half-wired: you cannot mark a batch job `background` or a chat `latency` per request against a real peer. A caller requesting private handling is silently processed as non-private: the exact quiet degradation §3.11 forbids, and a direct contradiction of the logged decision.

### D4 — Multi-GPU fit accounting (even split) contradicts the launched `--tensor-split` (VRAM-proportional) — HIGH / bug
**Where:** `internal/lease/safety.go:62-85` (`splitClaim`: `base := total / len(sorted)`); `internal/scheduler/tuning.go:41-54` (`tensorSplit` emits per-accelerator `VRAMTotalMB`). Both verified.
**Spec:** Doc 1 §3.2/D7: `max_util` is "never exceeded, regardless of priority or preemption"; §3.10: computed tuning must reach the launch (it does — but then the accounting must match it).
**Code:** the allocator charges a multi-accelerator claim **evenly** across the selected GPUs; the launch command tells llama.cpp to distribute weights **proportionally to VRAM**. On a heterogeneous group (24 GB + 8 GB), a 24 GB model is accounted 12/12 but actually loads ≈18/6: the big GPU carries ~6 GB more than the fit check assumed and can physically cross `max_util`, while the small GPU's accounting over-blocks future placements. Homogeneous groups are unaffected (even ≡ proportional).
**Impact:** a real path to violating the inviolable ceiling (D7) on exactly the heterogeneous fleets the project targets.

### D5 — Client disconnect/timeout is misclassified as instance failure → healthy warm instances get unloaded and deleted — HIGH / bug
**Where:** `internal/gateway/router.go:1383` (any read error → `streamFailureUpstreamRead`), `:1327-1330` (`streamFailureIndicatesInstance` returns true for *every* upstream-read error — verified: no `ctx.Err()`/`context.Canceled` guard anywhere in the classification), `:908-925` (`failPlacedStream` → report on `WithoutCancel` ctx so the unload *succeeds*), `internal/gateway/failure.go:21-48` (report = Unload + DeleteInstance). Non-stream path (`router.go:216-231`) has the same flaw, there ending in `markError` flipping a healthy instance to `InstError`.
**Spec:** DECISIONS 2026-05-29 ("failed instances are unloaded and deleted before retry") is for *failed* instances; Doc 1 §3.1 makes warm co-residency the central lever.
**Impact:** an agent client with a 30s timeout aborts a slow generation → the gateway evicts the warm instance other projects were batching onto → every subsequent request pays a multi-minute cold load. Routine cancels become fleet-wide warm-capacity churn. Compounded by L-class finding that `sseTailHasTerminal` does exact-byte JSON matching (`router.go:1341-1360`): a backend emitting `"type": "message_stop"` with a space is classified as truncation → same eviction path on a *successful* stream.

### D6 — Live-process Stop kills only the direct child, never the process group; SIGKILL fallback leaks grandchild engines **and deletes their reap record** — HIGH / bug
**Where:** `internal/backends/processadapter/adapter.go:188-242` and `internal/backends/llamacpp/adapter.go:206-263` (live stop: `process.Signal(...)` / `process.Kill()` on the single PID — verified); contrast the stored-process path which correctly group-kills via `syscall.Kill(-PGID, SIGKILL)` (`llamacpp/adapter.go:554-561`). Processes are launched with `Setpgid: true`, so the group exists precisely for this.
**Spec:** Doc 1 §3.10: "a crashed agent must not leave a zombie inference server holding VRAM"; DECISIONS 2026-06-01 (SIGTERM→SIGKILL "giving Docker-backed engine wrappers a real cleanup window").
**Impact:** a vLLM Docker wrapper (or `vllm serve`, which forks workers) that doesn't exit within grace gets SIGKILL on the wrapper only; the container/workers keep VRAM. `Stop` then reports success and calls `removeProcessRef` — the orphan's record is deleted, so the startup reaper can never find it either. On the Spark (`catastrophic`) this sets up the OOM the whole safety model exists to prevent. Aggravators: the vLLM/MLX adapters never receive `StopGracePeriod` from config (`cmd/mycelium/compute.go:195-198` — llamacpp/custom branches pass `cfg.StopGraceMS`, vllm/mlx don't → hardcoded 2s); and llamacpp live-stop sends SIGINT, not the logged SIGTERM (`adapter.go:243`).

### D7 — `Unload` drain-wait has no timeout; hard preemption waits on an **uncancelable** context → permanent wedge on a leaked in-flight count — HIGH / bug
**Where:** `internal/node/agent.go:165-196` (blocks on `<-drained`, no deadline); `internal/scheduler/service.go:641-663` (`finishPreemption` uses `cleanupContext` = `context.WithoutCancel`).
**Spec:** Doc 1 §3.6 "in-flight drains cooperatively" — but §3.11: a hang is worse than a loud stop.
**Code:** `BeginRequest`/`EndRequest` counts are driven by the coordinator over RPC with no TTL. A coordinator that dies (or whose end-request RPC fails) between begin and end leaves the count >0 forever; the instance is permanently undrainable. Any later `Unload` — including hard preemption, deliberately on `WithoutCancel` — blocks forever; the preempting job hangs for the life of the process. (Related: the gateway discards `EndRequest` errors — `router.go:791-793` `_ =` — so the leak is reachable from the happy path too.)
**Impact:** coordinator Mac reboots mid-stream against the 4090 box; later a 120B interactive hard-preempts the stranded instance; that submission wedges forever.

### D8 — Interrupted catalog install can commit a **truncated** model as a ready, registered preset — HIGH / bug
**Where:** `internal/catalog/install.go:227` (resume guard `if !fileExists(finalModel) && !fileExists(stageModel)` skips the copy when a partial stage file exists — verified), `:289-295` (unconditional `os.Rename(stageModel, finalModel)` then provenance + preset + `ready`), `copyFile` `:391` (`O_CREATE|O_EXCL`, **no remove of dst on error**, no fsync).
**Spec:** Doc 3 Phase 3 hard requirement: "an interrupted install leaves no half-materialized preset registered as usable." DECISIONS 2026-05-29/06-01 promise commit-ordering protection.
**Code:** the commit *ordering* (artifact → provenance → preset JSON → ready) is correct, but there is **no size/digest verification of the staged artifact** before the commit rename — even though the expected size is known for every importer and hf/oci verified a sha256 against the importer temp file (not the staged copy). Kill the process mid-copy (OOM, power), re-run `myce add-model`, and the truncated stage file is renamed into place and registered.
**Impact:** the next placement loads garbage; the backend crashes at launch (best case) or misbehaves (worst).

### D9 — Second `mycelium run` reaps the **live** daemon's backends, then dies on the port bind without cleanup — HIGH / bug
**Where:** `cmd/mycelium/compute.go:71-81` (reaper runs inside runtime build); `cmd/mycelium/peer.go:42-80` (port bound only *after* the full build; cleanup goroutine only fires on `ctx.Done()`, not on a `ListenAndServe` error). No pidfile/flock anywhere (grep-verified).
**Spec:** Doc 1 §3.10: the reaper exists for "orphaned backend processes … from a **previous** run."
**Impact:** with the launchd service installed (a first-class flow per DECISIONS 2026-06-03), an operator running `mycelium run` manually with the same config has the second process: read the shared SQLite process refs (indistinguishable from a crashed run — same identity), SIGTERM/SIGKILL the live service's inference backends mid-request, delete their refs, possibly launch pinned presets (grabbing VRAM), then exit on "address already in use" — skipping cleanup, leaving its own pinned backends orphaned, and leaving the healthy daemon's admission/lease state pointing at dead processes.

---

## 2. Scheduler / lease / estimate (Doc 1 §3.1–§3.6, Phase 0)

### S1 — Reserved headroom has no consumer: interactive work cannot land in the reserved space — MEDIUM / incomplete
`internal/lease/allocator.go:46` charges the headroom against **every** placement unconditionally; no flag/priority path admits an interactive job *into* it. Doc 1 §3.4: headroom exists "so interactive work always lands without a fight." As built it is a permanent utilization cut — the urgent job still waits, exactly as if nothing were reserved. The opt-in mechanism's stated semantics don't exist.

### S2 — Multiple reservations on one node silently collapse to the last one — MEDIUM / bug
`internal/lease/reservation.go:5-9`: `a.headroomByNode[nodeID] = claim` overwrites; `cmd/mycelium/peer.go:2365-2379` loops reservations into that map. A node with headroom + a pinned preset (or two pinned presets) keeps only whichever was configured last; the rest silently unenforced — anti-§3.11.

### S3 — Pinned reservations double-reserve once loaded, and always land on accelerator 0 — MEDIUM / bug
`cmd/mycelium/compute.go:119-150` loads the pinned preset (full claim, hardcoded `AcceleratorSet: []int{0}`); `cmd/mycelium/peer.go:2368-2372` *also* installs the same claim as permanent allocator headroom; `Fits` counts both. A pinned 9B on a 24 GB box blocks ~14 GB instead of ~7 GB — silently starving co-residency, the central lever (§3.1). Two pinned presets on a multi-GPU box always contend for GPU 0.

### S4 — Queue aging resets on every failed placement attempt → practical starvation — MEDIUM / brittle
`internal/scheduler/service.go:225-259`: re-`Enqueue` on `ActionQueued` stamps fresh `enqueuedAt`/`seq` (`queue.go:36-41`); the drain loop retries the head every tick. A background job needs ~200 uninterrupted minutes of waiting to outrank interactive (rank gap 200 × 1 min/min aging); every retry zeroes the clock. The unit test proves only in-queue ordering, never the integrated requeue path. Doc 3 Phase 0 hard requirement: "no starvation."

### S5 — Hard-preempt path never checks `CanStackLoad` on the target; local submit turns owner rejection into job **failure** — MEDIUM / drift + bug
`internal/scheduler/preempt.go:43-125` checks `Fits` only — the placer will propose preempt-then-cold-load onto a catastrophic node with an unrelated load in flight (§3.2 forbids stacked loads there). Owner admission correctly re-rejects at commit, but in the **local (non-coordinated)** submit path that `ErrNoFit` marks the job `failed` instead of queued (`service.go:121-125`; only `submitCoordinated` maps ErrNoFit/ErrStaleFence to re-queue). Placer-only consumers (the fleet-benchmark preflight simulator) see wrong preempt plans.

### S6 — Trace requirements not met: only the winner's score is recorded; dequeues untraced; preempt decisions lack select/score steps; gate test weakened to match — MEDIUM / drift
Doc 3 Phase 0 hard requirements: "every score appears in the `PlacementDecision.Trace`"; "every dequeue decision is traceable"; gate trace = "estimate/filter/select/score/preempt". Code: `placer.go:104-110` records only `scored[0]`; queue effective-priority math is invisible; the preempt path emits only `preempt`/`replace` after estimate/filter (`placer.go:134-156`); and `test/e2e/phase0_worked_example_test.go:69` asserts `estimate, filter, preempt, replace` — not the steps the gate names. The test also checks `max_util` on final state, not "at any step."

### S7 — Placement cost is exponential: 2ⁿ−1 accelerator subsets × uncached estimator calls — MEDIUM / brittle
`internal/scheduler/filter.go:64-84` enumerates all subsets; estimator invoked per node × per subset in both filter and preempt passes; the GGUF estimator (`estimate/gguf.go:38-86`) shells out to gguf-parser or RPCs `InspectModel` per call with no memoization — though the result is identical for every subset. An 8-GPU node ⇒ up to 255+255 evaluations per placement, on the hot path of every request.

### S8 — Owner leases get a manufactured 30-minute expiry; long generations have their lease force-released mid-flight — MEDIUM / bug
`internal/scheduler/service.go:1098-1100` stamps `now+30m` on every owner lease arriving without `ExpiresAt` (owner admission never sets one — `node/admission.go:366-377`); `ExpireLeases` runs every drain tick and releases through `ReleaseJob`. No renewal exists. A generation >30 min (the Spark 122B class — real per BLOCKERS) has its warm-KV lease accounting freed while still streaming. Also the self-heal gap inverse: a lease whose Commit response was lost on the wire is never reaped at runtime (reconcile runs only at startup) — capacity stranded until restart (`node/admission.go:128-202`).

### S9 — Unified-memory math is a dead 7-line stub; node memory baseline frozen at boot — MEDIUM / incomplete + brittle
Doc 1 §5 carves out `estimate/unified.go` ("Apple unified-memory single-pressure-domain math"); D9 makes unified memory one pressure domain with host RAM. The file's `unifiedMemoryPressureMB` is referenced only by its own test. The actual behavior is implicit: macOS discovery reports one accelerator with `VRAMTotalMB = hw.memsize` and `VRAMUsedMB =` **boot-time** used memory, captured once at agent construction and never refreshed (`internal/node/agent.go:96-114`, `cmd/mycelium/compute.go:48`). A Mac peer up for days reasons against a stale host-RAM baseline — other apps' churn is invisible to every fit check; the load-time-spike paranoia of §3.2 is undermined on exactly the unified-memory machines.

### S10 — Cold-load occupancy is double-counted between owner Commit and post-readiness BindInstance — MEDIUM / bug (conservative direction)
`internal/node/admission.go:576-591` synthesizes a Loading instance for the unbound lease; `internal/node/agent.go:310-331` registers the *real* loading instance with the same claim; both are visible to fit checks until the lease is bound **after** readiness (`scheduler/service.go:155-174`). For the whole load (minutes for big models) every Offer/Commit reserves 2× the claim → spurious `ErrNoFit`/429 for placements that genuinely fit. Defeats co-residency during exactly the long-load windows that matter. (The existing no-double-count test covers only post-bind.)

### Scheduler lows
- **S11 (low/bug):** filter trace can list a node in both `kept` and `dropped` (per-accSet drop reasons recorded even when another subset succeeds) — `filter.go:33-50,134-155`.
- **S12 (low/bug):** `replacementForVictim` ignores the displaced job's `NodeSelector` — a victim can be re-placed onto a node its label selector excluded (`preempt.go:167-197`).
- **S13 (low/brittle):** unknown `SpeedPref`/`Priority` values are silently accepted (hybrid behavior / rank-as-normal) instead of loudly rejected (`placer.go:268-287`, `preempt.go:234-245`).
- **S14 (low/brittle):** model-alias→preset resolution is first-registered-wins with no cross-preset comparison (`placer.go:26-36`); gateway alias index likewise silently first-wins (`router.go:71-80`).
- **S15 (low/brittle):** scorer "fit tightness" ignores existing occupancy — an empty unit and a nearly-full unit of the same size score identically (`scorer.go:19-27`), blunting pack-vs-spread (§3.3 step 5).
- **S16 (low/brittle):** `internal/locality/planner.go:285-298` treats `MaxUtil==0` as 1.0 (opposite of the allocator) and ignores the catastrophic margin.
- **S17 (low/incomplete):** `Job.DeadlineMS` (§3.8: "informs urgency/aging") is read nowhere.
- **S18 (low/incomplete):** `filterCandidates`, `tryPreempt`, `enactPreemption` have no production callers (coverage-padding dead code).
- **S19 (low/bug):** two compensation paths drop the original error when `releaseOwner()` also fails (no `errors.Join`, unlike sibling paths) — `service.go:146-162`.

**Verified conformant (core math):** `usable_vram = total×max_util − headroom` exact to the MB with conservative truncation and overflow-safe claim summation; catastrophic +5% margin and `CanStackLoad` enforced in filter, replacement, and owner commit; KV-aware claims everywhere (no weights-only path — estimators error rather than degrade); warm batching charges incremental KV and re-fits; loading counts as occupancy for all units; failed estimate never places; `latency`→`ActionDedicatedUnit` reachable; soft default queues and kills nothing; victims must be strictly lower priority, pinned exempt; fit-forced reallocation is the same preemption test via `Preset.NodeID` hard locality (no borrow/reclaim mechanism); placer is pure/peer-agnostic/deterministic and never mutates the snapshot; computed tuning (`--n-gpu-layers`, `--tensor-split`) genuinely reaches the launch command with user args winning.

---

## 3. Node agent / owner authority / backends / telemetry (Phase 1, §3.10–§3.12)

*(D6, D7, D8, D9, S8–S10 above also belong to this layer.)*

### N1 — No supervision after readiness: a crashed engine stays `ready` and becomes a zombie — MEDIUM / bug
No `Wait`/exit-watcher on the launched child (`llamacpp/adapter.go:109-143` — `Wait` happens only inside Stop) and no health re-check loop in the agent. A segfaulted llama-server: (a) stays a zombie process until Stop, (b) keeps being advertised `ready`, so the scheduler keeps placing on it; every request burns a failover attempt; admission still counts the dead instance's claim. The owner — the designated lifecycle authority — never notices. Anti-§3.11.

### N2 — Cold-start dedup ties the shared load to the first caller's context — MEDIUM / bug
`internal/node/agent.go:144-163`: the owner path runs `launchAndWait(ctx,…)` with the **first requester's** ctx; `finishLoad` publishes the error to all waiters. Caller A disconnects mid-load → load aborted, backend stopped → waiter B fails too, though B still wants the model. One client disconnect becomes a fleet-visible load failure. The load should decouple from any single requester once started (it's already lease-accounted).

### N3 — Spawn-before-record orphan window — MEDIUM / brittle
Both adapters `Start` the process **then** `ProcessRegistry.Add` (`processadapter/adapter.go:106-131`, `llamacpp/adapter.go:117-141`). A daemon SIGKILL between the two leaves a running engine with no ref — invisible to the reaper forever. Exactly the §3.10 failure class; write-intent-then-spawn would close it. Also reaper refs are keyed by node ID: renaming `compute.id` in config orphans the previous run's refs (`compute.go:71-78`).

### Node lows
- **N4 (low/drift):** `AdmissionController.Preempt` is hard-disabled ("direct lease preemption is disabled", `admission.go:398-408`) — Doc 2 §2 defines it; DECISIONS said it "stays a low-level operation," not that it errors always. The conformance suite was edited to assert the disablement.
- **N5 (low/incomplete, dead code in spec-named files):** `node/loadingstate.go` `WriteLoadingState` — zero production callers **and** never flushes (would buffer the SSE event for the whole load if wired); `node/agent.go:258` `RecordRun` — no callers, plus an unreachable default branch; `node/heartbeat.go` `HeartbeatTracker` — production-unused and not mutex-protected; `optimizer/reactive.go` `PlanReactiveRequeue` — dead, and its fallback invents an ad-hoc preset ID that DECISIONS 35 explicitly rejected.
- **N6 (low/bug):** `node/http.go:720-729` `writeJSON` panics on response-encode failure (client disconnect mid-encode → panic log per connection).
- **N7 (low/brittle):** accelerator-set comparison is order-sensitive in the agent (`loadKey`/`sameAcceleratorSet` don't sort; admission does) — `[0,1]` vs `[1,0]` defeats dedup and warm matching; saved only by the placer emitting canonical order, an unenforced invariant.
- **N8 (low/brittle):** duplicate-Record semantics diverge between the two `TelemetryStore` impls (`telemetry/store.go` plain INSERT errors on dup job_id, no busy-timeout; `store/sqlite` upserts) — the exact dual-impl drift Doc 2 §3 builds conformance suites to prevent, and group-sync re-imports make upsert-vs-error matter.
- **N9 (low/brittle):** overflow classification is substring matching with broad patterns (`"context window"`, `"too many tokens"` against the whole error body — `optimizer/reactive.go:74-85`); a non-overflow error mentioning the context window would be silently retried on a larger preset (anti-§3.11 in the false-positive direction). The live router path is otherwise correctly strict.
- **N10 (low/brittle):** `BindInstance` mutates lease state without bumping the fence (`admission.go:452-479`) — harmless today, but breaks the "fence = resource version" invariant every other mutation honors.

**Verified conformant:** owner authority core is sound — single-mutex transactional store with write-through SQLite persistence; Commit re-checks fit (incl. live agent instances) against current state; fence monotonic; stale fence → typed `ErrStaleFence` preserved over the wire as a stable code; concurrent commits provably serialize (real concurrent test exists); coordinators reach admission only via RPC. Startup reaper uses tracked refs with genuine PID-reuse defense (PGID + cmdline + start-time + zombie detection), aborts startup loudly on kill failure, deletes refs only after successful reap; clean shutdown unloads instances. Shedding is a loud `ErrNoFit` → HTTP 429, no local queue. Readiness gate + cold dedup + load timeout via injected Clock all real and tested. GGUF preflight + node-side `InspectModel` delegation real; nothing places on a failed estimate. Telemetry: RunMetric with real tokens/sec (streaming-aware), TTFT, load wall-clock, sampler-backed peak VRAM; session_metrics per the logged decision; per-project rollups.

---

## 4. Gateway / translate / profiles (Phase 2, §3.11, §7)

*(D2, D3, D5 above are gateway findings too.)*

### W1 — Mid-stream provider failure is silently truncated to the client — MEDIUM / drift
`router.go:922-924`: once any provider byte has been forwarded (`providerStarted`), `failPlacedStream` writes **no** SSE error event — the stream just ends without `[DONE]`; a test pins this (`TestRouterStreamProxiedTruncatedSSEFails` asserts the body must NOT contain `event: error`). DECISIONS 2026-05-29 says the opposite: "later failures on an already-started stream are emitted as SSE error events." Internally loud (job failed, truncation detected), externally the agent-workflow hazard §3.11 names: a half answer that parses as complete.

### W2 — Provider profiles are not data: `config/profiles/` is empty, no parser exists — MEDIUM / incomplete
Doc 1 §5: `config/profiles/  # provider profile YAML`; Doc 3 Phase 2: "provider-profile-as-data + per-provider parser, Olla-style"; D1 provenance names profile-as-data explicitly. Reality: five hardcoded Go literals (`gateway/profiles/profiles.go:33-80`), nothing in `cmd/` ever loads a profile from disk. Fail-loud-on-unknown is conformant; the data surface (add/adjust a provider without recompiling) doesn't exist. Not logged as a spec change.

### W3 — One bad discovered peer fails the entire fleet snapshot — MEDIUM / bug
`gateway/peer_directory.go:67-70`: `agentFor` errors (peer missing id/address, or **`Tunnel.Open` failure** — the likeliest failure for a peer that just went down) abort `Snapshot` entirely, failing the user's request even with healthy local compute. Post-setup RPC failures correctly degrade to `NodeUnreachable`. Contradicts both §3.12 ("capacity simply shrinks") and the logged decision ("snapshot failures shrink available capacity instead of failing the whole peer view").

### W4 — `Route()` error paths leak the lease when the telemetry error-sample emit fails — MEDIUM / bug
`router.go:220-270` (four branches): `recorder.emitError(...)` runs **before** `releaseAndFail`, and a non-nil sample error returns immediately — skipping lease release and job-fail. `Stream()` does it in the safe order. A telemetry hiccup coinciding with an upstream error pins the owner lease ~30 min and leaves the job non-terminal.

### W5 — Per-chunk synchronous telemetry on the streaming hot path; a telemetry blip kills a healthy stream — MEDIUM / brittle
`router.go:588-606`: every ≤32KB read fires a session sample; for a remote owner that is a **synchronous HTTP push** (`recordSample` → `PushSamples`) on a keep-alive-disabled client between upstream reads; `router.go:1292-1295`: a chunk-callback error aborts the copy (`streamFailureTelemetry`) — killing the user's generation, and (per W1) silently. O(stream-length) write amplification against the owner's single-connection SQLite. Nothing in the SSOT makes telemetry a per-chunk liveness dependency of relay (§7 step 7/8).

### W6 — Non-streaming requests hitting a cold model get zero bytes until the load finishes — MEDIUM / incomplete
The loading keep-alive exists only on the SSE path (`Route` passes `nil` for `beforeCold`, `router.go:170`; the `cold && req.Stream` branch in Route is unreachable from the server). Doc 1 §7 step 7: "loading-state SSE keeps the client alive during load." A plain JSON request triggering a 70B cold load (5m default, configurable longer) gets nothing; typical clients/proxies time out at 30–120s, the load completes anyway, capacity is wasted. Also: the streaming path sends exactly one `loading` event with no subsequent pings — an aggressive idle-timeout proxy can still drop a very long load.

### Gateway lows
- **W7 (low/bug):** error-status mapping — everything is 502 (unknown model should be 404/400; translation-unsupported 400; body-too-large 400 not 413; wrong method 404 not 405) — `server.go:34-86`.
- **W8 (low/incomplete):** no X-Myc-* headers on failure responses, even when a placement existed (`writeGatewayError`, `server.go:191-195`); `X-Myc-Trace` silently omitted on marshal failure (`headers.go:35-40`).
- **W9 (low/brittle):** overflow-retry rewrites the upstream `model` to the **preset ID** (`router.go:246,514`) — strict backends (vLLM validates served model name; DECISIONS 2026-06-01 hit exactly this) reject it, so the reactive requeue can fail with "model not found."
- **W10 (low/bug):** hop-by-hop headers from upstream are forwarded (only Content-Length stripped — `router.go:1384-1391`).
- **W11 (low/brittle):** unknown project IDs silently resolve to zero-value defaults (`router.go:693-696`); no panic-safe deferred lease release; no `GET /v1/models` (common SDK probe 404s); concurrent fleet snapshots swap `PeerDirectory.agents` under in-flight requests (correctness race under churn, `peer_directory.go:115-118`).
- **W12 (low/dead code):** `StickyTable` + `ConversationKey` parsed-then-dead (logged disabled, but retained); `withoutInstance` pruning is a no-op in production; one job per failover attempt means one client request can leave multiple registry rows (logged, but "job" identity is per-attempt).

**Verified conformant:** translation core is genuinely fail-loud and test-pinned — strict decode; tools/tool_choice/tool_calls, content blocks, images, unknown roles, multi-choice, unsupported response fields, unmappable stop reasons all error explicitly; a malformed tool-call can never become `{}`; streaming translation rejected loudly **before** any byte is written; passthrough preferred and byte-faithful; unknown profile/backend errors with no generic fallback. Failover is bounded, reports/unloads/deletes the failed instance, stops loudly on dirty-runtime errors, and classifies context overflow (4xx) before failover (5xx), requeuing on the next larger persisted preset. X-Myc headers set before first flush on streams. Cold-load SSE starts before node Load. Lease release flows through `WithoutCancel` with errors surfaced (modulo W4). 2026-06-04 supersessions honored: placement requires the coordinator runtime; private handling rejected where it reaches the router (but see D3); sticky never bypasses placement. SSE parsing handles CRLF, blank lines, `[DONE]`, 1MB-capped buffering with terminal detection beyond the cap.

---

## 5. Catalog / membership / discovery (Phases 3–4)

*(D8 above is the headline catalog finding.)*

### C1 — `seenNonces` grows without bound on the lifetime discovery watcher — MEDIUM / bug
`membership/peer_discovery_lan.go:148,310`: the replay-protection nonce set on the production `WatchPeers` path (runs for the process lifetime) is never pruned; a new nonce arrives every advertise interval (5s default) per peer, yet nonces are useless after the 5s advert TTL. Slow unbounded memory growth on every long-running peer.

### C2 — Remote-import temp files are never cleaned after success — MEDIUM / bug
`catalog/importers/importers.go:222-258` (`keep = true`) + `install.go:231-235`: hf/oci downloads land in `os.TempDir()`, get **copied** into the staging dir, and the multi-GB temp is left behind forever. Repeated installs fill /tmp.

### Catalog/membership lows
- **C3 (low/brittle):** with no join token configured, LAN advertisements are accepted unauthenticated (no HMAC/nonce/expiry) — reachable only in token-less standalone mode; blast radius limited to candidate-set pollution since admission is rpc_token-gated.
- **C4 (low/brittle):** `copyFile` lacks fsync before the commit rename (power-loss tail-corruption window; shares root cause with D8); concurrent installs of the same source fail with a raw `O_EXCL` "file exists" instead of "install already in progress."
- **C5 (low/brittle):** `/catalog/stage` RPC stages **any owner-readable local path** as a model (`peer.go:601-651`) — fine within the rpc-token trust boundary, deserves a path allowlist.
- **C6 (low/bug):** `LANTunnel.Open` drops its lock across listener creation — two simultaneous Opens for a node whose address changed can hand one caller a just-closed listener's address (`membership/tunnel.go:51-90`).
- **C7 (low/drift, logged):** `discovery_overlay.go` is a full generic implementation + memory backend rather than the spec's "interface-shaped stub, not half-built" — production-unreachable and loudly config-rejected (conformant by disablement), but the loudness lives in `config_validate.go`, not co-located with the code.

**Verified conformant:** commit ordering (artifact → provenance → preset JSON → ready) with a resume gate requiring both committed preset and provenance; all three importers (local, hf://, oci://) real end-to-end — exceeding the "≥1 real" bar; OCI manifest size + sha256 digest verification with loud failure on unsupported algorithms; durable install state written before progress callbacks; `myce add-model` materializes a loadable preset with provenance and progress. Token model is genuinely right: join_token = membership/visibility only (gates `/peer/health` only), rpc_token gates node/admission/registry/telemetry/catalog RPC, startup refuses join without rpc_token, constant-time comparisons throughout, rotation **and** revocation persisted as hashes with revoked-token rejection across restart, join URI never carries the rpc token. Signed UDP discovery (HMAC + nonce + expiry + replay rejection), malformed datagrams dropped without killing the scan, bounded scan deadline on an injected clock, shared cache scanner owns the listen port. Loopback tunnels reused while the address is stable; instance addresses rewritten to owner proxy paths; inbound Authorization stripped before reaching backends. No SSH outside smoke orchestration. Config secrets 0600.

---

## 6. Contracts / ports / test infrastructure (Doc 2)

### T1 — Port signatures have outgrown Doc 2 §2 without logged spec changes — MEDIUM / drift
- `AdmissionController.Offer(ctx, req, claim)` → `Offer(ctx, domain.AdmissionRequest)` (`ports/ports.go:69`).
- `Coordinator.Commit` returns `CommitOutcome{Decision, Lease}` not `domain.Lease`; `MarkRunning`/`Complete`/`Fail` added (`ports.go:92-100`).
- `NodeAgent.Load(ctx, Preset)` → `Load(ctx, LoadRequest)` plus `InspectModel` (sanctioned by §3.10) and `BeginRequest`/`EndRequest` (logged).
All semantically motivated, none logged as a signature change; Doc 2 §2's reference code no longer compiles against the real ports. Doc 3: "don't silently diverge."

### T2 — The spec's `orphaned` JobRecord status is unrepresentable — MEDIUM / drift
Doc 2 §2: `JobRecord.Status … queued | placing | running | done | failed | orphaned`. `domain.JobStatus` has no `orphaned` (grep-confirmed nowhere in the repo); recovery annotates a free-text `RecoveryNote` instead. Partition-stranded work is invisible to any registry query for the spec'd state.

### T3 — Missing compile-time `var _ ports.X` assertions where they matter most — MEDIUM / brittle
Doc 2 §2: "do this for every interface/impl pair." `node/http.go` `HTTPClient` implements `ports.JobStatusInspector` with no assertion — consumers discover it by **runtime type assertion** (`gateway/peer_directory.go:161`, `gateway/fleet.go:65`) on the Phase 6 recovery path that decides whether a dead peer's job gets re-run; also mock `PreemptForJob` and `cmd/mycelium/peer.go` `localPeerAgent.JobStatus` lack assertions. A signature change fails at runtime on a remote node instead of at compile time — the exact failure the rule exists to prevent.

### Test-infra lows
- **T4 (low/incomplete):** `ports.Store` and `ports.ModelRegistry` are dead contracts — no impl, no mock anywhere.
- **T5 (low/incomplete):** several added ports (`HostDetector`, `EngineDetector`, `PeerCatalogStager`, `ModelInventory`, `LocalityPlanStore`, `UnitResourceEstimator`) have no shared mock; notably `mocks.ResourceEstimator` doesn't implement `EstimateForUnit`, so the placer's unit-aware estimation branch can't be tested with the shared mock.
- **T6 (low/brittle):** mock estimator checks `ctx.Err()` **before** recording the call (`mocks/estimator.go:23-27`) — "was not called after cancel" assertions pass vacuously; inconsistent with the backend mock.
- **T7 (low/brittle):** real-time waits in the fast contract tier — `time.Second` watchdogs in `federation_conformance.go` (Doc 2 §1.4: a test that calls `time.Sleep`/real time "is a bug"); flake risk for the SQLite registry under `-race` on loaded CI.
- **T8 (low/brittle):** FakeClock zero/negative-duration timers never fire without an explicit `Advance` (real `time.NewTimer(0)` fires immediately) — a computed-remaining-timeout that reaches ≤0 blocks a test forever.
- **T9 (low/brittle):** the Allocator conformance suite doesn't touch the fit math (mock `Fits` is a configured boolean ignoring `existing`) — the suite that exists to catch mock/real divergence can't, for the most safety-critical contract. Mitigated: e2e tests wire the real allocator.
- **T10 (low/drift):** layout drift — domain is 4 files vs the spec's 9, ports is one file vs 11, `internal/safeid` exists undocumented (unlike `internal/hardware`, which got a PROPOSED SPEC CHANGE); LAN discovery/overlay/rpc live in `internal/membership`/`cmd` instead of `internal/peer` (`discovery_lan.go`, `rpc.go` per Doc 1 §5); `internal/scheduler` has no `selector.go`/`scheduler.go` (functionality present elsewhere). Navigation-by-path per AGENTS.md breaks for these.

**Verified conformant:** domain is genuinely logic-free and std-lib-only with every spec'd type/enum/error verbatim; **zero** `time.Now`/`Sleep`/`After`/`NewTimer`/`WithTimeout` in production `internal/` outside the sanctioned clock wrapper; zero `init()` wiring or mutable singletons; all four mandated conformance suites exist and run against mock + real (plus five beyond spec); mocks are hand-written, recording, failure-injecting, self-tested; FakeClock is correct and actually fixes a latent stopped-timer bug in Doc 2 §6's own reference; fixtures match the spec including the two named self-tests; `trace.Trace.Do` is line-for-line the spec.

---

## 7. CLI / daemon wiring / repo conformance (Doc 1 §4–§5, Doc 3 scaffold)

### O1 — The three skill files are stubs, not the Doc 1 §4 content — MEDIUM-HIGH / incomplete
`skills/backend-adapters.md` (13 lines), `kv-estimation.md` (12), `scheduler-model.md` (21) vs §4's requirements: worked llama.cpp/vLLM/MLX/custom adapter examples (command template, health path, env, capability map); backend-aware fit math + how to call gguf-parser + how vLLM/SGLang reservation maps to a claim; §3 condensed. None of that is present — a handful of rule bullets each. Unlogged. The encoded know-how Phase 0 was supposed to bank for later agents doesn't exist.

### O2 — `mycelium config init` / `bootstrap --apply` silently clobber an existing peer identity — MEDIUM / bug
`cmd/mycelium/config_init.go:79-87`, `bootstrap.go:77`: a fresh random peer ID + join/rpc/gateway tokens unconditionally overwrite `peer.json` — no existence check, no `--force`, no confirmation. Re-running init on a configured fleet member silently rotates identity and all credentials; discovery/registry records and other peers' seeds break with no warning. Anti-§3.11 for a destructive op.

### O3 — Group telemetry sync pulls **all history every interval** with a 16MB ceiling — MEDIUM / brittle
`cmd/mycelium/peer.go:2190-2241`: each optimizer tick (60s default), the selected compute peer fetches every run metric and every session sample (no since-cursor, no limit) from every reachable peer and re-records them. O(full history) network + SQLite churn per round; once any peer's history serializes past the 16MB peer-RPC body cap, the round fails loudly **every interval** and group analysis permanently stops importing from that peer.

### O4 — `test/integration/` is empty; `test/unit/` holds one file — MEDIUM / drift (unlogged)
Doc 2 §4 defines `integration/` as a distinct tier ("2+ real modules wired, external deps mocked, <10s, any module change"). Integration-style tests live in-package (pragmatic for the coverage bars) but the tier doesn't exist as invocable structure, and unlike the conformance-coverage decision, this restructuring was never logged.

### CLI/wiring lows
- **O5 (low/incomplete):** no `myce status` / `myce drain` (Doc 1 §5 names both); `NodeDraining`/`NodeMaintenance` are honored if set but **nothing can set them** — a box can't be drained before a reboot except by killing the peer.
- **O6 (low/bug):** `runPeer` error returns after `storesqlite.Open` skip `store.Close()`; store never closed on graceful shutdown either.
- **O7 (low/bug):** flag asymmetry — `--max-util 0`, `--disk-min-free-ratio 0`, `--load-timeout-ms 0` are silently ignored (sentinel-zero) while `--vram-mb` got a proper optional-flag wrapper.
- **O8 (low/brittle):** default peer ID `"peer_local"` — two hand-configured LAN peers collide in discovery/registry; validation doesn't reject the default when `join_token` is set.
- **O9 (low/brittle):** `controlcli` uses `http.DefaultClient`, contra the repo's own logged decision to keep peer-control HTTP off shared transports; `smokegate` uses a default-buffer `bufio.Scanner` (one >64KB `go test -json` line fails a 2h smoke spuriously).
- **O10 (low/hygiene):** `AGENTS.md` says "13 locked design decisions" (Doc 1 §6 has D1–D18); empty `internal/sharding/` directory — the literal name of hard-excluded D17 — and empty `internal/backends/procrunner/`; delete both.

**Verified conformant:** dispatch shape is right — `mycelium` (bare/`run`) = peer daemon, `ctl` = control surface, `myce` is a thin shim over shared `cmd/internal/controlcli`, rejected `server`/`node` subcommands fail loudly with the right message, `--join` routes to the peer; one listener mounts gateway + node/admission + peer health + registry + telemetry + catalog + admin; compute toggle honored; signal-driven shutdown unloads every owned instance; zero `init()` anywhere; every background loop is constructor-injected and Clock-driven; smoke fully isolated behind `//go:build smoke` (verified: `go test ./... -race` green with nothing powered on); `go.mod` is Go 1.23.x with the single logged sqlite dep and no libp2p; AGENTS.md contains every Doc 3 "Also produce" required statement; all Doc 1 §5 packages exist; Makefile smoke targets use `-count=1` and smokegate-required named tests.

---

## 8. Federation / optimizer (Phases 5–6, §3.7, §3.12)

*(D2 — rescue never re-executes — and O3 — unbounded telemetry sync — also belong to this layer.)*

### P1 — Registry LWW merge orders Fence **before** UpdatedAt; rescued jobs can never overwrite the dead coordinator's record → completed jobs get re-run — HIGH / bug + drift
**Where:** `internal/peer/registry.go:165-176` (`newerRecord`: `if next.Fence != current.Fence { return next.Fence > current.Fence }` runs before the UpdatedAt comparison — verified), duplicated in `internal/store/sqlite/store.go:895-906`; rescue write path `internal/peer/coordinator.go:131` (ClaimJob records fence 0).
**Spec:** DECISIONS 2026-05-29 logs the opposite order: "ordered by `UpdatedAt`, **then** fence, then a deterministic tie-break." Doc 3 Phase 6 hard requirement: "a stale row can only cause a redundant owner re-check"; §3.12: eventually-consistent registry.
**Code:** fences are **per-owner** monotonic counters, so comparing them across owners is meaningless — yet fence dominates the merge. Empirically confirmed by the auditing agent with a throwaway test: dead peer's record `{running, node-dead, fence 12}`; the rescuer re-coordinates (ClaimJob fence 0, commit on a fresh owner at fence 3, Complete at fence 3) and **every one of its Puts is silently dropped**. The registry permanently says the job is `running` on the dead node. Any later `RecoverPeer` for that peer (another observer, a restart — heartbeat state is in-memory — or a flapping peer) re-rescues and **re-runs work that already completed**, defeating the owner-double-check design. `recovery.go:234,247` (`rec.Fence++`) shows the bump-to-win requirement was known; the coordinator's rescue-path records don't do it. The wrong ordering is baked into `registry_test.go:45-51`.
**Impact:** registry never converges after a rescue; duplicate execution of completed jobs — exactly the corruption class D16 promises can't happen.

### P2 — Offer-stage replan exhaustion records the job `failed` (terminal) while the scheduler re-queues it → a dead coordinator's queued job is silently lost — HIGH / bug
**Where:** `internal/peer/coordinator.go:224` (offer-stage exhaustion → `recordStep(JobFailed, fence 0)` — verified) vs `:241-245` (commit-stage correctly records `JobQueued` for replanable errors); `internal/scheduler/service.go:237-246` (`coordinatedQueueErr` enqueues the same job locally on `ErrNoFit`/`ErrStaleFence`).
**Spec:** DECISIONS 2026-05-29: "exhausted replans **leave the job queued**"; Doc 3: "on exhaustion the job queues rather than force-commits."
**Code:** terminal-status dominance in the merge (`registry.go:166-168`) then blocks all later non-terminal updates for that job. If the coordinator dies, recovery's `unfinished()` filter sees `failed` (terminal) and never rescues the actually-queued job — the local-only-state loss D16 exists to prevent. The commit stage is correct and tested; the offer stage is the asymmetric hole.

### P3 — One bad record wedges the entire recovery pass for a dead peer, retried forever — MEDIUM-HIGH / bug
**Where:** `internal/peer/recovery.go:54-61,84-88` (first decide/rescue error aborts `RecoverPeer`); `cmd/mycelium/peer.go:1929-1940` (OnDead error → heartbeat leaves the peer not-dead → retried every 5s tick, forever). Trigger that exists today: `/registry/snapshot` serves records **unredacted** on the pull path (`peer.go:937-957`) — redaction happens only on push — so an encrypted private rescue payload replicates with `PayloadRedacted=false` and the keyless rescuer's decode fails, blocking rescue of *every other* unfinished job of that dead peer indefinitely. Latent today (private handling rejected at ingress per 2026-06-04), but any future decode failure has the same fleet-level wedging effect.

### P4 — A partitioned-but-alive coordinator's running job is killed at the live owner and re-run — MEDIUM / bug (design gap)
**Where:** `internal/peer/recovery.go:186-199` + `:77-83`: when the owner reports the job unfinished **with a live lease held**, the rescuer **releases another coordinator's active lease** and resubmits (the test asserts this is intended). §3.12 defines the owner double-check only for the "it finished" case; "partitions degrade, never corrupt." Under an asymmetric partition (rescuer can't reach the coordinator; both reach the owner), a healthy in-flight job is evicted mid-stream and duplicated. Bounded — the release goes through owner authority, so no double-booking — but it destroys live work, and the tradeoff is not logged.

### P5 — No rescue arbitration: every live peer recovers the same dead peer → up to N−1 duplicate re-runs — MEDIUM / bug
Heartbeat + recovery run on every fleet-joined peer; after rescuer 1 releases the lease, rescuer 2's `LeaseForJob` returns not-found, which maps to rescue-now (not skip). No claim/CAS on `JobRecord.Coordinator` (and P1 makes rescue records invisible anyway). A 4-peer fleet losing one peer re-runs each unfinished job up to 3×. Fast-tier tests only ever exercise a single rescuer.

### P6 — The Phase 5 gate's flagship scenario cannot be produced by the engine; the test was reshaped to fit — MEDIUM / drift
**Where:** `internal/optimizer/recommend.go:114-121`: `target = max(avg×1.5, p95, lifetime_max)` — the engine never recommends below the observed tail. Doc 3 Phase 5 gate check (1): "avg 4k, p95 12k against a 16k cap, with 6k already used by another project produces a recommendation of ~6k" (and Doc 1 §3.7's example explicitly recommends below p95/lifetime-max, with a tradeoff). With this clamp the SSOT scenario yields **no recommendation**, and `recommend_test.go:58-66` asserts exactly that ("produces nothing"). The never-below-tail clamp is defensible engineering but is gate-check dilution that was never logged (the logged decision covers only "average-with-headroom snapped to a shared context").

### P7 — Recommendations lack the §3.7 `tradeoff` field — MEDIUM / drift + incomplete
`Recommendation`/`domain.RecommendationRecord` carry observed stats + rationale + fit-proof evidence (good), but `tradeoff` — the cost side of the §3.7 example ("requests above 6000 tokens (≈4%/day) trigger a reactive requeue") — appears nowhere in the repo (grep: zero hits). Related to P6: with the never-below-tail clamp there is by construction nothing to trade off.

### P8 — Group-analysis "at most one round per interval" is best-effort under clock skew / divergent fleet views — MEDIUM / brittle
`internal/telemetry/group.go:11-43`: slot = wallclock/interval mod sorted ready-node list, evaluated by each peer with its own clock and its own fleet snapshot. Clock skew across a slot boundary, or two peers disagreeing on the ready list, → two peers both believe they're "the one" and both run. Mitigations are real (compute-only, deterministic recommendation IDs upsert-converge, no stuck state), so degradation is duplicate CPU never corruption — but the stated hard requirement is not actually guaranteed.

### P9 — A fleet-joined peer alone in a partition refuses ALL new work — MEDIUM / brittle (availability)
`internal/peer/registry_replicator.go:84-89`: `PushRecord` errors when zero peers are reachable; `ClaimJob` propagates it; submission fails ("no reachable registry peer for rescue copy") — even for purely local compute. Deliberate fail-loud durability (tested), and it honors D16 for *accepted* work, but it trades total submission availability under partition against §3.12's "capacity simply shrinks" — a spec-level tension not logged in DECISIONS. A two-peer fleet where one machine reboots = the survivor can't accept anything.

### Federation/optimizer lows
- **P10 (med-low/bug):** watch-driven push replication dies permanently after watcher eviction — the registry closes slow watcher channels (>128 buffered writes), the consumer returns silently on `!ok` with no re-subscribe and no log (`peer.go:1869-1871`); replication freshness silently degrades to the periodic sync for the life of the process.
- **P11 (low/brittle):** one malformed peer advertisement aborts the whole heartbeat tick (`heartbeat.go:58-60`) — misses aren't counted for *any* peer while the bad advert persists, stalling dead-peer detection fleet-wide.
- **P12 (low/brittle):** vLLM reload cost ignores "within the launched max" (§3.7/D4): consolidating *above* a vLLM instance's launched `max-model-len` is a real relaunch but still priced ≈free (`presets.go:56-65` — nothing carries the launched max).
- **P13 (low/brittle):** commit after the 30s offer TTL returns a generic (non-replanable) error → `JobFailed` instead of a re-plan (`admission.go:339-346`); with P2's terminal dominance the failure is sticky.
- **P14 (low/brittle):** every coordinator record step synchronously pushes to every reachable peer on the submission hot path (O(fleet)×O(steps) RPCs per job); the registry never prunes terminal records and `SyncOnce` pushes the full snapshot each interval — unbounded growth over fleet lifetime.
- **P15 (low/weakened test):** e2e Phase 6 check 6 uses `MaxMisses:1` + one tick (the heartbeat counts ticks, not clock time) and a recording-stub Rescue func — re-coordination of a dead peer's running job onto a live peer is never exercised end-to-end in the fast tier, which is exactly where P1 hides.

**Verified conformant:** coordinator does ClaimJob→registry, parallel snapshot fan-out, peer-agnostic Placer, owner Offer/Commit with the offer's fence, bounded deterministic replans (no sleeps), never force-commits, commit-stage exhaustion → queued + loud owner error. No self-preference (e2e-proven with the real Placer). Owner admission persists offers/leases/fences with startup reconcile, so a restarted owner cannot accept stale plans; wire codes preserve typed `ErrNoFit`/`ErrStaleFence` both directions. Registry is leaderless, authenticated, holds the full rescue envelope, survives restart, bounded watcher buffers, no goroutine leaks. Heartbeat: 5s/3-miss, exactly-once OnDead per observer, probe resets alive. Recovery: owner double-check via read-only inspectors; finished-at-owner skipped; unreachable owner → partition-evidence note, no rescue. Partition safety: failed snapshots shrink capacity (at the RPC layer — see W3 for the tunnel-setup exception); placer filters unreachable nodes. Group analysis runs only on compute-on peers, no permanent owner, mid-round death costs one interval. Optimizer is deterministic stats only (no ML), every recommendation carries observed stats + human-readable rationale + fit proof, auto-apply strictly per-project-gated default-off, consolidation math asserted, engine/benchmark recommendations advisory with explicit manual apply. All seven Phase 6 behavioral checks exist; check 2 (owner race) is notably honest — real `node.Admission` behind a barrier, exactly one concurrent commit wins, `max_util` enforced by the real allocator.

---

## 9. Roadmap creep & logged drift (built ahead of, or divergent from, the SSOT — all logged in DECISIONS)

Reported per Doc 3's rule that even logged divergence is divergence until the SSOT is revised:

| Item | Status | Notes |
|---|---|---|
| **Reverse benchmarking** (`internal/bench`, ~1.6k-line fleet runner, `myce benchmark`) | Built and **active**; §3.9/D13 say roadmap, "not built in MVP" | Facts-only constraints verified: children run background priority, `user_pick` only ever user-supplied, no scorer/judge/agent hooks anywhere |
| **Multi-user / privacy machinery** (SubmitterPolicy in owner admission, HandlingClass, AES-GCM rescue encryption, replication redaction) | Built, disabled by config validation (2026-06-04) | Dead-in-production roadmap code in the hottest authority path; any non-empty `Submitter` is guaranteed-rejected since no policy can exist |
| **Sticky routing** (`gateway/sticky.go`) | Built, parsed at ingress, used by nothing | §9: "deliberately not in the MVP gateway"; delete or quarantine |
| **Auto engine/param selection** (`optimizer/engine.go`, `engine/detector.go`) | Built and **active** (advisory-only, manual apply) | §9 roadmap item; respects the D13 facts-only boundary |
| **Cross-NAT overlay** | Generic impl + memory backend; production config rejects loudly | See C7 |
| **`internal/hardware` + `HardwareDetector`** | PROPOSED SPEC CHANGE (2026-06-02) | Coherent and tested; awaiting SSOT revision |
| **Disk headroom as placement constraint** | PROPOSED SPEC CHANGE (2026-06-02) | Reshapes Node (§3.8) and the filter pipeline (§3.3); note S-class side effect: nodes reporting no disk facts are unconditionally unschedulable (`scheduler/disk.go:9-12`) |
| **Cross-coordinator pre-send negotiation** | Added, then **fully removed** (2026-06-01 supersession) | Verified gone — no remaining negotiation code |

**Rejected-design scan (clean):** no leader/election/consensus-log code; no SSH in product paths (smoke orchestration only, logged); no cross-machine sharding (the only `--tensor-split` is intra-node, which §3.10 requires); no naive FIFO; no model-as-unit; no weights-only fit; no hard-default preemption; no in-process engines; no Docker-based Mac workers. Zero `TODO`/`FIXME`/`HACK` markers in the entire Go tree.

---

## 10. Priority order (what to fix first)

1. **Make the gate green again** — G1 (vet copylocks), G2 (cover `releaseErrAllowsOwnerFallback` — and replace its substring matching with typed errors), G3 (llamacpp stop-path tests). The repo's whole methodology rests on "never advance on a red gate," and it's red.
2. **P1 + P2** — fix the registry LWW merge to UpdatedAt-then-fence (per the project's own logged decision) or bump fences on rescue-path records, and record `queued` (not `failed`) on offer-stage replan exhaustion. Together these are the difference between "a stale row causes a redundant check" and "completed jobs get re-run / queued jobs get silently lost."
3. **D1** — re-place/re-queue idle preemption victims and strengthen the Phase 0 worked-example test to assert `Replacements` (it currently can't see the regression in the project's defining scenario).
4. **D2** — give queued/rescued jobs real semantics: either hold the HTTP request until drain executes it, or make drain/rescue actually replay the stored payload upstream and record the result. Today D16's rescue promise is structurally unfulfilled and queueing is client-visible failure.
5. **D4** — make `splitClaim` proportional to per-accelerator VRAM (matching `tensorSplit`), or launch with an even split; either way accounting and launch must agree (real `max_util` violation risk).
6. **D5 + W1** — check `ctx.Err()`/`context.Canceled` before classifying a stream/transport error as instance failure; emit the promised SSE error event after provider start.
7. **D6/D7** — group-kill on the live-stop escalation path (the PGID is already there), wire `StopGracePeriod` for vLLM/MLX, add a drain deadline to `Unload`, and TTL/reconcile in-flight counts.
8. **D8** — verify staged artifact size/digest before the commit rename; remove the partial file on copy error.
9. **D3** — restore the per-request control surface for authenticated callers (or at minimum the loud rejection of `X-Myc-Handling: private`), and log whatever the decision is.
10. **D9** — single-instance guard (pidfile/flock) before the reaper runs; run cleanup when `ListenAndServe` fails.
11. **G4** — re-enable the CI workflow or log a ratified spec change; G1–G3 on main are the proof it's needed.

Everything else (Mediums and Lows above) is real but second-order: recovery wedging and duplicate rescue (P3–P5), reservations semantics (S1–S3), aging reset (S4), 30-min lease expiry (S8), stale memory baseline (S9), cold-load double-count (S10), peer-directory all-or-nothing snapshot (W3), telemetry-on-the-hot-path (W5), partition submission availability (P9), the Phase 5 gate dilution (P6–P7), nonce/tmp leaks (C1–C2), the unlogged port-signature drift (T1–T3), skill-file stubs (O1), and config-init clobbering (O2) are the next tranche.
