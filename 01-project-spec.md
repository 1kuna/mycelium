# Mycelium — Project Spec (Document 1 of 3)

> **Mycelium** — a hardware-aware inference control plane for a heterogeneous home/lab fleet. Binary: `mycelium` — one role, a **peer**; every peer is equal (no permanent server/leader). A peer carries a **`compute` toggle (on/off)**: compute-on peers own accelerators, run backends, and share telemetry analysis; any peer (compute on or off) can submit work and coordinate a job. Control CLI: `myce`; decision headers: `X-Myc-*`. Note: lowercase "fleet" throughout is the common noun for your set of machines (Mycelium is the network across the fleet), not a second product name.

This is the "what": architecture, the core innovation, the state model with concrete data shapes, the repo layout, the design decisions (with rejected alternatives), a data-flow walkthrough, and the MVP phases. Document 2 (testing & modularity architecture) and Document 3 (the gated development guide) build on this.

---

## 1. What this is

Mycelium is a **hardware-aware inference control plane** for one person (or a small lab) running many heterogeneous local machines — NVIDIA, Intel Arc, Apple Silicon, AMD, mixed. Apps submit **intent** ("run a chat completion, interactive priority, for project X") and Mycelium decides which machine, which backend, which model preset, whether a suitable instance is already warm, whether to load or evict or queue, and how to honor the caller's speed and priority preferences. Apps never learn that the Spark was busy or that the B70 needed SYCL flags.

It is explicitly **not** a new inference engine. llama.cpp, vLLM, SGLang, MLX remain the engines. Mycelium owns the layer above them: the queue, priority, placement, resource leases, model lifecycle, backend launch profiles, node state, health, and the frontend-visible status of every job.

**North-star UX:** adding a machine feels like a node joining a hive (one command). Adding a model is a one-liner. Using the fleet is one OpenAI/Anthropic-compatible endpoint that hides all the hardware.

### Hard constraints

- **Single static Go binary**, one role: a **peer** (with `compute` on/off). No second runtime in the control plane, no separate server/worker builds. (Inference engines themselves remain whatever they are — Go binaries, Python containers, native subprocesses — Mycelium supervises them as processes; see Design Decision D2.)
- **Developed and tested primarily on macOS** with no specialty hardware powered on. Every node/backend/GPU sits behind a mockable contract; real hardware appears only in `smoke/` tests. The local dev Mac (and a second peer as a second real peer) prove the whole system end-to-end. This is a load-bearing constraint and drives Document 2.
- **CLI-subscription coding agents** (Codex, Claude Code) build this. The docs are the blueprint; the agent works phase by phase against runnable gates.
- **Local-first.** No assumed cloud, no assumed API costs. Anthropic/OpenAI-compatible surface is for *local* backends; cloud overflow is out of scope for MVP.
- **Heterogeneous and OS-spanning** from the design, even though the first proving ground is the Spark + B70 + a few Macs. Generalization is a goal, not a hard MVP gate.

### Why build instead of adopt (the short version)

Four existing projects were studied at the source level (GPUStack, llama-swap, Olla, LocalAI). Each is a strong **methods reference** for one layer, none is an adoptable base, and — critically — the combination of **request-level priority + preemption + cross-modality resource leases + fit-forced reallocation** that defines Mycelium's value is absent from all four at the code level. GPUStack queues *placements* not requests; llama-swap sheds-or-coexists on a single host; Olla does endpoint-tier priority only; LocalAI's newest distributed "SmartRouter" is reactive request-time routing, not a desired-state planner. The scheduler is the thing being invented; everything else is harvested. Layer-by-layer provenance is in §6 and inline throughout.

---

## 2. Architecture overview

```
   apps / agents (OpenAI & Anthropic clients, myce) ── submit to ANY peer's endpoint ──┐
                                                                                        │
   Every machine runs ONE `mycelium` daemon = a PEER. No leader, no required host.      │
   The peer you submit to becomes that job's COORDINATOR (role lasts one job).          │
                                                                                        ▼
 ┌─────────────────────── PEER (e.g. your local dev Mac, compute:off) ───────────────────────┐
 │  GATEWAY            ingress (OpenAI/Anthropic), intent+project defaults, X-Myc-*      │
 │  COORDINATOR        for jobs submitted here: snapshot the fleet (ask peers in         │
 │                     parallel) → run PLACER (the brain: filter→select→score) → pick    │
 │                     the best host → ask that host's owner to commit → relay results   │
 │  PLACER             pure placement logic (identical code every peer runs)             │
 │  TELEMETRY (local)  this peer's own run-metrics                                       │
 └───────────────┬──────────────────────────────┬───────────────────────────────────────┘
                 │ authenticated peer RPC        │ replicated, eventually-consistent
                 │ (LAN auto-discovery,          │ JOB REGISTRY + HEARTBEATS
                 │  no SSH)                       │ {job_id, coordinator, owner, status,
                 ▼                                ▼  request} → survive a peer's death
 ┌──────────── PEER (compute:on — Spark / 4090 box / second peer / …) ─────────────────────┐
 │  OWNER AUTHORITY    the SINGLE atomic authority for THIS machine's hardware.          │
 │                     a coordinator DECIDES; the owner COMMITS. local transactional     │
 │                     lease store serializes commits; stale plans rejected (versioned   │
 │                     compare-and-swap / fence). max_util + oom_severity enforced here. │
 │  NODE LIFECYCLE     load / ready-gate / stop / coexist; reaper; lease enforcement;    │
 │                     loading-state SSE; emits heartbeat + run-metrics                  │
 │  BACKEND ADAPTERS   llama.cpp │ vLLM │ MLX │ custom (exec/container)                   │
 │                     ONE model loads fully on ONE machine — job distribution,          │
 │                     never model distribution                                          │
 │  TELEMETRY (local)  this peer's own run-metrics; compute peers analyze AS A GROUP     │
 │                     (periodic, no permanent owner) — keeps that CPU off thin peers    │
 └───────────────────────────────────────────────────────────────────────────────────────┘
        … all peers symmetric; catalog/presets, optimizer, membership live on every peer …
```

Every machine runs one `mycelium` daemon — a peer. There is **no server and no leader**: you submit to whichever peer you like (its own address is your endpoint, like pointing a client at one LM Studio), that peer **coordinates** that one job — gathering a fleet snapshot by asking the others in parallel, running the placer brain, and asking the chosen host to commit — and the **resource-owning peer is the sole authority** for committing leases on its own hardware. Fleet state is each peer's local truth (asked on demand, lightly gossiped for visibility) plus a small replicated **job registry** for failure recovery. Backends are subprocesses each compute peer supervises. The CLI is `myce`.

---

## 3. Core innovation

Two subsystems are the non-trivial part and the reason Mycelium exists: the **scheduler + lease/allocation core**, and the **self-optimizing preset engine**. Everything else is well-trodden.

### 3.1 The resource model

- A **unit** is an allocatable accelerator or set of accelerators (one GPU; a multi-GPU group for a model that needs it; an Apple Silicon machine's unified-memory pool). Units, not machines, are what the scheduler allocates.
- A unit holds **multiple model instances at once** as long as `Σ(weights) + Σ(KV reservation at expected context × concurrency) ≤ usable_vram`, where `usable_vram = vram_total × max_util − reserved_headroom`. Co-residency is the **default**, not a special case. (Example: a free Spark hosting 3×9B chat + 1B ASR + 27B analysis simultaneously, KV permitting.)
- The loadable thing is a **preset**, not a "model": `(model, context_length, quant, backend, launch flags)`. Two projects wanting the same model at different context caps are two presets → two loads → no shared batching. Snap them to a shared context value and they collapse into one warm instance batching both. **That collapse is the central optimization lever** (see §3.4).
- **Fit is KV-aware.** Weights alone are not the constraint; KV at the requested context and concurrency is what actually overflows VRAM. Estimation is backend-aware (GGUF via gguf-parser preflight; vLLM/SGLang via their own `gpu-memory-utilization` reservation as the claim).

### 3.2 Per-machine safety (inviolable)

- Each node carries a **`max_util`** ceiling (e.g. 0.90). It is a **hard placement constraint** — never exceeded, regardless of priority or preemption. You set it; the scheduler obeys it.
- Each node carries an **`oom_severity`** tag because the failure modes differ:
  - `catastrophic` (e.g. the DGX Spark — OOM forces a power cycle): the scheduler keeps an extra margin under `max_util` **and refuses to stack concurrent model *loads*** on that unit (load-time spikes are where you blow past the line).
  - `soft` (e.g. the 4090 box — OOM crashes only the offending program): the scheduler may run closer to the ceiling.
- You set the ceiling; severity tunes how paranoid the scheduler is *beneath* it.
- **Enforcement is local to the owner.** `max_util` and `oom_severity` are checked and committed by the peer that owns the hardware, at commit time, in its own transactional store (§3.12) — a remote coordinator can *propose* a placement but can never make a node exceed its own ceiling.

### 3.3 The scheduler

Borrows GPUStack's **filter → select → score** structure and LocalAI SmartRouter's **runtime heuristics** (in-flight-aware replica choice, soft VRAM reservation, LRU-ish eviction), then adds the priority/preemption/lease layer that neither has.

Pipeline per job:

1. **Resolve preset** — apply the project/request context cap, quant, backend; produce the concrete preset to run.
2. **Estimate claim** — `weights + KV` for that preset at expected context/concurrency (backend-aware).
3. **Filter units** — drop units that can't satisfy `max_util` (with `oom_severity` margin), label/capability selectors, or fit.
4. **Select candidates** honoring **speed preference** (§3.5): `throughput` → pack onto units with compatible warm instances and batch; `latency` → grab the fastest free unit, or **dedicate** a unit to this one job; `auto` → fastest-and-most-available.
5. **Score** candidates (fit tightness for packing vs spread, warm-instance/model-file locality bonus, speed-pref alignment).
6. **Admit / queue / preempt** under the active **preemption mode** (§3.6). If a slot is needed and none is free: soft → queue; hard (opt-in) → displace a lower-priority running instance, which then re-places elsewhere if it fits or re-queues at its own priority.

### 3.4 Reservation primitives (opt-in, off by default)

Out of the box, no reservations and **soft** preemption: priority just orders the queue, the next freed slot goes to the highest-priority job that fits, nothing running is ever killed. The honest cost of that default: an urgent job with nothing reserved **waits for a slot**. To make it never wait, opt into one of:

- **Reserved headroom** — keep X GB or N% of a unit free so interactive work always lands without a fight.
- **Pinned model** — a preset stays warm on a specific unit, exempt from eviction ("always loaded, just in case").

### 3.5 Speed preference (the knob none of the four have)

Each job carries a speed preference; the scheduler honors it within fit/priority/reservation:

- **`throughput`** (default) — pack compatible presets onto units, batch compatible requests, maximize utilization.
- **`latency`** — isolate or route to the fastest free unit; or **dedicate** a unit to this one job even if that unit is slower. The subtle win: parking a job on the "only fits this" machine can be *correct* precisely because it keeps the fast units free for batching everything else.
- **`auto`** — pick fastest-and-most-available.

### 3.6 Preemption ladder (unified mechanism)

When a slot is needed and none is free:

- **soft** (default) — wait for a natural slot / queue.
- **hard preempt** (opt-in, per project/request or "allow for interactive") — a higher-priority job displaces a lower-priority running instance now (in-flight drains cooperatively); displaced work re-places elsewhere if it fits, else re-queues at its own priority.
- **reservation** (opt-in) — guaranteed interactive latency without relying on preemption at all.
- **fit-forced reallocation** (120B → Spark) runs the *same* priority test with the candidate set pinned to the one unit that fits: it displaces occupants only if it outranks them under the active mode, else it queues. No separate "borrow/reclaim" mechanism — it is allocation + the same preemption test.

Worked example: Spark free → 3 interactive 9B + 1B ASR + 27B background analysis co-reside (KV permitting). A 120B interactive job lands, nothing reserved, default soft mode but the job is flagged hard-for-interactive → hard-preempt the lowest-priority occupant (27B background), re-place it on a 4090 box if it fits else re-queue, 120B takes the Spark. Had the 27B carried `latency`, it would likely have been dedicated to its own box up front, leaving the Spark free to begin with.

### 3.7 The self-optimizing preset engine

The optimizer's objective: **minimize (reload/switch frequency + preset fragmentation) subject to fit, per-machine cap, and each project's latency target**, weighed against tokens/sec and load wall-clock. Fewer distinct presets at shared context values → more shared warm instances → more batching headroom; but a larger shared context reserves more KV → fewer co-resident instances → less batching. The optimizer trades these off; it does not blindly consolidate.

**Backend-aware reload cost** is essential: changing context = a reload for **llama.cpp/GGUF** (context fixed at load — high cost), but **vLLM** sets `max-model-len` at launch and pages KV under it without reload (≈ zero cost within the launched max). The cost model must know this per backend.

Two halves:

- **Reactive** (resilience, built early): a request overflows its preset's context cap → catch it, **immediately requeue** with a larger preset (observed-max × a buffer, snapped to the next shared value that fits). Per-backend error classification ("context overflow looks different on each engine") is the fiddly part.
- **Proactive** (the smart part, later phase): watch the rolling per-project context distribution; on a sustained shift (today's avg 20k vs the usual 6k) **recommend** an adjustment, or auto-apply if the toggle is on. Deterministic statistics + anomaly detection on telemetry — not LLM judgment — which is also what keeps it debuggable.

Recommendation example the engine should be able to emit:

```json
{
  "type": "context_cap_recommendation",
  "project": "project-A",
  "observed": { "window": "7d", "avg_tokens": 4000, "p95_tokens": 12000, "lifetime_max": 16000 },
  "current_cap": 16000,
  "recommended_cap": 6000,
  "rationale": "p95 is 12000; 6000 already used by project-B, so sharing the cap collapses two presets into one warm instance and removes ~14 reloads/day. Estimated batching headroom +1 concurrent 9B on the Spark.",
  "tradeoff": "requests above 6000 tokens (≈4%/day) trigger a reactive requeue to a larger preset.",
  "auto_apply": false
}
```

**Auto-apply gate:** recommend by default; auto-apply behind a per-project toggle that stays **off** until observed behaving in practice.

### 3.8 Concrete data shapes

These are the agent's reference. Shown as JSON; Go structs mirror them (Document 2 has the typed contracts).

**Job (the intent submitted to whichever peer you hit; that peer coordinates it):**

```json
{
  "id": "job_01HF...",
  "task_type": "chat",                       // chat | completion | embedding | vision | image | asr | diarization | rerank | tts
  "model": "qwen2.5-9b-instruct",            // logical model alias OR an explicit preset id
  "preset": null,                            // optional: pin to an exact preset
  "project": "project-A",
  "priority": "interactive",                 // interactive | normal | background
  "speed_pref": "auto",                      // throughput | latency | auto  (default throughput)
  "context_request": 8000,                   // optional override of project/preset cap
  "preemption": "inherit",                   // inherit | soft | hard_for_interactive | hard
  "streaming": true,
  "deadline_ms": null,                       // optional; informs urgency/aging
  "benchmark": null,                         // see §3.9 (roadmap); present => fan-out job
  "status": "queued",                        // queued | placing | loading | running | done | preempted | failed
  "placement": null                          // filled once admitted (see PlacementDecision)
}
```

**Node descriptor (each peer's own source of truth for its hardware; shared via snapshot):**

```json
{
  "id": "node_spark",
  "name": "dgx-spark",
  "address": "127.0.0.1:51847",              // the peer's reachable LAN address (host:port)
  "os": "linux",
  "labels": { "gpu.vendor": "nvidia", "gpu.kind": "gb10", "memory.class": "huge" },
  "max_util": 0.90,                          // hard ceiling
  "oom_severity": "catastrophic",            // catastrophic | soft
  "accelerators": [
    {
      "index": 0, "vendor": "nvidia", "kind": "gb10",
      "vram_total_mb": 131072, "vram_used_mb": 22000,
      "unified_memory": true,                // unified => one pressure domain w/ host RAM
      "compute_capability": "9.0", "arch_family": "blackwell"
    }
  ],
  "unified_memory": true,
  "speed_class": { "tokens_per_sec_ref": 145, "source": "probe", "probed_at": "..." },
  "status": "ready",                         // ready | maintenance | draining | unreachable
  "heartbeat_at": "2026-05-28T21:40:00Z"
}
```

**Preset (the loadable unit):**

```json
{
  "id": "preset_qwen9b_ctx8k_q4_llamacpp",
  "model_ref": "qwen2.5-9b-instruct",
  "backend": "llamacpp",
  "context_length": 8000,
  "quant": "Q4_K_M",
  "capabilities": ["chat"],
  "launch_profile": "llamacpp-cuda",         // resolves to a backend command template
  "est_weights_mb": 5600,
  "kv_per_token_mb": 0.18                     // for KV reservation math
}
```

**PlacementDecision + trace (what the scheduler produced, and why — observability):**

```json
{
  "job_id": "job_01HF...",
  "instance_id": "inst_7c2",
  "node_id": "node_spark",
  "accelerator_set": [0],
  "claim": { "weights_mb": 5600, "kv_reserved_mb": 1476 },
  "action": "placed_on_warm_instance",       // placed_on_warm_instance | loaded_new | queued | hard_preempted_then_loaded | dedicated_unit
  "speed_pref_applied": "auto",
  "trace": [
    { "step": "estimate",  "result": "weights=5600MB kv=1476MB @ctx8000" },
    { "step": "filter",    "kept": ["node_spark","node_4090a"], "dropped": { "node_4070ti": "fit", "node_b70": "label.vendor" } },
    { "step": "select",    "candidates": ["spark:warm-qwen9b", "4090a:cold"], "speed_pref": "auto" },
    { "step": "score",     "winner": "spark:warm-qwen9b", "reason": "warm instance + most available" },
    { "step": "admit",     "result": "batched onto warm instance, no eviction" }
  ]
}
```

**BenchmarkResult (roadmap; one row per model in a fan-out):**

```json
{
  "run_id": "bench_01HG...",
  "model": "qwen2.5-9b-instruct",
  "preset": "preset_qwen9b_ctx8k_q4_llamacpp",
  "node_id": "node_4090a",
  "output": "…model output…",
  "metrics": { "tokens_per_sec": 88.4, "ttft_ms": 210, "load_wall_clock_ms": 4200, "peak_vram_mb": 7100 },
  "user_pick": null,                         // optional: the user may mark a winner after reviewing the files; Mycelium never sets this
  "notes": null                              // optional free-form note the user can attach
}
```

### 3.9 Reverse benchmarking (roadmap — designed-for, not built in MVP)

A **benchmark job** is a parent Job whose `benchmark` field explodes one intent into a `(prompt × model)` matrix of background child jobs. Mycelium runs them across the fleet at low priority (never disrupting interactive work), and writes **one output file per model** alongside the objective per-run metrics it already records (tokens/sec, TTFT, load wall-clock, peak VRAM — facts, not opinions).

That is the whole v1 surface: the files and the facts. **Judging is entirely the user's, and entirely outside Mycelium.** They read the output files and pick a winner, or send the files to an agent of their own choosing — on their own volition. Mycelium does not orchestrate that hand-off, make a judging inference call, or hook into any agent harness. It ships no WER/exact-match/rubric/schema opinion either. (If a user has a hard criterion — Verso's WER against a reference transcript, say — they run it themselves on the files Mycelium produced. That is their own script; Mycelium ships no scoring interface, so there is no half-built extensibility hook to leave dangling.) Deliberately simple now; it can grow later if and when there is bandwidth.

Two payoffs: (1) discover which model can actually do the task, or rule the model out as the problem (the issue may be the prompt or pipeline, not the model); (2) if the user marks which models were acceptable, Mycelium knows those models and their sizes/metrics — and *that user-supplied pick* (never a Mycelium-computed score) can later feed the optimizer as a routing recommendation, e.g. "the 9B was judged sufficient for project A's task, stop routing it to the 27B."

**Forward-hooks paid now (near-free):** the `Job` model already supports parent/child fan-out, and telemetry already captures the per-run metrics from Phase 1. The only net-new build, deferred to roadmap, is writing the per-model output files and a comparison view (plus the optional user-pick field) — no judging logic, no agent integration.

### 3.10 Node-agent contract details (carried from the reference implementations)

Responsibilities the node agent owns that are easy to overlook; the reference repos either rely on them or were explicitly flagged for lacking them.

- **Node-side model-file parsing.** A coordinator deciding placement will not have every model file on disk. When a preset's file lives only on a particular node, *that node* parses it (gguf-parser locally) and returns the structured metadata the estimator needs; the coordinator never requires local access to the file. This is exactly GPUStack's remote-parse path and is the right shape for a heterogeneous fleet. Contract implication: the `ResourceEstimator` may delegate file inspection to the owning `NodeAgent` (a `ParseModel`/`InspectModel` method) when the file is not local to the coordinator.
- **Stale-process reaping after a crash.** On startup or reconnect, the node agent must find and clean up orphaned backend processes/containers from a previous run — a crashed agent must not leave a zombie inference server holding VRAM. llama-swap *lacks* this and the research flagged it as a gap to design in, not around.
- **Loading counts as occupancy.** A model that is mid-load occupies its unit for scheduling purposes; the slot is taken before health passes. This is already why a `catastrophic` unit refuses stacked loads (§3.2) — the general rule is that in-flight loads count against capacity, not just running instances.
- **The node sheds; the scheduler queues.** A saturated node returns a fast rejection (429-style) rather than building its own unbounded local queue. All queueing, priority, and retry decisions live in the control plane. The node agent does lifecycle + forwarding + lease enforcement, nothing more.
- **Computed tuning must reach the launch command.** Where the scheduler computes placement-time tuning (GGUF offload layers, multi-GPU tensor-split), those values must be injected into the backend launch, not computed and discarded. GPUStack's split between "placement intelligence" and "execution tuning" — it computes offload/tensor-split but its generic custom backend launches without them — is the trap to avoid.

### 3.11 Fail-loud stance (cross-cutting)

For an autonomous control plane, quiet degradation is worse than a loud stop — a lesson every reference repo taught by negative example. Mycelium fails loud, specifically:

- A **failed resource estimate** must NOT proceed to deployment — never place on a guess (GPUStack logs the failure and deploys anyway; don't).
- An **unknown provider profile** must NOT silently fall back to generic OpenAI-compatible routing — require an explicit opt-in (`type: auto` or similar), so a typo can't become wrong routing (Olla's silent fallback is the anti-pattern).
- A **non-overflow backend error** must NOT be silently requeued as if it were a context overflow (§3.7 reactive path) — classify, and fail loudly on anything that isn't a clean overflow.
- **Protocol translation** must error on an unsupported field rather than emit corrupted output — a malformed tool-call must not become `{}`, a bad SSE chunk must not be silently skipped (Olla's quiet translation degradation is dangerous for agent workflows).

### 3.12 Peer model & federated scheduling

Mycelium is **peer-native**: there is no server, no leader, no required host. Every machine runs one identical `mycelium` daemon — a peer — and any peer is a valid entrypoint. This is the second core subsystem alongside the scheduler/optimizer, and it is what lets the fleet keep working when any one machine goes down. The design separates two questions the old hub model conflated: *who decides where a job goes* (coordination) and *who is allowed to commit a resource* (authority). They are different, and keeping them separate is what makes "no leader" and "no consensus protocol" both true at once.

**Peer roles (not topology).** Every peer can submit work and coordinate jobs. A peer carries a **`compute` toggle**: compute-on peers own accelerators, run backends, and participate in group telemetry analysis; compute-off peers (a thin laptop) only submit and coordinate. This is a capability flag, *not* a server/worker split — there is no role that holds permanent authority over the fleet.

**Per-job coordinator (the dissolved leader).** The peer that receives a request becomes the **coordinator for that one job**, for its duration only; when the job ends the role evaporates. Nothing is elected, nothing is promoted, and three jobs submitted from three machines have three independent coordinators at once. There is no fleet leader to fail over because no job depends on a *fleet* leader — only on its own coordinator (the peer you submitted to, which is trivially up — you're using it) and on the owner of whatever resource it lands on. This is strictly simpler than leader election and removes the "leader is down, scramble to replace it" failure window entirely.

**Coordinator decides; owner commits (optimistic concurrency).** Coordination and authority are split:
- The coordinator **gathers a fleet snapshot by asking peers in parallel** (not a serial relay), runs the **Placer** (the exact filter→select→score brain of §3.3 — it is pure logic over a snapshot, so every peer runs identical code), and picks the best host. Polling in parallel is what enables up-front placement logic: see one machine that can host the whole job alone and leave a scarce unit (the Spark) free for a job that will actually need it, rather than stranding a 9B on the Spark.
- The coordinator never *mutates* a remote node. It **asks the owner to commit**. The **resource-owning peer is the single atomic authority for its own hardware**: it holds a tiny local transactional lease store (e.g. SQLite `BEGIN; check capacity under max_util/oom_severity; insert lease; COMMIT`) that is the *only* truth for "what is running on me." Every competing claim on a node funnels through that one owner, so the owner's local transaction is the serialization point that breaks every tie.
- Because decisions are made against a snapshot that may be a beat stale, commits use **optimistic concurrency**: the owner stamps its state with a monotonic version (a **fence**), the coordinator's plan references the version it saw, and the owner **rejects a stale plan at commit** ("your view is old, here's my current state, re-decide") rather than blindly obeying it. This is the same pattern as a `git push` rejected for being non-fast-forward; re-coordinating is `git pull --rebase`. **Two coordinators racing the same GPU cannot double-book it** — both ask the owner, its transaction serializes them, the first valid commit wins, the second is rejected and re-decides. No fleet-wide consensus is needed because the contended resource has exactly one owner doing the serializing. The fence token survives from the original federation idea, but shrunk to a per-resource version guard, not a distributed coordination scheme.

**Contention & priority are adjudicated at the owner.** Since all claims on a resource pass through its owner, the owner is also where contention resolves — coordinators never negotiate *with each other* (that would resurrect the leader/consensus problem). Priority is a property of the job carried in the plan; the owner compares it against current occupants and applies the §3.6 preemption ladder (soft by default). **At launch there are no cross-user priority tiers** — contention between equal jobs is resolved by the owner's serialized commit (first valid commit wins; the loser re-decides). Per-job priority (interactive/normal/background, §3.8) is for your *own* work — keeping a laptop responsive while a batch runs. Multi-user priority/permission tiers are roadmap (§9).

**No self-preference.** A job submitted from a compute-on peer does **not** prefer to run locally; it goes through the full host-selection process every time. The whole point is optimized compute — the machine already warm with that model (so it can run a second worker) frequently beats the one you happened to submit from, and you'd never discover that without asking the fleet first.

**Result flow.** Tokens stream back **through the coordinator**: you talk only to the peer you submitted to; it relays from the owner over peer RPC. (You hit your local dev Mac's endpoint → it coordinates → the job runs on the GPU box → the result streams back to your local dev Mac → out through the endpoint to you.) This is what makes "any peer is your one clean entrypoint" real.

**Resilience: a job registry + heartbeats (the right-sized "ledger").** Local-only state cannot survive a node death — if a job's existence lived only on the node running it, that node dying would erase the knowledge needed to rescue the work. So Mycelium keeps a **small replicated job registry**: `{job_id, coordinator, assigned_node, status, the submitted request}`, plus **heartbeats** for failure detection. When a peer goes silent, the others see its unfinished jobs in the registry and **re-coordinate them onto live peers** (re-running normal placement). Crucially this is **not** the proposal's consensus event-log of all scheduling state — it is a job registry, not an ordered ledger of every lease/preempt/migrate event, and it is allowed to be **eventually-consistent**, because *authority* still lives at the owner: a rescuer double-checks with the (live) owner before acting ("were you running J1?" "it finished, ignore the registry"), so a briefly-stale registry causes a redundant check, never a corruption or a double-booked accelerator. The registry holds the request payload so a job can be truly *rescued* (re-run), not merely flagged as lost.

**Telemetry: collected locally, analyzed by compute peers as a group.** Collection is inherently local — every node owns and stores its own run-metrics (no owner question). *Analysis* (the optimizer's distribution-shift detection, anomaly detection, consolidation recommendations of §3.7) is the one place a fleet-wide view is wanted, and it is run by the **compute-enabled peers as a group, periodically, with no permanent owner**: on a timer, some compute peer coordinates an "analysis round" exactly like any other job (gather each node's telemetry, run the deterministic stats on a compute node with spare CPU, write recommendations back) — whichever compute peer's timer fires that interval handles that round, a different one next time. This keeps the heavy CPU **on the machines built for work and off the thin/laptop peers** (if you have a dedicated inference box, you didn't want your laptop running like a jet engine), and it reuses the per-job-coordinator pattern rather than introducing a new role.

**Discovery & transport.** Peers find each other by **LAN auto-discovery** (mDNS/DNS-SD — the "node joins the hive" UX) and talk over **authenticated peer RPC**. **No SSH.** A shared join token is membership only (it lets a peer into the fleet); it is not per-operation auth and is rotatable/revocable. Cross-NAT/overlay membership (e.g. libp2p) is roadmap, behind the same discovery interface.

**Partitions degrade, never corrupt.** If peers can't reach each other, each peer keeps honoring its own already-granted leases and serving its own work; nobody can claim a resource from an owner it can't reach, so there is no split-brain over who holds what. Capacity simply shrinks to what each reachable owner can grant.

**One model, one machine — job distribution, never model distribution.** The full model loads on whichever single machine is selected; the fleet spreads *different jobs* across *different machines*. Mycelium deliberately does **not** shard one model across machines (no MLX-distributed / pipeline-parallel / hostfile / rank coordination, no cross-machine "unit"). That is a different problem (model distribution) that this project does not solve; it is explicitly out of scope, not a roadmap item.

---

## 4. Domain knowledge / skill files

The repo will carry an `AGENTS.md` (runtime agent behavior) and a small set of skill files (each < 400 lines, examples over abstractions) for the parts where the agent needs encoded know-how rather than rules:

- `skills/backend-adapters.md` — how a backend adapter is structured (command template, health path, env, capability map), with the llama.cpp / vLLM / MLX / custom examples worked.
- `skills/kv-estimation.md` — backend-aware fit math; how to call gguf-parser; how vLLM/SGLang reservation maps to a claim.
- `skills/scheduler-model.md` — the resource/lease/preemption model in §3, condensed, so the agent does not "improve" it back into a naive FIFO.
- `skills/peer-coordination.md` — the peer model of §3.12 condensed: coordinator-decides/owner-commits, optimistic-concurrency commit (fence/version), the job registry + heartbeat recovery, group telemetry analysis, and the inviolable rule that it is **job** distribution, never **model** distribution.

These are written during scaffolding (Phase 0) so later phases inherit them.

---

## 5. Repo layout

A single Go module. Deep directories that mirror the architecture so an agent navigates by path.

```
mycelium/
├── go.mod
├── AGENTS.md                         # runtime agent behavior in this repo
├── README.md
├── cmd/
│   └── mycelium/
│       ├── main.go                   # dispatch: run (peer daemon) | ctl
│       ├── peer.go                   # the peer daemon — gateway + coordinator + (if compute:on) node/backends
│       └── ctl.go                    # `myce` surface (status, add-model, drain, bench…)
├── internal/
│   ├── domain/                       # types shared across layers; NO logic, NO deps
│   │   ├── job.go                    # Job, Priority, SpeedPref, Preemption, Status
│   │   ├── node.go                   # Node, Accelerator, OOMSeverity, SpeedClass
│   │   ├── preset.go                 # Preset, Backend, Capability
│   │   ├── instance.go               # ModelInstance, Claim, InstanceState
│   │   ├── lease.go                  # Lease, Reservation (headroom|pinned)
│   │   ├── placement.go              # PlacementDecision, Trace, Action
│   │   ├── project.go                # Project defaults (priority, speed, ctx cap, toggles)
│   │   ├── peer.go                   # Peer, Roles (compute on/off), Fence, JobRecord (registry row)
│   │   └── errors.go                 # typed errors (overflow, no-fit, preempted, stale-fence…)
│   ├── ports/                        # interfaces (Protocols) every module implements
│   │   ├── scheduler.go              # Scheduler, Placer
│   │   ├── nodeagent.go              # NodeAgent (snapshot/load/unload)
│   │   ├── admission.go              # AdmissionController (owner-authority: offer/commit/release/preempt)
│   │   ├── coordinator.go            # Coordinator (claim/plan/commit/release a job)
│   │   ├── registry.go               # JobRegistry (append/watch/snapshot) + PeerDiscovery (advertise/peers/watch)
│   │   ├── backend.go                # BackendAdapter (launch/health/stop)
│   │   ├── estimator.go              # ResourceEstimator (KV-aware fit)
│   │   ├── catalog.go                # ModelRegistry (endpoint↔model), Catalog
│   │   ├── telemetry.go              # TelemetrySink / TelemetryStore
│   │   ├── optimizer.go              # Optimizer (recommend/auto)
│   │   └── transport.go              # Clock, peer RPC, Tunnel
│   ├── scheduler/                    # THE BRAIN (Phase 0)
│   │   ├── scheduler.go              # admit/queue/preempt loop
│   │   ├── queue.go                  # priority queue w/ aging
│   │   ├── filter.go                 # unit filters (cap, oom margin, labels, fit)
│   │   ├── selector.go               # candidate gen honoring speed_pref
│   │   ├── scorer.go                 # pack/spread/locality/speed scoring
│   │   └── preempt.go                # soft/hard ladder + reallocation
│   ├── lease/                        # allocation core (Phase 0)
│   │   ├── allocator.go              # units, claims, usable_vram math
│   │   ├── reservation.go            # headroom + pinned
│   │   └── safety.go                 # max_util + oom_severity enforcement
│   ├── estimate/                     # KV-aware fit (Phase 0/1)
│   │   ├── gguf.go                   # gguf-parser preflight wrapper
│   │   ├── vllm.go                   # utilization-fraction claim
│   │   └── unified.go                # Apple unified-memory single-pressure-domain math
│   ├── node/                         # NODE AGENT — owner of this machine (Phase 1)
│   │   ├── agent.go                  # lifecycle owner; heartbeat; lease enforce
│   │   ├── admission.go              # OWNER AUTHORITY: local transactional commit (max_util/oom check), fence/version, stale-plan reject
│   │   ├── process.go                # spawn/stop (cmd + cmdStop), readiness gate, reaper
│   │   ├── instance.go               # per-instance state machine
│   │   └── loadingstate.go           # SSE loading-state injection
│   ├── backends/                     # ADAPTERS (Phase 1+) — one model on one machine
│   │   ├── llamacpp/                 # native subprocess (incl. Metal on Mac)
│   │   ├── vllm/                     # container/subprocess
│   │   ├── mlx/                      # native Apple subprocess (single-machine only)
│   │   └── custom/                   # arbitrary exec/container template
│   ├── peer/                         # FEDERATION (Phase 6) — coordinate without a leader
│   │   ├── coordinator.go            # per-job: parallel snapshot → Placer → owner-commit, relay
│   │   ├── discovery_lan.go          # LAN auto-discovery (mDNS/DNS-SD)
│   │   ├── discovery_overlay.go      # cross-NAT overlay (roadmap; behind PeerDiscovery)
│   │   ├── registry.go               # replicated job registry (eventually-consistent)
│   │   ├── heartbeat.go              # liveness + dead-peer detection
│   │   ├── recovery.go               # re-coordinate a dead peer's unfinished jobs
│   │   └── rpc.go                    # authenticated peer transport (no SSH)
│   ├── gateway/                      # INGRESS (Phase 2)
│   │   ├── server.go                 # HTTP surface
│   │   ├── router.go                 # model-aware routing to a live instance
│   │   ├── profiles/                 # provider profiles (data + parser)
│   │   ├── translate/                # Anthropic↔OpenAI passthrough-or-translate
│   │   └── headers.go                # X-Myc-* decision/observability headers
│   ├── catalog/                      # CATALOG + INSTALL (Phase 3)
│   │   ├── gallery.go                # catalog item -> materialized preset
│   │   ├── importers/                # hf:// / OCI / local -> draft preset
│   │   ├── install.go                # async install jobs + progress
│   │   └── provenance.go             # record where a preset came from
│   ├── telemetry/                    # SUBSTRATE (wired Phase 1, used everywhere)
│   │   ├── sink.go                   # ingest run metrics + heartbeats
│   │   ├── store.go                  # local persistence (SQLite)
│   │   ├── rollup.go                 # per-project distributions
│   │   └── group.go                  # group analysis round across compute peers (Phase 6)
│   ├── optimizer/                    # SELF-OPTIMIZER (Phase 5)
│   │   ├── presets.go                # consolidation cost function
│   │   ├── reactive.go               # overflow -> requeue larger preset
│   │   ├── recommend.go              # distribution-shift detection + recommendations
│   │   └── apply.go                  # gated auto-apply
│   ├── membership/                   # ONBOARDING (Phase 4) — peer join + token
│   │   ├── token.go                  # shared join token (membership only; rotatable/revocable)
│   │   └── tunnel.go                 # loopback tunnel allocation
│   ├── bench/                        # REVERSE BENCH (roadmap; interfaces stubbed early)
│   │   ├── orchestrator.go           # explode (prompt × model) matrix
│   │   ├── present.go                # write per-model output files + objective metrics for the user to review
│   │   └── results.go                # run outputs + objective metrics + optional user-pick
│   └── store/                        # per-peer local persistence (SQLite)
│       └── sqlite.go
├── pkg/
│   └── api/                          # public request/response types
│       ├── openai.go
│       └── anthropic.go
├── config/
│   └── profiles/                     # provider profile YAML (ollama, vllm, llamacpp, …)
├── skills/                           # domain skill files (see §4)
└── test/
    ├── unit/        contract/        integration/        e2e/        smoke/
    ├── fixtures/                     # config + golden data
    └── mocks/                        # MockNode, MockBackend, MockEstimator, …
```

---

## 6. Key design decisions

Each states the choice, the reason, and the rejected alternative — so the agent does not "improve" it back to something already ruled out.

**D1. Build a thin control plane; do not adopt any of the four projects.**
Why: source-level study showed each is a methods reference for one layer and none has the priority/preemption/lease/reallocation core that is Mycelium's reason to exist.
Why not adopt GPUStack: enterprise shell (Postgres, RBAC, k8s/Higress, clusters/pools/cloud provisioning), Docker-only worker model, dropped macOS workers, and a scheduler that places replicas but has no request priority/preemption/queue.
Why not LocalAI as base: huge backend bundle, very wide gRPC surface, EdgeVPN substrate; its SmartRouter is reactive request-time routing, not a desired-state planner.
Provenance of what *is* reused: Gateway ← Olla methods (profile-as-data providers, raw endpoint↔model registry, endpoint-tier priority = local-first, passthrough-first Anthropic translation, `X-*` headers). Node agent ← llama-swap methods (Process state machine, cold-start dedup, `cmd`/`cmdStop`, matrix eviction-cost solver, loading-state SSE, 429 shedding). Scheduler ← GPUStack filter/select/score + GGUF preflight + durable claims + unified-memory accounting + locality scoring, plus LocalAI SmartRouter heuristics. Onboarding/catalog ← LocalAI token-overlay + gallery→preset + importers. None copied wholesale.

**D2. Go end-to-end for the control plane; backends stay polyglot.**
Why: hot-path layers (gateway, node agent) want Go's concurrency and a single static binary per node — which *is* the "copy one command to join" UX; both closest references (llama-swap, Olla) are Go; the contract+mock TDD setup neutralizes most of Python's iteration-speed edge.
Why not Python-then-port: hand-translating Go→Python→Go (your best references are Go), "temporary" Python accretes behavior miserable to port faithfully, and you end up maintaining two codebases or trashing working code.
Why not split (Go agent/gateway + Python brain): a second runtime + IPC fights "dead simple," and the brain does no ML — it shells out to gguf-parser (already a Go binary) and supervises vLLM/MLX as subprocesses, so Python buys nothing here. "Go everywhere" means the orchestrator launches whatever backend process fits (llama.cpp binary, vLLM container, MLX Python script) — not reimplementing engines in Go.

**D3. The loadable unit is a preset `(model, context, quant, backend, flags)`, not "a model."**
Why: it is what actually occupies VRAM and what requests batch onto; it makes the consolidation optimization expressible (collapse two presets at a shared context into one warm instance).
Why not model-as-unit: hides the context/quant dimension that determines fit and batching, making the optimizer impossible to express.

**D4. Fit is KV-aware and backend-aware, not weights-only.**
Why: KV at requested context × concurrency is what overflows VRAM; and "context change = reload" is true for GGUF but ≈ free for vLLM within its launched max — the cost model must know the difference.
Why not weights-only: systematically under-reserves, causing OOM under real context/concurrency; and a backend-blind reload-cost term would make the optimizer wrong for vLLM.

**D5. Soft preemption by default; reservations and hard preempt are opt-in (off by default).**
Why: gentle out of the box — priority only orders the queue, nothing running is killed; users opt into stronger guarantees only when they want them.
Why not hard-default: surprising and destructive for a single-user tool; an aggressive default that kills running work is the wrong first impression.
Honest tradeoff documented to the user: with defaults, an urgent job with nothing reserved waits for a slot.

**D6. Fit-forced reallocation reuses the preemption test; there is no separate "borrow/reclaim."**
Why: a job that only fits on one unit (120B → Spark) is just allocation with the candidate set pinned to that unit; it displaces occupants under the same hard/soft priority test, else queues. One mechanism, fewer concepts.
Why not a bespoke lending system: duplicate logic and an extra mental model for what is the same allocation decision.

**D7. Per-machine `max_util` is an inviolable hard constraint; `oom_severity` tunes paranoia beneath it.**
Why: the Spark power-cycles on OOM (catastrophic), the 4090 only crashes the program (soft) — the scheduler must keep extra margin and refuse stacked loads on catastrophic units.
Why not a single global utilization setting: ignores the real per-machine asymmetry and risks bricking the worst-consequence machine.

**D8. Backends are executable packages behind one adapter contract (launch/health/stop), supervised as subprocesses.**
Why: matches how llama.cpp/vLLM/MLX actually run; keeps Mycelium engine-agnostic; preserves exact tuning via command templates + explicit params.
Why not in-process bindings: couples Mycelium to engine internals, breaks the polyglot reality, and makes adding an engine a code change instead of an adapter.

**D9. Native Mac node (subprocess), not Docker; unified memory is one pressure domain.**
Why: GPUStack's Docker-only worker model is the wrong abstraction for Metal/MLX; native subprocess + treating Apple unified memory as a single reservable pool (not separate CPU-RAM and VRAM) is the clean design, and it also makes the node agent trivial to run on the dev local dev Mac.
Why not inherit GPUStack's container worker: it is precisely why GPUStack dropped macOS support.

**D10. Telemetry is wired from Phase 1, before the optimizer exists.**
Why: the optimizer is worthless without history, and retrofitting per-run metrics + per-project distributions later is miserable.
Why not add telemetry with the optimizer: you would rebuild every emit site and lose all early history.

**D11. The optimizer recommends by default; auto-apply is a per-project toggle, off until proven.**
Why: optimization decisions change latency and batching; the user wants to watch them behave before trusting them automatically. The engine is deterministic stats + anomaly detection, not LLM judgment — which keeps it debuggable.
Why not auto-apply by default: silent context/preset changes that surprise the user erode trust in exactly the subsystem meant to build it.

**D12. Onboarding MVP is a join token + LAN auto-discovery; a cross-NAT overlay slots in behind the same `PeerDiscovery` interface later.**
Why: token + LAN auto-discovery (mDNS/DNS-SD) gives the "peer joins the hive" UX with far less substrate than an overlay/VPN; designing `PeerDiscovery`/`Tunnel` as interfaces lets the overlay (cross-NAT) be added without touching callers. The token is membership only (rotatable/revocable), never per-operation auth.
Why not libp2p/EdgeVPN now: it brings DHT/relay/ledger/token-semantics complexity not needed for a LAN homelab MVP and fights the "dead simple" goal early.
Why not SSH-based transport: SSH-orchestrating peers is brittle and was only ever needed for cross-machine model launch, which Mycelium does not do (D17); peers talk over their own authenticated RPC.

**D13. Reverse benchmarking is composition + user-only judgment; Mycelium ships no quality heuristics and no agent integration. Roadmap, with two cheap hooks paid now.**
Why: the fleet already provides fan-out, placement, and per-run metrics; only the results/comparison subsystem is net-new, so it is deferred — but the `Job` model is made fan-out-capable and telemetry captures the metrics from Phase 1, so adding it later is not a retrofit.
Why v1 is just-the-output-files: what counts as the "best" output is task- and user-specific, so Mycelium computes no WER/exact-match/rubric/schema score and ships no scorer/plugin abstraction. Judging happens entirely outside Mycelium — the user reads the files, or sends them to an agent of their own choosing themselves; Mycelium does not orchestrate that hand-off or hook into any harness. A user with a hard criterion runs it on the files Mycelium produced; that is their code, not a Mycelium feature.
Why not build it in MVP: the core scheduler must land first; why not ignore it entirely: the two hooks cost ~nothing and the substrate is uniquely suited to it.

**D14. Peer-native, no leader: per-job coordinator over a permanent scheduler host.**
Why: the user submits from multiple machines and must not depend on one always-on host — the laptop has to keep working while the project server reboots, and one machine dying must not stop the rest. So every peer is equal and the coordinator is just "whoever received the job," for that job only.
Why not one elected scheduler leader: it makes ordinary use needlessly centralized and creates a leader-failover scramble; a per-job coordinator dissolves the leader concept entirely (nothing to elect, nothing to promote) and is strictly simpler.
Why not a hub-and-spoke server/worker split: that is the model this replaced; it puts authority and availability in one box.

**D15. Coordinator decides; resource owner commits — optimistic concurrency, not consensus.**
Why: separating "who decides placement" from "who may commit a resource" is what makes "no leader" and "no hand-rolled consensus" both true. Each node is the sole atomic authority for its own hardware via a local transaction; coordinators propose, owners commit, and a stale plan is rejected at commit by a per-resource fence/version (the `git push` non-fast-forward pattern).
Why not a replicated consensus log as the source of truth: keeping N copies of mutable scheduling state in agreement under failure *is* the hard distributed-systems problem; it is unnecessary because the contended resource already has exactly one owner to serialize it.
Why not coordinators negotiating with each other before committing: that re-introduces the leader/consensus problem for no gain — the owner is already the single adjudication point where contention resolves.

**D16. Resilience is a replicated job registry + heartbeats, not a consensus event-log.**
Why: a node death must be survivable, which needs *some* replicated knowledge of "which jobs exist and who owns them" — but only that. A small eventually-consistent registry `{job_id, coordinator, owner, status, request}` plus heartbeats lets peers detect a death and re-coordinate the dead peer's unfinished jobs; authority still lives at the owner, so a stale registry causes a redundant double-check, never corruption.
Why not the proposal's full ordered ledger of every lease/offer/preempt/migrate event: that is consensus-hard overkill; the registry is for visibility and recovery, not for authority.
Why store the request payload: so a job can be truly re-run (rescued), not merely flagged lost.

**D17. Job distribution, never model distribution.**
Why: the user wants the whole model on one selected machine and *different jobs* spread across machines — that is what the fleet is for, and the scheduler already does it. Sharding one model across machines (MLX-distributed / pipeline-parallel / hostfile + rank coordination) is a different, much harder problem this project deliberately does not solve.
Why this is a hard exclusion, not a roadmap item: it changes the resource model (a cross-machine "unit"), the transport (SSH/MPI), and the lease protocol (atomic multi-node commit / 2PC) — pulling in exactly the complexity the peer design removes. Keeping it out is what keeps single-machine jobs (one owner, one lease) simple.

**D18. Telemetry is collected locally and analyzed by compute peers as a group.**
Why: collection is inherently per-node; analysis wants a fleet-wide view but should run on the machines built for compute, not a thin laptop (you bought an inference box precisely so your laptop wouldn't run hot). So analysis is a periodic round coordinated by whichever compute peer's timer fires — no permanent owner — reusing the per-job-coordinator pattern.
Why not analysis on the submitting/user machine: pushes heavy CPU onto thin peers and centralizes a fleet function on a non-compute node.
Why not a single dedicated analyzer node: that is a soft leader with a failover gap; the rotating group avoids it.

---

## 7. Data flow — a typical request

1. An app sends an OpenAI/Anthropic chat request to **any peer's** endpoint (the peer you point your client at — its own address is your endpoint). That peer becomes this job's **coordinator**.
2. The coordinator extracts intent and merges **project defaults** (priority, speed_pref, context cap, preemption mode) with any per-request overrides → constructs a `Job`, and records it in the **job registry** (so it survives a coordinator death).
3. The coordinator **asks peers in parallel** for their current snapshots (warm instances, capacity, health). If a compatible warm instance already exists with capacity, placement can be near-immediate.
4. **Placer** (identical code on every peer): resolve preset (apply context cap) → **estimate** claim (weights + KV, backend-aware) → **filter** units (max_util with oom margin, labels, fit) → **select** candidates honoring speed_pref (pack vs isolate/dedicate vs fastest; **no self-preference**) → **score** → choose the best host. Parallel snapshots let it reason up front (e.g. keep the Spark free for a job that needs it).
5. The coordinator **asks the chosen host's owner to commit** the lease. The owner runs its **local transaction**: re-check fit under `max_util`/`oom_severity` against its *current* state, and either commit (insert lease, bump fence) or **reject the plan as stale** ("my view moved on") — in which case the coordinator re-decides against fresh truth. If a slot is needed and none is free, the owner applies the preemption ladder (soft → queue; hard if flagged → displace lowest-priority occupant, which re-coordinates elsewhere or re-queues).
6. The owner (now the resource-owning node) either reuses a warm instance or loads the preset (readiness-gated — no routing until health passes), grants the **lease**, and returns the **PlacementDecision + trace** to the coordinator.
7. The coordinator proxies the request to the instance over peer RPC and **relays the response back to the client**: **passthrough** if the backend speaks the client's wire format, else **translate** (Anthropic↔OpenAI). If the model was cold, **loading-state** SSE keeps the client alive during load. Tokens flow owner → coordinator → client; the client only ever talks to the coordinator.
8. The owning node emits **telemetry** (tokens/sec, TTFT, load wall-clock, peak VRAM, context used) to its **local** store; the coordinator attaches **X-Myc-*** decision headers to the response. On completion/failure the registry row is updated.
9. Periodically, the **compute peers** run a **group analysis round** over the fleet's telemetry: the optimizer reactively requeues on context overflow, and proactively detects distribution shifts to recommend preset/context adjustments (auto-applies only if the project's toggle is on).
10. If a peer **goes silent** (missed heartbeats), the others see its unfinished jobs in the registry and **re-coordinate them onto live peers**, double-checking the relevant owners before acting. The fleet keeps serving with whatever capacity remains reachable.

---

## 8. MVP phases (brief; expanded with gates in Document 3)

- **Phase 0 — Scheduler + lease + safety core.** The risky, novel piece, built first and entirely on mocks (mock nodes, mock backends, mock estimator). Proves admit/queue/soft-preempt/hard-preempt/reallocation, KV-aware fit, `max_util`/`oom_severity` enforcement, reservations. The Placer is intentionally **peer-agnostic** — pure logic over a fleet snapshot — so the same brain serves any coordinator later. No real hardware. Runs and is fully tested on the local dev Mac.
- **Phase 1 — Node agent (owner authority) + first backend (llama.cpp) + telemetry wiring.** Real per-node lifecycle (load/ready-gate/stop/coexist), the **owner-authority local commit** (transactional lease store, `max_util`/`oom_severity` enforced locally, fence/version + stale-plan rejection), lease enforcement, loading-state SSE, heartbeats, and the telemetry substrate emitting from day one. Validated on the local dev Mac (llama.cpp Metal) and a second peer as a real remote node.
- **Phase 2 — Gateway + routing + OpenAI/Anthropic surface.** Provider profiles, model-aware routing to a live instance, failover, passthrough-or-translate, `X-Myc-*` headers.
- **Phase 3 — Catalog + install + presets.** Catalog item → materialized preset, importers (`hf://`/OCI/local → draft preset), async install jobs with progress, provenance. "Add a model = one-liner."
- **Phase 4 — Peer membership + onboarding.** Join token + LAN auto-discovery, peer advertisement (incl. the `compute` flag), loopback tunnels. "A peer joins the hive."
- **Phase 5 — Optimizer.** Consolidation cost function, proactive distribution-shift recommendations, gated auto-apply (reactive overflow-requeue already landed in Phase 1's telemetry/runtime path).
- **Phase 6 — Federated coordination & resilience.** Make coordination peer-distributed (any peer coordinates a job: parallel-poll snapshot → Placer → owner-commit with optimistic concurrency → relay), the **job registry + heartbeats + failure recovery**, and **group telemetry analysis** across compute peers. Builds on Phase 1's owner-authority and Phase 4's discovery. This is the layer that makes "no leader, one machine down is business as usual" real.

Real specialty hardware (Spark, B70, 4090, desktop GPU) enters only at the `smoke/` tier and at phase boundaries — never as a development dependency.

---

## 9. Roadmap (post-MVP)

- **Reverse benchmarking** — the fan-out eval harness that writes per-model output files + objective metrics (§3.9); Mycelium supplies the run and the facts, the user does all the judging on their own (no agent integration). Hooks already paid in the Job model and telemetry.
- **libp2p / cross-NAT overlay** — membership beyond the LAN, behind the existing `PeerDiscovery`/`Tunnel` interfaces (D12).
- **Auto-calibration** — grow the join-time tokens/sec probe into fuller self-benchmarking that continuously refines `speed_class` and the optimizer's cost model.
- **Auto-apply optimization on by default** — only after the recommend-first engine is observed behaving in practice (D11).
- **Auto engine/param selection** — build the "best engine + params for this model on this machine" decision the four projects only seed; informed by accumulated telemetry and reverse-benchmark results. (LocalAI's meta-backend resolution — a logical name like "llama-cpp" resolving to the best concrete backend for the host — is the mechanism shape.)
- **Sticky / KV-cache-affinity routing** — route a multi-turn conversation's follow-up requests back to the same warm instance to reuse its KV cache, as a selector decorator over a conversation→instance TTL map (Olla's sticky-session shape). A large latency win for chat; deliberately not in the MVP gateway.
- **Privacy / sensitive-data handling** — a per-job handling class (e.g. "PII / must stay encrypted"). This is its own scoping effort because it reshapes how peers store and move data: encryption in transit *and* at rest, what the job registry may replicate (the request payload is replicated for recovery today — that changes), whether/how telemetry and failure-recovery work for protected jobs, and the transport's security model. Functionality first; this is a deliberate later body of work, not a flag bolted on.
- **Multi-user priority & permissions** — cross-submitter priority tiers and access control (today priority is per-job and contention is resolved equal-rights at the owner, §3.12). Adds a shared priority policy every owner applies identically, plus who-may-do-what.
- **Cross-coordinator pre-send negotiation** — letting coordinators coordinate *before* dispatch to optimize globally (vs. today's independent decide-then-let-the-owner-arbitrate). Useful eventually but reintroduces coordinator-to-coordinator coordination, so it is deferred until the simple model's limits are felt.
