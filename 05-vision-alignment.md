# Mycelium — Vision Alignment: One Endpoint, Automatic Workers (Document 5)

> **Status:** execution plan. This document is the scoped work list for making
> the product match the owner's vision. It follows the Document 3 method:
> phases in order, each with hard requirements, behavioral checks, and a
> runnable gate. Do not start phase N+1 until phase N's gate is green.
>
> **For agentic workers:** execute phase-by-phase. Record every resolved
> *Your call* in `DECISIONS.md`, every deferral in `BLOCKERS.md`. Do not
> reintroduce anything Document 1 §6 rejected.

## 0. The vision, as acceptance criteria

Every machine runs `mycelium`. The user points any OpenAI-compatible program
at any peer's endpoint, optionally sending intent (priority, project,
max-wait) with the request. The fleet automatically warms and spreads as many
workers as safely possible under demand, and shrinks when idle. The
consumption path requires **zero Mycelium-specific ceremony**: no pool
commands, no lease headers, no local proxy, no state files.

The single top-level acceptance test (**the vision smoke**, built in V5):

> Against a fleet with a model registered on ≥2 compute nodes and **nothing
> warm**, a plain OpenAI client (base URL + token only) runs a 32-concurrent
> batch of 200 requests with a per-request max-wait opt-in. All 200 complete.
> Response headers prove requests were served by **at least 2 distinct
> nodes** and **more than one instance**. After the batch plus the idle TTL,
> the auto-grown instances are unloaded. No operator command ran between
> "client starts" and "assertions pass."

### What this plan deliberately does NOT change (non-goals)

- Owner authority, fences, admission transactions — untouched.
- No leader, no consensus, no model sharding (Doc 1 §6 stands).
- Engine auto-install stays Doc 04 Phase G (gap-reported, not executed).
- Streaming leased execution, multi-user policy tiers — untouched.
- The evidence surfaces (preflight, debug, telemetry) — untouched, but
  demoted from the consumer happy path to operator runbooks (V5).

### Why this is small: the parts already exist

| Vision behavior | Existing mechanism | Missing piece |
|---|---|---|
| "notice demand" | placement traces already record drop/queue reasons; reconciliation task records + dedup exist (`internal/reconcile`) | emit tasks from the **live gateway/scheduler path**, not just the workload lane |
| "spread workers" | `internal/pool.BuildPlan` (safe max replicas/node); prewarm-through-admission executor (`TaskPrewarmReplica`) | a **background loop in the daemon** that connects signal → plan → prewarm, behind policy |
| "workers usable by plain clients" | prewarm semantics: admit → ready → **finish (release lease)** leaves a warm instance normal admission batches onto | nothing — do NOT use pool hold-leases for auto-scale; holds block normal admission |
| "survive cold start" | cold-load-inline, loading-state SSE, 503+Retry-After | **bounded opt-in wait** on sync requests |
| "shrink when idle" | instance lifecycle + owner unload path; injected clocks everywhere | idle reaper for **auto-grown** instances only |
| "priority per job" | `X-Myc-Priority` etc. + project defaults | nothing — document it |

---

## Phase V0 — Autonomy policy + live demand signals

**Goal:** the daemon knows what it is allowed to do on its own, and real
traffic produces durable, deduplicated demand evidence.

**Build:**

1. **Autonomy config** (`cmd/mycelium/peer.go` config structs, parsed with
   the existing peer-config loader):

```json
"autonomy": {
  "auto_scale": false,
  "auto_stage": false,
  "max_replicas_per_model_per_node": 4,
  "max_replicas_per_model": 8,
  "scale_cooldown_ms": 60000,
  "idle_ttl_ms": 900000,
  "max_wait_default_ms": 0
}
```

   Go shape (add to the peer config in `cmd/mycelium/peer.go`, mirrored in a
   new `internal/domain/autonomy.go` so `internal/*` packages never import
   `cmd/`):

```go
type AutonomyPolicy struct {
    AutoScale                  bool `json:"auto_scale"`
    AutoStage                  bool `json:"auto_stage"`
    MaxReplicasPerModelPerNode int  `json:"max_replicas_per_model_per_node"`
    MaxReplicasPerModel        int  `json:"max_replicas_per_model"`
    ScaleCooldownMS            int  `json:"scale_cooldown_ms"`
    IdleTTLMS                  int  `json:"idle_ttl_ms"`
    MaxWaitDefaultMS           int  `json:"max_wait_default_ms"`
}
```

   Defaults: everything **off/zero** — autonomy is opt-in per peer. Invalid
   values (negative caps, cooldown < 1s when auto_scale on) fail startup
   loudly.

2. **Demand signal emission** (`internal/reconcile/demand.go`, new):
   `RecordDemand(ctx, store, decision domain.PlacementDecision, reason string, now time.Time) error`
   converts a live placement outcome into reconciliation task records using
   the existing `PlanPlacement`/`RecordsFromPlan` shapes, with deterministic
   task IDs keyed by `(kind, preset, node, reason)` so repeated pressure
   upserts one task instead of accumulating thousands. A `LastSeen`/`Hits`
   counter on `domain.ReconciliationTaskRecord` records pressure intensity.

3. **Wire the emitters.** Call `RecordDemand` from the two live paths:
   - `internal/scheduler/service.go`: on `ActionQueued` outcomes and on
     owner `ErrNoFit` shed→replan exhaustion.
   - the gateway no-instance 503 path (`internal/gateway/router.go`), via
     the same store the runtime already has.
   Never emit on client errors (unknown model, bad request, auth) — only on
   capacity outcomes.

**Your call (V0):** where `Hits`/`LastSeen` live (new columns on the task
record vs. a separate demand table). Hard requirements: dedup is by stable
ID; a burst of 10k requests produces O(presets×nodes) rows, not O(requests);
records carry enough to plan (model, preset, node, context, concurrency).

**Behavioral checks (fast tier):** a burst of queued placements produces
exactly one demand task per (preset, node) with `Hits` incremented; client
errors produce none; autonomy config parse/validation table including the
loud-failure cases.

**Gate (V0):**
```bash
go test ./internal/reconcile/... ./internal/scheduler/... ./cmd/mycelium/... -race
go test ./... -race && make coverage
```

---

## Phase V1 — The auto-scale loop (demand → warm replicas, no leases)

**Goal:** under demand, the fleet grows warm replicas across every node that
can safely hold one, with no operator involvement.

**Build:**

1. `startAutoScaler(ctx, ...)` in `cmd/mycelium/peer.go`, alongside
   `startQueueDrainer` — a clock-injected loop (tick default 5s) that runs
   only when `autonomy.auto_scale` is true. Each tick:
   a. read open demand tasks (V0), group by model/preset;
   b. build a live `domain.FleetSnapshot` from the existing fleet source;
   c. run `pool.BuildPlan` with `Options{MaxPerNode: policy.MaxReplicasPerModelPerNode, MaxTotal: policy.MaxReplicasPerModel - existing}`;
   d. for each planned replica, launch through the **existing prewarm
      semantics**: background-priority job, node-pinned, admit → ready →
      **finish** so the lease is released and the warm instance serves
      normal admission. Reuse the in-daemon path (`scheduler.Service`
      submit + `FinishJob`), NOT peer-RPC-to-self.
   e. close the demand task with the outcome (scaled / refused+gap).

2. **Safety rails (hard requirements, all tested):**
   - serial cold loads per catastrophic node (launch one, wait ready, next);
   - per-(preset,node) cooldown after any failure (`ScaleCooldownMS`);
   - circuit breaker: 3 consecutive failures for the same (preset,node)
     marks the demand task failed-with-evidence and stops retrying until an
     operator clears it or new demand arrives after cooldown;
   - never exceeds plan caps; never bypasses owner admission; `max_util`,
     oom margins, and disk floors hold because the planner and admission
     already enforce them;
   - the loop is fully deterministic under `FakeClock` in tests.

3. **Provenance:** auto-grown instances are identifiable (label or field on
   `domain.ModelInstance`, e.g. `Origin: "autoscale"`) so V3 reaps only
   what autonomy created. Manual loads, pinned reservations, and pool-held
   instances are never auto-reaped.

**Your call (V1):** tick cadence vs. event-driven wakeup; whether refused
nodes re-plan every tick or exponentially back off. Hard requirement: an
idle fleet with no demand tasks does zero planning work per tick.

**Behavioral checks (fast tier, mock fleet + FakeClock):**
1. demand on a model with capacity on 2 nodes → replicas appear on both,
   bounded by caps, envelopes respected;
2. catastrophic node grows replicas strictly serially;
3. a failing node trips the breaker after 3 attempts and stops;
4. `auto_scale: false` → loop does nothing despite demand;
5. auto-grown instances carry provenance; a pool-held worker is untouched.

**Gate (V1):**
```bash
go test ./cmd/mycelium/... ./internal/reconcile/... -race
go test ./test/e2e/... -run AutoScale -race
go test ./... -race && make coverage
```

---

## Phase V2 — Bounded opt-in wait on the synchronous endpoint

**Goal:** a plain client's first-wave requests survive cold starts and
scale-up instead of erroring, without hidden unbounded blocking.

**Build:**

1. `X-Myc-Max-Wait-Ms` request header (`internal/gateway/headers.go`
   `HeaderMaxWait`, parsed in `server.go` next to the context-cap header;
   invalid values 400 loudly) + `Project.MaxWaitMS` default
   (`internal/domain/project.go`) + `autonomy.max_wait_default_ms` as the
   peer-level fallback. Effective wait = header > project > peer > 0.
2. In the gateway no-instance path (`internal/gateway/router.go`): when
   effective wait > 0, retry placement on a clock-injected backoff (start
   500ms, ×2, cap 5s) until the deadline; each retry is a full normal
   placement (so V1's newly warm replicas get picked up). Past deadline:
   the existing 503 + `Retry-After`. Zero wait: exactly today's behavior.
3. Client disconnect cancels the wait immediately (request context).
4. Emit V0 demand on the **first** miss, so waiting requests are what feeds
   the autoscaler.
5. Record wait time in the existing `X-Myc-Queue-Wait-Ms` response header
   (currently always 0 — make it real for waited requests).

**Hard requirements:** deterministic under `FakeClock`; the DECISIONS
2026-07-01 entry's contract holds (explicit caller-supplied deadline, same
owner-admission semantics, no implicit blocking); waiting requests do not
hold any lock that stalls other requests (verified by a concurrent test).

**Behavioral checks (fast tier):** cold fleet + wait → request succeeds once
a (mock) replica warms mid-wait and `X-Myc-Queue-Wait-Ms` > 0; deadline
exhaustion → 503 + `Retry-After`; no header/defaults → immediate 503;
disconnect mid-wait releases immediately; 50 concurrent waiters don't
serialize the router.

**Gate (V2):**
```bash
go test ./internal/gateway/... -race
go test ./... -race && make coverage
```

---

## Phase V3 — Idle scale-down

**Goal:** the fleet shrinks on its own; the vision includes not cooking your
machines all night.

**Build:**

1. Last-activity tracking per instance: stamp on the existing in-flight
   begin/end path (node-agent protocol already brackets requests), persisted
   or held in the daemon runtime keyed by instance ID.
2. `startIdleReaper(ctx, ...)` in `cmd/mycelium/peer.go`: clock-injected
   loop; when `autonomy.idle_ttl_ms` > 0, unload instances where
   `Origin == "autoscale"` AND idle > TTL AND in-flight == 0, through the
   normal owner unload path. Never touches manual, pinned, or pool-held
   instances. Hysteresis: never reap an instance younger than
   `ScaleCooldownMS` so scale-up/scale-down cannot oscillate.

**Behavioral checks:** idle auto-grown instance reaped after TTL under
`FakeClock`; active instance untouched; manual/pinned/pool instances never
reaped; reap → new demand → re-grown (the full cycle).

**Gate (V3):**
```bash
go test ./cmd/mycelium/... -race
go test ./test/e2e/... -run 'AutoScale|IdleReap' -race
go test ./... -race && make coverage
```

---

## Phase V4 — Policy-gated artifact staging

**Goal:** "spread across machines" includes machines that merely lack the
model file. (Engine setup stays report-only — Doc 04 Phase G.)

**Build:** when `autonomy.auto_stage` is true, the autoscaler drains
`TaskStageModelArtifact` demand by **executing** the staging the task
currently only plans: reuse the `myce models stage` / locality-apply
machinery in-daemon. Disk floors enforced (already are, in placement and
catalog). Staging is a durable job visible in `myce jobs list`; failure is
loud evidence, never a silent skip. `auto_stage: false` keeps today's
plan-only behavior.

**Behavioral checks:** demand from a node lacking the artifact + auto_stage
on → staged (mock transfer) → next scale tick grows a replica there;
auto_stage off → task stays planned with the setup gap; disk-floor node
refuses with evidence.

**Gate (V4):**
```bash
go test ./internal/reconcile/... ./internal/catalog/... -race
go test ./... -race && make coverage
```

---

## Phase V5 — Surface demotion, docs, and the vision smoke

**Goal:** the product's face matches the vision; ceremony becomes plumbing.

**Build:**

1. **README restructure.** README keeps: what Mycelium is, quickstart
   (run peer → add model → set `autonomy` → point client at endpoint →
   per-request intent headers), config reference, test gates. Everything
   else — preflight/evidence workflows, debug recipes, service-activation
   runbooks, codesign forensics, cold-load-cancellation proof, pool/lease
   internals — moves intact to `docs/runbooks/*.md` with a one-line index in
   the README. No content is deleted, only relocated.
2. **Demote the manual surfaces.** `myce pool` is documented as the
   *explicit reservation* tool (guaranteed capacity, ahead-of-time warming
   for a deadline) in `docs/runbooks/reserved-capacity.md`; `pool proxy` and
   the lease headers are documented there only. Consumer docs never mention
   them. Code stays — the autoscaler uses the planner, and reservations are
   a real feature.
3. **Token cleanup.** One shared gateway-token resolution helper used by
   benchmark, preflight, and pool proxy (single documented order); **remove
   the `OPENAI_API_KEY` fallback** (foot-gun: an unrelated real key silently
   sent to a LAN gateway) with a loud error message naming the supported
   sources. DECISIONS entry for the breaking change.
4. **The vision smoke** (`make smoke-vision`, `//go:build smoke`,
   `test/smoke/`): implements §0's acceptance test against the real fleet,
   env-configured like the other fleet smokes (gateway URL, token, model,
   expected node set). Asserts: all requests complete, ≥2 nodes and ≥2
   instances served (via `X-Myc-Node`/`X-Myc-Instance`), zero operator
   commands, and (long-tail, optional flag) idle reap afterward.

**Gate (V5):**
```bash
make ci
make smoke-vision   # real fleet; defer-and-log per AGENTS.md if hardware absent
```

---

## Risk table

| Risk | Mitigation (phase) |
|---|---|
| Runaway scale-up | plan caps + cooldown + circuit breaker + owner admission as the final authority (V1) |
| Scale thrash (up/down oscillation) | idle TTL ≫ cooldown; never reap younger than cooldown (V3) |
| Spark (catastrophic OOM) safety | serial cold loads, existing margins, caps; autoscaler never relaxes any Doc 1 §3.2 rule (V1) |
| Demand-table growth | deterministic task IDs upsert; closed tasks pruned by the existing compaction posture (V0) |
| Waiting requests pile up | bounded deadline, per-request context, no shared lock during wait, 503 past deadline (V2) |
| Silent behavior change for existing users | every autonomy behavior is opt-in and off by default; zero-config behavior is byte-identical to today (all phases) |

## Sequencing note for the executor

V0→V1→V2 is the minimum lovable slice — after V2, the Chandra/Atlas flow is:
set `autonomy.auto_scale: true` on the fleet once, point the CLI at the
gateway with a max-wait project default, run. V3–V5 complete the vision but
V0–V2 already delete the pool/proxy ceremony from the happy path. Ship and
live-verify V0–V2 before starting V3.
