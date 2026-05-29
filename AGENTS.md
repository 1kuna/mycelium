# AGENTS.md — working rules for Mycelium

You are building **Mycelium**, a hardware-aware inference control plane (a single Go binary that conducts existing inference engines across a fleet of heterogeneous machines). This file is the operating manual for any agent working in this repo. Read it before writing code.

## Read these first, in order

1. `01-project-spec.md` — *what* this is: the resource/lease/scheduler/optimizer model (§3), the data shapes (§3.8), the repo layout (§5), and the **13 locked design decisions with their rejected alternatives (§6)**.
2. `02-testing-architecture.md` — *how to verify*: the Go interface contracts, the hand-written mocks, fixture factories, `FakeClock`, the conformance-suite pattern, and the CI tiers.
3. `03-development-guide.md` — *what order*: the gated phase plan. **Work one phase at a time. Do not start phase N+1 until phase N's gate passes.**

Also read the skill files in `skills/` when touching their areas (`backend-adapters.md`, `kv-estimation.md`, `scheduler-model.md`).

## Build & test

```bash
go build ./...                                   # must be clean
go vet ./...                                     # must be clean
gofmt -l .                                       # must print nothing (run `gofmt -w .` to fix)
go test ./... -race                              # fast tiers: unit + contract + integration + e2e — NO hardware
go test ./... -covermode=atomic -coverprofile=all.out && go tool cover -func=all.out | grep total
go test -tags smoke ./test/smoke/... -timeout 20m   # smoke: REAL engines/machines — run only at phase boundaries
```

`go test ./...` must pass on a local dev Mac with **nothing powered on**. The `smoke` build tag (`//go:build smoke`) is the only thing that touches real hardware; never let a hardware dependency leak into the fast tiers.

## Hard rules (non-negotiable)

1. **Mock-first.** Every external dependency (node agent, backend engine, resource estimator, clock, telemetry sink, discovery, tunnel, store) is reached through an `internal/ports` interface and has a hand-written mock in `test/mocks` that records calls and can inject failures. Build the contract and its mock before the implementation.
2. **Inject the clock.** Never call `time.Now()` or `time.Sleep` in code under test. Take a `ports.Clock`; tests drive time with `FakeClock`. Aging, timeouts, TTLs, heartbeats, and backoff must all be deterministic under a fake clock.
3. **Conformance + compile-time both.** Every interface/impl pair gets a compile-time `var _ ports.X = (*Impl)(nil)` assertion **and** a behavioral conformance suite (`test/contract`) run against both the mock (fast) and the real implementation (smoke). Shape drift is caught by the compiler; behavioral drift by the suite. Do both.
4. **Fail loud, never quiet** (Doc 1 §3.11). Do not deploy on a failed resource estimate. Do not silently fall back to OpenAI-compatible routing for an unknown provider profile (require an explicit opt-in). Do not requeue a non-overflow error as if it were a context overflow. Do not let protocol translation emit corrupted output (a malformed tool-call must error, not become `{}`). For an autonomous control plane, a loud stop beats quiet corruption.
5. **Constructor injection only.** No package-level singletons, no `init()` wiring, no monkey-patching. Dependencies enter through constructors.
6. **Don't reintroduce a rejected design.** Doc 1 §6 lists 13 decisions and what each rejected. If a simpler-looking path contradicts one — naive FIFO scheduling, model-as-the-loadable-unit, hard-preemption-by-default, weights-only fit, in-process engine bindings, Docker-based Mac workers, a Python control plane — it was already ruled out. Don't "improve" back into it.

## Coverage gates

- 85%+ lines per module overall.
- **100%** on `internal/scheduler`, `internal/lease`, every conformance suite, and the fixture factories.
- Every error path tested; every public method tested; every mock's failure-injection exercised somewhere.

## Naming & layout conventions

- Binary: **`mycelium`**, run as `mycelium server` or `mycelium node`. Control CLI: **`myce`**. Decision/observability headers: **`X-Myc-*`**. Module path: `mycelium`.
- "**fleet**" (lowercase) is the common noun for the set of machines — Mycelium is the network across the fleet. It is not a second product name; don't rename it.
- Navigate by path: code lives under `internal/<module>` mirroring Doc 1 §5. Domain types in `internal/domain` (no logic, std-lib only); interfaces in `internal/ports`.

## Workflow

- **One phase at a time**, gate-green before moving on. The gate is the literal commands in Doc 3, not a judgment call.
- **When stuck, ship the gate-passing 80%** and leave a `// TODO(phase-N): …`. Note what you deferred at the top of the commit/PR. A gate-passing partial beats a blocked whole.
- **Parallel-OK** work is marked in Doc 3; within a phase, independent modules can proceed concurrently once the phase's contracts exist.
- Where Doc 3 says *Your call*, the approach is yours within the stated hard requirements. The hard requirements are not yours to relax.

## Autonomous operation (running under `/goal`)

This repo is built to be implemented by a long-running autonomous agent (Codex `/goal` or Claude Code `/goal`) with no human in the loop until a real wall is hit. The phase gates and conformance suites are the self-proof; the rules below keep you from drifting and tell you exactly when to stop.

**Decision protocol — three tiers:**

1. **Decide and log.** Anything Doc 3 marks *Your call*, plus every ordinary implementation choice, is yours. Make the best choice within the stated hard requirements, then record it in `DECISIONS.md` (one line: the choice + why). Don't stop to ask — decide and log.
2. **Defer and log.** If a piece is genuinely blocked, or needs an opinion you can't responsibly make alone (a real product judgment, a security/cost tradeoff with no clear answer, a spec point too ambiguous to resolve), set it aside: leave a `// TODO(blocked):` at the site, add an entry to `BLOCKERS.md` (what's blocked, why, what would unblock it), and keep working on everything that isn't blocked. Never stall the whole run on one stuck item.
3. **Terminal stop.** Stop the goal only when *all remaining* work falls into one of: (a) it requires hardware you don't have (see below), or (b) it requires a human decision recorded in `BLOCKERS.md` that you cannot proceed past. Then write a final `BLOCKERS.md` summary and stop. **Reaching this state with everything else green is success, not failure** — that is the intended end condition, not an error.

**Self-proof loop (this is how you don't drift):**

- After every unit of work, run the fast gates: `go build ./... && go vet ./... && go test ./... -race`. A red gate means fix it before continuing — never build on a red base.
- At a phase boundary, run that phase's full gate from Doc 3, including its named behavioral checks. **Do not advance to phase N+1 until phase N's gate is green.** The gate is the literal commands, not your judgment that it "looks done."
- The gates and conformance suites exist precisely so you can prove your own work and a reviewer can re-prove it. Trust them over your intuition.

**Drift prevention — the spec is read-only:**

- Treat `01-project-spec.md`, `02-testing-architecture.md`, and `03-development-guide.md` as the immutable contract. **Do not edit them.** If you believe the spec is wrong or incomplete, do NOT silently diverge in code and do NOT rewrite the spec to match a shortcut — add a `PROPOSED SPEC CHANGE` entry to `DECISIONS.md` (what, why, the change you'd make) and keep building to the spec as written until a human ratifies it.
- The locked decisions in Doc 1 §6 and the contracts in Doc 2 §2 win over anything in the reference repos or your own instinct.

**Hardware reality (what you can and cannot test yourself):**

- The dev machine is a local dev Mac **M4 Max, 64 GB**. It runs small GGUF models via llama.cpp Metal, so it is itself a real single node. **All of Phase 0 (mocks, zero hardware) and the single-node parts of Phase 1's smoke gate — real load → ready-gate → serve → graceful-stop → telemetry metric → reactive requeue on a small model — you can and should do yourself.**
- A **second machine (a second peer)** is available for multi-node testing, but only if its IP is in your environment/config. Anything needing a second node — the multi-node fleet smoke in Phase 1, the join smoke in Phase 4 — is a **defer-and-log** item (note "needs second peer address" in `BLOCKERS.md`) unless that IP is provided.
- Specialty hardware (the 4090 / desktop GPU / B70 boxes, the DGX Spark, vLLM/CUDA paths, `catastrophic`-OOM behavior, large models) is **not** available to you. Build those paths to the spec and cover them with mocks + conformance suites in the fast tiers; their real-hardware smoke checks are terminal-stop / human-run items — do not grind on them.

## Implementation gotchas (learned from the reference repos — watch for these)

- **Reap orphaned backends on startup.** A crashed node agent must not leave a zombie inference server holding VRAM. The agent's startup reaper finds and cleans up processes/containers from a prior run (Doc 1 §3.10).
- **A loading model already occupies its unit.** Count in-flight loads against capacity, not just running instances — this is why a `catastrophic` unit refuses stacked loads.
- **The node sheds; the control plane queues.** A saturated node returns a fast 429-style rejection; it never builds a local queue. Queueing, priority, and retry live in the scheduler.
- **Computed tuning must reach the launch.** If the scheduler computes offload layers or tensor-split, inject them into the backend command — don't compute and discard them.
- **Node-side parsing.** When the server can't see a model file, ask the owning node to parse it (gguf-parser locally) and return metadata; the server never needs the file locally.
- **SSE loading-state needs the no-buffer header.** When you write early SSE headers yourself (loading-state), set `X-Accel-Buffering: no` — a self-written status writer can miss what the reverse-proxy path sets automatically.
- **Guard the in-flight race window.** There is a window where a request has left the outer lock but hasn't registered on the per-instance in-flight wait group; a second guard at that boundary closes it before a graceful stop can race it.
- **Thread profile detail through passthrough.** Don't hard-code `/v1/messages` for Anthropic passthrough; use the selected endpoint's profile (`messages_path`, version, limitations).
- **Join token is membership, not auth.** Possessing the token lets a node join; it does not authorize backend operations. Make the token rotatable/revocable.

## Where the authority lives

The docs are the source of truth for *intent*; existing reference-repo code is information, not authority. When in doubt, the locked decisions in Doc 1 §6 and the contracts in Doc 2 §2 win. If you believe a decision is wrong, record it as a `PROPOSED SPEC CHANGE` in `DECISIONS.md` and keep building to spec — never silently diverge.
