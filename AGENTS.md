# AGENTS.md — working rules for Mycelium

You are building **Mycelium**, a hardware-aware inference control plane (a single Go binary that conducts existing inference engines across a fleet of heterogeneous machines). This file is the operating manual for any agent working in this repo. Read it before writing code.

## Repo and document map

- `cmd/` — peer and control CLI entrypoints; `internal/domain/` — data types; `internal/ports/` — dependency contracts; other `internal/` packages — implementations.
- `test/` — mocks, fixtures, conformance, integration, e2e, and hardware smoke tests.
- `01-project-spec.md` — behavior (§3), data shapes (§3.8), layout (§5), and locked decisions (§6). Read the sections governing the change.
- `02-testing-architecture.md` — interface contracts, mocks, `FakeClock`, conformance suites, and CI tiers. Consult when changing an interface or choosing verification.
- `03-development-guide.md` — phase dependencies and exact acceptance gates. Read the current phase and its prerequisites; gates must pass before advancing.
- `skills/` — area-specific guidance for backend adapters, KV estimation, and the scheduler model.
- `DECISIONS.md` / `BLOCKERS.md` — relevant decisions and unresolved dependencies; consult entries affecting the task.

Read by task and follow relevant dependencies; a small change does not require the entire document stack. The workflow below reflects Zach's approved instruction policy and supersedes older blanket reading/testing cadence and partial-completion language in the guides. Product requirements, locked decisions, coverage bars, and phase gates remain binding.

## Build & test

During iteration, format touched Go files and run affected package tests (with `-race` for concurrency changes). Broaden checks for shared contracts or cross-package changes; use the complete commands below for integration candidates and the exact guide gate at each phase boundary. Do not reformat unrelated files.

```bash
go build ./...                                   # must be clean
go vet ./...                                     # must be clean
gofmt -l .                                       # inspect output; format only files in scope
go test ./... -race                              # fast tiers: unit + contract + integration + e2e — NO hardware
go test ./... -covermode=atomic -coverprofile=all.out && go tool cover -func=all.out | grep total
go test -tags smoke ./test/smoke/... -timeout 20m   # smoke: REAL engines/machines — run only at phase boundaries
```

`go test ./...` must pass on a local dev Mac with **nothing powered on**. The `smoke` build tag (`//go:build smoke`) is the only thing that touches real hardware; never let a hardware dependency leak into the fast tiers.

## Hard rules (non-negotiable)

1. **Mock-first.** Every external dependency (node agent, backend engine, resource estimator, clock, telemetry sink, discovery, tunnel, store) is reached through an `internal/ports` interface and has a hand-written mock in `test/mocks` that records calls and can inject failures. Build the contract and its mock before the implementation.
2. **Inject the clock.** Never call `time.Now()` or `time.Sleep` in code under test. Take a `ports.Clock`; tests drive time with `FakeClock`. Aging, timeouts, TTLs, heartbeats, and backoff must all be deterministic under a fake clock.
3. **Conformance + compile-time both.** Every interface/impl pair gets a compile-time `var _ ports.X = (*Impl)(nil)` assertion **and** a behavioral conformance suite (`test/contract`) run against both the mock (fast) and the real implementation (smoke). Shape drift is caught by the compiler; behavioral drift by the suite. Do both.
4. **Fail loud, never quiet** (Doc 1 §3.11). Do not deploy on a failed resource estimate. Do not silently fall back to OpenAI-compatible routing for an unknown provider profile (require an explicit opt-in). Do not requeue a non-overflow error as if it were a context overflow. Do not let protocol translation emit corrupted output (a malformed tool-call must error, not become `{}`). Do not hide a partition or a stale-fence rejection — surface it and leave registry evidence. For an autonomous control plane, a loud stop beats quiet corruption.
5. **Constructor injection only.** No package-level singletons, no `init()` wiring, no monkey-patching. Dependencies enter through constructors.
6. **Peer model is law** (Doc 1 §3.12, D14–D18). No fleet leader and no permanent server: the per-job coordinator is whoever received the job. **Coordinator decides; the resource owner commits** — a coordinator never mutates another node's state, it proposes and the owner's local transaction commits or rejects (`ErrStaleFence`) via optimistic concurrency. Resilience is a small replicated **job registry + heartbeats**, never a consensus event-log. **Job distribution, never model distribution** — one model loads fully on one machine; do not shard a model across machines (no MLX-distributed / pipeline-parallel / hostfile / rank). No self-preference in placement. No SSH transport.
7. **Don't reintroduce a rejected design.** Doc 1 §6 lists the decisions and what each rejected. If a simpler-looking path contradicts one — naive FIFO scheduling, model-as-the-loadable-unit, hard-preemption-by-default, weights-only fit, in-process engine bindings, Docker-based Mac workers, a Python control plane, a single fleet leader / elected scheduler host (D14), a replicated consensus event-log as source of truth (D15/D16), cross-coordinator pre-send negotiation (D15), SSH-based peer transport (D12), or model sharding across machines (D17) — it was already ruled out. Don't "improve" back into it.

## Coverage gates

- 85%+ lines per module overall.
- **100%** on `internal/scheduler`, `internal/lease`, the federation authority/recovery packages (`internal/node/admission`, `internal/peer/coordinator`, `internal/peer/recovery`), every conformance suite, and the fixture factories.
- Every error path tested; every public method tested; every mock's failure-injection exercised somewhere.

## Naming & layout conventions

- Binary: **`mycelium`** — one role, a **peer**, started with `mycelium` (compute on/off via config/flag); there is no `server`/`node` subcommand and no leader. Control CLI: **`myce`**. Decision/observability headers: **`X-Myc-*`**. Module path: `mycelium`.
- "**fleet**" (lowercase) is the common noun for the set of machines — Mycelium is the network across the fleet. It is not a second product name; don't rename it.
- Navigate by path: code lives under `internal/<module>` mirroring Doc 1 §5. Domain types in `internal/domain` (no logic, std-lib only); interfaces in `internal/ports`.

## Workflow and authority

- Work one phase at a time. Use the exact gate and behavioral checks in `03-development-guide.md`; do not advance until it passes. Its parallel-safe work can proceed independently once the required contracts exist.
- Decide routine implementation details within the contract. Record choices that affect future work in `DECISIONS.md`; ask only about consequential unresolved product, architecture, security, or cost decisions.
- Commit verified checkpoints and continue toward the complete requested outcome. A passing gate is not permission to omit requested work or relax acceptance criteria.
- For blocked work, leave a relevant `// TODO(blocked):` and a `BLOCKERS.md` entry stating what, why, and what would unblock it. Continue independent authorized work. Stop only when done or all remaining work requires an external dependency or human decision; report the latter as blocked, not complete.
- Fix failures caused by the change before building dependent work on it. Record unrelated failures and their verification impact; never hide or weaken a gate.
- `01-project-spec.md`, `02-testing-architecture.md`, and `03-development-guide.md` are read-only contracts. Record a `PROPOSED SPEC CHANGE` in `DECISIONS.md` rather than editing them or silently diverging. Locked decisions in Doc 1 §6 and contracts in Doc 2 §2 govern over reference repos or intuition. Only the reading/testing cadence and checkpoint policy explicitly updated above supersede older workflow wording.
- Do not create or maintain a root `HANDOFF.md` unless Zach asks. Durable truth belongs in the SSOT docs, `DECISIONS.md`, `BLOCKERS.md`, `CODEX-FINDINGS.md`, topic runbooks, tests, or PM/project coordination surfaces.

## Hardware reality

- Phase 0 is entirely hardware-free. The dev Mac can run small GGUF models through llama.cpp Metal: with the required model/environment present, validate Phase 1's single-node load → ready → serve → graceful-stop → telemetry → reactive-requeue smoke path.
- A second peer is available only when its address is supplied in environment/config. Otherwise record the multi-node Phase 1 and Phase 4 smoke gates as needing a second peer address.
- Do not assume specialty NVIDIA/Intel/Apple accelerator hosts, vLLM/CUDA, catastrophic-OOM behavior, or large models are available. Implement their contracts with mocks and conformance tests; report unavailable real-hardware checks as blocked rather than grinding or fabricating results.

## Implementation gotchas (learned from the reference repos — watch for these)

- **Reap orphaned backends on startup.** A crashed node agent must not leave a zombie inference server holding VRAM. The agent's startup reaper finds and cleans up processes/containers from a prior run (Doc 1 §3.10).
- **A loading model already occupies its unit.** Count in-flight loads against capacity, not just running instances — this is why a `catastrophic` unit refuses stacked loads.
- **The node sheds; the coordinator queues.** A saturated node returns a fast 429-style rejection; it never builds a local queue. Queueing, priority, and retry live in the coordinator (the Placer).
- **Computed tuning must reach the launch.** If the Placer computes offload layers or tensor-split, inject them into the backend command — don't compute and discard them.
- **Node-side parsing.** When the coordinator can't see a model file, ask the owning node to parse it (gguf-parser locally) and return metadata; the coordinator never needs the file locally.
- **SSE loading-state needs the no-buffer header.** When you write early SSE headers yourself (loading-state), set `X-Accel-Buffering: no` — a self-written status writer can miss what the reverse-proxy path sets automatically.
- **Guard the in-flight race window.** There is a window where a request has left the outer lock but hasn't registered on the per-instance in-flight wait group; a second guard at that boundary closes it before a graceful stop can race it.
- **Thread profile detail through passthrough.** Don't hard-code `/v1/messages` for Anthropic passthrough; use the selected endpoint's profile (`messages_path`, version, limitations).
- **Join token is membership, not auth.** Possessing the token lets a peer join; it does not authorize backend operations. Make the token rotatable/revocable.
- **Owner commits; coordinator proposes (optimistic concurrency).** A coordinator never writes a remote node's lease state. It calls the owner's `AdmissionController.Commit` with the fence from the owner's offer; a stale fence returns `ErrStaleFence` and the coordinator re-plans. The owner's local transaction is the only serialization point — that is what prevents two coordinators double-booking one accelerator. Never add a global lock or a shared mutable queue to "fix" a race the owner already resolves.
- **Registry is for visibility + rescue, not authority.** The job registry is eventually-consistent and may be briefly stale; always re-check the live owner before rescuing a job a dead peer was running (it may have finished). Never treat the registry as the source of truth for what's running — the owner is.
