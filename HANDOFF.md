# Mycelium Owner-Thread Handoff

Last updated: 2026-06-22

This file supersedes the previous root `HANDOFF.md`. Treat that prior version as
a partial coordinator draft, not as authority. This handoff is intentionally
scoped to continuity and next steps; it does not claim new validation from this
handoff rewrite.

## Current Repo State

- Branch: `main`.
- Latest observed commit before this rewrite: `f382bd2 Clarify Mycelium handoff resume state`.
- Remote already configured: `origin https://github.com/1kuna/mycelium.git`.
- Source-of-truth docs remain `01-project-spec.md`, `02-testing-architecture.md`,
  and `03-development-guide.md`.
- `04-engine-bootstrap.md` is a future/extension spec. It should guide future
  bootstrap work, but it is not part of the current MVP proof gate unless Zach
  explicitly promotes it.
- GitHub Actions were intentionally disabled remotely because there are no
  runner credits. Use local `make ci`.

## Product Shape To Preserve

Mycelium is peer-native and LAN-local:

- `mycelium` is one peer daemon. There is no permanent server/node split.
- Any peer may coordinate a request.
- The selected resource owner commits leases and capacity locally.
- Registry replication is for visibility and rescue, not global authority.
- There is no fleet leader, consensus event log, SSH product transport, public
  WAN requirement, or model sharding across machines.
- Runtime placement must go through scheduler/coordinator plus owner admission.
  Do not reintroduce direct coordinator mutation of remote state.

## What Is Already Implemented

The repo has gone through several substantial implementation passes. The
important continuity points are:

- Durable runtime/control-plane storage exists for peer state, catalog/runtime
  metadata, jobs, leases/admission, telemetry, recommendations, and registry
  data.
- Owner admission and scheduler placement are the intended runtime authority:
  placement proposes, owner admission commits or rejects with fences, and live
  requests release through the owner path.
- Resource fit work includes multi-accelerator/unit-aware claims, context/KV
  accounting, reservations/pinned protection, disk-headroom placement filtering,
  max-util limits, and backend-aware estimation paths.
- Runtime hardening after the SSOT audits includes dynamic backend launch ports,
  process identity/reaping, gateway retries and queue draining fixes, degraded
  peer failures during recovery, runtime catalog metadata persistence, gateway
  token discovery, and operator diagnostic commands.
- Real fleet benchmarking machinery exists. It emits artifacts such as
  `report.html`, `manifest.json`, `events.jsonl`, `results.json`,
  `snapshots.jsonl`, `failures.json`, and per-job outputs. Historical live runs
  included MacBook/Mac mini/B70/Spark scenarios, but do not assume those exact
  services are still up without checking.
- Session telemetry was added on 2026-06-04. `session_metrics` records phase
  time-series samples alongside `run_metrics`; owner peers are authoritative for
  run/session telemetry, and remote-owner telemetry goes over authenticated
  telemetry RPC.
- The CLI exposes telemetry inspection via `myce telemetry samples`.
- Current MVP behavior intentionally rejects or disables some future-ish paths:
  sticky/private/overlay runtime choices were disabled in the bug-debt pass,
  `X-Myc-Handling: private` is rejected rather than accepted unsafely, and
  submitter policy is not wired until an authenticated caller policy exists.
- Engine bootstrap/adoption is partly implemented as host-local readiness and
  doctor tooling rather than full automatic engine installation. Current pieces
  include `bootstrap --doctor --save-plan`, `mycelium engines list|doctor|
  preflight|apply`, saved exact engine readiness facts, readiness-aware preset
  loading/preflight, `mycelium models compat`, `mycelium engines install-plan`,
  OpenVINO profile/wrapper support, and multi-backend host support. Install
  planning remains advisory unless explicitly applied.

## Validation Status

Do not report this handoff rewrite as a validation pass.

Known from repo evidence:

- `CODEX-FINDINGS.md` records a 2026-06-09 SSOT audit/triage and the fix pass
  status. Its P1/P2 runtime findings F-01 through F-07, F-12, F-14, and F-16
  were marked fixed there. F-08, F-09, and F-11 remain smoke proof gaps; F-13 is
  conformance expansion; F-15 is a spec-ratification/product-decision item.
- `BLOCKERS.md` has one active blocker from 2026-06-04: real-engine and real
  multi-peer smoke for the obvious-bug remediation pass could not be re-proven
  in that pass because smoke environment variables were unset. It says fast CI
  and env-independent smoke passed from that tree, but this handoff did not
  re-run them.

This handoff pass only performed read-only inspection plus this document edit.
It did not run `make ci`, broad tests, real smoke, or any push.

## Active Blockers And Cautions

- Real smoke state is drift-prone. Mac mini, B70, and Spark availability must be
  verified live before claiming fleet health.
- `ops/private/` contains local smoke notes/bundles. Treat them as private
  operational evidence; do not publish secrets, tokens, host-specific paths, or
  model artifacts.
- Spark memory must remain capped below the danger zone. Historical safe vLLM
  guidance used conservative caps such as `--gpu-memory-utilization 0.70`; docs
  require rejecting unsafe Spark configs at or above the 0.90 region.
- Disk headroom is product-important and implemented, but `CODEX-FINDINGS.md`
  still records it as SSOT drift unless the core docs have since been ratified.
- Full automatic cross-OS/architecture engine installation is not done. The
  current implementation is readiness discovery, doctor, compatibility, and
  explicit apply/adoption. `04-engine-bootstrap.md` is the spec outline for the
  future full bootstrap layer.
- Do not silently disable model reasoning/thinking to hide leaks or parser
  issues. Fix the provider payload, response boundary, or gateway translation
  path instead.

## Exact Resume Steps

Start every new continuation with:

```bash
git status --short
git log --oneline -8
sed -n '1,220p' HANDOFF.md
sed -n '1,220p' DECISIONS.md
sed -n '1,220p' BLOCKERS.md
sed -n '1,260p' CODEX-FINDINGS.md
```

Then choose the lane:

1. For ordinary code continuation, re-read the relevant SSOT section and the
   local code path, then run the smallest focused tests before `make ci`.
2. For current MVP proof, run local gates first:

   ```bash
   make ci
   ```

3. For live fleet proof, first verify peer health and configured tokens, then
   run phase-boundary smoke only with the required env/config:

   ```bash
   make smoke-local
   make smoke-fleet
   make smoke-benchmark-fleet
   ```

   Add `make smoke-spark-vllm` and `make smoke-b70` only when those machines and
   model paths are intentionally available.

4. For the Atlas/Qwen serving lane, do a tiny gateway canary first. Inspect the
   selected owner, preset, backend process, response `content`, and any
   `reasoning`/`reasoning_content` fields before running larger evals.
5. For engine bootstrap work, stay within the current readiness/adoption line
   unless Zach explicitly asks for install implementation. Useful commands:

   ```bash
   mycelium bootstrap --doctor --config ~/.mycelium/peer.json
   mycelium engines list
   mycelium engines doctor
   mycelium engines preflight
   mycelium models compat
   mycelium engines install-plan
   ```

6. For long-window optimizer/autoscaling work, use the existing
   `session_metrics` and `run_metrics` facts first. Add statistical analysis and
   recommendations from observed series before attempting any automatic
   saturation behavior.

## Files Worth Checking First

- `01-project-spec.md`, `02-testing-architecture.md`, `03-development-guide.md`
- `04-engine-bootstrap.md`
- `DECISIONS.md`
- `BLOCKERS.md`
- `CODEX-FINDINGS.md`
- `README.md`
- `cmd/mycelium/`
- `cmd/internal/controlcli/`
- `internal/gateway/`
- `internal/node/`
- `internal/peer/`
- `internal/scheduler/`
- `internal/store/sqlite/`
- `internal/telemetry/`
- `internal/bench/`

## Commit Guidance

Keep commits logical and local until the tree is proven. For this handoff task,
commit only `HANDOFF.md` and do not push.
