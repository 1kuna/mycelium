# Mycelium Owner-Thread Handoff

Last updated: 2026-06-27

This file supersedes the previous root `HANDOFF.md`. Treat that prior version as
a partial coordinator draft, not as authority. This handoff is intentionally
scoped to continuity and next steps; it does not claim new validation from this
handoff rewrite.

## Current Repo State

- Branch: `main`.
- Latest observed commit before this handoff refresh: `27a1e5f Compact peer registry snapshots`.
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
- Current Atlas-facing inference route remains MacBook gateway to B70 for proven
  real work, and Spark is now also a proven corrected-LAN inference owner. On
  2026-06-27 the MacBook LaunchAgent `com.mycelium.real-fleet` listened at
  `192.168.0.143:55291`, B70 listened at `192.168.0.153:55283`, and Spark
  listened at `192.168.0.16:55274`. Authenticated MacBook diagnostics showed
  both B70 and Spark seeds ready. A tiny strict JSON-schema
  `/v1/chat/completions` request for `qwen3.5-9b` routed to warm B70 `inst_2`
  with `X-Myc-Node: b70`, `X-Myc-Backend: vllm`,
  `X-Myc-Decision: placed_on_warm_instance`, `X-Myc-Structured-Output:
  json_schema`, and `X-Myc-Guided-Decoding-Requested: true`, returning
  parseable final JSON. A matching strict JSON-schema request for
  `spark-qwen9b` routed to Spark `inst_2` with `X-Myc-Node: spark`,
  `X-Myc-Backend: vllm`, `X-Myc-Decision: placed_on_warm_instance`,
  `X-Myc-Structured-Output: json_schema`, and
  `X-Myc-Guided-Decoding-Requested: true`, returning parseable final JSON.
  Spark owner snapshot reported `spark-qwen9b-vllm` ready at
  `127.0.0.1:55374`.
- Peer registry snapshot sync was compacted in `27a1e5f` so peer RPC snapshots
  carry only rescue-relevant job records rather than all terminal history.
  Before deploying that code live everywhere, stale runtime job records were
  cleared from the MacBook, B70, and Spark control DBs with timestamped backups;
  authenticated `/registry/snapshot` responses then stayed small and below the
  16 MiB peer RPC body cap.
- On 2026-06-27 the current Linux binaries containing `27a1e5f` were deployed
  to B70 and Spark through their existing `mycelium-real-fleet.service`
  user-systemd units. B70 reported SHA256
  `28f56acec536eb29b32084073ce39276fac35494b0c2a52dcb6177fe816973ba`; Spark
  reported SHA256
  `842e62c3f81bbfa9084fe6dc1d9470decc469d4fa9f2a888758c46bd9e3ff175`.
  Post-update diagnostics showed MacBook, B70, and Spark mutually reachable
  across the corrected LAN, B70/Spark `/registry/snapshot` responses were
  compact empty snapshots, and strict JSON-schema canaries through the MacBook
  gateway succeeded on both B70 and Spark after cold load.
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

Known from repo and live evidence:

- `CODEX-FINDINGS.md` records a 2026-06-09 SSOT audit/triage and the fix pass
  status. Its P1/P2 runtime findings F-01 through F-07, F-12, F-14, and F-16
  were marked fixed there. F-08, F-09, and F-11 remain smoke proof gaps; F-13 is
  conformance expansion; F-15 is a spec-ratification/product-decision item.
- Local smoke was re-proven on 2026-06-26 with an existing LM Studio Ministral
  GGUF after fixing the stale coordinator-runtime wiring and the joined-node fit
  bug. Do not treat the old "smoke env unset" note as the current blocker.
- Atlas has used the live MacBook/B70 route for strict-schema semantic and
  overlay gates. B70 Qwen/vLLM structured output is activated with vLLM
  structured outputs enabled in reasoning while preserving the reasoning/content
  boundary.
- The Atlas gateway telemetry headers from `3f125c9` are live on the MacBook
  gateway. A 2026-06-27 canary observed `X-Myc-Cold-Load`,
  `X-Myc-Queue-Wait-Ms`, `X-Myc-Upstream-Wall-Ms`,
  `X-Myc-Structured-Output`, and `X-Myc-Guided-Decoding-Requested`.
- `make ci` on 2026-06-27 previously passed formatting, build, vet, and
  `go test ./... -race`, but failed the coverage gate because
  `internal/enginecatalog` reported 78.3% statement coverage under the 85.0%
  per-package floor. Follow-on repo-local coverage passes raised
  `internal/enginecatalog`, `internal/enginecompat`, `pkg/api`,
  `internal/modelcompat`, `internal/catalog`, `internal/bootstrap`,
  `internal/backends/llamacpp`, `internal/backends/processadapter`,
  `cmd/internal/controlcli`, `internal/backends/openvino`, and `test/mocks`
  above their required floors. The final 2026-06-27 coverage pass added
  focused behavior tests for `cmd/mycelium` command/runtime contracts, restored
  `internal/scheduler` to its 100.0% required floor, and restored
  `test/contract` to its 100.0% required floor. `make coverage` now passes
  with `coverage ok: total 89.4%`.
- A later 2026-06-27 repo-local reliability pass reproduced and fixed the
  cold-load/client-timeout stale-job lifecycle bug in scheduler state handling:
  after a job row exists, post-placement terminal state repair and owner cleanup
  now use `cleanupContext(ctx)` so client cancellation cannot strand durable job
  rows in `placing` after an accepted cold-load attempt fails. Focused tests
  cover both coordinated and local cold-load cancellation. Live fleet
  deployment/canary proof was intentionally not run in that repo-local chunk.
- The README now includes a non-mutating operator recipe for proving the same
  stale-job cleanup after a future live activation. It tags a deliberately
  client-canceled cold-load request with `X-Myc-Project`, finds the resulting
  job in the coordinator store, verifies terminal job/registry state with
  `myce debug job --json`, and preserves a focused `myce debug bundle`. This
  prepares the live proof path only; it does not claim the deployed MacBook,
  B70, or Spark services are running the stale-job fix.
- On 2026-06-29 the Linux peers were updated to binaries containing the
  stale-job cleanup fix through their existing user-systemd services. B70
  reported SHA256
  `22adabba230d1418e13449fe9a8bcfe5b604b4fd8c2f5179c2277c9dbf9da09f` and
  `service active`, `peer ready`, `seeds ready 1/1`; Spark reported SHA256
  `6e19e91f8b97115f7545f33425c93f669ea5104f8f559727d1b36e966ab1df9d` and
  `service active`, `peer ready`, `seeds ready 2/2`. The MacBook gateway was
  not activated on the fixed binary because local `sudo` authentication failed
  before any user LaunchAgent bootout or system LaunchDaemon install happened.
  The existing user LaunchAgent remained on `/Users/zach/.mycelium/bin/mycelium`
  and rechecked healthy with B70/Spark seeds ready. A named fixed Darwin binary
  was prepared at `/Users/zach/.mycelium/bin/mycelium-99784cc`, but live Mac
  activation still needs valid local admin credentials or Zach running the
  documented `service install --system` command. A timeout canary against the
  still-old Mac gateway reproduced the stale `placing` job / `loading` registry
  bug; evidence is preserved under
  `/tmp/mycelium-stale-job-blocker-20260629T112957Z`, and the single row created
  by that verification attempt was manually terminalized after backing up the
  control DB.
- A follow-on 2026-06-29 read-only diagnostic pass found the Mac user
  LaunchAgent LAN failure is best explained by binary identity / Local Network
  privacy behavior rather than Mycelium config, listener binding, or launchd
  working-directory drift. The working live LaunchAgent still uses
  `/Users/zach/.mycelium/bin/mycelium` with the same peer config and reaches
  B70/Spark seeds. That old binary is ad-hoc/linker-signed as identifier
  `a.out` with CDHash `28e65885c95018a65a13c38b4251dd9edac7072f`; the prepared
  fixed Darwin binary `/Users/zach/.mycelium/bin/mycelium-99784cc` is a
  different ad-hoc identity, identifier
  `mycelium-99784cc-55554944e514dd7e344b3e12f6256629593404fd`, CDHash
  `c81c1321f2fb039cf3ef1c7a2d4eee0691068f2a`. Historical LaunchAgent logs for
  the current-binary attempt show the peer could start but then got
  `no route to host` for both corrected-LAN seeds, while the old binary under
  the same user LaunchAgent succeeds. The repo README now has a read-only
  checklist for comparing LaunchAgent plist, signing identity, quarantine/
  provenance xattrs, and logs. Without valid sudo/admin credentials, the only
  remaining non-root experiment is a deliberately approved stable signed
  bundle/helper identity plus a maintenance-window user LaunchAgent swap; that
  still mutates the live gateway and cannot live-prove the stale-job fix until
  the swap succeeds.

This handoff refresh performed repo authority inspection, `make ci`, a narrow
MacBook/B70 live canary, and Spark reachability classification. A later
repo-local coverage continuation ran focused package tests plus `make coverage`
and did not touch live services. It did not push.

## Active Blockers And Cautions

- Real smoke state is drift-prone. Mac mini, B70, and Spark availability must be
  verified live before claiming fleet health. Current evidence proves MacBook to
  B70 inference and MacBook to Spark inference; re-check live health before
  treating either owner as available for new scale work.
- The MacBook gateway binary itself remains intentionally rolled back to the
  known-good LaunchAgent-approved binary because the current Darwin binary loses
  corrected-LAN seed reachability under the user LaunchAgent. Do not replace
  `/Users/zach/.mycelium/bin/mycelium` again without a rollback-ready test or a
  stable LaunchDaemon/bundled-signing activation plan. The repo now provides a
  read-only `mycelium service plan` command; use it with `--system` and
  `--binary /Users/zach/.mycelium/bin/mycelium` to inspect the exact
  LaunchDaemon unit, install/status/reload commands, and rollback command before
  any privileged Mac activation attempt.
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
- The Atlas/Qwen reasoning-boundary repair details are preserved in
  `artifacts/atlas-serving-diagnostics/20260616-141132-qwen9b-reasoning-boundary/SUMMARY.md`.
  The important behavior is narrow: non-streaming Qwen3-family vLLM responses
  that length-finish inside `<think>` are normalized by moving leaked content
  into `reasoning` / `reasoning_content` and emptying final `content`. Streaming
  normalization was not implemented there.
- The cold-load/client-timeout stale-job issue observed in the older Atlas/Qwen
  lane now has repo-local scheduler regression coverage and a fix. Do not claim
  the deployed MacBook/B70/Spark services have this fix until they are updated
  and the README cold-load cancellation proof recipe proves terminal job cleanup
  on the live route.
- As of 2026-06-29, B70 and Spark have binaries containing the stale-job fix,
  but the MacBook gateway remains on the old known-good user LaunchAgent binary.
  Therefore the active consumer route is not live-proven for the stale-job fix.
  The next operator must activate the fixed Mac binary through the hardened
  system LaunchDaemon path from commit `99784cc`, using valid local admin
  credentials, before rerunning the cold-load cancellation proof.
- The non-root Mac user LaunchAgent path is not yet a safe activation route.
  Read-only diagnostics point to macOS binary identity / Local Network privacy
  behavior: the old ad-hoc binary identity reaches LAN peers, while the newer
  ad-hoc identity historically started but lost routes to both corrected-LAN
  seeds under launchd. Do not perform another blind user LaunchAgent binary
  swap. Use the README diagnostic checklist first, then either obtain valid
  admin credentials for the system LaunchDaemon route or get explicit approval
  for a stable signed bundle/helper identity maintenance test.
- Older thread dumps contain exact live-fleet endpoints, token-file locations,
  peer ports, and TTFT/TPS observations. Treat those as stale private
  operational evidence unless rechecked live; do not publish token values or
  host-specific private paths.

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

6. For the MacBook registry-compaction activation blocker, do not swap the user
   LaunchAgent binary blindly. First generate the non-mutating system service
   plan:

   ```bash
   mycelium service plan \
     --config /Users/zach/.mycelium/bench/real-fleet/peer.json \
     --binary /Users/zach/.mycelium/bin/mycelium \
     --name real-fleet \
     --system
   ```

   Then install only in a maintenance window with a known-good binary backup
   and immediate `service status` / seed-diagnostics rollback criteria.

7. For long-window optimizer/autoscaling work, use the existing
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
