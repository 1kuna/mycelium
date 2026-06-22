# Mycelium Handoff

Last updated: 2026-06-22
Repo: `/Users/zach/Developer/mycelium`
Branch: `main`
Remote: `origin https://github.com/1kuna/mycelium.git`

## Current State

Mycelium is the local inference fleet conductor: peers advertise capacity, the
coordinator schedules work, owners launch backend instances, and the gateway
exposes OpenAI-compatible routes to downstream projects such as Atlas.

The current local tree is a checkpoint of several follow-on slices after the
docs 01-03 SSOT audit:

- SSOT audit findings were written to `CODEX-FINDINGS.md` and then a bug-fix
  pass addressed the high-signal drift around queue draining, peer snapshot
  degradation, gateway failover, context overflow replanning, runtime metadata,
  and recovery continuation.
- Atlas/qwen serving diagnostics found that qwen3.5-9b on B70/vLLM could leak
  unfinished reasoning into OpenAI `message.content` when generation
  length-finished before the model emitted its reasoning boundary. The gateway
  now preserves first-class `reasoning` / `reasoning_content` fields and
  normalizes that Qwen length-finish boundary without disabling thinking.
- Engine/bootstrap work was expanded from static profiles into read-only host
  discovery, saved engine readiness facts, readiness-aware preset loading,
  model compatibility advice, and advisory install plans.
- The engine capability catalog now covers llama.cpp, vLLM, SGLang, MLX,
  OpenVINO, and custom profiles. It distinguishes `can_run_now`,
  `can_run_after_engine_setup`, `needs_artifact_format`, `not_supported`,
  blocked, incomplete compatibility keys, and legacy-unverified rows.
- `engines install-plan` is advisory-only. It emits approval/manual/dry-run
  actions, risks, and rollback notes. Runtime update checks and automatic
  rollback-aware upgrades are deliberately deferred in `04-engine-bootstrap.md`.

Local diagnostic artifacts under `artifacts/` are not intended for git history.
They include B70/Atlas serving evidence and a copied Linux binary from the live
reasoning-boundary deployment. The durable state for transfer is this source
tree, `HANDOFF.md`, `CODEX-FINDINGS.md`, `DECISIONS.md`, and the tests.

## Important Invariants

- Do not disable model thinking to hide leaked reasoning. Preserve reasoning at
  the provider/diagnostic boundary and expose only final answer content to
  downstream parsers.
- Treat direct vLLM output as lower-level evidence than Mycelium gateway output.
  Direct vLLM can still expose unfinished thinking in `content`; the Mycelium
  boundary owns the normalized downstream contract.
- Runtime installs, engine setup, downloads, service restarts, and upgrade checks
  require explicit operator approval. The current install-plan path is
  non-mutating.
- Saved engine readiness facts may authorize or block startup preflight. Missing
  old manual facts stay visible as `legacy_config_unverified`, not silently
  trusted as fully ready generated profiles.
- Before making claims about a live B70/Spark route, verify the exact preset,
  model id, owner instance, backend process, and gateway response shape.

## Most Recent Live Atlas/Qwen Evidence

The last live reasoning-boundary slice used B70 with qwen3.5-9b through vLLM
and Mycelium. The useful artifact root is local and ignored:

```text
artifacts/atlas-serving-diagnostics/20260616-141132-qwen9b-reasoning-boundary/
```

Key result:

- `reasoning_content` / equivalent is preserved separately when vLLM produces
  it.
- Qwen length-finished reasoning-only responses are normalized so final
  `content` is empty instead of polluted with unfinished thinking.
- Tiny text, one-image, and two-image canaries were run after the patched binary
  was deployed to B70.
- The next Atlas step was to retry Atlas overlay/source extraction through the
  Mycelium gateway, not to change Mycelium routing again unless new raw evidence
  shows a boundary regression.

## Current Open Work

This checkpoint has been committed and pushed to `origin/main`. The remaining
work is product/runtime continuation, not repo preservation.

1. If resuming Mycelium implementation, run the smallest focused tests first:
   gateway reasoning-boundary tests, model compatibility tests, engine catalog
   tests, install-plan tests, and bootstrap/readiness tests.
2. Before any live-serving claim, capture fresh owner/gateway facts from the
   live route. Warm-instance state is time-sensitive.
3. Before implementing engine apply, design the approval/apply/rollback boundary
   from `04-engine-bootstrap.md` Future Phase G. Do not turn advisory plans into
   mutation by shortcut.
4. Runtime update checks and automatic upgrades belong to the later update phase,
   not the current install-plan checkpoint.

## Known Blockers Or Cautions

- The live artifacts are intentionally local and ignored; a new system will not
  have the raw B70 diagnostic bodies unless they are copied separately.
- Some audit files besides `CODEX-FINDINGS.md` (`FINDINGS.md`,
  `FABLE-FINDINGS.md`) are broad/older audit evidence. Use `CODEX-FINDINGS.md`
  plus current code as the primary audit handoff unless Zach asks to preserve the
  other audit variants.
- Live B70/Spark checks may be stale after machine restarts or model unloads.
  Verify, do not infer from this document.

## Suggested Resume Prompt

Read `HANDOFF.md`, `DECISIONS.md`, `CODEX-FINDINGS.md`, and
`04-engine-bootstrap.md`. Then inspect `git status` and the latest pushed commit.
If the task is Atlas integration, verify the current B70 qwen3.5-9b route through
Mycelium with a tiny text canary before running any expensive Atlas page eval.
If the task is engine bootstrap, keep the path advisory/read-only until an
explicit apply/rollback design is approved.
