# Mycelium v2 engines independent review

Reviewed on branch `v2-fairness` at commit
`422ba635781037279044b4e385be6a949f48194e`. This review read `AGENTS.md`,
the approved `docs/design-proposal.md`, and `.reports/v2-engines-report.md`.

## Boundary

The implementation review covered `internal/myce/engine.go`, `server.go`,
`measure.go`, `model_files.go`, their tests, and the `API.modelCall` transport
portion of `models.go` added to the ownership boundary during review. Concurrent
fairness work in `maintainModels`, `ModelCall`, queue/types/worker code, CLI code,
and new fairness/usage/status files was preserved. `http.go` was read where
needed but not edited. No real machine, LibreOffice, fleet service, commit, or
push was used.

## Proven defects fixed

1. **Generic requests were buffered twice and rejected above 64 MiB.** The
   queue now writes generic bodies to a local, bounded 8 GiB temporary spool,
   removes it after the call, and rewinds it for the one allowed transport
   retry. The worker passes the incoming reader directly to the engine instead
   of allocating another full-body buffer. OpenAI JSON routes retain their
   existing 64 MiB in-memory limit. Content length, method, escaped path, query,
   end-to-end headers, cancellation, routing-header stripping, response status,
   and response streaming remain intact. Invalid routing metadata is rejected
   before any large spool is created.
2. **The first replay attempt closed the shared spool.** A regression using an
   actual first-worker connection abort found that Go's HTTP transport closes a
   raw `*os.File` request body. Each attempt now receives a bounded reader that
   cannot close the shared file; the second worker receives identical bytes and
   content length.
3. **Health redirects could produce false readiness.** The readiness client
   followed a loopback server's redirect and accepted a different endpoint's
   `2xx`. Redirects are no longer followed, and a process exit already visible
   when health turns successful is rejected.
4. **Mixed-backend device selectors leaked across process types.** Applying a
   CUDA binding could retain XPU selectors, and applying XPU could retain CUDA.
   Device binding now clears all supported accelerator selectors before setting
   only the selected platform's variables.
5. **Many-file recovery retained every source descriptor until the directory
   walk ended.** On a typical macOS 256-descriptor limit, ordinary HF-style
   bundles could fail recovery. Each input file is now closed immediately; a
   300-file regression covers the laptop failure shape.
6. **Unused request abstraction.** The unreferenced `RequestEngine` interface
   duplicated the concrete streaming contract and was removed. The separate
   buffered compatibility interface remains only where measurement warmups and
   existing engines use it.

## Reviewed with no additional proven defect

- Load, drain, cancellation, in-flight accounting, failed-load cleanup, failed
  unload memory/environment retention, and reverse-order measurement cleanup.
- Generic response streaming and abort behavior, hop-by-hop/routing header
  removal, escaped paths and query preservation, and cancellation propagation.
- Measurement baseline/absolute peak fit, incremental order-dependent deltas,
  native selected-device identity, transient sampling, probe failure, warmup,
  and cleanup reporting. The report correctly describes these as sampled
  system-level readings rather than allocator maxima.
- Bundle identity, traversal/symlink/collision refusal, resumable prefix hashing,
  complete verification before publication, directory syncing, and recoverable
  eviction.

## Verification

- Focused non-race engine/server/measurement/bundle/regression set: **pass**,
  package time 10.218 s.
- Focused race run: the new regressions passed, but the run failed once in
  existing lifecycle cleanup with `operation not permitted`.
- Isolated `TestGeneralServerLifecycle/ready -race -count=5`: **pass**, 3.837 s.
- Repeated full lifecycle race test (`-count=10`): **failed twice** because the
  pre-existing `ignore` mode exceeded its 3 s unload bound after process-group
  cleanup waited about 5 s. This was not a hang. The relevant wait/inspection is
  in unowned `worker_recovery.go`, so it was not changed here.
- `git diff --check`: **pass** before the final report addition.

The parent should run the full race/package gates once concurrent suites and
edits settle. The lifecycle cleanup flake above remains a gate if it reproduces
without concurrent test load.

## Remaining boundaries and coordination

- The arbitrary-server launch contract still selects an ephemeral port by
  opening and closing a listener before the child binds. Redirect false-success
  and already-observed child exit are fixed, but eliminating the small OS port
  allocation race completely would require a new inherited-listener or explicit
  readiness-handshake contract. That is a design/API change, not a narrow patch.
- The 8 GiB spool is bounded per request; the current API has no aggregate
  concurrent-spool quota. Auth and routing validation precede spooling, and the
  temporary file is local and removed on every normal/error return.
- Documentation outside this assignment still says generic bodies stop at
  64 MiB and request streaming is unsupported. It needs a separately owned
  update before this transport behavior is released.
- No change was needed in `http.go`. No `models.go` change beyond the explicitly
  authorized `API.modelCall` portion was made.

## Parent integration and final verification

The parent fixed a regression introduced by the streaming interface change:
ordinary OpenAI calls had also been routed through the generic worker endpoint,
which rejected the buffered fake engine. OpenAI routes now remain OpenAI routes
at the worker. `TestOpenAIRoutingKeepsBufferedEngineCompatibility` covers all
three endpoints, and the existing dev model end-to-end test passes again.

A further device-isolation regression confirmed that clearing catalog selectors
alone did not clear selectors inherited from the host environment. Explicit
bindings now clear inherited CUDA/XPU selectors before applying the selected
backend's environment. `TestBoundServerDropsInheritedOtherBackendSelectors`
passes under the race detector for CUDA, XPU and Metal using plain commands only.

Subsequent full race runs passed both packages (37.085s/108.549s and
26.906s/73.328s). Earlier lifecycle readiness and offline-output failures occurred
in overlapping runs; they are retained as observed failures, not erased. The
final combined-tree gates and screenshot are recorded in v2-fairness-report.md.

Final combined-tree race gate passed with package serialization: internal
84.998s, CLI cached from a passing run. The fixture startup deadline is four
seconds; real stop-grace and failed-health assertions are unchanged.
