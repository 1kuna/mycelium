# Mycelium Verification Findings

Audit date: 2026-06-08

Source of truth read: `01-project-spec.md`, `02-testing-architecture.md`, `03-development-guide.md`, plus `DECISIONS.md`, `BLOCKERS.md`, and git history for phase evidence.

Scope: read-only audit. The only source mutations were the required Section 5 probes on throwaway branches; each branch was hard-reset, deleted, and `git status --short --branch` returned clean `## main...origin/main`.

## Phase Inventory

| phase | status | evidence |
|---|---|---|
| Phase 0 | BUILT | `internal/scheduler`, `internal/lease`, `internal/estimate`, `internal/telemetry`, `test/e2e/phase0_worked_example_test.go`; `go test ./... -race` passed; scheduler and lease coverage are 100.0%. |
| Phase 1 | BUILT | `internal/node`, backend adapters, process reaper tests, node/admission tests; fast packages passed with race. Smoke needs real engine env. |
| Phase 2 | BUILT | `internal/gateway`, provider profile/translation tests; `go test ./internal/gateway/... -race` passed. Smoke needs llama.cpp env. |
| Phase 3 | BUILT | `internal/catalog`, importers, project/safeid validation; `go test ./internal/catalog/... -race` passed. Smoke needs local model/env. |
| Phase 4 | BUILT | `internal/membership`, peer config/join/discovery tests; `go test ./internal/peer/... ./internal/membership/... -race` passed. Real second-peer smoke is deferred unless env is provided. |
| Phase 5 | BUILT | `internal/optimizer`, telemetry rollup and group-analysis tests; `go test ./internal/optimizer/... -race` and optimizer-related `cmd/mycelium` tests passed. |
| Phase 6 | BUILT | `internal/peer` coordinator/recovery/registry plus `test/e2e/phase6_peer_test.go`; `go test ./test/e2e/... -run Peer -race` passed, with weak proof noted below for the owner-race shape. |

## Summary

| status | count |
|---|---:|
| PASS | 42 |
| FAIL | 2 |
| MISSING | 0 |
| WEAK | 2 |
| UNVERIFIED | 1 |
| N/A | 0 |

## Findings Rows

| id | system | claim | how checked | evidence | status | severity | note |
|---|---|---|---|---|---|---|---|
| F-001 | 4.1 compile/static | `go build ./...` and `go vet ./...` are clean (`03` gates). | `go build ./...`; `go vet ./...` | Both commands returned exit 0. | PASS | critical |  |
| F-002 | 4.1 compile/static | Implementations and mocks carry compile-time port satisfaction assertions (`02 §2`). | `rg -n "var _ ports\\." internal test/mocks cmd/mycelium` | Assertions found across real impls and mocks, including allocator, node HTTP/client, backends, discovery, telemetry peer client, and hand-written mocks. | PASS | medium |  |
| F-003 | 4.2 phase gates | Phase 0 fast gate and worked example prove estimate/filter/select/score/preempt with max-util intact (`03 Phase 0`, `01 §3.6`). | `go build ./... && go vet ./... && go test ./... -race`; coverage gate; `test/e2e/phase0_worked_example_test.go` | Race suite passed; scheduler and lease 100.0%; total 89.3%; worked example passed in the all-package run. | PASS | critical |  |
| F-004 | 4.2 phase gates | Phase 1 fast tier is locally green (`03 Phase 1`). | `go test ./internal/node ./internal/backends/... ./internal/lease ./internal/scheduler -race` via all-package race gate. | Included in `go test ./... -race`; package slice also passed during mutation baseline. | PASS | high | Smoke tier is separate and listed below. |
| F-005 | 4.2 phase gates | Phase 2 gateway fast tier is green (`03 Phase 2`). | `go test ./internal/gateway/... -race` | `ok mycelium/internal/gateway`, `ok mycelium/internal/gateway/profiles`, `ok mycelium/internal/gateway/translate`. | PASS | high | Smoke tier is separate. |
| F-006 | 4.2 phase gates | Phase 3 catalog fast tier is green (`03 Phase 3`). | `go test ./internal/catalog/... -race` | Catalog package race tests passed. | PASS | high | Smoke tier is separate. |
| F-007 | 4.2 phase gates | Phase 4 peer/membership fast tier is green (`03 Phase 4`). | `go test ./internal/peer/... ./internal/membership/... -race` | Peer and membership package race tests passed. | PASS | high | Real join smoke needs second peer/env. |
| F-008 | 4.2 phase gates | Phase 5 optimizer fast tier is green (`03 Phase 5`). | `go test ./internal/optimizer/... -race`; optimizer tests in `cmd/mycelium/main_test.go` | Optimizer package passed; group optimizer and telemetry-sync tests are present and pass in all-package race gate. | PASS | high |  |
| F-009 | 4.2 phase gates | Phase 6 e2e peer fast suite runs (`03 Phase 6`). | `go test ./test/e2e/... -run Peer -race -count=1 -v` | Six peer e2e tests passed: submit-anywhere, owner race/replan, direct stale fence, coordinated preemption, no-self/partition, and death recovery. Registry replication recovery was run separately in F-035. | PASS | high | Detailed hollow-test issues are recorded as F-031 and F-036. |
| F-010 | 4.2/4.3 CI gate | Canonical `make ci` must be green (`03` build/test contract). | `make ci` | Fails in coverage: `internal/hardware coverage 82.0% is below package minimum 85.0%`; `make: *** [coverage] Error 1`. | FAIL | high | This is the only red command in the canonical local CI gate. |
| F-011 | 4.3 coverage | Required 100% authorities and overall coverage pass (`02 §9`). | `go test ./... -covermode=atomic -coverprofile=/tmp/mycelium-all.out`; `go run ./tools/covergate ... -require internal/scheduler=1.0 -require internal/lease=1.0 -require-file internal/node/admission.go=1.0 -require-file internal/peer/coordinator.go=1.0 -require-file internal/peer/recovery.go=1.0` | `coverage ok: total 89.3%`; required scheduler, lease, admission, coordinator, recovery bars passed. | PASS | critical | This did not include package-min; F-012 covers that. |
| F-012 | 4.3 coverage | Every module/package should meet the 85% floor (`02 §9`, `AGENTS.md` coverage gates). | `make ci`; `go test ./internal/hardware -covermode=atomic -coverprofile=/tmp/mycelium-hardware.out && go tool cover -func=/tmp/mycelium-hardware.out` | `internal/hardware coverage 82.0%`; low functions include `parseXPUSMIStats 36.8%`, `xpuSampleFromMetrics 57.1%`, `statDisk 0.0%`. | FAIL | medium | Concrete gap is hardware detector coverage, not build correctness. |
| F-013 | 4.4 test infra | Code under test uses injected clock, not wall time (`02 §1.4`, pitfall 9). | `rg -n "time\\.(Now\|Sleep\|After\|NewTimer)\\(" --glob '*.go' . \| rg -v '^./internal/clock/\|^./test/smoke/\|^./test/\|_test\\.go:\|^./tools/'` | No matches. | PASS | critical |  |
| F-014 | 4.4 test infra | No package-level dependency singletons or `init()` wiring (`02 §6`). | `rg -n "func init\\("`; package-level var scan | No `init()` functions; var scan found constants/errors/regexes and test data, not dependency wiring. | PASS | medium |  |
| F-015 | 4.4 test infra | Behavioral conformance suites exist for dual-implemented interfaces (`02 §3`). | `rg -n "Run.*Conformance\|Conformance" test internal` | Conformance exists for allocator, backend adapter/process adapters, node agent, resource estimator, registry/discovery/watch, and mocks. | PASS | high |  |
| F-016 | 4.4 test infra | Mocks expose failure injection and tests exercise error paths (`02 §6`, §10 pitfall 8). | `rg -n "Err\|ReadyAfter\|Calls" test/mocks`; all-package race run | Hand-written mocks expose error fields/call recording; all-package tests exercise injected errors across coordinator, gateway, node, backend, telemetry, discovery. | PASS | high |  |
| F-017 | 4.4 test infra | Time-driven tests use `FakeClock` instead of sleeps (`02 §1.4`). | `rg -n "FakeClock\|Advance\\(" internal test cmd/mycelium`; all-package race run | Aging, TTL, heartbeat, optimizer interval, queue, and recovery tests instantiate `mocks.NewFakeClock` and pass under race. | PASS | critical |  |
| F-018 | 4.5 safety core | `max_util` is never exceeded (`01 §3.2`). | Tests: `TestFitsAccountsForExistingClaimsAndNodeUsage`, `TestCatastrophicUnitsKeepExtraMargin`, Phase 0 e2e. Mutation: neutered final max-util ceiling. | Mutation red: allocator tests failed on expected over-capacity claims; Phase 0 worked example failed. | PASS | critical | Section 5 probe confirmed tests catch this in allocator/Phase 0. |
| F-019 | 4.5 safety core | Catastrophic units keep margin and refuse stacked concurrent loads (`01 §3.2`, §3.10). | `TestCatastrophicUnitsKeepExtraMargin`; `TestCanStackLoadRefusesConcurrentLoadOnCatastrophicUnit`. Mutation: `CanStackLoad` always true. | Mutation red: `catastrophic unit should not stack concurrent loads`. | PASS | critical |  |
| F-020 | 4.5 safety core | Fit is KV-aware/backend-aware, not weights-only (`01 §3.1`, §3.4). | `go test ./internal/estimate ./internal/lease ./internal/scheduler -race`; estimator/allocator tests | Estimator and scheduler tests assert weights plus KV reservation and backend-aware parser paths. | PASS | critical |  |
| F-021 | 4.5 safety core | Preemption ladder: soft queues; hard opt-in displaces lowest priority and victim requeues/replaces (`01 §3.6`). | `go test ./internal/scheduler ./test/e2e -run 'Preempt\|Phase0' -count=1` via all-package race | Scheduler and Phase 0 e2e cover soft/default and hard-for-interactive single-victim paths. | PASS | critical |  |
| F-022 | 4.5 safety core | Speed preference branches are exercised (`01 §3.5`). | `rg -n "throughput\|latency\|auto" internal/scheduler/*_test.go`; `go test ./internal/scheduler -race` | Scheduler tests cover throughput packing, latency isolation, and auto fastest-available scoring. | PASS | high |  |
| F-023 | 4.5 safety core | Queue aging/no starvation is deterministic under `FakeClock` (`03 Phase 0 Your-call hard req`). | `go test ./internal/scheduler -run Queue -race`; queue tests using `mocks.NewFakeClock` | Queue priority/aging tests advance `FakeClock`; no wall waits. | PASS | critical |  |
| F-024 | 4.5 safety core | Placement decisions carry trace steps (`01 §3.8`, `02 §8`). | Phase 0 worked example and scheduler trace assertions in all-package race run. | Worked example checks estimate/filter/select/score/preempt trace shape, not only final node. | PASS | high |  |
| F-025 | 4.6 admission | Owner commit is atomic and serialized; two commits from same fence cannot both succeed (`01 §3.12`). | `TestAdmissionConcurrentOffersAllowSingleCommit`; mutation: stale-fence always accept. | Baseline passes; stale-fence mutation red in direct admission stale test and Phase 6 direct stale test. | PASS | critical | Internal admission test uses real concurrent goroutines against one `Admission`. |
| F-026 | 4.6 admission | Fence is monotonic and stale commits return `ErrStaleFence` (`01 §3.12`). | `TestAdmissionRejectsStaleFence`; `TestPhase6PeerOwnerRejectsDirectStaleFence`; mutation always-accept fence. | Mutation red: `second Commit err = <nil>` and `stale Commit err = <nil>`. | PASS | critical |  |
| F-027 | 4.6 admission | Coordinator does not directly mutate another node's lease/occupancy (`01 §3.12`). | `rg -n "leases\\[\|AdmissionState\|SaveAdmissionState\|Commit\\(" internal/peer internal/scheduler internal/gateway` plus code read. | Peer/scheduler/gateway call owner `Offer`/`Commit`/`Release`; direct lease maps are private to admission tests/helpers. | PASS | critical |  |
| F-028 | 4.6 admission | Startup reaper clears tracked prior backends at startup (`01 §3.10`). | Backend/node/process reaper tests in all-package race; `go test ./internal/backends/... ./internal/node -race`. | Reaper/process identity tests pass; real process smoke remains hardware tier. | PASS | high | Mock/fast tier only; smoke listed separately. |
| F-029 | 4.6 admission | Heartbeat missed threshold marks peer unreachable and scheduler stops placing there (`03 Phase 1 Your-call`). | `go test ./internal/peer ./internal/scheduler ./cmd/mycelium -run 'Heartbeat\|Unreachable' -race` via all-package gate. | Heartbeat/death tests use `FakeClock`; peer death recovery e2e passed. | PASS | critical |  |
| F-030 | 4.7 federation | Submit-anywhere: submitter remains coordinator and shared registry records both (`03 Phase 6 check 1`). | `TestPhase6PeerSubmitAnywhereRecordsSubmitterCoordinator` | Passed; registry records `peer-a` and `peer-b` as their submitted jobs' coordinators. | PASS | critical |  |
| F-031 | 4.7 federation | Owner-race test must run two coordinators against same live owner/fence and prove exactly one stale-fence loser (`03 Phase 6 check 2`). | `TestPhase6PeerOwnerRaceStaleFenceReplans`; mutation probes for max-util and fence. | Test passes, but code read shows scripted `peerRaceAdmission` with preprogrammed `commitErrs`, not two concurrent commits against real shared `node.Admission`; max-util mutation did not red this Phase 6 check. | WEAK | critical | The invariant is implemented elsewhere, but this Phase 6 capstone proof is hollow against the brief's exact race requirement. |
| F-032 | 4.7 federation | Direct stale fence is rejected even when stale view shows free capacity (`03 Phase 6 check 3`). | `TestPhase6PeerOwnerRejectsDirectStaleFence`; fence mutation. | Mutation red with stale commit returning `<nil>`. | PASS | critical |  |
| F-033 | 4.7 federation | Owner adjudicates hard preemption across coordinators (`03 Phase 6 check 4`). | `TestPhase6PeerCoordinatedPreemptionUsesOwnerAuthority` | Passed with real `node.Admission`, scheduler service, victim lease, and owner-authority preemption path. | PASS | critical |  |
| F-034 | 4.7 federation | No self-preference and partition safety (`03 Phase 6 checks 5 and 7`). | `TestPhase6PeerNoSelfPreferenceAndPartitionSafety` | Passed; compute-on submitter places on better/warm peer; unreachable peer path records partition/does not claim capacity. | PASS | critical | Combined test, but it exercises both named behaviors. |
| F-035 | 4.7 federation | Dead-peer recovery rescues unfinished work and skips owner-confirmed finished jobs (`03 Phase 6 check 6`). | `TestPhase6PeerDeathRecoveryViaHeartbeat`; `TestPhase6RegistryReplicationRecoveryWithSeparateStores`; mutation skip owner re-check. | Mutation red: rescued included `finished-at-owner`; baseline rescued only queued/unfinished job. | PASS | critical |  |
| F-036 | 4.7 federation | Optimistic-concurrency retry is bounded and queues rather than force-commits on exhaustion (`03 Phase 6 Your-call hard req`). | Mutation removed `replans >= c.maxReplans` check in `Coordinator.shouldReplan`; ran `go test ./internal/peer ./test/e2e -run 'TestCoordinatorCommitBranches\|TestCoordinatorErrorPaths\|TestPhase6PeerOwnerRaceStaleFenceReplans' -count=1`. | Mutation did not red: `ok mycelium/internal/peer`, `ok mycelium/test/e2e`. | WEAK | critical | The code has a bound, but the current tests do not prove the bound by mutation. |
| F-037 | 4.7 federation | Group-analysis turn-taking: at most one round per interval, compute-on only, no stuck promotion (`03 Phase 6`). | `go test ./internal/telemetry ./cmd/mycelium -run 'Group\|Optimizer' -race` via all-package gate. | `TestSelectGroupAnalysisNodeRotatesReadyComputeNodes`, `TestShouldRunGroupOptimizerSelectsOneReadyComputePeer`, and optimizer slot tests pass. | PASS | high |  |
| F-038 | 4.8 fail-loud | Failed resource estimates do not proceed to deployment (`01 §3.11`). | `go test ./internal/scheduler ./internal/gateway -run 'Estimate\|Unsupported\|NoFit' -race` via all-package gate. | Scheduler/gateway tests exercise estimator errors and unsupported estimates as failing/queued, not deployment. | PASS | critical |  |
| F-039 | 4.8 fail-loud | Unknown provider profile does not fall back silently (`01 §3.11`). | `go test ./internal/gateway/... -run 'Profile\|Unknown\|Unsupported' -race` | Gateway profile/translation tests pass; unknown/unsupported provider paths fail before upstream placement. | PASS | critical |  |
| F-040 | 4.8 fail-loud | Non-overflow backend error is not requeued as context overflow (`01 §3.11`). | `go test ./internal/gateway ./internal/scheduler -run 'Overflow\|NonOverflow\|Requeue\|Fail' -race` via all-package gate. | Error-classification tests pass; non-overflow failure records failure rather than overflow requeue. | PASS | critical |  |
| F-041 | 4.8 fail-loud | Protocol translation errors on unsupported fields/bad SSE instead of corrupting output (`01 §3.11`). | `go test ./internal/gateway/translate ./internal/gateway -race` | Translation tests cover unsupported tool/refusal/reasoning/audio/content-filter/multi-choice/stream failures. | PASS | critical |  |
| F-042 | 4.9 node agent | Coordinator delegates node-owned model parsing/inspection to owning node (`01 §3.10`). | `go test ./internal/estimate ./internal/node -run 'Parse\|Inspect\|NodeOwned\|GGUF' -race` via all-package gate. | Estimator/node inspection tests pass; coordinator does not require local file access for node-owned models. | PASS | high |  |
| F-043 | 4.9 node agent | Loading counts as occupancy before health passes (`01 §3.10`). | `go test ./internal/node ./internal/scheduler ./internal/lease -run 'Loading\|CanStackLoad' -race`; `CanStackLoad` mutation. | Mutation red in catastrophic load test; node/scheduler loading occupancy tests pass. | PASS | critical |  |
| F-044 | 4.9 node agent | Saturated node sheds; scheduler queues (`01 §3.10`). | `go test ./internal/node ./internal/scheduler -run 'NoFit\|Saturat\|Queue\|Shed\|429' -race` via all-package gate. | Node/admission returns no-fit/fast rejection; scheduler/service queue paths pass. | PASS | high |  |
| F-045 | 4.9 node agent | Computed launch tuning reaches backend command (`01 §3.10`). | `go test ./internal/backends/... ./cmd/mycelium -run 'llama\|vLLM\|Launch\|Args\|GPU' -race` via all-package gate. | llama.cpp and vLLM args tests pass; computed config is threaded to launch config. | PASS | high |  |
| F-046 | 4.10 rejected designs | Locked-out designs have not reentered production (`01 §6`). | `rg -n "ssh\|leader\|elect\|raft\|paxos\|consensus\|shard\|distributed\|2pc\|hostfile\|rank\|fifo\|docker" .` plus code read. | Hits are specs, docs, smoke orchestration, disabled overlay/private validation, or non-production comments/tests; production config rejects overlay and private storage. | PASS | high | SSH appears in smoke orchestration only, not peer transport. |
| F-047 | 4.11 smoke/hardware | Smoke tier proves real engines/machines and must be run only with hardware/env (`03` smoke gates). | `find test/smoke -type f -name '*_test.go'`; `rg -n "MYCELIUM_\|t\\.Skip\|Fatal\\(" test/smoke`; `go test -tags smoke ./test/smoke -run TestLANInstanceProxyThroughAuthenticatedTunnel -count=1` | 12 smoke files found. The no-hardware LAN tunnel smoke passed; remaining env-gated tests require llama.cpp/model, MLX/vLLM, DGX Spark/B70 hosts, second-peer URLs, or benchmark config. | UNVERIFIED | high | Human-runnable checklist below; overall smoke tier remains unverified. |

## Mutation Probe Details

| mutation | branch hygiene | relevant check | outcome |
|---|---|---|---|
| Neuter `max_util` ceiling check in `internal/lease/allocator.go` | Branch `audit-mut-maxutil-*`, hard reset, deleted, clean main. | `go test ./internal/lease ./internal/scheduler ./test/e2e -run 'TestFitsAccountsForExistingClaimsAndNodeUsage\|TestFitsAccountsPerAcceleratorForMultiUnitClaims\|TestFitsAppliesReservedHeadroom\|TestCatastrophicUnitsKeepExtraMargin\|TestLargeModelBalancingRespectsMaxUtil\|TestPhase0WorkedExample\|TestPhase6PeerOwnerRaceStaleFenceReplans' -count=1` | Red in allocator and Phase 0 worked example. Did not prove Phase 6 race max-util, contributing to F-031. |
| Make owner fence comparison always accept | Branch `audit-mut-fence-*`, hard reset, deleted, clean main. | `go test ./internal/node ./test/e2e -run 'TestAdmissionRejectsStaleFence\|TestAdmissionConcurrentOffersAllowSingleCommit\|TestPhase6PeerOwnerRaceStaleFenceReplans\|TestPhase6PeerOwnerRejectsDirectStaleFence' -count=1` | Red in `TestAdmissionRejectsStaleFence` and `TestPhase6PeerOwnerRejectsDirectStaleFence`. |
| Skip owner re-check in recovery | Branch `audit-mut-recovery-recheck-*`, hard reset, deleted, clean main. | `go test ./internal/peer ./test/e2e -run 'TestRecoveryRescuesDeadPeerUnfinishedJobsAfterOwnerCheck\|TestRecoveryCleanupRequiredErrorPaths\|TestPhase6PeerDeathRecoveryViaHeartbeat\|TestPhase6RegistryReplicationRecoveryWithSeparateStores' -count=1` | Red; finished-at-owner jobs were incorrectly rescued. |
| Remove optimistic-concurrency replan bound | Branch `audit-mut-replan-bound-*`, hard reset, deleted, clean main. | `go test ./internal/peer ./test/e2e -run 'TestCoordinatorCommitBranches\|TestCoordinatorErrorPaths\|TestPhase6PeerOwnerRaceStaleFenceReplans' -count=1` | Stayed green. Downgraded bounded-retry proof to WEAK (F-036). |
| Make `CanStackLoad` always true | Branch `audit-mut-stackload-*`, hard reset, deleted, clean main. | `go test ./internal/lease ./internal/node ./internal/scheduler -run 'TestCanStackLoadRefusesConcurrentLoadOnCatastrophicUnit\|TestAgentRejectsStackedLoadOnCatastrophicUnit\|TestPlacerRejectsCatastrophicStackedLoad\|TestPlacementSkipsLoadingCatastrophicUnit' -count=1` | Red in `TestCanStackLoadRefusesConcurrentLoadOnCatastrophicUnit`. |

## Top Risks

| id | severity | risk |
|---|---|---|
| F-031 | critical | Phase 6 owner-race capstone is scripted rather than a real concurrent same-fence owner race, so the exact no-double-booking fleet race is not mutation-proven at the federation layer. |
| F-036 | critical | Bounded stale-fence retry exists in code, but removing the bound did not red the selected tests. A future unbounded replan regression could slip through. |
| F-010 | high | `make ci` is red today because package-min coverage fails. This blocks the canonical local acceptance gate. |
| F-047 | high | Real smoke tier remains unverified locally without required hardware/env. Fast tiers are green, but real engine/fleet behavior still needs human-run smoke. |

## Smoke Checklist

| smoke file | proves | required env/hardware | audit status |
|---|---|---|---|
| `test/smoke/phase1_local_smoke_test.go` | Real local load -> ready -> serve -> graceful stop, telemetry, overflow/requeue/reaper behaviors. | `MYCELIUM_LLAMA_CPP_BINARY`, `MYCELIUM_LLAMA_CPP_MODEL`; local small GGUF on dev Mac. | UNVERIFIED, human-runnable locally. |
| `test/smoke/phase2_gateway_smoke_test.go` | Real gateway path through llama.cpp backend. | `MYCELIUM_LLAMA_CPP_BINARY`, `MYCELIUM_LLAMA_CPP_MODEL`. | UNVERIFIED. |
| `test/smoke/phase3_catalog_smoke_test.go` | Real catalog/install path with local model/backend. | `MYCELIUM_LLAMA_CPP_BINARY`, `MYCELIUM_LLAMA_CPP_MODEL`. | UNVERIFIED. |
| `test/smoke/phase4_join_smoke_test.go` | Real join flow or manual gateway join. | Either llama.cpp env for automated flow, or `MYCELIUM_JOIN_GATEWAY`, `MYCELIUM_JOIN_MODEL`; optional `MYCELIUM_JOIN_EXPECT_NODE`. | UNVERIFIED. |
| `test/smoke/phase6_federation_smoke_test.go` | Real two-peer federation submit/recovery surface. | `MYCELIUM_FEDERATION_GATEWAY_A`, `MYCELIUM_FEDERATION_GATEWAY_B`, `MYCELIUM_FEDERATION_MODEL`; optional expected nodes. | UNVERIFIED, needs two real peers. |
| `test/smoke/llamacpp_conformance_test.go` | Real llama.cpp backend conformance. | `MYCELIUM_LLAMA_CPP_BINARY`, `MYCELIUM_LLAMA_CPP_MODEL`. | UNVERIFIED. |
| `test/smoke/process_backends_conformance_test.go` | Real MLX/vLLM process backend conformance. | `MYCELIUM_MLX_BINARY`, `MYCELIUM_MLX_MODEL`; `MYCELIUM_VLLM_BINARY`, `MYCELIUM_VLLM_MODEL`; optional `MYCELIUM_VLLM_LAUNCH_ARGS`. | UNVERIFIED. |
| `test/smoke/spark_vllm_peer_smoke_test.go` | DGX Spark vLLM peer smoke. | `MYCELIUM_SPARK_SSH`, `MYCELIUM_SPARK_ADDR`. | UNVERIFIED, needs Spark host. |
| `test/smoke/hardware_detector_smoke_test.go` | Real Intel Arc Pro B70 hardware detection. | Run on Linux B70 host with `MYCELIUM_EXPECT_INTEL_ARC_B70=1`. | UNVERIFIED, needs B70 host. |
| `test/smoke/operability_locality_smoke_test.go` | Service install/start/uninstall, clean-home second-peer join, locality smoke. | `MYCELIUM_SMOKE_OPERABILITY_APPLY=1`; second-peer env `MYCELIUM_REMOTE_PEER_URL`, `MYCELIUM_REMOTE_PEER_RPC_TOKEN`, `MYCELIUM_REMOTE_PEER_SSH`; locality env `MYCELIUM_LOCALITY_DB`, `MYCELIUM_LOCALITY_RPC_TOKEN`, `MYCELIUM_LOCALITY_PEER_URLS`. | UNVERIFIED. |
| `test/smoke/fleet_benchmark_smoke_test.go` | Real fleet benchmark config execution and artifact output. | `MYCELIUM_BENCHMARK_CONFIG`; optional `MYCELIUM_BENCHMARK_OUT`. | UNVERIFIED. |
| `test/smoke/tunnel_proxy_smoke_test.go` | Authenticated LAN instance proxy path strips peer auth before backend. | No external hardware/env; uses `httptest`. | PASS: `go test -tags smoke ./test/smoke -run TestLANInstanceProxyThroughAuthenticatedTunnel -count=1`. |

## Final Counts

PASS: 42
FAIL: 2
MISSING: 0
WEAK: 2
UNVERIFIED: 1
N/A: 0
