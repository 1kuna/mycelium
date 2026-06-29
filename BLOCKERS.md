# BLOCKERS

## Active

- 2026-06-29: Verso Deep Agents/OpenAI tool-calling over Mycelium is not yet
  live-proven. A tiny tool-call probe through the MacBook gateway showed the
  current Spark 9B vLLM route rejects `tool_choice:"auto"` with vLLM's exact
  provider error that `"auto" tool choice requires --enable-auto-tool-choice and
  --tool-call-parser to be set`. The warm B70 9B vLLM route accepts the request
  but returned `finish_reason=length` with no `tool_calls`, so it is not a
  proven tool-call route. The saved B70 27B vLLM preset declares
  `--tool-call-parser hermes`, but the current gateway probe for
  `b70-qwen27b` returned `queued: no instance available`, and the preset does
  not currently declare `--enable-auto-tool-choice`. Plain strict-schema chat
  remains proven on B70 and Spark; tool-calling Deep Agents need a deliberately
  activated vLLM preset with both `--enable-auto-tool-choice` and a compatible
  `--tool-call-parser`, plus a tiny Mycelium gateway canary that returns a real
  OpenAI `tool_calls` message.
- 2026-06-29: Cold-load/client-timeout stale-job live proof is blocked at the
  MacBook gateway activation step, not at the Linux peers. B70 and Spark were
  updated through their existing `mycelium-real-fleet.service` user-systemd
  units to binaries containing the stale-job cleanup fix. B70 now reports
  SHA256 `22adabba230d1418e13449fe9a8bcfe5b604b4fd8c2f5179c2277c9dbf9da09f`
  on `192.168.0.153:55283`; Spark reports SHA256
  `6e19e91f8b97115f7545f33425c93f669ea5104f8f559727d1b36e966ab1df9d`
  on `192.168.0.16:55274`. The Mac safe activation path was hardened in
  commit `99784cc` so the generated system LaunchDaemon runs as user `zach`
  and keeps `/Users/zach/.mycelium` config/log/control-DB paths instead of
  touching them as root. A named Darwin binary
  `/Users/zach/.mycelium/bin/mycelium-99784cc` was built and ad-hoc signed
  with SHA256
  `3e13360898741e63a2a195d4d936f7aa3c006d5c7a6e6f9568bd4671cecec8d`.
  The system LaunchDaemon install was not attempted because `sudo -S -v`
  rejected the provided credential before any LaunchAgent bootout or system
  install command ran. The existing user LaunchAgent stayed running on the
  known-good old binary, with `service active`, `peer ready`, and
  `seeds ready 2/2`. A post-blocker client-timeout canary against that still-old
  gateway reproduced the stale lifecycle shape: job
  `gateway-1782561331393747000-10-1` was left `placing` while its registry
  record was `loading` assigned to B70. Evidence was preserved under
  `/tmp/mycelium-stale-job-blocker-20260629T112957Z`; because the stale row was
  created by this verification attempt, the Mac control DB was backed up to
  `/Users/zach/.mycelium/bench/real-fleet/backups/control-corrected-lan.before-stale-job-cleanup-20260629T113100Z.db`
  and only that job/registry record was manually terminalized to `failed` with
  `cleanup_required=false`. Remaining unblocker: run the documented system
  LaunchDaemon activation with valid local admin credentials, prove
  `service status --system` and seed diagnostics, then rerun the README
  cold-load cancellation recipe against the fixed Mac gateway.
- 2026-06-29: Read-only LaunchAgent diagnostics support a binary-identity /
  Local Network privacy hypothesis for the Mac user LaunchAgent LAN failure,
  not a Mycelium config or listener-binding bug. The working live LaunchAgent
  plist still runs `/Users/zach/.mycelium/bin/mycelium` from
  `/Users/zach` with the same peer config and reports B70/Spark seeds ready.
  The working binary is ad-hoc/linker-signed with identifier `a.out` and CDHash
  `28e65885c95018a65a13c38b4251dd9edac7072f`; the prepared fixed binary
  `/Users/zach/.mycelium/bin/mycelium-99784cc` is a different ad-hoc identity,
  identifier `mycelium-99784cc-55554944e514dd7e344b3e12f6256629593404fd`
  and CDHash `c81c1321f2fb039cf3ef1c7a2d4eee0691068f2a`. `spctl` rejects
  both, which is expected for these ad-hoc CLI binaries. Historical logs show
  the failed current-binary LaunchAgent attempts could bind/start but got
  `dial tcp ... no route to host` for both corrected-LAN seeds, while the old
  binary under the same user LaunchAgent succeeds. The remaining non-root path
  would be a stable signed bundle/helper identity that Zach explicitly approves
  and then tests as a maintenance-window user LaunchAgent swap. It still
  requires mutating the live gateway to prove and cannot live-prove the
  stale-job fix before that swap succeeds.
- 2026-06-27: MacBook LaunchAgent activation blocks rolling the committed
  registry-compaction binary live on the gateway. The current built Darwin
  binary can probe B70 and Spark when run interactively, but the same binary
  under a user LaunchAgent returns `no route to host` for both corrected-LAN
  seeds. A temporary LaunchAgent using the existing approved binary path and
  old binary succeeds, while a temporary LaunchAgent using the current binary
  path fails; Developer ID signing did not change the failure. A bounded
  real-path repro copied the current binary over
  `/Users/zach/.mycelium/bin/mycelium`, kicked `com.mycelium.real-fleet`, and
  `service status` failed with both seeds unreachable; the known-good backup was
  restored immediately and the gateway returned to `seeds ready 2/2`. B70 and
  Spark were safely updated to the current Linux binaries through their existing
  `mycelium-real-fleet.service` user-systemd units, and post-update strict
  JSON-schema canaries through the MacBook gateway succeeded on both owners.
  Remaining activation route for the MacBook is a stable Local Network
  permission strategy, likely the documented system LaunchDaemon path or a
  properly bundled/signed helper, rather than another blind user-LaunchAgent
  binary swap. The repo now has a read-only `mycelium service plan` command
  with `--system` and `--binary` support; it prints the exact LaunchDaemon unit,
  install/status/reload commands, and rollback command without touching the
  live gateway. Remaining live activation is an operator maintenance step:
  inspect `mycelium service plan --config <peer.json> --binary <stable-binary>
  --name real-fleet --system`, install the system service with sudo, run
  `service status`, and immediately restore the known-good binary/service if
  seed diagnostics fail.

## Resolved

- 2026-06-27: The cold-load/client-timeout stale-job lifecycle bug is fixed in
  the repo-local scheduler path. The reproduced failure was: a job row was
  saved as `placing`, the coordinator/owner accepted the cold-load attempt, the
  request context was canceled during `NodeAgent.Load`, and the old terminal
  `SaveJob(ctx, failed)` used the already-canceled context, leaving the durable
  job row stuck non-terminal. Scheduler cleanup now uses `cleanupContext(ctx)`
  for post-placement terminal job saves, queued/running state persistence, owner
  release notifications, and instance/lease persistence after accepted work.
  Focused regression tests prove both coordinated and local cold-load
  cancellation now persist the job as `failed` and release accepted owner work
  without using the canceled context. This is repo-local proof; re-run a tiny
  live cold-load timeout canary before claiming unattended live-fleet lifecycle
  behavior after a deployment.
- 2026-06-27: The repo-local coverage gate is resolved. Focused behavior tests
  raised `cmd/mycelium` from 81.7% to the 85.0% package floor by covering
  operator-visible command/runtime contracts: service diagnostics and service
  manager failures, backend routing and OpenVINO process launch arguments,
  config save/backup behavior, CLI helper parsing, auth/listen classification,
  and bootstrap compatibility profile selection. The full gate then exposed
  exact strict-floor gaps in `internal/scheduler` and `test/contract`; focused
  tests restored `internal/scheduler` to 100.0% and `test/contract` to 100.0%
  by covering queue/drain rejection paths and engine-installer progress events.
  `make coverage` now passes with `coverage ok: total 89.4%`.
- 2026-06-27: Corrected-LAN Spark inference and the peer-registry cap blocker
  are resolved from narrow live proof. Spark `mycelium-real-fleet.service`
  listens at `192.168.0.16:55274`; authenticated MacBook diagnostics showed
  both B70 `192.168.0.153:55283` and Spark ready. The Spark 9B model snapshot
  already existed under Spark's Hugging Face cache mount, and the Spark vLLM
  wrapper loaded it through Docker as `spark-qwen9b` on `127.0.0.1:55374`.
  Stale registry/runtime job records were cleared from MacBook, B70, and Spark
  control DBs with timestamped backups after first proving the oversized
  `/registry/snapshot` body; authenticated registry snapshots then stayed below
  the 16 MiB peer-RPC body cap. The code fix in `27a1e5f` compacts future peer
  registry snapshots to rescue-relevant job records only. A tiny B70 strict
  JSON-schema canary through `192.168.0.143:55291` returned parseable final JSON
  on warm B70 `inst_2`; a tiny Spark strict JSON-schema canary through the same
  gateway returned HTTP 200, `X-Myc-Node: spark`, `X-Myc-Backend: vllm`,
  `X-Myc-Decision: placed_on_warm_instance`, `X-Myc-Structured-Output:
  json_schema`, `X-Myc-Guided-Decoding-Requested: true`, `finish_reason=stop`,
  and parseable final JSON. Full-fleet readiness is now proven for the narrow
  B70-plus-Spark corrected-LAN route, not for broad load or Mac mini health.
- 2026-06-27: The old 2026-06-04 "real smoke env unset" blocker is superseded.
  The local smoke runtime path was fixed and `make smoke-local` was re-proven
  on 2026-06-26 with an already-present LM Studio Ministral GGUF, while the
  corrected-LAN MacBook/B70 path has separately handled real Atlas strict-schema
  semantic and overlay work. Remaining live-fleet uncertainty is now Spark peer
  reachability and the current coverage gate, not missing local smoke env.
- 2026-06-26: B70 Qwen/vLLM structured final-channel JSON is activated from the live Mycelium path. The B70 vLLM wrapper now passes `STRUCTURED_OUTPUTS_BACKEND="${MYCELIUM_B70_STRUCTURED_OUTPUTS_BACKEND:-auto}"`; the existing entrypoint converts that into `--structured-outputs-config {"backend":"auto","enable_in_reasoning":true}` while preserving `--reasoning-parser qwen3`. The old wrapper was backed up on B70 before the change. Mycelium node RPC unloaded the stale warm instance `inst_1` without restarting the B70 peer; the next MacBook gateway request cold-loaded B70 `inst_2` on `127.0.0.1:44673` with `X-Myc-Node: b70`, `X-Myc-Backend: vllm`, `X-Myc-Decision: loaded_new`, HTTP 200, `finish_reason=stop`, and final content `{"answer":"ok","row":4}` parseable as JSON from `response_format.type=json_schema`. A warm follow-up using vLLM `structured_outputs` extra body also returned parseable final JSON through the same Mycelium gateway/instance. Reasoning was not disabled or copied into content; the tiny structured canaries produced no `reasoning_content`.
- 2026-06-26: Local `make smoke-local` is re-proven from the current tree with an already-present local GGUF that can exercise Phase 2 overflow requeue. Local model search found `/Users/zach/.mycelium/models/stories260K.gguf` with `llama.context_length=128`, plus LM Studio GGUFs including `/Users/zach/.lmstudio/models/lmstudio-community/Ministral-3-3B-Instruct-2512-GGUF/Ministral-3-3B-Instruct-2512-Q4_K_M.gguf` with `mistral3.context_length=262144`. Focused smoke passed `TestPhase2GatewayLocalLlamaCppSmoke` and `TestPhase4JoinedNodeGatewaySmoke`; full `make smoke-local` then passed with `smoke ok: 8 passed, 0 skipped, 0 failed`. The current tree keeps the stale Phase 2 direct-router fix, the explicit-VRAM joined-node fit fix, and the failover subcase isolation needed after coordinator-runtime wiring.
- 2026-06-26: Corrected-LAN real-fleet gateway plus one B70 compute peer is resolved from live runtime proof. MacBook `com.mycelium.real-fleet` listens at `192.168.0.143:55291`; B70 `mycelium-real-fleet.service` listens at `192.168.0.153:55283` with UDP discovery `:57898`, compute=true, backend vLLM, and seed `192.168.0.143:55291`. Authenticated B70 `/peer/diagnostics` reports MacBook ready; MacBook `/peer/diagnostics` reports B70 ready while Spark remains connection-refused. A MacBook gateway `/v1/chat/completions` request for `qwen3.5-9b` cold-loaded B70 instance `inst_1` with `X-Myc-Node: b70`, `X-Myc-Backend: vllm`, `X-Myc-Decision: loaded_new`; a warm follow-up routed to the same instance with `X-Myc-Decision: placed_on_warm_instance`. B70 owner snapshot shows `inst_1` ready at `127.0.0.1:39447`. The model spent the small caps in Qwen reasoning and returned empty final content with non-empty `reasoning_content`, preserving the reasoning/content boundary.
- 2026-06-03: Real 122B-class fleet benchmark smoke is resolved against a real Spark-class peer and a real B70-class peer. The benchmark used an operator-local cached model snapshot, conservative vLLM memory settings, and an operator-local artifact directory. `TestFleetBenchmarkConservativeSmoke` passed in 758.39s. The artifact evidence proved Spark 9B cold load, B70 27B cold load, Spark 122B `hard_preempted_then_loaded`, Spark/B70 warm reuse, disk headroom, and reserved memory stayed below the catastrophic-safe usable limit. Exact host, path, and env values live in ignored private ops notes.
- 2026-06-02: DGX Spark vLLM peer routing smoke is re-proven from the current tree with conservative memory usage. `TestSparkVLLMPeerRoutingSmoke` passed in 150.65s, and smokegate reported `smoke ok: 1 passed, 0 skipped, 0 failed`. This proof uses a low-memory smoke model/path supplied by operator-local environment and does not restart the 122B production container.
- 2026-06-02: Current-scope LAN fleet smoke is resolved from the current commit. A stale second peer LAN address was corrected through local discovery, then a current `mycelium run` peer was copied to the second peer and started with `llama-server` plus the repo-local smoke GGUF. A current local dev Mac peer coordinated the run. The joined-node and submit-anywhere smoke passed with `smoke ok: 2 passed, 0 skipped, 0 failed`; a request submitted to the second peer gateway routed to the local dev Mac peer, proving reverse placement without self-preference. Exact LAN addresses and commands live in ignored private ops notes.
- 2026-06-01: SUPERSEDED by 2026-06-04 MVP cleanup: the earlier local libp2p overlay backend/smoke proof is no longer an active product claim. Production libp2p runtime and smoke coverage were removed; overlay config now fails loudly and LAN peer discovery is the MVP boundary.
- 2026-06-02: Current-scope local smoke is resolved with real llama.cpp. `make smoke-local` passed with operator-local llama.cpp binary/model env values, reporting `smoke ok: 8 passed, 0 skipped, 0 failed`, including `TestPhase2GatewayLocalLlamaCppSmoke` overflow requeue. The smaller repo smoke model still works for load/serve checks, but cannot prove overflow requeue because llama.cpp caps it to its 128-token training context.
- 2026-06-01: SUPERSEDED by LAN-only product scope: the WAN/relay/bootstrap overlay smoke blocker was removed. SSOT MVP acceptance is local-network peer discovery and routing; cross-NAT membership remains optional roadmap work behind `PeerDiscovery`/`Tunnel`, not an active blocker.
- 2026-06-01: DGX Spark vLLM adapter smoke is resolved without restarting the 122B production container. After power-cycling the Spark, `myc-vllm-smoke` was removed and `spark-vllm-qwen35-122b` was left stopped. A signal-safe Docker wrapper launched `nvcr.io/nvidia/vllm:26.04-py3` against cached `Qwen/Qwen3.5-0.8B` with `--gpu-memory-utilization 0.12 --max-model-len 512 --max-num-seqs 1 --max-num-batched-tokens 512 --disable-log-stats`; the cross-compiled Spark smoke binary passed `TestLocalVLLMConformance` in 260.98s, and Docker stats stayed around 4 GiB RSS with no `myc-vllm-*` containers left afterward.
- 2026-06-01: DGX Spark vLLM peer routing smoke is resolved. The smoke started a real `mycelium run` compute peer on the Spark using the signal-safe vLLM Docker wrapper, discovered it from a local gateway peer through seed-address peer discovery, routed `/v1/chat/completions` to the Spark smoke node, unloaded the instance, and left no smoke containers running. The smoke passed in 132.99s; observed Docker memory stayed below ~5.5 GiB RSS.
- 2026-06-02: B70 SSH access and hardware discovery are resolved from the current tree. Operator-local SSH aliases reached the Intel Arc Pro B70 box. A cross-compiled Linux smoke binary passed `TestLinuxIntelArcB70HardwareDiscovery` on the B70 with the expected smoke environment flag.
- 2026-05-29: Phase 1 local smoke is resolved. Installed Homebrew `llama.cpp` and downloaded the repo-local smoke GGUF; the local smoke command with operator-local env values passed.
- 2026-05-29: Phase 1 gate is green: `gofmt -l .`, `go build ./...`, `go vet ./...`, `go test ./... -race`, local smoke, and fleet smoke all pass; overall coverage is 92.5% with `internal/scheduler` and `internal/lease` at 100%.
- 2026-05-29: Real MLX single-node adapter smoke is resolved. Installed MLX-LM 0.31.3 in the repo-local ignored virtualenv and ran `make smoke-mlx` with operator-local env values; smokegate reports `smoke ok: 4 passed, 0 skipped, 0 failed`.
- 2026-05-29: Cross-machine MLX-distributed/model-sharding blocker is removed by spec decision D17. Mycelium now distributes jobs across peers and never shards one model across machines.
- 2026-05-29: Phase 6 fast gate is green. `go test ./internal/peer/... -race -covermode=atomic -coverprofile=fed.out` passed with total/internal peer coverage at 100.0%, `go test ./test/e2e/... -run Phase6 -race -count=1 -v` passed, and `go test -tags smoke ./test/smoke/... -run Federation -timeout 20m -count=1 -v` compiled the smoke package and skipped because the real gateway env was not set.
