# BLOCKERS

## Active

- 2026-05-29: vLLM/CUDA real-engine adapter smoke remains hardware/engine-gated. `command -v vllm` and `command -v nvidia-smi` returned no binary on Darwin arm64; run `MYCELIUM_VLLM_BINARY=... MYCELIUM_VLLM_MODEL=... go test -tags smoke ./test/smoke/... -run VLLM -timeout 20m -v -count=1` once that runtime/model exists.
- 2026-05-29: second peer reverse-dial peer smoke remains blocked by host networking behavior on `192.0.2.63`: after authenticated loopback tunnels were implemented, a second peer gateway still logs `dial tcp 192.0.2.91:<peer>: connect: no route to host` from the tunnel proxy when trying to coordinate work onto the local dev Mac. This blocks proving "submit to second peer, run on local dev Mac" on the current LAN until the host route/firewall behavior is fixed or the roadmap overlay transport exists.

## Resolved

- 2026-05-29: Phase 1 local smoke is resolved. Installed Homebrew `llama.cpp` (`llama-server` version 9380) and downloaded `.smoke-models/stories260K.gguf`; `MYCELIUM_LLAMA_CPP_BINARY=$(command -v llama-server) MYCELIUM_LLAMA_CPP_MODEL=<repo>/.smoke-models/stories260K.gguf go test -tags smoke ./test/smoke/... -run 'Local' -timeout 20m -v -count=1` passes.
- 2026-05-29: SUPERSEDED by peer pivot: Phase 1 fleet smoke previously passed against the old `mycelium node` command on the second peer at `192.0.2.63`. Re-run that evidence with peer-native `mycelium run` before treating it as current.
- 2026-05-29: Phase 1 gate is green: `gofmt -l .`, `go build ./...`, `go vet ./...`, `go test ./... -race`, local smoke, and fleet smoke all pass; overall coverage is 92.5% with `internal/scheduler` and `internal/lease` at 100%.
- 2026-05-29: Real MLX single-node adapter smoke is resolved. Installed MLX-LM 0.31.3 in `.venv-mlx` and ran `MYCELIUM_MLX_BINARY=<repo>/.venv-mlx/bin/mlx_lm.server MYCELIUM_MLX_MODEL=mlx-community/Qwen2.5-0.5B-Instruct-4bit make smoke-mlx SMOKE_JSON=/tmp/mycelium-smoke-mlx.json`; smokegate reports `smoke ok: 4 passed, 0 skipped, 0 failed`.
- 2026-05-29: Cross-machine MLX-distributed/model-sharding blocker is removed by spec decision D17. Mycelium now distributes jobs across peers and never shards one model across machines.
- 2026-05-29: Peer-native local dev Mac-to-second-peer smoke is resolved. With `llama-server` at `/opt/homebrew/bin/llama-server` and `.smoke-models/stories260K.gguf` copied to the second peer, a local dev Mac gateway peer discovered `macmini-peer`, placed a request there, returned HTTP 200 from `/v1/chat/completions`, and `mycelium ctl nodes list --db <local-store>` showed `macmini-peer ... ready`.
- 2026-05-29: Authenticated seed-address local dev Mac-to-second-peer smoke is resolved. The local dev Mac gateway joined with `mycjoin://192.0.2.63:<port>?token=...&rpc_token=...`, seeded the second peer through join-token-gated `/peer/health`, rejected unauthenticated `/snapshot` with HTTP 401, and returned HTTP 200 from `/v1/chat/completions` using the second peer llama.cpp backend.
- 2026-05-29: Loopback-tunneled local dev Mac-to-second-peer smoke is resolved. The second peer backend listened only on `127.0.0.1:<port>`, the local dev Mac gateway reached the second peer owner through the allocated peer tunnel, `X-Myc-Node` was `macmini-peer`, and `/v1/chat/completions` returned HTTP 200.
