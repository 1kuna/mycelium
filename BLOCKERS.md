# BLOCKERS

## Active

- 2026-05-29: vLLM/CUDA real-engine adapter smoke remains hardware/engine-gated. `command -v vllm` and `command -v nvidia-smi` returned no binary on Darwin arm64; run `MYCELIUM_VLLM_BINARY=... MYCELIUM_VLLM_MODEL=... go test -tags smoke ./test/smoke/... -run VLLM -timeout 20m -v -count=1` once that runtime/model exists.

## Resolved

- 2026-05-29: Phase 1 local smoke is resolved. Installed Homebrew `llama.cpp` (`llama-server` version 9380) and downloaded `.smoke-models/stories260K.gguf`; `MYCELIUM_LLAMA_CPP_BINARY=$(command -v llama-server) MYCELIUM_LLAMA_CPP_MODEL=<repo>/.smoke-models/stories260K.gguf go test -tags smoke ./test/smoke/... -run 'Local' -timeout 20m -v -count=1` passes.
- 2026-05-29: Phase 1 fleet smoke is resolved. Installed/updated Homebrew on the second peer at `192.0.2.63`, installed `llama.cpp`, copied the current `mycelium` binary and `.smoke-models/stories260K.gguf`, and started `mycelium node --listen 0.0.0.0:51847 --backend-listen 127.0.0.1:51848 --id remote-peer-192-0-2-63 --name second-peer --llama-server /opt/homebrew/bin/llama-server --vram-mb 24576`. `MYCELIUM_REMOTE_PEER_ADDR=192.0.2.63:51847 MYCELIUM_REMOTE_PEER_MODEL=$HOME/.mycelium/models/stories260K.gguf go test -tags smoke ./test/smoke/... -run 'Fleet' -timeout 20m -v -count=1` passes.
- 2026-05-29: Phase 1 gate is green: `gofmt -l .`, `go build ./...`, `go vet ./...`, `go test ./... -race`, local smoke, and fleet smoke all pass; overall coverage is 92.5% with `internal/scheduler` and `internal/lease` at 100%.
- 2026-05-29: Real MLX single-node adapter smoke is resolved. Installed MLX-LM 0.31.3 in `.venv-mlx` and ran `MYCELIUM_MLX_BINARY=<repo>/.venv-mlx/bin/mlx_lm.server MYCELIUM_MLX_MODEL=mlx-community/Qwen2.5-0.5B-Instruct-4bit make smoke-mlx SMOKE_JSON=/tmp/mycelium-smoke-mlx.json`; smokegate reports `smoke ok: 4 passed, 0 skipped, 0 failed`.
- 2026-05-29: Cross-machine MLX-distributed/model-sharding blocker is removed by spec decision D17. Mycelium now distributes jobs across peers and never shards one model across machines.
