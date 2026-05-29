# BLOCKERS

- 2026-05-29: Phase 1 local smoke is resolved. Installed Homebrew `llama.cpp` (`llama-server` version 9380) and downloaded `.smoke-models/stories260K.gguf`; `MYCELIUM_LLAMA_CPP_BINARY=$(command -v llama-server) MYCELIUM_LLAMA_CPP_MODEL=<repo>/.smoke-models/stories260K.gguf go test -tags smoke ./test/smoke/... -run 'Local' -timeout 20m -v` passes.
- 2026-05-29: Phase 1 fleet smoke is deferred until a second peer node address/config is available. Unblock by running `mycelium node` on the second peer and setting `MYCELIUM_REMOTE_PEER_ADDR` plus `MYCELIUM_REMOTE_PEER_MODEL` for the remote load/unload smoke.
