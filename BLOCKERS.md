# BLOCKERS

- 2026-05-29: Phase 1 local smoke is not proven yet. `go test -tags smoke ./test/smoke/... -run 'Local' -timeout 20m -v` currently skips because `MYCELIUM_LLAMA_CPP_BINARY` and `MYCELIUM_LLAMA_CPP_MODEL` are not set, and no `llama-server`/`llama-cli` binary is on `PATH`. Unblock by providing a llama.cpp server binary plus a small local GGUF model path, or by approving/installing/downloading those local smoke assets.
- 2026-05-29: Phase 1 fleet smoke is deferred until a second peer address/config is available. Unblock by providing the second-node address/config expected by the smoke test.
