# DECISIONS

- 2026-05-29: Use a single `cmd/mycelium` entry point with subcommand dispatch in `main.go`; this keeps Phase 0 minimal while preserving the spec's one-binary shape.
- 2026-05-29: Track coverage/profiles and local binaries in `.gitignore`; they are generated gate artifacts, not project source.
- 2026-05-29: Use a deterministic in-memory Phase 0 estimator that computes `weights + ceil(context*concurrency*kv_per_token)`; real GGUF parsing is Phase 1, but the contract and failure behavior are real now.
- 2026-05-29: Apply a fixed 5% extra total-memory margin to catastrophic units beneath `max_util`; it is simple, deterministic, and captures the spec's extra-paranoia requirement without hiding the hard ceiling.

