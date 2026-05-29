# DECISIONS

- 2026-05-29: Use a single `cmd/mycelium` entry point with subcommand dispatch in `main.go`; this keeps Phase 0 minimal while preserving the spec's one-binary shape.
- 2026-05-29: Track coverage/profiles and local binaries in `.gitignore`; they are generated gate artifacts, not project source.

