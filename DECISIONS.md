# DECISIONS

- 2026-05-29: Use a single `cmd/mycelium` entry point with subcommand dispatch in `main.go`; this keeps Phase 0 minimal while preserving the spec's one-binary shape.
- 2026-05-29: Track coverage/profiles and local binaries in `.gitignore`; they are generated gate artifacts, not project source.
- 2026-05-29: Use a deterministic in-memory Phase 0 estimator that computes `weights + ceil(context*concurrency*kv_per_token)`; real GGUF parsing is Phase 1, but the contract and failure behavior are real now.
- 2026-05-29: Apply a fixed 5% extra total-memory margin to catastrophic units beneath `max_util`; it is simple, deterministic, and captures the spec's extra-paranoia requirement without hiding the hard ceiling.
- 2026-05-29: Use a sorted-slice scheduler queue with effective priority `base + waited_minutes`; it is deterministic under `FakeClock` and makes starvation behavior easy to inspect.
- 2026-05-29: Score cold candidates with transparent integer components: warm/locality bonus, speed preference fit, and fit tightness; every chosen score is written into the placement trace.
- 2026-05-29: For Phase 0 hard preemption, choose the lowest-priority eligible victim, breaking ties by lowest in-flight count and then instance id; this is deterministic and favors minimal disruption.
- 2026-05-29: Phase 1 node-agent cold-load dedup returns the same ready instance to concurrent same-preset callers; the scheduler can choose later whether to batch or create another replica, but duplicate cold starts are never useful.
- 2026-05-29: Pin `modernc.org/sqlite` at v1.38.2 for the telemetry store because newer releases currently declare Go 1.24+ or 1.25+, while the project contract is Go 1.23.
- 2026-05-29: Keep reactive overflow handling as deterministic planning code in `internal/optimizer`: classify only explicit overflow errors, compute the next context cap, and let the normal scheduler prove fit.
- 2026-05-29: Use a tracked-process JSON file for node startup reaping; it is explicit, testable, and avoids broad process-name killing.
- 2026-05-29: Use a 5s heartbeat interval with 3 missed beats before marking a node unreachable; the tracker is clock-injected so tests advance time deterministically.
- 2026-05-29: Make the GGUF parser command template configurable and parse JSON metadata into `domain.ModelMetadata`; this avoids baking in a brittle CLI shape while still shelling out to the real parser path in production.
