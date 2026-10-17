# Mycelium v3

- `docs/design-proposal.md` is the approved design and the only product spec.
- Keep every feature easy to use and testable on a laptop with no GPU.
- One binary, one SQLite queue, plain-command workers. No peer network,
  leases, replicated registry, or restored v1 architecture.
- Batch commands, model calls/servers, and environments are implemented; keep them laptop-testable.
- Queue and scheduling decisions use an injected `Clock`. Never call
  `time.Now` or `time.Sleep` directly in queue or scheduling logic.
- Fake-clock unit tests cover scheduling, leave, loss, and retry decisions.
- `myce dev` uses the real API and command runner with fake GPU inventory.
- Checkpoints use `MYCE_CHECKPOINT_DIR`; assignments are NAS-relative and each
  worker configures its own local NAS root with `--checkpoint-root`.
- Treat worker results as attempt-specific; reject stale executions.
- No network, remote-storage or bulk I/O (blob hashing, manifest verification,
  large reads) inside `q.mu`; snapshot, work outside, then recheck fences inside.
  Output verification under the lock once stalled every host's control
  transactions (#80). The SQLite commit and small local state files are fine.
- Never expose or commit the fleet token. Jobs must not inherit `MYCE_TOKEN`.

## Map

- `cmd/myce`: CLI and executable dev end-to-end test.
- `internal/myce`: job format, scheduler, SQLite state, HTTP API, worker, dev.
- `docs/guide-resumable-jobs.md`: checkpoint contract and Python example.

## Build and verify

```sh
go build -o myce ./cmd/myce
go test ./... -race
go vet ./...
./myce dev
```

Work locally, stage explicit paths, and commit completed changes. Do not push
or touch fleet hardware without current task authorization.
