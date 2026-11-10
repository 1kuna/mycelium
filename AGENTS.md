# Mycelium v3

- `docs/design-proposal.md` is the approved design and the only product spec;
  `docs/DECISIONS.md` holds the maintainer's rulings.
- Keep every feature easy to use and testable on a laptop with no GPU.
- One binary, one SQLite queue, plain-command workers. No peer network,
  leases, replicated registry, or restored v1 architecture. MY-40 records a
  maintainer-directed shared-queue design study; the approved design still binds.
- Batch commands, model calls/servers, and environments are implemented; keep them laptop-testable.
- Queue and scheduling decisions use an injected `Clock`. Never call
  `time.Now` or `time.Sleep` directly in queue or scheduling logic.
- Fake-clock unit tests cover scheduling, leave, loss, and retry decisions.
- `myce dev` uses the real API and command runner with fake GPU inventory.
- Checkpoints use `MYCE_CHECKPOINT_DIR`; NAS assignments are relative and each
  worker configures its local NAS root with `--checkpoint-root`. Without a NAS,
  verified file-store generations provide portable progress.
- Treat worker results as attempt-specific; reject stale executions.
- No network, remote-storage or bulk I/O (blob hashing, manifest verification,
  large reads) inside `q.mu`; snapshot, work outside, then recheck fences inside.
  Output verification under the lock once stalled every host's control
  transactions (#80). The SQLite commit and small local state files are fine.
- Never expose or commit the fleet token. Jobs must not inherit `MYCE_TOKEN`.
- `skills/mycelium/SKILL.md` is the one guide every project's agents load to use
  Mycelium. When a change alters how consumers submit, wait, read status, fetch
  results or hold work, update that skill in the same change, so no project has
  to be told separately.

## Map

- `cmd/myce`: CLI and executable dev end-to-end test.
- `internal/myce`: queue/HTTP API, scheduler, worker, queue maintenance, installer, dev.
- `clients/python`: submission/cancellation and checkpoint client.
- `skills/mycelium`: the consumer skill (setup, harness and T3 binding references beside it).
- `docs/operations.md`: current runbook (install, upgrade, queue maintenance, recovery).
- `docs/reference/`: explanations, field semantics, tuning and finished migrations.
- `docs/status.md`: worker freshness, maintenance and status fields.
- `deploy`: manual service templates; `docs/reference/migrations.md`: old-state archives.
- `docs/files.md`: checkpoint contract and Python example.

## Merge and verify

Merge when quick plus targeted tests pass. Run the full gate once per release
batch and fix breakage forward. No independent review is required (MY-41).
Development and test tooling may be changed, reset or cleared when it blocks work;
see `docs/development.md#local-merge-proof`.

Keep process rules only where they protect production customers or secrets:
never expose the fleet token or let jobs inherit `MYCE_TOKEN`; keep this public
repository free of private host details, paths and secrets; do not break running
production customer jobs.
