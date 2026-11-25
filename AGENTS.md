# Mycelium v3

- `docs/design-proposal.md` is the approved design and the only product spec.
  `docs/DECISIONS.md` holds the maintainer's rulings. Find current guides in
  [the docs index](docs/START-HERE.md#where-to-go-next).
- Keep every feature easy to use and testable on a laptop with no GPU.
- One binary, plain-command workers, no restored v1 architecture. The queue
  is one SQLite queue until the approved shared-queue design
  (docs/design-queue-beyond-one-host.md, MY-40) reaches its stage 3 cutover:
  one PostgreSQL authority on the NAS, a renewable coordinator turn with a
  handover number, and a local SQLite copy beside each worker. Build only
  that design's stages; no peer voting or replicated payload files.
- Queue and scheduling decisions use an injected `Clock`. Never call
  `time.Now` or `time.Sleep` directly in queue or scheduling logic.
- Fake-clock unit tests cover scheduling, leave, loss, and retry decisions.
- `myce dev` uses the real API and command runner with fake GPU inventory.
- Preserve portable progress through the [checkpoint contract](docs/files.md#resumable-jobs).
- Treat worker results as attempt-specific; reject stale executions.
- No network, remote-storage or bulk I/O (blob hashing, manifest verification,
  large reads) inside `q.mu`; snapshot, work outside, then recheck fences inside.
  Output verification under the lock once stalled every host's control
  transactions (#80). The SQLite commit and small local state files are fine.
- `skills/mycelium/SKILL.md` is the one guide every project's agents load to use
  Mycelium. When a change alters how consumers submit, wait, read status, fetch
  results or hold work, update that skill in the same change, so no project has
  to be told separately.

## Merge and verify

Merge when quick plus targeted tests pass. Run the full gate once per release
batch and fix breakage forward. No independent review is required (MY-41).
Development and test tooling may be changed, reset or cleared when it blocks work;
see `docs/development.md#local-merge-proof`.

Keep process rules only where they protect production customers or secrets:
never expose the fleet token or let jobs inherit `MYCE_TOKEN`. Keep this public
repository free of private host details, paths and secrets. Preserve running
production customer jobs. Follow the [operations runbook](docs/operations.md)
for installation, release, maintenance and recovery.
