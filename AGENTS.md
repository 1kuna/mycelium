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
- Checkpoints use `MYCE_CHECKPOINT_DIR`; NAS assignments are relative and each
  worker configures its local NAS root with `--checkpoint-root`. Without a NAS,
  verified file-store generations provide portable progress.
- Treat worker results as attempt-specific; reject stale executions.
- No network, remote-storage or bulk I/O (blob hashing, manifest verification,
  large reads) inside `q.mu`; snapshot, work outside, then recheck fences inside.
  Output verification under the lock once stalled every host's control
  transactions (#80). The SQLite commit and small local state files are fine.
- Never expose or commit the fleet token. Jobs must not inherit `MYCE_TOKEN`.

## Map

- `cmd/myce`: CLI and executable dev end-to-end test.
- `internal/myce`: queue/HTTP API, scheduler, worker, queue maintenance, installer, dev.
- `clients/python`: submission/cancellation and checkpoint client.
- `docs/operations.md`: canonical install, queue maintenance and recovery.
- `docs/guide-status-v3.md`: worker freshness, maintenance and status fields.
- `deploy`: manual service templates; `docs/guide-fresh-install.md`: old-state archives.
- `docs/guide-resumable-jobs.md`: checkpoint contract and Python example.

## Build and verify

```sh
./scripts/gate.sh quick BASE   # per change (default); build, vet, targets, related race tests
./scripts/gate.sh full         # once per batch before a fleet deploy; dev e2e + one race suite
./myce dev # long-running local API and fake workers; Ctrl-C to stop
```

Hotfix: quick, deploy, then full, fixing forward. At most two full race suites
at once on one Mac (more causes false failures). Details: `docs/test-gates.md`.

Test behavior, security invariants and real incidents; add a focused regression
per observed bug. No speculative edge-case matrices, implementation-detail pins,
duplicate journeys or process self-audits (maintainer, 2026-10-22). Native GPU/daemon/identity/boot checks are separate
from laptop proof; see `docs/production-test-plan.md`.

Work locally, stage explicit paths, and commit completed changes. Do not push
or touch fleet hardware without current task authorization.
