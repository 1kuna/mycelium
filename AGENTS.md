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
- `docs/operations.md`: canonical install, queue maintenance and recovery.
- `docs/status.md`: worker freshness, maintenance and status fields.
- `deploy`: manual service templates; `docs/operations.md`: old-state archives.
- `docs/files.md`: checkpoint contract and Python example.

## Build and verify

```sh
./scripts/gate.sh quick BASE   # per change (default); build, vet, targets, related race tests
./scripts/gate.sh full         # once per batch before a fleet deploy; dev e2e + one race suite
./myce dev # long-running local API and fake workers; Ctrl-C to stop
```

Quick and full gates run on the development host: Darwin natively and Linux
inside the local VM invoked by the gate. Fleet gates are optional extra evidence.
Hotfix: quick, deploy, then full, fixing forward. The 2026-11-02/03 local cache
and disk saturation incident is mitigated by persistent gate-owned caches,
automatic idle cache trimming, a 45 GiB entry/42 GiB stop floor and capacity-aware
admission; preserve other owners' slots. Full admission follows measured CPU/RAM
(four-core peaks: native 7.01 GiB, Linux/VM 10.57 GiB), workstation headroom and
a configured static fleet-worker CPU share. Use `--exclusive` for isolated local timing; release
builds take priority over ordinary waiters. Details: `docs/development.md`.

Test behavior, security invariants and real incidents; add a focused regression
per observed bug. No speculative edge-case matrices, implementation-detail pins,
duplicate journeys or process self-audits (maintainer, 2026-10-22). Native GPU/daemon/identity/boot checks are separate
from laptop proof; see `docs/development.md`.

Before merging, follow `docs/development.md#local-merge-proof`: require independent
review and the PR's exact-head local full gate (quick for docs-only). Fleet gates
and automatic main runs provide optional extra evidence; node availability does
not gate development or merges. Inspect known failures and fix new failures forward.

Stage explicit paths and commit completed changes; push your own branches.
Tested fixes deploy to the fleet without a separate task (MY-28, MY-32, MY-33):
bundle them into one release at a time and roll back if the queue isn't healthy.
Critical patches may deploy during production runs so other work can proceed
(MY-39); keep making patch time quick. Ordinary updates wait. Fix runs failing
because of Mycelium.
Keep a healthy, tested release ready before each scheduled production window.
Fleet hardware changes still need a task that authorizes them.

This repository is public: keep host names, addresses, fleet layout, private
paths and other projects' details out of commits, issues and docs.
