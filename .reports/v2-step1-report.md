# Mycelium v2 step 1 implementation report

Implemented locally on `v2-step1`, created from the existing local `main`.
`docs/design-proposal.md` was read first and remains unchanged. No v1 code or
architecture was restored or copied.

## Built

- One Go executable, `cmd/myce`, with queue, join, leave/--now, submit, status,
  logs/--follow, cancel, and dev commands.
- One SQLite database: durable queue/fleet snapshot and ordered per-attempt log
  records. State changes are serialized and restored in memory if persistence
  fails. Standard SQLite WAL sidecars exist while the database is open.
- Authenticated HTTP JSON API and a small read-only, auto-refreshing HTML page.
  The page escapes job content and displays jobs, machines, attempts, and plain
  reasons for waiting. Phone widths use labeled cards.
- Strict YAML/JSON validation, required/preferred/any platform modes, named or
  lock-file environments, allowed machines, deadlines, and give-up times.
- Priority/readiness/deadline/wait scheduling, resumable preemption, two-minute
  stop grace, front-of-line retry, and protection of non-resumable running jobs.
- Workers report GPU type, available memory budget, ready environments, and
  loaded model hints. Commands run in their own process groups; stdout/stderr
  stream into SQLite. Workers retry transport outages without discarding their
  pending logs or completion result. Assignment redelivery handles an HTTP
  response lost before command startup.
- Stable checkpoint folders and MYCE_JOB_ID / MYCE_ATTEMPT /
  MYCE_CHECKPOINT_DIR. The fleet token is removed from command environments.
- Planned leave, interactive wait/stop choice, forced leave, 60-second loss,
  three-attempt limit, stale-copy stop on return, and stale result/log rejection.
  Ctrl-C stops taking work locally and preserves the checkpoint grace; a
  non-resumable command finishes before its worker exits.
- `myce dev` runs the actual API and command runner with N simulated GPU workers.
  `--gpu`, `--environments`, and `--models` can simulate other inventories.
- A 32-line AGENTS.md, the five-minute README quick start, and an 11-line Python
  checkpoint example in docs/guide-resumable-jobs.md.

## Verification

All checks used the local Mac and fake GPU inventory. No GPU probes, real fleet
machines, remote connections, LibreOffice, deployments, or pushes were used.

Final full race run (`go test ./... -race -count=1`), exit 0:

```text
ok  	mycelium/cmd/myce	9.873s
ok  	mycelium/internal/myce	1.591s
```

`go vet ./...`,
`go build -o /tmp/myce-v2-step1 ./cmd/myce`, and `git diff --check` also exited 0.

Unit tests use an injected fake clock for all scheduling/loss/grace boundaries.
They cover priority tiers, loaded model and ready environment affinity, earliest
deadline, waiting age, GPU/memory/allow-list/environment eligibility, platform
preference and fallback, preemption eligibility and one-victim behavior,
front-of-line resumption, both grace boundaries, all planned-leave choices,
sudden loss for resumable and non-resumable jobs, stale returns before and
after reassignment, the retry ceiling, cancellation, unavailable machines,
give-up expiry, validation, and durable state/log recovery after queue reopen.
HTTP tests cover authentication, malformed requests, the full job flow, and HTML
escaping. Worker tests execute real shell commands to check planned checkpoint
saving, waiting for non-resumable completion, cancellation before join, and
command startup failure.

Executable end-to-end evidence (`go test ./cmd/myce -race -run
TestDevCheckpointEndToEnd -v -count=1`):

```text
=== RUN   TestDevCheckpointEndToEnd
    main_test.go:208: real myce dev: production completed; experiment stopped, saved, resumed on attempt 2 from checkpoint 88
--- PASS: TestDevCheckpointEndToEnd (8.69s)
PASS
```

This builds the CLI with `-race`, launches `myce dev` with one fake worker,
submits a real checkpointing child process, submits production, and verifies
stop/save logs, production completion, nonzero progress on attempt 2, and both
jobs succeeding. The child also checks that it did not inherit the fleet token.

Manual CLI smoke: built binary started dev with two simulated CUDA workers;
submit/logs --follow/status showed the shell quick-start job succeeded with the
injected ID, attempt 1, and checkpoint folder. A metal/mlx job remained queued
with a plain explanation. Queue restart preserved the completed job, its logs,
and the waiting job. Cancel and planned/forced idle leaves were also exercised.
T3's collaborative browser inspected the empty and populated HTML page at
1280x800 and 390x844. After the mobile layout adjustment, measured page width
was exactly 390 at a 390-pixel viewport. The unavailable-metal/environment
reason was readable, and the page had no console errors. The expected brief
connection refusal during the local restart was followed by successful reload.

## Decisions and deferred work

- The design names platform/environment hooks without fixing their extended
  syntax. Required platform lists retain the approved YAML shorthand;
  required/preferred/any objects add the later-design modes. Environment is a
  name string or an object containing name or lock_file. Allowed machines and
  expiry use allowed_machines and give_up_at.
- One command runs at a time per worker. The reported free memory is the
  operator-declared budget minus the running job's stated reservation. This
  keeps step 1 small and avoids sharing a GPU among competing commands.
  Real-hardware discovery and dynamic measurement were not built or verified;
  users must explicitly configure the inventory they lend.
- A declared environment must already be reported ready. No environment
  installation or lock-file resolution is attempted in step 1; unavailable
  environments wait visibly. Model fields are affinity hints, not model serving.
- Readiness counts model and environment matches. Soft platform preference
  selects a suitable idle preferred machine when readiness is not worse, without
  changing the specified ordering of jobs on a given machine.
- Front-of-line retries precede ordinary waiting jobs after the specified
  readiness and deadline comparisons. This preserves the design's numbered
  priority/readiness/deadline order while advancing the resumed job's waiting
  position. The cap is three total command starts, including resumptions and
  explicit restarts. A command's own nonzero exit fails immediately.
- --now skips the non-resumable question and signals all commands; it still
  allows the two-minute checkpoint grace. Deadline is an ordering hint and
  expected_minutes is not enforced; give-up expires only queued work.
- Session IDs and attempt numbers identify stale executions, without leases or
  a replicated registry. Results and logs from stale copies are rejected and a
  returning worker is told to kill its copy. A physically disconnected worker
  cannot receive that stop before reconnecting: this is retryable, at-least-once
  command execution, not a guarantee of exactly-once external side effects.
  Commands must make retried output safe, as the checkpoint guide explains.
- The command runner targets macOS/Linux process-group signals. Windows,
  model calls/loading, automatic installation, multi-machine models, fairness
  quotas, and redundant queues remain outside step 1. No real-hardware
  acceptance is claimed. Dependencies are pinned pure-Go SQLite and YAML.

## DIGEST

- Built the SQLite batch queue, worker, JSON API, status page, and every step-1 CLI command.
- Added fake-worker dev mode, injected-clock scheduling, checkpoints, and leave/loss recovery.
- `go test ./... -race`, `go vet ./...`, build, and diff checks passed without a GPU.
- Executable dev test proved production preemption and checkpoint resumption on attempt 2.
- Added the quick start, resumable-job guide, and 32-line AGENTS.md.
- Inventory is explicit; model calls, environment setup, and real-hardware acceptance are deferred.
- Report: `.reports/v2-step1-report.md`; no push, remote machines, or LibreOffice.
