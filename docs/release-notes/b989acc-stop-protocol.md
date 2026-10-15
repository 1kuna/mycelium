# Frozen v2 Docker stop-protocol hotfix (#54)

Base: `b989acc138fc160c60430855e415ea3846048b44`.
Branch: `hotfix/b989acc-stop-protocol`.
Tested implementation head: `9900291b9bb9d55d4b195012bfe003faac1a52fc`.

The final published branch head includes this documentation-only commit after
that implementation head. Its full SHA and immutable release-note link are
recorded in the [final #54 report](https://github.com/1kuna/mycelium/issues/54).
A commit cannot contain its own SHA; resolve the documentation head with
`git rev-parse hotfix/b989acc-stop-protocol` or the linked report.

## Exact changes since the base

There are exactly two commits, in order:

1. `9900291b9bb9d55d4b195012bfe003faac1a52fc` — latch Docker TERM/KILL
   dispatch per container in the durable attempt record; preserve an existing
   stop deadline and latches on worker recovery; collect each container's exit
   code and bounded private stderr; prevent retention from discarding sidecars
   without matching evidence. Includes fake-clock/fake-Docker regressions and
   the explicitly opted-in real-Docker test.
2. This commit, `docs: record frozen v2 stop-protocol hotfix evidence` — adds only
   this release note. Use `git log --reverse b989acc..HEAD` for both full commit
   IDs, including this documentation commit's ID.

No #40 or #44 harness commits were cherry-picked. No later main/v3 feature,
macOS cleanup fix, queue scheduler change, PR, or merge is included.

## Stop and evidence behavior

Existing asynchronous inspection/reconciliation is retained. A blocking
`docker stop --timeout` would introduce an outstanding stop request whose
completion must be recovered separately from the current worker-owned deadline.
Instead, durable per-container latches precede the existing bounded Docker
signal calls. Ordinary stop sends one TERM, continues observing through the
original injected-clock grace (normally two minutes), and sends one KILL only
if the container still runs at the deadline. Existing explicit immediate-kill
paths retain their behavior. Repeated stop requests cannot renew the deadline.

Dispatch is at most once across uncertain replies/restarts: filesystem state and
Docker delivery are not atomic. A crash after persisting intent but before
sending TERM can therefore skip TERM, while preserving deadline escalation.
An uncertain KILL is never automatically resent; release stays unconfirmed until
positive exit/absence proof or the existing operator resolution procedure.
Restart cannot infer that process/CLI death proves container release.

Each settled container's exit code and stderr tail are saved in the private
attempt record and `<identity>.docker-evidence.json`, mode 0600 in the worker
spool. Capture uses the last 256 Docker log lines and at most 64 KiB of stderr
per container; stdout is excluded from this private tail. Sidecar stderr does
not enter queue logs, results, or public status. Existing main output behavior
is retained. The fleet token is excluded from Docker/payload environments.

The separate evidence file survives disposable container/custody retention and
must remain private with the spool; archive/manage it with private operational
evidence. Missing or mismatched exit evidence prevents sidecar removal, including
pre-upgrade records without captured evidence. Capture failure retains custody
for retry. Main success semantics are unchanged; inspect sidecar evidence rather
than treating queue success as proof of clean sidecar shutdown.

## Verification and current state

Implementation and isolated Docker runtime verification are complete. **The
required two-green-full-race gate is blocked**, so this is not fleet acceptance.
All results below use actual process exit codes, captured immediately; the gate
script uses Bash `${PIPESTATUS[0]}` and stops on failed checks. Full suites ran
uncached with race detection; final runs used `GOMAXPROCS=2`, `nice -n 19`, `-p 1`.

- Failing-first fake-Docker regression: **1** on original stop behavior (six
  TERM calls over three ticks); fixed targeted race regressions: **0**. Covers
  one TERM through grace, no premature KILL, one KILL at the exact fake-clock
  boundary, durable recovery mid-grace, private exit/stderr custody, byte bounds,
  and preservation/required matching of evidence before retention removal.
- Build: **0**. Vet: **0**. Executable dev checkpoint and model E2E: **0**.
- Linux amd64 and arm64 binary cross-compiles: **0 / 0**; ELF architectures
  verified. These binaries were not executed on fleet hosts.
- Preliminary full race suite: **1**. Final implementation full race suites:
  **1 / 1**. All three full runs fail only at unchanged
  `TestDisableFindsMarkedDetachedGroupAfterLeaderExit`,
  `internal/myce/host_disable_review_test.go:40`. Its isolated targeted race
  test also exits **1** on an untouched archive of the frozen base. This is
  the documented [#39 macOS limitation](https://github.com/1kuna/mycelium/issues/39),
  whose production fix is outside this hotfix's authorized scope. No test was
  skipped and no cleanup assertion was relaxed.
- Real Linux containers under isolated MacBook Docker Desktop: **0**. Main exit
  **0**, double-signal-sensitive sidecar exit **143**, TERM **1**, KILL **0**.
  TERM-ignoring sidecar exit **137**, TERM **1**, KILL **1**, using a shortened
  test-only deadline. Both cases retain private stderr/exit evidence.
- First real-test harness attempt: **2**, because the suite's protective fake
  CLI was selected, before any real containers were created. Correcting the
  explicit opt-in PATH produced the successful real check above.
- Real test-labelled container/network inventories empty; temporary test files
  moved to Trash after preserving and hash-verifying private evidence. No image
  pull, production host access, or fleet credential use.

Private raw evidence is retained in ignored project scratch, not published.

## Upgrade and remaining acceptance

Queue and **all workers must roll together** using the matching release.
Coordinate the drain/stop and preserve the existing SQLite state, worker spools,
private evidence, and checkpoint roots. Do not substitute empty spools or
roll back to a binary that ignores the durable signal latches while depending
on these stop guarantees. Old disposable sidecars without captured evidence
are retained for operator review rather than silently discarded.

Native B70 acceptance must be coordinated with Verso before re-pinning or
production deployment. Native GPU/driver behavior, workload qualification, and
production fleet rollout are **not verified** here. The orchestrator owns
separate authorization for any v3 forward-port and resolution of the macOS
full-suite blocker. Issue #54 remains open; no PR or main/v3 update is made.
