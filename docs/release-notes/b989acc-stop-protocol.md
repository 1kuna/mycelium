# Frozen v2 Docker stop-protocol hotfix (#54)

Base: `b989acc138fc160c60430855e415ea3846048b44`.
Branch: `hotfix/b989acc-stop-protocol`.
Stop-protocol implementation: `9900291b9bb9d55d4b195012bfe003faac1a52fc`.
Independent-review fix implementation: `09b7f83abb278b47b04686c0002ec111c561fa5d`.
macOS cleanup backport: `f5e24f8` (source `8863083f01ad9bffc4ccd4276f2062da01900d50`).
The final published head and immutable release-note link are recorded in the
[latest #54 report](https://github.com/1kuna/mycelium/issues/54).

## Exact changes since the base

1. `9900291b9bb9d55d4b195012bfe003faac1a52fc` — latch Docker TERM/KILL
   dispatch per container in the durable attempt record; preserve an existing
   stop deadline and latches on worker recovery; collect each container's exit
   code and bounded private stderr; prevent retention from discarding sidecars
   without matching evidence. Includes fake-clock/fake-Docker regressions and
   the explicitly opted-in real-Docker test.
2. `6377bb1cd3042dfeca3cc593cc4c80afa3c39938` — original stop-protocol release note.
3. `f5e24f8` — cherry-pick only #39's `8863083` from v3, including its tests and
   operations documentation. macOS can hide restricted executables' environment
   markers, so native execution records now make detached-descendant cleanup
   explicitly unconfirmed: disable returns nonzero, retains `DISABLED`, withholds
   the receipt, and blocks enable. Discoverable groups are still stopped.
   This corrects the baseline macOS failure that blocked both previous full
   race suites. Retrying disable cannot resolve the OS visibility limitation;
   never remove records or the flag to bypass it.
4. `cc625cc1e0ce555306f2273a9db0ea693ed3ad00` — release note for the
   authorized backport and renewed gates.
5. `09b7f83abb278b47b04686c0002ec111c561fa5d` — restore retryable TERM/KILL state
   after known non-dispatch persistence failures; retry definite daemon refusals;
   split refusal/uncertain-delivery regressions; make real-Docker evidence
   mandatory and add negative acceptance cases.
6. Following documentation-only commit updates this note with the tested fix
   head and fresh gates. Its final published SHA is in the linked #54 report.

For the #39 backport, diff review against `6377bb1` confirms its sole
production-code change is the
`runtime` import and an added `runtime.GOOS == "darwin"` conditional in native
secondary cleanup. Non-macOS paths, Linux systemd/cgroup cleanup, marker scans,
signaling, and grace logic are unchanged. The backport matches the source patch.
No #40/#44 harness commits or other v3 changes are included. No PR or merge.

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

## Original stop-protocol verification

The original stop-protocol implementation and isolated Docker runtime evidence
below predate the macOS backport. Renewed backport gate results are recorded
separately below; local verification is not fleet acceptance.
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
  now addressed by the explicitly authorized #39 backport above. These are
  historical pre-backport failures. No test was skipped.
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

## Renewed gates after the #39 backport

Historical backport verification: the required two-green-full-race gate passed on
Darwin/arm64, Go 1.25.6, at implementation head
`f5e24f8cac0cbd888a8c8c815bd3e57d9de24ca2`. The following documentation-only
commit does not change the tested source. Commands ran serially with
`GOMAXPROCS=2` and `nice -n 19`; each pipeline captured `${PIPESTATUS[0]}`
immediately and gated the next command on success. Every `tee` exit was **0**.

- `go build -o myce ./cmd/myce`: **0**.
- `go vet ./...`: **0**.
- `go test ./... -race -p 1 -count=1`, run 1: **0**
  (internal package 128.164s); run 2: **0** (internal package 131.916s).
- `go test ./cmd/myce -race -p 1 -count=1 -run '^TestDev(Checkpoint|Model)EndToEnd$'`:
  **0**. Both tests build and exercise the real `myce dev` executable with
  local fake workers, real API/runner, and clean shutdown.
- `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ... ./cmd/myce`: **0**.
- Same binary cross-compile for `GOARCH=arm64`: **0**. ELF x86-64 and AArch64
  architectures verified; binaries were not executed on Linux.
- Exact source-patch identity and non-macOS production-path comparison: **0**.
  Removing only the Darwin conditional and its import restores the original
  production file byte for byte. No additional production file changed.

No full-suite retry, test skip, assertion change beyond the original #39 fix,
Docker runtime rerun, machine access, or fleet acceptance is claimed. Raw gate
logs and exit ledger remain in ignored project scratch. Next: orchestrator
review and separately authorized native acceptance; #54 remains open.

## Independent-review fixes (issue #54)

The NO-GO review at cc625cc found a known non-dispatch path that incorrectly
consumed the live TERM/KILL latch. The follow-up implementation restores the
prior member (or removes a newly introduced member) if the pre-dispatch save
fails. The dirty-state retry therefore writes the restored latch before retrying
Docker. A transient failed write no longer permanently suppresses escalation.
The original injected-clock deadline is retained, including when it expires
while storage is unavailable; recovery then dispatches KILL without a late TERM.
Storage must become writable for durable dispatch to proceed.

A definite `Error response from daemon:` refusal now clears and persists only
the refused signal's latch, permitting a safe retry. Both TERM and KILL refusal
regressions require automatic reclamation after the daemon recovers. A separate
uncertain-delivery regression requires no repeated KILL across further healthy
reconciliations and release only after positive external exit proof. This
restores the prior refusal-recovery assertion rather than substituting an
external exit for it.

The real-Docker check now requires exactly two evidence records: one main with
exit 0 and one named sidecar with exit 143 or 137 as appropriate. It checks the
sidecar's exact fixture stderr content, the 64 KiB bound, one TERM, and zero or
one KILL. Missing/unreadable signal ledgers fail. CPU-only negative cases prove
that empty/omitted/duplicated records, missing/wrong exits, missing/wrong/oversized
stderr, and missing/unreadable/incorrect signal ledgers fail acceptance.

These changes do not make filesystem persistence and Docker delivery atomic.
A crash after saved intent but before dispatch, an ambiguous storage commit
followed by process loss, or uncertain Docker delivery still cannot prove that
a signal arrived. Uncertain KILL remains latched; positive exit/absence proof or
existing operator resolution remains required. This trade-off still needs
explicit release acceptance by the orchestrator; passing local gates is not
that acceptance. Existing evidence privacy/retention, attempt binding, token
filtering, and the sole #39 backport remain unchanged.

### Fresh verification of `09b7f83`

All code/test changes were verified before committing this implementation head;
the following release-note commit changes documentation only. Commands again
used serial `GOMAXPROCS=2`, `nice -n 19`, test `-p 1`, and immediate Bash
`${PIPESTATUS[@]}` capture, gating on command index 0 and `tee`. Every log-writer
exit was **0**.

- Failing-first P1 regression: **1** in all six initial TERM/KILL recovery cases
  on reviewed behavior. Final eight-case fake-store regression and targeted
  race checks: **0**. Acceptance negative cases cover both real-test scenarios.
- Build **0**, vet **0**, executable dev checkpoint/model E2E **0**.
- Initial full race run **1**, solely at unchanged
  `TestWorkerOrphanCleanupVerifiesIdentity` (25 ms checkpoint filesystem-probe
  timeout). Current and untouched `cc625cc` each passed three isolated checks
  (**0 / 0**); the cause of the initial timeout was not established. No unrelated
  assertion or timeout was changed.
- Two subsequent uncached full race suites **0 / 0** (internal package
  **136.234 s / 130.739 s**). No source changes between those runs.
- Linux amd64/arm64 cross-compiles **0 / 0**; ELF architectures verified.
- Strengthened real-Docker acceptance on isolated MacBook **0**: main **0** in
  both cases; signal-sensitive sidecar **143**, TERM **1**, KILL **0**; ignoring
  sidecar **137**, TERM **1**, KILL **1**. Required exact bounded stderr and exit
  evidence passed. No image pull; throwaway token; labelled test objects only.
  Binary and preserved log checksums matched; labelled container/network
  inventories empty, no test process running, owned temporary artifacts trashed.
- An intermediate expanded uncertain-delivery test exited **1** because it
  counted the second container's first KILL as a resend. Waiting for both initial
  uncertain calls fixed the fixture; the unchanged no-resend assertion passes
  in targeted and full gates. This was not a second full-suite failure.

Current state: implementation and requested local/runtime gates complete.
Private logs and exact exit ledger remain in ignored project scratch. Next:
orchestrator re-review and explicit acceptance of the documented crash/unknown
signal-delivery trade-off before separately authorized native fleet acceptance.
Issue #54 remains open.

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
separate authorization for any v3 forward-port. Issue #54 remains open; no PR
or main/v3 update is made.
