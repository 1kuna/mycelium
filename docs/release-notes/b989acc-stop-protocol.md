# Frozen v2 Docker stop-protocol hotfix (#54)

Base: `b989acc138fc160c60430855e415ea3846048b44`.
Branch: `hotfix/b989acc-stop-protocol`.
Latest tested orphan implementation: `520aa30a6e9f7011c641b345204ee9a0319f3378`.
The final published head is recorded below and
in the [#54 implementation report](https://github.com/1kuna/mycelium/issues/54).

## Changes since the base

- `9900291b9bb9d55d4b195012bfe003faac1a52fc`: original per-container stop
  intent, recovery deadline preservation, exit codes and bounded private stderr
  custody, retention protection, and fake/real-Docker regression tests.
- `6377bb1cd3042dfeca3cc593cc4c80afa3c39938`: original release note.
- `f5e24f8cac0cbd888a8c8c815bd3e57d9de24ca2`: the sole v3 backport,
  #39 source `8863083f01ad9bffc4ccd4276f2062da01900d50`. macOS can hide
  restricted executables' environment markers. Native execution records now
  make detached-descendant cleanup explicitly unconfirmed: disable returns
  nonzero, retains `DISABLED`, withholds its receipt, and blocks enable.
  Discoverable groups still stop. Linux production paths are unchanged.
- `cc625cc1e0ce555306f2273a9db0ea693ed3ad00`: backport release note.
- `09b7f83abb278b47b04686c0002ec111c561fa5d`: round-1 persistence fixes,
  mandatory real-Docker evidence validation and negative cases. Its signal-error
  classifier and latch rollback are superseded by the ruling below.
- `1a07b95530c87e863ec38f933db45bbfa34e76d5`: round-1 response release note.
- `e7745ab92cd50fc3f94401de90ef49b9901a66db`: apply the orchestrator's round-2 design ruling;
  remove signal-error classification and latch rollback, make TERM intent
  permanent, record delivery outcomes, and retry deadline KILL despite storage
  failures. Tests cover generic/prefixed uncertain errors, both crash boundaries,
  storage failure/recovery, restart, deadline retries and private custody.
- `34eb63c94c33bb44992d8381d1d8919c1cad4d42`: round-2 ruling release note.
- `520aa30a6e9f7011c641b345204ee9a0319f3378`: round-3 orphan correction:
  one bounded Docker stop per process lifetime, deadline KILL fallback, truthful
  missing-evidence report, lifecycle call-site audit and fake/real-Docker tests.
- Following documentation-only commit records the tested implementation SHA
  and fresh verification. A commit cannot embed its own SHA; the final head is
  recorded in #54 with an immutable link to this note.

No #40/#44 test fixes or other v3 changes are included. No new dependencies,
PR, merge, main/v3 update, production deployment or fleet access.

## Defined stop behavior

For durable attempts, the existing asynchronous reconciler and injected-clock
deadline remain. Existing bounded signal calls preserve attempt recovery without
introducing a separate outstanding Docker stop operation.

For durable attempts, TERM is attempted **at most once per exact container per
stop request**.
The worker saves intent before calling Docker and never clears a recorded intent,
regardless of success, generic daemon/runtime/transport error, or crash. A failed
intent write sends no TERM that tick; persistence is retried on subsequent ticks.
The candidate record is saved before changing live state, so no rollback is needed.

Private attempt evidence records `term_outcome` as `sent`, `error`, or
`unknown-after-restart`. `sent` means Docker returned success, not proof that the
application completed graceful shutdown. Intent is initially persisted with the
unknown outcome, then updated after delivery returns. A crash before dispatch or
before saving its result leaves the unknown outcome; restart never resends TERM.
Older intent records without an outcome are conservatively marked unknown when
collecting exit evidence.

At the original grace deadline (normally two minutes), KILL is idempotent and
repeats on errors while inspection confirms a running owned container. There is
no KILL latch, including when loading older records. Deadline KILL bypasses failed
spool writes, so persistent storage failure cannot suppress escalation. Normal
stops never KILL before their deadline; existing explicitly forced safety/operator
cleanup remains immediate. Repeated stop requests cannot extend the deadline.

A missed or failed TERM therefore means no graceful signal, followed by deadline
KILL. There is no crash-latched omitted-KILL condition requiring external recovery.
Unavailable Docker/inspection still retains custody and retries; elapsed time is
never release proof. After stopping, evidence and release persistence must succeed
before capacity is acknowledged as released.

## Orphans: round-3 correction

An orphan is a Mycelium-labelled container without a durable attempt record.
It has no owning job and does not use the per-attempt TERM latch. Instead, one
bounded `docker stop --time <grace>` is allowed per exact container/daemon per
process lifetime. Docker sends TERM once and KILL at its timeout. Whole-second
Docker grace is rounded up; the CLI call is bounded by that grace plus the
existing call allowance and any enclosing cleanup deadline.

A process-local standard `sync.Map` preserves that one call and its original
fallback deadline across reconstructed cleanup objects. Any stop error or
uncertain delivery consumes the call; subsequent reconciliation waits for grace,
then retries only idempotent KILL while inspection proves the container running.
Existing explicit forced containment/operator cleanup still uses immediate KILL.
A fresh process can issue one new orphan stop; this is deliberately not a durable
attempt/restart guarantee.

Orphan cleanup does not collect or claim durable attempt evidence. Its cleanup
report records **`exit code/stderr not retained: orphan without attempt record`**,
and `EvidenceSaved` remains false. A no-store save can no longer authorize the
per-attempt TERM path or claim private evidence was saved. Durable attempts still
require a successful evidence-file write before marking evidence saved.

Fake-clock/Docker/store regressions cover success, failed and uncertain stop,
reconstruction within a process, no early KILL, repeated failed KILL followed by
recovery, and absent attempt/evidence files. The existing host orphan test checks
the truthful report. A source-enumeration regression guards the sole durable TERM
call, orphan stop/KILL calls, stopped-only retention removal and network removal;
behavioral regressions prove both dispatchers do not loop TERM. Isolated real
Docker coverage adds graceful orphan exit 143 and stop-error fallback exit 137.

## Private evidence and operator runbook

For durable attempts, every settled main/sidecar retains its exit code and stderr
tail in the attempt
record and `<identity>.docker-evidence.json` in the private worker spool, mode
0600. Stderr is limited to the last 256 Docker log lines and 64 KiB per container.
Sidecar tails stay out of queue logs, results and public status. Main success
semantics and token exclusion are unchanged. Separate evidence survives container
retention; missing/mismatched evidence prevents sidecar disposal.

For a sidecar with **exit 137** and TERM outcome **`error` or
`unknown-after-restart`**, review its private attempt evidence. This is the defined
fallback after failed or uncertain graceful signaling; **no action is required
beyond reviewing evidence**. Do not clear intent, edit the spool, or resend TERM.
A successful main alone does not establish graceful sidecar shutdown. Preserve
private evidence before any container disposal; never publish raw stderr.

Upgrade queue and all workers together, preserving state, checkpoints and spool
custody. Native GPU/driver acceptance and Verso qualification remain separately
coordinated; laptop/Docker checks do not establish them.

## Historical verification and timeout investigation

Earlier pre-backport full race runs failed at
`TestDisableFindsMarkedDetachedGroupAfterLeaderExit`; the same failure occurred
on untouched `b989acc`. The authorized #39 backport subsequently passed both
required full race runs. Round-1 implementation also passed two full race runs
and strengthened isolated Docker acceptance after an initial failure below.

Preserved historical failure: `TestWorkerOrphanCleanupVerifiesIdentity` failed
at `w.start` with a checkpoint filesystem-operation timeout, before the orphan
identity/signaling assertions. Its test, `worker.go` preparation path and
`checkpoint_io.go` are byte-identical to frozen `b989acc`; that pre-existing path
uses a 25 ms real-I/O budget. A fresh untouched `b989acc` archive passes
`-race -p 1 -count=20` of this test (exit **0**, 3.314 s). The round-2 reviewer
also reported 20 passing repetitions. The original cause is unresolved; these
passes do not rule out a pre-existing latency problem. No timeout or identity
assertion was changed. Reopen checkpoint-I/O investigation on recurrence.

Other preserved historical failures: the first real-Docker harness selected the
protective fake CLI and timed out (exit **2**, no real containers); corrected
explicit PATH passed. Intermediate regression development included a fixture
synchronization failure, documented in the prior #54 author response.

## Fresh verification under the ruling

Development failure retained: the first full race run under the ruling exited
**1**, at `TestDockerDelayedStartSurvivesClientTimeout`,
`TestDockerAcceptedStartObservationSettlesLostResponse`, and
`TestDisableDocker/late`. An early KILL branch bypassed positive pending-start
acceptance. The fix keeps the original reconciliation order and bypasses only
persistence barriers when deadline/forced KILL is due. Existing delayed-start and
disable assertions are unchanged. Development prechecks also caught a test-helper
clock type error and an absent empty call ledger; corrected fixtures then passed.

Final implementation: `e7745ab92cd50fc3f94401de90ef49b9901a66db`; the subsequent
release-note-only commit does not change tested source. Local gates ran
serially with `GOMAXPROCS=2`, `nice -n 19`, test `-p 1`. Bash captures
`${PIPESTATUS[@]}` immediately and gates on both command index 0 and `tee`.
Private logs and exit ledger remain in ignored project scratch.


- Focused regressions **0**, build **0**, vet **0**.
- Two uncached full `go test ./... -race -p 1 -count=1`: **0 / 0**;
  internal package **133.570 s / 140.001 s**. No source changes between runs.
- Executable dev checkpoint/model E2E **0** (16.189 s).
- Linux amd64/arm64 binary cross-compiles **0 / 0**, ELF x86-64/AArch64 checked.
  These binaries were not executed on Linux fleet hosts.
- Strengthened real Linux-container acceptance on isolated MacBook **0**:
  main **0**, signal-sensitive sidecar **143** with TERM **1** / KILL **0**;
  ignoring sidecar **137** with TERM **1** / KILL **1** at its shortened test
  deadline. Exact private stderr, exit and TERM-outcome evidence passed.
  All 34 CPU validator cases passed in both full suites.
- Uploaded test-binary SHA-256 matched; copied private logs/exit ledger were
  hash-verified. No image pull; throwaway token; owned folder/labelled resources
  only. Final labelled container and network inventories are empty.
- Remote temporary files were removed recoverably with `/usr/bin/trash` after
  confirming no file was active and preserving hash-verified local evidence.
  Only empty directories remain: Docker's VM still holds directory handles.
  Their private pointer is retained for cleanup after handle release, without
  restarting Docker or disturbing unrelated workloads. No test payload remains.

The preceding verification applies to the earlier implementation. Round-3
correction results and final head are recorded in the latest #54 implementation
report. Next: orchestrator re-review and separately authorized native fleet/Verso
acceptance. Issue #54 remains open.


## Round-3 verification

Preserved initial full race failure: **1**, solely at
`TestProductionHoldForceStopsBlockedSaveAndResumesDurableGeneration`,
`production_hold_test.go:787`, during `w.start` checkpoint preparation:
`filesystem operation timed out`. The test, worker preparation and checkpoint-I/O
files are byte-identical to `34eb63c`. Ten targeted race repetitions on the current
source and ten on an untouched `34eb63c` archive both pass (**0 / 0**,
3.819 / 3.850 s). The precise timeout cause remains unresolved; this resembles the
historical 25 ms checkpoint-I/O failures but is not proven to share their cause.
No timeout or assertion was changed. Revisit checkpoint I/O on recurrence.

Fresh isolated MacBook Docker checks: **0**. Main exits **0**; signal-sensitive
sidecar exits **143** with one TERM and no KILL; ignoring sidecar exits **137**
with one TERM and one deadline KILL. Required private exit/stderr/TERM-outcome
assertions pass. New orphan cases exit **143 / 137** with one bounded stop each;
the injected stop-error case uses one fallback KILL after grace, including
reconstruction with a longer requested grace. No orphan evidence-saved claim.
Uploaded test-binary and copied private evidence archive hashes match. Test-label
container/network inventories are empty. No image pull; owned remote temporary
folder moved to Trash after confirming no active files and preserving evidence;
its original location is absent. No production fleet access.

Final source: `520aa30a6e9f7011c641b345204ee9a0319f3378`. All renewed checks use
this source; the following release-note commit changes documentation only.
Light serial runs: `GOMAXPROCS=2`, `nice -n 19`, tests `-p 1`. Bash captured
`${PIPESTATUS[@]}` immediately and gated command index 0 and the log writer.
Every final log-writer exit is **0**.

- Focused regressions **0**, build **0**, vet **0**.
- Two renewed uncached full `go test ./... -race -p 1 -count=1`: **0 / 0**;
  internal package **138.882 / 137.272 s**. No source changes between runs.
- Executable dev checkpoint/model E2E **0** (16.253 s).
- Linux amd64/arm64 cross-compiles **0 / 0**; ELF architectures asserted.
- Real Docker test process and SSH wrapper **0 / 0**, all four scenarios pass.
- Initial original-behavior regressions **1**; initial source-enumeration test
  fixture **1** (matched function names and missed nested retention argv), then
  corrected without production changes. Initial full race timeout **1** is
  preserved above, alongside the bounded current/baseline checks **0 / 0**.

Final head and normal-push/remote-equality results are in #54. Current state:
implemented and verified locally/on isolated Docker, pending orchestrator
re-review. Next: separately authorized native fleet/Verso acceptance. No PR,
issue closure, production access or native GPU qualification is claimed.


## Restart during main-completion grace (#55)

Recovery now resumes cleanup without a synthetic launch failure when the exact
main's completion was already durable before restart. Main exit zero succeeds
only after confirmed container cleanup, private evidence persistence, network
removal/absence, durable release and output finalization. Saved nonzero, memory,
OOM and containment failures retain their failure disposition. Main OOM observations are persisted
with terminal observation, before sidecar TERM. A main first found finished during
recovery remains interrupted/lost. Operator release cannot fabricate success.

A crash after durable release but before result save remains interrupted and retries.
A failed release write or network-removal retry may conservatively report containment
failure for disconnected containers; missing private attachment is never accepted.
New custody records bind job/attempt/machine/session as well as opaque execution
identity; recovered result files and worker-state results validate those fields.
Existing custody without the added binding retains its opaque identity check.
Queue current-attempt and exact release-key matching are unchanged.
Containment checks retain the pre-#55 behavior for stopped and running containers.

No stop dispatcher changes: permanent TERM intent, original grace, deadline KILL,
orphan stop bounds and private sidecar evidence/retention remain as in #54.
Fake-clock/fake-CLI regressions cover all seven findings cases, including repeated
restart, persistence/inspection/log/network faults, stale results, exact old
custody settlement and acknowledgement replay. Native Docker/GPU acceptance and
the v3 forward-port remain separate work; neither is authorized by this hotfix.

Round-1 verification on `hotfix/55-restart-mid-grace`: build **0**, two uncached
full race suites **0 / 0** (internal package **168.705 / 171.475 s**), vet **0**,
and `./myce dev` **0** after a real API/command smoke and graceful shutdown.
The smoke's submit/wait/log commands each exited **0** and confirmed jobs do not
inherit the token. Both full suites include the executable dev checkpoint/model
E2E tests. Runs were serial, `GOMAXPROCS=2`, `GOFLAGS=-p=1`, `nice -n 19`;
command exit codes were captured directly without pipelines. Final focused race
regressions also passed (**0**). No real Docker/GPU tests or remote hosts used.

Current state: implemented and verified locally for #55; committed for review.
Next: orchestrator review, then separately authorized native acceptance and v3
forward-port. No push, GitHub post, issue closure or fleet qualification.


### Round 2: conservative crash recovery and strict containment

The released-custody fallback again reports interruption when no result was saved;
only full recovery with output finalization and result persistence may succeed.
The stopped-container disconnection exception is removed, restoring the exact
`f5c4461` containment check. No new state, phases, stop-dispatch changes or queue
matching changes were introduced.

`TestDockerRestartReleasedWithoutResultStaysLoss` covers the unsaved-result crash
and failed output sync/close, with one queue loss and attempt-2 retry.
`TestDockerRestartDisconnectedSidecarIsContainmentFailure` covers unexpected
attachment loss and sidecar exit before inspection during grace.
Existing proof-failure tests now require containment failure after release-write
or network-removal failures leave disconnected containers. All seven #55 cases
remain covered, including full recovery success with one attempt and zero losses.

Round-2 local gates, serial with `GOMAXPROCS=2`, `GOFLAGS=-p=1`, `nice -n 19`,
and direct subprocess exit codes (no pipelines):

- `go build -o myce ./cmd/myce`: **0**.
- First `go test ./... -race -count=1`: **1**. Checkpoint filesystem-operation
  timeouts in `TestProductionHoldDisplacesBothCardsAndCooldown`,
  `TestWorkerOrphanCleanupVerifiesIdentity`, and
  `TestWorkerRelativeCommandUsesJobWorkdir`.
- Second `go test ./... -race -count=1`: **1**.
  `TestReviewDeadlineDelayedGateMustNotExecute` timed out waiting for its worker
  condition after 60 seconds. The first-run failures did not recur.
- `go vet ./...`: **0**.
- `./myce dev` with isolated local paths and loopback port: **0**, including real
  API submission, command execution, token noninheritance, wait/log verification
  and graceful shutdown. Wait and logs each returned **0**.

The focused non-race restart suite returned **0**; no #55 test failed in either
full race run. The four failing tests and checkpoint-I/O implementation are
unchanged from `915be3a` and `f5c4461`; the causes remain unresolved. No tests or
timeouts were weakened. Logs and direct gate receipts remain in the ignored
`.reports/issue-55/round-2/` directory.

Current state: both round-2 findings addressed and committed locally; the full
race gate is **not clear**. Next: orchestrator review and triage of the recorded
gate failures before acceptance. No implementation deviation from the ruling,
push, GitHub post, remote-host access, native acceptance or v3 forward-port.
