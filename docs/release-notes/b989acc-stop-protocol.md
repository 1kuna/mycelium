# Frozen v2 Docker stop-protocol hotfix (#54)

Base: `b989acc138fc160c60430855e415ea3846048b44`.
Branch: `hotfix/b989acc-stop-protocol`.
Tested ruling implementation: `e7745ab92cd50fc3f94401de90ef49b9901a66db`.
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
- Following documentation-only commit records the tested implementation SHA
  and fresh verification. A commit cannot embed its own SHA; the final head is
  recorded in #54 with an immutable link to this note.

No #40/#44 test fixes or other v3 changes are included. No new dependencies,
PR, merge, main/v3 update, production deployment or fleet access.

## Defined stop behavior

The existing asynchronous reconciler and injected-clock deadline remain.
A blocking `docker stop --timeout` would require recovering a separate outstanding
operation; the existing bounded calls implement this policy directly.

TERM is attempted **at most once per exact container per stop request**.
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

## Private evidence and operator runbook

Every settled main/sidecar retains its exit code and stderr tail in the attempt
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

Current state: requested implementation and verification complete; final head
and normal-push/equality results are recorded in #54. Next: orchestrator review,
separately authorized native fleet/Verso acceptance, and guarded artifact cleanup
when Docker releases its directory handles. Issue #54 remains open.
