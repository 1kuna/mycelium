# Frozen v2 Docker stop-protocol hotfix (#54)

Base: `b989acc138fc160c60430855e415ea3846048b44`.
Branch: `hotfix/b989acc-stop-protocol`.
Orphan implementation: `520aa30a6e9f7011c641b345204ee9a0319f3378`.
The published head is recorded in the
[#54 implementation report](https://github.com/1kuna/mycelium/issues/54).

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
Reconciliation preserves positive pending-start acceptance and bypasses only
persistence barriers when deadline or forced KILL is due.

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

Known flaky tests: see #57.
