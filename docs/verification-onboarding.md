# B0/B1/B6 onboarding evidence (#108)

This records the initial `8a5a590` implementation. Current review repairs and
verification are tracked in [the review evidence](verification-onboarding-review.md).

Current state: implemented and laptop integration-tested on the package-M base
`30ed4cc`. Both required unfiltered race commands have exit-0 results. The first
full repository run failed two existing test cases; an unchanged full retry passed.
The intermittent failures' cause remains unresolved and is not established as
preexisting. No fleet access,
SSH, native service installation, real credential inspection or native GPU
qualification was performed. B2/B3/B4 integration and B5/B7/B8 remain separate.

## Acceptance coverage

| Slice / acceptance | Evidence |
| --- | --- |
| B0: Linux amd64/arm64, native macOS arm64+cgo | `scripts/release.sh` builds all three actual executables; header/architecture checks, signed manifest and SHA256SUMS verified against the built bytes; Mac `version` reports `native_sensing=true`. |
| B0: trusted manifest, exact target and protocol | `TestReleaseTrustMatrixProtocolAndMacSensing`, `TestReleaseQueueApprovalAndAuthentication`, `TestReleaseVersionRefusesWrongArtifactProtocolOrTarget`, `TestBootstrapRequiresTrustedHTTPSOrigin`: wrong key, changed manifest, unsupported target, old protocol, absent native sensing, traversal and unsafe origins refuse. |
| B0: interrupted download; no Go/git; wrong/tampered bytes before install | `TestOneCommandEnrollmentEndToEnd`: preseeded partial resumes with HTTP Range; forbidden-tool shims stay unused; wrong-version executable and tampered bytes fail before redemption or config installation. |
| B1: existing invite/join path; default single command; optional file | End-to-end calls `inviteCommand(nil)` against queue-approved release metadata, executes its one physical output line, and checks `--out` equality, mode 0600 and overwrite refusal. Existing file-invitation privacy assertions remain under explicit `--private-file`. |
| B1: expiry, one-time scope, restart/replay, wrong pin, redirect | `TestEnrollmentTicketExpiryReplayRestartAndScope`, `TestEnrollmentTicketPinnedHTTPAndRedirect`: fake-clock exact expiry boundary, hashed SQLite storage, concurrent single redemption, restart persistence, no jobs/status authority, exact certificate mismatch and redirect refusal. |
| B1: no fleet credential in command/history/output/logs; private source | End-to-end checks the printed line and decoded payload, all command output, queue/host logs, original private invitation, and replay on a second fake host. `TestEnrollmentRunnerSmokeStripsTokenAndRetainsNoCustody` exercises the ordinary recorded runner without MYCE_TOKEN. |
| B1: installed rerun needs no new ticket | End-to-end repeats the spent-ticket command successfully with no additional redemption, download, config rewrite or manager restart. |
| B6: exact session, protocols, devices, CPU/RAM, budgets, runner and manager | `TestEnrollmentReadinessContract`, `TestEnrollmentWaitRequiresStableManagerAndSupportsPoliteWaiting` and end-to-end cover queue observation versus local receipt, expected identities/counts/totals, current service PID and release, acknowledged CPU/RAM policy, runner proof and five seconds of stable manager identity. |
| B6: no false ready on unreachable queue, 426, start loop or withheld device | `TestEnrollmentWaitUnreachableAnd426`, `TestEnrollmentServiceStartLoopRefused`, readiness-contract cases, and changing PID/restart-count wait test. `TestEnrollmentUpgradeRefusesQueueProtocolBeforeDrain` rejects 426 before changing maintenance or replacing the worker. |
| B6: same release no-op; changed release preserves custody and disabled policy | End-to-end verifies stable config/manager/download/redemption on rerun, then upgrades a disabled hypha and preserves a spool sentinel. `TestEnrollmentUpgradeCustodyBlocksAndCASDoesNotResumeDisabled` proves an outstanding exact attempt blocks upgrade and a concurrent operator disable defeats conditional resume, even at the same fake-clock timestamp. `TestEnrollmentUpgradePreservesDisabledAndDrained` also covers successful owned-drain/resume and preexisting drain preservation. |
| B6: polite waiting is successful, distinct from ready | Readiness-contract and stable-manager wait tests return `polite_waiting` for daily-use participation that is waiting for owner inactivity. Disabled/drained remain separate successful maintained states. |
| Laptop full journey | End-to-end builds two real CLI versions, uses an HTTPS publisher and pinned real queue, fake HOME and fake launchctl/systemctl starting the installed worker, real runner smoke, capability summary, replay/tamper/wrong-version refusal, no-op rerun and disabled upgrade. |

CPU/RAM equality and policy acknowledgement were tightened after new tests first
failed with false `ready`. The pre-drain protocol check likewise followed a
failing regression test. Further failing tests reproduced an old receipt falsely
certifying a replacement service; readiness now requires the current release and
the manager PID to match the receipt. Initial release/ticket/readiness tests preceded their
APIs. No existing assertion was removed or relaxed; old private-file invite
assertions now explicitly select the preserved compatibility mode.

## Decisions and limits

- Reuse existing invitation trust, SQLite, installer transaction, command runner,
  worker spool and queue host maintenance. No separate joining service or queue.
- Queue-approved release metadata lets bare `myce invite` print one command.
  Provision the publisher public key independently; neither a manifest nor a
  discovered queue supplies its own trust root. Standard HTTPS and Ed25519 plus
  SHA-256 cover distribution; the LAN client's full certificate-DER pin remains
  separate from curl's SPKI pin.
- Tickets default to 15 minutes, at most one hour, and authorize one enrollment.
  Save the redeemed invitation privately for interrupted setup. A response lost
  before saving needs a new ticket; it does not justify permitting replay.
- Printed lines freeze a release and its hashes. Repeat one for same-release
  recovery/health; use a newer release's line to upgrade. Installed hosts use
  saved authority and do not redeem the new line's ticket. Host identity is
  durable and unique; upgrades preserve queue-owned policy and custody.
- NAS mode follows #109; ordinary B2/B3/B4 installer options pass through. The
  laptop fixture explicitly uses fake inventory, storage and service managers.
- The small CPU runner smoke is enrollment proof, not accelerator kernel or
  application extension qualification. Native Linux managers/devices, actual
  launchd login/network permissions, Mac distribution signing/notarization and
  MacBook sleep/travel still require separately authorized acceptance.

## Validation

- Focused release/invite/enrollment suite: exit 0.
- `go test ./cmd/myce -run '^TestOneCommandEnrollmentEndToEnd$' -count=1 -timeout 5m`: exit 0.
- `go build ./...`: exit 0.
- `go vet ./...`: exit 0.
- Three-target release script, manifest signature and artifact checksums: exit 0.
- Native Mac `version` with exact release, protocols and sensing requirement: exit 0.
- Standalone `myce dev`: real authenticated API, one registered fake worker,
  graceful SIGINT shutdown, exit 0. The smoke harness was corrected to keep its
  log outside state roots and wait for the first worker check-in.
- `go test -race -p 1 -count=1 -timeout 30m ./internal/myce ./cmd/myce`: exit 0
  (`internal/myce` 925.473s, `cmd/myce` 94.312s). Admission counted one other
  `go test -race` PID before launch.
- `go test -race -p 1 -count=1 -timeout 30m ./...`: exit 1; admission counted
  one other race PID. CLI and hostbridge passed. Core failed
  `TestM4DevSplitConvertConcurrentRollback` (immediate legacy assignment missing)
  and `TestNativeGroupEarlierConsumerEndpoint` (15-second timeout). No data race
  was reported. Failure evidence is preserved for diagnosis; no assertions or
  deadlines have been relaxed.
- Unchanged targeted race repetitions (`-count=5`): exit 1; native endpoint timed
  out twice, while M4 passed all five. The same cases on an isolated archive of
  base `30ed4cc` passed five repetitions (exit 0). A diagnostic current-source
  archive with launch counters passed ten endpoint repetitions (exit 0). This
  establishes intermittence, not a resolved cause; diagnostic instrumentation is
  confined to ignored scratch copies.
- Unchanged full retry, `go test -race -p 1 -count=1 -timeout 30m ./...`: exit 0
  (`cmd/myce` 121.400s, `internal/hostbridge` 1.128s, `internal/myce` 1135.506s).
  Admission counted one other race PID. An additional zero-other-race restriction
  was dropped before any test started when more suites arrived; the assigned gate
  remained at most one other race PID. Earlier failures remain part of the
  evidence; the retry changed no assertions, filtering, count or timeout. This
  passing run does not establish a root-cause fix for the intermittent failures.
- The explicit summary-name assertion was added after the focused race pass,
  passed in the targeted end-to-end test, and is included in the full run.
  Production code is unchanged between the required runs.

Next step: integrate with B2/B3/B4, carry the intermittent existing-test failures
into integration/base review, and schedule native acceptance separately. Keep
#108 open for the other slices and native qualification.
