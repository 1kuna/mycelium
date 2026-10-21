# Onboarding review repairs for #108

Current state: all review fixes are locally verified. Focused tests, expanded
end-to-end recovery, builds, vet, dev and both required unfiltered serial race
commands pass. No fleet operations, service installation, real credential
inspection or accelerator qualification was performed.
The only native service read was a running local Apple LaunchAgent's
`launchctl print`; the committed fixture contains no original label, path,
argument, environment value or process ID.

## Fail-first evidence

All behavior-changing findings below were observed failing with exit 1 before
their production fix. Existing assertions remain; the busy-upgrade policy
expectation now requires enabled policy on refusal, as directed by this review.

| Review finding | Observed base failure | Passing coverage |
| --- | --- | --- |
| Top-level launchd state | Captured running service rejected because a later nested state was active | `TestEnrollmentLaunchctlCapturedNestedState`; realistic nested e2e manager output |
| Failed upgrade abandons drain | Second prepare lost self-made resume authority; busy refusal left policy drained | `TestEnrollmentUpgradeRerunResumesOwnDrain`, `TestEnrollmentBusyUpgradeDoesNotLeaveDrain`; timeout race and receipt-write rollback tests; interrupted new-binary e2e recovery |
| Ticket in child argv | Child inspection exited 41 on its argv | `TestBootstrapTicketAndInvalidDownloadRecovery/ticket` inspects argv/environment and requires the exact stdin ticket |
| Bootstrap credential duplicate | Successful e2e left the second invitation file | E2e asserts deletion after install; cleanup also runs after same-version recovery |
| Older invitation downgrades | Authentic older line succeeded against newer installed bytes; unordered versions were accepted | E2e refuses replay downgrade and exercises explicit override; `TestReleaseDowngradeOrdering` covers numeric ordering, equal versions and prereleases |
| Circular expected inventory | One visible XPU satisfied a saved two-XPU configuration without a typed reason | `TestEnrollmentConfiguredXPUAbsent`, `TestEnrollmentSavedConfigRejectsMissingXPU`; readiness separately checks saved settings/selectors |
| Explicit identity loses to HOME | Invite used the installed HOME authority instead | `TestInviteExplicitIdentityOverridesHome`; original private-file test isolates HOME |
| Corrupt full-size partial / redirect body | Returned generic rerun messages and retained invalid partial bytes | Bootstrap subprocess tests require typed refusal and deletion, and prove next-run recovery |
| Same-version options silently ignored | E2e saw no explanation | E2e requires explicit explanation that installer flags are not reapplied and still verifies no restart/config rewrite |
| Dead install flag | `--install=false` was accepted | `TestInviteDeadInstallFlagRemoved` |
| Ticket rows accumulate | Three rows remained where only the live unused row was needed | `TestEnrollmentTicketPrunesExpiredAndUsed` |
| Receipt rewrites / worker exit | Identical receipt changed mtime; failed write aborted the worker loop. A follow-up regression also reproduced monotonic-clock metadata causing a false rewrite after JSON decoding | `TestEnrollmentReceiptWrittenOnlyOnChange`, `TestEnrollmentReceiptFailureDoesNotStopWorker` (continued checkins and later successful retry) |
| Stability too short | Readiness returned after six observations / five seconds | Strengthened fake-clock stable-manager test requires at least 31 observations over 30 seconds |
| Fixed release target count | Base release command rejected build-target enumeration | `TestReleaseBuildTargetsList`; `TestReleaseMatrixUsesConfiguredTargets` extends the declared matrix without a count constant |

The release-command and independent configured-inventory baseline tests used
Go's file overlay with the exact `8a5a590` versions of `cmd/myce/release.go`
and `internal/myce/enrollment.go`, respectively; other sources were current.
Those both exited 1. The remaining listed initial failures ran before production
edits on the original worktree HEAD.

The corrected redirect test and added HTTP concurrent-redemption test already
passed on base (exit 0): those are stronger evidence for existing safeguards,
not newly reproduced security defects. The redirect destination would return a
valid invitation if followed, and its hit counter must remain zero. Eight
concurrent HTTP redemptions yield exactly one success.

## Decisions

- Reuse the existing pinned joining client, SQLite, installer, spool and host
  maintenance CAS. No new service or registry.
- Ordered semver in the signed manifest, compared with the installed executable's
  `version` output. Unordered releases fail closed. Explicit `--allow-downgrade`
  permits only an ordered downgrade.
- Refuse busy hosts before drain. If custody races with a drain, restore the
  self-made transition on timeout/cancellation with a separate cleanup budget.
  Save a private host/name/policy-timestamp resume receipt for install failures;
  a successful rerun, including one with already-installed new bytes, resumes it.
  Existing operator drain/disable remains respected and CAS fences later changes.
- Drain wait, the existing installer setup timeout, and each readiness wait have
  separate budgets. Readiness requires 30 stable seconds.
- Keep interrupted pre-install authority mode 0600; delete the bootstrap copy once
  installed joining authority is durable. Ticket transport is stdin only.
- Same-version reruns preserve no-op behavior and explicitly explain that settings
  changes use the ordinary installer path.
- The target data drives manifest validation, publisher enumeration and builds.
  The release script still requires its documented native Apple Silicon host.

## Validation

- Focused review/release/invite/enrollment tests: exit 0.
- Initial repaired full e2e: exit 0 (145.189 s).
- Expanded interrupted-upgrade e2e: exit 0 (210.430 s); final receipt-correction rerun exit 0 (211.429 s).
- `go build -o myce ./cmd/myce`: exit 0.
- `go vet ./...`: exit 0.
- Three-target release script with disposable standard-library Ed25519 key:
  exit 0; signed manifest creation and all artifact checksums exit 0.
- Native Mac exact version/protocol/sensing inspection: exit 0.
- `myce dev`: authenticated real API and one fake worker observed; SIGINT and
  graceful process exit 0. State is confined to ignored local scratch.
- An initial race sequence started with zero other real race PIDs, then was
  superseded by the receipt serialization correction. Its active command was
  allowed to finish; its empty supervisor was then terminated (exit 143). The
  incomplete/superseded sequence is not acceptance evidence or a flaky-test retry.
- `go test -race -p 1 -count=1 -timeout 30m ./internal/myce ./cmd/myce`:
  exit 0; gate counted one other real race PID. Core 975.638 s, CLI 276.759 s;
  total invocation 1269.3 s. No flaky-test retry.
- `go test -race -p 1 -count=1 -timeout 30m ./...`: exit 0; fresh gate counted
  one other real race PID. CLI 278.477 s, hostbridge 1.128 s, core 1243.586 s;
  total invocation 1527.8 s. No flaky-test retry.

No expectation, filter, race flag or required timeout was weakened. The e2e's
outer budget accommodates the stronger 30-second window and additional upgrade
attempts; the two known real-clock flaky tests and their deadlines were unchanged.
No authorized flaky-test retry was needed in either final command.

Next step: orchestrator integration with B2/B3/B4, then the remaining B5/B7/B8 and
separately authorized native fleet acceptance. #108 remains open.
