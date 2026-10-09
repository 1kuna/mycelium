# Mycelium v2 test reliability

Branch: `v2-test-reliability`, created from local `main` at
`29fd20a222b1a01e83f2d930c2f653d618a49663`. Scope is local laptop
verification with fake GPU inventory and fake model engines. No fleet hardware,
LibreOffice, production services, push, or changes to unrelated workloads.

## Diagnosis before changes

The native baseline started at a load average of **45.69 / 34.92 / 38.83** on
this ten-core Mac (Go 1.25.6, darwin/arm64). `go test ./... -race -count=1`
failed in `TestWorkerRestartCleansOrphanBeforeReassignment`: its three-second
readiness guard expired at 3.48 s. Package times were 33.343 s (CLI) and
109.295 s (runtime). No data race was reported. The existing weekend report and
engine review provide the other observed failure names; source tracing below
separates observer deadlines from scheduler decisions and product defects.

| Test / failure | Classification and root cause | Fix and retained assertion |
| --- | --- | --- |
| `TestDevProductionStartsUnder15Seconds`, `TestDevCheckpointEndToEnd` production timing | (a)/(c): `time.Since(submitted)` included submission HTTP, log upload, subsequent status/log HTTP, and scheduling of the test observer. It measured observation latency rather than the queue's submitted-to-start metric. | Assert `StartedAt - Queued` and equality with `StartLatencySeconds`, still strictly below 15 s. Wait up to 60 s for start/log evidence. Add `TestProductionStartLatencyFakeClock`: full 10 s grace, no early kill, production assigned first, 1 s command startup, exact 11 s recorded latency, and unchanged measurement after a delayed observer. Keep `TestDevProductionStartsUnder15SecondsRealTime` using real HTTP/processes and a TERM-ignoring experiment; skip it only under `-short`. CLI checkpoint/resume/attempt/log assertions remain. |
| `TestGeneralServerLifecycle` ready/ignore and stop escalation | (c): four-second startup and three-second total-unload assertions timed test-process startup, durable record writes, process reaping, and `ps` inventory alongside the configured stop grace. Those operations are subject to host load. | Await real startup/stop completion within 60 s. Preserve the 50 ms stop grace, actual process-group disappearance, request/device/token passthrough, and failure modes. `TestStopCommandTimeoutEscalates` retains its 150 ms grace and checks that durable engine/helper records contain no live PID. |
| Server cleanup `operation not permitted` | (b): a final group signal can encounter macOS `EPERM` during process-group exit. Focused repetition reproduced this in `TestStopCommandTimeoutEscalates` even after test deadlines were fixed. | `signalWorkerGroup` accepts `EPERM` only after a successful process inventory finds no live group members. Surviving members and failed inventory remain errors. Regression covers success, ESRCH, exiting-group EPERM, denied live members, inventory failure, and unrelated signal errors. Final group disappearance is still verified. |
| Stop-helper `context deadline exceeded` | (b): the shared stop grace can expire during durable helper-record writes, before `exec.CommandContext.Start`. The old path returned that expected escalation as a cleanup failure. A subsequent loaded-Mac repetition reproduced it. A helper killed at the deadline can also close its gate before the parent writes. | Continue server TERM/KILL escalation when the unstarted helper's context expires. Accept deadline-related EPIPE only while still requiring helper-group cleanup. `TestStopCommandExpiredGraceStillCleansServer` expires grace deterministically before helper launch and asserts the server group is gone and engine PID cleared. |
| `TestStopCommandDescendantsAreCleaned`, `TestGeneralServerUsesCatalogEnvironment`, `TestLlamaAdapterAndOrphanCleanup` | (c): short guards around real child startup, environment copy, control-command startup, and process inventory. A two-second helper budget also mixed durable writes with a descendant-cleanup test that does not test grace duration. | 60 s readiness/control-command guards; exact descendant cleanup, environment selection, passthrough, orphan identity, and idempotent unload checks remain. Separate short-grace escalation tests remain. |
| `TestDevCheckpointEndToEnd`, `TestDevModelEndToEnd` startup/shutdown | (c): startup is signaled by stdout; shutdown is signaled by process exit, but 8–10 s guards and 15–25 s scenario contexts could expire while the Mac scheduled subprocesses or synced SQLite/worker state. | Keep those events and all exit/status/job checks; bound waits and test HTTP clients at 60 s, return as soon as conditions hold, and identify the test/deadline on failure. No fixed startup sleep added. |
| `TestImmediateStopNotification` and one-second HTTP/channel guards | (a)/(c): the stop event, streaming arrival, transport retry, cache inventory, parallel-copy gates, cancelled request completion, and transient-peak probe already had an event/condition proving correctness; a one-second observer guard wrongly asserted OS scheduling speed. | Use event/condition waits with a 60 s safety deadline. Stop notification still occurs without a worker heartbeat loop; cache inventory still completes with its copy mutex held; parallel copies still enter both gates before release; cancellation still releases admission; streaming still reaches the engine before uploader EOF; retry still preserves exact bytes/length and call counts. |
| `TestPreDeployFullSystem`, `TestPreDeployModelCopyFaults`, `TestPreDeployEnvironmentBuildRestart` | (c): one-second worker/control HTTP clients and 5–10 s integration guards included SQLite/filesystem contention and queue-swap locking. Copy/load request contexts could expire without any injected network fault. | 60 s bounded real-I/O/event guards. Fake-clock queue loss/preemption/restart decisions, explicit partition transport, mid-copy gates, durable-prefix bytes, pin load count, verified publication, bad-master rejection, one attempt/start, and exact output counts remain. Model-load safety deadline is 60 s; idle setting is still one second. |
| `TestDevModelEndToEnd` one-second idle ready-status assertions | (a): after a successful answer, a subsequent HTTP status observation could arrive after the one-second idle timer had legitimately unloaded the model. This is a transient-state observation race, not evidence of a failed load. | Move the exact ready/status/idle/reload assertions to `TestModelReadyIdleReloadStatusFakeClock`, using the real status handler and injected clock. Both cycles assert ready, still ready 1 ns before expiry, draining at expiry, and absent after unload. CLI retains cold status, real successful answers, observed idle unload, and a successful call from the unloaded state proving reload; its one-second idle configuration is unchanged. |
| `TestWorkerRestartCleansOrphanBeforeReassignment` (new baseline reproduction), worker/recovery/sync/memory integration guards | (c): readiness or exit deadlines of 3–5 s included real process launch, durable state, logs, and process inventory. The failed crash-helper readiness assertion could itself leave an owned orphan after the helper was killed. | Shared 60 s condition/exit guards; attempt 2, old-group disappearance before reassignment, replay byte identity, durable output, and fail-closed memory assertions remain. Add deferred identity-verified crash-helper orphan cleanup. The orphan from this task's failed baseline was identified by its exact execution marker, checkpoint directory and 06:08:12 launch time, stopped, and verified absent; older unrelated groups were untouched. |
| `TestGeneralServerQueuePassthroughAndStreaming` fixed sleep | (a): a 120 ms child sleep attempted to create a streaming observation window. | Child holds the second chunk on an explicit release event. Parent receives the first chunk before releasing the second and asserts the exact remaining chunk, plus existing HTTP passthrough checks. |

The supporting guard changes cover
`TestCacheInventoryDoesNotBlockHeartbeatDuringCopy`,
`TestWorkerStreamsGenericBodyHeadersAndLength`,
`TestGenericDiskSpoolReplaysTransportRetry`,
`TestParallelCopiesActuallyRunTogether`, `TestSyncUsesRealAPIAndWorker`,
`TestMeasureProbeIncludesTransientLoadPeak`,
`TestModelLostWorkerRetriesInFlightCall`,
`TestCancelledModelCallDoesNotLoadRetryWorker`,
`TestWorkerOfflineLargeOutputIsDurable`,
`TestWorkerOrphanCleanupVerifiesIdentity`,
`TestWorkerPersistedResultReconcilesThenReplaysBeforeFresh`, and
`TestUnifiedProbeFailureStatusAndRecovery`, plus the shared worker fixture/await
helper. These were inspected for whether the duration was a product guarantee
or a synchronization guard before changing them. Scheduling, leave, loss,
retry, priority, grace, and idle decisions retain their injected-clock tests.
No queue/scheduler timing constants, production target, hardware configuration,
or race detector setting were relaxed. The readiness rejection timeout and
short escalation grace remain separate from positive startup waits.

## Verification

Final Go source/manifests snapshot SHA-256 (sorted path, NUL, file bytes):
`7616bf600d8fe84281fd6c22be8c69bcc8fc385d71f7beea9e7605c1f0d3d9bb`.

- **Mac current-load final source:** `go test ./... -race -count=3 -v` passed
  all three suite repetitions. CLI 78.987 s, runtime 299.332 s, total wall
  308.67 s. Load at start was 11.90 / 16.25 / 26.69; the other work changed
  load naturally during diagnosis, without intervention. Queue-measured
  production starts: CLI 3.891 / 1.366 / 1.362 s; TERM-ignoring real-time check
  8.035 / 10.806 / 10.375 s. Every value remains below the unchanged 15 s target.
- **Mac with eight additional CPU workers:** `go test ./... -race -count=3 -v`
  passed all three suite repetitions. CLI 81.246 s, runtime 283.201 s, total
  wall 285.26 s. Eight separate owned Python busy-loop processes stayed active
  throughout the command. Sampled load reached 41.53 / 29.20 / 27.01; end load
  was 41.24 / 32.21 / 28.34. All eight were terminated, reaped, and verified
  absent after the run. Mac `GOFLAGS` was empty and `GOMAXPROCS` unset for both
  final Mac commands; no test concurrency throttling or unrelated workload
  changes. All three real-time queue start measurements remained below 15 s.
- **Linux final source:** one uncached `go test ./... -race -count=1` passed
  in an owned Docker container, Linux/arm64 Go 1.25.6. CLI 27.881 s, runtime
  111.104 s, total wall 160.02 s including container/source-copy/tool startup.
  Existing cached image `myce-weekend-check-cache:20261009`, four-CPU/4 GiB
  container limits, inherited `GOFLAGS=-p=1`, `GOMAXPROCS=2`, and
  `GOTOOLCHAIN=go1.25.6`. Only a read-only snapshot of Go source and manifests
  was mounted; no GPU, fleet config or credentials. Containers are stopped
  and retained rather than deleted.
- **Supporting checks:** `go build ./...`, `go vet ./...`, and
  `git diff --check` passed. Ten focused race repetitions of server lifecycle,
  helper escalation, expired-before-launch grace, and permission-result
  regression passed after both product fixes. Streaming and expired-grace
  regressions passed three focused repetitions. A focused race `-short` run
  passed both new fake-clock tests and skipped only the labeled real-time
  production check.
- **Earlier diagnostic evidence:** baseline full Mac failure described above;
  focused ten-repeat server runs reproduced EPERM and then expired-helper
  context before their respective fixes. An intermediate three-repeat Mac
  full suite passed (CLI 86.555 s, runtime 323.117 s), and an intermediate
  Linux full suite passed (CLI 29.781 s, runtime 64.456 s). These precede the
  final failure-path cleanup amendment and are additional evidence, not the
  final-source acceptance count. One Docker preparation command encountered
  a missing `time` executable before tests started; the actual Linux runs used
  the host timer and both completed normally.

The suite has no newly imposed minimum sleeps: condition/event waits return
immediately on success. The final Linux single execution completed in 160 s
under host load and constrained CPUs, below the requested roughly three-minute
quiet-machine budget. The three-repeat Mac command averaged about 103 s per
suite repetition, including its initial build. This is measured loaded-host
verification; no separate artificially quiet Mac run was obtained by stopping
other work.

Raw logs, process PID lists and source snapshots remain outside the repository
under `/tmp/myce-reliability-*`. Production/start, state, output, checksum,
attempt, cleanup, streaming, and fail-closed assertions remain checked. Local
fake-backend tests are not fleet acceptance.


## DIGEST
- Committed on `v2-test-reliability`; each flake diagnosed in this report.
- Fake-clock logic and 60 s condition/event guards preserve assertions.
- Fixed server EPERM cleanup and expired stop-helper grace; regressions pass.
- Queue-timestamp production target stays below 15 s; labeled real-time check retained.
- Final full race suites: Mac 3/3, eight-worker CPU stress 3/3, Linux 1/1 passed.
- Build/vet, short-mode regressions and diff checks passed; single Linux run 160 s.
- No fleet access, LibreOffice, push, or unrelated workload changes; owned stress reaped.
