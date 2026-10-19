# Laptop component probes

Run all six component types with the real `myce dev` API, SQLite queue and
plain-command runner, plus the existing filesystem/service/kernel/Docker fakes:

```sh
go test -race -v -count=1 ./internal/myce ./cmd/myce -run '^TestDevProbe' -timeout 3m
```

Each named test reports PASS/FAIL and assertion failures explain the missing
effect. Use `-count=3` to repeat the probes. This is a composition of the existing
Go dev e2e helpers and component tests, not another canary framework. The CLI
and internal tests with the same name are two halves of one component type.
Stop at the first failing subcase; retain its output for review.

| Component / exact test name | Laptop fixture, assertions and finally | Native-only remainder (not run) |
| --- | --- | --- |
| Install/upgrade: `TestDevProbeInstallUpgrade` | Isolated install homes, fake manager and real same-binary worker child. Invalid configuration refuses; upgrade preserves config; real RC writer fixtures refuse without mutation; operator archival permits fresh CLOSED install; explicit revision opens admission. Child finishes CLOSED and status is read-only. Each fixture has one owner; install bound is 60s. | Real user manager, delegation, worker/root identity, protected binary, permissions and effective unit gates. |
| Ownership switch: `TestDevProbeOwnerSwitch` | One dev host and short real command; verify its file effect and exact release. Fake external owner holds the existing reconcile lock while open races close. Close wins, stale launch refuses. One 30s deadline including final local close and worker join. | GPU/daemon release and mutual exclusion with the real other owner. |
| Local rollback: `TestDevProbeOfflineRollback` | Real 2s command signals readiness with a file, then queue goes offline. Close stops the command before its final effect; premature open refuses. Reconnect settles exact custody, but fake consumer must obtain its own cancellation acknowledgement before reopening and rejects late publication. A second command is fenced and final host is CLOSED/settled with no active/unsettled custody. Reuse fake incomplete/late Docker disposition tests: an empty scan alone cannot certify release. The dev fixture has one 30s deadline including teardown. | Real process/cgroup, GPU and Docker release, late daemon operations, application cancellation/publication. |
| Reboot resume: `TestDevProbeRebootResume` | Restart the dev queue and workers against the same SQLite/host state. Saved revision/deadline survives; old-boot observations are unknown. Advance the dev clock inside and beyond the deadline; incomplete exclusion raises an alarm and refuses open. Explicit bounded retry requires positive evidence and gets zero renewed grace. Finally CLOSED and joined, within one 30s overall deadline including modeled downtime. | Authorized reboot, boot identity, systemd startup, real memory/custody reconciliation, no duplicate application execution. |
| Queue stop/start: `TestDevProbeQueueRestart` | One dev host, real API outage and SQLite reopen at the same endpoint while workers persist. Discard a committed POST response; replay preserves job/attempt and the command's append effect occurs exactly once. Changed body returns 409; both submit/cancel orders and a concurrent API race stay cancelled. Consumer persists a cancellation intent, cannot reopen from host settlement alone, drains the reservation, and submit stays cancelled. Advance 30d: unbound reservation submit returns 410, cancel is unknown, unseen is absent, retained cancelled job replays. Finally local CLOSED and worker join; one 30s total budget (stricter than the native 60s bound). | Real queue-service outage/recovery, transport loss, consumer persistence and application behavior across reconnect. |
| Rest verdict: `TestDevProbeRestVerdicts` | Reuse the complete closed/ready matrices: stale sources/boot/revision, missing custody/telemetry/queue, fenced vs unfenced/foreign/unknown placement, terminal unreleased attempts, required workers and mixed FAIL/UNKNOWN. Real dev API/CLI proves READY with active work, CLOSED with fenced jobs, reopen, and final CLOSED/PASS. Each observation is bounded to 10s; dev lifecycle has one 30s deadline. | Real cgroup/daemon/GPU/restart observations and memory; model/app health and consumer cancellation are separately owned. |

The dev clock stays private to `StartDev`; it advances queue decisions without
waiting for days. No production clock override, removal API, fleet access or
consumer database is introduced. Exact fake-clock boundaries and collection
matrices remain component tests. Real process readiness uses file effects;
cleanup is established by release/settlement and joining workers, not logs.
The dev clock's offset resets on process restart; the durable stop deadline does
not reset. A previous-boot observation never supplies fresh release evidence.

Also verify the clock and a standalone built binary:

```sh
go test -race -count=1 ./internal/myce ./cmd/myce -run '^TestDevClock'
mkdir -p .reports/wp10
go build -o .reports/wp10/myce ./cmd/myce
MYCE_DEV_SMOKE_BINARY="$PWD/.reports/wp10/myce" go test -race -v -count=1 ./cmd/myce -run '^TestDevStandaloneSmoke$'
```

`TestDevStandaloneSmoke` starts that exact binary with an isolated temporary
HOME, working directory, DB/checkpoints/host state, allowlisted environment and
dynamically allocated loopback port. Authenticated API readiness, a short
command's file effect and release, CLOSED settlement, owned-child exit and
listener disappearance must all pass within one 30s context including teardown.
It builds a temporary binary when the environment variable is omitted.
Bare `myce dev` is intentionally a long-running server.

Keep verbose probe output and command exits as laptop evidence. The native
checks above remain **needs-maintainer**; these probes do not authorize activation or
replace the consumer's separately scheduled native qualification.
