# Mycelium v2 fairness, usage, status and engine review

Date: 2026-10-09. Branch: `v2-fairness`, created from `main` at
`422ba635781037279044b4e385be6a949f48194e`.

Read `AGENTS.md` and the approved design before implementation. The requested
addition supersedes the design's earlier deferral of project fairness; the design
and guide now describe the authorized behavior. Work used only local fake GPU
inventory and ordinary command workers. No fleet machine, LibreOffice, push,
deployment, or installed machine service was used.

## Independent review of 422ba63

A separate T3 child reviewed the engine step without having authored it. Full
findings and regression evidence are in `v2-engines-review.md`, read alongside
the original `v2-engines-report.md`.

Fixed proven defects:

- Generic uploads were buffered twice and rejected over 64 MiB. They now use a
  bounded 8 GiB queue-local replay spool and a streaming worker-to-engine body.
  Content length, methods, queries, headers, cancellation and response bytes are
  preserved. OpenAI JSON keeps its 64 MiB limit.
- Transport attempt one closed the shared replay file; each attempt now receives
  a reader that cannot close the spool, and a real aborted-connection test proves
  attempt two receives the same body and length.
- Redirected health responses could incorrectly declare readiness. Health and
  proxy clients do not follow redirects; an already-observed process exit also
  rejects readiness.
- Explicit device selection clears stale catalog selectors and inherited host
  selectors for other GPU backends, without changing the global environment.
- Multi-file recovery held every source file open through the directory walk.
  Each closes immediately; a 300-file test covers this failure shape.
- An unused request interface was removed. Existing buffered compatibility is
  retained for OpenAI/fake engines and measurement warmups.

Integration caught and repaired an additional regression from the upload fix:
OpenAI calls had reached a generic worker endpoint requiring a streaming engine.
The queue now preserves the worker's three OpenAI endpoints; a targeted routing
regression and the existing dev/Python model tests cover the contract.

Measurement review found no further proven defect in selected-device readings,
baseline/peak/delta definitions, failed probes, warmup, reverse-order cleanup or
environment retention. Samples are system readings, not guaranteed allocator
maxima. No actual model or driver was evaluated on hardware.

## Fair sharing

Experiment selection compares each eligible project's weighted fair-share
**deficit**, rather than queue length. Its GPU-time ledger decays continuously
with a 24-hour half-life and includes still-running work before dispatch.
Competitors must fit the worker and pass their caps. Missing projects have weight
1; a sole project can consume all available capacity. Production and deadline
ordering retain the established comparator and preemption behavior.

`--project-config` configures persistent weights, concurrent experiment caps,
and a fleet GPU-memory share. Commands and assigned experiment calls count
against concurrency. Active model reservations count once per project and
worker, including reuse of a loaded model by another call. Production and deadline
work bypass experiment caps. Config parsing rejects unknown fields, invalid
numbers and additional YAML documents. The supplied map is copied before storage.

Status exposes cap reasons and fair-share explanations, including
`atlas is ahead of its fair share; verso goes first`. Pending and active model
calls now appear alongside command jobs in status and priority groups.

## Usage history

The SQLite queue atomically stores minute buckets split by machine/device,
project/job/model, with active, idle, pinned, declared memory and observed elapsed
time. Concurrent projects divide one device's elapsed time equally; it cannot
produce more than one active GPU-second per second. A loaded pin without work
remains idle and records reservation time separately. Idle loaded models also
retain memory samples.

After seven days, hourly aggregates retain dimensions and sums. Hourly retention
maintenance removes records older than 90 days. Fairness state, history and policy
survive queue restart. Unknown network/recovery time is excluded from the busy
and idle denominator. Partial boundary buckets are proportional estimates.

`myce usage --since 7d --by project|machine` and the authenticated usage API show
project GPU-hours, machine busy percent, idle hours, pin hours, preemptions and
production command submission-to-start acknowledgement latency. The report
answers occupancy, not physical GPU compute utilization or a causal comparison
against an unpooled fleet. Production start events persist separately from the
job's current attempt.

## Status page and local UI proof

The binary serves one self-contained page with plain CSS and vanilla JS. It
refreshes its same-origin fragment every five seconds without accessing the fleet
token. Cards show online state, platform, memory, jobs/models/calls, pins, cache,
environments and copy/build activity. The queue groups priorities and shows wait
reasons; failed jobs, lost machines and NAS failures are visible. Usage charts
are inline SVG from actual recorded samples. UTC day labels remain correct in a
browser using a different local time zone. No external assets or build step.

The screenshot `status-page.png` was captured from `myce dev` at local port 18731,
with three 64 GiB fake workers executing real Python sleep commands, actual
recorded Atlas/Verso usage and a fake-engine Atlas production pin. It shows no
invented seven-day history. Temporary database, worker spools and binaries are
under `/tmp`, outside the commit.

Browser verification checked desktop 1280px and phone 390px layouts, actual SVG
bars, refresh, empty states and memory/pin rendering. There was no horizontal
page overflow. Visual inspection found and fixed an overlapping environments
label on phones, then enlarged the desktop label column to avoid awkward wraps.
Smoke tests cover populated and empty snapshots and HTML escaping. Static UI
audit reported no warnings.

## Verification

- `go build ./...`: PASS.
- `go vet ./...`: PASS.
- `go test ./... -race -p 1 -timeout 30m`: PASS on the final tree, internal
  package 84.998s and CLI package cached from its passing run. Package execution
  was serialized to keep local process/disk contention bounded.
- Full Python suite using a temporary Python 3.13 environment with both YAML and
  OpenAI extras: PASS, all 9 tests, no skips; final-tree rerun 26.180s.
- Linux amd64 and arm64 cross-builds with `CGO_ENABLED=0`: PASS; binaries are
  outside the repository under `/tmp/myce-v2-fairness-build`.
- Fake-clock tests cover deficit/decay, weights, fit filtering, sole-project
  capacity, caps and production/deadline precedence, explanation strings,
  active/idle/pin accounting, overlapping projects, history retention,
  atomic rollback, restart persistence and production start/preemption events.
- CLI tests cover duration validation, authenticated usage API calls and output.
- Engine regressions cover streaming body delivery, spool limits/cancellation,
  replay after an actual transport failure, health redirects, device bindings,
  and many-file recovery. Existing streaming and measurement tests remain.

Earlier broad runs under concurrent load failed an offline-output timing check,
readiness and worker-leave timing checks and model end-to-end calls. The model failure was a real
integration regression and was fixed; subsequent full race runs passed. Readiness
fixtures retain the actual stop-grace assertion and allow startup a separate
four-second startup window. The targeted stop/worker-restart regressions also
passed three times under the race detector. The stop-grace assertion remains
three seconds; timeout/redirect fixtures remain 160 ms. No valid assertion was removed and no requirement weakened.
A redundant non-race run and obsolete pre-fix runs were interrupted; they are not
counted as successful verification. Unrelated workspace jobs were left alone.

## Limits and follow-up ownership

- Fairness is dispatch fairness; it does not interrupt a long experiment merely
  to rebalance shares. Pinned models and jobs that cannot fit remain hard capacity
  constraints. This is not a model-call rate limiter.
- The generic upload spool is bounded per request, not across concurrent requests.
  Mycelium's server-launch/upload contract owns the follow-up: introduce an
  aggregate spool quota if concurrent media uploads become a workload, and an
  inherited-listener/readiness handshake if port collisions are observed. The
  launcher's choose-then-close ephemeral port still has a small OS allocation race.
- Usage memory is declared workload memory and reported-budget occupancy, not
  kernel attribution. Without an explicit total budget, used memory is inferred
  from the highest reported free capacity. Native measurement remains a separate
  local hardware qualification task.
- There is no hardware qualification, fleet cutover or model-quality comparison
  in this result. Local functional proof does not claim those outcomes.

## DIGEST

Experiments now share GPU time by project weight, with caps and clear wait reasons.
Production and deadline order stays unchanged.
SQLite usage history shows busy time, idle time, pins and production start latency.
The phone-friendly status page includes fleet cards, queue reasons and SVG charts.
The independent engine review fixed uploads, readiness, device isolation and recovery.
Build, vet, race tests, all Python tests and both Linux builds pass.
The report and live fake-worker screenshot are saved; no fleet machine or push was used.
