# Develop and verify Mycelium

Use [quick/full gates](#laptop-test-gates) for laptop changes, the
[native acceptance checklist](#native-acceptance-checklist) for authorized device,
manager and boot proof, [component probes](#laptop-component-probes) for the real
API/runner, and [simulation](#scheduling-simulator) for synthetic policy comparisons.
Fake inventory never qualifies native hardware. The [worker protocol](#current-worker-protocol)
and [storage integration](#backed-storage-admission-integration) are developer
contracts, with unsupported/native gaps explicit.

Quick defaults to the merge base of the current branch and `origin/main`;
pass an ancestor `BASE` explicitly to check a wider batch. Every supplied ref
is reduced to its merge base with `HEAD`, so a moving `origin/main` never
selects unrelated changes that landed after the branch diverged. Known heavy
runs use the project's Mycelium submission path. Inspect host load/memory/disk
before local heavy work; do not exceed two full race suites on one Mac.

## Laptop test gates

Run `./scripts/gate.sh` (or `./scripts/gate.sh quick BASE`) for a change. The
base defaults to the branch merge base with `origin/main`; an explicit ref
uses its merge base too. Uncommitted and untracked changes remain included. Quick builds the CLI
and every release target, vets all Go packages, and race-tests changed packages with test names from matching file
prefixes, plus tests that directly reference changed exported symbols (using
diff hunk function names and changed declarations). Exported methods on private
receiver types match the concrete type rather than generic interface names such
as `Read` and `Close`, which otherwise select unrelated readers and entire dev
journeys. Changed non-Go files, including
docs/examples, also select tests with direct quoted paths relative to their Go
package. Helper-only or computed paths still need explicit focused testing or
`full`. Production Go edits in
`internal/myce` always include `TestWireSchemas`, since the reflected Job/Spec
schema includes nested types across that package. Edits to the shared memory
writers `attempt_memory.go`, `memory_learning.go`, `memory_envelope.go`,
`gpu_host_memory.go`, `capability_placement.go`, `work_observation.go` and
`queue.go` also include the tests in `process_memory_test.go` and
`attempt_memory_facts_test.go`: their private-method consumers escape direct
symbol matching (#493). This explicit consumer set does not follow transitive
calls or expand to a whole package. This is a textual heuristic,
not a dependency analyser. Review the printed selection: shared helpers and
indirect callers can need `full`. If any Go file has no related tests, quick
stops with a message requesting `full`; it never expands to a whole package.
Quick is feedback for the selected change, not evidence that every test passed.
Module dependency changes also require an explicit `full`. The selector
regression can be run with `python3 scripts/test-gate-related.py`.

Run `./scripts/gate.sh full` once for a batch before deployment. It runs the lean
full race suite once, including dev checkpoint/resume, model and enrollment
end-to-end tests. It does not repeat those packages in a second race pair or a
separate dev run. For an authorized urgent hotfix: run quick, deploy, run full
afterwards, and fix forward if full finds a problem. This workflow does not grant
fleet/deployment authorization.

Quick and full have no overall wall deadline. Each Go package runs under its
own output-progress watchdog, keyed by package, exact selected test expression,
OS/architecture, Go version and available/assigned CPU count. Timings live beside
Go's execution-host build cache (`mycelium-gate-timings`), shared across checkouts
on that host, and every invocation appends a sanitized record to
`gate-timings.jsonl`. Fleet profiles return that report with their result marker.
A new selection/toolchain/CPU configuration learns independently; a small quick
selection cannot shorten a full-package deadline.

A learned package stalls after **three times its expected package duration with
no output**. Expected duration is the latest completed successful observation
for that host/configuration/selection. Three intervals allow the normal expected
completion plus two missed completions for transient contention; the cost is up
to three expected durations to detect a silent hang. This is an operational
tolerance, not a statistical confidence claim or a wall-time limit. Every output
chunk resets only that package's silence clock. A run can exceed any previous
wall duration while progressing, and sibling output cannot hide its hang.
The watchdog starts at Go's JSON package-start event, after compilation: a warm
cache measurement must not kill cold compilation. Build/vet/compatibility steps
record elapsed time but do not infer silent deadlines from cache-dependent work.

Cold packages retain `go test -timeout=1h` as the finite toolchain backstop.
The prior ad-hoc 15-minute value would already truncate the measured 1,708s full
package; one hour allows over twice that observed duration for first-run learning.
This cold backstop can still reject an unprecedented, longer silent test; it is
an explicit bootstrap tradeoff, not a learned throughput claim. Subsequent runs
use `-timeout=0` and measured progress deadlines. All runs record elapsed time,
longest output gap and status, including failed/stalled runs; only successful,
uncensored observations train the cache. Regressions with fake time run via
`python3 scripts/test-gate-progress.py`; custody regressions run via
`python3 scripts/test-gate-budget.py`.

Each package has a unique marker and private HOME/TMPDIR. On a stall it stops
and verifies its own processes before releasing its package slot; an unconfirmed
package cleanup stops the gate. Both gate modes also stop all remaining owned
fixtures before removing scratch. Cleanup retains
the existing unique run marker, isolated HOME/TMPDIR, ancestry and OS file-handle
checks (macOS `libproc`, Linux `/proc`), PID start identities and individual
signals. TERM gets five seconds for fixture checkpoint handlers, then KILL and
one second for asynchronous exit verification; those custody bounds do not
limit useful gate work. The native handle probe retains its five-second isolation
bound and 0.2s post-KILL reap bound, so a stuck kernel probe retains scratch rather
than blocking cleanup forever. Any inventory/exit uncertainty retains scratch.

Go 1.25.6, a native macOS arm64 toolchain (for the CGO release target), `uv`
and Python 3 are current laptop prerequisites. Install prerequisites separately:
the gate runs offline and never downloads tools or modules. Seed dependencies
with your usual `go mod download` before running a first gate. The gate shares
Go's normal build and module caches (Go locks them safely across concurrent
runs); only HOME, TMPDIR and GOPATH point inside an ignored `.gate/run.*`
workspace, which is deleted on exit because it holds only rebuildable test state.
Failure logs belong in an
ignored `.reports/` folder (for example redirect stdout/stderr there).

On macOS, the full supervisor holds one of two cross-checkout filesystem locks
for its entire lifetime, independent of package commands or test expressions. It also
accounts for older standalone full race commands before entering a slot. The
slot also has a `mycelium-full-gate-N.custody` record beside its lock in
`GOCACHE`, containing the run marker and scratch directory. Only verified fixture
exit removes that record; failed inventory or supervisor death retains occupancy
after the OS releases the lock. Recovery requires verifying the recorded run's
fixtures have exited before removing its custody record. Never clear another
owner's record merely to admit more work.
The
two-suite boundary preserves the repository's observed Mac safety rule: more
concurrent full suites caused false failures. Linux fleet gates rely on the
queue's CPU and learned-memory admission instead of this Mac-only count; waiting
inside an already-admitted Linux job would strand its allocated resources. Do not
terminate another worker's tests. Build and test concurrency follows the execution host's affinity, Linux CPU
quota and optional `GOMAXPROCS` contract, rounded down to whole runnable cores
(with a one-core minimum). Profiles retain their explicit two-core reservation
and matching `GOMAXPROCS=2` contract. Each test command takes one build slot;
quick and full overlap no more packages than available cores, each under its
own selection and watchdog. Test parallelism uses that same CPU count.
Tests that mutate process environment remain serial.
`GORACE=atexit_sleep_ms=0` omits the race runtime's artificial one-second delay
on each helper-process exit; product code and assertions remain race-instrumented
and tests still join their workers. The external fake Docker CLI is built once
with `go test -c -race=false` and shared by fixture calls. It only simulates the
external daemon protocol; it never runs product tests. The worker and Docker
adapter execute in the parent under `-race`. Vet runs explicitly once, so test commands use
`-vet=off`. Ambient `MYCE_*` settings (including real-Docker opt-ins) are cleared.
The package fixtures isolate local storage and substitute synthetic GPU and daemon
probes; they don't need fleet or NAS access. HTTP integration tests use loopback
servers and local temporary directories, never external storage.

The `gate-full` execution profile submits the full gate as a Mycelium CPU job;
see [agent offload](adapter.md). Linux runs the Linux builds, vet and full
race/dev suite. The native Darwin CGO build explicitly skips there and remains
a separate macOS check; the Linux gate is not proof of that release target.

`myce run --profile wide-reads-benchmark` runs `BenchmarkStatusReadScale` on a
fleet CPU worker. It measures cold default/full/inspection HTTP reads (including
JSON) beside real SQLite worker check-ins at the one-second heartbeat cadence.
The tiny fixture has 16 retained jobs, four queued jobs, 16 curves and three
workers; the large fixture has 6,500 retained constrained executions, 110 queued
jobs, 1,400 curves and five workers. All queue decisions use a fixed injected
Clock. Results report read latency and read/check-in mutex hold separately.
Six measured reads per endpoint make the reported p99 the sample maximum,
not a production tail estimate. Compare accepted outputs from identical profile
versions and CPU placement; natural fleet latency remains release-owner proof.

### Release upgrade compatibility

Full also runs `python3 scripts/upgrade-compat.py run`. This builds race-instrumented
process fixtures from the candidate production sources and the deployed predecessor,
then runs real SQLite queues and worker startup loops over authenticated loopback
HTTP. Hardware probes and service managers are synthetic; no installed services,
fleet authority, native GPUs, or production databases are involved. This is test
proof, not automatic rollback machinery (MY-03) or native service qualification.
The full gate first runs `python3 scripts/test-upgrade-compat.py`, covering
termination of detached build commands and preservation of queue teardown failures
when a worker's protocol refusal is expected. SIGTERM/SIGHUP unwind the supervisor
and reap its owned commands before temporary state is removed.

The predecessor is s071 (`v3.1.0-rc.20261104.s071.100840b`), pinned to published
commit `100840b4094d77bbc69a80d807dba58722db877c`. No v3 release tags were published;
the harness uses this verified release commit rather than inventing a tag or
silently substituting current code. Update the pin/version when qualifying a new
release predecessor. The second baseline is the inventory-supporting #790 merge,
`8b2585e1d7edaff376e96430bc614daa9f788342`, which still sends full reports and
strictly rejects the newer delta fields. The profile prepares both Git sources and module dependencies
before the offline gate and declares `needs.network: true` for that preparation;
a local checkout containing both commits needs no network.
For a fresh offline source archive, first run:

```sh
python3 scripts/upgrade-compat.py prepare
python3 scripts/upgrade-compat.py run
```

Completed binaries are cached beneath Go's build cache, keyed by production source
bytes, predecessor SHA, fixture bytes, version, platform and Go toolchain. Historical
production code is never patched to make a pairing pass. The candidate carries the
incident's fix-forward pre-release suffix, `v3.1.0-rc.20261104.s072.abce497.1`.

The fixture uses the sanitized retained read-only capture from 2026-11-05
11:46:41 UTC: 6,286 retained jobs, 1,655 work curves and 1,384 storage observations.
The representative large worker had 27 ready environments, 2,381 native tools,
2,381–2,526 tools per inventory key, 67,972 repeated tool-name occurrences and
900,438 compact inventory JSON bytes; 76 distinct environments existed across
retained reports. The incident-size case uses 40 ready environments, 2,400 native
tools and two synthetic device slots, asserting at least 1,871,955 transmitted
bytes so small dev inventories cannot hide #789 again. Queue history and curves
remain present while the worker joins and sends a second accepted check-in.

Pairings cover candidate/candidate and predecessor-worker/candidate-queue. s071
predates tool inventory and rejects the candidate's new fields in its strict JSON
decoder; that reverse direction is an explicitly checked unsupported boundary.
The #790 worker joins the candidate with full inventory on every heartbeat, and
the candidate joins #790 without sending unadvertised delta fields. Another
journey keeps one candidate worker alive across queue replacement: first reopen
the candidate's disposable database, require its hash-only check-in to receive
an explicit full request and verify the refresh; then start #790 with a fresh
disposable database on the same loopback endpoint and require legacy full retry
and subsequent accepted full heartbeats. This tests worker fallback, without
claiming that an old queue can read a new queue's persisted data. Session and
keyed-attempt protocol must remain unchanged throughout.
The downgrade journey makes the candidate installer write an automatic-cache
config, runs the actual s071 CLI installer with `--config ... --dry-run`, then
starts an s071 worker from the same config and requires accepted check-ins.
It does not claim explicit-cache metadata is readable by s071 (#790).

The suite has a ten-minute total budget, including cold builds. Each build
command has a three-minute deadline, each fixture process a 45-second
test deadline, each join a 30-second deadline, and teardown eight seconds before
killing and reporting a stuck owned fixture. These bound broken-build and startup
costs without changing product deadlines. All cases run even after a failure.
To reproduce the two original incidents without modifying a checkout:

```sh
python3 scripts/upgrade-compat.py run --candidate abce497c
```

That command must fail both same-source startup and saved-config downgrade on the
original release. A passing current run requires the fixes from #790 and the
negotiated inventory protocol from #791.

The committed `release-build` profile reuses this environment for Linux release
artifacts, with literal version and commit arguments and build receipts. The
single fleet build command and local signed Darwin step are documented in
[release operations](operations.md#publish-and-approve-an-onboarding-release).
Release script regressions run inside `TestReleaseBuildProfile`; their tools
are fake and never access a signing key or keychain.

The `gate-quick` profile runs the quick gate the same way. A submitted source
archive has no Git history, so compute the related-test selection locally and
send it as a job input; the job runs `gate.sh quick --selection FILE` in place
of the Git diff:

```sh
mkdir -p .reports
python3 scripts/gate-related.py "$(git merge-base HEAD origin/main)" > .reports/gate-selection.tsv &&
  myce run --profile gate-quick --input gate-selection.tsv=.reports/gate-selection.tsv
```

Commit first: the source is `HEAD`, while the selection also sees uncommitted
files. A selection error (changed `go.mod`, unmapped code) means run `gate-full`;
the `&&` keeps a failed selection's empty file from submitting a test-free run.
Quick and full use the same execution-host progress supervision. Gate profiles
and converted adapter entries use `memory: auto` for quick and Darwin full,
allowing the queue's comparable
accepted unconstrained evidence to supply reservations, and exploration where
that evidence is absent or constrained. Quick uses `memory_class: per_item` so
its submitted selection participates in memory comparability; a smoke run cannot
teach a larger selection's envelope. Source changes still reuse the profile's
comparable work identity. After a full run inspect memory eligibility, ceiling
events and swap; a constrained peak is a lower bound, not a sizing sample.

The following is historical evidence from #766/#767, which initially retained
explicit Linux bounds after an auto candidate hit its cgroup ceiling. Linux
full retains main’s measured 6 GiB declaration pending separate memory-policy
qualification; adaptive output supervision does not change that bound.

Read-only job GET inspection on 2026-11-04 found these successful final attempts:

- Four recent Linux gates had total cgroup peaks 3.467369–3.548534 GiB,
  resident peaks 2.379230–2.456242 GiB and non-reclaimable peaks
  2.423759–2.508663 GiB. All four were learning-eligible, with zero cgroup
  memory events and zero swap. Their `resources` reported a learned envelope
  of 3.276315 GiB with 13–17 accepted observations, versus 7.125 GiB charged
  from the old declaration. Auto adds ordinary launch slack to the learned
  figure; it does not substitute a raw resident peak for a cgroup ceiling.
- Four recent Darwin gates reported 3.827286 GiB learned with 23–25 accepted
  observations, versus 4.125 GiB charged. Darwin RSS is sampled and admission
  only; Linux uses enforced cgroups. These are different measurements.
- Ten additional retained successful Linux attempts had total cgroup peaks
  3.181683–4.244785 GiB and non-reclaimable peaks 2.274590–2.476593 GiB.
  Their learned envelopes reached 3.767576 GiB with 48–55 accepted samples.
  Auto keeps the queue's measured range/25% margin and tracks comparable
  configuration changes; a fixed lower declaration would discard that adaptation.

Linux auto validation job `2686318521495ce7787d3a2269635f8a` passed in 1495s,
with 410.27s submission-to-command-start wait, but was constrained: its learned
3.276315 GiB + 0.125 GiB slack ceiling recorded 160 `max` events. Total cgroup
peak was 3.401348 GiB, non-reclaimable peak 2.427307 GiB, with zero swap/OOM.
This is a lower bound, not a new accepted sizing sample. #767 kept an explicit Linux
bound per #559: recent unconstrained total max 4.244785 GiB plus
max(range 1.063103, 25% of max 1.061196) = 5.307888 GiB, rounded to 6 GiB.
Its cost is 1.755215 GiB above that recent maximum; it saves 1 GiB per Linux
full gate versus the previous declaration. #559's older 4.472893 GiB maximum
also fits below 6 GiB with 34% headroom, though its wider historical range
would still round to 7 GiB. The revised bound uses the newer retained samples;
it is not proof against all future suite growth. Darwin has no corresponding
cgroup ceiling event and keeps its accepted RSS-based auto envelope.
Unknown configurations explore; inspect envelope basis, learning
reason, memory events and swap after changes.

A bounded recent-history snapshot contained 36 successful Linux full gates:
submission-to-command-start wait was median 96.65s, maximum 2082.78s (34.71m).
Fourteen successful Darwin gates waited median 36.65s, maximum 4052.41s (67.54m).
These include staging and admission, not exclusively RAM wait. Failed and
cancelled jobs are excluded; unfinished waits are not zero. The task's incident
report separately observed RAM waits of at least 30m.

A retained failed quick run built/vetted in 22s, finished 116 CLI tests in
128.095s, and had completed 286 core tests before the 295s supervisor cutoff.
A recent full run spent 482.033s in CLI and 1708.196s in core tests; the slowest
journeys were fleet-upgrade (149.46s) and enrollment (135.78s). Removing a broad
selection's unrelated main-only edits avoids executing these journeys for work
that did not touch them; genuinely related journeys remain required.

A paired fleet measurement on the same frozen diverged PR source reduced quick
wall time from 161s to 119s (42s, 26.1%). The old moving-main comparison selected
77 CLI + 125 core tests; merge-base selected 57 + 72, excluding only main-only
changes. CLI package wall fell from 124.847s to 83.215s and core from 72.136s
to 52.121s. Both accepted result markers were fetched and verified. Shared fleet
occupancy was uncontrolled, so the timing difference is an observed pair, not
an isolated scheduler or sustained-throughput benchmark. The reported incident's
three 296s timeouts were a different selection. This patch's validation quick
passed in 57s; that smaller selection is not the before/after comparator.

CI and PR gates retain the same `test` class and normal queue fairness. Reducing
the Linux declaration using unconstrained total peaks is the first intervention;
raising PR class would misstate urgency, and serializing CI would move backlog
into Actions. No runner services or global queue policy change here. Reconsider
submission concurrency only if measured waits remain high after right-sizing.

For ad-hoc suites, `myce run --profile go-test -- -race ./...` defaults to the
`-timeout 15m` backstop; the gate itself uses the learning policy above. Arguments after `--` go to `go test`; an
explicit `-timeout 20m` overrides that default. A profile selects **one** eligible
target from its `targets`, rather than running a platform matrix. To require a
platform, copy `.mycelium/profiles/go-test.yaml` into an ignored path such as
`.reports/go-test-darwin.yaml` and keep only:

```yaml
targets:
  - {os: darwin, arch: arm64, platform: cpu}
```

Retain the other profile fields, then submit with
`myce run --profile .reports/go-test-darwin.yaml -- -race ./...` from the repo root.
Use separate submissions with distinct target lists for cross-platform proof.

## Fan-out burst acceptance

Run the real `myce dev` runtime burst separately from race instrumentation:

```sh
GOMAXPROCS=4 go test ./internal/myce -run '^TestDevFanOutBurst62RealTime$' -count=1 -v
```

This laptop test uses real HTTP, SQLite, CPU commands and heartbeats, with
3,000 retained jobs (400 fan-out parents), concurrent status reads, and 62
simultaneous keyed parent submissions. Nearest-rank p99 is the maximum of 62
samples; all acknowledgements must be below 500ms. A running production command
spans the burst and six subsequent seconds; no attempt may incur a loss and
sampled accepted-heartbeat age must stay below the normal 5s quiet threshold.
The synthetic extension explicitly uses the supported unsandboxed mode; native
sandbox qualification remains separate. No fleet connection or GPU is needed.
This performance test is excluded from `-race` builds: instrumented service cost
is not release performance. Fake-clock loss and scope regressions still run
under race in quick/full gates. Run this acceptance alongside quick when changing
fan-out/control-path performance, and before a hotfix claims the burst target.

For queue observation/check-in contention, also run:

```sh
GOMAXPROCS=4 go test ./internal/myce -run '^TestStatusReadCheckinContention$' -count=1 -v
```

This separate `!race` regression measures 100 real SQLite check-ins alongside
62 concurrent readers (queued receipts and uncached bounded status), 5,000
retained jobs including 400 fan-out parents, 200 queued jobs and 1,000 curves.
Nearest-rank p99 must remain below 500ms, half the default one-second heartbeat
interval. A running production attempt must retain its original attempt with no
losses. Queue decisions use a fixed injected clock; this is a laptop contention
bound, while fake-clock contact/loss tests cover elapsed-time decisions under
race. It does not establish fleet capacity or native release qualification.

For retained-catalog transaction cost, run:

```sh
GOMAXPROCS=2 go test ./internal/myce -run '^$' -bench '^BenchmarkQueue(RecordsFleetCheckin|CatalogTransaction)$' -benchtime=30x -count=3
```

Both fixtures retain 6,500 constrained executions, 110 queued jobs and 1,400
curves with five workers. They report mutex hold, process CPU and allocations
for real SQLite check-ins and otherwise unchanged transactions. Queue time is
injected and fixed. Compare the same fixture/toolchain on both revisions;
these measurements do not establish natural fleet latency or worker freshness.

On macOS, the bounded helper reads cwd, executable and standard-descriptor paths
from `libproc` without enumerating or statting mounted filesystems. Apple's lsof
has no network-mount exclusion and even scratch-scoped scans can block on an
unrelated mount. Native probe errors still retain scratch. Run cleanup regressions
with `python3 scripts/test-gate-budget.py`.

## Automatic main full gates

The [`main-full` workflow](../.github/workflows/main-full.yml) runs only on pushes
to `main`, once for each merge result (or direct main fix). There are no PR,
manual-dispatch or schedule triggers. It selects `[self-hosted, mycelium-ci]`;
GitHub-hosted runners are never used. Do not add workflow concurrency grouping:
GitHub may replace a pending run in a group, losing proof for an intervening merge.
Three repository-scoped runners accept separate merges concurrently without
cancelling older runs. Each runner still waits for its own two Mycelium jobs and
verifies their accepted results. A backlog waits in Actions; no model callbacks,
polls or wakes are needed.

The throughput change in [#751](https://github.com/1kuna/mycelium/issues/751)
adds two lightweight submitter-role runner services to the fleet pool.
The T3 host and hosts in a production hold are
excluded from new runner placement. Both added services use the same repository
scope, `mycelium-ci` label, clean environment and low-priority settings described
below, with separate installation and work directories. No full suite runs in a
runner service; Mycelium continues to admit and place all gate work.

Measured on 2026-11-04, six recent completed Actions jobs occupied the single
runner for 25.67, 26.68, 27.63, 34.18, 37.08 and 44.92 minutes (median 30.91;
mean 32.69), excluding their Actions queue wait. These are runs `41751005700`,
`41753870854`, `41755027580`, `41755730952`, `41756251565` and `41757108412`;
both successes and failures count toward occupied capacity. Sixteen more runs
were queued at inspection. Three runner slots increase the median-based runner
capacity from 1.94 to 5.82 results/hour (mean-based: 1.84 to 5.51). This is a
capacity estimate at the observed service times, not a guarantee: shared fleet
CPU/RAM contention can still extend a run, and an existing burst takes time to
drain. The target is roughly an hour per new merge after that backlog drains.

Adding runners is the smallest change because the existing submitter already
waits cheaply through `myce wait`. Ending the workflow after submission would
lose its verified per-merge result unless a separate completion publisher and
credential were added. Subsequent measurements now justify the smaller Linux
full-gate declaration documented above; the initial runner-capacity estimate
used the previous declarations. Cost: two persistent runner installations, workspaces and small
submitter/wait processes, plus overlapping gate demand within ordinary Mycelium
admission; gate resources, test coverage and result acceptance stay the same.

**Before merging, require the PR's exact-head fleet full gate and independent
review**, then inspect main's workflow records and latest *completed* `main-full`
run, including its head SHA. For a docs-only change, a passing exact-head fleet
quick gate is sufficient in place of full.

```sh
gh api repos/1kuna/mycelium/commits/main --jq .sha
gh run list --repo 1kuna/mycelium --workflow main-full.yml --branch main --limit 20 \
  --json headSha,status,conclusion,url
gh run list --repo 1kuna/mycelium --workflow main-full.yml --branch main \
  --status completed --limit 1 --json headSha,status,conclusion,url
```

Queued or running main runs do not block a merge, and completed proof need not
match current main: shared fleet gates can lag behind frequent merges.
The next merging worker inspects a red completed run's platform summaries,
Mycelium job IDs and logs, then fixes its failures forward before unrelated
merges. A reviewed fix with its own passing gate may merge while main is red;
if the fix is already merged, record the failing test and the merged fix as
resolution evidence instead of waiting for the backlog to reach it. A merge with
no workflow run at all is a missing-run defect for that worker to repair, not
passing evidence. Inspect cancelled runs for interrupted proof as well.

Source: the maintainer's global CI rule, relayed in the 2026-11-04 merge-guidance
assignment. CI remains once per merge on self-hosted fleet runners; agents do
not wait on, poll or wake for main CI. Automatic main proof supplements each
PR's gate and the release full gate. Use the run's job summary to inspect both
platform outcomes and Mycelium job IDs.
The workflow's own check reports success/failure; no commit-status credential or
personal access token is needed.

[`scripts/actions-full.py`](../scripts/actions-full.py) freezes the corresponding
platform full-gate profile to the exact push SHA and submits it once for Darwin arm64
and once for Linux, eligible on either amd64 or arm64. CI preserves the profile's
targets, resources, environment, output contract and check. Linux is one chosen
target per merge, not an architecture matrix. The temporary architecture-wide
exclusion from [#527](https://github.com/1kuna/mycelium/issues/527) is removed after
the bounded native gate-like I/O measurement described in
[queue disk contention](operations.md#queue-disk-contention-527). The ordinary
Linux full profile and converted adapter already included both architectures.
Their measured 6 GiB declaration and Darwin's `memory: auto` remain as documented
above; #767 did not establish an unconstrained Linux auto envelope.
There are no host pins; Mycelium chooses each worker and
provides the existing `mycelium-gate` environment. Both submissions happen
before `myce wait` waits for all jobs, even if one fails. Successful jobs also
require `myce result` to download the accepted, verified `gate-result.txt` marker.
The runner does only source transport, waiting and small-result verification;
full suites never run on it directly. A profile exit 78 with `no_qualified_target`
is a qualification/total-allowance refusal, not a busy-worker indication. Busy
qualified hosts accept queued jobs and `myce wait` waits for them. CI reports
host-memory need and allowance numerically for that refusal; other qualification
details remain in the private receipt. It does not retry an impossible allowance
or weaken the learned memory envelope. Darwin proves the native CGO target; Linux
proves its native builds and full race/dev suite.

The deterministic `main-ci:SHA:OS` keys adopt the same jobs on a workflow rerun,
including after an uncertain submit. A failed suite is not automatically rerun.
The CLI handles immutable source uploads, idempotent submission and reconnects;
there is no custom watcher, cursor or status publisher. An interrupted workflow
leaves submitted fleet jobs running; rerun it to adopt those jobs. No custom
wall deadline kills healthy full suites. GitHub's own runner/job service limits
still apply. Inspect failures privately with `myce logs JOB_ID`; public logs and
the summary contain only job IDs, fixed states and exit codes, never raw CLI
receipts, remote reasons or host paths.

A submission refusal or outage is an **infrastructure failure**, distinct from a
submitted suite reporting `failed`. Its platform row says `infrastructure failure`
and the workflow emits a `CI infrastructure failure` error annotation: that
platform has no verified suite result. Submission may have been accepted despite
a lost reply. Resolve the infrastructure problem privately, then
rerun the same workflow/merge; the deterministic keys recover any jobs that were
accepted before the outage. There is no automatic retry for a failed suite.

### Public repository and private authority

The runner is registered to **this repository**, not an organization-wide pool.
Only reviewed code merged to main is eligible: the workflow has neither
`pull_request` nor `pull_request_target`, and fork/feature pushes cannot trigger
this repository's runner. Fork copies cannot address the upstream runner.
Under Settings → Actions → General, require approval for **all outside
contributors** (`approval_policy: all_external_contributors`). Keep that setting
when changing workflows. Repository administrators and main writers remain
trusted: review any workflow or submitter change before merging it.

The workflow grants only `contents: read` to the built-in `GITHUB_TOKEN` and
pins checkout to an immutable action SHA, with `persist-credentials: false`.
The service environment contains **no fleet token**. `myce` reads the host's
existing private installed worker/queue configuration and authority files;
credentials are not copied into the workspace, workflow, Actions secrets or
step environment. Start/configure the runner with a clean environment and no
`MYCE_*`, `GH_TOKEN` or `GITHUB_TOKEN` variables. Launch the service through
`env -i` with explicit HOME, USER, LOGNAME, PATH and locale settings, so it cannot
inherit credentials from the user service manager either. The submitter gives CLI children
only HOME, PATH, temporary/locale settings and GitHub's process-cleanup marker;
no GitHub token, fleet token or notifier is inherited. Profiles contain only
the gate's authored environment. Mycelium's worker also filters `MYCE_TOKEN`
from payload environments. This is a trusted-main service, not an isolation
boundary against a malicious main writer or another process under the host user.

### Runner operation

Install the standard GitHub Actions runner release for the host OS/architecture
in a hidden, stable directory. Before downloading, check free disk, memory
pressure, load and active work; this submitter should be light enough for an
always-on control host. Verify the release archive's SHA-256 against GitHub's
published asset digest. Obtain a short-lived registration token using existing
`gh` authentication (`POST repos/1kuna/mycelium/actions/runners/registration-token`)
and pass it directly to `config.sh` without printing or retaining it. Register
with repository URL `https://github.com/1kuna/mycelium`, label `mycelium-ci` and a
clear name such as `mycelium-ci-control`. Registration is persistent; no PAT or
registration token stays in a service definition. Leave automatic runner updates
enabled so they update the same installation in place.

Install/start the runner with its standard `svc.sh` user-service integration
on macOS (or equivalent user service on Linux). Use background/low-priority
service settings, a private installation directory and a minimal PATH containing
installed `myce`, Python 3 and Git. Keep the service identity/path stable and
avoid GUI windows or notifications. Use a generic private work directory outside
the personal home path, since checkout prints its workspace in public logs.
Confirm its repository scope and `online`
status using `gh api repos/1kuna/mycelium/actions/runners`, and inspect the local
service/process without dumping its environment or credential files.

On Linux, use separate systemd user units running the standard
`bin/runsvc.sh`, with `Nice=10`, reduced CPU/I/O weights and the same clean
environment. They do not depend on, restart or reconfigure the queue or worker
services. Before adding `mycelium-ci` to a new runner, verify installed-authority
queue access **from its service context**, not just from an SSH shell. These
units run a read-only installed-authority status check before starting. A failed
check prevents runner startup. On macOS, SSH access can succeed while a launchd
service gets `no route to host` under Local Network privacy; do not bypass that
policy or enable CI admission on such a runner. Use an authorized Linux fleet
host or leave approval to the host owner. Repository registration and an `online`
runner alone do not prove queue reachability.

Generated platform profiles and downloaded result markers live in job temp and
are deleted on exit. Only small CLI recovery receipts remain in the checkout;
GitHub terminates job-owned observers at job completion, then the next checkout
cleans old workspace state. Source-upload staging is removed by the CLI after
confirmed submission. Runner diagnostic logs stay private; do not upload them
as public artifacts. Use normal queue artifact retention for fleet outputs.

Validate the submitter offline with `python3 scripts/test-actions-full.py` and
the quick gate. After independent review and merge, verify the push-to-main run
uses the registered self-hosted runner and reports both accepted platform results
before calling activation complete. No full suite runs on a PR push.

## Native acceptance checklist

Use an explicitly authorized maintenance window. Laptop fixtures prove queue,
API and command mechanics. Native GPU, daemon, manager, memory and boot behavior
need separate evidence. Keep host identity, paths, payloads and secrets private.

> **Before queue cutover:** preservation checks must use
> `/api/status?detail=full` or complete `jobs=all` pagination, including private
> release-script copies. Default status (#439) can omit older completed jobs;
> omissions cannot prove preservation or release. Existing complete
> `/api/inspection` collectors remain supported. See
> [rolling fleet upgrades](operations.md#rolling-fleet-upgrades).

Record binary SHA/digest, config and secret references, device selection, manager
state and a recoverable rollback snapshot. Preserve current work and competing
owners. Verify NAS mount identity or verified file transport; qualify actual
runtime/model identity and simultaneous memory/headroom independently.

| Case | Required evidence |
| --- | --- |
| Install/redeploy | Saved settings, GPU environment and secret references preserved; inject failure and verify artifact/mode plus actual manager rollback. No local start gate or timer. |
| Queue maintenance | Both devices stop admission; drain preserves Started work; disable-stop retains exact custody until cleanup. Cancellation stays independent. |
| Freshness/outage | Missing/stale workers and queue outage cannot yield healthy quiescence or authorize new payloads. Reconnect never treats omission as release. |
| Reboot | Disabled policy survives; prior session/attempt evidence cannot authorize new work. Exact old-session cleanup and fresh worker reports precede resume. |
| Queue restart | Keyed lost-response replay returns the same job, changed specs conflict, cancellation reservations never run, stale protocols refuse and successful jobs do not rerun. |
| Runtime/resources | Verify native groups, Docker main/sidecar/network cleanup, pending daemon operations and RAM/cgroup limits. Timeout cannot settle custody. |

Bound each observation and record command exits and effects, release/rollback
receipts and unrun checks. Stop on the first failed native gate and preserve the
recovery snapshot. An actual reboot or service interruption needs its own task
authorization. Finish drained/disabled unless the task explicitly authorizes
resume. There is no offline rest certificate.

Use `myce measure` for the exact selected device/catalog, including representative
overlapping warmups. Qualification stays UNQUALIFIED until native simultaneous
peaks, runtime overhead, sidecars and OS headroom are measured. For private models
with `logs: none`, verify actual null stdout/stderr and Docker log-driver behavior,
including recovery; fake argv tests cannot qualify the daemon or journald.

## Pressure and status agreement

For #158, the laptop controlled-heartbeat fixture proves low live RAM holds
new work with `host_memory_headroom`, exposes live RAM separately from reserved
RAM, refuses the same need at worker launch, and admits it after headroom
recovers. It does not qualify OS PSI, macOS pressure, swap rate or live CPU use;
those remain unknown until implemented and measured.

Native follow-up owner: admission/status maintainer, in an explicitly authorized
maintenance window, one fleet check at a time. Confirm any operational holds
have been released before testing. This checklist does not schedule or authorize
a load run.

1. Confirm no competing native test or owner workload, current binary identity,
   free disk, memory, load and the worker's enforcement capability. Use a fresh
   explicit key; preserve current work and tokens privately.
2. Capture status JSON and the OS's available RAM/pressure readings at baseline.
   Use an existing bounded workload with an approved measured RAM need; avoid
   synthetic host exhaustion. A controlled memory-source fixture is sufficient
   for laptop proof but must not be presented as native telemetry evidence.
3. While available headroom is below that need, capture status, its typed queued
   hold, and evidence that no new payload starts. For a late change after
   assignment, capture the worker launch refusal and retry accounting. Compare
   declared need plus slack with the same measured signal at each fence.
4. After pressure clears, verify the same keyed job progresses, verify its
   accepted output and release, and preserve a short sanitized evidence ledger.
   Record unimplemented pressure signals explicitly as unknown. Keep #158 open
   for the OS-pressure/live-CPU/swap coverage and any unrun native checks.

## Laptop component probes

Use the real queue, API and command runner with fake GPU inventory. Bare
`myce dev` is a long-running server. The bounded standalone test starts an exact
binary with isolated HOME/state, authenticates, verifies one command's file
effect and exact release, drains through queue maintenance, then joins the child
and confirms its listener closed:

```sh
go build -o .reports/myce ./cmd/myce
MYCE_DEV_SMOKE_BINARY="$PWD/.reports/myce" go test -race -count=3 ./cmd/myce -run '^TestDevStandaloneSmoke$'
```

`TestMaintenance*` covers two-device admission/Started, drain versus stop,
restart/rejoin, stale or missing reports, unknown native/engine/Docker custody,
pending creates, cancellation and memory policy/debt. Queue decisions use injected
time. `TestDevProbeQueueRestart` exercises actual API outage, SQLite reopen,
lost-response keyed replay and cancellation without duplicate command effects.
`TestDevProbeRebootResume` restarts the real dev queue/workers with durable disabled
policy and requires fresh reports before quiescence. Process restart models the
protocol boundary; it does not claim an actual machine reboot.

Keep Docker stop/recovery/result, cancellation/all-attempt release, engine and
model recovery, RAM/cgroup/sidecar limits, files, installer rollback/GPU environment
and privacy suites. There are no local CAS, root-child or rest verdict matrices.
Record command exits; the [native test plan](development.md) separately
requires authorized daemon, memory, manager, GPU and reboot evidence.

`MachineProbe.Accelerators` is the shared onboarding, worker refresh and launch
probe. Its PCI snapshot supplies vendor IDs and per-device topology; NVIDIA UUID
and Level Zero enumeration supply runtime identities. Level Zero also measures
Intel device memory. The optional SYCL verbose query is memory telemetry only;
its display names never establish a device or its vendor.

A runtime is required only for a family present in PCI facts. For example, a
NVIDIA render node does not require an Intel runtime. Typed family failures retain
the healthy family's devices; unreadable or ambiguous endpoint facts withhold
only the affected component. Onboarding returns the healthy partial plan alongside
the diagnostic, while refusing to publish an incomplete installation. CUDA `N/A`
is unavailable telemetry, never evidence of unified topology.

`TestConsolidated*` covers B70 plus BMC/failed NVIDIA tooling, GB10 without Intel,
Apple without PCI sysfs, mixed and integrated devices, endpoint access failures,
and explicit discrete-memory budgets. `TestConsolidatedSparkAbsentXPUHealthy`
projects the shared probe through a worker and queue and requires healthy host
status. These hermetic fixtures supplement the separate native preflight gate.

## Onboarding benchmark

Throughput measurement is opt-in. Pass `myce join --onboarding-benchmark` to
measure before admission, or use the explicit `myce benchmark` command below.
Default joins do not run or wait for a benchmark and leave its cache untouched.
They report hardware facts and use existing placement fallbacks when throughput
is unknown. Results from an enabled benchmark are in
`capability_profile.benchmark` in the status API, the CLI capability summary, and
an additive benchmark detail in the status page. Fake inventory in `myce dev`
does not load the host; tests inject measurements instead.

The benchmark uses at most **nine minutes**, including waiting for permission to
use a daily machine. Each resource gets at most 60 seconds and runs serially:

- CPU: single-thread SHA-256 on an 8 MiB buffer, for about 250 ms.
- Memory: copies between two 64 MiB buffers for about 250 ms.
- Disk: write, sync and read 16 MiB in the worker's own state folder. This is a
  short filesystem measurement; cached reads are included, not sustained media speed.
- Network: download and hash-verify an 8 MiB synthetic object from the actual
  file store. Upload prepares the object outside the download timer. One fixed
  content hash is reused, so it occupies at most 8 MiB in the store. This measures
  end-to-end HTTP delivery including hashing, not raw link capacity.
- Accelerator: only qualified extensions with a timed reference command run.
  PyTorch repeats its existing small pointwise native-reference computation for
  two seconds, synchronizing each result through the host and checking stability.
  This measures short reference work, not peak matmul FLOPS. llama.cpp replays its
  unchanged tiny-model prompt and reports its completed token-evaluation rate,
  excluding model loading and prompt evaluation. Both final outputs must still
  match their frozen native references. No CPU fallback, new baseline, or tolerance
  change is introduced.

Extension setup, downloads and builds are explicit preparation and are outside
this short measurement. The starter versions with timed references are
`pytorch-script@0.2.4` and `llama-cpp@0.1.1`. Existing frozen versions keep working;
without a timed command their throughput stays unknown. An optional extension
`benchmark` argv receives the same check environment plus `MYCE_BENCHMARK_OUTPUT`.
It writes the actual reference output and a small timing JSON document:

```json
{"units": 100, "seconds": 2, "unit": "references/s", "reference_units": 1}
```

`units` counts completed work; `reference_units` counts units in one reference.
Supported units are `references/s` and `tokens/s`. Both counts and duration must
be positive and finite. Mycelium validates the output against the extension's
bundled native baseline; a timing failure cannot qualify an extension or widen
its tolerance.

Daily use follows the existing owner, power, heat and fresh-signal policy:
five idle minutes, external power for laptops and known safe heat. Owner return
or unsafe/unknown signals cancel a running measurement. The benchmark runs before
job admission and model loads, never over a recovered attempt. A per-spool benchmark lock prevents duplicate measurements in one worker
spool. Coordinate explicit measurements across device workers through ordinary
operator scheduling. A missing resource
or interruption is reported as partial, with a reason; retry on the next join with `--onboarding-benchmark`.
Complete results are reused when hardware/driver identity, CPU count, memory,
extension digests and file-store address are unchanged. Changed identities trigger
measurement on an opted-in rejoin, as with extension requalification; live driver replacement
while a worker runs is not supported.

Installed service configurations can set `"onboarding_benchmark": true`;
`service ... -- join` also accepts `--onboarding-benchmark` in its join argv.
Existing configurations default to false. On personal or polite hyphae, combine
the opt-in with `--daily-use` and the appropriate `--laptop` or `--desktop`
declaration so measurements respect owner activity, power and heat.

For an explicit isolated measurement, use your own state, prepared extensions
and test file-store queue. Add `--daily-use --laptop` on a daily laptop:

```sh
myce benchmark --gpu metal --state-dir ./benchmark-state \
  --extension-root ./extensions --queue http://localhost:18451 --daily-use --laptop
```

A complete explicit run exits zero. A partial run writes the profile and exits
nonzero. Missing throughput is never recorded as a zero-speed measurement.

Placement first applies all existing qualification, memory, availability, host,
project, checkpoint, priority, fairness, preemption, production-hold and start
authorization rules. Among free eligible hyphae it predicts one reference's
execution time plus known model/environment preparation and input-file delivery.
Measured predictions rank ahead of unknown ones. Comparisons within a frozen
reference group use predicted seconds; different references have a stable digest
order rather than an invented speed conversion. If neither candidate has a
prediction, first programs keep the memory-capacity fallback and other work keeps
its existing placement. Equal measured predictions use hypha ID.

This is a provisional workload estimate: there is no automatic model of arbitrary
program size or data-dependent execution time yet. CPU, memory and disk rates are
reported separately; they are not blended into an arbitrary accelerator score.
There is no queue for a busy faster hypha, automatic migration, or new service.

## Comparable accepted work (slice 1, #107)

Opt in per variant, using the same work key as its existing native baseline:

```yaml
variants:
  - target: {os: linux, arch: amd64, platform: cpu}
    # Existing needs, execution, check and baselines remain required.
    references:
      extension:example.stage:
        input_shape: small-v1
        unit: records
        units: 100
```

`units` is the amount in the bundled reference input. `input_shape` is an opaque
compatibility bucket, and `unit` is a generic unit name. Both use lowercase
letters/digits, dots, underscores or hyphens, up to 64 characters. The contract
promises one independent item and vector batch size one; recipe settings own
thread/cache behavior. Reference metadata never qualifies a variant: the exact
native-baseline check still must pass, and a changed recipe/reference invalidates
its digest and qualification.

An ordinary job selects the extension and declares
`workload: {input_shape: small-v1, units: 250}`. A matching qualified variant's
accepted success contributes queue-Clock elapsed time from Started to completion,
including cleanup/handoff but excluding preparation. Per-key estimates divide
accumulated elapsed seconds by completed units; a job's prediction is frozen at
start using prior samples, and its actual appears only after accepted success.
The CLI, HTML status and JSON job `timing` field show prediction/actual and the
sample count; absent old fields mean unknown. The key records exact work, shape,
recipe/reference, target, physical component, fingerprints and resource settings.

Failed, duplicate, stale, interrupted, stopped, resumed, release-pending and check
attempts never update estimates. Competing Mycelium host custody invalidates the
whole interval, even if that work finishes first. Queue restart or identity,
qualification or resource-setting changes also invalidate an in-progress sample.
This is queue-observed contention; it does not certify absence of external OS
work. Old scalar profiles supply no comparable samples. No automatic benchmark
sweep or placement change is enabled by these observations.

## Current worker protocol

Queue and workers use `X-Myce-Start-Gate: host-launch-v3` for worker
requests, including completion, log and checkpoint reports and worker events. An old or missing protocol
gets HTTP 426 with `worker_protocol_mismatch`. Pre-v3 formats remain unsupported. Upgrade the queue before keyed workers;
current host-launch-v3 one-attempt workers remain supported as described below.

Every worker reports a capability profile. Profile version 0 is valid;
unknown capacity withholds admission. Dedicated participation and explicit
laptop admission-only operation remain supported. Enforced host allowances
require measured RAM and declared job RAM. Every model report names its engine
attempt. Local sources report `verified`, `invalid` or `unknown`; only explicit
UNKNOWN gets the existing bounded ready-routing grace.

Every batch command, including non-production work, obtains the durable Started
acknowledgement for its current execution before its payload executes. Current queue maintenance, placement, cancellation and exact attempt/session
bindings are checked at that handoff. Docker, engine and preparation plans use
queue maintenance and retain their exact cleanup custody. A rejected or stale report does not
release another attempt's custody.

New programmatic installs store literal `MYCE_TOKEN` and `MYCE_QUEUE`
assignments in their private credential file, using the same EnvironmentFile
parser as externally provisioned secrets. Old JSON credential files are not
converted; provision current credentials through the secret mechanism when
installing fresh. No token is passed in argv, logs, or job environments.

Old persisted formats return `legacy_state_present`; operators archive and
install fresh as described in [Archive plus fresh install](operations.md).

Environment-build and model-copy offers carry exact queue preparation attempts.
The queue retains issued offers before delivery; workers recheck `/api/workers/launch`
at launch, including after cache waits and at command supervisor gates. The same
queue admission check protects batch Started and engine launches. Drain cancels
unstarted offers and retains started custody until an exact terminal report.

## M1 keyed attempt wire revision

`attempt_protocol: keyed-attempt-v1` opts a check-in into keyed reporting. The
queue echoes that value. Without it, the current single `running` / `started`
request and `job` / `hold` / `stop` / `kill` / `grace_seconds` / `priority_stop`
reply fields retain their existing behavior. This is an extension of the
existing host-launch-v3 protocol, with the same authenticated endpoints.

A keyed heartbeat/progress report uses:

```json
{
  "attempt_protocol": "keyed-attempt-v1",
  "machine": {"id": "worker", "session": "session", "...": "ordinary machine facts"},
  "attempts": [{"id": "job", "attempt": 1, "session": "session", "progress": 7, "started": true}]
}
```

The abbreviated machine above stands for the usual valid capability and resource
report. Omit `attempts` when idle. A keyed reply uses the same key for each
assignment or control decision:

```json
{
  "attempt_protocol": "keyed-attempt-v1",
  "attempts": [{"id": "job", "attempt": 1, "session": "session", "stop": true, "grace_seconds": 10}],
  "hold": false, "stop": false, "kill": false, "leave": false,
  "grace_seconds": 0, "checkpoint_root": "checkpoints"
}
```

An assignment adds `job` to that attempt entry, using the unchanged job-v1
schema. Its ID, attempt, session and machine must match the entry and worker.
A stopping entry can also carry `kill` and P3 `priority_stop`. An exact attempt
can be held during checkpoint reconciliation. Global model/preparation plans,
maintenance policy, checkpoint configuration and leave remain worker-scoped.
The legacy control booleans remain false on keyed replies and `job` is omitted.
Workers apply controls only to the matching `(id, attempt, session)`. A stop
for an unheld key in the worker's own session receives an exact release-only
acknowledgement, including when the original assignment reply was lost or
discarded. Foreign-session controls and unheld holds are ignored. Stop events
add `session` to their existing ID/attempt/deadline fields and are fenced by all
three fields before delivery.

Results, release-only acknowledgements, logs, checkpoint publication and
`/api/workers/started` already use `id`, `attempt`, `session`, and `machine`.
Those shapes and ownership checks are unchanged. P3 stop provenance and deadline
semantics are unchanged. No omitted keyed attempt proves cleanup, releases a
charge, or requeues a stopping job. Exact acknowledgement or the existing
immediately previous-session cleanup rule is still required. Durable worker
spools and queue restart retain the same key for buffered results. An exact
non-pending native release-only acknowledgement advances its recorded stop to
requeue or cancellation, without waiting for an omitted attempt in a heartbeat.

Compatibility and refusal:

- Current host-launch-v3 one-attempt worker → new queue: unchanged legacy form.
- Keyed worker → new queue: collection form; legacy one-attempt admission remains bounded by that adapter.
- Keyed worker → older unkeyed queue: fail with `worker_protocol_invalid` (HTTP 426)
  when the old queue returns HTTP 400 `request_invalid` for the keyed fields or
  lacks the revision echo. Other 400 errors retain their own status and code.
  Upgrade the queue first. Saved custody is retained; no automatic unkeyed downgrade.
- Unknown revision, mixed forms, duplicate keys or invalid key/session → `worker_protocol_invalid` (HTTP 426), before queue mutation.

Automatic downgrade would permit an unaddressed stop to affect a different
attempt, so clear refusal is the compatibility policy. Remove the legacy form
once every fleet host runs machine workers and the rollback window is closed.

`attempts` has no wire cardinality bound. Reconcile and answer each key independently,
including stale-key stops and omitted stopping keys. Duplicate keys are invalid.
Current machine workers report `machine.devices` and supervise concurrent keyed
attempts using the one shared CPU/RAM ledger and one batch slot per GPU. Each
check-in offers at most one additional assignment. The bounded legacy worker
adapter admits one attempt and cannot discard neighboring controls. See
[the machine contract](design-proposal.md#one-machine-one-worker).

## Scheduling simulator

`myce sim` replays synthetic GPU work through the **real** queue, batch dispatch,
model-call placement, memory admission, priority reclamation, and decaying
fair-share accounting. In-process workers run timers instead of processes or
models. A fake clock jumps between arrivals, finishes, stop acknowledgements,
model readiness/idle expiry, availability changes and worker-loss deadlines.
No GPU, network connection, token, SQLite file, model download or real sleep is
needed. Model loads admitted during a check-in settle at that same fake-clock
instant, including after command cleanup; an unrelated future event must not
delay their startup timer or create an artificial load timeout. The simulator skips persistence and assumes a reachable pretend NAS.
It aborts on queue errors. Finished jobs are removed after their metrics are
collected; production does not use that removal path.

```sh
go build -o /tmp/myce ./cmd/myce
/tmp/myce sim --fleet examples/sim/fleet.yaml --workload examples/sim/workload.yaml \
  --days 7 --seed 1 --json /tmp/baseline.json
/tmp/myce sim --days 7 --seed 1 --what-if split-batch --json /tmp/comparison.json
```

The defaults resolve relative to your working directory. JSON defaults to
`sim-results.json`; use `--json` to choose the output. YAML parsing rejects unknown
fields, multiple documents and invalid values. Runs allow at most 90 days and
20,000 jobs. The supplied week runs in seconds on a laptop.

## Inputs and comparisons

Fleet workers are individual cards with names, platforms, usable `memory_gb`,
shared physical `host`, usable `host_memory_gb`, `unified`, and relative `speed`.
The two CUDA workers on `host-b` share one host RAM budget; the Metal worker
on `host-c` shares GPU/CPU RAM. Budgets are **GiB after reserves**, not apparent
model-file sizes.
No availability windows means always present. Windows use Monday=0 through
Sunday=6, with start-inclusive/end-exclusive hours in a repeating synthetic
UTC week. `departure: planned` (default) saves resumable work and restarts
non-resumable work. `departure: loss` cuts execution without a final checkpoint;
the real queue's priority loss timeouts and majority-silence guard determine
when batch work becomes available again. Interrupted model calls fail visibly
on abrupt loss; this simulator does not emulate HTTP response replay.

A workload may include `policy:` with the same version/tiers/projects mapping as
`myce policy set`; the simulator installs it through the real queue policy API.
Legacy top-level `projects:` still supplies weights/caps; do not also specify
`policy.projects`. Batch streams accept `class:` or legacy `priority:`, normalized just as at
submission. Model calls still use
legacy priorities. Shared-tier dispatch, backfill and status use the same queue
planner; the simulator adds no fairness ordering of its own.

Models specify
identity, file bytes (when known), GPU/host memory, platforms and local startup
time. Streams specify project, priority, model, batch versus `call: true`,
platform/memory for batches, duration, checkpoint interval, repeating arrival
interval in hours, first arrival hour, count and optional deadline seconds from
submission. Durations mean base-speed compute seconds. Per-job seeded jitter is
±20%; `duration_jitter: 0` makes hand-checkable scenarios exact. Startup time and
compute time divide by the worker's speed. All model files are assumed already
cached on compatible workers: startup times exclude NAS copy/verification.

A batch process loads its native model on each attempt; `needs.model` remains
only the real scheduler's cache hint. The simulator does **not** invent persistent
batch residency. Model calls use real model slots and load/unload acknowledgements,
with one in-flight call per model slot, allowing real co-residency admission.
Resumable processes save current progress within a modeled one-second SIGTERM
response; abrupt loss retains only complete periodic checkpoints. Checkpoint
intervals are wall seconds of compute and progress carries between worker speeds.

`batch_files: 12` marks a serial transcription batch. `--what-if split-batch`
submits the same seeded file work as 12 separate jobs instead of one. It changes
no scheduler policy. The comparison uses only matching batches finished in both
runs; JSON also retains every batch's finish time or null. Negative “seconds
sooner” means splitting was slower. Completion counts and unfinished batches
expose censoring. Additional comparisons can simply use edited fleet/workload
YAML files; there is no plugin system.

## Reading the numbers

| Metric | Meaning |
|---|---|
| Production wait median / p95 / max | Submission to first command start, or to first ready-model call assignment. Batch native model initialization happens after command start and is reported as loading. Nearest-rank percentiles; unstarted work is counted separately, never assigned a zero wait. |
| Deadline misses | Due jobs not successfully finished by their deadline, including unfinished/failed jobs whose deadline passed within the horizon. |
| Experiment throughput | Completed queue jobs per simulated day, per project; split-file jobs change this unit. |
| Experiment mean finish time | Submission-to-success seconds, for completed experiments only; unfinished count must also be read. |
| Project GPU hours / share / weight | Active device time divided equally among simultaneous commands/calls. Includes batch initialization and stop grace. Experiment-only GPU hours/share are also in JSON. Weight percent is a global comparison, not an entitlement when projects cannot compete on the same hardware. |
| Idle with fitting work waiting | Online device-hours with no command, call or model load while the real queue's fit/cap checks admit waiting work there. Memory-incompatible or capped work does not count. Empty or incompatible idle time appears through low utilization. |
| Machine utilization | Command/call device-hours divided by online device-hours, summed per physical GPU worker. Loading-only intervals appear separately and are not called compute utilization. |
| Loads / reloads / loading seconds | Every batch initialization or real slot load; subsequent loads of the same model on the same worker are reloads. Loading seconds count occupied device time once per device even if loads overlap. |
| Preemptions / lost work | Real queue's preemption records; compute seconds discarded by forced non-resumable stops. Resumable graceful stops save progress; loss since the last checkpoint is separately reported for abrupt departures. |
| Never placeable | Submitted jobs that cannot fit any configured fleet worker even when empty, regardless of temporary availability. This differs from unfinished backlog. |
| Batch completion mean | Time until every file in each completed marked batch finishes. Per-batch timestamps and unfinished counts are in JSON. |

## Limits

This is a measuring tool for the scheduler under stated assumptions, **not** a
fleet benchmark or hardware qualification. All example host/model/project
identities, topology, speeds, memory budgets,
file sizes, startup times and arrival/duration values are invented for illustration.
They contain no fleet measurements or consumer traces. File sizes do not establish
native peak memory. Streams arrive independently rather than forming a dependency
graph, and cross-platform portability is hypothetical.

The workers have continuous healthy heartbeat coverage and instant pull/stop
notification outside the modeled stop response. Transport polling, HTTP overhead,
real checkpoint I/O, NAS transfers, environment builds, external GPU owners,
thermal effects, concurrent inference slowdown and partial response custody are
not simulated. Thus the run gives an optimistic scheduling response bound.
Model-call `max_wait` is a transport/API timeout and is not simulated. This loop
uses the real scheduler entry points but does not test HTTP, process control,
SQLite durability or native engines. Whole-fleet silence follows the real
majority guard; work may remain pending until reconnect instead of being retried.

The queue's scan implementation has equivalent performance shortcuts for large
backlogs: one ordinary candidate per project, cached fair scores within a scan,
priority-specific reclaimable memory, and no reclamation scan for experiments.
Within a read-only device scan it also reuses identical resource projections
when no preparation is active and sorts each project's candidates once for
exclusion enumeration. These snapshots are discarded on placement mutations.
They do not add placement or fairness rules; regression tests compare decisions
against the original scans. Fake-clock tests cover preemption, graceful leave,
sudden loss/checkpoints, exact weighted allocation, fitting-idle accounting,
model loading/reuse, impossible work, deadlines, batching and determinism.

Production release planning counts commands already being stopped. After stop
completion, it retains release credit until the next worker memory report or
the original stop grace expires.
Each waiting batch needs a separate card; release credit never admits a command
before a free-memory report. The one-production/two-experiment fixture therefore
interrupts exactly one experiment and starts production after its one-second save.

## Per-piece pipeline comparison

```sh
myce sim --pipeline-scenario separate-stage --seed 1 --json pipeline.json
python3 scripts/sim-per-piece.py --binary ./myce --out .reports/per-piece
```

The harness uses slice 4's fast runtime (`GOMAXPROCS=1 GOGC=400`) with three
parallel seeds. Fixed scenarios cover a homogeneous joint-acceptance control,
heterogeneous waves and a short tail, separate stage capacity, shared stage
capacity, and the original seven-day mixed-production workload with an identical
predeclared downstream stage in each mode. `--what-if per-piece` also accepts a
custom workload: add `downstream` to a stream with `batch_files` and curves:

```yaml
downstream:
  duration_seconds: 60
  platforms: [xpu, cuda]
  memory_gb: 1
  host_memory_gb: 1
  publication_seconds: 2
  transfer_seconds: 1
  preparation_seconds: 1
  intermediate_bytes: 1048576
```

These are explicit synthetic costs, not native measurements. Producer publication
and verification cost is charged per piece after compute, before acceptance;
input transfer and preparation precede downstream command start. Both modes
predeclare the same jobs and use the real queue planner and dependency binding.
Only the baseline's simulator-only eligibility barrier waits for producer gather.
There is no downstream event handler making placement decisions.

JSON records keyed coverage, first downstream start, acceptance-to-ready/start,
full pipeline makespan, producer gather, production p95 and unstarted count,
failed/cancelled/unfinished work, allocation utilization, and intermediate bytes.
Retained intermediate peak includes consumed outputs; pending intermediate peak
counts accepted pieces still waiting for their consumer. Report gather slowdown
and every scenario's regression alongside the mean gain. The overlap gate
requires complete output coverage, an earlier downstream start, lower makespan,
exactly unchanged production p95 and no extra failures/cancellations/unfinished
work. Failed gates still write JSON and exit nonzero. The homogeneous control
expects equality because all pieces accept jointly; it supplies no speed claim.
The lean simulator regression separately exercises controlled producer loss/retry
and parent abort, outside the speed comparison.

## Placement scenario probes

Use the checked `examples/sim/value-placement/`, `value-placement-cpu/` and
`value-placement-idle-cpus/` scenarios to compare scarce-resource preservation,
CPU overflow and idle peer backfill. Speeds and capacities are synthetic.
Use the same inputs, seed and horizon for comparisons, and report failed and
unfinished work alongside makespan and production p95. A mixed backlog does not
establish whole-workload makespan. Historical run receipts remain in Git history.

The global next-pull plan shares candidate, scarcity and liveness projections
within one read-only pass; snapshots expire on mutation. Temporary occupancy
never redefines host value. Deterministic `TestValuePlanningWorkCounts` exercises
bounded projection work. Optional `-tags placement_cost` with
`TestValueCheckinCost` measures check-in cost including SQLite; it is not a
machine-dependent timing gate or native performance promise.

`go test ./internal/myce -run '^$' -bench '^BenchmarkQueueCheckinFleet$' -benchtime=20x`
measures a real SQLite check-in with 60 queued and 160 retained jobs, five
workers, two validation reservations and 100 curve rows. Each queued job has
768 synthetic input references to exercise manifest-dependent memory identity;
these are invented inputs, not a captured fleet workload. `hold-ns/op` measures
the update transaction; `all-holds-ns/op` also includes snapshot/report locks.
All scheduling uses a frozen injected clock. Add `-cpuprofile=checkin.cpu` to
profile it. The #661 laptop comparison reduced the update mean from 275.5 ms
to 30.6 ms (maximum 309.0 to 32.6 ms); total lock time fell from 275.5 to
31.5 ms. Repeated recipe/input identity hashing dominated before the change.
Plain identities are now computed outside the lock and fenced by the committed
queue revision and original job pointer; stale snapshots use ordinary planning.
This is synthetic scale evidence, not attribution of live disk stalls (#527)
or proof that worker freshness remains stable after deployment.

`BenchmarkQueueCheckinLiveScale` compares 160 and 6,500 retained terminal jobs
with keyed answers and ledgers, including one 17,400-entry ledger. Both cases
have 60 distinct queued auto-memory recipes, 120 paused jobs, five occupied
workers checking in every injected second, and 1,300 learned curve aggregates.
Run with `-benchtime=50x -cpuprofile=checkin.cpu`. At 6,500 retained jobs, the
curve-index comparison reduced mean update hold from 58.2 to 13.1 ms (maximum
77.2 to 21.6 ms). Repeated full-curve scans and value copies dominated planning;
the transaction now indexes exact comparable memory work and borrows the
matching aggregates. Observation writes invalidate that index; rollback and
commit discard it. Retained terminal count alone was flat in the comparison.
These laptop timings do not establish the full cause of a live queue stall.

`GOMAXPROCS=4 go test ./internal/myce -run '^TestCheckinRetainedHistoryContention$' -count=1 -v`
replays six concurrent worker check-ins and 60 concurrent job GETs per round,
with 6,500 retained constrained executions, 110 queued jobs and 1,400 curves.
It reports nearest-rank hold p50/p99 (including the check-in's snapshot locks),
job GET mutex waits and total process CPU per check-in across 20 rounds. The
hold p99 must be below 100ms. Unlike empty terminal receipts, these retained
jobs carry timing and memory-attempt evidence used by cold-memory planning.
This `!race` laptop regression includes real SQLite commits and uses an injected
clock for every decision; CPU accounting includes the concurrent readers. It
does not establish live fleet latency or native hardware qualification.

## Backed storage admission integration

The unfinished native storage implementation and its original integration/native
qualification instructions are preserved on [`park/storage-protection`](https://github.com/1kuna/mycelium/tree/park/storage-protection).
Main keeps decode-only compatibility for retained state, ordinary output
reservations/upload admission, caps/reserve, accepted manifests, checkpoint
transfer, async NAS publication and exact-attempt custody. New protected-storage
and worker-local task-output submissions are rejected; historical protected
records with unresolved custody remain held and protected from collection.
Settled collection acknowledgements retain provenance while allowing ordinary
file release, retention and history cleanup. Protected child-state discovery
remains a read-only startup fence, including the pre-index crash window.

The usefulness verdict and future backend slice are tracked in
[#564](https://github.com/1kuna/mycelium/issues/564) and
[#504](https://github.com/1kuna/mycelium/issues/504). No native qualification or
activation is implied by laptop tests. See [operations](operations.md#parked-native-storage-integration-564).

## Machine-worker native qualification

Laptop concurrency/mask fixtures do not qualify runtime visibility, device fault
isolation, delegated cgroups, service reboot or the simultaneous memory envelope.
During a separately authorized maintenance window, record private binary/config
hashes, exact attempts and outputs, then prove:

- One worker/session, stable full inventory, fixed per-device budgets and one
  CPU/RAM charge; identity mismatch/unavailable probe withholds only affected
  components and matching identity recovers after restart.
- Two independent GPU attempts and helpers see only their assigned card.
  CPU main/helpers/managed containers enumerate zero CUDA/XPU devices using the
  actual installed runtimes (`libcudart`/`sycl-ls`), not just environment strings.
  A nonzero probe exit is not automatically proof of select-none; Metal remains
  explicitly unenforced for CPU work.
- Exact cancellation/member OOM cleans one subtree while its neighbor continues
  with unchanged identity, healthy heartbeat and correct accepted output.
- Over-budget, unsupported count/masks and optional device allowances refuse or
  narrow correctly. Production host holds and maintenance cover all devices/CPU,
  and no replacement starts before exact release. Planned stops preserve retry
  counters; a heartbeat stall charges each affected attempt with the proper bound.
- Durable checkpoints, disabled policy, jobs/keys/counters and release custody
  survive queue restart, reboot and installer redeploy. Split units do not return.
  Conversion rollback/forward remains disabled until exact restoration verifies;
  compatible clients and native consumer health precede reopening submissions.

For onboarding, ASPEED/BMC-only PCI evidence with failing NVIDIA tooling may be
CPU-only; actual NVIDIA hardware or unknown PCI facts retain typed driver failures.
A working integrated Intel runtime yields XPU/unified memory; missing runtime may
leave CPU, while a broken runtime refuses the incomplete install. Mount inventory
shares the enclosing validation deadline. Cancellation preserves cancellation
rather than becoming `nas_unresponsive`; abandoned context-bound checks cannot
serve late success. Durable worker operations separately consume their own late
completion so completed publication/preparation is not repeated. Native installer
progress/stall budgets remain as documented in operations; historical test timings
and download receipts are in Git history, not current qualification claims.

## Tool inventory check-in protocol

Workers first send the existing full `machine.tool_inventory` request. A queue
advertises `tool_inventory_protocol: tool-inventory-v1`, with
`tool_inventory_hash` and `tool_inventory_version` acknowledging the accepted
report. New workers also advertise `tool_inventory_v1` in the existing
`machine.capabilities` list. Only after this advertisement does a worker send the additive top-level
check-in fields `tool_inventory_hash` (SHA-256 of canonical JSON: sorted keys,
sorted/deduplicated tool names) and `tool_inventory_version` (increasing on change
within its machine/session). Unchanged reports omit `machine.tool_inventory`.
A changed inventory includes the full map with the new hash/version.

The queue answers `tool_inventory_required: true` when the matching snapshot is
missing or full contents do not match the hash. The next heartbeat resends the
full map; meanwhile missing inventory never qualifies `needs.tools`. The queue
persists only the session/hash/version fence. Inventory contents are immutable,
in-memory references, so a restart requests a full refresh and stale reports
cannot roll back a newer version, including across queue restarts. New sessions
must establish their own inventory. A worker process recovering custody in its
existing session first negotiates above the durable version fence, then resends
the full report; an unversioned report cannot overwrite it. Validation, canonicalization, hashing and
machine JSON copying precede the queue transaction. Transaction rollback and
detached diagnostic planning preserve the immutable reference.

An older worker can continue sending the full inventory. Against an older queue
that supports inventories but rejects additive request fields, a new worker
keeps the full legacy shape because support was never advertised. If a previously
negotiated queue is replaced by a strict older binary, an HTTP 400
`request_invalid` decode refusal permits one retry using the full legacy shape.
Other refusals retain their existing behavior. This does not
extend compatibility to queues predating `machine.tool_inventory`. Tool-based
placement and the worker's effective-PATH launch recheck retain their meaning.
The queue's status projection omits the full inventory payload.

`TestToolInventoryWorkerHandshake` exercises the real HTTP check-in path with the
#790 fixture (40 prepared environments, 2,400 native executable names), inventory
change, cache mismatch and restart. Fake-clock regressions cover session/version
fences, a delayed concurrent report, rollback and diagnostic placement. The
upgrade-compatibility process suite owns cross-binary coverage separately.

For a before/after comparison, run `BenchmarkToolInventoryFleet` with
`-benchtime=25x`. Its source is compatible with #790: run `full` there and `delta`
on the candidate. Five synthetic CPU workers report against real SQLite with
6,286 settled jobs, 1,655 retained curves and 1,384 storage observations. It
reports check-in bytes and total queue mutex hold p50/p99, including transaction
snapshots/commits and check-in read phases; JSON decoding stays outside that hold.
The settled backlog isolates inventory overhead and does not model concurrent
job GET traffic, active scheduling load, native device probes or live latency.

## CPU and I/O collector cost (#857 item 6)

The collectors are diagnostic. Linux reuses the existing 200 ms cgroup sampler
cadence and reads three aggregate counter files, with no descendant walk. Three
300 ms laptop benchmark runs over a synthetic cgroup tree measured 39.4–45.4 us
per complete CPU/stat/PSI sample (about 0.020–0.023% of one core at that cadence).
This measures parser/filesystem overhead on ordinary local files, not native
Linux virtual-file cost or contention. Host PSI adds one local read per existing
heartbeat, through the existing single-outstanding local-observation probe. That
probe's 25 ms control budget prevents a slow read from blocking heartbeat work;
it can leave that heartbeat's pressure unknown and never spawns overlapping reads.

Twenty native macOS process-tree CPU reads measured median 32.065 ms, p95
35.676 ms and maximum 38.702 ms under shared load. At the 500 ms RSS cadence,
median inventory cost would consume 6.4% of a core per attempt. CPU therefore
reuses the existing five-second CPU/liveness cadence (tenfold fewer inventories,
about 0.64% at that median). The cost is missed short-lived child CPU; the source
is explicitly a lower bound, with root exit rusage supplementing sampling. The
inventory deadline shares that cadence and its existing 200 ms drain allowance;
a hung ps can delay final observation by that budget, while control stays separate.
No new refusal, policy threshold, host preference or controller setting is added.

Reproduce parser/filesystem cost with
`go test ./internal/myce -run '^$' -bench '^BenchmarkAttemptResourceCgroupSample$'`.
`MYCE_NATIVE_RESOURCE_TEST=1 go test ./internal/myce -run '^TestAttemptResourcesNativeRead$' -count=1 -v`
is a separate read-only native process-tree/host-PSI check. Normal gates use fake
cgroup/proc data and an injected clock. The initial implementation ran that native
check on macOS (CPU readings and explicit unsupported I/O); native Linux delegated
cgroup CPU/stat/PSI and Linux host PSI were not run. No daemon, GPU, I/O isolation
or loaded queue-latency qualification follows from these measurements.
