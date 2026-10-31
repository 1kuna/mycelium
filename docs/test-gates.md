# Laptop test gates

Run `./scripts/gate.sh` (or `./scripts/gate.sh quick BASE`) for a change. The
base defaults to `origin/integ/108`; select the commit before your change when
working elsewhere. Quick builds the CLI and every release target, vets all Go
packages, and race-tests changed packages with test names from matching file
prefixes, plus tests that directly reference changed exported symbols (using
diff hunk function names and changed declarations). Changed non-Go files, including
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

Quick has a five-minute work/termination budget, including cache setup and builds;
on expiry it stops its fixture processes and suggests `./scripts/gate.sh full`
(exit 124). It sends TERM at 295 seconds and KILL after five seconds if needed,
then verifies fixture exit before removing scratch. Cleanup may extend the wall
time; scratch is retained if a fixture cannot be stopped or inventoried. Cleanup
uses the unique inherited run marker, isolated HOME/TMPDIR, ancestry and OS file
handles in scratch (macOS's native `libproc` API, or Linux `/proc`). The macOS
probe reads kernel process paths without walking mounts, while retaining
same-user discovery of reparented fixtures. It has a five-second
timeout and at most a 0.2s reap after killing the owned probe: stalled network filesystem I/O must not
block the final report. A failed or timed-out inventory retains scratch and
never authorizes fixture signals or deletion. It remembers start
identities through TERM/KILL, including environment-scrubbed or reparented
commands, and signals individual PIDs. The old 175-second
deadline left only 19 seconds for builds/vet after an
observed 156-second related-test selection, and repeatedly truncated passing work
under shared load (#204). Five minutes keeps quick below the full gate's target
without reducing test selection. Quick tests use Go's `-timeout` set before each
invocation to the remaining
supervisor work budget minus ten seconds, preserving goroutine dumps on hangs
without an independent three-minute package cap (#382). Compilation occurs
before Go starts its test timer, so sufficiently slow compilation can still
leave the outer supervisor to terminate the run before a dump. Selection, race
instrumentation and concurrency are unchanged.
The #379 replay on a shared macOS host selected 316 internal test names (six
Linux-only names), with 305 passes and five existing opt-in skips. The internal
package reported 220.406s (238s including compilation); the CLI selection and all
build/vet steps brought the successful gate to 271s. This exceeds the former
180s package cap while fitting the unchanged overall budget.
Full runs have no package-wide wall deadline:
a healthy two-core Linux core suite has taken nearly fifteen minutes. Observe
the JSON log stream and stop a run with no log growth for twenty minutes
rather than killing active tests. Timings
include build, vet, the three release target builds, test execution and scratch
cleanup.

Go 1.25.6, a native macOS arm64 toolchain (for the CGO release target), `uv`
and Python 3 are current laptop prerequisites. Install prerequisites separately:
the gate runs offline and never downloads tools or modules. Seed dependencies
with your usual `go mod download` before running a first gate. The gate shares
Go's normal build and module caches (Go locks them safely across concurrent
runs); only HOME, TMPDIR and GOPATH point inside an ignored `.gate/run.*`
workspace, which is deleted on exit because it holds only rebuildable test state.
Failure logs belong in an
ignored `.reports/` folder (for example redirect stdout/stderr there).

Before a full run, use `pgrep -fl 'go test'` and wait if two full race suites are
already active. Do not terminate another worker's tests. The gate allows two
Go packages at once (`-p 2`), rather than unbounded compiler parallelism. The
suite runs at most four independent tests concurrently within each package.
Quick overlaps up to two selected packages with their individual test-name
expressions; it waits for both results even if one fails. Each quick test
command uses one Go build slot, keeping the total at two.
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
see [agent offload](agent-offload.md). Linux runs the Linux builds, vet and full
race/dev suite. The native Darwin CGO build explicitly skips there and remains
a separate macOS check; the Linux gate is not proof of that release target.

For ad-hoc suites, `myce run --profile go-test -- -race ./...` defaults to the
same `-timeout 15m` as the full gate. Arguments after `--` go to `go test`; an
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
One repository-scoped runner accepts jobs sequentially without cancelling older
merges. A backlog waits in Actions; no model callbacks, polls or wakes are needed.

**Check main's latest run before merging**, including its head SHA:

```sh
gh api repos/1kuna/mycelium/commits/main --jq .sha
gh run list --repo 1kuna/mycelium --workflow main-full.yml --branch main --limit 1 \
  --json headSha,status,conclusion,url
```

A completed successful run must match current main. Missing, queued, running or
failed is not passing evidence. Before merging, run targeted tests and obtain
independent PR review. The next merge worker fixes a red main forward; agents do
not wait on or poll CI. Automatic main proof supplements the release full gate.
Use the run's job summary to inspect both platform outcomes and Mycelium job IDs.
The workflow's own check reports success/failure; no commit-status credential or
personal access token is needed.

[`scripts/actions-full.py`](../scripts/actions-full.py) narrows the existing
`gate-full` profile to the exact push SHA and submits it once for Darwin arm64
and once for Linux arm64. As a temporary mitigation for
[#527](https://github.com/1kuna/mycelium/issues/527), the generated CI profiles
exclude the Linux amd64 target to keep full suites off the current queue host.
Profiles have no negative host selector; target exclusion is the smallest
supported exclusion and also excludes other Linux amd64 workers. Native Linux
amd64 full-suite coverage is deferred; cross-build checks still run. Restore that
target only after the production hold ends and queue I/O protection is qualified.
Resources, environment, output contract and check
remain unchanged. There are no host pins; Mycelium chooses each worker and
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

The unactivated watcher from [#427](https://github.com/1kuna/mycelium/pull/427)
was retired in favor of the standard Actions runner under
[#425](https://github.com/1kuna/mycelium/issues/425), following the maintainer's
2026-10-31 ruling: “i use self hosted runners only”.
