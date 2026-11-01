# Develop and verify Mycelium

Use [quick/full gates](#laptop-test-gates) for laptop changes, the
[native acceptance checklist](#native-acceptance-checklist) for authorized device,
manager and boot proof, [component probes](#laptop-component-probes) for the real
API/runner, and [simulation](#scheduling-simulator) for synthetic policy comparisons.
Fake inventory never qualifies native hardware. The [worker protocol](#current-worker-protocol)
and [storage integration](#backed-storage-admission-integration) are developer
contracts, with unsupported/native gaps explicit.

Choose `BASE` explicitly when running `./scripts/gate.sh quick BASE`; the script's
historical default is not a suitable change base for every branch. Known heavy
runs use the project's Mycelium submission path. Inspect host load/memory/disk
before local heavy work; do not exceed two full race suites on one Mac.

## Laptop test gates

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
see [agent offload](adapter.md). Linux runs the Linux builds, vet and full
race/dev suite. The native Darwin CGO build explicitly skips there and remains
a separate macOS check; the Linux gate is not proof of that release target.

The full-gate profile and its converted adapter entry declare 7 GiB of CPU host
memory. Successful unconstrained attempt telemetry inspected on 2026-11-01 had
Linux total cgroup peaks of 2.699532–4.472893 GiB and Darwin sampled process-group
RSS up to 2.999222 GiB. Linux resident counters alone reached only 2.816639 GiB;
the total peak also covers file-backed charges. The budget is the largest total
peak plus the larger of its observed range (1.773361 GiB) and 25% of that peak,
rounded up: 6.246254 GiB becomes 7 GiB, with ordinary launch slack additional.
This margin prevents ceiling pressure or OOM from observed run-to-run variation
and growth between samples, at a cost of 2.527107 GiB above the largest total
peak. It is an observed envelope; revisit it if the suite grows, hits its ceiling,
or approaches the budget. On Linux workers with enforced cgroups, the memory
ceiling includes the declaration and configured launch slack; a confirmed OOM
on the final attempt appears as `failed (out of memory)` in CI. Other workers,
including Darwin, use admission-only accounting. Unhinted `memory: auto`
intentionally explores available host headroom after unusable constrained evidence, which made this repeatedly
measured CPU gate reserve scarce accelerator capacity; see
[#559](https://github.com/1kuna/mycelium/issues/559) for evidence and follow-up.

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

## Backed storage admission integration

The unfinished native storage implementation and its original integration/native
qualification instructions are preserved on [`park/storage-protection`](https://github.com/1kuna/mycelium/tree/park/storage-protection).
Main keeps decode-only compatibility for retained state, ordinary output
reservations/upload admission, caps/reserve, accepted manifests, checkpoint
transfer, async NAS publication and exact-attempt custody. New protected-storage
and worker-local task-output submissions are rejected; historical protected
records remain held and protected from collection.

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
