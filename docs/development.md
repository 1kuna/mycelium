# Develop and verify Mycelium

Use [quick/full gates](#laptop-test-gates) for laptop changes, the
[native acceptance checklist](#native-acceptance-checklist) for authorized device,
manager and boot proof, [component probes](#laptop-component-probes) for the real
API/runner.
Fake inventory never qualifies native hardware. The [worker protocol](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/development.md#current-worker-protocol)
and [storage integration](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/development.md#backed-storage-admission-integration) are developer
contracts, with unsupported/native gaps explicit.

## Private developer evidence

Git ignore rules do not exclude Go source from `go list ./...`, `go vet ./...`
or other recursive Go commands. Give any ignored evidence directory containing
Go files its own local, ignored `go.mod`, for example:

```sh
mkdir -p ops/private
# Create only when absent; preserve an existing module definition.
test -e ops/private/go.mod || printf 'module local.invalid/private-evidence\n' > ops/private/go.mod
```

The nested module is excluded from the parent module's `./...` traversal, even
when evidence contains mixed packages or C files. Keep this file local with the
evidence. New scratch directories can instead start with `.` or `_`, which Go
also skips. Do not delete evidence to make tooling pass.

## Laptop test gates

From a Git checkout on macOS or Linux, with Go 1.25.6+, a native C compiler
and Python 3.9+, run:

```sh
./scripts/gate.sh quick
go test -race -count=1 ./internal/myce -run '^TestName$'
./scripts/gate.sh full
```

Quick and targeted tests run without a GPU, fleet credentials or a Linux VM.
Use `myce dev --workers 1 --fake-unified-memory` to exercise unified GPU
placement with ordinary shell commands and a fake accelerator.
Go may fetch missing module dependencies. Python client checks also require
Python 3.11 (on PATH or already installed through uv).

Quick builds the native CLI, vets the Go packages, checks Markdown links and
runs the race-tested selection printed in its log:

- Changed Go tests, tests with matching file prefixes, direct symbol and fixture
  consumers, explicit indirect consumers, and the wire schemas for core code
  edits. Quick compares against the merge base with `origin/main` by default,
  including local uncommitted and untracked changes. Pass `quick BASE` for a
  wider batch. Review the selection and run targeted tests for indirect callers.
  Module edits and unmapped live Go files require full.
- Always run generated status docs, queue planning and status output canaries,
  and queue restart with durable assignments and logs. Entry coverage, inventory
  restart and CLI wait outcomes exercise surfaces that failed during
  [r6.36 full qualification](https://github.com/1kuna/mycelium/issues/1983).
  The typed RAM-hold and release regression caught by
  [r6.37 full qualification](https://github.com/1kuna/mycelium/issues/2000) also
  runs every time. Check placement digests, status contracts and incident
  replays explicitly when changing the corresponding behavior.
- Run the standalone `myce dev` smoke for gate changes and changes that select
  no Go tests.
- Changes under `skills/` or `clients/python/` run the runnable consumer
  examples through a real `myce dev` instance.

Full runs all Go packages under the race detector, the Python client suite and
the adjacent-release check below. Run it once per release batch on a host with
room for the Go cache and test scratch. Native Go tools use their normal cache.
The gate uses a private fixture HOME and a one-hour Go test timeout per package.
Release builds cover the cross-architecture build matrix. The full suite retains
security, incident and consumer-facing tests under [MY-53](DECISIONS.md#my-53).

### Local Linux VM

For Linux-specific behavior from a Mac, run the same commands in a Linux
checkout or guest of your choice. Native tests use the host's OS and toolchain.

### Submission advice attribution (#1065)

`BenchmarkSubmissionAdvice` uses the same incident-size retained history (6,500
constrained executions, 110 queued jobs, 1,400 curves, five synthetic workers).
It reports marginal commit-side snapshot capture mutex time separately from
post-commit capture/decoding, waiting reasons and deadline guidance. The
`submission_advice` telemetry event reports those advice phases after unlock;
existing update telemetry continues to measure the durable transaction.

Retained payloads are borrowed read-only; only active jobs, worker ledgers and
private planner caches are owned. Cold decoding happens outside the live mutex,
once for unchanged immutable records across commits; the benchmark reports that
cold sample separately from steady-state costs. This is fixture evidence, not a
live tail claim; cold decoding remains visible and there is no history cutoff.
The pre-isolation and post-isolation measurements are in Git history (#1065).

The HTTP regression blocks immutable decoding, completes a later control commit,
and requires the original response to finish while another owner holds `q.mu`.
Advice uses the accepted commit's policy and retained memory evidence. Cold
decode, detached active/worker copies and private observation-catalog space all
charge the existing measured request budget before construction.
If diagnostic allocation cannot fit, preserve successful acceptance, policy and
deadline-guidance fields with an explicit unavailable-blocker explanation. This
prevents a post-commit error from encouraging duplicate unkeyed submissions;
the cost is temporarily less detailed advice until headroom recovers. Normal
headroom preserves the existing advice values. Cold/warm constrained-capacity
HTTP regressions cover unkeyed acceptance and exact keyed replay. A separate
warm-curve regression with few jobs proves catalog allocation cannot bypass the
allowance and that advice recovers on the same job when headroom returns. Production tail
verification belongs to the separately authorized release after deployment.

### Release upgrade compatibility

The full gate runs `scripts/upgrade-compat.py` against the current sources and
one pinned predecessor under [MY-42](DECISIONS.md#my-42). The r6.20 candidate
pairs with r6.19 (`7f10215c8bcdcb2a97c7be45c7f1fd041ccd1bc3`).
For an additional live patch predecessor, pass its exact commit and version to
both `prepare` and `run` with `--previous SHA --old-version VERSION`. Advance `PREVIOUS`, `OLD_VERSION` and `VERSION` together for a
release.
`prepare` fetches and caches the exact public predecessor if needed before the
compatibility run.

For the file-store first-collection check, run
`scripts/upgrade-compat.py file-holdings --previous SHA --old-version VERSION`.
A trusted `git archive` passed with `--previous-source` lets the fleet run it
offline. The predecessor seeds real SQLite state and payloads. The candidate's
first tick has to preserve a full keyed answer and an unsettled claim while
collecting only the released digests that no holder still needs.

Real loopback queue and worker processes cover both directions, retained history,
command completion with confirmed custody release, unchanged keyed attempt identity,
saved-config readability by the predecessor,
and an uninterrupted worker across current/previous queue restarts with inventory
refresh. Synthetic hardware and service managers keep the checks laptop-testable.
The supervisor bounds execution and reaps its owned children before scratch
cleanup. Cached binaries include source identity, fixture bytes, version and Go
toolchain. This proves adjacent-release interoperation; native rollout and running
customer-job preservation still require their own release checks.

The committed `release-build` profile reuses this environment for Linux release
artifacts, with literal version and commit arguments and build receipts. The
single fleet build command and local signed Darwin step are documented in
[release operations](operations.md#publish-and-approve-an-onboarding-release).
Release script regressions run inside `TestReleaseBuildProfile`; their tools
are fake and never access a signing key or keychain.

### Fleet gates

Commit first so the frozen `HEAD` and selection describe the same source.
`scripts/gate-fleet-quick.sh [BASE]` submits the same selection to the Linux
and Darwin quick profiles and waits for both. A submitted source archive has no
Git history, so the script supplies `gate-selection.tsv` as a job input.

Run fleet full with `myce run --entry mycelium-gate-full --out DIR` for Linux or
`mycelium-gate-full-darwin` for Darwin. Linux full retains its measured 6 GiB
declaration (#767). Darwin full and both quick legs use learned reservations.
Each profile publishes `gate-result.txt` after success. On failure inspect
`myce logs JOB` and fetch `myce result JOB --attempt N --out DIR` to read
`kept/gate-failures.txt` with named Go test failures or the failed setup step.

For ad-hoc tests, `myce run --profile go-test -- -race ./internal/myce -run '^TestName$'`
selects one eligible target. The default timeout is 15 minutes and a later
`-timeout` overrides it. For a specific OS, copy the profile and narrow its
`targets` to that OS.

## Automatic main full gates

The [`main-full` workflow](../.github/workflows/main-full.yml) runs on pushes
to `main` and tests the triggering SHA. Its concurrency group keeps the
running job and coalesces pending arrivals. Coverage of an intermediate SHA is
optional under the [maintainer's CI direction](DECISIONS.md#my-41).

### Local merge proof

Under [MY-41](DECISIONS.md#my-41), merge when quick plus the change's targeted
tests pass. No independent review, review rounds, exact-head full gate per PR,
main-CI wait or gate-exception procedure is required. Run the full suite once
per release batch and fix breakage forward. Automatic main runs remain diagnostic
feedback; failures are work to fix, not a hold on unrelated merges.

Immediately before merging, run `scripts/gate-merge-check.sh` on the committed
branch (#1224). It fetches `origin/main` and first runs
`scripts/gate-revert-check.py`, which fails if the branch would restore any file
to an older version that a newer main commit changed. It prints each file and
the commits it would undo, so a stale tree committed onto a fresh tip cannot
silently revert merged work, as the holds commit did to #1356 and #1357. For an
intended revert, pass `--allow-reverts` and say so in the PR. The script then
writes the would-be merge with `git merge-tree` (no working-tree change), checks
it out at `.gate/merge-check`, and runs `go build ./...`, `go vet ./...` and a
Linux `go vet ./...`. If the merge conflicts or would break compilation or vet, rebase and rerun. The detector regression is
`python3 scripts/test-gate-revert-check.py`.

Queued or running main runs do not block a merge. Inspect optional results for
useful failure evidence and fix regressions forward; nobody waits on, polls or
wakes for main CI. Coalescing means an intermediate merge may have no main run.
The summary's tested SHA matches the event commit carrying the workflow check.

The workflow's own check reports success/failure. No commit-status writer or
personal access token is needed.

The [`main-full` workflow](../.github/workflows/main-full.yml) runs one
`myce run --entry NAME --key KEY --out DIR` per platform (#1341): the
`mycelium-gate-full-darwin` adapter entry for Darwin arm64 and
`mycelium-gate-full` for Linux, eligible on either amd64 or arm64. The adapter
freezes the checked-out event commit (`git: HEAD`) and keeps the entry's
targets, resources, environment, output contract and result check. The runner's
installed `myce` must include `run --wait/--out` (#1343). Linux is one chosen
target per run, not an architecture matrix. The temporary architecture
exclusion from [#527](https://github.com/1kuna/mycelium/issues/527) is removed after
the bounded native gate-like I/O measurement described in
[queue disk contention](reference/queue-io.md#queue-disk-contention-527). The ordinary
Linux full profile and converted adapter already included both architectures.
Their measured 6 GiB declaration and Darwin's `memory: auto` remain as documented
above; #767 did not establish an unconstrained Linux auto envelope.
There are no host pins; Mycelium chooses each worker and
provides the existing `mycelium-gate` environment. Both runs start together;
the checkout's local submission lock makes the second wait while the first
prepares (#1383). CI then waits for both, even if one fails. The entry's result check runs inside the
job, so success means `gate-result.txt` reads `gate full: exit=0`; `--out` then
downloads the accepted, size- and SHA-256-verified outputs.
The event checkout supplies its profiles, relative inputs and helpers together.
The runner freezes source/inputs, waits and verifies small accepted results; full suites never run on it directly. A submission exit 78 with `no_qualified_target`
is a qualification/total-allowance refusal, not a busy-worker indication. Busy
qualified hosts accept queued jobs and `myce run` waits for them. CI reports
`submission refused (exit 78)`; the qualification details stay in the private
run log. It does not retry an impossible allowance
or weaken the learned memory envelope. Darwin proves the native CGO target; Linux
proves its native builds and full race/dev suite.

The deterministic `main-ci:SHA:OS:RUN_ID:RUN_ATTEMPT` keys identify this
workflow attempt's two jobs for the event SHA. Repeating a submission within
that attempt adopts the same job, including after an uncertain submit. A workflow rerun uses fresh
attempt keys: an older attempt's cleanup cannot cancel its replacement, and a
cancelled key cannot poison a rerun. There is no automatic retry loop.
The CLI handles immutable source uploads, idempotent submission and reconnects;
there is no custom watcher, cursor or status publisher.

On SIGINT or SIGTERM the step forwards SIGTERM to each `myce run` by PID; `myce
run` detaches with exit 126 and leaves its job running. When the step fails
or is cancelled, a final step runs `myce cancel --wait --project mycelium --key
KEY` for both keys. This also fences a submission whose receipt was lost or which
reaches the queue after cancellation. Terminal suite results are preserved by the queue's existing
cancellation semantics. Queued gates lose their capacity-accumulation claim;
running gates follow normal stop/release custody. Cleanup never scans jobs by
age or cancels PR gates, another project's jobs, another merge or another attempt.

GitHub's six-hour job limit is an external owner deadline. The test step has a
345-minute limit, which leaves checkout and the five-minute cleanup step inside
the 360-minute job limit. These reserves trade up to fifteen minutes of possible
gate progress for cleanup before GitHub can kill the job. A timeout fails CI and cancels unfinished gates even if healthy;
it is not a new general gate watchdog. Cleanup reports only fixed confirmation
states; unconfirmed cancellation fails CI and requires private reconciliation
with that attempt's exact keys. An unavailable queue or abruptly lost runner can
prevent confirmation; repeat `myce cancel --wait --project mycelium --key
main-ci:SHA:OS:RUN_ID:RUN_ATTEMPT` for both platforms after authority is restored.
Only never-started concurrency-pending workflows are replaced. Both steps use the
event SHA for this attempt's keys.

Each `myce run`'s output goes to a private file in the runner's temporary
folder, which is emptied after the job. Inspect failures privately with `myce
logs JOB_ID`; public logs and the summary contain only job IDs, fixed states and
exit codes, never raw CLI receipts, remote reasons or host paths. A failed suite
shows the command's exit code, `failed (exit N)`.

A submission refusal or outage is an **infrastructure failure**, distinct from a
submitted suite reporting `failed`. Its platform row says `submission refused (exit N)`, or
`infrastructure failure` for exit 125 (wait, download or cancel failure after
acceptance) and exit 2 (an installed CLI without `--out`): that platform has no
verified suite result. Submission may have been accepted despite
a lost reply. Resolve the infrastructure problem privately, then
repeat cleanup for the original attempt. Optional CI failures are reported on
that run; later pushes test their own event commits rather than retrying historical failures.

### Public repository and private authority

The runner is registered to **this repository**, not an organization-wide pool.
Only code merged to main is eligible: the workflow has neither
`pull_request` nor `pull_request_target`, and fork/feature pushes cannot trigger
this repository's runner. Fork copies cannot address the upstream runner.
Under Settings → Actions → General, require approval for **all outside
contributors** (`approval_policy: all_external_contributors`). Keep that setting
when changing workflows. Repository administrators and main writers remain
trusted: validate workflow or submitter changes with quick and targeted tests.

The workflow grants only `contents: read` to the built-in `GITHUB_TOKEN` and
pins checkout to an immutable action SHA, with `persist-credentials: false`.
The service environment contains **no fleet token**. `myce` reads the host's
existing private installed worker/queue configuration and authority files;
credentials are not copied into the workspace, workflow, Actions secrets or
step environment. Start/configure the runner with a clean environment and no
`MYCE_*`, `GH_TOKEN` or `GITHUB_TOKEN` variables. Launch the service through
`env -i` with explicit HOME, USER, LOGNAME, PATH and locale settings, so it cannot
inherit credentials from the user service manager either. The workflow starts `myce` through `env -i`
with only HOME, PATH and TMPDIR;
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

Generated profiles and result markers live in job temp and are deleted after
owned work settles. Only small CLI recovery receipts remain in the event checkout;
GitHub terminates job-owned observers at job completion, then the next checkout
cleans old workspace state. Source-upload staging is removed by the CLI after
confirmed submission. Runner diagnostic logs stay private; do not upload them
as public artifacts. Use normal queue artifact retention for fleet outputs.

Validate workflow changes with the quick gate and a targeted run of the step
against a stub `myce` (pass, failure and SIGINT/SIGTERM forwarding). After merge, verify the push-to-main run
uses the registered self-hosted runner and reports both accepted platform
results and the tested event SHA before calling activation complete. No full
suite runs on a PR push.

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
| Runtime/resources | Verify native groups and sidecar cleanup and RAM/cgroup limits. Timeout cannot settle custody. |

Bound each observation and record command exits and effects, release/rollback
receipts and unrun checks. Stop on the first failed native gate and preserve the
recovery snapshot. An actual reboot or service interruption needs its own task
authorization. Finish drained/disabled unless the task explicitly authorizes
resume. There is no offline rest certificate.

Collect native telemetry for the exact selected device/catalog, including
representative overlapping loads and requests. Qualification stays UNQUALIFIED until native simultaneous
peaks, runtime overhead, sidecars and OS headroom are measured. For private models
with `logs: none`, verify actual null stdout/stderr, including recovery; tests
cannot qualify journald.

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
restart/rejoin, stale or missing reports, unknown native/engine custody,
cancellation and memory policy/debt. Queue decisions use injected
time. `TestDevProbeQueueRestart` exercises actual API outage, SQLite reopen,
lost-response keyed replay and cancellation without duplicate command effects.
`TestDevProbeRebootResume` restarts the real dev queue/workers with durable disabled
policy and requires fresh reports before quiescence. Process restart models the
protocol boundary; it does not claim an actual machine reboot.

Keep stop/recovery/result, cancellation/all-attempt release, engine and
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

<a id="comparable-accepted-work-slice-1-107-my-49"></a>

For comparable timing identity and current fields, see
[measurement mechanisms](reference/measurement-mechanisms.md#comparable-observations)
and [elapsed time](status.md#execution-elapsed-time-last-accepted-duration-and-application-progress).
The placement tests are `last_duration_test.go` and `completion_placement_test.go`.

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


## Fault evidence cost and native qualification

The device-fault monitor reuses its existing kernel cursor and attempt-memory
samples. It adds no periodic sampler. A scan with matching lines reads host
memory/page tables and attempt PIDs once per affected device; burst lines share
that observation with independent kernel timestamps. No host/kernel/core I/O
runs under the queue mutex. Queue attachment compares exact attempt/session
identities and bounded metadata only.

Each proc/journal section has a 64 KiB read/retention boundary: ordinary memory
tables are a few pages, while unusually large NUMA tables are explicitly marked
truncated. An envelope equal to one worst-case escaped four-table host snapshot (1.5 MiB)
bounds each device and current attempt's retained history; a conservative escaped-JSON size charge
avoids serialization under the mutex. Oldest events yield first; omissions are
visible. These bounds prevent driver storms/large proc tables from growing
control transactions with uptime; they do not gate healthy work or alter
placement. Capture durations are recorded per snapshot for native measurement.

Fatal-signal metadata queries reuse the monitor's two-second journal I/O budget,
only after a matching fatal exit and outside process/queue locks. The cost is
up to that budget in result latency; timeout reports missing evidence and never
blocks process-control locking. The exit is published before collection so the
teardown classifier sees it immediately. One small PID pipe preserves an
unwrapped plain command's child identity without tracing or modifying its
environment. Joined native members reuse the launch handshake's payload PID;
other wrappers need exact-attempt cgroup core metadata or report unknown. Core
queries cover the execution lifetime and match current-boot payload PIDs, allowing
core writing to delay Wait without losing earlier crash metadata. Systemd does
not export process births; kernel crash timestamps have second resolution.
Newest-first bounded journal reads retain recent evidence on long attempts. Synthetic
`BenchmarkFaultEvidenceCapture` measures parsing/attribution overhead, not GPU
sampling, real proc/PID I/O or native journal latency.

Laptop checks use injected clocks, kernel lines, cgroup files and wait statuses;
core records/maps are synthetic. Native acceptance, after an authorized release,
requires a naturally occurring fault: verify kernel/capture/sample alignment,
exact PID ownership, page counters, native query latency and capture availability.
For a natural signal exit verify any core pointer and privately recover stack,
argument registers and ELF build IDs. Do not induce GPU faults or enable cores
as part of the laptop/full gate. Current historical causes for #155/#300 remain
unknown; this capture is not a preventive fix or a benign classification.

## Generated status documentation

Run `go generate ./internal/statusdoc` after changing status types. The field
inventory in [status](status.md#generated-field-inventory) is derived from the
compact status, snapshot and job-page types and their reachable objects; edit
behavior explanations in [the reference](reference/status-behavior.md).
`TestStatusFieldsCurrent` runs in every quick gate, including docs-only changes.
Quick and full also run `python3 scripts/check-doc-links.py` to check local
Markdown paths and heading anchors in root documents, `docs/` and `skills/`.
Mechanism chapters linked from the approved design remain part of that design.

### Durable worker log batches

Workers replay up to sixteen existing 16 KiB offset records per
`POST /api/workers/log-batch`, with one SQLite transaction. The 256 KiB source
batch reduces a 5,670,640-byte replay to 22 requests/commits while bounding each
writer turn to sixteen log inserts. JSON escaping can expand text sixfold
(about 1.5 MiB plus record envelopes); the request memory budget still accounts
for wire buffers and decoded allocations before the queue lock. Record
construction, validation, JSON encoding/decoding and log-file reads run outside
that lock. The queue's SQLite transaction remains inside it.

A 404 on the new route falls back to the unchanged single-record endpoint.
A typed 413 splits a batch at its original record boundaries. The worker saves
its pending span before sending and advances only the acknowledged prefix;
a lost acknowledgement, growing log, restart or later smaller memory allowance
therefore replays the same bytes at each original offset. Old workers keep
using the unchanged endpoint with new queues. No database format changes.

`event=queue_stalled` records one typed JSON evidence object at the watchdog
return, with its branch, window, last progress and a bounded tail of eight
probe start/end times, RTTs, typed errors, p99, release counts and waiters.
`dropped_probes` counts older observations omitted from long calls. Unobserved
progress is marked `seen=false`; last-progress time then means request start.
This observes the existing watchdog; it does not change its liveness decisions.
`request_id` matches the `X-Myce-Request-ID` header and the queue's sampled
`event=slow_request` for both log routes, which includes job/attempt,
offset/length, status, request duration and mutex wait/hold. IDs must be opaque
32-character lowercase hex strings; arbitrary headers, URLs, credentials,
payloads and raw transport errors never enter these records. A probe can
report a stall as the main response arrives; a fully received success or
refusal still wins that race. Such an evidence record describes the watchdog's
observation, not proof that the log was uncommitted.
