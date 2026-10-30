# Laptop test gates

Run `./scripts/gate.sh` (or `./scripts/gate.sh quick BASE`) for a change. The
base defaults to `origin/integ/108`; select the commit before your change when
working elsewhere. Quick builds the CLI and every release target, vets all Go
packages, and race-tests changed packages with test names from matching file
prefixes, plus tests that directly reference changed exported symbols (using
diff hunk function names and changed declarations). Production Go edits in
`internal/myce` always include `TestWireSchemas`, since the reflected Job/Spec
schema includes nested types across that package. This is a textual heuristic,
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

On macOS, the bounded helper reads cwd, executable and standard-descriptor paths
from `libproc` without enumerating or statting mounted filesystems. Apple's lsof
has no network-mount exclusion and even scratch-scoped scans can block on an
unrelated mount. Native probe errors still retain scratch. Run cleanup regressions
with `python3 scripts/test-gate-budget.py`.

## Automatic main full gates

**Check main's latest status before merging** and fix a red main forward. Read the SHA and both contexts, not the overall combined state (which
can include unrelated checks):

```sh
gh api repos/1kuna/mycelium/commits/main/status \
  --jq '{sha, gates: [.statuses[] | select(.context | startswith("mycelium/full-")) | {context, state, description}]}'
```

Require `mycelium/full-darwin` and `mycelium/full-linux` for the current main SHA.
Missing, pending or error is not passing evidence. Run the targeted local gate
and obtain independent PR review before merging; agents do not poll or wake to
wait for main's full suites. A failed suite is fixed forward by the next merge
worker. Automatic main proof supplements the release full gate.

The optional [`scripts/main-ci.py`](../scripts/main-ci.py) fleet-side watcher
uses the existing `gate-full` profile at each exact main SHA. It narrows only its
source SHA and platform targets: Darwin arm64, and Linux amd64/arm64. Each gets
one submission, with no host pin; Mycelium chooses placement and supplies the
existing `mycelium-gate` environment. Darwin includes the native CGO build;
Linux runs its native builds and full race/dev suite. The profile's existing
result check determines job success. No GitHub-hosted runner, GitHub Actions
workflow, PR trigger, test schedule or model callback is involved.

The watcher checks main every 60 seconds and queues every first-parent commit,
including squash merges and direct main fixes. It does not run each constituent
commit of a merge. Bootstrap tests current main once; it does not replay the v1
history. A persistent cursor catches all intervening commits after downtime.
Every discovered SHA immediately gets the two pending contexts. One commit's
two platform jobs may be active at a time; later commits wait instead of
cancelling earlier proof. Consequently a busy fleet or merge backlog can delay
results beyond an hour; pending statuses make that visible.

Deterministic `main-ci:SHA:OS` keys and the CLI's existing receipts recover the
same submissions after watcher restart or an ambiguous submit reply. Existing
`myce run` detached observers handle queue reconnects, job completion, accepted
results and observer recovery; the watcher reopens the same receipt each pass.
Terminal states become public `success`/`failure` commit statuses, with job IDs;
infrastructure errors become `error` and retry the same key next pass. A failed
test is terminal and is never automatically rerun. No agent is invoked. Public
status descriptions contain no raw error output, host names, credentials or
local paths. Inspect a failed job through the authenticated Mycelium job/log
commands using the ID in its status. The watcher retains a small results ledger
and CLI receipts, removes generated profiles after terminal publication, and
relies on normal queue artifact retention. CLI source uploads are removed after
confirmed submission. Do not remove pending receipts or the cursor. A main
history rewrite stops discovery for explicit operator reconciliation.

### Approval and activation

**Prepared only; not activated by this PR.** Activation requires approval for:

- One quiet Linux fleet user service using the provided
  [`deploy/myce-main-ci.service`](../deploy/myce-main-ci.service) template,
  a dedicated non-shallow repository clone, Python 3.11+, Git, `gh`, an installed
  `myce` CLI and private persistent state. This is an observer/submitter; it
  does not run tests directly. The clone retains Git history for catch-up.
- One fine-grained GitHub credential restricted to this repository, with
  **Commit statuses: write** and mandatory **Metadata: read** only. Public Git
  fetches need no repository contents credential. Collect it through T3
  `request_secret` only after approval, then deliver it to a private `0600`
  environment file as `GH_TOKEN`. Do not put credentials in Git, service
  definitions, command arguments, statuses or job payloads.
- Access to the existing private installed Mycelium client authority on that
  submitter, including its verified queue identity. Prefer that configuration
  over another copy of the fleet token. If a new queue credential is necessary,
  ask before collecting it through T3 `request_secret`. Never print it.

No runner registration, host installation or secret provisioning is part of the
PR. After approval, the activation owner chooses an ordinary available fleet
submitter (this choice does not pin job placement), substitutes the service's
`REPLACE_*` paths and uses its private environment file to provide `GH_TOKEN`,
`PATH` and any existing client configuration required by `myce`. The script
removes GitHub credentials from Mycelium subprocesses and all `MYCE_*` settings
from Git/GitHub subprocesses. Its profile contains only the gate's authored
runtime environment; jobs must never receive the fleet token. Run as a trusted
service account: environment filtering is not isolation against hostile jobs
running under the same OS user.

First validate offline with `python3 scripts/test-main-ci.py`. After approval,
run one foreground observation pass with the same private service environment:

```sh
python3 scripts/main-ci.py --repo REPO --state PRIVATE_STATE --myce MYCE --once
```

This submits actual work and writes public statuses. Verify the two pending
contexts reference current main, then install/start the rendered user service
using the host's normal service management. Verify both terminal statuses and
accepted Mycelium job results before calling activation complete. Stop the
service to suspend new submissions; already submitted jobs and their observers
continue. Restart with the same clone, state, CLI receipts and credentials to
recover them. Do not run a second instance against a different state directory.
The state lock rejects concurrent instances sharing that directory.

Diagnosis recorded in [#425](https://github.com/1kuna/mycelium/issues/425): Actions
was enabled but had zero workflows and zero registered runners. The former
workflow was moved out of `.github/workflows` on June 3, then removed during the
v3 archive. Its final June run's hosted coverage step failed; its self-hosted GPU
job was cancelled without taking a step. These historical runs did not provide
today's Darwin/Linux full proof. Restoring that workflow would also restore
hosted/PR execution contrary to current policy.
