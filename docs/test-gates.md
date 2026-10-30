# Laptop test gates

Run `./scripts/gate.sh` (or `./scripts/gate.sh quick BASE`) for a change. The
base defaults to `origin/integ/108`; select the commit before your change when
working elsewhere. Quick builds the CLI and every release target, vets all Go
packages, and race-tests changed packages with test names from matching file
prefixes, plus tests that directly reference changed exported symbols (using
diff hunk function names and changed declarations). This is a textual heuristic,
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
handles in scratch (macOS's built-in `lsof`, or Linux `/proc`). It remembers start
identities through TERM/KILL, including environment-scrubbed or reparented
commands, and signals individual PIDs. The old 175-second
deadline left only 19 seconds for builds/vet after an
observed 156-second related-test selection, and repeatedly truncated passing work
under shared load (#204). Five minutes keeps quick below the full gate's target
without reducing test selection. Quick tests use Go's `-timeout 0`: the outer
supervisor bounds all selected packages together, so a large related selection
can use the remaining budget instead of hitting an independent three-minute
package cap (#382). Selection, race instrumentation and concurrency are unchanged.
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
