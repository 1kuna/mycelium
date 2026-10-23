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

Quick has a hard three-minute wall deadline, including cache setup and builds;
on expiry it stops its process group and suggests `./scripts/gate.sh full`
(exit 124). It sends TERM at 175 seconds and KILL after five seconds if needed.
Interrupted cleanup may leave its ignored scratch folder for recovery. The full
target is ten minutes. Timings
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

A faster Linux runner and submission of the full gate as a Mycelium CPU job
remain proposals. Linux cannot build the native Darwin CGO target without the
appropriate cross toolchain, so an SSH flag alone would not reproduce this gate.
