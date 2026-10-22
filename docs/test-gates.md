# Laptop test gates

Run `./scripts/gate.sh` (or `./scripts/gate.sh quick BASE`) for a change. The
base defaults to `origin/integ/108`; select the commit before your change when
working elsewhere. Quick builds the CLI and every release target, vets all Go
packages, and race-tests changed packages with test names from matching file
prefixes. If a prefix has no tests it runs that package. Review the printed
selection: changes to shared helpers can need a wider package run or `full`.
Quick is feedback for the selected change, not evidence that every test passed.
Module dependency changes fall back to all packages.

Run `./scripts/gate.sh full` once for a batch before deployment. It runs the lean
full race suite once, including dev checkpoint/resume, model and enrollment
end-to-end tests. It does not repeat those packages in a second race pair or a
separate dev run. For an authorized urgent hotfix: run quick, deploy, run full
afterwards, and fix forward if full finds a problem. This workflow does not grant
fleet/deployment authorization.

The quick target is three minutes; the full target is ten minutes. Timings
include build, vet, the three release target builds, test execution and scratch
cleanup. A package fallback may take longer than a small matching test set.

Go 1.25.6, a native macOS arm64 toolchain (for the CGO release target), `uv`,
Python 3, and `/usr/bin/trash` are current laptop prerequisites. Install
prerequisites separately: the gate runs offline and never downloads tools or
modules. Seed dependencies with your usual `go mod download` before running a
first gate. Caches are copied into an ignored `.gate/run.*` workspace; HOME,
TMPDIR, GOCACHE, GOMODCACHE and GOPATH all point inside it. On APFS this uses
copy-on-write clones. The owned workspace goes to Trash on exit, so this is
recoverable cleanup, not physical disk reclamation. Failure logs belong in an
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
