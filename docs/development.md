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
selects unrelated changes that landed after the branch diverged. Development gates run directly on the development host, independently of fleet
nodes and queue availability. On Darwin, the default gate includes native Darwin
and a directly invoked local Linux VM. Inspect load/memory/disk before heavy work;
use the capacity admission below rather than creating additional test processes.

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

The local gate trims its dedicated Go build cache to 12 GiB at every entry and
exit, including overlapping runs. It evicts oldest Go object files only; Go
rebuilds them on demand. Concurrent compilation can temporarily grow the cache
between these scans. Module downloads, timing and compatibility records, and
custody receipts are retained; symlinked directories are never traversed.
After affirmative cleanup, successful and failed runs both remove their frozen
source tree and source/runtime archives. Unconfirmed cleanup retains the tree
for diagnosis without retaining admission; verdicts and receipts remain available in either case.

## Laptop test gates

On the 64 GB development host, install Lima and prepare the local Linux VM once
using [local gate setup](#local-linux-vm). Then the normal commands prove both
platforms without `myce run` or fleet credentials:

```sh
./scripts/gate.sh quick
./scripts/gate.sh full
```

`GATE_TARGET=native` runs only the current OS, including inside fleet profiles.
Use it for focused diagnosis or a separately owned baseline; record that it is
partial platform proof. On Linux, the gate runs natively. Darwin CGO still needs
a native Mac toolchain.

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

Full partitions both `internal/myce` and the CLI into balanced chunks. It aims
for eight refill waves per process slot, stopping when the longest indivisible
root sets the lower bound, and starts the longest estimated chunks first. The
active process limit is the available CPU count, capped at nine, with
`GOMAXPROCS=1`, `GOFLAGS=-p=1` and `-parallel=1` per process. One-core allocations
run each package intact. Quick selection and its parallelism are unchanged.
Cross-release upgrade compatibility starts in one of the same slots, alongside
the test chunks, and its failure contributes to the final gate verdict. An
unconfirmed cleanup fails that gate and retains diagnostics while releasing local admission.

Each gate builds one race-instrumented `myce_test` CLI for its dev journeys and
shares its absolute path through `GATE_TEST_CLI_BINARY`. Ordinary `go test` builds
the helper on demand. Core shards also share the external non-race Docker
simulator through `GATE_TEST_DOCKER_BINARY`; worker/adapter assertions remain
race-instrumented. Each watchdog copies those images into its private temporary
root so executable-path cleanup still identifies environment-scrubbed fixtures.
Upgrade fixtures retain separately linked versioned images:
the signed artifact identity and embedded version remain part of their proof.

The planner asks Go for the current race-enabled inventory, including runnable
examples and fuzz seed tests, under the same fixture watchdog used for tests.
Every root runs in exactly one anchored `-run` partition; descendants stay with
their root. `scripts/gate-test-durations.json` contains measured scheduling hints
for balancing work, never a coverage list. New or unmeasured roots always run.
Each partition retains independent HOME, temporary files, process custody and
progress learning; the shared Go build cache avoids recompiling the package for
each partition. The scheduler writes its native selection manifest beside the
temporary selection file, and prints each partition in the gate log. Run
`python3 scripts/test-gate-shards.py` for the planner regression checks.

Quick and full have no overall wall deadline. Each Go package runs under its
own test-progress watchdog, keyed by package, exact selected test expression,
OS/architecture, Go version and available/assigned CPU count.
The full scheduler also keys those timings by shard concurrency, since each
individual shard uses one Go CPU at every allocation size. Timings live beside
Go's execution-host build cache (`mycelium-gate-timings`), shared across checkouts
on that host, and every invocation appends a sanitized record to
`gate-timings.jsonl`. Fleet profiles return that report with their result marker.
A new selection/toolchain/CPU configuration learns independently; a small quick
selection cannot shorten a full-package deadline.

A learned package stalls after **three times its expected package duration with
no test lifecycle progress**. Expected duration is the latest completed successful observation
for that host/configuration/selection. Three intervals allow the normal expected
completion plus two missed completions for transient contention; the cost is up
to three expected durations to detect a test that stops advancing. This is an operational
tolerance, not a statistical confidence claim or a wall-time limit. Go JSON lifecycle events (`start`, `run`, `pause`, `cont`, `pass`, `fail`,
`skip`) reset only that package's progress clock. Diagnostic `output` events,
including reconnect warnings, do not renew it; a stalled report names unfinished
tests. Non-test commands retain output-based observation without learned deadlines. A run can exceed any previous
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
longest progress gap, unfinished tests and status, including failed/stalled runs; only successful,
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

Local development admission uses an atomic directory per heavy run, shared
across Git worktrees and ordinary clones under
`~/.cache/mycelium-gates/control`. It registers legacy checkout report roots,
recognizes both existing `local-go-slot` lanes and
preserves unknown/live ownership and automatically reclaims dead holders. Qualified hosts admit full and quick runs
from their measured per-leg CPU/RAM and live capacity, rather than a fixed full
count. The four-core qualification observed native peak 7.01 GiB and Linux/VM
peak 10.57 GiB. With the measured 5.43 GiB host-memory variation reserve, single-run
gate-memory requirements are 12.45 GiB native and 16.01 GiB Linux. These observations
are host qualification, not constants for other installations. These requirements
are checked against live available RAM, which already reflects fleet-worker
and other background residency. No fleet-worker RAM allowance is added, and no
worker RSS is subtracted again. Live pressure and swap guards remain active.
Full admission reserves each active run's entire measured peak on top of the new
run's peak and shared headroom. This conservatively counts current RSS again
to cover later growth without assuming shared pages are reclaimable. CPU budgets
sum across runs; Linux demand also fits the VM's configured CPU/RAM capacity.
Additional full-test slots require a digest-bound native and Linux/VM memory
measurement. Exclusive single-platform calibration can collect that evidence;
its provisional memory envelope scales the existing platform measurement.

`--platform both` launches the native and local-VM legs together from one frozen
source. Full gates default to eight test slots **per platform** when the
`full_slot_measurement` evidence qualifies both platforms and live available
memory covers their combined measured peaks, active reservations and existing
headroom. Otherwise they use four. Selection is repeated under the admission
lock while waiting; it never launches one leg first to squeeze into RAM.
`--cores 4` explicitly retains four slots; quick gates still default to four.
The eight-slot profile contains `slots: 8` and digest-bound `evidence` entries
in the same format as `memory_measurement.evidence`; it introduces no RAM limit.
The eight-slot envelope uses the maximum native RSS in its identified evidence,
at least the four-slot native peak, and retains the maximum measured
host-transport and VM RSS. Use representative current build/shard observations
when replacing older evidence. The four-slot fallback uses the native observations made at four slots,
instead of charging it for the historical eight-slot native peak. Both retain
the measured VM residency and shared headroom. Failed test verdicts are valid resource samples;
refused or incomplete observations cannot qualify a profile.

Test slots are concurrent processes, not CPU isolation. A measured eight-slot
full overlaps I/O waits within the existing qualified CPU share per leg;
admission continues charging that entire share, including the fleet/workstation
reserves, so later gate work cannot occupy it. CPU use may burst above that
share; the slot comparison and full wall measurement qualify the tradeoff.
The owner receipt records slots and CPU share separately. VM slot and residency
checks and live pressure/swap guards remain in force. Admission sums the measured
working sets plus the existing shared headroom; native memory is not charged to
the VM. Each leg retains its own cleanup receipt. Ordinary test failure collects
both verdicts; a signal or unconfirmed cleanup cancels the peer and retains
uncertain custody. Both-platform elapsed time measures their overlap, rather
than adding the two leg walls. Concurrent wall improvement is measured in the
release-batch full, not inferred from individual peak RSS.

Full-gate chunk weights use platform-specific passed-root timings when available,
with the generic hints and a default weight for new roots. The planner targets
eight refill waves, stopping at the longest root's measured duration: smaller
chunks than that cannot shorten the indivisible root and add process startup
cost. Whole root names remain anchored exactly once, including their subtests;
`QueueHistoryBurst` keeps its shared 1k/10k comparison in one process. Hints never
remove coverage. Chunk counts adapt to current inventory and measured work.



Under MY-41, warning memory pressure alone does not stop admission or running
work. Missing observations log a warning and continue; they are not evidence of
critical pressure or swap growth. Complete process-table telemetry uses the
30-second observer allowance; timeouts retain their command/error in the log. Critical pressure, actual swap growth and the disk floors still
stop work. Quick and focused requests can use a separate
`quick_memory_measurement` containing digest-bound successful quick observations
and their maximum `peak_bytes`. The development-host observations peak at
2.78 GiB across 39 successful quick phase records, versus 7.01 GiB for fulls.
Native quick charges that measured peak plus shared host headroom; Linux also
charges measured VM residency. Live available memory already reflects other
running processes, so quick does not charge their entire peaks again. This
avoids the roughly 30 GiB reservation for a small selection. Missing quick
evidence falls back to the full envelope. This optional quick envelope may be
refreshed while runs are active: existing reservations keep their original
charges and only new requests use the new measurement.

All requests keep their FIFO tickets. When the head lacks live available memory,
the first later request whose measured memory envelope fits may take one admission,
subject to every existing CPU, RAM, VM, custody and disk guard. That one bypass is
recorded durably on the head's ticket before the new owner is published. No further
request may overtake that head, even after the fitting run exits, so a continuous
stream of smaller work cannot starve it. The cost is waiting behind the head again
after one useful run if external memory remains unavailable. A head that fits,
unknown or changed calibration, and exclusive timing drains retain strict FIFO.
Automatic fulls advertise their smallest eligible slot envelope; later callers
recheck current full reservations rather than trusting an earlier available-RAM
sample. This addresses measured head-of-line stalls with no active local gates,
without lowering an envelope or changing a resource guard.

Local admission automatically archives a slot or custody record when its holder
PID is absent (including a zombie), or its recorded start identity differs from
the live process. The host admission lock serializes reclamation and new
reservations across checkouts. Live or unverifiable holders remain protected;
never-admitted waiters are queue tickets, not supervisors. No global quiet,
Docker/guest inventory or manual reconciliation is required to release dead
holder capacity. `--reconcile` remains a compatibility entry point for the same
automatic operation.

Cleanup and inventory failures produce exit 125, a negative cleanup receipt and
`cleanup-error.json` with the exception and traceback. They release admission
anyway, including when a Docker daemon becomes unreachable after creating a
container. Current labelled fixtures journal daemon use before creating resources;
a gate that never used Docker needs no daemon cleanup. A real cleanup failure
never becomes a passing verdict. Source, logs and verdict receipts remain in
the original run directory. Native process identity, marker, ancestry and handle
checks still protect live fixtures; recovery never signals another run.
Complete handle inventories consider process instances born no earlier than the
holder: a fixture cannot predate the process that created it. Older system
session helpers therefore cannot turn dead scratch into an unknown inventory.

Dead scratch is renamed into `selfheal-backups` beside its original `.gate`
directory, on the same filesystem. Its archived custody record identifies the
backup. At subsequent admissions and gate exit, backup collection keeps at most
eight runs and 2 GiB of rebuildable scratch, expiring it after 24 hours. These
bounds provide a short debugging window without accumulating abandoned test
homes. A live fixture or unavailable scratch inventory preserves scratch in
place; later invocations retry its collection after the fixture exits. Protected
live scratch is excluded from the disposable backup bounds; that observation
does not reserve capacity. Unknown holder records are
preserved rather than guessed dead. Production job custody is outside this
local development-tool recovery.

Unqualified hosts retain two heavy lanes and one full. Qualified hosts retain
measured CPU/RAM admission and compatibility guards for older helpers. Guards
leave once all execution reservations release or are reclaimed. Configuration
and evidence digests still bind each live resource group. New reservations
persist PID start identity and planned scratch before launching work. Termination
signals cannot interrupt the budget's cleanup or receipt publication.

The installed local fleet worker can coexist with local gates. CPU admission
adds the static `fleet_worker_cpu_reserve_cores` from private local gate config,
declared local gate cores and the host-qualified workstation reserve. The
configured fleet share is 4 cores on the qualified development host, matching
its admitted worker CPU budget when set. An operator updates this share when
changing that worker budget; do so after local resource custody has cleaned up,
under the host admission lock. Drift can overcommit CPU and slow work; the
orchestrator accepts that tradeoff because CPU overcommit is not a safety hazard.
Admission performs no fleet status reads, allowance caching or generation fencing.
There is no queue/network dependency in this path. Receipts record the static
fleet/workstation CPU shares and available/required/physical RAM. Worker
process-tree CPU/RSS may be recorded as observations and do not determine demand.
RAM admission uses measured per-leg requirements against live available memory,
with normal-pressure waiting and live pressure, sustained-swap and disk stops.
Installed-worker fleet custody is separate from local gate lanes and is never
modified by local admission. Unknown/local native full custody remains binding;
all custody still blocks idle cache eviction. Ordinary admission does not require
an idle host.
Swap-baseline calibration excludes local gate lanes, while the fleet worker and
its current job remain measured background. Record that job and its usage with
the baseline; do not wait for the fleet worker to become idle.
All local requests share one FIFO and print their queue position: quick, full,
focused commands, release builds and calibration. Later requests cannot overtake
an earlier waiter. A FIFO head reserves the next opportunity; smaller later runs
wait even when they could fit, allowing headroom to accumulate for the oldest
request. This prevents the observed starvation of ordinary quicks by successive
release-labelled requests. Quick requests always use normal admission, including
when `--priority release` or `GATE_PRIORITY=release` is supplied. Release labels
on fulls also give no queue precedence; calibration labels only qualify isolated
measurements. Waiting tickets encode the older exclusive-calibration drain so
frozen older callers also honor the FIFO. That compatibility encoding reserves
no execution capacity and grants no priority to the admitted gate.
No polling or relay is needed. Ctrl-C/SIGTERM cleans a waiting ticket; a killed waiter is
removed only after OS process identity proves it has exited or is a zombie. Execution slots
and unknown holder custody are separate from that queue cleanup. There is
no queue timeout. With normal pressure, insufficient available memory is
transient capacity: keep the FIFO ticket and print available/required GiB until
memory frees. Active-run future peaks remain reserved. Disk below the floor,
non-normal pressure and sustained swap remain real stops; a single-run memory
requirement exceeding host physical memory refuses instead of waiting forever.
A reserved calibration full can wait for an identified comparison
batch to exit without entering gaps between that batch's runs.
To measure a higher native CPU count, use
`gate-local.py full --platform native --cores 8 --exclusive --priority calibration`.
Only this exclusive native full may exceed the existing measured count; its
provisional memory charge scales the measured native peak by the CPU ratio,
retains host headroom and the live guards, and labels the receipt as calibration.
It does not change admission's enabled profile. After comparing the same source
at the old and candidate counts, a faster healthy measurement can enable
`resource_admission.native_measured_cores`; its digest-bound evidence must include
an exclusive native full at that count. Linux retains its separately measured
count. Release work finishes first; do not queue this exclusive calibration
before an in-flight release's Mac leg has released capacity.

For comparable timings, `./scripts/gate.sh full --exclusive` (or
`gate-local.py full --exclusive`) drains all local gate work and reserves all
local capacity until verified cleanup. A queued exclusive waiter also blocks
later work through the same FIFO, so it can drain. Permanent fleet jobs and other
projects remain measured background; exclusive is not a promise of an idle
workstation. Release builds use the same admitted command wrapper (use an
absolute output path because source runs in a snapshot):

```sh
python3 scripts/gate-local.py --command \
  go build -o "$PWD/.reports/release-myce" ./cmd/myce
```

A run needs 45 GiB disk free at entry. The host measurement supervisor remains
active through source preparation, Linux staging and guest execution, stopping
its owned supervisor below 42 GiB. On macOS, non-normal memory pressure stops
owned work immediately. A qualified idle-host swap baseline supplies the peak
background swapout and swap-use growth rates and the observation interval.
Two consecutive intervals above the background swapout rate, or with swap use
growing above baseline alongside the gate/VM working set, stop owned work.
Two intervals are the minimum evidence of continuation rather than one counter
batch; the detection cost is two measured sampling periods. An isolated batch
under normal pressure does not prove thrashing. Each receipt records the baseline,
its evidence digest and the observed windows. Hosts without that qualification
retain the strict new-swapout stop. Checksum-pinned calibration inputs are copied into gate-owned state when loaded;
removing a source worktree does not remove its qualification evidence. Missing
evidence drops that calibration with a warning and uses default admission
after active calibrated owners finish. A checksum mismatch still refuses.
Missing nonce-bound cleanup receipts, SSH failure
or supervisor death retain capacity; exit status alone never releases it. These floors preserve room for the workstation
and recovery after the 2026-11-02/03 saturation incident.

Darwin development uses a gate-owned Go build cache shared across checkouts.
Only idle gate-owned build objects are automatically evicted above 12 GiB;
module downloads, compatibility sources and timing history are retained. Linux
uses persistent guest build/module caches and the same idle eviction policy.
No other project's cache is trimmed. Qualified hosts use a private
`gate-capacity.json` memory measurement: native sampled peak plus observed
available-memory variation as headroom; Linux/both use the larger of native
peak or VM residency plus host supervision, and the same headroom. Guest RSS
is already inside VM residency and is not added again. Concurrent admission
verifies digest-bound completed native/Linux measurement evidence. Keep the
evidence paths/digests private.
A profile-declared Linux need is explicitly provisional until a real four-core
Linux measurement replaces it. Unqualified hosts retain the initial 22 GiB
entry value (16 GiB starting reserve plus profile-declared 6 GiB); that fallback
is not a measured execution envelope. Compressed pages receive no free-memory
credit. The 12 GiB build-cache ceiling is an initial storage budget;
`.reports/gate-local.jsonl` records actual growth.

The guest guardian publishes custody before cache readiness, and channel EOF
revokes its owned work, including when the host transport dies. Guest cache use
also has a durable nonce marker: supervisor death cannot make cache users look
idle merely by releasing a descriptor. An unknown marker prevents automatic
trimming until its exact cleanup is reconciled. Helpers are frozen once with
product/test sources; both legs receive those identical helper bytes.

Host-admitted native runs verify their canonical lane, source identity and
admitting ancestor before replacing the legacy suite-count lock with measured
resource custody. The admission identity is removed from fixture environments.
Native-only local fulls publish a host-wide custody reservation under the same
admission lock, including when using a custom build cache. They cannot enter an
active measured resource group; their reservation also blocks group creation.
Fleet jobs keep their separate custody and remain background. Native-only
legacy/fleet Mac gates retain the following custody mechanism.
On macOS, the full supervisor holds one of two cross-checkout filesystem locks
for its entire lifetime, independent of package commands or test expressions. It also
accounts for older standalone full race commands before entering a slot. The
slot also has a `mycelium-full-gate-N.custody` record beside its lock in
`GOCACHE`, containing the holder PID and process start identity, then the run
marker and scratch directory before fixtures start. Records publish atomically.
Normal cleanup releases the record even if fixture or daemon inventory fails;
the failed verdict and diagnostic remain. At the next admission, a dead or
reused holder is archived automatically under the admission/slot locks. Scratch
collection has the separate live-process and bounded-backup checks described
above. Old marker-only records without a holder identity still require scoped
fixture-exit proof. Live or unverifiable holders remain protected, without a
global-quiet or manual-reconciliation step. Fleet custody remains separate.
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

`BenchmarkReadStages` compares the legacy capture/queued planner with published
immutable inputs on the same #1030 fixture (160 or 6,500 retained constrained
executions, 110 queued jobs, 1,400 curves and five occupied workers). Run it with
`-run '^$' -bench '^BenchmarkReadStages$' -benchtime=20x`. It reports capture,
derivation, JSON encoding, live-mutex hold, allocation and response bytes for
default/full status and queued/running/terminal receipts. Capture includes mutex
hold; do not add it twice. First materialization is included, and later reads
reuse the generation. This isolates projection from usage/file-store I/O and
transport; it does not establish production tail latency.
`TestReadSnapshotBulkWorkDoesNotHoldQueue` blocks record decoding while a real
control commit must complete, proving lock separation without a machine-tuned
timing threshold. Existing terminal-commit, cache, custody, redaction and
contention checks remain unchanged.

### Submission advice attribution (#1065)

`BenchmarkSubmissionAdvice` uses the same incident-size retained history (6,500
constrained executions, 110 queued jobs, 1,400 curves, five synthetic workers).
It reports marginal commit-side snapshot capture mutex time separately from
post-commit capture/decoding, waiting reasons and deadline guidance. The
`submission_advice` telemetry event reports those advice phases after unlock;
existing update telemetry continues to measure the durable transaction.

Before isolation, a native ARM64 fixture with `GOMAXPROCS=2` and three measured
calls spent 61.31 ms/op holding the live mutex (16.93 ms wait reason, 42.94 ms
status waiting reason, 1.44 ms deadline guidance). After isolation, marginal
snapshot pinning takes about 3.9 microseconds/op inside the commit, with no
post-commit acquisition. Warm advice takes 10.24 ms/op (8.12 ms capture/charged
active copies and catalog reservation, 0.82 ms wait reason, 0.64 ms status reason, 0.60 ms deadline
advice), versus 61.39 ms/op before. Retained payloads are borrowed read-only;
only active jobs, worker ledgers and private planner caches are owned. One cold
decode measured 1.72 seconds outside the live mutex; it happens once for
unchanged immutable records across commits. The benchmark reports that cold
sample separately from steady-state costs. This is fixture evidence, not a live
tail claim; cold decoding remains visible and there is no history cutoff.

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
one pinned predecessor under [MY-42](DECISIONS.md#my-42). The r6.11 candidate
pairs with r6.10 (`302620cacaace8a16e7ed424e1183dbb10f46414`) as its only
historical source. Advance `PREVIOUS`, `OLD_VERSION` and `VERSION` together for a
release, along with `internal/myce/testdata/r6.10-queue.sql` and its generator.
`prepare` fetches that exact public source before the offline gate; local gate
snapshots carry only that predecessor archive.

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

The `gate-quick` profile runs the quick gate the same way. A submitted source
archive has no Git history, so compute the related-test selection locally and
send it as a job input; the job runs `gate.sh quick --selection FILE` in place
of the Git diff:

```sh
mkdir -p .reports
python3 scripts/gate-related.py "$(git merge-base HEAD origin/main)" > .reports/gate-selection.tsv &&
  myce run --profile gate-quick --input gate-selection.tsv=.reports/gate-selection.tsv
```

That submits one Linux leg. When the development host can't run gates
(MY-37), use `scripts/gate-fleet-quick.sh [BASE]` instead: it computes the same
selection and submits it to `gate-quick` (Linux) and `gate-quick-darwin`
(darwin/arm64, placed by Mycelium on any Mac worker), then `myce wait`s both and
exits 0 only when both legs succeed. The Darwin leg runs the same
`GATE_TARGET=native` quick gate as the local native leg, including the native
CGO darwin/arm64 build, so a fleet quick is two-platform proof. Fleet legs
(`GATE_FLEET_PROFILE=1`, set by `scripts/gate-profile.sh`) skip
`scripts/test-gate-local.py`: they never run `gate-local.py` or its host FIFO
admission, and those tests probe the executing host's `ps`/`sysctl` latency
(a busy fleet Mac timed out child registration). Every local gate still runs them.

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

## Local Linux VM

Install `lima` through Homebrew. This headless Ubuntu VM uses Apple's native
virtualization, needs no Docker Desktop UI and can be stopped without changing
fleet services:

```sh
limactl create --name=mycelium-gates --tty=false .mycelium/lima-gates.yaml
limactl start --tty=false mycelium-gates
# First provision installs HWE; boot the installed kernel before any test work.
limactl stop mycelium-gates
limactl start --tty=false mycelium-gates
limactl autostart enable --condition=login mycelium-gates
```

The checked-in template installs Ubuntu's supported HWE kernel, `bubblewrap`
and `nsenter` from the start. The GA 6.8 kernel is insufficient: native
isolation requires **Landlock ABI >= 6**, plus the actual confinement probe
described in [operations](reference/native-execution.md#native-payload-isolation). A kernel
version alone does not qualify confinement. Keep the previous kernel; only
restart an existing VM after verifying it is idle. Never weaken isolation or
disable the global AppArmor restriction to make a gate pass. Subsequent starts
reuse the installed prerequisites without repeating package installation.

The 80 GiB disk is sparse. Provision Go 1.25.6+, GCC, Git, Python 3, uv and
`uv python install 3.11` inside the VM; install Docker separately for real-daemon
focused tests. Follow upstream checksum verification for the Go archive.
Both platforms receive the same immutable snapshot of tracked and unignored
source files, including uncommitted edits; host HOME/NAS directories are not mounted. Compilation and
fixture I/O use the guest-native filesystem. Persistent guest caches are
`~/.cache/mycelium-gates/go-build` and `~/.cache/mycelium-gates/go-mod`.
Preparation downloads dependencies before the offline gate. `GATE_VM` selects a
separately provisioned Lima instance. The default gate CPU budget is four cores per platform leg. Run
`./scripts/gate.sh full` (or `python3 scripts/gate-local.py full --platform both`)
to launch native and Linux concurrently from one snapshot after combined CPU/RAM
admission. It waits for capacity rather than falling back to sequential legs.
Separate `--platform native` and `--platform linux` invocations measure separate
legs; a shell loop around them adds their walls.

### Guest disk budget and reclamation

The template's 80 GiB size follows the November 10, 2026 Linux full-leg disk
samples at source `a313cfa6ed201f9b7ce8157dcffdabb38003cd67` (793.45 seconds).
The 100 GiB virtual disk provided a 95.82 GiB root filesystem. Its lowest
available space was 74.99 GiB: at most 20.84 GiB was unavailable, including
filesystem reserves. Available space fell 2.52 GiB from entry to its low point.
Reducing virtual capacity by 20 GiB projects 54.99 GiB available at that peak,
9.99 GiB above the existing 45 GiB entry floor and 12.99 GiB above the 42 GiB
stop floor. This leaves roughly four observed transient-growth intervals above
entry admission. The sampled full failed tests; these are disk observations,
not passing-suite qualification or a bound on future growth. Re-measure after
toolchain, fixture or concurrency changes using the Linux row's
`min_free_bytes` in `.reports/gate-local.jsonl`, alongside guest filesystem size.

Virtual capacity and host allocation are different: changing the template does
not shrink an existing VM, and an 80 GiB sparse disk need not occupy 80 GiB.
First inspect `limactl list`, host gate custody and guest active markers. While
idle, trim the guest's gate-owned Go objects with the frozen/current
`scripts/gate-cache.py` under the guest cache admission lock; preserve module
downloads, timing evidence and compatibility material still in use. Do not
delete retained test output or unknown guest files as cache cleanup.
Then return unused filesystem blocks through the supported guest operation:

```sh
limactl shell mycelium-gates -- df -k /
limactl shell mycelium-gates -- lsblk -D
limactl shell mycelium-gates -- sudo -n fstrim -v /
```

The measured VZ disk advertised discard and accepted `fstrim`. Its trimmed-byte
count describes free guest extents, not bytes necessarily reclaimed on the host;
compare host allocated blocks before and after. Never truncate or rewrite the
VM image to reclaim space. If a smaller virtual capacity is still needed,
recreate from the checked-in template only after no gate is executing and
unique guest material has been preserved. Retain the toolchain versions and
verified upstream installation inputs, repeat the setup above (including the
HWE reboot), restore the documented prerequisites, and run a focused Linux gate
before admitting the next full. Under MY-41 this is development-tool maintenance;
it does not authorize interrupting running production worker jobs.

### Focused Linux commands

For a focused Linux command with the same host admission, disk supervision and
fixture cleanup:

```sh
python3 scripts/gate-local.py --platform linux --command \
  go test -race -count=1 ./internal/myce -run '^TestName$'
```

`--repo DIR --ref SHA` selects committed product/test bytes from another checkout.
For exact-head release proof, use:

```sh
python3 scripts/gate-local.py full --repo DIR --ref SHA
```

`gate-linux.py` is the transport helper; call the admitted wrapper above for work.
Instrumentation helpers are overlaid identically on both platforms and identified
separately from product/test sources. Compatibility predecessor archives are
created from the actual Git root and checked for their commit and module/core
contents before either full leg. Failed source snapshots remain for diagnosis;
confirm fixture exit before removal. Success removes only run source/scratch.
Explicit refs resolve once to a commit SHA before archiving. Working-tree runs
verify captured bytes, executable modes and HEAD before testing, and mark any
difference from the pinned commit as dirty. Inherited `MYCE_*` authority is
removed before the first admission or measurement subprocess.

Each run prints its private `.reports/local-gates/UUID` receipt directory. Retain
`manifest.json`, `source-identity.json`, and nonce-bound `native-cleanup.json`,
`linux-cleanup.json`, `overall-cleanup.json`. The append-only
`.reports/gate-local.jsonl` schema 1 has a common `run_id` and `phase` of `host`,
`native` or `linux`; rows identify `source_sha`, `source_dirty`,
`source_tree_sha256`, `source_archive_sha256` and `instrumentation_sha256`. A clean commit plus matching
archive/instrumentation digests identifies exact-head proof; HEAD alone does not.
Rows record wall/user/system seconds, sampled RSS sum (shared pages may count
twice), disk/cache deltas, host pressure/swap observations, all VZ process RSS and
foreground process-launch latency. Guest RSS and host VM RSS are separate
observations. The host row includes preparation and both legs. Launch latency is
a workstation responsiveness proxy, not a UI latency guarantee.

Stop with `limactl stop mycelium-gates`; disable login startup with
`limactl autostart disable mycelium-gates`. Before deletion, verify no guest work
or unique evidence remains. `limactl delete mycelium-gates` removes its disk and
caches. The existing native Mycelium worker and live fleet policy are independent.

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

The [`main-full` workflow](../.github/workflows/main-full.yml) provides
**coalesced, optional evidence of main commits**. It runs only on pushes
to `main`, on `[self-hosted, mycelium-ci]`; there are no PR, manual-dispatch or
schedule triggers. Its workflow/ref concurrency group keeps the running member
and replaces an older pending member with the newest pending member
(`cancel-in-progress: false`). Started runs are never cancelled to reduce backlog:
runner cancellation cleanup previously killed shared helpers
([#919](https://github.com/1kuna/mycelium/issues/919),
[#1159](https://github.com/1kuna/mycelium/issues/1159)). See
[GitHub's concurrency contract](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency).
The deliberately disabled server-side workflow remains disabled; this change
does not authorize enabling it or triggering a run.

Each retained run checks out and tests its event's own commit (`github.sha`),
so GitHub attaches the result to the same SHA that was tested. The summary
records that tested SHA, always equal to the event SHA. The concurrency group
retains the newest pending **arrival**, not necessarily the newest commit:
GitHub does not guarantee event order. In the rare case that a delayed B event
replaces pending C, B may be the last commit tested. The orchestrator explicitly
accepts this because main CI is optional evidence; pre-merge exact-head proof
remains the per-PR guarantee. Runs do not resolve or chase the current main tip.

Zach, 2026-11-09, relayed by the orchestrator and received by the CI implementation
worker: “yes skip CI runs i think. we should be running the tests where it makes
sense not repetitive nonsense that just wastes compute for no gain.” The
orchestrator's scope decision after review round 5 withdraws the review-imposed
requirement to prove every intermediate main tree in CI. **Each PR's pre-merge
exact-head local proof is the per-PR guarantee.** Main CI is optional additional
evidence; coalesced intermediate commits need no separate main run. A failed or
missing optional main run does not replace or waive pre-merge proof.

Historical runner-capacity evidence (before coalescing): the change in [#751](https://github.com/1kuna/mycelium/issues/751)
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

That historical estimate assumed overlapping main runs. The concurrency group
now admits one workflow at a time; existing runners are retained without service
changes. That earlier throughput estimate does not describe the serialized
workflow. The submitter still waits cheaply through `myce wait` and verifies
accepted results rather than reporting success immediately after submission.

### Local merge proof

Under [MY-41](DECISIONS.md#my-41), merge when quick plus the change's targeted
tests pass. No independent review, review rounds, exact-head full gate per PR,
main-CI wait or gate-exception procedure is required. Run the full suite once
per release batch and fix breakage forward. Automatic main runs remain diagnostic
feedback; failures are work to fix, not a hold on unrelated merges.

Gate tooling is development tooling: if its custody or guard state blocks work, reset it (stop the gate VM, clear the control-directory records) and fix the tool forward.

The memory guard stops only on critical pressure or real swap growth. The local gate implements that guard; unavailable telemetry warns and continues. Development gates, custody,
admission and guards may be changed freely when they block work. Earlier tooling
custody and guard restrictions in this document are superseded by MY-41. Preserve
running production customer jobs, secrets and public-repository privacy.

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
run` cancels its key and exits 130 once the queue confirms. When the step fails
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

A stale diagnostic log or publication reply is not a release acknowledgement.
The attempt actor sends an exact release-only completion and retains its durable
Job/Result until that request succeeds, even if replay or an earlier completion
was stale. Failed release delivery retries through the same actor.
An acknowledged Stop+Kill completion already proves release; its synthetic stale
publication status does not trigger another completion.

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
the real queue's adaptive loss allowance and majority-silence guard determine
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

`GOMAXPROCS=2 go test ./internal/myce -run '^$' -bench '^BenchmarkCheckinBodyPhases$' -benchtime=20x`
uses the same retained-evidence shape with five occupied workers and sequential
check-ins. It reports maintain, placement/reconciliation, deferred environment,
qualification/model/copy plans, health, efficiency, retained-index construction,
transaction body, persistence and record snapshot milliseconds per check-in.
Child phases are inclusive: retained-index construction is already inside the
planning/health phases. `all_holds-ms/op` includes the check-in's other mutex
holds, including snapshot preparation; benchmark wall time also includes work
outside the lock. These opt-in timers never control scheduling.

Cold retained execution facts are indexed by full comparable work/configuration
and OS, reducing only exact-attempt peaks/recovery margins with identical
provenance and end time. Unchanged cold facts survive planning passes and
transactions. Cold writes, collection, terminal transitions, rollback and record
rebuilds invalidate the index; live and transaction-touched cold facts are
reduced afresh per immutable pass. Check-in preparation copies relevant cold
fields under the mutex and hashes/reduces outside it, installing only the same
cold-record generation. Inventory views and lower-bound answers remain private
to each immutable pass. Current owner identity, qualification, unreported/gone
state and time fences are still checked, including permanent-capacity views.
The original full-history evaluator and frozen admission/health digests provide
decision-equivalence regressions. This optimization does not change scheduling
policy or persistence and does not establish live latency under competing I/O.

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
activation is implied by laptop tests. See [operations](reference/file-store.md#parked-native-storage-integration-564).

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


## Existing test-suite pruning record

[#1086](https://github.com/1kuna/mycelium/issues/1086) owns the measured suite
reduction and assertion-transfer ledger; [#1128](https://github.com/1kuna/mycelium/pull/1128)
owns package-slot scheduling. Keep their current proof and failure-backed coverage
on those records rather than starting a parallel pruning effort. A source-function
count or measured idle slot is not wall-time/fleet-cost savings. Compare accepted
before artifacts with the first combined after full, recording exact source,
platform/toolchain, package overlap, wall and resource-cost coverage. Preserve
original verdicts, retained incident/security assertions and unproved donors;
no runtime target by itself waives the existing gates.

The 50% function-count target influenced #1086 planning; it is not acceptance under this v2.1 assignment. Do not delete or consolidate an additional test merely to reach a number. For every removed or replaced test, retain the duplicate/obsolete reason, any incident/security behavior, named surviving assertions and validation disposition. Keep uncertain donors. Treat the existing 21m30s figure as #1086's historical work-plan goal, not a v2.1 budget or gate exception; retaining an operational time budget requires a measured cost rationale and its source authority. Report actual before/after wall and fleet resource cost, with missing measurements explicit. A final count or passing full alone does not prove retained behavior.

Dated inventory: the November 9, 2026 06:10 UTC common-base projection was
3,396 → 1,698 Go Test functions at `67a487f3`, superseding the earlier
3,377 → 1,680 snapshot. These are historical draft-source inventories, not
merged counts, passing cases or measured speedup. The issue’s 06:20 UTC update
withdraws the numeric target and records a per-cut rationale audit in progress.

Next action: the #1086 coordinator retains each cut’s reason, surviving assertions
and validation, keeps uncertain donors, and reports actual wall and
fleet resource cost for the identified combined source. Missing measurements
and unresolved behavior remain explicit; the final count or a passing full
alone does not complete acceptance.

### Fault evidence cost and native qualification

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
sampling or native journal latency. A Darwin arm64 synthetic run measured 3.623 µs/op,
1,256 bytes/op and 30 allocations/op (200 ms benchmark); this excludes real
proc/PID I/O and is not a native latency claim.

Laptop checks use injected clocks, kernel lines, cgroup files and wait statuses;
core records/maps are synthetic. Native acceptance, after an authorized release,
requires a naturally occurring fault: verify kernel/capture/sample alignment,
exact PID ownership, page counters, native query latency and capture availability.
For a natural signal exit verify any core pointer and privately recover stack,
argument registers and ELF build IDs. Do not induce GPU faults or enable cores
as part of the laptop/full gate. Current historical causes for #155/#300 remain
unknown; this capture is not a preventive fix or a benign classification.
