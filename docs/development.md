# Develop and verify Mycelium

Use [quick/full gates](#laptop-test-gates) for laptop changes, the
[native acceptance checklist](#native-acceptance-checklist) for authorized device,
manager and boot proof, [component probes](#laptop-component-probes) for the real
API/runner.
Fake inventory never qualifies native hardware. The [worker protocol](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/development.md#current-worker-protocol)
and [storage integration](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/development.md#backed-storage-admission-integration) are developer
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

The local gate retains its dedicated Go build cache while measured free disk
covers the entry floor plus recent cache growth. Entry and exit scans evict
only the shortfall, oldest Go object files first, preserving objects inside
Go's mtime protection interval. There is no fixed cache size cap. Go rebuilds
evicted objects on demand. Module downloads, timing and compatibility records, and
custody receipts are retained; symlinked directories are never traversed.
After affirmative cleanup, successful and failed runs both remove their frozen
source tree and source/runtime archives. Unconfirmed cleanup retains the tree
for diagnosis without retaining admission; verdicts and receipts remain available in either case.

At every entry and exit it also trims the default Go build cache (`go env GOCACHE`),
which workers' direct `go test` runs fill and nothing else trims (#1224). On
2026-11-11 that cache reached 51 GB, at about 10 GB/h, and held the disk under
the 45 GiB entry floor, which blocked every local gate. The budget is derived:
free disk must cover the entry floor plus the bytes written to that cache in the
last hour. Only the shortfall is evicted, oldest first, so with ample free disk
nothing is removed. Go refreshes an entry's mtime at most once an hour, so the
trim never removes an entry younger than an hour plus five minutes. Anything a
concurrent build used in the last five minutes therefore survives. It skips a
cache on another filesystem and never fails a gate.
`python3 scripts/gate-default-cache.py --dry-run` prints what it would remove;
the regression is `python3 scripts/test-gate-default-cache.py`.

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
`gpu_host_memory.go`, `capability_placement.go`, `work_history.go`, `memory_observation_identity.go` and
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

Every quick selection also includes a fixed canary set (`CANARIES` in
`scripts/gate-related.py`, #1224): the complete planning-outcome digest, status
rendering in `internal/myce` and `cmd/myce`, and opening the previous release's
database. Textual selection missed exactly these whole-output checks for several
merges on 2026-11-11. They cost about 12 s of race test time (11.8 s in
`internal/myce`, 0.5 s in `cmd/myce`, measured on the development host). A docs-only change still runs the standalone dev smoke. Renaming a
canary away fails the selector regression.

A change under `skills/` or `clients/python/` also selects the consumer skill
contract test, `test_client.DevTests.test_consumer_skill_examples`. It executes
the guide's CLI ladder and Python example against a real `myce dev`, and no Go
test covers it (#1224). It runs under the gate's Python 3.11 and costs one
`myce dev` start: 28 s measured with a warm build cache. Only that test runs in
quick; the rest of the Python client suite runs in full.

Run `./scripts/gate.sh full` once for a batch before deployment. It runs the lean
full race suite once, including dev checkpoint/resume, model and enrollment
end-to-end tests. It does not repeat those packages in a second race pair or a
separate dev run. It also runs the whole Python client suite
(`clients/python/tests`, 87 tests, 71-86 s measured warm here)
as one early-starting entry beside the cross-release check. Tests for the
optional PyYAML and OpenAI extras skip, because the offline gate interpreter
does not have them. For an authorized urgent hotfix: run quick, deploy, run full
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
the helper on demand. Each watchdog copies that image into its private temporary
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
OS/architecture, Go version and available/assigned CPU count. Rebuilt test
images retain this identity and its finite learned bounds.
The full scheduler also keys those timings by shard concurrency, since each
individual shard uses one Go CPU at every allocation size. Timings live beside
Go's execution-host build cache (`mycelium-gate-timings`), shared across checkouts
on that host, and every invocation appends a sanitized record to
`gate-timings.jsonl`. Fleet profiles return that report with their result marker.
On failure, the watchdog writes a compact receipt from the Go event stream.
It names the failing package and tests, or records a build, watchdog,
signal or exit reason when no named test failed. The receipt survives a
truncated console. Local Darwin orchestration retains `native-failures.txt`
and `linux-failures.txt` in the per-run receipt directory as applicable.
Direct Linux gates print the path to their per-run failure receipt.
For fleet gates, fetch the failed attempt with
`myce result JOB --attempt N --out DIR` and read `kept/gate-failures.txt`.
The fleet attempt's `summary.json` carries worker-level failure reasons, including
failures before the gate starts.
A new selection/toolchain/CPU configuration learns independently; a small quick
selection cannot shorten a full-package deadline.

A learned package stalls after **three times its expected package duration with
no test lifecycle progress**. In-test expected duration is the latest completed
successful package duration for that host/configuration/selection.
Three intervals allow the normal expected
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
For compiled images, package-start precedes executable initialization. Startup
uses a separate bound: three times the largest successful package-start to first
test-lifecycle gap in the last three first launches of distinct executable
images. The window uses the same three-observation contention tolerance and
bounds retained history; warm repeats do not evict first-launch evidence. Image
SHA-256 identifies a first launch only, never the history key. Once the first
named test starts, the ordinary learned in-test silence bound applies.
Darwin evidence in #1817 measured a 1.158s first-launch silence gap versus
0.075–0.115s later gaps; the old 0.245s warm baseline allowed only 0.735s.
Only successful, uncensored runs teach either phase. Failed initialization
cannot inflate startup history, and the in-test duration does not retain an
all-time maximum. Legacy caches without startup measurements bootstrap that
phase once with the existing one-hour cold backstop, enforced by the watchdog
from package-start even before Go's own timer starts. Their warm in-test bound
still applies after the first test event. Later images reuse measured startup
history rather than entering cold learning again.

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
runs). Ordinary fleet jobs receive the worker's durable per-host Go build cache;
the gate preserves that path across HOME isolation. Gate builds use `-trimpath`
so identical packages reuse content-addressed objects when a later job or
worktree has a different source directory. Tests still execute with `count=1`;
cached test results cannot replace a fresh verdict. Only HOME, TMPDIR and GOPATH point inside an ignored `.gate/run.*`
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

Full-gate chunk weights use the largest measured duration across platforms for
roots taking at least two seconds, and 0.1 seconds for other roots. The planner targets
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

Paired quicks use one frozen source for both platforms. If their combined
memory envelope does not fit live available RAM, they automatically run native
then Linux, reserving the larger individual envelope, one leg's CPU share and
the later guest's VM capacity from admission through cleanup. Both individual
legs must fit before either starts. The Linux leg begins only after native
cleanup is confirmed; an ordinary test failure still collects both verdicts,
while a guard refusal, signal or unconfirmed cleanup stops the next leg.
Quicks retain concurrent execution when the combined envelope fits; fulls keep
their existing concurrent-platform admission. Every existing guard remains active.

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

For release acceptance, freeze a candidate branch such as `release/r6.11` and
run repeated full sweeps there before repairing newly discovered flakes. Land
fixture fixes on main and cherry-pick them onto the candidate; each candidate
change restarts the consecutive-green count. Other main merges do not change
the candidate. The release is cut from the qualified candidate SHA.

Run the release performance check once per release batch, on the candidate SHA,
alongside the release full:

```sh
myce run --profile release-perf -- --baseline "$deployed_release_sha"
```

Set `deployed_release_sha` to the full commit SHA of the deployed release. The
profile builds both revisions with the same Go toolchain and runs
`BenchmarkStatusReadCheckinContention` in one four-core Linux CPU assignment,
without race instrumentation. It runs the informational standalone candidate
measurement first, followed by the paired deployed and candidate legs.
The paired candidate p99 must be below 500 ms. The report shows both paired
p99 values side by side. If either leg is missing or errors, the paired check-in
verdict fails. A deployed baseline p99 at or above 500 ms is still a measurement.

The candidate's standalone `BenchmarkCheckinRetainedHistoryContention`
(all-hold p99 < 100 ms), `BenchmarkDevFanOutBurst62` (ack p99 < 500 ms,
heartbeat age and no losses), and `BenchmarkDevProductionStart`
(submit-to-start < 15 s) still bind. `release-perf.txt` records the benchmark
results and ends with the overall verdict. `release-perf.log` keeps the raw
output. Both files are retained on failure. The functional tests retain the
load-independent assertions (#1224), including the queue-mutex and attempt-loss
checks.

On the local Darwin gate, declare a release full with
`./scripts/gate.sh full --priority release`. It keeps its FIFO position, drains
existing local gates, then holds admission until its cleanup completes. Later
quick requests wait or use an authorized fleet fallback; no owner is manually
stopped. Quick/command requests cannot claim this priority. Every memory, CPU,
VM and disk guard still applies. Ordinary fulls retain shared admission.

Local admission automatically archives a slot or custody record when its holder
PID is absent (including a zombie), or its recorded start identity differs from
the live process. The host admission lock serializes reclamation and new
reservations across checkouts. Live or unverifiable holders remain protected;
never-admitted waiters are queue tickets, not supervisors. No global quiet,
guest inventory or manual reconciliation is required to release dead
holder capacity. `--reconcile` remains a compatibility entry point for the same
automatic operation.

Cleanup and inventory failures produce exit 125, a negative cleanup receipt and
`cleanup-error.json` with the exception and traceback. They release admission
anyway. A real cleanup failure
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
Cache retention follows measured free disk and recent growth; only old
rebuildable objects are evicted when headroom is short. Module downloads,
compatibility sources and timing history are retained. Linux uses persistent
guest build/module caches with idle eviction based on free disk and growth.
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
credit. `.reports/gate-local.jsonl` records actual growth; cache retention has
no fixed size ceiling.

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
and tests still join their workers. Vet runs explicitly once, so test commands use
`-vet=off`. Ambient `MYCE_*` settings are cleared.
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
`prepare` fetches that exact public source before the offline gate; local gate
snapshots carry only that predecessor archive.

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

Linux full retains main's measured 6 GiB declaration pending separate
memory-policy qualification; adaptive output supervision does not change that
bound. #767 kept that explicit bound per #559 after an auto candidate hit its
cgroup ceiling: the recent unconstrained total cgroup maximum plus
max(range, 25% of max), rounded up to 6 GiB. It is not proof against future
suite growth. Auto adds ordinary launch slack to the learned figure and keeps
the queue's measured range/25% margin; it does not substitute a raw resident
peak for a cgroup ceiling. Darwin RSS is sampled and admission only, has no
corresponding cgroup ceiling event, and keeps its accepted RSS-based auto
envelope. Unknown configurations explore; inspect envelope basis, learning
reason, memory events and swap after changes.

The 2026-11-04 sizing samples, gate wait snapshots and quick-selection timing
pair behind these choices are in #766, #767 and Git history.

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
virtualization, needs no desktop UI and can be stopped without changing
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
`uv python install 3.11` inside the VM. Follow upstream checksum verification for the Go archive.
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
