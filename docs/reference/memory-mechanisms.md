# Memory mechanisms

Part of the [approved design](../design-proposal.md), moved verbatim apart
from relative links. Product contracts and maintainer rulings are unchanged.

### Unified CUDA free-RAM floor
<a id="amendment-count-the-idle-footprint-once-2026-11-11"></a>

Hosts running only Mycelium jobs give their memory to jobs (maintainer,
2026-11-11; MY-34). The unified CUDA physical free-RAM floor is compared with
live used RAM, which already contains the OS, driver and idle-service footprint.
A measured allowance already excludes that footprint, so physical RAM minus a
measured allowance would subtract it twice. With a measured allowance and no
shared-interactive profile, the floor is the existing OS/driver reserve alone,
and the allowance still caps reservations. The same holds for an authored
allowance: an allowance of any origin caps charges and is never a floor, so the
idle footprint is not withheld twice (orchestrator decision, 2026-11-11, under
MY-34). Only shared-interactive hosts keep the larger of the reserve and
physical RAM minus the allowance, as their stricter owner protection under
MY-45. The reserve is the configured value, with no built-in minimum (MY-34,
MY-48).

### Charging running attempts by use

A running attempt is charged the memory it uses, not its declared envelope,
only where a guard contains its growth: started native CPU-only work with a
declared size, on a dedicated host with Linux cgroup v2 ceilings or the Darwin
RSS guard, with learned evidence for that work on that target and a fresh
exact-attempt sample. The charge is the larger of the learned need, the
attempt's high water and its live forecast, plus the measured overshoot before
containment acts (on Darwin, growth rate times the guard's reaction window),
bounded by the enforced ceiling. On a unified CUDA host with cgroup v2
ceilings, GPU work is charged by use too: measured on a GB10, the cgroup
contains host, managed and pinned allocations, while device allocations
(`cudaMalloc`) escape it and RSS but appear in the per-process NVML sample and
in host availability. Each share is charged the larger of its accepted peak on
that target (without the envelope's uncertainty), the attempt's high water and
its forecast, from a fresh per-process device sample, bounded by the envelope.
Metal work with a declared size is guarded and charged on `phys_footprint`:
measured on M4 Macs, Metal buffers raise it 1:1 while RSS sees none of them.
The Darwin worker sums it over the attempt's owned tree at the RSS guard's
cadence and stops the attempt past its unified envelope (device plus host plus
slack) after the same consecutive samples. The attempt's peak records RSS as
the host share and the footprint beyond RSS as the device share, so unified
sizing can learn Metal work. Its charge is the larger of those accepted peaks,
its high water and its forecast, plus drift and the guard's measured overshoot,
bounded by the envelope plus that overshoot. First runs, auto-sized Metal work,
non-cgo Darwin builds, discrete accelerators, multi-card attempts,
interactive hosts and missing, stale or estimated samples keep the envelope.
When by-use jobs grow back into memory lent to their neighbours and live free
memory can no longer absorb the growth expected before a stop completes, one
job yields through the planned-stop path: the lowest tier first, then by-use
work, then the most recently started. It checkpoints if resumable, then
requeues with no retry charge (`memory_overcommit_yield`). Production yields
only to protect other running production work on the host, and only if
resumable.

Placement and both worker launch checks reserve that same measured growth
through stop grace plus sensor freshness, including unified device growth.
This physical headroom can exceed the bounded charge's sample-age forecast;
it does not raise the attempt's ceiling or its allowance charge. Release
planning returns the corresponding headroom as well as the charge. A yielded
job stays queued while the surviving lenders still need that headroom; fresh
measurements or their exact release admit it again, without a timed cooldown.
The last started copy's lending-yield cause survives requeue and queue restart.
On that same host, readmission must fit the live attempts' unconsumed ceilings
and the resumed job's limit without lending again. A flat or missing growth
sample cannot clear that claim; measured room or exact release can. Other
hosts and first admissions keep ordinary by-use lending. Unknown physical
telemetry withholds admission but never counts as measured pressure for a yield.

### Job memory envelopes
<a id="amendment-job-memory-envelopes-146"></a>

**Local host calibration (#1044).** Each comparable observation retains its
own physical-demand floor and measured range. With complete non-reclaimable
CPU, xe or unified CUDA host evidence, its forecast is range maximum plus the
larger of that range's spread, measured sampling growth and launch drift. Keep the
cumulative resident/driver peak plus launch drift as another floor, including
when the range restarted after incomplete coverage. Take the largest of these
per-observation bounds. Do not add a newer range's variation to an older,
already larger cumulative peak, or treat differences between allowance-key
maxima as another measured range. Incomplete observations independently retain
their total-cgroup peak plus sampling growth, with launch slack added separately;
except for the covered exact-local unified CUDA case below. These margins cover measured between-attempt variation,
growth before the next sample and snapshot-to-launch drift, respectively.
They are empirical bounds, not guarantees about an unseen tail; pressure,
OOM recovery, physical admission and release custody still bind.

Unified CUDA complete evidence includes the cgroup non-reclaimable counters,
exact-attempt proc pin/lock readings and a conservative absolute whole-host
physical residual for uncharged driver allocations. Missing or interrupted
coverage retains total-cgroup sizing. The residual keeps unrelated occupancy
and possible device overlap; it is an upper bound, not a per-client NVIDIA
host-allocation counter. Device sample coverage, extrema and step variation are
retained per attempt for native qualification. Device peaks and their conservative
growth margins remain separate and are added once to the host charge. Retained independent maxima
cannot establish a physical union or justify subtracting cache or device
overlap. Native coexistence and output proof remain separate from calibration.

Explicit `needs.host_memory_gb` and GPU `needs.memory_gb` retain their current
admission meanings and defaults, including production. Optional `needs.memory:
auto` first selects fresh accepted whole-attempt peaks ([comparable
observations](../design-proposal.md#comparable-observations)) for the same work, recipe, input shape
and machine/component, times 1.25. Host RAM uses the largest comparable
accepted peak across sibling devices on the same host, target and memory layout
when the environment matches and the source device's hardware/backend
fingerprint still matches its current inventory (including the Linux kernel).
Device-memory evidence retains its per-device identity and transfer margins.
For local Linux CPU work, at least two complete, unpressured non-reclaimable
attempt peaks per contributing observation enable peak plus the largest of the
observed range (maximum minus minimum), 25% of the retained component peak, and
the existing snapshot drift floor (at least 128 MiB). Two attempts establish a
range, not confidence in an unseen tail; retain the existing 25% allowance for
unobserved growth between 200 ms samples. Keep all comparable resident peaks as
a floor. Missing/incomplete local observations keep total-peak sizing. GPU work
uses the same conservative total-peak fallback unless discrete Linux xe/XPU
attempts also retain complete per-client driver system/GTT allocations,
independent runtime xe debugfs GTT-manager usage, and whole-host
MemAvailable-drop evidence from before payload launch. For that case, use the
non-reclaimable peak plus the peak of max(driver allocations, whole-host drop
minus synchronized current components, zero), also floored by whole-host drop.
Retain all resident/driver floors, then the same range/25%/launch-drift margin.
Two complete attempts per contributing local observation remain necessary. This
is an observed envelope; native pressure-free concurrency and output parity
require separate proof. Without local history, use the same recipe/work/ shape
from another machine: prefer the same OS/architecture/accelerator/memory-layout
family, times 1.25 × 1.5; otherwise times 1.25 × 2. Label both as a
cross-machine estimate. For CPU work, accepted same-OS evidence takes
precedence over the conservative foreign-OS prior. Constrained same-OS
observations are lower bounds only and cannot replace that prior. If the prior
exceeds every qualified target-OS allowance, fresh comparable same-OS
lower-bound evidence (with the existing 25% margin) and any submitter hint must
fit before permitting one bounded exploration on the largest qualified
allowance. Keep normal launch slack and live admission guards.
Assignment durably consumes this exception per frozen work/configuration and
target OS, including across resubmission, history collection and restart. A
clean result learns through the single eligibility gate; OOM gets no retry, and
constrained or otherwise ineligible results cannot reopen the exception.
Successful constrained attempts retain their ordinary job outcome. The foreign
prior remains available with the capacity-remedy diagnostic. Claiming at
assignment may spend a probe before launch, preventing duplicate launches after
a lost offer. Qualification and output parity remain unchanged. For a
discrete-GPU target with only one accepted sample from another target/layout
family, the same durable probe applies when the transferred host estimate
exceeds every eligible allowance in the target family. The measured peak plus
25% and any host hint must fit the largest allowance after launch slack and
drift margin. Keep total-cgroup sizing when complete evidence cannot
distinguish reclaimable cache; a smaller resident figure alone is not enough.
Assignment consumes one probe per frozen work/configuration and target
OS/architecture/backend/layout. Accepted target-family evidence replaces the
prior; OOM gets no retry and ineligible results cannot reopen it. Unified
targets, multi-card work and fan-out retain their existing rules. Next use an
optional submitter hint; a bootstrap declaration is not required and recipe
defaults do not count as submitter hints. Without either, allow an exploratory
first run only with measured memory, a known allowance (physical RAM if
uncapped), at least half that allowance available after existing reserves and
charges, and no protected attempt or model residency charging a quarter of the
allowance. Use that available room, less existing per-attempt slack, as the
envelope. Lower-priority work remains reclaimable through the existing release
planner; release acknowledgement still precedes admission. Accepted exploratory
peaks become measurements. Status shows the source, envelope and enforcement
mode. Memory learning is independent of throughput occupancy comparability:
clean, attempt-specific single-piece serial fan-out children teach the ordinary
recipe's memory class, including piece arguments, while their throughput stays
unknown. Operator resource allowances and power mode remain throughput fences,
but do not identify memory need. Existing allowance-stale memory evidence is
revalidated through the normal curve-recovery path; hardware, backend,
environment, recipe and reference changes still invalidate it. No stored key
rewrite or database edit is needed. Input-size/duration scaling per recipe is
later work. Reservations are frozen per attempt; stale, failed, OOM,
constrained and resumed observations cannot shrink an envelope. Resumed jobs
can use prior accepted estimates. An auto OOM or constrained attempt
invalidates comparable learned memory points across machines and families,
without recording a bound or setting the performance Forbidden flag. To measure
the true need after a ceiling-bound run, submit the same comparable work with
`memory: auto` and no submitter hint: a CPU run starts at its retained measured
floor; GPU work keeps its device containment contract. When a constrained peak invalidated the measurement, its source reads
`exploratory (previous peak constrained)`. A successful unconstrained
exploration re-teaches the peak. Ceiling pressure alone does not retry or
change the job's outcome. After OOM, one retry keeps a job-local host floor
with 25% more room, on the same host if it fits, without raising its GPU
reservation from a host OOM. If that floor exceeds every eligible host
allowance or card, fail explicitly with the required GiB. Device reservations
never exceed the card total and never stand in for measured device peaks.
Member caps are optional; explicit caps remain supported and fit the job
envelope.

Linux native attempts use their existing shared parent cgroup for main and all
sidecars, bounded/zero swap and a job OOM cause. Sample whole-parent
anon + kernel + shmem from `memory.stat` every 200 ms and retain the kernel's
total high-water mark from `memory.peak`. Complete new measurements also retain
anon + kernel + shmem + unevictable + file_dirty + file_writeback as a
conservative non-reclaimable peak for CPU work. All six counters must be present.
Retain all kernel/shmem memory and conservatively double-count overlapping
locked/dirty/writeback pages rather than omit pages requiring I/O before reclaim.
Clean file cache is excluded only with the complete local evidence above.
The unevictable LRU counter is
not a DMA pin counter: XPU pinned host buffers, shared USM CPU storage, CUDA
registered/mapped host memory and driver allocations are not bounded by VRAM
measurements. Discrete xe/XPU attempts additionally retain all DRM system/GTT
host-object allocations, deliberately double-counting any overlap with cgroup
charges. Add absolute xe debugfs GTT-manager use on every participating card,
conservatively double-counting its overlap with client GTT objects. This runtime
source is required: missing, unreadable or malformed manager counters invalidate
GPU completeness. Direct reads use the worker's existing permissions. An optional
operator-owned `--gpu-telemetry-helper` executable can supply the same raw manager
text keyed by PCI, with a timestamp within its current invocation; stale, failed,
oversized or missing evidence rejects completeness. It receives no arguments,
job environment or fleet credentials and runs outside the resident sampler with
the same coverage limits. The installer preserves this opt-in setting but
installs no helper, privilege grant or permission change; service privilege
policy is unchanged. The provided unprivileged `myce gpu-telemetry-client` uses
`/run/mycelium/gpu-telemetry.sock`: no request payload, a fresh per-connection
JSON snapshot, and EOF. A separately installed root-owned broker supplies the
raw counters; no sudo/setuid is used by the worker or client. See the [operations contract](../reference/host-capacity.md#optional-privileged-gpu-telemetry-source).
Retain a whole-host availability-drop floor from an empty-cgroup baseline;
unrelated RAM release can mask that net delta, so it never replaces absolute
driver accounting. Missing DRM client identity invalidates the entire attempt.
Keep resident/component/host-pressure sampling independent of driver I/O. One
driver collection has a 200 ms coverage window, matching resident cadence; a gap
or final age beyond two periods rejects completeness. Attempt completion waits
at most one period for a cancelled collector; late results cannot change its
published peak. This bounds observation latency, never job execution time.
Every host/driver sample must succeed; existing nonzero host swap is allowed.
`memory_peak.host_sampling` retains the first failed host predicate, successful
collection count, maximum read duration/gap and final age. Read errors use bounded
source/class labels rather than raw errors or private paths. CUDA proc status may
vanish between cgroup enumeration and reading: only a fresh exact-subtree inventory
confirming that PID has left permits skipping it; absolute physical/global pin
counters still cover residual allocations. Missing live-PID counters, permission
errors, slow reads and real gaps continue to reject completeness.
Host OOM or memory PSI `some avg10 >= 1%` rejects learning. Additionally,
baseline the host and attempt-cgroup cumulative `some total` counters before
payload/sidecar launch, while the attempt cgroup is empty. Reject learning if
either counter's increase in microseconds is at least 1% of the elapsed attempt
window measured with the injected Clock. `some` includes `full` stalls, so do
not add those totals again. The total-delta check catches short stalls before
Linux refreshes avg10 (roughly every two seconds); avg10 is an extra signal,
never the only check. Exclude baseline/final sensor-read time from the elapsed
denominator, conservatively counting any stalls during those reads. The same
1% band means 2 ms stalled in a 200 ms attempt, or 100 ms in ten seconds.
Hierarchical parent PSI covers descendants; capped members retain their own
pressure/event checks.

Missing/unreadable/malformed host or cgroup PSI, counter resets, or a nonpositive
observation window makes the Linux sample ineligible for **all** memory-envelope
learning, including ordinary RSS/total fallback. Preserve raw diagnostic peaks
with `pressure_unavailable: true` on the result and attempt facts, across the
worker/queue JSON boundary and SQLite restart. Unknown pressure cannot refresh
or replace prior qualified memory evidence and does not invalidate it. Real
pressure still invalidates comparable peaks under the existing policy. Missing
coverage also prevents complete component/GPU evidence. No swap allowance can
override either stall check or missing coverage. Queue support for this additive
result field must precede workers sending it.

Host swap-used growth and cumulative swap-out bytes are compared independently
with a budget of 1% of the smaller of the attempt's resident peak and the
minimum host MemAvailable. Reject if either exceeds that budget. Compare
whole-attempt maxima against the prelaunch baseline at finish, rather than
against startup's near-zero RSS; historical swap is never new pressure. One
percent bounds the potentially hidden resident error far below the existing 25%
growth margin, while the availability bound prevents large jobs excusing swap
churn as headroom disappears. Also reject if minimum MemAvailable falls below
max(operator host reserve, 25% of resident peak, 128 MiB): preserve the
existing growth and minimum launch-drift headroom even without a PSI spike.
These thresholds bound measurement error and available growth room; they are
not a claim of native two-card qualification. Other GPU work retains total-peak
sizing. Native GPU concurrency acceptance is separate from estimator
implementation. Missing counters, older workers, macOS RSS evidence and
cross-machine transfers retain conservative sizing. This does not relax
pressure rejection. Device peaks remain separate. Exact-local unified CUDA work
with at least two complete accepted host observations uses the largest accepted
device peak plus measured between-attempt peak spread (at least launch drift).
Retain that range across accepted completions; a loading increment from zero to
an already reserved peak is diagnostic, not a second whole device allocation.
Live growth forecasts use the current increment/rate and keep the existing fresh
sample, physical headroom and stop-grace guards. The CUDA absolute outside-root
residual includes device pages: preserve each complete combined physical upper
bound plus its measured host range/growth and device variation independently of
non-reclaimable host plus device. Do not add device again to the residual or
subtract independently sampled maxima to claim a unique-page union. A physical range needs two complete accepted attempts to measure variation.
It supersedes incomplete total-cgroup-plus-device forecasts only if the complete
forecast covers every retained total-cgroup-plus-growth floor and every
resident-plus-device forecast, including measured device variation. An uncovered
peak restores the conservative additive fallback. Incomplete-only kinds keep
their existing estimator; missing history is never relabelled as cache. Keep each
cumulative physical peak plus drift independently of a restarted range's forecast.
If later incomplete sampling clears the range while retaining a corrected CUDA
combined peak, keep that peak plus measured growth independently. Compute the
incomplete additive fallback from host/cgroup counters; adding device demand to
the cached combined physical peak would charge the same device twice. This does
not discard a larger accepted host-derived bound or prove unique job ownership.
Physical-range variation, sampling growth and device variation share the forecast
margin; do not add a device allowance again to an already combined bound. Cross-machine transfer, unknown attribution, recovery/retry floors
and rejected-pressure exclusions retain their conservative rules.

**Ceiling-bound envelopes.** There is no low-resident file-cache exception. OOM, swap-out,
anonymous workingset refault, or any `memory.events` `max > 0` / `high > 0`
constrains the measurement, including pressure in capped members. A pressured
sample cannot establish true need even when its resident-only peak stays low.
The queue applies this rule to older-worker results and excludes already-stored
pressure evidence from estimates. Successful unconstrained exploration can
re-teach the peak. File-heavy jobs can therefore reserve more memory or explore
again; pressure invalidates the measured range too, so the next comparable
unhinted attempt explores. A smaller resident-based ceiling is not a promise
that a file-heavy workload will avoid reclaim pressure. Ceiling-bound explicit-memory runs cannot seed an
auto estimate. Explicit admission and job outcomes retain their existing behavior.
The attempt's `memory.events` max/high/OOM counts are kept beside its peak;
absent counts mean they were not read, not zero.
Workers without cgroups (macOS) learn the summed RSS of the attempt's process
groups, sampled every 500 ms outside the queue lock; single-command jobs are
also raised to the main process's exit max RSS (native groups are not). The
sum counts shared pages once per process but omits compressed pages, members
that left the group and spikes between samples, so it can read high or low;
rusage alone reports only the largest waited process. These peaks are
measured but unenforced. Host swap and pressure are machine-wide and cannot
attribute the cause to this attempt. Darwin warning/critical memory-pressure
levels (2/4), swap-out bytes beyond the same 1% budget, or available RAM below
the same headroom floor reject that attempt's learning. Normal level 1 and
small swap-out remain eligible; missing pressure/swap-out readings reject
learning. These host signals never invalidate other hosts' learned peaks; a
real OOM still does. Pressure reads share a 200 ms observation budget, bounding
extra sampling/teardown work; failure costs only learning eligibility. The next
run retains its existing ×1.25 or larger headroom.
Darwin Metal/unified-memory jobs still explore, since no device peak is
measured.
Fully capped native groups preserve member-level OOM attribution and kill mode.
GPU samples belong to the attempt's process inventory where supported, only for
auto or estimate-feeding attempts, every five seconds and until unsupported.
Independent host and GPU sampler goroutines max-track under one small mutex;
cancellation joins both before the final read, so a slow GPU query cannot
delay host sampling. The GPU query keeps its existing two-second timeout.
Without an in-flight host tick, mark the result constrained and do not learn
from start or post-exit readings alone.
Empty process rows, uninitialised counters and transient query timeouts retry;
missing tools or unparseable counters stop GPU sampling. A retained recipe GPU
default is named as such beside a measured host source.
Missing counters stay unknown. Native measurement on a GB10 (#1044) found
`cudaMalloc` allocations outside memcg charges, so unified hosts add measured
host and GPU peaks. An unknown device peak cannot supply an exact unified estimate.
Keep the existing operator/driver reserve and
per-attempt slack; do not invent another fixed reserve. Live available RAM and
the shared machine allowance both bound admission, with a visible refusal reason.
For unified CUDA, retain the physical [free-RAM floor](../design-proposal.md#unified-cuda-free-ram-floor) equal to the existing OS/driver reserve. Fresh raw host observations and outstanding growth bound queue
admission; workers recheck raw RAM at launch. Fresh exact-attempt direct CUDA
readings on unified hosts can credit already-resident physical memory through
the existing single-process union lower bound: max(all credited host pages,
one process's device counter), never their sum or summed process counters.
Missing, stale, ambiguous, fallback and multi-card evidence keep conservative
admission. This credit reduces pending physical growth while retaining the
charged envelope, unconsumed device demand, growth-stop forecast and OOM
protection. Cgroup credit never reduces the device vector. Lower tiers must also leave room for a sole qualified host's
queued or forecast production envelope with existing estimator margins. Existing
production holds, preemption and work-scoped parity approvals remain authoritative.

### Learned memory by default and bounded first runs
<a id="amendment-learned-memory-by-default-and-bounded-first-runs-585-559"></a>

Under MY-31, agents state needs, not memory numbers they would have to guess.
A plain job with no usable memory figure is accepted as `memory: auto`;
partial figures stay submitter hints. Explicit figures keep their meaning.
Plain auto commands get a comparable memory identity from project, name, run,
environment and inputs, so their accepted peaks teach later runs through the
existing single eligibility gate.

A CPU first run with no usable evidence or hint starts at one maximum supported
kernel page (64 KiB), plus the existing per-attempt launch slack. This is an
accounting floor, not a prediction of an arbitrary command's memory need.
Authored member caps, measured constrained peaks and retry floors can only
raise it. Accepted comparable family peaks still use their measured spread
and growth; there is no guessed fraction of host RAM or multiplier of a
failed envelope.

On workers advertising `first_run_floor_v1`, non-production CPU work outside
the shared-interactive role grows into room measured by the worker's local
ledger. Each sample subtracts other charges, their unused reservations, the
owner reserve and measured growth over the sampling interval plus stop grace.
The queue charge follows measured use upward. Other CPU first runs retain a
strict attempt ceiling, raised to the measured 5.75 MiB native shell startup
envelope when configured launch slack supplies less. That envelope is the
larger of a 3,895,296-byte Linux total cgroup peak and a 3,375,104-byte Darwin
RSS peak plus 2,621,440 bytes of measured growth, rounded to 64 KiB pages.
It is a measured startup seed, not a bound on an arbitrary workload; larger
commands use measured-stop recovery. These paths do not gain allowance-wide
lending. Unknown CPU work needs a kernel ceiling or sampled guard; an
admission-only host cannot contain it. Linux `myce dev` reuses the owned-session
RSS guard because its foreground workers have no delegated cgroup; installed
Linux workers retain kernel containment. A measured memory stop requeues at the
peak plus measured uncertainty without an infrastructure loss or retry charge.
Recovery demand is bounded by permanent capacity, independently of live free
RAM. Exact-attempt release custody remains authoritative. Launch-drift history,
its learned margins and the `cold_probe_margin` hold are removed. Existing
wire diagnostic fields remain readable during the adjacent-release transition.

GPU first runs keep their separate device and host accounting below: unknown
driver/device allocations cannot be inferred from CPU RSS (#596, #1044).

On a unified-memory host, a plain `memory: auto` GPU job with no accepted estimate owns the
card exclusively and reserves the host's whole shared physical headroom
(`unifiedGPUFirstRun`, since e0202e34b): without a device sample, the shared
pool cannot be split into two separate size guesses. With neither figure
stated, the device side is a one-page marker of exclusivity, not a device-use
estimate, and the enforced host member gets the rest of the pool. A hint
figure keeps its value. GPU allocation failures are not classified, so the
device side never doubles on failure. Learning needs a complete sample: where
the driver reports per-process device memory (measured on a GB10, #1044; see
[charging by use](../design-proposal.md#charging-running-attempts-by-use)), an accepted run's host
and device peaks become the learned estimate, and later runs are sized and
charged from it. A host-only sample never turns unknown device use into
accepted sharing evidence, so such work keeps exploring. The cost is one
exclusive host per cold run; a declared `--mem` is not `memory: auto` and skips
it. Extension jobs keep their exploration.

Job GET `memory_envelope.basis` is `declared`, `learned`, `hint` or
`exploratory`. `evidence` counts the accepted attempts behind a learned figure
or a similar-work bound. Status and waiting reasons show both. The local queue
policy field `learned_memory: false` turns all of this off: omitted figures are
rejected again, and cold runs explore host-wide. Unset means on. An omitted
field in a policy update keeps the saved switch.

