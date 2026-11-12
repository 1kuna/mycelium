# Shared and polite hosts

Daily-use (polite) hyphae, OS owner signals, and the opt-in shared interactive host
profile, including how to measure and configure it. See also
[shared-host pressure freshness](../shared-hosts.md).

## Polite part-time hyphae

Dedicated hyphae keep the existing default: offer resources whenever awake.
For a daily-use machine, add `--daily-use` to its usual `myce join` command.
No GPU, power, fan, sleep or screen settings are changed. No input is recorded.

```sh
myce join --daily-use --max-jobs 1
```

From the next release, daily-use hyphae take work after **locally measured owner idle recovery**, on confirmed external
power when battery-equipped, with a known safe heat signal. Batteryless desktops
need a confirmed batteryless power report. Plugging in never overrides owner
activity. An unknown required signal means waiting with a reason in CLI/HTML
status and `capability_profile.availability` in the status API. Aggregate idle
seconds, power source and thermal state are reported; input contents aren't.

A detected laptop needs external power even in dedicated mode. `--laptop`
explicitly enforces that rule when automatic battery detection is unavailable.
Use dedicated mode for batteryless servers; daily-use mode for personal machines.
The battery-equipped classification is retained throughout a worker session
if a subsequent power read fails. Daily-use flags are available on foreground `join`; installed services can
pass them through `service ... -- join`. Installation alone is not an opt-in.

Signals refresh independently of queue connectivity using the host's measured
probe cost and signal-change cadence. Required readings expire at their frozen
observation horizon. A sampling gap, including sleep, requires fresh awake
evidence; the worker never prevents sleep. See [adaptive probes](../host-probes.md).
Owner return, battery use, unsafe heat or unknown required signals stop admission
locally and through the queue. Resumable commands get SIGTERM and up to ten
seconds to checkpoint, then are killed if needed. Non-resumable commands stop
immediately and later restart from scratch. These planned stops don't spend the
infrastructure-loss retry budget. Checkpoints use explicit NAS roots or the
[verified file transport](../files.md) when no shared storage is configured.

Model loads and calls also stop when availability is withdrawn. Pins never
override owner availability. Engines stay reserved until cleanup confirms
release; queued callers may retry another hypha. Withholding remains temporary:
fresh eligible signals allow new work without rejoining.

`--max-jobs N` is the **one optional per-machine limit** (default zero: unlimited).
It counts simultaneous batch jobs plus in-flight model calls across all workers
with the same `--host-id`. A stopping batch continues to count until acknowledged.
Ready idle model servers aren't jobs. If workers report different positive
limits, the smallest is used; configure the same limit on every worker of a
physical machine. Lowering a limit blocks new work rather than killing existing
jobs. Memory capacity remains an independent admission constraint. This limits
concurrency, not watts, GPU percentage, fan speed or noise.

## OS signals and limits

- **macOS:** the binary links CoreGraphics' combined-session aggregate event age,
  IOKit's power-source snapshot, and Foundation's process thermal state. It needs
  no event tap, keylogging, screen capture or sensor permission. Daily work
  withholds at fair/serious/critical; nominal must persist for the measured cooling/reheat recovery span after
  a heat hold. Building on macOS needs cgo and the usual Xcode command-line SDK.
  A macOS binary built without cgo reports unknown signals and waits in daily mode.
  The aggregate query covers the worker's login session; fast user switching
  and coverage of other logged-in users remain unverified.
- **Linux:** logind, queried through `loginctl`, covers every listed user session.
  Work waits for locally measured idle recovery after every user session's idle hint, which is more
  conservative than claiming it is a last-input timer. Hint age uses the same
  OS monotonic clock as logind, rather than boot uptime that includes sleep. No user session, an
  uncovered session, denied access or missing desktop idle hints means waiting.
  Power comes from read-only kernel `power_supply` attributes. Heat comes from
  recognized CPU/GPU hwmon sensors with valid max/critical limits and no faults.
  All discovered relevant sensors must be usable. A max alarm/limit holds work;
  a critical-only sensor uses critical minus 10°C. Recovery needs readings at
  least 5°C below the guard (or a more conservative supplied hysteresis) for
  the measured thermal recovery span. Unknown heat holds work. The recognized sensor list is intentionally
  small; unsensed accelerators and these margins are not hardware-qualified.
- **WSL2:** owner activity, power and physical heat must come from the **Windows
  host**, never the Linux guest. The Windows host bridge that supplied them was
  removed (MY-49) until a Windows machine joins, so a WSL2 worker reports owner
  idle, power and heat as **unknown** and never falls back to guest signals.
  An externally powered desktop explicitly joined with `--daily-use --desktop` may use nominal measured GPU
  heat, with status **CPU thermal coverage waived for desktop**. A detected
  battery vetoes the waiver; without `--desktop`, CPU coverage stays required.
  Use `--daily-use --laptop` for a laptop; the flags are mutually exclusive.
  Native session, logon, networking and install behavior require the separate acceptance plan.

Thresholds follow #25's research where compatible with #43. Unlike the research
note's proposed missing-heat exception, missing required heat **withholds** daily
work, with the explicit-desktop CPU waiver described above. These OS observations
aren't a
promise that a machine is silent, that a motionless owner is absent, or that all
physical components have thermal coverage. No quietness claim is made.

Laptop fixtures prove signal policy and fake-clock decisions. Actual OS owner,
power and thermal signals require native qualification; see
[development](../development.md#native-acceptance-checklist).

## Shared interactive hosts

These hosts can run fitting light work through Mycelium. Use the opt-in
[shared-host profile](#shared-interactive-hosts) to combine measured interactive RAM
reserves, #353 external peaks, native command nice=10, positive CPU/RAM budgets
and last-resort placement. `myce host profile HOST --shared=true
--interactive-reserve-gb GiB` requires a drained/disabled, fresh quiescent host;
`--shared=false` removes the profile without resuming admission. Measured OS
memory pressure or foreground scheduling latency stops work through planned
checkpoint/requeue with hysteresis. Linux execution cgroups cap aggregate CPU
time. macOS applies `setpriority(PRIO_DARWIN_PROCESS, pid, PRIO_DARWIN_BG)`
(equivalent to `taskpolicy -b`) to gated main payloads and native sidecars.
Children inherit background CPU/I/O scheduling and lower timer/network priority;
worker control stays outside background mode. Hosts without the profile retain
their existing policy. macOS also caps aggregate declared cores at the CPU allowance, and clamps common runtime thread
counts to each command's declared cores. Payloads can ignore those thread hints:
aggregate CPU time and RAM ceilings remain unenforced on macOS. See the
shared-host guide for thresholds and qualification limits. To compare nice-only
and background scheduling through an isolated CPU dev worker, build the CLI and
run `python3 scripts/measure-shared-host.py --myce ./myce`. The canary uses two
CPU children and one second of foreground samples per mode; each payload
self-expires after three seconds to bound interference if the probe fails.
It measures a light workload, not saturation or a latency guarantee. Existing
exclusions, drain ownership and storage fences apply.

An orchestration machine, developer workstation or interactive server is usable
compute capacity. Opt it into the shared-host profile when protecting its owner
matters more than throughput. Any number of hosts can use it. Agents submit
portable light work normally; the queue chooses an ordinary fitting host first
and uses a shared host immediately when ordinary hosts cannot admit it.
Existing project restrictions, priorities, exclusions and maintenance still win.

## Supported work

The queue stores `policy.shared_host.interactive_reserve_gb` per physical host.
Enabling it requires positive CPU/RAM allowances and fresh quiescent custody
while drained or disabled. This avoids applying a new execution-priority policy
to already running processes. It sets the existing `last_resort` policy; that
flag cannot be cleared while the profile is active. Profile removal leaves
placement, budgets, independent exclusions and drain/disable in place.

The profile admits native CPU commands with already prepared environments,
including CI/tests and small builds. It withholds accelerator work through the
existing `kind=gpu` exclusion matching path, including future devices, without
creating or editing an operator's selector. Explicit selector exclusions remain
independent. Extension execution, new environment/extension preparation,
and model loading/copying are withheld on this profile
until their execution budgets and priority are covered. A job's own shell can
still compile code: that work inherits its command's priority.

Commands and native sidecars receive nice >= 10 before their execution gates
open. macOS additionally applies
`setpriority(PRIO_DARWIN_PROCESS, pid, PRIO_DARWIN_BG)` (equivalent to
`taskpolicy -b`) to every gated supervisor and member. Because a successful external
request does not always leave the payload at its requested priority, each
profiled payload runs through the system `/usr/bin/nice -n 10` and
`/usr/sbin/taskpolicy -b` after its gate opens, establishing nice >= 10 and its
own inheritable background policy before executing the command. The nice
increment can further lower inherited priority; it never raises it. This covers ordinary
commands, native main commands and sidecars, including their descendants.
It lowers CPU, I/O, timer and network priority. Worker heartbeats and queue
control retain their original priority. A failed external background-policy
application logs a warning; the payload still establishes its own policy.
If the system helper cannot establish policy or execute the command, that
execution fails rather than launching the command without its policy.
The existing physical-host CPU ledger caps aggregate
cores at `cpu_budget_cores`. The queue charges each logical job at the larger
of its declaration and its observed peak core use, rounded up. CPU counters come
from existing per-attempt telemetry. Increasing usage over elapsed sample time
raises the learned need, retained through checkpoint/requeue. Missing, repeated
or reordered counters never lower that need. This learned lower bound is per
logical job, not a qualified prediction for a different command or another
submission. Common Go, OpenMP, BLAS and Rayon runtime
thread-count variables are clamped to each command's declared cores, including
sidecars. A payload can ignore these variables or start more processes.

Linux installed workers use delegated cgroup v2 CPU controls. Generated/manual
units delegate `memory cpu`; the worker remains in its existing `worker` leaf.
Only attempts enter the sibling `shared-execution` subtree, with `cpu.weight=10`
and `cpu.max=CPU_BUDGET_CORES*100000 100000`. The quota aggregates main commands,
sidecars and descendants across attempts. Existing per-attempt/member memory
limits and exact cleanup custody remain underneath it. Controls are read back
before admission and rechecked in the bounded worker observation lane. Missing
CPU delegation withholds production admission. Aggregate containment requires
one machine worker per physical host; legacy multiple workers are withheld.
The dev harness explicitly reports admission-only CPU simulation on Linux.

macOS has no qualified aggregate CPU-time containment backend here. Status says
`background task policy; declared-core cap; CPU time unenforced`. Background QoS,
runtime concurrency hints and declared reservations protect fitting cooperative
work; they are not a hard share or stall-proof guarantee. macOS RAM ceilings
also remain unenforced. No profile is enabled by installation or upgrade.

## Measure the memory reserve

Use [external-memory history](../status.md#non-mycelium-ram-history)
from #353: physical-minus-reclaimable RAM minus simultaneous worker-tree RSS.
Observe representative overlapping agents, interactive apps and external work;
check sample coverage and bucket times. A short window is partial evidence,
not a measured week or a future bound. Do not add another OS/queue baseline:
those processes are already in the residual. RSS can overcount shared pages,
miss compressed memory and miss spikes, so this is admission evidence rather
than hard isolation.

Measure the **additional** RAM needed by representative owner activity beyond
the retained external peak, using the same observation method before/during that
activity. Supply that positive increment as `--interactive-reserve-gb`. If the
activity was already observed, retain an explicit allowance for another such
burst. Record the workload and coverage with the sizing decision. There is no
universal fixed reserve or machine-name rule. Until that measurement exists,
leave the host drained rather than inventing capacity.

For each admission the protected reserve is that measured additional burst.
The historical allowance is physical RAM minus the seven-day external maximum
minus the protected reserve, clamped at zero and rounded down to 0.25 GiB.
The peak already includes observed external growth; scaling that entire baseline
by another percentage does not measure the next interactive burst. The generic
#353 dry-run suggestion keeps its 25% growth guard for sizing without a measured
burst; it is separate from this opt-in profile's measured reserve.
Full reservations consume the historical allowance. It can only
reduce the operator's configured RAM budget, never raise it. Live raw available
RAM minus the protected reserve and running attempts' remaining growth is also
an upper bound. Existing after-reserve live availability remains another upper
bound, so the old reserve is not subtracted twice. Missing live/history evidence
or a worker policy mismatch withholds new work. History ages with the injected
queue clock; no bulk probe or file work occurs under the queue mutex.

Immediately before execution the worker rechecks raw live RAM minus the same
configured interactive reserve, bounded by its existing after-reserve availability.
It cannot credit sampled RSS as available memory.
The queue still enforces the historical bound. Shared-host status reports the
configured reserve, acknowledgement and admission reason. A full reserve can
leave zero admitted RAM; the existing drain remains authoritative regardless.

## Configure deliberately

Upgrade the queue and workers together before using the additive check-in fields.
On an authorized host, inspect `myce host status HOST` and the history first.
The following placeholders are measured values, not defaults:

```sh
myce host budget HOST --dry-run # Generic sizing advice; retains the 25% guard.
myce host drain HOST --wait
myce host budget HOST --cpu-budget-cores MEASURED_LIGHT_CORES --memory-budget-gb MEASURED_ALLOWANCE_GIB
myce host profile HOST --shared=true --interactive-reserve-gb MEASURED_BURST_GIB
myce host status HOST
# Resume only when authorized to release this drain.
myce host resume HOST
```

`myce host profile HOST --shared=false` removes the profile while drained or
disabled. It never resumes admission or clears last-resort placement. API clients
POST `action: profile` and `shared_host: {interactive_reserve_gb: N}` to the
existing maintenance endpoint; zero explicitly removes the profile.
Workers acknowledge the exact profile through check-in before new admission.
`myce dev --gpu cpu --workers 1` supports these same API/CLI operations and runs
real commands; no GPU, production service or fleet edits are required.

A checkpoint-unavailable warning remains a separate storage constraint.
Resumable/checkpoint-dependent work waits under the existing contract. A profile
does not repair storage or authorize clearing maintenance.

## Pressure response

Only shared-profile workers collect pressure samples. An independent lane reads
the OS memory-pressure level or Linux memory/CPU PSI, then performs one OS timer wake
proportional to measured probe cost and CPU capacity. Each host learns its own
wake/PSI prediction band; adverse samples freeze the healthy baseline and fresh
readings back at its mean recover it. OS memory warnings remain immediate.

Probe cadence and freshness follow this host's cost, variation and signal changes.
Missing, failed or stale observations remain unknown and withhold new admission.
Negotiated observations include queue processing delay; queue contact cannot
renew them. The first latency interruption
starts a job/host hold that requires a measured quiet recovery span. See
[adaptive host observations](../host-probes.md) for timing, protocol compatibility,
remaining inventory and native qualification limits.

On measured pressure, the queue withholds new work on the physical host and
attaches `shared_host_pressure` planned-stop provenance to each exact running
attempt, whatever its tier ([MY-45](../DECISIONS.md#my-45): the interactive
host's owner outranks Mycelium work). The existing preemption path grants
resumable work its configured checkpoint stop grace (normally 10 seconds);
non-resumable work stops immediately. Confirmed host-relative swap growth with
scheduling latency also triggers this policy, using the existing host-health
observation rather than another sampler. When pressure clears, the yielded job
is placed again, subject to the owner-latency recovery hold. Dedicated
(non-shared) hosts never acquire this policy. Existing
checkpoint preparation resumes the same logical job and submission key, without
infrastructure retry/loss charges. Independent command/OOM failure, cancellation,
stale results and exact cleanup acknowledgement retain their existing precedence.
Elapsed grace never proves release. Status exposes the sample, latch reason,
admission diagnostic and actual CPU enforcement mode.

The response can cost one sampling interval plus checkpoint grace, and pressure
can stop useful progress. PSI's averaged recovery may withhold capacity after
stalls end. Larger RAM reserves and a smaller CPU allowance reduce interference
but admit less work; a Linux hard quota also costs throughput while the owner is
idle. Keep existing operator drains authoritative. Extension execution,
new environment/extension preparation and models remain withheld; this change
does not broaden execution eligibility or qualify their pressure handling.

## Qualification and laptop checks

`python3 scripts/measure-shared-host.py` compares a normal-priority foreground
5 ms wake probe and fixed CPU burst at baseline and beside two CPU-bound
background commands. It runs for three seconds per phase and reaps its own
commands. It neither installs services nor enables a profile. This bounded
canary does not qualify all-core saturation, hostile payloads, memory pressure
or Linux containment. Check local headroom before running it; use representative
owner work for native qualification. [#389](https://github.com/1kuna/mycelium/issues/389)
tracks that acceptance separately.

Laptop regressions use fake-clock yield/recovery/stale samples and aggregate CPU
admission/cgroup files. `TestSharedHostDevPressureCheckpointRequeueAndCancel`
starts the real `myce dev` API/worker/command runner with injected OS pressure and
real local checkpoint copying. It verifies checkpoint, planned requeue, resume on
attempt 2 and cancellation/release. `TestSharedHostDevCommandPriority` verifies
real main/sidecar nice values, concurrency hints, token filtering and unchanged
worker-control priority. No GPU or fleet changes are involved.
