# Shared interactive hosts

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
independent. Docker, extension execution, new environment/extension preparation,
model loading/copying and onboarding benchmarks are withheld on this profile
until their execution budgets and priority are covered. A job's own shell can
still compile code: that work inherits its command's priority.

Commands and native sidecars receive nice >= 10 before their execution gates
open. macOS additionally applies `taskpolicy -b` to every gated supervisor and
member, inherited by descendants. Worker heartbeats and queue control retain
their original priority. The existing physical-host CPU ledger caps aggregate
**declared cores** at `cpu_budget_cores`. Common Go, OpenMP, BLAS and Rayon runtime
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

Use [external-memory history](guide-status-v3.md#non-mycelium-ram-history)
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

Only shared-profile workers collect pressure samples. A bounded background lane
reads `kern.memorystatus_vm_pressure_level` (the macOS memory-pressure notification
level) or Linux `/proc/pressure/memory` `some avg10`, then measures twenty normal-
priority 5 ms timer wakes. The maximum lateness measures scheduling interference,
including competing owner/agent work, without recording apps or owner input.
It cannot attribute contention to a particular process. Probes never run under
the queue mutex or synchronously block heartbeat delivery for their full duration.

Thresholds and recovery bands:

| Signal | Yield | Recover |
| --- | --- | --- |
| macOS memory level | warning (2) or critical (4) | normal (1) |
| Linux memory PSI `some avg10` | >= 1% | <= 0.1% |
| Foreground-class wake lateness | >= 20 ms | <= 5 ms |

macOS uses the OS's pressure warning rather than inventing a free-RAM percentage.
The PSI bands represent 100 ms / 10 ms of stalled time per ten seconds. They are
conservative latency budgets; native Linux interference calibration remains
unverified. The wake bands use the canary below: its observed p99 was below
3 ms and maximum below 7 ms; 5 ms permits recovery above normal p99, while
20 ms detects a delay exceeding a 60 Hz interaction frame and well above the
observed maximum. This is limited local evidence, not representative long-term
owner qualification. Native pressure-stress calibration remains outstanding;
this task intentionally does not manufacture memory pressure on a shared host.

Samples refresh at the existing 5-second owner-signal cadence and expire at its
15-second freshness bound. Worker-local age is translated to queue receipt time;
worker wall timestamps cannot renew evidence. Missing, malformed, failed or
stale observations remain **unknown** and withhold new work. Unknown alone does
not stop running work or clear a pressure latch. A fresh low sample for every
signal clears the latch; middling samples retain it. There is no extra recovery
sleep. This amplitude hysteresis avoids stopping/restarting at one threshold.

On measured pressure, the queue withholds new work on the physical host and
attaches `shared_host_pressure` planned-stop provenance to each exact running
attempt. The existing preemption path grants resumable work its configured stop
grace (normally 10 seconds); nonresumable CPU work stops immediately. Existing
checkpoint preparation resumes the same logical job and submission key, without
infrastructure retry/loss charges. Independent command/OOM failure, cancellation,
stale results and exact cleanup acknowledgement retain their existing precedence.
Elapsed grace never proves release. Status exposes the sample, latch reason,
admission diagnostic and actual CPU enforcement mode.

The response can cost one sampling interval plus checkpoint grace, and pressure
can stop useful progress. PSI's averaged recovery may withhold capacity after
stalls end. Larger RAM reserves and a smaller CPU allowance reduce interference
but admit less work; a Linux hard quota also costs throughput while the owner is
idle. Keep existing operator drains authoritative. Docker, extension execution,
new environment/extension preparation and models remain withheld; this change
does not broaden execution eligibility or qualify their pressure handling.

## Local interference evidence (2026-10-30)

`python3 scripts/measure-shared-host.py` runs a normal-priority foreground 5 ms
wake probe and a fixed CPU burst, first at baseline and then beside two CPU-bound
commands with native background taskpolicy and nice=10. It lasts three seconds
per phase, leaves owner capacity, and kills/reaps its own commands. On an 18-core
Darwin machine, with existing concurrent owner/agent work:

| Foreground measurement | Baseline | Two background commands |
| --- | ---: | ---: |
| Wake-delay p99 | 2.583 ms | 2.539 ms |
| Wake-delay maximum | 6.271 ms | 3.001 ms |
| Fixed CPU-burst p99 | 0.533 ms | 0.573 ms |
| Samples | 422 | 423 |

The foreground remained at nice=0. This bounded two-command canary observed no
foreground starvation. It does not test all-core saturation, a hostile payload,
memory pressure, or Linux native containment. It neither installs services nor
enables any profile. Repeat with representative owner work before authorizing
native enablement; parent [#389](https://github.com/1kuna/mycelium/issues/389)
retains that operational qualification.

Laptop regressions use fake-clock yield/recovery/stale samples and aggregate CPU
admission/cgroup files. `TestSharedHostDevPressureCheckpointRequeueAndCancel`
starts the real `myce dev` API/worker/command runner with injected OS pressure and
real local checkpoint copying. It verifies checkpoint, planned requeue, resume on
attempt 2 and cancellation/release. `TestSharedHostDevCommandPriority` verifies
real main/sidecar nice values, concurrency hints, token filtering and unchanged
worker-control priority. No GPU, Docker or fleet changes are involved.
