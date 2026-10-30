# Shared interactive hosts

An orchestration machine, developer workstation or interactive server is usable
compute capacity. Opt it into the shared-host profile when protecting its owner
matters more than throughput. Any number of hosts can use it. Agents submit
portable light work normally; the queue chooses an ordinary fitting host first
and uses a shared host immediately when ordinary hosts cannot admit it.
Existing project restrictions, priorities, exclusions and maintenance still win.

## First phase

The queue stores `policy.shared_host.interactive_reserve_gb` per physical host.
Enabling it requires positive CPU/RAM allowances and fresh quiescent custody
while drained or disabled. This avoids applying a new execution-priority policy
to already running processes. It sets the existing `last_resort` policy; that
flag cannot be cleared while the profile is active. Profile removal leaves
placement, budgets, independent exclusions and drain/disable in place.

The first phase admits native CPU commands with already prepared environments,
including CI/tests and small builds. It withholds accelerator work through the
existing `kind=gpu` exclusion matching path, including future devices, without
creating or editing an operator's selector. Explicit selector exclusions remain
independent. Docker, extension execution, new environment/extension preparation,
model loading/copying and onboarding benchmarks are withheld on this profile
until their execution budgets and priority are covered. A job's own shell can
still compile code: that work inherits its command's priority.

Commands and native sidecars receive nice=10 before their execution gates open
on macOS and Linux. Their descendants inherit it. Worker heartbeats and queue
control stay at normal priority. The existing CPU allowance limits aggregate
**declared cores**, not actual CPU time. Set application parallelism to the
declaration (for example `GOMAXPROCS` and build/test worker counts). Nice=10
reduces competition under load and may slow completion; it is a scheduling
policy, not a measured CPU share or a hard cap. macOS Metal isolation and RAM
ceilings remain unenforced. Trusted CPU-only declarations are cooperative there.

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

For each admission the protected reserve is the larger of that measured
increment and the existing #353 growth guard (25% of the seven-day external
maximum). The historical allowance is physical RAM minus that maximum minus
the protected reserve, clamped at zero and rounded down to 0.25 GiB, using
`SuggestHostBudget`. Full reservations consume this allowance. It can only
reduce the operator's configured RAM budget, never raise it. Live raw available
RAM minus the protected reserve and running attempts' remaining growth is also
an upper bound. Existing after-reserve live availability remains another upper
bound, so the old reserve is not subtracted twice. Missing live/history evidence
or a worker policy mismatch withholds new work. History ages with the injected
queue clock; no bulk probe or file work occurs under the queue mutex.

Immediately before execution the worker rechecks raw live RAM and the configured
interactive reserve. Its additional 25% live-used-RAM guard is conservative
(including other Mycelium use); it cannot credit sampled RSS as available memory.
The queue still enforces the historical bound. Shared-host status reports the
configured reserve, acknowledgement and admission reason. A full reserve can
leave zero admitted RAM; the existing drain remains authoritative regardless.

## Configure deliberately

Upgrade the queue and workers together before using the additive check-in fields.
On an authorized host, inspect `myce host status HOST` and the history first.
The following placeholders are measured values, not defaults:

```sh
myce host budget HOST --dry-run --headroom-gb MEASURED_BURST_GIB
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

## Pressure response and remaining choices

Phase one withholds **new** work as live RAM shrinks; it does not stop Started
commands on owner activity or OS pressure. Existing owner tiers still preempt
through their normal exact-attempt path. Follow-up [#390](https://github.com/1kuna/mycelium/issues/390)
tracks Linux PSI/macOS memory_pressure and interactive-load sampling, measured
thresholds/recovery hysteresis, checkpoint and requeue through planned preemption,
and CPU containment (Linux cgroup weight/quota; qualified macOS QoS/share options).
Unknown samples must remain unknown, and release must still be acknowledged
before replacement work starts. Broader preparation/Docker/extension eligibility
belongs to that follow-up. Parent: [#389](https://github.com/1kuna/mycelium/issues/389).

The owner chooses the measured interactive reserve and CPU allowance: larger
reserves protect more bursts and admit less work. Seven-day maxima retain bursts
but can withhold capacity after an unusually busy day; increasing throughput by
using percentiles needs separate evidence. A strict CPU cap costs completion
time even when the owner is idle, while weight/priority alone cannot guarantee
latency. Pressure preemption adds checkpoint/restart cost and needs interference
measurements. Keep the conservative first phase until those choices are qualified;
no native stall-prevention guarantee follows from laptop tests.
