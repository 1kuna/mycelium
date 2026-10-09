# Sharing GPUs and measuring the pool

Experiment projects share available GPU time. Production and deadline scheduling
remain unchanged. A project running alone can use the entire pool.

For each worker, the scheduler considers only jobs that fit and pass their caps.
It totals those projects' recent usage, computes each project's weighted share,
and selects the project with the largest deficit (share minus usage). Model and
environment readiness, deadlines, front-of-line retries, and waiting time break
ties as before. Usage decays continuously with a 24-hour half-life. Running work
is accounted before each scheduling decision, including work still in progress.
This is dispatch fairness: a long non-resumable experiment is not interrupted
just to rebalance projects. Production preemption remains immediate.

Start the queue with `--project-config projects.yaml` (also supported by `myce dev`):

```yaml
verso:
  weight: 3
atlas:
  weight: 1
  max_concurrent_experiments: 2
  max_memory_share: 0.25
```

Missing projects have weight 1 and no caps. Configured weights must be positive.
A concurrency cap includes running/stopping experiment commands and assigned
experiment calls. A memory cap applies to experiment reservations across online
workers' reported memory budgets, including each active model once per project
and worker. Production and deadlines bypass both caps. Caps are hard limits even
when a project is alone. Policies persist in SQLite; omitting the config on queue
restart preserves them. Supplying a config replaces the policy map.

`myce status` and the page show cap or fairness reasons, for example:
`atlas is ahead of its fair share; verso goes first`. NAS and machine fit failures
remain visible. The page is available at the queue root URL; it uses no external
assets, frontend build, or fleet token. Like the existing page, it is read-only
and visible to anyone who can reach that URL. The API remains token-protected.

## Usage

```sh
myce usage --since 7d --by project
myce usage --since 24h --by machine
```

The authenticated `GET /api/usage?since=168h` API returns project GPU-hours,
machine busy percent, idle hours, pinned reservation hours, daily totals,
preemptions, and production submission-to-start acknowledgement seconds.
The command always includes machine occupancy to answer whether pooling fills
idle time. Durations must be positive and no greater than 90 days.

Usage is **observed allocation time**, not hardware compute utilization. A started
command or assigned model call makes its device busy. Simultaneous projects share
that device's elapsed time equally, so busy GPU-hours never exceed observed device
hours. An assigned call's time includes the worker HTTP trip. Model loading and
idle loaded models reserve memory but do not count as active compute. Pins are
reported separately and never turn an idle GPU into a busy GPU.

The queue integrates state changes into per-minute SQLite samples, split by
machine, physical device, project, job, and model. Samples include active/idle,
pinned reservation time and declared workload memory in GiB-seconds. Machine
cards use reported free memory against the configured budget; if no budget is
reported, capacity is the highest observed free memory, so used memory is an
estimate. Pinned memory is a subset of used memory, not extra consumption.
Workers report one physical device each; use unique worker IDs per device.

A minute can contain multiple workload rows. After seven days, samples are
combined into hourly rows retaining dimensions and sums. Retention maintenance
runs hourly and removes data older than 90 days. Queries proportionally estimate
partial boundary buckets; daily charts use UTC days and only actual samples.
Gaps without heartbeat coverage (including restart recovery and network loss)
are unknown time, not idle time. The busy denominator includes only observed
online intervals. Consequently, usage reports cannot claim pooling efficiency
for unobserved downtime. Queue state and samples commit in one transaction.

The status page refreshes every five seconds, preserves an explicit refresh
failure indicator, supports phones, and draws charts as inline SVG. The first
chart appears after an observed interval. No historical data is invented.

All fairness, accounting, retention, and explanation tests use an injected clock
and fake inventory on a laptop. `myce dev` uses the real queue, HTTP API, and
command runner, with fake GPUs.

Within each priority, status lists running work first and then waiting commands
in the dispatch scheduler's current order. Busy-machine explanations combine
capacity and ordering (including fair share). This is a projection of current
inventory and usage, not a reservation: arrivals, usage, availability and which
worker frees first can change it. Non-runnable/capped jobs follow runnable jobs
with their blocking reason; model calls join the same order when their model route is eligible.
Job IDs show eight characters; hover for the full ID or use `myce status --full`.
Attempts start at 1; jobs not yet assigned show no attempt. Running/waiting ages
use the queue clock. A resumed job shows time since its latest return to the queue.
Finished durations appear when start and finish are known.
