# Scheduling and placement

Describe requirements and verified targets; the queue chooses a qualified
machine. Eligibility and owner tiers precede weighted fairness, available capacity,
victim disruption and least-valuable fit. Same-tier indefinite work has no bounded
wait guarantee. Advice and estimates are snapshots, never reservations.

CPU backfill also accounts for queued fan-out pieces and their retries. Each
piece is an ordinary serial job with its own resource vector and destination;
finite CPU executions, including running pieces, can supply projected RAM/cores
as they finish. New work behind that demand's tier/fair turn may use only the
remaining capacity. An immediately usable production destination takes precedence
over a different host's projected releases. Projections never authorize an early
start or release existing custody; withdrawal removes the admission fence.

- [Placement and memory](#placement-without-hardware-requirements-v3)
- [Owner priority policy](#owner-priority-policy)
- [Fair sharing and usage](#sharing-gpus-and-measuring-the-pool)
- [Declare requirements and inspect portability](#declare-requirements-let-mycelium-place)

Declared independent pieces are a current contract in the [spec](design-proposal.md)
and [jobs reference](jobs.md). Placement keeps one measured record per kind of
work, its last accepted duration and peak (MY-49); there are no throughput
curves, benchmarks, completion forecasts or batch plans.
Automatic same-tier scarce-host yield and arbitrary workflow orchestration
remain pending in [#324](https://github.com/1kuna/mycelium/issues/324) and
[#107](https://github.com/1kuna/mycelium/issues/107). A job no single card holds
gets several cards of one host from its `memory_gb`
([cards per job](design-proposal.md#inventory-and-resource-ownership)); cross-machine
gang execution remains deferred. Portability advice does not implement migration
or qualify new variants.

## Placement without hardware requirements (v3)

Submit a kind of work or a program as described in
[Write an extension](adapter.md). No hardware fields are needed.
Optional `size_hint.memory_gb` and `size_hint.host_memory_gb` are lower bounds,
never requirements to submit a job. Plain command jobs keep their contract.

The queue requires a qualified extension for the work and accelerator type.
The capability profile bounds each hypha's capacity. Separate memory must fit
accelerator-reachable memory and additional system memory independently. Unified
memory must fit their sum in one shared pool. For example, 64 GiB RAM plus
24 GiB accelerator memory fits RAM-heavy work but not 40 GiB accelerator work;
128 GiB unified memory fits that work if enough of its shared pool is free.
Live free-memory checks, operator budgets and reservations still apply. Unknown
native profile sizes wait; explicitly fake resources use their configured
allowance. Workers must report the current protocol and a capability profile; unknown capacity withholds admission.

### Typed memory is a limit, not a reservation

A typed `host_memory_gb` (or CPU `memory_gb`) is the job's enforced limit and
the cap on its charge. Once the work has accepted runs on the target, or its
entry two runs in its family, admission charges what those runs reached: the
accepted peak plus its measured spread and growth (MY-31). The worker still
enforces the typed figure: a cgroup v2 ceiling on Linux, the sampled RSS guard
on macOS. A job that grows past its charge is charged by use up to that limit,
and if neighbours were admitted into the room it no longer leaves, the
lowest-tier lender yields through the planned-stop path before the host runs
out (`memory_overcommit_yield`). Production, GPU device memory, the shared
interactive host, hosts without either guard, OOM retries and workers
from the previous release keep the typed figure as the charge. With nothing
learned the typed figure is the charge. The envelope shows `basis: learned`,
`charge_host_gb` and `limit_host_gb` ([status](status.md)).

Memory measured on one OS does not size work on another: the measurement
differs (macOS samples process RSS, Linux reads the cgroup) and so does the
work's footprint (one suite peaked 51% higher on Linux than on macOS). A
foreign peak only floors the first run on the other OS, which runs as that
OS's ordinary first run until the work has a measurement there.

Among admissible targets for the same work, preserve scarce machines first
(MY-18). Measured accelerator backends and usable memory tiers, plus host RAM
tiers above the fleet minimum, form capability bands. Each band counts physical
hosts, regardless of their number of devices or worker rows. Compare how many
bands each host supplies alone, then how many it shares with one other host,
and so on: fewer rare bands wins. Equal vectors prefer smaller usable host RAM,
then the shorter last accepted duration of this kind of work, affinity and
stable target identity. Unified
RAM remains one physical allowance; host and accelerator RAM are never added to
invent capacity or value. Unknown inventory has no invented cheap score; a known
fitting host precedes it, but an unknown-only admissible target still runs.

Eligibility, qualification, owner policy, priority tier, project deficit, free
capacity before preemption, and victim disruption costs all precede value.
Temporary occupancy does not change a host's capability bands. A busy, lost,
owner-active, recovering or otherwise inadmissible cheap host cannot make work
wait on an idle scarce host. Value creates no reservation, changes no fair-share
charge and never preempts equal-tier work. Pulls, status and simulation use the
shared planner; status reports the host scarcity vector and usable RAM, or an
explicit unknown reason. The duration tie-break is described in
[status](status.md#execution-elapsed-time-last-accepted-duration-and-application-progress).
Explicit speed/deadline goals are a later slice; no new goal or deadline policy
is introduced here.

Successful current attempts save observations in the same SQLite queue. The
next matching submission and already queued matching programs reuse the largest
observed needs with 25% headroom, never below the extension's reservations or
optional hints. Learning is per project, program description, arguments,
configuration, input references, frozen extension digest and accelerator platform.
Changing any of these starts a new learning record. Input references must be
immutable: NAS transport cannot detect replacement bytes at submit time. Use a
new reference when changing an input; [file-store inputs](files.md)
carry content identities. Measurements are observations, not a guarantee that
future data-dependent executions need exactly the same memory.

The command runner records OS peak process RAM. Extensions may write a bounded
JSON report to worker-owned `MYCE_USAGE_FILE`, containing `memory_gb`, optional
`host_memory_gb`, and `out_of_memory`. Values are finite, nonnegative GiB.
The path belongs to the exact attempt and is outside its published outputs.
Malformed, oversized and symlink reports are ignored; missing accelerator
observations retain the extension floor. OS peak RAM includes the interpreter
and may include shared accelerator allocations, so unified-memory learning can
conservatively overreserve. Descendant-process RAM is not a complete process-tree
measurement. No configured budget is labelled as an observation.

The PyTorch starter (since v0.2.3) runs the script in its Python process, preserving its
arguments, imports and signal handlers. CUDA/XPU report allocator high-water
marks; MPS samples current allocations every 50 ms, so brief peaks can be missed.
Its bundled-reference qualification remains required. Unsupported counters
remain unknown. This starter does not silently select the CPU.

An explicit extension OOM or kernel memory-ceiling OOM queues **one** retry on
an eligible hypha with no smaller memory pool and at least one strictly larger
pool. Reservations double after the failure. If no such hypha is available the
job explains the wait; it does not loop on the same or an equal-size hypha.
A second OOM fails visibly. OOM does not count as infrastructure loss, successful
learning or permission to loosen extension qualification. Cancel, production
start deadlines and cleanup/release gates still apply.

## Owner priority policy

The queue stores one policy in SQLite. Tiers are ordered from highest to lowest;
a job matches the first selector in the first matching tier. Each selector is
`project/class`, with Go `path.Match` globs independently in both components.
Overlap is legal. Tier names must be unique and the final tier must include
`*/*`. Repeated selectors and selectors following a catchall produce warnings.

```yaml
version: 1
tiers:
  - name: urgent
    selectors: ["transcribe/production"]
  - name: shared
    selectors: ["sim/*", "transcribe/experiment"]
  - name: other
    selectors: ["*/*"]
projects:
  transcribe: {weight: 1}
  sim: {weight: 1, max_concurrent: 2}
```

The optional `projects` mapping uses the existing project settings, including
weights, allowed machines and experiment caps. Omitted weights default to one.
Omitting `projects` preserves existing settings; supplying it replaces the map.
An explicit `projects: {}` clears all project settings and is retained by exports.
There is no second project priority store. `queue --project-config` and
`dev --project-config` seed settings only when no policy has been stored. Once
stored, the live SQLite policy is authoritative: restarting or reinstalling with
the same or edited project config does not replace it or advance its revision.
To deliberately change settings, export the current policy, edit its `projects`
mapping, and import it with `myce policy set FILE --if-revision N` as below.

The optional `hosts` mapping is the live [production hold](jobs.md#production-hold-on-a-gpu-host)
policy, keyed by physical host ID, with the same rules: omitting it preserves
host policy, supplying it replaces the map, and `hosts: {}` removes every hold.
Each entry has `production_hold` and `production_hold_scope`; a hold requires
`production_hold_scope: device` (MY-50: no machine is reserved), and the
removed whole-machine form is refused.
`queue --host-config` seeds host policy once; see
[changing host policy](operations.md#change-live-project-policy).

**Production approval by kind of machine (project-owned, MY-51).** Each project
approves, for an exact work identity, the kinds of machine whose outputs it has
shown match its reference for that work (MY-19). A kind names what the work
needs or is compatible with, never a host: an OS, a CPU architecture and/or an
accelerator platform. Every host of an approved kind may run that production,
including hosts that join later. The project edits its approvals with the same
authority it uses to submit (the fleet token and its project name). No queue
policy edit is needed (MY-44, #1341), and a project's write never changes
another project's approvals or the policy revision.

A kind is written either as words or as `os/arch/platform`:

| Kind | Matches |
| --- | --- |
| `cuda` | any OS and architecture with a CUDA accelerator (`*/*/cuda`) |
| `mac` | macOS on any architecture and platform (`darwin/*/*`) |
| `linux,x86` | Linux on amd64, any platform (`linux/amd64/*`) |
| `arm,metal` | arm64 with Apple Metal (`*/arm64/metal`; `mlx` and `mps` mean `metal`) |
| `linux/amd64/cuda` | exactly that kind; `*` or an empty part matches anything |

OS words are `linux`, `mac` (`darwin`, `macos`) and `windows`; architecture
words are `x86` (`amd64`, `x86_64`) and `arm` (`arm64`, `aarch64`); any other
word is a platform as workers report it (`cuda`, `xpu`, `metal`, `cpu`). A kind
that matches every machine is refused. CPU-only work runs as platform `cpu`.

Approvals default to none. When an exact work key is present, its kinds are a
hard boundary for production placement: initial assignments, fan-out children
and retargeting must run as an approved kind. This applies to historical
registrations and measured variants too. An explicitly empty list admits no
machine for that work. Execution qualification alone does not grant parity.

To approve a kind, first run a check or experiment of that work that compares
outputs with your reference under your own criteria and succeeds only when they
match. Declare its needs (for example `needs.platform`) so Mycelium places it on
a machine of the kind you want; do not pin a host. Then name that job as
evidence:

```sh
myce parity add --project transcribe --work kind:asr --evidence JOB_ID              # the kind it ran as
myce parity add --project transcribe --work kind:asr --kind cuda --evidence JOB_ID  # a broader kind it ran as
myce parity list --project transcribe
myce parity remove --project transcribe --work kind:asr --kind cuda
```

The queue accepts the approval only when the evidence job belongs to the same
project, ran the same exact work identity as a check or experiment (not
production), succeeded, and its accepted attempt ran as the approved kind.
Without `--kind`, the exact kind it ran as (`os/arch/platform`) is approved; a
named kind must match that run. The entry records the job, its attempt and the
queue time it was added. Any other evidence is refused with
`parity_evidence_required` and one next action. Removing a kind needs no
evidence; removing the last one leaves an empty list that admits no machine.
The HTTP routes are `GET /api/parity?project=P`, `POST /api/parity` and
`POST /api/parity/remove`, with the body `{project, work, kind, evidence}`.

Keys match the job's work identity: `kind:<kind>` for kind jobs,
`program:<type>` for programs, or `extension:<name>` for extension-only jobs.
Approval for `kind:asr` never approves separation or another work identity.
Projects own the parity criteria and must revisit approval when their work or
platform changes (remove the kind, compare again, approve it again). Mycelium
does not infer parity from timing, measured quality or native checks.
With one fleet token, the project name is the authority, as for submit and
cancel: the queue refuses another project's evidence, which guards against
mistakes, not against a hostile token holder.

**A new host of an approved kind.** Agent default, not a maintainer ruling
(MY-51 leaves this open; #1503): Mycelium adds no output comparison of its own
for a new host of an approved kind. The check that runs there is the project's
own declared one: work with a production approval is extension work, and every
host runs that extension's declared native `check` against its baseline
automatically, once per extension version, platform and environment, before any
of that work, production included, is admitted there
([native qualification](adapter.md)). A host whose check fails or has not run
takes none of that work. Nothing further gates the kind approval.

**Migration (MY-42).** The previous release approved hosts by name. On first
open, a queue converts each host-named approval into the kinds that work runs as
on that host (its CPU and each accelerator), keeping the approval's evidence,
marked `evidence.migrated` with `migrated_from` naming the host. Every host the
old list admitted is still admitted. A host that has not yet reported its OS and
architecture stays approved by name (`pending_host`, with a warning in `myce
parity list`) and is converted when it checks in. A `--project-config` or dev
config that still sets `production_parity_hosts` is accepted, converted the same
way and logged as a warning; a previous-release client that sends `host` to
`myce parity add` or `remove` (or to the old `/api/parity-hosts` routes) is
converted with a warning in the reply. Conversion can widen production to
other hosts of the same kind, which is the point of MY-51; a project that does
not want one of its converted kinds removes it with `myce parity remove --kind`.

For one release the queue also keeps two read-only host mirrors so a rollback to
the previous release enforces the hosts the kinds admit at the last write: the
host-form project store and the `projects.P.production_parity_hosts` policy
field. `myce policy get` shows the field with a deprecation warning, and `myce
policy set` accepts it unchanged or omitted but refuses a change with
`parity_hosts_project_owned` and the `myce parity` command to use instead.
Approvals written while rolled back are not carried forward on the next
upgrade; repeat them. The next release removes the mirrors, the `host` request
field and the `/api/parity-hosts` routes.

The project's approvals also permit **new occupancy spillover**: a queued production job
bound by a production hold can move when equal-or-higher production work blocks
its RAM or device admission and an approved alternative can admit it immediately.
The alternative must still pass placement, maintenance, freshness, memory,
environment and exact frozen extension version/digest qualification checks.
Running work keeps its attempt and host custody. Without an exact work entry,
existing measured-parity and registration rules apply. Measurements never widen
an explicit approval list. Disk recovery obeys the same production boundary.
Queued bindings to hosts no longer of an approved kind are cleared and placed again,
including bindings saved by older releases or retained after holds are disabled.
Preparation lookahead considers only parity-eligible production candidates on
each host. Parity-held demand cannot withhold another project's preparation on
an unapproved host; eligible production retains its tier and preemption priority.

Within each tier, fitting projects take turns by weighted recent usage before
model/environment warmth. Usage has a demand/service-derived horizon and separate stable
`tier/resource-family/project` keys. GPU work uses normalized allocated device
seconds; CPU work charges allocated core-seconds in a separate family. Equal usage favors fewer
active allocations per weight, then the existing affinity/deadline/queue order.
Thus a warm project cannot monopolize newly available workers. Non-fitting jobs
do not block compatible backfill or consume a turn. Idle, unpinned same-tier model
residency can be reclaimed through the existing release planner; active calls
and loading attempts are not interrupted for same-tier fairness.

`weight` is positive and defaults to one. `max_concurrent` is an optional
nonnegative cap across all classes and tiers for that project (zero means no
cap). It counts running/stopping batch attempts and assigned model calls,
including preparation before Started. Existing `max_concurrent_experiments` and
`max_memory_share` retain their experiment-only scope. Both cap types apply
independently. Updates use the same policy revision CAS; caps remain hard limits
when a project is alone. Reducing a cap withholds new admissions and does not
stop existing work. Optional `max_retained_file_bytes` and
`max_added_file_bytes_24h` limit a project's non-production queue file-store
use ([file-store quotas](reference/file-store.md#per-project-file-store-quotas-464)).

The accounting migration is versioned and durable. Old project totals lack
class metadata, so they are preserved once in the current catchall GPU tier.
New usage remains attached to stable tier names across reordering and restart;
removed tiers stay inactive without transferring their history to other names.
See [fairness and usage](scheduling.md) for accounting details.

Use the queue owner credential (`MYCE_TOKEN`) and queue address (`MYCE_QUEUE`):

```sh
myce policy get
myce policy get --yaml > policy.yaml
myce policy set policy.yaml --if-revision 0
myce status
```

`get` returns JSON with `revision`, `policy` and optional `warnings`. `--yaml`
exports an importable policy and prints its revision to stderr. `set` validates
and atomically replaces the policy only if the revision matches, then returns
the new record. Read the current revision before editing; a conflict requires
reading and reconciling the newer policy. YAML is import/export, not live state.
Flags may precede the filename; the shown filename-before-flags form also works.

Authenticated HTTP clients use `GET /api/policy` and
`PUT /api/policy` with `{"expected_revision": 0, "policy": {...}}`. A stale
revision returns HTTP 409 with code `policy_revision_conflict`. Missing revision
and invalid policies return 400. This uses the existing queue owner/fleet token;
priority markers confer no authorization.

Jobs may declare `class: production`, `class: experiment` or any other marker.
The default class is `experiment`. Neither project nor class can contain `/`.
Legacy `priority` remains accepted and is normalized to class at admission; if
both are supplied they must agree. Legacy deadline validation and production
retry/start behavior remain unchanged. A custom high-tier class does not acquire
production reliability semantics.

Before any policy is configured, revision zero synthesizes these same tiers
through the same matcher: `*/production`, then `*/deadline`, then `*/*`.
The catchall tier is named `experiment` for compatibility and also accepts new
classes. Submitted classes and submission-key fingerprints remain unchanged by
policy updates. Existing jobs are ranked using the current policy at each
planning decision; changing policy does not immediately stop all lower work.

Status JSON includes `spec.class` and each job's `priority_policy` with `tier`,
zero-based `index`, `selector` and `revision`, plus the current policy record on
the snapshot. CLI and browser status show the tier, selector and revision.
Placement, preparation and existing reclamation use one tier comparator.

Placement, status and simulation use the same admission planner and comparator.
Higher tiers can reclaim batch attempts, native sidecar groups, model calls and
loads, idle engines (including pins), and environment builds. Each exact planned
stop records `priority_preemption`, demand/victim identity and policy revision.
Production victims keep their infrastructure retry budget; experiments keep
losses and checkpoint progress. A successful completion wins over preemption;
cancellation wins over both. Independent exit/OOM still fails under the shared
outcome classifier. A deadline authorizes stopping, never assumes cleanup.

Resumable victims get ten seconds by default, non-resumable commands stop
immediately. Deadline demand preserves non-resumable commands and keeps the
120-second grace for resumable commands. `queue --preemption-stop-grace`
configures the other priority-stop interval; the legacy
`--production-stop-grace` flag remains an alias. After two displacements, resumed
work gets at least that interval of runtime before another priority stop. Pins
are desired state and are restored when capacity permits. Model requests accept
`X-Myce-Class` (or legacy `X-Myce-Priority`); conflicts are rejected. Interrupted
OpenAI streams emit an SSE error with `type: interruption` and
`code: priority_preemption`; generic streams use the `X-Myce-Error-Code` trailer.
Partial output is never transparently replayed.

Equal-tier projects share available targets. One machine worker may run CPU
attempts and one batch attempt per GPU concurrently within the shared ledger. Same-tier work is not stopped
to rebalance usage: a single indefinite allocation can occupy a scarce device
indefinitely. Strict higher tiers can starve lower ones.

**Maintenance resumes.** Work Mycelium stopped for its own maintenance (host or
device maintenance with `--stop`, or a critical update) was running ahead of
everything still queued, so it keeps that place. Until its resumed attempt
starts it ranks ahead of its tier's other queued work, fair turn included;
among such resumes, the earliest first submission goes first. Higher tiers still
outrank it and production still preempts it. Its host drains toward it: the
backfill fence projects the release of every finite batch attempt on the host,
including accelerator work, and later work of its tier or lower may use only
the host RAM and cores beyond its need, never its card. The exception is work
expected to finish before the resume could start anyway: its `wall_time`, or
else the last accepted duration of the same work, ends no later than the
projected point at which enough running attempts have finished for the resume
to fit. Running attempts are never stopped for it, and when an attempt it needs
has no bound or measurement, nothing backfills past it.

## Sharing GPUs and measuring the pool

Projects in the same owner-configured tier share available GPU time, including
projects with different job classes. Higher tiers still win. A project running
alone can use the entire compatible pool.

For each worker, the scheduler considers only jobs that fit and pass their caps.
It totals those projects' recent usage, computes each project's weighted share,
and selects the project with the largest deficit (share minus usage). Model and
environment readiness, deadlines, front-of-line retries, and waiting time break
ties within the selected project. Across projects, equal usage favors fewer
active allocations per weight before warmth. Usage is keyed by stable tier
name, resource family and project. Its exponential history horizon follows the
current demand/service cycle: the largest of the weighted project-wait round,
the longest covered active allocation age,
and the latest completed allocation's covered duration in that tier/resource
family. The wait round sums the oldest waiting age once per ready, uncapped
project, multiplied by that project's policy weight divided by the smallest
current demand weight. Thus one turn at the smallest weight retains a complete
weighted round (3:1 demand retains four units, independent of the first winner).
Many tiny jobs from one project cannot inflate it by their count. A completed allocation supplies the
initial measured scale. Until that sample exists, preserve prior history and
count new covered service without decay; no daily half-life is guessed.

Every project in the ledger, including absent projects, uses the same horizon.
The queue integrates its continuously changing value analytically, so unrelated
reads/check-ins cannot change debt by splitting an unchanged service interval.
Idle ledgers decay on the latest completed service quantum. Active covered ages
are retained by exact attempt/session identity across reconnects and queue
restarts; unknown downtime adds neither service nor age.

A fresh short burst can shorten the retained history; sustained service or
long waits lengthen it. While a project waits and competitors keep receiving
finite allocations, its old debt decays and their new service accumulates,
allowing the waiter to regain a turn. No fleet-tuned time floor, ceiling or
reporting-retention cutoff controls this history. The separately retained
90-day usage report is unchanged. Historical usage is an admission signal,
not a promise of capacity on incompatible or occupied resources.
GPU device-seconds and CPU core-seconds use separate accounting families. Assigned batch preparation and model loading charge their owning
demand once, as do calls sharing a model allocation. Idle models/pins do not
consume fair usage. Running work is accounted before each scheduling decision.
This is admission fairness: same-tier work is not interrupted to rebalance
projects, and an indefinite allocation has no bounded-wait guarantee.

Set weights and caps in the revision-CAS [owner policy](scheduling.md).
The legacy `--project-config projects.yaml` option (also supported by `myce dev`)
seeds those same settings only when no policy has been stored:

```yaml
transcribe:
  weight: 3
sim:
  weight: 1
  max_concurrent: 3
  max_concurrent_experiments: 2
  max_memory_share: 0.25
```

Missing projects have weight 1 and no caps. Configured weights must be positive.
The optional `max_concurrent` cap counts running/stopping commands and assigned
calls across every tier/class, including preparation before Started. Zero means
unlimited. The legacy experiment concurrency cap counts only experiment commands
and assigned experiment calls. The memory cap applies to experiment reservations across online
workers' reported memory budgets, including each active model once per project
and worker. Production and deadlines bypass the legacy experiment caps; the all-class cap still applies. Caps are hard limits even
when a project is alone. Policies persist in SQLite; queue restarts preserve them
even when a project config is supplied. To apply a deliberate change, export the
current owner policy, edit its `projects` mapping, and import it with
`myce policy set FILE --if-revision N`. Editing the startup file alone does not
change an existing policy.

`myce status` and the page show cap or fairness reasons, for example:
`transcribe goes first (sim is ahead of its fair share)`. The reason also reports
retained resource-seconds, the weighted share among fitting competing projects,
signed debt (positive means ahead, negative means behind), the measured horizon,
weighted project-wait round, observed service age and the completed service
quantum. A zero completed quantum means initial learning, with history preserved
until an allocation completes. The horizon is shared across a tier/resource
family; fit/cap filtering still determines the projects used for each target
comparison. CPU seconds are core-seconds; GPU seconds are device-seconds.
NAS and machine fit failures
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
idle loaded models reserve memory but do not count as active compute. Fair
admission usage separately includes allocated batch preparation and model load
time, without changing these started-work reporting definitions. Pins are
reported separately and never turn an idle GPU into a busy GPU.

The queue integrates state changes into per-minute SQLite samples, split by
machine, physical device, project, job, and model. Samples include active/idle,
pinned reservation time and declared workload memory in GiB-seconds. Machine
cards use reported free memory against the configured budget; if no budget is
reported, capacity is the highest observed free memory, so used memory is an
estimate. Pinned memory is a subset of used memory, not extra consumption.
A machine worker reports a device vector; count each physical device once and
shared CPU/RAM once per host. Legacy per-device reports are transitional only.

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

## Live portability advice

From the next release, plain-command reports include `authored_eligibility`,
`environment_coverage` and `parity` per target. Resolve includes the same report
under `portability`. Rows come from observed inventory, authored exact targets
and the selected immutable Pixi lock's package keys, including absent targets.
All member environments intersect; unknown uv/binary/installed-tool metadata
does not imply universal compatibility. Unsupported runtime ports and stale
inventory remain explicit. Member `builds` distinguish ready, prepare-needed and
reported failure; host `admission` is the current planner snapshot as authored,
separate from permanent eligibility and dependency candidates. Read-only advice
never prepares, reserves, probes or rewrites targets. A lock covers dependencies,
not application binaries, shell assumptions, runtime downloads or output parity.

`require_target` on a plain command requests admission checking, not native
baseline qualification. Native extensions retain exact qualification. Existing
accepted placement records remain historical; no stored labels or intent are
rewritten. Reusable commands still require explicit targets, and `portable: true`
only permits architecture omission within a declared OS. Automatic omitted-target
semantics and failure-isolated discovery remain a separate #853 follow-up.

## Declare requirements, let Mycelium place

Agents state requirements rather than hosts: work kind or command, compatible OS/architecture/backend targets,
accelerator count/class, memory and environment. Mycelium selects an eligible
host using qualification, owner policy, available capacity and least-valuable-fit
ranking (MY-18). For ordinary CPU work, for example:

```yaml
needs: {platform: cpu, cpu_cores: 2, host_memory_gb: 2}
targets:
  - {os: linux}
  - {os: darwin}
environment: portable-tools
portable: true             # verified complete environment/payload coverage
```

Job targets require `os`. To omit `arch`, declare `portable: true` after verifying
that the complete payload runs on every matching architecture. This includes
plain commands (such as worker-built `go test`), scripts, environments, uploaded
and NAS inputs, archives, and runtime downloads. Mycelium uses the author's
declaration rather than inspecting payloads or inferring portability from builds.
Omit `platform` when `needs` already expresses the backend.

```yaml
portable: true
targets: [{os: linux}]
```

Without the declaration, an OS-only target receives: “declare portable: true if
this runs on any arch, or name the arch”. For an architecture-specific program,
state the requirement, for example `targets: [{os: linux, arch: arm64}]`.

Matching hosts still pass all resource, environment and qualification checks;
MY-18 chooses the least valuable fit among them. The declaration does not create
missing extension variants, bypass qualification or ignore environment coverage.
Extension variants retain complete, exact OS/arch/platform targets. MY-19 output
parity still applies before a production variant joins a split.

An exec-format failure is an ordinary command failure. Its launch diagnostic
explains that the program was built for another architecture and suggests
stating the required arch or declaring verified portability. Mycelium learns no
architecture exclusions and does not move the job in response to that error.

Use targets whose tools and outputs you have verified. `portable-tools` stands
for a registered catalog recipe covering those targets; native entries may use
their existing bundled per-variant environments. A single-platform lock requires
a platform restriction until another recipe is proven, not a particular host.
Use a multi-platform lock where possible and separate recipes where packages
differ. Cached packages alone prove neither command binding nor qualification;
custom-command reuse remains [#380](https://github.com/1kuna/mycelium/issues/380).

Host/device pins are exceptions for specific hardware, approved production
parity, or hardware qualification. Record an adapter exception alongside the pin:

```yaml
requirement_reasons:
  host: Approved production output parity currently covers this host only.
  device: This diagnostic qualifies the selected physical accelerator.
```

Declare only the reasons that apply. These are authored explanations, not proof
of parity or additional placement authority. A backend reason such as
`backend:cuda` explains a backend limit; it does not explain a particular host or
device. Keep the supporting evidence in the project's own records. Free-form
reason text is omitted from advice output.

`myce run` for adapters/profiles and `myce submit` print a non-blocking advisory
for pins without a declared matching reason. From the next release, plain profiles and job YAML also accept
`requirement_reasons`; submission retains the map with the job and private receipt.
Status/job GET show sanitized constraint, authored/missing state and source field,
while omitting raw text. Keep intentional exception evidence in authored metadata. `myce portability --adapter …` uses the same
advice in its notes. This identifies an unexplained restriction, not a proven
unnecessary pin; it changes no intent, target, qualification or production rule.
Full live-coverage warnings at registration remain
[#326](https://github.com/1kuna/mycelium/issues/326).

Use the job's placement rationale when reporting “Mycelium placed it on X because
Y”. `machine` and `allocated_devices` are assignment results.
Status already reports scarcity/admission rationale. Retained intent summaries
and clearer origin/explanation are tracked in
[#418](https://github.com/1kuna/mycelium/issues/418): current job/status APIs redact
exact targets and environment recipes, so an audit cannot infer their absence.

## Inspect coverage

Ask before authoring an adapter, and again when inventory or requirements change:

```sh
myce portability
myce portability --adapter .mycelium/adapter.yaml --entry train
myce portability --extension example@1 --project example --json
```

The first command needs only installed queue authority and returns the cluster's
observed OS/architecture/backend combinations, including CPU hardware. No fleet
list is configured. `--queue`, `--config` and `--joining-file` use the same pinned
authority as `myce run`; keep credentials in installed configuration.

Each target separates observed inventory, declared coverage, Mycelium runtime
support, variant metadata digest, resources, lock inputs/coverage, worker build
state, preparation, qualification identity and current admission. Declared exact
targets absent from inventory remain visible. Heartbeat and hardware observation
times are separate; stale or incomplete inventory cannot claim current admission.
Busy, leaving or maintained workers keep their declared coverage. An unsupported
Mycelium runtime requires a runtime port independently of application changes.

Adapter mode reads the selected entry's current local metadata and small,
explicit lock/manifest/build inputs relative to the source directory (or the
current project root). It does not archive Git sources or read a source snapshot.
The metadata budget is 1 MiB, matching the extension description limit; larger or
unavailable inputs remain missing/unknown. It never executes payloads, solves
packages, registers an extension, submits a job or changes placement. Native
adapter metadata has no frozen registration digest, so it cannot establish that
an existing worker is prepared or qualified. Use the registered extension form
for that evidence. Variant digests describe the recipe metadata supplied to the
report, not a new registration. Plain entries retain their own resource needs,
target requirements, catalog environment and existing result checks.

Registered extension mode reads the exact immutable queue registration and its
verified bundled inputs. Per-work check states remain separate; admission names the direct extension
entry (or the first declared legacy kind/program). Qualification uses the same target, fingerprint,
environment and check identity predicate as dispatch. Supply `--project` to
include that project's admission policy; without it admission cannot claim a
project-specific pass. Admission uses current planner resources, policy,
maintenance and release gates. It is a snapshot, never a reservation or a claim
that unverified installed tools will work.

A missing variant is a portability gap, not a hard software requirement. Optional
`requirement_reasons` in an extension or adapter entry records authored reasons:

```yaml
requirement_reasons:
  backend:cuda: This dependency requires the CUDA runtime.
```

This is explanatory metadata, not a new placement constraint. It does not widen
or narrow targets or bypass qualification. The report identifies the constraint
and whether its reason is authored or unknown. Free-form reason text stays in
the source metadata because it may contain private paths or credentials. Worker
error/log text, command arguments, file contents and raw host IDs are also
excluded; host ordinals and hashed qualification identities permit comparison
within the snapshot. Do not put secrets in identifier fields.

Advice names the actual package target (for example `linux-aarch64` when an
arm64 Linux worker is observed). Include `uv.lock` with `pyproject.toml`, or
`pixi.lock` with `pixi.toml`; reuse multi-target locks where valid and split recipes
where packages differ. Pixi's selected environment target declaration is reported
separately from a successful native frozen install. uv/platform marker coverage
remains unknown until native frozen installation verifies it; a present lock does
not prove wheel, ABI or toolchain support. Missing explicit bundled build
arguments point back to their argument position without disclosing source paths.

Use `MYCE_DEVICE` and assigned visibility. Queue backend `metal` maps to the
application's `mps` device. Add CPU implementations explicitly, with their own
resources and checks. GPU qualification must exercise its selected backend and
never silently fall back to CPU. Replace OS-specific shell/filesystem assumptions
with portable APIs, prepare through the existing operator catalog or bundled
variant mechanism, and qualify natively against the recorded baseline. MY-19
still requires owner-accepted output parity before splitting work across targets.

## API

Authenticated `GET /api/portability` returns inventory-only advice.
`GET /api/portability?extension=example%401&project=example` reads a registration.
Authenticated `POST /api/portability` accepts one of:

```json
{"extension":"example@1","project":"example"}
```

```json
{"recipe":{"name":"example","version":"1","variants":[]},"project":"example","requirement_reasons":{"backend:cuda":"Authored reason"}}
```

```json
{"spec":{"project":"example","run":{"command":["python3","run.py"]},"needs":{"cpu_cores":2,"memory_gb":1,"platform":"cpu"},"targets":[{"os":"linux","arch":"arm64","platform":"cpu"}]}}
```

`recipe` uses extension metadata including optional bundled `files` (JSON byte
arrays use base64). It may be incomplete so missing inputs can be explained.
The API permits the same 2 MiB JSON transport as extension registration to
accommodate base64 encoding of the bounded description bundle.
Advice never resolves caller-supplied filesystem paths, fetches assets or executes
commands. The JSON result is the shared projection used by the text command.
Unavailable authentication/API is an error, not an all-platforms pass; an empty
successful inventory explicitly reports `unknown`.

## Laptop proof

`myce dev --gpu cpu` runs the real queue and command runner without a GPU.
The focused `TestPortabilityDevMixedInventory` check starts that dev runtime and
posts recovering, synthetic Linux amd64/XPU, Linux arm64/CUDA and Darwin
arm64/Metal worker check-ins through `/api/workers/checkin`. These metadata-only
workers accept no payloads. Advice includes their exact targets and CPU
alternatives; adding another synthetic target changes the report. This proves
inventory-driven guidance, not native accelerator execution or qualification.

Host/device-pin advisories are described above. Automatic live-coverage warnings
at registration and scarce-host yield remain later phases of
[#324](https://github.com/1kuna/mycelium/issues/324); advice does not implement yield.
