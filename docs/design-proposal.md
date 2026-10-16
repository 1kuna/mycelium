# Mycelium v2: design (approved by maintainer 2026-10-08)


## Why a redesign

v1 tried to do too much at once and never thought about how people would use or test it. It was too heavy to run in production and too hard to test. v2 has one rule above all others: **every feature must be easy to use, and easy to test on a laptop with no GPU.** If a feature can't meet that bar, it waits.

## What it is

One shared queue of GPU jobs for every project: Verso, Atlas, experiments, and future projects. **GPU work only:** every job must need a GPU. Work that only needs a CPU, such as Atlas's SharePoint watching, stays outside the queue and runs wherever it runs today. Projects submit jobs. Machines pull the most important job they can run. No machine sits idle because its "own" project is quiet.

## The parts

| Part | Where it runs | What it does |
|---|---|---|
| Queue service | B70 CPU, always on | Holds the jobs in one local database file. Decides which job goes where. Serves a small HTTP API and one status page. |
| Worker | Every GPU machine that is in the fleet right now | Reports its free memory, GPU type and loaded models. Asks for work, runs jobs, sends back logs and results. |
| `myce` command | Anywhere | Submit, watch, cancel and read logs. `myce join` starts a worker on this machine. |

- **Permanent machines:** the DGX Spark (GPU work only). The B70's GPUs are lent by Verso: Verso starts the worker when it is idle and stops it when it needs the GPUs back. Verso keeps full control of the B70.
- **Machines that come and go:** the Mac minis, the MacBooks and the 4090 PC. Running `myce join` adds a machine; stopping the worker removes it.
- All machines are on one local network. Workers find the queue at a fixed address on the B70, and a join token keeps strangers out.
- Single Go binary, as in v1. There is no peer network, no leader election and no replicated state. If the queue service restarts, queued and running batch jobs survive in its database file, and workers wait and reconnect. Model calls that were in flight are not replayed and fail; callers must retry them.

## A job

```yaml
name: atlas-reindex-2026-10-08
project: atlas
priority: experiment        # production | deadline | experiment
deadline: null              # a time, required when priority is "deadline"
run:
  command: ["python", "reindex.py", "--batch", "7"]
  workdir: /srv/atlas
  env: {}
needs:
  memory_gb: 40
  platform: [cuda]          # any of: cuda (Spark, 4090), xpu (B70), metal (Macs)
  model: qwen3-32b          # optional; used to group jobs that share a model
expected_minutes: 90        # rough guess; used for planning, never enforced
resumable: true             # true = the job saves checkpoints and can be stopped and picked up later
```

The queue gives every job a few environment variables: its job id, a checkpoint folder on the NAS, and the attempt number. Because the checkpoint folder lives on the NAS, a paused job can resume on any machine. A resumable job only has to (1) save to the checkpoint folder now and then, (2) save and exit when it gets a stop signal, and (3) pick up from the checkpoint folder when it starts. The docs will show this in about ten lines of Python.

## How the queue decides

When a worker asks for work, the queue picks, from the jobs that fit this machine's free memory and GPU type:

1. the highest priority first: production, then deadline, then experiment;
2. within the same priority, a model already loaded, then cached locally, then needing a NAS copy;
3. then the earliest deadline;
4. then the job that has waited longest.

**Making room.** Production is checked first on every check-in and immediately
on submission. When no idle fitting machine has room, stop lower-priority work
right away through an authenticated long-poll notification, without waiting for
its next check-in. Resumable work receives SIGTERM and 10 seconds by default to
save; the queue's `--production-stop-grace` configures that duration. Deadline
work still allows 2 minutes and only stops resumable lower-priority jobs.
Production also stops non-resumable lower-priority work immediately. Stopped
jobs return to the front of their priority line without consuming an
infrastructure retry; non-resumable jobs start over with a new execution attempt.
**Make experiments resumable and save checkpoints often**: production may arrive
at any moment, and a power cut offers no grace at all. Status reports production
submission-to-start acknowledgement latency; the laptop dev test exercises the actual process
start under 15 seconds, excluding model load time.


**Optional physical-host hold (approved extension, 2026-10-11).** A queue host
policy may opt into whole-machine production takeover, default off. Production
selects one eligible physical host; all lower-priority work on that host stops
safely, regardless of how much room the production request needs. Stopped
experiments rejoin the shared queue and resume on any compatible machine, with
no infrastructure retry charged for preemption. Production admission waits for
all lower-priority cleanup acknowledgements. Unknown cleanup survives loss and
restart; an elapsed stop grace never proves release. After the final production
demand and cleanup settle, an idle cooldown (default 180 seconds, configurable
per host) excludes lower-priority admissions. New production resets that idle
interval; pins remain protected. This covers submitted GPU work, not CPU
publication. Project machine allow-lists apply at every priority and to model
calls; commands and servers receive their current placement in worker-owned
environment variables. See [the configuration and exact semantics](job-reference.md#physical-host-production-hold).

**Machines leaving.** Workers pull jobs, so a job is never tied to a machine until it starts. That means there is no list of waiting jobs per machine to clean up. Leaving works like this:

- **Planned leave** (`myce leave`, or Ctrl-C on the worker). The machine stops taking new jobs at once. Resumable jobs get the stop signal, save to the NAS, and go back to the front of their line. For jobs that can't be resumed, `myce leave` asks whether to wait for them to finish or to stop them now and put them back in the queue. `myce leave --now` skips the question and stops everything.
- **Sudden loss** (unplugged, asleep, network drop): without a check-in, the queue marks the machine gone after 5 seconds for production work, 20 seconds for deadline work, or 60 seconds for experiments (defaults based on the machine's active job priority). Its running jobs go back in the queue. Resumable jobs continue from their last checkpoint. Others start over. A job fails loudly after three counted infrastructure losses; planned stops to make room for production or deadline work, or because a machine is leaving, do not count. Model calls that were in flight are retried on another machine, and models loaded there are marked as gone.
- **Late return.** A network split can briefly leave the same job running on two machines. When contact returns, the queue keeps the resumable copy with the newest complete checkpoint, or the current attempt when there is no newer checkpoint, and tells the other copy to stop. Jobs must make output writes safe to repeat.
- **Jobs no remaining machine can run.** These are jobs pinned to the machine that left, or jobs that need a GPU type no current machine has (for example, Apple-only work when every Mac is away). They stay in the queue, and `myce status` says plainly why each one is waiting: "no machine in the fleet can run this right now (needs metal)". A job can set a give-up time, after which it is marked failed instead of waiting forever.

**Power and network loss are normal, not rare.** They happen regularly at maintainer's house, so every part must survive them without anyone stepping in.

- **The queue machine (B70) loses power.** The database is written safely (every change is fully saved to disk before it's confirmed), so a power cut never corrupts it or loses a submitted job. The queue starts automatically when the machine boots. After a restart it does *not* immediately declare every job lost. It waits a few minutes for workers to reconnect and report what they're still running or have finished, and only then puts back in line the jobs nobody reported.
- **A worker machine loses power.** This is handled as a sudden loss: its jobs go back in line after the priority-based timeout above (5, 20, or 60 seconds). On the always-on machines, the worker starts automatically at boot. On startup it cleans up any job processes left over from before the power cut, and then reports in fresh.
- **A worker loses network but keeps power.** It keeps running its current jobs, saves their logs and results locally, and keeps trying to reconnect. It does not start new jobs. When it reconnects it sends everything it saved. If the queue already gave a job to another machine, the queue reconciles the overlapping copies: the resumable copy with the newest complete checkpoint keeps running, otherwise the current attempt does, and the other stops. Output writes must be safe to repeat.
- **The whole house network drops, or the queue machine loses network.** If a strict majority of workers (including all of them) go quiet at once, the problem is almost certainly the network, not every machine at the same time. So the queue does not put everything back in line. It waits until machines reconnect, or until it can confirm specific machines are really gone.
- **The NAS is unreachable.** Experiments and production that declares `resumable: true` wait for their checkpoint folder. Non-resumable production starts with local scratch storage and does not require the NAS. NAS probes and checkpoint reads/preparation have short bounded waits outside the queue lock. A displaced experiment that cannot save within its grace is force-stopped and reported interrupted; production proceeds after memory release, and the experiment later resumes its last durable save. Running jobs otherwise keep going and retry their saves. `myce status` says plainly that the NAS is down.
- **Saved progress is never half-written.** The resumable-jobs guide requires writing each checkpoint to a temporary file and then renaming it, so a power cut mid-save leaves the previous checkpoint intact. When resuming or reconciling copies, a job uses the greatest monotonic generation recorded atomically in its checkpoint; file times and clocks on other machines do not determine progress. Legacy saves rank lowest with deterministic tie-breaks documented in the resumable-jobs guide.
- **Tests:** each of these cases has a test using fake time and pretend machines: queue restart, worker restart, worker cut off from the network, the whole network dropping, and the NAS going away.

**Data stays local.** Every machine in the fleet is on-premises, so job data never leaves our machines. A job can also list the specific machines it is allowed to run on.

## Using it

```
myce submit job.yaml        # prints the job id
myce status                 # what's running where, what's waiting, and why
myce logs <id> --follow
myce cancel <id>
myce join                   # turn this machine into a worker
myce leave [--now]          # take it out of the fleet
```

The same actions are available as plain HTTP calls, so projects can submit from code. The status page shows the same information as `myce status`.

These GPU-work and setup features are implemented now:

- **Any GPU work, not just batch jobs and OpenAI model calls.** GPU commands run as batch jobs; general HTTP model servers keep their existing interfaces and are loaded and unloaded by the queue.
- **GPU preference.** A job either requires a platform (`cuda`, `xpu`, `metal`/MLX), or states a preferred one, or doesn't care. If it doesn't care, it goes to any machine that can run it.
- **Environments.** A job names the software it needs, either a named environment (like `torch-cuda` or `mlx`) or a lock file. Workers report which environments they already have ready. The queue prefers a machine whose environment is ready, the same way it prefers a machine with the model already loaded. If none has it ready, a machine that *can* build it does so first.
- **Automatic setup.** `myce install worker` detects the GPU and operating system and installs the worker and its boot service; `myce install queue` sets up the queue service. Matching software recipes build on demand, rather than all being installed up front. Bare `myce join` uses the same GPU detection for a foreground worker; explicit GPU and device flags override it.
- **Model reservations are implemented now:** project-owned pins can hold a model continuously or within explicit weekday/time-zone windows. See Models below.

## Testing

- `myce dev` runs the queue and fake workers in one process on a laptop. Fake workers pretend to have any GPU and run jobs as ordinary processes.
- The scheduling rules are plain functions with a fake clock. Each rule above gets a test that runs in milliseconds.
- One end-to-end test submits a resumable job, stops it to make room, and checks that it resumes from its checkpoint.
- Real hardware is tested by running `myce join` on a real machine. There is no separate hardware test framework.

## Not in the first version

- Splitting one model across machines. It's not needed, because the largest load fits on the Spark.
- Automatic memory estimation. The job states what it needs.
- A backup copy of the queue service.

Each of these can be added later if real use shows a need.

## Later: any GPU work, on the best machine (the hard part)

Automatic detection of dependencies for an arbitrary project is not built yet. Jobs must name their software recipe or lock file.

## Order of work

1. Queue, worker and `myce` for batch jobs, working entirely on a laptop (`myce dev`) with tests.
1b. Model calls: workers load and unload models on demand (using llama.cpp, plus vLLM where available).
2. Queue on the B70 CPU, worker on the Spark, first real jobs from Atlas.
3. Add the Macs and the 4090.
4. Verso lends the B70 GPUs.
5. Move experiments over, with the checkpoint guide.

## Decisions (maintainer, 2026-10-08)

1. **GPU work only.** CPU-only work stays outside the queue.
2. **No app holds a GPU continuously; that is the point.** Every app is reworked so its GPU use becomes jobs. That takes two kinds of job:
   - **Batch jobs:** a command that runs and finishes (indexing, training, transcription runs). Described above.
   - **Model calls:** a single request to a model, such as an embedding for one search or one chat completion. Apps send these to the queue in the standard OpenAI request format, with a priority. The queue sends the call to a machine that already has that model loaded. If no machine has it loaded, the queue has a worker load it, and the first call waits for the load. An unpinned loaded model that hasn't been used for a while (default 10 minutes), or whose memory a more important job needs, is unloaded. So a model stays loaded only while it's actually being used.
   Atlas search, for example, stops holding a GPU. Each query becomes a model call, and when nobody is searching, the GPU is free for other work.
3. **Checkpoints and shared files live on the NAS**, so jobs can resume on any machine.
4. **One local network.** No Tailscale or internet exposure needed.
5. **Plain commands, not containers.** Containers would add isolation, and would bundle each job's software so it runs the same on every machine. Plain commands mean each machine needs the job's software installed, but they are much simpler and work on the Macs, where GPU containers don't. Containers can be added later for the Linux machines if setting up software becomes a pain.

## Models: NAS masters, local cache, and production pins (maintainer, 2026-10-09)

The NAS holds the master copy of every model file. `myce model fetch` explicitly
downloads an approved Hugging Face HTTPS file into a NAS folder, verifying its
expected byte count and SHA-256. Loading never fetches from the internet.
The catalog `path` names the NAS master: preferably relative to each worker's
`--model-root`, or an absolute NAS mount path when all workers share that path.
Real models require `size_bytes` and `sha256`. Keep immutable masters and update
the catalog when changing weights.

Each worker has a local `--model-cache` directory (default under its state
folder) and `--model-cache-bytes` limit (default 100 GiB). First use checks local
free space and cache capacity, moves least-recently-used unloaded copies to
Trash, writes a `.partial` file, verifies size and SHA-256, syncs, and renames.
Loaded/loading/draining models are protected from cache eviction. Restart retains
interrupted copies and resumes from their byte offset after rehashing the prefix. With `--model-root`, Trash lives under the NAS root;
cross-filesystem eviction saves and verifies a recovery copy before finishing
the move. Without a model root, user Trash is used; it may not release local
blocks, in which case the load refuses clearly. Never empty Trash automatically.
Worker check-ins report verified cached model names, available cache bytes (the lesser of
filesystem free bytes and remaining cache allowance), and errors. Status exposes
those reports. Copy and verification time are part of model load time. Large
models always load from local disk. `direct_nas: true` is an explicit per-model
exception intended for small models; verify it before reading the NAS master.

`myce model pin MODEL --project PROJECT --machine MACHINE` holds a production
model on one worker. `--platform cuda|metal|xpu` instead chooses one eligible
worker of that platform, retaining the assignment while healthy. Pins are stored
in SQLite and shown in status. They reserve the model's GPU memory and prevent
idle unloading, priority reclamation, and disk-cache eviction while loaded.
`myce model unpin MODEL --project PROJECT` releases protection. Optional
`--weekdays 1,2,3,4,5 --start 08:00 --end 20:00 --timezone America/New_York`
implements reservations now. Windows are start-inclusive/end-exclusive and must
not cross midnight. Initial pin warmup and unavailable hardware remain visible;
a ready pin serves calls without a model load wait. A pin cannot promise
availability while its machine or NAS is down.

### Pre-copying (maintainer, 2026-10-09)

`myce model sync MODEL --to machine|platform|all` persists a request to copy NAS
masters before use; `--cancel` pauses that target request without discarding
partial bytes. Pins warm/protect every possible landing worker's disk cache,
including before a scheduled window. Idle workers pre-copy queued and recently
used models, most-needed first. Real jobs, engine loads and model calls pause
background copies. One copy per worker is the default, configurable with
`--model-copy-parallel`. Cache limits include partials and full active copy
reservations; background eviction never displaces pinned, loaded, active or
more-used models. Final size and SHA-256 verification precede atomic publication.
Status reports progress, errors and measured achieved copy speed. Placement
estimates remaining NAS bytes divided by that worker's measured speed (40 MB/s
until measured), prefers verified cached models, and keeps engine startup time
separate from this transfer estimate. Everything remains laptop-testable.

## Fair sharing, usage history, and status (maintainer, 2026-10-09)

Experiments share GPU time between competing runnable projects by configurable
weight (equal by default), using recent usage with a 24-hour decaying half-life.
Select the greatest fair-share deficit before existing affinity/wait tie-breaks.
A sole project can use everything. Production and deadline order is unchanged.
Optional experiment concurrency and GPU memory-share caps are project-specific.
Status explains fairness and cap decisions in plain words.

The same SQLite queue keeps minute-level device/project/job/model allocation
samples for seven days, then hourly totals through 90 days. Busy allocation,
idle time, memory, pins, preemptions and production start latency distinguish
active pool use from reservations and unknown downtime. `myce usage` reports
these totals. A self-contained responsive status page shows the fleet, queue,
seven-day SVG charts and problems. See [the guide](guide-fairness-usage.md) for
configuration, accounting definitions and known measurement limits.

## Docker command containment (maintainer, 2026-10-11)

The authorized Verso gap-3 extension adds opt-in `run.docker` for a supported,
direct Docker batch command. One worker-owned create/start request, immutable
attempt labels and durable cleanup custody keep daemon-owned containers within
the existing worker/SQLite accounting contract. Unconfirmed release retains
the physical host's exclusion across loss/restart. Plain commands remain the
default; no container scheduler, peer network or extra service is introduced.
The exact supported subset and limits are in [job reference](job-reference.md#managed-docker-batch-commands)
and [operations](operations.md#docker-cleanup-custody).

## Per-attempt RAM ceilings (authorized Verso gap-2 extension)

Declared batch/model host RAM is enforced on Linux cgroup v2 workers with
systemd memory delegation. Each execution remains inside its worker service's
cgroup, so service-wide disable still contains every native descendant.
Workers report unsupported enforcement plainly and keep laptop admission-only
execution. An optional physical-host budget caps the sum of declared ceilings,
including configurable slack, and retains unconfirmed allocations after loss.
Managed Docker batches install and inspect daemon-side memory limits. OOM is a
normal failure rather than infrastructure loss. The configuration, unsupported
cases and qualification limits are in [operations](operations.md#host-ram-admission-and-ceilings).

## Experimental v3 file movement (resolved #24, slice #42)

On the separate v3 branch the queue hypha can store SHA-256-identified files and
serve resumable HTTP transfers, with bounded caches on execution hyphae. Inputs
and models are explicit; outputs and checkpoint generations are published only
after verified durable transfer. Checkpoint commands still use
`MYCE_CHECKPOINT_DIR`; local-only progress cannot resume elsewhere. The single
SQLite queue, plain commands, injected scheduling clock and attempt ownership
remain. Explicit NAS configuration retains the v2 path. One store is not a backup.
See [files without a NAS](guide-files-v3.md) for configuration and retention limits.

## Shared native server entry additions (approved 2026-10-16, #68)

Server entries may set `project` and `allowed_machines`. Models stay shared
across projects; owner and caller placement policies intersect. Optional `env`
contains non-secret literals; `secret_env` references worker-local named secrets,
read only at launch from private mode-0600 files, with `logs: none` required.
An explicit per-folder/per-host `local_source` approval permits in-place model
use, including HF blob symlinks within the approved root. Fingerprint once,
check metadata subsequently, and require explicit reapproval after changes.
This opt-in source is an exception to the normal verified-copy contract;
ordinary copied-model custody is unchanged. Authenticated header-less `/v1`
calls default to the entry owner and `experiment`; a per-project production
default requires explicit operator opt-in. Existing entries retain defaults.
See [operator configuration](design-68-server-entries.md).

## Log opt-out rollout (#68 review decision)

Workers advertise `logs_none` in optional `capabilities`; the queue assigns
`logs: none` batches and server entries only to those workers. Waiting status
names the missing capability. Environment build output remains in environment
records, without per-job copies or job-log fallback text for opted-out jobs.
Roll queue and workers together; do not downgrade with opted-out work live.
See [log policy and downgrade behavior](job-reference.md#scheduling-and-results)
and the [native proof checklist](guide-servers.md).
