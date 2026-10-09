# Mycelium v2: design (approved by Zach 2026-10-08)


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
- Single Go binary, as in v1. There is no peer network, no leader election and no replicated state. If the queue service restarts, workers wait and reconnect; nothing is lost because the queue writes everything to its database file.

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
2. within the same priority, a job whose model is already loaded on this machine (this avoids slow model loads);
3. then the earliest deadline;
4. then the job that has waited longest.

**Making room.** If a production or deadline job is waiting and no machine has room, the queue may stop a lower-priority job that is marked `resumable`. That job gets a stop signal and a grace period (default 2 minutes) to save and exit, then goes back to the front of its priority line. A job that is not resumable is never stopped to make room; it finishes first.

**Machines leaving.** Workers pull jobs, so a job is never tied to a machine until it starts. That means there is no list of waiting jobs per machine to clean up. Leaving works like this:

- **Planned leave** (`myce leave`, or Ctrl-C on the worker). The machine stops taking new jobs at once. Resumable jobs get the stop signal, save to the NAS, and go back to the front of their line. For jobs that can't be resumed, `myce leave` asks whether to wait for them to finish or to stop them now and put them back in the queue. `myce leave --now` skips the question and stops everything.
- **Sudden loss** (unplugged, asleep, network drop): after 60 seconds without a check-in, the queue marks the machine gone. Its running jobs go back in the queue. Resumable jobs continue from their last checkpoint. Others start over, up to 3 attempts, then fail loudly. Model calls that were in flight are retried on another machine, and models loaded there are marked as gone.
- **Late return.** If a lost machine comes back while its old job is still running, the queue tells it to stop that copy, so one job never runs twice.
- **Jobs no remaining machine can run.** These are jobs pinned to the machine that left, or jobs that need a GPU type no current machine has (for example, Apple-only work when every Mac is away). They stay in the queue, and `myce status` says plainly why each one is waiting: "no machine in the fleet can run this right now (needs metal)". A job can set a give-up time, after which it is marked failed instead of waiting forever.

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

## Testing

- `myce dev` runs the queue and fake workers in one process on a laptop. Fake workers pretend to have any GPU and run jobs as ordinary processes.
- The scheduling rules are plain functions with a fake clock. Each rule above gets a test that runs in milliseconds.
- One end-to-end test submits a resumable job, stops it to make room, and checks that it resumes from its checkpoint.
- Real hardware is tested by running `myce join` on a real machine. There is no separate hardware test framework.

## Not in the first version

- Splitting one model across machines. It's not needed, because the largest load fits on the Spark.
- Automatic memory estimation. The job states what it needs.
- Fairness quotas between projects.
- A backup copy of the queue service.

Each of these can be added later if real use shows a need.

## Later: any GPU work, on the best machine (the hard part)

This is not in the first version, but the job format is designed now so it won't have to change later.

- **Any GPU work, not just batch jobs and model calls.** Anything that needs a GPU can be a job.
- **GPU preference.** A job either requires a platform (`cuda`, `xpu`, `metal`/MLX), or states a preferred one, or doesn't care. If it doesn't care, it goes to any machine that can run it.
- **Environments.** A job names the software it needs, either a named environment (like `torch-cuda` or `mlx`) or a lock file. Workers report which environments they already have ready. The queue prefers a machine whose environment is ready, the same way it prefers a machine with the model already loaded. If none has it ready, a machine that *can* build it does so first.
- **Automatic setup.** One install command on a new machine detects its GPU and operating system, installs the worker, and sets up the matching environments.
- **Reserving models at set times.** If a person ever needs instant answers, a production model could stay loaded during set hours. Today the callers are background systems, so waiting for a model to load is fine.

The first version already has the hooks for this: the `platform` and `environment` fields exist from day one, and the scheduling rules treat "environment ready" like "model loaded".

## Order of work

1. Queue, worker and `myce` for batch jobs, working entirely on a laptop (`myce dev`) with tests.
1b. Model calls: workers load and unload models on demand (using llama.cpp, plus vLLM where available).
2. Queue on the B70 CPU, worker on the Spark, first real jobs from Atlas.
3. Add the Macs and the 4090.
4. Verso lends the B70 GPUs.
5. Move experiments over, with the checkpoint guide.

## Decisions (Zach, 2026-10-08)

1. **GPU work only.** CPU-only work stays outside the queue.
2. **No app holds a GPU continuously; that is the point.** Every app is reworked so its GPU use becomes jobs. That takes two kinds of job:
   - **Batch jobs:** a command that runs and finishes (indexing, training, transcription runs). Described above.
   - **Model calls:** a single request to a model, such as an embedding for one search or one chat completion. Apps send these to the queue in the standard OpenAI request format, with a priority. The queue sends the call to a machine that already has that model loaded. If no machine has it loaded, the queue has a worker load it, and the first call waits for the load. A loaded model that hasn't been used for a while (default 10 minutes), or whose memory a more important job needs, is unloaded. So a model stays loaded only while it's actually being used.
   Atlas search, for example, stops holding a GPU. Each query becomes a model call, and when nobody is searching, the GPU is free for other work.
3. **Checkpoints and shared files live on the NAS**, so jobs can resume on any machine.
4. **One local network.** No Tailscale or internet exposure needed.
5. **Plain commands, not containers.** Containers would add isolation, and would bundle each job's software so it runs the same on every machine. Plain commands mean each machine needs the job's software installed, but they are much simpler and work on the Macs, where GPU containers don't. Containers can be added later for the Linux machines if setting up software becomes a pain.
