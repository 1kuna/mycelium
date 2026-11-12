---
name: mycelium
description: "Use when work needs fleet compute (heavy tests, builds, sims, training, transcodes, model runs), when you submit, check, wait on or fetch a Mycelium job, when a job is waiting, when your work must go first, or when you need to pause your own queued job."
---

<!-- Public consumer guide. Edit this contract in place; detail lives in references/. -->

# Mycelium

Mycelium runs your commands on a fleet of CPU and GPU machines through one
queue. You say what the work needs; Mycelium picks the machine, runs it and
hands back verified files. Climb the ladder only as far as your work needs.

## Ground rules

- Use the installed `myce` CLI or Python `Client.from_installed()`; both find
  their own credentials. Never open, print, copy, pass or set fleet credentials.
  A missing CLI or refused authority is a hold for the installation owner: keep
  its typed refusal and keep heavy work off your own machine.
- Describe what the work needs, never which machine: `--gpu cuda` finds CUDA
  hardware. Pin hardware only for a [real requirement](references/submit.md#explicit-placement-restrictions).
- `myce COMMAND --help` is the flag reference. A field your installed queue or
  worker doesn't return is unknown, not false.

## 1. One command

```sh
myce run -- sh -c 'echo "hello from $(uname -sm)"'
```

This submits the command, streams its logs and exits with its exit code. It runs
with the worker's installed tools in an empty folder. `--cpu N`, `--mem 8G` and
`--gpu cuda|xpu|metal` describe needs; leave memory out and Mycelium learns it.

## 2. Files back

```sh
myce run --out accepted --output result.txt -- sh -c "printf 'mycelium-ease-audit-ok\n' > result.txt"
cat accepted/result.txt
```

`--out DIR` waits, downloads each `--output` the command wrote and checks its
size and SHA-256. It exits `0` when every file is verified, with the command's
own code when it failed, and `125` when the job was accepted but the trip didn't
finish: rerun the same command to pick it back up
([all exit codes](../../docs/adapter.md#run-wait-and-fetch-in-one-command)).
Then check the files yourself; exit zero doesn't prove they're right.

## 3. A reusable entry

To send your project's code, its inputs and environment, give the work an entry.
From the project root:

```sh
myce init --entry hello -- sh -c "printf 'mycelium-ease-audit-ok\n' > result.txt"
```

`myce init` writes `.mycelium/adapter.yaml` with one entry. With no command it
runs `python -m pytest`, and it uses a `pixi.lock` or `uv.lock` it finds as the
entry's environment. Add the files you want back:

```yaml
project: example
entries:
  hello:
    run: {command: [sh, -c, "printf 'mycelium-ease-audit-ok\n' > result.txt"]}
    outputs: {result.txt: results}
```

Outputs may be files, folders (`plots/`) or globs (`"logs/*.txt"`), and `--out`
recreates the subfolders. To get logs from a failed run without exiting 0, add
`keep_on_failure: [logs/]` and fetch them with `myce result JOB_ID --attempt N
--out DIR`.

The entry sends your committed `HEAD`, so commit what it runs. Untracked files
that aren't ignored make `HEAD` dirty, so ignore your `--out` folder:

```sh
printf 'accepted/\n.mycelium/receipts/\n' >> .gitignore
git add .gitignore .mycelium/adapter.yaml && git commit -qm "Run hello on Mycelium"
myce run --entry hello --wait --out accepted
```

The same entry, source and arguments make the same job, so rerunning after a
lost reply adopts it and fetches again; pass a new `--key NAME` to run again on
purpose. Arguments after `--` are appended to the command. `myce spec check
--resolve --entry hello .mycelium/adapter.yaml` previews placement without
submitting. Only production entries need a target list; every field is in
[the adapter reference](../../docs/adapter.md).

## 4. Environment

- **Installed tools** are the default. List what the command calls in
  `needs: {tools: [git, node]}` so placement skips machines without them.
- **Locked environments**: `environment: {backend: pixi, lock_file: pixi.lock}`,
  `{lock_file: uv.lock, python: "3.12"}` or
  `{backend: npm, lock_file: package-lock.json}`. A relative `lock_file`
  is read from the project root and uploaded with its companion file; workers
  install from the lock, never solve, and reuse it
  ([environments](references/submit.md#environments)).
- **Threads and scratch** are set for you: `MYCE_CPUS` is the job's CPU share,
  common thread pools (`OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `GOMAXPROCS` and
  others) default to it, and `TMPDIR` and `MYCE_SCRATCH_DIR` point at the
  attempt's scratch. Size worker pools from `MYCE_CPUS`.
- **Network** is off for the command unless you declare `needs: {network: true}`.

## 5. Inputs

Small files travel with the job: `inputs: {DEST: local/path}` in the entry, or
`--input DEST=PATH` for one run. Declare NAS data instead of copying it; the
worker stages it on local disk and verifies it before the command starts, so
processing never runs at network speed (MY-36):

```yaml
    shared_inputs:
      footage: {nas: media/day1.mov, sha256: SHA256_HEX, size_bytes: 2000000000}
```

The command reads the local copy at `$MYCE_INPUT_FOOTAGE`. NAS paths are
relative to the NAS root ([inputs](references/submit.md#inputs-checkpoints-and-pieces)).

## 6. Many pieces

To run one command over many inputs, add `pieces:` to the entry. One run and one
wait cover them all, and results come back as `KEY/OUTPUT`:

```yaml
    pieces:
      - {key: a, args: [--slot, a], inputs: {slot.yaml: slots/a.yaml}}
      - {key: b, args: [--slot, b], inputs: {slot.yaml: slots/b.yaml}}
```

Each piece is its own job, placed, retried and recovered separately, up to 256
per entry ([pieces](../../docs/jobs.md#plain-command-pieces)).

## 7. Checkpoints and resumable work

A stopped job loses everything since its last checkpoint. For anything long,
save state atomically into `$MYCE_CHECKPOINT_DIR`, restore from it at start, and
declare `resumable: true`; Python's `mycelium.Checkpoint` and `StopSignal` do
the saving and the signal. A stop sends SIGTERM to the process group with 10
seconds (production preemption) or two minutes (deadline, planned leave) before
SIGKILL, so also save periodically. Checkpoints are kept 72 hours after the job
ends; declare outputs for anything you need
([checkpoints](references/submit.md#inputs-checkpoints-and-pieces)).

A resume stays on the target (OS/arch/platform) that wrote the newest
checkpoint, waiting for a host of that target even while it is busy or in
maintenance; status shows the wait as `checkpoint_target`. It moves to another
target only when no host of that target remains in the fleet, or when the job
declares `resume_compatibility` listing both targets because its checkpoints
are tested to resume there. `portable: true` covers the program, not its
checkpoints.

## 8. Classes and tiers

The class decides who waits and who gets stopped. Pick it before you submit;
don't find out by losing work.

| Class | Use for | What happens |
| --- | --- | --- |
| `production` | Work a person or customer is waiting on, as your project defines it (user-facing renders, deliveries) | Goes first and is never preempted by a lower tier; takes capacity from deadline and experiment work; must start within its start window (default 150 s) or fails visibly; needs explicit `targets` (MY-19) |
| `deadline` | Work with a real owner-set date (`--class deadline --deadline RFC3339`) | Ahead of experiments; stops only resumable experiment work |
| `experiment` (default) | Everything else: CI, research, sweeps, previews | Production can stop it at any time and requeue it; non-resumable work restarts from scratch |

To let production run on another host, your project adds it itself after a passing output-comparison check or experiment there: `myce parity add --project P --work WORK --host HOST --evidence JOB_ID` (no queue-owner edit; [details](../../docs/scheduling.md)).

Equal tiers never preempt each other. Within a tier the queue goes by
readiness, deadline and age, with no ordering hint, so order your own work with
the right class, `after:` stages and checkpoints rather than pauses or lock
files. Lost machines and critical Mycelium updates can interrupt any tier; the
job requeues on the same key and resumes from its checkpoint
([classes](references/waiting.md#classes-and-preemption)).

## Pipelines

Submit every later stage up front with `after:`, so the queue can see the whole
pipeline. Each stage starts once its producer's outputs are accepted, and
cancelling the producer cancels what depends on it:

```yaml
name: reduce
project: example
after: {job: PRODUCER_JOB_ID, outputs: {a.json: a/metrics.json, b.json: b/metrics.json}}
needs: {platform: cpu, cpu_cores: 1}
run: {command: [sh, -c, 'cat a.json b.json > combined.json']}
outputs: {combined.json: results}
```

`myce run --entry NAME` without `--wait` prints the producer's receipt with
its `job_id`; outputs of a pieces producer are named `KEY/OUTPUT`. Submit the
stage with `myce submit FILE` or Python `client.submit(spec)`
([stages](../../docs/jobs.md#start-a-consumer-after-every-producer-piece-succeeds)).
A submitted production stage counts as open production work while it waits on
`after:`, so drains and fleet upgrades keep a host able to start it; open a
[production lease](references/waiting.md#production-leases) only for stages you
can't submit ahead.

## When a job fails or waits

- **Failed:** read its `finding` (printed by `myce wait`, shown by status and
  `myce result`): `owner` is `job`, `mycelium` or `unknown`, and `action` is the
  one thing to do. A `dead_end_wait` names what waiting can't fix; do it, then
  submit again.
- **Waiting:** read `myce status JOB_ID` once. Its `hold` gives one reason, an
  owner and one `next:` action. Fix what you authored, or record the owner and
  action on your work record and move on. Keep the same job and key; resubmitting,
  pinning or running locally only hides a hold ([waiting](references/waiting.md)).
- **Long waits:** one foreground `myce wait JOB_A JOB_B` covers up to 256 jobs,
  and a timeout leaves them running. `queue_stalled` means the queue stopped
  making progress: read status once, then repeat the same command
  ([wait and status](references/wait-and-status.md)). For a wake while you're
  away, bind a notifier ([T3 binding](t3-binding.md)).
- **Stopping:** `myce cancel --wait JOB_ID` exits 0 once the job won't run and
  its resources are released (75 while release is pending); `command_started:
  false` means no command began. `myce pause JOB_ID --reason TEXT` holds your
  own queued job, and `myce resume JOB_ID` keeps its place.

Finished jobs' files are kept for 3 days by default, then expire
(`result_expired`); download results within that window. The job record itself
is kept for 14 days after the job finishes. After that, `myce status JOB_ID`
and job GET answer `410 job_expired` with a one-line summary (project, name,
final state, finish time, host, duration, peak memory), and its logs are gone.
A submission key still replays the same job ID and final state. What the queue
learned from the job (memory evidence, durations, envelopes) stays. Record any
detail you need from a job within 14 days.

## Python

```python
from pathlib import Path
from mycelium import Client

client = Client.from_installed()
outcome = client.run_entry("hello", out="accepted")
assert Path("accepted/result.txt").read_bytes() == b"mycelium-ease-audit-ok\n"
```

`run_entry` is `myce run --entry ... --wait --out` (same key, same job) and
raises `EntryFailedError` on failure. For specs built in code, use
`client.submit`, `client.wait_many` and `client.download_results`
([Python client](../../clients/python/README.md)).

## What you can now delete

In an existing project, Mycelium now does these; delete your own:

- submit, wait, fetch and verify scripts: `myce run --entry NAME --wait --out DIR`;
- marker files proving a job finished: the exit code and verified files;
- tar steps: `inputs:`, `pieces:` and folder or glob outputs;
- exit-0 wrappers that return logs from a failure: `keep_on_failure`;
- thread-count variables and temp-dir setup: `MYCE_CPUS` and `TMPDIR`;
- host pins and per-host entries: needs, with no target list off production;
- shell NAS copies: `shared_inputs`;
- one submission per input in a loop: `pieces:`.

`myce adapter convert` turns old profiles into minimal entries
([setup](setup-project.md)).

## References

- [Submit](references/submit.md): needs, source and keys, environments,
  isolation, models, inputs, checkpoints, placement restrictions.
- [Waiting](references/waiting.md): holds, classes and preemption, device and
  storage holds, pausing, production leases.
- [Wait and status](references/wait-and-status.md): batch waits, status
  (host RAM is `host_memory`; older RAM fields are deprecated), stalled queues,
  observers, progress.
- [Results](references/results.md): output failures, diagnostics, large outputs.
- [Setup](setup-project.md), [harness](harness-binding.md) and
  [T3 binding](t3-binding.md): adoption, skill install, notifiers.
- [Owner operations](references/owner-operations.md): queue-owner writes.
- No fleet: `myce dev -- sh script.sh` runs your script against a temporary
  queue with fake GPUs ([quick start](../../docs/START-HERE.md)).
- Problems: search the public issues, then file one with expected and observed
  behavior, version, job and attempt, and no credentials or private paths.
