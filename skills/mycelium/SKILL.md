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

When status says `waiting for fresh accelerator inventory`, leave the existing
job queued: Mycelium holds GPU placement until a fresh scan arrives, while CPU
work continues. `myce status --sections machines --json` exposes
`inventory_probe.state`, `operation`, `pending_seconds` and `error_class` for the
queue owner. Worker contact alone does not prove usable accelerator inventory;
resubmitting the job does not repair that evidence. Report a persistent wait to the queue owner with those fields.

## 1. One command

```sh
myce run -- sh -c 'echo "hello from $(uname -sm)"'
```

This submits the command, streams its logs and exits with its exit code. It runs
with the worker's installed tools in an empty folder. `--cpu N`, `--mem 8G` and
`--gpu cuda|xpu|metal` describe needs; leave memory out and Mycelium learns it.
A typed memory figure is a limit, not a reservation: once the work has run,
Mycelium charges what it measured and keeps your figure as the ceiling.

## 2. Files back

```sh
myce run --out accepted --output result.txt -- sh -c "printf 'mycelium-ease-audit-ok\n' > result.txt"
cat accepted/result.txt
```

`--out DIR` waits, downloads each `--output` the command wrote and checks its
size and SHA-256. It exits `0` when every file is verified, with the command's
own code when it failed, and `125` when the job was accepted but the trip didn't
finish: rerun the same command to pick it back up
([all exit codes](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#run-wait-and-fetch-in-one-command)).
Then check the files yourself; exit zero doesn't prove they're right.

Killing a waiter never cancels its job. Ctrl-C, SIGTERM or parent exit detaches
`myce run` with code `126` (job continues), printing the job ID and key. Re-attach
by rerunning the same command with the same `--key`; entries retain their generated
key when intent is unchanged. Inline commands need an explicit `--key` to re-attach
by rerunning. Cancel only explicitly: `myce cancel --wait JOB_ID`.


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
`advanced: {keep_on_failure: [logs/]}` and fetch them with `myce result JOB_ID --attempt N
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
submitting. To test the same entry on each platform, use `myce run --entry
hello --platform metal --wait` (or `cuda`, `xpu`, `cpu`); preview with `myce spec
check --resolve --entry hello --platform metal .mycelium/adapter.yaml`. The flag
only narrows platforms the entry allows; a refusal names the allowed set.
GPU work allows CPU only through `advanced: {allow_cpu_fallback: true}`; the
flag does not opt in. Each explicit platform gets a different submission key
from the unrestricted run and other platforms. `--gpu` and `--cpu` remain
unsupported with entries. Only production entries need a target list; every field is in
[the adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md).

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
- **Big scratch** (renders, builds, unpacked archives): add
  `disk: {scratch_bytes: N}` with the work dir plus `TMPDIR` peak, so the job
  waits for a host with room instead of failing inside a full disk. After one
  accepted run Mycelium uses the measured peak instead; per-item work (new
  inputs each run) uses its entry's peaks after two accepted runs
  ([disk needs](references/submit.md#disk-needs)).
- **Network** is off for the command unless you declare `needs: {network: true}`.

## 5. GPU work on every accelerator

One PyTorch entry can run on CUDA, Intel Arc (XPU) and Metal, wherever there is
room ([example](https://github.com/1kuna/mycelium/tree/main/docs/examples/run-anywhere)):

- **Entry:** `needs: {platform: any}` and a `uv.lock` environment. No target
  list, host or memory figure.
- **Lock:** one `pyproject.toml` lists the OS/architecture pairs in
  `[tool.uv] environments` and takes Linux amd64's `torch` from PyTorch's XPU
  index (`explicit = true`); other platforms use the PyPI wheel. List
  `triton-xpu` as a direct dependency for Linux amd64, or uv looks for it on
  PyPI and the lock fails. `uv lock` writes one lock for every platform.
- **Device:** take it from `MYCE_DEVICE`, which Mycelium always sets, with
  `torch.accelerator.current_accelerator()` as the fallback outside Mycelium.
  Then call `torch.empty(1, device=device)` so a wrong torch build fails at
  once instead of training on CPU.
- **Calls:** device-neutral ones (`torch.accelerator.synchronize()`,
  `torch.accelerator.max_memory_allocated()`), not `torch.cuda.*`, and float32,
  since Metal has no float64.

GPU work never falls back to CPU unless the entry sets
`advanced: {allow_cpu_fallback: true}` (MY-54). Without it, `platform: any`
never places on CPU. Don't set `PYTORCH_ENABLE_MPS_FALLBACK`. To run on CPU on
purpose, add an entry with `needs: {platform: cpu}`.

For ordinary PyTorch this replaces per-backend entries or variants, a lock per
accelerator family, target lists, and qualification runs before experiment
work. Use [variants](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#write-an-extension)
when the software differs per platform (MLX on Mac, vLLM on CUDA), when a
service sidecar is part of the job, or for production, which still needs
explicit targets and MY-19 output parity per kind of machine.

## 6. Inputs

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

For data that must leave the execution worker, set
`advanced: {worker_data: ephemeral}` on an offline, non-resumable command with
managed inputs and declared outputs, using installed tools. It stages privately
without persistent payload caches, retains successful output bytes until durable
queue acceptance, then verifies removal of local inputs, extracted/shared data,
scratch/temp/private home, checkpoints, outputs and delivered logs before release.
Check job `worker_data_cleanup` and the attempt-history receipt: `failed` or
pending cleanup is not a satisfied policy, even if the result succeeded. Retries
restage inputs; startup recovers process custody and cleanup. Supporting Linux
namespace workers with file core routing or the qualified Mycelium core router
are required; other combinations are refused with `job_worker_data_unsupported`.
Piped core handlers ignore
`RLIMIT_CORE`; only Mycelium's qualified router discards ephemeral cores unread
and records signal/time/attempt metadata. Other piped handlers and older routers
remain refused. Operators can [switch routing and undo it](https://github.com/1kuna/mycelium/blob/main/docs/operations.md#linux-core-routing-for-ephemeral-attempts).
Unlink is not secure SSD erasure; use full-disk encryption for at-rest protection.
Queue/NAS retention is unchanged
([scope and limits](https://github.com/1kuna/mycelium/blob/main/docs/files.md#ephemeral-worker-data)).

## 7. Many pieces

To run one command over many inputs, add `pieces:` to the entry. One run and one
wait cover them all, and results come back as `KEY/OUTPUT`:

```yaml
    pieces:
      - {key: a, args: [--slot, a], inputs: {slot.yaml: slots/a.yaml}}
      - {key: b, args: [--slot, b], inputs: {slot.yaml: slots/b.yaml}}
```

Each piece is its own job, placed, retried and recovered separately, up to 256
per entry ([pieces](https://github.com/1kuna/mycelium/blob/main/docs/jobs.md#plain-command-pieces)).

## 8. Checkpoints and resumable work

To keep NAS writes off a resumable payload, opt in with
`advanced: {resumable: true, checkpoint: local_first}` and use the updated Python
`Checkpoint.save(value)` default name. This versioned policy uses managed local
`MYCE_CHECKPOINT_DIR`, captures only an author-published immutable manifest, and
waits for verified queue acceptance before `save()` returns. Another compatible
host resumes the newest accepted generation. Existing NAS semantics stay the
default; cross-target `resume_compatibility` still applies.

V1 supports one self-contained JSON save up to 1 MiB. Binary/multi-file/named
saves receive `checkpoint_protocol_unsupported`; unsupported job combinations
receive `checkpoint_policy_unsupported`. Local headroom refusal is
`CheckpointError.code == "checkpoint_local_capacity"`. Queue congestion applies
cooperative save backpressure; preserve published bytes and never overwrite the
marker before its acknowledgement. Arbitrary payload writes have no disk bound.

With a configured NAS checkpoint root, the queue mirrors accepted generations
using its existing asynchronous publisher. `file_checkpoints`/`job.progress`
mean queue-portable; `checkpoint_mirrors` separately shows NAS state. A stalled
NAS does not block payload saves while queue capacity remains, but lag can be
unbounded and pending mirrors retain their bytes. Consumers needing NAS
durability wait for verified mirror state. Settled checkpoints retain the
72-hour grace. Size the recovery budget from save interval, work-unit time and
measured capture/upload tail; unpublished local work cannot survive disk loss.
See the [full protocol](https://github.com/1kuna/mycelium/blob/main/docs/files.md#local-first-checkpoints-1600).


A stopped job loses everything since its last checkpoint. For anything long,
save state atomically into `$MYCE_CHECKPOINT_DIR`, restore from it at start, and
declare `advanced: {resumable: true}`; Python's `mycelium.Checkpoint` and `StopSignal` do
the saving and the signal. A stop sends SIGTERM, then SIGKILL to the whole
process group 10 seconds (production preemption) or two minutes (deadline,
planned leave) later, so also save periodically. A GPU or device job's whole
group gets the SIGTERM; a CPU-only job's SIGTERM goes only to the launched
process, so a wrapper script must forward it to the process that saves. Checkpoints are kept 72 hours after the job
ends; declare outputs for anything you need
([checkpoints](references/submit.md#inputs-checkpoints-and-pieces)).

Normal fleet upgrades may checkpoint-stop resumable non-production jobs after a
bounded natural-completion wait. The wait uses the job's last measured planned
stop-to-release cost, capped at the two-minute checkpoint grace (one grace when
there is no measurement). SIGTERM then gives two minutes to save, including GPU
work. The same job requeues with its completed attempt identity and checkpoint
progress preserved, no loss/retry charge, and `maintenance_resume`; the resumed
execution takes the next attempt number. Non-resumable and production work
finish naturally in normal upgrades. Fleet status lists planned jobs, reasons
and deadlines in `planned_stops`, and requested signals in `stop_requests`.

If Mycelium refuses launch before the command starts, the job requeues without
spending its infrastructure-loss budget, even if the host has recovered before
the refusal is reported. Three such refusals do not exhaust that budget. Real
lost executions still count; keep the same job and submission key while it retries.

The same rule covers input staging before command start. A snapshot or NAS read
I/O error or no-progress timeout requeues as Mycelium infrastructure without
spending losses or infrastructure retries, preferring another eligible host.
Status names the staging cause. A completed read with a wrong snapshot hash
still fails as `model_snapshot_mismatch`; repair the source or declaration.

A resume stays on the target (OS/arch/platform) that wrote the newest
checkpoint, waiting for a host of that target even while it is busy or in
maintenance; status shows the wait as `checkpoint_target`. It moves to another
target only when no host of that target remains in the fleet, or when the job
declares `advanced.resume_compatibility` listing both targets because its
checkpoints are tested to resume there. `advanced.portable: true` covers the
program, not its checkpoints.

## Essentials first, the rest under `advanced`

A job or entry states what you care about up front: the command, inputs and
outputs, `needs`, environment and class. Everything else goes under
`advanced:` and has a good default: resume and checkpoint settings, isolation,
wall time and start windows, disk and memory hints, placement exceptions, log
and failure-evidence settings. Set one only when the work needs it
([advanced settings](https://github.com/1kuna/mycelium/blob/main/docs/jobs.md#advanced-settings)).
Status shows an `advanced` object only for the settings you changed.

GPU work never runs on CPU by default: with no GPU free it waits. If your
command really works on CPU and a slow run beats waiting, set
`advanced: {allow_cpu_fallback: true}`; it still takes a GPU when one fits.
Older top-level placement of these settings works for one release and prints a
deprecation note; move them under `advanced:` when you touch the file.

## 9. Classes and tiers

The class decides who waits and who gets stopped. Pick it before you submit;
don't find out by losing work.

| Class | Use for | What happens |
| --- | --- | --- |
| `production` | Work a person or customer is waiting on, as your project defines it (user-facing renders, deliveries) | Goes first and is never preempted by a lower tier; takes capacity from deadline and experiment work; must start within its start window (default 150 s) or fails visibly; needs explicit `targets` (MY-19) |
| `deadline` | Work with a real owner-set date (`--class deadline --deadline RFC3339`) | Ahead of experiments; stops only resumable experiment work |
| `experiment` (default) | Everything else: CI, research, sweeps, previews | Production can stop it at any time and requeue it; non-resumable work restarts from scratch |

Production approval names a kind of machine, never a host (MY-51). To let production run on another kind, your project approves it itself after a passing output-comparison check or experiment on a machine of that kind (declare its needs; don't pin a host): `myce parity add --project P --work WORK --evidence JOB_ID` approves the kind it ran as; add `--kind cuda` (or `mac`, `linux,x86`, `linux/amd64/cuda`) for a broader kind it ran as. Every host of an approved kind may then run that production. `myce parity list --project P` shows your kinds; `myce parity remove ... --kind KIND` withdraws one ([details](https://github.com/1kuna/mycelium/blob/main/docs/scheduling.md)).

Equal tiers never preempt each other. Within a tier the queue goes by
readiness, deadline and age, with no ordering hint, so order your own work with
the right class, `after:` stages and checkpoints rather than pauses or lock
files. Lost machines and critical Mycelium updates can interrupt any tier; the
job requeues on the same key and resumes from its checkpoint. Work that
Mycelium stopped for its own maintenance or updates resumes ahead of the
tier's other queued work, and its host drains toward it
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
([stages](https://github.com/1kuna/mycelium/blob/main/docs/jobs.md#start-a-consumer-after-every-producer-piece-succeeds)).
A submitted production stage counts as open production work while it waits on
`after:`, so drains and fleet upgrades keep a host able to start it; open a
[production lease](references/waiting.md#production-leases) only for stages you
can't submit ahead.

## When a job fails or waits

For a preservation snapshot, `myce status --history --json` streams one complete
document from a consistent committed publication, with bounded job projection
on the queue. Save it to a file and check exit status before using it. Python
`client.status(full=True)` returns the same document and allocates it in client
memory. Ordinary status stays compact; use one job's status for diagnosis.

- **Failed:** read its `finding` (printed by `myce wait`, shown by status and
  `myce result`): `owner` is `job`, `mycelium` or `unknown`, and `action` is the
  one thing to do. A `dead_end_wait` names what waiting can't fix; do it, then
  submit again. A failed job stays bound to its submission key: submitting the
  same key returns that failed job (`submission_reused`), and `myce resume` only
  resumes paused jobs, so submit the repeat with a new `--key`.
- **Waiting:** read `myce status JOB_ID` once. Its line gives one reason and one
  `next:` action (`--history` adds the evidence). On releases with staging phase
  telemetry, `myce status JOB_ID --history --json` retains `staging_timings` per
  attempt: cache hits/misses and phase calls/seconds/failures. Phases nest and
  can overlap; do not sum them or label assignment-to-staged as transfer time. Fix what you authored, or record the owner and
  action on your work record and move on. Keep the same job and key; resubmitting,
  pinning or running locally only hides a hold ([waiting](references/waiting.md)).
  A hold with `runs_now_on` names platforms that would run the job now: add
  them to its targets and environment rather than naming a host.
- **Environment builds (r6.16+):** one worker's package failure excludes only
  that worker/target/hash; keep the same queued job for another compatible worker.
  `no_buildable_worker` means every freshly observed compatible worker failed.
  Read `myce status --sections machines` for its hash, target, failure and log
  reference. Repair the worker, then `myce env retry WORKER HASH OS ARCH PLATFORM`,
  or submit changed recipe inputs. Process restarts and idle polling do not
  clear failures; transient network/disk failures and planned stops remain holds.
- **Drained hosts (r6.16+):** production waiting only on drained hosts keeps
  the same job and key, with its relative start window paused for capacity.
  Status says `waiting for drained host ... to return`; the operator resumes
  the host after maintenance. Explicit `start_by` and `give_up_at` still bind;
  deliberately disabled hosts still fail new production work promptly.
- **Long waits:** submit without `--wait` and you are woken once when the job
  finishes. From a shell whose harness exports a wake channel (T3 today), that
  needs no setup ([T3 binding](t3-binding.md)). Otherwise `myce run` prints a
  `wake:` line: run that one `myce wait JOB_ID` as a background task, and its
  exit wakes you. Never loop short `--timeout` waits. One `myce wait JOB_A JOB_B`
  covers up to 256 jobs, and a timeout leaves them running. `queue_stalled`
  means the queue stopped making progress: read status once, then repeat the
  same command ([wait and status](references/wait-and-status.md)).
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

For memory qualification, retain the exact attempt's `memory_peak`, including
`gpu_host` pin/physical evidence and `device_sampling` coverage/variation when
present. A CUDA proc physical upper bound can include unrelated occupancy and
device overlap; it does not justify an overlap deduction or a smaller device
growth allowance. Incomplete readings keep conservative sizing. See the
[memory status fields](https://github.com/1kuna/mycelium/blob/main/docs/status.md).

## Python

```python
from pathlib import Path
from mycelium import Client

client = Client.from_installed()
outcome = client.run_entry("hello", out="accepted")
assert Path("accepted/result.txt").read_bytes() == b"mycelium-ease-audit-ok\n"
```

`run_entry` is `myce run --entry ... --wait --out` (same key, same job) and
raises `EntryFailedError` on failure. Ctrl-C or SIGTERM raises `EntryInterrupted`
with `exit_code=126`, the job ID and key; repeat `run_entry` with that same key
to re-attach. Only `client.cancel` or `client.cancel_submission` cancels. For specs built in code, use
`client.submit`, `client.wait_many` and `client.download_results`
([Python client](https://github.com/1kuna/mycelium/blob/main/clients/python/README.md)).

## What you can now delete

In an existing project, Mycelium now does these; delete your own:

- submit, wait, fetch and verify scripts: `myce run --entry NAME --wait --out DIR`;
- marker files proving a job finished: the exit code and verified files;
- tar steps: `inputs:`, `pieces:` and folder or glob outputs;
- exit-0 wrappers that return logs from a failure: `keep_on_failure`;
- thread-count variables and temp-dir setup: `MYCE_CPUS` and `TMPDIR`;
- host pins and per-host entries: needs, with no target list off production;
- shell NAS copies: `shared_inputs`;
- model download jobs pinned to a host: declare `model_snapshots`;
- one submission per input in a loop: `pieces:`.

`myce adapter convert` turns old profiles into minimal entries
([setup](setup-project.md)).

## Check your project's efficiency

After a batch, run `myce efficiency --project NAME` once. It shows what your
own steering cost you: the share of jobs that name hosts or targets, the queue
hours they waited while an excluded host had room, and how far typed
`host_memory_gb` and `cpu_cores` sit from what Mycelium measured. Act on each
`advice:` line in your entries, then drop the steering it names.

## References

- [Submit](references/submit.md): needs, source and keys, environments,
  isolation, models, inputs, checkpoints, placement restrictions.
- [Waiting](references/waiting.md): holds, classes and preemption, device and
  storage holds, pausing, production leases.
- [Wait and status](references/wait-and-status.md): batch waits, status
  (host RAM is `host_memory`; older RAM fields are deprecated), stalled queues,
  observers, progress.
- [Results](references/results.md): output failures, diagnostics, large outputs,
  and startup publication progress. Released results replay without withholding
  the host's free compute; retain their original job and attempt identity.
- This guide ships with the `myce` binary: `myce skill install` installs it and
  upgrades refresh it ([setup](setup-project.md#install-the-agent-skill)).
- [Setup](setup-project.md), [harness](harness-binding.md) and
  [T3 binding](t3-binding.md): adoption, skill install, notifiers.
- [Owner operations](references/owner-operations.md): queue-owner writes. Transient
  NAS failures warn without blocking install or fleet upgrade. Receipts retain
  `storage_warning` (root/operation/errno, duration and timeout); capable queues
  also show it in fleet status. Runtime root health still gates dependent work
  until storage recovers. Configuration/state errors remain fatal and include
  `storage_failure` in supported fleet status; retain that evidence for the
  storage owner and leave an operation-owned drain in place.
- No fleet: `myce dev -- sh script.sh` runs your script against a temporary
  queue with fake GPUs ([quick start](https://github.com/1kuna/mycelium/blob/main/docs/START-HERE.md)). Inside it,
  every `myce` command and `Client.from_installed()` use the dev queue, even on
  a machine that has joined a fleet.
- Problems: search the public issues, then file one with expected and observed
  behavior, version, job and attempt, and no credentials or private paths.
