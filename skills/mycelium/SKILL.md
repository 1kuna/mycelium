---
name: mycelium
description: "Use when work needs fleet compute (heavy tests, builds, sims, training, transcodes, model runs), when you submit, check, wait on or fetch a Mycelium job, when a job is waiting, when your work must go first, or when you need to pause your own queued job."
---

<!-- Public consumer guide. Edit this contract in place; detail lives in references/. -->

# Mycelium

Mycelium runs your commands on a fleet of CPU and GPU machines through one
queue. You say what the work needs; Mycelium picks the machine, runs it and
hands back verified files. Climb the ladder only as far as your work needs.

Default `myce status` puts unhealthy hosts, holds and waiting work first; alerts
are one trailing count. Use `myce status --sections alerts` for records or
`myce efficiency` for sizing and placement findings. Host health openings and
closings go to the installation owner's Mycelium inbox when its hook is configured.

## Ground rules

- Use the installed `myce` CLI or Python `Client.from_installed()`; both find
  their own credentials. Never open, print, copy, pass or set fleet credentials.
  A missing CLI or refused authority is a hold for the installation owner: keep
  its typed refusal and keep heavy work off your own machine.
- Describe what the work needs, never which machine: `--gpu cuda` finds CUDA
  hardware. Pin hardware only for a [real requirement](references/submit.md#explicit-placement-restrictions).
- Shared interactive hosts refuse `batch`, `test`, `ci`, `rollout` and `eval`
  classes and same-named policy tiers, including when the fleet is full. Submit
  those classes normally; no host exclusion is needed to protect the agents
  workstation. Light work still uses its measured allowances. Queued GPU work
  protects its resolved CPU/RAM needs from CPU backfill, including internal
  extension checks, CPU maintenance resumes, and waits for a finite GPU execution
  to finish. Freed capacity accumulates toward that demand; portable queued CPU
  work can use another eligible host. No fixed core reserve or GPU-host exclusion is needed.
  Existing CPU work keeps its stop/checkpoint contract and may need to finish.
- `myce COMMAND --help` is the flag reference. A field your installed queue or
  worker doesn't return is unknown, not false.

For a host-memory wait, per-job JSON's `admission_wait.first.targets[]` retains
`host_memory_ledger` on supporting queues. Compare physical room
(`capacity_gb - growth_gb`) with allowance room (`allowance_gb - reserved_gb`)
at that snapshot's timestamp. Charges alone do not explain live occupancy;
do not combine an earlier wait with a later machine reading. Missing ledgers
on older records are unknown, not zero usage. On supporting unified CUDA
workers, fresh exact-attempt single-process device evidence also earns resident
credit through max(all credited host pages, one device process counter). Device
counters are never summed for this credit. Missing/stale/ambiguous evidence and
older workers retain conservative pending growth; this does not reduce charges
or qualify native coexistence.

When status says `waiting for fresh accelerator inventory`, leave the existing
job queued: Mycelium holds GPU placement until a fresh scan arrives, while CPU
work continues. `myce status --sections machines --json` exposes
`inventory_probe.state`, `operation`, `pending_seconds` and `error_class` for the
queue owner. Worker contact alone does not prove usable accelerator inventory;
resubmitting the job does not repair that evidence. Report a persistent wait to the queue owner with those fields.

A `host_backfill` hold (`host_capacity_accumulation`) names queued work whose
resolved host RAM or cores Mycelium is protecting in tier/fair-turn order.
Even a GPU job that fits now can wait when starting it would deepen that demand's
capacity deficit. Existing attempts finish normally; the hold clears when the
named demand starts or no longer withholds capacity. Keep the existing job queued.

A maintenance-interrupted resumable job keeps its job ID. Leave it queued while
Mycelium restores its checkpoint; a slow copy retains that same attempt and
writer. Refused copies with proof no payload ran are reclaimed after release.
Older orphan copies under a cancelled job expire with its whole checkpoint
folder, normally 72 hours after the job ends, once every attempt is released.

Work yielded to production stays off that host while the production attempt
prepares its command, including a launch-capacity retry. Queue start
authorization alone does not end this hold: the worker must report command
start or confirm release. Other compatible hosts and unrelated work using spare
capacity remain eligible; keep the existing job queued.

Job JSON timestamps preserve nanoseconds. `start_latency_seconds` is the wall-time
difference from `queued` to `started_at`, or from `dependency.ready_at` for a
dependent job. It uses the timestamps exposed by the API, not process-local
monotonic clock readings.

## 1. One command

```sh
myce run -- sh -c 'echo "hello from $(uname -sm)"'
```

This submits the command, streams its logs and exits with its exit code. It runs
with the worker's installed tools in an empty folder. `--cpu N`, `--mem 8G` and
`--gpu cuda|xpu|metal` describe needs; leave memory out and Mycelium learns it.
A typed memory figure is a limit, not a reservation: once the work has run,
Mycelium charges what it measured and keeps your figure as the ceiling.
An unknown CPU command starts with a small physical accounting floor. Its
charge follows measured use on supporting workers; protected/shared or older
workers keep strict containment with a measured shell startup minimum. A memory
stop reruns the same job at its measured peak without an infrastructure loss; keep its job and key.

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

- **Native execution only:** managed Docker is retired. A `run.docker*`
  request receives `job_docker_retired`; use a locked environment and native
  sidecars ([migration](references/submit.md#environments)).
- **Installed tools** are the default. List what the command calls in
  `needs: {tools: [git, node]}` so placement skips machines without them.
- **Probed software capabilities** use native `checks.qualification` and a
  bundled `baselines: {extension:ENTRY_NAME: ...}` comparison, with exact
  targets and the same environment as the payload. For software EGL, run the
  application's small render probe with `LIBGL_ALWAYS_SOFTWARE=1` and
  `EGL_PLATFORM=surfaceless`; assert llvmpipe and the rendered pixels before
  writing `$MYCE_CHECK_OUTPUT`. Set those variables on the payload too.
  Mycelium caches pass/fail per frozen entry, environment and host/device
  fingerprint and places useful work only on passing hosts. Drop
  `allowed_machines`; `needs.tools` alone cannot prove rendering works. See the
  [complete declaration](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#probe-a-software-capability-instead-of-pinning-a-host).
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

### Keep a model loaded across inputs

For independent stages using the same model, author that model as an existing
[catalog HTTP server](https://github.com/1kuna/mycelium/blob/main/docs/services.md#resident-engines-for-independent-stage-jobs)
and opt the entry in:

```yaml
    advanced: {resident: {model: speech-engine}}
    run:
      command: [sh, -ec, '"$MYCE_RESIDENT_CLIENT" resident --path /infer --input input.bin > result.json']
    outputs: {result.json: results}
```

Declare the input, supported platform and the client's staging/CPU memory as
usual. `pieces` still gives one independently accepted result per input. The
catalog server's measured budget covers model/runtime plus one request peak;
Mycelium charges it once per host/card and serializes calls to that copy.
The stage command is a client; move model loading into the server, whose exact
runtime/environment/model identity Mycelium freezes. A registered extension can
set `resident: {model: speech-engine}` beside its variants.

The supplied helper uses attempt-owned pipes, grants no fleet credentials or
raw engine URL, and works without `needs.network`. It accepts `--method`,
`--path`, `--content-type` and `--input` (stdin by default); stdout is the successful
response body. A failed request fails the command without transport replay.
Make requests sequentially within an attempt. Pending same-stage jobs retain
the loaded engine; otherwise measured load cost/demand spacing determine idle
release. Memory pressure or production demand can reclaim it earlier through
acknowledged unload. Failed/cancelled inputs keep their normal isolated result
and dependency rules. `worker_data: ephemeral` is refused with this opt-in;
shared server memory and request spools are outside attempt-only cleanup.
Qualify exact native outputs and load/steady/request memory peaks before production.

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
`advanced: {worker_data: ephemeral}` on an offline, non-resumable command or
extension with managed inputs and declared outputs. Environments, sidecars,
`after` inputs and fan-out pieces are supported on v2-capable workers. It stages privately
without persistent payload caches, retains successful output bytes until durable
queue acceptance, then verifies removal of local inputs, extracted/shared data,
scratch/temp/private home (including sidecars), checkpoints, outputs and delivered
logs before release. Environment caches, extension source and model caches stay
read-only. Ephemeral attempts never read, pin or populate persistent payload
caches, even for a warm hash pinned by another job; that other job's independent
copy stays under its own retention. This does not purge earlier ordinary jobs.
Check job `worker_data_cleanup` and the attempt-history receipt: `failed` or
pending cleanup is not a satisfied policy, even if the result succeeded. Retries
restage inputs; startup recovers process custody and cleanup. Supporting Linux
namespace workers with file core routing or the qualified Mycelium core router
advertising `worker_data_ephemeral_v2` are required; v1 workers cannot run the
expanded scope. Resumability, custom workdirs (including extension workdirs),
program jobs, extension checks, model servers/snapshots, resident model runtimes, diagnostics,
`keep_on_failure` and alternate output delivery remain refused with
`job_worker_data_unsupported`. Environment recipes, extension source/assets and
authored settings are shared software/metadata: put customer data only in
declared managed inputs, never in software recipes, arguments or env values.
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

Declare `advanced: {resumable: true}` when a command depends on saved progress.
NAS-backed resumable jobs wait for saved progress storage while the queue or
worker checkpoint root is unavailable. Positive `advanced.disk.checkpoint_bytes` also
declares checkpoint use and retains that storage hold, even without resumability;
it does not enable portable file-store saves or resumable preemption.
Non-resumable jobs without declared checkpoint bytes at every priority
can run through that outage; their `MYCE_CHECKPOINT_DIR` is writable attempt-local
storage, even if the command writes saves there. Those writes are not restored
or published as portable progress. Declare outputs for files you need back.
Extension qualification checks are non-resumable and use this local storage too.
Ephemeral worker data always keeps checkpoints local, even with declared bytes.
Declared NAS inputs and outputs still require their own storage to be available.

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


Job status/progress comes from the executing worker, never from reading the
NAS. Use the updated Python `Checkpoint` helper for live checkpoint progress:
after a durable NAS save it publishes a tiny local notification through the
worker-provided `MYCE_CHECKPOINT_NOTIFY`. Custom JSON-manifest writers can
[implement the same notification](https://github.com/1kuna/mycelium/blob/main/docs/files.md#resumable-jobs).
Arbitrary writers without it report unknown progress between resume/stop/release/
finalization events; portable scarce-host yield waits for positive publication
evidence. There are zero periodic NAS checkpoint walks or root stats per attempt.
Keep checkpoint saves and manifests outside the top-level output directory;
checkpoint progress discovery never enters that output tree.
`local_first` retains its existing local marker and acknowledgement contract.

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

Normal fleet upgrades preserve running batch attempts through live takeover:
processes, logs, memory guards and results keep the same attempt identity.
Where takeover cannot be proved (including older workers and resident bridges
not yet qualified for detached custody), the update waits for natural completion
and reports why. Normal updates never checkpoint-stop or kill work. Only an
explicit critical update retains the existing interruption fallback when takeover
is unavailable; `stop_requests` records it. Keep the same job and submission key
through a worker update; resubmitting can duplicate work.

If Mycelium refuses launch before the command starts, the job requeues without
spending its infrastructure-loss budget, even if the host has recovered before
the refusal is reported. Three such refusals do not exhaust that budget. Real
lost executions still count; keep the same job and submission key while it retries.

A `shared_gpu_pressure` stop also requeues the same logical job without spending
failure budgets. Keep its job and key while the queue retries. A stopped copy's
`recoverable: false` prevents re-adopting that execution; it does not make the
logical job terminal. CUDA startup before the attempt creates a context reports
zero use only after a successful process enumeration; its declared or learned
device vector remains charged. Failed or N/A probes remain unknown, and uncertain
unified-memory sharing still sheds to exclusive execution.

The same rule covers input staging before command start. A snapshot or NAS read
I/O error or no-progress timeout requeues as Mycelium infrastructure without
spending losses or infrastructure retries, preferring another eligible host.
Status names the staging cause. A completed read with a wrong snapshot hash
still fails as `model_snapshot_mismatch`; repair the source or declaration.

A command failure concurrent with a recorded checkpoint-root stall or failure
also requeues without spending losses or infrastructure retries, preferring
another eligible host. Its `checkpoint_storage_interrupted` finding names the
storage evidence (timestamp, no-progress age and available operation/errno).
Keep the job and key while it retries. Healthy storage or merely slow probes
leave command failures owned by the job; exit codes alone never establish a
storage fault.

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
| `ci`, `test`, `batch`, `rollout`, `eval` | Bulk work of those kinds; set the class so placement knows (`--class ci`) | Same as `experiment`, and never placed on a shared interactive host such as the agents' machine, even when the fleet is full; no host exclusion needed |

Production approval names a kind of machine, never a host (MY-51). To let production run on another kind, your project approves it itself after a passing output-comparison check or experiment on a machine of that kind (declare its needs; don't pin a host): `myce parity add --project P --work WORK --evidence JOB_ID` approves the kind it ran as; add `--kind cuda` (or `mac`, `linux,x86`, `linux/amd64/cuda`) for a broader kind it ran as. Every host of an approved kind may then run that production. `myce parity list --project P` shows your kinds; `myce parity remove ... --kind KIND` withdraws one ([details](https://github.com/1kuna/mycelium/blob/main/docs/scheduling.md)).

When ready production needs a host's RAM, CPU or device, the existing planner
reclaims useful lower-tier holders, including when their charges exceed a
reduced host allowance (MY-56). Resumable work keeps its checkpoints and
requeues without charging losses or infrastructure retries. Admission waits
for exact cleanup acknowledgement. Backfill cannot deepen ready production's
host deficit; compatible other hosts remain eligible. No host is reserved
while production is absent, and physical headroom still bounds concurrency
across independent cards.

Equal tiers never preempt each other. Within a tier the queue goes by
readiness, deadline and age, with no ordering hint, so order your own work with
the right class, `after:` stages and checkpoints rather than pauses or lock
files. Memory safety stops have distinct causes: `shared_gpu_pressure` for
shared-card growth or uncertain attribution, and `memory_overcommit_yield`
when running work grows into lent host RAM. These can interrupt equal tiers;
they are not a waiting job's priority preemption. A lending yield waits on
measured headroom before readmission; keep the same job and key. A wait saying
`waiting for lent host memory` means the same host cannot yet fit the running
attempts' remaining ceilings and this job's limit without lending again. A flat
growth sample does not clear it; measured capacity or exact release does.
Lost machines and critical Mycelium updates can interrupt any tier; the
job requeues on the same key and resumes from its checkpoint. Work that
Mycelium stopped for its own maintenance or updates resumes ahead of the
tier's other queued work, and its host drains toward it
([classes](references/waiting.md#classes-and-preemption)).

For NAS checkpoints, write files in the final directory (file renames are fine)
and publish a small commit manifest last. Avoid renaming freshly written
directories: SMB deferred close can return EACCES/EBUSY; if unavoidable, retry
with a short backoff through the mount's `closetimeo` window.

## Pipelines

For an ordinary producer, omit `after.piece` and map its declared output names
directly: `after: {job: PRODUCER_JOB_ID, outputs: {input.json: result.json}}`.
The producer must already be submitted in the same project. This works in an
adapter entry with `myce run --entry consumer`; use a new key when changing the
dependency. The queue stages verified accepted bytes, so keep producer results
out of the source payload. For a fan-out producer, whole-parent mappings use
`KEY/OUTPUT_NAME`, as below; `piece: KEY` allows an ordinary consumer to start
when that piece is accepted. Dependent entries currently omit `checks.result`.
A rejected run prints and saves `refusal_code` and `refusal_reason` in its
receipt; `spec check --resolve` checks the same source/output dependency rules.

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
  `execution_evidence` applies only to its named attempt: an earlier
  `command_started: true` does not mean the final attempt ran. Supporting
  workers preserve startup error classes without optional diagnostics and
  requeue cancelled preparation with positive no-main-execution evidence
  without spending retry budget. `prelaunch_cause_missing` means Mycelium
  retained proof main did not start but lost the cause; give the queue owner
  the job ID and attempt for lifecycle/result inspection. It is not proof of
  a payload failure or of a particular maintenance/shutdown cause.
- **Waiting:** read `myce status JOB_ID` once. Its line gives one reason and one
  `next:` action (`--history` adds the evidence). On releases with staging phase
  telemetry, `myce status JOB_ID --history --json` retains `staging_timings` per
  attempt: cache hits/misses and phase calls/seconds/failures. Phases nest and
  can overlap; do not sum them or label assignment-to-staged as transfer time. Fix what you authored, or record the owner and
  action on your work record and move on. Keep the same job and key; resubmitting,
  pinning or running locally only hides a hold ([waiting](references/waiting.md)).
  A native-qualification wait names the automatic check and its current hold.
  When an unrun check's transferred estimate exceeds physical RAM, it uses the
  ordinary guarded first-run envelope to obtain a native measurement; status
  says `transferred estimate exceeds physical capacity`. Local measurements,
  declared member needs and OOM floors remain binding. Only those requirements
  can fail a check with `dead_end_wait` and `cannot fit on any eligible host`;
  a transferred extrapolation alone cannot. Read the check's own status for
  the measured demand/capacity comparison. Reduce genuinely oversized check
  requirements or provide capable hardware before retrying qualification.
  Qualified GPU work also probes one card when only a transferred qualification
  estimate exceeds native capacity. Cold probes fit observed idle headroom and
  retain exact-session prelaunch deficits; keep the same queued job and key.
  A hold with `runs_now_on` names platforms that would run the job now: add
  them to its targets and environment rather than naming a host.
- **Environment builds (r6.16+):** one worker's package failure excludes only
  that worker/target/hash; keep the same queued job for another compatible worker.
  If no known host can build it, or every compatible builder has deterministically
  failed, the job ends with `dead_end_wait` and one finding/action. Repair and
  resubmit with a new key; terminal jobs do not reopen. Unknown/offline inventory
  remains a wait. Read `myce status --sections machines` for its hash, target, failure and log
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
- Foreground wait/result collection and owner cancellation suppress later wakes; `myce run --supersedes JOB` explicitly replaces a same-project job’s wake without cancelling its execution.
- **Stopping:** `myce cancel --wait JOB_ID` exits 0 once the job won't run and
  its resources are released (75 while release is pending); `command_started:
  false` means no command began. `myce pause JOB_ID --reason TEXT` holds your
  own queued job, and `myce resume JOB_ID` keeps its place.
  A `custodian_startup_failed` finding means the detached command owner exited
  before staging or payload launch; supporting workers release that exact
  assignment through normal completion. Missing custodian observations alone
  retain custody and need the queue owner's diagnosis; keep the existing job ID.
  Supporting workers also reconcile an initialized, never-started helper after
  restart when exact helper death, manager absence and no launch custody are proven.
  The installation owner can recover the exact legacy Linux pre-entry refusal
  offline with `resolve-custodian-startup` under the [operations runbook](https://github.com/1kuna/mycelium/blob/main/docs/operations.md#recover-the-legacy-pre-entry-refusal).
  Consumers keep their existing jobs and checkpoints; do not resubmit or edit custody.
  An inspection refusal is not a detected holder: the installation owner diagnoses
  it and may use the runbook's explicit privileged read-only Linux inspector.

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
present. A CUDA proc physical upper bound includes device and possibly unrelated
occupancy. With repeated complete accepted local evidence, Mycelium keeps that
combined bound independently of non-reclaimable host plus device, and uses
between-attempt device peak variation rather than adding startup allocation twice.
Retain `host_sampling` too: its predicate and read/gap/final-age timings explain
incomplete host evidence; device completeness alone does not establish host
coverage. A rejected CUDA proc pin read additionally records `failed_pid`,
`failed_state`, `failed_counter` and `failed_counter_reason` (`missing` or
`malformed`). Keep those failure-only fields with the exact attempt; missing
fields on an older worker leave the cause unknown. An exiting PID is skipped
only after a fresh subtree inventory confirms it left; a listed owner with
unreadable counters still makes sampling incomplete.
Repeated complete physical forecasts supersede legacy additive sizing
only when they cover every retained total-cgroup floor and resident-plus-device
forecast. Incomplete-only history and uncovered larger peaks keep conservative
sizing. A later coverage gap retains an earlier accepted combined CUDA physical
bound plus measured growth independently of the host/cgroup-plus-device fallback;
device demand is not added again to that already combined bound. The bound can
include unrelated whole-host occupancy; a statistical outlier alone does not
justify lowering it. Unknown
or stale live device attribution still requires exclusive execution. For local
benchmark telemetry on a worker-only host, omit `--queue-pid`; its RSS is marked
`not_configured`. See the
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

After a batch, run `myce efficiency --project NAME` once. It shows named-host
list usage (real pins), those jobs' plain queue wait and current stranded jobs
that would fit with host lists removed while platform targets stay intact.
Platform targets describe software compatibility and are counted separately;
they are not named-host pins. Plain wait does not establish pin cost or an exact
counterfactual saving. Review host lists for a general platform, memory or tool
need; keep intentional situational pins. It also compares typed
`host_memory_gb` and `cpu_cores` with measured use.

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
  Upload retries use the accepted offset, including a fully received upload
  whose publication must finish. A 500 needs the queue's `file_upload` event
  (operation/error class and offsets); retain the same receipt and bytes.
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
  queue with fake GPUs and an owned-process RSS guard for CPU work
  ([quick start](https://github.com/1kuna/mycelium/blob/main/docs/START-HERE.md)). Inside it,
  every `myce` command and `Client.from_installed()` use the dev queue, even on
  a machine that has joined a fleet.
- Problems: search the public issues, then file one with expected and observed
  behavior, version, job and attempt, and no credentials or private paths.
