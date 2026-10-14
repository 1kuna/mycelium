# Write an extension (v3 slice 1)

An extension is one small folder: `extension.yaml`, plain notes and the few
scripts it needs. Use it to teach Mycelium a **kind of work** (preferred) or a
**program type**. A **hypha** must be **qualified** for that exact version and
kind/program type before it gets a job. Decisions: issues #18, #20, #35 and #36.
This is an opt-in v3 path; existing v2 commands and environments still work.

## Author the folder

```yaml
name: example
version: '1'
purpose: Describe the useful GPU work in one sentence.
platforms: [metal, cuda, xpu]
kinds: [example-work]
program_types: [example-script]
memory_gb: 2
host_memory_gb: 1
resumable: true
setup: [python3, setup.py]
run: [python3, '{extension}/run.py']
check: [python3, check.py]
notes: |
  State prerequisites, model identity and limitations here.
```

Names, versions and work types use lowercase letters, digits, `.`, `_` and `-`.
There must be a purpose, supported platforms, at least one work type, positive
GPU memory reservation and a run argv. Setup and check are optional; without a
check and matching native baseline, the version stays not qualified. No implicit
shell, background service or dependencies in the description format. Explicit
`[sh, script.sh]` is fine. Include only regular files, no symlinks. Total folder
contents are limited to 1 MiB. Execute scripts through their interpreter; file
execute permissions are not part of the frozen content identity.

Setup downloads models, packages, source or installers into
`MYCE_EXTENSION_CACHE`, once per hypha/version/platform. They stay outside the
small folder. Use pinned versions and verify model/download hashes. Setup is
explicit, with a bounded timeout; it is never silently started by a queued job.
The cache is trusted local code, not a sandbox. Setup/check obtain an exclusive
cache lock; running jobs hold a shared lock. A setup failure clears qualification.
Cache cleanup is explicit, not automatic in this slice.

Scripts receive `MYCE_EXTENSION_DIR`, `MYCE_EXTENSION_CACHE`, `MYCE_PLATFORM`,
`MYCE_DEVICES` and ready `MYCE_DEVICE` (`cuda:0`, `xpu:0`, `mps`; `cpu` only for
an explicitly fake test extension). Setup/check and jobs never inherit
`MYCE_TOKEN`. Cached `venv/bin` and `bin` precede the ordinary PATH. Run argv
expands `{extension}` and `{cache}` without shell expansion. The run command
receives kind arguments, or the program's local input filename and arguments.
It also receives `MYCE_WORK`, `MYCE_EXTENSION_NAME` and `MYCE_EXTENSION_VERSION`.

## Bundle a native baseline, then replay it

Record inputs **and actual outputs** on hardware that runs this work natively.
An author's matching native baseline is usable. Never fabricate one from a
known-answer score or from a failing hypha. Add `baselines` entries keyed by
`kind:NAME` or `program:TYPE`:

```yaml
baselines:
  kind:example-work:
    input: baseline/input.json
    output: baseline/native-output.json
    measure: numeric
    absolute: 0
    relative: 0
    native: Describe the native backend, software, model and precision.
    variation: Describe the repeated measurements supporting these allowances.
```

Zero here is a **template**, not a recommended universal tolerance. Declare
allowances from measured variation and bundle the supporting evidence. Widening
an allowance needs an explicit reviewed change and a new frozen version. There
is no automatic learning from failures. The first extension for a kind supplies
its native reference; later extensions should reuse that matching reference.

For each handled work type, Mycelium runs check with `MYCE_CHECK_WORK`,
`MYCE_CHECK_INPUT` (the bundled input) and `MYCE_CHECK_OUTPUT` (a fresh output
path). Check must replay that input on the assigned resource and write **actual
output**, never a similarity score. It should use the same software/model/run
path as jobs and fail if the GPU is unavailable. No CPU fallback.
Mycelium compares this output with the bundled reference using the work type's
chosen measure. The core measures are `exact` (byte equality) and `numeric`
(matching nonempty JSON arrays, each value within
`absolute + relative * abs(reference)`). This narrow measure set can grow when
a new kind needs it. Nonfinite values, shape mismatch and missing output fail.
A missing baseline remains not qualified, with a reason.

Qualification is persistent local state per version, content digest, work type
and hypha hardware/driver fingerprint. Join computes the fingerprint; rejoin
when hardware, drivers or OS change. A changed/unavailable identity invalidates
qualification. Edited descriptions/scripts/references require a new version.
Checks sample the bundled work: they do **not** establish similarity for unseen
jobs, models or precisions (#18/#34). Keep that limitation in the description.

## Install and qualify

Use the same explicit local root for CLI and join. Configure caller credentials
privately as usual; keep tokens out of argv and job environments.

```sh
myce extension add --root ./extension-state ./extensions/example
myce extension setup --root ./extension-state --gpu metal example@1
myce extension check --root ./extension-state --gpu metal example@1
myce extension list --root ./extension-state --gpu metal
myce join --extension-root ./extension-state --job-file-root ./shared
```

Flags precede the folder or `name@version`. `add` registers the frozen small
folder with the queue and installs it locally. On another hypha use `add` again
with the identical folder; registration is idempotent. `add --local` skips queue
registration for preparation without a queue. `list` without `--gpu` prints
local descriptions; with it, it prints qualified/not-qualified and reasons.
For selected CUDA/XPU resources pass the same `--device` to setup/check and join.
Local caches are independent of queue restart. The queue persists descriptions
and each job's candidate snapshots in its existing SQLite database. API:
`POST /api/extensions` registers folder transport; `GET /api/extensions` lists
queue descriptions, both authenticated. Registration never executes code.

## Describe jobs without hardware

```yaml
name: useful-work
project: demo
priority: experiment
kind: example-work
args: [--input, input.json, --output, output.json]
inputs: {input.json: inputs/input.json}
outputs: {output.json: results}
size_hint: {memory_gb: 4, host_memory_gb: 2} # optional
resumable: true
```

Or replace `kind`/`args` with:

```yaml
program:
  type: pytorch-script
  file: train.py
  args: [--input, input.json, --output, output.json]
inputs: {train.py: programs/train.py, input.json: inputs/input.json}
```

Exactly one of kind, program or the existing v2 `run.command` is accepted.
The program file names an input, not a machine path. CLI YAML, API JSON and
Python `Client.submit(dict)` use these identical fields. Kind/program jobs do
not declare `needs`, an environment, command or workdir. Ordinary `run.env` can
supply program configuration, but cannot override worker/extension identity,
resource selectors or the ready resource string. Mycelium derives conservative
memory reservations from the extensions and optional size hint. Capability profiles and measured program needs are used by the v3 queue;
see [placement and measurement limits](guide-placement-v3.md). Multiple matching extensions
are tried in deterministic name/version order, only when qualified. A job
records the exact chosen name/version/content digest on every attempt.

The shared-storage transport is deliberately the existing NAS model in this
slice. `--job-file-root` is the hypha's local mount; every input/shared prefix is
relative to it. Mycelium copies inputs into a private per-attempt working folder,
then runs the extension there. Successful outputs go to
`PREFIX/JOB_ID/ATTEMPT/LOCAL_NAME`; `job.output_files` gives these relative names.
Consume outputs only from the successful current attempt. Publication never
overwrites another attempt, including a late stale execution. Shared-storage
transport without a NAS remains #24, and is not implemented here. Missing files,
unsafe paths, unavailable storage and copy errors fail visibly. No personal or
private machine paths need appear in a job.

The existing scheduler keeps placement, priorities, project allow-lists and
memory accounting. The existing command runner keeps start authorization,
process-group stopping, memory ceilings where configured and checkpointing via
`MYCE_CHECKPOINT_DIR`. Set `resumable: true` only when the program/extension
implements the existing checkpoint contract; extensions declaring no resume
support are ineligible. Non-resumable remains the default. Waiting jobs explain
missing extensions/qualification. Setup/check failures stay not qualified;
other qualified hyphae/extensions may run the job. GPU starters never accept
fake/CPU placement. `myce dev --extension-root ... --job-file-root ...` exercises
the actual API and runner using deliberately fake extension fixtures.

## Starter extensions

- `extensions/pytorch-script`: PyTorch 2.8.0, extension v0.2.1, CUDA/XPU/MPS, unmodified programs
  choosing `torch.device(os.environ['MYCE_DEVICE'])`. Includes a small native
  MPS pointwise float32 reference and repeated measurements. This qualifies
  that sample only, not every PyTorch model or script.
- `extensions/llama-cpp`: pinned llama.cpp build with Metal/CUDA/SYCL, or an
  explicit Vulkan build. Build tools and accelerator toolchains must be available.
  Offload/resource flags belong to the extension, never the caller. Models are
  job inputs. Includes a short native Metal prompt/output reference with pinned
  tiny-model identity and two identical native repetitions; other backends must
  pass it before they qualify. This reference does not cover arbitrary models.

Before publishing, test on a laptop with a fake extension: folder validation,
frozen versions, absent baseline, setup/check failures, identity changes,
routing both forms and waiting explanations. Then run the ordinary build, vet,
race suite and dev end-to-end. Use isolated native hardware only when authorized.
