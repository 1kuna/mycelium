# Write an extension

For new composed extensions, hand your agent [AGENT-GUIDE.md](extensions/AGENT-GUIDE.md).
Use `myce portability` before choosing targets and
`myce portability --extension NAME@VERSION --project PROJECT --json` after
registration to compare coverage, preparation and native qualification with live
inventory. See [live portability advice](portability.md). A missing variant is
a portability gap; document genuine hard constraints with `requirement_reasons`.
The legacy run/setup format and native baseline rules below remain supported.

Jobs declare software requirements; Mycelium selects the host and device.
Author every feasible platform variant with its own locks and native checks.
Reserve host/device pins for specific hardware, approved production parity or
qualification; see the [placement contract](portability.md#declare-requirements-let-mycelium-place).

New registrations default to the native sandbox. At the top level of
`extension.yaml`, `isolation: unsandboxed` plus a required nonempty `reason`
declares an exception for main/check payloads and all their sidecars. Status
and job GET expose that public reason. Pre-policy registrations without the
server-owned default marker retain legacy unsandboxed execution, even after
identical registration replay/restart. To opt in, author a new version/digest
with `isolation: sandboxed`; existing frozen jobs retain their original policy.
See [native isolation](native-isolation.md) for permissions and remaining checks.

An extension is one small folder: `extension.yaml`, plain notes and the few
scripts it needs. Use it to teach Mycelium a **kind of work** (preferred) or a
**program type**. A **hypha** must be **qualified** for that exact version and
kind/program type before it gets a job. Decisions: issues #18, #20, #35 and #36.
Extensions and plain commands share the same v3 runtime and software environments.

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
Mycelium sets `PYTHONDONTWRITEBYTECODE=1` for extension setup, checks and jobs
to keep ordinary Python imports from writing `__pycache__`/`.pyc` files into
the frozen library. Generated caches belong in `MYCE_EXTENSION_CACHE`; all
files in the library remain part of its immutable identity.

Setup downloads models, packages, source or installers into
`MYCE_EXTENSION_CACHE`, once per hypha/version/platform. They stay outside the
small folder. Use pinned versions and verify model/download hashes. Setup is
explicit, with a bounded timeout; it is never silently started by a queued job.
The CLI default `--timeout` is ten minutes. For setup, it bounds the whole
operation. For `extension check`, it bounds each check command's execution,
from the queue-recorded `Started` acknowledgement to completion. Queue admission
and environment/command preparation do not consume that execution budget.
Each check has a separate one-hour window to start, enforced by the existing
start-deadline mechanism without changing its experiment priority. If that
window expires, the command exits unsuccessfully and the local report says
`never started: <admission reason>` (for example, waiting for host RAM), rather
than `check failed`. It grants no qualification. An execution timeout remains a
failed check. Both limits are enforced by the queue, including after the CLI
disconnects; Ctrl-C still cancels the CLI's own check and confirms release.
Keep qualification checks and memory guards intact when allowing build time.
The cache is trusted local code, not a sandbox. Setup/check obtain an exclusive
cache lock; running jobs hold a shared lock. A setup failure clears qualification.
Cache cleanup is explicit.

Scripts receive `MYCE_EXTENSION_DIR`, `MYCE_EXTENSION_CACHE`, `MYCE_PLATFORM`,
`MYCE_DEVICES` and ready `MYCE_DEVICE` (`cuda:0`, `xpu:0`, `mps`; `cpu` only for
an explicitly fake test extension). Setup/check and jobs never inherit
`MYCE_TOKEN`. Cached `venv/bin` and `bin` precede the ordinary PATH. Run argv
expands `{extension}` and `{cache}` without shell expansion. The run command
receives kind arguments, or the program's local input filename and arguments.
It also receives `MYCE_WORK`, `MYCE_EXTENSION_NAME` and `MYCE_EXTENSION_VERSION`.

## Use automatic memory in a native variant

A composed native variant can omit both numeric memory floors:

```yaml
variants:
  - target: {os: linux, arch: amd64, platform: cuda}
    needs: {memory: auto}
    execution:
      main:
        command: [sh, '{extension}/run.sh']
    check: [sh, check.sh]
    references:
      kind:example-work: {input_shape: sample-v1, unit: records, units: 10}
    # Bundle the matching native baseline under baselines.kind:example-work.
```

Use the usual pinned environments and native baselines. The standard
`myce extension check` submits `needs.memory: auto` for this variant. Without
an accepted comparable peak or a submitter hint, it explores available memory
under the existing [memory envelope rules](design-proposal.md#amendment-job-memory-envelopes-146):
measured host memory, a known allowance, sufficient free headroom and no
protected memory-heavy work. Missing telemetry or headroom leaves it waiting;
qualification still requires a successful check and matching baseline output.

With an authored `references` entry, a successful solo auto check can teach
its memory peak for later work with the same frozen variant, work type and
input shape. Later jobs declare `workload: {input_shape: sample-v1, units: 10}`.
Checks teach no throughput or timing data, and the peak stays usable for 30
days from the check even after the point's throughput ages out. Failed or ceiling-constrained peaks
cannot seed an estimate. The variant's auto mode applies when the caller omits
`needs.memory`; an explicit caller mode takes precedence. Numeric variants keep
their existing reservations, and legacy extensions still require numeric floors.

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

For different native backends, use `platform_baselines` instead of `baselines`:

```yaml
platform_baselines:
  metal:
    program:example-script:
      input: baseline/input.json
      output: baseline/mps-output.json
      measure: numeric
      absolute: 0
      relative: 0
      native: Describe the actual MPS recording.
      variation: Link its repeated measurements.
  cuda:
    program:example-script:
      input: baseline/input.json
      output: baseline/cuda-output.json
      measure: numeric
      absolute: 0
      relative: 0
      native: Describe the actual CUDA recording.
      variation: Link its repeated measurements.
```

Use one map style per extension. With `platform_baselines`, a missing matching
platform/work reference stays not qualified; it never falls back to MPS or any
other backend. Existing frozen extensions with `baselines` keep their declared
common-reference contract.

To record a reference, run the extension's actual check on authorized **native**
hardware after its pinned setup and backend check succeed. Supply the bundled
input as `MYCE_CHECK_INPUT`, a new recording path as `MYCE_CHECK_OUTPUT`, and the
same `MYCE_DEVICE`/resource selector and cached Python used for jobs. Run at least
three repetitions. Retain each actual output, software/backend versions,
architecture, precision, exit codes and measured spread in a bundled measurement
file. Copy an actual recorded output into the corresponding platform reference;
do not compute the reference from known answers or copy another backend's output.
Keep existing references intact. Review the measurements and freeze the scripts,
reference and description under a new extension version **before** installing
and qualifying it. Recording is an explicit author operation; qualification
never records or updates its own reference. No wider allowance is implied.

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

The host/device selectors in this section identify hardware being qualified.
Ordinary submissions below leave host/device choice to Mycelium.

On an installed worker, run locally with the worker enrolled and its queue
running. Authority, host ID, extension root (`state_dir/extensions`) and runtime
check environment cache default to its installed configuration. `--config FILE`
selects a custom installation; explicit `--host`, `--root` and
`--environment-cache` override these defaults. Keep tokens out of argv and job
environments. Select the qualification platform and device explicitly:

```sh
myce extension add ./extensions/example
myce extension check --gpu xpu --device 0 example@1
myce extension list --gpu xpu --device 0
```

Runtime check performs preparation through the enrolled worker before execution.
For CPU qualification use `--gpu cpu` without `--device`. Without installed
configuration, supply `--host` and `--environment-cache` as well, or start
`myce dev` for automatic local defaults. For a manual join, use the same extension
root for CLI and `join --extension-root`; a runtime check with a different root
from the installed worker warns that it does not qualify the live worker.

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

Exactly one of kind, program or the plain-command `run.command` is accepted.
The program file names an input, not a machine path. CLI YAML, API JSON and
Python `Client.submit(dict)` use these identical fields. Kind/program jobs do
not declare `needs`, an environment, command or workdir. Ordinary `run.env` can
supply program configuration, but cannot override worker/extension identity,
resource selectors or the ready resource string. Mycelium derives conservative
memory reservations from the extensions and optional size hint. Capability profiles and measured program needs are used by the v3 queue;
see [placement and measurement limits](guide-placement-v3.md). Multiple matching extensions
are tried in deterministic name/version order, only when qualified. A job
records the exact chosen name/version/content digest on every attempt.

On v3, [files without a NAS](guide-files-v3.md) supports `submit --files-from`
and hash-addressed `files`, private staging and accepted `output_refs`. Model
files can be inputs too. The following NAS transport remains available unchanged.

The shared-storage transport uses the existing NAS model. `--job-file-root` is the hypha's local mount; every input/shared prefix is
relative to it. Mycelium copies inputs into a private per-attempt working folder,
then runs the extension there. Successful outputs go to
`PREFIX/JOB_ID/ATTEMPT/LOCAL_NAME`; `job.output_files` gives these relative names.
Consume outputs only from the successful current attempt. Publication never
overwrites another attempt, including a late stale execution. The no-NAS path uses the same private attempt isolation, with HTTP transfers
and durable publication instead of shared paths. Missing files,
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

- `extensions/pytorch-script`: PyTorch 2.9.1+cu130 on Linux CUDA (arm64/amd64), 2.8.0 on MPS/XPU,
  extension v0.2.3, CUDA/XPU/MPS, unmodified programs
  choosing `torch.device(os.environ['MYCE_DEVICE'])`. Includes a small native
  MPS reference retained unchanged and a separately recorded GB10 CUDA
  pointwise float32 reference, each with zero measured spread. Setup requires a
  real assigned-backend kernel launch. XPU stays unqualified until an author
  supplies its matching native reference. This qualifies
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

For optional timed native-reference work, see [Onboarding benchmark](guide-benchmark-v3.md).
