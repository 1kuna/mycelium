# Submit

Detail for step 1–2 of [the guide](../SKILL.md). A field or option your
installed component lacks is unknown; check [capability evidence](release-behavior.md#capability-evidence).

## Ways to submit

- Adapter entry: `myce run --entry NAME --key KEY` (the default for reusable work).
- Compatibility profile: `myce run --profile NAME --key KEY`.
- One-off: `myce run --cpu N -- CMD`; `--gpu cuda|xpu|metal` requests an
  accelerator, `--mem` then describes device memory and `--host-mem` host memory.
  One-offs have no key, notifier or input-file contract; use an adapter for those.
- Job file: `myce submit FILE`, or Python `Client.submit`; both resolve
  installed authority.

Run `myce spec check --resolve` first (adapters: `--entry NAME FILE`). Resolve
does not submit, upload, prepare an environment or qualify a device. An
unchecked dependency, unknown observation or missing qualification has an owner
action; a fitting target is not evidence of native output parity.

## Needs, targets and variants

Describe needs, class and verified targets, without a host or device selector.
Memory may be omitted for automatic learning; declared limits keep their
containment meaning. CPU forecast reservations stay within the job's actual hard
cgroup ceiling, as do hard-contained remaining-growth forecasts; measured demand,
probe floors, live RAM and allowance checks still apply.

Register supported implementations as variants of one extension, each with its
target, resources, environment and native check. Mycelium selects among
qualified variants automatically, preserves scarce capacity and uses comparable
measured completion data; unknown timing is not a reason to select a machine. A
single implementation for one OS/architecture/backend is a real coverage limit;
a pin does not make it portable. `myce portability --extension NAME@VERSION`
shows observed gaps and preparation actions. Preparation, immutable dependency
locks and a successful smoke test do not establish MY-19 production parity;
native variants need their own qualification and output criteria. See
[adapters/variants](../../../docs/adapter.md), [environments](../../../docs/environments.md),
[scheduling](../../../docs/scheduling.md), and [setup](../setup-project.md) for
managed CPU target discovery (`portable: true`).

Extension registration freezes content; it is not an owner review. Native checks
run automatically on first demand against the author's baseline, and a failed
check holds that extension. `unsandboxed: legacy extension registration` is a
status diagnostic, not an approval request. The
[execution gate audit](../../../docs/execution-gates.md) lists the remaining protections.

## Source, keys and recovery

Directory source includes ignored files except `.git` and `.mycelium/receipts`;
links are refused. Git source such as `source: {git: HEAD, paths: [scripts/]}`
sends committed selected bytes, so commit those paths first. The adapter itself
is frozen separately. Keep private receipts and ignore them in Git.

Choose the key once and keep it; never regenerate it when replaying the same
operation. The same key recovers accepted intent; changed inputs, recipe or
intent need a new key. A new key alone cannot repair a deterministic environment
failure: correct its immutable recipe. For an uncertain reply, rerun the
original saved command with the same key and frozen inputs, and keep the
original notifier binding if you supplied one. A failed observation or delivery
does not create a new job.

Submission and worker writes take fair turns. Production acceptance returns its
durable receipt without rebuilding optional planner diagnostics;
`deadline_guidance.details_deferred: true` means read the accepted job for full
current blockers, not resubmit. Submission responses aim well within a 20-second
submit budget, which is not a start-time guarantee. Input verification, storage
failure and shutdown remain separate causes.

Go/CLI and Python recover a missing resumable upload session (404) by checking
the content-addressed blob; success requires server SHA-256 verification plus
matching client size/ETag. If the blob is also absent, the same upload retries
within its owner deadline (Go context or Python `transfer_timeout`). Keep source
files unchanged during upload: clients bind completion to the source size and
SHA-256, so another uploader's matching blob cannot turn a changed-source retry
into success. Typed integrity, authority and storage-policy refusals still need
their owner. Upload errors use the standard JSON code/error envelope; HEAD carries
`X-Myce-Error-Code`. See [file transfer recovery](../../../docs/files.md#transfer-behavior).

The Python client shares retry classification and pacing across requests.
Overload retries follow the current Retry-After plus positive jitter, bounded by
the caller's deadline. Keyed submissions and waits recover transient TLS
record/EOF failures; certificate, identity and protocol refusals stay fatal.
Permanent identity, authentication, integrity and configuration failures keep
their typed stop.

## Isolation and dependencies

Plain native commands are unsandboxed by default under MY-25: they read
whatever the worker user can, including NAS shares and other jobs' files.
From the worker release containing #1309, they (and `isolation: unsandboxed`
jobs) can't read the worker's installed configuration, fleet credential,
joining or environment files, or the worker user's `~/.ssh`. A job therefore
can't submit through the worker's installed authority or use the worker
user's SSH keys; pass such access as a declared input instead.
Declare `isolation: sandboxed`, or use managed Docker, for an enforced boundary;
new native extension registrations have their own sandbox policy.

For CI, meaning builds and tests of your repository's code, declare
`isolation: ci`. It needs no owner approval. The job then can't list or read the
NAS, the worker user's home, the shared model library or other jobs' files on
Linux or macOS workers. HOME is a private directory in attempt scratch, so git
and Go/npm/pip/cargo caches work there. It still sees its declared inputs, model
snapshots, prepared environments and system toolchains. It runs offline unless
you declare `needs.network: true`, and Linux workers can't run networked native
jobs yet, so prepare or vendor dependencies there. Toolchains installed in the
worker user's home aren't visible; declare an environment instead. Only workers
advertising `native_isolation_ci` run these jobs. Read
[native isolation](../../../docs/native-isolation.md) for platform limits.
Declare tool, environment, network and model dependencies through their
[adapter fields](../../../docs/adapter.md); Mycelium cannot infer hidden shell
dependencies.

Inline jobs accept a project Pixi lock. Give `lock_file` relative to the
project root (the folder holding `.mycelium/`, or the working directory), the
same base as `inputs`; `myce run` uploads `pixi.lock` and the adjacent
`pixi.toml` with the submission, so the queue needs no path to them. `python`
is optional. The queue freezes the bytes and their digest into the job, so
later edits need a new submission:

```sh
# env/pixi.toml and env/pixi.lock come from `pixi lock` in your project.
cat > .mycelium/adapter.yaml <<'YAML'
schema: 1
project: example
default: pixi-task
entries:
  pixi-task:
    source: {directory: source}
    class: experiment
    run:
      command: [sh, -c, "printf 'pixi-ok\n' > result.txt"]
    needs: {platform: cpu, cpu_cores: 1, memory_gb: 0.125}
    targets:
      - {os: darwin, arch: arm64, platform: cpu}
      - {os: linux, arch: amd64, platform: cpu}
      - {os: linux, arch: arm64, platform: cpu}
    environment: {backend: pixi, lock_file: env/pixi.lock, platforms: [cpu]}
    outputs: {result.txt: results}
    checks:
      result: [test, -s, result.txt]
YAML
myce spec check --resolve --entry pixi-task .mycelium/adapter.yaml
myce run --entry pixi-task --key "pixi-task-$(date -u +%Y%m%dT%H%M%SZ)"
```

An absolute `lock_file` is still read by the queue at its own path. Each
input is limited to 1 MiB; oversized or missing inputs report a
`path_diagnostic` (`side: submitter` for a relative lock, `side: queue` for
an absolute one), naming any required companion. See
[project lock files](../../../docs/environments.md#project-lock-files).

Workers install with **Pixi 0.81.0** (`pixi install --locked`; they never
solve). When the queue reads or receives a Pixi lock, it runs the same
up-to-date check with that version and refuses a stale lock at submission
and in `myce spec check --resolve` with `job_environment_invalid`, Pixi's
exact mismatch and the fix. A lock written by an older Pixi can fail here (for
example `__unix=0` where 0.81 expects `__unix=0=0`): run `pixi lock` with
Pixi >= 0.81.0, commit the new lock and resubmit. Native extensions with Pixi
recipes get the same check at registration. If the queue host lacks Pixi
0.81.0, the check is skipped and the worker still refuses at preparation.

To read back which lock a job froze, `myce status JOB --json` (and the Python
client's `get(job_id)`) includes `environment_lock`: the backend and each frozen
input's sha256, for example
`{"backend": "pixi", "sha256": {"pixi.lock": "…", "pixi.toml": "…"}}`. Compare it
with `shasum -a 256 env/pixi.lock env/pixi.toml`. It appears on single-job
replies only, not in status lists; `spec.environment` stays projected away.
Plain `myce status JOB` prints it as one `environment lock` line.

## Models

Declared `model_snapshots` need no queue-owner approval (MY-44): absent model
policy permits every repository. Optional `models.sources` positive patterns
restrict repositories and `!OWNER/NAME` patterns deny them, deny first; empty
sources restrict nothing. Snapshots need an exact commit and verified bytes;
loading a snapshot is not qualification or parity. `myce model fetch` accepts any
HTTPS file URL with the exact size and SHA-256. Size and cache limits apply;
omitted or zero `free_floor_bytes` uses the worker cache volume's headroom rule
(1% of capacity, at least 256 MiB). See
[model snapshots](../../../docs/files.md#declared-model-snapshots-553) and
[model masters](release-behavior.md#explicit-model-master-downloads-source-audited-verify-installed-support).

For catalog image chat, use the Python client's openai extra:
`Client.from_installed().openai(project=PROJECT, priority="experiment")`, keeping
the installed TLS transport and base64 `image_url` parts. The model must already
be in the matching catalogs. See [image-chat example](../../../examples/image-chat.py).

## Inputs, checkpoints and fan-out

Stage NAS inputs locally before processing, at every size (MY-36). Declare
read-only shared inputs with relative NAS source, SHA-256 and size, then read
`$MYCE_INPUT_SOURCE`; declared legacy path inputs copy into the attempt folder.
Resolve alone does not verify worker NAS bytes. For submitter-local bytes, use
the upload/local-input contract. Keep integrity and capacity refusals:
re-uploading identical oversized inputs cannot make them fit. Mutable
`MYCE_CHECKPOINT_DIR` state is not an input-media cache. See
[files/checkpoints](../../../docs/files.md).

Claim `resumable: true` only after implementing atomic checkpoints and a real
restore. Safe movement needs a compatible qualified resume/result contract,
committed progress and exact release; a stop request is not migration.
Disposable non-resumable experiments need the explicit preemptible contract, not
an inference from experiment class.

A new `after:` consumer can bind retained, unexpired accepted outputs from a
succeeded fan-out parent even if the parent later received a cancellation
intent. A parent cancelled before success cancels its dependents with
`upstream_cancelled` (reason `upstream parent cancelled`); expired outputs return
`result_expired`.

Opt-in XPU fan-out sharing needs fresh VRAM counters from the separately
administered read-only telemetry broker. An older GTT-only broker still supports
host RAM accounting; without VRAM evidence, extra sharing is withheld while
ordinary exclusive admission continues within its budget. Ask the installation owner to
qualify the [updated broker](../../../deploy/gpu-telemetry/README.md) rather than
starting vendor-tool samplers yourself.

## Explicit placement restrictions

Use `allowed_machines`, an approved host exclusion or `needs.gpu.devices` only
when the task really restricts hardware: a specific physical device, an
owner-approved dedicated machine, an explicit prohibition on a personal host, or
native qualification/benchmarking of exact hardware. Record the requirement and
evidence in `requirement_reasons`. Capacity waiting, urgency, a past incident,
cached software or a familiar machine name are not requirements; declare the
environment and stage inputs instead of pinning to their location. Application
backend flags may consume the worker's assigned visibility; never set
`MYCE_DEVICE` to choose a card before submission.

Apply placement changes to future work. Keep the frozen spec, key and receipts of
accepted or unconfirmed submissions. If a consumer put a host list in its key or
environment identity, removing it needs a deliberate migration of future
operations; rewriting an accepted operation can duplicate work.

A configured project/work `production_parity_hosts` list is an owner restriction
on every production placement, including fan-out and recovery, and Mycelium
enforces it without a matching job pin. Native qualification, measured quality
and historical registration cannot admit an unlisted host. An empty list holds
production; only the owner can widen it after output comparison. Checks and
experiments can collect that evidence. A removed host's queued binding is placed
again; running execution and release custody stay with the current attempt.
Parity-held production cannot withhold another project's environment
preparation on an unapproved host.

For throughput comparisons, declare the implementations and a comparable
workload identity so Mycelium learns placement from accepted measurements. Use
the exact-hardware qualification path only when measuring that hardware is the
task; final runs use the unpinned entry.
