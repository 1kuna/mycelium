# Submit

Detail for the ladder in [the guide](../SKILL.md): ways to submit, needs,
source and keys, environments, isolation, models, inputs, checkpoints and
placement restrictions.

## Ways to submit

- Adapter entry: `myce run --entry NAME`, with `--wait --out DIR` for the whole
  trip. This is the default for project work.
- Compatibility profile: `myce run --profile NAME` (same receipt and recovery;
  `myce adapter convert` turns profiles into entries).
- One-off: `myce run -- CMD` runs an installed command; `--out DIR --output NAME`
  returns its files. `--gpu cuda|xpu|metal` requests an accelerator, and `--mem`
  then means device memory and `--host-mem` host memory. One-offs take no
  source, key, notifier or input files; use an entry for those.
- Job file: `myce submit FILE`, or Python `client.submit(spec)`. Use it for
  `after:` stages and specs you build in code.

`myce spec check --resolve --entry NAME .mycelium/adapter.yaml` asks the queue,
read-only, where the entry can run and names anything missing. It submits,
uploads, prepares and qualifies nothing. A fitting target is not evidence of
native output parity.

## Needs, targets and variants

Describe needs and class, without a host or device selector. Leave memory out
and Mycelium learns it; a declared limit keeps its containment meaning.

A non-production entry without `targets` runs on any machine type its platform,
lock and `needs.tools` fit. A first failure on a type the recipe never succeeded
on is a coverage finding, not your job's failure: the queue skips that type for
the entry and requeues elsewhere, and `entry_coverage.findings` names the type
and its fix. Production keeps explicit targets so MY-19 output parity holds
([entries without targets](../../../docs/adapter.md#entries-without-targets)).

Register supported implementations as variants of one extension, each with its
target, resources, environment and native check. Mycelium selects among
qualified variants and preserves scarce capacity. A single implementation for
one OS/architecture/backend is a real coverage limit, which a pin doesn't fix.
`myce portability --extension NAME@VERSION` shows gaps and preparation actions.
Native variants need their own qualification and output criteria before
production. See [adapters](../../../docs/adapter.md),
[environments](../../../docs/environments.md) and
[scheduling](../../../docs/scheduling.md).

Extension registration freezes content; it is not an owner review. Native checks
run automatically on first demand against the author's baseline, and a failed
check holds that extension. The
[execution gate audit](../../../docs/execution-gates.md) lists the protections.

### Disk needs

A job placed on a host without room for its working files fails inside the job
and wastes the placement. Disk admission charges each host volume for what the
job writes there and holds the job until a host has that much free above its
floor. Nothing has to be declared; these optional `disk:` hints help the first
runs of large work:

```yaml
disk:
  scratch_bytes: 64424509440   # peak work dir + TMPDIR, outputs written there included
  input_bytes: 10737418240     # only when input manifests carry no sizes
  output_bytes: 2147483648     # only when output sizes are unknown
  checkpoint_bytes: 1073741824 # resumable work without a measured resume manifest
```

After one accepted run, the measured work dir and `TMPDIR` peak for the same kind of
work (program, source, configuration, input shape and machine type), plus its
measured margin, replaces `scratch_bytes`. Work that runs once per item (new
inputs or source each run) learns from its entry instead: after two accepted
runs of the same project and name on that machine type, their peak plus the
spread between items replaces `scratch_bytes`, and status shows storage basis
`family`. A measured value always beats a hint.
An environment that a host has not built yet is charged to that host's
environment volume at the size another host reported for the same key. A job
that fits nowhere right now waits with `disk_headroom` and a reason such as
`disk: 60.0 GiB needed, 48.5 GiB free on HOST`. When no reporting host could
hold it even with its disk empty, submission is refused with the same code.

## Source, keys and recovery

An entry's source defaults to committed Git `HEAD`. Untracked files that aren't
ignored count as dirty and refuse the run, so ignore output folders.
`source: {git: HEAD, paths: [scripts/]}` sends selected committed paths;
`source: {directory: DIR}` sends a folder (ignored files included, `.git` and
`.mycelium/receipts` excluded, links refused). The adapter is frozen separately,
so uncommitted edits to it are fine. Keep `.mycelium/receipts/` private and
ignored.

The default key derives from the entry, source and arguments, so the same run
twice is one job. Pass `--key NAME` for an intentional repeat, and keep it when
replaying the same operation. After a lost reply, `submission unconfirmed` or
exit 69 `input storage unconfirmed`, rerun the original command with the same
key and frozen inputs; a failed observation or delivery doesn't create a new
job. A new key alone can't fix a deterministic environment failure: correct the
recipe. `deadline_guidance.details_deferred: true` on a production receipt means
read the accepted job for full blockers, not resubmit.

Go/CLI and Python uploads resume an interrupted session by checking the
content-addressed blob, and success needs server SHA-256 verification plus
matching size. Keep source files unchanged during upload. Typed integrity,
authority and storage-policy refusals need their owner. See
[file transfer recovery](../../../docs/files.md#transfer-behavior).

The Python client retries overload after the current Retry-After within your
deadline, and keyed submissions and waits recover transient TLS record/EOF
failures. Certificate, identity, authentication and configuration failures stay
fatal; unkeyed writes and model calls are not replayed.

## Environments

Without an environment the command uses the worker's installed tools; list them
in `needs.tools` (PATH executables, not packages) so placement skips machines
without them. For exact versions, use a lock file. `myce init` picks up
`pixi.lock` or `uv.lock` and writes the environment for you.

A relative `lock_file` is read from the project root (the folder holding
`.mycelium/`), the same base as `inputs`, and `myce run` uploads it with its
companion: `pixi.toml` for `pixi.lock`, `pyproject.toml` for `uv.lock`,
`package.json` for `package-lock.json`. The
queue freezes those bytes and their digest into the job, so later edits need a
new run. An absolute `lock_file` is read by the queue at that path instead. Each
file is limited to 1 MiB; a missing or oversized one reports a `path_diagnostic`
naming the side (`submitter` for a relative path, `queue` for an absolute one)
and any missing companion.

```sh
# env/pixi.toml and env/pixi.lock come from `pixi lock` in your project.
cat > .mycelium/adapter.yaml <<'YAML'
project: example
entries:
  pixi-task:
    source: {directory: source}
    run:
      command: [sh, -c, "printf 'pixi-ok\n' > result.txt"]
    environment: {backend: pixi, lock_file: env/pixi.lock}
    outputs: {result.txt: results}
YAML
myce run --entry pixi-task
```

Workers install with **Pixi 0.81.0** (`pixi install --locked`) and never solve.
The queue runs the same up-to-date check at submission and in `spec check
--resolve`, and refuses a stale lock with `job_environment_invalid` and Pixi's
exact mismatch. A lock written by an older Pixi can fail here (for example
`__unix=0` where 0.81 expects `__unix=0=0`): run `pixi lock` with Pixi 0.81.0
or later, commit it and run again.

npm projects use `environment: {backend: npm, lock_file: package-lock.json}`.
`node_modules` and post-install tool downloads such as headless browsers are
prepared once per lock on each worker and reused read-only; see
[npm package locks](../../../docs/environments.md#npm-package-locks).

`myce status JOB --json` and Python `client.get(job_id)` show the frozen lock as
`environment_lock`, for example
`{"backend": "pixi", "sha256": {"pixi.lock": "…", "pixi.toml": "…"}}`. Compare it
with `shasum -a 256 env/pixi.lock env/pixi.toml`. See
[project lock files](../../../docs/environments.md#project-lock-files).

Every native job is told its share: `MYCE_CPUS`, thread defaults
(`OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `OPENBLAS_NUM_THREADS`,
`VECLIB_MAXIMUM_THREADS`, `NUMEXPR_NUM_THREADS`, `RAYON_NUM_THREADS`,
`GOMAXPROCS`), and `TMPDIR` plus `MYCE_SCRATCH_DIR` in attempt scratch. A value
you set in `run.env` wins. See
[job environment](../../../docs/reference/native-execution.md#job-environment).

## Isolation

Plain native commands run unsandboxed by default (MY-25): they read what the
worker user can, including NAS shares and other jobs' files. They can't read the
worker's installed configuration, fleet credential, joining or environment
files, or the worker user's credential stores (`~/.ssh`, `~/.config/gh`,
`~/.git-credentials`, `~/.netrc`, `~/.docker/config.json`, `~/.aws`,
`~/.config/gcloud`, `~/.kube`, the macOS login keychain). A job therefore can't
submit through the worker's authority or act as the worker user on GitHub,
registries, clouds or SSH; pass such access as a declared input. A Linux worker
without Landlock ABI 2 logs that it can't withhold them. Declare
`isolation: sandboxed`, or use managed Docker, for an enforced boundary.

For builds and tests of your repository, declare `isolation: ci`. It needs no
owner approval. The job then can't read the NAS, the worker user's home, the
shared model library or other jobs' files. HOME is a private folder in attempt
scratch, so git and Go/npm/pip/cargo caches work there. It sees its declared
inputs, model snapshots, prepared environments and system toolchains, and runs
offline unless you declare `needs.network: true`. Linux workers don't run
networked native jobs yet, so prepare or vendor dependencies there. Toolchains
installed in the worker user's home aren't visible; declare an environment.
Only workers advertising `native_isolation_ci` run these jobs. See
[native isolation](../../../docs/native-isolation.md).

## Models

Declared `model_snapshots` need no queue-owner approval (MY-44). Optional
`models.sources` patterns restrict repositories (`!OWNER/NAME` denies, deny
first); empty sources restrict nothing. Snapshots need an exact commit and
verified bytes; loading a snapshot is not qualification or parity. SMB `XSym`
emulated links resolve to verified blob bytes or refuse with
`model_snapshot_mismatch`/`model_snapshot_escape`; keep the original hub cache
intact rather than copying weights. See
[model snapshots](../../../docs/files.md#declared-model-snapshots-553).

Fetch a model master file by file with
`myce model fetch BUNDLE/FILE --model-root PARENT --url URL --size-bytes BYTES --sha256 HASH`.
It stages beside the root, resumes interruptions and publishes only verified
files; invalid bytes stay in the reported quarantine. Loading never downloads
weights. See [master downloads](../../../docs/services.md#explicit-master-download-and-pins).

Model loads have no fixed total cutoff while copy, load or health signals
advance; keep the same request while it progresses. Custom servers report
`MYCE_LOAD_PROGRESS` records ([adaptive readiness](../../../docs/readiness.md)).
For catalog image chat, use the Python client's openai extra:
`Client.from_installed().openai(project=PROJECT, priority="experiment")`, with
base64 `image_url` parts. See [image-chat example](../../../examples/image-chat.py).

## Inputs, checkpoints and pieces

`inputs: {DEST: local/path}` and `--input DEST=PATH` upload files from the
project root; they are frozen and verified with the job. Inside a job, read
paths relative to its working folder.

NAS data is declared in `shared_inputs` with a path relative to the NAS root,
its SHA-256 and size. The worker stages it on local disk at every size (MY-36),
verifies it and gives the command `$MYCE_INPUT_<NAME>`. Archives add
`archive: tar.gz` and `unpacked_bytes`; a large tree uses `manifest_file`.
`myce spec check --resolve --inspect-nas` reports cached worker existence (not
byte) evidence. A missing NAS source fails before launch with
`input_staging: not_found`. Keep integrity and capacity refusals: re-uploading
identical oversized inputs can't make them fit. See
[shared inputs](../../../docs/files.md).

Claim `resumable: true` only after implementing atomic checkpoints and a real
restore. Each save writes a temporary file in `$MYCE_CHECKPOINT_DIR`, fsyncs and
renames it, carrying a rising `myce_checkpoint.generation`; Python's
`Checkpoint` does this, and `StopSignal` sets a flag on SIGTERM so you save
between work units. Checkpoints are not results and not an input cache: a
checkpoint is kept 72 hours after the job ends. A disposable non-resumable
experiment may declare `preemptible: true` so it can leave a scarce host by
restarting from scratch (MY-26); class alone never implies it. See
[checkpoints](../../../docs/files.md#resumable-jobs).

Resumes stay on the checkpoint's target. The queue records the OS/arch/platform
of the attempt that wrote the newest accepted generation and places the resume
only on a host of that target while one remains in the fleet, even if it is
busy, disabled for maintenance or temporarily lost; status names the wait
`checkpoint_target`. A host leaving the fleet ends the wait. If your checkpoints
replay identically across targets, declare it with tested `resume_compatibility`
entries for each target (the same declaration scarce-host yield uses); a resume
then moves between any listed targets. `portable: true` says only that the
program runs on any arch.

Plain `pieces:` (1–256, unique lowercase keys) run one command per piece. Each
piece's `args` follow the base command and any CLI arguments; its `inputs` reach
only that piece. A failed piece doesn't stop the others; the parent fails once
all settle and names the failed keys, while accepted pieces keep their outputs.
Cancelling or pausing the parent covers every unfinished piece. Pieces can't be
combined with `after:`, Docker, `workdir`, kind/program, `workload` or
`fan_out:`. See [pieces](../../../docs/jobs.md#plain-command-pieces).

An `after:` stage binds retained accepted outputs of a succeeded parent, even if
the parent later received a cancellation. A parent cancelled before success
cancels its dependents with `upstream_cancelled`; expired outputs return
`result_expired`. See [stages](../../../docs/jobs.md#start-a-consumer-when-one-piece-is-accepted).

Opt-in XPU fan-out sharing needs fresh VRAM counters from the separately
administered telemetry broker; without them extra sharing is withheld while
ordinary admission continues. Ask the installation owner to qualify the
[broker](../../../deploy/gpu-telemetry/README.md) rather than starting samplers.

## Explicit placement restrictions

Use `allowed_machines`, an approved host exclusion or `needs.gpu.devices` only
when the task really restricts hardware: a specific physical device, an
owner-approved dedicated machine, an explicit prohibition on a personal host, or
qualifying or benchmarking exact hardware. Record the requirement and evidence
in `requirement_reasons`. Capacity waiting, urgency, a past incident, cached
software or a familiar machine name are not requirements; declare the
environment and inputs instead of pinning to where they live. Application
backend flags may consume the worker's assigned visibility; never set
`MYCE_DEVICE` to choose a card.

Apply placement changes to future work. Keep the frozen spec, key and receipts
of accepted or unconfirmed submissions. If a consumer put a host list in its key
or environment identity, removing it needs a deliberate migration of future
operations; rewriting an accepted operation can duplicate work.

A configured project `production_parity_hosts` list restricts every production
placement, including pieces and recovery, without a matching job pin. An empty
list holds production; only the owner can widen it after output comparison.
Checks and experiments can collect that evidence.

For throughput comparisons, declare the implementations and a comparable
workload identity so Mycelium learns placement from accepted measurements. Use
the exact-hardware path only when measuring that hardware is the task; final
runs use the unpinned entry.
