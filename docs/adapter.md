# Author reusable project work

For reusable project work, author **one `.mycelium/adapter.yaml`**, choose an
entry, submit it, wait, then retrieve verified results. Start with a command and
its existing inputs/outputs; add complete native variants when qualification is
needed. Keep application orchestration and business side effects in the caller.
CPU commands are supported alongside GPU work.

| Need | Choose | Completion behavior |
| --- | --- | --- |
| One installed command | `myce run -- CMD` | Waits and streams logs; `--out DIR --output NAME` returns files; `--detach` returns the ID |
| Explicit job YAML/JSON or durable caller lifecycle | `myce submit job.yaml` | Returns a job ID; caller waits/cancels |
| New reusable project integration | [`myce init`](#initialize-an-adapter), then `myce run --entry NAME` | Returns a receipt; `--wait --out DIR` does the whole trip |
| Existing project profile | `myce run --profile NAME` | Same receipt/recovery path; conversion is explicit |
| Shared kind/program with frozen native recipes | Registered extension | Native baseline qualification before useful work |

Read the [entry contract](#project-instruction-file), then the
[source and receipt reference](#project-profiles) when you need recovery or
per-item inputs. [Native extensions](#write-an-extension) covers registry callers,
baselines and composed helpers. [Conversion](#convert-profiles-and-extensions)
never switches callers or removes old readers. See [jobs](jobs.md) for keyed
intent/cancellation, [files](files.md) for custody, and
[scheduling](scheduling.md) for live portability advice.

A non-production entry or profile may omit `targets` entirely: it runs on any machine type its platform, Pixi lock and declared
tools fit, and Mycelium learns its coverage
([entries without targets](#entries-without-targets)). Production entries keep
explicit targets (MY-19).

A flat non-production native CPU entry or compatibility
profile may instead set `portable: true` and omit targets when all member environments
have selected immutable managed Pixi lock coverage. Mycelium derives candidates
from those locks, supported runtimes and fresh inventory; see
[managed CPU discovery](environments.md#cpu-tools-with-pixi). `portable: true`
asserts the entire payload. Unknown coverage requires explicit verified targets.
Nonempty explicit targets always bind. Production widening still uses exact
MY-19 acceptance through native variants/baselines or an explicit owner-accepted
target list; automatic plain-command production discovery remains held.

Before authoring, run `myce portability`, then
`myce portability --adapter .mycelium/adapter.yaml --entry NAME`. Declare verified
targets and software requirements; Mycelium chooses the host. Document a hard
host/device exception in `requirement_reasons`. Missing variants are gaps, and
MY-19 output parity precedes production splits. Authored `requirement_reasons` are frozen in submitted specs and private receipts. Status/job GET expose sanitized constraint/presence/source metadata, while raw text remains redacted; see [status](status.md#authored-requirement-explanations-748). Static job validation is
`myce spec check FILE`; it grants no live admission or native qualification.
`myce spec check --resolve --entry NAME .mycelium/adapter.yaml` asks the queue,
read-only, which targets can run a command entry and how to stage what they
cannot see ([resolve](jobs.md)). Native variant entries also return metadata and
lock-input gaps; their local recipes remain unqualified until frozen registration
and qualification. Optional `--inspect-nas` gathers cached worker existence
signals for declared NAS shared inputs; repeat after heartbeats and treat absent
evidence and bytes as unverified. This requires upgraded queue and workers.

## Project instruction file

Plain native command entries default to **unsandboxed** under MY-25;
`isolation: sandboxed` explicitly opts in. `isolation: ci` is sandboxed plus a
private HOME and no view of the worker's home, NAS or shared model library. Use
it for builds and tests of repository code ([CI mode](native-isolation.md#ci-mode-1298)). New native variant/extension
registrations default to sandboxing. An explicit top-level
`isolation: unsandboxed` requires a nonempty `reason` and covers main,
result checks, queued qualification payloads and sidecars. Pre-policy registered
extensions retain their frozen policy; publish a new version with
`isolation: sandboxed` to opt in. The reason is public in status/job GET. See
[native isolation](native-isolation.md) for the ordinary host permissions of
unsandboxed stages, retained launch/device/cleanup checks and durable legacy
registration policy. This is an entry choice, not a global or per-sidecar flag.

`.mycelium/adapter.yaml` describes named entries in strict YAML. `myce run`
selects its `default`; `myce run --entry NAME` selects another entry.
`--adapter FILE` explicitly selects a file. Arguments after `--` append literally
when `--entry` or `--adapter` selects the entry. Plain `myce run -- CMD` retains
its installed-command behavior, including in a project containing an adapter.

The smallest file is a project and one command:

```yaml
project: example
entries:
  test:
    run: {command: [python3, -m, pytest]}
```

Every other field is optional. The defaults are:

| Field | Default |
| --- | --- |
| `schema` | `1` |
| `default` | the only entry; with several entries and no default, `myce run` needs `--entry NAME` |
| `source` | committed Git `HEAD` |
| `class` | `experiment` |
| `needs` | `{cpu_cores: 1, memory: auto, platform: cpu}` |
| `environment` | installed tools on the worker (the same as `environment: {}`) |
| `checks` | none: the command's exit status and required outputs decide success |
| `targets` | any machine type that fits (non-production only; see below) |
| `disk` | none: disk admission uses manifest sizes and learned peaks; add `{scratch_bytes: N}` for a large work dir plus `TMPDIR` before the first accepted run ([disk needs](../skills/mycelium/references/submit.md#disk-needs)) |

A fuller entry adds what it needs:

```yaml
project: example
default: analyze
entries:
  analyze:
    source: {git: HEAD, paths: [scripts/]}
    run: {command: [python3, scripts/analyze.py]}
    environment: {backend: pixi, lock_file: pixi.lock}
    outputs: {result.json: results}
    checks:
      result: [python3, scripts/analyze.py, --check]
```

### Entries without targets

A non-production entry or profile that omits `targets` runs on any machine type
(OS, architecture and platform) where its `needs.platform`, its Pixi lock and its
`needs.tools` fit. A Pixi lock that declares its platforms limits the entry to
those OS/architectures; a uv lock or installed tools do not limit it.

The queue keeps a small coverage record per project entry and recipe. The recipe
is the entry's command, checks, common inputs, environment, lock bytes, tools and
platform. Source commits and per-run arguments are not part of it, except that
`memory_class: per_item` makes arguments and per-item inputs part of it.

- The first run on a machine type the recipe has never succeeded on is an
  ordinary attempt.
- If it fails there, the failure is a **coverage finding**, not the job's
  failure. The queue records the finding, skips that machine type for this entry,
  and requeues the job on another type. The job's `entry_coverage.findings` and
  attempt history name the type and its one action: make the command, tools or
  lock file work there. Otherwise it stays skipped.
- Any change to the recipe clears its skips, so the next run may try those types
  again. Waste is bounded to one attempt per machine type per recipe.
- A failure on a type the recipe already succeeded on is the job's own failure.
- If the job fails on every type it can use and the recipe has never succeeded
  anywhere, the failure is not type-specific. The job fails with the command's
  error and the skips are cleared.
- If no type remains where the recipe has succeeded (for example, that machine
  left), the job fails with the coverage finding as its reason.

Placement explanations and `no_qualified_target` refusals name a skipped type
as `entry coverage`; the read-only `--resolve` preview does not yet include
coverage records. Fan-out entries may omit targets too, but do not yet record
coverage findings; their failures stay ordinary piece failures. A coverage skip
is not a hold. Explicit targets always bind and never get a coverage record.

Production entries keep explicit targets. A production entry, or a `--class
production` override, without targets is refused before submission. The queue
refuses it too when a priority policy promotes the job to the production tier.
This keeps MY-19 output parity in force before production use.

Declare targets only when your software has a hard requirement, or for
production. Otherwise let Mycelium choose the host. Add variants and lock coverage for other feasible platforms as the live
portability report directs. Host/device selectors require a specific hardware,
approved production parity or qualification exception; document it with
`requirement_reasons`. See the [placement contract](scheduling.md#declare-requirements-let-mycelium-place).
Source defaults to committed
Git `HEAD`, class to `experiment`, and resumability to false. The authored
adapter is frozen separately, so its uncommitted edits are allowed; other selected
Git source must be clean. An unrelated entry edit does not change selected intent
or source identity. Directory/snapshot sources retain the existing safety rules.
Relative source and input paths resolve from the invocation root; invoke the
conventional file from its project root.

`run` and composed `execution` are alternatives. Existing resource, allowlist,
I/O, diagnostic and fan-out contracts still apply. `checks.result` runs after the
main command on each attempt or piece, before accepted output publication.
Omitting it requires command success and required-output verification. An argv is
never evaluated as shell text unless the author explicitly chooses a shell.

A production entry may declare `class: production` and `start_window: 600s`.
This is the same [relative job start window](jobs.md#production-start-deadline):
positive Go duration, at most `1h`, default `150s`, inherited by executing fan-out
children from their assignment (dependent jobs start their budget at readiness).
An explicit absolute `start_by` takes precedence. Keep the policy on the entry,
alongside class and fan-out, rather than in hardware variants or extension recipes.
Experiment entries omit it and retain no start deadline.

On supporting queue, worker and CLI releases, an entry can explicitly set
`fan_out: {share_xpu: true, max_concurrency: 2, pieces: [...]}` to request
measured sharing among opted-in single-card discrete-XPU executions. Omission
or `false` preserves existing fan-out admission and submission bytes. Every
co-tenant must opt in; tier, qualification, fresh card/host/attempt headroom,
growth guard and exact release still bind. This is an execution policy on the
job, not a new extension digest or an increased allowance. First compare the
same frozen workload and extension digest on the same release at experiment
class with the option off/on. Production opt-in requires owner-accepted
measured benefit and unchanged parity.

A native fan-out entry may depend on an existing fan-out parent in the same
project: set `after: {job: PARENT_ID, outputs: {first/input: first/result}}`
alongside `fan_out` and `workload`, omitting `after.piece`. Each mapping names a
local input and a gathered `KEY/OUTPUT_NAME`. The queue waits for the whole
parent to succeed, binds its accepted outputs and producing attempts, and
cancels unfinished downstream work transitively when the upstream is cancelled.
The existing durable `--key`, sidecar, qualification and start-window contracts
apply. Piece-selected fan-out consumers and dependent result checks remain
unsupported. See the [job dependency contract](jobs.md).

Native profiles and adapter variants can declare executable prerequisites with
`needs.tools: [python3.12, node, npm, npx, git]`. Names are PATH executables,
not packages or version constraints; use a locked environment for exact versions.
Resolve/placement report `tools_missing` with missing names and the worker.
A cold environment may prepare first; its inventory is checked before dispatch.
The launch check uses the effective PATH, including the prepared environment and
`run.env` overrides. Worker inventories are captured at startup and when a new
immutable environment becomes ready; launch rechecks catch subsequent removals.
This declaration covers the main native command, not model runtimes.

An entry's environment is installed tools (`environment: {}`), a named catalog
environment, or an inline lock such as
`environment: {lock_file: uv.lock, python: '3.13'}` or
`environment: {backend: pixi, lock_file: pixi.lock}`. A relative `lock_file` is
read from the project root and `myce run` uploads it with its companion
(`pyproject.toml` for `uv.lock`, `pixi.toml` for `pixi.lock`, `package.json` for
`package-lock.json`), so the queue needs
no access to it. An absolute `lock_file` is read by the queue at that path. Either
way the queue freezes the bytes and their digest into the job
([project lock files](environments.md#project-lock-files)). Existing native
composed environment maps are supported; single `environment` and
`environments` cannot coexist.

Native entries use complete `variants`, each with one exact `target`, `needs`,
`execution`, optional existing `environments`, and `checks.qualification` plus
native baselines. They cannot mix flat recipe fields with variants. Keep their
small description/source closure within the host's measured source memory allowance;
large immutable assets use file references. Preparation triggers exact-host/device
checks from worker-owned scratch. Useful work waits for an accepted baseline
comparison; a warm repeat reuses matching evidence. Failure remains visible and
unqualified until an explicit qualification retry or a changed identity. Result
checks never replace native qualification.

Registration returns non-blocking live portability warnings: missing recipes,
declared-but-unqualified targets and unknown inventory are separate. Re-run the
live report after requirements or inventory change. Status and job GET expose
`variant_eligibility` per host/device and conditional `variant_advice` for scarce
capacity; [status](status.md#variant-eligibility-499) explains the fields.
New multi-variant registrations require the candidate's kind of machine
(OS/arch/platform) among the project's own approved kinds for that work before
production placement (MY-51). First compare outputs with the reference within
the project's accuracy criteria (MY-19) on a machine of that kind, then
`myce parity add ... --evidence JOB_ID`; native baselines alone are
insufficient. Checks/experiments and historical registrations
retain their contracts. Per-metric parity design remains #145.

After staging local data and qualifying approved alternatives, remove convenience
host/device pins from future authored submissions and resolve again. Genuine
hardware/validation restrictions keep their `requirement_reasons`. Existing
accepted work is not cancelled or rewritten to move it.

### Run, wait and fetch in one command

```sh
myce run --entry analyze --wait --out accepted-results
myce run --out accepted-results --output report.json -- python report.py
```

`--wait` submits through the same receipt path, then waits, prints state
changes to stderr and streams command logs to stdout, downloads the accepted
outputs into `--out DIR` and verifies each file's SHA-256 and size against the
recorded `output_refs`. `--out` implies `--wait`; without `--out` nothing is
downloaded. The entry's `checks` still run inside the job. `--wait`/`--out` also
accept `--profile NAME`, `--key`, `--input`, `--class` and `--deadline`, and
refuse `--detach`. Rerunning the same command with the same intent adopts the
existing job (and fetches its outputs again) rather than submitting twice.

For a one-off command, `--out DIR` needs one or more `--output NAME`: the
relative files the command writes in its working folder. Mycelium attaches a
small stored `.myce-outputs` manifest listing them, so the outputs return
through the verified file store.

Exit codes with `--wait` or `--out` match the Python client's `run_entry`:

| Code | Meaning |
| --- | --- |
| `0` | The job succeeded and every output was verified into `DIR` |
| the command's own | The job failed with the command's exit code (`1`–`255`) |
| `1` | The job failed or was cancelled without a command exit code (lost machine, signal, cancelled elsewhere) |
| `130` | Interrupted (Ctrl-C or SIGTERM); cancellation confirmed, job will not run |
| `64`, `65`, `69`, `74`, `75`, `78` | Submission refused, with the typed codes in [project profiles](#project-profiles); `64` also covers flag misuse |
| `125` | The job was accepted but `myce run` could not finish the trip: waiting or transport failure, output download/verification failure, an unconfirmed cancellation, or parent exit |

The stderr reason names the cause. On Ctrl-C or SIGTERM the run cancels by project and
submission key, then repeats the idempotent cancel for up to 10 seconds until
a running process's release is confirmed and says so (`release confirmed`, or
`release still pending`). An unconfirmed cancellation exits `125` and prints the
project and key to cancel through the cancellation API. Submission refusals
print the retained receipt to stderr; stdout carries only command logs. If the
parent process exits, the job continues and the run exits `125`. Without these
flags, the receipt output and exit codes below are unchanged.

### Separate steps

Adapter submission retains the existing profile receipt/observer recovery contract:

```sh
myce run --entry analyze
myce wait JOB_ID
myce result JOB_ID --out accepted-results
```

Submission returns a receipt; foreground `myce wait JOB_ID` is sufficient for
adapter and profile completion, with no notifier or harness binding required.
Use [completion notification](#completion-notification) only when detached delivery
is needed. `myce result` verifies accepted artifact SHA-256 and size; then apply
the project's own acceptance check. Queue outages never execute locally. Keep private receipts while recovery
matters, and use a new explicit `--key` for an intentional repeat.

## Initialize an adapter

From the project's root:

```sh
myce init
myce init -- python scripts/analyze.py
myce run --entry test
```

`myce init` writes `.mycelium/adapter.yaml` with one entry named `test`
(`--entry NAME` picks another name). It finds the project's lock file and uses it
as the entry's environment:

- `pixi.lock` with `pixi.toml` becomes `environment: {backend: pixi, lock_file: pixi.lock}`.
- `uv.lock` with `pyproject.toml` becomes `environment: {lock_file: uv.lock, python: "3.X"}`.
  The Python version comes from `.python-version`, then from the `requires-python`
  lower bound; `--python 3.X` sets it explicitly.
- With both locks present, choose one with `--lock pixi.lock` or `--lock uv.lock`.
- Without a lock, pass the command after `--`; it runs with the worker's installed tools.

The default command is `python -m pytest`; arguments after `--` replace it. The
entry has no wrapper, no result marker, no target list and no needs; the defaults
above apply. An existing adapter is never overwritten. Initialization validates
the entry and never submits it. Commit the source the entry should run, and
ignore `.mycelium/receipts/`.

`myce profile init` still writes the older profile starter for existing callers.

## Initialize a profile

From the project's root:

```sh
myce profile init --name test
myce profile init --name render --environment render-tools -- python render.py
```

Initialization inspects `go.mod`, `uv.lock`/`pyproject.toml`, or Python
requirements/test configuration. Go defaults to `go test ./...`. Python uses
`uv run` and pytest, honoring the project's Python version and dependency files;
an existing uv lock uses `--locked`. A requirements project installs pytest and
`requirements.txt` in a uv-managed environment. Without a lock, dependency
resolution can change: lock the project before relying on reproducible runs.
An explicit argv after `--` selects a script or arbitrary command instead.
Supply the managed catalog environment containing its required tools.

The default environment is the existing
[`mycelium-gate`](../env/mycelium-gate/README.md) catalog recipe (Go, Python, uv,
Git). It must be registered on the queue; use `--environment NAME` for another
registered recipe. Initialization neither registers recipes nor installs host
tools. In `myce dev`, pass a catalog with `--environment-config`; the same
registration YAML works with a local path to the recipe's lock.

The command writes `.mycelium/profiles/NAME.yaml`, validates its job fields,
and prints `myce run --profile NAME`. It does not submit or run the suite.
Existing profiles are preserved. The starter uses one CPU core, automatic memory,
and the current OS/architecture as a conservative target, with no host pin or
host-specific path. `--target OS/ARCH` chooses a different starter target; add
alternatives only after verifying the toolchain and project there.

Review the profile, ignore `.mycelium/receipts/`, and commit the intended source
before submitting. `--source-ref COMMIT` explicitly tests an existing committed
tree without including uncommitted starter files; the default `HEAD` requires a
clean tree. Source transport retains its normal link and special-file checks.
The generated wrapper checks its tool, forwards appended arguments, and writes
a result marker only after the command succeeds. The profile's `check` verifies
that marker before accepting outputs. A green starter run proves the selected
command; broaden the suite and its output checks for the project's needs.

## Project profiles

`myce run --profile gate-full` reads `.mycelium/profiles/gate-full.yaml`, submits
and prints a JSON receipt. A profile needs only `project` and
`run.command`; `check`, `class`, `needs` and (outside production) `targets` take
the [adapter defaults](#project-instruction-file). Use `--profile path.yaml` for an explicit file.
Use `myce run --profile go-test -- -race -run '^TestX$' ./internal/myce` for ad-hoc Go tests with the catalog-backed Go/Python/uv/git environment.
Arguments after `--` append literally to `run.command`. The new flag belongs on
`run`: `submit job.yaml` keeps its existing job-ID output and wire contract.

A profile is ordinary batch YAML plus `source` and `check` (an argv array). Reuse
`run`, `needs`, `environment`, `inputs`, `outputs`, `project`, and `class` from
[job reference](jobs.md). Declare exact `targets` with `os`, `arch`, and
`platform` (the existing extension target grammar). Plain command profiles use
an isolated working folder; extension/sidecar profiles are outside this
MVP. `environment` selects an existing immutable catalog recipe; `{}` explicitly
uses installed tools. The gate profile selects the locked `mycelium-gate` catalog
environment and checks the toolchain before running. It declares two CPU cores,
matches Go parallelism to that declaration, and declares 6 GiB for Linux based
on total cgroup peaks plus measured variation. Darwin uses `memory: auto` with
accepted comparable evidence; see [test-gate sizing](development.md).
Ordinary launch slack is additional.
Unknown configurations can still explore; check job GET `resources` and
`memory_envelope` before dropping a measured declaration. Profiles opt into
automatic memory with:

```yaml
needs:
  cpu_cores: 2
  memory: auto
  platform: cpu
```

Without a prior peak or submitter hint, the first run explores using available
host headroom. A successful solo run with an unconstrained host-memory peak
teaches later comparable jobs. The same host uses the accepted peak with 25%
headroom over the larger resident/total cgroup peak; estimates from another host
use the existing larger margins. An OOM or constrained auto attempt clears
comparable memory peaks. Any `memory.events` `max > 0` or `high > 0` marks the
peak constrained, including capped members and file-backed pressure with low
resident-only use. Older-worker results and stored pressure samples are excluded
as well. File-heavy jobs may reserve more memory or explore again. To get a
measured-need run after hitting the ceiling, submit the same comparable work with
`memory: auto` and no submitter hint: the next run explores from twice the
constrained peak (host-wide once that reaches half the allowance, #559),
labelled `exploratory (previous peak constrained)` when a constrained peak
invalidated the measurement. A successful unconstrained exploration re-learns the peak. Ceiling pressure
alone does not retry or change the job's outcome. Ceiling-bound explicit-memory
peaks cannot seed auto estimates; explicit declarations otherwise keep their
existing behavior. Optional member caps still apply, so remove a binding member
cap if the job should measure its need within the shared job envelope.

Memory comparability uses the project, profile YAML `name`, and a digest of the
base command/configuration, the check,
environment and common profile input bytes. Per-submission `--input` files and
arguments appended after `--` are excluded by default. Use `memory_class: per_item`
in the profile to include both when items differ materially. The frozen source archive digest is excluded,
so a new commit of the same suite can reuse its peak. Changing the command or
base configuration starts a separate memory class. The same record keeps the
kind of work's last accepted duration (MY-49). Plain commands outside the profile
submission contract still require hints or explore on each run.

Choose exactly one source:

- `source: {git: HEAD}`: archive the committed tree. A tree hash or commit hash
  also works. The dirty-worktree check applies only to literal `HEAD`, excluding
  Mycelium's own receipts. Other Git references archive their resolved committed
  tree without checking worktree changes. Explicit `allow_dirty: true` acknowledges
  using only committed bytes; dirty/untracked changes and the Git directory are excluded.
- `source: {git: HEAD, paths: [scripts/, pyproject.toml]}`: archive only these
  literal committed files/directories, retaining repository-relative names and
  executable modes. The identity covers selected names, modes and raw Git blobs;
  Git archive attributes do not transform them. Each selection must exist and
  contain regular files. The dirty HEAD check is scoped to these paths. An
  unrelated commit leaves both default keys and the queue spec unchanged, so a
  fresh checkout adopts prior jobs. An empty list, links, submodules, escaping
  paths and pathspec magic are refused. `paths` requires a Git source.
- `source: {snapshot: source.tar}`: an explicit uncompressed tar archive.
- `source: {directory: prepared-source}`: explicitly upload regular files in that
  directory. `.git` and `.mycelium/receipts` are excluded. Prepare this directory
  deliberately: other ignored files are included, and no secret scan is implied.

Source paths and `inputs` values resolve from the project root (the parent of
`.mycelium` for conventional profiles, otherwise the current directory). Git and
snapshot sources keep relative symlinks that resolve inside the source; links that
leave it, absolute links, link loops and entries beneath a link are refused.
Directory sources reject symlinks. All sources reject hardlinks, special files and
escaping names. File modes survive the source archive. Inputs map clean destination filenames to explicit local
files; they are frozen and applied after source extraction. Outputs map required
relative files, folders (`plots/`) or globs (`"logs/*.txt"`) to ordinary result
prefixes; `--out DIR` and `myce result recover` recreate the subfolders. `after`
stages, `output_destination`, `storage_protocol` and extension jobs still name
individual files ([folder and glob outputs](files.md#folder-and-glob-outputs-1341)). Command exit, then `check` exit,
then existing attempt-specific output verification must all succeed. Retrieve
accepted outputs through `Client.DownloadResult` / Python `download_result`;
these verify their SHA-256 and size.

For independent items, commit one profile and add repeatable `--input DEST=PATH`
flags to each submission. Relative local paths resolve from the project root,
just like common profile inputs. Destinations cannot duplicate or overlap another
input or use Mycelium's reserved staging names. Both common and item files are
frozen; job keys include their content identities and appended arguments.
Identical bytes at another local path retain the same identity. Common source
and input blobs upload once and are reused through the queue file store.

```yaml
# .mycelium/profiles/analyze.yaml
name: analyze
project: example
class: experiment
source: {git: HEAD, paths: [scripts/]}
environment: {}
run: {command: [sh, scripts/analyze.sh]}
inputs: {reference.json: data/reference.json}
needs: {platform: cpu, cpu_cores: 1, memory: auto}
targets: [{os: darwin, arch: arm64, platform: cpu}]
outputs: {result.json: results}
check: [test, -s, result.json]
```

The script reads `item` and `reference.json`, takes any appended arguments and
writes `result.json`. Set targets to the OS/architecture the script supports.

```sh
for item in data/items/*; do
  myce run --profile analyze --input "item=$item" -- --mode scan
done
```

Each receipt identifies one job. Replay unchanged items to adopt their earlier
jobs; changing an item or selected source bytes creates new jobs. The first
successful unconstrained auto run teaches later items in the profile's memory
class. With `memory_class: per_item`, each distinct item/argument set learns
separately. Job and artifact retention still apply.

Profiles upload their frozen source archive and explicit inputs to the queue's
local file store in either checkpoint mode. A NAS fleet keeps its existing
`--checkpoints` / worker `--checkpoint-root` configuration; input downloads and
verified result uploads use the queue API without changing NAS checkpoint paths.

Profile source/input and result blobs expire **3 days after the job becomes
terminal**, configurable on the queue with `--file-retention` (default `72h`).
Download results within that window. Job/key answers and hashes remain, with
`files_expired: true`; a keyed resubmit adopts the same job and never reruns it.
`Client.DownloadResult` / Python `download_result` fail with typed HTTP 410
`result_expired` after expiry. Replaying `myce run --profile` refreshes the job and
reports `result_expired` with exit 65, including for a receipt whose notification
was already delivered. Open downloads finish, but a disconnected caller gets no
extra grace beyond the window. Checkpoints and catalog storage remain protected.
Abandoned profile job-input uploads are reclaimed only when positively marked in
queue SQLite state, unreferenced by every job/keyed answer, checkpoint, catalog/model,
extension, environment and pending reclamation list, and older than the configured
window by both publish time and file mtime, outside the six-hour publication grace.
Job admission clears that marker; normal terminal retention then applies. Unmarked
imports (`myce file put`, models, Go `UploadFile`, older clients) are never orphan-swept,
including when the model catalog is empty or edited. Reusing an unmarked import as
a job input does not mark it; a generic import of marked bytes clears its marker.
Re-uploaded or HEAD-verified bytes receive a six-hour publication grace, covering
later multi-GB uploads, slow NAS, or a queue outage/deploy between the first file
and submit/complete establishing its reference. This does not restore an
expired job's result-recovery window. Reusable worker caches can evict expired
copies, so expiry does not block future profile input downloads. Marked interrupted
uploads are reclaimed after an idle retention window and publication grace;
POST/HEAD/PATCH refresh their idle time. Unmarked imports remain protected.

Before uploading, a profile preflights its complete input manifest against the
queue file-store allowance and disk floor (1%, minimum 256 MiB). Upload creation
rechecks capacity; preflight makes no reservation. HTTP 507 `file_store_capacity`
names the store and needed/free/shortfall bytes with a recovery action. The saved
receipt retains this typed refusal and the CLI exits 75 for retry. `myce status`
reports queue file-store used, free and reclaimable bytes. See
[operations](reference/file-store.md#stored-job-file-retention).

Before any upload or submission, `.mycelium/receipts/` contains a synced, private
receipt with frozen source/input hashes, argv and submission key. Gitignore this
folder in each consuming project. The default key derives from profile, args and
source/input identities; identical work adopts its previous job. Git keys use
the resolved tree (or selected paths when scoped), while directory and extra inputs are hashed in place. Use
`--key KEY` to name an operation explicitly. Reusing it adopts the frozen intent
even if local source changes; changed profile/args fail with a conflict. Use a new key for an
intentional repeat. Keep receipts while recovery matters. Upload copies are
retained only until a job ID is durable, then removed because the verified queue
file store owns the bytes. Retries hash in place and copy only a new intent. Stale unpublished
`.prepare-*` folders are swept under the submission lock.
Do not put credentials in a profile, command, or `run.env`.

Queue submission atomically checks a known qualified target's total admitted
capacity, target, environment and placement policy. Occupancy, owner activity,
cooldown, maintenance, leaving/recovery and allowance acknowledgement do not
turn temporary holds into qualification failures; they queue work. Dispatch
and launch continue to enforce live admission. No host is chosen by the caller,
and there is no local fallback. Replay of an accepted key works after targets
disappear. Exit codes are `64` invalid profile/source, `65` intent conflict or
queue rejection, `69` queue/upload/authority unavailable, `74` observer/notifier
arming failed, `75` file-store capacity refusal or another local submission is
preparing, and `78` no qualified target. Failures after intent persistence print the receipt as well as a typed
stderr reason. An uncertain submit retains the key for safe recovery.

`no_qualified_target` names the closest eligible machine and its first failed
fit check, including exact resource needs and allowances (host RAM includes
admission slack). The HTTP error and CLI stderr carry that detail; the printed
and saved profile receipt retain it as `refusal_code` and `refusal_reason`.
Eligible targets rank ahead of placement mismatches, followed by the fewest
failed checks, smallest proportional resource shortfall, then machine/device
name. Retrying an accepted intent clears the saved refusal.

## Failed-attempt diagnostics

Profiles can opt into failure evidence independently of their successful outputs:

```yaml
diagnostics:
  reports:
    - {path: junit.xml, format: pytest-junit}
# For Vitest: format: vitest-junit, generated with --reporter=junit --outputFile=junit.xml
```

Use `diagnostics: {}` for startup/lint/build failures with no test report. This
contract supports plain commands with stored inputs (including profile source
archives). A failed terminal attempt publishes `summary.json`, `log-tail.txt`
and available raw reports as `reports/0.xml`, etc. The summary includes job ID,
attempt, machine/session execution identity, command exit/cause, failed test
identifiers and report errors. Missing or malformed reports keep the original
failure and tail; startup errors never acquire invented test names. Adapter
parsing happens in the worker, not the queue.

For exact pytest node IDs, run
`python -m pytest --junitxml=junit.xml -o junit_family=xunit1`. The adapter combines JUnit's file, classname and test name,
including class names and parameter IDs. With xunit2, record an explicit
`nodeid` property through pytest's `record_property("nodeid", request.node.nodeid)`
fixture (for example in an autouse fixture). Reports without sufficient identity
remain downloadable, with a summary error instead of a guessed node ID. Vitest
preserves the JUnit testcase `name` verbatim as the ID and retains `classname`
and `file` separately for disambiguation.

```sh
myce result JOB --attempt 1 --out failure-evidence
```

Select a recorded failed attempt explicitly; the default `myce result JOB --out
DIR` still requires accepted success outputs and returns `result_not_accepted`
for failures. `Client.DownloadDiagnostic(ctx, jobID, attempt, name, dest)` uses
queue-recorded references, SHA-256/size verification and atomic file replacement.
Diagnostic references live in `attempt_history[].diagnostics.files`; they never
populate `output_refs`, satisfy a dependency, or change pass/fail acceptance.
Stale worker completions cannot attach them to a newer execution.

The tail keeps the last 64 KiB (empty when `logs: none`). Up to eight reports,
each at most 1 MiB, bound parsing/transfer after a failure; unavailable, larger,
or invalid reports are noted in the summary. The summary is at most 1 MiB; if
structured identifiers exceed that limit, use the retained raw reports. These
limits keep failure handling small for targeted runs. Existing authenticated
file transport and terminal retention apply: **3 days after terminal completion**
by default, configurable with `--file-retention`. Expiry returns `result_expired`
(HTTP 410), while job/attempt metadata and hashes remain. Diagnostic contents
include raw job logs and reports; treat them like other private job artifacts.

## Installed client commands

On an enrolled machine these commands read the installed worker config, or the
installed queue config when no worker config exists. They retain the joining
certificate pin; consumers do not read config files or handle credentials.
Explicit `--queue` / `--token` and `MYCE_QUEUE` / `MYCE_TOKEN` keep working.
`--config` and `--joining-file` remain available for explicit authority selection.
As with `run`, `MYCE_QUEUE` overrides the invitation address for `status`, `logs`, `cancel`
and `file get`, including when `--joining-file` is explicit.
Extension `add`, `check`, `list` and `setup` accept the same authority flags;
`add` and non-static `check` resolve installed authority when contacting the queue.
Local listing, setup, `add --local` and `check --static` need no credentials.
Operator host maintenance (`myce host drain|disable|resume|status|budget|placement`, see
[operations](operations.md#queue-maintenance)) uses the same installed authority.

```sh
myce status
myce wait JOB_ID
myce logs JOB_ID
myce logs JOB_ID --follow
myce cancel JOB_ID
myce cancel --project PROJECT --key KEY --wait  # exit 0 only once release is confirmed
myce result JOB_ID --out ./accepted-results
# For an explicitly supplied file identity (not accepted-result resolution):
myce file get --size-bytes BYTE_COUNT SHA256 ./new-destination
```

`myce result` resolves the succeeded job's accepted output manifest and calls
`DownloadResult` for every artifact. It verifies SHA-256 and byte count before
publishing each local file. Failed/running/cancelled jobs are refused with
`result_not_accepted`. Server-labelled errors, such as HTTP 401 authentication
failures or an unknown job, retain the server's code; HTTP 410 reports
`result_expired`. Only transport or verification failures, including hash/size
mismatches, report `result_download_failed`. Downloads of any size continue
while bytes flow; a socket operation exceeding the idle budget (at least 60
seconds, or 30 seconds plus two full-blob hash passes at 100 MiB/s) fails with
`result_download_failed: file transfer stalled`. All use exit code 1, with the
typed code first on stderr and no timestamp.
Existing destination files are replaced only after that artifact verifies.
Result directories may contain earlier verified artifacts if a later one fails.

```python
from mycelium import Client

client = Client.from_installed()
job = client.wait("JOB_ID")
client.download_result(job["id"], "result.json", "./accepted-results/result.json")
```

The Python library reads installed authority itself and retains the certificate
pin. `Client.from_installed()` requires **OpenSSL (3 recommended)** for
the queue's Ed25519 joining certificate. macOS `/usr/bin/python3` from Xcode
Command Line Tools uses LibreSSL and cannot complete that handshake; use
Homebrew or python.org Python. The client raises `UnsupportedTLSRuntimeError`
on LibreSSL before reading installed authority or attempting a connection;
macOS system LibreSSL is unsupported.
`Client(url=..., token=...)` keeps its existing explicit-credential behavior.
Neither flow adds credentials to job environments; jobs never inherit `MYCE_TOKEN`.

## CPU profile environments

Select a queue-registered locked recipe with `environment: cpu-tools`; use
[CPU tools with Pixi](environments.md#cpu-tools-with-pixi) to author/register it.
Catalog reload is a queue restart, not a consumer API. Preserve existing catalog
entries and check [preinstalled compatibility](environments.md#preinstalled-environment-compatibility).
Workers acquire the pinned, checksum-verified backend when absent and need
network during preparation; the backend executable is not added to payload PATH.
Declare tools such as uv in the environment itself.

Retain profile source, targets, outputs and result check. CPU jobs use
`needs.platform: cpu` or CPU cores with no platform selector. `platform: any` or
`preferred: [cpu]` does not select CPU; disjoint job/environment platforms fail
with `job_environment_platform_mismatch`. GPU hosts serving CPU work use the CPU
recipe, with its `bin` first on PATH. Recipes never substitute CPU/GPU implicitly.

## Completion notification

Set `MYCE_NOTIFY` or pass `--notify /path/to/executable`. This is one local
executable, not shell text. It must:

1. Exit nonzero from `--check` when its receiver/configuration is missing.
2. On ordinary invocation, accept JSON on stdin and exit zero only after delivery.
3. Own its delivery timeout/retry policy; keep credentials in private configuration.


One detached `myce wait --receipt ...` observes each job. A startup handshake and
an inherited OS lock establish observation; rerunning adopts the lock holder.
No PID guessing is used. The receipt reports `observation: armed|not_armed` and
`delivery: armed|not_armed|not_configured|adopted_other_notifier`. Both become
non-armed when observation exits; a configured delivery then reads `not_armed`.
Missing/invalid hooks never
report delivery armed. `delivery: armed` means configured, not confirmed
delivered; the separate `delivery_status` becomes `delivered` or `failed`. A receipt
file is a past snapshot, not proof of a live watcher; rerun with `--key` to check
the OS lock and adopt or recover observation. Completion records
terminal `status` and immutable `result_reference`; observer errors instead record
`observation_error: timeout|transport_error|interrupted|tls_identity`, leaving job
state alone and attempting a notifier event with `status: observation_failed` and
`observation_error`, even on interruption.
Transient queue outages retry inside the observer. Permanent TLS identity or
certificate verification failures end observation and notify `tls_identity`.
Observation has no time limit while the queue answers, so a long job on a live
queue keeps its observer. If the queue gives no answer at all (no reply, or a
bare proxy 5xx) for one hour of the submitter's awake time, the observer ends
that observation with `transport_error` and notifies `observation_failed`; the
job continues. An answer of any kind, an overload refusal included, resets the
hour, which is well beyond queue restarts, upgrades and host reboots. A
receipt folder or observer registry that is deleted retires its watch at once,
and the observer exits when nothing is left. Rerun after observer loss or delivery
failure to recover the same job
and retry the same event delivery identity. IDs include a per-receipt nonce, so
recreating a receipt cannot suppress a new event at a deduplicating receiver.
Observation-failure IDs also include a nonce saved on each arm, so a later arm
failing the same way produces a new wake. Terminal IDs remain stable across arms.

A rerun adopts an already-running observer and prints its receipt directory;
it never replaces or stops that owner. If another thread in a shared checkout
submitted the same work, the live observer may use a different notifier.
When the caller's effective notifier differs from that observer's saved binding,
the returned receipt reports `delivery: adopted_other_notifier`, with no notifier
path or credentials. An omitted notifier inherits the saved binding. Observation
remains armed and the command exits 0: the job and observer are healthy, but this
caller must wait in the foreground with `myce wait JOB_ID` or record a hold,
rather than expect its own wake. The owner's saved receipt and notifier are
unchanged. Replaying while that owner is alive does not change the binding.

For an observer started by a build before
this fix that is stuck queued/armed because its installed certificate pin was
lost, the owning session finds its
own `myce wait --receipt RECEIPT_DIR` process in `ps` by that exact receipt path
and stops it. SIGTERM or Ctrl-C first sends an `observation_failed` wake with
`observation_error: interrupted`; the terminal wake follows after rearm. Both
wakes are expected. Then rerun the original profile with the same `--key`. Once
the old owner releases the OS lock, the rerun re-arms observation using the fixed
installed authority, retains the job ID and retries notification. An exited
observer needs only the same-key rerun. No new submission key is needed.

Completed successful terminal delivery is not sent again. Receipt snapshots
cannot promise survival of a later observer-machine failure; this MVP has no
queue outbox or automatic boot restart.

Notifications contain only `delivery_id`, `job_id`, `key`, terminal `status`
(or `observation_failed` plus `observation_error`), and `result_reference` (output
names, hashes and sizes). Choose non-sensitive output names. The notifier never
receives the fleet token. It and its private receiver configuration stay on the
submitting machine, outside job argv and worker
payloads. Changing a hook while a watcher is alive does not replace that watcher;
finish or recover observation before changing the binding.

## Dogfood

The checked-in `gate-full` profile runs the real full gate from a committed
source snapshot with Go 1.25.6 or newer, Python 3.11, Git and uv from the
[`mycelium-gate` CPU environment](../env/mycelium-gate/README.md). Bash is supplied
by the worker host. The gate's HOME/XDG isolation hides uv-managed interpreters (#169). Before isolation,
`scripts/gate.sh` resolves installed Python 3.11 with `uv python find --offline
--no-python-downloads 3.11` and exposes only a `python3.11` symlink in a private
scratch bin directory on PATH. This covers both profile and local gate runs
without letting the interpreter's installation directory shadow other tools.
Missing Python fails during gate setup; the catalog environment supplies it for
the profile. Local gate runs still need an installed Python 3.11. Dependency
preparation happens before the gate goes offline. On Linux, Darwin CGO release
builds explicitly skip and require separate native macOS verification; Linux
builds, vet and the full race/dev suite still run. This is laptop proof, not
native fleet qualification.

For local acceptance, build `myce`, start `myce dev --gpu cpu --workers 1
--file-store DEV_FILES --db DEV_DB` on loopback, then submit the profile with the
dev queue/token in environment and a local test notifier. Commit the intended
source first. Wait once using the armed observer, inspect its terminal receipt,
and verify `gate-result.txt` through the accepted result API. Do not launch an
additional full suite while two are already active.
Add `--checkpoints DEV_CHECKPOINTS` to exercise NAS checkpoint semantics with a
local test folder while keeping the same stored source/input/result transport.

A submit reply and the saved/printed profile receipt expose
`submission_reused: true` when the queue returned an existing project/key binding.
The receipt preserves that submission fact on local replay; it is not a claim
that a new attempt ran. An older receipt without the field has no recorded
adoption diagnostic. A cancelled-before-submit binding can also be reused.

A new worktree or receipt root can adopt an already-terminal job with the same
project/key and intent. Its new receipt has a new nonce, so observing that job
can deliver terminal status again with a different `delivery_id`. Delivery IDs
are stable within a preserved receipt, not globally across receipt roots.
Receivers should also recognize a job/status/result already processed across
roots. Keep caller receipts for recovery; use a fresh explicit `--key` when a
new execution is required (for example a release canary).

## Queue health notification

Queue operators can start `myce queue --health-notify /path/to/executable`
(or set `MYCE_HEALTH_NOTIFY`). This is separate from the submitting agent's
`MYCE_NOTIFY` / `run --notify` binding. The executable uses the same `--check`,
JSON stdin and successful-delivery exit contract described above. Point its private
configuration at the operator's own wake/notification receiver; no consumer polling
dependency is added. The queue refuses startup when a configured hook fails
its check. Queue service operators must preserve these flags/environment in their
service configuration; this slice does not install a receiver or alter fleet services.

`--health-storage-delay 5m` controls continuous storage unavailability before an
alert (positive duration). The hook receives openings and closings of
host-level problems only; its input contract and kind list are in
[status](status.md#health-notifier-hook-contract). Read
`health_alerts` in authenticated status for one record per open or recently
closed problem of every kind and its delivery acknowledgements. Keep host names non-sensitive. Events omit raw errors and paths,
and the hook environment excludes `MYCE_TOKEN`. Hook output is not logged.

Condition alerts deduplicate continuously unhealthy episodes into one record per
problem. Transport is paced
to at most one hook per second, retries failures after one minute and retains the
same delivery identity across restart/ambiguous delivery; receivers must deduplicate
that identity. The 30-second hook budget also applies to `--check`. Keep hook
credentials in private local configuration, with delivery through a quiet channel.

This notification feature does not change retry or release policy.


## Write an extension

Inspect the application's existing command/HTTP boundary, locks, model identity,
input/output filenames, sidecars and measured resources. Reuse its algorithms and
launchers. Author a complete recipe per target with no inheritance or implicit
fallback. `myce extension init` creates a skeleton; `--gpu fake` is laptop
simulation only. Real CPU enrollment/qualification is supported with `--gpu cpu`;
GPU qualification must run the selected backend and cannot fall back to CPU.
Current native execution is Unix; native Windows needs its own runtime port.
Use logical `{extension}`, `{work}`, prepared `{env.NAME.python}`/`{env.NAME.root}`
and verified `{assets.NAME}` paths; large model weights do not belong in the
memory-admitted description bundle. A smoke check alone never supplies native qualification.

Jobs declare software requirements; Mycelium selects the host and device.
Author every feasible platform variant with its own locks and native checks.
Reserve host/device pins for specific hardware or qualification; production
approval names kinds of machine and needs no pin; see the [placement contract](scheduling.md#declare-requirements-let-mycelium-place).

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
contents fit the host's measured memory allowance; see
[request budgets](request-budgets.md). Execute scripts through their interpreter; file
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
than `check failed`. It grants no qualification. A check that never started
(window expired, or cancelled by Ctrl-C or the caller's timeout while queued)
has no verdict, so a device's earlier report for that work, qualified or not,
is restored unchanged; only a check that started and failed revokes it. While a
check is waiting or running, the device's report reads `check required`. An
execution timeout remains a failed check. Both limits are enforced by the queue, including after the CLI
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
`myce extension check` submits `needs.memory: auto` for this variant. A check
uses earlier accepted checks of the same variant and work. A new version also starts from its nearest prior version's accepted memory on
the same platform and native identity. Checks include prior workload peaks when
both versions and the check launch the same main command, assets and environment.
Shared incidental helpers alone do not prove workload comparability. Separate
reference-only checks keep their own evidence.
Measured peak variation and sampling growth supply headroom. Accepted evidence
of the new version replaces the prior; it never grants qualification. Without
usable evidence, the first run reserves a measured physical host-memory opportunity
(its card still explores), under the existing
[memory envelope rules](design-proposal.md#amendment-job-memory-envelopes-146).
An unknown native accelerator check waits instead of
using a tiny leftover ledger reservation as a hard limit. A host cgroup OOM
retains the killed peak as a floor and grows from measured demand, sampling
growth and ceiling overshoot. Available RAM constrains admission and never sets
the recovery demand. Recovery has no fixed
exploratory retry count; device-OOM handling is separate.
Missing telemetry or headroom leaves it waiting; qualification still requires a
successful check and matching baseline output.

With an authored `references` entry, a successful solo auto check can seed
the memory envelope of later work with the same frozen variant, work type and
input shape until that work has accepted peaks of its own, which then replace
the seed. Later jobs declare `workload: {input_shape: sample-v1, units: 10}`.
Checks teach no duration, and the peak stays usable for 30 days from the check.
Failed or ceiling-constrained peaks
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
see [placement and measurement limits](scheduling.md). Multiple matching extensions
are tried in deterministic name/version order, only when qualified. A job
records the exact chosen name/version/content digest on every attempt.

On v3, [files without a NAS](files.md) supports `submit --files-from`
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

## Complete tiny example

[http-stage](../examples/extensions/http-stage/) contains the unchanged
standard-library CPU app, its HTTP service, uv requirements, one complete target,
a replay check and actual CPU reference bytes. Its main declaration is:

```yaml
main:
  environment: app
  command: ['{env.app.python}', '{extension}/app.py', input.json, result.json]
  env: {SERVICE_URL: '{sidecars.service.url}'}
```

Copy the folder. Change only `target.os` / `target.arch` to your laptop's exact
values before registration (the checked-in example is Darwin arm64). Keep
`app.py` unchanged. Start dev and perform step 5 using `http-stage@1`.

Install the client in the caller's existing virtual environment. It requires
**Python ≥3.10** and no third-party runtime dependencies (macOS system Python 3.9
is too old). Use the app's Python, for example:

```sh
.venv/bin/python -m pip install /path/to/mycelium/clients/python
# For a uv-managed venv: uv pip install --python .venv/bin/python /path/to/mycelium/clients/python
```

The integration path is one general call:

```python
from mycelium import Client
job = Client().run("http-stage@1", inputs={"input.json": "input.json"},
                   outputs="results", project="demo", class_="batch",
                   key="document-revision-1", timeout=60)
# Existing result processing reads results/result.json.
```

`inputs` maps the app's filenames to caller-local files; `outputs` is the local
destination directory. The extension's top-level `outputs` declares the result
filenames. Python spells the job class `class_` because `class` is a keyword.
Repeat the **same extension, inputs, project, class and key** after a lost response;
`submission_key` reconciles it to the original job. For a repeatable key, hash
all inputs in stable filename order plus the stage and extension version, e.g.
`sha256(stage + version + sorted input names/content hashes)`. After a version
bump, `submission_key_conflict` means the old key already names a different job
spec; include the new version in the key. Use a new key only for new business work.
A failed `run` raises Python `APIError` (`job_id`, `code`) or Go `*myce.HTTPError`
(`JobID`, `Code`), with a `myce logs SHORT_ID` hint.
Cancel even after a lost response with `client.cancel_submission("demo", key)`.

`Client()` reads `MYCE_QUEUE` (default `http://127.0.0.1:7331`) and
`MYCE_TOKEN` (default empty; required). Dev's token is `dev-token`; export it.
A `run`/wait timeout leaves the job queued or running and reports its ID and
latest status reason. Cancel explicitly with the call above if work is no longer needed.

For multiple stages, use one extension per stage. The caller downloads A's
intermediate and uploads it as B's input; keep orchestration in the caller.
Input keys map fixed extension filenames to any caller-local path. If the caller
chooses the final output path, download the fixed name then rename it, e.g.
`(Path("results") / "report.csv").replace(report_path)`. Set `init --outputs
'["report.csv"]'` to match the command; filenames cannot be inferred from arbitrary argv.

Go's client supplies the same path, bounded by a context:

```go
job, err := client.Run(ctx, "http-stage@1", map[string]string{"input.json": "input.json"},
    "results", myce.RunOptions{Project: "demo", Class: "batch", Key: "document-revision-1"})
```

Both clients fetch only successful accepted-attempt artifacts, verify hashes and
replace local result files. Lower-level `SubmitAndWait`, `DownloadResult` and
cancel-by-key remain available for custom callers.

## Baseline keys and replay

Select an extension as `NAME@VERSION` (for example `http-stage@1`). In each
variant's `baselines`, use **`extension:NAME`** for direct extension jobs:

```yaml
check: ['{env.app.python}', '{extension}/check.py']
baselines:
  extension:http-stage:
    input: input.json
    output: output.json
    measure: exact
    native: Describe how the actual native reference was recorded
    variation: Describe repeated native runs and acceptable variation
```

The bundled `input` and recorded native `output` are extension-relative files.
The check reads `MYCE_CHECK_INPUT` and writes actual output to `MYCE_CHECK_OUTPUT`;
it must replay the unchanged app, never copy the reference. `measure: numeric`
compares JSON arrays with explicit `absolute` and/or `relative` tolerances;
`exact` compares bytes. Extensions that also expose named kinds or program types
use `kind:KIND` or `program:TYPE` for those separate checks. See the
[complete variants example](../examples/extensions/http-stage/extension.yaml).

## Debug a failed job

Run `myce status`, then `myce logs SHORT_ID` (or `myce logs --follow SHORT_ID`).
Every job command accepts the unique short ID shown by status; an ambiguous
prefix requires a longer ID. The minimum prefix length is **8 characters**. Logs
contain the app's stdout/stderr, including its Python traceback. `myce wait SHORT_ID` reports the terminal state.

Code that always crashes cannot qualify; input-dependent failures in qualified code
use the job-failure path above.

For a failed runtime check, the CLI prints the actual local command-log path
under `EXTENSION_ROOT/cache/NAME/VERSION/PLATFORM/check-*.log`. Internal check jobs
are labelled in status; their logs are also available by short ID. Inspect the
traceback, fix the extension, bump its version if files changed, then add and check
again. Keep private logs local.

For a GPU app, replace the target and recipe with its real pinned dependencies:

```yaml
target: {os: linux, arch: amd64, platform: cuda}
environments:
  app: {backend: uv, python: '3.11', lock_file: gpu/requirements.txt}
needs: {memory_gb: 8, host_memory_gb: 4}
execution:
  main:
    environment: app
    command: ['{env.app.python}', '{extension}/existing_gpu_app.py']
```

Use the app's existing device setting with `MYCE_DEVICE`, or a tiny launcher if
needed; run an actual backend operation in its check and retain matching native
references. No GPU/OS claim follows from a laptop fake check.

Common failures: registering edited files under a frozen version; missing paired
locks; unsupported or incomplete target; absolute/shared input paths; filenames
that overlap or escape; checks that copy expected outputs; hidden root installs;
fixed ports; bare `python3` checks instead of `{env.NAME.python}`; accidental inherited credentials; consuming failed/stale attempts;
changing the spec under an existing submission key. Report the actual caller diff,
qualified targets and remaining prerequisites. Never weaken checks to get green.


## Convert profiles and extensions

`myce adapter convert` reads only explicitly selected sources and prints
deterministic adapter YAML to stdout. Its field and caller-change report goes to
stderr. It does not submit work, prepare environments, or change saved jobs,
keys, receipts, checkpoints, registry snapshots or qualification reports.

Review a conversion from the project root:

```sh
myce adapter convert \
  --profile .mycelium/profiles/gate-full.yaml \
  --profile .mycelium/profiles/go-test.yaml \
  --out .mycelium/adapter.new.yaml
```

`--profile PATH` and `--extension DIR` are repeatable and may be combined.
Profiles must agree on their project. Extension-only conversion requires
`--project NAME`; an explicit project must match every profile. Entry names
retain the original `name`. Conflicting names are refused, even for identical
sources. With several entries, `--default ENTRY` chooses the default; otherwise
the first sorted entry name is used. A single entry is the default, so no
`default` (or `schema`) line is written. Source-flag order does not change the
output.

`--out` alone reports a plan and writes nothing. After reviewing stdout and the
report, repeat the same invocation with `--apply`. Apply requires `--out` and an
existing parent directory, and creates the destination exclusively. Existing
files and symlinks are refused, including the input files. Originals stay in
place; conversion does not switch callers or retire old readers.

Profiles convert to the minimal entry form. `project` moves to the header,
`check` becomes `checks.result`, and fields equal to an adapter default are
omitted: `name` (the entry key), `class: experiment`, the default needs
`{cpu_cores: 1, memory: auto, platform: cpu}`, `source: {git: HEAD}` and an
absent check. Only `run.command` and `project` are required; `class`, `needs`,
`check`, `targets` and `source` may be missing (a missing source becomes git
`HEAD`, the adapter default, and the report says so).

Targets on non-production entries are dropped and named in the report: such an
entry runs wherever its needs fit and the queue learns coverage. Pass
`--keep-targets` to keep them pinned (this repository's own gate adapter does,
to keep separate darwin and linux entries). Production entries always keep
explicit targets, and a production profile without targets is refused.

Inline lock environments (`environment: {lock_file: uv.lock | pixi.lock |
requirements.txt, ...}`, with `backend: pixi` for Pixi) are kept as written;
`myce run` uploads the lock and its companion (`pyproject.toml`, `pixi.toml`)
from the project root exactly as the profile reader did. Older shapes the
profile reader still accepts, such as `priority:` instead of `class:`, scalar
`environment: NAME`, scalar `platform:` and `memory_gb:`, convert unchanged.
Machine allowlists, run argv/env, memory class, input/output filenames,
diagnostics and source selection survive. A field the adapter cannot represent
(for example `run.workdir`, sidecars or `execution`) is refused
with one message naming it, and nothing is printed or written. Check the
result with `myce spec check --resolve ADAPTER`.
Standard `.mycelium/profiles/` roots are respected; convert Git profiles from
their original project root. Other explicit profile paths already resolve from
the invocation root. Runtime `--input` arguments remain caller-owned.

Native checks become `checks.qualification`; their baselines and references
keep their existing acceptance criteria. Setup, assets, resumability,
member caps/readiness and existing inline composed environments are preserved.
Native source points to the original small extension directory relative to the
invocation root, so sources need not move and existing commands keep working.
The host's measured native source memory allowance applies. Output lists use the
existing `filename: results` convention; omitted outputs remain omitted.
Reference `sweeps` and the legacy `benchmark` field are retired (MY-49); an
extension or adapter declaring them is refused.

Direct native entries require their existing `extension:NAME` baseline on every
variant. Sources with only kind/program baselines are refused with guidance to
retain their low-level registry callers; conversion never invents a direct-work
baseline or relabels evidence from another work type.

Legacy platform-only extensions lack exact OS/architecture. Supply explicit
alternatives for every platform, for example:

```sh
myce adapter convert --extension path/to/native --project example \
  --target linux/amd64/cuda --target darwin/arm64/metal
```

Only matching legacy platforms use these flags. Composed exact targets stay
unchanged. Conversion reports all selected-source errors and missing explicit
native references; no partial adapter is emitted. It never invents targets,
references, output declarations or locks. Catalog environments stay catalog
environments; project environment and build-eligibility work (#251/#252) is
outside this converter.

Native names and version labels remain authored values, including baseline
keys. Freezing adds a content-derived internal version suffix, changing bundle
identity and requiring fresh qualification. Old qualification passes remain
historical and are never relabelled.

For the checked-in starter, invoke from this repository root:

```sh
myce adapter convert --extension examples/extensions/http-stage \
  --project adapter-example
myce run --adapter examples/adapters/http-stage.yaml --entry http-stage
```

`.mycelium/adapter.yaml` stages this project's gate-full and go-test entries
alongside the originals. Select `--entry mycelium-go-test` explicitly for a
small filtered test; the default `mycelium-gate-full` runs the full gate.

Consumer cutover and retirement of source readers require verified consumers
under [#254](https://github.com/1kuna/mycelium/issues/254); the converter does not
perform that cutover. Existing frozen identities and low-level registry callers
remain supported.

### Execution wall-time allowance

An entry or profile may declare top-level
`wall_time: 30m`, a positive Go duration measured from accepted Started for each
attempt. Omission is unlimited. Use measured runtime or a real consumer bound;
`expected_minutes` and priority `deadline` do not enforce execution duration.
Expiry fails with typed `execution/wall_time_limit`, stops the exact whole tree,
and retains cleanup/partial-output custody. Ordinary command-failure policy
applies (no automatic retry or infrastructure-loss charge). Reconnect and worker
or queue restart cannot renew the allowance. Job status exposes the limit,
expiry and remaining seconds. See [the full contract](jobs.md#per-attempt-wall-time).

### Packaged Darwin library dependencies

`spec check --resolve` and `portability --adapter` inspect executable files and
`.dylib`/`.so` library candidates in a selected directory source. Their `linkage`
warnings name the payload-relative binary and its absolute non-system dylib
install name when that path is missing on the submitter. This is local evidence,
not a claim about any remote worker. Selected Git/snapshot sources are explicitly
uninspected locally; unrelated working-tree bytes never substitute for them.

New profile submissions now use the same worker-side source extraction and explicit
input replacement as adapters, before any command or check starts. Existing saved
receipts retain their frozen submission format; use a new intent/key to adopt this
preparation path.

Darwin workers repeat this read-only check on staged payload runtime candidates,
including script-launched helpers, before starting any member. They inspect the
worker architecture's slice in universal binaries. A confirmed missing absolute
library produces `execution_cause.class: payload_library_missing`, with
`linkage.binary` and `linkage.library`, and fails the payload without charging
infrastructure retries or losses. Bundle/relink the library or choose a compatible
environment before resubmitting. The library name comes from payload bytes;
Mycelium never adds its private staging prefix to this diagnostic.

This is a packaging check, not a dynamic-loader emulator or complete compatibility
proof: Apple `/usr/lib/` and `/System/Library/` paths can live in the dyld shared
cache and are exempt; relative and `@rpath`/`@loader_path`/`@executable_path` names,
optional weak imports, and present libraries do not warn. Runtime downloads,
nested archives, undeclared plugin loads and prepared environment contents are
not enumerated. The candidate filter avoids reading data/weights and catches
runtime libraries even without executable bits; it may include an unused packaged
helper. Header/load-command reads do not read symbols or binary sections and add
no file-size cap, subprocess, network request or queue-lock I/O. Unreadable or
malformed candidates are unknown, not proof of a missing library.
