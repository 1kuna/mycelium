# Software environments

Jobs run plain commands with a local environment prepended to `PATH`. There are
no containers, GPU emulation, or automatic platform fallbacks. A platform
requirement stays strict; a preference permits other supported platforms; `any`
permits only platforms for which the environment has a recipe.

## Shared catalog

Keep the catalog and its inputs on the NAS, for example
`NAS/mycelium/environments/catalog.yaml`. Start the queue with
`--environment-config /queue/nas/mycelium/environments/catalog.yaml`. Workers
receive immutable recipes from the queue. They do not need to parse the catalog.

```yaml
environments:
  torch-cuda:
    cuda:
      python: '3.12'
      lock_file: torch-cuda/requirements.txt
  torch-xpu:
    xpu:
      python: '3.12'
      lock_file: torch-xpu/uv.lock
  mlx:
    metal:
      python: '3.12'
      lock_file: mlx/uv.lock
  llama-cpp:
    cuda:
      binary: binaries/cuda/llama-cli
    metal:
      binary: binaries/metal/llama-cli
```

Each entry is a map from platform (`cpu`, `cuda`, `xpu`, `metal`; `fake` for laptop
acceptance tests) to a build recipe. Python recipes require `python` and
`lock_file`. Use `requirements.txt`, or `uv.lock` together with the adjacent
`pyproject.toml` (either file may be named as `lock_file`). Pin requirements;
uv's frozen sync consumes the snapshotted lock without updating it. Project installation is
excluded: job source stays in `run.workdir`, while the environment installs the
locked dependencies. Local path/workspace dependencies must be made available
as installable packages; a lock snapshot does not copy a project tree.

Binary recipes use `binary` for a NAS executable, or `build` for a command
argument array that produces executables under `{root}/bin`. An optional `build`
after `binary` can finish setup. There is no implicit shell. For example:

```yaml
environments:
  custom-gpu-tool:
    cuda:
      build: ["/shared/build-gpu-tool", "--output", "{root}/bin"]
```

Build tools must already be present on that machine. The build receives a cache
folder as `{root}` and runs with that folder as its working directory. It must
finish successfully and must not leave daemons running. Builder subprocesses
are owned process groups, stopped when background building yields to real work,
and checked for orphaned descendants on restart. These are trusted local
commands with worker-user permissions, not a sandbox.

Relative catalog inputs resolve beside the catalog. For binary inputs on
machines with different NAS mount paths, set worker `--environment-root` to its
local catalog directory. The binary's content digest is checked before copying;
a changed NAS executable fails clearly. Absolute binary paths require the same
path on every eligible worker. Python files are snapshotted at submission, so
workers do not read the submitting machine's paths.

Select the environment in a job:

```yaml
environment: torch-cuda
needs:
  memory_gb: 12
  platform: any
run:
  command: [python, train.py]
  workdir: /shared/project
```

Add the usual `name`, `project`, `priority`, and `resumable` fields. This example
can run only on CUDA despite `platform: any`, because `torch-cuda` has no other
recipe. Status describes missing platform/environment capacity.

## CPU tools with pixi

A `cpu` recipe supports CPU work on Linux and Darwin, including the CPU target
of a GPU host. Python (`requirements.txt` or `uv.lock`), pixi, and binary/build
recipes all accept `cpu`. The recipe must support the target's OS/architecture;
`cpu` does not make a Linux binary portable to Darwin. There is no implicit
fallback between CPU and GPU recipes. `platform: any` still permits only a
matching recipe and does not turn a GPU job into a CPU job. Select CPU execution
with `needs.platform: cpu` (or CPU cores without a platform selector).
Neither `platform: any` nor `platform: {preferred: [cpu]}` places work on CPU.
If the job's permitted platforms have no overlap with its environment recipes,
submission fails with HTTP 400 `job_environment_platform_mismatch`.

For Python 3.12, uv and ffmpeg from conda-forge, create `cpu-tools/pixi.toml`:

```toml
[workspace]
name = "cpu-tools"
channels = ["conda-forge"]
platforms = ["linux-64", "linux-aarch64", "osx-arm64", "osx-64"]

[dependencies]
python = ">=3.12,<3.13"
uv = "*"
ffmpeg = "*"
```

Run `pixi lock --manifest-path cpu-tools/pixi.toml` when authoring the recipe.
Keep the generated `pixi.lock` beside the manifest; it pins the actual tool and
dependency versions for each declared OS/architecture. Workers install this
lock with `pixi install --locked`; they do not solve it again. Workers reuse a
suitable pixi on their configured PATH or download the pinned, checksum-verified
pixi release on first use. Preinstallation is unnecessary; workers need outbound
network access for that download and the locked dependencies. The Python, uv
and ffmpeg used by the job come from the prepared environment, independent of
those tools on the host.

Cover every feasible target returned by `myce portability`. A one-platform lock
limits environment coverage, not host identity: declare the matching platform
until another recipe is verified, then widen targets. Keep distinct per-platform
recipes when dependencies differ; native variants carry their own bundled locks.
Package-cache readiness alone does not qualify a different command. See
[placement and lock guidance](portability.md#declare-requirements-let-mycelium-place)
and [custom-command environment reuse #380](https://github.com/1kuna/mycelium/issues/380).

Register it through the existing shared catalog:

1. Put `cpu-tools/pixi.toml` and `cpu-tools/pixi.lock` beside the queue's existing
   NAS catalog, in its `cpu-tools/` subdirectory. Catalog pixi recipes snapshot
   these two files; keep bundled activation/setup inputs in an extension if
   they need a complete workspace snapshot.
2. Add the following entry to that catalog (preserve the other entries). Before
   enabling the first catalog, read the
   [preinstalled-environment compatibility warning](#preinstalled-environment-compatibility):

   ```yaml
   environments:
     cpu-tools:
       cpu:
         backend: pixi
         lock_file: cpu-tools/pixi.lock
         environment: default
   ```

3. Have the queue operator start/restart the queue with that catalog selected
   by `myce queue --environment-config /queue/nas/mycelium/environments/catalog.yaml`
   alongside its usual options. For an installed queue, retain its configured
   catalog path. Catalogs are read at queue startup, with no live registration
   endpoint or reload command. Existing submitted jobs retain their snapshots;
   changed files apply to new submissions after the catalog is reloaded.
4. Submit a job or profile with `environment: cpu-tools` and CPU needs:

   ```yaml
   environment: cpu-tools
   needs: {platform: cpu, cpu_cores: 2, memory_gb: 2}
   run:
     command: [python, process.py]
   ```

The worker uses the existing environment preparation and cache, activates the
locked pixi environment and puts its `bin` first on `PATH`. Commands such as
`python`, `uv` and `ffmpeg` then resolve there. Consumers add catalog entries
through the queue operator; `join --environments` is the compatibility mode for
preinstalled tools, not catalog registration. No worker host-tool installation
or per-host recipe registration is needed for the job's declared dependencies.

## Project lock files

A job can bypass the named catalog, including when a catalog with unrelated
entries is loaded:

```yaml
environment:
  lock_file: /queue/nas/project/uv.lock
  python: '3.12'
  platforms: [cuda, xpu]
needs:
  memory_gb: 8
  platform: {preferred: [cuda]}
```

The queue process must be able to read the input files at submission time,
using its own filesystem paths and permissions. For `requirements.txt`, only
that file is read; for `uv.lock` or `pyproject.toml`, both adjacent files are
required. Use an absolute path visible to the queue, such as
`/queue/nas/project/requirements.txt`. A NAS path must use the queue's mount
path, which may differ from the submitter's or worker's mount path. Relative
inline paths resolve from the queue process's working directory, not beside the
profile (relative catalog inputs instead resolve beside the catalog).

`myce run --profile` sends `environment.lock_file` unchanged. Its source archive
and `inputs` uploads do not turn that path into a queue-readable environment
input: a submitter-local lock works only if the queue can also read that exact
path. Put the lock on queue-readable storage before submitting, or ask the queue
operator to register a named catalog recipe and select it with
`environment: NAME`. The queue snapshots Python inputs for workers; workers do
not need the original lock path.

A missing, unreadable, oversized (over 1 MiB), or unsupported lock input is
rejected before acceptance with HTTP 400 `job_environment_invalid`. Filesystem
failures use the generic message “environment must identify one configured
environment or supported lock_file recipe” to avoid disclosing queue-local paths.
That message alone does not diagnose catalog lookup or platform support. Check
the exact lock basename, required companion files, and readability as the queue
service user. A readable `requirements.txt` with `python: '3.12'` and
`platforms: [xpu]` is supported with or without a catalog; XPU profile acceptance
also requires a compatible known target. Unknown named catalog entries are a
separate rejection, reported as `unknown environment "NAME"`.

`platforms` is optional
and defaults to every platform; state it when dependencies only support a
subset. Exactly one of `name` and `lock_file` is allowed. Changing the content,
Python version, or platform changes the cache identity. Identical content at
different NAS paths reuses the same environment.

For CPU-only work, use a CPU recipe and select CPU execution explicitly:

```yaml
environment:
  lock_file: /queue/nas/project/uv.lock
  python: '3.12'
  platforms: [cpu]
needs:
  cpu_cores: 2
  memory_gb: 2
  platform: cpu
```

## Cache, placement, and failures

`join --environment-cache DIR` defaults to `STATE_DIR/environment-cache`;
`--environment-cache-bytes` defaults to 20 GiB. Keys include input content,
platform, Python version, and binary/build identity. Workers report verified
published hashes and measured build durations. Within a priority tier, placement
prefers environment and model ready, then either ready, then cold setup. Loaded
model hints beat merely cached models at equal combined readiness. Missing
build times use 60 seconds; measured durations, or the worker's mean build time,
estimate subsequent builds alongside remaining NAS model-copy time.

Idle workers build waiting-job environments in queue order. Ready assignments,
model loads/calls, leaving, and network loss pause background building. A paused
build restarts from scratch; completed environments are retained. On restart,
partial builds are detected under OS use/build locks and retired to `.Trash`. There
is one build per hash per local cache at a time, including across processes.
Published cache entries use least-recently-used eviction. A shared use lease
protects builds and running jobs against eviction.

A failed builder fails affected waiting jobs with a clear reason. Its full log
is uploaded in bounded, deduplicated chunks and is readable with `myce logs ID`;
local `HASH.build.log` and worker report/upload offsets survive restart. Cache
publication follows file sync and an atomic ready-marker rename. Job environment variables
include `VIRTUAL_ENV` for Python environments and `MYCE_ENVIRONMENT_DIR` for the
cache root. Jobs and builders never receive `MYCE_TOKEN`.

The environment cache's internal `.Trash` is temporary retirement storage.
Cleanup, admission and attempt release purge retired environments once no
attempt or preparation holds their use/build locks. The size limit includes
published environments, other partial builds and all remaining retired bytes.
Admission purges before enforcing the limit and before the build free-space
check, so retirement frees disk rather than accumulating outside the budget.
Disk exhaustion fails visibly. Local `HASH.build.log` files remain outside the
retired entries; protect runtime/build logs like other job logs.

Workers reuse suitable uv/pixi tools on their configured `PATH`; otherwise the
environment builder downloads the pinned, checksum-verified backend on first
use and caches it locally. Workers need outbound network access for backend and
dependency downloads. Acquired backend tools are invoked by absolute path and
are not automatically put on the job's PATH: declare uv in the locked environment
when the job itself needs it. Package caches use the environment cache's shared
download area. Model loading continues to use NAS masters only.

## Command environment and native library precedence

This contract applies whenever Mycelium binds a prepared environment, for CPU,
CUDA, XPU and Metal/MPS alike. A plain native command without a prepared
environment keeps its existing worker runtime environment (MY-25).

Mycelium starts with the worker's allowlisted base environment, including its
installed GPU runtime settings, then applies the command's authored `run.env`
(or `execution.main.env` / sidecar `env`). Binding sets `MYCE_ENVIRONMENT_DIR`
to the prepared cache root. For uv/binary recipes it sets `VIRTUAL_ENV` to
`ROOT/venv` and prepends `ROOT/venv/bin`, then `ROOT/bin`, to `PATH`. Pixi's
saved locked activation replaces authored values for the variables it sets,
including `PATH` and `CONDA_PREFIX`; it clears `VIRTUAL_ENV`. Worker control
variables and device ownership cannot be overridden by activation.

After activation, Mycelium prepends these native library directories in order:

1. `PREFIX/lib`
2. `PREFIX/lib64`
3. `ROOT/lib`
4. `ROOT/lib64`

`ROOT` is `MYCE_ENVIRONMENT_DIR`; `PREFIX` is `ROOT/venv` for uv/binary
recipes, or pixi's activated `CONDA_PREFIX`. These directories are included even
when absent; Mycelium does not recursively discover package-specific directories.
On Linux this sets `LD_LIBRARY_PATH`. On Darwin it sets `DYLD_LIBRARY_PATH`
and prepends to `DYLD_FALLBACK_LIBRARY_PATH` only when explicitly supplied,
leaving dyld's default fallback intact otherwise. Darwin's loader rules still apply, including
SIP stripping `DYLD_*` when launching protected system executables. For a bound
environment, Mycelium restores these two loader variables with `/usr/bin/env`
after its shell supervisor, before executing the payload. A payload that launches
another protected executable (including a system-shell script) may lose them
again and must use a suitable interpreter or wrapper. Mycelium does
not forward ambient worker `DYLD_*` variables. It does not alter `LIBRARY_PATH`,
which is a compiler/linker search path, rather than a dynamic-loader control.

The remaining value follows the prepended directories: pixi activation's value
if supplied, otherwise the command's authored value if supplied, otherwise the
allowlisted worker value. Thus an install-time system GPU runtime cannot shadow
a library in a standard environment library directory. Retaining ambient paths
afterward permits host drivers and other libraries absent from the environment
to resolve. This is library precedence, not a hermetic loader or ABI-compatibility
guarantee; the extension still owns its dependency closure and qualification.

For package-specific or extra native libraries, declare the loader variable in
the member's `env`, using its bound root; for example on Linux:

```yaml
execution:
  main:
    environment: main
    command: ["{env.main.python}", app.py]
    env:
      LD_LIBRARY_PATH: "{env.main.root}/venv/lib/python3.12/site-packages/vendor/lib"
```

Use `DYLD_LIBRARY_PATH` on Darwin, or a locked pixi activation script when that
backend owns the extra directories. Values are literal substitutions, not shell
expansion: `$LD_LIBRARY_PATH` does not append the worker's path. Multiple authored
directories use the OS path-list separator (`:` on Linux and Darwin). Declaring
a loader value replaces its ambient value; the standard directories above still
come first. Specify any needed nonstandard host directory explicitly. Prefer
package-relative RPATH/RUNPATH where the package supports it.

Sidecars inherit the main environment unless they select their own. Main's
authored env is not inherited; each sidecar declares its own extra directories.
Mycelium reapplies library precedence after sidecar env overrides. Switching
bindings removes the previous environment's managed library directories and any
extra paths inside its root, so another member's runtime does not remain in the
search path. Device-selection variables such as `ZE_AFFINITY_MASK`,
`CUDA_VISIBLE_DEVICES`, `ONEAPI_DEVICE_SELECTOR`, and `SYCL_DEVICE_FILTER` are
kept; final worker device binding takes precedence over authored settings.
Jobs and builders never inherit `MYCE_TOKEN`.

## Preinstalled-environment compatibility

For existing installations without an environment catalog, explicit
`join --environments NAME,...` remains a preinstalled-environment compatibility
mode. It does not build anything or provide content-based identity. With a
catalog, unknown named environments are rejected. Adding the first catalog
therefore rejects new submissions using names advertised only through worker
`--environments`; register those names in the catalog before enabling it.

## Extension preparation and pixi

Compatible demand automatically fetches a registered extension's exact SHA-256
folder and prepares its named environments and immutable assets. This is trusted
native execution as the worker user, under the same queue launch authorization,
maintenance/cancellation custody and cache budget as existing builds. No GPU job
is assigned merely to install dependencies. Preparation is single-flight per
cache key on the worker, and failures remain visible without a rebuild loop.
A new content or host fingerprint permits a fresh preparation attempt.

An extension variant can wrap an existing pixi workspace:

```yaml
variants:
  - target: {os: linux, arch: amd64, platform: cuda}
    environments:
      main:
        backend: pixi
        manifest: env/pixi.toml
        lock_file: env/pixi.lock
        environment: default
      helper:
        backend: uv
        python: '3.12'
        lock_file: helper/requirements.txt
    # needs, execution and check retain the existing variant contract.
```

`manifest` is optional and defaults to `pixi.toml` beside `pixi.lock`.
`environment` defaults to pixi's `default` environment. The complete extension
folder is snapshotted and verified by digest, preserving the workspace's relative
paths for activation scripts and other bundled inputs. Pixi owns mixed conda/PyPI
dependencies. The extension owns every version, channel, platform and variant.
Mycelium supplies mechanisms and
**guarantees no packages**. Existing uv recipes remain supported.

Workers reuse suitable pixi on PATH or acquire the pinned release on first use,
as described above; outbound network access is required for downloads.
Workers run `pixi install --locked`, which rejects a
manifest/lock mismatch without re-solving, then `pixi shell-hook --json --locked`
for activation. `--frozen` permits mismatched inputs and is deliberately not used.
Pixi and uv install into stable cache prefixes; only the final ready marker is
atomically published. Cancelled/crashed unready prefixes retire to internal
cache Trash after exact process cleanup and are purged once unreferenced.
Pixi's package cache lives inside the entry and counts against its budget. Builds can optionally run argv under
`build` with `{root}` and frozen bundled `{extension}` inputs, inside the prepared
environment. There is no implicit shell or Docker requirement.

Real-pixi integration tests run automatically when pixi is on PATH, or when
`MYCE_PIXI_BIN` points to a binary. They use empty locked environments without
package downloads or a GPU, and cover bundled activation through a worker job:

```sh
MYCE_PIXI_BIN=/path/to/pixi go test ./internal/myce -race -run '^TestPixiReal' -v
```

Small wrapper files belong in the extension folder. Large immutable files use
`assets: {model: {sha256: '<64 hex digits>', size_bytes: 123}}` at extension scope,
backed by the existing queue file store. `{assets.model}` resolves to the verified
worker cache copy in setup, main and sidecar argv/env/workdir. Assets are plain
files; this slice does not add archive extraction. Environment and asset use
locks remain held until the whole native process group, including sidecars, is
gone. Retired environment bytes are purged after those references are released.

Preparation alone never qualifies a host. Variant qualification records bind
host, exact OS/arch/backend, extension digest, host fingerprint, all environment
keys, check contract digest and successful check invocation ID. Placement rejects
missing or stale identities. E3's `myce extension check` flow will populate these
records; E2 does not automatically run qualification checks. Legacy extension
checks retain their existing compatibility contract.
