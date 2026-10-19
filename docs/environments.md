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

Each entry is a map from platform (`cuda`, `xpu`, `metal`; `fake` for laptop
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

## Project lock files

A job can bypass the named catalog:

```yaml
environment:
  lock_file: /queue/nas/project/uv.lock
  python: '3.12'
  platforms: [cuda, xpu]
needs:
  memory_gb: 8
  platform: {preferred: [cuda]}
```

The queue must be able to read the lock and companion `pyproject.toml`. For
requirements, use `/queue/nas/project/requirements.txt`. `platforms` is optional
and defaults to every platform; state it when dependencies only support a
subset. Exactly one of `name` and `lock_file` is allowed. Changing the content,
Python version, or platform changes the cache identity. Identical content at
different NAS paths reuses the same environment.

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
partial builds are detected under OS build locks and moved to `.Trash`. There
is one build per hash per local cache at a time, including across processes.
Published cache entries use least-recently-used eviction. A shared use lease
protects builds and running jobs against eviction.

A failed builder fails affected waiting jobs with a clear reason. Its full log
is uploaded in bounded, deduplicated chunks and is readable with `myce logs ID`;
local `HASH.build.log` and worker report/upload offsets survive restart. Cache
publication follows file sync and an atomic ready-marker rename. Job environment variables
include `VIRTUAL_ENV` for Python environments and `MYCE_ENVIRONMENT_DIR` for the
cache root. Jobs and builders never receive `MYCE_TOKEN`.

Trash preserves interrupted builds and evictions. The size limit bounds active
published environments and counts other partial builds at admission; builds
monitor size and pause/fail if the limit or free-space floor is exceeded.
Recovery storage is separate; moving files to same-disk Trash
does not reclaim physical free space. Disk exhaustion fails visibly. Never empty
Trash automatically. Runtime/build logs may contain project output and should be
protected like other job logs.

`uv` must be installed and visible in the worker's configured `PATH` for Python
recipes. The installer preserves that PATH for the boot service; doctor checks
uv. Its download and package caches live inside the build folder. Environments
can download their declared dependencies, but model loading continues to use NAS
masters only.

For existing installations without an environment catalog, explicit
`join --environments NAME,...` remains a preinstalled-environment compatibility
mode. It does not build anything or provide content-based identity. With a
catalog, unknown named environments are rejected.

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

The worker requires pixi on PATH; it never installs pixi automatically. A host
without it is unqualified with a missing-prerequisite reason. Automatic pixi
installation is a follow-up. Workers run `pixi install --locked`, which rejects a
manifest/lock mismatch without re-solving, then `pixi shell-hook --json --locked`
for activation. `--frozen` permits mismatched inputs and is deliberately not used.
Pixi and uv install into stable cache prefixes; only the final ready marker is
atomically published. Cancelled/crashed unready prefixes move recoverably to
cache Trash after exact process cleanup. Pixi's package cache lives inside the
entry and counts against its budget. Builds can optionally run argv under
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
gone. Cache Trash is retained evidence, not reclaimed disk space.

Preparation alone never qualifies a host. Variant qualification records bind
host, exact OS/arch/backend, extension digest, host fingerprint, all environment
keys, check contract digest and successful check invocation ID. Placement rejects
missing or stale identities. E3's `myce extension check` flow will populate these
records; E2 does not automatically run qualification checks. Legacy extension
checks retain their existing compatibility contract.
