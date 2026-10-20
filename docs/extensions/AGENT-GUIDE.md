# Give this to your agent: wrap an existing app

Deliver a small extension folder, a passing check on each authorized target,
and the smallest caller/configuration diff. Preserve the app's algorithms,
layout, command arguments and result format. Mycelium supplies mechanisms,
not application packages. Do not add a scheduler, retry loop, environment builder
or service supervisor to the app.

1. **Inspect before writing.** Read the app's setup/run instructions and locks.
   Find its existing HTTP endpoint setting or finite command boundary. Inventory
   argv, environment variables, prerequisites, model/data identities, input/output
   filenames, sidecars, memory and supported OS/architecture/backend combinations.
   Reuse its launchers and uv/pixi locks. An image freeze is not an installable lock.
2. **Write one complete variant per target.** No inheritance or implicit fallback.
   `myce extension init --name app-stage --gpu fake --command
   '["{env.app.python}","{extension}/app.py","input.json","result.json"]' ./extension` creates a skeleton. Flags
   precede the folder. Replace its purpose/resources, bundle only small regular
   files, and declare the existing environment and execution (example below).
   `fake` runs CPU code in laptop dev with simulated GPU inventory. Real CPU
   enrollment is separate work; do not advertise fake as a CPU fleet.
   Current native runtime supports Unix; Windows needs its runtime port and check.
3. **Keep paths logical.** `{extension}` is the frozen folder, `{work}` the attempt
   workspace, `{env.NAME.python}` / `{env.NAME.root}` prepared environment paths.
   uv accepts bundled `requirements.txt` or paired `uv.lock`/`pyproject.toml`;
   declare `python`. Pixi accepts paired `pixi.lock`/`pixi.toml` and an optional
   environment name. Pin dependencies and reuse the actual project locks.
   Large assets use hash-addressed `assets` and `{assets.NAME}` through the
   existing file store, not machine paths or bundled model weights.
4. **Describe the whole group.** `needs.memory_gb` is the accelerator reservation
   (a positive dev placeholder for fake); `needs.host_memory_gb` covers the whole
   simultaneous main/helper RAM envelope. Sidecar `memory_gb` is host RAM,
   not accelerator memory; include it in that envelope. Declare `outputs: [result.json]`
   at the top level: these are the app's unchanged relative result filenames.
   Do not claim CPU core admission or hard caps on unsupported hosts. `port: 0`
   allocates loopback; readiness gates the main command. Helpers get
   `MYCE_SIDECAR_PORT` / `MYCE_SIDECAR_URL`; members get
   `MYCE_SIDECAR_SERVICE_PORT` / `MYCE_SIDECAR_SERVICE_URL` for `name: service`.
   Use `{port}` in the helper argv and `{sidecars.service.url}` in the app's
   existing endpoint variable. Let Mycelium select devices: use `MYCE_DEVICE`
   (`cuda:0`, `xpu:0`, `mps`) and its visibility environment. Never set
   `CUDA_VISIBLE_DEVICES`, XPU selectors, `MYCE_*` control variables or tokens
   in authored member env. No implicit shell expansion or background daemons.
5. **Start the queue, then check.** For a laptop trial, start this first and
   leave it running in another terminal:

   ```sh
   myce dev --db ./state/queue.db --checkpoints ./state/checkpoints \
     --file-store ./state/files --extension-root ./extension-state \
     --host-id dev-host --workers 1
   ```

   In the terminal where you register, check and call the client:

   ```sh
   export MYCE_QUEUE=http://127.0.0.1:7331
   export MYCE_TOKEN=dev-token  # local dev default only; keep real tokens private
   myce extension check --static ./extension
   myce extension add --root ./extension-state ./extension
   myce extension check --root ./extension-state app-stage@1
   ```

   Static validation executes nothing and does not qualify. Runtime checks run
   the declared check (or main as a smoke test), with the same setup, environment,
   sidecars and cleanup as a job. A smoke test proves execution; a replay against
   recorded native output checks correctness. See [baseline keys and replay](#baseline-keys-and-replay).
   Changed files under a frozen version require a version bump and another check.

   Dev automatically detects the local worker settings. The explicit equivalent is:

   ```sh
   myce extension check --root ./extension-state --gpu fake --device 0 \
     --host dev-host --environment-cache ./state/workers/fake-1/environment-cache \
     app-stage@1
   ```

   `fake-1` is the worker name; `0` is its simulated device selector. Dev creates
   the environment cache next to its queue database under `workers/fake-1/`;
   the worker builds the declared environments there. Main, check and helpers
   using the same declaration share that environment.

   Outside dev, supply `--host`, `--gpu`, `--device` and `--environment-cache`
   from the local worker's configuration. `--host` is its configured host ID,
   not an SSH destination. Run locally on each authorized target, with that
   worker enrolled and its queue running. A mistaken selection leaves existing
   qualifications untouched. A check that executes and fails removes its prior
   qualification. A Linux check cannot qualify a Mac; environment or host
   fingerprint changes require another check. `--digest SHA256` asserts content.
6. **Swap the endpoint at the caller boundary.** Keep the app and its helper
   unchanged. For a finite command, replace its local invocation with the single
   `Client.run()` call below: Mycelium uploads, submits, waits and downloads.
   For an HTTP model consumer, change its endpoint to the corresponding Mycelium
   model endpoint and preserve its protocol. Disable overlapping app-side placement
   for this route. Keep business keys, cancellation and result processing in the app.
7. **Mark job types and set tiers once.** Jobs carry `project` and `class`;
   `default_class` in the extension is an optional default. Export
   `myce policy get --yaml > policy.yaml`, edit ordered tiers, then
   `myce policy set policy.yaml --if-revision N` using the printed current
   revision. Example: `urgent` selects `['demo/interactive']`, `shared` selects
   `['demo/batch', 'other/*']`, final `other` selects `['*/*']`. Same-tier work
   shares available capacity. See [policy details](../guide-priority-policy.md).

## Complete tiny example

[http-stage](../../examples/extensions/http-stage/) contains the unchanged
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
`submission_key` reconciles it to the original job. Use a new key only for new
business work. A failed job raises a typed error with `myce logs SHORT_ID`.
Cancel even after a lost response with `client.cancel_submission("demo", key)`.

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
[complete variants example](../../examples/extensions/http-stage/extension.yaml).

## Debug a failed job

Run `myce status`, then `myce logs SHORT_ID` (or `myce logs --follow SHORT_ID`).
Every job command accepts the unique short ID shown by status; an ambiguous
prefix requires a longer ID. Logs contain the app's stdout/stderr, including its
Python traceback. `myce wait SHORT_ID` reports the terminal state.

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
