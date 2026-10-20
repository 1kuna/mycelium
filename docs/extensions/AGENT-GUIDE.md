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
   '["python3","{extension}/app.py"]' ./extension` creates a skeleton. Flags
   precede the folder. Replace its purpose/resources, bundle only small regular
   files, and declare the existing environment and execution (example below).
   `fake` runs CPU code in laptop dev with simulated GPU inventory. Real CPU
   enrollment is a separate resource slice; do not advertise fake as a CPU fleet.
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
   simultaneous main/helper RAM envelope. Sidecars need positive `memory_gb`.
   Do not claim CPU core admission or hard caps on unsupported hosts. `port: 0`
   allocates loopback; readiness gates the main command. Helpers get
   `MYCE_SIDECAR_PORT` / `MYCE_SIDECAR_URL`; members get
   `MYCE_SIDECAR_SERVICE_PORT` / `MYCE_SIDECAR_SERVICE_URL` for `name: service`.
   Use `{port}` in the helper argv and `{sidecars.service.url}` in the app's
   existing endpoint variable. Let Mycelium select devices: use `MYCE_DEVICE`
   (`cuda:0`, `xpu:0`, `mps`) and its visibility environment. Never set
   `CUDA_VISIBLE_DEVICES`, XPU selectors, `MYCE_*` control variables or tokens
   in authored member env. No implicit shell expansion or background daemons.
5. **Check, then register/use.** Run static validation before execution:

   ```sh
   myce extension check --static ./extension
   myce extension add --root ./extension-state ./extension
   myce extension check --root ./extension-state --gpu fake --device 0 \
     --host dev-host --environment-cache ./state/workers/fake-1/environment-cache \
     app-stage@1
   ```

   Configure `MYCE_QUEUE` / `MYCE_TOKEN` privately. Run checks only on authorized
   hosts. `--host` is **this local worker's configured host ID**, not SSH or a
   remote request. Use the same root, environment cache, backend, device and host
   ID as that worker. Run the command locally on **each** target host; offline
   hosts remain unchecked. `--digest SHA256` asserts frozen content identity.
   Static checks execute nothing and never qualify. Runtime checks require a matching enrolled local worker. They automatically
   use E2 preparation, then submit an ordinary job restricted to that worker,
   with normal admission, sidecars, readiness and cleanup. A declared check runs from the extension folder; otherwise main runs
   as a minimal smoke in its declared workdir (default: an empty private workspace).
   Smoke success qualifies
   execution only. For correctness, bundle actual recorded native baseline
   input/output and a replay check; see [baseline rules](../write-an-extension.md).
   A qualification binds the content digest, exact target, host fingerprint,
   environment recipe identities and check invocation. Changes require recheck;
   changed frozen files require a new version. A Linux check cannot qualify Mac.
6. **Swap the caller seam.** For an existing HTTP consumer, change its endpoint
   to the corresponding Mycelium model endpoint; preserve its protocol. For a
   finite stage, replace the existing subprocess call with the generic client
   submit/wait/result calls below. Disable overlapping app-side placement for
   this route. Keep business keys, cancellation and result processing in the app.
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
`app.py` unchanged. Start `myce dev --db ./state/queue.db --checkpoints
./state/checkpoints --file-store ./state/files --extension-root ./extension-state
--host-id dev-host --workers 1`. Perform step 5 using `http-stage@1`.

The app's caller boundary becomes the following; retry **the identical spec and
key** after a lost response. No new key until it represents new business work.
The Python client has no required third-party dependencies.

```python
from mycelium import Client
client = Client()  # MYCE_QUEUE and MYCE_TOKEN
spec = {"name": "stage", "project": "demo", "class": "batch",
        "extension": "http-stage@1", "submission_key": "document-revision-1",
        "files": {"input.json": client.upload_file("input.json")},
        "outputs": {"result.json": "results"}}
job = client.submit_and_wait(spec, timeout=60)
if job["state"] != "succeeded":
    raise RuntimeError(job.get("reason", job["state"]))
client.download_result(job["id"], "result.json", "result.json")
# Cancel even if submission's response was lost:
# client.cancel_submission("demo", "document-revision-1")
```

Go's existing `Client` supplies `IdentifyFile`/`UploadFile`, `SubmitAndWait`,
`GetJob`, `DownloadResult` and `CancelSubmission` with context bounds. Both
clients fetch artifacts only from the successful accepted attempt, verify hashes,
and take an explicit destination; never treat worker paths as caller paths.

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
fixed ports; accidental inherited credentials; consuming failed/stale attempts;
changing the spec under an existing submission key. Report the actual caller diff,
qualified targets and remaining prerequisites. Never weaken checks to get green.
