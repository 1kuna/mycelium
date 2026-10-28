# Calling general model servers

A catalog model can run any trusted local HTTP server. Speech, separation,
alignment, OCR and face APIs share the queue's model loading, pins, priority,
draining, preemption, idle unloading, pre-copying and worker-loss handling.
No GPU or runtime substitution happens. A command must keep its server in the
foreground, bind loopback, and keep descendants in its owned process group.
These are plain commands with worker-user permissions, not a sandbox.

```yaml
models:
  - name: speech
    engine: server
    max_in_flight: 1
    environment: speech-xpu
    path: speech-weights.bin
    size_bytes: EXACT_BYTES
    sha256: APPROVED_HASH
    memory_gb: MEASURED_WEIGHTS_PLUS_ACTIVATIONS_AND_RUNTIME_GIB
    host_memory_gb: MEASURED_ADDITIONAL_CPU_SIDE_RAM_GIB
    platforms: [xpu]
    device: '0'
    device_env:
      ONEAPI_DEVICE_SELECTOR: 'level_zero:0'
      ZE_AFFINITY_MASK: '{device}'
    start_command: [python, /shared/speech/server.py, --port, '{port}', --weights, '{model_path}']
    health_url: 'http://127.0.0.1:{port}/health'
    ready_timeout: 5m
    stop_timeout: 20s
    # Optional control endpoint, followed by process-group TERM, then KILL:
    # stop_command: [python, /shared/speech/stop.py, --port, '{port}']
```

Use the environment catalog from [environments](environments.md). The queue
snapshots its platform recipe into the load plan; the worker builds/acquires
that environment and holds its use lock until unload. Its `bin` and `venv/bin`
precede PATH. `{environment}` is the published environment directory;
`{model}` and `{model_path}` are the verified local weights path; `{device}` is
the physical selector; `{port}` is a chosen loopback port. Templates substitute
individual argv strings with no implicit shell. The health check requires HTTP
2xx. The whole load, including copies, environment setup and admission waits,
is bounded by `max_wait`. The optional `ready_timeout` starts at engine launch
and additionally bounds process readiness. Timeout or crash
kills the owned group and returns a visible error. The optional stop command
and TERM share `stop_timeout` (default 10 seconds), followed by KILL and group
verification. Failed unload retains its memory reservation and environment lease.

Until package M migration, start one worker per physical card, using
`join --name b70-gpu0 --gpu xpu --device 0 --memory-mode fixed
--memory-gb REVIEWED_ALLOWANCE`. Device selectors
apply to batch commands and server processes without changing global env.
For XPU, `ZE_AFFINITY_MASK` names physical cards while `ONEAPI_DEVICE_SELECTOR`
uses indices after that mask: physical device `1` becomes `level_zero:0` /
PyTorch `xpu:0`. `MYCE_DEVICES` always retains the physical selector.
Under the [package M target](design-proposal.md#one-machine-one-worker-m0m4-100-decision-2026-10-20),
one worker owns the machine inventory and a server selects at most one card.
Existing multi-card worker or model masks such as `0,1` (including inputs to
`WithModelDevice`) must fail at install/config load with typed
`gpu_count_unsupported`, before launching anything; do not silently truncate or
split them. This is the M2 target, not a claim that the legacy worker already
rejects them. One job using multiple GPUs on the same machine is deferred to
[#105](https://github.com/1kuna/mycelium/issues/105). CUDA visibility also
renumbers selected cards from zero; a single selected card is `cuda:0` inside
the process.
Keep `{device}` for physical selectors, not framework-local device arguments.
A server launched through `docker run` must explicitly forward these variables
with Docker `--env NAME` and expose the required device nodes; a Docker client
environment is not automatically the container environment.
CUDA selectors resolve to a stable GPU UUID used by both server execution and
`nvidia-smi --id` inventory; numeric CUDA ordering cannot change the measured card. XPU admission remains
an explicit conservative fixed allowance, not a live external-allocation guard.
Catalog `device` restricts placement to workers advertising that selector.
Every card worker on one machine shares `--host-id` (default OS hostname).
Optional catalog `host_memory_gb` and batch `needs.host_memory_gb` reserve RAM
from that shared host allowance, rather than independently per card. See
[host admission](operations.md#host-ram-admission-and-ceilings) for the unified-memory rule.
No second worker/controller may own that card. Under M2, XPU identity is PCI BDF
or Level Zero UUID; the worker and every member force
`ZE_ENABLE_PCI_ID_DEVICE_ORDER=1`. Verify ordinal-to-identity mapping at worker
and member start; a mismatch withholds that device with
`device_identity_mismatch`. CPU execution must hide both CUDA and XPU, using
an empty `CUDA_VISIBLE_DEVICES` and explicit select-none `ZE_AFFINITY_MASK` and
`ONEAPI_DEVICE_SELECTOR`; deleting XPU selectors exposes defaults. Verify zero
visible accelerators with the actual runtimes, including sidecars/containers;
inability to enforce this withholds CPU work with
`cpu_accelerator_isolation_unavailable`. A selector is framework isolation,
not an OS security boundary; qualify it with the real runtime before cutover.

`max_in_flight` optionally limits simultaneous calls per loaded copy (set it to
`1` for serial speech servers). Omitted or `0` means unlimited; negative values
are rejected. Full copies leave calls queued in the existing priority/deadline
order; another free loaded copy may serve them. Completion, cancellation and
worker loss release capacity. Use the same catalog on queue and workers.

## HTTP passthrough

```sh
curl "$MYCE_QUEUE/models/speech/transcribe?language=en" \
  -H "Authorization: Bearer $MYCE_TOKEN" \
  -H 'X-Myce-Project: transcribe' -H 'X-Myce-Priority: production' \
  -H 'Content-Type: audio/wav' --data-binary @qualification.wav
```

`/models/NAME/ANY_PATH` forwards any HTTP method, query, request body and end-to-end
headers. Response status, headers and bytes pass through with immediate flushing;
there is no JSON, audio, SSE or multipart interpretation. Fleet authorization,
Myce routing headers and hop-by-hop headers are consumed, never sent to the
engine. Generic request bodies use a bounded **8 GiB local disk spool** at the
queue for placement/retry; larger payloads fail explicitly. The worker streams
the body to the engine, preserving content length. The spool is removed on every
normal/error return. `queue --upload-spool-bytes` sets a shared positive byte
budget (default **16 GiB**, enough for two maximum-size uploads); lower it on
small disks. `--upload-spool-dir` selects the directory, otherwise a private
`<db>.uploads` directory beside the queue DB is used. Known lengths reserve before reading; chunked uploads reserve
before each disk write. Bytes stay charged while waiting, running and retrying.
A full budget returns plain HTTP **507** explaining the limit; an over-8-GiB
request returns **413**. Failed/aborted uploads remove their files and release
their bytes. The budget is for this queue process, not other disk users; leave
space for SQLite and other workloads. OpenAI
JSON routes keep the 64 MiB in-memory limit. WebSockets remain unsupported.
The queue waits for the upload before placement; it does not stream uploads
through the queue while retaining an unbounded replay buffer.
Responses stream without buffering. A generic stream that loses its worker after
bytes start aborts its connection; no JSON/SSE error is inserted into binary data.
Before response bytes start, worker transport failure may retry once on another
worker or a fresh engine attempt only with catalog `retry_safe: true` (default
false). Side-effecting server APIs must be idempotent or accept an application
idempotency key: a failed transport cannot prove the server did no work.

The ordinary `/v1/chat/completions`, `/v1/completions`, `/v1/embeddings` routes
continue working for OpenAI-speaking engines, with the unchanged JSON body.

## Private workloads

Generic uploads use 0600 files in a 0700 queue-owned directory. Startup takes
an exclusive directory lock and deletes owned `myce-model-request-*` leftovers;
cleanup failure refuses startup. Normal completion, upload/admission errors and
cancellation unlink the upload and release quota. Choose a dedicated **local
disk** directory with `queue --upload-spool-dir /local/private/myce-uploads`;
never put document spools on NAS. Keep the DB local too. Legacy uploads in the
old OS temporary directory are not claimed automatically; handle them through
an authorized local cleanup. Unlinking is not forensic erasure. RAM-backed
storage needs its own swap/core-dump policy.

Set `logs: none` on both example server entries. Server and stop-command stdout/stderr
go to the OS null device, without creating server output logs. The default
`logs: keep` (also omitted) captures output. Queue and join accept
`--log-retention`, default `168h` (seven days), strictly positive. Queue batch
logs expire by final completion age (submission age for terminal jobs with no
completion timestamp). Worker batch/server logs expire by last-write age at
startup/within one minute. Active server logs and current unacknowledged batch
replay remain protected. Retention does not bound active log size or securely
erase SQLite/WAL, backups or disk remnants. Environment build logs remain under
their own environment records; they are not copied to jobs with `logs: none`.
Batch jobs also accept top-level `logs: none`, with the same null-device output
behavior and no queue log rows or worker attempt log; see the
[job reference](job-reference.md#scheduling-and-results). Keep confidential
inputs/outputs out of captured batch output.

`logs: none` server entries require a worker advertising `logs_none`; older
workers cannot receive their load plans or serve their calls. A queue-first
upgrade drains live opted-out servers until each host's worker is upgraded.
Upgrade queue and workers together, and drain opted-out work before any downgrade. See the
[job reference](job-reference.md#scheduling-and-results) for downgrade behavior.

Before claiming native Docker qualification, use an authorized disposable daemon
and a retained managed container with `logs: none` to verify:

- `docker logs CONTAINER` refuses because the `none` driver does not support logs.
- The daemon's container directory has neither `*-json.log` nor
  `container-cached.log`, including after completion and worker recovery.
- Managed `create` still succeeds when that daemon has default `log-opts` set
  for its normal logging driver. Check the container's actual driver is `none`.

The fake Docker tests cover argv and recovery behavior; they do not establish
these daemon results or native journald behavior. For native journal proof, have
a harmless payload assert fd 1 and fd 2 identify the null device, then emit
synthetic markers and confirm neither marker reaches the worker's journal.

Disable request logging in the **exact vLLM 0.27.1 launch command** too.
`--disable-log-requests` is a candidate flag to **check** against that version's
`vllm serve --help` or source before use: no offline vLLM source/docs were
available to verify its 0.27.x spelling. `logs: none` gives Mycelium's
deterministic no-capture guarantee independently of that flag. Check that
vLLM and wrappers do not write separate request files or send telemetry.

Models accept `request_timeout` (default `10m`; positive, zero/omitted uses the
default). It bounds dispatched inference across retries, including stalled
headers/body reads. Placement still has separate `max_wait`. Before response
bytes, expiry returns HTTP 504, `model request timeout exceeded`. Started chat
streams receive a fixed SSE timeout error; started generic streams abort and
must be discarded. The worker also bounds its engine request and releases its
in-flight count on failure. Upload duration is not bounded by this field.

Generic `retry_safe` defaults to false. Opt-in permits one transport retry on
another copy or fresh engine attempt only before client response bytes. This is
an assertion of replay safety, not deduplication or exactly-once execution. OpenAI chat/completions/
embeddings retain one pre-response transport retry; detected engine death also
requires `retry_safe: true`. HTTP error bodies and headers become
a fixed message plus HTTP status; control errors/public diagnostics never echo
upstream bodies.

After readiness, the supervisor watches process exit for the entire engine
lifetime. It also repeats the startup health check every **100 ms**, with a
**five-second timeout per probe**, requiring HTTP 2xx without redirects. Readiness is
withdrawn after **at least three consecutive failures spanning at least 15
seconds**, measured from the start of the first failed probe. Fast HTTP failures
keep probing through that window; a successful probe resets both the failure
count and window. Probes are serial and each has a five-second deadline. Process
exit requires no health-failure threshold.
At the next worker check-in the slot leaves `ready`, routing stops, and cleanup
terminates/verifies the owned group. Memory and the environment lease stay held
until cleanup succeeds. `unload_failed` stays held visibly and never auto-reloads.

Pinned entries and entries with queued demand reload automatically after cleanup.
Every failed load fails its waiting calls promptly, including repeated model
verification failures. Unpinned demand does not retry failed warm-up; pin warm-up
retains its automatic retry policy. Recovery waits apply to runtime crashes.
Startup and runtime failures back off for 1s, 2s, 4s, 8s, 16s, then at most 30s,
using the injected queue Clock. Brief readiness does **not** clear crash history:
backoff resets after **60 continuously healthy seconds**. Even a recovered probe
failure between heartbeats interrupts that healthy interval. Status exposes
`attempt`, `crash_count` (failures since that reset), `retry_at`, `retry_delay`,
`healthy_since`, and a fixed `error`; queued-call reasons explain pending cleanup,
loading or the next restart time. Confirmed cleanup releases physical custody
and environment leases. During crash backoff, an active pin keeps its scheduler
GPU/host-memory reservation (`retry_reserved`); batches cannot borrow it. Unpinning
releases that planning reservation. Unpinned crash backoff holds no memory.
With no pin or remaining demand, the failed slot is removed. Pins and retry state
persist; a new worker session reloads. If a rejoining worker still holds a
different engine attempt, it drains and cleans that attempt before starting the
replacement. Cleanup failure blocks the replacement and retains custody. A load
plan expires after `max_wait` plus five seconds of check-in slack, using the
injected queue Clock (`load_until`), covering the whole load rather than just
process readiness. Expiry fails waiting calls promptly as a load failure
(`runtime_failed: false`) and withdraws the load; it never credits unconfirmed
memory. After cleanup is acknowledged, pins retry with backoff, while unpinned
demand cannot drive retries of the failed load.

Queued calls may wait for recovery only within their existing `max_wait`.
The failed-worker exclusion is scoped to an engine attempt: a fresh attempt on
the same worker is eligible. In-flight calls with `retry_safe: false`
fail cleanly on detected engine death or worker-to-engine transport failure,
including OpenAI routes. Non-streaming OpenAI responses are buffered at the
worker as well as the queue so an incomplete engine response fails before
headers are sent. Opted-in calls retain one pre-response retry, now also on a
fresh engine attempt. Ordinary
OpenAI transport failures without confirmed engine death retain their existing
single pre-response retry. No replay is allowed
after response bytes have started. A worker rejects missing, stale or unhealthy
engines before forwarding. Such calls have not executed and may retry once even
with `retry_safe: false`; an internal attempt-bound rejection receipt distinguishes
these from possibly executed engine failures. Attempt identity travels only between queue
and worker and is never forwarded to the engine. A late report or completion
from an old attempt cannot make the replacement ready or consume its capacity.

Machine-only egress for arbitrary commands, proxies, installers and server
telemetry still requires host/network permissions and locally provisioned
weights/environments. Keep project/model/job labels, argv and configuration
metadata non-sensitive: authenticated status still shows these supplied fields.
`llama.cpp` is a preset of this same supervised server engine. The vLLM preset
starts `vllm serve` on loopback and supplies `served_name`, `max_context`,
`kv_cache_dtype` and `gpu_memory_utilization` from `vllm`. `args` supplies the
exact reasoning/tool parsers and other reviewed flags. Owned flags cannot be
overridden. The catalog name is also registered as a served alias when different
from `served_name`, so unchanged OpenAI bodies can name the catalog entry.
Hugging Face and transformers offline mode is enforced; missing weights fail.
Trusted custom commands must respect the no-download contract too.

See the [placeholder server catalog](../examples/):
two generic servers for a roughly 35B chat model and an OCR model, both vLLM
0.27.1. Use exact approved argv, not the vLLM preset. CPU FastEmbed stays outside
the GPU queue. This is not deployable or Spark qualification. Read official model and
runtime guidance and record the effective contract before a hardware run.

## Immutable multi-file weights

For HF/vLLM/transformers, `path` names an immutable NAS directory and `files`
lists **every** required config, tokenizer and weight file:

```yaml
files:
  - path: config.json
    size_bytes: EXACT_BYTES
    sha256: APPROVED_FILE_HASH
  - path: model-00001-of-00002.safetensors
    size_bytes: EXACT_BYTES
    sha256: APPROVED_FILE_HASH
```

The top-level `size_bytes` is the sum. Its `sha256` is SHA-256 of UTF-8 JSON
`[[path,size,lowercase_sha256],...]`, sorted by path, no whitespace, Go JSON string
escaping (including `<`, `>` and `&`). `ModelFilesIdentity` defines it. Use the
approved source manifest, not a directory containing unreviewed mutable downloads.
All files are verified before publishing a local directory. Prefix copies resume;
corrupt partial bundles go to recoverable Trash. Admission counts full directory
bytes and protects loaded/pinned bundles just like single files. Unexpected local
files, symlinks, path traversal and colliding names fail. Bundle roots must use
canonical paths without symlink components (macOS use `/private/var`, not `/var`).
`direct_nas` verifies the complete listed bundle before bypassing the local cache.

The NAS is still the master; no model-weight download or quantization conversion
is part of load. Declared software environments may build on demand. `model sync`, pins and cache telemetry work for bundles. Preserve
model directories/configuration with recorded immutable identities; laptop
process tests do not certify real speech, OCR, vLLM or face model quality.

## Shared entries, local secrets and approved model folders

### Shared entries and placement

Optional `project` identifies the entry owner, accounts for its idle model
reservation, and supplies missing `/v1` project labels. Models remain shared:
any project may call or pin any entry. Explicit caller labels retain their own
call accounting and scheduling policy.

Optional `allowed_machines` intersects the owner's project machine policy, the
caller's policy, platform/device eligibility and pin restrictions. Empty lists
are unrestricted at that layer. Calls wait if no eligible worker is available;
status shows the placement fences. A tightened fence drains excluded copies and
retains memory custody until the worker confirms unloading. Load plans carry
current fences; workers recheck them. Pre-copy and pins obey the same fences.

### Environment and local named secrets

`env` is a map of non-secret literals, applied to start and stop commands.
`secret_env` maps environment-variable names to local secret names. Provision
one regular **mode-0600 file** per name in a **mode-0700 directory**, then pass
`myce join --secret-root SECRET_DIRECTORY`. Directory traversal, symlink files,
missing files and permissive modes refuse loading. Files may contain up to
64 KiB; one trailing LF or CRLF is stripped. No remote secret service is used.

Only names enter catalogs and queue messages. Values are read immediately before
launch and kept in process memory, including the stop helper's environment.
`secret_env` requires `logs: none`: even a server that prints its environment
must not leave secret values in Mycelium logs. Errors never include secret
contents or local secret paths. Plain commands can read their supplied secrets;
operators must also control application telemetry and external log sinks.

The maps cannot override `MYCE_*`, accelerator selectors, offline controls,
`LLAMA_ARG_*`, or worker-controlled executable/environment paths. Use `device`
and `device_env` for device selection. `MYCE_TOKEN` is stripped; provisioning
its value under another local secret name is rejected. Keep `env` literals
non-secret; arbitrary credentials cannot be identified from their spelling.

### Approved in-place model folders

On each host, explicitly approve the exact folder and a root bounding both the
folder and any symlinked HF snapshot blobs:

```sh
myce model approve-local APPROVAL_NAME \
  --path MODEL_FOLDER --approved-root SNAPSHOT_AND_BLOB_ROOT \
  --model-approval-root LOCAL_APPROVAL_DIRECTORY
```

This is a local command and does not contact the queue. It hashes regular files
once, records the canonical folder, host, complete file/directory metadata and
symlink targets, and prints portable `local_source`, `files`, `size_bytes` and
`sha256` fields. Put those fields in the entry. Omit `path`, `file_store` and
`direct_nas`. Start the worker with the same `--model-approval-root` directory.
Keep the approval directory mode 0700 and records private and local; they contain source paths and host
identity. File names in the portable manifest should also be reviewed before
sharing a catalog.

Loads use the original folder as `{model_path}`, with no copy, NAS fallback,
rehash or external-file eviction. Workers advertise only approved identities.
Every check uses the file set, size, mtime, mode, device/inode and symlink-target
metadata. A changed/missing file, new file, changed blob or different host
invalidates readiness. Re-run the explicit approval command after changes and
update the catalog manifest if its content identity changed. All symlink targets
must stay within the approved root; directory symlinks and special files are
rejected. Hard links are regular files: another name for the same inode can
exist outside that root, so the root does not bound hard-link aliases. This is
acceptable because approval trusts operator-controlled immutable storage, not a
filesystem sandbox. Protect every alias from writes; inode/size/mtime checks
detect ordinary changes through any alias. The ordinary copied-model no-symlink
rules are unchanged.

Each heartbeat checks all cheap local approval records before any directory walk,
including when an earlier walk is still blocked. Missing, revoked or host-mismatched
records report `invalid`. Completed metadata results survive a sibling timeout;
only unfinished checks report `unknown`. Record preflight and the metadata pass
have separate 250 ms I/O budgets; checks do not accumulate blocked filesystem calls.
Results are shared with cache inventory, and old valid results are not replayed.

`unknown` blocks new loads, pre-copy and slot creation. Ready servers, including
pinned slots, keep serving and routing for up to five continuous minutes of unknown
state, measured by the queue Clock and persisted with the slot. A definite report
resets the timer. Beyond five minutes the queue drains, with a clear status reason
and rate-limited warning; memory stays reserved until unloading is confirmed.
A load already in progress may drain immediately on unknown; this is an accepted trade-off.
Changed folders and revoked approvals still drain immediately. Missing required approval evidence blocks new loads; current worker/report protocol is mandatory. Worker
warnings appear at most once per minute; expired checks cannot authorize new work.

Protect approved snapshots/blobs against writes and HF cleanup while used.
Metadata checks trust operator-controlled immutable storage; they do not detect
a malicious writer preserving all observed metadata. Retain a recovery source.

### Header-less OpenAI clients

Fleet bearer authentication is always required. On `/v1` routes, an owned entry
supplies its `project` when `X-Myce-Project` is absent and uses `experiment` when
`X-Myce-Priority` is absent. A project policy can explicitly set
`default_call_priority: production`; that opt-in applies to missing priority
headers for the effective caller project. An explicit caller project therefore
uses its own production opt-in even on another owner's entry; the owner's
default does not override it. Explicit priorities remain supported, including
production calls under the existing contract. Project headers are trimmed.
Empty project/priority and duplicate routing headers fail. A deadline header is
ignored for non-deadline calls; deadline priority requires an RFC3339 deadline.

Entries without `project` retain the existing required-header behavior. Generic
`/models/NAME/PATH` calls validate required headers before reading or spooling
the body. Valid requests retain upload spooling; these
changes do not resolve the separate body-storage privacy hold.


The [placeholder server catalog](examples/) needs reviewed argv, environment recipes, measured memory and local approval output. Native runtime and application telemetry require separate qualification.

### GPU selection limits and host holder checks

Device masks select the accelerator for cooperative runtimes; they do not deny
OS access to other device nodes. For example, `xpu-smi discovery` enumerates
cards directly and can briefly open another card's DRM node even with
`ZE_AFFINITY_MASK` set. Such an open alone does not establish out-of-mask compute.
Docker exposure of `/dev/dri` and per-attempt memory cgroups do not provide a
GPU device allowlist either. Apply the same limits to sidecars.

For host-level holder checks on Linux, run `sudo fuser -v /dev/dri/*` (or
`sudo lsof /dev/dri/*`) on the host, outside the attempt/container. Correlate
PIDs with `ps -fp PID`, attempt process groups and `/proc/PID/cgroup`; map DRM
nodes to PCI identities with `readlink -f /sys/class/drm/renderD*/device`.
Check activity for the corresponding card with the vendor's monitoring tools.
A holder list is a point-in-time view: short discovery opens can disappear,
and a holder is not necessarily doing compute. Check the whole physical host,
including external programs, before concluding a card is idle.

The recorded decision in [#9](https://github.com/1kuna/mycelium/issues/9) is to
leave hard device-cgroup isolation out for now: per-attempt masks, stable
identities and start-time mapping checks cover the cooperative execution model,
and no cross-card compute incident was established. Revisit if dual-GPU
qualification shows out-of-mask access or interference. These selection controls
are not a security boundary for hostile commands.
