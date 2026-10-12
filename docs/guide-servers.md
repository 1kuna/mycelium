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
      ONEAPI_DEVICE_SELECTOR: 'level_zero:{device}'
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
2xx. Loading includes copies and environment setup inside `max_wait`; the
optional `ready_timeout` additionally bounds process readiness. Timeout or crash
kills the owned group and returns a visible error. The optional stop command
and TERM share `stop_timeout` (default 10 seconds), followed by KILL and group
verification. Failed unload retains its memory reservation and environment lease.

Start one worker per physical card, using `join --name b70-gpu0 --gpu xpu
--device 0 --memory-mode fixed --memory-gb REVIEWED_ALLOWANCE`. Device selectors
apply to batch commands and server processes without changing global env.
CUDA selectors resolve to a stable GPU UUID used by both server execution and
`nvidia-smi --id` inventory; numeric CUDA ordering cannot change the measured card. XPU admission remains
an explicit conservative fixed allowance, not a live external-allocation guard.
Catalog `device` restricts placement to workers advertising that selector.
Every card worker on one machine shares `--host-id` (default OS hostname).
Optional catalog `host_memory_gb` and batch `needs.host_memory_gb` reserve RAM
from that shared host allowance, rather than independently per card. See
[host admission](operations.md#host-ram-admission) for the unified-memory rule.
No second worker/controller may own that card. A selector is framework isolation,
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
  -H 'X-Myce-Project: verso' -H 'X-Myce-Priority: production' \
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
worker only with catalog `retry_safe: true` (default false). Side-effecting server APIs must be idempotent or accept an application
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

Set `logs: none` on both Atlas entries. Server and stop-command stdout/stderr
go to the OS null device, without creating server output logs. The default
`logs: keep` (also omitted) captures output. Queue and join accept
`--log-retention`, default `168h` (seven days), strictly positive. Queue batch
logs expire by final completion age (submission age for terminal jobs with no
completion timestamp). Worker batch/server logs expire by last-write age at
startup/within one minute. Active server logs and current unacknowledged batch
replay remain protected. Retention does not bound active log size or securely
erase SQLite/WAL, backups or disk remnants. Environment build logs are outside
this policy. Batch commands must never print confidential inputs/outputs.

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
another copy only before client response bytes. This is an assertion of replay
safety, not deduplication or exactly-once execution. OpenAI chat/completions/
embeddings retain one pre-response retry. HTTP error bodies and headers become
a fixed message plus HTTP status; control errors/public diagnostics never echo
upstream bodies.

Pinned warm-up failures report a fixed error and `retry_at` in status, then
retry after 1s, 2s, 4s, up to 30s, using the queue Clock. Success clears backoff.
Only successfully cleaned failures retry; `unload_failed` stays held visibly.
Pins and retry state persist; a new worker session reloads.

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

See the [Atlas placeholder catalog](../examples/atlas-judge-vllm.example.yaml):
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
