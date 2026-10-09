# Calling models

Apps send one OpenAI request at a time to the queue. They never own a GPU or
manage a model process. The queue loads a configured model on an eligible
worker, waits for readiness, and forwards the original JSON unchanged.

## Try it without a GPU

```sh
go build -o myce ./cmd/myce
./myce dev --workers 1 --model-idle 5s
```

Dev offers `dev-model`, an explicitly fake engine producing `fake answer` or a
fixed embedding. Batch commands still run as real processes. Call the model:

```sh
curl http://127.0.0.1:7331/v1/chat/completions \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -H "X-Myce-Project: demo" \
  -H "X-Myce-Priority: production" \
  -d '{"model":"dev-model","messages":[{"role":"user","content":"Hello"}]}'
./myce status
```

After five seconds without calls, status shows the model draining and then
absent. A second call loads it again. `dev --model-config fake-models.yaml`
accepts a custom catalog only when every engine is `fake`.

## Request contract

The queue supports `POST /v1/chat/completions`, `POST /v1/completions`, and
`POST /v1/embeddings`. Chat supports `"stream": true`, forwarding and flushing
server-sent events. Other endpoints use complete JSON responses.

All calls require the fleet token and these headers:

| Header | Value |
|---|---|
| `Authorization` | `Bearer <MYCE_TOKEN>` |
| `X-Myce-Project` | A nonempty project name |
| `X-Myce-Priority` | `production`, `deadline`, or `experiment` |
| `X-Myce-Deadline` | RFC3339 timestamp, required for `deadline` priority |

Priority metadata stays out of the OpenAI body. The body must name a model in
the catalog. The engine validates its remaining OpenAI fields; Mycelium does
not rewrite sampling settings, prompts, tools, or model responses. Request
bodies are limited to 1 MiB. Errors have an OpenAI-style `error.message`.

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:7331/v1",
    api_key="dev-token",                 # use your fleet token outside dev
    default_headers={
        "X-Myce-Project": "atlas",
        "X-Myce-Priority": "production",
    },
    timeout=600,                         # include your configured load wait
    max_retries=0,                       # Mycelium already retries worker loss once
)
response = client.chat.completions.create(
    model="dev-model",
    messages=[{"role": "user", "content": "Hello"}],
)
print(response.choices[0].message.content)

for chunk in client.chat.completions.create(
    model="dev-model",
    messages=[{"role": "user", "content": "Hello"}],
    stream=True,
):
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

Embeddings and text completions use `client.embeddings.create(model=..., input=...)`
and `client.completions.create(model=..., prompt=...)` with the same headers.

## Model catalog and worker setup

Queue and workers read the same short YAML file via `--model-config`:

```yaml
idle_timeout: 10m
max_wait: 5m
models:
  - name: chat
    engine: llama.cpp
    path: /shared/models/chat.gguf
    memory_gb: 24
    platforms: [cuda, metal, xpu]
    args: [--ctx-size, "32768"]
```

Names are unique. `path` is a local GGUF file on every eligible worker; there is
no download or installation step. `memory_gb` is the operator's total GPU memory
budget for weights, KV cache, and concurrent engine slots. `platforms` limits
placement. `args` supplies model-specific llama-server options, including
context/template/sampling flags and `--embedding` for embedding models. The
adapter owns `--host`, `--port`, `--model`, and `--alias`; args cannot override
those. It supplies `--n-gpu-layers 99` by default. Supply the intended GPU-layer
count in args when the model needs another value.

Install an appropriate GPU-enabled `llama-server` build yourself: CUDA, Metal,
or Intel SYCL for `xpu`. Before running a real model, verify its official model
card, recommended generation settings, effective context/output limits,
reasoning mode, and chat template; record those with any evaluation. The
adapter does not guess generation settings or download a different model.
No real GPU/model validation is implied by laptop tests.

For a LAN worker, bind its authenticated model proxy to an address reachable
by the queue and advertise that exact URL:

```sh
# Set MYCE_TOKEN securely in both terminals; never put it in the catalog.
myce queue --model-config models.yaml --listen 192.0.2.10:7331 \
  --checkpoints /shared/checkpoints
myce join --queue http://192.0.2.10:7331 --name gpu-worker --gpu cuda \
  --memory-gb 48 --model-config models.yaml \
  --model-listen 192.0.2.20:7332 --model-advertise http://192.0.2.20:7332
```

The default proxy binds loopback on a random port for dev/local workers.
The actual engine always binds loopback; only the token-protected worker proxy
is LAN-visible. Model configuration hashes must match between queue and workers,
so a mismatched catalog fails visibly rather than loading another model silently.
`--model-idle` and `--model-wait` override YAML timeouts. These timeouts are not
part of the model identity hash. Queue and worker wait values should agree.
vLLM is deferred; unsupported engine names fail configuration validation.

## Scheduling, draining, and recovery

Calls share batch priority, loaded-model affinity, earliest deadline, and
waiting-age ordering. An already loaded eligible model is preferred. Otherwise
all fitting free workers are considered before making room. CUDA workers report
live free GPU memory from `nvidia-smi`, optionally capped with `--memory-gb`,
then reduced by `--reserve-memory-gb`. Resident models and running commands are
already reflected in that reading; only not-yet-reflected loading reservations
are subtracted by the queue. XPU/Metal/fake use an explicit configured budget
minus reserve and account for our batch/model reservations once. XPU/Metal
cannot observe changing external allocations with this configured source: choose
and maintain a conservative allowance. Workers recheck before every command or
model load. Each worker runs at most one batch, alongside models that fit.

Idle time starts after load readiness or the most recent call finishes.
`idle_timeout` defaults to ten minutes. A higher-priority batch or call can drain
lower-priority models to reclaim memory. Protection follows the highest priority
of currently admitted calls, not historical use; an idle model is reclaimable
as experiment priority. Draining stops new queue admissions;
already admitted calls may finish. The worker unloads only when both the queue's
admitted calls and its local in-flight calls reach zero. Memory is released only
after the worker reports successful unload. Failed unload retains its reservation
and appears in status. Lower-priority resumable batches may save and pause for
calls, following the batch stop-grace rule; non-resumable batches finish first.

`max_wait` defaults to five minutes and bounds queue/load waiting (including a
retry's placement). It is not an inference execution limit. Clients should use
a timeout that includes loading and inference. The load process itself has the
worker's configured maximum wait. A failed load returns a clear error and a
later request can try loading again.

Quiet-worker timeouts follow the highest-priority active workload (including
model loading/calls): production 5s, deadline 20s, experiment 60s, configurable
on the queue. Worker check-ins default to 1s. When a worker is marked lost, its
model slots disappear. A transport failure or worker loss retries a call once on a
different worker. Complete responses are buffered, so a partial non-streaming
response can be retried before returning any bytes. A chat stream can retry
before its first bytes; after it starts, a stream error event explains that
retry is unsafe. Callers must discard failed streams and decide whether a new
request is appropriate. If no alternative worker fits, the remaining wait
expires with a clear error. A strict-majority network outage retains assignments
rather than pretending every machine died.

Worker startup locks its spool, checks saved engine process identities, and
kills leftover owned engine process groups before reporting fresh. Engine logs
and identity records live in the worker spool. Engines never inherit `MYCE_TOKEN`.
Planned leave drains model calls before the worker disappears.

Model inventory is durable in the queue's SQLite state, but active HTTP bodies
and response streams are not replayable across queue restart. Those clients
receive a connection failure and must decide whether to resubmit. Restart
recovery waits for workers to report their loaded engines; stale sessions cannot
serve newly assigned calls. This is one queue and plain-command workers, with
no peer registry, leases, containers, or continuously reserved app GPUs.

`myce status`, `/api/status` (`loaded_models`), and the status page show each
model's worker, loading/ready/draining state, idle seconds, calls in flight, and
unload errors. In-flight calls show zero idle seconds.
