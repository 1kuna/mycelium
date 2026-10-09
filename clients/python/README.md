# Mycelium Python client

Python 3.10+, no runtime dependencies for dict submission, queue access, or
checkpoint helpers. Install locally:

```sh
python -m pip install ./clients/python
python -m pip install './clients/python[yaml,openai]'
```

For a repository URL, substitute its owner and immutable commit/ref:

```sh
python -m pip install 'mycelium-client[yaml,openai] @ git+https://github.com/OWNER/mycelium.git@REF#subdirectory=clients/python'
```

```python
from pathlib import Path
from mycelium import Client

client = Client()  # MYCE_QUEUE (default localhost:7331), MYCE_TOKEN required
job = client.submit(Path("job.yaml"))  # yaml extra; str means YAML text
# Or client.submit({"name": ..., "project": ..., "priority": ..., ...})
for chunk in client.logs(job["id"], follow=True):
    print(chunk["text"], end="", flush=True)
result = client.wait(job["id"], timeout=600)
assert result["state"] == "succeeded", result.get("reason")
print(client.status())  # machines, jobs with waiting reasons, loaded models
print(client.list())
print(client.get(job["id"]))
client.cancel(job["id"])
```

Dicts and YAML accept the design's platform list / `any` shorthand and named
environment strings; the client normalizes them to API objects without mutating
inputs. YAML timestamps must include a timezone. Unknown fields and invalid
job specifications fail through the server's validation. `APIError` retains
HTTP status and body. Network errors propagate; submission is not retried
implicitly. `wait` returns all terminal states, including failed/cancelled, and
raises `TimeoutError` on timeout. Timing out does not cancel the job.

Logs yield records with `seq`, `attempt`, and `text`, fetched incrementally from
the real logs API. `follow=True` polls until terminal state, then drains remaining
logs. Save the last `seq` and pass `after=seq` to reconnect without duplicates.
Without follow, all currently available pages are drained. Close the generator
to stop following; an HTTP call can block for the configured request timeout.

## Standard OpenAI SDK

```python
with client.openai(project="atlas", priority="production") as sdk:
    vector = sdk.embeddings.create(model="my-model", input="query")
    response = sdk.chat.completions.create(
        model="my-model", messages=[{"role": "user", "content": "Hello"}],
        stream=True,
    )
    for chunk in response:
        print(chunk)
```

This returns the standard SDK and sets `X-Myce-Project`, `X-Myce-Priority`, and
optional `X-Myce-Deadline` headers. Deadline priority requires an RFC3339 deadline.
The helper defaults to a 600-second SDK timeout and disables automatic retries
to avoid silently replaying calls. Both can be overridden with SDK kwargs.
Model serving must be configured on the queue/workers; the client installs no
engines. The dev server exposes `dev-model` through its fake engine.

## Resumable commands

```python
from mycelium import Checkpoint, StopSignal

checkpoint = Checkpoint()  # requires MYCE_CHECKPOINT_DIR
step = checkpoint.load(0)
with StopSignal() as stop:
    while step < 100 and not stop.requested:
        # Do one bounded unit of work; make its outputs safe to retry.
        step += 1
        checkpoint.save(step)
    checkpoint.save(step)  # final save before exit
```

The helper uses the worker's current attempt directory. Workers copy the newest
previous attempt's saves there. `save(value, name="checkpoint.json")` writes one
JSON value to a unique same-directory `.tmp`, flushes and fsyncs it, atomically
replaces the target, then fsyncs the directory. `load(default=None)` selects the
newest complete `.json` by filesystem modification time, ignoring temporary
files. Do not mix unrelated JSON files into this directory. Invalid complete
JSON fails loudly. I/O failures propagate: log and retry failed NAS saves in
your command; do not claim progress was saved. Failed temp files remain for
inspection. Periodic saves protect against abrupt loss; signals only help with
planned pauses. `StopSignal` sets `requested` on SIGTERM/SIGINT and restores
previous handlers on exit. Use it on the main thread and save outside the signal
handler. See `docs/guide-resumable-jobs.md` for NAS and retry requirements.

## Tests (no GPU)

```sh
python -m pip install -e './clients/python[yaml,openai]'
python -m unittest discover -s clients/python/tests -v
```

Tests build the local Go CLI into a temporary directory and launch `myce dev`
on an ephemeral loopback port with one fake GPU worker and real command runner.
They cover submission, wait/failure/timeout, authentication, paged log cursors,
cancellation, YAML, SDK embeddings/chat/streaming, atomic checkpoint failure,
signals, and production preemption/resumption. Optional-extra tests skip when
those extras are absent. No fleet access is required.
