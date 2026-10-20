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

For retryable submissions, store a stable `submission_key` in the job mapping or
YAML before the first POST. Keys are unique within a project. Repeating the same
spec/key returns the original job, including terminal jobs. A changed spec raises
`APIError` with `status == 409` and `code == "submission_key_conflict"`. Expired
keys raise status 410 with code `submission_key_expired` and cannot create jobs.
A cancelled key reservation returns its terminal cancelled job for any spec.
Keep the exact original submitted spec for retries; generated defaults in the
returned job are not part of that original request.

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

The helper uses the worker's current attempt directory. Workers copy the highest-generation
previous attempt's saves there. `save(value, name="checkpoint.json")` writes one
JSON value to a unique same-directory `.tmp`, flushes and fsyncs it, atomically
replaces the target, then fsyncs the directory except on positively identified
Linux CIFS mounts, where directory fsync is a kernel no-op. Unknown mounts retain
directory fsync; file fsync and atomic rename always remain. NAS/server durability
and mount options still require qualification. `load(default=None)` selects the
highest-generation complete `.json` (legacy files rank at zero; ties use the
lexicographically greatest filename), ignoring temporary
files. Do not mix unrelated JSON files into this directory. Invalid complete
JSON fails loudly. I/O failures propagate: log and retry failed NAS saves in
your command; do not claim progress was saved. Failed temp files remain for
inspection. Periodic saves protect against abrupt loss; signals only help with
planned pauses. `StopSignal` sets `requested` on SIGTERM/SIGINT and restores
previous handlers on exit. Use it on the main thread and save outside the signal
handler. See `docs/guide-resumable-jobs.md` for NAS and retry requirements.

## Tests (no GPU)

```sh
# From the repository root, using an installed Python 3.10+ (here, Python 3.14).
PYTHONPATH=clients/python/src:clients/python/tests python3.14 -m unittest test_client.DevTests.test_production_start_deadline -v
PYTHONPATH=clients/python/src python3.14 -m unittest discover -s clients/python/tests -v
```

Tests build the local Go CLI into a temporary directory and launch `myce dev`
on an ephemeral loopback port with one fake GPU worker and real command runner.
They cover submission, wait/failure/timeout, authentication, paged log cursors,
cancellation, YAML, SDK embeddings/chat/streaming, atomic checkpoint failure,
signals, and production preemption/resumption. No client installation or optional
dependency download is needed for this run. Without the optional extras, only
`DevTests.test_yaml` (`install yaml extra`) and `DevTests.test_openai_sdk`
(`install openai extra`) may skip. With both extras installed, those tests run
against the local dev server too. No fleet access or paid service call is required.

Saves contain `{"myce_checkpoint": {"generation": N}, "value": ...}`; `load()`
returns the original value. Each save increments the highest generation retained
in the attempt directory, including after worker resume or helper restart. Use one
writer per attempt; file mtimes and machine clocks do not affect resume ordering.
Existing unwrapped JSON checkpoints still load. See `docs/guide-resumable-jobs.md`
for worker selection and non-JSON atomic-manifest rules.

## Wrap an existing finite app stage

Give your agent the [extension creation guide](../../docs/extensions/AGENT-GUIDE.md)
for a complete submit/wait/result example. `upload_file` stages hash-addressed
inputs; `submit_and_wait` requires a stable submission key and retries one lost
transport response; `download_result` resolves accepted output and verifies its
hash before replacing an explicit destination. `cancel_submission(project, key)`
also works when the submit response was lost. HTTP conflicts propagate.
