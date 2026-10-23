# Start here: use Mycelium v3

Mycelium shares GPU work across projects through one queue. Start with the
smallest GPU operation that preserves your existing inputs, outputs and runtime.
The [approved design](design-proposal.md) is the only product spec. CPU watching,
parsing, request coordination and publication stay in the caller.

## Pick your path

| Need | Guide |
| --- | --- |
| A command that finishes | [Keyed submission and cancellation](integrating.md#keyed-submission-and-cancellation), [job fields](job-reference.md#batch-yaml) |
| Long work that survives stopping | [Resumable jobs](guide-resumable-jobs.md), [Python checkpoint helpers](../clients/python/README.md#resumable-commands) |
| OpenAI chat, completion or embedding calls | [Model calls](guide-model-calls.md) |
| Other model HTTP interfaces | [General servers](guide-servers.md) |
| Dependencies on workers | [Software environments](environments.md) |
| Inputs, outputs, models and checkpoints without a NAS | [Files](guide-files-v3.md) |
| Describing a kind of work or program | [Extensions](write-an-extension.md), [placement](guide-placement-v3.md) |
| Adding a worker | [Joining](guide-joining-v3.md), [polite daily-use participation](guide-polite-hyphae.md) |
| Installation, ownership or recovery | [Operations](operations.md), [typed status](guide-status-v3.md) |

Commands and extensions are supported paths in the same runtime. Fake inventory
runs actual laptop commands; it cannot qualify a real GPU library or model.

## Add a hypha

Run `myce invite` on an authorized installed hypha, then paste the one output line
on the new machine. No Go/git or credential-file transfer is needed. The queue
must have an approved release; [joining](guide-joining-v3.md) covers that setup,
NAS prerequisites, readiness states and upgrades. `--out FILE` optionally saves
the command. A polite waiting enrollment succeeds without claiming work is ready.

## Try it on a laptop

Follow the [laptop quick start](try-it-locally.md#five-minute-start). Keep dev
running and use its loopback address and synthetic `dev-token` only for the demo.
Use a fresh working directory for isolated state; if port 7331 is occupied, choose
another free port via `dev --listen 127.0.0.1:PORT` and use that port in callers.
Do not stop another process to free a demo port.

For a Python caller, install from the checkout into your project's own Python
3.10+ environment. From the Mycelium checkout:

```sh
python -m pip install './clients/python[yaml,openai]'
```

From another repository use the actual checkout location, supplied privately.
Dict submission needs neither optional dependency; YAML needs `yaml` and OpenAI
calls need `openai`. See [the Python client](../clients/python/README.md).

## Waiting for long jobs (agents)

Record the full job ID. Start **one** detached `myce wait ID --timeout 2h`, with
its output and exit code recorded, then end the agent turn. Read logs/status
once after that wait exits. Do not keep an AI agent polling a running job.
Use separate output/exit files for each wait.

Wait exits `0` for all success, `1` for failed/cancelled, `2` for usage/auth/API
errors, `3` for timeout and `4` for interruption. It reconnects through transient
outages. Timeout/interruption does not cancel work or prove completion: keep the
IDs and arrange another single wait if necessary. See [wait flags](job-reference.md#cli-flags-and-defaults).

## Integration checklist

1. Identify the GPU boundary, existing runtime/model identity and complete-result
   checks. Keep CPU callers outside. Use a project-owned switch with explicit
   tests for the old and queue paths; do not silently use another GPU owner.
2. Configure queue credentials privately. Never copy the caller's whole environment
   into a job, publish its queue address, or pass the fleet token in argv/job YAML.
   The worker strips `MYCE_TOKEN`; other application secrets need explicit care.
3. Persist a unique `(project, submission_key)` and its canonical spec before
   submitting. Retry that identical spec only while the consumer operation is
   open. After it closes, cancel by key and retain exact acknowledgement coverage.
   See [the worked example](integrating.md).
4. Choose priority, platform, measured memory, software environment and placement
   deliberately. Production can preempt non-resumable work. Missing capacity,
   model files or startup still adds latency; a production pin must become ready
   before directing traffic to it. See [scheduling](job-reference.md#scheduling-and-results).
5. Arrange code and immutable inputs on eligible workers using explicit NAS
   mounts or verified file transport. Submission does not copy your repository.
   Do not change model, precision, context or preprocessing during integration.
6. Make checkpoints atomic and attempt-specific before setting `resumable: true`.
   Save periodically and on SIGTERM; test stop/save/restart on a later attempt.
   A cancellation test alone does not prove resume. Keep outputs safe to repeat.
7. Test the real dev API/command runner, including lost submission replies,
   conflicting keys, cancellation release, stale results and failure reporting.
   Assert that executed commands cannot read the fleet token. Fake model answers
   prove transport mechanics only.
8. Persist consumer cancellation and late-publication fences independently.
   Use [queue maintenance](integrating.md#maintenance) and exact release status,
   then verify the application's own identity/health before routing traffic.
9. Record the commands, result identities, complete outputs, checkpoint behavior
   and remaining native checks. Keep deployment approval and hardware qualification
   separate from laptop implementation proof.

## More references

[Job reference](job-reference.md), [operations](operations.md),
[native test plan](production-test-plan.md), [deployment templates](../deploy/README.md),
[sharing and usage](guide-fairness-usage.md), [onboarding benchmark](guide-benchmark-v3.md)
and [synthetic scheduler simulation](simulator.md) cover the remaining details.
