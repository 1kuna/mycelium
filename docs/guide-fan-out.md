# Declared independent pieces

The [slice 4 amendment](design-proposal.md#declared-fan-out-and-batch-planning-slice-4-107-approved-2026-10-24)
defines this contract. It uses ordinary extension jobs and existing artifact
transport. Applications supply homogeneous independent pieces and own semantic
splitting and reduction. Mycelium plans, retries and gathers them.

Declared calls fall back to serial admission when undeclared calls share the slot.

```yaml
name: parameter-sweep
project: research
priority: experiment
extension: simulator@1
workload: {input_shape: fixed-v1, units: 100}
files: {}                       # immutable common input manifest, or NAS inputs
outputs: {metrics.json: results}
fan_out:
  max_concurrency: 8             # ceiling, not a fixed batch size
  pieces:
    - {key: first, args: ["--seed", "1"]}
    - {key: second, args: ["--seed", "2"]}
```

Keys must be unique clean names; there are at most 256 pieces. `workload.units`
describes **one** piece, using the matching variant reference's unit and shape.
The existing `resumable` contract applies to each fixed wave. Arbitrary programs,
dynamic enumeration, general stage graphs and timed input accumulation are outside
this slice. Whole-parent dependencies below are the bounded #180 extension. Common inputs are staged through the normal file-store/NAS path for each
wave; arguments select the relevant data. No private input paths go in curves.

A variant opts into measured wave execution by adding `run` to its reference
sweep, alongside `command`, `counts` and `max_concurrency`:

```yaml
sweeps:
  extension:simulator:
    command: [python3, "{extension}/reference.py"]
    run: [python3, "{extension}/pieces.py"]
    counts: [1, 2, 4, 8]
    max_concurrency: 8
```

The author promises that `run` uses the same concurrency mode and shape measured
by the correctness-checked reference. No measured point grants resource capacity.
The queue conservatively scales per-piece CPU/RAM/device floors, includes measured
high-water memory and charges shared hosts once. Existing priority, fairness,
placement and owner controls still apply. Short groups and tails use smaller
measured counts. Without a safe matching wave recipe/curve, each piece uses the
ordinary serial command and its own arguments.

For a measured wave the worker sets `MYCE_BATCH_CONCURRENCY` and
`MYCE_BATCH_MANIFEST=.myce-batch.json`. This small JSON file is in the attempt's
working directory and contains `parent`, `pieces` (key and args), `concurrency`,
common `args`, `inputs`/`files` names and keyed `outputs` names. Write each declared
output under `KEY/OUTPUT_NAME`, e.g. `first/metrics.json`. The worker publishes
these through the existing attempt-specific verified artifact path. The manifest
name is reserved. A wave owns one command slot, including on GPUs.

The returned parent job's `fan_out` has a dispatch cursor, child job IDs and
`results` keyed by original piece key. Each result names the accepted execution
and its output references. The parent succeeds only after every piece succeeds;
its final `output_refs`/`output_files` use `KEY/OUTPUT_NAME`. A failed wave fails
the parent. An OOM forbids its exact measured count and fails the wave; it does
not qualify the same occupancy for another attempt. Infrastructure loss and planned interruption use existing retry
policy on that wave alone. Membership and measured count stay fixed across
retries, preserving the recipe's vector-batch semantics. A retry waits if no
component can safely admit that count and its resource envelope. Late attempts
cannot replace accepted results. Parent cancellation stops its children and
reports outstanding cleanup. Persisted membership/results survive queue restart.

## Observing parents and native children

Save the parent job ID returned by submission. For a keyed submission, bind that
ID using the exact `spec.project` and `spec.submission_key` echo in the Job reply
([identity contract](design-proposal.md#submission-identity-and-cancellation-80)).
Read the parent with authenticated `GET /api/jobs/{id}` (Go `GetJob`/`Wait`, Python
`get`/`wait`). Its top-level `fan_out.children` lists the native child job IDs
materialized so far; more can appear as dispatch proceeds. Fetch those jobs by ID.
Each child's top-level `batch.parent` is the parent job ID, and `batch.pieces`
identifies the original piece keys in that fixed wave. A child has its own job
ID and no `spec.submission_key`; the parent's key is not copied to children.
One child may execute several pieces, so do not assume one child per piece.

An assigned child's top-level `extension` reports the selected `name`, `version`
and frozen `digest`. This identifies the executable selection, not the parent:
many unrelated jobs can select the same extension. Use `fan_out.children` and
`batch.parent` for the relationship, and `fan_out.results` for accepted per-piece
executions and outputs. An unassigned logical parent may omit top-level
`extension` because selection occurs when a child is assigned.

GET `spec` is a redacted view, not the original submission. As documented in the
[status/Job view contract](guide-status-v3.md), GET retains payload
redaction while echoing project/key identity. It omits `spec.extension`,
`spec.extension_digest` and `spec.fan_out`, including the declaration's piece
arguments; observe top-level progress and child selection instead. Retain the
submitted declaration if you need arguments before dispatch. Use
`cancellation_scope.pieces` for every declared key, including undispatched keys;
`fan_out.children` lists only materialized waves.

## Abort and wait for release

Cancel the parent once with `myce cancel PARENT` or
`myce cancel --project PROJECT --key KEY`. Both routes fence all declared pieces
and registered dependents, including blocked/ready consumers and retried waves.
Valid future consumers of the retained cancelled parent are cancelled on admission.
Terminal results remain intact; cancellation adds durable exclusion metadata.

Read `GET /api/jobs/PARENT` (Python `client.get(parent_id)`) to observe
`cancellation.release`, or use `myce status PARENT`, which prints cancellation,
pieces and members. Wait until release is `confirmed` or `not_applicable`;
terminal job state alone does not establish release. `pending` covers all old
and current producer/consumer copies until exact cleanup. The logical parent
reports `confirmed` after attempted descendants settle even though its own
`release_confirmed` stays absent. Never-attempted cancelled members explicitly
report `not_applicable`, without manufacturing a physical release.

`cancellation_scope.pieces` includes each `key`, `materialized`, `accepted`,
optional `wave_id`, `will_not_run`, aggregate `release`, and registered dependent
IDs in `dependents`. Its `members` lists all retained wave/dependent jobs with
`job_id`, `state`, `will_not_run` and aggregate `release`. Empty lists are explicit.
Before abort, release only describes attempts so far; inspect `will_not_run`
for exclusion. No child cancellation loop is needed.

Scope survives restart and includes future registered consumers on later reads.
It enumerates retained records: jobs are not automatically removed; keyed
tombstones expire 30 days after explicit removal, followed by permanent digest
markers. Once the source is unavailable, GET cannot enumerate it and new
dependencies are rejected. Artifact expiry does not erase retained scope or
settle custody. Keep parent identity and acknowledgement locally across outages.

## Binding model-call curves

An extension-command sweep alone does not qualify server calls. Compute the
exact server configuration digest with the read-only command:

```sh
myce model measurement-digest MODEL --model-config models.yaml --environment-config environments.yaml
```

The reference sweep must declare that digest as `server_digest` and actually
exercise that exact model/server, shape, environment and cache/context settings.
The model catalog's `measurement` declares `extension_digest`, `work` (the exact
reference work type) and the same `server_digest`. Changing the model bytes,
command, settings, bounds or resolved environment invalidates the binding.
Existing variant correctness qualification and exact component/fingerprint,
allowance, freshness, OOM and measured-memory checks remain mandatory.

Requests opt in with generic headers `X-Myce-Fan-Out` (group key),
`X-Myce-Input-Shape` and `X-Myce-Work-Units`. Compatible pending calls in the same
project, class/tier, model, shape and size can use measured engine occupancy,
bounded by configured `max_in_flight`. A running wave's chosen occupancy is not
increased by later arrivals; a tail can use fewer slots. Unknown binding means
serial admission for declared calls. Requests without the declaration keep
their existing routing/caps, including zero-as-unlimited. Model calls retain
their existing ephemeral request/retry/streaming semantics; this is not a durable
workflow API or a second batching timer.

## Laptop simulator comparison

```sh
myce sim --fleet examples/sim/fleet.yaml --workload examples/sim/declared-fan-out/workload.yaml --what-if declared-batch --days 7 --seed 1 --json sim-results.json
```

This compares unsplit, naive split and declared execution using identical arrival
draws and policy. The added curves are explicitly synthetic work-units/second,
not measurements of any fleet. Execution timers use these rates while the real
queue chooses placement/count and handles preemption. The JSON records the
paired mean makespan gain, worst individual batch regression, production-p95
delta and gate result. A failed gate writes the JSON and exits nonzero. Each
comparison uses the same synthetic component inventory and shared host ledger;
virtual per-component workers retain the simulator's single-assignment transport.
Native qualification and
deployment require separate authorization.

## Start a consumer when one piece is accepted

Submit the producer first, then predeclare an ordinary next-stage job for each
known piece. The queue owns readiness even after the submitter exits:

```yaml
name: consume-first
project: research
submission_key: operation-42.consume.first
extension: summarizer@1
after:
  job: FULL_PRODUCER_JOB_ID
  piece: first
  outputs: {input.json: metrics.json}
```

`outputs` maps the consumer's local input name to the producer's declared output
name. The full parent ID, project, piece and output names are checked at submit.
One selected-piece edge is supported; it cannot be combined with `fan_out`, checks, programs,
Docker or a caller-selected working directory. Dependency inputs cannot collide
with other inputs or outputs, or mix stored-file and NAS input transports.

The wire state stays `queued`. `dependency.state` is `blocked` until acceptance,
then `ready`; `dependency` records the parent, piece, exact producing `execution`,
`ready_at`, and immutable `files` or NAS `inputs`. The submitted `spec.after`
remains unchanged. A measured wave accepts all its pieces together after its
complete output manifest is verified. Availability means normal scheduling
eligibility; it reserves no resources. Consumers stage and verify the bound input
through the existing artifact transport before command execution.

The generated production start deadline and runnable wait age start at readiness.
Declare `start_window: 600s` on a production consumer job to give it a ten-minute
relative budget. Omission keeps the `150s` default; experiments omit it and have
no start deadline. The declaration also works on a selected adapter entry in
`.mycelium/adapter.yaml`. It accepts positive Go duration strings up to `1h`;
the cap keeps unavailable capacity/preparation waits bounded while allowing
contention. Longer budgets delay startup failure accordingly.
Explicit `start_by`, `give_up_at` and `deadline` remain absolute. Accepted pieces
survive sibling failure; consumers of unaccepted pieces fail with structured
`diagnostic_code: upstream_failed`. Cancelling the parent is a pipeline abort,
including after gather: unfinished consumers stop, future consumers are cancelled,
and the cancellation acknowledgement stays `release: pending` until all exact
producer/consumer copies are released. Terminal results remain intact. Cancelling
one consumer has no reverse effect.

Producer gather still requires every producer piece and does not wait for
consumers. Save the consumer IDs if the application needs all downstream results.
Submission keys make an interrupted finite submission list safe to resume; they
do not make the pipeline atomic. Partial parent results and bound consumer inputs
use their existing artifact retention windows. A new binding to an expired result
returns HTTP 410 `result_expired`; an existing key still replays its original job.

## Start a consumer after every producer piece succeeds

Omit `after.piece` to wait for the whole fan-out parent. Map its gathered
`KEY/OUTPUT_NAME` names to explicit local inputs. Even when only one output is
mapped, every declared producer piece must succeed before the consumer is ready:

```yaml
name: reduce
project: research
run: {command: [sh, -c, 'cat first/input second/input > combined']}
needs: {platform: cpu, cpu_cores: 1, memory_gb: 0.1}
after:
  job: FULL_PRODUCER_JOB_ID
  outputs: {first/input: first/metrics.json, second/input: second/metrics.json}
outputs: {combined: results}
```

The consumer may itself declare `fan_out`:

```yaml
name: next-stage
project: research
class: production
start_window: 600s
extension: summarizer@1
workload: {input_shape: fixed-v1, units: 100}
after:
  job: FULL_PRODUCER_JOB_ID
  outputs: {first/input: first/metrics.json, second/input: second/metrics.json}
outputs: {summary.json: results}
fan_out:
  max_concurrency: 8
  pieces:
    - {key: first, args: [--input, first/input]}
    - {key: second, args: [--input, second/input]}
```

Inputs are explicit immutable common inputs, staged for each wave. Piece args or
the existing batch manifest select the keyed paths; there is no implicit key
substitution or input filtering. A measured wave's manifest lists these bound
inputs along with submitted common inputs. Register subsequent consumers against
this stage's parent ID, then wait only on the final job. No caller-side gather wait
is needed. Membership and accepted bindings survive queue restart.

The logical consumer's generated deadline begins at readiness. Each executing
child inherits `start_window` and gets that full relative budget from its own
assignment, including later waves. An explicit absolute `start_by` takes
precedence and is preserved by every child instead. If it expires before all
pieces are assigned, the parent fails without dispatching an expired tail.
After a started child loses
its machine, recovery gets the original window length from loss detection;
unstarted retries keep their cutoff. Job GET shows `spec.start_window`, current
`spec.start_by`, and effective top-level `start_window` in nanoseconds. See
[production start deadline](job-reference.md#production-start-deadline).

For this form `dependency.executions` records the exact accepted producing
execution for each mapped source key. `dependency.execution` is zero-valued;
it applies only to the selected-piece form. Parent failure fails a blocked
consumer with `upstream_failed` without an attempt. Cancellation propagates through
downstream parents, their children and ordinary consumers, and release includes
every exact attempt. Terminal results remain intact. Existing readiness clocks,
absolute deadlines, expiry and transport checks still apply.

Only one already submitted fan-out parent in the same project may be named.
Ordinary-job sources, multiple-parent joins, forward references, dynamic piece
creation, timed accumulation and atomic graph submission remain out of scope.

## Per-recording stage chains

For an ASR → separation → naming → render pipeline, submit a separate fan-out
parent for **each stage of each recording**. Naming can be a one-piece fan-out;
render is the ordinary sink. Submit stages in order using their returned parent
IDs, then wait only on render. Different recordings' chains can progress
independently.

A stage-wide fan-out across recordings cannot feed independent per-recording
downstream stages as each recording becomes ready: `after.piece` supports only an
ordinary sink, ordinary results cannot be upstream sources, and a whole-parent
edge waits for every recording in that parent. Keep the parent boundary at one
recording when you need per-recording progress.

This illustrative recipe uses two known chunks and extension-defined filenames,
arguments and workload shapes. Supply the actual extension recipes and immutable
ASR inputs for the recording; Mycelium does not discover chunks:

```python
project, recording = "research", "recording-1"

def stage(name, pieces, outputs, *, after=None, files=None):
    return client.submit({
        "name": f"{recording}-{name}", "project": project,
        "submission_key": f"operation-42.{recording}.{name}",
        "extension": f"{name}@1",
        "workload": {"input_shape": f"{name}-v1", "units": 1},
        "files": files or {}, "outputs": outputs,
        "fan_out": {"max_concurrency": 8, "pieces": pieces},
        **({"after": after} if after else {}),
    })

asr = stage("asr", [
    {"key": "a", "args": ["--input", "a.wav"]},
    {"key": "b", "args": ["--input", "b.wav"]},
], {"words.json": "results"}, files=recording_files)
separation = stage("separation", [
    {"key": "a", "args": ["--input", "a/input"]},
    {"key": "b", "args": ["--input", "b/input"]},
], {"segments.json": "results"}, after={"job": asr["id"], "outputs": {
    "a/input": "a/words.json", "b/input": "b/words.json"}})
naming = stage("naming", [
    {"key": "whole", "args": ["--input", "a.json", "--input", "b.json"]},
], {"labels.json": "results"}, after={"job": separation["id"], "outputs": {
    "a.json": "a/segments.json", "b.json": "b/segments.json"}})
render = client.submit({
    "name": f"{recording}-render", "project": project,
    "submission_key": f"operation-42.{recording}.render", "extension": "render@1",
    "after": {"job": naming["id"], "outputs": {"input.json": "whole/labels.json"}},
})
result = client.wait(render["id"])
```

Repeat those submissions with a distinct recording identity/key namespace for
each recording, registering every chain before waiting on the final sink IDs.
