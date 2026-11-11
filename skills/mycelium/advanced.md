# Mycelium consumer contracts

Start with [the first-run guide](SKILL.md). For an optional behavior, check the
required installed CLI, client, queue and worker components using
[capability evidence](references/release-behavior.md#capability-evidence).
The source reference and its release notes do not establish deployed support.

## Submit

Use an existing adapter entry with `myce run --entry NAME --key KEY`, or a
compatibility profile with `myce run --profile NAME --key KEY`. For a one-off,
`myce run --cpu N -- CMD` supports CPU work; `--gpu cuda|xpu|metal` requests an
accelerator, `--mem` then describes device memory, and `--host-mem` host memory.
One-offs have no key/notifier/input-file contract; use an adapter for those.

Run `myce spec check --resolve` first. For adapters, select `--entry NAME` and
the file. Resolve does not submit, upload, prepare an environment or qualify a
device. An unchecked dependency, unknown observation or missing qualification
has an owner action; a fitting target is not evidence of native output parity.

Describe needs, class and verified targets; submit the work without a host or
device selector. Memory may be omitted for automatic learning; declared limits
retain their containment meaning. Register supported implementations as variants
of one extension, with each variant's target, resources, environment and native
check. Mycelium selects among qualified variants automatically. A single
implementation for one OS/architecture/backend is a real coverage limit; a pin
does not make it portable. Use `myce portability --extension NAME@VERSION` for
the observed gaps and preparation actions. Preparation, immutable dependency
locks and a successful smoke test do not establish MY-19 production parity.
Native variants need their own qualification and output criteria.
See [adapters/variants](../../docs/adapter.md),
[environments](../../docs/environments.md) and [scheduling](../../docs/scheduling.md).
Check [setup](setup-project.md) before using managed CPU target discovery.

Prepare directory or selected committed Git source as described in SKILL.
Keep private receipts; the same key recovers accepted intent, whereas changed
inputs/recipe/intent need a new key. Correct a deterministic environment failure
in its immutable recipe; a new key alone cannot repair the same failed recipe.

Plain native commands are unsandboxed by default under MY-25. New native
extension registrations and retained policies have their separate contracts.
Use explicit sandboxing or managed Docker for the supported enforced boundary;
read [native isolation](../../docs/native-isolation.md) for platform limits.
Declare tool, environment, network and model dependencies through their
[supported adapter fields](../../docs/adapter.md); hidden shell dependencies
cannot be inferred by Mycelium. Model snapshots need immutable identity and
verified bytes; qualification/parity is separate from loading a snapshot.
From the release implementing MY-44, no `models` policy is needed: every model
repository is permitted by default. Optional `models.sources` positive patterns
restrict repositories; `!OWNER/NAME` patterns deny them, with deny taking
precedence. Empty sources impose no restriction. `myce model fetch` also accepts
any HTTPS file URL with the required exact size and SHA-256, rather than a fixed
download host. Size/cache limits still apply;
omitted/zero `free_floor_bytes` uses the worker cache volume's physical headroom
rule (1% of capacity, at least 256 MiB). See [model snapshots](../../docs/files.md#declared-model-snapshots-553).

Extension registration is content freezing, not an owner review; native checks
run automatically on first demand against the author's baseline. A failed check
still holds that extension. `unsandboxed: legacy extension registration` is a
status diagnostic, not an approval request; authors can choose explicit isolation
for new versions. Consumer result checks, production output parity and explicit
host restrictions retain their contracts. The [execution gate audit](../../docs/execution-gates.md)
explains the remaining protections.

Stage NAS inputs locally before processing for every size (MY-36). Declare
read-only shared inputs with relative NAS source, SHA-256 and size, then read
`$MYCE_INPUT_SOURCE`; declared legacy path inputs copy into their local attempt
folder. Resolve alone does not verify worker NAS bytes. For submitter-local
bytes use the supported upload/local-input contract. Preserve integrity and
capacity refusals; re-uploading identical oversized inputs cannot make them fit.
Mutable `MYCE_CHECKPOINT_DIR` state is not an immutable input-media cache. See
[files/checkpoints](../../docs/files.md) and [cache behavior](references/release-behavior.md#other-release-dependent-contracts).

For resumable work, implement atomic checkpoints and actual restore before
claiming `resumable: true`. Safe movement requires a compatible qualified
resume/result contract, committed progress and exact release; a stop request is
not migration. Disposable non-resumable experiments need the explicit supported
preemptible contract, not an inference from experiment class. See
[checkpoint/yield contracts](../../docs/files.md) and [scheduling](../../docs/scheduling.md).

## Explicit placement restrictions

Use `allowed_machines`, an approved host exclusion, or `needs.gpu.devices` only
when the task actually restricts hardware: a specific physical device, an
owner-approved dedicated machine, an explicit prohibition on a personal host,
or a native qualification/benchmark of exact hardware. Record the requirement
and evidence in `requirement_reasons`. Capacity waiting, urgency, a previous
incident, cached software, or a machine's familiar name are not requirements.
Declare the environment and stage inputs instead of pinning to their location.
Application backend flags are fine when they consume the worker's assigned
visibility; do not set `MYCE_DEVICE` to choose a physical card before submission.

A configured project/work `production_parity_hosts` list is an owner restriction
on every production placement, including fan-out and recovery. Mycelium already
enforces it; consumers need no matching job pin. Native qualification, measured
quality and historical registration cannot admit an unlisted host. An empty
list holds production; only the owner can widen it after output comparison.
Checks and experiments can collect that evidence. A removed host's queued
binding is placed again; running execution and release custody stay with the
current attempt. Parity-held production cannot withhold another project's
environment preparation on an unapproved host.

For throughput comparisons, declare the implementations and comparable workload
identity so Mycelium learns placement from accepted measurements. Use the exact
hardware qualification path only when measuring or qualifying that hardware is
the task; ordinary final runs go through the unpinned adapter entry.

## Wait and check status

Use one foreground `myce wait JOB_A JOB_B` for an owned batch on supporting
clients (at most 256 distinct IDs in the source-audited CLI). `--json` adds full
diagnostics when available; state/reason observation can continue if optional
detail reads are unavailable. Scalar batch observation carries revisions and
both supported releases provide it. Queues below the current/previous release
floor must be upgraded; clients do not fall back to per-job polling. `myce status JOB_ID` is the
explicit diagnostic read; keep full/history status out of loops.

Python callers use `Client.from_installed()`, `client.wait(JOB_ID)` or
`client.wait_many([JOB_A, JOB_B])`; a list returns terminal Jobs in input order.
Preserve the installed pinned opener. See [Python client](../../clients/python/README.md).

`myce wait --timeout DURATION` and Python caller timeout bound observation;
timeout/interruption leaves the job alive. Without an owner/job deadline there
is no fixed observer lifetime. Follow current Retry-After and sanitized retry
causes. Pending work is not failure; a notification is not accepted output.

For unconfirmed input storage/submission, retain receipt/key and rerun the
original saved command with frozen inputs. Preserve a supplied notifier on
replay. A failed observation or delivery does not create a new job. Newer
clients recover interrupted transfers/queue restarts under the caller's bound;
permanent identity, authentication, integrity and configuration failures retain
their typed stop. Older clients may need the installation owner to verify
unchanged trust before same-key replay. See [transfer/recovery contracts](../../docs/files.md)
and [release-dependent changes](references/release-behavior.md#other-release-dependent-contracts).

Profile/adapter receipts use one managed observer per user registry across
worktrees. It intentionally survives submitter exit; ordinary foreground waits
end on parent exit. Never start `--observer-dir --detached` yourself or create
per-job detached waits. Inside managed attempts/dev sessions, the observer stays
inside that session's cleanup custody.

On supporting clients, `myce wait --observer-status` reads local build/phase/read/
delivery/error metadata and lifetime-lock ownership without queue requests.
A held lock proves ownership, not advancement, receipt delivery or receiver work.
A quiet held long-poll can be healthy without a state change. Missing legacy
metadata or an old phase is unknown evidence, not a cause diagnosis.
Cooperative upgrades require compatible running owners; a new CLI binary does
not replace an existing observer image. On the release containing #919's
supervision fix, observer child death and silent coordinator/authority reads
recover automatically, preserving durable arms and pending delivery IDs. Local
status includes retained `supervision` recovery evidence. Native login/boot and
supervisor-process recovery remain #1166; legacy/unsupervised-owner recovery is an
operator action that preserves all durable arms, proves exact owner exit and
then replays the original key/binding. See [operator recovery](../../docs/reference/completion-observer.md#completion-observer-upgrade-and-recovery-next-release).

For queue releases containing the established-response shutdown repair in #1135,
cutover closes interrupted TLS sockets before another encrypted record can be
sent. Earlier queues can produce a real bad-record-MAC refusal during shutdown
even with unchanged trust. Keep integrity refusals permanent and report them
to the authority owner. Same-key recovery still requires that owner's
verification of unchanged trust and identity; never broaden the client TLS
classifier to recover a watch. Verify the running queue separately from CLI
and observer versions.

For detached completion, use [T3 binding](t3-binding.md). The notifier deduplicates
stable delivery_id across ambiguous acknowledgement/restart. Local receipt
`delivery_status: delivered` records notifier success; transport acceptance,
durable receiving-thread arrival and owner disposition are separate evidence.
Do not claim them from a receipt or PID alone.

- For a long detached job, give it one stall check at about twice its expected duration, bound to your thread, verifying one thing (state or log growth), and delete it when done. If duration is unknown, record that uncertainty and a next owner for follow-up rather than inventing an ETA; use the foreground wait while this session owns completion.

## Stage progress and honest ETA

**Requires matching progress/runtime support; see [capability evidence](references/release-behavior.md#capability-evidence):** report current-stage work through the existing
`MYCE_PROGRESS_FILE` contract, without credentials or log parsing. For example,
a two-stage CPU command can publish at each batch and stage transition:

```python
import json
import os
from pathlib import Path

progress = Path(os.environ["MYCE_PROGRESS_FILE"])

def report(stage, completed, total, unit, *, total_work_known=False):
    temporary = progress.with_name(progress.name + ".tmp")
    temporary.write_text(json.dumps({
        "stage_index": stage, "stage_count": 2,
        "completed": completed, "total": total, "unit": unit,
        "total_work_known": total_work_known,
    }))
    os.replace(temporary, progress)

report(1, 10, 10, "records")       # Stage one finished; job is still running.
report(2, 0, 100, "iterations")    # Stage-two duration has no timing evidence.
# Run refinement, reporting batches; write outputs and exit normally afterward.
```

Indices are one-based. Omit `stage_count` if later stages are not yet known;
omit both stage fields if stage identity is unknown. `total_work_known` defaults
false; a known stage total alone never makes the entire workload known. Set it
true only when the complete workload across all stages is known. Counts remain
stage-local, even then: unequal stages cannot be averaged into a job percentage.
Rewrite at least every 30 seconds to keep reporting evidence fresh.

Read `myce status JOB_ID` or Python `Client.get(JOB_ID)`: `application_progress`
has stage index/count, work-known declaration, exact attempt, age and `stale`.
`runtime` separately has elapsed and remaining time, source, sample count,
history age and limitations. Missing timing stays unknown; completing stage one
never yields zero whole-job remaining. Historical ETA must describe the complete
pipeline under comparable workload/reference identities, not just its first
stage. An exceeded estimate becomes unknown. See the
[report contract](https://github.com/1kuna/mycelium/blob/main/docs/status.md#execution-elapsed-time-remaining-estimates-and-application-progress).

Checkpoint `progress` is a durable generation number, not a percentage or ETA.
Its existence does not prove that all stages or outputs are recoverable. Neither
stage completion nor a checkpoint means job success or resource release: keep
the same job ID, wait for success, and verify the usual results.

## Mycelium repository merge gates

Read [the repository merge procedure](../../docs/development.md#local-merge-proof)
and [current rulings](../../docs/DECISIONS.md#my-41). Under MY-41, merge on quick
plus targeted tests, run the full suite once per release batch, and fix breakage
forward. No independent review or per-PR full is required. Development gates,
admission and stale custody may be repaired or reset when they block iteration;
protect running production jobs and secrets. Use the local gate for development;
fleet compute remains available within the task's authority. Preserve actual
failed, interrupted and skipped results. This consumer reference grants no
deployment authority.

## Results

`myce result JOB_ID --out DIR` retrieves accepted output and verifies manifest
sizes/hashes. Apply the project's own output acceptance too. Failed jobs have no
accepted outputs; `myce result JOB_ID --attempt N --out DIR` requests diagnostic
artifacts, which may be unavailable. A recovered result is a separately
identified artifact and does not change the original execution verdict.

Workers and queues with #871's finalization correction retain
`execution_cause.finalization`: `step`, declared `output`/`destination`, a fixed
safe `error`, optional HTTP status and `command_exit_code: 0` when observed.
The queue binds names to the frozen output declarations; expanded worker roots
and transport text are excluded. Finalization failures have a metadata-only
`attempt_history[].diagnostics.summary` when artifact capture was not requested
or yielded no files. `result --attempt N` writes that summary as `summary.json`;
its size/hash is computed from the recorded metadata, without requiring a blob
upload. Optional logs/reports still require diagnostic capture and storage.
Queue-backed outputs must be regular files; archive a directory or declare its
individual files. A pending upload/reservation retains bytes and retries without
turning a successful command into an infrastructure loss or accepted result.

For large outputs, publish to the project's own storage in the job and return
a small path/size/SHA-256 receipt. Verify downstream copies and complete every
use before releasing only your own terminal results:

```sh
myce file release plan --project P JOB_ID... > release-plan.json
# Review files, protections and all downstream custody before this irreversible apply.
myce file release apply --accept-expiry release-plan.json
```

`file_release_stale` calls for a new reviewed plan; protected/live/unresolved
custody cannot be forced. Collection is asynchronous and released references
return result_expired. Project attribution is not an authorization check.
Keep plan/apply receipts and verified copies. Omitted retention protects until
explicit release only on a supporting queue; saved positive windows still bind.
See [result retention and release](../../docs/files.md) and [operations](../../docs/operations.md).

## Urgency and pausing

Class expresses urgency: production only for work that the project's rulings
call production; deadline only with a real owner-set date; experiment otherwise.
Production can preempt lower tiers; eligibility, qualification, capacity and
exact release still apply. Equal-tier work normally waits. Messages asking
other consumers to stop do not alter queue order.

Use job-bound validation reservations for isolated qualification, not an
invented project hold or higher class. `myce extension check --reserve` binds
one; `myce reserve add SELECTOR --job JOB_ID --owner OWNER --reason TEXT` binds
an existing validation job. Device checks default to their selected device;
whole-host isolation needs its actual reason. Inspect reserve list after an
unconfirmed write before retrying the same explicit --id. Never infer a confirmed
reservation from an empty response. See [validation reservations](../../docs/operations.md#reserve-a-host-or-its-devices-for-an-existing-validation-job).

Pause only your own queued job with `myce pause JOB_ID --reason TEXT`; optional
`--until RFC3339` resumes at the requested time. `myce resume JOB_ID` retains its
key/queue age. Assigned/running attempts cannot be paused; do not cancel and
resubmit to change their place. Shared policy belongs to the queue owner.

## When a job waits

Read its waiting reason once, then fix your authored needs/input/recipe or name
the next owner and action on the existing issue. Unknown telemetry, unconfirmed
cleanup, build offers and prepared environments are different states; only
actual fresh reports prove progress. Do not erase custody, raise shared budgets,
repeat submissions or run heavy work locally to bypass a hold.

On queue builds with placement coverage diagnostics, `missing_extension` needs
a registered implementation; `execution_coverage` needs a compatible variant;
`native_qualification` needs its exact native check; `production_parity` or
`production_parity_hosts` needs reference-output evidence and the stated owner.
These differ from occupied capacity or environment preparation. Detailed status
also explains a single freshly observed compatible host and an owner's parity
list size. These are current coverage limits, not instructions to add a pin.
Stale or missing inventory is unknown, not proof that hardware is unique.

Default status is a partial history view. Prefer the known job ID or batched
wait; full history/paging is explicit. Estimates require comparable complete
work and remain uncertain, not deadlines. Distinguish queue wait, preparation,
execution, output acceptance and release. See [status contracts](../../docs/status.md)
and [release behavior](references/release-behavior.md).

For a Mycelium defect, search the public issues first and report a sanitized
minimal case with component version, exact job/attempt and expected/observed
behavior; omit secrets, private paths, receiver URLs and installation identities.
Keep independent authorized work moving.

## Credentials

Use the installed-authority CLI or Python `Client.from_installed()`; never open,
print, copy, pass or set fleet credentials. Missing/failed authority is an
installation-owner hold. `myce submit FILE` also resolves installed authority.
Queue-owner writes retain installed configuration and certificate trust; read
[owner operations](references/owner-operations.md) before those actions.

For catalog image chat, use the current Python client with its openai extra and
`Client.from_installed().openai(project=PROJECT, priority="experiment")`.
Preserve installed TLS transport and use base64 image_url parts. The model must
already be present in the matching catalogs; an example name proves nothing
about your installation. See [image-chat example](../../examples/image-chat.py).

## Diagnostic reference: one blocker

Supporting resolve/status/wait responses name a blocking dependency, owner and
action. An offer is not a started build; unknown observation is not measured
zero capacity. Keep typed reasons and original intent, and fix the cause rather
than retrying unchanged deterministic failures. See [diagnostics](../../docs/status.md).

## Benchmark telemetry receipts

On a client exposing `myce telemetry capture`, use its bounded local sampler
with a declared run/host reference and retain the versioned JSONL beside exact
attempt evidence. It issues no queue requests. Read [benchmark telemetry](../../docs/benchmark-telemetry.md)
for timing, units, missing coverage and attribution. Earlier installed releases
do not imply support; actual replay permission and parity remain separate.

## Diagnostic reference: project doctor

On supporting clients, `myce doctor --project` is a read-only project check;
`--entry`, `--adapter` and `--json` select its supported inputs/output. It checks
authority, source/adapter/resolve/dependencies and completion configuration,
then names a next owner/action. It submits/builds/uploads nothing and does not
run a supplied notifier, prove its receiver or execute application result checks.
Use the first-run journey and T3 binding for those acceptance checks.

## Native member admission diagnostics

On matching queue/worker builds, launch/resource_limit/memory_capacity retains
safe numeric refusal evidence and waits on the same logical job without spending
infrastructure retries. A helper that ran does not prove main ran or grant
whole-attempt unlaunched proof. Exact cleanup/release and absolute deadlines
still bind. Read [launch capacity waits](../../docs/status.md) before relying on
a worker-only update with an older queue; keep the same job/key.

Supporting workers include aggregate `live_host_growth` contributors and sample
ages in `execution_cause.admission`; compare those terms before attributing the
total to the launching job. With the queue's late-handoff correction, positive
`main: not_started` capacity evidence also protects an older worker's
`launch/io_error` refusal from loss charges, without releasing its supervisor.
