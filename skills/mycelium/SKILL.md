---
name: mycelium
description: "Use when work needs fleet compute (heavy tests, builds, sims, training, transcodes, model runs), when you submit, check, wait on or fetch a Mycelium job, when a job is waiting, when your work must go first, or when you need to pause your own queued job."
---

<!-- Public consumer guide. Verify installed components; source history is not deployed capability. -->

# Mycelium

Mycelium runs CPU and GPU work through one queue. Describe the work's needs and
class; Mycelium chooses eligible placement. Start with `myce version` and the
installed command's help. Optional contracts name their required components in
[release behavior](references/release-behavior.md#capability-evidence).
A merged change or a “next release” note does not establish installed support.

**Default: declare needs, submit once, wait, verify the result.** Use the
project's adapter entry with its supported implementation variants, environment,
inputs and class. Leave machine and device selection to Mycelium. A backend
requirement such as CUDA already selects compatible hardware; even if only one
host can satisfy it, no host pin is needed. Worker-provided `MYCE_DEVICE` and
assigned runtime visibility tell the command what it received.

Resolve the entry, submit with a retained key, and wait on the returned job ID
using the first-run commands below. Mycelium chooses a qualified variant that
fits, preserves scarce capacity, and uses comparable measured completion data
within that placement order. Unknown timing is not a reason to select a machine.
For a genuine wait, read `myce status JOB_ID` once and follow its blocker's owner
and action. Keep the job ID while capacity or preparation recovers.

Exact hardware qualification and owner-imposed restrictions are exceptional;
see [explicit restrictions](advanced.md#explicit-placement-restrictions).

Use the installed CLI or Python `Client.from_installed()`, which resolve
credentials internally. Never open, print, copy, pass or set fleet credentials.
A missing CLI or failed authority is an installation-owner hold. Preserve its
typed refusal; heavy local execution is not a fallback. Queue-owner policy and
model writes use [installed owner authority](references/owner-operations.md).

## Try locally without a fleet

On macOS or Linux with Git, Go 1.25.6+ and a Unix shell, follow the
[quick-start commands](../../docs/START-HERE.md#five-minute-start).
`./myce dev -- sh examples/quickstart.sh` supplies a temporary queue and workers,
runs real commands with fake GPU inventory, waits and fetches verified output.
It needs no installed fleet or token setup. Plain `myce dev` stays foreground
until stopped. This local fixture does not qualify native hardware.

From the release containing #868's admission-wait evidence, production job
status includes `admission_wait`: first/latest blockers, candidate targets,
exact capacity holders, memory forecasts and sample ages, and actual scheduler
preemption decisions. Check `device_fault` before attributing RAM pressure to
tests; a quarantined device cannot serve work after CPU holders stop. Missing
values and dropped history remain unknown. `preemption.source` identifies an
actual planner decision versus a diagnostic projection; the separate target
field `release_commitment` reports retained releases still pending.
It clears once those exact releases are acknowledged, even if RAM or another
blocker still prevents admission.
`last_planner_decision` keeps its original timestamp and wave vector.
Read `latest.closed_at`/`latest.closed_reason` for ended waits and `capture_gaps`/`deferred_at`
for capture deferred to protect control service. See [wait evidence](../../docs/status.md#retained-production-admission-waits-next-release-868).

From the queue release containing #1158, overdue waits in every tier can
include `admission_wait.latest.targets[].admission_decision`. Read `outcome`,
`stage`, and `predicate` for the actual check-in's first failure; identify the
observation with `at` and `checkin_machine`/`checkin_session`. `not_considered`
means selection was skipped, not a rejected candidate. `not_selected` means an
eligible candidate lost selection order. The threshold is one advertised or
measured check-in opportunity, not an invented start promise. Absence is unknown;
unchanged evidence retains its timestamp. Sibling `predicates` remain diagnostic
projections. See [actual decisions](../../docs/status.md#actual-all-tier-admission-decisions-next-release-1158).

## First run: adapter to verified result

For an existing project, use its reviewed adapter entry or compatibility profile;
[setup](setup-project.md) covers authoring and installation. For an empty project,
this dependency-free CPU fixture uses only worker PATH tools:

From the release containing #909, per-job JSON retains `last_memory_admission`
for a blocked RAM comparison: envelope, drift, signed headroom and charge/growth
owners. It survives startup failure and restart. Read its timestamp as historical
evidence, not live usage or the job's only blocker; a retry starts a new episode.

```sh
mkdir -p source .mycelium
printf 'First-run source fixture\n' > source/README.txt
printf '.mycelium/receipts/\n' >> .gitignore
cat > .mycelium/adapter.yaml <<'YAML'
schema: 1
project: example
default: ease-audit
entries:
  ease-audit:
    source: {directory: source}
    class: experiment
    run:
      command: [sh, -c, "printf 'mycelium-ease-audit-ok\n' > result.txt"]
    needs: {platform: cpu, cpu_cores: 1, memory_gb: 0.125}
    targets:
      - {os: darwin, arch: arm64, platform: cpu}
      - {os: linux, arch: amd64, platform: cpu}
      - {os: linux, arch: arm64, platform: cpu}
    environment: {}
    outputs: {result.txt: results}
    checks:
      result: [sh, -c, 'test "$(cat result.txt)" = mycelium-ease-audit-ok']
YAML
```

The tiny memory declaration bounds this fixture. Real work can omit memory to
learn it; consult [submission contracts](advanced.md#submit) before changing
resources, targets or isolation. Plain native commands default to unsandboxed
under MY-25; new native extension registrations have their separate sandbox
policy. Declare `isolation: sandboxed` when that boundary is needed.

From the release implementing MY-44, declared `model_snapshots` need no queue-owner
approval: absent model policy permits every repository. Optional explicit source
restrictions and physical disk/cache limits still apply; snapshots need an exact
commit and verified bytes. See [snapshot submission](advanced.md#submit).

On releases with the contained-forecast correction, CPU forecast reservations
stay within their actual hard cgroup ceiling; measured demand and probe floors
remain protected. Hard-contained CPU remaining-growth forecasts also stay within
the full ceiling; live RAM and allowance checks still apply. See
[memory sizing](advanced.md#submit).

Prepare directory source deliberately: ignored files are included except `.git`
and `.mycelium/receipts`; links are refused. A Git source such as
`source: {git: HEAD, paths: [scripts/]}` sends committed selected bytes; commit
those paths first. The adapter itself is frozen separately. Retain private
receipts and ignore them in Git.

Resolve read-only, then submit once. Choose the key once and retain its value
for recovery; do not regenerate it when replaying this operation.

```sh
myce spec check --resolve --entry ease-audit .mycelium/adapter.yaml
key="ease-audit-$(date -u +%Y%m%dT%H%M%SZ)"
myce run --entry ease-audit --key "$key"
```

Save the receipt's job ID and key. Changed intent needs a new key; a lost reply,
`submission unconfirmed` or exit 69 `input storage unconfirmed` calls for the
original command/key and receipt. Keep frozen inputs and the original notifier
binding when one was supplied. See [recovery](advanced.md#wait-and-check-status).

From the queue release containing #1233, submission and worker writes take fair
turns. Production acceptance returns its durable receipt without rebuilding
optional planner diagnostics. `deadline_guidance.details_deferred: true` means
read the accepted job for full current blockers; it does not weaken acceptance
or call for resubmission. The contention regression targets responses well
within a 20-second submit budget, not a start-time guarantee. Input verification,
storage failure and shutdown remain separate causes; retain the same spec/key
when a response is uncertain.

Replace JOB_ID with that receipt's ID. No notifier or harness setup is needed:

```sh
myce wait JOB_ID
myce result JOB_ID --out accepted
printf 'mycelium-ease-audit-ok\n' > expected.txt
cmp expected.txt accepted/result.txt
```

Success requires a succeeded job, verified output size/SHA-256 from `result`,
and this application comparison (23 bytes including newline). A wake, terminal
receipt or command exit alone is insufficient.

Python callers can submit, wait and fetch the same CPU fixture directly. Keep
the submission key and spec unchanged when retrying an uncertain reply. Run in
a fresh directory; `Client.from_installed()` uses the installed authority.

```python
from pathlib import Path
from mycelium import Client

client = Client.from_installed()
source = Path("README.txt")
source.write_text("First-run source fixture\n")
input_ref = client.upload_file(source, project="example")
job = client.submit_and_wait({
    "name": "ease-audit",
    "project": "example",
    "class": "experiment",
    "submission_key": "ease-audit-python-01",
    "files": {"README.txt": input_ref},
    "run": {"command": ["sh", "-c", "printf 'mycelium-ease-audit-ok\\n' > result.txt"]},
    "needs": {"platform": "cpu", "cpu_cores": 1, "memory_gb": 0.125},
    "outputs": {"result.txt": "results"},
}, timeout=60)
assert job["state"] == "succeeded", job.get("reason")
result = client.download_result(job["id"], "result.txt", Path("accepted/result.txt"))
assert result.read_bytes() == b"mycelium-ease-audit-ok\n"
```

The Python client shares retry classification and pacing across requests.
Overload retries follow the current Retry-After plus positive jitter, bounded
by the caller's deadline. Keyed submissions and waits recover transient TLS
record/EOF failures; certificate, identity and protocol refusals remain fatal.
Unkeyed writes and model calls are not automatically replayed.

From the queue release containing #1164, a new `after:` consumer can bind
retained, unexpired accepted outputs from a succeeded fan-out parent even if
the parent later received a cancellation intent. A parent cancelled before
success still cancels its dependents with `upstream_cancelled` and reason
`upstream parent cancelled`; expired outputs return `result_expired`.

From the worker release containing #1168, opt-in XPU fan-out sharing requires
fresh VRAM counters from the separately administered read-only telemetry broker.
An older GTT-only broker still supports host RAM accounting; missing VRAM
evidence withholds extra sharing. Ask the installation owner to qualify the
[updated broker](../../deploy/gpu-telemetry/README.md), rather than starting GPU
vendor-tool samplers to unblock placement. Ordinary exclusive admission remains
available within its configured budget.

## Observe and recover

The current/previous release pair uses batch-v1 waits. An older queue without
that endpoint requires an upgrade; clients no longer fall back to per-job polling.

From the worker/queue release containing #811, filesystem pressure reclaims
inactive rebuildable caches automatically across cache classes. Keep the same
job while pending demand triggers cleanup; active readers, queued references,
exact-attempt custody and last unique sources stay protected. Machine
`hygiene_collection.disk_budget` reports physical headroom/reservations and
`foreign_disk_growth`; `scratch.retained_bytes_by_reason` explains preserved
attempt storage. Foreign or explicit external compiler caches are warned about
and never deleted. Installation owners can select verified local snapshot
acquisition with supported `myce install worker --config CONFIG --model-root PATH`
on the #1248 worker release. This leaves NAS checkpoints, environment sources
and immutable masters under their existing contracts. See
[worker budgets](../../docs/reference/worker-caches.md#shared-filesystem-budget-811-next-release)
and [local acquisition](../../docs/reference/models.md#local-acquisition-root-1248-next-release).

Keep owned waits in the foreground. Supporting clients accept one batch of up
to 256 distinct IDs: `myce wait JOB_A JOB_B`. A wait timeout or interruption leaves
the jobs alive; retain the IDs. Never build per-job background wait processes,
repeated status/get loops or replacement submissions to obtain progress.

Use `myce status JOB_ID` and `myce logs JOB_ID` for a real blocker. Default fleet
status is compact current work, machines, holds and active alerts; absence is
not proof of completion or loss. Retained evidence/telemetry is opt-in:
`myce status --sections jobs,machines` adds detailed diagnostics;
`--sections work,storage,health,efficiency,usage,diagnostics` selects retained
collections. Job reasons in the compact view are last committed facts, not a
new placement plan. `myce status --jobs all --limit N --before CURSOR --json`
pages history at negotiated capacity; Python `client.list()` walks those pages.
`myce status --history` / `GET /api/status?detail=full` /
`client.status(full=True)` restores every existing field for full inspection or
preservation. See [section coverage](../../docs/status.md#reading-myce-status).
Use `client.status(sections="work,storage")` for a selected evidence read. Follow
Retry-After and typed recovery diagnostics; do not change identity or trust to
evade them. Permanent authority/TLS/integrity/configuration refusals go to their
owner. An older observer's restart classification may require the installation
owner to verify identity before same-key replay.

From the worker/queue release containing #871's finalization correction, an
exit-zero command can still fail its declared output contract. Read
`execution_cause.finalization` in status/GET for the step, declared relative
output or destination, sanitized error and observed command exit zero. A
directory is not a regular-file output; declare individual files or an archive.
`myce result JOB_ID --attempt N --out DIR` retrieves a safe `summary.json` for
these failures even without optional diagnostic artifacts. Upload/publication
errors retain local results for retry; a publication-pending job is not accepted
success. Keep its identity and resolve the reported storage/permission problem.

For detached completion, bind a notifier to your own thread with
[T3 binding](t3-binding.md). Supporting clients expose local observer diagnostics
through `myce wait --observer-status`; a PID/lock proves ownership, not delivery.
From the release containing #1160, shared owners use their own session and a
clean allowlist environment, excluding runner/job tracking and MYCE_TOKEN while
preserving PATH/locale for legacy notifiers. Managed attempts/dev sessions keep
their existing cleanup custody. From the release containing #919's supervision
fix, that detached process supervises a separate observer child: death or silent coordinator/authority reads
trigger automatic adoption of durable arms without another keyed command.
`--observer-status` includes `supervision`, with restart count, exit signal/code
when known, and the retained pre-recovery diagnostic. A pending notifier payload
survives replacement with the same delivery ID; receivers must deduplicate that
ID if acknowledgement was interrupted. No compute is resubmitted.
Recovery allowances grow from protocol holds, existing notifier deadlines and
measured service cost; intentional Retry-After waits are preserved.
Native login/boot supervision, recovery of the supervisor itself and a canonical
installed executable remain [#1166](https://github.com/1kuna/mycelium/issues/1166).
Existing processes need an ordinary supported handoff/start to gain this behavior;
merged source alone does not upgrade them.
See [upgrade and recovery](../../docs/reference/completion-observer.md#completion-observer-upgrade-and-recovery-next-release).
Legacy or unsupervised shared-observer recovery belongs to its operator. Keep stable
receipts and delivery IDs; do not kill a shared observer or edit its registry.

If blocked, record expected/observed behavior, component version, job/attempt,
next owner and action on the existing work record, then continue independent
authorized work. Search existing issues before reporting a Mycelium failure.
The public repo must receive no credentials, webhook URLs, private paths or
installation details.

From the queue release containing #1142, `device_fault_last_healthy` holds a
job that lost an attempt to a driver fault before it can consume that host's
last healthy accelerator of the same backend. Keep the job ID and submission
key: another host, spare healthy capacity or a health-proven recovery can make
it eligible. This queue protection works with older workers.

The typed hold pauses the remaining relative start/recovery budget; status says
`paused for device recovery`. The saved amount persists through capacity waiting
after clearance and restart. Explicit absolute `start_by` and queued-only
`give_up_at` still bind during waiting. A started infrastructure interruption
renews the original first-start window, even when explicit `start_by` supplied
its length; the renewed cutoff remains absolute during the hold. Unstarted
losses never renew it. If recovery never becomes eligible, the hold terminates
one existing maximum start window (one hour) after the queue-recorded fault loss
with `diagnostic_code: device_fault_recovery_timeout`. Preserve that terminal
result; a late clear cannot revive it. Fresh inventory showing both cards
healthy releases the guard after an operator clear as well as automatic recovery.

Automatic quarantine clearance additionally needs the supporting worker release and its
vendor health tool; unknown health without supported capability evidence keeps quarantine. Status
`devices[].quarantine_recovery` records the fault event, healthy interval,
measured observation window and clear evidence. With `device-health-v1`,
`devices[].quarantine_recovery_blocked` explains the last rejected proof with a
sanitized code and component or probe exit status. `unsupported_on_device`
lists a required component excluded only after a fresh healthy same-model
sibling reports it undeterminable in the same health batch. Every remaining
required component must be OK; Unknown without that comparison (including a
single-card host), stale comparison, or a monitoring gap retains quarantine.
Successful command exit alone is not proof. Repeated-fault escalation still
needs the maintenance owner. Do not resubmit to bypass the hold or clear real
devices as a consumer. See [device quarantine](../../docs/reference/accelerator-quarantine.md#accelerator-driver-fault-quarantine).

From the release containing #1170, Go/CLI and Python recover a missing resumable
upload session (404) by checking the content-addressed blob. Success requires
server SHA-256 verification plus matching client size/ETag checks. If the blob
is also absent, the same upload retries within its existing owner deadline
(Go context or Python `transfer_timeout`); keep the original file, digest and
submission key. A late retry after completion is safe. Typed integrity,
authority and storage-policy refusals still need their owner. Upload errors use
the standard JSON code/error envelope; HEAD carries `X-Myce-Error-Code`.
Keep source files unchanged during upload. Clients bind completion to the source
size and SHA-256, including bytes read for the failed PATCH; another uploader's
matching blob cannot turn a changed-source retry into success.
See [file transfer recovery](../../docs/files.md#transfer-behavior).

From the queue release containing #1162, inline jobs accept
`environment: {backend: pixi, lock_file: /queue/shared/project/pixi.lock, platforms: [cpu]}`.
The queue must read both `pixi.lock` and adjacent `pixi.toml` at submission;
`python` is optional. Each input is limited to 1 MiB; oversized Python/Pixi inputs report
`path_diagnostic.cause: too_large`, naming any required companion. Profile inputs/source do
not upload these files. Workers build the frozen snapshots through Pixi.
See [project lock files](../../docs/environments.md#project-lock-files) for
queue-relative paths and typed read diagnostics.

From the owned-NAS fault follow-up, Mycelium's own staging/hash and checkpoint
I/O failures also withhold work that depends on the affected network root,
through the same host-health reason and alerts. Independent local work remains
eligible. Data faults recover only after three fresh cache-bypass read/reopen
proofs; metadata or cached probe success cannot clear them. No sentinel write
is required on read-only inputs. Unsupported cache bypass retains the hold for
the storage owner. Sibling roles on one backing share one latch and recovery
owner. A worker restart forgets this latch: the next dependent attempt may fail
again and re-latch it. Operator exclusions remain independent. This is automatic
after the worker release; it does not observe arbitrary consumer syscalls.
Keep the same job while the root recovers;
do not work around the hold by pinning a host or changing mounts.

From the worker/queue release containing #1227, accepted exact-attempt scratch
is reclaimed automatically; terminal failed/cancelled scratch follows the queue's
positive file-retention policy or explicit owner file release. The former fixed
24-hour scratch promise is retired under MY-34. Unknown data and pending
upload/replay/custody remain protected. Recover required partial files before
the configured deadline or release; use the existing reviewed file-release
plan/apply flow only after preserving them. See
[worker scratch retention](../../docs/reference/worker-caches.md#worker-scratch-retention).

## Read for the relevant branch

- **One-off commands, GPU/memory, variants, models, checkpoints or NAS inputs:**
  [submit](advanced.md#submit); resolve before relying on a target.
- **Many jobs, Python, retries, observers or detached completion:**
  [wait/recovery](advanced.md#wait-and-check-status) and [T3 binding](t3-binding.md).
- **Large outputs, diagnostics, retention or early release:**
  [results and custody](advanced.md#results); verify downstream copies first.
- **Urgency, reservations, pausing or placement blockers:**
  [class and validation](advanced.md#urgency-and-pausing) and
  [waiting reasons](advanced.md#when-a-job-waits).
- **Application progress, ETA or benchmark telemetry:**
  [stage progress](advanced.md#stage-progress-and-honest-eta) and
  [bounded telemetry](advanced.md#benchmark-telemetry-receipts).
- **Project setup, profiles or harness routing:** [setup](setup-project.md),
  [project doctor](advanced.md#diagnostic-reference-project-doctor),
  [harness integration](harness-binding.md).
- **Optional release-dependent behavior:** [release behavior](references/release-behavior.md).
  Check the installed component before relying on it.
- **Mycelium repository changes or release maintenance:**
  [repository gates](advanced.md#mycelium-repository-merge-gates) and
  [owner operations](references/owner-operations.md).

Point-in-time completion contract: orchestrator decision, 2026-11-09.
From the queue release containing the #1052 committed-capture correction, a
per-job GET completes from one published committed observation, including cold
and parent reads, without waiting for the live control transaction mutex. A
commit during encoding affects the next read; it does not restart the current
one. Treat the response as a point-in-time observation, and retain mutation
acknowledgements and accepted-result checks as the authority for those actions.
Canceled reads return retryable 503 `status_unavailable`; use the client's
existing backoff and the same job/key rather than resubmitting work.

From the next queue release, status builds derive worker-contact diagnostics
and retained history outside the queue mutex, preserving current contact and
the existing cache freshness contract. Slow-request mutex timing excludes
response-cache refresh/build time; older status/job mutex figures may include
that off-lock work and need a direct queue-lock measurement for attribution.

## Fault evidence (next release)

Device `fault_evidence`, job `fault_evidence` and terminal `attempt_history`
retain bounded attempt/PID/memory/page snapshots for observed driver faults.
Fatal native signals add `signal_evidence`, including readable fault address,
loaded-library mapping identities and existing core pointers/capture availability.
Use one `myce status JOB_ID` inspection and compare kernel, capture and sample
timestamps; delayed samples, ambiguous CUDA-device attribution and missing native
cores do not establish a cause. Report `unknown` until the causal chain is proved.
Core enablement is a separately authorized operator task; agents must not change
limits, core patterns or drivers. See [fault evidence](https://github.com/1kuna/mycelium/blob/main/docs/status.md#fault-evidence-next-release).
