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

From the queue release enforcing explicit production parity lists, a configured
project/work `production_parity_hosts` entry bounds all production placements,
including fan-out and recovery. Native qualification, measured quality and old
registrations cannot admit an unlisted host. The owner must add an approved host
to widen the list; an explicitly empty list holds that production work. Checks
and experiments remain available for collecting comparison evidence.
The typed `production_parity_hosts` blocker identifies this hold even for
extensions without variants. A removed host's queued binding is placed again;
running execution and release custody remain with its current attempt.
Parity-held production cannot withhold another project's environment preparation
on an unapproved host. Eligible production retains its tier/preemption priority.

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

Keep owned waits in the foreground. Supporting clients accept one batch of up
to 256 distinct IDs: `myce wait JOB_A JOB_B`. A wait timeout or interruption leaves
the jobs alive; retain the IDs. Never build per-job background wait processes,
repeated status/get loops or replacement submissions to obtain progress.

Use `myce status JOB_ID` and `myce logs JOB_ID` for a real blocker. Default fleet
status is a partial history page; absence from it is not a lost or incomplete job.
Use `myce status --history` only when full retained history is needed. Follow
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
their existing cleanup custody. A dead owner still recovers on the next keyed
command; automatic supervision is deferred to [#1166](https://github.com/1kuna/mycelium/issues/1166).
See [upgrade and recovery](../../docs/operations.md#completion-observer-upgrade-and-recovery-next-release).
Legacy or stalled shared-observer recovery belongs to its operator. Keep stable
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
`give_up_at` still bind. If recovery never becomes eligible, the hold terminates
one existing maximum start window (one hour) after the queue-recorded fault loss
with `diagnostic_code: device_fault_recovery_timeout`. Preserve that terminal
result; a late clear cannot revive it. Fresh inventory showing both cards
healthy releases the guard after an operator clear as well as automatic recovery.

Automatic quarantine clearance additionally needs the supporting worker release and its
vendor health tool; unsupported/unknown health keeps quarantine. Status
`devices[].quarantine_recovery` records the fault event, healthy interval,
measured observation window and clear evidence. Repeated-fault escalation still
needs the maintenance owner. Do not resubmit to bypass the hold or clear real
devices as a consumer. See [device quarantine](../../docs/operations.md#accelerator-driver-fault-quarantine).

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
