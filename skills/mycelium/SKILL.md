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
it eligible. This queue protection works with older workers. Automatic
quarantine clearance additionally needs the supporting worker release and its
vendor health tool; unsupported/unknown health keeps quarantine. Status
`devices[].quarantine_recovery` records the fault event, healthy interval,
measured observation window and clear evidence. Repeated-fault escalation still
needs the maintenance owner. Do not resubmit to bypass the hold or clear real
devices as a consumer. See [device quarantine](../../docs/operations.md#accelerator-driver-fault-quarantine).

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
