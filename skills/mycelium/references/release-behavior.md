# Release-dependent behavior

These implementation notes were moved from the consumer entry point at main
`67a487f330a1afcb2e6b95d40e4b899e0b2c8303`. The seven sections below preserve their
source wording except heading qualification and relative-link repair. They are
source/contract evidence, not a release announcement. Relative phrases such as
“next release” or “release containing #N” in that preserved text do not establish
availability; apply the component-specific checks below before relying on them.
No first signed-release minimum is inferred from a merge, a tag name or this file.

## Capability evidence

Known source floor means a verified source reference, not a claim it is the
first implementation or first deployed release. Use installed command usage and
component-specific running image/manifest/capability evidence. Unknown support
stays unknown; source ancestry alone does not prove native behavior or delivery.
A CLI version does not establish queue, worker or long-running observer image.

| Optional branch | Known source reference / required component | Required installed evidence |
|---|---|---|
| Schema1 adapter/profile and single-job lifecycle | Current CLI/client/queue contracts in adapter/jobs/files docs at the source pin above | Installed CLI usage and a resolve -> keyed submit -> wait -> verified result journey; no first release asserted |
| Batched wait and parent-bound foreground ownership | #761, known merge e2486789863cf09c0b9ef889909fe77e9280d380; newer batch clients #1053 | CLI/Python batch support and queue batch/scalar fallback; CLI limit256 remains distinct from negotiated API capacity |
| Observer metadata/cooperative upgrade | #922, known merge17f4b55f4904251bd8ca2e3246d4607c43e1403c; recurrence #1062 | Actual observer commit, wait --observer-status fields and compatible handoff; a new CLI alone is insufficient |
| Queue health configuration explanation | #869, known merge1c83e2e9e367c5f7c5ee6aacbc3f56d6f479667f | Running queue health_alert_delivery fields; configured/pending is not receiver delivery |
| Installed owner writes | #1080, known merge7ffbdcd64e4de19b1bb79eae4640ba7b9b3f48be; CLI plus installed authority | ownerClient route/trust supported; see owner-operations; retain typed refusal |
| Resumable upload recovery | #1103, known main merge06c5d3586ca4b859dac5fabb7937a3165b40d53d; Go/CLI and Python | Installed client source/usage and same-offset/identity recovery; older clients require their documented manual same-key path |
| Seven moved internal sections below | Implementation at the source pin above; individual detector/collector/protocol support can require queue AND workers | Current running component identities, negotiated fields and relevant behavior; signed-release minimum not established |
| Corroborated Linux pressure and measured host allowances | #1110 / #1059; matching queue/worker source, including #1059 at the source pin above | Observed pressure_evaluation/host_memory_derivation plus qualification; source fields do not authorize a shared-budget change |
| Cold CPU probe admission versus execution containment | #1116 / #1117; matching queue/worker source at the pin above | Fresh Started/host-memory evidence and retained exact-attempt measured peaks; no stale resident credit; see process growth |
| Established-response TLS shutdown recovery | #1074 / #1135; running queue plus compatible observer/client | Queue socket interruption fix and a real permitted cutover preserving original watches; retain all certificate/pin/integrity refusals; source regression is not deployed proof |
| Plain native jobs can't read worker credentials or `~/.ssh` | #1309; workers | Worker release containing #1309; a Linux worker without Landlock ABI 2 logs that it can't withhold them |
| `isolation: ci` for CI builds/tests | #1298; queue and workers | Worker capability `native_isolation_ci` in status; a queue without the mode refuses the value at submission |
| Progress/runtime, diagnostics, telemetry and early result release | Corresponding source/docs at the pin above; required CLI/client/queue/worker depends on branch | Installed option/field support, exact-attempt coverage and project acceptance; missing fields are unknown |

No installed-version inventory is embedded in this public guide. Record private
component proof on the existing work record. If a minimum signed release cannot
be established, keep the capability check explicit and ask the installation
owner to establish it rather than guessing a version or extracting credentials.

## Other release-dependent contracts

These are the canonical homes of release-note detail removed from the first-run
path; the supported features and their safeguards remain there:

- Memory envelopes, growth forecasts, declared/learned containment, pressure,
  fair share, job/resource liveness and complete-stage ETA:
  [status](../../../docs/status.md), [process growth](../../../docs/process-memory-growth.md),
  [native isolation](../../../docs/native-isolation.md) and [scheduling](../../../docs/scheduling.md).
- Portable target discovery, native variants/qualification/parity, tool/runtime
  dependencies, source packaging/linkage, staged fan-out and submission advice:
  [adapters](../../../docs/adapter.md), [environments](../../../docs/environments.md)
  and [setup](../setup-project.md).
- NAS/local shared inputs, dynamic cache allowances, verified transfer recovery,
  model snapshots, output retention/recovery/release and mutable checkpoints:
  [files](../../../docs/files.md) and [operations](../../../docs/operations.md).
- Read/transport admission, batch capacity, Retry-After, observation/retrieval
  recovery, runtime/wall/start deadlines and diagnostic metadata:
  [request budgets](../../../docs/request-budgets.md), [jobs](../../../docs/jobs.md)
  and [status](../../../docs/status.md).
- Worker freshness/loss, sensor validity, queue-delay accounting and launch
  handshakes: [host probes](../../../docs/host-probes.md),
  [status](../../../docs/status.md) and [operations](../../../docs/operations.md).
- Packaged models and loading/serving: [services](../../../docs/services.md) and
  [readiness](../../../docs/readiness.md). Native acceptance remains separate.

## Adaptive model readiness (source-audited; verify installed support)

Omitted model `max_wait` follows caller cancellation and actual load failures;
omitted `idle_timeout` follows measured model demand and reload cost. A healthy
load with advancing copy/load/health signals has no automatic total load
cutoff. Keep the same request while it progresses. Learned loads detect silent
stalls; an unknown cold load retains custody without a guessed deadline. Observable
CPU time, reads and memory growth from shared per-attempt observations count as
activity; unavailable counters remain unknown. Learned stall checks allow the
shared sensor time to deliver its next observation. A cold load with no observed
activity while eligible demand waits exposes an owner-attention reason and raises
`model_load_needs_attention` through the existing health channel; the alert does
not stop it. Explicit caller, `max_wait`, `ready_timeout`
and measurement `--load-timeout` deadlines still apply where configured. For
custom servers, report monotonic `MYCE_LOAD_PROGRESS` records as described in
[adaptive readiness](https://github.com/1kuna/mycelium/blob/main/docs/readiness.md).
Stdout progress records require captured logs; `logs: none` preserves null output
descriptors and uses health/OS activity instead.
Managed Docker still uses local digest-pinned images with `--pull=never`.


## Explicit model master downloads (source-audited; verify installed support)

Declared `model_snapshots` also handle SMB/CIFS `XSym` emulated symlinks from
the next release: jobs get verified blob bytes, or a `model_snapshot_mismatch`
or `model_snapshot_escape` refusal. Keep the original hub cache intact; do not
copy weights or rewrite shared links to work around a refusal. See
[declared snapshots](submit.md#models) for submission and read-only access.

For every declared bundle file, including nested paths, run
`myce model fetch BUNDLE/FILE --model-root PARENT --url URL --size-bytes BYTES --sha256 HASH`.
The root must contain the bundle. Fetch stages beside it, resumes interruptions,
restarts when `Range` is ignored, and publishes only verified files. Invalid
bytes stay in the reported staging quarantine. Retry a directory-sync or
concurrent-fetch error after its cause clears; repeating a completed fetch
verifies the master without replacing it. Keep staging while a fetch is active.
Verify the whole manifest before use; loading never downloads weights. See
[explicit master downloads](https://github.com/1kuna/mycelium/blob/main/docs/services.md#explicit-master-download-and-pins).


## Normal fleet updates (source-audited; verify installed support)

On releases containing #900's idle-moment upgrade slice, a normal fleet update
keeps busy enabled hosts `pending` and available for new work until a fresh
natural idle moment. Other idle hosts can update first. Keep using the queue;
do not cancel experiments to finish a drain. Started attempts finish naturally,
and exact release custody still gates restart. The separately requested
`--wait-production-idle` policy continues to hold new production assignments.


## Automatic host hygiene (source-audited; verify installed support)

On releases exposing `health_alert_delivery`, status explains whether the
queue health notifier is configured and counts retained unacknowledged alerts.
Without a configured hook, `delivered: false` is diagnostic-only history by
design. With a hook, false alone cannot establish whether delivery was attempted
or failed; the queue owner checks sanitized hook and commit evidence. This
channel is separate from your job completion notifier and does not prove a
thread wake. Older releases omit configuration; do not infer a cause from false.

Machine status `host_health` explains automatic admission drains with typed
causes, persisted recent samples and the host's own healthy baseline. Mycelium
resumes after measured recovery; an operator drain remains independent. Running
attempts retain custody. From the queue release containing #1110, Linux PSI above its learned band
alone no longer blocks admission, production included. The queue re-evaluates
older workers' memory-pressure drains: three distinct healthy swap/wake samples
clear the effective drain under sustained ordinary work without a worker
restart. Corroborated swap-growth or local wake-latency deterioration still
protects the host; stale/unknown evidence and operator drains still bind.
Read `host_health.pressure_evaluation` for `pressure_limit`, `baseline_samples`,
`corroboration`, `recovery_samples` and the original `reported_drain_reason`;
`host_health.drain_reason` is the effective queue admission reason. Fewer than
eight baseline samples leaves the limit uncalibrated and retains the reported
drain. A later worker upgrade also prevents the false local PSI latch.
From the next release, `mount_probe_slow` is advisory:
successful or progressing slow metadata operations preserve admission, with
measured evidence in `host_mount_degraded` alerts. Failed operations and
`mount_probe_stalled` still withhold dependent work; the latter uses the unchanged
measured per-operation no-progress allowance, not whole-sequence elapsed time.
`host_mount_unavailable` wakes if an advisory becomes a stall/failure. Cold access
still requires proof. Independent local work can remain eligible. Existing OS automounts are re-triggered
with backoff. `mount_linux_operator_required` needs the storage operator to
restore the configured mount; `mount_macos_approval_required` needs the owner to
restore it and approve Keychain if requested. `host_mount_action_required` uses
the existing health wake channel, including escalation after a generic alert.
`mount_probe_stalled` retains its one request without blocking heartbeats; ask
the storage owner to inspect the OS session. Workers never prompt for credentials
or run privileged mount commands. `host_degraded` and
`host_mount_degraded` use existing health notifications; silent-worker alerts
carry `host_silent`. Read the job's waiting reason or one needed status snapshot;
no agent polling or drain/resume loop is needed. Foreign CPU/RSS leaders are
context for the host owner: never stop their processes or services. See
[host-health fields](https://github.com/1kuna/mycelium/blob/main/docs/status.md#automatic-host-health-834-next-release).
Older queues omit the new status fields; workers retain local history and use
reversible availability withholding for unknown health or a confirmed local drain
until support is negotiated.
From the checkpoint-probe correction, checkpoint health uses the existing tiny
exclusive create/close/remove check instead of enumerating the checkpoint root.
This removes unnecessary directory work; genuinely slow checkpoint metadata
still withholds dependent jobs under the unchanged measured thresholds.


## Automatic worker collection (source-audited; verify installed support)

Mycelium maintains its own file/environment budgets during idle retention as
well as new staging. Machine `hygiene_collection` shows effective limits,
collection errors and protected/busy entries. A cache can remain over budget
while references, recent use or locks protect data; do not remove it manually to
force admission. Attempt custody and result/diagnostic retention still bind.
`pending` can mean conservative shared-inode accounting left work for another
sweep. Collector status requires matching queue support; older queues keep
their supported request shape while local collection continues.
Installed/rollback images, active state, receipts and unknown data are retained;
foreign services/processes and other projects' shared caches are owner context
only. See [worker cleanup](https://github.com/1kuna/mycelium/blob/main/docs/status.md#worker-cleanup-834-next-release).


## Queue efficiency (source-audited; verify installed support)

Use `myce efficiency` for one concise read of reservation versus measured use,
blocker wait totals, authored exclusions stranding fitting work, prediction
errors and wasted/suspect attempts. `myce efficiency --json` reads the immutable
maintenance report without fetching job payloads; status/inspection JSON also
include `efficiency`. Read its `at` and coverage: nullable CPU/I/O counters,
lower-bound flags, elapsed attempt windows and dropped-attempt count distinguish
observed use from unknown. CPU/I/O counter windows can differ from the
reservation horizon; they are not a utilization ratio. Missing memory samples
are not zero use. `efficiency_regression` wakes use the existing
health notification path and this installation's learned baselines. Investigate
an offender with its per-job status; keep authored hard requirements and parity
checks. See [queue efficiency](https://github.com/1kuna/mycelium/blob/main/docs/status.md#queue-efficiency-next-release-857).


## Adaptive host observations (source-audited; verify installed support)

On negotiated `host-probe-v1` workers, owner/pressure status includes measured
probe period, cost and frozen validity. Read age against that observation's
validity; a successful check-in cannot renew a failed probe. From the host-health
delivery correction (#1025), new health observations include the same learned
report-gap allowance as shared pressure. Stopped or failed sensors still lose
admission while worker contact continues. From the queue-delay correction,
host-health decisions exclude actual incoming queue processing and mutex
occupancy from delivery aging. A fresh replacement currently preparing outside
the mutex pauses only its own session's previous evidence; stale/failed input
cannot renew it, and cancellation or rejection removes that credit. Diagnostic
`age_seconds` still includes that wall time; age above validity alone does not establish sensor staleness. A stopped
sensor's worker-local age still expires, and continuous queue unavailability has
no finite wall-time detection bound while its control path remains unavailable.
Owner-latency holds now require fresh healthy evidence spanning the attempt's measured interference
interval, rather than a fixed retry delay. Other eligible hosts remain available.
Older workers retain legacy fields and age rules. See
[host observations](https://github.com/1kuna/mycelium/blob/main/docs/host-probes.md).
From the next release, hardware discovery backs off independently of changing
device usage. Discrete XPU memory/activity uses a separate shared host sample;
missing, ambiguous or stale counters remain unknown and withhold
telemetry-dependent sharing. A heartbeat never refreshes a cached sample's age.
