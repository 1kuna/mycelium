---
name: mycelium
description: "Use when work needs fleet compute (heavy tests, builds, sims, training, transcodes, model runs), when you submit, check, wait on or fetch a Mycelium job, when a job is waiting, when your work must go first, or when you need to pause your own queued job."
---

<!-- Public consumer guide: no host names, fleet layout, private paths or project names. Update with consumer behavior changes; release qualifiers distinguish merged code from the deployed release. -->

# Mycelium

## Try locally without a fleet

On macOS or Linux with Git, Go 1.25.6+ and a Unix shell, follow the five
[quick-start commands](../../docs/START-HERE.md#five-minute-start).
`./myce dev -- sh examples/quickstart.sh` supplies private temporary local
connection settings, submits a real CPU command and a fake-GPU command, waits
and fetches verified output. No installed fleet or token setup is needed.
The queue and workers stop automatically; `quickstart-result/result.txt` remains.
Plain `myce dev` retains its existing persistent, foreground behavior.
From the next release, command-scoped dev waits for both local workers to check
in before starting your command, so a target-required profile can run first.

From the next release, a worker fleet proof may report `slot_mismatch` with
separate running and `installed_slot` hashes after a failed update. This does not
prove activation. Report it to the Mycelium maintenance owner; an unapproved
slot refuses recovery with `fleet_slot_unapproved`. Use the supported installer
and preserve the refusal and identities for that owner.

From the next queue release, request bodies, wait batches and status history pages
follow current queue capacity; 413 refusals name the cause and fitting size.
Follow returned page cursors and retain the same submission key after a refusal.
Custom peers can negotiate `capacity-v1` through HTTP headers without changing
legacy JSON. See [request budgets](https://github.com/1kuna/mycelium/blob/main/docs/request-budgets.md).

From the next release (#891), extension reports distinguish “not yet checked
on this device” from a changed hardware/driver fingerprint, changed extension
fingerprint (digest), or unavailable fingerprint. Run the extension check for
unchecked versions; a missing fingerprint is unknown identity, not evidence of
a change. Qualification still requires a successful check.

## First run: adapter to verified result

Mycelium runs CPU and GPU work through one queue. Describe needs and verified
targets; Mycelium chooses placement. Use the installed `myce` CLI, which resolves
authority internally. Never open, print, copy, pass or set fleet credentials.
A missing CLI or failed authority is a setup hold for the installation owner.
After upgrading both queue and worker, learned and exploratory native
CPU `memory: auto` jobs can grow their charge from fresh measurements while the host has room. OS
pressure, exhausted measured headroom, the configured host allowance and
explicit limits still stop growth. Linux retains a physical/allowance kernel
ceiling; macOS uses sampled RSS and pressure with no kernel ceiling or
shared-page credit. Missing/stale telemetry protects the execution ceiling.
`memory_envelope.enforcement` names the acknowledged behavior; older queues or
workers retain the previous estimate-sized ceiling. From the next release, cold
CPU probes also use the adaptive guard, so a small residual admission envelope
is not their execution limit. Native CPU auto-memory OOMs and native accelerator-check host cgroup OOMs
retain killed-peak floors and retry against usable physical headroom;
exploratory under-sizing does not spend a fixed attempt budget.
Growth/overshoot stays replaceable uncertainty, rather than a permanent floor.
If the measured lower bound exceeds every eligible capacity, the job reports
a capacity refusal. Inspect each attempt's envelope and learning reason. Declared numeric limits,
explicit member caps and Docker behavior are unchanged.
From the release containing #1011, typed `launch/resource_limit/memory_capacity`
refusals wait on the same job and submission key without spending infrastructure
retries or losses, including after a helper ran or Started was authorized. Keep
that key: Mycelium uses ordinary lower-tier preemption and exact release before
reuse; equal-tier production waits. Unchanged capacity evidence is not re-offered.
Generated startup windows pause for capacity; explicit `start_by`, `start_window`,
give-up deadlines and cancellation still apply. `execution_evidence` describes
component boundaries separately from cleanup; a helper that ran is never proof
that the whole attempt stayed unlaunched.

From the next release, `host_capacity_accumulation` means new CPU work is waiting
so ready CPU or accelerator work can acquire host RAM/cores as running CPU
attempts finish. Mycelium chooses one fitting target; earlier fair turns,
higher tiers and spare capacity elsewhere remain eligible. See
[waiting reasons](advanced.md#when-a-job-waits).
On releases with inventory deltas, status omits full executable inventories. Use
resolve/placement diagnostics for `needs.tools`; workers automatically refresh
inventory after queue restart before qualifying tool-dependent work.

This dependency-free CPU fixture uses only worker PATH tools (`sh`, `printf`,
`cat`, `test`), not Python, Docker or a managed environment. From an empty project
directory, create an inert source file and `.mycelium/adapter.yaml`:

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

The tiny declared memory bounds this fixture; real work can omit memory to learn
it. From the next release, cold auto-memory work reserves measured physical
headroom on one host.
Native CPU probes and extension checks leave this job's measured launch-error
reserve unused; unrelated inventory drops and other jobs' old refusals do not
shrink their envelopes. Strict worker checks refuse unseen drops and teach
the same job's next offer. Unknown accelerator checks wait for a physical
opportunity rather than starting with a tiny leftover ledger reservation.
A running CPU probe can share after fresh complete cgroup
measurements establish its live footprint and growth over a sampling horizon,
extended by the measured growth rate through the sample's age;
supporting native guards use physical/allowance containment. Missing/stale measurements retain the full
charge. Protected attempts block only capacity they actually reserve, rather
than excluding every exploratory job. A clean accepted completion teaches a
reusable bound.
Comparable peaks use observed variability and sampling growth; cross-target
estimates need paired accepted runs and measured transfer error. Unknown pressure
and incomplete counters remain conservative. From the next release, a submission blocked only by learned or exploratory memory sizing is accepted and waits with `diagnostic_code: memory_evidence_recovering`. Fresh comparable evidence or eligible capacity re-evaluates the same job; do not resubmit. Declared impossible needs still refuse. Constrained recovery uses the observed peak and overshoot headroom, bounded by eligible allowances without lowering measured need, explicit containment or the job's OOM retry floor. See [memory sizing](advanced.md#submit).
From the next release, constrained recovery pairs each failed run's growth or
overshoot margin with that run's peak. A different constrained run contributes
its peak as a lower bound, without inheriting that margin. Replaceable uncertainty
uses the candidate target's allowance and measured usable headroom, leaving launch
slack and measured drift unused. Measured peaks, authored/member requirements,
job-local OOM floors and release custody still bind. A newer accepted same-host
completion can replace an older constrained class prior; it cannot erase a later
constrained peak or a job-local OOM floor. Retained matching evidence does not
expire merely with elapsed age. A smaller recovery attempt
remains guarded and can fail on unseen growth; sampled Darwin RSS cannot contain
a burst between observations. Keep the same queued job rather than resubmitting.

From the next release (#954), Darwin process growth pairs each RSS increment
with its own interval. Flat live samples report zero growth/rate; retained peak
growth is historical sizing evidence. Positive slopes still extend through
sample age. Measured physical RAM bounds forecast charges while retaining actual
RSS and known floors; RSS grants no resident credit. A forecast is not measured
allocation or proof of a historical refusal. See [process growth](../../docs/process-memory-growth.md).

Plain native commands default to **unsandboxed** under MY-25; explicitly set
`isolation: sandboxed` to opt in. New native extension registrations default to
sandboxing; retained registrations keep their frozen policy.

Directory source needs no commit. Deliberately prepare it: ignored files are
included except `.git` and `.mycelium/receipts`; links are refused. For real Git
work, `source: {git: HEAD, paths: [scripts/]}` sends committed selected paths:
commit those source bytes, while the adapter's uncommitted edits are frozen
separately. Keep private receipts ignored and retained for recovery.

Resolve read-only, then submit once:

From the next release, resolve and `myce portability --adapter
.mycelium/adapter.yaml --entry NAME` also expose lost platform breadth: authored
target exclusions, additional selected-lock dependency candidates, every member's
coverage, native preparation and current admission remain separate. Run this
before choosing targets and after inventory or recipe changes. A dependency
candidate is not application compatibility or owner-accepted production parity.
Plain-command `require_target` checks admission, not native qualification.
Explicit targets remain binding. From the next release, a native CPU entry or
profile may set `portable: true` and omit targets to discover candidates from
all members' selected immutable managed Pixi locks and fresh worker inventory.
This declares the whole payload portable, including binaries, shell commands,
archives and runtime downloads. Unknown coverage grants no candidate. Failed
preparation excludes only its exact target/recipe candidate; keep the same job
while a viable alternative runs. Fix a deterministic recipe failure with changed
immutable inputs and a new key. Automatic plain-command discovery is for
non-production tiers: production needs the existing native variant/baseline and
MY-19 parity workflow, or an explicit owner-accepted target list. Preparation,
exit-zero checks and `portable: true` never constitute parity acceptance.

```sh
myce version
myce spec check --resolve --entry ease-audit .mycelium/adapter.yaml
# Choose once; retain this key for retries of this operation.
key="ease-audit-$(date -u +%Y%m%dT%H%M%SZ)"
myce run --entry ease-audit --key "$key"
```

From the release containing #866, resolve and adapter portability reports warn
about missing absolute non-system Mach-O dylib install names in directory-source
runtime candidates, naming the packaged binary and library. This is submitter-local
evidence; Git/snapshot source inspection stays unknown until worker staging.
New profile submissions extract source and apply explicit inputs during worker
staging, as adapters do, so Darwin prelaunch can inspect packaged helpers before
launch. Existing saved receipts retain their original intent; use a new key for
changed intent. Darwin prelaunch checks the staged payload again. `payload_library_missing` is a
payload/environment failure with `execution_cause.linkage.binary` and `.library`,
not an infrastructure loss/retry. Bundle and relink the dependency or choose a
compatible environment, then resubmit changed intent. System/loader-relative
imports and present libraries do not warn. This check does not prove dynamic
loader resolution or application compatibility. See [packaged Darwin libraries](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#packaged-darwin-library-dependencies-next-release-866).

From the release containing #1008, an all-disabled eligible-host production
refusal has `diagnostic_code: host_unavailable` and cause
`launch/host_unavailable/no_eligible_host_enabled` (operation/class/predicate),
with unchanged reason text. Read optional `execution_evidence` on the Job and
each `cancellation_scope.members[]` entry: `attempt`, `main`, `sidecars`, and
`environment_preparation`. Each component is `not_started`, `may_have_run`, or
`unknown`; absent/unrecognized evidence is unknown. Before assignment, attempt
zero proves all three `not_started`. For assigned native work, main includes
its extension wrapper; sidecars are collective; environment acquisition is
conservatively `may_have_run` even on a cache hit. Sidecars/build hooks can run
before main's Started acknowledgement. Memory refusals retain existing
`launch/resource_limit/memory_capacity` with `prelaunch_refused` where applicable,
or native-group `command/stage_failed/memory_capacity`; those fields, interruption,
`starts` and absent `started_at` alone never prove no user code ran. A safe
no-user-code retry needs all three `not_started` across every attempt and each
materialized member, plus the independent release contract. Earlier independent
preparation/qualification jobs are outside this evidence. Incomplete recovery
and older workers remain unknown. Result/run clients return the same Job;
artifact downloads still require accepted outputs. See
[execution evidence](https://github.com/1kuna/mycelium/blob/main/docs/status.md#positive-execution-evidence-1008-next-release).

Resolve uploads nothing; `needs-staging` for the source archive is expected:
`run` stages it automatically. Fix actual input/environment/eligibility blockers
before submitting. Save the receipt's `key` and `job_id`. After a lost reply,
repeat the same command with the saved key to recover that job; changed intent
or an intentional repeat needs a new key.

For optional NAS existence checks, add `--inspect-nas` to resolve. For one path,
repeat after two successful worker heartbeats: the first delivers the request,
the next returns metadata. More paths or slow metadata can take further
heartbeats. `pending` remains unknown; `present` proves neither size nor hash.
From the next queue release, all queue listeners share these observations;
completed samples expire with worker-session freshness rather than a fixed
minute. Staging still verifies the declared bytes before execution.

If a profile or entry exits 69 with `input storage unconfirmed`, its intent and
frozen inputs remain in the receipt. The diagnostic names preflight or upload,
the input, and a sanitized HTTP status/code or transport class. Rerunning the
same command resumes from that receipt without
duplicating anything. Keep its key and receipt rather than making a new submission.

Replace `JOB_ID` below with the receipt's ID. No notifier or harness setup is
required. Wait once in the foreground; exit 0 means the job succeeded. A timeout
or interrupted wait leaves the job alive; resume waiting on the same ID.

```sh
myce wait JOB_ID
myce result JOB_ID --out accepted
printf 'mycelium-ease-audit-ok\n' > expected.txt
cmp expected.txt accepted/result.txt
```

From the next release, `wait` prints one short line per state change (progress
on stderr, terminal states on stdout). Use `myce wait --json JOB_ID` for full
reasons and initial diagnostics, or `myce status JOB_ID` for current details.
Relative result directories also work under symlinked working-directory ancestors;
symlinks below the chosen output directory remain refused.

Success means `result` verified output hashes and sizes **and** `cmp` exited 0:
the exact result is 23 bytes including newline. A wake or command exit alone is
insufficient. On a release containing #811, omitted retention protects results until explicit release; saved positive operator windows still apply.

On releases with the mixed-version cache correction, workers without an optional
shared-input cache-capacity report retain staging-time admission. Reporting workers
are checked before placement; staging capacity failures give required/available/limit bytes.

Use `myce status` for a fleet overview and `myce status --history` for all retained
jobs. **Available from the next release:** status and inspection JSON include
`freshness.captured_at`, `served_at`, `age_seconds` and `cached`; `now` remains
capture time. Interpret worker freshness and diagnostics at that time, allowing
for the response age. Inspection and filtered/full status take a fresh capture;
default status learns its cache lease from local capture/read/write costs, with immediate
terminal/release invalidation.

From the next release, mixed CPU/RAM waits name occupied cores on eligible
hosts alongside the other hosts' RAM shortages. Reserved cores are admission
accounting, not measured CPU utilization; a RAM figure alone does not explain
why every eligible host is waiting.

From the next collector release, job GET/status `resource_attempts` reports
cumulative CPU time, block I/O and latest attempt PSI; machine `io_pressure`
reports host PSI. Read the exact attempt/session, source, age and unavailable
reason. Missing means unknown. macOS sampled CPU is a lower bound and I/O PSI
is explicitly unavailable. These observations do not grant capacity or change
admission. See [CPU and I/O fields](https://github.com/1kuna/mycelium/blob/main/docs/status.md).

From the next release, repeated native plain commands can learn seconds/job
without `workload` units. Status work curves and attempt timing explain the exact
command/source/input/environment comparison, clean sample count and observed
range. A single sample leaves variability unknown; ranges are not deadlines.
Changed source/input bytes, resumptions, overlapping work, pressure and missing
CPU scheduling-pressure/I/O counters leave speed unknown. Quota throttling alone
does not establish a clean CPU comparison. Legacy unversioned inputs and external
work directories/runtimes do not establish a comparison. Describe frozen work
normally: Mycelium uses learned speed after admission and least-valuable fit,
and never waits for a busy faster machine. This does not qualify a platform or
replace output-parity checks.

From the next release (#879), comparable completion forecasts expose
`timing.service_estimate`: raw service seconds, signed correction seconds,
accepted pair count, uncertainty and a fallback reason. `predicted_seconds`
includes that correction. CLI/HTML placement reasons explain it too. Pair count
is distinct from raw timing sample count; observed error ranges are not confidence
bounds or deadlines. Preparation/transfer remain separate and uncalibrated.
Missing or incompatible pairs retain the raw estimate or its unknown state;
forecasts grant no capacity, reserve no slot and do not move running work.

If waiting or failed, read `myce status JOB_ID` and `myce logs JOB_ID`; keep
repeated job observation on the per-job/wait interface. Record a hold with its next owner and continue
independent work. Heavy local execution is never a fallback. For a Mycelium
failure, search existing issues, then report version, job/attempt, expected and
observed behavior; omit credentials, webhook URLs, private paths and host details.

From the next release (#881), job `health_recovery` retains an `owner_required`
decision after repeated infrastructure failures: `action: none` means Mycelium
performed no automatic repair and left capacity unchanged. The summary names
what happened and `next_step` names the owner's action. For `live_host_growth`,
the Mycelium maintenance owner inspects forecast bounds and paired queue/worker
snapshots; the comparison alone does not prove stale free RAM. Keep the same job
ID and submission key. Do not raise shared budgets, clear custody or resubmit to
bypass the finding. It uses the existing health notification channel; no new
notifier setup is needed. Later progress does not establish a causal repair.

On a release containing #813, a projected queued job's waiting reason explains
retained resource-seconds, weighted share, signed fair-share debt (positive ahead,
negative behind), the measured history horizon, weighted oldest project-wait
round, observed active service age and completed service quantum. The horizon follows current demand/service
cycles; no fixed daily half-life controls when a waiting project recovers a turn.
A zero completed quantum means initial learning: history stays intact until an
allocation completes. Unknown reconnect/restart gaps add no observed service.
Tiers, eligibility and explicit
caps/quotas still bind; running same-tier work is not stopped to repay debt.

On releases with adaptive observation admission (#800), healthy queues accept reads at any rate. HTTP 429 `client_rate_limited` means **fleet-wide control pressure**, not a per-client quota. Supporting queues also return `code_alias: observation_shed_control_pressure`. `Retry-After` is the running queue's measured delay rounded up to seconds, typically a few seconds: follow that signal rather than waiting minutes. Installed CLI GETs already retry with positive jitter (#819), bounded by caller cancellation or an explicit deadline, without a fixed retry count. From the release containing #960, stopping recovery prints the last interval. Prefer batched waits; production-class job observations and scalar batch waits remain available. Earlier releases retain the fixed-rate stopgap.

From the release containing #959, interrupted `myce logs JOB` prints `myce logs JOB --after SEQ` on stderr. Resume with that cursor after the reported Retry-After to avoid replaying completed pages; add `--follow` to keep following. From the release containing #890, read-only `myce policy get` resolves installed worker, then queue authority without setting or printing `MYCE_TOKEN`.

**Available from the next release (#810):** CLI and Python waits adapt to measured response cost and revisions, back off scalar reads while unchanged, and approach expected events. Negotiated held long-polls resume immediately so progress stays observable; each CLI job gets an initial diagnostic read to retain its deadline. Reads honour 429/503 `Retry-After` with jitter; old queues use adaptive scalar reads. Bound observation with `myce wait --timeout DURATION`, Python `wait(timeout=SECONDS)`, or the job's deadline; timeout leaves work alive. Without an owner/job deadline there is no fixed observer lifetime. File transfers learn stall windows from complete service periods between socket readiness waits, including buffered draining and producer/consumer work; individual buffered copies and the first arrival cannot establish throughput. Go checks for available socket progress before declaring an expired-poller stall. Before a service period is established and during queue verification before headers, only the owner bounds the operation (Go context deadline, Python `transfer_timeout`).

**Input and accepted-result recovery (next release, #831/#751):** profile/adapter input storage retries span preflight and reconciled uploads under the caller's one context deadline; each retry reports its sanitized cause and honours `Retry-After`. `myce result` uses the same transient recovery for accepted retrieval, including CI. Permanent authentication, TLS, capacity, integrity and configuration errors stop immediately. No retry count or new default deadline is imposed; without an owner deadline, interrupt to stop observation/recovery and resume the saved intent later.

## Adaptive model readiness (next release)

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

## Explicit model master downloads (next release)

Declared `model_snapshots` also handle SMB/CIFS `XSym` emulated symlinks from
the next release: jobs get verified blob bytes, or a `model_snapshot_mismatch`
or `model_snapshot_escape` refusal. Keep the original hub cache intact; do not
copy weights or rewrite shared links to work around a refusal. See
[declared snapshots](advanced.md#submit) for submission and read-only access.

For every declared bundle file, including nested paths, run
`myce model fetch BUNDLE/FILE --model-root PARENT --url URL --size-bytes BYTES --sha256 HASH`.
The root must contain the bundle. Fetch stages beside it, resumes interruptions,
restarts when `Range` is ignored, and publishes only verified files. Invalid
bytes stay in the reported staging quarantine. Retry a directory-sync or
concurrent-fetch error after its cause clears; repeating a completed fetch
verifies the master without replacing it. Keep staging while a fetch is active.
Verify the whole manifest before use; loading never downloads weights. See
[explicit master downloads](https://github.com/1kuna/mycelium/blob/main/docs/services.md#explicit-master-download-and-pins).

## Normal fleet updates (next release)

On releases containing #900's idle-moment upgrade slice, a normal fleet update
keeps busy enabled hosts `pending` and available for new work until a fresh
natural idle moment. Other idle hosts can update first. Keep using the queue;
do not cancel experiments to finish a drain. Started attempts finish naturally,
and exact release custody still gates restart. The separately requested
`--wait-production-idle` policy continues to hold new production assignments.

## Automatic host hygiene (next release)

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
attempts retain custody. From the next release, `mount_probe_slow` is advisory:
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

## Read before extending this fixture

- **Long or multi-stage jobs:** [stage progress and honest ETA](advanced.md#stage-progress-and-honest-eta), including a credential-free reporting example. Checkpoints are separate recovery evidence.
- **Benchmark host telemetry:** [bounded local capture and receipt schema](advanced.md#benchmark-telemetry-receipts) for queue/worker RSS, external RAM and per-card memory/activity; missing samples remain unknown.
- **Unified CUDA sharing (next release):** small declared or learned jobs can share with fresh, reconciled RSS/cgroup evidence even when CUDA reports `N/A`. Helper occupancy is sampled before main but does not mean main started; device counter sums do not prove unique physical residency or release device growth reservations. Uncertain or stale attribution keeps execution exclusive; [sharing and growth rules](advanced.md#submit) explain the estimate and planned stops.
- **Discrete XPU fan-out sharing (next supporting release, default off):** explicitly set `fan_out.share_xpu: true`; every co-tenant must opt in and have fresh card/host/attempt headroom. Compare off/on at experiment class on the same release before owner-accepted production use. No allowance increase is implied. See [sharing and growth rules](advanced.md#submit).
- **GPU, memory, portability, dependencies or models:** [submission contracts](advanced.md#submit), including hard-requirement pins, native qualification, output parity and checkpoints.
- **Many jobs, detached completion or observer recovery:** [waiting contracts](advanced.md#wait-and-check-status); optional [T3 binding](t3-binding.md). Use one foreground batched wait on supporting clients; never construct per-job background waits. From the next release, `myce wait --observer-status` reports local observer progress; newer clients request cooperative upgrades. Legacy shared-observer recovery belongs to its operator.
- **Large outputs, diagnostics, quotas or early release:** [results and custody](advanced.md#results). Release only your own terminal results after all downstream uses and verified copies are complete.
- **Reservation confirmation (#979, next release):** `reserve add` fails on rejected or unconfirmed writes instead of printing an empty record. Keep an explicit `--id`, inspect `reserve list` after uncertain confirmation, and retry the same binding.
- **Urgency, reservations or pausing:** [class and validation rules](advanced.md#urgency-and-pausing). Class is the only urgency lever; shared policy changes belong to the queue owner.
- **Blocked preparation or placement:** [diagnosis](advanced.md#when-a-job-waits).
- **Real adapters, native variants or legacy formats:** [adapter contract](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md), [project setup and profile compatibility](setup-project.md), [harness routing](harness-binding.md).
- **Mycelium repository changes:** [merge gates](advanced.md#mycelium-repository-merge-gates). Feature availability is version-dependent; see [release qualifiers](advanced.md).

For a blocking dependency, next owner and action, see the [diagnostic reference](advanced.md#diagnostic-reference-one-blocker). For one read-only consumer check on supporting releases, use [project doctor](advanced.md#diagnostic-reference-project-doctor).

## Automatic worker collection (next release)

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

## Queue efficiency (next release)

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

## Adaptive host observations (next release)

On negotiated `host-probe-v1` workers, owner/pressure status includes measured
probe period, cost and frozen validity. Read age against that observation's
validity; a successful check-in cannot renew a failed probe. From the host-health
delivery correction (#1025), new health observations include the same learned
report-gap allowance as shared pressure. Stopped or failed sensors still lose
admission while worker contact continues. An unprecedented delay can exceed the
learned prediction; read the frozen validity and age together. Owner-latency holds
now require fresh healthy evidence spanning the attempt's measured interference
interval, rather than a fixed retry delay. Other eligible hosts remain available.
Older workers retain legacy fields and age rules. See
[host observations](https://github.com/1kuna/mycelium/blob/main/docs/host-probes.md).


## Adaptive worker loss (next release, #661)

Worker loss and status freshness follow the session's observed check-in cadence
and control-path variation. Queue transaction time does not count as worker
silence, and an in-flight check-in protects only its worker. During a queue-first
upgrade, headerless
sessions qualify on their second accepted arrival, adding at most one observed
send interval plus control-path delay to the first offer. Startup reports can
qualify earlier; existing attempts retain custody through this idle response.
Existing model engines retain their plans and cleanup deadlines while cadence
qualifies; this cannot start a new preparation on a suspect host.
Queue slowness alone does not charge any class a retry or infrastructure loss. A dead worker normally
needs roughly two send intervals plus learned variation and a maintenance turn;
overload adds actual blocked queue time. Under continuous saturation, silence
still becomes `suspect: true` within the measured wall-arrival allowance plus the next
maintenance/observation turn. A suspect host receives no new work and is never
shown healthy; existing attempts retain custody and spend no retry budget.
Pending native qualification checks retain useful demand during suspicion and
resume under the same identity after fresh contact; suspicion grants no launch.
An already started preparation retains its exact custody through later stages;
an unstarted cached plan cannot start while suspect.
The wall allowance includes observed control delivery cost even for workers
advertising shorter send intervals, starting with measured completed-response
cost. Confirmed loss requires both a missed wall arrival and enough available
control service; its opportunity clock excludes blocked queue time.
Status and NAS inspection delivery share that wall allowance. The
existing health channel raises `worker_suspect` for owner attention. A new
arrival clears suspicion. Confirmed loss has no finite trustworthy bound while
control cannot process evidence. Keep the same job/key and custody while control
recovers; ask the queue owner to inspect persistent suspicion. `last_seen` is still the last accepted wall timestamp; use the
reported freshness at snapshot capture time. Physical pressure, memory and disk
sample validity remains independent and can withhold new work. Saved operator
timeouts are minimums; default zero learns timing. No class upgrade is needed to
protect work from control delay. See [loss timing](../../docs/design-proposal.md#adaptive-worker-loss-661-my-31my-34).
