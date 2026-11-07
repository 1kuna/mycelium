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

## First run: adapter to verified result

Mycelium runs CPU and GPU work through one queue. Describe needs and verified
targets; Mycelium chooses placement. Use the installed `myce` CLI, which resolves
authority internally. Never open, print, copy, pass or set fleet credentials.
A missing CLI or failed authority is a setup hold for the installation owner.
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
Cold ceilings leave the host's measured check-in/launch drift unused; strict
worker launch checks still refuse a larger unseen drop and teach the next probe.
A running CPU probe can share after fresh complete cgroup
measurements establish its live footprint and growth over a sampling horizon,
extended by the measured growth rate through the sample's age;
its containment ceiling stays fixed. Missing/stale measurements retain the full
charge. Protected attempts block only capacity they actually reserve, rather
than excluding every exploratory job. A clean accepted completion teaches a
reusable bound.
Comparable peaks use observed variability and sampling growth; cross-target
estimates need paired accepted runs and measured transfer error. Unknown pressure
and incomplete counters remain conservative. From the next release, a submission blocked only by learned or exploratory memory sizing is accepted and waits with `diagnostic_code: memory_evidence_recovering`. Fresh comparable evidence or eligible capacity re-evaluates the same job; do not resubmit. Declared impossible needs still refuse. Constrained recovery uses the observed peak and overshoot headroom, bounded by eligible allowances without lowering measured need, explicit containment or the job's OOM retry floor. See [memory sizing](advanced.md#submit).
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

Resolve uploads nothing; `needs-staging` for the source archive is expected:
`run` stages it automatically. Fix actual input/environment/eligibility blockers
before submitting. Save the receipt's `key` and `job_id`. After a lost reply,
repeat the same command with the saved key to recover that job; changed intent
or an intentional repeat needs a new key.

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
insufficient. Download within retention (default seven days).

On releases with the mixed-version cache correction, workers without an optional
shared-input cache-capacity report retain staging-time admission. Reporting workers
are checked before placement; staging capacity failures give required/available/limit bytes.

Use `myce status` for a fleet overview and `myce status --history` for all retained
jobs. **Available from the next release:** status and inspection JSON include
`freshness.captured_at`, `served_at`, `age_seconds` and `cached`; `now` remains
capture time. Interpret worker freshness and diagnostics at that time, allowing
for the response age. Inspection and filtered/full status take a fresh capture;
default status can reuse a generation for up to one second, with immediate
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

If waiting or failed, read `myce status JOB_ID` and `myce logs JOB_ID`; keep
repeated job observation on the per-job/wait interface. Record a hold with its next owner and continue
independent work. Heavy local execution is never a fallback. For a Mycelium
failure, search existing issues, then report version, job/attempt, expected and
observed behavior; omit credentials, webhook URLs, private paths and host details.

On a release containing #813, a projected queued job's waiting reason explains
retained resource-seconds, weighted share, signed fair-share debt (positive ahead,
negative behind), the measured history horizon, weighted oldest project-wait
round, observed active service age and completed service quantum. The horizon follows current demand/service
cycles; no fixed daily half-life controls when a waiting project recovers a turn.
A zero completed quantum means initial learning: history stays intact until an
allocation completes. Unknown reconnect/restart gaps add no observed service.
Tiers, eligibility and explicit
caps/quotas still bind; running same-tier work is not stopped to repay debt.

On releases with adaptive observation admission (#800), healthy queues accept reads at any rate. During measured control pressure, expensive/lower-value nonproduction reads can return 429; honor `Retry-After` and prefer batched waits. Production-class job observations and scalar batch waits remain available. Earlier releases retain the fixed-rate stopgap.

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
attempts retain custody. NAS/checkpoint degradation withholds dependent work;
independent local work can remain eligible. Existing OS automounts are re-triggered
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
the existing leaving signal for a confirmed local drain until support is negotiated.
From the checkpoint-probe correction, checkpoint health uses the existing tiny
exclusive create/close/remove check instead of enumerating the checkpoint root.
This removes unnecessary directory work; genuinely slow checkpoint metadata
still withholds dependent jobs under the unchanged measured thresholds.

## Read before extending this fixture

- **Long or multi-stage jobs:** [stage progress and honest ETA](advanced.md#stage-progress-and-honest-eta), including a credential-free reporting example. Checkpoints are separate recovery evidence.
- **Unified CUDA sharing (next release):** small declared or learned jobs can share with fresh, reconciled RSS/cgroup evidence even when CUDA reports `N/A`. Uncertain or stale attribution keeps execution exclusive; [sharing and growth rules](advanced.md#submit) explain the estimate and planned stops.
- **GPU, memory, portability, dependencies or models:** [submission contracts](advanced.md#submit), including hard-requirement pins, native qualification, output parity and checkpoints.
- **Many jobs, detached completion or observer recovery:** [waiting contracts](advanced.md#wait-and-check-status); optional [T3 binding](t3-binding.md). Use one foreground batched wait on supporting clients; never construct per-job background waits.
- **Large outputs, diagnostics, quotas or early release:** [results and custody](advanced.md#results). Release only your own terminal results after all downstream uses and verified copies are complete.
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
