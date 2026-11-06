---
name: mycelium
description: "Use when work needs fleet compute (heavy tests, builds, sims, training, transcodes, model runs), when you submit, check, wait on or fetch a Mycelium job, when a job is waiting, when your work must go first, or when you need to pause your own queued job."
---

<!-- Public consumer guide: no host names, fleet layout, private paths or project names. Update with consumer behavior changes; release qualifiers distinguish merged code from the deployed release. -->

# Mycelium

## First run: adapter to verified result

Mycelium runs CPU and GPU work through one queue. Describe needs and verified
targets; Mycelium chooses placement. Use the installed `myce` CLI, which resolves
authority internally. Never open, print, copy, pass or set fleet credentials.
A missing CLI or failed authority is a setup hold for the installation owner.
From the next release, `host_capacity_accumulation` means new CPU work is waiting
so ready accelerator work can acquire host RAM/cores as running CPU attempts
finish. Mycelium chooses one fitting target; higher tiers and spare capacity
elsewhere remain eligible. See [waiting reasons](advanced.md#when-a-job-waits).
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
headroom on one host until a clean accepted completion teaches a smaller bound.
Comparable peaks use observed variability and sampling growth; cross-target
estimates need paired accepted runs and measured transfer error. Unknown pressure
and incomplete counters remain conservative. See [memory sizing](advanced.md#submit).
Plain native commands default to **unsandboxed** under MY-25; explicitly set
`isolation: sandboxed` to opt in. New native extension registrations default to
sandboxing; retained registrations keep their frozen policy.

Directory source needs no commit. Deliberately prepare it: ignored files are
included except `.git` and `.mycelium/receipts`; links are refused. For real Git
work, `source: {git: HEAD, paths: [scripts/]}` sends committed selected paths:
commit those source bytes, while the adapter's uncommitted edits are frozen
separately. Keep private receipts ignored and retained for recovery.

Resolve read-only, then submit once:

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

Success means `result` verified output hashes and sizes **and** `cmp` exited 0:
the exact result is 23 bytes including newline. A wake or command exit alone is
insufficient. Download within retention (default seven days).

On releases with the mixed-version cache correction, workers without an optional
shared-input cache-capacity report retain staging-time admission. Reporting workers
are checked before placement; staging capacity failures give required/available/limit bytes.

If waiting or failed, read `myce status JOB_ID` and `myce logs JOB_ID`; keep full
`myce status` out of loops. Record a hold with its next owner and continue
independent work. Heavy local execution is never a fallback. For a Mycelium
failure, search existing issues, then report version, job/attempt, expected and
observed behavior; omit credentials, webhook URLs, private paths and host details.

On releases with adaptive observation admission (#800), healthy queues accept reads at any rate. During measured control pressure, expensive/lower-value nonproduction reads can return 429; honor `Retry-After` and prefer batched waits. Production-class job observations and scalar batch waits remain available. Earlier releases retain the fixed-rate stopgap.

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

## Automatic host hygiene (next release)

Machine status `host_health` explains automatic admission drains with typed
causes, persisted recent samples and the host's own healthy baseline. Mycelium
resumes after measured recovery; an operator drain remains independent. Running
attempts retain custody. NAS/checkpoint degradation withholds dependent work;
independent local work can remain eligible. `host_degraded` and
`host_mount_degraded` use existing health notifications; silent-worker alerts
carry `host_silent`. Read the job's waiting reason or one needed status snapshot;
no agent polling or drain/resume loop is needed. Foreign CPU/RSS leaders are
context for the host owner: never stop their processes or services. See
[host-health fields](https://github.com/1kuna/mycelium/blob/main/docs/status.md#automatic-host-health-834-next-release).
Older queues omit the new status fields; workers retain local history and use
the existing leaving signal for a confirmed local drain until support is negotiated.

## Read before extending this fixture

- **Long or multi-stage jobs:** [stage progress and honest ETA](advanced.md#stage-progress-and-honest-eta), including a credential-free reporting example. Checkpoints are separate recovery evidence.
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
