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
it. Plain native commands default to **unsandboxed** under MY-25; explicitly set
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

If waiting or failed, read `myce status JOB_ID` and `myce logs JOB_ID`; keep full
`myce status` out of loops. Record a hold with its next owner and continue
independent work. Heavy local execution is never a fallback. For a Mycelium
failure, search existing issues, then report version, job/attempt, expected and
observed behavior; omit credentials, webhook URLs, private paths and host details.

## Read before extending this fixture

- **GPU, memory, portability, dependencies or models:** [submission contracts](advanced.md#submit), including hard-requirement pins, native qualification, output parity and checkpoints.
- **Many jobs, detached completion or observer recovery:** [waiting contracts](advanced.md#wait-and-check-status); optional [T3 binding](t3-binding.md). Use one foreground batched wait on supporting clients; never construct per-job background waits.
- **Large outputs, diagnostics, quotas or early release:** [results and custody](advanced.md#results). Release only your own terminal results after all downstream uses and verified copies are complete.
- **Urgency, reservations or pausing:** [class and validation rules](advanced.md#urgency-and-pausing). Class is the only urgency lever; shared policy changes belong to the queue owner.
- **Blocked preparation or placement:** [diagnosis](advanced.md#when-a-job-waits).
- **Real adapters, native variants or legacy formats:** [adapter contract](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md), [project setup and profile compatibility](setup-project.md), [harness routing](harness-binding.md).
- **Mycelium repository changes:** [merge gates](advanced.md#mycelium-repository-merge-gates). Feature availability is version-dependent; see [release qualifiers](advanced.md).

For a blocking dependency, next owner and action, see the [diagnostic reference](advanced.md#diagnostic-reference-one-blocker). For one read-only consumer check on supporting releases, use [project doctor](advanced.md#diagnostic-reference-project-doctor).
