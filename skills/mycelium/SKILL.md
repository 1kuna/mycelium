---
name: mycelium
description: "Use when work needs fleet compute (heavy tests, builds, sims, training, transcodes, model runs), when you submit, check, wait on or fetch a Mycelium job, when a job is waiting, when your work must go first, or when you need to pause your own queued job."
---

<!-- Public consumer guide. Edit this contract in place; detail lives in references/. -->

# Mycelium

Mycelium runs CPU and GPU work through one queue. You describe what the work
needs; Mycelium picks the machine, runs it and returns verified output. Every job
takes one path: **declare needs → submit once → one batched wait → fetch → verify.**

## Ground rules

- Use the installed `myce` CLI or Python `Client.from_installed()`; both resolve
  credentials themselves. Never open, print, copy, pass or set fleet credentials.
  A missing CLI or refused authority is a hold for the installation owner: keep
  its typed refusal; heavy local execution is not a fallback.
- Leave machine and device choice to Mycelium. A backend such as `platform: cuda`
  already selects compatible hardware, even when only one host has it. The worker
  tells the command what it received through `MYCE_DEVICE` and assigned
  visibility. Restrict hardware only for a real requirement, such as an exact
  device under qualification or an owner-dedicated or prohibited host; see
  [explicit restrictions](references/submit.md#explicit-placement-restrictions).
- Check `myce version` and the command's `--help`. A field your installed queue
  or worker doesn't return is unknown, not false; see
  [release behavior](references/release-behavior.md).

## 1. Declare needs

An existing project keeps its work in `.mycelium/adapter.yaml`; use its entry.
For a new project, one entry is enough. This dependency-free CPU fixture uses
only tools on each worker's PATH:

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

`needs` is what the work uses. `targets` lists the OS/architecture types the
software supports, not machines; `myce portability` shows your cluster's types.
On real work, omit memory and Mycelium learns it; the tiny value here only
bounds the fixture. A directory source includes ignored files except `.git` and
`.mycelium/receipts`, and refuses links; `source: {git: HEAD, paths: [...]}`
sends committed bytes instead. GPUs, models, environments, NAS inputs,
checkpoints and isolation are in the [submit reference](references/submit.md).

## 2. Submit once

```sh
myce spec check --resolve --entry ease-audit .mycelium/adapter.yaml
key="ease-audit-$(date -u +%Y%m%dT%H%M%SZ)"
myce run --entry ease-audit --key "$key"
```

Resolve is read-only: it reports which targets can run the entry and names
anything missing. `run` prints a receipt with `job_id`; keep the job ID and key.
The key makes submission safe to repeat. After a lost reply, `submission
unconfirmed` or exit 69 `input storage unconfirmed`, rerun the same command with
the same key and frozen inputs and you get the same job. Changed intent needs a
new key. A one-off without inputs or outputs can use `myce run --cpu N -- CMD`.

## 3. One batched wait

```sh
myce wait JOB_ID
```

One foreground `myce wait JOB_A JOB_B` covers up to 256 jobs. A timeout or
interruption leaves the jobs running; keep the IDs and wait again. Progress
comes from that wait or one status read, never from polling loops, per-job
background waiters or repeat submissions. For completion while you're away,
bind a notifier to your own thread with [T3 binding](t3-binding.md).

## 4. Fetch, 5. verify

```sh
myce result JOB_ID --out accepted
printf 'mycelium-ease-audit-ok\n' > expected.txt
cmp expected.txt accepted/result.txt
```

`myce result` downloads accepted outputs and verifies each file's size and
SHA-256. Success takes all three: the job `succeeded`, `result` verified the
files, and your own application check passed (here 23 bytes, newline included).
A wake, notification, terminal receipt or exit code alone is not success; an
exit-zero command can still fail its output contract. See [results](references/results.md).

## Python

The same journey, run in a fresh directory:

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

Keep the key and spec unchanged when retrying an uncertain reply. Batch waits
use `client.wait_many([JOB_A, JOB_B])`. The client retries overload and
transient TLS failures within your deadline; unkeyed writes and model calls are
not replayed.

## When a job waits

Read `myce status JOB_ID` once. Its blocker names an owner and an action: fix
what you authored (needs, inputs, recipe), or record that owner and action on
your work record and keep other work moving. Keep the same job ID and key while
capacity, preparation, a device or storage recovers. A hold is resolved by its
owner; resubmitting, pinning a host, editing mounts, raising shared budgets,
clearing devices or running locally only hides it.
On the interactive host, owner pressure checkpoints or stops and requeues your
job (same job and key, no retry charge); wait as normal. Mark long jobs
resumable to keep progress.

Class sets urgency: `production` only for work your project's rulings call
production, `deadline` only with a real owner-set date, `experiment` otherwise.
Pause only your own queued job with `myce pause JOB_ID --reason TEXT`; `myce
resume JOB_ID` keeps its key and queue age. [Waiting](references/waiting.md)
explains each blocker, admission evidence, reservations and pausing.

Default `myce status` is a compact view of current work, not proof that a job
finished or was lost; details, history and paging are opt-in through
[wait and status](references/wait-and-status.md).

## Report a problem

Search the public issues first. On your existing work record, note expected and
observed behavior, component version, job and attempt, and the next owner. The
public repository gets no credentials, webhook URLs, private paths or
installation details.

## Try it without a fleet

On macOS or Linux with Git, Go 1.25.6+ and a Unix shell, follow the
[quick start](../../docs/START-HERE.md#five-minute-start):
`./myce dev -- sh examples/quickstart.sh` starts a temporary queue and workers
with fake GPU inventory, runs real commands, waits and fetches verified output.
It needs no installed fleet and qualifies no hardware. Plain `myce dev` stays
in the foreground until stopped.

## References

Load the page for your branch:

- [Submit](references/submit.md): one-offs, GPU and memory, variants and
  portability, models, environments, NAS inputs, checkpoints, fan-out,
  isolation, uploads, explicit placement restrictions.
- [Waiting](references/waiting.md): blockers, admission evidence, device and
  storage holds, urgency, reservations, pausing.
- [Wait and status](references/wait-and-status.md): batch waits, status sections,
  observers and detached completion, progress and ETA, telemetry, project doctor.
- [Results](references/results.md): output-contract failures, diagnostics,
  large outputs, release and scratch retention.
- [Project setup](setup-project.md), [harness binding](harness-binding.md),
  [T3 binding](t3-binding.md): authoring, profiles, skill install, notifiers.
- [Release behavior](references/release-behavior.md): capability evidence and
  component-dependent contracts.
- [Owner operations](references/owner-operations.md): queue-owner writes, release
  work, Mycelium repository merges.
