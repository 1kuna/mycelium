---
name: mycelium
description: "Use for fleet compute and Mycelium jobs: choose resources and class, submit, wait for completion, inspect holds, fetch results, or pause and resume work."
---

# Mycelium

Describe what the work needs and let the queue choose a machine. Use installed
`myce` or Python `Client.from_installed()`. Keep fleet credentials private and
use the existing installed authority. For a missing CLI, start with
[Setup](setup-project.md#find-the-installed-cli). Send authority refusals to the
installation owner with the typed reason.

## Submit and fetch

```sh
myce run --out accepted --output result.txt -- sh -c 'uname -sm > result.txt'
cat accepted/result.txt
```

A one-off runs with installed tools in an empty worker directory. Use `--cpu N`
for logical cores. For CPU jobs, `--mem 8G` sizes host RAM. With
`--gpu cuda|xpu|metal`, `--mem` sizes device memory and `--host-mem` sizes
additional host RAM. Omit memory to use automatic sizing. In a plain spec or
entry, size CPU RAM with `needs.memory_gb` and additional GPU host RAM with
`needs.host_memory_gb`. Those figures seed an estimate. Use
`advanced.memory_limit_gb` for an explicit host RAM cap.
`advanced.size_hint` is for extension work. See
[needs and sizing](references/submit.md#needs-targets-and-variants).

Use `--key NAME` on inline commands you may need to recover. The default
inline key is fresh for each invocation. `--detach` returns a job ID.
`--out DIR` waits and verifies the size and SHA-256 of each declared output.
Exit 0 establishes verified delivery. Check the content against your task.
See [exit codes](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#run-wait-and-fetch-in-one-command)
and [result recovery](references/results.md) for interrupted publication,
failed-attempt diagnostics and storage release.
Follow the [runnable examples](references/quickstart.md) to try the full path
from a one-off command to a reusable entry and Python.

## Reusable entries

```sh
myce init --entry report --output report.json -- python scripts/report.py
myce run --entry report --wait --out accepted
```

Run from the project root. `myce init` writes `.mycelium/adapter.yaml` and
uses a local Pixi or uv lock when present. Commit the source it runs. Ignore
`.mycelium/receipts/` and your output folder. Declare files, folders or quoted
globs as outputs. Entries derive a key from frozen intent. Recover with the
same command, source, inputs and key. Change the key to repeat unchanged work
intentionally. After a terminal failure, fix the cause and use a new key if
the corrected submission would otherwise replay the failed job.

Use `myce spec check --resolve --entry NAME .mycelium/adapter.yaml` for a
read-only placement preview. `myce run --entry NAME --platform cuda` narrows an
entry to an allowed platform. Use declarations in the entry for resource
sizing. Use a pin only for a [hard requirement](references/submit.md#explicit-placement-restrictions).

- [Environments and isolation](references/submit.md#environments) covers locked
  uv, Pixi and npm, installed tools, and native execution. Use
  [native checks](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#probe-a-software-capability-instead-of-pinning-a-host)
  for capabilities that executable names alone cannot establish.
- [One PyTorch entry across accelerators](https://github.com/1kuna/mycelium/tree/main/docs/examples/run-anywhere)
  shows a multi-platform lock and runtime device selection using `MYCE_DEVICE`.
  Use qualified variants when implementations differ. GPU work requires an
  accelerator by default. For a compatible plain command, set
  `advanced: {allow_cpu_fallback: true}` only when CPU results are acceptable.
  For extension work, declare a CPU variant.
- [Inputs, checkpoints and pieces](references/submit.md#inputs-checkpoints-and-pieces)
  covers `--input DEST=PATH`, verified local staging of `shared_inputs`,
  runtime bindings, `pieces:` and extension fan-out. Submit known later
  stages with `after:` so the queue sees the pipeline and starts them on
  accepted producer outputs. See [dependent stages](https://github.com/1kuna/mycelium/blob/main/docs/jobs.md#start-a-consumer-after-every-producer-piece-succeeds).
- [Resident engines](https://github.com/1kuna/mycelium/blob/main/docs/services.md#resident-engines-for-independent-stage-jobs)
  covers reusing a catalog model across independent inputs.
- [Ephemeral worker data](https://github.com/1kuna/mycelium/blob/main/docs/files.md#ephemeral-worker-data)
  covers compatibility, cleanup receipts and release. Check the cleanup receipt
  for the exact attempt before claiming local removal.
- [Disk needs](references/submit.md#disk-needs) covers first-run sizing for
  large scratch, unknown input/output sizes and checkpoint storage. Use
  `MYCE_CPUS` for thread pools and `TMPDIR` or `MYCE_SCRATCH_DIR` for scratch.

Put requirements, class, commands and I/O up front in the spec. Use
[`advanced`](https://github.com/1kuna/mycelium/blob/main/docs/jobs.md#advanced-settings)
for opt-ins and other settings as needed. Submit a spec with `myce submit FILE`
or Python `client.submit(spec)`.

## Classes and tiers

Choose a class before submission. Owner policy maps project/class pairs to
ordered tiers. Default order is production, deadline, then experiment. The
planner prefers fitting free capacity, applies fair sharing within a tier and
waits for exact release when it displaces work.

| Class | Use |
| --- | --- |
| `production` | User-facing work defined as production by its project. Declare explicit targets and approve each eligible machine kind after output parity (MY-19/MY-51). |
| `deadline` | Work with a real owner-set deadline. Supply `--deadline RFC3339` with the class. |
| `experiment` | Default for research, previews and sweeps. A priority stop can requeue it. Work without resumability starts over. |
| `ci`, `test`, `batch`, `rollout`, `eval` | Bulk work. Shared interactive hosts refuse these classes and same-named owner tiers. |

Read [classes and preemption](references/waiting.md#classes-and-preemption)
for start windows, retry budgets, memory-safety stops, scarce-host yield and
parity approval. Use a [production lease](references/waiting.md#production-leases)
only for future production stages you cannot submit yet. A lease protects drain
coverage and grants no priority or reservation.

Within a project and tier, stages with verified upstream outputs get first use
of shared host capacity before the project's new root jobs.

## Checkpoints

Make work checkpointable or otherwise resumable. When higher-priority work
needs its memory, Mycelium can stop and requeue the attempt. Work that cannot
resume starts from scratch.

Before marking work `advanced: {resumable: true}`, make it save atomically
under `MYCE_CHECKPOINT_DIR`, reopen the last verified generation at startup
and handle TERM within its stop grace. Save periodically. Python
`Checkpoint` and `StopSignal` implement the publication and stop pattern.
Keep saving children alive until they finish and put checkpoint files outside
the output tree. Read [the checkpoint guide](https://github.com/1kuna/mycelium/blob/main/docs/files.md#resumable-jobs)
for storage holds, target compatibility, NAS save notification, retention and
the `checkpoint: local_first` opt-in. A queue-accepted generation is portable
to a compatible worker. Check verified NAS mirror state separately when NAS
durability is required. Status comes from the running worker.

## Wait, inspect and recover

- Use foreground wait for immediate work. One `myce wait JOB_A JOB_B` observes
  up to 256 jobs. Ctrl-C or wait timeout leaves jobs running. Recover a
  detached run with the same known key or use its returned job ID.
- For long entries, submit without `--wait`. Use the [T3 wake binding](t3-binding.md)
  when available. Otherwise follow the receipt's `wake:` instruction and
  own one wait for the batch. Use [wait and status](references/wait-and-status.md#observers-and-detached-completion)
  to verify observer ownership and recover uncertain delivery.
- For a blocker, read `myce status JOB_ID` once and follow `hold.next` or
  `finding.action`. `--history --json` adds exact-attempt diagnostics. Keep
  the job and key through capacity, maintenance, preparation and recovery
  waits. Missing fields are unknown. See [holds](references/waiting.md) for
  capacity versus coverage, memory, storage and device evidence. Route owner
  actions with the job ID, attempt and typed reason. Keep full history reads
  out of loops.
- A `memory_wait` or occupied-card `capacity` hold clears as room or custody
  becomes available. A `memory_capacity` hold gives the estimate and the
  allowance of each eligible pool that is too small even when empty.
- Fetch with `myce result JOB_ID --out DIR`. Fetch successful results before
  file expiry (72 hours by default). See [results](references/results.md) and
  [retention](https://github.com/1kuna/mycelium/blob/main/docs/retention.md#job-records)
  for protected files, logs and historical records.
- For a failing or stalled queue call, consult [queue recovery](references/wait-and-status.md#slow-or-stalled-queue).
  Repeat safe reads or the same keyed write within the caller's budget. Keep
  permanent TLS, authority and integrity refusals with their original reason.
- For fleet startup, upgrade, containment migration or host-level recovery,
  give the installation owner the typed evidence and keep existing job IDs.
  See [owner operations](references/owner-operations.md).
- Use `myce efficiency --project NAME` for sizing and placement evidence, and
  [file holdings](references/results.md#large-outputs-and-release) before
  releasing finished files.

## Pause, resume and cancel

Pause your own queued job with `myce pause JOB_ID --reason TEXT` and resume it
with `myce resume JOB_ID`. Add `--until RFC3339` for automatic resume. For a
running job, use `myce pause JOB_ID --yield --project NAME --reason TEXT`.
Wait for the attempt's exact release and `paused` state before resuming.
Non-resumable yield requires `--confirm-restart`. See
[pausing](references/waiting.md#reservations-and-pausing) for API fencing.

`myce cancel --wait JOB_ID` waits for exclusion and release. A timeout before
acknowledgement leaves the outcome uncertain. A pending release at the deadline
exits 75 with the last acknowledgement. Retry by the same ID or project/key.
For fan-out, cancel the parent and read its aggregate release. See
[abort and wait for release](https://github.com/1kuna/mycelium/blob/main/docs/jobs.md#abort-and-wait-for-release).

## References

- [Setup](setup-project.md), [harness binding](harness-binding.md) and
  [T3 binding](t3-binding.md).
- [Python client](https://github.com/1kuna/mycelium/blob/main/clients/python/README.md)
  for `run_entry`, keyed submission, batch waits and verified downloads.
- [Laptop quick start](https://github.com/1kuna/mycelium/blob/main/docs/START-HERE.md#five-minute-start)
  uses `myce dev -- COMMAND` to exercise the real API and worker with fake GPU inventory.
- File problems on GitHub with expected and observed behavior, version, job
  and attempt. Keep credentials and private paths out of the report.
