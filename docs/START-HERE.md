# Start here: move a project's GPU work to Mycelium v2

Read this first when working in another repository. Follow the links for your chosen path; you do not need to read every guide before trying a job.
On the experimental v3 branch, see [automatic joining and capability profile v0](guide-joining-v3.md), [polite part-time hyphae](guide-polite-hyphae.md), and [seeing your mycelium](guide-status-v3.md). The v2 setup below remains available.

## 1. What Mycelium is

Mycelium is one Go program, `myce`, that shares GPUs across projects.
A queue service stores jobs in one local SQLite database and chooses their order.
A worker is a process on a GPU machine that runs commands and reports results.
`myce join` auto-detects its GPU like `myce install worker`; `--gpu` and
`--device` provide explicit overrides; see [worker setup](operations.md#inventory-and-foreground-startstop).
`myce dev` keeps fake GPU inventory.
Attempts and model servers get `MYCE_PLATFORM`, `MYCE_DEVICES`, and `MYCE_MACHINE`; see [placement and host holds](job-reference.md#attempt-placement-environment).
A job is either a command that finishes or one request to a queue-managed model.
**GPU work runs as queue jobs; the project never holds a GPU between jobs.**

CPU-only work stays outside: file watching, parsing, preparing inputs, search
that already runs on the CPU, and coordinating requests. Do not turn CPU work
into GPU work as part of this migration. The queue can keep a model loaded for
calls; the queue owns that process and GPU memory, not the calling project.
The [approved design](design-proposal.md) is the product specification.

## 2. Pick your path

Several rows may apply to one project. Start with the smallest GPU operation
that produces a useful result and preserve its existing inputs and outputs.

| What the project needs | Read for details | Apply below |
|---|---|---|
| One-off GPU command or batch job (a command that runs and finishes) | [Integration examples](integrating.md#atlas-batch) and [job fields](job-reference.md#batch-yaml) | Try-out; checklist steps 1–6 and 8–9 |
| Long experiment that survives being stopped | [Resumable jobs](guide-resumable-jobs.md) and [Python checkpoint helpers](../clients/python/README.md#resumable-commands) | Checklist step 7; test stop/save/restart |
| Production work that must start promptly | [Production handoff](integrating.md#production-handoff), [production reservations](integrating.md#nas-model-masters-and-instant-production), and [scheduling](job-reference.md#scheduling-and-results) | Checklist step 4; production `start_by` defaults to submission + 150s; measure startup separately from model loading |
| Calls to a model the queue keeps loaded, using OpenAI-style chat, text, or embeddings | [Model calls](guide-model-calls.md) | Checklist steps 3–5 and 8–9; use calls from the CPU caller |
| Speech, OCR, or another model HTTP server | [General model servers](guide-servers.md) | Preserve the HTTP interface; set `max_in_flight: 1` for serial servers |
| Python packages or other software built on workers | [Software environments](environments.md) | Checklist step 6; a ready environment is required before execution |
| A model file from network-attached storage (NAS), the shared file server | [Model masters and cache](guide-model-calls.md#explicit-master-download-and-pins) and [cache operations](operations.md#model-master-folder-worker-cache-and-production-reservations) | Checklist step 6; arrange verified files before real use |

Production priority is not a promise of zero wait. Missing capacity, software,
model files, or model startup can still delay it. A pin is a queue-owned model
reservation that prevents unloading. For latency-sensitive model calls, agree on
pins with Zach and wait for ready status before directing traffic to them.

For whole-machine takeover and a default 180-second idle cooldown, opt into
`queue --host-config` for the shared physical `host_id`; pins remain protected.
Project `allowed_machines` in `--project-config` restricts commands and model calls.
See [the exact policy and recovery contract](job-reference.md#physical-host-production-hold).

## 3. Ten-minute try-out: laptop, no GPU

You need Go 1.25.6 or newer and a Unix shell. These fake GPUs only simulate
inventory; job commands really execute on your laptop. This shell job is a
simulation of GPU work, not a real GPU test. Use temporary state outside repos.

In terminal A, build from the Mycelium checkout, then start dev:

```sh
cd /Users/zach/Developer/mycelium
go build -o /tmp/myce-starthere ./cmd/myce
mkdir -p /tmp/myce-starthere-demo
cd /tmp/myce-starthere-demo
MYCE_TOKEN=dev-token /tmp/myce-starthere dev --workers 1 --memory-gb 8
```

Leave it running. Dev starts a local queue and one fake worker, with a real
command runner. If port 7331 is occupied, do not stop its owner: choose an unused
port with `dev --listen 127.0.0.1:PORT` and use that port in the address below.
Use a fresh temporary directory if this demo directory already holds old work.

In terminal B, create a YAML file (the text job description):

```sh
cd /tmp/myce-starthere-demo
export MYCE_QUEUE=http://127.0.0.1:7331
export MYCE_TOKEN=dev-token
cat > job.yaml <<'YAML'
name: starting-guide-demo
project: demo
priority: experiment
run:
  command: [sh, -c, 'echo "job=$MYCE_JOB_ID attempt=$MYCE_ATTEMPT"; echo "checkpoint=$MYCE_CHECKPOINT_DIR"; sleep 60; echo done']
needs:
  memory_gb: 1
  platform: any
resumable: false
YAML
id=$(/tmp/myce-starthere submit job.yaml)
/tmp/myce-starthere status --full
/tmp/myce-starthere logs "$id"
/tmp/myce-starthere cancel "$id"
(/tmp/myce-starthere wait "$id" > wait.log 2>&1; echo "$?" > wait.exit) &
```

End your turn. Once the shell reports that background command has exited, read:

```sh
cat wait.log wait.exit
/tmp/myce-starthere logs "$id"
/tmp/myce-starthere status --full
```

Status may first show `queued`. The background wait ends when cancellation
finishes; its exit code is `1` for this cancelled job. Read the saved exit code
and logs after it exits. The final state should be `cancelled`. Logs include the
job and attempt (one execution of that job) and its checkpoint folder. If you
cancel before it starts, there may be no command logs.
The HTML status page is also at the queue address. It is read-only.

Ctrl-C in terminal A stops dev. Its database, checkpoints, and worker state
remain in the demo directory; dev can reuse them. Move the demo directory to
Trash when finished. Never run two dev processes against the same database.
For parser help, run `/tmp/myce-starthere dev --help` (and replace `dev` with
`submit`, `status`, `logs`, `wait`, or `cancel`). Wait help exits zero;
the other commands' help prints usage and exits nonzero.

## Waiting for long jobs (agents)

Submit the job and record its full ID. Start **ONE** cheap background command,
`myce wait "$id"` (or `myce wait "$id" && <next step>`), then end your turn.
Use the host's detached-task facility, or `nohup` with redirected output if the
shell will close. Do not keep an AI agent alive checking status. Only after the
wait exits, read `myce logs "$id"` / status and the wait's exit code once.
The try-out above saves that code in `wait.exit`; use separate files per wait.

`myce wait ID [ID...]` waits for every listed job, prints one final-state line
per job, and reconnects after queue/network outages. Exit codes: `0` all
succeeded; `1` any failed/cancelled; `2` usage/auth/API error (including an unknown
ID); `3` timeout; `4` interrupted. Optional `--timeout 2h` bounds only the wait
and **does not cancel jobs**. A timeout or error is not job completion: retain
the IDs and arrange another single background wait if work is still running.
See [wait flags and results](job-reference.md#cli-flags-and-defaults).

## 4. Integration checklist

1. **Find the real GPU boundary.** List each GPU command and model call, its
   inputs, outputs, existing model identity, and how failure is reported.
   Keep the CPU caller outside the queue. Preserve behavior with migration off.
   Do not replace a project's model, precision, context, or preprocessing.

2. **Add a project-owned on/off switch, default off.** This is your project's
   configuration, not a built-in Mycelium flag. Off must retain the existing
   path. On must select the queue path and fail visibly if it cannot work;
   do not silently fall back to another GPU owner. Tests explicitly choose both.
   Ensure a queued command cannot recursively submit its own GPU work again.

3. **Configure the caller privately.** Zach or the queue operator supplies
   the reachable queue address and shared fleet token. Read `MYCE_QUEUE` and
   `MYCE_TOKEN` from the caller's environment. Do not guess a production address
   from a guide. Use the local values above only for dev. Keep the fleet token
   out of commits, job YAML, logs, command arguments, and job environments.
   Do not copy the caller's entire environment into `run.env`.
   Workers remove `MYCE_TOKEN` before execution, but other project secrets are
   not automatically removed. Pass only the job's necessary configuration.

4. **Choose project and priority deliberately.** Use a stable nonempty `project`
   label for command jobs; model calls carry the same label in routing headers.
   `production` is for live work; `experiment` is for background research.
   `deadline` is between them and requires a timezone-bearing `deadline` time.
   A deadline orders waiting work; it does not kill a running command.
   Production can stop lower-priority work, including non-resumable commands
   that then restart from scratch. Resumable work normally gets 10 seconds to
   save for production; deadline preemption gives two minutes and only stops
   resumable work. See [the reference](job-reference.md#scheduling-and-results).

5. **Choose the command line (CLI) or Python at the CPU caller.** The CLI serves
   manual trials and shell callers; `submit` prints the job ID. Retain that ID
   and use `myce wait` as described above.
   For Python 3.10+, install the client from the Mycelium checkout into the
   project's own virtual environment (an isolated Python/package directory):

   ```sh
   python -m pip install '/Users/zach/Developer/mycelium/clients/python[yaml,openai]'
   ```

   Run that with your environment's Python; macOS system Python may be too old.
   The package imports as `mycelium`. The optional `yaml` dependency reads YAML;
   `openai` supports the standard OpenAI client. Dict submission needs neither.
   From another repo, a relative `./clients/python` install path will not work.
   For a different checkout location, use its absolute path.

   ```python
   from pathlib import Path
   from mycelium import Client
   client = Client()  # reads MYCE_QUEUE and MYCE_TOKEN
   job = client.submit(Path("job.yaml"))
   result = client.wait(job["id"], timeout=600)
   if result["state"] != "succeeded":
       raise RuntimeError(f"job ended {result['state']}: {result.get('reason', '')}")
   ```

   `Path` means a YAML file; a string means YAML text. Read logs with
   `client.logs(job["id"], follow=True)` and cancel with `client.cancel(job["id"])`.
   A wait timeout does not cancel a job. CLI log following also does not assert
   success: use `myce wait` to report the outcome. See [client details](../clients/python/README.md).
   For OpenAI calls use `Client.openai` as shown in [the integration examples](integrating.md#python-project-client).
   For other HTTP APIs follow [general servers](guide-servers.md#http-passthrough).
   A batch's `needs.model` is a placement/copy hint; it does not start a server.

6. **Make execution possible on every eligible worker.** Choose measured
   `needs.memory_gb` and optional additional `needs.host_memory_gb`, supported `needs.platform`, and any `allowed_machines`.
   `host_memory_gb` reserves regular RAM shared by the host; unified memory also
   charges GPU allocations. See [RAM admission](operations.md#host-ram-admission).
   `platform: any` is for genuinely portable commands, including laptop fakes;
   it does not convert CUDA software to Metal or XPU. Use an argument array in
   `run.command`, not an implicit shell. Set an existing absolute `run.workdir`.
   Submission sends a job description, not your repository or input files.
   Local laptop paths work in dev because caller and worker share a machine;
   they will not appear on remote workers without explicit provisioning.
   Keep shared inputs/outputs on agreed mounts or install the source on workers.
   For dependencies, choose a named `environment` recipe or a queue-readable
   lock file via [environments](environments.md). Workers can build those recipes;
   a lock snapshot does not copy your project source or local dependencies.
   For models, arrange immutable NAS masters, verified sizes/checksums, and
   worker-local caches using [the model guide](guide-model-calls.md).
   Plain commands must receive/use their own configured model file path;
   `needs.model` does not rewrite command arguments or supply a model-path variable.
   Model uploads share a 16 GiB queue storage budget (`--upload-spool-bytes`);
   a full budget returns HTTP 507. See [upload limits](job-reference.md#cli-flags-and-defaults).
   Before qualifying several resident models, `myce measure --warmup WARMUPS.json
   --warmup-parallel N` overlaps warm-ups after serial loads when N > 1; see
   [joint measurement](operations.md#general-model-servers-and-joint-measurement).

7. **Implement checkpoints before claiming resumption.** A checkpoint is saved
   progress from which the command can restart. Use `MYCE_CHECKPOINT_DIR`, set
   by the worker to this attempt's directory; do not override it in the job.
   Queue `--checkpoints` and worker `--checkpoint-root` point to each machine's
   mount of the same shared NAS directory. Dev creates a local equivalent.
   Use [the checkpoint guide](guide-resumable-jobs.md) or Python `Checkpoint` and
   `StopSignal` helpers. Save atomically (a complete replacement, never a half
   file), save periodically, and save/exit on SIGTERM, the stop signal.
   Handle failed saves visibly and retry temporary storage failures.
   Make output writes safe to repeat or isolate them by attempt too.
   Set `resumable: true` only after stop/restart tests prove continued progress;
   otherwise leave it false. Cancelling ends the job; it is not a resume test.

8. **Test through the real local queue.** First check planned demand with
   `myce sim --fleet FLEET.yaml --workload WORKLOAD.yaml --days 7 --seed 1 --json /tmp/myce-plan.json`;
   see [the simulator and its limits](simulator.md). It does not qualify hardware.
   Start dev as above, then run the
   project's integration tests with the switch off and on. Off must need no
   queue. On must submit/call, verify complete results, report failure, and
   cancel where appropriate. Assert the token is absent in executed commands.
   Fake inventory does not emulate GPU libraries: use a clearly labelled fake
   work command for laptop tests. For model calls, dev's `dev-model` produces
   fake answers; never use those as evidence of model quality.
   For resumable work use one worker with an 8 GiB budget, an experiment needing
   all 8 GiB, then production needing all 8 GiB. Wait for a saved checkpoint
   before production submission. Verify a later attempt resumes saved progress
   and completes; [the worked example](integrating.md#verso-production-and-resumable-work)
   shows the full sequence. Size both jobs to force competition, not coexistence.

9. **Prepare the handoff and leave the switch off.** Write the migration note
   described below. Bring unresolved capacity, missing source, runtime identity,
   or ownership questions to Zach before any real-machine change.

## 5. Done means

Show Zach passing existing/affected tests with the switch off, and passing
queue integration tests with it on against `myce dev`. Include observed job
IDs, terminal states, complete outputs, and stop/checkpoint/restart evidence
where applicable. Test failures and queue errors must be visible to the caller.

Write a migration note in your project's docs, as the Verso and Atlas migrations
do: inventory GPU paths and CPU work left outside, describe off/on behavior,
record source/runtime/model requirements, test commands/results, remaining gates,
and the ordered enable/disable procedure. Separate prepared changes from
hardware-qualified or deployed work. Include how to restore the previous path.

**Nothing has changed in production.** The switch remains off. Laptop success
proves integration mechanics, not real GPU capacity, latency, or model quality.
Zach decides when to enable it after the required hardware and ownership gates.

## 6. Rules / don'ts

- Do not start or keep a project-owned GPU server between jobs.
- Do not queue CPU-only work or nest queue calls inside a GPU-holding command.
- Do not expose the token or pass it into command jobs.
- Do not claim submission, finished log following, or fake inference is success.
- Do not assume retries mean outputs happen exactly once; make repeats safe.
- Do not install services, change GPU ownership, or deploy as a laptop test.
- Do not silently substitute models, runtimes, or unsupported GPU platforms.

## 7. Everything else

- [README](../README.md): read for the repository overview and quick command map.
- [Approved design](design-proposal.md): read for product intent and boundaries.
- [Integration examples](integrating.md): read for complete Atlas/Verso-shaped recipes.
- [Job reference](job-reference.md): read for all job fields, states, flags, and HTTP contracts.
- [Resumable jobs](guide-resumable-jobs.md): read for saved-progress and failure recovery rules.
- [Model calls](guide-model-calls.md): read for OpenAI routing, verified model files, and pins.
- [General servers](guide-servers.md): read for other HTTP interfaces and multi-file models.
- [Environments](environments.md): read for dependency recipes, lock files, builds, and cache failures.
- [Python client](../clients/python/README.md): read for submission, results, logs, and checkpoint helpers.
- [Operations](operations.md): read when preparing worker/queue setup, monitoring, or recovery.
- [Production test plan](production-test-plan.md): read before an authorized real-hardware acceptance run.
- [Sharing and usage](guide-fairness-usage.md): read for project weights, experiment limits, and usage reports.
- [Examples](../examples/): use as configuration starting points; replace and qualify placeholders.
- [Deployment templates](../deploy/README.md): read only when preparing authorized service installation.

Docker batch jobs must opt into [managed containment](job-reference.md#managed-docker-batch-commands); [unconfirmed cleanup](operations.md#docker-cleanup-custody) retains the physical host's reservation.
To take a worker host out immediately and keep it out across reboots, run `myce disable`; use `myce enable` to rejoin ([operations](operations.md#take-a-machine-out-of-mycelium)).

## v3 extension work

The separate v3 branch adds small extensions and jobs described as a kind of
work or a program. This is opt-in and does not change the v2 migration steps.
Start with [write an extension](write-an-extension.md); the decisions are in
issues #18, #20, #35 and #36. Production remains on its frozen version.

## v3 files without a NAS

[Files without a NAS](guide-files-v3.md) explains the default v3 queue file store,
explicit input uploads, bounded local caches, model imports, returned outputs and
checkpoints resumed on another hypha. Existing explicit NAS roots still work.

Measured throughput and placement: [Onboarding benchmark](guide-benchmark-v3.md).
