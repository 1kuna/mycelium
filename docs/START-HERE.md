# Submit work and follow its lifecycle

Mycelium runs CPU and GPU work through one durable SQLite queue. Start with a
single command, then use [one project adapter](adapter.md) for reusable work.
The [approved design](design-proposal.md) is the product spec.

## Five-minute start

With Go 1.25.6+ and a Unix shell, from this checkout:

```sh
go build -o myce ./cmd/myce
./myce dev --workers 2 --token dev-token
```

Keep dev running. In a second terminal in the checkout:

```sh
export MYCE_QUEUE=http://127.0.0.1:7331
export MYCE_TOKEN=dev-token
./myce run -- echo hello
```

The command waits, streams logs and exits with the result. Plain `run` defaults
to CPU; GPU inventory in dev is fake, but commands and the API are real. The
loopback address and synthetic token above are for this demo only. Dev persists
its state; use a fresh working directory for an independent demo. If port 7331
is occupied, select another with `dev --listen 127.0.0.1:PORT` and update callers.
Ctrl-C stops your dev process; do not stop another process to free a port.

## Make work reusable

Use `.mycelium/adapter.yaml` for new project integrations. This minimal entry
copies committed README content to an accepted output:

```yaml
schema: 1
project: example
default: readme
entries:
  readme:
    source: {git: HEAD, paths: [README.md]}
    run: {command: [sh, -c, 'cat README.md > result.txt']}
    needs: {platform: cpu, cpu_cores: 1, host_memory_gb: 1}
    targets: [{os: darwin, arch: arm64, platform: cpu}]
    environment: {}
    outputs: {result.txt: results}
    checks:
      result: [sh, -c, 'test -s result.txt']
```

Run `myce portability` and replace the example target with verified targets for
your project. Commit the intended README source; the adapter itself is frozen
separately. Ignore `.mycelium/receipts/`, then:

```sh
myce run --entry readme --key readme-1
myce wait JOB_ID
myce logs JOB_ID
myce result JOB_ID --out accepted-results
```

Use `./myce` instead if the demo binary is not on PATH. The first command prints
a receipt containing `job_id` and `key`; substitute that ID in the next commands.
Adapter submission returns before completion. Same-key replay recovers the same
intent; a deliberate new execution needs a new key. Queue outages leave work in
the queue. [The adapter reference](adapter.md) explains sources, environments,
qualification, profiles and conversion, with a small choice table.

## What happens after submission

1. The queue records durable intent. A unique project/key binds an immutable
   request; retry an identical request after a lost reply, never a changed spec.
2. The scheduler matches qualified targets, owner policy and resources. Waiting
   is visible with a reason. It prepares inputs and environments before execution.
3. A worker obtains exact-attempt start authorization, runs the command and its
   result check, and confirms process cleanup. Jobs never inherit `MYCE_TOKEN`.
4. The queue accepts current-attempt outputs only after verification. `result`
   verifies SHA-256 and size; the caller still checks application correctness.

Preemption, loss and retry can create later attempts. Set `resumable: true` only
for commands implementing [atomic checkpoints](files.md#resumable-jobs).
Cancellation intent and confirmed resource release are distinct. In durable
integrations, persist the key/spec, cancel by key when the consumer closes, retain
exact acknowledgement coverage, and fence late application side effects. See
[jobs and cancellation](jobs.md#keyed-submission-and-cancellation).

## Find the next detail

| Task | Reference |
| --- | --- |
| Commands, job YAML, waiting, cancellation, fan-out/dependencies | [Jobs](jobs.md) |
| One reusable project instruction file or native extension | [Adapter](adapter.md) |
| Locked dependencies and worker preparation | [Environments](environments.md) |
| Inputs, accepted outputs, diagnostics, checkpoints and retention | [Files](files.md) |
| OpenAI calls, general model servers and helper processes | [Services](services.md) |
| Priority, fairness, placement and portability | [Scheduling](scheduling.md) |
| Install, join, participate, upgrade or recover | [Operations](operations.md) |
| Waiting/readiness and field meanings | [Status](status.md) |
| Trust boundaries and platform enforcement limits | [Native isolation](native-isolation.md) |
| Gates, simulator, protocol and native acceptance | [Development](development.md) |

The automated installation route requires a verified NAS mount. Standalone dev
and file-store execution/checkpoints support no-NAS use; they do not remove that
installer prerequisite. To add a machine to an installed cluster, run `myce invite`
on an authorized hypha and paste its one output line on the new machine. The queue
needs an approved signed release. Enrollment is separate from application/native
qualification. [Operations](operations.md) covers both routes and readiness states.

For Python, install `./clients/python` into your project's Python 3.10+ environment.
Optional `[yaml,openai]` extras support those protocols; dict submission needs
neither. See the [package API](../clients/python/README.md).
