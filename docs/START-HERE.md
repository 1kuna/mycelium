# Start here

This page takes you from one laptop to two of your own machines, then through a
first real job: submit it, follow it and get its output.

A few words you'll see in commands and output:

- **queue** (or hub): the one machine that keeps the job list. It's a single
  `myce` process with a SQLite file.
- **worker**: a `myce` process on a machine that runs jobs. The hub can run a
  worker too.
- **hypha**: Mycelium's name for one machine in your cluster. Status output uses
  it, for example `hypha=laptop`.
- **fleet token**: the shared password every machine and client uses to talk to
  the queue. Treat it like a password.

## 1. Try it on one laptop

With Git, [Go](https://go.dev/dl/) 1.25.6+ and a Unix shell on macOS or Linux
(the first build downloads Go modules, so it needs internet access):

```sh
git clone https://github.com/1kuna/mycelium.git
cd mycelium
go build -o myce ./cmd/myce
./myce dev -- sh examples/quickstart.sh
cat quickstart-result/result.txt
```

The script runs a CPU command that prints `hello from CPU`, then a job on a
pretend GPU, waits for it to succeed and downloads its checked result. The last
line prints `hello from Mycelium`. Both jobs really run through the queue and
worker; only the GPU is simulated, so you need no GPU or drivers.

`./myce dev -- COMMAND` starts a temporary queue with two pretend machines on a
free local port, points COMMAND at it (ignoring any other Mycelium settings),
and removes everything except your downloaded results when COMMAND finishes or
you press Ctrl-C. No token setup or second terminal is needed. To try your own
command, run `./myce dev -- ./myce run -- echo hello`. Plain `./myce dev` keeps
running until Ctrl-C and keeps its state in the current folder.

## 2. Run it on two machines

You need two Linux or Apple Silicon Mac computers on the same local network, and
you need to know the hub's local address (HUB_IP, for example from
`ipconfig getifaddr en0` on a Mac or `hostname -I` on Linux). Pick one of two
routes:

- **Try it in terminals** (below): no installation and no shared drive. Jobs run
  while the terminals stay open.
- **Install it for good:** the queue and workers run as background services,
  start at boot and recover on their own. This route needs a shared network
  drive (NAS) mounted on every machine. The README's
  [setup prompt](../README.md#set-it-up-on-your-computers) has your coding agent
  follow [operations](operations.md) steps 0 to 7 for you.

### Try it in terminals

Use this on a trusted home or office network only: clients reach the queue over
plain HTTP, and the fleet token is the only lock.

**On the hub**, in the `mycelium` checkout from step 1, create a private
settings file holding a new random token and the hub's address, then start the
queue on that address:

```sh
umask 077
printf 'MYCE_TOKEN=%s\nMYCE_QUEUE=http://HUB_IP:7331\n' "$(openssl rand -hex 32)" > mycelium.env
set -a; . ./mycelium.env; set +a
./myce queue --listen HUB_IP:7331
```

Leave it running. The queue keeps its state in `myce-state/` in this folder. If
macOS asks whether `myce` may accept incoming connections, allow it; on Linux,
open TCP port 7331 in your firewall if you run one.

**On the second machine**, get a `myce` binary built for it. Either clone and
build there as in step 1, or cross-build on the hub and copy it over, for example
`GOOS=linux GOARCH=amd64 go build -o myce-linux ./cmd/myce` (build a Mac binary
on a Mac). Copy `mycelium.env` over privately, for example with `scp`, and keep
it out of Git and chat. Then, in that folder:

```sh
set -a; . ./mycelium.env; set +a
./myce join --name second-machine
```

`join` connects to the queue named in `MYCE_QUEUE` and waits for work. Add
`--daily-use` on a computer someone uses, so it only takes work after five idle
minutes on external power. To have the hub run jobs too, start another
`./myce join --name hub` there, in a second terminal and its own folder, after
loading `mycelium.env` the same way.

From any machine with `mycelium.env` loaded, check that the worker joined:

```sh
./myce status
```

Each worker appears as a `hypha` line with its CPUs, memory and GPUs, and
`available for work` when it can take a job. Press Ctrl-C in a terminal to stop
that process. Delete `mycelium.env` from each machine when you're done; anyone
who has it can run commands on your machines.

## 3. Run a first real job

With `mycelium.env` loaded, or on any machine of an installed setup (which reads
its own saved settings):

```sh
./myce run -- uname -a
./myce run --cpu 2 --detach -- sh -c 'sleep 20; echo done'
```

The first command waits, prints each state change, then the command's output.
Mycelium picks the machine; `myce status` shows which one ran each job.
`--detach` prints only the job ID and returns at once. Follow that job with:

```sh
./myce wait JOB_ID      # one line per state change; exit 0 on success
./myce logs JOB_ID      # the command's output
./myce status           # every machine and recent job, with a reason when a job waits
./myce cancel JOB_ID    # stop it
```

Ask for hardware with `--gpu cuda`, `--gpu xpu` (Intel) or `--gpu metal`
(Apple), and for memory with `--mem 8G`. Leave `--mem` out and Mycelium measures
what the job uses and learns from it. `run` passes arguments literally, so use
`sh -c '...'` for pipes or `&&`. [Jobs](jobs.md#one-line-commands) lists every
option, and [status](status.md) explains what the fields mean.

A plain `run` gives you the command's output as logs. To get files back, declare
them as outputs in a project instruction file (next section) or job YAML, then
download them with `myce result JOB_ID --out DIR`, as the quick start did.

## 4. Make work reusable

For a project you'll run more than once, describe its work in one file,
`.mycelium/adapter.yaml`. This minimal entry copies the committed README to a
result file:

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

`targets` lists the machine types the entry may run on. Run `myce portability`
to see the types in your cluster and replace the example (`darwin/arm64` is an
Apple Silicon Mac). `source` sends the committed `README.md`, so commit it
first. Add `.mycelium/receipts/` to `.gitignore`, then:

```sh
./myce run --entry readme --key readme-1
./myce wait JOB_ID
./myce result JOB_ID --out accepted-results
```

The first command prints a JSON receipt and returns before the job finishes;
use its `job_id` in the next commands. Running it again with the same `--key`
returns the same job instead of starting a duplicate; use a new key to run it
again. [The adapter reference](adapter.md) covers sources, environments, checks
and profiles.

## What happens after you submit

1. The queue saves the job before it replies, so it survives a queue restart. A
   repeated submission with the same project and key returns the same job, as
   long as the request is identical.
2. The scheduler picks a machine with the hardware and free capacity the job
   needs. A waiting job shows the reason in `myce status`.
3. A worker runs the command, then any check, and makes sure its processes have
   exited. Jobs never see the fleet token.
4. Only output files from an attempt that passed its checks become the result.
   `myce result` verifies each file's size and SHA-256 hash as it downloads;
   whether the content is right is still yours to judge.

A job can run more than once: when a machine disappears, or when more important
work needs its machine. Set `resumable: true` only for commands that save
[checkpoints](files.md#resumable-jobs) and can pick up from them. Cancelling
asks the job to stop; `myce wait` shows when it has. For programs that submit
work and must survive their own restarts, see
[keyed submission and cancellation](jobs.md#keyed-submission-and-cancellation).

## Where to go next

| Task | Reference |
| --- | --- |
| Commands, job YAML, waiting, cancelling, fan-out and dependencies | [Jobs](jobs.md) |
| One reusable instruction file per project | [Adapter](adapter.md) |
| Software environments prepared on each machine | [Environments](environments.md) |
| Input and output files, checkpoints | [Files](files.md) |
| Model calls, model servers and helper processes | [Services](services.md) |
| Priority, fairness and placement | [Scheduling](scheduling.md) |
| Install as services, add machines, upgrade, recover | [Operations](operations.md) |
| What status fields mean | [Status](status.md) |
| What a job can and can't reach on a machine | [Native isolation](native-isolation.md) |
| Building and testing Mycelium itself | [Development](development.md) |

Once Mycelium is installed as services, add a machine by running `myce invite`
on any machine already in the cluster and pasting the one line it prints on the
new machine. That line downloads a signed `myce` release your hub has approved;
without one, copy an invitation file instead. [Operations](operations.md#4-add-each-other-machine)
covers both.

For Python, install `./clients/python` into your project's Python 3.10+
environment. The optional `[yaml,openai]` extras add YAML job files and
OpenAI-style model calls; submitting a plain dict needs neither. See the
[package API](../clients/python/README.md).
