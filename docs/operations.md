# Operating Mycelium v2

One CPU queue, one SQLite database, plain-command GPU workers. These instructions
are local installation recipes, not a fleet deployment. Do not stop or replace
Atlas, Verso, or GPU guards/controllers. The old B70 v1 service was already
removed on 2026-10-09; do not reinstall it. Other legacy data is outside v2 scope.
Spark's Atlas GPU ownership still requires an agreed handoff.

## Paths and prerequisites

| Item | v2 path/name |
|---|---|
| Binary | `~/.local/bin/myce` |
| Database, spools, local logs | `~/.local/share/myce` |
| Private config | `~/.config/myce` |
| Linux user units | `myce-queue.service`, `myce-worker.service` |
| Mac launchd labels | `local.myce.queue`, `local.myce.worker` |
| Shared progress | NAS root supplied with queue `--checkpoints` and worker `--checkpoint-root` |

These are distinct from v1's `mycelium` paths and services. Before installation,
inspect existing destination paths and stop if their ownership is unclear.
Use Go 1.25.6+ to build for the target OS/architecture. Plain job commands,
Python runtimes, environments and GPU-enabled llama-server must already be
installed on eligible workers. There is no automatic environment installation or model download at load;
`myce model fetch` is the explicit master-download command.

Mount the NAS before creating the root. Verify the actual share identity, write
permissions, and a write/read/rename across participating machines. Configure
the mount to fail closed so an unmounted share cannot become a writable local
substitute. Mycelium probes access, not remote-server identity. Spark uses `/mnt/nas/mycelium/checkpoints`; B70 may use
`/mnt/nas/mycelium/checkpoints` or `/mnt/nas/mycelium/checkpoints`; Mac might use
`/Volumes/NAS/mycelium/checkpoints`. These may differ: both must refer to the
**same shared directory**. Assignment paths are relative on the wire, resolved
against the worker root; the queue retains its own absolute paths. Never put
SQLite or the worker spool on the NAS.

## Install binary and private configuration (Linux or Mac)

Run on the intended machine, from the checkout. This block stops at an existing
binary; upgrades use the separate procedure below.

```sh
set -eu
umask 077
test ! -e "$HOME/.local/bin/myce"
mkdir -p "$HOME/.local/bin" "$HOME/.local/share/myce" "$HOME/.config/myce"
go build -trimpath -o "$HOME/.local/bin/myce" ./cmd/myce
chmod 700 "$HOME/.local/bin/myce" "$HOME/.local/share/myce" "$HOME/.config/myce"
```

Create `~/.config/myce/queue.env` privately with `MYCE_TOKEN=<fleet token>`;
worker.env contains the same token plus `MYCE_QUEUE=http://<queue LAN IP>:7331`.
Use a secret manager/editor, mode 0600; never put the token in shell history,
argv, the repo, model catalogs, logs or NAS. Do not enable shell tracing. Source
only your trusted file for foreground CLI commands:

```sh
set -a
. "$HOME/.config/myce/worker.env"
set +a
"$HOME/.local/bin/myce" status
```

Queue binds loopback by default; explicitly select its trusted LAN address for
fleet use. Recheck port 7331 availability first. The HTML page is read-only but
unauthenticated; API/model routes use bearer auth. No TLS, public exposure,
proxy, or firewall changes are part of these recipes.

## Inventory and foreground start/stop

CUDA queries `nvidia-smi` before each start/load and check-in. Exactly one
GPU is supported. Numeric readings use dedicated GPU memory; `N/A`, `[N/A]`,
`Not Supported`, and `[Not Supported]` automatically select unified memory.
Metal always uses unified memory. Linux reads `MemAvailable` from `/proc/meminfo`;
macOS runs `vm_stat` without cgo and uses free + inactive pages with the reported
page size, excluding active, wired and compressed pages. This is a conservative
reclaimable-memory estimate, not a GPU allocation guarantee.

`--reserve-memory-gb` subtracts an OS/other-program reserve from each live
reading, clamped at zero. An optional positive `--memory-gb` caps the reading
**before** reserve. Values are GiB. For Spark, start with a **16 GiB reserve**;
raise it for Atlas growth, other programs and measured pressure. Existing Atlas
resident memory is already excluded from `MemAvailable`; the reserve provides
additional headroom, not a substitute for measuring Atlas usage or owner admission.
See [Spark worker example](../deploy/myce-spark-worker.service).

`--memory-mode auto` is the default; `unified` explicitly selects OS accounting,
`dedicated` requires numeric CUDA telemetry (invalid for Metal), and `fixed`
requires a positive operator-selected `--memory-gb` budget. Fixed mode never
probes live memory and cannot observe external allocations: choose it only with
an independently enforced conservative allowance. XPU/fake keep configured
budgets by default; XPU can explicitly select `unified` where applicable.
A failed live source reports zero and its reason in `myce status` and the status
page, refuses new work, and retries on subsequent checks. A cap alone never
hides a failed probe. Own batch/model reservations are deducted without double
subtraction. This is memory admission, not permission to bypass a GPU owner or guard.

```sh
# Queue terminal: MYCE_TOKEN is already exported privately.
"$HOME/.local/bin/myce" queue --listen 127.0.0.1:7331 \
  --db "$HOME/.local/share/myce/queue.db" \
  --checkpoints /Volumes/NAS/mycelium/checkpoints \
  --production-loss-timeout 5s --deadline-loss-timeout 20s \
  --experiment-loss-timeout 60s --restart-grace 3m
# Mac worker terminal; example budget only, choose a safe measured/configured value.
"$HOME/.local/bin/myce" join --name mac-worker --gpu metal \
  --memory-gb 16 --reserve-memory-gb 4 --heartbeat 1s \
  --checkpoint-root /Volumes/NAS/mycelium/checkpoints \
  --state-dir "$HOME/.local/share/myce/worker"
```

For CUDA use `--gpu cuda --reserve-memory-gb VALUE`; for XPU use
`--gpu xpu --memory-gb VALUE --reserve-memory-gb VALUE`. Configure unique stable
names. The queue heartbeat-loss defaults are 5s production, 20s deadline, 60s
experiment; worker heartbeat defaults to 1s. Startup recovery remains 3m. Production preemption gives resumable work 10s
by default; deadline preemption and planned leave give 2m. Avoid heartbeat intervals near or above the shortest loss timeout.

Ctrl-C/SIGTERM tells a worker to leave: no new work, resumable jobs save/exit,
non-resumable jobs finish. `myce leave --name mac-worker` asks wait/stop if needed;
`--now` signals everything but still permits checkpoint grace. Keep the local
spool across restarts. Queue Ctrl-C stops the API without deleting durable jobs.
Workers continue through temporary queue/network loss and replay logs/results.

## Render service files

This local renderer makes a private file from the checked-in template. Edit the
configuration assignments for the target host, select the role and OS, and run
from the checkout. For live CUDA/Metal, budget `"0"` means no optional cap; XPU/fixed
need a positive configured budget. For Mac workers select GPU `"metal"`.
No service is started by rendering. The Mac token prompt is hidden; Linux reads
the private EnvironmentFile at service startup instead.

```sh
python3 - <<'PYRENDER'
import getpass, os, pathlib, plistlib
role = "worker"                  # queue or worker
system = "mac"                   # mac or linux
queue_ip = "127.0.0.1"           # use the queue LAN address for fleet use
root = "/Volumes/NAS/mycelium/checkpoints"  # host's mount of the shared root
worker_name, gpu = "mac-worker", "metal"
budget, reserve = "16", "4"       # examples: choose a conservative allowance
home = str(pathlib.Path.home())
replacements = {"REPLACE_HOME": home, "REPLACE_QUEUE_IP": queue_ip,
    "REPLACE_CHECKPOINT_ROOT": root, "REPLACE_WORKER_NAME": worker_name,
    "REPLACE_GPU": gpu, "REPLACE_BUDGET_GB": budget, "REPLACE_RESERVE_GB": reserve}
output = pathlib.Path(home) / ".config/myce/rendered"
output.mkdir(parents=True, exist_ok=True, mode=0o700)
if system == "mac":
    name = f"local.myce.{role}.plist"
    config = plistlib.loads((pathlib.Path("deploy") / name).read_bytes())
    config["EnvironmentVariables"]["MYCE_TOKEN"] = getpass.getpass("Fleet token: ")
    if not config["EnvironmentVariables"]["MYCE_TOKEN"]:
        raise SystemExit("token required")
    for i, value in enumerate(config["ProgramArguments"]):
        for old, new in replacements.items(): value = value.replace(old, new)
        config["ProgramArguments"][i] = value
    for key in ("WorkingDirectory", "StandardOutPath", "StandardErrorPath"):
        config[key] = config[key].replace("REPLACE_HOME", home)
    config["EnvironmentVariables"]["MYCE_QUEUE"] = f"http://{queue_ip}:7331"
    data = plistlib.dumps(config)
elif system == "linux":
    name = f"myce-{role}.service"
    text = (pathlib.Path("deploy") / name).read_text()
    for old, new in replacements.items(): text = text.replace(old, new)
    if "REPLACE_" in text: raise SystemExit("unresolved service field")
    data = text.encode()
else: raise SystemExit("system must be mac or linux")
path = output / name
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "wb") as stream: stream.write(data)
print(path)  # path only; never print credentials
PYRENDER
```

For both roles run the renderer once per role. Installed Mac worker templates
use Metal; Linux supports the configured GPU selection. Replace the
`/path/to/rendered` prefix in the following installation commands with
`$HOME/.config/myce/rendered` (the destination printed above).

## Linux user services

Render private copies of the templates by replacing every `REPLACE_*` value.
The units use `%h` for the user's home directory. Install only the role needed;
a queue-only host must not receive an enabled worker.

```sh
mkdir -p "$HOME/.config/systemd/user"
# First edit deploy templates into private files outside this checkout.
install -m 600 "$HOME/.config/myce/rendered/myce-queue.service" "$HOME/.config/systemd/user/myce-queue.service"
# Worker host instead:
# install -m 600 "$HOME/.config/myce/rendered/myce-worker.service" "$HOME/.config/systemd/user/myce-worker.service"
systemd-analyze --user verify "$HOME/.config/systemd/user/myce-queue.service"
systemctl --user daemon-reload
systemctl --user enable --now myce-queue.service
systemctl --user is-active myce-queue.service
journalctl --user -u myce-queue.service -n 30 --no-pager
```

For workers substitute `myce-worker.service` in the verification/start commands.
User services run at login; to run before login, an administrator must explicitly
configure `loginctl enable-linger USER`. Do not change existing linger settings
incidentally. Stop/start only the chosen v2 role:

```sh
systemctl --user stop myce-worker.service
systemctl --user start myce-worker.service
# For permanent deactivation:
systemctl --user disable --now myce-worker.service
```

Worker stop can wait for a non-resumable command indefinitely. Decide whether to
finish or call `leave --now` before stop. Do not lower timeout to force a kill.
Services use restart-on-failure; an API leave alone is not durable deactivation.
For B70 lending, Verso must own the start/stop decision after its own admission
checks; this document does not install a controller or bypass existing scheduling.

## macOS launchd

Render templates in `deploy/` into private copies with every `REPLACE_*` value
resolved, including an absolute home, IP, root, worker name, budget/reserve and
private token. launchd does not expand `$HOME` or `~` in these fields. The worker
source and label are `local.myce.worker.plist` and `local.myce.worker`.

```sh
mkdir -p "$HOME/Library/LaunchAgents"
install -m 600 "$HOME/.config/myce/rendered/local.myce.worker.plist" "$HOME/Library/LaunchAgents/local.myce.worker.plist"
plutil -lint "$HOME/Library/LaunchAgents/local.myce.worker.plist"
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/local.myce.worker.plist"
launchctl print "gui/$(id -u)/local.myce.worker"
tail -n 30 "$HOME/.local/share/myce/worker.log"
```

For queue installation use `local.myce.queue.plist` and `local.myce.queue`.
LaunchAgents start at login. Boot before login requires an administrator-owned
LaunchDaemon under `/Library/LaunchDaemons`, explicit `UserName`, absolute paths
and `launchctl bootstrap system`; that consequential machine change is separate.
Stop via bootout so launchd cannot restart a deliberately leaving worker:

```sh
launchctl bootout "gui/$(id -u)" "$HOME/Library/LaunchAgents/local.myce.worker.plist"
# Start it again:
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/local.myce.worker.plist"
```

The worker plist has an unlimited exit timeout for non-resumable completion;
choose finish or explicit `leave --now` before stopping. Check status and the
process exit, not just launchctl's command return.

## Upgrade

Build and check the new binary first, then drain/stop only v2 services using the
platform's commands above. Stop workers before replacing their binary; wait for
owned processes to exit. For a queue upgrade leave workers running only when
prepared for interrupted model HTTP requests and the recovery grace. Never run
two queue processes against one database or two workers against one spool.

With queue stopped, make a private recoverable snapshot of the entire local
state, including SQLite sidecars if present. Preserve NAS checkpoints in place.
Example on either OS:

```sh
set -eu
umask 077
go build -trimpath -o "$HOME/.local/bin/myce.next" ./cmd/myce
upgrade_dir="$HOME/.local/share/myce-upgrade-$(date +%Y%m%dT%H%M%S)"
test ! -e "$upgrade_dir"
mkdir -m 700 "$upgrade_dir"
cp -p "$HOME/.local/bin/myce" "$upgrade_dir/myce.previous"
cp -Rp "$HOME/.local/share/myce" "$upgrade_dir/state"
test -x "$upgrade_dir/myce.previous"
test -d "$upgrade_dir/state"
mv "$HOME/.local/bin/myce.next" "$HOME/.local/bin/myce"
```

Restart the selected role, inspect service logs and `myce status`, and verify a
small agreed job completes. A binary start is not migration acceptance. On
failure stop again and retain evidence; restore the previous binary and its
matching state only after checking compatibility. Never overwrite current state
with an older snapshot while new jobs or results exist. No schema rollback or
production cutover is implied by this recipe.

## Uninstall without deleting data

Drain work and stop/disable v2 services (Linux) or bootout v2 agents (Mac).
Do not remove shared NAS checkpoints or touch v1. Use Trash, never permanent
removal. macOS provides `/usr/bin/trash`; Linux requires an available
`trash-put` from trash-cli. If unavailable, stop after deactivation and retain
files until a recoverable Trash tool is available.

```sh
# Linux, after services are inactive:
command -v trash-put
trash-put "$HOME/.local/bin/myce" "$HOME/.config/myce" "$HOME/.local/share/myce"
# Trash each installed v2 unit, then reload the user manager.
trash-put "$HOME/.config/systemd/user/myce-worker.service"
systemctl --user daemon-reload
trash-list
# Restore uses trash-restore; inspect its recorded original paths before selection.
```

```sh
# Mac, after agents are booted out and processes have exited:
/usr/bin/trash "$HOME/.local/bin/myce" "$HOME/.config/myce" "$HOME/.local/share/myce"
/usr/bin/trash "$HOME/Library/LaunchAgents/local.myce.worker.plist"
# Finder > Trash: verify each item is present and Put Back is available.
```

Trash only actually installed units/plists; queue hosts use their queue file.
Before moving state, verify a private backup can be read and its binary restored
using the upgrade snapshot procedure. Afterward verify recovery records/items
and original paths; do not empty Trash. Deactivation alone does not reclaim disk
space, and this is not destruction of job history or checkpoints.

## Monitoring and failures

Use `myce status`, `myce logs ID --follow`, the HTML page, and service logs.
Monitor SQLite disk/WAL space, worker spools (acknowledged logs/results remain),
checkpoint space, model errors, and NAS availability. Archive retained spools
only while their worker is stopped and after checking recovery requirements.
Missing NAS prevents new starts; running jobs must retry saves themselves.
Directory access cannot prove a real mount. Strict-majority silence preserves
assignments; a single lost worker may restart work elsewhere. Disconnected
copies may overlap until reconciliation. Atomic checkpoints and retry-safe
outputs are required, not exactly-once external side effects.

Model calls are not durable HTTP requests across queue restart. Started streams
cannot safely replay. An unloaded/draining model reserves memory until unload
acknowledges. For full request/catalog contracts and field defaults see
[reference](job-reference.md), [integration recipes](integrating.md), and the
[model](guide-model-calls.md) / [checkpoint](guide-resumable-jobs.md) guides.

The Linux queue template uses `PrivateDevices=yes` to hide GPUs, one Go CPU,
and initial limits of 25% CPU and 512 MiB RAM. Verify user-manager support and
GPU hiding before accepting the B70 queue; stop on unsupported sandboxing.
B70 is queue-only at `192.0.2.153:7331`; never enable a worker there.
CUDA inventory requires exactly one GPU. Spark unsupported memory fields select
Linux `MemAvailable`; missing/invalid OS telemetry blocks work. Reserve 16 GiB
initially and verify actual headroom under Atlas load before admission. See
[the production test plan](production-test-plan.md).

## Private founder test preparation

[The production test plan](production-test-plan.md) is the single ordered founder
checklist, including cross-builds, private configuration, CPU-only B70 queue,
Spark unified-memory worker, NAS verification, measured pre-copy, calls, batch,
faults and undo. Follow it instead of assembling a sequence from these general
installation recipes. No fleet operations were performed during the local review.

## Model master folder, worker cache, and production reservations

Create a NAS `models` folder and mount it on each eligible worker. Keep the
catalog paths relative to that folder, with trusted `size_bytes` and `sha256`
for every real model. Download only by an explicit `myce model fetch` command;
see [model setup](guide-model-calls.md). No download or install occurs at load.
Run workers with `--model-root` set to their mount, `--model-cache` on local disk,
and an explicit `--model-cache-bytes` allowance. Ensure the worker can write the
NAS folder's `.Trash/myce-model-cache` for recoverable evictions; cross-filesystem
moves keep a verified recovery copy before releasing local blocks. This can
consume NAS space, so manage recovery retention manually; never automatically
empty Trash. Without `--model-root`, evictions use user Trash and may not release
physical disk blocks. Disk checks fail closed if space is still insufficient.

Status shows cached models, free cache bytes, and copy/disk errors. Cache
inventory does not block heartbeats while a large copy is in progress. Corrupt or
partial copies are never published. Restart retains `.partial` bytes and rehashes
the prefix before resuming; only a full size/SHA-256 check publishes the copy.
Loading, ready, and draining models cannot be evicted from the local cache.
`direct_nas: true` is reserved for an operator-approved small model.

Pin with `myce model pin MODEL --project atlas --machine spark` or `--platform
cuda`; inspect status for assigned worker and ready state. Pins persist across
queue restarts and protect GPU memory even against production batch work. Optional
weekday/time-zone windows activate reservations. Unpin with `myce model unpin
MODEL --project atlas`. A worker leaving still drains its models; availability
cannot survive loss of the only eligible worker.

Production is considered first at check-in and on submission; a long-poll control
connection delivers stop requests immediately. Keep check-ins enabled for
inventory and loss detection; notifications do not replace heartbeats. Resumable
victims get `--production-stop-grace` (10s default); non-resumable victims stop
immediately and restart later without a loss retry. Deadline victims still get
2m and must be resumable. Strongly prefer resumable experiments with frequent
atomic checkpoints. Status reports production start acknowledgement latency, using queue time to avoid clock skew;
for batch commands without model loads it measures queue clearance directly.

## Pre-copy models before production

Use `myce model sync MODEL --to MACHINE`, `--to cuda|metal|xpu`, or `--to all`.
Machine names must have joined once; platform/all requests also apply to future
workers. Requests persist in the queue SQLite file. Status lists the requests,
per-worker bytes copied/total, paused/failed/ready state and errors, measured
bytes per second, and estimated NAS copy seconds per model. `myce model sync
MODEL --to TARGET --cancel` stops that request and suppresses idle copies for the
same target/model; partial bytes remain resumable. Repeat without `--cancel` to
resume. Cancellation cannot override a pin's required copy.

Pins pre-copy to every compatible landing worker, including before a scheduled
window; only the assigned worker reserves GPU memory and loads the engine.
An idle worker also copies models needed by compatible queued jobs or recent
use. Recent-use counts expire after 24 hours without use. Most-needed models go
first, with pins first. A batch, model load/call, or queue disconnection cancels background copying;
copy loops check cancellation between bounded chunks and during prefix hashing
and recoverable eviction. An already outstanding NAS filesystem syscall still
obeys the OS mount timeout, but it does not block worker heartbeats. No new
background work starts while real work is assigned.

`myce join --model-copy-parallel 1` is the default: one NAS copy per worker.
Increase only after measurement; NAS bandwidth is shared. Each copy reserves
its full size before admission. Partial bytes count toward `--model-cache-bytes`;
pre-copy eviction protects pins, loaded/loading/draining engines, active batch
models and active copies, and never replaces a more-used model with a less-used
one. Refusal is visible in status. Evictions remain recoverable in Trash.

Before a future authorized production test, measure free local disk and leave
headroom for the OS, checkpoints and other applications. About 100 GB free on
B70 does not safely support a 100 GiB active cache; choose a smaller allowance
from live inventory. Spark's larger disk still needs an explicit limit. Queue
placement uses verified cache affinity and remaining bytes / measured achieved
copy speed; until measured it uses an explicit conservative 40 MB/s prior.
This estimates NAS transfer, not engine startup or inference. Speeds are saved
in worker state and refreshed by actual copies, including resumed transfers.
Measure a cold copy independently on each worker, then pre-copy every production
model and confirm verified `ready` cache reports before sending traffic. A pin
must additionally report its loaded engine ready.

## Add a Mac mini or the 4090 PC in 2 minutes

Prerequisites: the matching `myce` binary, mounted NAS with an existing
`mycelium/checkpoints` directory, uv on PATH, and an existing queue address and
fleet token. A Windows 4090 PC uses its Linux installation/WSL environment with
CUDA visible to `nvidia-smi`; native Windows services are not supported.
Set `MYCE_TOKEN` privately in the shell; avoid putting it in a command argument.

On a Mac mini (adjust the mount name):

```sh
export MYCE_QUEUE=http://QUEUE_LAN_ADDRESS:7331
myce install worker --nas /Volumes/NAS --dry-run
myce install worker --nas /Volumes/NAS
myce doctor
```

On the Linux 4090 PC:

```sh
export MYCE_QUEUE=http://QUEUE_LAN_ADDRESS:7331
myce install worker --nas /mnt/nas --dry-run
myce install worker --nas /mnt/nas
myce doctor
```

Pass `--environment-config NAS/mycelium/environments/catalog.yaml` and
`--model-config NAS/models/catalog.yaml` when using those catalogs (substitute
absolute mounted paths). The setup summary shows OS/architecture, GPU platform,
memory mode/reserve, checkpoint root, cache locations/limits, config and service.
Apple Silicon uses unified memory with 4 GiB reserve, ordinary CUDA uses
nvidia-smi with 2 GiB reserve, and Spark GB10 uses unified memory with 16 GiB
reserve. Integrated Intel XPU uses OS memory with 8 GiB reserve. For discrete
Intel GPUs, setup uses the device memory reported by verbose sycl-ls; if that
cannot be measured, supply an explicit fixed device budget. Unknown GPU or
memory detection fails clearly. OS CPU architecture is detected, not emulated.

Default local setup is `~/.local/share/mycelium/worker`. The service uses a
private 0600 credentials file and preserves the installer shell's executable
PATH, so launchd can find uv installed in a user directory. Edit `config.json`
for operational settings; keep `credentials.json` private. A worker starts with
an empty environment cache and builds waiting-job software on demand.

Linux systemd *user* services start at login. Boot before login additionally
requires an administrator to enable lingering for that user (`loginctl
enable-linger USER`); the installer deliberately does not obtain privileges.
A macOS launchd agent starts when its user logs in, not before login. First
software/model preparation can take longer than machine registration.

To install the queue instead, provide a LAN bind address explicitly:

```sh
myce install queue --nas /mnt/nas --listen 0.0.0.0:7331 --dry-run
myce install queue --nas /mnt/nas --listen 0.0.0.0:7331
myce doctor --config ~/.local/share/mycelium/queue/config.json
```

Queue setup defaults to loopback binding. Point clients/workers at its reachable
LAN address. Mount the NAS before starting the service; setup never creates a
fake local replacement for an unavailable mount. Doctor checks the mount and
write access (its generated probe is retained in NAS `.Trash`), queue
reachability/authentication, GPU visibility for workers, uv, and free disk space.
A healthy doctor is a prerequisite check, not a real-hardware workload proof.

`myce uninstall --dry-run` previews removal of the worker setup. `myce uninstall`
stops/disables its service, then moves service, private credentials, binary,
spools and caches to recoverable local `.Trash` beneath the installation root.
Use `--config .../queue/config.json` for queue removal. Nothing on the NAS is
removed. Review recovery contents privately; never commit credentials or empty
Trash automatically. An existing installation is never overwritten silently.

See [environments](environments.md) for catalog authoring, lock files, build
logs, cache limits, and interrupted-build recovery. Laptop tests use fake GPU
probe output, temporary home directories and injected service runners; they
never install actual boot services.

## General model servers and joint measurement

Catalog `engine: server` owns a foreground HTTP process with `start_command`,
`health_url`, `ready_timeout`, optional `stop_command`, `stop_timeout`, named
`environment`, `memory_gb`, and `device`/`device_env`. `engine: vllm` is a preset
with served name, context, KV dtype, utilization and extra parser arguments.
Single-file masters or immutable `files` manifests use the same cache and pins.
See [general server contract](guide-servers.md) for full fields and examples.

Call `/models/NAME/PATH` with any method, body and end-to-end headers, plus the
usual fleet token, project and priority. Responses stream; request bodies have
an explicit 8 GiB generic disk-spool limit (OpenAI JSON retains 64 MiB).
The worker streams generic uploads to the engine. Batch `needs.model` remains a scheduling hint, not a
request to launch a server. Queued commands never inherit the fleet token.

`join --device ID` isolates commands/engines to one card and binds CUDA memory
probes. XPU fixed admission cannot detect other owners. Before enabling warm
production pins, run `myce measure` locally in an approved maintenance window
with the exact worker identity/spool, explicit platform/device/budget and model
list. It holds all models together, optionally warms them, writes JSON and text,
and unloads them. Missing native counters fail visibly. See the separate
[B70 weekend measurement](production-test-plan.md#b70-gpu0-weekend-joint-residency-measurement)
checklist; this is not the first-machine Spark acceptance test.
