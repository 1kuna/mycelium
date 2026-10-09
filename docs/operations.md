# Operating Mycelium v2

One CPU queue, one SQLite database, plain-command GPU workers. These instructions
are local installation recipes, not a fleet deployment. Do not stop or replace
Atlas, Verso, GPU guards/controllers, or legacy Mycelium. Discovery on 2026-10-08
found an active B70 `mycelium-real-fleet.service`; preserve its binary, state,
service and port. Spark's Atlas GPU ownership also requires an agreed handoff.

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
installed on eligible workers. There is no environment/model downloader.

Mount the NAS before creating the root. Verify the actual share identity, write
permissions, and a write/read/rename across participating machines. Configure
the mount to fail closed so an unmounted share cannot become a writable local
substitute. Mycelium probes access, not remote-server identity. Linux might use
`/mnt/nas/mycelium/checkpoints`; Mac might use
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
experiment; worker heartbeat defaults to 1s. Startup recovery remains 3m and
stop grace 2m. Avoid heartbeat intervals near or above the shortest loss timeout.

Ctrl-C/SIGTERM tells a worker to leave: no new work, resumable jobs save/exit,
non-resumable jobs finish. `myce leave --name mac-worker` asks wait/stop if needed;
`--now` signals everything but still permits checkpoint grace. Keep the local
spool across restarts. Queue Ctrl-C stops the API without deleting durable jobs.
Workers continue through temporary queue/network loss and replay logs/results.

## Render service files

This local renderer makes a private file from the checked-in template. Edit the
configuration assignments for the target host, select the role and OS, and run
from the checkout. For CUDA, budget `"0"` means no optional cap; XPU/Metal/fake
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

Commands below are for a future authorized test, not executed during review.
Run only after the [test-plan prerequisites](production-test-plan.md) are met.
No host service is started by this preparation. Use fail-fast shells; retain
partial installations for inspection on failure. The reviewed catalog is an
operator-owned prerequisite, not the example catalog.

Mac, from this checkout, `set -eu`; use SSH aliases `b70` and `spark` as user:

```sh
mkdir -p /tmp/myce-production-build
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /tmp/myce-production-build/myce-amd64 ./cmd/myce
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o /tmp/myce-production-build/myce-arm64 ./cmd/myce
test -s ops/private/myce-models.yaml
for host in b70 spark; do
  ssh "$host" 'set -eu; test ! -e ~/.local/bin/myce; test ! -e ~/.config/myce; test ! -e ~/.local/share/myce; test ! -e ~/.config/systemd/user/myce-queue.service; test ! -e ~/.config/systemd/user/myce-worker.service; test "$(loginctl show-user user -p Linger --value)" = yes; findmnt -T /mnt/nas -o SOURCE,TARGET,FSTYPE'
done
ssh b70 'test -z "$(ss -ltnH "sport = :7331")"'
```

Success: absent v2 destinations, existing linger, free 7331, both mounts show
CIFS `//storage.example/share`. Stop otherwise; never create a local NAS substitute.

```sh
for host in b70 spark; do
  ssh "$host" 'set -eu; umask 077; mkdir -p ~/.local/bin ~/.config/systemd/user; mkdir -m 700 ~/.config/myce ~/.local/share/myce'
done
scp /tmp/myce-production-build/myce-amd64 b70:/home/user/.local/bin/myce
scp /tmp/myce-production-build/myce-arm64 spark:/home/user/.local/bin/myce
for host in b70 spark; do
  scp ops/private/myce-models.yaml "$host":/home/user/.config/myce/models.yaml
  ssh "$host" 'chmod 700 ~/.local/bin/myce; chmod 600 ~/.config/myce/models.yaml; sha256sum ~/.local/bin/myce'
done
shasum -a 256 /tmp/myce-production-build/myce-*
```

Success: corresponding binary hashes match. Privately generate/send one token
from Mac zsh, without tracing, printing it, or writing it in the checkout:

```sh
MYCE_TEST_TOKEN=$(openssl rand -hex 32)
printf 'MYCE_TOKEN=%s\nMYCE_QUEUE=http://192.0.2.153:7331\n' "$MYCE_TEST_TOKEN" | ssh b70 'umask 077; cat > ~/.config/myce/queue.env'
printf 'MYCE_TOKEN=%s\nMYCE_QUEUE=http://192.0.2.153:7331\n' "$MYCE_TEST_TOKEN" | ssh spark 'umask 077; cat > ~/.config/myce/worker.env'
unset MYCE_TEST_TOKEN
sed -e 's/REPLACE_QUEUE_IP/192.0.2.153/g' \
    -e 's|REPLACE_CHECKPOINT_ROOT|/mnt/nas/mycelium/checkpoints|g' \
    -e 's| --restart-grace| --model-config %h/.config/myce/models.yaml --restart-grace|' \
    deploy/myce-queue.service > /tmp/myce-production-build/myce-queue.service
scp /tmp/myce-production-build/myce-queue.service b70:/home/user/.config/systemd/user/myce-queue.service
ssh b70 'set -eu; test -w /mnt/nas; mkdir -p /mnt/nas/mycelium/checkpoints; chmod 600 ~/.config/systemd/user/myce-queue.service'
```

After admission, use a conservative *reading cap* from the approved test budget
and reserve; enter them in GiB (not total machine RAM). A cap of 0 means no
additional cap on the live reading; 16 GiB is the suggested initial Spark
reserve. The generated worker uses automatic unified-memory detection. Mac zsh:

```sh
read 'MYCE_TEST_CAP?Approved free-memory cap (GiB): '
read 'MYCE_TEST_RESERVE?Approved reserve (GiB): '
sed -e 's/REPLACE_QUEUE_IP/192.0.2.153/g' \
    -e 's/REPLACE_WORKER_NAME/spark-v2-test/g' -e 's/REPLACE_GPU/cuda/g' \
    -e "s/REPLACE_BUDGET_GB/$MYCE_TEST_CAP/g" -e "s/REPLACE_RESERVE_GB/$MYCE_TEST_RESERVE/g" \
    -e 's|REPLACE_CHECKPOINT_ROOT|/mnt/nas/mycelium/checkpoints|g' \
    -e 's| --heartbeat| --model-config %h/.config/myce/models.yaml --model-listen 192.0.2.16:7332 --model-advertise http://192.0.2.16:7332 --heartbeat|' \
    deploy/myce-worker.service > /tmp/myce-production-build/myce-worker.service
```

Return to the production test checklist; this preparation starts no service.

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
first, with pins first. A batch or model load/call cancels background copying;
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
