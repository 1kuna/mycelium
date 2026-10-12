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
| Linux user units | `myce-queue.service`, `myce-worker.service`, `myce-worker@INSTANCE.service` |
| Mac launchd labels | `local.myce.queue`, `local.myce.worker` |
| Shared progress | NAS root supplied with queue `--checkpoints` and worker `--checkpoint-root` |

These manual-template paths are distinct from v1's `mycelium` paths and services.
`myce install` instead defaults to `~/.local/share/mycelium/ROLE` and macOS
`local.mycelium.ROLE` labels; inspect their ownership separately. Before installation,
inspect existing destination paths and stop if their ownership is unclear.
Use Go 1.25.6+ to build for the target OS/architecture. Project source/workdirs
and build tools must already be available on eligible workers. Workers build declared environment recipes on
demand (Python recipes need uv); GPU-enabled llama-server can be provided on
PATH or by a recipe. Model weights never download at load;
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
token-protected; HTML/API/model routes require the fleet token. No TLS, public exposure,
proxy, or firewall changes are part of these recipes.

## Inventory and foreground start/stop

`myce status` reads `MYCE_TOKEN` as before. Browsers can authenticate the HTML
page with username `myce` and the fleet token as the Basic-auth password;
refreshes reuse browser credentials. API clients use Bearer auth. Never put
tokens in URLs, printed commands, catalogs or screenshots.

Queue and join accept `--log-retention 168h` (default seven days). Confidential
model entries should use `logs: none`; read the
[private workload settings](guide-servers.md#private-workloads). Public error
messages deliberately omit raw upstream, subprocess and filesystem text.

CUDA queries `nvidia-smi` before each start/load and check-in. `join --device ID`
selects one card per worker and resolves CUDA selectors to stable UUIDs for both
inventory and execution. Multi-GPU hosts can run separately named workers for
separately selected cards. Without a selector, live dedicated/auto inventory
rejects multiple CUDA devices rather than summing memory. Numeric readings use
dedicated GPU memory; `N/A`, `[N/A]`,
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
See [Spark worker example](../deploy/myce-spark-worker.service); install that example as `myce-worker.service`.

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

`myce join` without `--gpu` uses installer GPU detection and its memory/reserve defaults; explicit device and memory flags override them. Use `--gpu` to bypass detection.

For explicit CUDA use `--gpu cuda --reserve-memory-gb VALUE`; for XPU use
`--gpu xpu --memory-gb VALUE --reserve-memory-gb VALUE`. Configure unique stable
names. The queue heartbeat-loss defaults are 5s production, 20s deadline, 60s
experiment; worker heartbeat defaults to 1s. Startup recovery remains 3m. Production preemption gives resumable work 10s
by default; deadline preemption and planned leave give 2m. Avoid heartbeat intervals near or above the shortest loss timeout.

Ctrl-C/SIGTERM tells a worker to leave: no new work, resumable jobs save/exit,
non-resumable jobs finish. `myce leave --name mac-worker` asks wait/stop if needed;
`--now` signals everything but still permits checkpoint grace. Keep the local
spool across restarts. Queue Ctrl-C stops the API without deleting durable jobs.
Workers continue through temporary queue/network loss and replay logs/results.
An existing `state.json` must use the Mycelium worker schema: `session`, `fresh`
and `offset` are required, and unknown fields are refused before recovery.
Foreign, incomplete or newer incompatible state produces `read worker state`
and remains untouched. Preserve that spool for inspection; do not replace it
with an empty spool to claim cleanup. Unrelated files are not imported as state.
The `operator-stop.json` snapshot uses the same strict schema; invalid markers
refuse startup, while an unchanged `state.json` snapshot remains compatible.

## Take a machine out of Mycelium

Run as the OS user that runs the worker, on that machine:

```sh
myce disable --reason "Verso rollback drill"
myce enable
```

`disable` immediately persists `~/.local/share/myce/DISABLED` (private JSON:
operator UID, UTC time, reason, grace seconds, generation). All workers run by that user,
including per-card workers and installed workers with custom spools, share this
flag and register their spool locations there. It survives reboot. `join` and
installed worker services exit successfully while the flag exists, before GPU
or queue checks; systemd's restart-on-failure and launchd's unsuccessful-exit
restart therefore do not loop. Linux worker units also have a flag condition.
Run the same commands separately for workers run by another OS user.

On Linux under the systemd user worker service, ownership is the worker unit's
cgroup. Disable stops that whole cgroup, including jobs, model servers, build
processes, and descendants that change sessions/groups or daemonize. Units use
`KillMode=control-group`, `SendSIGKILL=yes`, and `TimeoutStopSec=10s` by default.
`--grace 3s` allows up to 3 seconds before systemd receives a whole-unit SIGKILL
(maximum 1 minute). The requested grace must fit the unit's configured
`TimeoutStopSec`; increase that setting before requesting more than the default
10 seconds. This works with systemd versions that cannot mutate service timeouts
through `set-property`.
`--now` sends SIGKILL to all members through systemd, then stops the unit.
Confirmation requires the retained cgroup's `cgroup.procs` to be empty and
`cgroup.events` to say `populated 0` (which includes nested cgroups), plus an
inactive/failed unit. A cgroup removed by systemd counts as empty only when the
stopped unit no longer reports a cgroup. A stop request alone is not confirmation.
Invalid units, unreadable cgroups, a changed cgroup, or remaining processes fail
closed. No PID/PGID records authorize signaling in this path. This controls only
`myce-worker.service` and `myce-worker@INSTANCE.service` for this OS user. Instance
names contain only letters, digits, dash, and underscore. Workers discover their
own systemd unit from the invocation's kernel cgroup and record it in their
registry. Default disable stops every allowed unit registered in the selected
host control directory and reports each result; every requested unit must be
confirmed before a cleanup receipt is written. It does not stop Verso's legacy
controller or unrelated services/processes.

For one card only, use `myce disable --unit myce-worker@gpu1.service` and
`myce enable --unit myce-worker@gpu1.service`. The unit must already be registered
in this host control directory. Its generation-bearing flag and receipt live in
`units/myce-worker@gpu1.service/` below that directory. Every instance also obeys
the shared host flag: enabling one cannot bypass a disabled host. Targeted control
leaves sibling units and queue host admission alone; its work follows ordinary
worker leave/loss handling. Docker cleanup targets only attempts recorded in that
instance's spool, including retained released custody, using exact attempt labels.

Install an independent Linux instance with
`myce install worker --instance gpu1 --device 1` plus the normal GPU, queue and NAS
options. Instance names and physical device selectors are independent. The
installer renders `myce-worker@gpu1.service`, uses a separate `worker-gpu1/`
installation/spool, and preserves the same cgroup kill policy and both disabled
conditions. The default worker name gains the instance suffix; `--name` can
override it. Use its printed `--config` path for doctor, uninstall, and custom-root
controls. A manual template is available at `deploy/myce-worker@.service`; resolve
its device and other fields before installation. `--host-state-dir DIR` is also
saved by the installer, so isolated services and controls share the same authority.
Existing workers must run the updated binary once to record their unit custody.

Re-running either installer with identical flags upgrades its binary and unit,
reloads the service manager and preserves local state. Changed configuration
prints a diff and requires `--force`; role/service/binary/spool paths cannot be
relocated over an installed role. The previous unit is kept in local `.Trash`.
Linux worker installation also writes a private `gpu.env` beside `config.json`.
It captures only recognized runtime controls from the invoking shell: `LD_LIBRARY_PATH`,
`LIBRARY_PATH`, selected `ZE_*`, `ONEAPI_*`, `SYCL_*`, and `CUDA_*` variables.
It never imports arbitrary prefixed variables, credentials, or `MYCE_TOKEN`.
Source the trusted oneAPI `setvars.sh`/profile before installing if that is what
makes foreground GPU detection work. PATH is captured in the private config and
`gpu.env`; newline, quote and backslash values are rejected. Reinstallation
refreshes this runtime file. Changes to saved PATH or GPU/device settings require
`--force`, like other configuration changes.

After writing/reloading the unit, install runs the installed binary's
`detect --config PATH` through `systemd-run --user --wait --pipe`, loading the
same `gpu.env`, PATH and device binding. This command reads no credentials.
It checks actual read/write access to render/NVIDIA card nodes, the running
user manager's groups, and runtime inventory. Missing nodes or denied access
produce driver/group diagnostics, including administrator-only `sudo usermod
-aG render/video <worker-user>` guidance; install never runs sudo. ACL access
can be sufficient without those groups. A failed validation refuses installation
acceptance and does not enable/start the new worker; written files remain for
inspection/retry. Upgrades do not stop an already running worker on failure.
`--instance` and `--device` follow the same check and selected XPU runtime binding.
Use `--allow-deviceless` explicitly for CPU/fake test workers. Without `--gpu`,
failed foreground detection then requires a positive `--memory-gb` test budget
and configures the fake platform. This flag bypasses service GPU validation,
not the normal scheduler/checkpoint contracts.

Re-run the installer with the updated staging binary to upgrade the role binary.
Manual unit users must install the updated template and reload their user manager.
The flag condition and the worker's successful exit while disabled prevent
restart loops, including after reboot.

On macOS/launchd or foreground `myce join`, recorded supervisors and
attempt/engine/build groups receive the existing bounded cleanup. This path
prints **not running under the systemd service; ownership is best-effort**.
Exact markers and process birth identities reduce stale-record mistakes, but
inspection followed by signaling cannot prove ownership atomically or discover
children that remove their markers. Successful secondary cleanup is best-effort,
never cgroup certification. An empty registry returns nonzero and cannot produce
a cleanup receipt. Use `--state-dir DIR --name NAME --host-id HOST` to adopt a
known old spool; registered spool locks must be released before a receipt.
Other users and unregistered workers remain outside this secondary scope.

`--timeout 30s` sets one overall wall-clock deadline for disable; its default is
chosen grace plus 20 seconds. At that deadline the command stops waiting,
returns nonzero with unconfirmed work, and retains the flag. Cancellation fences
new signals and acknowledgement of unfinished cleanup; no cleanup receipt is issued for
unconfirmed work. A blocked kernel filesystem operation cannot be interrupted
by Go: flag durability is claimed only after write/fsync succeeds, and any
in-progress operation remains under the control lock until it returns. No NAS
operation gates disable. Queue notifications are best-effort, launched alongside
cleanup and never awaited by disable. A pending/unconfirmed notification message
is not proof that the fleet queue rejected the request; another registered queue
may be offline, or delivery may still be in flight. An acknowledgement may remain pending if
the CLI exits before delivery; the durable receipt reconciles at enable/rejoin.

Host control carries a durable monotonic generation in the local flag/receipt
and queue host record. A stale enable or disable is rejected; retrying the same
disable acknowledgement requeues jobs only once. Enable must follow acknowledged
cleanup. Generations survive reboot and reconciliation retries, including a lost
enable response. Acknowledgements are independent for each registered queue and
host: accepted targets are removed from the pending receipt, and offline targets
remain durable. Old laptop/drill queue registrations cannot veto a worker's
rejoin to its own queue. A worker reconciles only its configured queue and host.
Until that acknowledgement succeeds it stays running, sends held heartbeats,
and admits no batch, model, copy or environment work. CLI/HTML/JSON status shows
`held: enable not yet acknowledged (retrying)` (`enable_pending` in JSON).
Reconciliation uses injected-clock backoff of 1, 2, 4, 8, 16, then 30 seconds;
network I/O remains bounded. Local host/unit flags still cause successful exit.
If the queue is unavailable, held status cannot reach it; inspect the worker log.
Upgrade the queue before workers for the new held-status protocol; older queues
reject unknown check-in fields. Preserve the host control directory; do not reset its counter.
One OS-user control directory is the ordering authority for each physical host.
A reachable queue fences every card on that host and requeues displaced jobs
without charging a retry. Exact saved attempts can refund an outage loss.
Interrupted model HTTP calls return an error and must be retried by the caller.

Use `--config /absolute/path/config.json` for a custom installer root.
`MYCE_HOST_STATE_DIR` / `--host-state-dir` selects an isolated control directory
for laptop drills; every worker and control command must use the same directory.
`enable` archives the flag and receipt in recoverable local `.Trash`, starts the
detected service, or prints the saved foreground join command. It refuses an
unconfirmed disable. Concurrent controls fail rather than waiting on their lock.
The flag is persisted before installer configuration discovery; the model proxy
also refuses disabled-host admission, including a request-body read race.

Docker cleanup is part of the same confirmation and overall deadline. The trusted
`DOCKER_HOST` (default local Unix socket) and endpoints saved in registered
worker custody are inventoried with the exact `myce.attempt` label. Every object
is inspected before signaling; only objects with that nonempty label qualify.
Labeled execution receives TERM, then KILL at the original disable grace deadline
(`--now` kills immediately). Native cleanup does not grant Docker a fresh grace.
Stopped container objects are retained; “gone” means no running execution, not
container deletion. Unlabeled containers, including legacy Verso production,
are never signaled.

Pending create/start custody is reconciled even after the client or worker dies.
An empty scan cannot settle an uncertain operation: a late accepted start must
be positively observed and stopped, or independently fenced through the existing
`resolve-docker --confirm-gone` procedure. Unknown create acceptance remains
unconfirmed even after a discovered late container is stopped. Slow, hanging or
refusing Docker retains DISABLED, returns nonzero, and reports known attempt and
container IDs; an unavailable inventory is explicitly unconfirmed, not a claim
that all IDs are known. A missing Docker CLI is **not applicable** only when
there is no unresolved custody. An unreadable worker registry cannot certify
pending-operation absence. Saved obligations with a missing CLI still fail.
Workers refuse startup and Docker create/start dispatch while DISABLED is present.
Use the same `--host-state-dir` (or `MYCE_HOST_STATE_DIR`) for custom controls.
Offline `resolve-docker` is serialized with host controls, preserves the flag,
and records only the operator's independent release proof; it never launches work.

Check exit status and the confirmation line. Systemd success requires both the
empty worker service cgroup and confirmed labeled Docker cleanup (or Docker not
applicable). Secondary success remains best-effort registry cleanup. This does
not fence arbitrary external Docker clients or other users; unregistered delayed
launches cannot be proved absent. Queue status is separate: verify **disabled by
operator** and **requeued** when notifications or reconciliation have reached
it. On failure, keep the flag and rerun disable. After enable, verify a newly
joined worker and an agreed small job.

## Emergency stop when the myce binary is broken

Run as the worker's OS user. Stop each installed Mycelium worker by its exact
name; for an instance substitute `myce-worker@gpu1.service` below. Retain its
cgroup path before stopping, because systemd may remove the empty cgroup:

```sh
unit=myce-worker.service
# Use this unit's actual --state-dir (inspect systemctl --user cat "$unit").
worker_spool="$HOME/.local/share/myce/worker"
# Snapshot operator intent BEFORE stopping; no myce binary is needed.
umask 077
cp "$worker_spool/state.json" "$worker_spool/operator-stop.json.tmp" &&
  sync "$worker_spool/operator-stop.json.tmp" &&
  mv "$worker_spool/operator-stop.json.tmp" "$worker_spool/operator-stop.json" &&
  sync "$worker_spool"
# If snapshotting failed, fix that error before stopping when practical.
worker_cgroup=$(systemctl --user show "$unit" -p ControlGroup --value)
systemctl --user disable --now "$unit"
systemctl --user show "$unit" -p ActiveState -p MainPID -p ControlGroup
if [ -n "$worker_cgroup" ] && [ -e "/sys/fs/cgroup$worker_cgroup/cgroup.procs" ]; then
  cat "/sys/fs/cgroup$worker_cgroup/cgroup.procs"
  cat "/sys/fs/cgroup$worker_cgroup/cgroup.events"
fi
```

Confirm inactive/failed, `MainPID=0`, and an empty retained `cgroup.procs` with
`populated 0` in `cgroup.events` (including descendants). If the cgroup was
removed, confirm the stopped unit now reports no `ControlGroup`. Do not treat
stop-command success alone as proof. Repeat for all owned instances for a
host-wide emergency stop.

Containers require separate cleanup. Inventory the exact Mycelium attempt label
and inspect each selected ID before removing only those labeled objects:

```sh
docker ps --filter label=myce.attempt --format '{{.ID}}'
docker inspect --format '{{ index .Config.Labels "myce.attempt" }}' CONTAINER_ID
# Only after confirming its nonempty Mycelium attempt label and ownership:
docker rm -f CONTAINER_ID
# After ALL containers for the exact attempt are nonrunning/absent:
docker network ls --filter label=myce.attempt=ATTEMPT_ID --no-trunc
docker network inspect --format '{{ index .Labels "myce.attempt" }}' NETWORK_ID
# Confirm exact ownership; disconnect owned stopped endpoints first.
docker network rm NETWORK_ID
# Repeat filtered inventory and confirm no labeled network remains.
```

For a single instance, filter with `label=myce.attempt=ATTEMPT_ID` from its saved
spool custody and leave sibling containers alone. Never remove unlabeled or
Verso/Atlas containers. Pending Docker create/start custody still requires the
recovery procedure under **Docker cleanup custody**; an empty inventory cannot
rule out a delayed start.

The marker is exactly `<worker --state-dir>/operator-stop.json`; for the manual
unit above it is `$HOME/.local/share/myce/worker/operator-stop.json`. Its format is
an unmodified JSON snapshot of that spool's `state.json`, containing active
`session`, `identity`, and `job.id`/`job.attempt`. Installed instances can use
other directories: use the selected unit's actual spool, never a sibling's.
Startup durably imports intent only if these fields match the current execution
and the snapshot has no completed result. Stale markers cannot apply to a new
attempt; preserve the marker with the spool as evidence. This is intent, not
proof of process/container/network release. Never use `touch DISABLED` instead.

The queue handles host loss and requeues unfinished batch work after its
configured loss/recovery handling; model calls fail and callers retry them.
After cleanup and reconnect, the worker's exact-attempt operator acknowledgement
refunds that attempt's infrastructure loss once, including restoring jobs failed
only by the three-loss limit. Operator stops acknowledged before loss are never
charged. Real crashes and other attempts retain their losses. Without this
snapshot or an ordinary disable receipt, a systemd stop cannot reliably be
distinguished from a crash and can consume the loss allowance.
An existing ordered host disable may keep work held until its cleanup receipt
reconciles. Restore a working binary, then recover the exact unit:

```sh
systemctl --user enable --now myce-worker.service
myce enable
# Instance: enable --now myce-worker@gpu1.service; myce enable --unit myce-worker@gpu1.service
```

Use the original config/control directory. If enable reports unconfirmed cleanup,
rerun `myce disable` with the repaired binary before enabling. Never write,
remove, or replace `DISABLED` by hand: it carries a generation tied to the cleanup
receipt and queue ordering. Keep the control directory and worker spool.

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

Worker service stop uses its configured cgroup timeout (10 seconds by default).
Drain non-resumable work with `leave` before stopping when it should finish.
Services use restart-on-failure; an API leave alone is not durable deactivation.
Use `myce disable` above for bounded, persistent host removal.
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

The Linux user queue uses `PrivateDevices=no`, `NoNewPrivileges=yes`, one Go CPU,
and initial limits of 25% CPU and 512 MiB RAM. Device namespaces (even with
`PrivateUsers=yes`) can fail with 218/CAPABILITIES in unprivileged user managers.
The queue never opens GPU devices and refuses inherited GPU descriptors at
startup; this is not an OS device-access sandbox. Verify it holds no GPU
descriptors before accepting a queue. Worker units retain GPU access and
`Delegate=memory`.
B70 is queue-only at `192.0.2.153:7331` for the first Spark acceptance test;
never enable a B70 worker as part of that test. CUDA dedicated/auto inventory
requires one selected device (or one device in unselected output). Spark
unsupported memory fields select
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
see [model setup](guide-model-calls.md). Model weights never download at load;
a declared software environment may build on demand.
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
Create private `~/.config/myce/fleet.env` containing literal `MYCE_TOKEN` and
`MYCE_QUEUE` assignments, as in the deployment sequence below. Avoid putting
credentials in arguments. `--environment-file PATH` selects a different file.

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
`--model-config NAS/mycelium/models/catalog.yaml` when using those catalogs (substitute
absolute mounted paths). The setup summary shows OS/architecture, GPU platform,
memory mode/reserve, checkpoint root, cache locations/limits, config and service.
Apple Silicon uses unified memory with 4 GiB reserve, ordinary CUDA uses
nvidia-smi with 2 GiB reserve, and Spark GB10 uses unified memory with 16 GiB
reserve. Integrated Intel XPU uses OS memory with 8 GiB reserve. For discrete
Intel GPUs, setup uses the device memory reported by verbose sycl-ls; if that
cannot be measured, supply an explicit fixed device budget. Unknown GPU or
memory detection fails clearly. OS CPU architecture is detected, not emulated.

Default local setup is `~/.local/share/mycelium/worker`, with its own binary,
config and state; credentials stay in the private fleet EnvironmentFile. Linux uses `myce-worker.service`; macOS uses
`local.mycelium.worker.plist` / `local.mycelium.worker`, unlike the manual template.
Automatic CUDA setup requires exactly one GPU unless `--device` selects a card.
Use `--instance` for separate Linux per-card units. The service reads the private
0600 fleet EnvironmentFile, and preserves the installer's executable PATH so
launchd can find tools in a user directory. Re-run with flags and `--force` for
changed operational settings. A worker starts with
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
LAN address. Mount the NAS before installing. Linux installs require the exact `--nas` root
in `/proc/mounts` by default. Only local tests should opt out with
`--require-mounted-nas=false`. Installers create only the `mycelium/checkpoints`,
`mycelium/models` and `mycelium/environments` namespace, refuse symlinks, and
write/sync/rename/remove a small generated probe in each directory. Doctor checks the mount and
write access (its generated probe is retained in NAS `.Trash`), queue
reachability/authentication, GPU visibility for workers, uv, free disk space,
and Linux user lingering (doctor fails unless it is enabled).
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

## Short B70 and Spark deployment

Build the same revision for both hosts, then copy the amd64 binary to the B70
and arm64 binary to the Spark's staging directory. These commands are a recipe;
addresses, NAS mounts and device budgets are operator inputs. No command here
changes legacy Verso ownership, Atlas services, earlyoom, mounts or user lingering.
The production handoff and native device/memory acceptance remain separate gates.

```sh
GOOS=linux GOARCH=amd64 go build -trimpath -o /tmp/myce-linux-amd64 ./cmd/myce
GOOS=linux GOARCH=arm64 go build -trimpath -o /tmp/myce-linux-arm64 ./cmd/myce
scp /tmp/myce-linux-amd64 <B70_USER>@<B70_ADDRESS>:myce
scp /tmp/myce-linux-arm64 <SPARK_USER>@<SPARK_ADDRESS>:myce
```

On each host, create the same private fleet file without displaying its token.
Replace the placeholder privately; use simple literal assignments (no `export`,
shell interpolation or multiline values). Installed units load this file and the
worker's captured GPU runtime file; secrets never enter the unit or argv.

```sh
umask 077
mkdir -p "$HOME/.config/myce"
cat > "$HOME/.config/myce/fleet.env" <<'ENV'
MYCE_TOKEN=<FLEET_TOKEN>
MYCE_QUEUE=http://<B70_LAN_ADDRESS>:7331
ENV
chmod 600 "$HOME/.config/myce/fleet.env"
chmod 700 "$HOME/myce"
```

On the B70, prepare the two policy inputs (retain existing approved policies
rather than overwriting them). The hold protects production **inside Mycelium**;
Verso separately controls legacy production. The shared **3 GiB** host budget
accounts for legacy production outside Mycelium; it is 3 GiB across both cards.

```yaml
# ~/.config/myce/projects.yaml
verso:
  weight: 1
  allowed_machines: [b70-gpu0, b70-gpu1, spark]
```

```yaml
# ~/.config/myce/hosts.yaml
hosts:
  b70:
    production_hold: true
    production_idle: 180s
  spark:
    production_hold: false
    production_idle: 180s
```

Source the trusted existing oneAPI runtime before installing B70 workers. Replace
`<B70_NAS_MOUNT>`, `<GPU0_GIB>`, `<GPU1_GIB>` and `<GPU_RESERVE_GIB>` with the
mounted root and qualified device budgets. Do not use `--allow-deviceless`.

```sh
"$HOME/myce" install queue --nas <B70_NAS_MOUNT> \
  --listen <B70_LAN_ADDRESS>:7331 \
  --project-config "$HOME/.config/myce/projects.yaml" \
  --host-config "$HOME/.config/myce/hosts.yaml"
"$HOME/myce" install worker --nas <B70_NAS_MOUNT> \
  --name b70-gpu0 --host-id b70 --instance gpu0 --device 0 \
  --gpu xpu --memory-mode fixed --memory-gb <GPU0_GIB> \
  --reserve-memory-gb <GPU_RESERVE_GIB> --reserve-host-memory-gb 4 \
  --host-memory-budget-gb 3 --memory-ceiling-slack-gb 0 --start-disabled
"$HOME/myce" install worker --nas <B70_NAS_MOUNT> \
  --name b70-gpu1 --host-id b70 --instance gpu1 --device 1 \
  --gpu xpu --memory-mode fixed --memory-gb <GPU1_GIB> \
  --reserve-memory-gb <GPU_RESERVE_GIB> --reserve-host-memory-gb 4 \
  --host-memory-budget-gb 3 --memory-ceiling-slack-gb 0 --start-disabled
```

`--start-disabled` writes an ordered per-unit DISABLED flag and cleanup receipt
before installation, registers the unit for boot without starting it, and adopts
its spool into host control. Unit conditions and the service entry point prevent
joining, including after reboot. An existing host-wide gate and receipt stay
intact. Reinstallation with this flag confirms cleanup of an existing worker;
unconfirmed native/Docker custody fails closed. When Verso separately releases
ownership, clear any shared gate while the card gates remain, then explicitly
`myce enable --config .../worker-gpu1/config.json --unit myce-worker@gpu1.service`
for the released card. Enabling GPU0 needs its own coordinated production handoff.
Set the shared budget once while both workers remain disabled. Verso uses this
local command for every switch and fallback; it needs no queue connection and
works with stopped services. The durable override wins over install-time flags.

```sh
"$HOME/myce" host-memory-budget set 3
"$HOME/myce" host-memory-budget show
```

Verso's production switch order is: keep 3 GiB while legacy production owns the
GPUs; clear and verify legacy GPU owners; set 27 GiB; then enable GPU0. Retain the
approved host production hold until Verso's separate production admission gate
releases it. If a shared DISABLED gate exists, clear it while both per-card gates
remain in place, before enabling the selected card.

```sh
# After legacy GPU owners are confirmed cleared:
"$HOME/myce" host-memory-budget set 27
"$HOME/myce" enable --unit myce-worker@gpu0.service
```

On fallback, complete and verify the disable first. A failed disable blocks the
fallback. Only then set 3 GiB and let legacy production load.

```sh
"$HOME/myce" disable --now && "$HOME/myce" host-memory-budget set 3
# Legacy production loads only after both commands succeed.
```

Budget updates use atomic replacement with file/directory fsync and ordered
generations in the shared host-state directory next to DISABLED. All instances
read it at heartbeats and admission; new admissions use a raise without restart.
A lower budget preserves running work and blocks further admissions while
status reports `over budget: N GiB in use > M GiB`. Status usage is the sum of
reserved per-attempt/model host ceilings (including slack), not measured RSS.
`show` prints the effective value and source; use `--host-state-dir DIR` for a
custom shared directory, or `show --config PATH` for a custom install fallback.
Corrupt/regressed overrides fail admissions closed. Never relocate the spool or
reset generations during a reinstall.

On the Spark, after the coordinated Atlas loan, use the unified GPU pool,
**16 GiB** reserve and **105 GiB** host budget. Keep earlyoom armed.

```sh
"$HOME/myce" install worker --nas <SPARK_NAS_MOUNT> \
  --name spark --host-id spark --device 0 --gpu cuda --memory-mode unified \
  --reserve-memory-gb 16 --reserve-host-memory-gb 16 \
  --host-memory-budget-gb 105 --require-memory-ceilings
```

Append `--dry-run` to each install command to inspect its generated unit without
writes or service changes. A dry run does not prove systemd or NAS acceptance.
After actual installation, inspect exact units and authenticated `myce status`;
B70 workers must remain unjoined. Use `myce doctor --config PATH` for prerequisites.
Source fleet.env privately (`set -a; . "$HOME/.config/myce/fleet.env"; set +a`)
for client commands, never with shell tracing or token output.

`--require-memory-ceilings` checks the worker's own service cgroup at every
startup: memory delegation, confirmed worker leaf membership, an empty service
parent, and create/write/read/remove of a child with `memory.max=134217728`,
`memory.swap.max=0`, `memory.oom.group=1`. Failure exits 78 with a clear message;
`RestartPreventExitStatus=78` prevents a restart loop. Required workers reject
batch attempts and model loads with unset/zero `host_memory_gb`, even without a
host budget. This is a real kernel-control preflight, not GPU workload proof.
Qualify actual per-attempt limits and GPU/driver accounting before admission.

Queue defaults: loopback listen; role-local `state/myce.db` and `state/uploads`;
16 GiB upload spool; 7-day log retention; 3-minute restart grace; production,
deadline and experiment loss timeouts 5s/20s/60s. Queue units set
`PrivateDevices=no`, `NoNewPrivileges=yes`, `Nice=10`, `CPUQuota=25%`,
`MemoryMax=512M`, `TasksMax=64`, `GOMAXPROCS=1` and a 150s stop timeout. Flags
`--no-new-privileges`, `--nice`, `--cpu-quota`, `--memory-max`
and `--tasks-max` change that envelope. Worker defaults: role-local spool,
`~/.local/share/myce` host control, 1s heartbeat, 4 GiB host reserve, 0.125 GiB
ceiling slack, zero swap, 100 GiB model cache and 20 GiB environment cache.
NAS roots default to `<nas>/mycelium/{checkpoints,models,environments}`.
The install help lists all policy, path, timeout, cache and memory overrides.

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

## Host RAM admission and ceilings

Declare batch `needs.host_memory_gb` or catalog `host_memory_gb` in GiB for
additional CPU-side memory, including the measured load and overlapping
activation peak. Qualify the actual wrapper and workload before setting the request. Unset/zero retains
GPU-only admission on uncapped hosts. A configured host ceiling budget rejects
zero host declarations, including on sibling workers. Host requests require
valid live RAM telemetry.

All per-card workers on one physical machine must share a stable `join
--host-id` (default hostname); use unique host IDs across different machines.
Linux uses `MemAvailable`; macOS uses free plus inactive pages from `vm_stat`.
`--reserve-host-memory-gb` (default 4 GiB) withholds OS/other-workload headroom.
The queue uses the lowest current allowance reported by workers on that host
and subtracts all outstanding declared allocations, including loading models
and stopping/draining work. This is intentionally conservative: resident memory
already reduces the live OS allowance, and its declared reservation is retained
to cover growth rather than guessing process usage. A slower/stale sibling
report can temporarily reduce admission until its next check-in. This is
aggregate admission; external allocations can still race it. Workers repeat
live refusal after preparation and before batch/model starts.

On dedicated GPUs the host pool charges only `host_memory_gb`. On unified
Metal/Spark/integrated machines, GPU and CPU allocations share one host pool:
charge `memory_gb + host_memory_gb` once per allocation, not two independent
RAM budgets. `host_memory_gb` means additional CPU-side memory, never the
combined GPU-plus-CPU footprint. Existing GPU-only allocations also reserve
their GPU portion against that pool when placing a RAM-requesting neighbor.
GPU admission remains a cap; passing both checks does not charge a second
GPU allocation. The OS reserve is the larger GPU/host reserve, applied once
to the shared allowance. Auto CUDA detects unsupported NVIDIA memory fields;
explicit `--memory-mode unified` and Metal always use this rule.


Linux cgroup v2 workers installed as systemd user services enforce declared
ceilings. The generated and manual worker units use `Delegate=memory` and retain
`KillMode=control-group`. At startup the worker moves itself into the service's
`worker` leaf, then enables the delegated memory controller in the service
parent. Each native batch attempt and model-server execution gets a unique
`attempt-<execution-id>` or `engine-<execution-id>` child of that same parent.
The worker writes and reads back `memory.max`, `memory.swap.max` and
`memory.oom.group=1`, then confirms the gated process's `cgroup.procs` membership
before releasing the command. Model stop helpers share the server's ceiling.
No attempt is moved into a separate systemd scope. `myce disable` therefore
still stops the worker **and every attempt child**, and its parent
`cgroup.events populated` check includes those descendants.

`join` and `install worker` accept:

- `--host-memory-budget-gb`: aggregate host ceiling budget; default `0` leaves
  legacy admission unchanged. Use the same value and `--host-id` on all cards.
  The queue takes the smallest positive budget reported for that physical host,
  caps its live RAM allowance, and subtracts outstanding job/model ceilings.
  Capped hosts retain charges after loss or unconfirmed cleanup, including model
  residency, until the exact execution is released or a fresh worker proves
  prior-session cleanup. Do not reduce declarations to fit a guessed budget.
- `--memory-ceiling-slack-gb`: extra memory per declared allocation; default
  `0.125` GiB (128 MiB). Slack counts against the aggregate budget. Unified
  machines enforce `memory_gb + host_memory_gb + slack` once; dedicated cards
  enforce `host_memory_gb + slack`.
- `--memory-swap-max-bytes`: swap allowance per attempt; default `0`. Swap is
  separate from the RAM budget; leave it zero for the no-swap envelope.

The kernel can OOM-kill an undersized attempt. The worker reads
`memory.events oom_kill` and reports `exceeded its <actual limit> GiB memory
ceiling`, kills remaining descendants, confirms release and removes the empty
kernel cgroup. This is a normal command failure: jobs become **failed**, with
no infrastructure loss and no automatic retry. It preserves existing checkpoint
files. A pinned model retains its pin and follows the existing bounded load
retry backoff; an unpinned failed load is removed. Ready model-server OOMs are
also detected at heartbeat and reported as failed after cleanup. Cleanup failure
retains RAM custody instead of granting a new allocation.

macOS, cgroup v1, missing delegation and foreground `myce join` retain
admission-only execution. Startup logs and `myce status` say exactly
`memory ceiling not enforced on this host (reason)`. Installer and doctor inspect
the live service's delegation and controller; startup can still be in progress,
so refresh worker status before claiming enforcement. Doctor reports this as an
advisory capability, not a required laptop prerequisite. If a qualified worker
cannot install/confirm an attempt's limit or membership, it refuses that payload;
it does not silently run it without a ceiling.

Managed `run.docker: true` jobs are limited by the Docker daemon, since container
processes are outside the worker service. The adapter supplies `--memory`;
optional per-container `memory_swap` supplies `--memory-swap` (total RAM plus
swap GiB), otherwise Docker defaults apply. It injects
`MYCE_MEMORY_LIMIT_BYTES`, and verifies the labeled container's HostConfig before
start and during reconciliation. Missing or changed limits refuse start or kill
the owned container; `State.OOMKilled` reports the same ordinary failure. An
unmanaged shell wrapper **must** pass the provided limit itself, for example:

```sh
docker run --memory "$MYCE_MEMORY_LIMIT_BYTES" \
  --memory-swap "$MYCE_MEMORY_LIMIT_BYTES" IMAGE COMMAND
```

Unmanaged/nested containers and Docker-backed catalog servers cannot be
certified by the native cgroup or inspected automatically; use the managed batch
adapter and its custody/label contract for qualified container work.

B70 coexistence uses **3 GiB total Mycelium ceilings**, shared across both cards.
Install with `--host-memory-budget-gb 3 --memory-ceiling-slack-gb 0
--memory-swap-max-bytes 0` and set the durable shared override to 3. Declare at
most `needs.host_memory_gb: 3` for a full-envelope experiment; with the default
0.125-GiB slack, declare at most 2.875 GiB. The coordinated production switch
raises the shared override to 27 GiB only after legacy GPU owners are cleared.
See the short deployment section for the switch/fallback command order. These
budgets are operator settings, not code defaults or measured qualification.

The budget bounds admitted attempt/server RAM ceilings, not every byte used by
the worker, environment builds, copies, Docker daemon or unrelated software.
Reserve measured overhead separately. Admission-only hosts do not provide a
kernel bound. GPU/device allocations that the driver does not charge to memcg
still require native qualification, especially on unified-memory hardware.
No configuration prevents unrelated programs from allocating their own RAM.

Optional CPU-only native check: on Linux with an already delegated cgroup v2
parent and the test runner in its `worker` leaf, set
`MYCE_TEST_MEMORY_CGROUP=/sys/fs/cgroup/.../OWNED_PARENT` and run
`go test ./internal/myce -run '^TestMemoryCeilingLinuxIntegration$' -v`.
It creates an owned 32-MiB child, attempts a 128-MiB allocation, checks kernel
OOM evidence and removes that empty cgroup. It skips without explicit delegation;
no fleet or GPU access is needed.

Production can stop lower-priority commands or drain unpinned models on another
card of the same host when that releases RAM; GPU reclaim remains card-local.
Pinned models and failed unloads remain protected. Reservations follow the
existing attempt, loss and restart reconciliation rules. Status shows host RAM
headroom/probe errors and a plain RAM waiting reason.

For several warm audio models, use `measure --warmup WARMUPS.json
--warmup-parallel N` with representative requests, stopped local join and the
exact worker spool. Loads stay serial; N bounds requests after all models load.
`--load-timeout 10m` can qualify slow startup (default 5m). Reports separate
load and warm-up seconds and record the combined sampled peak. Brief spikes
can be missed; real CUDA/XPU qualification remains necessary.

## Forget old queue registrations

`myce host-queues list` prints the registered queue URLs in this OS user's shared
host state. `myce host-queues forget <queue-url>` forgets one stale registration
without contacting the queue. Both accept `--host-state-dir DIR`; `--config PATH`
checks an installed worker config and selects its host-state directory unless
explicitly overridden. Flags can follow the URL, for example:

```sh
myce host-queues list --host-state-dir "$HOME/.local/share/myce"
myce host-queues forget http://127.0.0.1:12345 --host-state-dir "$HOME/.local/share/myce"
```

Forget refuses a queue used by a configured installed worker or a locked local
worker spool. It checks default installation configs, the config beside a
registered spool, and the installed config path recorded by updated workers.
For an older custom installation, supply its `--config`; an unverifiable legacy
worker unit is refused. `--force` explicitly overrides these checks. A forgotten
queue is registered again when a worker configured for it rejoins.

The operation uses the host control/launch locks and one fsynced atomic,
generation-bearing tombstone. Existing disable and pending enable receipts
ignore the forgotten registration; remaining queues retain their original
control generations and can reconcile independently. Worker/unit/spool custody
is retained for cleanup. No worker is stopped, no host/unit flag is cleared,
and no queue admission state is changed remotely. Forgetting a queue is not
proof of cleanup or an instruction to resume work.

## Docker cleanup custody

For opt-in `run.docker` batches, the local `<identity>.docker.json` record is
fsynced before create/start and before forced KILL. It retains the unique
attempt identity, exact container ID when known, daemon endpoint, pending
operation, stop deadline and forced/released disposition. Preserve the worker
spool during upgrades/restarts; an empty replacement spool is not release proof.

The worker reports creating/start pending, running, stopping, released or
**cleanup unconfirmed**. Stop sends container TERM, then KILL at the original
injected-clock grace boundary. Every matching container is inspected for the
exact ownership label before signaling. A final successful scan must prove
all are nonrunning (or absent), followed by owned endpoint disconnection, labeled
network removal and confirmed network absence. Stopped container objects remain
as evidence until retention expires; private networks are removed.

`join --container-retention 24h` keeps stopped containers for 24 hours after
confirmed cleanup and successful queue result acknowledgement (the default).
`--container-retention 0` requests removal immediately after acknowledgement;
negative durations are refused. `install worker` accepts the same flag and
persists it in the worker config. The worker checks retention in the background
once per minute and after each acknowledgement when retention is zero, using
its injected clock. Restart preserves the first acknowledgement timestamp.

Removal inspects both `myce.attempt=<execution ID>` and `myce.worker=<worker ID>`
on every container and requires a settled nonrunning state. Docker removal
never uses force. Running, unconfirmed, unacknowledged, active, foreign-worker,
and unlabelled containers are protected. Failures are logged and retried on the
next retention sweep; they never become cleanup failures or fence admission.
Successful retention also removes only that attempt's cidfiles, Docker custody,
result and acknowledgement records. Captured Mycelium logs keep their independent
`--log-retention` policy. Existing evidence without a durable acknowledgement
marker, including pre-upgrade evidence and rejected stale results, is preserved;
retention does not guess acknowledgement from file age or queue job state.
Each lifecycle Docker call has a 2-second I/O timeout; retries run independently
of heartbeat/control, checkpoint walks and queue log upload. Grace is never
renewed by a Docker retry. Restart reconciles the saved daemon and containers
before fresh readiness, including when the old CLI/process group has vanished.

An empty scan does not settle a timed-out create/start. A definite daemon
refusal can settle without execution. A lost start response can settle only
after positive inspection witnesses the exact created container running,
followed by confirmed cleanup: the adapter sends one start, never retries it,
and rejects restart policies. Until that witness, a future delayed start is
still possible. If no definite disposition or positive acceptance evidence
arrives, retry cleanup indefinitely and preserve exclusion; do not clear state
or treat a daemon restart, worker reboot, elapsed grace or empty scan as proof.
After two minutes of unresolved custody, the queue durably marks that exact
attempt escalated, emits `event=docker_cleanup_escalated` in its log, and shows
**operator action required (myce resolve-docker)** in `myce status` and the status
page. The injected Clock measures from the first unresolved observation; retries
and queue restart do not renew it. The reservation stays held. This bounds silent
waiting, not physical recovery: Verso has only the B70, so alternate placement is
not a recovery plan for its production jobs.

Operator recovery is explicit and offline:

1. Stop the affected worker/service and prevent automatic restart. Keep its spool.
2. Independently fence all accepted/delayed Docker operations on the saved daemon
   endpoint, and verify the exact attempt's execution, GPU use, RAM and labeled
   private networks are gone.
   Empty scans, worker exit and elapsed time alone do not prove this. Do not stop
   unrelated or legacy Verso containers. If a qualified fence cannot be established,
   keep the hold and escalate host maintenance.
3. Read the opaque identity from that worker's `state.json` and matching
   `<identity>.docker.json`. Run
   `myce resolve-docker --state-dir /absolute/worker/spool --attempt-id ID --confirm-gone`.
   This takes the exclusive worker lock, refuses a different attempt, and durably
   records operator-confirmed forced release. It makes no Docker or network calls.
4. If DISABLED is present, rerun `myce disable` to confirm host cleanup, then
   `myce enable`, using the same host control directory. Resolution never removes
   the flag. Restart the same worker with the same spool/queue/name. It sends the existing
   exact job/machine/session/attempt release acknowledgement before log replay.
   Confirm the queue hold clears in status. The command alone does not clear the
   queue; keep the old spool until that acknowledgement is accepted.

`--confirm-gone` is an operator assertion of external proof, not an automatic
check or permission to release uncertain memory. A crash after the custody write
is recoverable; restart reads the durable disposition without killing containers.

Queue obligations survive stop expiry, machine loss, queue restart and fresh
worker sessions, even on hosts without production-hold policy. Unconfirmed
Docker custody blocks new batch/model/preparation allocations on the implicated
physical host, retains declared host RAM, and appears in CLI/HTML/JSON status.
Production may use other eligible hosts; it cannot borrow memory from that
obligation. An attempt-specific release acknowledgement precedes log upload;
stale acknowledgements cannot settle a different attempt. Job retry/terminal
state after worker loss is separate from the old host's physical custody.

The default suite uses a fake CLI plus daemon state outside the CLI's process
group; no Docker daemon or GPU is needed. The optional
`TestDockerRealCheckpointResume` is disabled unless `MYCE_REAL_DOCKER_TEST=1`
and `MYCE_REAL_DOCKER_IMAGE` names an already-local digest-pinned shell image.
It never pulls an image and retains uniquely labelled stopped test containers.
Do not enable it on a shared Mac or fleet without current authorization.
Native device/driver release and aggregate memory limits remain separate gates.

Linux installer EnvironmentFile and ConditionPathExists paths are unquoted;
paths containing whitespace, quotes or backslashes are rejected. ExecStart
arguments remain quoted. `--private-devices=true` is rejected for Linux user
queues; the default uses the startup GPU-descriptor check described above.
Installer linger advice reads loginctl and requests enable-linger only when off.
