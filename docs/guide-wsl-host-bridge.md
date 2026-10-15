# Windows host signals for a WSL2 daily-use hypha

The `myce-host-bridge` companion runs as the Windows user at interactive logon.
The Linux `myce` worker stays in WSL2. The companion reads aggregate idle time
and actual system power state; it never reads input contents, raw input devices,
window contents, process/application names, screenshots, or user names. Session
IDs and connection states are checked locally and are never sent to WSL or the
hub. There is no service, driver, administrator requirement, or firewall change.
Elevated invocation is refused.

**Thermal coverage remains conservative.** The helper queries the driver-installed
Windows system-directory `nvidia-smi.exe` (never PATH/current-directory lookup):
`--query-gpu=temperature.gpu,temperature.gpu.tlimit --format=csv,noheader`.
All GPU rows must parse; missing/unsupported values remain unknown. NVIDIA
T.Limit is signed **remaining headroom**, not an absolute ceiling: at/below zero
means serious, below five degrees means fair, otherwise nominal. This follows
the Linux probe's five-degree recovery margin; the existing 60-second cooling
dwell still applies. See [NVIDIA temperature semantics](https://docs.nvidia.com/deploy/nvidia-smi/index.html#temperature).
The query is hidden, bounded by a 750 ms deadline and invoked only when sampled.

GPU heat appears in `gpu_heat`, with `heat_coverage=gpu-only`. It cannot certify
CPU/host thermal coverage; host `heat` remains `unknown`. On an externally
powered desktop explicitly declared with `--desktop` (and no detected battery),
daily admission uses GPU heat and reports **CPU thermal coverage waived for desktop**. Laptops
still wait with **host CPU thermal coverage unavailable**. Unknown power,
unknown owner activity, and unknown or hot GPU heat continue to withhold work.
Generic ACPI thermal zones do not establish CPU coverage; no nominal CPU
reading is inferred from an unidentified zone. No driver, service, third-party
sensor install or guest fallback is required for the desktop exemption.

## Build on a Mac or Linux laptop

```sh
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build \
  -ldflags='-H=windowsgui' -o myce-host-bridge.exe ./cmd/myce-host-bridge
go build -o myce ./cmd/myce
```

This small Go executable intentionally imports only `internal/hostbridge` and
Windows syscall wrappers. The queue and command runner use Unix process APIs;
the Windows companion does not cross-compile that whole runtime or run jobs.
The GUI subsystem prevents a terminal opening at each Windows logon. It has no
window, UI, input hooks, or access log. Use `Start-Process -Wait -PassThru` to
check install/uninstall exit codes with a GUI binary; errors do not have a
console. A console build without `-ldflags` is useful for troubleshooting.

## One-step install and uninstall

Stage `scripts/windows-host-bridge/*` and the Windows executable together in
`%USERPROFILE%\Downloads\MyceliumHostBridge-Setup`. From the regular Windows
desktop double-click **Install.cmd**; double-click **Uninstall.cmd** to remove
the installed helper. Setup checks each binary exit code, starts the installed
helper immediately, and leaves a failed window open. Do not run as administrator.
The original downloaded executable must remain outside the installed directory.

PowerShell one-line alternative:

```powershell
& "$env:USERPROFILE\Downloads\MyceliumHostBridge-Setup\Setup.ps1" -Action Install
```

For an authorized remote operator using the same account as the logged-on owner:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File "$env:USERPROFILE\Downloads\MyceliumHostBridge-Setup\Remote-Setup.ps1" -Action Install
```

This uses the standard Windows Task Scheduler COM API: InteractiveToken (no
password) and LUA/limited run level. The one-shot task runs the **same Setup.ps1**
in the existing desktop session and is deleted after completion. Aggregate
receipt fields verify exit code, session and elevation; no credential is logged.
The exact task name is `MyceliumHostBridge-Setup`. Setup deletes it as its
last step on success or failure, even after a launcher timeout. A retry first
removes an inactive leftover with that exact name and refuses a running or queued
setup; it never kills an active installer. Replace Install with Uninstall for
either command. Downloads files and aggregate receipts are intentionally retained after uninstall.
No logoff, service, persistent task, administrator install or settings change
is required. The helper also refuses installation outside the covered console.

Installation creates only that previously absent directory with the executable
and `bridge.json`, plus the single `MyceliumHostBridge` value in
`HKCU\Software\Microsoft\Windows\CurrentVersion\Run`. It refuses an existing
directory or startup entry. Before writing the random 256-bit credential, the
directory gets a protected DACL granting only this user's SID access; children
inherit it. No credential is put in the Run value, arguments, output, or fleet
token. Installation does not start a session-0 helper, change WSL, or alter any
other startup values. Windows Startup policy can disable/delay Run entries;
natural logon operation needs native verification.

Keep `bridge.json` private. Do not print/copy it into a report, issue, shell
history, job environment, or repository. Programs with the same Windows user
rights (including that user's WSL/root processes) can read it: this is a local
user trust boundary, not isolation from hostile code running as the owner.

## Connect from WSL2

The bridge listens **only** on IPv4 `127.0.0.1:47353`. WSL2 **mirrored** networking
is required for WSL-to-Windows IPv4 loopback. NAT fallback, a LAN listener,
portproxy, firewall exceptions and administrator changes are not provided.
If loopback is unavailable, work waits.

Set `MYCE_HOST_BRIDGE_CONFIG` to the WSL path of the installed Windows config
(using the actual local profile path privately):

```sh
export MYCE_HOST_BRIDGE_CONFIG='/mnt/c/Users/<owner>/AppData/Local/MyceliumHostBridge/bridge.json'
./myce join --daily-use --laptop --max-jobs 1 --joining-file '<private invitation path>'
```

For a desktop, use `--desktop`; it is mutually exclusive with `--laptop`.
Without `--desktop`, CPU coverage is required. Detected battery hardware still
requires CPU thermal coverage and confirmed AC.

The path is explicit to avoid guessing Windows user/session identity. WSL reads
the file through its existing Windows filesystem access. No credential is
passed in argv or HTTP. Each request has a random nonce and HMAC-SHA256 request
authentication; the response signs the nonce and entire body with a separate
direction tag. A replayed response, impersonator, modified response or wrong credential
fails closed. Requests are read-only; request nonces are not an access-audit or
server-side replay ledger. The loopback transport relies on OS isolation from
packet capture by other users; same-user code and administrators are trusted. Proxies, redirects, DNS hostnames and non-loopback config addresses
are rejected. Requests have a 1.5-second deadline and 4 KiB response limit.
Only `GET /v1/signals` is exposed. No remote control/stop endpoint exists.

Each authenticated request samples fresh signals, never a cached observation.
Windows `ProcessIdToSessionId`, `WTSEnumerateSessions` and
`WTSGetActiveConsoleSessionId` must establish one active console session matching
the helper, both before and after the `GetLastInputInfo` query. Session 0, RDP,
additional/disconnected sessions, denied enumeration and transitions are unknown.
This deliberately conservative first version does not aggregate helpers across
multiple sessions. Idle arithmetic handles tick wrap and rejects future tick
anomalies. It counts at most the continuously observed awake interval, resets
on changed last-input ticks, and resets after gaps over 15 seconds/clock rollback.
Startup/reconnect therefore requires five newly observed idle minutes. This is
aggregate input inactivity, not proof that a motionless owner or controller user
is absent; no claim of covering every input device is made.

`GetSystemPowerStatus` supplies actual AC/DC and battery classification. Failed,
unknown or invalid values stay unknown. Battery saver vetoes work through the
existing battery hold. A desktop is not declared externally powered merely
because it has no battery. `--laptop` conservatively requires confirmed AC even
when battery detection fails. Native hardware/API behavior remains to be tested.

Missing config, stopped helper, read/auth/transport failure or uncovered session
never falls back to guest readings. Existing five-second sampling, local job
withdrawal, 15-second staleness, cooling dwell and checkpoint behavior apply.
The normal Linux probes are unchanged on non-WSL kernels. Dedicated mode retains
its existing policy; use `--daily-use` for a personal machine.

## Uninstall and undo

Double-click **Uninstall.cmd**, or use Setup.ps1/Remote-Setup.ps1 with
`-Action Uninstall`. Both use the original downloaded executable.

Uninstall checks ownership of the Run command, removes that one value, revokes
`bridge.json`, waits up to five seconds for the executable to become removable,
and removes only its two known files and the empty installation directory. The
running helper notices config revocation within a second and closes its server.
If the binary stays locked or unrelated files exist, it reports failure and
preserves them; inspect, then retry. It never kills an unrelated PID or removes
a folder recursively. Reinstall creates a fresh key. Remove the WSL environment
setting (`unset MYCE_HOST_BRIDGE_CONFIG`) and any explicit shell configuration
you added. No system startup/service/driver setting needs undoing.

## Native verification

Use an isolated Mac-hosted hub with saved-address joining and the real WSL
worker. Discovery is a separate concern; do not change firewall/networking
settings to test this helper. Verify artifact hashes before installation.
Check limited interactive install, protected directory/config ACL, Run value,
loopback listener, real authenticated signals, continuous five-minute idle
sampling, fail-closed CPU coverage, easy uninstall residue checks and reinstall.
Leave the helper installed when requested and stop the owned worker, hub,
forwarding processes and test distro. Never synthesize owner input or treat a
GPU-only nominal reading as host coverage. Native sleep, battery unplug, RDP,
fast user switching and natural logon require separate checks.

## Laptop verification

`internal/hostbridge` tests cover conservative session coverage, tick wrap,
awake continuity, AC/battery/unknown/saver, authenticated round-trip, wrong keys,
replay/forgery, malformed responses and reversible install ownership/failure.
`internal/myce` runs the authenticated bridge through the actual WSL detection
path with fake signals and Clock, checking owner return, battery, unknown/unsafe
heat, config revocation, stopped-helper fail-closed behavior and no guest probes.
These are laptop tests and cross-compiles, not native Windows qualification.
