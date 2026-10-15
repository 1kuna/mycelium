# Windows host signals for a WSL2 daily-use hypha

The `myce-host-bridge` companion runs as the Windows user at interactive logon.
The Linux `myce` worker stays in WSL2. The companion reads aggregate idle time
and actual system power state; it never reads input contents, raw input devices,
window contents, process/application names, screenshots, or user names. Session
IDs and connection states are checked locally and are never sent to WSL or the
hub. There is no service, driver, administrator requirement, or firewall change.
Elevated invocation is refused.

**Thermal coverage is currently unknown on Windows.** There is no uniform
nonprivileged Windows API that establishes live CPU and GPU thermal coverage.
The helper reports `heat=unknown`; it does not use the unpopulated WMI
`Win32_TemperatureProbe.CurrentReading`, invent a nominal reading, or substitute
WSL guest sensors. Consequently daily-use work **continues to withhold**, even
when idle and power are known. A separately qualified Windows thermal source is
still needed before enabling daily work. This implements the host bridge, not
complete Windows daily-use thermal qualification.

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

## Install and start (regular interactive Windows PowerShell)

These are instructions for a separately authorized native task. Do not launch
`serve` from WSL interop, SSH/session 0, a service, or an elevated terminal. Those
contexts cannot establish the intended console-session coverage. Install may be
prepared via regular-user SSH, but **start must be in the desktop session** (or
at the next natural user logon). Do not log the user off to test it.

Keep the original downloaded executable outside the installation directory:

```powershell
$download = (Resolve-Path '.\myce-host-bridge.exe').Path
$p = Start-Process -FilePath $download -ArgumentList 'install' -Wait -PassThru
if ($p.ExitCode -ne 0) { throw 'Helper install failed; inspect with a console build' }
$root = Join-Path $env:LOCALAPPDATA 'MyceliumHostBridge'
$helper = Join-Path $root 'myce-host-bridge.exe'
$helperProcess = Start-Process -FilePath $helper -ArgumentList 'serve' -PassThru
```

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

Run the **original downloaded executable**, from outside the installed folder:

```powershell
$p = Start-Process -FilePath $download -ArgumentList 'uninstall' -Wait -PassThru
if ($p.ExitCode -ne 0) { throw 'Helper uninstall incomplete; inspect and retry' }
```

Uninstall checks ownership of the Run command, removes that one value, revokes
`bridge.json`, waits up to five seconds for the executable to become removable,
and removes only its two known files and the empty installation directory. The
running helper notices config revocation within a second and closes its server.
If the binary stays locked or unrelated files exist, it reports failure and
preserves them; inspect, then retry. It never kills an unrelated PID or removes
a folder recursively. Reinstall creates a fresh key. Remove the WSL environment
setting (`unset MYCE_HOST_BRIDGE_CONFIG`) and any explicit shell configuration
you added. No system startup/service/driver setting needs undoing.

## Exact native acceptance plan (separate authorization, at/after 08:50 EDT)

1. Verify the authorized branch commit and SHA-256 of both Mac-built executables
   after copying to a new private test folder. No native compile/install of
   dependencies, CUDA, GPU workload, sensor drivers, stress or game interruption.
   Keep the #52 envelope: Linux nice 19, four-CPU affinity, `GOMAXPROCS=4`, memory
   allowance at most the approximate 4 GiB aggregate, track owned PIDs and RSS.
   Fail the task if limits or source custody cannot be established.
2. Record the target distro's initial state and whether other distros/workloads
   are active. Save **exact original bytes** and SHA-256 of `%USERPROFILE%\.wslconfig`
   in approved private storage, recording whether it was absent. Preserve all
   existing settings. Set only `networkingMode=mirrored` in its `[wsl2]` section
   (create that section/file if absent; stop for duplicate/ambiguous entries).
   Apply/restart only within the later task's explicit disruption authorization:
   Windows can require `wsl --shutdown` for global config. If another active
   distro would be affected, stop and report the gate; do not shut it down.
   `wsl --terminate <test distro>` alone does not prove global config took effect.
   Record the edited file hash too; undo must stop if the current file differs
   from that owned edit, preserving concurrent changes. On restart require
   `wslinfo --networking-mode` to report `mirrored`; do not infer
   success from the file edit. No firewall change, host reboot or logoff.
3. Install using the commands above as the regular user. Privately verify the
   single Run value, protected DACL and inherited config permissions, new file
   names/counts, and loopback-only listening socket; an existing install/port
   collision stops the test. Start `serve` in the real interactive desktop
   session using the commands above. If only SSH/session 0 is available, defer
   the trustworthy idle test until natural logon/user desktop launch; never use
   session-0 success as console coverage. Keep the exact helper PID.
4. Set the WSL config path privately, join a throwaway pinned invitation to an
   isolated local-test hub, and inspect `capability_profile.availability`.
   Expect real `idle_known=true`, near-zero idle while the owner is active,
   `power=ac` on this desktop if the API confirms it, `heat=unknown`, and
   `available=false`. Check again after five observed minutes without asking
   the user to stop gaming; if idle cannot occur naturally, mark that pending.
   Heat unknown must still withhold. A harmless queued command must remain at
   attempt 0/unassigned, then cancel only that test job. No CUDA test here.
5. With an intentionally wrong **private copy** of the config key, expect
   unknown host signals and no work; restore the original path without printing
   either key. Stop only the owned helper PID (or revoke config via uninstall)
   and verify WSL returns unknown/no guest fallback on the next sample. An
   optional helper deliberately started from session-0 interop must yield
   `idle_known=false`; avoid port collision, stop only that owned process, then
   restart the interactive helper. Do not create/switch/disconnect user sessions
   or unplug power for this background test. Those scenarios have fake tests;
   native fast-user-switch, RDP, battery, sleep and thermal qualification remain
   pending unless separately authorized.
6. Stop the owned worker/hub cleanly; cancel test jobs. Uninstall using the
   original executable, and verify Run value absent, helper exited, port closed,
   owned config/executable absent. Undo the WSL setting by restoring the exact
   backed-up `.wslconfig` bytes (or remove only the newly created file if it was
   absent), verify its original hash, and apply using the same authorized
   distro/global restart gate as above. Confirm the original networking mode
   and distro states. If restoring an originally running distro is outside the
   authorized scope, report it rather than launching unrelated work. Remove
   credentials and owned temp files only after owned processes stop and useful
   sanitized evidence is retained. Report actual exit codes, hashes/counts and
   aggregate signals only; no user/machine names, paths, addresses or keys.

The networking switch is independent of installing the helper; uninstall does
not modify `.wslconfig`. If any native gate fails, stop, undo only owned changes,
verify the prior state, and report the remaining hold. Never weaken the thermal
or interactive-session rule to get an available result.

## Laptop verification

`internal/hostbridge` tests cover conservative session coverage, tick wrap,
awake continuity, AC/battery/unknown/saver, authenticated round-trip, wrong keys,
replay/forgery, malformed responses and reversible install ownership/failure.
`internal/myce` runs the authenticated bridge through the actual WSL detection
path with fake signals and Clock, checking owner return, battery, unknown/unsafe
heat, config revocation, stopped-helper fail-closed behavior and no guest probes.
These are laptop tests and cross-compiles, not native Windows qualification.
