# macOS SMB recovery

An operator-owned LaunchAgent can restore a user-owned SMB volume after it
disappears from the mount table. The helper asks macOS NetAuth to mount the
configured share using a passwordless URL and requests that NetAuth suppress
its authentication dialog. It verifies the exact path and source before
reporting recovery. Qualify reuse of the existing saved login from the actual
GUI launchd domain before deploying. This path requires a logged-in session
with its credential store available.

Use this helper on a dedicated fleet Mac with a dedicated mount path. Any
existing underlying directory must be empty and owned by the console user.
The console user configures the target and no other process writes into it
or its ancestors. NetFS mounts by pathname, so the handoff cannot be atomic. A
local process deliberately racing directory replacement during the call is
outside the supported configuration.

## Install the LaunchAgent

1. Start with an authorized idle test share and the worker's usual console
   account. For a mounted volume, record the exact local mount path and
   literal kernel SMB source with `/sbin/mount -t smbfs`. If the volume is
   absent, use a private, previously captured mount-table row from that Mac
   or the operator's provisioning record of the exact path and source for
   that Mac. Verify the record still matches the intended share and saved
   login. If neither record exists, have the owner restore the volume once
   in the GUI session and capture the resulting row. Keep these values private.
2. Build the [helper](../../deploy/macos-nas-remount.m) with the Command Line
   Tools. Replace the two example values below. Preserve the literal escaped
   mount-table source and prefix it with `smb:` to form the URL. Supply no
   password in the URL, plist, shell history or environment.

   ```sh
   mountpoint='/Volumes/disposable'
   smb_source='//operator@nas.example.test/disposable'
   smb_url="smb:$smb_source"
   recovery_dir="$HOME/Library/Application Support/Mycelium/NAS Recovery"
   label='local.mycelium.nas-remount'
   plist="$HOME/Library/LaunchAgents/$label.plist"
   domain="gui/$(id -u)"
   umask 077
   install -d -m 0700 "$recovery_dir"
   recovery_dir=$(cd -P "$recovery_dir" && pwd -P)
   state_identity=$(/usr/bin/stat -f '%Ud:%Ui' "$recovery_dir")
   mkdir -p "$HOME/Library/LaunchAgents"
   helper="$recovery_dir/remount"
   xcrun clang -fobjc-arc -Wall -Wextra -Werror \
     -framework Foundation -framework NetFS deploy/macos-nas-remount.m -o "$helper"
   "$helper" --check "$smb_url" "$mountpoint" "$smb_source" "$recovery_dir" "$state_identity"
   ```

   Use the share's default macOS mount path directly under `/Volumes`, with
   no symlinks in the path. If macOS removes the directory on unmount, the
   helper passes a null mount path to NetFS to use its system-chosen mountpoint.
   Leave an absent target for NetFS to create. If an empty, console-user-owned
   local target survives, the helper requests that directory explicitly. The
   plist pins the recovery directory's device and inode. Follow the stop
   procedure below before moving or replacing the target, its ancestors or
   the recovery directory.
3. With the timer still unloaded, confirm `--check` returns 0 or 69. If the
   volume is absent, check the NAS lockout policy and account headroom, then use
   the recorded inputs for a supervised one-shot recovery. Correlate it with
   NAS login counts as described below. Require `--check` to return 0 after the
   one-shot before enabling the timer. If a safety check refuses recovery,
   stop and inspect the cause before continuing.

   ```sh
   "$helper" "$smb_url" "$mountpoint" "$smb_source" "$recovery_dir" "$state_identity"
   ```

4. Verify the plist and label are unused in this GUI domain. Create the
   LaunchAgent and load it as the console user. A healthy tick reads cached
   mount inventory. The minute interval retries a recovery that stopped
   before NetFS. Launchd runs at most one helper instance at a time. Run
   the mounting form by hand only while the agent is unloaded and all
   previous mount requests have ended.

   ```sh
   plutil -create xml1 "$plist"
   plutil -insert Label -string "$label" "$plist"
   plutil -insert ProgramArguments -array "$plist"
   plutil -insert ProgramArguments.0 -string "$helper" "$plist"
   plutil -insert ProgramArguments.1 -string "$smb_url" "$plist"
   plutil -insert ProgramArguments.2 -string "$mountpoint" "$plist"
   plutil -insert ProgramArguments.3 -string "$smb_source" "$plist"
   plutil -insert ProgramArguments.4 -string "$recovery_dir" "$plist"
   plutil -insert ProgramArguments.5 -string "$state_identity" "$plist"
   plutil -insert LimitLoadToSessionType -string Aqua "$plist"
   plutil -insert RunAtLoad -bool YES "$plist"
   plutil -insert StartInterval -integer 60 "$plist"
   plutil -lint "$plist"
   launchctl bootstrap "$domain" "$plist"
   ```

### Helper exit codes

| Code | Meaning |
| --- | --- |
| 0 | Cached inventory has the exact SMB source at the configured path, and neither hold record is present. |
| 64 | Invalid arguments, URL or path syntax. |
| 69 | `--check` found the share absent and the target eligible for recovery. The target is either absent directly under local `/Volumes` or an empty, canonical, user-owned local directory. |
| 71 | The cached mount table could not be read before attempting recovery. |
| 73 | The helper could not read, persist or clear its attempt state. |
| 75 | A transport failure was recorded, or the persistent network retry budget defers a NetFS call. A later tick retries automatically. |
| 77 | An authentication attempt is held after an interrupted, authentication-like or unknown NetFS outcome. |
| 78 | A URL, target or inventory check refused recovery before NetFS. |
| 79 | This or a prior NetFS attempt recorded a mount refusal that needs operator inspection. |
| 80 | The recovery directory is missing or fails its pinned identity, local-path, ownership or mode check. |

`--check` uses the same recovery-directory identity and holds as the timer.
It reads cached inventory without SMB I/O and does not call NetFS or write
state. Exit 69 checks the target. The timer also checks the retry budget
before invoking NetFS.

The helper creates and syncs `auth-hold` in the pinned local mode-0700
directory before calling NetFS. It clears the marker after a verified
mount or when it can prove it stopped before NetFS. An interrupted or
uncertain attempt keeps the hold. To accept a successful call, the helper
requires the single path returned by NetFS and the cached kernel inventory
to agree on the configured path, and the inventory to show the exact SMB
source there. If NetFS chooses a suffixed path or the inventory shows the
wrong source, the helper changes the hold to `refused-mount` and returns 79.
Later ticks and checks return 79 even if an exact mount appears in the
inventory. The helper leaves any mount it finds in place.

After a transport failure (`ENETDOWN`, `ENETUNREACH`, `EHOSTDOWN`,
`EHOSTUNREACH`, `ECONNREFUSED`, `ENOTCONN`, `ENETRESET`, `ECONNABORTED`,
`EPIPE` or `ETIMEDOUT`), the
helper clears `auth-hold`, returns 75 and retries on a later tick without
operator action. It persists a budget of two failures per 15-minute window,
with at least two minutes from the recorded failure to the next eligible
attempt. The window starts at the first recorded failure, so recovery after
the network returns takes at most about 15 minutes, and the helper makes at
most about eight attempts an hour while the network stays down.

With a valid saved credential, a transport failure is not a rejected login.
The errno alone cannot prove that, though. Apple's SMB client maps a received
logon failure to `EAUTH`, but if the NAS rejects a stale credential and the
reply is lost, the call can end in `ETIMEDOUT`, `ENOTCONN` or `EPIPE`, and an
internal reconnect can end in a reachability code
([session retry][apple-session], [mount code][apple-mount]). The retry budget,
not the errno, bounds failed logins in that case: at most three NetFS calls in
any rolling five minutes, and Apple can retry authentication inside one call.
Before enabling the timer on a worker, verify the NAS account-lockout policy
and the account's remaining headroom, and correlate the pilot's NetFS calls
with the NAS's session-setup and failed-login counts. Results the client
reports as rejected (`EAUTH`, unsupported authentication) and any unknown
status keep `auth-hold` until an operator clears it. The budget also bounds
load on the server during an outage.

If the inventory read after NetFS fails, the helper cannot prove the mount and
records `refused-mount`. Later ticks return 79 until an operator inspects the
mount, even if the exact mount then appears. A backward wall-clock correction
can lengthen the retry wait.

[apple-session]: https://github.com/apple-oss-distributions/SMBClient/blob/99da2d9a4bbe17cde3cee382ad3edc16e25f5535/lib/smb/ctx.c#L3129-L3147
[apple-mount]: https://github.com/apple-oss-distributions/SMBClient/blob/99da2d9a4bbe17cde3cee382ad3edc16e25f5535/lib/smb/ctx.c#L3326-L3340

### Clear a hold or roll back

1. Unload with `launchctl bootout "$domain/$label"`. Use
   `launchctl print "$domain/$label"` to confirm the service is absent.
   Check the process list for the configured helper and any manual run.
   Confirm any outstanding NetFS request reached a terminal outcome using
   the helper and macOS NetAuth/SMB logs. If an interrupted run leaves the
   request state uncertain, leave the agent unloaded and the state intact
   until the operator can establish that no mount request is in flight.
2. Check cached mount inventory and inspect any refused mount before
   clearing state or re-arming. Verify the exact source and path, and
   inspect the underlying target once any conflicting mount has been
   safely resolved. Verify the recovery directory still matches the
   device and inode in the plist. For exit 73, repair the local state
   storage and inspect the remaining records. For exit 80, recover the
   original directory and its records where possible. Before pinning a
   replacement during a deliberate reinstall, investigate the old hold
   and mount outcome under the same stop procedure.
3. To re-arm, remove the inspected `auth-hold` or `refused-mount` from
   the verified directory. Preserve `network-retry` so the budget survives
   re-arming. Run `--check` with the plist's arguments and continue when it
   returns 0 or 69. Then run `launchctl bootstrap "$domain" "$plist"`.
   For rollback, remove the unloaded plist from `~/Library/LaunchAgents`
   and retain the recovery state for inspection. Leave any restored mount
   under OS control.

## Qualify recovery

1. With the disposable share idle, use `/sbin/umount "$mountpoint"` without
   force or observe a natural disappearance. Confirm the SMB mount disappears
   and record whether macOS removed the underlying directory. If you can run
   the helper's `--check` while the share is absent, expect 69 with either a
   missing or an eligible empty directory. Observe the agent restore the exact
   source and path and the check return 0 without operator action or a prompt.
   Verify a bounded read of a known file as the worker account. To qualify
   recreation of the mountpoint, observe a recovery that starts with the
   target absent.
2. Repeat with the screen locked and with the network restored late in the
   authorized test window. Collect the helper's fixed outcome messages with
   `log show --last 10m --predicate 'eventMessage BEGINSWITH "Mycelium NAS remount"'`.
   Correlate each numeric NetFS outcome and the logged budget usage with
   server-side SMB session setup and failed-login counts. A single NetFS
   call can try more than one login internally. If the test produces a
   hold, verify subsequent ticks make no calls and follow the stop
   procedure before re-arming. Investigate a still-listed, stalled volume
   through the OS or storage owner. Leaving it mounted lets the OS reconnect it and preserves active
   handles.
3. Repeat for the configured worker mount on one drained Mac at a time.
   Require recovery at the original path, fresh successful worker probes and
   a representative NAS-dependent job before returning the worker to normal
   service. Use the stop procedure above for rollback.

For local fixture verification, build and run
[`deploy/test_macos_nas_remount.m`](../../deploy/test_macos_nas_remount.m)
with the same compiler flags. Its simulated inventory and NetFS calls require
no SMB server, credential or launchd changes.

## Pre-login recovery with autofs

A system autofs map can restore an SMB mount on access before GUI login.
Confirm the system context already has a noninteractive credential accepted
by the SMB server, such as a site-managed Kerberos identity. A login Keychain
item does not establish this. If a dedicated least-privilege account is
needed, provision it together with a system credential delivery mechanism
supported by the host and NAS. Keep passwords out of maps, URLs and process
arguments.

Back up `/etc/auto_master`. Add one master-map entry for an unused test path
and create the root-owned mode-0600 map. For example, use the following row
in `/etc/auto_master`.

```text
/Volumes/mycelium-autofs /etc/auto_mycelium_nas
```

Put this row in `/etc/auto_mycelium_nas`.

```text
disposable -fstype=smbfs,nopassprompt ://operator@nas.example.test/disposable
```

Run `sudo automount -vc` and access a known file under
`/Volumes/mycelium-autofs/disposable`. Verify the
expected `smbfs` source and the worker account's access. Repeat after an
authorized boot before GUI login and when the network arrives late. Require
an actual SMB backing behind the autofs placeholder. Qualify the production
path before readmission. To roll back the test, restore the master map and
reload automount once the test mount is idle.

## Isolate an upgrade-time failure

On a drained worker with a disposable share and the queue held on one
release, compare idle SMB traffic, installer storage preflight alone,
artifact transfer and local writes alone, a same-image worker restart, and
activation of a different signed UUID with the same designated requirement.
Run one step at a time and record UTC start and end times. In each arm, use
an independent bounded metadata probe on a second path and sample cached
mount inventory. On a new failure, correlate the client syscall and SMB
request with TCP reset or retransmission times, client SMB reconnect and
network policy records, and the NAS's per-client SMB request, lease, queue
and disk latency. Use authorized capture on both ends and keep those logs
private. The first stalled operation and the eventual disappearance are
separate observations.
