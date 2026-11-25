# macOS SMB recovery

An operator-owned LaunchAgent can restore a user-owned SMB volume after it
disappears from the mount table. The helper asks macOS NetAuth to mount the
exact share at the original path using a passwordless URL and requests that
NetAuth suppress its authentication dialog. Qualify reuse of the existing
saved login from the actual GUI launchd domain before deploying. This path
requires a logged-in session with its credential store available.

## Install the LaunchAgent

1. Start with an authorized idle test share and the worker's usual console
   account. From a known-good mount, record the exact local mount path and
   literal kernel SMB source with `/sbin/mount -t smbfs`. Use the same
   URL spelling and SMB account as the existing saved login. Keep these
   values private.
2. Build the [helper](../../deploy/macos-nas-remount.m) with the Command Line
   Tools. Replace the three example values below. URL-escape special
   characters in the user or share. Supply no password in the URL, plist,
   shell history or environment.

   ```sh
   smb_url='smb://operator@nas.example.test/disposable'
   mountpoint='/Volumes/disposable'
   smb_source='//operator@nas.example.test/disposable'
   recovery_dir="$HOME/Library/Application Support/Mycelium/NAS Recovery"
   label='local.mycelium.nas-remount'
   plist="$HOME/Library/LaunchAgents/$label.plist"
   domain="gui/$(id -u)"
   umask 077
   install -d -m 0700 "$recovery_dir"
   mkdir -p "$HOME/Library/LaunchAgents"
   helper="$recovery_dir/remount"
   auth_hold="$recovery_dir/auth-hold"
   xcrun clang -fobjc-arc -Wall -Wextra -Werror \
     -framework Foundation -framework NetFS deploy/macos-nas-remount.m -o "$helper"
   "$helper" --check "$smb_url" "$mountpoint" "$smb_source"
   ```

   The last command returns 0 for the exact mount, 69 when absent, and 78 for
   a conflicting source or target. It reads cached inventory without SMB I/O.
   The underlying target must be an empty local directory owned by the console
   user. When initial creation under `/Volumes` is needed, an authorized
   administrator can use `sudo install -d -m 0700 -o "$(id -un)" "$mountpoint"`
   after verifying no mount or local data occupies that path.
3. Verify the plist and label are unused in this GUI domain. Create the
   LaunchAgent and load it as the console user. A healthy tick reads cached
   mount inventory. The minute interval permits a retry after network
   return, with at most one outstanding NetAuth call under launchd.

   ```sh
   plutil -create xml1 "$plist"
   plutil -insert Label -string "$label" "$plist"
   plutil -insert ProgramArguments -array "$plist"
   plutil -insert ProgramArguments.0 -string "$helper" "$plist"
   plutil -insert ProgramArguments.1 -string "$smb_url" "$plist"
   plutil -insert ProgramArguments.2 -string "$mountpoint" "$plist"
   plutil -insert ProgramArguments.3 -string "$smb_source" "$plist"
   plutil -insert ProgramArguments.4 -string "$auth_hold" "$plist"
   plutil -insert LimitLoadToSessionType -string Aqua "$plist"
   plutil -insert RunAtLoad -bool YES "$plist"
   plutil -insert StartInterval -integer 60 "$plist"
   plutil -lint "$plist"
   launchctl bootstrap "$domain" "$plist"
   ```

## Qualify recovery

1. With the disposable share idle, use `/sbin/umount "$mountpoint"` without
   force or observe a natural disappearance. Confirm that the empty
   underlying directory survives and the helper's `--check` returns 69
   while the share is absent. Observe the agent restore the original mount
   and the check return 0 without operator action or a prompt. Verify a
   bounded read of a known file as the worker account. A successful NetFS
   call alone establishes neither the mount identity nor share health.
2. Repeat with the screen locked and with the network restored late in the
   authorized test window. Collect the helper's fixed outcome messages with
   `log show --last 10m --predicate 'eventMessage BEGINSWITH "Mycelium NAS remount"'`.
   An explicit authentication or permission refusal creates a local
   `auth-hold` file. Resolve the refusal in the host session before removing
   that file and starting the same agent again. Investigate a still-listed,
   stalled volume through the OS or storage owner. The helper leaves it in
   place so the OS can reconnect it and active handles retain their mount.
3. Repeat for the configured worker mount on one drained Mac at a time.
   Require recovery at the original path, fresh successful worker probes and
   a representative NAS-dependent job before returning the worker to normal
   service. For rollback, run `launchctl bootout "$domain/$label"` and remove
   that plist from `~/Library/LaunchAgents`. This leaves any restored mount
   under the OS.

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
