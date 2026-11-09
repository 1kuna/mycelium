# Optional xe CCS preemption timeout

This is an administrator-installed Linux host template, not an automatic
Mycelium installation or a recommended public tuning default. It requires
Python 3.9+, systemd-udev and xe's engine-class sysfs interface. The shipped
configuration is unset and performs no writes. See
[evidence and tradeoff](../../docs/host-probes.md#optional-xe-ccs-preemption-timeout).
Installation/application requires separate authorization after trial results;
these instructions do not imply that the trial passed.

## Mechanism and ordering

Use a synchronous udev `RUN` helper rather than an asynchronous systemd device
activation: the short, local writes complete during each primary DRM card add,
including boot coldplug, driver rebind and hotplug. No network, GPU workload or
daemon is started. The worker drop-in waits for udev processing and independently
verifies the configured value before every worker start. This works with the
normal **user** worker unit; ordering a root system unit `Before=` a user unit
would not order the two managers. `udevadm settle` uses its standard 120-second
timeout: a stuck udev queue prevents worker startup rather than bypassing the
ordering requirement, at the cost of delaying startup for unrelated udev work.
Verification failure also prevents startup; udev helper errors alone do not
prevent device registration, so inspect the journal after every device add.

The helper discovers every xe primary card, then every tile/GT CCS class. It
only writes `engines/ccs/preempt_timeout_us`, reads the live sibling
`preempt_timeout_min` / `preempt_timeout_max` (never `.defaults` bounds), validates
all selected ranges before writing, and reads back and logs each write. It
does not modify bounds, `job_timeout_ms`, timeslices or other engine classes.
Kernel rejection/readback failure is fatal; completed writes on other devices
are retained in the rollback record. Repeated application skips matching values.
No xe/CCS device is a successful no-op.

Prior values are recorded before the first write in root-only
`/var/lib/myce-xe-preempt-timeout/prior.json`, keyed by PCI device and tile/GT,
not volatile card number. The record survives reboot and repeated application.
Serialized callbacks prevent state updates racing with one another or restore.
Do not edit state or run another tuning tool concurrently. GPU queue creation
must be quiescent during initial application and rollback: existing queues keep
their old timeout. The drop-in gates worker startup, not already-running jobs
or unrelated applications. Native boot/hotplug/udev ordering remains to be
qualified on an authorized idle host; fake sysfs does not prove it.

## Install after separate authorization

First stop/drain GPU work through the normal maintenance procedure, stop the
worker and any other queue creators, and record the worker's unit, manager,
enabled/active state and all current CCS values. Back up existing destination
files; do not overwrite another installation's state. Keep config/helper and
all parent directories root-owned and unwritable by job accounts.

From this directory (commands shown for a future authorized administrator):

```sh
sudo install -o root -g root -m 0755 myce-xe-preempt-timeout /usr/local/libexec/myce-xe-preempt-timeout
sudo install -o root -g root -m 0644 myce-xe-preempt-timeout.conf /etc/myce-xe-preempt-timeout.conf
```

Edit **only** `/etc/myce-xe-preempt-timeout.conf`: leave
`PREEMPT_TIMEOUT_US=` to preserve kernel/administrator settings, or explicitly
set `PREEMPT_TIMEOUT_US=1280000` only if that diagnostic trial value is approved.
Only unsigned decimal values and comments are accepted; this file is not a shell
script. Removing the value later stops future writes but does **not** undo
existing settings; use rollback below.

```sh
sudo install -o root -g root -m 0644 70-myce-xe-preempt-timeout.rules /etc/udev/rules.d/70-myce-xe-preempt-timeout.rules
sudo udevadm control --reload-rules
sudo /usr/local/libexec/myce-xe-preempt-timeout apply
```

Install `70-xe-preempt-timeout.conf` into the actual worker unit's drop-in
directory. For the default user unit, as the worker service account:

```sh
mkdir -p ~/.config/systemd/user/myce-worker.service.d
install -m 0644 70-xe-preempt-timeout.conf ~/.config/systemd/user/myce-worker.service.d/70-xe-preempt-timeout.conf
systemctl --user daemon-reload
systemctl --user cat myce-worker.service
/usr/local/libexec/myce-xe-preempt-timeout verify
systemctl --user start myce-worker.service
```

For a differently named/templated unit use its actual name. For a system worker,
install the same drop-in as root under `/etc/systemd/system/UNIT.service.d/`
and use system `systemctl` commands. Check `/usr/bin/udevadm` exists and that the
worker's existing sandbox allows this read-only verification. Repeat for every
worker that can create GPU queues. Do not replace existing `ExecStartPre` lines.
No system service is enabled by this template.

## Verify and roll back

Read values directly and inspect logs after install and each separately
authorized reboot/device-add test:

```sh
/usr/local/libexec/myce-xe-preempt-timeout verify
grep -H . /sys/class/drm/card[0-9]*/device/tile*/gt*/engines/ccs/preempt_timeout_us
journalctl -t myce-xe-preempt-timeout
systemctl --user status myce-worker.service
```

Confirm every xe CCS value, successful readback logs, successful worker pre-start
commands and that newly created execution queues are used. A readback proves
the host setting, not workload stability or the value of already-created queues.
Collect kernel reset evidence independently during the confirmation workload.

To uninstall, quiesce all queue creators again and stop the worker. Remove the
rule first and let in-flight callbacks finish before restoring:

```sh
sudo rm /etc/udev/rules.d/70-myce-xe-preempt-timeout.rules
sudo udevadm control --reload-rules
sudo udevadm settle
sudo /usr/local/libexec/myce-xe-preempt-timeout restore
```

`restore` reads back and logs restoration of each recorded prior value (normally
640000). If the trial value had already been set manually at first application,
that value is the recorded prior value; to explicitly return to 640000 instead,
set `PREEMPT_TIMEOUT_US=640000`, run `apply`, then `verify`, before removing the
config. Restore must succeed before removing helper/state. Check values directly.
Disconnected devices cannot be restored in sysfs: retain their record until
reconnected/restored, or confirm they have been powered off so their next boot
uses kernel defaults with this rule removed. Do not delete unresolved records.

Remove only this worker drop-in and reload its manager; preserve other drop-ins:

```sh
rm ~/.config/systemd/user/myce-worker.service.d/70-xe-preempt-timeout.conf
systemctl --user daemon-reload
sudo rm /usr/local/libexec/myce-xe-preempt-timeout /etc/myce-xe-preempt-timeout.conf
sudo rm /var/lib/myce-xe-preempt-timeout/prior.json /var/lib/myce-xe-preempt-timeout/lock
sudo rmdir /var/lib/myce-xe-preempt-timeout
```

For a system worker remove the corresponding system drop-in and reload that
manager instead. If application was never configured and state does not exist,
skip its removal. Restore operator backups where applicable, then restart the
worker only if it was previously active; queues must be recreated to receive
the restored timeout. No driver reload or reboot is required to set the value.

## Laptop tests

```sh
python3 -B deploy/xe-preempt-timeout/test_xe_preempt_timeout.py
```

Run from the repository root. All subprocesses use temporary sysfs/config/state;
the tests never write to real sysfs, install files or contact a host.
