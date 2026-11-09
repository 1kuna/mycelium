# Optional xe CCS preemption timeout

This is an administrator-installed Linux host template, not an automatic
Mycelium installation or a recommended public tuning default. It requires
Python 3.9+, GNU coreutils `/usr/bin/timeout`, systemd-udev and xe's engine-class
sysfs interface. The shipped configuration is unset and performs no writes.
See [evidence and tradeoff](../../docs/host-probes.md#optional-xe-ccs-preemption-timeout).
Application requires a clean confirmation batch and separate authorization;
these instructions do not imply that the trial passed.

## Mechanism and isolation

There are two independent application paths:

- The udev `ACTION=="add"` rule matches xe primary DRM cards, including boot
  coldplug, driver rebind and hotplug. Its short `RUN` command uses GNU timeout:
  a 10-second deadline and two-second termination grace (12 seconds total,
  subject to OS scheduling). `--nonblocking-lock` refuses contention immediately
  with status 75 rather than waiting behind another application. The callback
  does not start a daemon or wait for the global udev queue.
- The separate `myce-xe-preempt-timeout.service` is a root `Type=oneshot`, enabled
  through `WantedBy=multi-user.target`. It applies and reads back every present
  xe CCS attribute at boot, with `TimeoutStartSec=12`, `TimeoutStopSec=2` and
  control-group termination. It runs after coldplug events are dispatched;
  devices appearing later are covered by the udev rule.

**Nothing is added to the worker unit's startup path.** There is no worker
ordering, dependency, pre-start hook, Python supervisor or udev-settle wait.
The value is inherited when a GPU **job** creates execution queues, well after
boot; it need not precede worker startup. Check successful application before
submitting trial GPU work. Existing execution queues keep their old value.
Failure or timeout of this optional service does not fail the worker or any
other service. The boot target only wants this unit; it does not require success.

Timeouts contain a failed diagnostic application; they cannot repair a hung
kernel or force a process out of uninterruptible D-state. Such a helper can
remain blocked after termination is requested. There is no dependency connecting
its lifecycle to the worker's. The timeout budgets prevent ordinary stalled
reads or lock waits from holding the diagnostic operation indefinitely, at the
cost of leaving tuning unapplied when the budget expires. They are explicit
operational budgets, not measured driver timings. If a callback skips a busy
lock, times out or fails, inspect the logs and reapply/verify before GPU work;
the boot scan is not a guarantee that a failed later hotplug has been tuned.

The helper discovers every xe primary card, then every tile/GT CCS class. It
only writes `engines/ccs/preempt_timeout_us`, using the live sibling
`preempt_timeout_min` / `preempt_timeout_max` (never `.defaults` bounds). It
validates all selected ranges before writing, reads back and logs each write,
and skips already-matching values. Bounds, `job_timeout_ms`, timeslices and
other engine classes are untouched. No xe/CCS device is a successful no-op.

`verify` is read-only. A mismatch logs `verification: mismatch, status=1` to
stderr and syslog and exits 1; read/config/range errors log status 2 and exit 2.
Apply/restore also log mismatch status 1 on failed write readback, and log their
completion status. Udev timeout diagnostics appear in the udev journal; service
timeouts appear in its own systemd journal. These are operator evidence, not
worker start gates. A failed multi-device application can leave completed writes
on earlier devices; their prior values remain in the rollback record.

Prior values are recorded before the first write in root-only
`/var/lib/myce-xe-preempt-timeout/prior.json`, keyed by PCI device and tile/GT,
not volatile card number. The record survives reboot and repeated application.
A lock serializes state updates and restore. Do not edit state or use another
tuning tool concurrently. Keep GPU queue creators quiescent during initial
application and rollback so new queues receive the intended value.

## Install after separate authorization

Drain GPU work through the normal maintenance procedure and stop queue creators.
Record all current CCS values and service states. Back up existing destination
files; do not overwrite another installation's state. Keep config/helper and
all parent directories root-owned and unwritable by job accounts.

From this directory (commands for a future authorized administrator):

```sh
sudo install -o root -g root -m 0755 myce-xe-preempt-timeout /usr/local/libexec/myce-xe-preempt-timeout
sudo install -o root -g root -m 0644 myce-xe-preempt-timeout.conf /etc/myce-xe-preempt-timeout.conf
```

Edit **only** `/etc/myce-xe-preempt-timeout.conf`: leave
`PREEMPT_TIMEOUT_US=` to preserve kernel/administrator settings, or explicitly
set `PREEMPT_TIMEOUT_US=1280000` only if that diagnostic trial value is approved.
Only unsigned decimal values and comments are accepted; this is not a shell
script. Removing the value stops future writes but does **not** undo existing
settings; use rollback below.

```sh
test -x /usr/bin/timeout
sudo install -o root -g root -m 0644 70-myce-xe-preempt-timeout.rules /etc/udev/rules.d/70-myce-xe-preempt-timeout.rules
sudo install -o root -g root -m 0644 myce-xe-preempt-timeout.service /etc/systemd/system/myce-xe-preempt-timeout.service
sudo udevadm control --reload-rules
sudo systemctl daemon-reload
sudo systemctl enable --now myce-xe-preempt-timeout.service
```

Starting this oneshot performs the initial application/readback; no synthetic
device event, worker unit change, user-manager reload or worker restart is
needed. Confirm success before resuming GPU job submissions. Native boot and
device-add proof require a separately authorized idle-host test.

## Verify and roll back

After installation and each authorized reboot/device-add test:

```sh
sudo /usr/bin/timeout --verbose --kill-after=2 10 /usr/local/libexec/myce-xe-preempt-timeout verify
grep -H . /sys/class/drm/card[0-9]*/device/tile*/gt*/engines/ccs/preempt_timeout_us
journalctl -t myce-xe-preempt-timeout
journalctl -u myce-xe-preempt-timeout.service -u systemd-udevd.service
systemctl status myce-xe-preempt-timeout.service
```

Confirm every xe CCS value, successful readback logs and exit status. A readback
proves the host setting, not workload stability or the value of existing queues.
Collect kernel reset evidence independently during the confirmation workload.
There is no worker telemetry change in this template.

To uninstall, quiesce GPU queue creators again. Remove the rule, reload udev,
disable/stop the optional unit, and let in-flight callbacks finish:

```sh
sudo rm /etc/udev/rules.d/70-myce-xe-preempt-timeout.rules
sudo udevadm control --reload-rules
sudo systemctl disable --now myce-xe-preempt-timeout.service
sudo udevadm settle --timeout=15
sudo /usr/bin/timeout --verbose --kill-after=2 10 /usr/local/libexec/myce-xe-preempt-timeout restore
```

Stop here on any failure/timeout; preserve helper/state and inspect the remaining
processes before retrying. Never remove the lock/state while a helper still uses
it. `restore` reads back and logs each recorded prior value (normally 640000).
If the trial value had already been set manually before first application, that
value is the recorded prior value. To explicitly return to 640000 instead, set
`PREEMPT_TIMEOUT_US=640000` and run the bounded `apply` and `verify` commands
before removing config. Check values directly; rollback must succeed first.

Disconnected devices cannot be restored in sysfs: retain their record until
reconnected/restored, or confirm they have been powered off so their next boot
uses kernel defaults with the rule removed. Do not delete unresolved records.
Once no helper is active and restoration is verified, remove only these files:

```sh
sudo rm /etc/systemd/system/myce-xe-preempt-timeout.service
sudo systemctl daemon-reload
sudo systemctl reset-failed myce-xe-preempt-timeout.service
sudo rm /usr/local/libexec/myce-xe-preempt-timeout /etc/myce-xe-preempt-timeout.conf
sudo rm /var/lib/myce-xe-preempt-timeout/prior.json /var/lib/myce-xe-preempt-timeout/lock
sudo rmdir /var/lib/myce-xe-preempt-timeout
```

If config was always unset or no xe device was present, state may not exist;
skip its removal. Restore operator backups where applicable, then resume GPU
work. Queues must be recreated to receive the restored timeout. No worker unit
changes, driver reload or reboot are needed to set or restore the value.

## Laptop tests

From the repository root:

```sh
python3 -B deploy/xe-preempt-timeout/test_xe_preempt_timeout.py
```

All helper invocations use temporary sysfs/config/state. The bounded-udev FIFO
regression runs the actual RUN command under GNU `timeout` (`gtimeout` also works
on macOS); it is explicitly skipped if coreutils is absent. Run it in a local
Linux VM to cover that path without installing anything on the development host.
Tests never write real sysfs, install services or contact fleet hosts. Laptop
proof does not qualify native xe boot/hotplug or uninterruptible driver stalls.
