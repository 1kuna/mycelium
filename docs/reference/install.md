# Installer transaction details

What `myce install queue|worker` does beyond the
[install and upgrade procedure](../operations.md#install-and-upgrade): live-slot
reconciliation, the rollback snapshot and service-manager timeouts.

For an existing fleet-capable worker, installation compares the live process
proof with the saved executable slot before taking its rollback snapshot. If
publication succeeded but activation failed, the two hashes can differ. Only a
slot matching the current queue-approved release's signed target, size and hash
qualifies automatically; the provisioned publisher key still verifies it. The
qualified **slot bytes** become the rollback target, even when the old process is
still running. The private `slot-reconciliation.json` receipt retains both
identities and this choice. An activation failure restores those slot bytes;
it does not promise to recreate the old process image. Existing custody,
maintenance, native signature and service checks still apply. A stopped service
has no live image to reconcile; legacy installs without fleet proof keep their
existing path.

Unknown or unapproved slot bytes refuse with `fleet_slot_unapproved`, naming the
running version/hash and the slot hash before any install publication or restart.
Verify and approve the slot release through the ordinary release workflow, or
have the authorized owner recover the installation; do not override the refusal.
`myce status` and host status JSON expose `installed_slot`, `slot_mismatch` and
`slot_error` in the worker fleet proof before any install is attempted. An old
worker must first support these observations to report them.

The transaction snapshots binary, config, credentials, GPU environment and
service files/modes plus actual manager registration/activity. Failure restores
that snapshot and verifies it independently. Incomplete rollback requests affected
services stopped and reports remaining differences; preserve the snapshot for
recovery. Effective drop-ins that override execution, credentials or containment block activation,
except queue `ExecStart` overrides: these are preserved with a warning so their
settings can be migrated before removal. Consumer files are preserved. Check
health and exact cleanup before resuming.

From the next release (#1081), Linux installer stop/start/restart waits read the
unit's effective `TimeoutStopUSec` and `TimeoutStartUSec` from systemd, including
drop-ins. Starts/restarts allow both a pending stop and the start phase, plus two
measured manager round trips for dispatch/completion. Zero/infinite manager
timeouts retain operator cancellation without an artificial client deadline.
Unreadable, missing or malformed timeouts log a warning and also leave the wait
to systemd, retaining caller cancellation without a guessed client deadline.
An unavailable manager deadline can therefore leave the installer waiting until
systemd completes the job or the operator cancels it.

Darwin lifecycle commands (`bootout`, `bootstrap`, `kickstart` and `kill`) and
their label/activity observations wait for launchd completion with caller
cancellation, without an additional installer deadline. The loaded job's
`exit timeout` only bounds its process's termination grace; it does not bound
launchd's transaction, filesystem I/O, label removal or activation. A measured
`print` round trip cannot bound those later operations either. Ordinary setup
probes retain their setup allowance. A slow or stuck launchd transaction can
therefore keep the installer waiting until it completes or the operator cancels.
After bootout, installation observes label removal before reusing the label.
After bootstrap or kickstart, it observes a running service rather than treating
the command acknowledgement as activation. An exited or unloaded job fails
activation and triggers rollback.

Recovery ignores the failed install's cancellation and restores files before
reloading the manager. An interrupted Darwin recovery command is reconciled
through a fresh manager inspection: unload again if still loaded, bootstrap if
absent, activate if not running. It does not restart an already active restored
service. A repeated interruption or another manager error is reported with the
failed recovery phase and final snapshot differences; successful file restores
alone never count as a complete rollback. If recovery cannot verify the snapshot,
it requests affected services stopped and reports any stop failure too, without
claiming an unverified stopped state. Recovery has no artificial total deadline;
a pending launchd operation can keep it waiting. Full release acceptance still
requires a native install on a quiescent host.
