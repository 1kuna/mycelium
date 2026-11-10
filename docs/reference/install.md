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
that snapshot and verifies it independently. Incomplete rollback stops affected
services and reports differences; preserve the snapshot for recovery. Effective
drop-ins that override execution, credentials or containment block activation,
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
systemd completes the job or the operator cancels it. This fallback is Linux-only;
launchd stop waits read the loaded job's effective `exit timeout` from
`launchctl print`, including the system-defined default when the plist omits
`ExitTimeOut`. Bootout and subsequent label-removal observation share that exit
allowance, two measured manager round trips and one observation tick. Unknown
or infinite exit timeouts leave the wait to launchd with caller cancellation;
unknown values log a warning. Other launchd commands retain their ordinary
setup probe allowance. Recovery ignores the
failed install's cancellation, gives each command its own fresh allowance, and
inspects final manager state with an unexpired context. A failed manager itself
can still prevent inspection; that error is reported rather than claiming a
known state.
