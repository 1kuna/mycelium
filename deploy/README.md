# Service templates

Prefer [generated service installation](../docs/operations.md#install-and-upgrade).
If rendering a manual definition, replace every placeholder and qualify the
actual service context before activation. Use one definition per role and state
directory. Linux main services run in the user manager. macOS templates use
`local.myce.queue` and `local.myce.worker`; generated units use
`local.mycelium.ROLE`. Keep credentials in private files, outside the units.

The Linux worker template uses `service --config` with an installed worker
configuration. Keep only required runtime values in the private GPU environment
file, such as an approved library location or oneAPI controls. Verify device
detection under the service manager. The generated installer captures this
environment and validates it in the manager context. See
[native worker setup](../docs/operations.md#install-and-upgrade) and
[host capacity](../docs/reference/host-capacity.md).

The manual queue template carries explicit legacy loss-time minima and a v2
description. For the current defaults use the installer. The queue verifies
it has no inherited GPU descriptors. Linux user managers may reject private
device namespaces, including with `PrivateUsers=yes`.
Keep queue CPU scheduling independent of job-execution politeness.

Before cutover, use the [backup and restore procedure](../docs/operations.md#queue-backup-restore-and-relocation)
and collect complete [preservation evidence](../docs/operations.md#preserve-accepted-work-evidence-across-a-release).
Verify the NAS mount identity when configuring a shared root. A directory
probe alone cannot establish it.

- [Read-only GPU telemetry broker](gpu-telemetry/README.md) runs as a separate
  root system service with an unprivileged client.
- [Optional xe preemption timeout](xe-preempt-timeout/README.md) has its own
  authorization, verification and rollback procedure.
- [Ephemeral core routing](../docs/operations.md#linux-core-routing-for-ephemeral-attempts)
  covers the crash router and its custody requirements.
