# Operations reference

How Mycelium's operator surfaces behave. Procedures stay in the
[runbook](../operations.md); each page here explains one area and links back to
its procedure.

| Page | Covers |
| --- | --- |
| [Queue maintenance](queue-maintenance.md) | Exclusions, device maintenance, last-resort placement, drain semantics, control timing, queue diagnostics and profiling, validation reservations |
| [Fleet upgrades](fleet-upgrades.md) | Host order, how a service verifies and installs a release, health checks, failure codes and repairs |
| [Installer transactions](install.md) | Slot reconciliation, rollback snapshot, service-manager timeouts |
| [Onboarding and joining](onboarding.md) | Automatic joining, invitation files, NAS selection, build tools, discovery and networking, capability profiles, invite-line upgrades |
| [NAS mounts](nas-mounts.md) | Linux CIFS boot integration, worker mount fault detection and recovery |
| [Host RAM and CPU capacity](host-capacity.md) | Host RAM admission and ceilings, privileged GPU telemetry, CPU allowances |
| [Accelerator quarantine](accelerator-quarantine.md) | Driver fault detection, quarantine and clearing |
| [Native execution](native-execution.md) | Sandbox default and the unsandboxed exception |
| [Queue file store](file-store.md) | Job file retention, production reserve, per-project quotas, early release |
| [Worker caches](worker-caches.md) | Worker scratch retention, file and environment caches, environment catalog |
| [Models](models.md) | Model storage, servers, joint measurement, engine interval history |
| [Shared and polite hosts](shared-and-polite-hosts.md) | Daily-use hyphae, OS owner signals, shared interactive host profile |
| [Queue disk I/O](queue-io.md) | Disk contention evidence, SQLite limits, systemd I/O weights |
| [Completion observer](completion-observer.md) | Observer supervision, upgrade, legacy recovery, delivery acceptance |
| [Migration archive](migrations.md) | Retired migration policy and parked storage guards |

The [release floor](version-floor.md) records the supported adjacent pair and the exact retained contracts.
