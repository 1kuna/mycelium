# Service templates

> **Before queue cutover:** migrate preservation checks, including private
> release scripts, to `/api/status?detail=full` or complete `jobs=all` pagination.
> Bounded default status (#439) omits older completed jobs. Existing complete
> `/api/inspection` collectors remain supported; see
> [rolling fleet upgrades](../docs/operations.md#rolling-fleet-upgrades).

The optional [xe GPU telemetry broker](gpu-telemetry/README.md) is a **root system
service**, with a read-only unprivileged helper for workers retaining
`NoNewPrivileges=yes`. It is installed separately from the user units below.

Follow [operations](../docs/operations.md) for install, start, stop, upgrade,
and recoverable uninstall on Linux/macOS. Qualify each rendered definition before native activation. Linux files are **user** systemd units, not system
units. Replace every `REPLACE_*` field before installing. macOS's worker filename is `local.myce.worker.plist` and its label is
`local.myce.worker`; the queue is `local.myce.queue.plist` / `local.myce.queue`.

For a generated install, role binary/config/state default to
`~/.local/share/mycelium/ROLE`; macOS labels use `local.mycelium.ROLE`.
Manual templates require explicit substituted paths and use `local.myce.*`
macOS labels. Pick one definition per role; never run both against the same
state. Service definitions contain placeholders, not credentials.
[Installer overlay and rollback](../docs/operations.md#install-and-upgrade)
preserve omitted settings, artifacts and service state. Nonempty old state
without `.myce-format` returns `legacy_state_present`: follow
[archive plus fresh install](../docs/guide-fresh-install.md), preserving local data.

Queue and worker NAS mount paths may differ while mapping the same shared root.
A checkpoint-root directory probe is not verification of the remote mount's identity.

The Linux worker templates invoke the same installed binary's `service --config`
entry, matching generated units. Replace binary and config placeholders before
use. Queue maintenance controls admission. There is no local start condition or
reconciliation timer; native service activation remains an authorized operation.

The Linux user queue uses `PrivateDevices=no`, `NoNewPrivileges=yes`, normal
OS/Go CPU scheduling, and an initial 512 MiB RAM limit. It has no default CPU
quota or reduced process priority: control transactions and worker heartbeats
must not inherit job-execution politeness limits. The installer retires its exact
old `Nice=10` / `CPUQuota=25%` default when reading a saved queue config, so a
normal upgrade also repairs that configuration. Explicitly different operator
CPU settings remain preserved. macOS queue plists likewise have no execution
politeness settings. Device namespaces (even with
`PrivateUsers=yes`) can fail with 218/CAPABILITIES in unprivileged user managers.
The queue never opens GPU devices and refuses inherited GPU descriptors at
startup; this is not an OS device-access sandbox. Verify it holds no GPU
descriptors before accepting a queue. Worker units retain GPU access and
`Delegate=memory cpu`.
CUDA dedicated/auto inventory requires one selected device (or one device in
unselected output); use a configured device and unique worker names on multi-GPU
hosts. Unified-memory workers use OS telemetry; missing/invalid telemetry blocks
work. Qualify memory policy independently of installation.
See [the production test plan](../docs/production-test-plan.md).

Linux manual worker templates load the selected `gpu.env` file. Include only the GPU runtime
variables your foreground detection needs (for example `LD_LIBRARY_PATH` and
recognized Level Zero/oneAPI/SYCL controls), and qualify detection under the user
manager before starting. `myce install worker` automates the separate private
`gpu.env` capture and transient-service validation, including per-card instances;
see `docs/operations.md`. Do not copy the whole shell environment into a unit.

Linux installer EnvironmentFile and ConditionPathExists paths are unquoted;
paths containing whitespace, quotes or backslashes are rejected. ExecStart
arguments remain quoted. `--private-devices=true` is rejected for Linux user
queues; the default uses the startup GPU-descriptor check described above.
Installer linger advice reads loginctl and requests enable-linger only when off.
