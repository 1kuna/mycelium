# Service templates

Follow [operations](../docs/operations.md) for install, start, stop, upgrade,
and recoverable uninstall on Linux/macOS. These templates have not been
installed on fleet machines. Linux files are **user** systemd units, not system
units. Replace every `REPLACE_*` field before installing. macOS's worker filename is `local.myce.worker.plist` and its label is
`local.myce.worker`; the queue is `local.myce.queue.plist` / `local.myce.queue`.

v2 binary: `~/.local/bin/myce`; local state: `~/.local/share/myce`; private
configuration: `~/.config/myce`. These are manual-template paths; `myce install`
instead uses `~/.local/share/mycelium/ROLE` and macOS `local.mycelium.ROLE` labels.
Do not overwrite or disable legacy `mycelium`
binaries, state, or services. Service definitions contain no real credentials.
Queue and worker NAS mount paths may differ while mapping the same shared root.
A checkpoint-root directory probe is not verification of the remote mount's identity.

The worker template is batch-only. For model serving add `--model-config`,
`--model-listen`, and `--model-advertise` with the matching catalog and reachable
proxy address; see the [model guide](../docs/guide-model-calls.md). Installing a
worker is not authorization to compete with an existing GPU owner.

The Linux user queue uses `PrivateDevices=no`, `NoNewPrivileges=yes`, one Go CPU,
and initial limits of 25% CPU and 512 MiB RAM. Device namespaces (even with
`PrivateUsers=yes`) can fail with 218/CAPABILITIES in unprivileged user managers.
The queue never opens GPU devices and refuses inherited GPU descriptors at
startup; this is not an OS device-access sandbox. Verify it holds no GPU
descriptors before accepting a queue. Worker units retain GPU access and
`Delegate=memory`.
B70 is queue-only at `192.168.0.153:7331` for the first Spark acceptance test;
never enable a B70 worker as part of that test.
CUDA dedicated/auto inventory requires one selected device (or one device in
unselected output); use `join --device ID` and unique worker names on multi-GPU
hosts. Spark unsupported memory fields select
Linux `MemAvailable` automatically; missing/invalid OS telemetry blocks work.
[myce-spark-worker.service](myce-spark-worker.service) is the Spark example with
`--memory-mode auto --reserve-memory-gb 16`. Its zero cap uses the live reading;
choose a positive cap if the admitted workload needs a stricter allowance.
See [the production test plan](../docs/production-test-plan.md).

Linux manual worker templates load `worker.env`. Include only the GPU runtime
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
