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

The Linux queue template uses `PrivateDevices=yes` to hide GPUs, one Go CPU,
and initial limits of 25% CPU and 512 MiB RAM. Verify user-manager support and
GPU hiding before accepting the B70 queue; stop on unsupported sandboxing.
B70 is queue-only at `192.0.2.153:7331` for the first Spark acceptance test;
never enable a B70 worker as part of that test.
CUDA dedicated/auto inventory requires one selected device (or one device in
unselected output); use `join --device ID` and unique worker names on multi-GPU
hosts. Spark unsupported memory fields select
Linux `MemAvailable` automatically; missing/invalid OS telemetry blocks work.
[myce-spark-worker.service](myce-spark-worker.service) is the Spark example with
`--memory-mode auto --reserve-memory-gb 16`. Its zero cap uses the live reading;
choose a positive cap if the admitted workload needs a stricter allowance.
See [the production test plan](../docs/production-test-plan.md).
