# Service templates

Follow [operations](../docs/operations.md) for install, start, stop, upgrade,
and recoverable uninstall on Linux/macOS. These templates have not been
installed on fleet machines. Linux files are **user** systemd units, not system
units. Replace every `REPLACE_*` field before installing. macOS's worker filename is `local.myce.worker.plist` and its label is
`local.myce.worker`; the queue is `local.myce.queue.plist` / `local.myce.queue`.

v2 binary: `~/.local/bin/myce`; local state: `~/.local/share/myce`; private
configuration: `~/.config/myce`. Do not overwrite or disable legacy `mycelium`
binaries, state, or services. Service definitions contain no real credentials.
Queue and worker NAS mount paths may differ while mapping the same shared root.
A checkpoint-root directory probe is not verification of the remote mount's identity.

The worker template is batch-only. For model serving add `--model-config`,
`--model-listen`, and `--model-advertise` with the matching catalog and reachable
proxy address; see the [model guide](../docs/guide-model-calls.md). Installing a
worker is not authorization to compete with an existing GPU owner.
