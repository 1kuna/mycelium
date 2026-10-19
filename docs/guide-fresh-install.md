# Retiring RC state (#90)

Old formats are refused; operators archive and install fresh. There is no
in-place converter, recovery journal, or `myce host convert` command. The RC
installation's queued work was canary/test work, and is not imported. Production
that runs outside the queue stays under its existing owner's control.

Queue startup checks the persisted `queue.data` schema marker through read-only
SQLite access. Worker startup checks the small leading `schema_version` marker
in `state.json`, `operator-stop.json`, engine/attempt records, and saved
environment-build records. Host control checks old marker names
and registry headers. No legacy jobs, reports, receipts, or process records are
decoded or adopted. Refusal returns `legacy_state_present`, names the archive
paths, and instructs a fresh install. It does not archive, modify, or delete the
old state, including SQLite WAL files.

The operator must first stop the old queue, workers, and their controllers, and
prevent their old service definitions from restarting during the archive/install
window. Keep those services stopped until their matching new binaries and unit
definitions are installed. Moving data alone does not stop an old executable.

For a default Linux installation, archive these whole directories outside their
active lookup paths, preserving their contents:

- `~/.local/share/mycelium/queue/state/` (database and its side files, upload
  spool, joining identity, and other queue data).
- `~/.local/share/mycelium/worker-gpu0/state/` and
  `~/.local/share/mycelium/worker-gpu1/state/` (worker spools and caches).
- `~/.local/share/myce/` (shared host intent, old registry, flags and receipts).

An uninstanced worker uses `~/.local/share/mycelium/worker/state/`. Saved
`state_dir`, `db`, `host_state_dir`, `model_cache`, and `environment_cache`
values select the actual paths; archive any separately configured old data/cache
roots as well. A queue refusal names both its database and containing directory.
Keep checkpoints and unique evidence separately; a fresh queue does not adopt
old checkpoints or attempts. Do not remove any archive as part of this procedure.

Keep each role's `config.json`, binary/service locations, and private secret
references in place. Run `myce install queue --config <queue-config>` and
`myce install worker --config <worker-config>` for each worker. The WP7 overlay
reads the saved configuration first; omitted options preserve queue address,
NAS, name, Docker endpoint, environment-file reference, and all other saved
settings. Only explicitly supplied flags change configurable values. An RC
config without `docker_endpoint` keeps its empty value; specify `--docker-host`
explicitly to pin an endpoint. Provision credentials through the existing secret
mechanism; old JSON credentials are not converted.

`myce-worker@gpu0.service` and `myce-worker@gpu1.service` are still valid current
unit names. Install replaces each selected RC unit file at that same path with
the current definition, including `_host-start-check`, and reloads the manager.
It preserves the previous file through the WP7 installer transaction. No second
worker unit is created. Workers are enabled but never started by install; each
must be installed before the operator starts any worker. Current closed upgrades
do not open the host or renew its revision.

An empty fresh worker installation provisions revision 1 CLOSED, with no previous
custody to settle (`host_settlement: confirmed`). Surviving worker spool/cache
data prevents that empty-install confirmation. This is a statement about prior
Mycelium custody, not a privileged physical-exclusion PASS. Native exclusion and
readiness still require their normal observations. Both the unit condition and
service entry stay fenced while CLOSED.

Read `myce host status --local --config <worker-config> --json`. Require
`desired.state == "closed"` and `host_settlement == "confirmed"`. A separately
authorized open uses **`desired.revision`** from that envelope:
`myce host open --local --config <worker-config> --expect-revision <revision>`.
Status intentionally has `accepted_revision: null`; it is not an open revision.
A stale revision is refused. New work is admitted only after an explicit open
and the worker's current revision/session handshake. Native activation remains
a separate operator task; laptop fixtures do not authorize fleet changes.
