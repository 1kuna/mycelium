# Offline persisted-state conversion

The current binary refuses old queue/spool formats and old worker protocol
clients. Upgrade the queue and every worker together; HTTP 426 means the worker
must be upgraded. This procedure prepares a **CLOSED** conversion bundle. It
never stops services, contacts queues/Docker, starts work, or installs the bundle.
Live inventory and deployment require their own authorization.

Use a stopped, private local copy, with the old processes and service restart
sources stopped by their owner. Keep the original binary/configuration and a
WAL-consistent backup. Do not copy a running SQLite database without its WAL:
use SQLite's backup facility, or copy the database and its sidecars after the
writer has stopped. Existing queue, worker and controller lock files are checked,
but locks cannot prove that an omitted service or external daemon is stopped.

The source directory has these explicit locations:

- `queue.db`, with any `queue.db-wal`/`queue.db-shm` and lock files;
- `workers/<label>/`, one complete spool per worker, including results, logs,
  process records, Docker custody and local checkpoint payloads;
- `host/`, the complete shared host-control directory, including per-unit state;
- `checkpoints/<job>/attempt-<n>/`, any NAS checkpoint copies being qualified;
- optional install directories containing paired `config.json` and old JSON
  `credentials.json` files.

Include every referenced spool and custody record. Do not include unrelated
caches or filesystem trees. No symlinks, devices or sockets are accepted. The
converter never follows absolute paths found inside records. A registry maps to
exactly one included spool through its saved session; missing, conflicting or
ambiguous mappings block publication. Missing owner/session/engine attempt or
load deadline, unsupported schema and unsequenced saves also block publication.
No timestamp, filename ordering or invented generation resolves these blockers.

Run as the intended service account, with an explicit physical host identity and a new target directory. Its
parent must exist on storage with room for staging and preserved originals:

```sh
myce state convert --source ./offline-source --target ./converted \
  --host-id example-host --dry-run
myce state convert --source ./offline-source --target ./converted \
  --host-id example-host --apply
```

These commands run before credential loading. JSON output is private operational
evidence: retain it locally, including blockers, table counts and hashes. It is
not suitable for posting publicly. Source files are never edited. The target
retains their original bytes under `.conversion/originals/`, with original modes
recorded in `.conversion/manifest.json`; backup permissions are restricted.
The copied SQLite database is integrity-checked, updated transactionally, fully
checkpointed, then closed. Logs, usage, jobs, attempts, charges and results stay
intact. Retired control maps remain historical JSON, and retired receipts and
registries remain under `.conversion/retired-host/`. Exact receipt executions
become pending obligations. No historical receipt becomes local release proof.

Every saved queue host and the local host are fenced CLOSED with new revisions.
The local revision exceeds the saved counters. Local release and queue settlement
start unconfirmed. Registry spool paths point inside the published target;
old commands, installed-config pointers and cache pointers stay inert in backup.
Current installer/worker registration must restore the installed configuration
bindings before rest/ready can pass. Queue and worker binaries must use this same
protocol. An explicit open operation after fresh reconciliation is required.

The final publication is one same-filesystem directory rename after file and
directory syncs and a complete source recheck. A sibling `.conversion.lock`
serializes converters. Dry runs and failures retain private `.myce-convert-*`
staging directories beside the target; they are unpublished evidence, not active
state. Keep them until their owner has verified they are unused and disposable.
To recover from interruption, rerun the same command with the same immutable
source and target. Before publication it builds another stage; after publication
it verifies source, original and converted hashes plus CLOSED state and returns
the completed manifest. A modified/used target is refused, never overwritten.

The target is a validated bundle, not an installation switch. Before starting
services, the install owner must bind the queue DB, host-state directory, every
worker spool, credentials, checkpoint roots and required-worker configuration to
the intended converted locations. Keep NAS-relative job assignments and each
worker's configured NAS root consistent. Do not start a second queue or restart
an old worker against either copy. Confirm counts and exact release custody from
the native stopped backup, then start the matching queue CLOSED and reconcile.
A blocked conversion leaves the original version's state intact; do not reset,
cancel jobs, edit rows, or delete checkpoints to bypass it. Preserve the source
and bring the blocker to its owner.
