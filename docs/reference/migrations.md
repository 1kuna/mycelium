# Migration archive

Finished one-time migrations. Current installations do not need these steps; they
stay here for an installation that still has split per-device workers, and for
the rollback and Docker-retention steps that read the conversion journal. Git
history holds earlier retired procedures. For pre-v3 state use
[archive old state and install fresh](../operations.md#archive-old-state-and-install-fresh).

## Convert split workers to one machine worker (M4)

Follow the [accepted maintenance gates](#convert-split-workers-to-one-machine-worker-m4)
and [native qualification](../development.md#machine-worker-native-qualification).
Conversion is an operator-run maintenance action on the queue host and every
converted worker host, including the single-card worker in the same
window. Commands below use placeholders for approved installation paths and
accounts. Load authentication through the existing private secret mechanism;
never print a token or pass it on the command line.

Pause ordinary submissions and inspect the source queue's maintenance controls.
An older source may not have queue-owned host maintenance: use its confirmed
host-close/discharge route or the [verified-empty alternative](#verified-empty-conversion-and-publication).
On the source lineage supporting `host close --local --wait-released`, exit 1
with `release_not_confirmed` means re-check status; it is not permission to
release custody. Require confirmed settlement and zero custody, or independently
prove the verified-empty alternative's conditions. Record source identities,
sessions, unit enablement/activity, config and binary digests, device identities
and spool evidence. Stop the exact units and snapshot binaries (including any
root-owned binary separately), configs, units, registries and database. Upgrade
the stopped queue first; only
then use the new queue's host-maintenance commands below. Its one-time import
preserves closed `host_intents` as disabled maintenance; disagreeing per-worker
host memory allowances refuse migration with `machine_migration_conflict`.

Disable all affected physical hosts through the upgraded queue. Keep the
current binaries and a queue/database/config backup as well as the installer's
rollback journals. Require fresh reports and no attempts, preparations, engines,
buffered results, releases or queued/paused/waiting jobs resolved to retiring
names:

```sh
myce host disable HOST --stop --wait --timeout 5m
myce host disable SINGLE_CARD_HOST --stop --wait --timeout 5m
myce host status HOST
myce host status SINGLE_CARD_HOST
```

Stop the exact recorded queue unit before offline conversion. Use the new binary
from a staging location; do not copy it into old split worker installations.
Write an explicit ownership map in private storage, for example:

```json
[
  {"from":"HOST-gpu0","to":"HOST","device":"0"},
  {"from":"HOST-gpu1","to":"HOST","device":"1"},
  {"from":"SINGLE_CARD_HOST","to":"SINGLE_CARD_HOST","device":"0"}
]
```

Preview every before/after project, pin, server and host-policy change, then
commit it while the queue remains stopped:

```sh
systemctl --user stop QUEUE_UNIT
NEW_MYCE install queue --config QUEUE_CONFIG --machine-map PRIVATE_MAP --dry-run
NEW_MYCE install queue --config QUEUE_CONFIG --machine-map PRIVATE_MAP
NEW_MYCE install queue --config QUEUE_CONFIG --dry-run
NEW_MYCE install queue --config QUEUE_CONFIG
```

The last command installs the compatible queue binary and restarts its recorded
unit. Verify durable disables, job/key/retry/checkpoint/release state and the single-card worker's
legacy reconnect before changing workers. `machine_migration_queued_jobs` lists
queued, paused or waiting jobs to settle or cancel/resubmit through clients;
accepted specs are never rewritten. Unrestricted paused jobs survive conversion
and reopen with their pause metadata, submission key and original queue age.
Partial-card project fences and unrepresentable intersections refuse with
`machine_migration_conflict`. Reconcile those policies explicitly before retry.
A partial file publication blocks queue startup: rerun the identical offline
conversion or its queue rollback command. Never clear the database or journal.

On each worker host, pass a fresh destination config and **all** source configs:

```sh
NEW_MYCE install worker --config MACHINE_CONFIG \
  --from-worker-config GPU0_CONFIG --from-worker-config GPU1_CONFIG --dry-run
NEW_MYCE install worker --config MACHINE_CONFIG \
  --from-worker-config GPU0_CONFIG --from-worker-config GPU1_CONFIG
```

On the single-card host use one source, retaining its UUID selector:

```sh
NEW_MYCE install worker --config SINGLE_MACHINE_CONFIG \
  --from-worker-config SINGLE_SOURCE_CONFIG --dry-run
NEW_MYCE install worker --config SINGLE_MACHINE_CONFIG \
  --from-worker-config SINGLE_SOURCE_CONFIG
```

A single source with automatic device selection is resolved on that host using
the normal accelerator inventory. The preview shows the detected UUIDs and keeps
the source's memory settings for each device in its configured accelerator
family. Explicit source selectors are preserved without probing. An automatic
source cannot be combined with other source workers. Preview does not require
queue registration; installation still requires the source in the maintained
queue with the existing identity and settlement checks. For a source absent from
the queue, use the ordinary in-place install path and the
[fresh-state procedure](../operations.md#archive-old-state-and-install-fresh) when required; a successful
conversion preview does not authorize bypassing those checks.

The source manager stop must complete the same-name worker's normal leave.
Failure restores the previous install and keeps queue maintenance active.
Worker-local catalog placement fences are rewritten from explicit queue retirement
records, preserving local environment/secret values and original bytes for rollback.
The exact legacy single-card session may reconnect with its old catalog hash only while
disabled with no obligations; its model endpoint is withheld. New machine workers
still require the current catalog hash.
Source definitions are retired recoverably and persistent wants links removed;
spools, binaries and all caches remain intact. The gpu0 model/environment caches
and limits remain the sole machine caches unless the explicit migration below is
selected. Per-device selectors, fixed capacities,
reserves, shared roots, secret references and the host allowance are retained.
For a new conversion, add `--machine-cache-root` to move generated per-device
cache settings to `MACHINE_STATE/model-cache` and
`MACHINE_STATE/environment-cache`. For an already converted worker, use
`myce install worker --config MACHINE_CONFIG --machine-cache-root --dry-run`,
then repeat without `--dry-run` during an authorized maintenance window.
The saved conversion journal identifies generated source defaults; custom paths
are preserved unless selected with `--model-cache` or `--environment-cache`.
The preview shows both paths, retained allocated bytes and available disk space.
Installation requires a disabled or drained, idle, Healthy host. It uses the
normal saved-config overlay and install rollback, including same-binary
reconfiguration. Limits stay unchanged, old directories remain intact, and
environments at the new root will be prepared again. An ordinary upgrade keeps
saved cache paths; this option does not copy or delete cache contents. Keep the
journal and old directories for rollback; cleanup needs separate authorization.

Shared-field conflicts require a named explicit installer flag; budgets are
never added. Conversion reads a legacy source's persisted
`host_state_dir/host-state.json` allowance before comparing shared settings;
that effective value wins over stale installer flags. Missing/invalid allowance
in a local-control source refuses with `machine_memory_allowance_unavailable`;
use an explicit `--host-memory-budget-gb` only after establishing the intended
allowance. Sources without retired local-control configuration retain their
saved queue bootstrap value. Existing queue-owned policy remains authoritative
and is never replaced by later worker reports. The private
`MACHINE_CONFIG.m4-rollback.json` journal contains exact
original file bytes/modes and unit registration/activity. Preserve it securely.
Interrupted worker conversion requires its rollback command before retry.

Inspect effective manager configuration, exactly one worker unit/process, device
inventory and maintenance state. During the agreed test phases explicitly resume
hosts, run the entire native checklist, then disable/stop/wait before further
maintenance. Installer success does not qualify hardware. An ordinary `install
worker --config MACHINE_CONFIG` redeploy preserves the converted device settings;
repeating the completed conversion is idempotent. An old split config or an
unselected split unit fails the deploy gate with `machine_worker_conflict` or
`machine_migration_incomplete`.

### Verified-empty conversion and publication

This is an explicit alternative to persisted source-session close/discharge
receipts for supported split installations, never a normal admission shortcut.
Freeze producers, stop source user units, disable mapped hosts, and stop the
queue before its offline exclusive-lock conversion. Supply every mapped source
config, including a single-worker host:

```sh
myce install queue --config QUEUE_CONFIG --machine-map PRIVATE_MAP \
  --drain-evidence=verified-empty \
  --from-worker-config SOURCE_0 --from-worker-config SOURCE_1 --dry-run
```

After reviewing the plan, repeat without `--dry-run`. The local systemd user
manager and source spools provide the evidence; copied remote attestations do
not authorize conversion. The supported locked database must be readable and
contain zero nonterminal jobs (including queued, paused, waiting, running,
stopping and preparing jobs, and retained submission records), plus zero
mapped-host releases/obligations. Pausing work does not make a queue empty; age
never proves discharge.

For each exact source/session, require the loaded unit identity/fragment,
`ActiveState=inactive`, `MainPID=0`, no pending unit job and empty cgroup-v2
`cgroup.procs` throughout its subtree. If systemd cleared `ControlGroup`, derive
the normal path from its reported containing slice and unit name; an absent
subtree requires a readable containing slice. Hold the existing `worker.lock`
through conversion/publication. Require a marked v3 spool and exact saved session.
Use the read-only supported sparse-spool inspector; no cleanup or inferred ack.

Retained results/released Docker records are settled history only when the locked
queue confirms the exact job/attempt/worker/session release (or that superseded
copy's own release). A retention ack, newer attempt or terminal state alone is
insufficient. At each configured Docker endpoint, including the explicit local
default when Docker was not configured, successful `docker ps` must show zero
running containers with the source `myce.worker` label and zero with any
`myce.attempt` label. Disabling future Docker use does not waive this check.
Unknown/unreadable evidence refuses. Blocking spool files retain individual reasons.

Typed refusals distinguish `machine_drain_queue_not_empty`,
`machine_drain_obligations_pending`, `machine_drain_unit_active`,
`machine_drain_unit_unreadable`, `machine_drain_cgroup_populated`,
`machine_drain_cgroup_unreadable`, `machine_drain_spool_unsettled`,
`machine_drain_evidence_missing`, `machine_drain_queue_unreadable`,
`machine_drain_docker_inaccessible` and `machine_drain_container_running`.
Preview/refusal persists no receipt. Successful retirement atomically records
`verified_empty_drains`: injected-Clock time, exact machine/host/session, inactive
unit/substate/PID, cgroup evidence, sorted length-delimited top-level spool/format
SHA-256, Docker observations and six zero queue/custody counts. These receipts
are installation history, never reusable health/admission authority. They can
retire a same-name source to Gone without inventing a previous-session chain.

Queue retirement commits a pending-publication fence; startup refuses partial
policy publication. Identical-command retry finishes files or the journal
restores them, never a database snapshot. Unknown policy edits or mode/content
changes block with `machine_migration_conflict`. Project policies cannot express
partial-card allowlists and must refuse widening; a pin/single-host server can
retain its card restriction. Queue conversion records only model/placement
identities and file hashes, never model secret/environment values. Private
mode-0600 journals retain exact bytes, modes and manager state; referenced secrets
remain references. Retire only matching selected source-unit/config/spool/host/
queue enable records, including supported `worker-*.json` records. Unknown or
unselected split units block ordinary redeploy as well as conversion. Write-ahead
receipts cover each publication and preserve original modes.

### Additional rollback fences

Worker rollback inspects both source and reused destination spools, exact
process/cgroup/session custody, buffered results and Docker records before any
manager mutation. Dry rollback is read-only; real restoration holds the exclusive
spool lock after stop. Unknown changed file/mode blocks restoration; unchanged
shared catalogs are not rewritten. Interrupted plans use journaled source identity;
a missing destination unit is valid rollback input. CLI cancellation invokes
bounded restoration independently of the cancelled operation. A reused machine
spool survives forward/rollback/forward with its historical custody intact.

Queue rollback refuses any queued/failed/cancelled machine-era allowlist intent
that would disappear, including resumable work; cancellation alone cannot clear
that fence. Succeeded history remains exact. Representable pin additions/removals
and preemption counters survive; unrepresentable host-wide pins require an
explicit card/removal decision. Retired source sessions may report exact late
custody but cannot revive themselves. Fresh evidence is required for a new
forward conversion after rollback; committed publication retry uses its existing
conversion receipt.

A full production-snapshot abort is stricter than worker/topology rollback:
require no accepted new business intent, stop all new writers, archive synthetic
probe evidence, restore the entire pre-window DB/config/registry/enablement and
each separately hashed binary. Never open mutated state with an old binary or
overwrite newer checkpoints. Otherwise remain disabled and repair forward.
Verify actual legacy reconnect/refusal, not merely wire compatibility.
`RestartPreventExitStatus=78` contains only an actual exit 78, not a retrying HTTP
refusal. Preserve checkpoint IDs, attempts and highest complete generations;
missing accepted progress holds resumable work rather than restarting it empty.

### Roll back without losing accepted intent

Pause ordinary submissions and disable/stop/wait on both hosts. Require all new
attempts, group members, engines, results and releases settled. Restore worker
installations first while maintenance remains active, then restore queue fences
and legacy identities offline. This ordering allows the upgraded queue to verify
exact cleanup before the worker stop. Early legacy reconnects remain refused
until queue rollback finishes; no ordinary admission is enabled.

```sh
NEW_MYCE install worker --config MACHINE_CONFIG --rollback-machine --dry-run
NEW_MYCE install worker --config MACHINE_CONFIG --rollback-machine
NEW_MYCE install worker --config SINGLE_MACHINE_CONFIG --rollback-machine
systemctl --user stop QUEUE_UNIT
NEW_MYCE install queue --config QUEUE_CONFIG --rollback-machines --dry-run
NEW_MYCE install queue --config QUEUE_CONFIG --rollback-machines
systemctl --user start QUEUE_UNIT
```

Keep the upgraded queue binary/database, all new jobs/keys/counters and accepted
checkpoint generations. The journal restores source configs, units, enablement
and verified prior activity; never merge spools. Current pin additions/removals
and counters survive when their card restriction is representable. A new
host-wide pin or an unknown policy edit blocks restoration until explicitly
reconciled, preserving that intent. Unknown cleanup/restoration keeps admission
disabled. Verify both source workers and the single-card worker reconnect, device isolation,
secret/root custody and consumer health before the operator accepts rollback.
Forward conversion can be repeated under the same maintenance gates.

## Migrate storage guards to automatic admission

The unfinished migration is parked on [`park/storage-protection`](https://github.com/1kuna/mycelium/tree/park/storage-protection).
`myce file admission` and its activation/rollback endpoints are removed from main.
Retained activation metadata cannot disable configured caps/reserve/floors.
See [the current storage behavior](file-store.md#parked-native-storage-integration-564) and
[#504](https://github.com/1kuna/mycelium/issues/504) for the backend qualification
and preservation requirements before any future replacement.
