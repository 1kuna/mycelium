# Design deployment history

Historical material moved from the approved product spec on 2026-10-28 under
MY-01. It records deployment plans and evidence, not current product requirements
or current qualification. Private deployment host names are replaced with generic
machine IDs; all other excerpt text is preserved verbatim.

## Retry agreement

**Production infrastructure retries (#80, agreed with Verso 2026-10-19).** An interrupted production batch job requeues at the front of its priority line under the same job ID and `submission_key`, at most **2** times (`queue --production-infrastructure-retries`, a nonnegative limit clamped to two; zero disables requeue). Worker restart or loss, an unreported attempt after queue restart, undelivered/lost results, worker interruption/release, planned leave and reboot all use the existing requeue path. A queue restart that reconnects a still-current running command or its buffered result does not interrupt it. The third interruption fails with `infrastructure_retries_exhausted` and a terminal timestamp. The durable job counter `infrastructure_retries` records requeues used and is an additive optional integer in status and job-v1 (omitted at zero). It survives restart; release evidence cannot refund or revive an exhausted production budget.

Classification uses Mycelium-recorded facts for native, native-group and Docker attempts. Infrastructure evidence is a durably recorded stop or kill delivered to that member before its exit was observed; loss of observation; a Mycelium-side pre-execution or launch refusal/failure (including authorization, exec/spawn or environment preparation infrastructure failure); or Mycelium's own probe proving that an allocated readiness port belongs to a process outside this group when readiness fails. Exit codes, payload stderr, stop intent and signals recorded after exit observation are never infrastructure evidence. Independent member exits, OOM (including an extension job), and readiness timeouts without such evidence fail production immediately without an infrastructure retry. A successful terminal outcome is preserved. Cancellation takes precedence: once `will_not_run: true` is acknowledged, no interruption can requeue the job. Superseded production attempts cannot recover ownership or publish a late result; exact release evidence can still discharge their custody. At most one attempt's result is published. Commands and output writes must still be safe to repeat; external side effects are not exactly-once. Experiments retain their existing retry, checkpoint recovery and planned-stop behavior.

A production attempt that had started gets its original-length start window from interruption detection, including a worker interruption/release. Planned leave keeps its original start deadline. A requeued job may wait for its maintained host to resume within that window; a new production submission to only disabled eligible hosts still fails immediately. Unstarted attempts do not renew their start deadline.

## Production migration context

**Pre-v3 formats are refused; operators archive and install fresh.** Pre-marker queue state is not imported. Stop the old services and prevent their restart, archive their queue/worker data directories and shared host-control directory, preserve configuration and secret references, then install matching current binaries and units. All current queue, worker and cache state roots carry `.myce-format` with `v3` and a newline. One startup gate checks every root before any filesystem write: absent or empty roots receive a fsynced marker first; non-empty roots without that exact marker return `legacy_state_present` naming the root. No content-specific legacy detection or migration of pre-marker development state is supported. There is no pre-v3 converter or dual runtime decoder. The separately gated M4 conversion below starts from production's marked v3 state. Preserve archives, checkpoints and unique evidence; this rule does not authorize deleting them or changing production outside the queue.

Installation overlays explicit options on saved current configuration, preserving
queue address, NAS, names, Docker endpoint, GPU environment and secret references.
Matching binaries and service definitions replace each selected installation.
There is no local start condition, stage-only host gate or timer installation.
Linux workers start as ordinary-account services with `Restart=on-failure`,
`Delegate=memory`, `KillMode=control-group` and bounded stop. Installer rollback
restores files, modes and actual manager registration/activity, and reports any
incomplete restoration. Use queue maintenance to keep admission disabled across
an upgrade. Saved configurations containing removed local-control fields must
be rendered in the current schema; preserve the old config and secret references.

## Machine-worker transition context

## One machine, one worker (M0–M4, #100; decision 2026-10-20)

The **physical machine is the hypha**. One worker process owns its GPU device
inventory, logical CPU cores, host RAM and, later, other components. maintainer rejected
one Mycelium instance per GPU. This amendment defines package M; R1, P3 and
D5 are its implementation base. M0 is the approved target, not a claim that
M2–M4 or native qualification have shipped. Until migration, existing
per-GPU installations use the compatibility rules below. This section governs
that transition where older guides describe per-GPU workers.


## Compatibility and consumer gates

### Reliability, holds and compatibility

Keep the job-v1 endpoints, states, project/submission-key replay, cancellation
and optional field omission rules. Store the original client spec/fingerprint
unchanged; validate new placement separately. `existingSubmission` returns
before new-submission validation, so an existing key replay across migration
returns the same logical job even when its original spec names retired workers.
`machine` is the actual executing machine string (`machine-a` after migration); never synthesize a per-GPU worker identity in a new
assignment. Add diagnostic `allocated_devices: ["0"]` to the assignment/job and
record that selection durably on every copy/release. It is empty for CPU work.
A client must not derive device identity from the machine string. Regenerate
the job-v1 schema/examples in M3; existing clients need no new required fields.

Keep production takeover/hold keyed by physical host, optional and default off.
When enabled it fences lower-tier work across **all** devices and CPU/RAM on
that host, and waits for the same exact cleanup before production admission.
A free neighboring GPU does not bypass it. Without takeover, fit-based P3
preemption remains normal. Queue drain/disable outranks placement, preparation,
pins, holds and Started; a hold cannot resume a maintained host. No cooldown or
allowance rewrite is introduced.

Cancellation still wins over requeue; independently observed exit/OOM and
completion retain D5 precedence over mere stop intent. Queue-recorded
`planned_host_maintenance` takes precedence over priority preemption for the
same attempt; neither consumes infrastructure retries/losses. Preserve the
first bounded stop deadline, cause provenance and original start-window rules.
For migration/redeploy/reboot, record maintenance **before** stopping workers.
Ordinary leave/loss/restart without that queue-recorded cause retains the
existing infrastructure classification and production limit of **at most two**
requeues. Do not reset counters or conceal independent stage failure. Exact
release gates resource reuse; only the current accepted attempt publishes a
result. Previous-session cleanup can discharge only the exact custody it proves,
not all work ever associated with a machine.

Compatibility choices are deliberately confined to existing interfaces:

- **Retired device names fail clearly.** M4 rewrites queue-side project policy,
  pins, server restrictions and host policies to actual machine IDs plus any
  device restriction; there is no alias table or `aliases` config. Retired
  legacy machine records remain solely for exact late-result rejection and
  release, and name their replacement machine/device for diagnostics. A new
  submission or policy/pin/server update referencing `machine-a-gpu0` after retirement
  returns HTTP 400 `machine_renamed`, naming the replacement:
  `allowed_machines: [machine-a]` plus `needs.gpu.devices: ["0"]` for a job (or that
  interface's device selector). Config load reports the same typed reason.
  Do not parse arbitrary `-gpuN` suffixes or resolve retired names as aliases.
  Existing raw specs, history and submission fingerprints stay intact; keyed
  replay takes the early return described above. M4 requires Verso #298 merged
  and **no queued jobs referencing retired names** before conversion.
- **Old per-GPU workers can talk to a new queue.** Current unkeyed v3 workers
  keep one batch attempt each, restricted to their reported device and R1's
  common host ledger. M1 keyed workers also retain one-attempt admission until
  M3. Legacy sibling views may coexist during a queue-first upgrade; a machine
  worker and a legacy worker for the same physical host may not. M4 first
  reconciles and retires every legacy session under maintenance. An unretired
  conflict receives HTTP 409 `machine_worker_conflict`, with no new admission
  or implied release. Pre-v3 workers remain refused. Delete the legacy unkeyed
  worker form once **every fleet host runs machine workers and the rollback
  window has closed**; historical exact-attempt custody remains available.
- **Status has one machine row with a device vector**, host CPU/RAM/debt,
  per-device capacity/health/qualification, active keyed attempts, engine slots
  and outstanding releases. Legacy rows are marked transitional/historical,
  never counted again as capacity after retirement. Lost device evidence cannot
  erase an old charge. `eligible_hosts` remains physical host scope; unresolved
  names remain in `eligible_unknown_machines` as today; known retired names
  fail new intent with `machine_renamed`. Do not sum CPU/RAM once per device or sum unified memory into separate physical pools.
- **Qualification and servers stay device-specific.** Reuse extension digest,
  exact target and hardware/driver/baseline fingerprint, now keyed by machine
  and device. A pass on device 0 does not qualify device 1. Reuse an existing
  report only when its complete identity still matches; otherwise unqualified.
  Environment recipes remain OS/arch/platform keyed, not GPU-index keyed.
  Model caches remain disk artifacts; resident engines, calls, pins, readiness
  and release are keyed by machine/device/model/execution. Existing model
  `device` restrictions narrow the component choice and must agree with it.
  A machine pin selects one eligible device and retains it while healthy;
  a pin's optional device restriction selects the requested card. M4 rewrites
  legacy pins before retiring their machine names. Route to an exact ready slot.
  Retain shared model memory behavior, owner tiers, local-source approvals, secret
  handling, streaming interruption and no replay after output starts. Separate
  device engines may share verified weights on disk, not process custody.


## M4 conversion and window

### Installation, checkpoints and rollback (M4)

Fresh installs create one `myce-worker.service`; `myce join` enumerates the
machine in one foreground process. Explicit device flags restrict the offered
inventory rather than create a new hypha. The existing per-device memory,
reserve and selector settings become entries in the saved machine config;
host policy stays host-wide. No automatic GPU driver, OS, package or other host changes are authorized.
The one-command fleet-upgrade amendment below permits replacing only Mycelium's
own installed binary and restarting only its existing service unit after an
explicit operator command, for an approved signed release, without downgrades.

**Deploy gate:** do not deploy the M2 binary to the current split per-GPU fleet
workers before this M4 conversion. The queue refuses another inventory-reporting
machine identity on the same physical host with HTTP 409
`machine_worker_conflict`: "run one machine worker per host; see M4 install
conversion". A queue-first upgrade does not authorize upgrading split workers;
convert their units/config/spools under the maintenance gates below.

Extend the current installer transaction rather than add a migration daemon.
For an existing split install, use a destination `--config MACHINE_CONFIG` and
repeatable `--from-worker-config SOURCE_CONFIG`, with `--dry-run` showing the
complete plan. Sources must describe every known per-GPU worker on that host,
including `myce-worker@gpuN` units. Explicit sources prevent guessing ownership
from a unit-name glob. Unselected known workers block with
`machine_migration_incomplete`; conflicting shared settings block with
`machine_migration_conflict` and named fields. An explicit existing installer
option can resolve a setting; the installer never silently chooses the first
value, adds host budgets, drops a secret reference or overwrites unknown state.

The queue itself runs on **machine-a**, so the window covers its upgrade and restart
recovery as well as both fleet workers. **Upgrade machine-b to a machine worker in
the same window**, retaining machine name `machine-b` and its single CUDA device
bound by UUID. This avoids a permanent legacy fleet dependency, exercises the
new queue's real reconnect path and gives the unkeyed adapter a deletion path.
The singular worker wire form remains bounded to the queue-first interval, but
the actual production binary must be tested separately: 08816cf retries a removed
host-control endpoint with HTTP 404 before check-in. Queue and **all machine-a and machine-b
workers must cut over in the same window**; old workers cannot provide service on
M. machine-b is unavailable from the queue upgrade until its own conversion and new
machine check-in. Prestage both hosts' artifacts, mappings and restore manifests;
after queue migration and offline topology conversion, convert machine-b first, then
machine-a, deferring lengthy qualification until both have reconnected under disabled
maintenance. A failed machine-b conversion is an explicit abort point: while intake
is frozen and no new business intent has been accepted, settle custody and restore
the complete pre-window queue/worker snapshot. Unresolved custody blocks restore;
keep admission disabled. Do not start old machine-b workers against M as a fallback.
Final acceptance requires machine-b's upgraded machine worker. Include machine-b work and
consumers in the agreed drain. No rolling service restart is implicitly safe.

1. Require Verso #298 and #106 merged into the staged commit (verify ancestry
   and maintainer-published binary SHA-256), plus a production-operator-agreed
   window. The starting point is **production 57ed4e6 queue/root and 08816cf
   workers**, not integ: it has local host intent/registry but no queue-owned
   maintenance API or host memory budget record. Freeze Verso/other intake.
   The preferred route on that lineage uses its root
   `host close --local --wait-released` bridge; the explicit verified-empty
   alternative below requires no privileged close.
   exit 1 with `release_not_confirmed` means re-check status, never release.
   Require confirmed settlement, zero custody, then stop the exact units and
   snapshot binaries (root separately), configs, units, registries and DB.
   Upgrade the stopped queue first and let the M build open/migrate its state.
   Only now use M's queue-owned maintenance commands: disable **machine-a and machine-b**
   with stop and wait for every
   batch/member, preparation, engine, buffered result and release to reconcile.
   Require no queued jobs whose job or resolved placement references a retiring
   name; let clients cancel/resubmit before the window, never rewrite keyed
   specs in place. A missing/stale worker without the exact production discharge
   receipt, or any residual obligation, fails the gate unless the explicit
   verified-empty route below independently proves the stopped sources empty.
   Preserve queue/key/counter state and checkpoint generations. Record source
   IDs, sessions and device identities.
2. Snapshot the queue binary, SQLite state and queue-side config, both workers'
   binaries/config/secret references, environment files, spools and actual unit
   registration/activity through the existing rollback mechanism. Preserve NAS/
   checkpoint/model roots, cache paths/limits, Docker opt-in and endpoint,
   availability/reserve settings, cgroup limits and every XPU hotfix setting.
   Conflicting NAS roots require an explicit canonical root resolving the same
   relative checkpoint names; verify access before changing anything. Select
   **one machine cache root: the existing gpu0 root** and its reviewed limit.
   Leave the other roots untouched for separately authorized later cleanup;
   do not add per-device cache-root overrides or relocate absolute-path
   environments. Existing shared-root locks/publication remain intact.
3. Convert the **queue-side configuration** explicitly: project policy
   `allowed_machines: [machine-a-gpu0, machine-a-gpu1, machine-b]` becomes `[machine-a, machine-b]`;
   preserve card-specific intent using the appropriate device restriction.
   Rewrite pins and server `allowed_machines`/`device` restrictions and reconcile
   host policies onto the physical host IDs without summing duplicate CPU/RAM
   budgets. Preview each before/after setting, preserving intersections; any
   fence that cannot be represented equivalently blocks with
   `machine_migration_conflict`, never broadens placement. Already host-keyed
   policies remain unchanged. Reject existing multi-card masks as above.
4. The queue upgrade projects production's persisted closed `host_intents`
   into disabled queue-owned maintenance, preserving the equal per-worker host
   allowance once (disagreeing allowances refuse startup). Exact discharged
   source-session receipts authorize offline conversion of stopped/Gone sources
   without pretending they are fresh. No age-based release or fabricated session
   is allowed. This one-time projection adds no production host-control runtime
   or API to M. Reconnecting legacy workers remain held by M maintenance.
   Stop the M queue for
   offline conversion, then restart against the preserved DB and converted config.
   Verify queue restart recovery: jobs, keys, counters, maintenance, pins,
   checkpoints and all exact custody are preserved; no idle/loss inference
   releases resources or starts work. Verify machine-b's discharged old session and
   refusal behavior, then its upgraded worker's reconnect. Failed queue recovery
   or upgraded reconnect is a failed gate, not permission to clear state.
5. Stop/disable the exact source worker units and verify process/daemon cleanup
   on both machines. For machine-a, **remove `myce-worker@gpu0` and
   `myce-worker@gpu1` from the host/installer's persistent enable set**, as well
   as disabling their systemd registrations; merely stopping/disabling them
   leaves redeploy able to recreate them. Archive source configs/spools
   recoverably; never merge spool files, invent a previous-session chain or
   rename attempt ownership. Retire fully settled legacy sessions in the
   maintained queue, retaining their exact historical records and replacement
   diagnostics. They may acknowledge old custody but never regain admission.
6. Install/enable one machine unit on each host with a fresh spool. Inspect
   effective manager settings; verify machine-a's full stable two-XPU inventory and
   fixed budgets, machine-b's UUID-bound CUDA inventory, and one CPU/RAM allowance
   per host. Confirm machine-b's new session reconnects and reconciles without
   duplicate capacity, lost history or counter resets. Keep maintenance active
   through conversion, reboot and redeploy; execution uses the qualification
   phases below. Re-running a completed plan is idempotent; interrupted steps
   resume the saved transaction or restore it, never enable both generations.
   Unknown restoration keeps admission off.
7. Return to ordinary workloads only after the qualification checklist below
   and explicit window acceptance. Implementation or installer success alone
   is not qualification.

Checkpoints do not contain worker topology. Keep logical job IDs, submission
keys, attempt numbers/counters and NAS-relative paths unchanged; resume the
last atomically complete monotonic generation with a new exact attempt. The
same applies to accepted file-store generations and immutable output refs.
No local-only or half-written progress is promoted by migration. A missing
previously accepted checkpoint/root leaves work waiting; it never silently
restarts saved resumable work from an empty directory. First attempts with no
saved progress and deliberately nonresumable jobs start normally.
Old per-GPU attempt identity remains available for late result rejection and
exact release after retirement; retirement never rewrites accepted intent.

Rollback is maintenance on both hosts followed by exact cleanup of **all**
new attempts, then restoration of the snapshotted queue-side config and source
worker files/units, persistent enable sets and verified prior activity,
including machine-b's prior worker. Keep the upgraded queue for a topology rollback:
it supports the bounded singular worker wire form, but actual production binaries
may remain contained and unable to check in. Under maintenance, deactivate the machine sessions and reactivate
only recorded source identities, preserving their historical records, all jobs/
keys/counters and checkpoint progress. Record actual reconnect/refusal behavior.
Restoring working production requires the separately gated snapshot abort below.
Never run old and new workers together. A full queue downgrade may use its
pre-window database snapshot only if no subsequently accepted intent
would be lost; otherwise refuse downgrade and retain the compatible queue for
repair. Never open M-mutated state with an older binary or overwrite newer
checkpoints with a snapshot. A rollback that cannot prove cleanup/restoration
is incomplete, leaves admission disabled and preserves recovery artifacts.

### M4 implementation decisions

The conversion uses the installer's existing file transaction and service-manager
snapshot. Private mode-0600 `CONFIG.m4-rollback.json` journals retain original
files and unit registration/activity; they contain credential copies when those
are installer-owned and must stay outside public reports. Referenced secret
files are preserved by reference, never imported into the plan. Source spools,
binaries and all caches remain on disk; source unit/config definitions move to
recoverable transaction backups. In the current installer the persistent enable
set also includes the **production-lineage** `worker-*.json` records in
`~/.local/share/myce` (or the source config's explicit `host_state_dir`). Retire
only records whose config/unit/spool/host/name/queue match the selected sources;
refuse unknown or conflicting records. Journal and restore their exact bytes on
rollback. Remove these records and systemd registrations/config artifacts, and
refuse any unselected split unit. The production root copy's local open/close no
longer controls the converted worker: use M `host disable HOST --stop --wait`,
`host status HOST`, and `host resume HOST`. Retain the old root executable only
for the explicitly gated frozen-snapshot abort, never as M host control. Source units must also pass the existing Mycelium worker-unit
allow-list before any manager action. The gate applies to ordinary redeploys as well as fresh installs.

Queue conversion is an explicit offline `myce install queue --config ...
--machine-map ...` operation under the existing SQLite exclusive lock, using the
injected Clock for freshness. The map names every source and its physical host
and component, including machine-b's same-name entry. A durable pending-publication
fence commits with retirement; queue startup refuses partial policy publication.
Retry the identical command to publish the remaining policy files, or restore
through the journal. Neither action restores a database snapshot. Policy files
must match the saved original or converted typed values; unknown edits block
with `machine_migration_conflict`. Projects lack a device restriction, so a
partial-card project allow-list blocks rather than widening to the entire host.
A pin or single-host server allow-list can retain its card through `device`.
Queued jobs resolved through project, pin or server fences also block retirement
with `QueuedRetiredMachinesError` (`machine_migration_queued_jobs`), including
logical job IDs and retiring machine names. Jobs and keyed specs are unchanged.

**Verified-empty drain (decision 2026-10-21).** Exact persisted production close
and source-session discharge receipts remain the preferred route. Operators who
cannot or choose not to run privileged production close may instead stop the
source user units, freeze all producers, disable the mapped hosts in M, and run:

```sh
myce install queue --config QUEUE_CONFIG --machine-map MAP \
  --drain-evidence=verified-empty \
  --from-worker-config SOURCE_0 --from-worker-config SOURCE_1 --dry-run
```

Repeat without `--dry-run` after reviewing the plan. Supply every mapped source
config, including a single-worker host's config when converting that host. The
tool inspects the **local systemd user manager** and source spools; copied remote
attestations cannot authorize conversion. The offline database must be an
existing, readable supported queue held under its exclusive lock. This route
conservatively requires **zero queued, running, stopping or preparing jobs in the
whole queue**, including retained submission records, and zero host releases or
obligations on the mapped hosts. It never infers discharge from worker age.

For each exact queue machine/session and source config, verify the loaded unit's
identity and fragment path, `ActiveState=inactive`, `MainPID=0`, and no pending
unit job. Verify cgroup v2 and read `cgroup.procs` throughout the unit subtree.
When systemd clears an inactive unit's `ControlGroup`, derive its normal path
from the manager's reported containing slice and exact unit name; an absent
subtree is accepted only with a readable containing slice. Missing or unreadable
kernel evidence refuses. Hold the source's existing exclusive `worker.lock`
through conversion and publication, require its marked v3 spool and exact saved
session, and use the C4 production/M spool inspector without cleanup or inferred
acknowledgements. Retained results and released Docker records are settled
history only when that locked queue accounts for the exact job/attempt/worker/
session: a current attempt must be terminal with confirmed release; a superseded
attempt must have its own confirmed-release copy. A legacy Docker retention ack
may be absent after a stale completion; its presence, age or a newer attempt is
never release proof. Pending, unreleased or operator-confirmed Docker custody,
unreadable evidence and active process state still refuse. Report every blocking
spool filename with its specific reason. Hash the format marker and top-level
JSON evidence in sorted name order with length-delimited file names/sizes and
SHA-256.

At each configured Docker endpoint (explicit local default when Docker was not
configured), successful `docker ps` must show zero running containers labelled
with the source's `myce.worker` and zero with `myce.attempt`. The latter is a
conservative endpoint-wide fence covering older attempt-only labels. Docker
being disabled for future jobs does not waive this observation. Inaccessible
Docker returns `machine_drain_docker_inaccessible`; running labelled containers
return `machine_drain_container_running`. Other distinct typed refusals include
`machine_drain_queue_not_empty`, `machine_drain_obligations_pending`,
`machine_drain_unit_active`, `machine_drain_unit_unreadable`,
`machine_drain_cgroup_populated`, `machine_drain_cgroup_unreadable`,
`machine_drain_spool_unsettled`, `machine_drain_evidence_missing` and
`machine_drain_queue_unreadable`. Refusal and preview never persist a receipt.

The conversion record's `verified_empty_drains` receipts commit atomically with
source retirement: injected-Clock timestamps, machine/host/session, inactive unit
and substate/MainPID, cgroup path and empty/absent observations, spool SHA-256,
Docker observation time/count, and all six zero queue/custody counts. They are
historical installation evidence, never reusable health or admission authority.
The verified stopped source becomes Gone even for a same-name single-worker
conversion, allowing a fresh machine spool without fabricating a leave/session
chain. Publication retries use the already committed conversion; forward after
rollback requires fresh evidence. Without the flag all existing conversion gates
remain unchanged.

This is safe because there is **no producer, no custody, and no process can hold
a device**: intake is frozen and the queue is stopped/locked, mapped hosts remain
disabled, no pending intent or release remains, existing spool locks exclude
worker restart through retirement, units/cgroups are stopped/empty, and the
daemon independently proves there is no labelled container execution. Any
unknown observation refuses. It adds no production host-control API, privilege
requirement, remote attestation system or runtime compatibility path.

Rollback reverses the current representable pins, preserving additions, removals
and preemption counters. A new host-wide pin that cannot be expressed on split
workers blocks rollback until the operator explicitly selects a card or removes
it. Unknown policy edits similarly require reconciliation before restoration.
Rollback **refuses**, rather than rewriting keyed job intent, while any queued,
failed or cancelled job has a machine-era `allowed_machines` reference that
would disappear (including resumable/requeueable jobs). Succeeded historical
jobs retain their exact identity. Cancellation alone does not remove this gate;
retain the M topology until referencing intent is reconciled explicitly outside
the converter. This is independent of the much
stricter no-new-business-intent gate for restoring a production DB snapshot.
Queue-side rollback reactivates only recorded legacy identities and refuses the
machine generation until forward conversion. Retired records retain their old
sessions for exact result/release handling; a new check-in cannot revive them.
Worker-local catalogs use the queue's explicit retired records to rewrite only
placement fences, retaining local environments/secrets and exact original file
bytes in the private worker journal. Write-ahead SHA-256/mode/removal receipts
bind every publication, including intermediate writes before a crash. Rollback
checks all changed files before any manager mutation; unknown content/mode
returns typed `machine_migration_conflict` with sanitized file/difference names.
Unchanged files, especially a shared catalog already converted by the queue,
are not rewritten. Policy/catalog publication preserves original modes. SQLite
stores only model names, placement restrictions and original/converted hashes;
model environment values and secret references never enter conversion state.
Interrupted plans read source identity from the journal, even after source files
move to recoverable backups. Missing destination units are valid rollback input.
CLI SIGHUP/SIGINT/SIGTERM cancel setup; recovery ignores cancellation within its
bounded restoration context. Dry rollback performs maintenance, file ownership
and spool inspection and prints the file/unit plan without writes; after a real
stop it also acquires the exclusive spool lock before restoring source units.
The same machine spool survives forward -> rollback -> forward. Inspect both
source and reused destination spools: heartbeat and operator-stop state,
`attempt-*.json`, `*.result.json`, `*.docker.json`/ack and `engine-*.json`, using
the sparse formats actually written by production 08816cf as well as M. Retained
results and released Docker records require the exact queue settlement described
above, including explicit released copies for superseded attempts; a legacy
retention acknowledgement is not release authority. Process/cgroup custody must
be absent.
No daemon calls, orphan cleanup or inferred result acknowledgement are made by
this read-only gate. This handles a separate local catalog on machine-b as
well as a shared catalog already converted by the queue. During the queue-first
interval only the recorded same-name legacy session may report the saved old
catalog hash while its host is disabled with no obligations; its model endpoint
is withheld. Inventory workers, unknown sessions and resumed hosts still require
the current hash. This exception ends with conversion/rollback and cannot admit
work. machine-b uses a fresh spool only after its same-name source completes the normal
leave handshake or passes the explicit verified-empty retirement gate above.
No prior-session relationship is invented. The compatible
queue and all subsequently accepted jobs, keys, counters and checkpoints survive
worker rollback. A separate queue downgrade remains subject to the stricter
no-new-intent rule above. In the production-snapshot abort, stop all M writers,
archive post-upgrade synthetic probe evidence, restore the pre-window DB/configs,
local host intent/registries, exact units/enablement and each binary from its own
backup with `sha256sum -c`. Never open an M-mutated DB with 57ed4e6. Upgraded queue
startup failure, incompatible Verso client and failed verification all take this
same abort branch while intake remains stopped and no new Verso/other business
intent was accepted; after that cutoff, keep admission disabled and repair
forward. Restored 08816cf workers are not assumed runnable on the M queue: record
actual check-in refusals and exit behavior during the rollback interval. The
laptop historical-binary drill observes 08816cf retrying a removed host-control
route with HTTP 404 before check-in; direct retired check-ins return
`machine_renamed`. Their
`RestartPreventExitStatus=78` suppresses restart only when the process actually
exits 78; a retrying HTTP refusal is not an exited/safely restored worker.


## Native qualification

### Native machine-a maintenance-window qualification

Record binary/config digests, exact attempt IDs, masks, measured capacities and
results; keep private operational details in approved private evidence. Stop at
the first failed gate, restore the queue config and worker installations as
above when its gates permit; otherwise remain disabled and repair forward.
Verify restoration before ending the window. Conversion/inspection occurs disabled. During the
agreed window the production operator pauses ordinary submissions using its
existing controls and accounts for queued demand; explicitly resume the queue
only for test phases, then disable/stop/wait again before reboot, redeploy or
rollback. There is no test bypass of maintenance or Started. Final acceptance
reopens ordinary submissions and resumes both hosts. These checks are unrun by M0:

- Confirm Verso #298 merged, the agreed window, both durable host disables,
  fresh evidence, zero obligations and no queued retired-name references.
  Inspect converted queue-side project policies, pins, server restrictions
  and host policies. Verify typed `machine_renamed` guidance for a new job using
  `machine-a-gpu0`, while an existing keyed replay returns its original job.
- Upgrade/restart the **machine-a queue** and verify persisted maintenance, jobs,
  submission keys, retry counters, pins, checkpoints and release custody.
  Observe machine-b reconnect before conversion, then upgrade it and require a
  single machine worker/session with UUID-bound CUDA, preserved config and
  successful reconnect/recovery. Test a small machine-b GPU job in the test phase.
- Inventory **two XPUs** with distinct PCI BDF/Level Zero UUID mappings,
  `ZE_ENABLE_PCI_ID_DEVICE_ORDER=1` in worker and members, unchanged fixed
  per-device budgets, one machine-a worker process/unit and one CPU/RAM allowance.
  Exercise a mapping mismatch and require `device_identity_mismatch` with no
  start on that device; verify the mapping again after restart. Exercise a
  transient and an empty startup probe, then matching-identity recovery, plus a
  driver-version/name update that does not withhold the same hardware. A Level
  Zero probe failure at startup must report `device_probe_unavailable` and allow
  qualified CPU work while withholding accelerators.
- Run two small GPU jobs concurrently, one on each XPU, including a native
  sidecar in each. Inspect actual device visibility/allocation: main and helper
  see only their assigned card as in-job device 0. Test an optional card selector;
  it must not spill onto its neighbor.
- Run a **CPU attempt beside GPU work** and enumerate what its main, native
  sidecar and managed container can actually see through CUDA and XPU runtime
  APIs: require **zero accelerators**, an empty `CUDA_VISIBLE_DEVICES`, effective
  select-none `ZE_AFFINITY_MASK` and `ONEAPI_DEVICE_SELECTOR`, and forced PCI
  ordering. Empty/unset environment checks alone do not pass. Repeat on machine-b
  for CUDA; failure withholds CPU work instead of claiming isolation. The
  visibility probes require `python3` and a loadable `libcudart` on CUDA hosts,
  and `sycl-ls` on PATH on XPU hosts. Qualify the installed `sycl-ls` behavior
  under `ONEAPI_DEVICE_SELECTOR=!*:*`, including its exit status and output;
  a nonzero exit is not automatically evidence of zero accelerators.
- Cancel/kill one exact attempt and separately force a member OOM. Require
  whole-group cleanup/release and immediate stage failure for OOM; the neighbor
  continues with unchanged process/device, heartbeat and correct accepted
  result. No host-wide signal, cgroup kill or GPU reset may be used as an
  attempt-local cleanup shortcut.
- Reject over-budget work, count > 1 and multi-card model/attempt masks with
  their typed errors. Verify worker `--device 1` reports/allocates only card 1,
  `--device 0,1` offers both, and an omitted flag reports all cards. Confirm fixed
  XPU accounting under concurrent load,
  per-attempt/member ceilings and no stale result publication.
- Exercise production hold across both devices and CPU work, priority planned
  stops more than twice without retry charges, exact-ack replacement and the
  two-infrastructure-requeue bound. Confirm a machine heartbeat stall uses the
  shortest active-work timeout and charges each affected production attempt.
  Drain/disable must still win, including preparation/Started races and
  independent failure precedence.
- Save a durable checkpoint, stop under maintenance, reboot and verify only
  machine units return. Reconcile queue/worker custody and resume the exact
  logical job from its accepted generation. Redeploy through `myce install`;
  verify queue config, the chosen gpu0 cache root (or explicitly migrate generated
  cache settings with `install worker --machine-cache-root`), untouched other
  caches and hotfix budgets. Inspect the persistent enable set and systemd registration:
  neither `@gpu0` nor `@gpu1` may return. Exercise rollback to the two machine-a source
  workers and machine-b's source worker, then forward again while disabled,
  including queue recovery and machine-b reconnect on both paths.
- Recheck production/consumer health and obtain window acceptance before the
  final return to ordinary workloads; record qualification separately for each
  device and the shared RAM envelope.


## Rollout estimates and blocked window

### Mergeable slices and estimates

M0 may merge alongside M1; implementation order is **M1 → M2 → M3 → M4** on the
isolated after-Verso track. Do not merge into the consumer's production build or
operate hardware until the existing GPU/cutover gates and maintenance window
permit it. The queue keeps M1's one-attempt admission rule through M2; M3
lifts that rule for machine workers and M4 migrates installations. Estimates are
changed/added hand-written production LOC and test/doc LOC, with one engineer's
days including focused validation/review; generated schemas and hardware wait
are excluded. They replace the previous package M estimates.

| Slice | Mergeable outcome | Production LOC; test/doc LOC; days | Acceptance |
| --- | --- | --- | --- |
| M1 (existing owner) | One keyed revision with an `attempts` collection; queue admits one attempt. | Existing M1 brief/estimate; excluded below. | Legacy/new queue compatibility, strict decode and multiple-assignment rejection, session-fenced stop, stale result rejection, exact release-only requeue and retained restart custody. |
| M2 | Additive machine inventory/config; replace current-job/process pointers, spool and recovery with keyed collections; per-attempt device env, stable identities and D5 subtrees. Reuse cache/detection/runner implementations. | 700–1,100; 900–1,400; 5–7 days | Fake two-device inventory plus real CPU helpers execute concurrent harness assignments; stop/member failure leaves neighbor running; select-none CPU masks, UUID/BDF mapping rechecks and mismatch refusal, group masks/token exclusion, parallel readiness/control, aggregate local refusal, every-attempt restart and buffered-result replay. M1 queue admission remains one attempt; compatibility guards reject unsupported inventory/replies clearly. |
| M3 | Enumerate (machine, device) targets using the existing per-target planner and host ledger; per-device engines/qualification, selectors, retired-name errors, job schema and machine status; enable queue concurrency. No vector-fitting planner. | 400–650; 650–1,000; 3–5 days | **`myce dev` multi-device scenario** through the real API/runner: concurrent GPU helpers, CPU backfill, independent cancellation, restart/replay and complete process/listener teardown. Fake-clock heterogeneous targets, same-card exclusion, shared RAM/unified debt, R1 victim usefulness/build hand-back and deterministic heuristic; new retired-name intent fails without changing keyed replay; count > 1 rejection; holds/maintenance, >2 planned stops, mixed-tier shortest loss timeout and bounded per-job infrastructure requeue, stale/missing reports and exact cleanup across restart. Check-in benchmark remains within R1's accepted 1.5× baseline gate on its scenario set. |
| M4 | Transactional queue/config and worker conversion on machine-a and machine-b, enable-set removal, rollback and operator docs; laptop migration first, separately accepted native window. | 250–450; 450–700; 3–4 days plus native window | Fake service-manager/filesystem tests for queue upgrade/restart, split→machine, machine-b reconnect/upgrade, conflict/missing-source/queued-retired-name refusal, preserved roots/secrets/hotfix flags, one gpu0 cache root with other roots untouched, enable-set removal, injected failure at each mutation, rollback and idempotent redeploy. Native checklist above before hardware acceptance. |

#### M2 implementation decisions

- The one locked machine spool indexes independent exact-attempt state files.
  Each uses the existing worker state and runner; reopen also scans those files
  so a crash between child persistence and index persistence retains custody.
  The old single-attempt state is adopted in place before new admission. Buffered
  results keep their session until every recovered attempt is acknowledged.
  Only singular legacy adoption may fill an absent assignment session from its
  saved spool session; nonempty mismatches and keyed-file mismatches fail closed.
- Canonical IDs follow saved hardware identities across enumeration order changes.
  A changed XPU ordinal or different hardware identity withholds that saved
  component with `device_identity_mismatch`; discovery never substitutes another
  card for it. Probe errors use `device_probe_unavailable`; both conditions are
  re-evaluated on every probe, and matching identity restores admission. Driver
  versions and names are informational. The native probe uses NVIDIA UUID inventory and the
  installed Level Zero API through Python's standard-library `ctypes`, with PCI
  ordering forced in the probe and all members. Native qualification remains an
  M4 gate; an unavailable or ambiguous runtime probe cannot authorize a launch.
- CPU and unselected XPU controls are `ZE_AFFINITY_MASK=2147483647` and
  `ONEAPI_DEVICE_SELECTOR=!*:*`; CUDA's select-none mask is the empty string.
  Native CPU admission requires actual installed-runtime enumeration with these
  masks. Metal has no device-masking environment equivalent. Under the #116
  contract, CPU work on macOS runs cooperatively without Metal isolation; status
  reports `cpu_isolation=not-enforced` and says GPU isolation is not enforced.
  CPU/RAM remain admission budgets, not hard macOS ceilings. CUDA/XPU masks and
  runtime enumeration are unchanged. Workers report CPU readiness before
  placement; unsupported/unqualified targets are withheld with
  `cpu_accelerator_isolation_unavailable`. A qualification loss before launch
  withdraws that CPU target and requeues without infrastructure-loss retries.
  `myce dev` and the protocol harness inject fake runtime visibility evidence,
  alongside fake inventory, and still run real CPU payloads.
- Until M3 enumerates placement targets, an inventory report projects one
  component into the existing planner. Scalar GPU fields add no capacity. The
  worker harness supplies explicit `allocated_devices` for concurrent jobs;
  the real queue continues to admit one attempt per worker.
- The queue refuses a second inventory-bearing machine identity on a live
  physical host. The existing `myce dev` clock retains its synthetic shared-host
  fixtures; this exception does not apply to an ordinary queue. Empty CPU
  inventories remain explicit through JSON cloning and reporting.
- The default single-card fake runtime treats its old empty selector and
  canonical inventory ID `"0"` identically for extension qualification. Native
  hardware identity checks remain required before launch.

#### M3 implementation decisions

- A check-in reconciles every exact attempt and offers at most one additional
  assignment. Subsequent check-ins fill other eligible components through the
  same planner; the collection and active-attempt count have no cardinality cap.
  CPU targets have no accelerator batch slot and share the host CPU/RAM ledger.
- Historical machine records carry explicit `replacement: {machine, device}`
  migration guidance. M4 creates that metadata transactionally. M3 refuses new
  intent naming those records and never infers retirement from a name suffix.
- Device entries carry their extension fingerprint and reports. The machine's
  `cpu_extension_fingerprint` identifies CPU qualification; the existing scalar
  reports retain the first-device compatibility view alongside CPU reports.
  Residency and pin assignment use the actual machine plus selected device.
- Unknown or unavailable explicit device selections expose the additive job
  diagnostic `diagnostic_code: no_matching_device`. The original submission
  fingerprint remains unchanged. Stop events return an `events` collection
  with an aggregate cursor, retaining the first event's fields for the bounded
  legacy adapter. Each exact event carries priority-stop provenance before
  signal delivery, so a checkpoint handler that exits zero after TERM remains
  an interrupted attempt even when the event precedes its heartbeat directive.
- Returning collection attempts recover independently against their selected
  slots and the existing retained CPU/RAM charge. A neighboring device cannot
  invoke the legacy scalar cap; conflicting slots or allowance debt still
  refuse recovery. Model copies use the shared verified cache, with eligibility
  checked against every component and one copy offer per model. A pending model
  load waits while its component is withheld by a probe, within the existing
  queue load deadline. A healthy offer snapshots that component before the
  asynchronous load; the normal fresh identity check still gates launch.
- A recovered legacy GPU copy without component evidence retains its original
  singular recovery guard and excludes all GPU targets until exact cleanup;
  no device identity is invented. Check-ins returning immediate cleanup for a
  reported attempt offer no new batch assignment, allowing startup supervisors
  to drain recovered custody and complete their existing fresh-session handoff.
- Startup check-ins explicitly set additive `recovering: true` until every
  saved attempt and buffered result is acknowledged and the existing fresh-session
  handoff completes (#106). Reports alone cannot distinguish startup recovery
  from ordinary concurrent work. Reconcile exact custody and stop/kill controls,
  including owned kill-phase attempts, but offer no batch assignments (including
  redelivery), preparation or model work. Recovery refuses admission **before**
  allocating an attempt: no attempt number, copy history, loss or infrastructure
  retry is consumed. Do not refund/reuse offered identities; stale execution
  fences remain monotonic. The worker rejects an assignment in a recovery reply
  as a protocol error, retaining custody, rather than reporting an interrupted
  execution. Actual launch/infrastructure failures keep their existing classification.
  The queue persists `recovering` and queue-observed `recovering_since` from
  each check-in; every device and CPU target of that machine is admission-blocked
  in placement, preemption planning and launch authorization. Other machines
  remain eligible, so production can preempt there. Refusing new admission must
  not hold existing supervisors' buffered-result replay. Status displays recovery
  and its start time, with `recovery_needs_attention` after **two minutes** of
  continuous recovery (the ordinary cleanup escalation interval). Heartbeats do
  not reset that timer. A check-in clearing recovery clears the timer and warning
  automatically after the worker finishes draining.
  **Deploy the #106 queue before workers.** A new worker receiving an assignment
  during recovery from a pre-#106 queue fails closed with HTTP 426
  `worker_protocol_invalid`, tells the operator to upgrade the queue first,
  and retains saved custody. A queue lacking keyed-attempt-v1 also fails closed
  with that upgrade instruction.
- For machine workers, the planner credits lower-tier stopping attempts in both
  grace and kill phases as soon-free capacity (#106), but only **before
  `StopAt + preemptionGrace()`** (ten seconds after `StopAt` by default).
  Within that window prefer the pending release over a new stop on another
  device. At the bound, stop crediting a wedged kill and preempt useful capacity
  elsewhere, on the same or another machine, within production's start window.
  A confirmed exit retains planning credit until fresh inventory arrives or the
  same absolute bound, whichever comes first. Neither completion nor heartbeats
  extend it. This is only a release projection: device/CPU/RAM charges
  and exact cleanup acknowledgements still gate replacement admission. Ordinary
  non-recovering workers may continue backfilling unaffected capacity. The bounded
  legacy adapter retains its existing fallback to another worker after stop
  grace, including expiry of acknowledgement credit.

M2–M4 total: **1,350–2,200 production LOC; 2,000–3,100 test/doc LOC;
11–16 engineer days**, excluding M1 and native access delays. M2 grows from
3–4 to 5–7 days because singular execution pointers, durable spool formats and
restart/result custody all become collections; changing only the launcher is
insufficient. M3 is 3–5 days: removing aliases and vector fitting reduces design
scope, but per-device engines/qualification, status and end-to-end concurrency
still require integration. M4 is 3–4 days because the queue and machine-b conversion
are part of the transaction/window, not an assumed external upgrade.

Every slice keeps `myce dev` laptop-testable with the real API/command runner
and fake multi-GPU inventory;
run focused fake-clock/process tests, build/vet and serialized race checks.
Laptop success cannot establish actual XPU masks, device fault isolation,
cgroup/daemon behavior, reboot or the measured production memory envelope.

Delete the per-GPU **worker identity** and service-instance creation path after
migration: duplicate host participation/CPU/RAM reports, singular current-job
and process pointers, global device env, per-GPU worker model-slot keys and
per-GPU maintenance fan-out for new machines. Replace them with the inventory,
keyed supervisors and existing host policy. Keep only the bounded legacy wire
adapter until every fleet host runs machine workers and the rollback window
has closed, then delete the unkeyed worker form. Retain historical exact-attempt
custody. Add no new peer registry, lease, local host-control daemon, cache
installer, application-specific scheduler or second victim planner.

Open delivery risks are actual multi-XPU enumeration/stable mapping, device-local
native isolation and driver failures that can affect the whole host, stale
qualification/approval fingerprints, installer source-config conflicts, and
planner check-in cost. Keep pre-existing planner defects tracked in #104
separate; their feasibility/preparation repairs must land before M3 enables
concurrency. Enumerating device targets must not conceal them. CPU core
reservations do not enforce CPU time. Same-tier indefinite work still has no bounded-wait
promise, and multi-device attempts remain deferred to #105. Current state:
M0 specifies the target; M1 has its own implementation/review owner. Next step:
orchestrator review/merge of the revised M0 and M1, then M2's fake-device gate.
The M4 window remains blocked on Verso #298 and its explicit acceptance gates.


## Consumer migration inventory

Read-only source inventory on 2026-10-27 establishes these migration obligations,
not native execution evidence. Keep private paths, host IDs, data and receipts
in consumer repos; use sanitized fixtures in Mycelium.

| Consumer | Preserve and prove at its existing seam |
| --- | --- |
| Verso | Two offline-suite entries retain Pixi CPU tools, wrapper, exact target and JUnit check. Its native naming extension retains CUDA variant, sidecar, baseline/result names and production submit/key/cancel seam. Model endpoints remain unchanged; the consumer decides Docker/native direction. |
| Forge | Backend/frontend/tooling entries retain targeted/full distinctions, macOS-only suites, placement restrictions, Python/Node prerequisites and markers/archive. Copy exact temporary queue-side lock bytes only under the approved environment option; do not infer Linux qualification from a list. |
| Valence | Pytest/browser-e2e entries retain explicit Git bundle input and stale-bundle check (source omits `.git`), resources/thread limits, installed Python/Node/browser prerequisites and success markers. Conversion does not substitute a new environment. |
| video-editor | Render/canary and per-item catalog entries retain frozen scene/binary inputs, snapshot identities, large frame archives, semantic checks, private placement and existing holds. Conversion must not launch another render. |
| Cassette | Generation/validation/evaluation entries retain the exact native CPU runtime/source closure, checksum checks, seed/candidate coverage, artifacts and holds. Historical loader alternatives stay historical; new uv/Pixi runtimes or GPU ports need separate parity/native proof. |

Convert Mycelium's gate/go-test profiles and starter extensions first. Prove a
small CPU fixture and fake native variant in `myce dev`, then compare sanitized
consumer conversions through the same lowering. Consumer owners apply changes,
update harness/CI callers and record one small authorized accepted-result proof
per active entry type, requalifying changed native digests. A held consumer stays
held with owner/next action and blocks removal of any reader it still uses.
Retire redundant docs/examples and update the offload skill with cutover.
