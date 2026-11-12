# Queue maintenance reference

How the commands in the [queue maintenance runbook](../operations.md#queue-maintenance)
behave: selector exclusions, per-device maintenance, last-resort placement, drain
and disable semantics, control-request timing, queue diagnostics, profiling and
validation reservations.

## Exclusion semantics

Selectors are literal: `host=HOST`, `kind=gpu` (all accelerators, including
CUDA, XPU, Metal and dev fake devices), `platform=PLATFORM`, or
`host=HOST;devices=DEVICE,DEVICE`. A host can qualify either kind or platform,
for example `host=compute-a;platform=cuda`. Quote selectors containing `;` so
the shell passes them as one argument. Device lists accept displayed IDs or
stable identities, resolve them to physical identities, sort and deduplicate
them, and print the durable selector. Use that printed selector for removal,
especially after a worker's ordinals change or devices leave inventory.
Kind/platform settings remain in force for future matching devices and hosts;
they are not expanded into a snapshot of today's inventory.

These queue-owned settings persist in SQLite across restarts. They reuse the
per-device admission fence below: new placements and unstarted launches avoid
excluded components, while Started attempts and active model calls finish.
They require no worker restart or configuration edit. A host's GPU exclusion
leaves its CPU work and RAM allowances available. Whole-host exclusion calls
the existing host drain transaction, covering CPU, RAM and accelerators together.
Adding a whole-host exclusion to an already drained/disabled host is refused;
manage that existing maintenance with `myce host` instead. Remove the host
exclusion to resume its drain. If another operator has since drained or disabled
the host independently, removal leaves that maintenance intact. `host resume`
cannot bypass an active whole-host exclusion.

`myce status` (CLI, JSON and web) and `myce exclude list` show each active
selector, reason, queue timestamp and `set_by`. `--by OPERATOR` supplies the
attribution; it defaults to the current OS username. This is an operator-supplied
label under the existing shared fleet-token authority, not a separately verified
login. The commands reuse installed pinned operator authority. The CLI resolves
the selector and reads its revision before editing; a competing edit is rejected
as stale. API clients use `GET /api/exclusions/resolve?selector=...`,
`GET /api/exclusions`, and `POST /api/exclusions` with `action` (`add`/`remove`),
the resolved `selector`, `set_by`, `if_revision`, and optional add `reason`.
The API retains inactive revision records; `exclude list` displays active ones.

Exclusions combine additively with each other, per-device disables and host
maintenance. Removing one restores only capacity not withheld by another fence.
Use the CPU/RAM allowances under [CPU capacity and allowances](host-capacity.md#cpu-capacity-and-allowances-r1)
to reserve a chunk of those resources; there is no new CPU/RAM slicing protocol.
`--reserve-cpu-cores` and `--reserve-host-memory-gb` provide initial owner reserves;
`myce host budget HOST --cpu-budget-cores N --memory-budget-gb GiB` durably changes
the queue's allowances while the host is drained. Resume after the change and
allow worker views to acknowledge the policy. Zero CPU budget means automatic
capacity minus reserve, not zero available cores; RAM budgets must be positive.

The `exclude add|remove|list` shape keeps host, fleet and list selectors in one
small command family. The [single-device controls](../operations.md#set-aside-one-accelerator) remain available as the
original independent per-device setting.

## Device maintenance, placement and drain semantics

`DEVICE` is the displayed inventory ID (for example `0`) or its stable
`identity` from `myce host status HOST`. The CLI resolves it to the physical
identity before submitting a conditional edit. The queue persists the policy
by host, platform and identity in SQLite, independently of ordinals, worker
sessions and health reports. Queue and worker restarts preserve it; an index
reused by a different physical device does not inherit the old disable.
If an inventory identity is missing or ambiguous, inspect host status and use
the stable identity rather than guessing an index. Workers without a stable
inventory cannot use this control.

Disabling admission preserves Started batch attempts, running preparations,
engines and active calls. It withdraws unstarted batch offers and refuses new
batch placements, calls, engine launches and device-specific preparation
launches on that device. Model-cache copies use shared host storage and remain
allowed. Healthy siblings retain their budgets and CPU work
remains eligible. Existing work can still finish or stop under ordinary
cancellation, failure and priority rules. This command does not edit the
worker installation/configuration and needs no restart. Host drain/disable
remains an independent fence; enabling a device does not resume its host.

Host status returns `device_maintenance` records with identity, reason,
`updated_at` (injected queue time) and revision. CLI/web status lists disabled
devices, including retained policies for absent devices. Enable clears the
reason but keeps a revision record. Mutations use the same installed operator
authority as host drain. The CLI reads the current per-device revision and
sends `if_revision`; a concurrent edit is rejected as stale, never retried
blindly. API clients post `action`, `platform`, `identity`, optional `reason`
and required `if_revision` to `/api/hosts/HOST/devices/maintenance`; revision
zero means no prior record. Device edits preserve the host drain owner's
receipt and timestamp.

`host placement` changes the queue-owned policy for a physical host, including
all its worker/device views. It can be changed while enabled, persists across
queue restarts and worker sessions, and cannot be overridden by check-ins.
The default is `last_resort: false`. When true, ordinary admissible hosts rank
first, followed by last-resort hosts; MY-18 value ordering remains unchanged
within each group. For model calls, a ready or loading engine on a last-resort
host beats a cold ordinary host (readiness before value). If no ordinary host can admit the work in the current plan,
fallback is immediate, with no waiting or reservation. Fairness, priority,
preemption, eligibility and cleanup custody retain their existing rules.

`host status HOST` reports `policy.last_resort`; status API machine rows expose
`last_resort` when true, and CLI/web status labels the host's placement policy.
Queued job `reason` and the assigned attempt's `timing.placement_reason` explain
last-resort fallback and retain the value-ranking explanation. These reasons are
diagnostic text.

Drain preserves Started work. Disable with `--stop` requests bounded stops of
exact current attempts, preparations and engines. Issued preparation plans count
as work before delivery. Every launch rechecks queue authorization; drain cancels
unstarted plans, and started preparations retain custody until exact completion.
Upgrade queue and workers together for the current launch-gate protocol. Planned maintenance does not
consume infrastructure retries; cancellation and independently observed stage
failures still take precedence. A successful mutation is durable policy
acceptance. `--wait` requires drained/disabled state, fresh consistent known
workers and zero work, preparations, engines or pending cleanup. Timeout and
missing reports leave custody unresolved. Resume does not discharge old work.

## Outages and control timing

During queue outage, no maintenance mutation or new Started authorization can
succeed. Inspect owned work before using separately authorized service/container
tools. Preserve results and custody for reconnect; there is no offline release
guarantee. Consumer cancellation and application health remain independent.

Launch authorization and the separate Started hand-off each use the normal
ten-second control-request budget. Measured queue-lock waits reached 3.54 seconds,
exceeding the former three- and two-second deadlines. The standard budget admits
these bursts without retrying or opening either gate before a positive reply.
An unreachable queue still fails within ten seconds per request; the longer wait
costs at most seven/eight extra seconds at the respective boundaries. Exact
attempt, session, cancellation and placement checks remain authoritative.
The local exec-receipt budget starts afresh after Started approval. Authorization
failure remains an infrastructure interruption under the bounded retry policy.

A check-in from the current accepted worker session counts as contact before
waiting for the queue lock, for at most 30 seconds from its arrival. Request
cancellation retains only that original bounded credit, so a client deadline
during queue contention does not turn queue delay into worker silence (#343).
It neither refreshes `LastSeen` nor extends the arrival deadline. A noncancelled
rejection removes pending credit. An accepted check-in refreshes contact when
processing finishes. A report rejected by later validation
can therefore delay loss detection by at most 30 seconds, without refreshing
`LastSeen`; an unauthenticated or mismatched-session report receives no pending
credit. Queue capacity limits still require separate correction; start requests
that exceed their ten-second budget remain bounded failures.

Status capture batches retained fan-out cancellation membership once per capture,
including removed waves and consumers. It reuses that read-only view for scope
and release checks instead of repeatedly scanning all retained jobs/submissions
under the queue lock. The view is discarded after capture; exact execution
acknowledgements still determine release, and elapsed time never proves cleanup.

The queue emits one `event=slow_launch_authorization` line for an authorization
taking at least 500 ms. `duration_ms` covers the authorization, including the
checkpoint probe, queue-lock wait and persistence; `mutex_wait_ms` isolates waiting
for `q.mu`, and `sqlite_commit_ms` measures the SQLite commit itself (excluding
statement execution and state serialization). `ok` reports authorization success.
The line contains no worker identity, payload, filesystem path or transport error text;
it is written after releasing the queue lock. These timings are diagnostic wall
time, independent of the injected scheduling clock.

`event=slow_queue_mutex_hold` identifies expensive queue updates (including Tick,
check-ins and scheduling), status projection and batch-log retention. Every hold
is measured; the local service distribution controls which diagnostics are
emitted (`sample_every=1`). `operation` names the code operation and
`duration_ms` excludes acquisition. Logging and operation-name resolution happen
after unlocking without request identifiers. Absence of a line does not rule out
contention. These measurements use diagnostic wall time, never the scheduling
clock.

Update holds also report `records_snapshot_ms` (undo records and in-memory
snapshots), `usage_accounting_ms`, `fn_body_ms` (mutation plus health/maintenance),
`persist_commit_ms` (usage writes, state serialization and SQLite commit), and
`notify_ms` (wake notification and final commit/rollback bookkeeping). These
non-overlapping phases cover the measured hold, including failed updates.
Persistence additionally reports `persist_marshal_ms` (dirty-record encoding
and fragment preparation), `persist_sql_ms` (transaction setup/usage
writes and state statements), and `persist_sync_commit_ms` (SQLite commit with
FULL synchronization, not isolated fsync). `persist_payload_bytes` and
`persist_records` count changed state payloads/rows submitted to SQL; they exclude
usage payloads, SQL keys, WAL page overhead and later checkpoint writes. Failed
transactions can report attempted writes. These subphases do not include every
comparison or post-commit bookkeeping step in `persist_commit_ms`.

From the incremental-persistence release, normalized state v2 separates fields
larger than the database's own page size from small control fields. Unchanged
payload fields remain in place; keyed live answers reference their committed job
and retain a complete answer when that job is removed. All fields, removals and
the format marker commit atomically in the existing FULL-synchronous transaction.
Startup reads whole-state and normalized-v1 databases and converts them once;
older readers refuse v2 instead of silently losing fields. Preserve a compatible
binary with the backup when planning recovery; do not open v2 with an older binary.

Normalized state v3 stores each machine in its own record. Transaction mutators
explicitly journal jobs, machines and catalog entries before changing decisions;
unchanged records are neither cloned nor encoded for change detection. Derived
reason text and temporary measurement labels have separate in-memory journals.
Startup upgrades v2 in the same atomic FULL transaction as its first decisions.
A failed upgrade retains the previous layout and marker. Readers predating v3
refuse the new layout; recovery needs a compatible binary or a consistent backup
from before the upgrade.

A transaction runs the full maintenance planner once. Check-ins keep that pass
before assignment so deadline, loss and custody fences still apply to every
new offer. Fan-out, dependency and pause decisions then reach a fixed point
through a transaction-local worklist of changed jobs and their relations,
including changes made after planning. The final closure runs before efficiency
accounting and the same atomic FULL commit. A rejected transaction rolls back
the whole closure. Repeated maintenance requests settle that worklist without
repeating inventory-wide planning. Host-hold target/preemption planning runs
once; release-only finalization can clear a hold after the transaction's last
custody change.

`BenchmarkCheckinPersistence` reports control-hold and persistence phases,
changed payload bytes and private-fixture WAL growth for retained-history and
768-input-manifest workloads. It uses real SQLite commits and an injected queue
clock. WAL recycling is disabled only for that disposable measurement; neither
benchmark timings nor WAL growth establish live physical write rate or freshness.

## Opt-in live queue profiling

Start the queue with `--debug-listen 127.0.0.1:7332` to enable a separate
`net/http/pprof` listener. It is **off by default** and accepts only literal
loopback IP addresses (IPv4 or IPv6); wildcard, hostname and non-loopback binds
are rejected. Neither the fleet API nor its joining listener serves profiling.
Once enabled at startup, operators can profile the running queue without a
restart. An already running queue without this flag needs a future authorized
release/restart before profiling is available; there is no remote enable switch.

The debug listener has no fleet authentication. Local users can access it;
profiles and `/debug/pprof/cmdline` can contain sensitive process information.
Keep profiles private, restrict local access to the queue host, and never publish
raw profiles. Access from an operator machine through SSH forwarding, using your
configured SSH alias for the queue host:

```sh
ssh -N -L 127.0.0.1:17332:127.0.0.1:7332 QUEUE_SSH_ALIAS
```

In another terminal on the operator machine, capture one short CPU profile and
one heap profile. These commands write profiles locally, with private permissions,
without writing queue state or copying credentials:

```sh
umask 077
curl --fail --max-time 45 -o queue-cpu.pprof 'http://127.0.0.1:17332/debug/pprof/profile?seconds=10'
curl --fail --max-time 45 -o queue-heap.pprof 'http://127.0.0.1:17332/debug/pprof/heap'
curl --fail 'http://127.0.0.1:17332/debug/runtime'
go tool pprof -top /path/to/matching/myce queue-cpu.pprof
go tool pprof -top -inuse_space /path/to/matching/myce queue-heap.pprof
```

For capture directly on the queue host, use port `7332` instead of `17332` and a
private operator-owned output directory. Use the exact queue binary for symbols.
Profiling adds CPU, memory and response latency load: take a short capture during
the observed slowdown, one at a time, and compare queue timing logs before and
after. Heap capture above does not force GC; avoid `gc=1` during a latency incident
because it forces a collection. Stop the SSH tunnel when finished.
`/debug/runtime` reports cumulative `gc_pause_total_ns`, `gc_cycles`, current
`heap_alloc_bytes`, `heap_inuse_bytes`, `heap_sys_bytes` and goroutine count.
Reading these runtime statistics can itself briefly pause the process; they are
collected only on a debug request, never under the queue mutex.

## Validation reservation semantics

Acquisition is serialized with placement, launch and Started. Cached unstarted
offers are withdrawn using planned maintenance cleanup without spending an
infrastructure retry. Started commands/calls and started preparations continue.
Device reservations retain sibling-device and CPU capacity. They do not change
host drains, device maintenance, exclusions, budgets or production-hold policy;
automatic or conditional release removes only the reservation's own fence.

The authenticated API is `POST /api/reservations` with
`{id, selector, job, owner, reason, max_lifetime}`, `GET /api/reservations`, and
`POST /api/reservations/{id}/release` with `{if_revision, reason}`. Keep the
returned ID, canonical selector and revision. Retrying the same ID and binding
does not renew or resurrect it; a different binding is stale. The owner is
attribution under existing operator authority, not another authentication role.
Runtime checks may include the same options under `extension_check.reserve`
on `POST /api/jobs`; the job and reservation commit or roll back together.

Queue SQLite persists the binding, stable target, original expiry and release
receipt. Success, failure and cancellation release it automatically only after
**every known attempt** and its Started preparations have confirmed physical
cleanup. A terminal result, elapsed stop grace, missing worker or queue restart
never proves cleanup. Status
distinguishes `draining`, `ready`, `in_use`, `waiting_for_release` and `released`,
and shows `reserved for OWNER: REASON` plus pending work/evidence.

Whole-host reservations for CPU validation wait for host work and physical
cleanup, but do not wait for GPU admission to become enabled. A shared interactive
host's profile deliberately excludes GPUs; that does not prevent its native CPU
validation from becoming ready. Ordinary CPU, environment and owner-pressure
admission checks still apply.

The maximum lifetime bounds a never-ready admission fence as well as lost
bindings. If a reservation has never been ready and its bound job is still
queued with no retained execution or preparation custody, expiry marks it
`expired`: other jobs regain admission, while the bound job remains queued.
Explicitly release that record before reserving the validation again. Same-ID
replay never renews it. `ready_at` records the first observed ready transition;
`expired_at` records admission expiry, **not physical release**.

Once ready, preparing or running bound work and unconfirmed cleanup retain the
reservation beyond its lifetime. Status reports `reserved past max lifetime`
with the blocking reason; expiry never cancels work or confirms physical release.
An absent binding with no retained execution custody releases with the visible
reason `maximum lifetime elapsed`. A terminal bound job releases after every
attempt physically releases. Independent drains and exclusions still apply.
Conditional operator release changes only the reservation's admission fence and
requires its observed revision and a reason. Use indefinite `exclude`/host
maintenance for an operator-controlled hardware hold instead.

Laptop proof uses the real API and command runner with fake inventory. Start
`myce dev --workers 1 --devices 2` and use its printed queue settings; add a local
example such as `examples/extensions/http-stage`, then run
`myce extension check --reserve http-stage@1` and inspect `myce reserve list`.
No GPU is needed. `TestReservationDev` additionally queues ordinary work on the
reserved fake device and proves it runs after the bound validation releases;
`TestExtensionAgentDevEndToEnd` exercises the CLI flag with the real check runner.
