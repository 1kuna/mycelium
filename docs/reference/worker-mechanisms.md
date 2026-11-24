# Worker mechanisms

Part of the [approved design](../design-proposal.md), moved verbatim apart
from relative links. Product contracts and maintainer rulings are unchanged.

### Inventory and resource ownership

Device IDs are unique canonical nonnegative decimal strings, stable against
hardware identity (CUDA UUID, PCI BDF or Level Zero UUID), not discovery order,
driver version or display name. Recheck runtime identity at startup and before
every member launch. XPU discovery and every member force
`ZE_ENABLE_PCI_ID_DEVICE_ORDER=1`. A mismatch withholds that device with
`device_identity_mismatch`; unavailable probes use `device_probe_unavailable`;
missing budget uses `device_budget_unavailable`. Re-evaluate these conditions;
matching identity restores eligibility. Accepting replacement hardware requires
settling the old custody, preserving its spool, a fresh `--state-dir` and native
requalification. Other healthy components remain usable.

Worker `--device 0,1` offers two independently selectable cards; omitted means
all. Unmatched allowances expose `device_selector_unmatched`; no matched card
refuses startup. Fixed XPU budgets/reserves stay per device and never substitute
host RAM or unreliable telemetry. Unified capacity uses measured memory when
`--memory-gb 0`; a nonzero allowance caps it. Live headroom and reserves still apply.

Assignment atomically reserves host cores/RAM and zero, one or several GPUs of
one host with their VRAM in SQLite before delivery. Preparation, stopping, engines and unconfirmed release remain
charged exactly once. Discrete VRAM and additional host RAM fit separately;
unified allocations share one physical pool. Host reserves, launch slack and
preparation/core hand-back are machine-wide. Lowered allowances expose debt and
withhold new work; they never resize running ceilings. Omission, elapsed grace,
service exit or missing inventory never refunds custody.

**Shared GPUs (MY-30, MY-31).**
A GPU holds several single-card batch attempts when their memory fits: the
newcomer's declared or learned device vector and its host vector must fit the
card's and host's existing headroom beside every co-tenant's charge, unified
hosts counting the one pool. A finished co-tenant stays charged until exact
release. A learned host
peak without a device measurement retains a recipe-default device figure;
attempts with that figure keep the card to themselves until
an accepted device measurement supplies their learned vector. The
envelope's `source` identifies `GPU recipe default` in this case. Model-server,
extension-check, fan-out, exploratory first-run
and multi-card attempts keep the card to themselves, as does a hint that names
no device memory (its device figure is only a default), and a newcomer never
joins a higher tier's attempt. For placement, equal tiers never preempt; a higher tier stops the
fewest co-tenants whose release frees enough on its selected host/cards and
starts there only after that exact release; a multi-card demand stops every
co-tenant on its selected cards. On XPU and CUDA cards (including unified-memory
CUDA), a higher tier instead stops every lower-tier co-tenant and waits for each
exact release even when memory would fit beside them. XPU teardown can reset the
card's engines; CUDA
co-tenants can grow past their reservations and starve a higher-tier model load
without reliable fresh per-process growth evidence. The cost is lost mixed-tier
co-residency on XPU and CUDA. Equal-tier packing, including experiments packing
among themselves, and other platforms are unchanged. Waiting reasons name
`device N shared with K attempts`. CPU attempts can run alongside GPU work and one another within host resources and caps;
model engines retain their usual shared-memory admission. `needs.gpu.devices` narrows candidate IDs, not the
number allocated. CPU work cannot have a GPU selector. Malformed/duplicate IDs,
empty explicit sets and invalid counts use `gpu_selector_invalid`. An explicit
count above one and multi-card model masks use `gpu_count_unsupported`, never
partial reservation or reduction. Unknown/offline selections remain queued with
`no_matching_device`.

Shared-card memory growth uses fresh per-process device samples from check-ins
(younger than ten seconds, using the injected clock). If the sum of each
tenant's larger measured use or reservation, plus resident model charges,
exceeds the card budget, stop the lowest tier; among equals choose the tenant
most over its reservation. If unified-memory attribution instead becomes
uncertain, prefer an unobservable tenant within the lowest tier, so another
tenant's measured growth does not make it the victim of missing evidence.
This safety stop may displace an equal tier. It
retains normal checkpoint grace and exact-release custody and requeues without
spending a failure budget. Each grower's measured peak × 1.25 becomes a durable
device-memory floor for its next placement; running reservations stay fixed.
Single tenants are unaffected. Every tenant needs fresh per-process evidence
before the guard acts. Missing or stale samples establish no growth, and the
running job's reason reports unavailable per-process evidence. Host memory-drop
estimates cannot attribute co-tenants and do not trigger this guard. At
check-in send time, workers read the sampler's latest per-attempt device
counter independently of cached actor reports. Slow actor log/result I/O must
not hide fresh device evidence. This refresh retains the sensor's original
timestamp; it cannot make a stalled sensor fresh or renew old host-memory
credit.

A successful complete CUDA process enumeration with no context belonging to
the attempt reports zero device use, including during CPU-side imports and
model preparation. Its full declared or learned device vector remains charged.
Failed queries, listed N/A counters and missing PID inventory remain unknown;
they do not become zero or extend the sensor freshness allowance.

**Cards per job (MY-30).**
`memory_gb` (declared or learned) decides the cards. Work one card of its host
can hold gets one card. Work no single card holds, but N same-backend discrete
cards of one host hold in equal shares, gets the fewest such cards: the N with
the most free memory, reserved together with one release record, each charged
`memory_gb / N`, host RAM and cores once. All are visible to every member
(`MYCE_GPU_COUNT`, `MYCE_DEVICES`); preemption reclaims them as one attempt, and
a card stopped for a waiting multi-card demand is withheld from equal and lower
tiers until it starts, while the host's usable cards can still hold it. Model-server and extension-check jobs keep one
card; unmeasured `memory: auto` first runs explore one card, and multi-card
attempts do not feed per-card memory learning. Extension work needs every card
qualified; no separate multi-card parity check exists yet. Using more cards for
speed when one fits is not planned (MY-49 removed work curves). Cross-machine gang execution
remains deferred.

### Attempt fencing and containment

Every supervisor, result, log, checkpoint, launch, Started, stop and release is
bound to `(machine, session, job, attempt)` and its selected device. Private
per-attempt environments never mutate the worker environment. CUDA uses the
assigned UUID; XPU uses the verified physical affinity mask and renumbered
selector (one assigned card is in-job device 0); Metal uses device 0. Hide other
families. CPU uses empty `CUDA_VISIBLE_DEVICES`,
`ZE_AFFINITY_MASK=2147483647` and `ONEAPI_DEVICE_SELECTOR=!*:*`, qualified by actual
runtime enumeration. Unavailable CUDA/XPU select-none evidence withholds CPU
with `cpu_accelerator_isolation_unavailable`; loss before launch requeues without
an infrastructure loss. macOS CPU work remains cooperative, with Metal isolation
explicitly `not-enforced`. No child inherits `MYCE_TOKEN`; authored environment
cannot widen device visibility. These masks are not a hostile-code sandbox.

Each native attempt has its own control/main/sidecar subtree and verified
aggregate/member ceilings before exec where supported. Engines have independent
subtrees. Attempt stop/OOM/cleanup never targets siblings or the worker service.
The worker service retains `Delegate=memory` and `KillMode=mixed`, but installed
batch commands run under an independent per-attempt custodian: a delegated
systemd scope in `myce-jobs.slice` on Linux or a separate launchd job on Darwin.
Its runner owns waits, memory/RSS guards, cache pins and durable results while
the worker owns queue communication and log/result publication. A service stop
detaches that connection; it does not signal the attempt. Private durable
records bind the exact attempt/session, custodian PID and kernel birth, and
manager placement. Startup adopts only verified custody, preserving the
original queue session; an unverified record stays held, never guessed from
a PID number. Other legacy recovery still verifies cleanup and replays results.
Resident bridges and model engines retain worker-bound custody and require
natural drain for normal upgrades. Linux attribution spans both the worker
root and detached jobs slice, so siblings remain known Mycelium processes.
Linux shared-host work retains its worker-bound aggregate CPU quota and
naturally drains for updates; detaching only batch jobs would split the hard
cap from resident engines. Unsupported platforms remain admission-only and
required ceilings refuse them. Core reservations do not otherwise enforce CPU
time.

One machine heartbeat uses its session's adaptive contact evidence for every
active class. A genuine worker loss affects every owned attempt independently,
including each production retry counter (at most two infrastructure requeues).
Unknown release stays charged; a timeout cannot authorize overlap. A single
attempt's failure does not mark its neighbors lost. Control/heartbeats and bulk
preparation/result I/O remain independent; bulk I/O never holds the queue lock.

Spool reopen reconciles every saved attempt and buffered result before new
admission. `recovering: true` blocks batch, model and preparation admission at
planning and launch without allocating an attempt or consuming a retry. Exact
cleanup/replay continues; all machine targets are blocked while other machines
remain eligible. Persist `recovering_since`; continuous recovery reports
`recovery_needs_attention` after two minutes, with no heartbeat reset. Clearing
recovery clears the warning. Legacy GPU custody lacking device evidence blocks
all GPU targets until exact cleanup; do not invent a component.

### Placement, protocol and compatibility

Use one shared planner for jobs, models and status. Enumerate
(machine, device) GPU targets and (machine, no device) CPU targets. Qualification,
engines, pins and readiness are exact-device identities; a neighbor's check never
qualifies the selected card. Disk weights may share caches, not engine custody.
Policy/allowlists/platform/device restrictions intersect. Prefer free capacity
before stops, retain single-sufficient-victim/greedy/reverse-prune selection and
its nonoptimality, and count useful host resources only once. Fairness remains
allocated device-seconds or CPU core-seconds; grouping cards resets no usage/caps.

Pending stops are planning projections only. For machine workers, lower-tier
grace/kill releases have credit only before `StopAt + preemptionGrace()` (ten
seconds by default). Confirmed exits retain credit until fresh inventory or that
same bound. Neither heartbeats nor completion extend it. Then seek useful
capacity elsewhere; admission still requires exact cleanup acknowledgements.
Physical-host production holds fence every device and CPU resources; an opt-in
device scope keeps their release custody and leaves reclamation to this planner. Maintenance
outranks holds, preparation and Started. Planned maintenance/preemption preserves
infrastructure counters; independent stage failure remains failure, completion
wins over stop intent, and cancellation wins over both.

`host-launch-v3` plus echoed `attempt_protocol: keyed-attempt-v1` carries a
collection of exact keyed reports/controls without a wire cardinality cap.
Strict decoding refuses mixed forms, duplicates, invalid sessions or revision
mismatch before mutation. Keyed workers require the echo and fail closed with
HTTP 426 `worker_protocol_invalid` against older queues; upgrade the queue first,
preserving saved custody. The bounded current unkeyed v3 adapter remains for
supported old workers, restricted to one attempt/device and the common host
ledger. Pre-v3 is refused. No automatic downgrade. See [wire details](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/development.md#current-worker-protocol).

New assignments use physical `machine` plus additive `allocated_devices` (empty
for CPU). Retired identities exist only for exact historical result/release
handling and explicit replacement diagnostics. New intent naming them fails
`machine_renamed`; never infer aliases from `-gpuN`. Existing keyed replay returns
the original logical job before new-placement validation and preserves its
fingerprint, counters and checkpoint generations. Legacy sessions and a machine
worker cannot coexist on the same host. The unkeyed adapter can retire only after
all hosts convert and the rollback window closes; historical custody remains.

Split-worker conversion and rollback are below the [MY-42 release floor](../DECISIONS.md#my-42)
and remain only in source history. Retained historical identities and exact custody
still bind. Keep native device isolation, reboot/manager, driver fault and
simultaneous memory qualification separate from laptop proof in
[development](../development.md#native-acceptance-checklist).

