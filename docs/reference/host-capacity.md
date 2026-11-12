# Host RAM and CPU capacity

How host RAM is admitted and enforced, the optional privileged GPU telemetry
source, and CPU allowances. Change allowances with the
[queue maintenance commands](../operations.md#queue-maintenance).

## Host RAM admission and ceilings

The separately installed [root broker templates and helper wrapper](../../deploy/gpu-telemetry/README.md)
implement the Unix-socket contract in follow-up PR #422, using fresh fixed-inventory
reads after each request. They do not set the worker flag or restart the worker.

### Optional privileged GPU telemetry source

Discrete xe/XPU host-envelope learning requires both per-process DRM fdinfo and
runtime xe GTT-manager accounting. By default the worker reads debugfs with its
existing permissions. If those reads are unavailable, sizing stays conservative.
An administrator may instead provide an existing read-only telemetry executable:

```sh
myce join --gpu-telemetry-helper /usr/local/libexec/gpu-telemetry-client ...
```

The same optional flag is saved by worker installation as `gpu_telemetry_helper`
and preserved by upgrades. An explicit empty value disables the helper. The
path must be absolute. The worker executes that exact path directly, without
shell expansion, arguments or stdin, from `/`. Its environment contains only
`PATH=/usr/bin:/bin`, `LANG=C`, and `LC_ALL=C`; it receives no fleet token or job
environment. The executable is operator authority, never part of a job spec.

#### Root-owned broker and unprivileged client

The privileged-source arrangement works with `NoNewPrivileges=yes` on the
worker; it does not require changing an existing `NoNewPrivileges=no` setting.
Inspect the actual service policy rather than assuming the installer default.
A separate administrator-owned broker reads debugfs; the worker's helper
is an unprivileged client. The binary provides `myce gpu-telemetry-client`, which
connects to the Unix stream socket **`/run/mycelium/gpu-telemetry.sock`**. An
operator-owned wrapper at the configured helper path runs:

```sh
#!/bin/sh
exec /usr/bin/env -i PATH=/usr/bin:/bin LANG=C LC_ALL=C /absolute/path/to/installed/myce gpu-telemetry-client
```

The absolute binary path is chosen at installation. `--socket PATH` on the client
command supports an explicitly administered alternative or a laptop test socket;
it is never derived from a job. The helper receives no arguments from the worker.
Neither process invokes sudo, setuid or a privileged executable. This client
needs no queue connection or credentials. Mycelium installs neither the wrapper
nor the broker, and changes no service privilege policy.
The separately installed [reference broker](../../deploy/gpu-telemetry/README.md)
implements this contract with fixed administrator-configured debugfs sources.

The broker contract is:

1. Listen on `AF_UNIX/SOCK_STREAM` at `/run/mycelium/gpu-telemetry.sock`. The client
   connects and immediately closes its write half. **Zero request bytes followed
   by EOF** is the entire request. Reject nonempty requests. There are no requested
   paths, PIDs, commands, identities or device arguments.
2. After accepting that request, take a UTC timestamp immediately **before the
   first debugfs read**. Freshly read `/sys/kernel/debug/dri/<PCI>/gtt_mm` for all
   configured xe cards. The broker owns this fixed source inventory; clients
   cannot alter it. `<PCI>` is a full lowercase domain address, for example
   `0000:01:00.0`; resolve numeric debugfs directories to their PCI identities.
   Return raw text containing `use_type: 1`, `use_tt: 1`, and
   `usage: <unsigned integer bytes>`. No net delta or VRAM substitution.
3. Assemble the entire JSON reply before sending. On any read failure, unsupported
   card or incomplete counter set, close without a snapshot. On success send
   exactly one JSON object, at most 64 KiB, and close the connection. No banner,
   status envelope or trailing stream. Use a 150 ms total connection deadline
   from accept, including receipt of request EOF and sending the reply, leaving
   time for client startup and delivery inside the worker's existing 200 ms
   collection window. Do not serve a cached or periodically refreshed
   snapshot and do not timestamp old data as fresh.

The required JSON fields are:

```json
{
  "sampled_at": "2026-11-02T12:00:00.123456Z",
  "gtt_mm": {
    "0000:01:00.0": "use_type: 1\nuse_tt: 1\nusage: 167231488\n",
    "0000:02:00.0": "use_type: 1\nuse_tt: 1\nusage: 87408640\n"
  }
}
```

`sampled_at` is a UTC RFC3339 timestamp with subsecond precision, on the same
host clock as the worker. `gtt_mm` maps each full lowercase PCI address to its
unmodified debugfs text. Include every card an attempt can use: the worker
requires its selected card and every other card present in its DRM clients.
The reference broker also supplies optional `vram` entries keyed by the same PCI
identities, each with unsigned `total_bytes` and `used_bytes`. These come from a
read-only Xe memory-regions query with uncensored usage evidence;
GTT storage is never used as VRAM. Missing, censored or invalid entries withhold
telemetry-dependent sharing and never trigger vendor-tool fallback. Ordinary
configured-budget admission is unchanged. The [broker installation guide](../../deploy/gpu-telemetry/README.md)
describes the read-only DRM policy and older-kernel accounting limitations of
this additive source. Existing GTT-only brokers remain usable for host RAM accounting.

The helper forwards the broker's timestamp and text unchanged. A timestamp
before the current helper invocation or after it finishes is rejected, so a
cached snapshot is not accepted even when recent. A malformed, absent or stale
reply, missing card, nonzero helper exit, oversized response or deadline expiry
makes the attempt's GPU telemetry incomplete. A configured failing helper does
not fall back to another source. The client emits no stdout until broker EOF
and the size/deadline checks succeed, even for a complete-looking JSON prefix.

Install the broker's binary, configuration and service unit as root-owned and
unwritable by worker/job accounts. Use a root-owned `/run/mycelium` directory
with the configured client group and mode **0750**, and a socket owned by
`root:<client-group>` with mode **0660**. Render `REPLACE_CLIENT_GROUP` identically
in the broker socket and tmpfiles templates. Prefer the worker's existing primary
group, avoiding account mutation and user-manager credential refresh. The
group's socket write permission permits connecting, not changing the broker's
sources. The parent directory must have no worker/group write permission, so
clients cannot replace the socket. Keep the wrapper and executable chain
root-owned, mode **0755**, in directories unwritable by workers/jobs. The broker
uses no queue authority or fleet token and provides no filesystem-write or
administration operation to clients. Verify these owners/modes after every
broker restart. These are operator-enforced installation requirements: the
client does not check filesystem ownership/modes or authenticate the peer. It
only bounds and forwards the reply; the worker parses the JSON and validates
freshness, participating cards and counters before using it as evidence.

Activate only after separately installing and qualifying a conforming broker:

1. Read `Uid`, `Gid`, and `Groups` from the actual worker's `/proc/PID/status`.
   Select its existing primary group (or an existing supplementary group already
   held by that PID), and resolve the numeric GID with `getent group`. Render
   `REPLACE_CLIENT_GROUP` in both broker templates with that group. Create the
   root-owned directory and socket with the matching group and modes above;
   verify them after boot/socket recreation. No account change is required.
2. Verify access with one fresh complete two-card client call under the actual
   worker PID's exact numeric Uid/Gid/Groups and service restrictions. A fresh
   login shell alone is insufficient. A worker-only restart does not refresh a
   persistent user manager's supplementary groups; using its existing primary
   group avoids any user-manager restart. Existing installations may use
   a `60-worker-primary-group.conf` socket drop-in and matching tmpfiles override.
   Preserve applicable overrides until deliberately replaced; see the [template instructions](../../deploy/gpu-telemetry/README.md#existing-primary-group-override-444).
3. Qualify repeated wrapper calls under the service's actual user, groups and
   restrictions, and additionally under `NoNewPrivileges=yes` if the service
   currently uses `no`. Preserve the existing service policy; no sudoers grant
   or privilege relaxation is needed. Check fresh complete per-card replies,
   failure behavior and broker resource use before enabling the helper.
4. With a compatible release installed, use the normal drained in-place worker
   installation procedure, selecting the saved `--config WORKER_CONFIG` and
   adding `--gpu-telemetry-helper /absolute/wrapper`. Inspect `--dry-run` before
   applying it, preserving saved settings. Inspect the saved config and running
   service, then qualify real attempt telemetry before claiming reduced sizing.

The client has its own 200 ms connection/read deadline and 64 KiB response limit;
the worker separately enforces its full driver-collection coverage window.
Resident/pressure sampling remains independent. The same manager parser and
conservative absolute manager-plus-client accounting apply, including overlaps.
Missing evidence is unknown, never zero. Complete fresh repeated local samples,
pressure rejection, retained floors and native concurrency/output proof remain
required. Clearing the helper on a host without readable debugfs restores
conservative sizing for new evidence; it does not revoke already learned history.

### Admission and enforcement

Declare batch `needs.host_memory_gb` or catalog `host_memory_gb` in GiB for
additional CPU-side memory, including the measured load and overlapping
activation peak. Qualify the actual wrapper and workload. Unset/zero retains
GPU-only admission on uncapped hosts. A configured host ceiling budget rejects
zero host declarations, including on sibling workers. Host requests require
valid live RAM telemetry.

All per-card workers on one physical machine must share a stable `join
--host-id` (default hostname); use unique host IDs across different machines.
Linux uses `MemAvailable`; macOS uses free plus inactive pages from `vm_stat`.
`--reserve-host-memory-gb` (default 0) withholds extra OS/other-workload headroom;
on unified memory the larger of it and `--reserve-memory-gb` applies once.
The queue uses its durable host memory policy, within every worker's acknowledged
allowance, and subtracts every outstanding charge (below), including loading models
and stopping/draining work. A slower/stale sibling
report can temporarily reduce admission until its next check-in. This is
aggregate admission; external allocations can still race it. Workers repeat
live refusal after preparation and before batch/model starts.

Each host keeps one memory ledger and one rule: room = capacity − Σ charge.

- Capacity is the smallest live worker reading less one margin: the largest of
  the explicit host reserve, the measured idle burst, the unified CUDA physical
  floor and the interactive owner reserve, applied once, never stacked. It is
  lowered to the host's one allowance, which workers enforce at launch.
- A running attempt is charged its measured use plus forecast growth, bounded by
  the ceiling that contains it (its cgroup or sampled guard, else
  physical RAM). Measured high water is always kept. Live availability already
  excludes resident memory, so that is credited once.
- An attempt keeps its declared or learned envelope as the least it is charged,
  unless it may lend the unused part: it is started, contained by a guard,
  freshly sampled and has accepted history for this work and target (or is a
  cold probe with complete evidence), and it is not on the shared interactive
  host. A lender that grows back is stopped before the kernel would.

Placement, release planning, exploratory sizing, fairness and backfill all read
room after the releases they plan: each released owner returns its charge. A
started unified CUDA attempt also returns its device memory to the live side,
since no cgroup sample proves it. Admission still waits for the confirmed
release and the worker's live recheck.

On dedicated GPUs the host pool charges only `host_memory_gb`. On unified-memory
machines, GPU and CPU allocations share one host pool:
charge `memory_gb + host_memory_gb` once per allocation, not two independent
RAM budgets. `host_memory_gb` means additional CPU-side memory, never the
combined GPU-plus-CPU footprint. Existing GPU-only allocations also reserve
their GPU portion against that pool when placing a RAM-requesting neighbor.
GPU admission remains a cap; passing both checks does not charge a second
GPU allocation. The OS reserve is the larger GPU/host reserve, applied once
to the shared allowance. Auto CUDA detects unsupported NVIDIA memory fields;
explicit `--memory-mode unified` and Metal always use this rule.

Linux cgroup v2 workers installed as systemd user services enforce declared
ceilings. The generated and manual worker units use `Delegate=memory cpu` and retain
`KillMode=mixed`. At startup the worker moves itself into the service's
`worker` leaf, then enables the delegated memory controller in the service
parent. Each native batch attempt and model-server execution gets a unique
`attempt-<execution-id>` or `engine-<execution-id>` child of that same parent.
The worker writes and reads back `memory.max`, `memory.swap.max` and
`memory.oom.group=1`, then confirms the gated process's `cgroup.procs` membership
before releasing the command. Model stop helpers share the server's ceiling.
No attempt is moved into a separate systemd scope. Ordinary service stop contains
the worker and every attempt child. Queue maintenance waits for exact cleanup,
including child cgroups; an elapsed stop deadline cannot release memory.

`join` and `install worker` accept:

- `--host-memory-budget-gb`: initial queue allowance input, default `0` for
  uncapped admission-only operation. The first report bootstraps queue policy;
  subsequent worker reports cannot change it. Configure a positive allowance
  with `host budget HOST --memory-budget-gb GiB` while drained/disabled.
  Both cards receive it through checkin; until both acknowledge it, admission
  stays within the lower of the policy and their acknowledged values.
  Retain all attempt/model charges, slack and unconfirmed releases.

- `--host-memory-budget-measured`: opt in to a measured allowance (next
  supporting queue and worker release). Keep a positive `--host-memory-budget-gb`
  as the authored fallback. Zero keeps its legacy uncapped meaning when this
  mode is off. On an existing host, queue policy remains authoritative: drain
  and confirm idle custody, then use
  `myce host budget HOST --memory-budget-measured=true` to retain the existing
  positive budget as fallback. `--memory-budget-measured=false` restores that
  fixed fallback. A numeric `--memory-budget-gb` sets a fixed budget unless
  measured mode is also explicitly selected in the same command.

  Update saved configuration only through the supported installer overlay:
  `myce install worker --config WORKER_CONFIG --host-memory-budget-gb FALLBACK_GIB --host-memory-budget-measured=true --dry-run`,
  inspect the plan, then repeat without `--dry-run` during an authorized idle
  restart. Preserve version pins, reserves, device maintenance and quarantine.
  Changing saved configuration alone never overrides existing queue policy.

  The measured allowance is detected physical RAM minus the seven-day maximum
  qualified idle physical usage minus a reserve. The reserve is the larger of
  the worker's existing host reserve and observed idle peak-to-typical variation.

  Each host has a role (MY-45), set with `myce host role HOST --role ROLE`
  while enabled; it is kept with the queue's host policy and defaults to `node`:

  - `node`: a managed on-demand machine with nothing persistent on it.
    Capacity is live availability minus one margin. The allowance is physical
    RAM minus the idle p50 minus that margin, the larger of the worker's host
    reserve and the observed idle burst above p50. That never leaves less than
    the observed idle maximum and counts neither the burst nor the reserve
    twice; the live bound subtracts the same margin from current availability.
  - `services`: the host running every project's always-on services, with its
    accelerators still schedulable. Those services are persistent measured
    external demand, so the baseline is their idle p95 instead of p50, with the
    same single margin.
  - `interactive`: the agents' host. It requires the shared-interactive profile
    (`profile --shared=true` sets this role; `--shared=false` returns the host
    to `node`) and keeps the rule above plus the profile's limits.

  A worker that runs with `--daily-use` or is a laptop keeps the rule above
  whatever its role. The idle baseline already contains the OS, drivers, other
  resident services and the idle worker, so their measured use is kept back
  without a separate estimate. The explicit reserve stays an operator override
  for overshoot that is not yet measured, such as GPU allocations on unified
  memory or the Darwin sampling interval; lower it in the worker's saved
  configuration once that overshoot is measured. `host_memory_derivation`
  reports `role`, `dedicated`, `baseline_quantile_gb` and `reserve_source`
  (`explicit_reserve` or `idle_burst`).
  Raw availability already excludes unavailable OS pages: Linux `MemAvailable`
  preserves kernel low watermarks; Darwin excludes active, wired and compressed
  pages. Idle usage includes Mycelium control overhead, so RSS subtraction cannot
  double-credit shared pages. This deliberately leaves observed burst room
  unused, while preserving the operator's protection floor and live pressure,
  growth, execution-ceiling and release-custody guards. It does not bound unseen
  future external demand or make sampled Darwin containment a kernel guarantee.
  Idle histogram quantiles use each bin's minimum as a conservative lower bound;
  exact observed maxima remain intact. Quantization may enlarge the variation
  reserve, but cannot erase variation within one bin, including after compaction.

  Only new `external_memory.quiescent_v1` history qualifies. The worker must
  report no local attempts (including cleanup), models or preparation, and the
  queue must reconcile zero whole-host obligations and no live sibling worker.
  Existing RSS-residual history cannot establish ownership of GPU/driver pages,
  detached processes or multiply counted shared pages; it remains diagnostic.
  Its old peaks and histograms cannot reconstruct historical idle-window counts.
  The new `24h`/`7d` summaries report qualified sample and observed window counts;
  a window starts on an observed idle transition or after lost contact, not at
  every heartbeat. Their absence means unknown coverage, not no historical idle.

  While qualified history or the current versioned physical observation is
  unavailable (including history expiry), the existing authored budget remains
  active; learning never requires stopping admission to force an idle window.
  `host_memory_derivation` in machine JSON names `authored_fallback` and its
  reason, or `quiescent_v1` with baseline, reserve, allowance and coverage.
  A genuinely derived zero means exhausted capacity, never uncapped. Each
  worker enforces the allowance it last acknowledged, so until every worker
  acknowledges a change the queue admits within the lower of the new allowance
  and the acknowledged ones: a rise applies after acknowledgement, a fall at
  once, and admission never pauses. `host_memory_policy_mismatch` reports a
  pending acknowledgement of an authored value or of the measured mode (not of
  each measured value) by a live worker, for host readiness (`myce host status`
  and `--wait`); it is not an admission gate. Existing work retains
  custody if a new observation tightens the allowance.

- `--memory-ceiling-slack-gb`: extra memory per declared allocation; default
  `0.125` GiB (128 MiB). Slack counts against the aggregate budget. Unified
  machines enforce `memory_gb + host_memory_gb + slack` once; dedicated cards
  enforce `host_memory_gb + slack`.
- `--memory-swap-max-bytes`: swap allowance per attempt; default `0`. Swap is
  separate from the RAM budget; leave it zero for the no-swap envelope.

The kernel can OOM-kill an undersized attempt. The worker reads
`memory.events oom_kill` and reports `exceeded its <actual limit> GiB memory
ceiling`, kills remaining descendants, confirms release and removes the empty
kernel cgroup. This is a normal command failure: jobs become **failed**, with
no infrastructure loss and no automatic retry. It preserves existing checkpoint
files. A pinned model retains its pin and follows the existing bounded load
retry backoff; an unpinned failed load is removed. Ready model-server OOMs are
also detected at heartbeat and reported as failed after cleanup. Cleanup failure
retains RAM custody instead of granting a new allocation.

macOS, cgroup v1, missing delegation and foreground `myce join` retain
admission-only execution. Startup logs and `myce status` say exactly
`memory ceiling not enforced on this host (reason)`. Installer and doctor inspect
the live service's delegation and controller; startup can still be in progress,
so refresh worker status before claiming enforcement. Doctor reports this as an
advisory capability, not a required laptop prerequisite. If a qualified worker
cannot install/confirm an attempt's limit or membership, it refuses that payload;
it does not silently run it without a ceiling.

Containers a payload starts through a host container runtime run outside the
worker service, so the native cgroup cannot contain or certify them.

Preserve the conservative existing value as UNQUALIFIED input. Change queue
allowance only during drained/disabled maintenance after checking retained work.
Native capacity requires separately authorized simultaneous-envelope and
headroom measurement; laptop fixture values do not qualify native admission.

The budget bounds admitted attempt/server RAM ceilings, not every byte used by
the worker, environment builds, copies, Docker daemon or unrelated software.
Reserve measured overhead separately. Admission-only hosts do not provide a
kernel bound. GPU/device allocations that the driver does not charge to memcg
still require native qualification, especially on unified-memory hardware.
No configuration prevents unrelated programs from allocating their own RAM.

Optional CPU-only native check: on Linux with an already delegated cgroup v2
parent and the test runner in its `worker` leaf, set
`MYCE_TEST_MEMORY_CGROUP=/sys/fs/cgroup/.../OWNED_PARENT` and run
`go test ./internal/myce -run '^TestMemoryCeilingLinuxIntegration$' -v`.
It creates an owned 32-MiB child, attempts a 128-MiB allocation, checks kernel
OOM evidence and removes that empty cgroup. It skips without explicit delegation;
no fleet or GPU access is needed.

Production can stop lower-priority commands or drain unpinned models on another
card of the same host when that releases RAM; GPU reclaim remains card-local.
Pinned models and failed unloads remain protected. Reservations follow the
existing attempt, loss and restart reconciliation rules. Status shows host RAM
headroom/probe errors and a plain RAM waiting reason.

For several warm audio models, use `measure --warmup WARMUPS.json
--warmup-parallel N` with representative requests, stopped local join and the
exact worker spool. Loads stay serial; N bounds requests after all models load.
`--load-timeout 10m` can qualify slow startup (default 5m). Reports separate
load and warm-up seconds and record the combined sampled peak. Brief spikes
can be missed; real CUDA/XPU qualification remains necessary.

## CPU capacity and allowances (R1)

`myce join --gpu cpu` enrolls a GPU-less Linux/macOS worker without GPU probes.
Bare join/install selects CPU when supported GPU detection finds no accelerator;
a detected GPU with incomplete/ambiguous capacity still needs an explicit fix.
The CPU target reports logical CPUs and measured system RAM, with no fabricated
accelerator. `myce dev --gpu cpu --workers 1` exercises the real API/runner.

Set `--reserve-cpu-cores 1` (default) to leave logical cores for the owner.
`--cpu-budget-cores N` caps the remainder; zero uses detected cores minus
reserve. These options are also durable install settings. The existing
`--host-memory-budget-gb` and `--reserve-host-memory-gb` govern RAM.

The queue adopts the initial CPU allowance, then owns it alongside RAM:

```sh
myce host drain HOST --wait
myce host budget HOST --cpu-budget-cores 4 --memory-budget-gb 8
myce host resume HOST
```

Allow each worker view to acknowledge the policy; until then admission stays
within the lower acknowledged value and host readiness reports the mismatch. Status shows free/admitted logical cores and unavailable-capacity
reasons. CPU/GPU views sharing `host_id` share both ledgers. Pending releases
retain cores/RAM until exact cleanup is acknowledged. One attempt per worker
remains the R1 execution limit. Core counts reserve capacity, with no CPU-time
kernel enforcement in R1. Existing Linux memory ceilings and macOS admission-only
semantics apply unchanged. Native Windows/WSL CPU qualification is later work.
