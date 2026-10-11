# Verso synthetic pressure tools

Standalone test tooling; no queue/worker/scheduler/product code changes. Uses the
existing Python `Checkpoint` and `StopSignal` helpers. Python 3.10+ and Unix flock
are required. CPU tests need only the standard library. XPU/CUDA jobs require an
already-qualified PyTorch runtime; this tooling installs nothing.

## Local verification

```sh
python3.13 -m unittest discover -s scripts/pressure -v
go build -o /tmp/myce-pressure ./cmd/myce
python3.13 scripts/pressure/pressure.py smoke --binary /tmp/myce-pressure \
  --events /tmp/myce-pressure-UNIQUE/events.jsonl
python3.13 scripts/pressure/pressure.py check \
  --events /tmp/myce-pressure-UNIQUE/events.jsonl
```

The smoke directory must not exist. Smoke starts only its own loopback `myce dev`
with one fake 8-GiB worker and fresh SQLite/checkpoint/spool paths. Commands
allocate 8 MiB real host RAM. It forces competition with an 8-GiB experiment and
an 8-GiB production reservation, proves three attempts across two successful
jobs, and requires resumed completed progress. It terminates only its own dev
process. It is not a hardware test. Evidence persists; move disposable evidence
to Trash when done. Smoke does not implement fault injection or a soak.

## Prepare a GPU1 job without submitting it

This example prints JSON only. The runtime, paths, worker name and reservations
are provisional until the orchestrator approves/calibrates the GPU1 window.

```sh
python3.13 scripts/pressure/pressure.py spec --backend xpu --card 1 \
  --machine b70-pressure-gpu1 --python /APPROVED/XPU/bin/python \
  --events /APPROVED/LOCAL/RUN/events.jsonl --units 300 --unit-seconds .2 \
  --host-mib 512 --gpu-mib 1024 --memory-gib 4 --host-reservation-gib 1
```

Generate the eventual native spec from the approved staged B70 checkout, so its
command uses that worker's actual absolute path, including `clients/python/src`.
The Mac example is preparation only. Do not assume submit copies
code or inputs. Explicit worker `--device 1` must map to physical GPU1; Mycelium
sets `ONEAPI_DEVICE_SELECTOR=level_zero:1` and `ZE_AFFINITY_MASK=1`. The job
requires those values and exactly one visible GPU, then uses logical `xpu:0`.
Native topology/selector verification is still required before allocation.
With the r2 kernel fence exposing only GPU1 nodes, the native library enumerates
that physical card as index0. Use the qualified `--card 1 --selector 0` pairing
and worker/measure `--device 0` **only inside that fence**; fdinfo must still
prove PCI07:00.0. The full-host index1 examples do not apply inside the fence.
CUDA also requires exactly one visible device; its physical identity must be
verified by the operator. No CPU fallback is permitted.

The collector de-duplicates descriptors per PID/start identity. `drm_inventory`
records executable, cgroup and unique node/client identities without argv/env.
An operator-pinned `drm_owners(..., baseline=..., allowed=...)` excludes only exact
pre-existing display identities from allocation counts and reports their measured
resident memory, de-duplicated by shared DRM client. Unknown processes, changed
clients, missing baseline identities and unreadable evidence fail closed. Pin the
baseline only after read-only qualification; never learn it during a workload.
The standalone `physical` mode remains conservative and includes all holders;
native orchestration must provide pinned baseline and current approved processes.

GPU jobs touch a uint8 payload plus three 1024-square float32 matrices and run
matrix multiplication with synchronization throughout each unit. Host bytearray
pages are touched. Explicit payload sizes exclude interpreter, allocator,
driver/pinned memory and matrix/runtime overhead; declare measured conservative
GPU and host reservations, then impose independent approved aggregate limits.
Do not use zero host reservations for native work. Allocation is released before
the finish receipt; native driver release still requires independent proof.

After approval, `submit` accepts the same flags as `spec`, uses explicit
`--queue`/`MYCE_QUEUE` and private `MYCE_TOKEN`, records the acknowledged job ID
and submission time, and prints only the ID. It never retries an uncertain POST;
reconcile it before retrying. `--priority production --units 1` makes a fake
production job. Experiments are resumable; `job --resumable` handles manual
command execution. `--refuse-stop` ignores the stop request while still saving
periodically, for an approved force-stop test. Non-checkpointable experiments
remain outside this test queue unless their loss/restart is explicitly accepted.

## Invariants and continuous observation

`check` streams records; `watch --queue URL` reads status every 0.5 s and follows
the ledger incrementally. It writes only IDs/states/attempts/machines and Linux
whole-host MemTotal/MemAvailable, never command/env/log contents. `watch` needs
the token only in its private caller environment; jobs reject its presence.
All writers use a locked, fsynced append. Use one local B70 ledger for native
tests; NAS checkpoints remain separately attempt-isolated. Stream each record
with a 1-MiB bound. Do not mix remote clock domains in one ledger.

The checker detects lost saved units, repeated completed units, invalid unit
numbers, duplicate attempt/submission, missing jobs, terminal regressions,
incomplete success, queue double ownership, and start latency above `--bound`
(default 30 s). This is **command start**, not model readiness. Faults/outages do
not suspend the bound. `--minimum-available-gib` defaults to 4 and checks observed
Linux headroom; it does not enforce a memory limit or establish a measured
production reserve. Non-resumable recomputation can intentionally trip the
duplicate-work invariant. `finish()` also requires terminal state for every
submitted job; live observation does not require jobs to be terminal yet.

Test commands take nonblocking card-specific advisory locks and report an
overlap before allocating. This proves only participating test commands did not
overlap; it cannot certify arbitrary Docker processes or the GPU driver.

For hardware acceptance use `--require-physical --pci APPROVED_BDF` on the
checker/watch, repeating `--pci` for every tested card. Missing scope fails.
The read-only Linux collector
is `physical --pci APPROVED_BDF` (repeat `--pci` for both cards). It requires
existing PCI DRM paths and sufficient read permissions, walks DRM descriptors,
uses numeric group/start identities, and hashes owner tokens. It reads no
command lines or environments and sends no signals. Unsupported/malformed or
unreadable identity produces `valid=False`. Run it in a separately approved
read-only privilege scope; do not silently omit inaccessible processes. Native
PCI/fdinfo and permission qualification is still outstanding.

Another qualified collector may append the same records via `pressure.emit`:

```python
emit(events, "physical", valid=True, owners={"physical-card-1": ["attempt-token"]})
```

`owners` includes **every physical card in the approved test scope**, maps each
card to distinct allocation-owning attempts (not raw process count), and includes
unexpected owners. Use `valid=False` for unreadable/uncertain identity. Empty
lists require positively verified absence and settled delayed starts. Evidence
must share the local boot/monotonic clock and refresh within 3 seconds. This
collector is implemented but native qualification is an explicit prerequisite. The
checker refuses missing, stale, unknown or overlapping physical evidence.
There is no supported automatic pass across a reboot: a changed boot produces
a hold requiring retained-state/offline reconciliation and a fresh phase ledger.

Checkpoint identity binds the job, workload contract, completed counter and
digest. Corrupt or changed saves fail before allocation. Atomic complete saves
use the existing helper; `.tmp` files are ignored, save failures retry visibly,
and only a durable save counts as a completed resumable unit. Unfinished work
may repeat after KILL; completed work must not. Receipts cannot prove unobserved
external effects exactly once. Evidence and checkpoint access errors fail visibly.

Exit 0 means final checker/local smoke pass; 1 means an observed invariant
violation; 2 means invalid/missing/malformed evidence or a harness failure.
Inspect the retained JSON ledger and result, not exit 0 alone. `watch` stops on
the first violation and does not kill, restart or otherwise repair any service.
The existing `scripts/soak/soak.py` remains unchanged and provides CPU-only random
fault orchestration. Native Docker/NAS/reboot injection and the two-card/idle
policy tests remain in the orchestrator-approved pressure plan, not in smoke.

The detached `stage_ab.py` runner is pinned to the r2 paths/unit and GPU1-only
kernel policy. It writes per-scenario ledgers/results and terminal markers, uses
18-GiB observed host headroom plus 8-GiB cgroup cap, and refuses unknown users.
`finalize_stage_ab.py` is its ExecStopPost cleanup, including recoverable Trash.
CPU-only overlay tests and receipt-triggered GPU faults are reported separately;
polling receipt boundaries do not qualify every internal gate/fsync crash point.

The native counter CLI takes about 1 second and therefore runs separately from
the 0.5-second physical/status/headroom observer. Only the exact operator-launched
passive counter PID/start/executable/cgroup is exempted via `observers`; all its
DRM clients remain in the inventory. This is distinct from the pinned display
baseline. Unknown or reused counter identities still fail closed.

Rerun2 source is Mycelium06fbe0b. Its generation-aware Python client is
used from the frozen source tree; six skew and missing-NAS probes now run as
normal pass/fail checks. Whole-machine production hold is excluded. During
measurement, direct xpu-smi stats children of the exact live myce-measure
process may observe the card; PID/start, executable, arguments and cgroup
are checked. Their holder evidence remains recorded.

The de2365b retry uses the r2b folder/unit. Counter registration and the
physical scan share a lock; process allow-lists are resolved after collecting
holders. Ownership failures retain sanitized process/device evidence.
Approved follow-ons use host-config production_hold=true, production_idle=180s,
one B70 GPU1 worker only, and a Spark worker with unified38GiB reserve (24GiB
Atlas plus14GiB existing Verso loan). Portable checkpoints have an invariant
contract independent of backend; allocations are16MiB. The detached Mac
transport supplies loopback SSH forwarding and cleans up its exact worker,
ports and install. Cross-host monotonic receipts retain original clocks and
RTT uncertainty. Full-Spark uses a38GiB cap and38GiB reserve, reporting zero.

Round3 uses fresh r3 paths and starts at A3; completed r2e A1/A2 and the two
fixed-defect probes are retained, not repeated. ENOENT/ESRCH descriptor races
rescan the process fd directory; vanished processes rescan proc. Permission
failures, unresolved live identities and unknown GPU holders still fail closed.
Spark active capacity is40GiB minus38GiB reserve (2GiB), enough for the
portable job’s1GiB device plus0.1GiB host request; full stays38 minus38.
