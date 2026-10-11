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
CUDA also requires exactly one visible device; its physical identity must be
verified by the operator. No CPU fallback is permitted.

The Oct 11 B70 preflight found shared display DRM descriptors in PID1, logind
and GNOME. This collector counts process groups, so their shared client trips
`physical_double_owner` before any test allocation. Preserve that hold until a
qualified collector accounts for shared display clients and independent driver
memory; do not filter their owner tokens to make the checker pass.

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
