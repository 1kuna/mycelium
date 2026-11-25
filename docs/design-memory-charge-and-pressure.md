# Memory charge, learning and unified-memory counting

Status: **draft, revision 2, for orchestrator approval; do not implement or
merge.** Issue [#2086](https://github.com/1kuna/mycelium/issues/2086).
Pressure-stop learning moved here from
[#2091](https://github.com/1kuna/mycelium/issues/2091). Revision 2 answers the
independent review of revision 1 (PR #2114); the disposition of each finding is
on the PR.

This note amends the approved [memory admission design](design-memory-admission.md)
in four places: the floor after a limited run (its section 2.3), what a stop
teaches (2.6), unified counting (2.2, 2.9) and how an exclusive head waits
(2.4, 2.11). Everything else there stands, including rule Q, the pressure
trigger, the stop order, containment (2.5) and the discrete-card rule that new
accelerator work runs alone on its card. Pressure *detection* stays exactly as
on main (#2091, review round 1 of #2106).

## Why one design

Four PR rounds (#2089 twice, #2098 twice, #2103 once, #2106 twice) each changed
`resolvePooledSize`, `fitPoolCharge` and the evidence projection separately.
Each fix opened a different under- or over-charge path, because the three
functions disagreed on three questions. Which memory figures belong to the
attempt? How are cgroup and device memory combined? Which inputs may lower a
charge? This note answers each question once. The implementation then has one
footprint function, one charge function and one learning record per attempt.

## 1. Counting

Every figure is **attempt-scoped** (it counts only the attempt's own processes
or cgroup) or **host-wide**. Host-wide figures never raise a charge, floor,
estimate, ratio or escalation. They drive live room, the pressure trigger and
diagnostics, and they may *lower* one figure: the upper bound of an attempt
that ran alone on its pool (section 1.4).

### 1.1 Components

For each pool, the worker reports these components in one snapshot per sample,
with the host's `available` reading from the same snapshot and a completeness
flag per component:

- `c_L`, the cgroup lower figure: `anon + shmem + (kernel − slab_reclaimable)`
  from the attempt cgroup's `memory.stat`. Revision 1 used all of `kernel`,
  which includes reclaimable slab.
- `c_U`, the cgroup upper figure: `anon + kernel + (file − active_file −
  inactive_file)`. The kernel documents `anon` and `file` as type-based counters
  and `active_file`/`inactive_file` as reclaim lists, and notes that shmem sits
  on the anon lists ([cgroup v2][k-cg], `memory.stat`). So the bracketed term is
  every charged file page that is not on a reclaimable file list: shmem,
  mlocked file pages and file pages held off the lists. Reclaimable page cache
  stays out, as in the approved design. `c_U >= c_L` always, and `c_U` is never
  below revision 1's `c`, so this change cannot lower a charge.
- `d_sum`, `d_max`: the sum and the largest of the attempt's per-process CUDA
  device counters (`nvidia-smi --query-compute-apps`).
- `g_sum`, `g_max`: the sum and the largest of the attempt's per-client DRM
  host-memory figures (`drm-resident-system` or `drm-resident-gtt`, deduplicated
  by client id). The sampler already collects these on discrete XPU
  (`internal/myce/attempt_memory.go`, `xpuAttemptDeviceMemory`). Revision 1
  dropped them.
- `v_sum`, `v_max`: the same for DRM `drm-resident-vram*`, the card pool.
- `r_sum`, `r_max`: per-process resident or footprint counters where no cgroup
  exists.

The worker reads `memory.stat` fields it does not read today (`file`,
`active_file`, `inactive_file`, `slab_reclaimable`). All come from the same
small file it already reads each sample.

### 1.2 Coverage

Each component counts some allocation classes and misses others. A number may
size a charge only for a class it is known to count. The table records what is
measured and what is not.

| Allocation class | `c_L` | `c_U` | `d` | `g` | `available` falls | Source |
|---|---|---|---|---|---|---|
| `malloc` (anon) | yes | yes | no | no | 1:1 | #1044 probe |
| shmem / tmpfs | yes | yes | no | no | expected 1:1 (on the anon lists); not probed | kernel doc |
| reclaimable page cache | no | no | no | no | no | kernel doc |
| reclaimable slab | no | yes | no | no | partly | kernel doc |
| unified `cudaMalloc` | no (+0.002) | no | yes (2218 MiB for 2 GiB plus context) | — | 1.01:1 | #1044 probe |
| unified `cudaMallocManaged`, GPU-touched | `memory.current` +1.00; field **unmeasured** | **unmeasured** | **unmeasured** | — | 1:1 | #1044 probe |
| unified `cudaMallocHost` (pinned) | `memory.current` +1.00 as **file** pages; shmem or not **unmeasured** | yes *if* off the file lists | **unmeasured** | — | 1:1 | #1044 probe |
| `cudaHostRegister` | **unmeasured** | **unmeasured** | **unmeasured** | — | **unmeasured** | none |
| discrete CUDA pinned host | **unmeasured** | **unmeasured** | **unmeasured** | — | **unmeasured** | none |
| XPU system / GTT objects | memcg-charged? **unmeasured** | **unmeasured** | — | yes | **unmeasured** | DRM doc, sampler |
| Metal buffers | — | — | — | — | `phys_footprint` +1.00 | #1044 probe |

That `cudaMallocHost` pages sit off the file lists is an inference: if they were
on them, `MemAvailable` would count about half of them as reclaimable, and the
probe saw `available` fall 1:1. Stage 0 measures it.

**Qualification.** A platform family's pool row is **qualified** when an
isolated probe (stage 0b) shows, for every allocation class that family
supports, that `U` rises at least as much as `available` falls and `L` rises no
more than it falls, and records which fields moved. An unqualified row still
produces evidence and raises charges. It never lowers one: until its row
qualifies, a pool's charge is `max(new charge, today's charge)` (invariant P8).

### 1.3 The interval

One function turns the components into an interval **[L, U]** per pool. `L` is a
proven lower bound on what the attempt occupied. `U` is an upper bound, the
figure charges are sized in.

| Platform | Pool | `L` | `U` | Complete when |
|---|---|---|---|---|
| Linux, cgroup v2, no accelerator | RAM | `c_L` | `c_U` | every sample read `memory.stat` |
| Linux, no cgroup | RAM | `r_max` | `r_sum` | every sample enumerated the group |
| Darwin | RAM (unified) | largest owned `phys_footprint` | summed owned `phys_footprint` | the owned-footprint sampler covered the run |
| Discrete CUDA | RAM | `c_L` | `c_U` | as Linux |
| Discrete CUDA | each card | `d_max` | `d_sum` | no `N/A` row and no unreported tick for the attempt's processes on that card |
| Discrete XPU | RAM | `max(c_L, g_max)` | `c_U + g_sum` | `memory.stat` every sample, and `g` every sample unless stage 0b shows XPU system/GTT objects are memcg-charged |
| Discrete XPU | each card | `v_max` | `v_sum` | every attempt fd on that card read |
| CUDA unified | RAM (one pool) | `max(c_L, d_max)` | `c_U + d_sum` | both every sample, after the first CUDA use |

Why the rows differ:

- **Per-process counters can share pages.** Forked workers share copy-on-write
  RSS. CUDA IPC and DMA-buf imports map one allocation in several processes or
  clients. A sum can count a page twice, and one counter is a floor on their
  union. Where the attempt has one process, `L = U` for that component.
- **Overlap between `c` and a driver counter is unmeasured.** If `d` or `g` also
  counts pages the cgroup charges (managed, pinned, GTT), `c_U + d_sum` counts
  them twice. That is safe for `U`. `max(c_L, d_max)` is a lower bound either
  way. If stage 0b shows a driver counter and `c` are disjoint, that row's `L`
  becomes `c_L + d_max` (or `c_L + g_max`).
- **Completeness is per pool.** A missing card reading makes that card's pool
  incomplete and nothing else. The RAM pool keeps its own completeness and its
  own evidence (section 3.4).

**`memory.peak` stays a diagnostic.** It includes reclaimable file cache. On the
unified worker's retained CUDA attempts (n = 615), `memory.peak` exceeded the
`anon + shmem + kernel` peak by a median of 0.04 GiB, a 90th percentile of
2.5 GiB and a maximum of 59.3 GiB. The worker keeps reporting it, with
`memory.peak − c_U` per attempt, so the coverage audit can find classes the
table misses.

These figures are **host-wide**, and the interval excludes them:

- `MemAvailable` and its drop since launch, which is the launch-delta term inside
  today's `CombinedHostGB`;
- the unified device figure taken from the "host memory drop outside Mycelium
  cgroups";
- the absolute physical residual and the host GUP pin count;
- host PSI and swap, and `nvidia-smi` device totals.

On the same 615 attempts, `CombinedHostGB` exceeded the cgroup figure by a
median of 26.1 GiB and a maximum of 64.9 GiB. The evidence projection takes
`max(…, CombinedHostGB)`, so host-wide memory reaches floors today.

### 1.4 An alone run's physical bound

While an attempt is the only Mycelium attempt on a pool, everything it occupies
is in `used = total − available` from the same snapshot. Other owners only add to
`used`. So for that attempt, at each sample:

```text
Û = max(L, min(U, used))
```

is also an upper bound, and the run's recorded upper figure is the peak of `Û`.
It stops a summed counter from inflating forever: five processes mapping one
22 GiB allocation plus 2 GiB of cgroup memory give `U = 112` but `used` near
24 GiB plus the host's idle footprint. If `used < L` in a snapshot, the readings
disagree, and that sample is incomplete. This is the one place a host-wide
reading enters a recorded figure. It can only lower `U`, never below `L`, and
only for an attempt that was alone on that pool.

### 1.5 Clean, complete and versioned

A run is **complete** on a pool when every component of that pool's row is
complete. Only complete, unlimited successes are **clean** on that pool. Clean
runs alone feed peak history, the ratio ring and feedback for that pool. Any
run, complete or not, contributes the `L` of its complete components as a floor
candidate, because a partial lower bound is still a lower bound.

Each record carries a **counting version**. A ratio pairs two runs only when both
have the same version. Retained data shows why. On the unified worker,
`(c + d)/c` has a median of 11.9 over 428 complete clean CUDA runs. Five clean
runs had no device reading. Pairing one of those runs with a complete run would
put a ratio near 12 or 1/12 into the ring.

## 2. The charge

A pool's admission decision has two independent parts: **bytes** `B`, a number
that is shared against room, and **state** `X`, either `shared` or `alone`.
Revision 1 encoded alone as an infinite byte figure. That let a byte cap erase
exclusivity and let exclusivity skip the fit check. Here they never mix.

Inputs for job `j` on pool `p` of host `h`:

| Symbol | Meaning |
|---|---|
| `O` | owner resize (`myce job resize`): an owner scheduling charge `O_c` and the owner kill ceiling `O_k` |
| `R` | recovery charge: a scheduling charge Mycelium records separately from `O`, set only by evidence from attempts run after the latest resize (section 3) |
| `Lim` | `memory_limit_gb`, enforced as the authored cgroup limit. It is **contained** on `p` when that limit covers every component of `U` on a qualified row. That holds for Linux RAM without a driver term, and for Darwin (the footprint guard covers Metal). It does not hold on unified CUDA, where `cudaMalloc` escapes `memory.max`, nor on XPU RAM until stage 0b shows GTT is memcg-charged. |
| `A(h)` | pool allowance: capacity − keep, or the card's share |
| `F` | floor: the largest qualified `L` retained for this exact work, platform and pool, over the last 20 attempts |
| `S` | open search step from section 3, in bytes, or none |
| `P` | largest retained clean exact peak, in `U` units (`Û` for an alone run) |
| `E` | estimate, below |

**Estimate `E`** is the approved estimator with two changes:

1. Use the exact level, then family, then foreign calibration, as approved, with
   `E = max(f × m, P)`. A factor `f` is read only by rule Q, from ratios between
   complete runs that share a counting version.
2. When no level's factor meets rule Q, the approved step 3 applies: move up,
   keeping every more specific level's maximum as a floor, and end at the
   platform level. So `E = max(H, P_exact, P_family, q)`, where `H` is a typed
   figure or size hint and `q` is the platform first-run quantile. The quantile
   enters only through that `max`; it never caps a repeat. `E` is **unknown**
   only when there is no peak, no `H` and no platform quantile. Revision 1 made
   every repeat without a rule-Q factor unknown, which ran clean repeats alone
   that a first run would share. That broke the approved step 3 and the
   orchestrator's 2026-11-23 ruling that unified-memory hosts share from the
   first accelerator run.

A discrete card still runs work with no device evidence at the exact or family
level alone on its card, as approved.

**Charge**, per pool:

```text
1. Bytes.   If O is set:  B = max(O_c, R).
            Else:         B = max(E, F, S), or no bytes if E is unknown and F, S are absent.
2. Cap.     If the authored ceiling (O_k if O is set, else Lim) is contained
            on p: B = min(B, that ceiling).
3. State.   X = alone if any of:
              - B is absent (unknown estimate);
              - the open search ended in alone (section 3);
              - the card rule above;
              - the pool's last limited end was classified unknown on p.
            Otherwise X = shared. The cap in step 2 never changes X.
4. Fit.     Always, for every h:
              if X = shared and B <= A(h):  admit as shared at B;
              else if F <= A(h):            admit alone on p of h, charged A(h)
                                            (an absent F counts as 0);
              else:                         h is ineligible for p.
5. Capacity. memory_capacity only from section 3.3: an attributable failure
            while alone, with no eligible h whose A(h) exceeds the measured need.
```

Step 2 caps bytes only, and step 4 runs whatever step 2 did. Two of the
review's counterexamples:

- `E = 96`, `F = 20`, `Lim = 80` contained, `A = 48`: `B = 80 > 48`, and
  `F <= 48`, so the job runs alone on an empty host. Revision 1 returned 80 and
  stranded it.
- A failed search with `Lim = 32` contained: `B = 32`, and `X` stays alone. The
  limit bounds the bytes; it does not cancel the exclusivity the search earned.
  A clean success closes the search and returns the job to sharing.

**Alone.** A job running alone is admitted only when no other attempt holds the
pool and `available − keep >= F`, by the same predicate in the queue and in the
worker's launch check (section 4.2). While it runs, nothing else is admitted to
that pool. Alone is a state, not a byte figure. On a discrete XPU or CUDA host,
alone on a card fences that card only. The RAM pool keeps its own state.

**Invariants.** Each one is a property test over random inputs:

- **P1, floor.** With no `O` and no contained `Lim`, `B >= max(F, P, S)`
  whenever bytes exist.
- **P2, ceiling.** A contained limit only lowers bytes:
  `B(Lim) = min(B(no Lim), Lim)`, and `X(Lim) = X(no Lim)`. An uncontained limit
  changes neither. The kill ceiling is `Lim` or `O_k`, never `B`.
- **P3, monotone.** `B` is non-decreasing in `F`, `S`, `P`, `R`, `f` and every
  clean peak. The only `min` is step 2.
- **P4, independence.** No host-wide input raises any `B`, `F`, `S` or `P`. The
  only host-wide effect on a recorded figure is section 1.4, which lowers an
  alone run's `U` and never below `L`. The platform quantile enters only as a
  term of `max`.
- **P5, no stranding.** If `F <= A(h)` on some eligible `h`, the job is
  admissible on an empty `h`, for every `Lim`, `S` and `X`.
- **P6, no oversharing.** If `B > A(h)` or `X = alone`, the job is never admitted
  beside a holder of that pool on `h`.
- **P7, owner.** With `O` set and no post-resize evidence, `B = O_c` exactly, even
  below `F`. Nothing automatic ever changes `O_k`.
- **P8, lowering gate.** On a pool whose platform row is unqualified, `B` is at
  least today's charge for the same inputs.
- **P9, progress.** After a limited end classified overrun, possible overrun or
  unknown, the next offer differs from the last: higher `B`, alone, fewer
  eligible hosts, or a terminal reason. No job is readmitted at the same bytes
  and state after such an end.

## 3. Learning

### 3.1 One sealed record per attempt

The queue keeps one learning record per attempt under the existing exact-attempt
key (job, attempt, session). It holds `C` (the admitted bytes), `X`, the
charge's basis (estimate, hint, search, alone, limit, owner or recovery), per
pool the merged `[L, U]` and `Û` with completeness, and the cgroup
`memory.events` counters.

The first write is the **stop receipt**. The queue writes it in the same
transaction as a pressure-stop decision, from its freshest attempt snapshot.
Facts that arrive later for the same attempt (release receipt, final result,
late result) merge into the record: maxima for figures, logical OR for OOM
flags and event counters. Merging is commutative, so the record does not
depend on arrival order.

**The record must be sealed before the job is offered again.** Revision 1 let a
release readmit attempt 2 at 60 GiB before a late OOM would have raised it to
100. Now the job's next attempt waits, inside the existing release custody,
until the record is sealed by one of:

1. **The final result** for that exact attempt. The worker reads the attempt
   cgroup's `memory.events`, the last snapshot and the exit cause before it
   removes the cgroup, and sends them with the result.
2. **A release receipt that carries the same memory fields.** A release-only
   receipt now includes the cgroup's final `memory.events` and last snapshot,
   read before cgroup removal. A release receipt without those fields (an older
   worker) seals the record as **unknown** on every pool it lacks, and the
   next offer follows the unknown row. It never readmits at the same charge.
3. **Loss of the host**, through the ordinary lost-host bound. The record seals
   with the stop receipt and the last fresh snapshot. No memory-specific custody
   wait may outlive the loss.

Facts that arrive after sealing are kept in the record as diagnostics. They
can raise `F` and `P` for offers made after they commit. They never change
the sealed classification, the already-admitted next attempt, or its host.

### 3.2 Which end it was

The kind of end comes from the attempt's own cgroup counters. The kernel
counts `oom` in a cgroup when *that cgroup's* limit was reached and an allocation
was about to fail, propagated to ancestors only; `oom_kill` counts processes in
the cgroup killed by any OOM killer ([cgroup v2][k-cg], `memory.events`).

- **Local-limit failure**: the attempt cgroup's `oom > 0` while it has an
  authored ceiling (`Lim` or `O_k`). Confirmed at the owner's ceiling.
- **Slice or host OOM**: `oom_kill > 0` with the attempt's own `oom = 0`. The
  worker slice or the host ran short.
- **`OOMKill`-only**: the result reports an OOM kill with no cgroup counters.
  This is recorded as an OOM, never dropped or reclassified as pressure.
- **Pressure stop**: the queue's stop receipt.

Every limited end, of any kind, then compares the attempt with its bytes per
pool:

- **Overrun** when `L` over the complete components exceeds `C`. The attempt
  provably exceeded its charge.
- **Possible overrun** when `L <= C < U` on a complete pool. The upper bound
  exceeds the charge, but double-counted or shared pages may explain it.
- **Victim** when the pool is complete and `U <= C`.
- **Unknown** when the pool has no complete `U` and `L <= C`.

Revision 1 also called an OOM an overrun when every co-tenant stayed within its
charge and the pool was above keep. That is removed. A pool 3 GiB above keep
does not show that a 32 GiB-charged attempt had 32 GiB available, and other
owners can take memory without being anyone's co-tenant.

### 3.3 What each end teaches

| End | Recorded | Next offer | Feedback |
|---|---|---|---|
| Clean success | `U` (or `Û` if alone) into peak history and ratio pairs. `L` into `F`. Closes any open search and clears `R`. | `E` from the new history | `−(1 − U/C)` when the basis was the estimate |
| Success, incomplete or limited | `L` of complete components into `F` | unchanged except through `F` | none |
| Local-limit failure at `Lim` or `O_k` | `L` into `F` | fails visibly with the ceiling and peak, as today. The ceiling is the owner's. | none |
| Overrun | `L` into `F`. `S = U + (U − C)`. If `C` was itself a search step, the search ends in alone. Under `O`, the step goes to `R`. | `max(E, F, S)` (or `max(O_c, R)`), no attempt or loss charge | `+1` when the basis was the estimate |
| Possible overrun | as overrun | as overrun | none: the estimator's error is not proven |
| Victim | `L` into `F` | the same bytes and state, offered only when live room fits | none |
| Unknown on pool `p` | `L` of complete parts into `F` | `p` alone; other pools unchanged | none |
| Attributable failure while alone | `L` into `F`; `Û` kept for host eligibility | see below | none |

**Attributable failure while alone.** The attempt ran alone on `p`, ended
limited (slice or host OOM, `OOMKill`-only, or a pressure stop), its pool was
complete and qualified, and `Û` at the end was at least `A(h)`. Then the attempt
itself used the whole allowance, so `h` cannot hold this work. `h` becomes
ineligible for this exact work on `p`, with the attempt named. The job is
offered alone only on hosts with `A(h') > Û`. When no such host is eligible, the
job fails `memory_capacity`, naming `Û`, the attempt and the largest allowance.
If the failure was not attributable (`Û < A(h)`, or the pool was incomplete),
something else took the memory. The job stays alone and waits for room as a
victim. No retry is counted. For an attempt that ran alone, this rule replaces
the overrun, victim and unknown classes above, since its charge was the whole
allowance.

Rationale:

- **The step `U + (U − C)` is the starting rule, in one unit.** `U` already
  includes the device and driver terms, so the "cgroup + device when overlap is
  unknown" case is built in.
- **An inflated `U` costs one alone run, not exclusivity forever.** A possible
  overrun from a summed counter may push `S` above the allowance, and the job
  runs alone. The alone run records `Û`, which `used` bounds. The next estimate
  uses `Û`, and the job returns to sharing when `Û` fits.
- **A failed extrapolation ends the search.** If a search step is itself
  overrun, growth is not predictable from overshoot, and the job runs alone,
  where section 1.4 measures it.
- **No fixed attempt count.** Revision 1 promised at most three limited
  attempts. Victim waits, other owners' memory and host loss all break that
  count. P9 replaces it: every limited end that is not a victim changes the next
  offer.
- **`F` is a floor at ×1, and the search step lasts one round.** On main,
  `estimateWithFloor` keeps `charge >= 2 × Lower` for good. One stop at 21 GiB
  therefore charges 42 GiB forever (compare #2030). Here, a clean success closes
  the search and `E` takes over. `F` ages out with its 20-attempt window.
- **Feedback stays.** It measures the estimator's error only. Search, alone,
  hint, limit, owner and recovery charges do not move the confidence target.

### 3.4 Discrete cards: per-pool evidence

On a discrete XPU or CUDA host, each card and the RAM pool keep their own
completeness, classification and search.

- A missing card reading makes that card's pool unknown. The job runs alone on
  that card only. Its RAM charge comes from qualified RAM evidence as before,
  and the RAM room and the worker's live-capacity check still apply.
- A run with a complete RAM row stays RAM evidence when its card row is
  incomplete.
- On a two-card host, one job alone on card 0 does not fence card 1. A second
  job can run on card 1 when the RAM pool has room for both.

Before any admission change goes live on discrete XPU, the shadow replay must
cover the two-card production workload from #2030 and show both cards in use
concurrently whenever RAM room fits. This is a paying consumer's production
host.

### 3.5 Owner resize and the recovery charge

This replaces revision 1's maintainer question. It is the orchestrator's
contract decision, adopting the reviewer's recommendation; it is not a
maintainer ruling.

- The owner controls the kill ceiling `O_k`. Nothing automatic changes it.
- The owner's scheduling charge `O_c` is the bytes until evidence from an attempt
  run *after* the resize warrants more. Older evidence, which the owner may have
  meant to override, never does.
- An overrun or possible overrun under `O` records `R`, a separate scheduling
  charge, or selects alone, by the rules above. Status shows `O_c`, `R` and the
  attempt that set `R`. A clean success clears `R`.
- A local-limit failure at `O_k` fails visibly. The owner decides whether to
  resize again.

### 3.6 `memory_limit_gb` on unified CUDA

`memory_limit_gb` stays enforced as the authored cgroup limit everywhere. On
unified CUDA it no longer caps the pool charge, because ordinary `cudaMalloc`
escapes the cgroup and the limit does not contain the job. Main caps it there.
**This changes scheduling for every unified-CUDA job with a limit:** its charge
can rise above the limit to the measured device use. The implementing stage
updates `skills/mycelium/SKILL.md` and `docs/jobs.md` in the same change
(AGENTS.md), saying that the limit bounds cgroup memory only on unified CUDA.
Before rollout, the stage-0 replay and a live shadow run the existing two-job
pair from #2086 (the holder and the waiter) under the new rule and report both
charges and whether they still run together.

## 4. Room on a pool

### 4.1 Credit

For each running attempt `i` on a pool:

```text
held_i   = A(h) if alone, else max(C_i, highest U_i so far)
credit_i = K_i from the snapshot that supplied available, else 0
pending  = sum over i of max(0, held_i − credit_i)
room     = available − keep − pending − other existing terms
```

A credit says "these bytes are already missing from `available`, so do not hold
them back twice". So a credited byte must be (a) charged to this attempt only,
and (b) absent from the same `available` reading. `K_i` credits only components
that meet both:

| Component | Credit | Why |
|---|---|---|
| `anon`, `shmem`, `kernel − slab_reclaimable` | yes | A memory cgroup charges a page to exactly one cgroup ([cgroup v2][k-cg]). None of these is in `MemAvailable`'s reclaimable terms ([proc][k-proc]). |
| reclaimable slab, page cache | 0 | `MemAvailable` already counts them as available. |
| file pages off the file lists (`c_U − c_L` part) | 0 until stage 0b | Physical accounting is inferred, not measured. |
| unified `d_max` | yes, only when `i` is the only attempt on the pool with device rows in that snapshot | `cudaMalloc` lowers `available` 1:1 (#1044). With one device attempt, no other attempt can claim the same bytes. |
| unified `d` with two or more device attempts | 0 | Cross-attempt sharing cannot be excluded from the counters. |
| XPU `g`, Darwin footprint, RSS-only | 0 until stage 0b | Physical accounting or exclusivity unmeasured. |

Where both a cgroup part and `d_max` qualify, `K_i = max(c_L, d_max)` until stage
0b shows them disjoint, then their sum. Stale, incomplete or mismatched
snapshots credit 0.

Revision 1 relied on two shortcuts that are dropped. It assumed no supported
path creates CUDA IPC between separate jobs, which is a driver-wide disjointness
claim with no proof. And it treated `Σ credit <= capacity − available` as a
safety check. That check cannot catch two attempts each crediting the same
10 GiB when other memory already fills the gap. It stays as a diagnostic: a
violation zeroes that snapshot's credits and is logged.

**The cost.** With two CUDA attempts on one unified pool, their device bytes are
held back twice, so room for a third admission is smaller than it physically
is. The two running attempts are unaffected, and the pressure trigger reads
`available` directly. Lifting this needs a proof that two attempts cannot share
device allocations. That is an issue for later, not this design.

### 4.2 Waiting for alone

An alone job needs an empty pool. Revision 1 did not say how it gets one. Today
backfill rejects every job behind an exclusive RAM head
(`poolBackfillFits`, `internal/myce/memory_pools.go`), while custody can stop
that head starting beside a holder it cannot release (`planPoolCustody`). A
persistent same-tier holder then fences the host forever.

- **Releasable holders.** A holder of pool `p` on `h` is releasable for head `j`
  when it is a finite batch attempt (it will end), or the existing custody
  planner can release it for `j` (lower-tier preemptible work, or a model slot
  reclaimable for `j`). Services, loaded engines not reclaimable for `j`, and
  same-tier attempts with no end are **persistent**.
- **Drainable route.** `(h, p)` drains for `j` when `F <= A(h)` and every holder
  is releasable. Among drainable routes, the existing placement order picks one,
  the **drain target**. Only the drain target's pool is fenced: backfill admits
  nothing new on `p` of that host while `j` waits there, and the planner
  releases what it may. Other hosts, and other pools of that host, are
  unaffected.
- **No drainable route.** `j` fences nothing. It waits with `memory_wait` and a
  reason that names each persistent holder and the route it blocks, using the
  existing custody waiting reason. Backfill runs normally. The decision is
  recomputed whenever the holder set changes.
- **One predicate.** The queue's alone admission and the worker's launch check
  call one pure function: no other attempt holds `p` by the caller's inventory,
  and `available − keep >= F` from the caller's own reading. A worker refusal
  returns `memory_wait`, costs no attempt and keeps the drain target.

## 5. Failures and what prevents each

| Failure (source) | Cause on main, in a PR or in revision 1 | Prevented by |
|---|---|---|
| ×2.277 single-run inflation, 46.95 GiB charged for a 20.62 GiB peak (#2086) | A bucket-1 ratio quantile read from a ring that mixes incommensurable pairs | Complete-only clean runs, counting version, ring rebuilt. That the ring caused 2.277 is an inference; stage 0 owes the reconstruction. |
| Unrelated platform quantile undercharging a repeat (#2089 r1) | The quantile used as a cap | P4: the quantile is a `max` term only |
| Fallback bypass strands a job (#2089 r1) | A cap skipped `fitPoolCharge` | Step 4 always runs; P5 |
| Above-allowance forecast admitted beside a holder (#2089 r2) | The fallback lowered the charge | P6 |
| Forecast discards a higher exact peak, 60 → 30 GiB (#2089 r2) | Forecast replaced `P` | P1, P3: `E >= P` |
| Cold path lowers a typed figure, 40 → 20 GiB (#2089 r2) | The cold prior overrode `H` | `E` change 2: `H` is a `max` term. Tested separately from an explicit limit, which caps and never raises (P2). |
| Host-wide launch delta becomes a 70 GiB floor, then 140 GiB (#2106 r1) | `max(…, CombinedHostGB)` and ×2 floors | Section 1; P4; `F` at ×1 |
| Pressure stops learn nothing; the job loops at 60 GiB (#2091, #2106 r2) | The bystander check left out device memory; the floor was skipped | Section 3.2 in `U` and `L`; P9 |
| Memory limit silently becomes a reservation (#2106 r2) | The limit used as a floor | P2 |
| Victim on a lost host waits forever (#2106 r2) | Custody kept open for a cleanup that cannot arrive | Seal by host loss (3.1); test T6 |
| Cgroup and device summed in one path, max'd in another (#2089, #2106) | Two projections | One footprint function. A test fails if other code combines device and host fields. |
| OOM floor lost to release-first ordering or `OOMKill`-only (#2089 r1, r2) | Stale result dropped; OOM reclassified as pressure | Sealed record (3.1); `OOMKill`-only kept as an OOM (3.2) |
| Shared CUDA memory credited twice (#2089 r2) | `c + d_sum` credited | Section 4.1 |
| Pinned `cudaMallocHost` file pages missed by a "complete" sample (review r1, P1.1) | `anon + shmem + kernel` omits file pages | `c_U`; coverage table; P8 until qualified |
| Discrete-XPU driver system/GTT dropped (review r1, P1.1) | Revision 1's XPU row had no `g` | XPU RAM row includes `g` |
| Same 10 GiB credited by two attempts; reclaimable slab credited (review r1, P1.2) | Credit on `L`; capacity check as proof | Section 4.1 credit table |
| `memory_capacity` from an ambiguous floor (review r1, P1.2) | Any `F > A` failed the job | Capacity only after an attributable failure while alone (3.3) |
| Contained limit strands `E = 96, F = 20, Lim = 80` at `A = 48` (review r1, P1.3) | The limit step returned before the fit check | Cap on bytes only; step 4 always |
| `Lim = 32` turns alone into a shared 32 GiB charge (review r1, P1.3) | Alone encoded as bytes | `X` separate from `B`; P2 |
| Victim inferred from "3 GiB above keep" (review r1, P1.3, P1.5) | Co-tenant OOM inference | Removed; victim needs complete `U <= C` |
| Summed counter makes exclusivity permanent (review r1, P1.4) | `U = 112` for 24 GiB physical | `Û` (1.4); possible overrun earns no feedback |
| Exclusive head fences a host it cannot enter (review r1, P1.4) | Backfill rejects behind an alone head; custody cannot release a persistent holder | Section 4.2 |
| Release readmits before a late OOM (review r1, P1.5) | Max-merge without a seal | Seal before readmission (3.1) |
| Missing card telemetry serialises a two-card host (review r1, P1.6; #2030) | Whole-run completeness | Per-pool completeness (3.4) |
| Downgrade doubles `F` (review r1, rollout) | Old `estimateWithFloor` doubles `Lower` | `F` is never written to `Lower` (section 6) |

## 6. Stages

Each stage merges on quick and targeted tests (AGENTS.md). Everything except
stage 0b runs on a laptop with fake samples and an injected `Clock`. New state
goes in new tables, because the previous release rejects unknown
`memory_evidence` kinds.

**Compatibility.** The new queue never writes `F`, `S` or `R` into the old
`Lower` field: the old binary's `estimateWithFloor` doubles `Lower`, so a
downgrade would charge twice the floor. Legacy limited-run rows keep being
written from the worker's legacy fields, as the old binary would write them, for
one release. An old worker's samples and receipts lack the new fields, so a
new queue treats them as incomplete and unqualified: no credit, unknown on a
limited end, and P8. An old queue ignores the new worker fields.

| Stage | Change | Acceptance | Rollback |
|---|---|---|---|
| 0a | Offline replay of the pure functions over a read-only copy of retained history. No behaviour change. | Reconstruct the ×2.277 factor from the retained ring, or report it as unknown with the missing inputs named. Report rebuilt ring factors per platform and bucket. Replay the incident charges (46.95, 55.45 and 60 GiB, the 70 GiB floor, 60 → 30, 40 → 20) and the #2086 holder-and-waiter pair under 3.6. Replay the #2030 two-card workload under 3.4. | none needed |
| 0b | Isolated coverage probes, with release authorization, on idle hosts away from production: unified CUDA, discrete CUDA and discrete XPU. Synthetic jobs allocate each class in section 1.2 in 1 GiB steps and record every `memory.stat` field, `d`, `g`, `v` and `available`. | Fill the coverage table. A row is qualified only by this evidence. The probes also check the `memory.events` attribution in 3.2 with a local limit and with a slice limit. | none needed |
| 1 | Footprint function and worker components, per-pool completeness, `Û`, counting version, ring rebuilt into a new table, credit table, P8 gate. Release and final results carry the cgroup's last `memory.events` and snapshot. | Properties: `L <= Û <= U`; rows match section 1.3; P4 and P8 under host-wide fuzz; no ratio pairs across versions; credits zero for every ambiguous component; the #2089 r2 double-credit replay and the 10 GiB two-attempt case. | The previous binary reads its untouched rows |
| 2 | One pure `charge()` for bytes and state, replacing the `resolvePooledSize` and `fitPoolCharge` composition; the shared alone predicate; drain target. | P1–P9 as property tests. Replays of every #2089 and revision-1 counterexample in section 5. | Revert the binary; the function is stateless |
| 3 | Sealed learning record, stop receipt, end kinds, classification, search, recovery charge | Fake-clock tests below | The previous binary ignores the new tables |

Fake-clock tests for stage 3, with stage 2's queue and worker:

- **T1, loop.** The 60 GiB pressure loop: every limited end changes the next
  offer (P9), and the run ends in success, an alone run, `memory_capacity` or a
  victim wait.
- **T2, release then OOM.** A release receipt without memory fields, then a late
  OOM result: no offer at the old charge in between.
- **T3, `OOMKill`-only.** Kept as an OOM, never as pressure.
- **T4, late fact.** A result after sealing raises only offers made after it.
- **T5, victim.** A victim needs complete `U <= C`; a stop with `L <= C < U` is a
  possible overrun; neither "above keep" nor co-tenant charges classify.
- **T6, lost host.** A lost-host victim leaves `stopping` within the loss bound.
- **T7, ordering.** Every arrival permutation before sealing gives the same
  sealed record.
- **T8, permanent holder.** An alone head beside a persistent same-tier holder
  fences nothing, waits with the holder named, and other jobs still backfill.
- **T9, exclusive launch.** The worker refuses an alone launch when its own
  inventory shows another attempt on the pool, with `memory_wait` and no attempt
  charge, and the queue's predicate agrees on the same inputs.
- **T10, per card.** Missing telemetry on card 0 sends the job alone on card 0
  while a job runs on card 1.
- **T11, local versus slice.** `oom > 0` with an authored ceiling fails visibly;
  `oom_kill > 0` with `oom = 0` takes the classification path.

**Before any new charge or credit goes live** on a platform family:

1. A shadow run on the live queue computes new bytes, state and credits beside
   the current ones for every admission, without acting on them. Its
   report lists every admission where they differ.
2. That family's isolated stage-0b check has qualified its row, for CUDA and for
   XPU separately.
3. T1–T11 pass, including permanent holders, exclusive worker launch, and
   release and OOM ordering.
4. Old and new worker against old and new queue, all four combinations, run the
   stage-3 tests that apply. A downgrade with an active search or recovery
   charge leaves the old binary charging what it would have without the
   upgrade, never twice `F`.
5. For unified CUDA, the #2086 pair replay under 3.6. For discrete XPU, the
   #2030 two-card replay under 3.4.

## 7. Questions for the maintainer

None. The owner-resize question in revision 1 is settled by the orchestrator's
contract decision in section 3.5. The other rules follow the rulings (#1972: a
limit is an explicit opt-in ceiling and typed figures are estimates; MY-34;
MY-56; the orchestrator's 2026-11-23 first-run ruling that unified hosts share
from the first accelerator run and discrete cards run new accelerator work
alone) or are technical.

[k-cg]: https://docs.kernel.org/admin-guide/cgroup-v2.html
[k-proc]: https://docs.kernel.org/filesystems/proc.html
