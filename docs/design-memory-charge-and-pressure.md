# Memory charge, learning and unified-memory counting

Status: **draft for orchestrator approval; do not implement or merge.** Issue
[#2086](https://github.com/1kuna/mycelium/issues/2086). Pressure-stop learning
moved here from [#2091](https://github.com/1kuna/mycelium/issues/2091).

This note amends the approved [memory admission design](design-memory-admission.md)
in three places: the floor after a limited run (its section 2.3), what a stop
teaches (2.6), and unified counting (2.2, 2.9). Everything else there stands,
including rule Q, the pressure trigger, the stop order and the backfill rule.
Pressure *detection* stays exactly as on main (#2091, review round 1 of #2106).

## Why one design

Four PR rounds (#2089 twice, #2098 twice, #2103 once, #2106 twice) each changed
`resolvePooledSize`, `fitPoolCharge` and the evidence projection separately.
Each fix opened a different under- or over-charge path, because the three
functions disagreed on three questions. Which memory figures belong to the
attempt? How are cgroup and device memory combined? Which inputs may lower a
charge? This note answers each question once. The implementation then has one
footprint function, one charge function and one learning record.

## 1. Definitions

Every figure is **attempt-scoped** (it counts only the attempt's own processes
or cgroup) or **host-wide**. Host-wide figures never enter a charge, floor,
estimate, ratio or escalation. They drive only live room, the pressure trigger
and diagnostics.

For each pool, the worker reports these components per sample, with a
completeness flag:

- `c`: kernel-charged memory of the attempt cgroup. A page is charged to exactly
  one cgroup, so `c` is exclusive to the attempt.
- `d_sum`: the sum of the attempt's per-process device counters.
- `d_max`: the largest single per-process device counter.
- `r_sum`, `r_max`: the same sum and maximum for per-process resident or
  footprint counters, where no cgroup exists.

One function turns them into an interval **[L, U]** per pool. `L` is a proven
lower bound on what the attempt occupied. `U` is the largest attempt-scoped
figure, the one charges are sized in.

| Platform | Pool | `L` | `U` | Complete when |
|---|---|---|---|---|
| Linux, cgroup v2 | RAM | `c` = peak of `anon + shmem + kernel` | `c` | every sample read `memory.stat` |
| Linux, no cgroup | RAM | `r_max`, the largest process RSS | `r_sum`, the process-group RSS | every sample enumerated the group |
| Darwin | RAM (unified) | largest owned `phys_footprint` | summed owned `phys_footprint` | the owned-footprint sampler covered the run |
| Discrete CUDA | RAM | `c` | `c` | as Linux |
| Discrete CUDA | card | `d_max` | `d_sum` | no `N/A` row and no unreported tick for the attempt's processes |
| XPU (discrete) | RAM / card | `c` / DRM fdinfo resident, deduplicated by client id | `c` / same | as Linux / every attempt fd read |
| CUDA unified | RAM (one pool) | `max(c, d_max)` | `c + d_sum` | both of the above every sample, after the first CUDA use |

The cases are separate for these reasons:

- **Per-process counters can share pages.** Forked workers share copy-on-write
  RSS. CUDA IPC maps one allocation in several processes. A sum can therefore
  count a page twice, and one process's counter is a floor on their union.
  Where the attempt has one process, `L = U`.
- **Unified CUDA overlap is unknown.** On GB10-class hardware, ordinary
  `cudaMalloc` escapes the cgroup and RSS. Managed and pinned allocations are
  cgroup-charged ([#1044 probe](https://github.com/1kuna/mycelium/issues/1044)).
  Whether the per-process device counter also includes those cgroup-charged
  pages is not measured. So `c + d_sum` is the safe upper figure and
  `max(c, d_max)` is the proven lower one. Stage 4 measures the overlap. If the
  counter excludes cgroup-charged pages, that driver's `L` becomes
  `c + d_max`.
- **`memory.peak` is recorded but is not a floor.** It includes reclaimable file
  cache. On the unified worker's retained CUDA attempts (n = 615), `memory.peak`
  exceeded the `anon + shmem + kernel` peak by a median of 0.04 GiB, a 90th
  percentile of 2.5 GiB and a maximum of 59.3 GiB. The starting rule named
  `memory.peak` first. This note drops it as a floor because of that maximum.

These figures are **host-wide**, and the table above excludes them:

- `MemAvailable` and its drop since launch, which is the launch-delta term inside
  today's `CombinedHostGB`;
- the unified device figure taken from the "host memory drop outside Mycelium
  cgroups";
- the absolute physical residual and the host GUP pin count;
- host PSI and swap, and `nvidia-smi` device totals.

On the same 615 attempts, `CombinedHostGB` exceeded the cgroup figure by a
median of 26.1 GiB and a maximum of 64.9 GiB. The evidence projection takes
`max(…, CombinedHostGB)`, so host-wide memory reaches floors today.

A run is **complete** when every component of its platform row is complete.
Only complete, unlimited successes are **clean**. Clean runs alone feed peak
history, the ratio ring and feedback. Any run, complete or not, contributes the
`L` of its complete components as a floor, because a partial sum is still a
lower bound. Each record carries a **counting version**. A ratio pairs two runs
only when both have the same version. Retained data shows why. On the unified
worker, `(c + d)/c` has a median of 11.9 over 428 complete clean CUDA runs.
Five clean runs had no device reading. Pairing one of those runs with a
complete run would put a ratio near 12 or 1/12 into the ring.

## 2. The charge

Inputs for job `j` on pool `p` of host `h`:

| Symbol | Meaning |
|---|---|
| `O` | owner resize (`myce job resize`) |
| `Lim` | `memory_limit_gb`. It is **contained** on `p` when the attempt ceiling covers all of `U`. That holds on Linux RAM (the cgroup) and Darwin (the footprint guard covers Metal). It does not hold on unified CUDA, where device allocations escape `memory.max`. |
| `A(h)` | pool allowance: capacity − keep, or the card's share |
| `F` | floor: the largest `L` retained for this exact work, platform and pool, over the last 20 attempts |
| `S` | open search step from section 3: bytes, `alone`, or none |
| `P` | largest retained clean exact peak, in `U` units |
| `E` | estimate, below |

**Estimate `E`** is the approved estimator with three changes:

1. Use the exact level, then family, then foreign calibration, as approved, with
   `E = max(f × m, P)`. A factor `f` is read only by rule Q, from ratios between
   complete runs that share a counting version.
2. If the work has clean exact or family peaks but no level meets rule Q, then
   `E = max(H, P)` when a typed figure or hint `H` exists. Otherwise `E` is
   **unknown**.
3. The platform first-run quantile applies only to work with no clean exact or
   family peak. It never raises or caps a repeat. With no peak, use `H`, then
   the platform quantile, then **unknown**. A discrete card still runs new
   accelerator work alone.

**Charge**, in order of precedence:

```text
1. Owner resize:   if O is set, charge = O.                       (stop)
2. Need:           need = max(E, F, S)      with unknown or alone taken as +inf
3. Limit:          if Lim is contained on p: charge = min(need, Lim)   (stop)
4. Fallback:       if need <= A(h): charge = need
                   else if F <= A(h): run alone on p of h
                   else: h is ineligible for p
5. Capacity:       if F > A(h) on every eligible h,
                   fail memory_capacity, naming F and its attempt.
```

A job that runs **alone** is admitted only when no other attempt holds the pool
and `available − keep >= F`. While it runs, nothing else is admitted to that
pool. Alone is a state, not a byte figure. Under a contained limit, alone is
never needed, because the ceiling bounds the job.

**Invariants.** Each one is a property test over random inputs:

- **P1, floor.** With no `O` and no contained `Lim`, the charge is at least
  `max(F, P, S)`, or the job runs alone.
- **P2, ceiling.** A contained limit only lowers:
  `charge(Lim) = min(charge(no Lim), Lim)`. An uncontained limit leaves the
  charge unchanged. The kill ceiling is `Lim` or `O`, never the charge.
- **P3, monotone.** The charge is non-decreasing in `F`, `S`, `P`, `f` and every
  clean peak. The only `min` is step 3.
- **P4, independence.** For work with a clean exact or family peak, the platform
  first-run quantile and every host-wide input leave the charge unchanged.
  Fuzzing `MemAvailable`, launch delta, residual and other attempts gives an
  identical charge.
- **P5, no stranding.** If `F <= A(h)` on some eligible `h`, the job is
  admissible on an empty `h`.
- **P6, no oversharing.** If `need > A(h)`, the job runs alone on `h` and never
  beside a holder.
- **P7, owner.** With `O` set, the charge is exactly `O`, even below `F`.

## 3. Learning

**One record.** The queue keeps one learning record per attempt, keyed by job,
attempt and session. It holds `C` (the admitted charge), the charge's basis
(estimate, hint, search, alone, limit or owner), and the merged `[L, U]` with
completeness.

The first write is the **stop receipt**. The queue writes it in the same
transaction as a pressure-stop decision, from its freshest attempt sample. It
includes whether every co-tenant was within its charge and whether the pool was
above keep. A release, a final result or a late result for the same attempt
only merges into the record, taking maxima. Lifecycle still accepts only the
current execution. A late measurement can raise the next charge but can never
lower it or move the job. Merging by maxima is commutative, so the outcome does
not depend on arrival order.

**Classification** of a limited end, meaning an OOM or a pressure stop:

- **Overrun** when `U > C`, or when the attempt itself reported OOM while every
  co-tenant was within its charge and the pool was above keep. In that second
  case, the pool had at least `C` free for the attempt, so its need exceeded
  `C`.
- **Victim** otherwise.
- **Unknown** when no complete `U` exists.

| Terminal kind | Recorded | Next charge | Feedback |
|---|---|---|---|
| Success, clean | `U` into peak history and ratio pairs. `L` into `F`. Closes any open search. | `E` from the new history | `−(1 − U/C)` when the basis was the estimate |
| Success, incomplete or limited | `L` of the complete components into `F` | unchanged, except through `F` | none |
| OOM at its own ceiling (`Lim` or `O`) | `L` into `F` | fails visibly, as today. The limit is the owner's. | none |
| OOM or pressure stop: overrun | `L` into `F`. `S = U + (U − C)` when `U > C`, else `2C`. If `C` was itself a search step, `S = alone`. | requeue at `max(E, F, S)`, with no attempt or loss charge | `+1` when the basis was the estimate |
| OOM or pressure stop: victim | `L` into `F` | the same charge, readmitted only when live room fits it | none |
| OOM or pressure stop: unknown | `L` of complete parts into `F` | `S = alone` | none |
| Release only | merges into the attempt's record | decided by the record's classification | none |
| Lost host | the last fresh attempt sample merges into the record | apply the stop receipt's classification if a stop was issued; else `F` only | none |

Rationale for the rules:

- **The step `U + (U − C)` is the starting rule, in one unit.** `U` always
  includes the device term, so the "cgroup + device when overlap is unknown"
  case is built in.
- **A failed extrapolation ends the search.** If a charge set by a search step
  is itself overrun, the work's growth is not predictable from its overshoot.
  The approved design runs unknown work alone. That bounds the sequence for one
  exact work to three limited attempts: the original charge, one search step,
  and one alone run. After that come success, `memory_capacity`, or a
  victim-only wait.
- **`F` is a floor at ×1, and the search step lasts one round.** On main,
  `estimateWithFloor` keeps `charge >= 2 × Lower` for good. One stop at 21 GiB
  therefore charges 42 GiB forever (compare #2030). Here, a clean success closes
  the search and `E` takes over. `F` ages out with its 20-attempt window.
- **Lost hosts use the ordinary path.** A pressure victim on a lost host leaves
  `stopping` through the ordinary lost-host recovery. No memory-specific custody
  wait may outlive the loss.
- **Feedback stays.** It measures the estimator's error only. Search, alone,
  hint, limit and owner charges do not move the confidence target.

## 4. Physical room on unified memory

For each running attempt `i` on a unified pool:

```text
held_i   = A(h) if alone, else max(C_i, highest U so far)
credit_i = current L_i if the sample is fresh and complete, else 0
pending  = sum over i of max(0, held_i − credit_i)
room     = available − keep − pending − other existing terms
```

Credits must not exceed `capacity − available`. That check runs once per ledger
update.

**Why nothing is credited twice:**

- `available` counts each physical page once.
- A credit subtracts at most `L_i`, a lower bound on what attempt `i` occupies
  now. `c` is exclusive by memory-cgroup charging. `d_max` is one process's
  allocations, and each process belongs to exactly one attempt cgroup.
- Sharing across attempts would need CUDA IPC between separate jobs, which no
  supported path creates. The capacity check catches any gross violation.
- Today, the summed device figure `c + d_sum` is credited, so memory one
  allocation shares across processes is credited more than once (#2089,
  review 2).

**The cost.** When `c` and `d` are really disjoint, the ledger holds back up to
`U − L` of growth the attempt has already made. On the incident shape
(`c ≈ 2`, `d ≈ 22` GiB, one CUDA process), `L = 22` and `U = 24`, so the cost is
small. It is large only for multi-process device sharing, or when `c` and `d`
are both large. If stage 4 shows the counters are disjoint, `L` becomes
`c + d_max` and the cost falls to intra-attempt IPC sharing.

## 5. Past failures and what prevents each

| Failure (source) | Cause on main or in the PR | Prevented by |
|---|---|---|
| ×2.277 single-run inflation, 46.95 GiB charged for a 20.62 GiB peak (#2086) | A bucket-1 ratio quantile read from a ring that mixes incommensurable pairs: cgroup-only runs, the `CombinedHostGB` launch delta, and summed versus owned figures | Complete-only clean runs, counting version on ratio pairs, ring rebuilt (stage 1). That the ring caused 2.277 exactly is an inference; stage 0 recomputes it. |
| Unrelated platform quantile undercharging a repeat (#2089 r1) | The first-run quantile used as a cap on repeats | P4. The quantile applies only to work with no exact or family peak. |
| Fallback bypass strands a job (#2089 r1) | A cap skipped `fitPoolCharge`, leaving the charge above the allowance | Step 4 and P5 |
| Above-allowance forecast admitted beside a holder (#2089 r2) | The fallback lowered the charge to the typical figure | Step 4 and P6: run alone, never share |
| Forecast discards a higher exact peak, 60 → 30 GiB (#2089 r2) | Forecast replaced `P` | P1 and P3: `E >= P` |
| Cold path lowers a typed figure, 40 → 20 GiB (#2089 r2) | The cold accelerator prior overrode `H` | `E`, change 3: `H` precedes the prior |
| Host-wide launch delta becomes a 70 GiB floor, then a 140 GiB charge (#2106 r1) | `max(…, CombinedHostGB)` in the projection, and ×2 floors | Section 1: host-wide figures are excluded. P4, `F` at ×1. |
| Pressure stops learn nothing, so the job loops at 60 GiB (#2091, #2106 r2) | A "bystander" check compares a figure that leaves out device memory, and the floor is skipped | Overrun is tested in `U`, so `S > C`. A failed search runs alone. |
| Memory limit silently becomes a reservation (#2106 r2) | The limit used as a floor | P2. On unified CUDA, main caps the charge at an uncontained limit, which undercharges device memory. P2 fixes that too. |
| Victim on a lost host waits forever (#2106 r2) | Custody kept open until a cleanup that cannot arrive | Lost-host row in section 3, and fake-clock test T6 |
| Cgroup and device summed in one path, max'd in another (#2089, #2106) | Two projections | One footprint function. A test fails if any other code combines device and host fields. |
| OOM floor lost to release-first ordering or an `OOMKill`-only result (#2089 r1 and r2) | A stale result is dropped. An OOM kill is reclassified as pressure, and the bystander check skips it. | Stop receipt plus max-merge record; `F` is recorded on every kind (P8) |
| Shared CUDA memory credited twice in the room ledger (#2089 r2) | `c + d_sum` credited | Section 4: credit `L`, with the capacity check |

## 6. Stages

Each stage merges on quick and targeted tests (AGENTS.md). Everything runs on a
laptop with fake samples and an injected `Clock`. New state goes in new tables,
because the previous release rejects unknown `memory_evidence` kinds.

| Stage | Change | Laptop acceptance | Rollback |
|---|---|---|---|
| 0 | Offline replay of the pure functions over a read-only copy of retained history. No behaviour change. | Report the rebuilt ring factors per platform and bucket, and how many unified repeats would run alone because no level meets rule Q. Replay the incident charges: 46.95, 55.45 and 60 GiB, the 70 GiB floor, 60 → 30, 40 → 20. | none needed |
| 1 | Footprint function and worker components. Evidence projection uses `[L, U]`. Host-wide figures removed. Complete-only clean history. Counting version. Ring rebuilt into a new table. Ledger credits `L`. | Properties: `L <= U`; the unified rows match section 1; P4 against host-wide fuzz; no ratio pairs differing versions; credits stay within `capacity − available`. #2089 r2's double-credit replay. | The previous binary reads its untouched rows |
| 2 | One pure `charge()` replacing the `resolvePooledSize` and `fitPoolCharge` composition | P1–P7 as property tests. Replays of every #2089 counterexample. | Revert the binary; the function is stateless |
| 3 | Learning record, stop receipt, classification, search step | Fake-clock tests. T1: the 60 GiB pressure loop ends in at most three limited attempts. T2: release, then OOM result. T3: `OOMKill` with no ceiling diagnostic. T4: a late result after the next admission raises only the following charge. T5: a victim is not escalated. T6: a lost-host victim leaves `stopping` within the loss bound. T7: every arrival-order permutation gives the same next charge. | Previous binary ignores the new table. `F` stays compatible with `Lower`. |
| 4 | Fleet check on the unified-memory worker, with release authorization, idle and isolated from production | (a) Counter-overlap probe: `cudaMalloc`, managed, pinned and `cudaHostRegister`, comparing cgroup and per-process deltas. (b) One accelerator job alone, then two together, capturing `c`, `d_sum`, `d_max`, `available`, ledger room and credits at each sample. (c) One bounded unreserved-growth stop. Accept when the sum of credits never exceeds `capacity − available`, every charge matches `charge()`, and the stop escalates as section 3 says. | Leave the disjoint-counter flag off; stages 1–3 stand alone |

## 7. Question for the maintainer

1. **When an attempt proves an owner resize too small, should Mycelium raise the
   charge automatically?** Proof means an overrun or an OOM above `O` with
   co-tenants within their charges. Recommended: **yes**. The raise is recorded
   in the job's resize ledger, and the raised charge applies to the next
   attempt. With **no**, the job holds with a typed reason until its owner
   resizes it again. Either way, it no longer loops at `O`.

Nothing else here is a product choice. The other rules follow the rulings
(#1972: a limit is an explicit opt-in ceiling; MY-34; MY-56) or are technical.
