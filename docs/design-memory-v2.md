# Memory v2: admit on what is free, learn what jobs use

**Approved by the maintainer on 2026-11-27 (MY-60).** This
replaces the [approved memory admission design](design-memory-admission.md)
and the parked draft [#2114](https://github.com/1kuna/mycelium/pull/2114). It
contains no code. Direction: the maintainer, 2026-11-26, "the redesign should
possibly be a full redesign instead of constant patching" (#2086).

## Summary

1. Today a job's memory is a prediction (the "charge"), and four separate parts of Mycelium each turn that prediction into a different answer.
2. Every time two of them disagreed we added a special case. There are now about 36 of them, in 8,750 lines.
3. v2 makes the live reading of free memory the truth, and learns each job's size from what its earlier runs actually used.
4. A job starts when its expected peak fits in memory nobody else has claimed. Every decision counts a running job the same way: the larger of its expected peak and the most it has used so far.
5. Memory is counted the same way everywhere. One rule per platform, so the scheduler, the stop logic, the learning and `myce efficiency` agree.
6. A job loading inside its own size is never stopped. Where the OS can't fence GPU memory, the worker kills a job that outgrows its size once the machine is into its reserve. That reserve is sized from the fastest growth measured (8 GiB a second) times how long the kill takes.
7. After any memory stop, the retry waits for room for what ended the attempt: a larger size if the job outgrew its own, or the other owners' measured peak if their growth ended it, held twice as long each time the same burst returns. Otherwise it waits with a reason that says what is missing. Any repeat stops left are bounded, and stage 0 counts them.
8. Over 14 days of real runs, these sizes would have reserved 1.08–1.19× what repeat jobs used. Today's charges reserve 1.41–2.17×. "Alone" becomes a number: the whole pool.
9. The decision code shrinks from about 4,800 lines to about 2,800, in five stages. Each stage ships on its own and is testable on a laptop. Each hardware-dependent path stays off until its probe passes.
10. You decided both safety choices yes (MY-59, section 6): killing an oversized job without letting it save, and making production jobs the kernel's preferred victims. The second is a bias, not a guarantee.

## 1. The root problem

**Mycelium treats a prediction as the truth about memory, and the hardware's own reading as a secondary check.** Each job gets a charge: a measured peak scaled by calibration ratios, a typed figure, or a first-run prior. Four parts of the system then turn that charge into separate answers: the queue's ledger, the worker's launch gate, the pressure forecast and the learning store. Each works from its own mix of ten partial counters, combined five different ways (Appendix A.2), because on unified CUDA no single counter holds a job's memory. Predictions are often wrong: by the approved design's own measurements, when evidence is thin, one run in a hundred needs more than ten times its history. So the four answers disagree, and each disagreement became an incident. Each fix reconciled one pair of models by adding a state (alone, a floor, a sealed record, a startup-loss repeat, an attempt-presence fence), and none removed a model. The code grows with every contingency because the thing it reconciles, a prediction held up against physical readings, never goes away.

The evidence:

- The queue offered a job 25 times and the worker refused it each time (#2091).
- A job with a 21 GB peak was charged 47 GB (#2086).
- On 2026-11-26 at 18:24Z a model load was stopped while still inside its own reservation, with 63 of 88 GiB used, because the forecast projected its own load forward.
- An exclusive retry blocked 93 waiting jobs (#2179).
- Since 1 November, 158 merges to main have touched these files and added 4,645 net lines. The last two design rounds each found six new P1s, and each fix needed a new mechanism.

## 2. The model

### Physical facts

- **F1.** Each pool of memory has a fixed size, and the OS or driver can report how much of it is free right now.
- **F2.** Other owners' use changes on its own, so a host keeps a margin for it (MY-45, MY-48).
- **F3.** The same work on the same platform needs about what it needed before. In 14 days of retained runs, 85% of repeat runs stayed within the largest of their earlier peaks, and the median overshoot of the rest was 2% (Appendix A.5).
- **F4.** The Linux kernel enforces a cgroup's `memory.max` by killing inside that cgroup. Nothing enforces three other things:
  - device memory on a discrete card;
  - device allocations on unified CUDA, which escape the cgroup (#1044, #2091);
  - a process group on macOS.
- **F5.** Mycelium acts only on what it has sampled. The worker reads each attempt's cgroup every 200 ms and its device memory every 5 s (`attempt_memory.go:17`, `host_memory.go:443`), and checks in every second. A queue stop also waits for the check-in round trip (9.5 s p50 under load, #2179) and a 10 s grace. The fastest growth measured is 8.06 GiB/s, a unified CUDA load: a 4 GiB reserve lasts 0.5 s. Kill-to-free time is unmeasured (probe 0b-2).

### Six concepts

| Concept | What it is | Rests on |
|---|---|---|
| **Pool** | Host RAM, and each discrete card; a unified host has only RAM. Each pool has one live **free** reading, sampled by the worker: the OS's available memory less keep, or for a card, the driver's free figure. Its **capacity** is the host allowance for RAM, or the card's size. | F1 |
| **Keep** | What the pool leaves free: the measured idle burst or the owner reserve, unchanged. A pool the kernel doesn't contain adds a **stop term**, its platform's fastest measured growth × the worker's stop time (detection plus kill latency), so growth during an immediate stop (R4) can't reach the owners' part. | F2, F4, F5 |
| **Footprint** | One number per attempt per pool, from one counting rule per platform (table below). Admission, holds, stops, learning, status and `myce efficiency` all call the same function. | F1 |
| **Expected peak (E)** | The largest clean footprint among the last 20 runs of this work on this platform family. A stop over E or a kernel kill raises it until the next clean run (R5). | F3 |
| **Hold** | What a running attempt keeps others out of: `max(E, high-water) − footprint`, never below 0, where high-water is its own peak so far. On a *contained* pool, once the attempt has stopped growing for one reaction time, its hold drops to `high-water − footprint`. An E raised by a stop is never lent this way. | F3, F4, F5 |
| **Slack (S)** | `free − Σ holds`: memory nobody has claimed. An attempt's **commitment** is `max(footprint + hold, high-water)`: the larger of its E and its high-water, or its high-water once a contained pool lends. An attempt that has gone past its E counts at what it has proven it reaches. S rises by exactly its commitment when it ends. On RAM, S is also capped at `allowance − Σ commitments` (MY-45). A pool's **reach**, `S + Σ commitments`, is its slack with nothing running. Every projection below uses these sums. | F1 |

### Counting rule

| Platform | RAM footprint | Card footprint | RAM pool contained? |
|---|---|---|---|
| Linux, CPU work | the attempt cgroup's `anon + shmem + kernel`, excluding page cache | none | yes: the worker slice's `memory.max` |
| Linux, discrete GPU | the same, plus the driver's per-client system memory where that sits outside the cgroup | per-process device memory, summed: NVML, or DRM fdinfo `drm-resident-vram*` | only after probe 0b-3 shows the driver's memory is inside the cgroup |
| Linux, unified CUDA | `anon + shmem + kernel`, plus per-process device memory summed | none: unified | no |
| macOS | `phys_footprint` summed over the attempt's process tree; Metal buffers count one for one (#1044) | none: unified | no |

A run is **clean** only when every counter in its row was read for the whole run. A missing counter is never filled in from a host-wide figure.

### Rules

- **R1 Admit.** A job starts when `E ≤ S` on every pool it needs.
  - The worker computes S from one sample.
  - The queue uses the worker's S, less the E of every assignment that sample doesn't yet list.
  - The worker's launch check runs the same function.
- **R2 Production.** When R1 fails for production, release lower-tier attempts in stop order only if releasing all of them would make it fit: `E ≤ S + Σ lower-tier commitments`, each the larger of that attempt's E and high-water. Otherwise it waits for other production, never for lower tiers (MY-56), and is never overcommitted against it.
- **R3 Fair waiting.** A pool's **head** is the highest-ranked job that fits its reach but not S. Its earliest start `t*` is the first time S, plus the commitments of attempts expected to end by then (each the larger of its E and high-water), covers its E. A job behind the head starts if it fits S and either:
  - it is expected to finish by `t*` (an unknown duration never finishes); or
  - S at `t*`, less its own E, still covers the head's E.

  This is EASY backfill, and it replaces the pool accumulation fences (#2186).
- **R4 Stop.** The queue forecasts each pool one reaction time ahead, from the worker's report.
  - Each attempt grows at its own measured rate; while it is within its E, no further than its hold.
  - Anything else that uses memory keeps its measured rate: the fall in free memory that the attempts' own growth doesn't explain.
  - The pool is **short** when that forecast takes free memory below zero. On macOS, the OS's warning pressure level also counts as short.
  - Stop one attempt at a time: the lowest tier first, then any attempt over its E (largest overshoot first), then resumable before non-resumable, then the newest.
  - On a pool the kernel doesn't contain, the worker itself stops any attempt over its E while the pool is below keep: at once, from its own sample, with no checkpoint grace (question 2). A queue round trip can take seconds (F5). The kernel acts the same way at `memory.max`.

  A job loading inside its own hold, with room for it, is never stopped.
- **R5 Learn.** Each pool learns on its own.
  - A clean run adds its peak to the window, and E returns to the window's maximum.
  - A stop over its E, or a kernel kill (a failed allocation never shows in the peak), raises the next E on that pool to `2 × max(peak, E)`.
  - A stop within its E leaves E alone. S already held the attempt's growth up to E, so the shortfall came from outside it: from an attempt over its E, whose high-water already holds what it reached, or from the pool's other owners. For the owners, the pool gets an **owners' hold**, `X − x` and never below 0:
    - x is their use now: the pool's used memory less the attempts' footprints;
    - X is the use the stop's forecast took them to.

    S subtracts it like any other hold, so R1 waits until a job fits beside the owners' peak, not just their current use. A later stop within E only raises X. The hold lapses once their use has stayed below X for its window, 24 h at first. A stop within E that sets an owners' hold on a pool whose last one lapsed less than one window ago reinstates it with twice that window: 48 h, then 96 h. The window returns to 24 h once a full window passes after a lapse with no reinstatement.
    On a discrete card, the driver free counter and the attempt counters have separate cadences.
    The difference between them cannot attribute growth to another owner.
    Card stops use driver free and the current attempts' growth.
    Card admission uses driver free and the current reservations, including each attempt's
    measured high-water and capacity still held pending exact release (#2264).
  - Each pool's raised E is capped at that pool's reach, the largest on any host that can run the job. If the cap leaves it no higher than the measured peak or the old E, a retry can't do better, so the job waits under the existing `memory_capacity` hold. So does any job whose E on some pool exceeds that pool's reach. The hold names the pool, its peak, any owners' peak and the reach, and clears when that reach grows past them.
  - A kill at the job's own `memory_limit_gb` still fails it (#1972).

  So each retry has room for what was measured to end the attempt before it, or waits with the reason. Appendix C walks the repeat stops that remain.
- **R6 First runs.** With no runs of the exact work, E falls back in order:
  1. this work family's peaks on this platform;
  2. a typed figure or size hint (host plus device on unified memory);
  3. the median first-run peak on this platform family;
  4. the pool's reach, which happens once per platform family per installation.

  On a discrete card with no measured device peak, the card's E is the whole card. The reason is physical: a failed allocation can land on a co-tenant. This keeps the 2026-11-23 agent default.

  For one-shot XPU extension workloads that declare `workload`, card sharing
  requires a clean peak in the exact work's window for the same immutable
  staged inputs. The number of records can stay at one while the input and
  its peak grow (#2279). A new input or a mutable input path takes the whole
  card. Once covered, it uses the exact window's maximum under R5.

"Alone" is no longer a state. It is just E equal to the pool's reach. Whether two jobs that fit one GPU *should* share it is a throughput question, not a memory one. The existing comparison of alone and shared runs answers it (#2037).

### Constants (MY-34)

| Constant | Value | Failure it prevents | Cost | Basis |
|---|---|---|---|---|
| Window | 20 clean runs | Old versions of the work setting today's E | A rare large run is forgotten after 20 runs | The maximum of 20 runs covers the next run 95% of the time (order statistics); 94% measured (approved design §2.3) |
| Overshoot step | × 2 | Repeated near-miss retries (#1484) | Up to 2× hold on one retry | Exponential search |
| Owners' hold window | 24 h with the owners' use below X, before any lapse back-off | The same owners' burst stopping a job again | Up to `X − x` idle for at least a day after each stop within E | The shorter of the two external-memory windows each host already keeps (24 h and 7 d, `external_memory.go`). Whether owners' bursts recur daily is unmeasured; the stage 0 shadow counts repeat owners' holds. |
| Lapse back-off | Window × 2 for each reinstatement within one window of a lapse (24, 48, 96 h); back to 24 h after one full window without one | The same owners' burst stopping a job once per window, indefinitely, after each lapse | `X − x` idle twice as long after each reinstatement | MY-34: a burst that returned after its hold lapsed is measured evidence that it recurs, so the hold lengthens on that evidence alone, and resets when the evidence stops. Exponential, like the overshoot step. |
| Reaction time | queue: check-in period + stop grace; worker: detection + kill latency | Stopping too late | none new | Existing values; probe 0b-2 |
| Keep | per host, plus the stop term on uncontained pools | Starving the host and its owners; a host OOM during an immediate stop | Idle memory: 8.06 GiB per second of stop time on the unified host | MY-45, MY-48; measured rate × measured stop time |
| Kernel kill order | lower tiers +1000 and production 0 today (`memory_ceiling.go:400`); production +500 (question 3, MY-59) | The kernel killing a host service, such as the queue, in a host-wide OOM | Production dies before smaller host processes | The score adds `adj/1000` of RAM and swap: +1000 outranks every unadjusted process, +500 only smaller ones (question 3) |

Gone: the confidence target, rule Q, ratio rings, transfer from other platforms, and the platform quantile.

## 3. The known cases

Unified CUDA rows assume probes 0b-1 and 0b-2 pass, and that keep's stop term there stays under about 15 GiB (a stop time under 1.8 s at 8.06 GiB/s). A longer stop time lowers S one for one.

| Case | What happened | Under v2 |
|---|---|---|
| **47 GB holder against a 21 GB peak** (#2086) | Charged 46.95 GiB, which is 2.277 × 20.62 from a ratio ring that mixed runs with and without device readings. Room was 43.6 GiB while the host had 88.8 GiB available. Three jobs waited 3–3.5 h. | E is 20.99 GiB, its clean peak, so its hold is about 0.4 GiB. S is about 69.5 GiB: 96 allowance − 21.0 − 5.5. The 24.4 GiB waiter starts at once. The 60 GiB waiter, sized only by its typed figures, then waits for a holder to finish: a real limit of the allowance. |
| **FP8 canary loop** (#2091) | Stopped nine times and readmitted at the same 60 GiB each time. On r6.49 the forecast projected 47.75 GiB of loss from a 4.34 GiB/s load against 33.57 GiB of room. #2129 then made retries exclusive and paused a second stop. | Its footprint went past 60; the device counter alone read 79.6. So the stop counts as over E, and R5 raises E to `min(2 × 79.6, 96) = 96`, the host's reach (its allowance). The retry waits for the pool and holds all of it, and its own load doesn't move S. If it outgrows 96 too, the cap can't rise past its peak, so it waits under `memory_capacity` with the measured peak and the 96 GiB reach. That is at most two attempts, not nine. #2091 inferred this may be its true outcome. |
| **In-charge victim stop**, live, 2026-11-26 18:24Z | An 88 GiB-charged model load had used about 63 GiB (device 61.8, host 1.3). The forecast projected 88.7 GiB of loss (8.06 GiB/s × 11 s) against 50.6 GiB of room and stopped it, though the job could grow at most about 25 GiB more inside its own charge. Its retry is now exclusive and waits for the whole pool. | The forecast caps its growth at its 25 GiB hold, which the 50.6 GiB of free memory covers while keep's stop term stays under 25.6 GiB. No stop, and S is at most 8 GiB: the 96 GiB allowance less its 88 GiB. |
| **In-charge victim stop**, the 87 GiB job (#2091) | Each counter alone sat under its charge (69.0 cgroup, 80.0 device). By host-wide subtraction it held 93–97 GiB. Its retry ran exclusive at the same 87. | The one rule sums the counters, so the overshoot shows, but the raw sum overstates it: a sibling attempt of the same workload read 32.9 + 79.6 GiB while the host showed about 95. With the overlap corrected (probe 0b-1), its footprint is about its host-wide 93–97 GiB. It is stopped first as the over-E attempt, and no co-tenant of its tier goes before it. R5 caps the raise at the 96 GiB reach: from a 93 GiB peak it retries at 96, and from 97 it waits under `memory_capacity` with that figure. |
| **Alone holder blocking 93 waiting jobs** (#2179) | After one pressure stop, the retry's RAM and card pools went `alone`, so every pooled candidate conflicted with it. Maintenance then visited every waiting job: O(waiting × targets × live). Check-in hold rose from 0.7 s to 2.4 s. | There is no alone state. The retry has a number: its own E if it was a victim, with the owners' peak held on the pool, or doubled if it went over E and capped at that pool's reach. The 93 pieces compare E with S like any job, with no custody conflicts and no release planning between same-tier jobs. If the retry's E is the whole reach, it becomes the head, and R3 lets short pieces finish around it. |
| **About 3.7× over-reservation** | Live at 22:28Z: 261 reserved against 70 measured GiB-hours. That figure covers only the current 8.8 h and 4.6 h windows, and on unified hosts "measured" leaves out device memory. 14-day per-program figures: 2.9× on unified CUDA and 2.1× on the XPU host. The current pool estimate charges about 3× (2.97×) the summed host and device peaks of 126 accepted unified GPU attempts. | Replayed over 14 days of repeat runs, E would reserve: 1.19× on unified CUDA (today's charges: 2.17×), 1.08× on the XPU host (1.46×) and 1.17× on Macs (1.41×). Holds on contained pools also shrink to the proven high-water. `myce efficiency` counts the same footprint as admission. |
| **One GPU could hold two jobs**, unified (#2037) | Once a production model stage's charge was corrected from 102.6 to 24.4 GiB, two ran together, and each took 8–9.5 min against 162 s alone. | Memory says both fit. The throughput comparison (#2037, merged 2026-11-24) makes that work exclusive on the device. Memory and throughput stay separate decisions. |
| **One GPU could hold two jobs**, XPU (#2185) | 973 of 973 XPU attempts recorded no device memory. Every card E was the whole card, so no XPU card was ever shared. | v2 needs a card counter as much as today's code does. Stage 2 fixes it after probe 0b-4 names the cause. With it, a card's E is its measured peak, and two jobs share a card when both fit. |
| **Typed 16 GiB holder at a 4.0 GiB peak** (audit) | It held 15.2 GiB as pending growth, which left 1.45 GiB of room for a 9.76 GiB piece. | If probe 0b-3 shows the XPU host's RAM is contained, the hold falls to 3.2 GiB and room rises to about 9.4 GiB. If not, the hold stays at 15.2 GiB until this first run ends, and later runs of the family hold about 3.2 GiB. At today's keep, the 9.76 GiB piece still misses by about 0.3 GiB, because the host has 12.6 GiB free above keep and the holder may return to its 4.0 GiB peak. Smaller jobs fit. The RAM install (#2034) is the real fix. |
| **90 GiB demand fencing 7 small jobs** (#2186) | The accumulation-fence budget was negative, so nothing backfilled beside 40.6 GiB of room. | Under R3, the small jobs whose learned durations end before the head's earliest start begin now, and so does any job that leaves S at that time covering the head's 90 GiB. The head isn't delayed. |
| **Stale inventory around restarts** (audit cause 2) | 137 jobs and 498–612 job-hours, low confidence. Mostly device inventory and worker contact. | There's no ledger to rebuild. A restarted worker's first sample carries free memory and each attempt's footprint and high-water. The queue keeps only each attempt's E. Device inventory is outside this design, and an unknown reading still admits nothing. |

## 4. What it deletes

The dedicated memory code is 37 files and 8,750 lines today, plus 48 test files with 9,966 lines (Appendix A.1). About 3,950 of those lines, in 20 files, sample memory and enforce containment, and they stay. The other 4,800, in 17 files, decide: estimates, ledgers, gates, forecasts, fences and exclusivity. The stages remove about 3,600 of those and add about 1,600, which leaves about 2,800. Overall that is about 2,000 lines fewer (−23%). An estimated 5,000 test lines go with the mechanisms they test. Of the 36 special cases in Appendix B, 9 rules remain. Appendix B gives each case's fate, and Appendix E gives each stage's tests.

| Stage | Changes | Deletes | Removes |
|---|---|---|---|
| **0 Shadow** | One footprint function per platform. The worker computes holds, S, reach and each platform's fastest growth, and reports them. Status and `myce efficiency` show v2 numbers beside today's. Fake-clock replays of every case in section 3. No decision changes. | none | (measures the rest) |
| **1 Stop on real shortage** | R4's per-attempt forecast replaces the whole-host forecast. The card guard becomes the same check on card pools. The worker's immediate stop and keep's stop term on uncontained pools. Production's kill score (question 3, MY-59). | The whole-host forecast and the separate card guard | in-charge victim stops |
| **2 Learn E** | R5, R6 and the counting rule, with R5 on both result paths. The XPU card counter (#2185). | Rule Q, ratio rings, confidence feedback, foreign and platform quantiles, evidence repair (after the MY-42 floor check), combined and GPU-host bounds, the bystander test. Then the exclusive retry after a pressure stop, the alone-stop pause and the requeue route scan. | 2–3× inflation (cause 3), uncounted cards (cause 4), the same-charge loop |
| **3 Worker-owned slack** | Holds replace charges. The queue admits on the worker's S. The launch check is the same function. | The two-arm queue ledger, the RAM gate and its fences, launch-capacity waits and startup-loss repeats, retained host charges, sample-identity fences, `fitPoolCharge`, alone custody, the unified CUDA floor | full charge held until the end (cause 1), the launch-refusal loop |
| **4 Fair waiting** | R3 EASY backfill. | Pool backfill fences; the legacy envelope reader, one release later (MY-42) | fence holds (#2186) |

**Safeguards stay until replaced.** Today's pressure-retry safeguards run on two result paths:
- the normal one, through `requeueMemoryPressure` (`queue.go:2739` and `:2778`);
- the release-only one (`queue.go:2577`), which calls `holdAlonePressure` and then `retry`.

Stage 2 deletes them only after R5's raise and hold run on both paths, each with a replay.

**Probe gates.** A path turns on for a platform only after that platform's probe passes. Until then the platform keeps today's path. The probes need release authorization.
- **0b-1, unified attribution.** Known `cudaMalloc`, `cudaMallocHost`, managed and `malloc` sizes each count once in the footprint, and footprints explain the host's drop within its idle burst. It gates R4's per-attempt split (stage 1) and learning from the summed footprint (stage 2) on unified CUDA.
- **0b-2, stop time and victim order,** on each uncontained platform. Allocate past free memory at 1, 4 and 10 GiB/s. Record the worker's time from detection to memory freed, whether the device or driver read fits the 200 ms sampler, and which process the kernel kills. It gates the immediate stop and keep's stop term (stage 1). At today's 5 s device period, the stop term alone would exceed 40 GiB on unified CUDA.
- **0b-3, XPU containment.** Driver system memory against the cgroup's charge. It gates treating that host's RAM as contained, and so lending there (stage 3).
- **0b-4, XPU card counter** (#2185). One running attempt's DRM fdinfo lines and the selected device's PCI address. The fix may need both the PCI identity and the fdinfo unit parsing, which accepts only bare numbers, `KiB` and `MiB` today. It gates card E from measurement (stage 2).

Stopping comes first, so that the tighter packing in later stages can't create false stops. Each stage is one release, runs under `myce dev` with fake GPU inventory and fake memory readings, and keeps scheduling decisions on the injected clock. It merges on quick plus targeted tests and its incident replays (MY-41, the #1972 rollout ruling). The consumer skill and `docs/jobs.md` change in stage 3, when the job's `memory` fields change.

**Decision lines by stage** (non-test; removals by file in Appendix A.1, additions estimated):

| Stage | Adds | Removes | Decision lines after |
|---|---:|---:|---:|
| Today | | | 4,795 |
| 0 Shadow | 500 | 0 | 5,295 |
| 1 Stop on real shortage | 250 | 250 | 5,295 |
| 2 Learn E | 400 | 1,100 | 4,595 |
| 3 Worker-owned slack | 250 | 1,950 | 2,895 |
| 4 Fair waiting | 200 | 330 | 2,765 |
| **Total** | **1,600** | **3,630** | **2,765** |

With the 3,955 lines that stay, the memory code goes from 8,750 to about 6,720 lines.

## 5. Risks, and the measured check for each

### 5.1 Pools the kernel doesn't contain

The expected peak bounds admission, not allocation. On these pools nothing stops an attempt that outgrows its E except Mycelium, so each one keeps a residual risk.

| Pool | What escapes | Protection | What's left |
|---|---|---|---|
| Unified CUDA RAM | device allocations (F4) | The worker stops an attempt over its E once the pool is below keep (question 2). Keep's stop term covers 8.06 GiB/s × the stop time. | Growth faster than any measured, or an owner burst during the same stop. The kernel then picks a victim (question 3). |
| Discrete-GPU host RAM | the driver's system memory, unless probe 0b-3 shows it inside the cgroup. The queue's own host is one (MY-45). | The same, at that platform's own measured rate | The same. Until stage 0 and probe 0b-2 measure the rate and stop time, the path stays off. |
| macOS RAM | the whole process tree | The same, and the OS warning level counts as short | The same |
| A discrete card | one process's device memory | Card E is the whole card until measured (R6) | An overshoot fails an allocation, possibly a co-tenant's. The host is unaffected. |

On a contained pool the kernel kills inside the worker slice, which bounds the host. What's left is a co-tenant killed inside the slice, and R5 gives it a doubled E for one run.

Live check: kernel OOM kills of non-Mycelium processes per week (kernel log) should be zero, beside the count of immediate stops.

Without question 2, an over-E attempt keeps its 10 s grace. At 8.06 GiB/s that is 80 GiB, more than any keep could hold. On these pools v2 would then have to forecast loads within E uncapped, which brings back today's in-charge stops, or leave the host to the kernel.

### 5.2 Other risks

| Risk | Why it could happen | Check that would show it |
|---|---|---|
| **E too small** | 15% of repeat runs exceeded the largest of their earlier peaks: median overshoot 2%, p90 24–35%, worst 172× (Appendix A.5). Small overshoots use slack. Large ones cost one stop and a doubled E. | Stage 0 shadow: R4 stops per week split into over-E and victim, against today's `memory_pressure` stops. Attempt-hours burned (audit baseline: 45.4 h a week). |
| **Unified counters overlap** | `anon + shmem + kernel` plus device memory may count pinned or managed pages twice. #2114 found 24 GiB showing as 112 in a five-process case, and a live attempt read 32.9 + 79.6 = 112.5 GiB while the host showed about 95. Because every decision uses the same rule, a constant overlap cancels in holds. E and reach are absolute, though: overlap inflates E (idle memory) and can push a job to the `memory_capacity` hold early. | Probe 0b-1. If pages are counted twice, the rule subtracts the attempt's pinned pages, which the sampler already reads. |
| **Raised E and owners' holds idle memory** | A raise lasts until the next clean run. An owners' hold lasts at least a day, twice as long each time the same burst returns, and a large owners' burst makes a large one. | Shadow: raises and owners' holds per week, and the GiB-hours each holds. |
| **Lending on contained pools** | A holder that stopped growing and then grows again forces the newest job to stop. | Shadow: count stops whose victim started after the holder stopped growing. Compare that with the waits recovered (audit: 261–767 job-hours a week behind holder charges). |
| **Queue and worker skew** | The queue admits against a sample taken before its newest assignments. Each unlisted assignment counts at its E, and the launch check is the same function. | Launch refusals with an unchanged S should be zero. #2091 reached 25 for one job. |
| **Residual repeat stops** | After the lapse back-off and the high-water commitment, repeats are bounded: an owners' burst after a lapse stops a job at most at 24, 72, 168 h… after the first stop, or about once per 48 h if it returns only after a reset. A within-E job beside an attempt over its E can be stopped on each fast climb until that attempt ends (Appendix C). | Stage 0 shadow: repeat stops per week, meaning a second R4 stop of the same job on the same pool, split by cause (lapsed owners' hold, co-tenant over E, own overshoot). |
| **EASY delays the head** | A duration estimate that is too short lets a backfill run past the head's start. | Fake-clock tests and live comparison of the head's actual start with its earliest start. |
| **Mixed versions** | The adjacent release must interoperate (MY-42). The S report is negotiated as a capability, like `host-ram-gate-v1`. | `scripts/upgrade-compat.py` in both directions at every stage. |

## 6. Questions for the maintainer

1. **Approve the replacement and its five stages?** It supersedes the 2026-11-23 design. It also replaces one agent default: "start at nine in ten", meaning the confidence target that sized charges.
2. **Stop an oversized job without letting it save? Decided: yes (MY-59).** On a host that can't fence GPU or driver memory, a job that grows past its expected peak while the host is below its reserve is killed at once by the worker, with no checkpoint grace. That includes production, which loses unsaved progress. Without it, growth during the grace couldn't be bounded (section 5.1).
3. **Rank production ahead of host processes in the kernel's kill order? Decided: yes (MY-59).** Lower tiers already rank first (+1000), which holds against every unadjusted process. Production is at 0 today, level with host processes. At +500, the kernel prefers a production job to any host process using less than the job's own counted memory plus half of RAM and swap. That protects small services such as the queue (3.4–3.7 GB RSS, #2179). It is a bias, not a guarantee:
   - on a 32 GiB host, a production job using 0.5 GiB scores like 16.5 GiB, below an owner process using 20 GiB, so the owner process dies first;
   - on unified CUDA the kernel may not count GPU memory toward the score at all (probe 0b-2 records it).

   It changes nothing until the kernel is already killing.

---

## Appendix A. Evidence

### A.1 Code and churn (origin/main `0b07970c9`, 2026-11-26)

- **37 dedicated non-test files, 8,750 lines.**
  - **Stay, 20 files, 3,955 lines:** sampling and containment. `memory_ceiling` (812), `attempt_memory` (743), `model_memory` (382), `gpu_memory` (316), `rss_guard` (285), `external_memory` (246), `gpu_host_memory` (187), and 13 smaller sampling files (984).
  - **Decide, 17 files, 4,795 lines:** `memory_pools` (879), `host_memory` (622), `memory_envelope` (416), `priority_preemption` (388), `memory_evidence_repair` (331), `host_backfill` (304), `memory_estimator` (292), `memory_evidence` (277), `launch_capacity` (259), `memory_admission` (247), `memory_pressure` (244), `memory_room` (181), `memory_learning` (108), and four smaller files (247).
  - **Removed by stage** (estimates from function sizes):
    - Stage 1, about 250: the whole-host forecast in `memory_pressure` (~110), `shared_gpu_pressure` (64), and the host forecast helpers in `host_memory` (~75).
    - Stage 2, about 1,100: `memory_estimator` (292), `memory_evidence_repair` (331), sizing and bounds in `memory_envelope` (~200), pooled sizing and the pressure requeue in `memory_pools` (~220), and the alone-pressure hold in `memory_pressure` (~55).
    - Stage 3, about 1,950: `memory_admission`, `memory_room`, `launch_capacity`, `host_ram_gate_wire`, `host_memory_budget` and `memory_planning` (870); the ledger, custody and RAM gate in `memory_pools` (~530); and releases, samples and retained charges in `host_memory` (~545).
    - Stage 4, about 330: the pool fences in `memory_pools` (~130), their part of `host_backfill` (~100), and the legacy envelope reader (~100).
    - Kept and reused, about 1,170: `priority_preemption` (R2's release path), `memory_evidence` (stores the window), `memory_learning` (mostly storage learning), and the rest of `host_backfill`, `memory_envelope` and `memory_pressure`.
- **48 dedicated test files, 9,966 lines.** About 84 other functions also branch on memory, inside check-in, completion and worker launch. They aren't counted.
- **Churn.** Since 2026-11-01, 260 commits and 158 merges to main touched those files: +10,771 / −6,126 lines. In the three days after the 2026-11-23 approval, 23 merges touched them.
- **Design rounds.**
  - #2089: two review rounds.
  - #2098: two rounds, then replaced.
  - #2103: closed.
  - #2106: three rounds.
  - #2114: two rounds, six P1s each, then parked.
  - #2129 and #2180 followed.

### A.2 Ten RAM figures, combined five ways

An attempt's peak record carries `HostGB`, `CgroupGB` (`memory.peak`, which includes page cache), `NonreclaimableGB`, `CombinedHostGB`, `DeviceGB`, and five `GPUHost` parts: pinned, physical, driver, host drop and outside. Each use combines them differently:

| Use | Combination | Code |
|---|---|---|
| Learning | unified: `HostGB + DeviceGB`, raised to `CombinedHostGB` | `memory_evidence.go:70-76` |
| Live ledger footprint | the cgroup footprint, else non-reclaimable plus device on unified | `memory_pools.go:479-487` |
| Ledger hold | the maximum of charge, peak, current and the retained charge | `memory_pools.go:489-504` |
| Victim overshoot | live footprint − typed host need | `memory_pressure.go:146-160` |
| Efficiency and history | the maximum of `HostGB`, non-reclaimable, `memory.peak` and combined | `memory_envelope.go:371-385` |

The typed need adds GPU to host on unified hosts (`host_memory.go:25-34`). The unified "combined" bound mixes host-wide figures (`gpu_host_memory.go:36-48`; #1690: 51.3 GiB from a host-derived bound).

### A.3 Live reads, 2026-11-26 about 22:28–22:31Z (read-only)

- **`myce efficiency`:**
  - XPU host: 103.6 reserved against 27.5 measured GiB-hours over 8.8 h (3.76×).
  - Unified CUDA host: 155.7 against 42.7 over 4.6 h (3.65×).
  - Together: 3.71×.
- **What "measured" leaves out.**
  - On the unified host, "measured" is the cgroup figure, so device memory is left out.
  - Snapshot: 55.4 GiB reserved, 18.5 measured, 53.5 physically used of 121.1, with about 4 GiB used at idle.
  - So Mycelium's real use there was close to its reservation at that moment.
  - The XPU host records no device usage at all (zero device-covered seconds).
- **Live `memory_pressure` stop at 18:23:57Z.**
  - Trigger: `stop_grace`, with 54.56 GiB available, 4 GiB reserve, a sustained 8.06 GiB/s, and 88.7 GiB of forecast loss against 50.6 GiB of room.
  - Charge: 88 GiB (78 device + 10 host, typed).
  - Peaks: device 61.83, host 1.25, cgroup 1.74.
  - Its retry now waits for the whole pool under #2129.

### A.4 Utilization audit, 2026-11-26 (#1493, #2183–#2186)

- **Reserved against measured** (per-program, 14 days): 2.9× on unified CUDA and 2.1× on the XPU host.
- **Needless waits** (7 days). 1,674 jobs waited at least 10 min. Of those:
  - 454 would have fit if holders were charged at their measured peaks (261–767 job-hours);
  - 122 had their own charge above their later peak;
  - 27 couldn't share a card because XPU device memory is unmeasured;
  - 47 were held by the accumulation fence;
  - 137 hit stale inventory.
- **The median blocking holder** had a charge of 1.08× its peak. Most of the loss was pending growth held until the end, not an inflated charge.
- **XPU host:** about 16.9 GiB usable of 31.2.

### A.5 Replay of E against today's charges

- **Method.** The data is 14 days of retained attempts: `learning_reason` eligible, with a host peak. Footprint is `host_gb`, plus `device_gb` for GPU work on the unified host; a unified GPU attempt without a device reading is skipped as not clean. Identity is project, name, args, program, extension and the needs other than memory, so it approximates the exact key. For each attempt that had an earlier clean run, E is the largest of the last 20 earlier peaks.
- **Today's charge** is the envelope's host figure.

| Platform family | Attempts | E ÷ peak | Today's charge ÷ peak | Peak above E | Overshoot p50 / p90 / max |
|---|---:|---:|---:|---:|---|
| Unified CUDA | 1,983 | 1.19 | 2.17 | 14.9% | 1.02 / 1.31 / 172.7 |
| XPU host (RAM) | 1,391 | 1.08 | 1.46 | 15.7% | 1.01 / 1.24 / 17.0 |
| macOS | 978 | 1.17 | 1.41 | 14.7% | 1.02 / 1.35 / 8.2 |

On the unified host, the current pool estimate charges 24.2× the host-only peak of 126 accepted GPU attempts with a device reading, and 2.97× their summed host and device peak.

## Appendix B. Today's special cases and their fate

| # | Today | v2 |
|---|---|---|
| 1 | Six estimate levels (exact, family, foreign, platform, hint, unknown) | R6's four-step fallback |
| 2 | Rule Q quantile gate | deleted |
| 3 | Ratio rings by level and bucket | deleted |
| 4 | Confidence feedback | deleted |
| 5 | Transfer from other platforms | deleted |
| 6 | Limited-run floor × 2 | R5 |
| 7 | `fitPoolCharge` fallback above the allowance | E capped at that pool's reach; above it, the `memory_capacity` hold |
| 8 | Cold accelerator: drop alone, then use the CPU prior | R6 |
| 9 | Typed limit caps typical and charge | an explicit limit caps E where it contains |
| 10 | Owner resize fixes charge and ceiling | kept |
| 11 | Card alone without device evidence | E is the whole card |
| 12 | Exclusive retry after a pressure stop (#2129) | R5's raised E, in stage 2, on both result paths |
| 13 | Pause on a stop while alone | R5's `memory_capacity` hold when the cap can't rise |
| 14 | Requeue route scan into `memory_capacity` | R5: the same hold, from each pool's reach |
| 15 | Bystander test (stopped with peak ≤ charge) | R5: a stop within E keeps E, and the pool holds its owners' measured peak |
| 16 | Separate release-only learning path | one record path |
| 17 | Pool custody conflicts (alone against peers, engines and cards) | deleted |
| 18 | Head budget, `poolBackfillFits` | R3 |
| 19 | Launch-capacity wait with startup-loss repeat | deleted |
| 20 | Worker RAM gate, unreported assignments and attempt-presence fence | the S report lists its attempts |
| 21 | Worker RAM shortfall for reclaim | R2 on S |
| 22 | Two-arm room (allowance − charges, live − pending) | one free reading, less holds |
| 23 | Unified live reading unknown without fresh health evidence | freshness of the worker's sample |
| 24 | Unified CUDA floor | deleted; keep covers it |
| 25 | Darwin warning-pressure trigger | kept: the warning level counts as short |
| 26 | `next_sample`, `stop_grace` and `below_reserve` triggers | one R4 forecast per pool, with planned growth capped at holds, and the worker's immediate stop below keep |
| 27 | Shared-GPU guard (`device_full`, `over_budget`) | R4 on the card pool |
| 28 | Production stops only for production, and only if resumable | kept, except the worker's immediate stop (question 2) |
| 29 | Four device sources (host delta, unified resident estimate, phys footprint beyond RSS, owned footprint) | one counting rule |
| 30 | Combined and GPU-host bounds | deleted from decisions |
| 31 | Sample identity, bootstrap and cached-sample fences | one synchronized sample per check-in |
| 32 | Durable retained host charge across restarts | each attempt stores E; footprint and high-water are read again |
| 33 | Legacy envelope compatibility | one release, then deleted |
| 34 | Legacy evidence repair | deleted after the MY-42 floor check |
| 35 | Allowance-based reclaim branch | R2 |
| 36 | Typed GPU added to host on unified hosts | only in R6's typed first-run E |

## Appendix C. Rule details

- **Holds.**
  - High-water is the attempt's largest footprint this run.
  - "Stopped growing" means the high-water hasn't risen for one reaction time, measured on the worker's injected clock.
  - An attempt over its E holds `high-water − footprint` and ranks as over E in R4.
  - A loaded catalog engine is an attempt with its own E.
- **The S report.** Each check-in carries, per pool:
  - free, keep (with its stop term), the sum of holds, S, reach, the unexplained decline rate and the sample time;
  - each attempt's key, footprint, high-water, growth rate and E.

  The worker takes the free reading and the attempt footprints in the same pass, so a load burst can't make them disagree. The queue subtracts the E of each assignment the report doesn't list. That is the only fence. An unknown or stale reading admits nothing, as today. The queue's work per check-in is arithmetic over the report, with no storage or bulk reads under `q.mu`.
- **Launch.** The worker runs R1 again with a fresh sample. A refusal carries that sample, so the next offer uses it. A repeat refusal therefore needs a real change on the host.
- **R2 release planning.** Releasing attempt *i* raises S by its commitment. After the release is confirmed, the next report decides. This reuses today's release custody and stop path.
  - Review case: on a 96 GiB pool, a production attempt at its 80 GiB peak has hold 0 but commitment 80. Another production job with E 40 finds `S + Σ lower-tier commitments` at most 96 − 80 = 16, so nothing is released and it waits for production. The old test compared production's holds (0) plus 40 with 96, and would have released every lower tier for nothing.
- **R3 earliest start.** `S(t)` is S plus the commitments of attempts expected to end by `t`, and `t*` is the first `t` with `S(t) ≥` the head's E. Durations are the learned durations already used for placement; an unknown duration never ends.
  - Review case: a 96 GiB pool runs nine 10 GiB jobs at their peaks, so S is 6, and a 90 GiB head waits. `S(t)` first covers 90 when the ninth job ends (6 + 9 × 10 = 96), so `t*` is the last current finish. A new 10 GiB job starts only if it ends by `t*`; otherwise `S(t*) − 10 = 86 < 90` refuses it. The head starts at `t*`. The old projection subtracted holds only, so jobs at their peak (hold 0) looked absent, each staggered job passed, and the head never started.
- **Immediate stop (R4).** It runs on the worker, from its own attempt sample, against the E each assignment carries. The worker kills the attempt's process group and reports the stop, with its footprint and E, through the same result path as a kill-limit hit. R5 then treats it as over E. Its stop time is the detection period of the slowest counter in the footprint plus the kill latency.
- **R5 holds.** The queue keeps the uncapped raise with the hold. Once that pool's reach exceeds both the measured peak and the old E, the job is retried at the raise capped at that reach.
  - **Why the owners' hold, not E or keep.** A stop within E measured the owners, not the job. Raising E by the shortfall makes the job claim memory it never uses, while its next attempt still starts whenever the owners are below their peak, so the same burst stops it again. Raising keep doesn't help either: keep is also the line R4 stops at, so it moves the stop as far as it moves admission. S subtracts holds, but R4's stop line is free memory, which a hold doesn't lower. So an owners' hold reserves the owners' peak at admission without moving the stop.
  - **Review case, a recurring owners' burst.** A RAM pool has 96 GiB above keep. A lone job sits flat at its E of 80 GiB, so its hold is 0. The owners' use reaches 10 GiB, which leaves 6 free, and R4 forecasts 7 more. Free would reach −1, so R4 stops the job, which is within E.
    - Old R5: E becomes 81, the next attempt fits, and the same burst stops it at 81, 82 and so on, one GiB at a time, until the cap forces a hold.
    - Now: E stays 80, and X is 17. Once the owners fall back to 10, their hold is 7, so S is `96 − 10 − 7 = 79` and the reach is also 79. 80 exceeds that reach, so the job waits under `memory_capacity`, naming 80, the owners' 17 GiB peak and the 79 GiB reach. Smaller jobs still start: a 70 GiB job fits beside a 17 GiB owners' peak.
    - If the owners stay under 17 for 24 h, the hold lapses, the reach is back to 86, and the job starts. If the same 10→17 burst returns while it runs, R4 stops it within E again and reinstates X = 17, now with a 48 h window, and the next time 96 h. Counting from the first stop, the k-th repeat can come no earlier than `24 × (2^k − 1)` h: 24, 72, 168 and 360 h, so at most four repeats in 30 days, where a fixed 24 h lapse allowed one a day. A burst that returns only after a full window with no reinstatement finds the window reset to 24 h, so it can stop the job about once per 48 h at worst. Owners' use above 17 raises X as before.
  - **Review case, a within-E victim beside an attempt over its E.** A 96 GiB pool above keep, with no owners' use. A higher-tier attempt has E 60 but a high-water of 75, and cycles between 70 and 75. Its commitment is `max(60, 75) = 75` at both ends of the cycle, so S is `96 − 75 = 21`, and a lower-tier job with E 20 fits. At their marks the two leave 1 GiB free, so no real shortage occurs while the co-tenant stays at or under 75.
    - R4 caps growth at the hold only within E. The co-tenant is over its E, so its forecast uses its measured climb rate. At 70, free is `96 − 70 − 20 = 6`, so over the 11 s queue reaction time any climb faster than `6 / 11 ≈ 0.55` GiB/s (70 to 75 in under about 9 s) forecasts free below 0. R4 stops the lowest tier first: the 20 GiB job, within its E. R5 sets no owners' hold (x and X are 0) and keeps E at 20, so S returns to 21 and R1 readmits it.
    - **So the loop remains**, on each fast climb, for as long as that co-tenant attempt runs, which for a loaded engine can be long. It ends with that attempt: its clean run adds 75 to the window, so the next run of that work has E 75, R4 caps its growth at its hold, and the forecast never passes 75. Slower climbs never stop the job. The stage 0 shadow counts these repeats (section 5.2).
  - **Review case, a card stop beside a larger RAM pool.** The host's RAM reach is 87 GiB and its card is 24 GiB, with nothing else on the card. The job is stopped over its card E at a 22 GiB card peak, so the raise is 44.
    - Old R5: capped at the largest reach, 87, card E stays 44. R1 can never admit 44 on a 24 GiB card, even an idle one, and the capacity hold doesn't fire because 44 is under 87. The job waits without a reason.
    - Now: card E is capped at the card's reach, 24. That is above the 22 GiB peak, so one retry holds the whole card. If it fits, the run is clean and E returns to its peak. If it needs more, its next stop raises card E to 48, which caps at 24, no higher than the old 24. So the job waits under `memory_capacity`, naming the card, its peak and the 24 GiB reach. RAM's 87 never enters.
- **Explicit limits.**
  - `memory_limit_gb` is still the opt-in kill limit (#1972): the attempt's own `memory.max` on Linux, or the RSS guard on macOS.
  - It caps E only where it contains everything. On unified CUDA it doesn't contain device memory, so it doesn't cap E there.
  - An owner resize sets E and the limit together, as today.
- **Status.**
  - Job GET `memory`, per pool: `expected_gb`, `evidence {level, runs}`, `hold_gb`, `footprint_gb`, `peak_gb` and `limit_gb`.
  - `typical_gb` and `charge_gb` stay for one release.
  - There is one waiting reason, `memory_wait`, giving need, free, keep and held by others.
  - `needs.memory: auto`, typed figures and size hints all still mean a first-run estimate.

## Appendix D. Alternatives considered

1. **Admit on free memory plus only short-horizon growth for every running job.** Each job would reserve its growth rate × the reaction time, as Borg's decaying reservations do. Adopted only for attempts on contained pools that have stopped growing.
   - Rejected for growing work. A known load would lend out its own path and then stop whoever borrowed it.
   - Rejected on uncontained pools. There, a reaction that comes too late is a host OOM.
2. **A per-job `memory.high`.** Rejected for the approved design's reasons: it stalls a job until somebody raises the limit, and binary updates replace the worker under running jobs (MY-55).
3. **Interval counting `[L, U]`** (#2114). Rejected. Every consumer had to pick an end, and that multiplied the rules. v2 uses one point figure everywhere and lets the live reading absorb the error.
4. **Calibrated quantiles.** Rejected. In the replay, the window's maximum averaged 1.08–1.19× the next peak, while today's charges averaged 1.41–2.17×. Tails are handled by containment, R4 and R5, not by reserving for them, which is the approved design's own conclusion.
5. **Lending production's unused hold to lower tiers.** MY-56 allows it, but it adds churn. Deferred until there is stage 3 data.
6. **Stop decisions on the worker.** Adopted only for R4's immediate stop, which needs no tier choice: the victim is the over-E attempt itself. Other stops stay on the queue, which keeps tiers and release custody.
7. **Faster sampling instead of keep's stop term.** Rejected as the protection. Sampling faster shortens detection but not the kill latency, and a queue round trip dominates whenever the queue decides. The sample period is instead one input to the stop time, so the stop term shows what each second costs.
8. **Uncapped forecasts for loads within E on uncontained pools.** Rejected. That is today's rule, and it stopped the 18:24Z load inside its own reservation.

## Appendix E. Stages and their tests

- **All stages.**
  - Fake-clock tests for admission, holds, stops and learning.
  - Replays of every row in section 3.
  - A laptop run under `myce dev`.
  - `scripts/upgrade-compat.py` in both directions.
  - Tests for removed mechanisms are deleted with them (MY-53).
- **Stage 0.**
  - Property tests:
    - S never falls under growth within E;
    - the forecast never stops growth that fits inside holds and free memory;
    - holds never go negative;
    - S rises by exactly an attempt's commitment when it ends;
    - the queue's and the worker's S agree for the same sample.
  - A committed, synthetic replay fixture built like Appendix A.5.
  - Probes 0b-1 to 0b-4 (section 4) are separate and need release authorization.
- **Stage 1.**
  - Replaying the 2026-11-26 18:24Z case doesn't stop the load.
  - An over-E grower is chosen before a same-tier co-tenant within its E.
  - Below keep on an uncontained pool, the worker stops the over-E attempt without grace, from its own sample, and reports it through the kill-limit result path.
  - Keep's stop term equals the platform's fastest growth × its stop time.
  - Production still reclaims.
- **Stage 2.**
  - E equals the window's maximum, and returns to it after a clean run.
  - R5's raises: over E and a kernel kill. A stop within E keeps E and sets the owners' hold.
  - The reach cap: on the review's 100 GiB host (9 GiB non-job use, 4 GiB keep, 96 GiB allowance), a raised E is at most 87 GiB, and a larger measured peak waits under `memory_capacity`.
  - The card case in Appendix C caps card E at the card's 24 GiB reach, not RAM's 87, and ends in a clean run or `memory_capacity`.
  - The #2091 replay takes at most two attempts, then the hold with its measured peak.
  - The recurring-burst case in Appendix C: E stays 80, the job waits under `memory_capacity` at a 79 GiB reach, an owners' burst no larger than 17 GiB doesn't stop it while the hold stands, and the hold lapses after 24 h below 17. A stop after the lapse reinstates it for 48 h, then 96 h, and the window returns to 24 h after one full window without a reinstatement.
  - The co-tenant case in Appendix C: the over-E co-tenant commits 75 at both ends of its cycle, so S is 21; after its clean run its next E is 75, and the 20 GiB job is no longer stopped.
  - The normal and release-only result paths reach the same next E.
  - A missing counter makes a run not clean.
  - The XPU fdinfo units and PCI identity parse.
  - The #2086 replay charges 20.99 GiB.
- **Stage 3.**
  - Same-charge launch refusal (#2091) drops from 25 offers to 0 without a change.
  - Holds shrink after growth stops on contained pools only, and never for a raised E.
  - The audit's 16 GiB holder case.
  - The review's R2 case releases nothing.
  - The #2179 waiting-set benchmark.
- **Stage 4.**
  - Short backfill runs before a waiting 90 GiB head without delaying it.
  - The review's staggered case: the head starts when the ninth job ends.
  - An unknown duration does not backfill.
  - The #787 starvation regression.

## Sources

- The approved [memory admission design](design-memory-admission.md) and its sources (Linux cgroup v2, PSI and `/proc` documentation; Kubernetes node-pressure eviction; systemd-oomd; Verma et al., Borg, EuroSys 2015, section 6.2). Borg's section 5.5 on resource reclamation is cited from memory, not re-read.
- Linux `oom_badness()` in `mm/oom_kill.c`: a process's score is its resident pages, swap and page tables, plus `oom_score_adj` thousandths of RAM and swap. Cited from memory, not re-read.
- Lifka, "The ANL/IBM SP scheduling system", JSSPP 1995 (EASY backfilling). Cited from memory, not re-read.
- Mu'alem and Feitelson, IEEE TPDS 12(6), 2001 (conservative backfilling). Cited from memory, not re-read.
- Issues #625, #806, #1044, #1484, #1500, #1690, #2037, #2086, #2091, #2179, #2183–#2186 and #1493. PRs #2089, #2106, #2114, #2129 and #2180.
