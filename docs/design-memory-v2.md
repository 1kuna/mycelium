# Memory v2: admit on what is free, learn what jobs use

**Draft. Do not merge until the maintainer approves.** If approved, this
replaces the [approved memory admission design](design-memory-admission.md)
and the parked draft [#2114](https://github.com/1kuna/mycelium/pull/2114). It
contains no code. Direction: the maintainer, 2026-11-26, "the redesign should
possibly be a full redesign instead of constant patching" (#2086).

## Summary

1. Today a job's memory is a prediction (the "charge"), and four separate parts of Mycelium each turn that prediction into a different answer.
2. Every time two of them disagreed we added a special case. There are now about 36 of them, in 8,750 lines.
3. v2 makes the live reading of free memory the truth, and learns each job's size from what its earlier runs actually used.
4. A job starts when its expected peak fits in memory nobody else has claimed. A running job claims only what it has proven it uses, plus its expected peak while it is still growing.
5. Memory is counted the same way everywhere. One rule per platform, so the scheduler, the stop logic, the learning and `myce efficiency` agree.
6. A job is stopped only when memory would really run out before Mycelium can react. A job loading inside its own claim, with room for it, is never stopped.
7. Over 14 days of real runs, these sizes would have reserved 1.08–1.19× what repeat jobs used. Today's charges reserve 1.41–2.17×.
8. "Alone" stops being a special state. A job that needs the whole machine just has a size equal to the machine.
9. The decision code shrinks from about 5,000 lines to about 1,900, in five stages. Each stage ships on its own and is testable on a laptop.
10. Biggest risk: on the unified-memory CUDA host, GPU memory isn't fenced by the OS. Section 5 says how v2 protects the host and which test proves it.

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
- **F5.** Mycelium needs one sample period plus the stop grace to free memory. On the unified host the approved values are 1 s + 10 s.

### Six concepts

| Concept | What it is | Rests on |
|---|---|---|
| **Pool** | Host RAM, and each discrete card; a unified host has only RAM. Each pool has one live **free** reading, sampled by the worker: the OS's available memory less keep, or for a card, the driver's free figure. Its **capacity** is the host allowance for RAM, or the card's size. | F1 |
| **Keep** | What the pool leaves free for the host and its owners. Unchanged: the measured idle burst or the owner reserve. | F2 |
| **Footprint** | One number per attempt per pool, from one counting rule per platform (table below). Admission, holds, stops, learning, status and `myce efficiency` all call the same function. | F1 |
| **Expected peak (E)** | The largest clean footprint among the last 20 runs of this work on this platform family. | F3 |
| **Hold** | What a running attempt keeps others out of: `max(E, high-water) − footprint`, where high-water is its own peak so far. On a *contained* pool, once the attempt has stopped growing for one reaction time, its hold drops to `high-water − footprint`. | F3, F4, F5 |
| **Slack (S)** | `free − Σ holds`: memory nobody has claimed. On RAM, S is also capped at `allowance − Σ (footprint + hold)`, so the host allowance still bounds Mycelium's share (MY-45). If every job stays within its E, S doesn't fall. | F1 |

### Counting rule

| Platform | RAM footprint | Card footprint | RAM pool contained? |
|---|---|---|---|
| Linux, CPU work | the attempt cgroup's `anon + shmem + kernel`, excluding page cache | none | yes: the worker slice's `memory.max` |
| Linux, discrete GPU | the same, plus the driver's per-client system memory where that sits outside the cgroup | per-process device memory, summed: NVML, or DRM fdinfo `drm-resident-vram*` | only after probe 0b shows the driver's memory is inside the cgroup |
| Linux, unified CUDA | `anon + shmem + kernel`, plus per-process device memory summed | none: unified | no |
| macOS | `phys_footprint` summed over the attempt's process tree; Metal buffers count one for one (#1044) | none: unified | no |

A run is **clean** only when every counter in its row was read for the whole run. A missing counter is never filled in from a host-wide figure.

### Rules

- **R1 Admit.** A job starts when `E ≤ S` on every pool it needs.
  - The worker computes S from one sample.
  - The queue uses the worker's S, less the E of every assignment that sample doesn't yet list.
  - The worker's launch check runs the same function.
- **R2 Production.** When R1 fails for production, check whether production's own holds plus its E fit the pool's capacity. If they do, release lower-tier attempts in stop order until R1 holds, through the existing release path. Production never waits on lower tiers (MY-56), and production is never overcommitted against other production.
- **R3 Fair waiting.** A pool's **head** is the highest-ranked job that fits the pool's capacity but not S. A job behind the head starts if it fits S and either:
  - its expected finish comes before the head's earliest start, with unknown durations counted as never finishing; or
  - it leaves the head's E free at that time.

  This is EASY backfill, and it replaces the pool accumulation fences (#2186).
- **R4 Stop.** The worker forecasts each pool one reaction time ahead, from the same sample.
  - Each running attempt grows at its own measured rate. While an attempt is within its E, it grows no further than its hold.
  - Anything else that uses memory keeps its measured rate: the fall in free memory that the attempts' own growth doesn't explain.
  - The pool is **short** when that forecast takes free memory below zero.

  This is today's arithmetic (the sustained rate over the sample period plus the stop grace), split per attempt. A job loading inside its own hold, with room for it, is never stopped. On macOS, the OS's warning pressure level also counts as short.
  - Stop one attempt at a time, choosing in this order:
    1. the lowest tier;
    2. any attempt over its E, largest overshoot first;
    3. resumable before non-resumable;
    4. the newest.
  - On a pool the kernel doesn't contain, an attempt over its E while the pool is below its keep is stopped at once, with no checkpoint grace. That is what the kernel itself does at `memory.max`.
- **R5 Learn.**
  - A clean run adds its peak to the window.
  - A run that hit a kill limit, or was stopped while over its E, sets the next E to `min(2 × its measured peak, the pool's capacity)`.
  - A run stopped while within its E changes nothing.
  - A job fails as `memory_capacity`, with the measured figure, only if it hits the limit while its E is already the whole pool.
- **R6 First runs.** With no runs of the exact work, E falls back in order:
  1. this work family's peaks on this platform;
  2. a typed figure or size hint (host plus device on unified memory);
  3. the median first-run peak on this platform family;
  4. the whole pool, which happens once per platform family per installation.

  On a discrete card with no measured device peak, the card's E is the whole card. The reason is physical: a failed allocation can land on a co-tenant. This keeps the 2026-11-23 agent default.

"Alone" is no longer a state. It is just E equal to the whole pool. Whether two jobs that fit one GPU *should* share it is a throughput question, not a memory one. The existing comparison of alone and shared runs answers it (#2037).

### Constants (MY-34)

| Constant | Value | Failure it prevents | Cost | Basis |
|---|---|---|---|---|
| Window | 20 clean runs | Old versions of the work setting today's E | A rare large run is forgotten after 20 runs | The maximum of 20 runs covers the next run 95% of the time (order statistics); 94% measured (approved design §2.3) |
| Overshoot step | × 2 | Repeated near-miss retries (#1484) | Up to 2× hold on one retry | Exponential search |
| Reaction time | sample period + stop grace | Stopping too late | none new | Existing approved values |
| Keep | per host | Starving the host and its owners | Idle memory | MY-45, MY-48 |
| Kernel kill order | lower tiers 1000, production 500, host processes unchanged | The kernel killing a host service, such as the queue, in a host-wide OOM | Production goes before host services | The kernel adds `adj/1000` of RAM to a process's OOM score, so 500 keeps both orderings unless one process holds half the RAM |

Gone: the confidence target, rule Q, ratio rings, transfer from other platforms, and the platform quantile.

## 3. The known cases

| Case | What happened | Under v2 |
|---|---|---|
| **47 GB holder against a 21 GB peak** (#2086) | Charged 46.95 GiB, which is 2.277 × 20.62 from a ratio ring that mixed runs with and without device readings. Room was 43.6 GiB while the host had 88.8 GiB available. Three jobs waited 3–3.5 h. | E is 20.99 GiB, its clean peak, so its hold is about 0.4 GiB. S is about 69.5 GiB: 96 allowance − 21.0 − 5.5. The 24.4 GiB waiter starts at once. The 60 GiB waiter, sized only by its typed figures, then waits for a holder to finish: a real limit of the allowance. |
| **FP8 canary loop** (#2091) | Stopped nine times and readmitted at the same 60 GiB each time. On r6.49 the forecast projected 47.75 GiB of loss from a 4.34 GiB/s load against 33.57 GiB of room. #2129 then made retries exclusive and paused a second stop. | Its footprint went past 60; the device counter alone read 79.6. So the stop counts as an over-E stop, and R5 sets E to the whole pool. The next attempt waits for the pool and holds all of it, and its own load doesn't move S. A same-charge retry can't happen: an over-E stop always raises E, and growth within E is stopped only when the host itself has no room for it. If it outgrows the whole pool, it fails `memory_capacity` with the measured peak. #2091 inferred this may be its true outcome. |
| **In-charge victim stop**, live, 2026-11-26 18:24Z | An 88 GiB-charged model load had used about 63 GiB (device 61.8, host 1.3). The forecast projected 88.7 GiB of loss (8.06 GiB/s × 11 s) against 50.6 GiB of room and stopped it, though the job could grow at most about 25 GiB more inside its own charge. Its retry is now exclusive and waits for the whole pool. | The forecast caps its growth at its 25 GiB hold, which the 50.6 GiB of free memory covers. No stop, and S stays at 8 GiB: the 96 GiB allowance less its 88 GiB. |
| **In-charge victim stop**, the 87 GiB job (#2091) | Each counter alone sat under its charge (69.0 cgroup, 80.0 device). By host-wide subtraction it held 93–97 GiB. Its retry ran exclusive at the same 87. | The one rule adds the cgroup's non-cache figure to the device counter, so the overshoot shows: a sibling attempt of the same workload read 32.9 + 79.6 GiB. It is stopped first as the over-E attempt, no co-tenant goes before it, and its next E is the whole pool. |
| **Alone holder blocking 93 waiting jobs** (#2179) | After one pressure stop, the retry's RAM and card pools went `alone`, so every pooled candidate conflicted with it. Maintenance then visited every waiting job: O(waiting × targets × live). Check-in hold rose from 0.7 s to 2.4 s. | There is no alone state. The retry has a number: unchanged if it was a victim, raised if it went over E. The 93 pieces compare E with S like any job, with no custody conflicts and no release planning between same-tier jobs. If the retry's E is the whole pool, it becomes the head, and R3 lets short pieces finish around it. |
| **About 3.7× over-reservation** | Live at 22:28Z: 261 reserved against 70 measured GiB-hours. That figure covers only the current 8.8 h and 4.6 h windows, and on unified hosts "measured" leaves out device memory. 14-day per-program figures: 2.9× on unified CUDA and 2.1× on the XPU host. The current pool estimate charges about 3× (2.97×) the summed host and device peaks of 126 accepted unified GPU attempts. | Replayed over 14 days of repeat runs, E would reserve: 1.19× on unified CUDA (today's charges: 2.17×), 1.08× on the XPU host (1.46×) and 1.17× on Macs (1.41×). Holds on contained pools also shrink to the proven high-water. `myce efficiency` counts the same footprint as admission. |
| **One GPU could hold two jobs**, unified (#2037) | Once a production model stage's charge was corrected from 102.6 to 24.4 GiB, two ran together, and each took 8–9.5 min against 162 s alone. | Memory says both fit. The throughput comparison (#2037, merged 2026-11-24) makes that work exclusive on the device. Memory and throughput stay separate decisions. |
| **One GPU could hold two jobs**, XPU (#2185) | 973 of 973 XPU attempts recorded no device memory. Every card E was the whole card, so no XPU card was ever shared. | v2 needs a card counter as much as today's code does. The DRM fdinfo parser fix is part of stage 2's counting rule. With it, a card's E is its measured peak, and two jobs share a card when both fit. |
| **Typed 16 GiB holder at a 4.0 GiB peak** (audit) | It held 15.2 GiB as pending growth, which left 1.45 GiB of room for a 9.76 GiB piece. | If probe 0b shows the XPU host's RAM is contained, the hold falls to 3.2 GiB and room rises to about 9.4 GiB. If not, the hold stays at 15.2 GiB until this first run ends, and later runs of the family hold about 3.2 GiB. The 9.76 GiB piece still misses by about 0.3 GiB, because the host has 12.6 GiB free above keep and the holder may return to its 4.0 GiB peak. Smaller jobs fit. The RAM install (#2034) is the real fix. |
| **90 GiB demand fencing 7 small jobs** (#2186) | The accumulation-fence budget was negative, so nothing backfilled beside 40.6 GiB of room. | Under R3, the small jobs whose learned durations end before the head's earliest start begin now, and so does any job that leaves the head's 90 GiB free at that time. The head isn't delayed. |
| **Stale inventory around restarts** (audit cause 2) | 137 jobs and 498–612 job-hours, low confidence. Mostly device inventory and worker contact. | There's no ledger to rebuild. A restarted worker's first sample carries free memory and each attempt's footprint and high-water. The queue keeps only each attempt's E. Device inventory is outside this design, and an unknown reading still admits nothing. |

## 4. What it deletes

The dedicated memory code is 37 files and 8,750 lines today, plus 48 test files with 9,966 lines (Appendix A.1). About 3,800 of those lines sample memory and enforce containment, and they stay. The other 5,000 decide: estimates, ledgers, gates, forecasts, fences and exclusivity. That part drops to about 1,900. Overall that is about 3,350 lines fewer (−38%). An estimated 5,000 test lines go with the mechanisms they test. Of the 36 special cases in Appendix B, 9 rules remain. Appendix B gives each case's fate, and Appendix E gives each stage's tests.

| Stage | Changes | Deletes | Removes |
|---|---|---|---|
| **0 Shadow** | One footprint function per platform. The worker computes holds and S and reports them. Status and `myce efficiency` show v2 numbers beside today's. Fake-clock replays of every case in section 3. No decision changes. | none | (measures the rest) |
| **1 Stop on real shortage** | R4's per-attempt forecast replaces the whole-host forecast. The card guard becomes the same check on card pools. Immediate stops for over-E attempts below keep on uncontained pools. The kernel kill order. | Exclusive retry after a pressure stop, the alone-stop pause, the requeue route scan | in-charge victim stops, the same-charge loop |
| **2 Learn E** | R5, R6 and the counting rule. The XPU card counter (#2185). | Rule Q, ratio rings, confidence feedback, foreign and platform quantiles, evidence repair (after the MY-42 floor check), combined and GPU-host bounds, the bystander test | 2–3× inflation (cause 3), uncounted cards (cause 4) |
| **3 Worker-owned slack** | Holds replace charges. The queue admits on the worker's S. The launch check is the same function. | The two-arm queue ledger, the RAM gate and its fences, launch-capacity waits and startup-loss repeats, retained host charges, sample-identity fences, `fitPoolCharge`, alone custody, the unified CUDA floor | full charge held until the end (cause 1), the launch-refusal loop |
| **4 Fair waiting** | R3 EASY backfill. | Pool backfill fences; the legacy envelope reader, one release later (MY-42) | fence holds (#2186) |

Stopping comes first, so that the tighter packing in later stages can't create false stops. Each stage is one release, runs under `myce dev` with fake GPU inventory and fake memory readings, and keeps scheduling decisions on the injected clock. It merges on quick plus targeted tests and its incident replays (MY-41, the #1972 rollout ruling). The consumer skill and `docs/jobs.md` change in stage 3, when the job's `memory` fields change.

## 5. Risks, and the measured check for each

| Risk | Why it could happen | Check that would show it |
|---|---|---|
| **Host OOM on unified CUDA** | Device allocations escape the cgroup, and a job can outrun one sample. Mitigations: unified holds are the full E for the whole run, R4 forecasts each attempt's growth at its own rate, an over-E attempt below keep is stopped at once, and the kernel kill order puts every Mycelium attempt ahead of host processes. What's left: an overshoot faster than keep ÷ sample period (4 GiB/s at 4 GiB keep and 1 s sampling) can still reach the kernel, which then kills a Mycelium attempt. | **Probe 0b**, on an idle unified host with release authorization: allocate past free memory at 1, 4 and 10 GiB/s. Record which process the kernel kills, the worker's stop latency, and the queue process's RSS and request latency. **Live:** zero kernel OOM kills of non-Mycelium processes per week (kernel log), and over-E stops per week. |
| **E too small** | 15% of repeat runs exceeded the largest of their earlier peaks: median overshoot 2%, p90 24–35%, worst 172× (Appendix A.5). Small overshoots use slack. Large ones cost one stop and a doubled E. | Stage 0 shadow: R4 stops per week split into over-E and victim, against today's `memory_pressure` stops. Attempt-hours burned (audit baseline: 45.4 h a week). |
| **Unified counters overlap** | `anon + shmem + kernel` plus device memory may count pinned or managed pages twice. #2114 found 24 GiB showing as 112 in a five-process case, and a live attempt read 32.9 + 79.6 = 112.5 GiB while the host showed about 95. Because every decision uses the same rule, a constant overlap cancels in holds. E and capacity are absolute, though: overlap inflates E (idle memory) and can push a job to the whole pool early. | Probe 0b: known `cudaMalloc`, `cudaMallocHost`, managed and `malloc` sizes, comparing the cgroup, device and host-drop readings. If pages are counted twice, the rule subtracts the attempt's pinned pages, which the sampler already reads. |
| **Lending on contained pools** | A holder that stopped growing and then grows again forces the newest job to stop. | Shadow: count stops whose victim started after the holder stopped growing. Compare that with the waits recovered (audit: 261–767 job-hours a week behind holder charges). |
| **Queue and worker skew** | The queue admits against a sample taken before its newest assignments. Each unlisted assignment counts at its E, and the launch check is the same function. | Launch refusals with an unchanged S should be zero. #2091 reached 25 for one job. |
| **EASY delays the head** | A duration estimate that is too short lets a backfill run past the head's start. | Fake-clock tests and live comparison of the head's actual start with its earliest start. |
| **Mixed versions** | The adjacent release must interoperate (MY-42). The S report is negotiated as a capability, like `host-ram-gate-v1`. | `scripts/upgrade-compat.py` in both directions at every stage. |
| **XPU or discrete-host RAM not contained** | Driver memory may sit outside the cgroup. Until probe 0b qualifies a platform row, that row's holds stay at the full E. | Probe 0b on the XPU host compares driver system memory with the cgroup's charge. |

## 6. Questions for the maintainer

1. **Approve the replacement and its five stages?** It supersedes the 2026-11-23 design. It also replaces one agent default: "start at nine in ten", meaning the confidence target that sized charges.
2. **Host safety on unified memory.** On a host that can't contain GPU memory, a job that outgrows its expected peak while the host is below its reserve gets stopped at once. That includes production, and the job loses unsaved progress. In addition, every Mycelium job ranks ahead of host processes in the kernel's kill order. Recommended: yes. Without it, the kernel picks the victim, and on the queue host that can be the queue itself (MY-45).

---

## Appendix A. Evidence

### A.1 Code and churn (origin/main `0b07970c9`, 2026-11-26)

- **37 dedicated non-test files, 8,750 lines.** The largest are `memory_pools` (879), `memory_ceiling` (812), `attempt_memory` (743), `host_memory` (622), `memory_envelope` (416), `priority_preemption` (388), `model_memory` (382), `memory_evidence_repair` (331), `gpu_memory` (316), `host_backfill` (304), `memory_estimator` (292), `rss_guard` (285), `memory_evidence` (277), `launch_capacity` (259), `memory_admission` (247), `external_memory` (246) and `memory_pressure` (244).
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
| 7 | `fitPoolCharge` fallback above the allowance | E capped at the pool's capacity |
| 8 | Cold accelerator: drop alone, then use the CPU prior | R6 |
| 9 | Typed limit caps typical and charge | an explicit limit caps E where it contains |
| 10 | Owner resize fixes charge and ceiling | kept |
| 11 | Card alone without device evidence | E is the whole card |
| 12 | Exclusive retry after a pressure stop (#2129) | deleted |
| 13 | Pause on a stop while alone | deleted |
| 14 | Requeue route scan into `memory_capacity` | R5: only a limit hit at the whole pool |
| 15 | Bystander test (stopped with peak ≤ charge) | R5: within E changes nothing |
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
| 26 | `next_sample`, `stop_grace` and `below_reserve` triggers | one R4 forecast per pool, with planned growth capped at holds |
| 27 | Shared-GPU guard (`device_full`, `over_budget`) | R4 on the card pool |
| 28 | Production stops only for production, and only if resumable | kept, except R4's immediate stop (question 2) |
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
  - free, keep, the sum of holds, S, the unexplained decline rate and the sample time;
  - each attempt's key, footprint, high-water, growth rate and E.

  The worker takes the free reading and the attempt footprints in the same pass, so a load burst can't make them disagree. The queue subtracts the E of each assignment the report doesn't list. That is the only fence. An unknown or stale reading admits nothing, as today. The queue's work per check-in is arithmetic over the report, with no storage or bulk reads under `q.mu`.
- **Launch.** The worker runs R1 again with a fresh sample. A refusal carries that sample, so the next offer uses it. A repeat refusal therefore needs a real change on the host.
- **R2 release planning.** Stopping attempt *i* frees its footprint plus its hold. After the release is confirmed, the next report decides. This reuses today's release custody and stop path.
- **R3 earliest start.** The head's earliest start is the time when the pool's capacity, less the holds of running attempts expected to still be running, first fits its E. Durations are the learned durations already used for placement; an unknown duration never ends.
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
6. **Stop decisions on the worker.** That would shorten the reaction time by the check-in delay. Deferred: the queue keeps tiers and release custody.

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
    - the queue's and the worker's S agree for the same sample.
  - A committed, synthetic replay fixture built like Appendix A.5.
  - Probes 0b are separate and need release authorization:
    - unified CUDA counter overlap and OOM victim order;
    - XPU driver memory against the cgroup;
    - the XPU fdinfo format.
- **Stage 1.**
  - Replaying the 2026-11-26 18:24Z case doesn't stop the load.
  - An over-E grower is chosen before a co-tenant within its E.
  - Below keep on an uncontained pool, the over-E attempt is stopped without grace.
  - Production still reclaims.
- **Stage 2.**
  - E equals the window's maximum.
  - The three R5 outcomes.
  - A missing counter makes a run not clean.
  - The XPU fdinfo units parse.
  - The #2086 replay charges 20.99 GiB.
- **Stage 3.**
  - Same-charge launch refusal (#2091) drops from 25 offers to 0 without a change.
  - Holds shrink after growth stops on contained pools only.
  - The audit's 16 GiB holder case.
  - The #2179 waiting-set benchmark.
- **Stage 4.**
  - Short backfill runs before a waiting 90 GiB head without delaying it.
  - An unknown duration does not backfill.
  - The #787 starvation regression.

## Sources

- The approved [memory admission design](design-memory-admission.md) and its sources (Linux cgroup v2, PSI and `/proc` documentation; Kubernetes node-pressure eviction; systemd-oomd; Verma et al., Borg, EuroSys 2015, section 6.2). Borg's section 5.5 on resource reclamation is cited from memory, not re-read.
- Lifka, "The ANL/IBM SP scheduling system", JSSPP 1995 (EASY backfilling). Cited from memory, not re-read.
- Mu'alem and Feitelson, IEEE TPDS 12(6), 2001 (conservative backfilling). Cited from memory, not re-read.
- Issues #625, #806, #1044, #1484, #1500, #1690, #2037, #2086, #2091, #2179, #2183–#2186 and #1493. PRs #2089, #2106, #2114, #2129 and #2180.
