# Design proposal: memory admission from evidence and physical pools

Status: **approved for implementation on 2026-11-23**.
[Section 7](#7-spec-text-this-would-replace) lists the affected parts of
[the original design](design-proposal.md). The evidence below was gathered
against `main` at `b9660ef10` and the live queue on release r6.34.
The [maintainer's rulings](DECISIONS.md) record the rollout and consumer contract.

## Summary

Mycelium treats "how much memory will this job use?" as something it can work
out exactly. It computes one number per job, the *envelope*, and uses it three
ways at once: as the guess, as the amount subtracted from the machine, and as
the kernel's kill limit. Every error in the guess then becomes a failure:

- too low, and the job is killed or refused while the machine has room (#1967);
- too high, and memory sits reserved but unused while other work waits (#787, #1966);
- no history at all, and the number becomes either almost nothing or the whole
  machine, because there is nothing to compute it from (#1967, #1970).

Each fix has added a condition to the computation. The function that sizes one
job (`memoryCandidateOwned`) is now 277 lines with 63 branch points, and its
file has had 101 commits in the 30 days it has existed.

The proposal replaces that with five rules:

1. **Estimate, don't prove.** Each job gets two figures from the cluster's own
   history: what this work typically uses, and a cautious figure it stays under
   nine times in ten. Work never seen before starts from what new work on this
   cluster has needed, which today is about 1 GiB typical and 3 GiB cautious
   for CPU jobs.
2. **An estimate is never a kill limit.** Only physical limits kill: the
   memory a machine can give jobs, a card's memory, or a limit the submitter
   explicitly asked for.
3. **Admit on what is physically free**, less what running jobs are still
   expected to grow into.
4. **When a machine actually runs short, stop the right job.** The
   lowest-priority job that is furthest over its estimate checkpoints and
   requeues. If Mycelium is too slow, the kernel kills in the same order.
5. **One way to wait.** One typed wait reason, one reservation rule for the
   job at the head of a machine's line.

Replayed over the 5,213 clean memory measurements the live queue retains, the
estimator in rule 1 covered 88% of runs at the nine-in-ten target while
charging 2.25 GiB per GiB used, using no tuned constants
([section 5.2](#52-estimator-replay-over-retained-evidence)). It would remove
about 8,500 of the roughly 14,000 lines in the memory area and add about
1,350 ([section 3](#3-what-it-deletes)).

The maintainer's answers and the orchestrator's defaults appear in
[section 6](#6-open-questions).

## 1. Root cause

### 1.1 What was measured

Sources: `git log` on `main`; `myce status --json` read-only from a build of
tag `s086r6.34` at 2026-11-23 22:00Z; the cited issues.

| Measurement | Value |
|---|---|
| Non-test files matching the issue's name pattern | 59 files, 17,057 lines. About 3,800 of those are file-store, storage and installer code that only share a word. Memory scope is about 13,300 lines, plus 6 related files (about 750 lines) the pattern misses: about 14,000 in all. |
| Commits to those files since 2026-10-01 | 390 of the repository's 2,621 non-merge commits; 102 and 109 in the two busiest weeks |
| `memory_envelope.go` | created 2026-10-24; 101 commits since; 21 begin with "fix" |
| Retained memory measurements on the live queue | 5,400 rows in 5,400 distinct evidence keys; 4,975 (92%) hold a single sample |
| Rows unusable for sizing | 1,085 (20%): 944 because a driver or backend fingerprint changed, 141 because a component is absent |
| Run-to-run spread within a key with two or more samples (288 keys) | (max − min) / max: median 6%, 90th percentile 32% |
| Total cgroup peak ÷ resident peak (3,322 rows) | median 1.29, 90th percentile 2.74: file cache dominates some jobs |
| First run of a work family, CPU (2,314 families) | resident peak: median 0.93 GiB, 90th percentile 2.97 GiB, 99th 5.72 GiB |
| First run of a work family, accelerator host RAM | XPU (86 families): median 6.3, 90th 7.0 GiB. CUDA (62): median 5.2, 90th 20.2 GiB. Metal (23): median 0.6, 90th 4.2 GiB |
| Free memory drift between placement and the worker's launch check | 50–100 MB in a laptop walkthrough (#1484); 2 MB was enough to refuse in #1970 |
| Linux memory pressure (PSI `some`) under ordinary load | avg10 1.2–1.5 with 17 GiB available and no swap growth (#1110); it grew in 28% of 200 ms intervals (#1484) |
| Share of admissions sized as exploratory | 25% on r6.13 (#1484) |

The live snapshot also showed the pattern directly. On the first Linux worker
(31 GiB, 24 GiB allowance) one accelerator job held 12.6 GiB of host RAM and
the whole 30 GiB card, labelled "accepted prior invalidated … unknown device
peak; exclusive card". Its own measured peaks were 5.1 GiB resident and 7.7 GiB
total. The host reported 9.8 GiB available and offered 4.2 GiB. On a 24 GB Mac
node, a first-run job held 9.5 GiB as "shared physical headroom; exclusive
device" and left 0.2 GiB of room with 10.3 GiB available.

### 1.2 Seven structural causes

Each cause states what the code does (read from the code), which incidents it
produced (measured), and what is inference.

**C1. One number does three jobs.** The envelope is the prediction, the
admission charge and the kill limit. The worker writes it to the attempt
cgroup's `memory.max` (`memory_ceiling.go`, `create`); the kernel's soft
limit, `memory.high`, is used nowhere. So a prediction error is a kill.
*Incident:* #1967. A 5.75 MiB "accounting floor" was written as `memory.max`;
the kernel journal shows a cgroup OOM at a 5,888 KiB limit killing the launch
helper, which surfaced as `io_error`. The spec's own text calls that figure
"an accounting floor, not a prediction".
*Inference:* any design in which a guess is enforced as a kill limit will keep
producing this class, whatever the guess is.

**C2. With no history there is no prior, so sizing picks an extreme.** A CPU
first run is sized at one kernel page plus a measured shell-startup seed
(`first_run_floor.go`). An accelerator or unified-memory first run is sized at
the host's entire remaining room (`physicalProbeMemoryStatus`,
`unifiedGPUFirstRun`).
*Incidents:* #1967 (near zero), #1970 (whole room, so zero margin), and the
`echo` charged 17.3 GiB in #1484.
*Measured:* the cluster's history already says what a first run needs
(section 1.1). The #1967 job was sized 165 times below the median CPU first
run. The #1970 job was given 16.9 GiB where 90% of XPU first runs peaked under
7.0 GiB.

**C3. Evidence is keyed too finely to accumulate.** A measurement is filed
under 13 fields, including host, card, hardware, driver fingerprint,
environment and an argument hash. 92% of keys hold one sample, and a driver
update discards 17% of rows. The learner is therefore nearly always short of
evidence, and the code compensates with a ladder of fallbacks: sibling device,
work family, cross-machine transfer factors, foreign-OS priors, one-shot
bounded explorations, hints, physical probes. Each has its own margins and
failure handling (`memory_envelope.go`, `learned_memory.go`,
`memory_exploration.go`, `adaptive_memory.go`).
*Inference:* memory need depends on the work and the platform family far more
than on the exact host or driver build. The retained rows cannot test this
directly, because each exact key occurs once; the family-level replay in
section 5.2 is the available check.

**C4. Learning demands certainty from each measurement.** A sample teaches
only if it is "complete": six cgroup counters, pressure under 1%, swap under
1% of a budget, an availability floor, driver client identity, a debugfs
counter that may need a privileged helper, sampler gaps under two periods. An
OOM or a ceiling-bound run "invalidates comparable learned memory points
across machines and families" ([memory mechanisms](reference/memory-mechanisms.md)).
*Measured:* between-run spread (90th percentile 32%) is far larger than the
error those rules remove from one sample. The live XPU job above was running
on an invalidated prior with measured peaks in hand.
*Inference:* precision spent on single samples is wasted while the estimate's
real uncertainty is run-to-run variation.

**C5. Admission is byte-exact and computed twice from different snapshots.**
The queue sizes and admits from its ledger; the worker rechecks against live
free memory with a strict comparison (`worker_attempts.go`, `live_host_growth`).
The queue's view is one check-in old, measured at 1–3 s and up to 40 s (#1484).
`MemAvailable` is itself "an estimate" ([kernel, proc][k-proc]).
*Incidents:* #1970 was refused 16.8755 against 16.8738 GiB. #1962 records three
refusals before a fourth attempt started. When the size is *derived from the
room*, the two sides are equal by construction, so any drift refuses.
Today's fix for #1970 (#1974) adds an idle-burst margin to that same
construction.

**C6. Reservation is the only safety mechanism, so safety costs capacity.**
Because the sum of envelopes within the allowance is what protects the host,
each job must reserve near its worst case. That strands memory (#787: 86.4 GiB
reserved on a host using 6.1 GiB). Layers were then added to win it back:
charging by use under five preconditions, cold-probe lending, typed charges
below limits, resident credits, a two-arm room formula, and a yield path for
when lending goes wrong (`memory_room.go`, `memory_admission.go`). Host
backfill fences then protect large waiting jobs from the small ones those
layers admit, and the fences produced #1717, #1813, #1824 and #1966 in turn.
*Inference:* the fence incidents are downstream of over-reservation. With
charges near real use, host RAM binds less often and one reservation rule is
enough.

**C7. The same physical event has many classifications.** "Not enough memory
at launch" surfaced as a requeued `prelaunch_refused` (attempt 1 of #1970), a
terminal `resource_limit` (attempt 2), an `io_error` (#1967), an OOM retry
with a floor, a yield, and an RSS-guard stop. There are several sizing paths
times several launch paths (attached, detached custodian, sidecar group,
deadline-gated helper), and each pair needs its own classification (#1962).
*Unknown:* why attempt 2 of #1970 was terminal was not traced here; the worker
fixing #1970 owns that.

**In one sentence:** the code treats a statistical estimate as an exact
account. It tries to prove a bound for each job and then enforces the proof,
so each error is a dead job or idle memory, and each patch adds an exception
to the proof instead of tolerance for error.

### 1.3 Incident map

| Issue | What happened | Cause | Under this design |
|---|---|---|---|
| #1967 | 5.75 MiB ceiling killed a CPU first run | C1, C2 | No per-job kill limit from an estimate; first run charged the cluster's prior |
| #1970 | First-run XPU sized to the whole room, refused by 2 MB, then terminal | C2, C5, C7 | Size never derives from room; launch refuses only below the typical figure; one requeueing wait |
| #1966 | Surplus RAM withheld beside a GPU chain | C6 | One backfill budget leaves small jobs everything the head can never use |
| #1962 | Three launch refusals before a start; typed detail lost | C5, C7 | Same as #1970; one wait reason with three numbers |
| #1824 | Final stages starve behind the same project's new GPU work | C6, plus an ordering choice | Smaller charges ease it; the ordering choice is [question 6](#6-open-questions) |
| #1813 | Start budget expired behind a fence | C6, C7 | Every memory wait is one state, and that state pauses relative start budgets |
| #1717 | CPU work starved accelerator-only work on a scarce host | C6 | The head job's reservation covers every lower-ranked job on that host |
| #787 | 52 GiB job starved; 86.4 GiB reserved on a host using 6.1 GiB | C6 | Charges follow evidence; the head job holds a reservation |
| #1484, #1110 | First runs lost to drift; pressure noise stopped jobs | C2, C5 | As #1970; pressure noise is not a stop signal |
| #1958, #1955 | Checkpoint expiry and input-claim holdings | not memory | Out of scope: file custody |

The backlog audit on #1972 labelled 71 open issues `memory-redesign` in five
groups. They map onto the causes as follows. The grouping is the audit's;
each issue keeps its own acceptance, and this design does not claim to close
any of them by itself.

| Audit group | Issues | Causes | Where this design answers |
|---|---|---|---|
| Cold starts, sizing and probe budgets | 11 | C1, C2, C5 | 2.3, 2.4 |
| Learned peaks, accounting and telemetry | 15 | C3, C4 | 2.2, 2.3 |
| Backfill, contention and reservations | 20 | C6 | 2.4, 2.11; #1824 is question 6 |
| Shared device memory and host pressure | 16 | C1, C6 | 2.5, 2.6, 2.9; depends on #1762 and #1771 |
| Placement, launch and maintenance boundaries | 9 | C5, C7 | 2.4 for launch refusals; qualification and maintenance start probes are outside this design |

## 2. The proposed model

### 2.1 Vocabulary

- **Pool.** A physical body of memory with a capacity and a live reading. Host
  RAM is one pool. Each discrete accelerator card's memory is one pool. A
  unified-memory host has only the RAM pool, because its device allocations
  come out of host RAM.
- **Footprint.** One measured number per pool for a running attempt: the
  memory it holds that cannot be given back without swapping.
- **Estimate.** Two figures per job and pool. `typical` is the median of
  comparable runs. `charge` is the figure the job is expected to stay under at
  the confidence target.
- **Held.** `max(charge, peak footprint so far)` for a running attempt.
- **Keep.** What a host must leave free for itself and its other owners. This
  is the existing per-host figure: the measured idle burst or the explicit
  reserve (MY-45, MY-48). This design does not change how it is derived. The
  RAM pool's capacity is physical RAM less the host's measured idle footprint,
  so `capacity − keep` is today's host allowance.

### 2.2 Measuring a run

One footprint per pool, sampled at the existing cadence; the run's evidence is
its peak.

| Platform | RAM-pool footprint | Device-pool footprint |
|---|---|---|
| Linux, cgroup v2 | `anon + shmem + kernel` from the attempt cgroup's `memory.stat`. File cache is left out because the kernel's availability estimate counts it as reclaimable ([proc][k-proc]); Kubernetes excludes inactive file cache from its working set for the same reason ([eviction][k8s]). | per-process device memory from the driver (NVML; [DRM fdinfo][k-drm] `drm-resident-<region>`) |
| Linux, unified CUDA | the above plus per-process device memory. Device allocations escape the cgroup but appear per process and in host availability (measured on a GB10, #1044). | none: it is the RAM pool |
| macOS | summed `phys_footprint` of the attempt's process tree. Metal buffers raise it one for one (measured on M4, #1044). | none: unified |

For accelerator work on a discrete-card host, the driver also pins host memory
outside the cgroup. Where the driver reports a per-client system-memory
figure, it is added. For a job that ran alone on the host, the pool's own drop
in availability is used when it is larger. That one rule replaces the
outside-root residual accounting.

A run is **clean** unless its memory was held back, in which case it is
**limited** and its peak is only a lower bound. Limited means: stopped or
killed for memory, an explicit limit was hit (`memory.events` `max` or `oom`,
[cgroup v2][k-cg]), or its cgroup swapped out. Nothing else disqualifies a
sample. PSI totals and `memory.peak` are recorded beside the peak for
diagnosis and for the shadow comparison in section 4; they do not gate
learning.

### 2.3 Estimating

One confidence target `c` (proposed 0.9) and one counting rule:

> **Rule Q.** A `c`-quantile is read from `k` samples only when
> `k ≥ c / (1 − c)` (9 samples at 0.9). It is the `⌈(k+1)·c⌉`-th smallest.

Rule Q is the standard distribution-free bound from order statistics: the next
draw exceeds the largest of `n` earlier comparable draws with probability
`1/(n+1)`, whatever the distribution. The cluster's own history matches it:

| Earlier runs in the family (`n`) | 1 | 2 | 3–4 | 5–9 | 10–19 | 20 |
|---|---|---|---|---|---|---|
| Next run exceeded their maximum (measured) | 57% | 33% | 33% | 19% | 11% | 6% |
| `1/(n+1)` | 50% | 33% | 25–20% | 17–10% | 9–5% | 5% |
| 90th percentile of next ÷ maximum (measured) | 1.66 | 1.14 | 1.35 | 1.03 | 1.00 | 0.98 |
| 99th percentile of next ÷ maximum (measured) | 13.4 | 27.6 | 34.8 | 1.35 | 1.39 | 1.75 |

The last row is why no reservation scheme can be the safety mechanism: with
thin evidence, one run in a hundred is more than ten times anything seen
before. Containment has to handle it (section 2.5).

**Evidence levels.** Memory evidence is keyed by the work and the platform
family (OS, accelerator backend, unified or discrete), not by host, card,
driver build, environment hash or allowance:

1. *exact*: today's comparable-work identity (recipe or program, arguments,
   configuration) on this platform family;
2. *family*: today's memory family (project and name, or extension and work
   type) on this platform family;
3. *platform*: every work family's first run on this platform family.

Evidence for the same work on a different platform family is one more level
between 2 and 3, calibrated the same way and skipped when it cannot be.

**The estimate.**

1. Take the most specific level with at least one clean run. Let `m` be the
   maximum of its last 20 clean peaks and `n` their count.
2. Scale it by how wrong that kind of evidence has been here:
   `charge = f × m`, where `f` is the `c`-quantile, by rule Q, of the store's
   own history of *next peak ÷ maximum of the previous `n`* at that level,
   bucketed by `n` as in the table above. `typical` is the median of those
   peaks.
3. If rule Q cannot be met for `f`, move up one level. Moving up never
   forgets: the more specific level's own maximum and median stay as floors.
4. At the platform level, `charge` is the `c`-quantile of first-run peaks and
   `typical` their median.
5. If rule Q cannot be met there either (a new installation, or a new platform
   family), the estimate is **unknown** and the job runs alone on that pool.
   A new installation therefore runs its first nine distinct jobs per platform
   family one at a time per host, and shares from then on.

**Floors and hints.**

- A *limited* run sets a floor for that job's next attempt and its exact work:
  `charge ≥ 2 ×` the limited peak and `typical ≥` that peak. Doubling is
  exponential search: it reaches any need in at most `log₂(pool ÷ first peak)`
  attempts. Today's rule relearns "a floor only slightly higher, and loses an
  attempt each time" (#1484). A limited run invalidates nothing else.
- When the floor exceeds every eligible pool, the job fails with a typed
  capacity reason that names the measured figure.
- A figure the submitter typed, or a size hint, is the estimate while the work
  has no clean exact or family evidence. Once evidence exists, the evidence is
  used. Whether a typed figure is also a kill limit is
  [question 1](#6-open-questions).

The estimator is a pure function of a small store: at most 20 peaks per
identity, and a ring of 200 ratios per level and bucket. It needs no bulk
reads under the queue lock.

### 2.4 Admission

For each pool `p` a job needs on a host:

```
room(p) = available(p) − keep(p) − Σ over running attempts of max(0, held − footprint)
```

`available` is the worker's live reading. An unknown reading admits nothing on
that pool, as today. For a discrete card, `available` is capacity less the sum
of `held`, lowered to the device's own free reading.

- **R1, every tier.** Admit when `charge ≤ room(p)` on every pool the job needs.
- **R2, production only (MY-56).** If R1 fails but
  `charge + Σ held by production ≤ capacity(p) − keep(p)`, plan releases of
  lower-tier work on that pool until R1 holds, through the existing release
  custody. Production is never overcommitted against other production, and
  never waits on lower tiers.
- **Launch check on the worker.** The worker recomputes `room` from its own
  reading and refuses only when `typical > room`. The gap between `charge` and
  `typical` is the job's own measured uncertainty, so a launch is never
  refused over drift smaller than the uncertainty that sized it.
- **One wait.** A refusal returns the job to the queue with reason
  `memory_wait` and the numbers behind it (need, available, keep, held by
  others). It costs no attempt, is never terminal, and pauses relative start
  budgets (#1813).
- **Unknown estimate.** The job is admitted when no other attempt holds that
  pool, and nothing else is admitted there while it runs. "Alone" is a state,
  not a byte figure, so it cannot miss by 2 MB.

Sizes never derive from room. That removes the construction behind #1970.

### 2.5 Containment: what is hard and what is soft

**A prediction is never a kill limit.** What can kill is physical, or
explicitly asked for.

| | Linux, cgroup v2 | macOS | Discrete card |
|---|---|---|---|
| Hard limit | The worker slice's `memory.max`, set to the host's allowance. This exists today as the joint ceiling. It is the one kernel kill limit, and it protects the host and its other owners. | None exists in the OS for this use. | The card's capacity. An allocation beyond it fails inside the process that asks. |
| Per attempt | A cgroup for accounting; `memory.oom.group = 1`; `oom_score_adj` by tier (both exist today). No `memory.max`, except an explicit submitter limit or an authored member cap. | Footprint sampling by the existing guard. | Alone on the card until its device peak is measured. |
| If Mycelium reacts too slowly | The kernel kills inside the slice, highest OOM score first ([proc][k-proc]), so lower tiers go before production and the whole attempt goes as a unit. | The OS's own pressure handling. | The asking process gets the error. |

This is the division of labour the kernel documentation recommends for cgroup
v2. Its usage guidelines call overcommitting soft limits "a viable strategy",
with "a management agent" that can "monitor and take appropriate actions such
as granting more memory or terminating the workload", and they define
`memory.max` as the hard limit at which "the OOM killer is invoked in the
cgroup" ([cgroup v2][k-cg]). Here the worker is that agent, the estimates are
the soft limits, and the slice ceiling is the hard one. Kubernetes and Borg
run the same way. The kubelet schedules on requests and, under node pressure,
evicts by whether usage exceeds the request, then priority, then the size of
the overage ([eviction][k8s]). Borg's node agent "assigns memory to containers
based on predicted future usage" and, when a machine runs out, "terminates
tasks, from lowest to highest priority" ([Verma et al., section 6.2][borg]).

**Why not a per-job `memory.high`.** The kernel's soft limit throttles a
cgroup that passes it and "should be used in scenarios where an external
process monitors the limited cgroup" ([cgroup v2][k-cg]). A job that outgrew a
wrong estimate would stall until the worker raised the limit. Mycelium
replaces its worker binary under running jobs (MY-55), so a missed raise would
freeze work that had memory to run. This design sets no per-job
`memory.high`. A slice-level `memory.high` just under the slice's `memory.max`
is a possible later refinement and is not part of this proposal.

**Hosts where the slice ceiling is unavailable** (older service units outside
the slice; `myce dev` foreground workers) use the sampled path, as macOS does.

**Two open issues are prerequisites on Linux.** Workers installed by earlier
releases sit outside the slice and need a supported unit migration (#1771).
An unprivileged worker can raise `oom_score_adj` but not lower it (measured in
#1762; the kernel documents the same rule, [proc][k-proc]), so tier scores
must be relative to the inherited value: the highest tier keeps it and lower
tiers are raised. The kill order the slice produces also still needs the VM
evidence #1500 asks for.

### 2.6 When a pool runs short

Each worker checks its own pools at its sampling cadence.

**Trigger.** A pool is short when
`available − keep < growth × (sample period + stop grace)`. `growth` is the
pool's measured recent rate of loss, and stop grace is the approved value.
macOS also treats the OS's own warning pressure level as short. This is the
arithmetic today's yield path uses, applied to the pool and not to one
lender.

Linux PSI is recorded but is not a trigger in the first version. The measured
`some` signal rises under ordinary load with 17 GiB free (#1110, #1484). A
`full`-stall trigger, as [systemd-oomd][oomd] uses at a default of 60%, is
evaluated in the shadow phase and added only if it would have fired before a
kernel kill.

**Who stops.** One attempt at a time, re-evaluated when its release is
confirmed:

1. the lowest tier on the pool;
2. within the tier, an attempt over its `charge` before one within it, largest
   overage first;
3. then resumable before non-resumable, then the most recently started.

Kubernetes ranks usage-over-request first and priority second ([eviction][k8s]).
Tier comes first here because MY-56 says production does not wait on
co-tenants. Production stops only as it does today: to protect other running
production work, and only if resumable.

**What stopping means.** The existing planned-stop path: checkpoint if
resumable, requeue, no retry or loss charge, one cause (`memory_pressure`). An
attempt that was over its charge is a limited run and gets the doubled floor.
An attempt stopped within its charge was an innocent victim and keeps its
estimate. A kernel kill inside the slice is classified the same way.

### 2.7 Growth

A running attempt that passes its charge is not stopped for that. Its `held`
follows its peak, so room for new admissions shrinks. If the pool has room,
nothing else happens and the store learns a larger peak. If the pool is
short, section 2.6 applies. There is no lending, no growth forecast per
attempt and no grant to renew.

### 2.8 Shared hosts and roles

Roles (MY-45) change `keep` and nothing else. A `node` keeps its measured idle
burst or explicit reserve. The `services` host keeps its measured persistent
demand. The `interactive` host keeps its owner reserve and still declines bulk
classes. Owner-activity and polite-host signals are outside this design and
unchanged. Because `available` is a live physical reading, another owner's
memory use lowers room without any accounting on Mycelium's side.

### 2.9 Unified and dedicated memory, multiple cards

- **Unified** (Apple silicon, unified CUDA): one pool and one scalar footprint
  per attempt. There is no split to guess, so the exclusive first run, the
  one-page device marker and the unified free-RAM floor all go. The floor's
  purpose is served by `keep`. A first run is charged the platform prior like
  any other job. On unified CUDA the slice ceiling does not contain device
  allocations (#1044), so the sampled trigger is the protection there, as on
  macOS.
- **Dedicated cards:** a RAM pool plus one pool per card. Device pools are
  never overcommitted, and work with no device evidence at the exact or
  family level runs alone on its card for one run. The reason is physical: no
  mechanism contains device memory, and a failed allocation can land on a
  co-tenant.
- **Multiple cards (MY-30):** the card count is part of the exact identity. A
  job on `k` cards holds a device charge on each and one RAM charge on the
  host. Work spreads across cards only after evidence shows one card is too
  small, as today.

### 2.10 Production priority

- No machine is reserved (MY-50). Production holds only its own `charge`.
- Lower tiers use everything else until production needs it, then R2 and the
  stop order move them (MY-56).
- Production's guarantee is R2's sum, checked against the pool's capacity and
  not against live room. It holds even if every lower-tier job misbehaves.
- The unanswered half of MY-56 (stop non-resumable lower-tier work, or let it
  finish) is unchanged by this design and is [question 4](#6-open-questions).

### 2.11 Waiting fairly: one reservation rule

Per host, the highest-ranked queued job that fits the host's capacity but not
its current room is the host's **head**. Ranking is the existing order. While
a host has a head, jobs ranked behind it share one **backfill budget**:

```
budget = capacity − keep − (held by attempts that never finish) − head's charge
```

A job behind the head is admitted when it fits `room` now and the attempts
admitted behind the head, including it, together hold no more than the
budget. Attempts that never finish are resident engines and pins.

The attempts that were running before the head arrived will finish, and
everything admitted since fits inside the budget, so the head starts as soon
as they do: a stream of small jobs cannot starve it (#787, #1717). Small jobs
still use the memory the head can never use, including the surplus beside a
GPU chain (#1966). The rule needs no duration estimates. It is conservative
backfilling without the time dimension (Mu'alem and Feitelson, see Sources).
It replaces the device-pending, release-room and probe variants of the host
backfill fence. Maintenance-drain and fleet-start fences concern host
maintenance and are not touched.

### 2.12 Every constant and its basis (MY-34)

| Constant | Value | Failure it prevents | Cost | Basis |
|---|---|---|---|---|
| Confidence target `c` | 0.9 | Estimates exceeded often enough to cause stops | Idle memory: replay charged 2.25× use at 0.9 and 2.91× at 0.95 | A policy choice with a measured tradeoff: [question 2](#6-open-questions) |
| Rule Q sample count | `c/(1−c)` | Reading a quantile the data cannot support | Early jobs use a broader level or run alone | Order statistics; distribution-free; confirmed in section 2.3 |
| Evidence window | 20 clean peaks | Old versions of the work setting today's size; unbounded storage | A rare large run is forgotten after 20 runs | The smallest window whose maximum is by itself a 95% bound |
| Calibration ring | 200 ratios | Unbounded storage | None measurable | Enough to read a 99% quantile by rule Q |
| Floor after a limited run | × 2 | Repeated near-miss retries (#1484) | At most 2× over-hold on that job's next run | Exponential search; independent of any deployment |
| Reaction time | sample period + stop grace | Stopping too late | None new | Existing approved values |
| `keep` | per host | Starving the host or its owners | Idle memory | Measured per host, or the maintainer's figure (MY-45, MY-48) |

Priors, scale factors, growth rates and room are all measured on the running
system. No value is tuned to one deployment.

### 2.13 Status and the consumer contract

- Job GET gains `memory`: per pool, `typical_gb`, `charge_gb`,
  `evidence {level, runs}`, `held_gb`, `peak_gb`, and `limit_gb` only when
  explicit. `memory_envelope` stays readable for one release (MY-42).
- One waiting reason, `memory_wait`, with need, available, keep and held.
- `needs.memory: auto`, typed figures and size hints are all accepted and all
  mean "a starting estimate".
- The consumer skill and `docs/jobs.md` change in the same change (AGENTS.md).

## 3. What it deletes

An estimate from file-level classification, not a compiled diff. Expect ±20%.

| Goes entirely | Lines |
|---|---|
| Sizing: `memory_envelope.go`, `learned_memory.go`, `memory_exploration.go`, `adaptive_memory.go`, `memory_recovery.go`, `extension_memory_seed.go`, `typed_memory_charge.go`, `first_run_floor.go` | 2,966 |
| Charging and room: `memory_room.go`, `memory_admission.go`, `launch_capacity.go`, `device_resident_credit.go`, `helper_memory_admission.go` | 1,347 |
| Evidence plumbing: `memory_planning.go`, `memory_sizing_facts.go`, `retained_memory.go`, `memory_observations.go`, `memory_observation_identity.go`, `memory_learning.go`, `checkin_memory_identity.go` | 1,171 |
| Completeness accounting: `gpu_host_memory.go`, `cuda_host_memory.go`, `unified_cuda_admission.go`, `unified_attempt_memory.go`, `unified_cuda_calibration.go`, `device_memory_sampling.go` | 1,144 |
| About half of `learned_memory_guard.go` (the adaptive ceiling; the footprint guard stays); `discrete_fanout_sharing.go` | about 200 |
| **Subtotal** | **about 6,830** |

| Shrinks | From → to |
|---|---|
| `attempt_memory.go` (one footprint per pool) | 1,140 → about 400 |
| `memory_ceiling.go` (cgroup, OOM score, slice ceiling, cleanup stay) | 849 → about 500 |
| `host_memory.go` (release planning stays) | 601 → about 250 |
| `host_backfill.go` (one reservation rule) | 388 → about 150 |
| **Subtotal removed** | **about 1,680** |

New code: estimator about 350, evidence store and projection 250, pool room
and admission 200, shortage rule 250, sampling glue 150, status 150. **About
1,350.**

**Net:** about 8,500 lines removed and 1,350 added. The memory area goes from
about 14,000 to about 6,900 lines (−51%), and its sizing, charging and
learning core from about 8,500 to about 1,350 (−84%). About 11,000 test lines
are named for deleted files; rule tests and incident replays replace them at
an estimated 1,500–2,500 lines. Tests of behaviour consumers rely on stay or
are rewritten as replays (MY-53). `reference/memory-mechanisms.md` goes from
469 lines to about 80, and `testdata/envelope-calibration.json` (205 KB) goes.

Concepts that stop existing: the envelope as a ceiling; exploratory probes and
whole-room offers; the first-run floor and startup seed; bounded-exploration
claims; transfer factors and foreign priors; OOM floors and censored-growth
recovery; lending by use, by probe and by typed charge; resident and device
credits; the two-arm room; launch-gap and drift-margin learning; evidence
completeness and its pressure thresholds; the unified exclusive first run and
its marker; invalidation of other evidence by one bad run; the `learned_memory`
policy switch.

Stays as it is: host readings and idle history (MY-45), the measured
allowance, cgroup creation and cleanup, OOM scoring, the slice ceiling, the
footprint and RSS samplers, preemption placement and exact release custody,
production backoff, model-engine measurement, device telemetry sources,
admission decision records, component reservations, and everything about
files and storage. The memory use of the privileged GPU telemetry helper
goes; whether the helper itself can go depends on its other readers and is
checked at implementation.

## 4. Migration

Queue state and learned evidence persist in SQLite, production runs twice a
week, and only the adjacent release interoperates (MY-42). Preserve exact
attempt custody and existing observations during rollout. The approved rollout
uses quick, targeted and replay evidence in place of the original extended
shadow window described below.

The implementation projects legacy peaks into a separate `memory_evidence`
table. It retains the `myce-normalized-state-v3` marker and legacy observation
rows. During an adjacent-release rollback, keep pooled workers in place while
their running attempts settle. The old queue reads the persisted envelope and
custody, and new pooled offers require the new queue. A subsequent upgrade
recovers the separately stored pooled evidence. Linux workers without verified
slice containment continue using the legacy assignment contract.

The original staging plan proposed a `memory_model` field in the durable queue policy (revision CAS, like
`learned_memory` today) takes `legacy`, `shadow` or `pooled`, with a per-tier
override. Workers advertise a `pooled_memory_v1` capability. A worker without
it keeps legacy envelopes, so a mixed fleet is safe.

1. **Project the evidence. No behaviour change.** A re-runnable projection
   reads the existing observation rows and writes the new store (identity
   levels, platform family, peak, clean or limited). Old rows are not
   rewritten. The 944 rows discarded for a driver fingerprint change become
   usable again.
2. **Shadow. No behaviour change.** With `shadow`, the queue computes the new
   estimate beside every legacy envelope and records both on the job. At
   completion it records the peak. Status reports, per platform family: the
   share of runs within the new `charge`, new and legacy charged GiB per GiB
   used, and how often each launch check would have refused. Exit when the
   report covers every platform family in use and two production windows, and
   coverage meets `c` within sampling error.
3. **Pooled for non-production tiers, host by host.** A host qualifies when
   its worker advertises the capability and, on Linux, its slice ceiling is
   confirmed and tier scoring works unprivileged (#1771, #1762). A Linux host
   that has not reached that state uses the sampled path or stays on legacy.
   Attempts already running keep the containment they started with;
   in the room formula a legacy attempt's `held` is its envelope. Production
   stays on legacy.
4. **Pooled for production**, after a production window in which shadow shows
   no production job outside its charge-plus-room and no memory wait caused
   by a lower tier.
5. **Delete** the legacy code, wire fields and old rows once every
   installation and the production database are past the adjacent-release
   floor (MY-42).

Rollback through step 4 is setting the policy back. No data conversion is
involved, because legacy evidence is untouched until step 5. Worker upgrades
use live takeover as now (MY-55). Fixes to legacy paths that are still live,
such as #1969 and #1974, keep landing until step 5.

## 5. Validation

### 5.1 Incident replays

The simulator was removed on 2026-11-12 (MY-49), so replays are fake-clock Go
tests driven through the real queue and worker code with fake memory readings
and the existing fake cgroup filesystem. All are laptop-testable. Each row is
one table case.

| Replay | Setup from the incident | Must hold |
|---|---|---|
| #1967 | Never-seen production CPU job with a start deadline on a cgroup worker; helper peaks at 5.9 MiB, payload at 31.8 MiB | Starts on attempt 1; no kill limit below the slice; the peak is learned |
| #1967, new install | The same job with an empty store | Runs alone and starts |
| #1970 | First-run XPU job; allowance 24 GiB, room 16.9 GiB; available drifts down 2 MB before launch | Charged the XPU prior, not the room; launches |
| #1970, real shortage | The same, with available falling 12 GiB before launch | `memory_wait`; requeued; no attempt consumed; never terminal; start budget paused |
| #1962 | Detached launch gate with three near-miss launches | No refusal while `typical` fits; the wait's numbers survive the gate |
| #1966 | One card; a GPU holder with host RAM `R`; a queued GPU successor; CPU jobs | CPU work up to the surplus is admitted; a larger one waits; the successor starts on release |
| #787 | A 52 GiB job on a 96 GiB host under a stream of small jobs measured near 1 GiB | Small jobs are charged near their use; the head starts when its blockers finish; nothing admitted delays it |
| #1717 | Accelerator-only work queued before CPU work on a scarce host | The head reservation holds; portable CPU work goes elsewhere |
| #1813 | A production job waiting behind the head past its start window | Stays queued with the budget paused; dispatches with a fresh budget |
| Shortage | Two experiment jobs and one production job; one experiment passes its charge; available falls | That experiment stops with `memory_pressure`, no retry charge, and its floor doubles; production and the innocent job are untouched |
| Kernel backstop | A slice OOM event from the fake filesystem | The lowest tier is killed as a unit and requeued through the same path |
| Runaway | A job that doubles until no pool fits | Fails with the typed capacity reason after at most `log₂` attempts |
| Mixed fleet | One legacy and one pooled worker | Legacy keeps envelopes; neither over-admits |

### 5.2 Estimator replay over retained evidence

Done once by hand for this proposal, and to be kept as a test over a sanitized
fixture (hashed identities and numbers only). The 5,213 clean retained rows
were replayed in recorded order through section 2.3, each row estimated only
from rows before it.

| Target | Runs estimated | Ran alone | Within `charge` | Charged ÷ used | Misses: median, 90th percentile |
|---|---|---|---|---|---|
| 0.90 | 5,119 | 94 | 87.8% (family level 89.7%, platform level 85.6%) | 2.25 | 0.38 GiB, 2.2 GiB |
| 0.95 | 5,030 | 183 | 94.1% (family 96.4%, platform 91.6%) | 2.91 | 0.48 GiB, 3.0 GiB |

Limits of this replay:

- The retained peaks were recorded under today's hard ceilings, so runs that
  needed more were killed and are missing. The true tail is heavier than
  shown.
- Each retained row is its own exact key, so the exact level could not be
  exercised. Charged ÷ used at the family level includes spread between
  differently sized items; exact evidence should be tighter.
- Row order is by recorded time and is approximate.
- There is no legacy figure on the same rows. Step 2 of the migration
  measures legacy and new on the same jobs.

### 5.3 Live acceptance

Native acceptance is separately authorized, as the approved design requires.

- **Shadow report** (step 2): coverage meets `c`; new charged ÷ used is below
  legacy on the same jobs.
- **Linux cgroup host:** a non-production test job that outgrows its estimate
  is not killed while the pool has room. Under induced shortage the stop order
  holds. With the worker paused, the kernel kills the lowest tier first and
  the host stays healthy.
- **Unified CUDA host:** device allocations appear in the footprint, and the
  sampled trigger fires before host exhaustion.
- **macOS node:** footprint sampling and the pressure trigger behave the same.
- **Discrete card:** new work runs alone once, then shares.
- **Production windows:** zero launch refusals from drift; zero production
  waits on memory held by lower tiers; no production job stopped for memory.
- **Arrival numbers (MY-52):** idle hours while runnable work waited fall on
  the hosts that showed stranded reservations.

### 5.4 Not verified in this pass

- Kernel behaviour is cited from documentation, not exercised on the fleet.
  This pass did not touch the fleet.
- Which live Linux workers already have the slice ceiling in force was not
  checked.
- Device allocations escaping the cgroup on unified CUDA, and Metal buffers in
  `phys_footprint`, rest on the repository's earlier measurements (#1044).
- macOS has no kernel containment. A process that allocates faster than the
  sampler can exhaust memory between samples, as it can today.
- The estimate that host RAM binds less often once charges follow use (C6) is
  inference. The shadow report tests it.

## 6. Open questions

The answers were recorded on 2026-11-23. Typed memory values seed the estimate
and an explicit opt-in sets a limit. Production can stop and requeue lower-tier
work that cannot checkpoint. The consumer skill explains the restart risk.
Trim tests for removed mechanisms and run quick, targeted tests and incident
replays before merging. Rollout follows verification without an extended
shadow phase. Same-project, same-tier precedence for downstream work lands
in a separate change for #1824. The orchestrator chose the initial 90% target
with outcome-driven adjustment and first-run sharing on unified memory.
[Decision record](https://github.com/1kuna/mycelium/issues/1972#issuecomment-7115860549)

The original questions and recommendations follow.

1. **Is a typed memory figure an estimate or a kill limit?** Today
   `memory_gb: 4` on a CPU job is enforced: at 4 GiB the job is killed.
   - (a) *Estimate.* It seeds the charge until Mycelium has measured the work.
     A submitter who wants a cap sets an explicit limit in the advanced
     section.
   - (b) *Limit*, as today.
   - **Recommend (a).** MY-31 says agents state needs and Mycelium learns
     memory. A typed guess that kills is the same failure as #1967 with a
     human's number.
2. **How tightly should machines be packed?**
   - (a) *Nine in ten:* jobs stay within their charge 90% of the time. Replay:
     2.25 GiB charged per GiB used.
   - (b) *Nineteen in twenty:* 2.91 GiB charged per GiB used.
   - **Recommend (a).** A job over its charge costs nothing unless the machine
     is also full, and then it costs one low-priority job a pause.
3. **Should new accelerator work on a unified-memory machine share from its
   first run?** Today it gets the whole machine for that run.
   - (a) *Share*, charged what new work of that kind has needed here.
   - (b) *Keep the machine to itself* for one run.
   - **Recommend (a).** On machines with separate card memory, new work still
     gets the card to itself for one run, because nothing can contain card
     memory.
4. **Production needs memory held by lower-priority work that cannot
   checkpoint.** Unanswered since 2026-11-16 (MY-56).
   - (a) *Stop it and restart it later*, as today.
   - (b) *Let it finish while production waits.*
   - **Recommend (a).** MY-56 says production does not wait. The design works
     either way.
5. **Rollout order.**
   - (a) Shadow first, then non-production tiers host by host, then production
     after a clean window.
   - (b) Everything at once after shadow.
   - **Recommend (a).** Each step is one policy value and reverts the same way.
6. **Within one project and tier, should a job whose earlier stages are done
   go ahead of that project's new work?** (#1824, already waiting on the
   maintainer.)
   - **Recommend yes.** Finished pipelines deliver sooner and other projects
     are unaffected. It is independent of this design and can be approved
     separately.

## 7. Spec text this would replace

On approval, these parts of the approved design change:

- [Memory](design-proposal.md#memory): "Per-attempt RAM ceilings" (declared
  RAM enforced as a ceiling), "Unified CUDA free-RAM floor", "Charging running
  attempts by use", "Job memory envelopes" and "Learned memory by default and
  bounded first runs", with all of [memory mechanisms](reference/memory-mechanisms.md).
  "Host roles and measured capacity" stays.
- [How the queue decides](design-proposal.md#how-the-queue-decides): the
  host-RAM sentences of "Making room" (signed reservation debt, backfill that
  cannot deepen a deficit) become R2 and section 2.11. The aggregate allowance
  and cgroup enforcement stay as the slice ceiling.
- [Least valuable machine that fits](design-proposal.md#least-valuable-machine-that-fits):
  the capacity-accumulation paragraph becomes section 2.11.
- "OOM is a normal failure" stays true for an explicit limit and for a job
  that outgrows every eligible pool. A stop for pool shortage is an
  interruption, not a failure.

Nothing here adds a peer network, a lease, a second queue or a restored v1
mechanism. It runs in one binary against the one SQLite queue.

## Sources

- [Linux kernel, cgroup v2 memory controller][k-cg]: `memory.high`,
  `memory.max`, `memory.events`, `memory.oom.group`, usage guidelines. Fetched
  2026-11-23.
- [Linux kernel, PSI][k-psi]: `some`, `full`, triggers. Fetched 2026-11-23.
- [Linux kernel, /proc][k-proc]: `MemAvailable`, `oom_score_adj`. Fetched
  2026-11-23.
- [Linux kernel, DRM client usage stats][k-drm]. Fetched 2026-11-23.
- [Kubernetes, node-pressure eviction][k8s]: working set, eviction order,
  `oom_score_adj` by class. Fetched 2026-11-23.
- [systemd-oomd][oomd]. Fetched 2026-11-23.
- [Verma et al., "Large-scale cluster management at Google with Borg",
  EuroSys 2015][borg], section 6.2. Read 2026-11-23.
- Mu'alem and Feitelson, "Utilization, predictability, workloads, and user
  runtime estimates in scheduling the IBM SP2 with backfilling", IEEE TPDS
  12(6), 2001. Cited from memory, not re-read.
- The order-statistics bound in rule Q is elementary; conformal prediction
  generalises it (Vovk, Gammerman and Shafer, *Algorithmic Learning in a
  Random World*, 2005). Cited from memory, not re-read.

[k-cg]: https://docs.kernel.org/admin-guide/cgroup-v2.html
[k-psi]: https://docs.kernel.org/accounting/psi.html
[k-proc]: https://docs.kernel.org/filesystems/proc.html
[k-drm]: https://docs.kernel.org/gpu/drm-usage-stats.html
[k8s]: https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/
[oomd]: https://www.freedesktop.org/software/systemd/man/latest/systemd-oomd.service.html
[borg]: https://research.google.com/pubs/archive/43438.pdf
