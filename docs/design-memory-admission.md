# Memory admission from evidence and physical pools

Approved for implementation on 2026-11-23. See the [maintainer rulings](DECISIONS.md) and the [original proposal and evidence](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/design-memory-admission.md).

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
   For unified CUDA RAM with one exact or family observation, bound the
   ratio prediction by the larger of the calibrated first-run platform
   charge and the largest clean peak from the work family. Apply the bound
   when the platform charge meets rule Q and provides headroom above the
   observed peak. Any measured shortage floor still applies.
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
  [question 1](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/design-memory-admission.md#6-open-questions).

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

Linux workers without a confirmed slice ceiling refuse fresh assignments.
Darwin and the fake development runner have admission paths for their local
memory observations.

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
  finish) is unchanged by this design and is [question 4](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/design-memory-admission.md#6-open-questions).

### 2.11 Waiting fairly: one reservation rule

Per host, the highest-ranked queued job that fits the host's capacity but not
its current room is the host's **head**. Ranking is the existing order. While
a host has a head, jobs ranked behind it share one **backfill budget**:

```
budget = capacity − keep − (held by attempts that never finish) − head's charge
```

A job behind the head is admitted when it fits `room` now and the attempts
admitted behind the head, including it, together hold no more than the
budget. Loaded catalog engines count against the budget until they unload.

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
| Confidence target `c` | 0.9 | Estimates exceeded often enough to cause stops | Idle memory: replay charged 2.25× use at 0.9 and 2.91× at 0.95 | A policy choice with a measured tradeoff: [question 2](https://github.com/1kuna/mycelium/blob/fd74bf1b5e0c74feaabef92541a4f80e2164496d/docs/design-memory-admission.md#6-open-questions) |
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
