# Measured throughput, batch placement, and future workflows

Research recommendation, 2026-10-22. Code inspected at `7163a22` on
`integ/108`; sources checked on this date. This is input to a future amendment,
not a change to the [approved design](../design-proposal.md). Today’s execution
contracts remain in force. Scope follows [#107](https://github.com/1kuna/mycelium/issues/107),
including maintainer’s [concurrency requirement](https://github.com/1kuna/mycelium/issues/107#issuecomment-6345956097),
[workflow idea](https://github.com/1kuna/mycelium/issues/107#issuecomment-6346064875),
and [general-workflow clarification](https://github.com/1kuna/mycelium/issues/107#issuecomment-6346082344).

## Recommendation and worked examples

Keep the existing planner. Give it a small table of measured **aggregate
throughput versus concurrency or batch size**, keyed by work type, exact
recipe/input shape, and physical component. Seed it with the extension’s own
short, correctness-checked reference workload; refine it from accepted work.
Choose both where to run and how much to admit there. Start with size buckets,
simple averages, and deterministic scheduling; no learned predictor or bandit
is needed initially.

Applications declare pieces and qualified per-platform recipes. Mycelium never
ports code or invents a safe split. Priority, fairness, memory admission,
preemption, and owner activity remain hard constraints. Later, use generic
stages, edges, fan-out/fan-in, batch policies, and runtime hints to accumulate
compatible downstream work in the same SQLite queue.

**All fleet rates, times, and allocations below are illustrative, unverified
hardware assumptions, not measurements or qualification claims.** Each requires
the same correctness contract and a working recipe on the selected platform.

### Forty naming requests: count aggregate work, not one request’s speed

Assume 400 generated tokens per request, identical prompt/context bounds, one
warm engine per component, and no prefill, transfer, or setup cost for this
arithmetic. The entries are **tokens/s per active request**, not aggregate rates:

| Component | 1 request | 2 concurrent | 4 concurrent | 8 concurrent |
| --- | ---: | ---: | ---: | ---: |
| Mac mini, Metal | 25 | 22 | 18 | 12 |
| Spark, CUDA | 50 | 40 | 30 | 20 |
| Each B70 card, XPU (two cards) | 35 | 28 | 18 | 10 |

On the Mac, serial execution takes `40 × 400 / 25 = 640 s`. Eight concurrent
requests deliver `8 × 12 = 96 tokens/s`: five waves take `5 × 400 / 12 = 166.7 s`,
or 3.84 times the throughput. Lower speed per request can be much better for
finishing the batch.

A feasible fleet plan is Mac: 8 requests at concurrency 8 (33.3 s); Spark: 16
at concurrency 8 (two 20 s waves); each B70: 8 at concurrency 8 (40 s).
That finishes all 40 in **40 s** under these assumptions. Saturated aggregate
throughput is `96 + 160 + 80 + 80 = 416 tokens/s`; the fluid lower bound is
`16,000 / 416 = 38.5 s`. Integer waves explain the gap. This is a feasible
example, not a claim of global optimality or real fleet performance.

For one request the Mac takes 16 s at concurrency 1; sharing with seven others
raises its service time to 33.3 s. Small tails, latency-sensitive work, cold
models, and long prompts can therefore choose a different count or machine.
Use one batching-capable engine, not eight copies of the model. The current
one-independent-batch-attempt-per-GPU rule still applies; concurrency here means
model calls within an engine or items within one explicitly batch-capable command.

### CPU robotics sweep: more slots can make throughput worse

Assume 48 independent fixed-step simulations, each needing one core and 1 GiB,
with CPU recipes on all three hosts and enough capacity after owner reserves.
These are **seconds per simulation at the stated concurrency**:

| Host CPU | 1 | 4 | 8 | Example assignment | Finish |
| --- | ---: | ---: | ---: | --- | ---: |
| B70 host | 10 | 11 | 16 | 24 simulations, concurrency 8 | 48 s |
| Spark | 12 | 14 | 24 | 16 simulations, concurrency 8 | 48 s |
| One mini | 15 | 18 | 40 | 8 simulations, concurrency 4 | 36 s |

This finishes in 48 s versus 216 s for the mini alone at concurrency 4.
On that mini, concurrency 8 yields only `8/40 = 0.20 simulations/s`, below
`4/18 = 0.22`; filling every possible slot is the wrong objective. An actively
used MacBook contributes zero admitted capacity. The B70 host’s CPU/RAM budget
is counted once across both cards; isolated CPU/GPU rates are not assumed
additive when they compete for memory bandwidth.

## Prior art: what to reuse

Paper results describe their evaluated systems, not expected gains on this fleet.
The last column is a recommendation for Mycelium.

### Heterogeneous scheduling and learning

| Primary source | What it does | Copy / avoid |
| --- | --- | --- |
| [Gavel, OSDI ’20](https://www.usenix.org/conference/osdi20/presentation/narayanan-deepak) | Uses job/hardware throughput and an effective-throughput abstraction to adapt scheduling policies to heterogeneous accelerators. | Copy work-specific measurements; avoid a cluster-scale optimization and round scheduler. |
| [Sia, SOSP ’23 paper/artifact](https://github.com/siasosp23/artifacts/blob/aaf7a0de7b6cb5eab06ea020467ede5d2fd18e70/sia-sosp23.pdf) | Models goodput across GPU type, count, and batch configurations; seeds profiles and refines them online, with optimized allocations. | Copy sparse seeds plus observations; avoid its training-specific goodput model and integer optimizer. |
| [Pollux, OSDI ’21](https://www.usenix.org/conference/osdi21/presentation/qiao) | Jointly tunes allocation and training batch size; goodput includes statistical efficiency. | Copy the distinction between speed and useful progress. Never change a training minibatch’s semantics without the recipe’s permission. |
| [Themis, NSDI ’20](https://www.usenix.org/conference/nsdi20/presentation/mahajan), [Gandiva, OSDI ’18](https://www.usenix.org/conference/osdi18/presentation/xiao), [Tiresias, NSDI ’19](https://www.usenix.org/conference/nsdi19/presentation/gu) | Respectively address finish-time fairness; introspection/time-sharing of repetitive training; and scheduling with little job-size knowledge. | Retain Mycelium’s owner tiers and fair share. Observe repetitive work, but do not import auctions, transparent migration, or a replacement priority policy. |
| [HTCondor ClassAds/matchmaking paper](https://research.cs.wisc.edu/htcondor/doc/hpdc98.pdf) | Matches job and machine attributes through requirements and preferences. | Separate eligibility from ranking; static resource labels alone cannot predict throughput. |
| [Slurm heterogeneous jobs](https://slurm.schedmd.com/heterogeneous_jobs.html) | Co-schedules components with different resource requests as one heterogeneous job. | Useful for future gang execution [#69](https://github.com/1kuna/mycelium/issues/69), not necessary for independent pieces. |
| [Nomad scheduling](https://developer.hashicorp.com/nomad/docs/concepts/scheduling/how-scheduling-works), [Kubernetes scheduling framework](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/) | Feasibility then ranking; Kubernetes also separates reservation and binding phases. | Copy filter/score/commit boundaries. Bin packing is not a throughput estimate; importing either control plane violates the deployment goal. |
| [StarPU performance-model source](https://github.com/starpu-runtime/starpu/blob/db86d8fe0d9e0c95f2126fadf07e658e96104b3d/include/starpu_perfmodel.h) | General task-runtime cost models include history per architecture and regression, with sample counts and variance. | Copy observable, work-specific history. Try simple size regression only if bucketed estimates demonstrably fail. |
| [UCB bandits, Auer et al.](https://homes.di.unimi.it/~cesabian/Pubblicazioni/ml-02.pdf) | Balances exploration and exploitation under a stochastic reward model. | Eventually useful for choosing safe probes. Start with bounded idle probes; changing contention and owner activity do not satisfy a simple stationary reward assumption. |

### Ray, batching, and serving

Ray supports general Python computation; **it is not AI-only**. Mycelium’s useful
distinction is plain-command execution, prepared platform recipes, polite home
machines, and one binary/queue. [Ray’s resource model](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html)
is a relevant baseline rather than a reason to build an AI-specific scheduler.

| Primary source | What it does | Copy / avoid |
| --- | --- | --- |
| [Ray resources](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html), [scheduling](https://docs.ray.io/en/latest/ray-core/scheduling/), [autoscaler](https://docs.ray.io/en/latest/cluster/vms/user-guides/configuring-autoscaling.html) | Logical CPU/GPU/memory/custom resources express heterogeneous admission; locality also matters. Autoscaling follows requested logical resources, not observed device utilization. | Keep recipe eligibility and locality explicit. Resource quantities are not physical isolation or learned work-type speeds; autoscaling is irrelevant to a fixed home fleet. |
| [Ray ActorPool](https://docs.ray.io/en/latest/ray-core/actors/actor-utils.html), [Serve batching](https://docs.ray.io/en/latest/serve/advanced-guides/dyn-req-batch.html) | ActorPool dispatches across a fixed actor pool; Serve batches requests with maximum size, wait timeout, and concurrent-batch limits. | Copy reusable workers and size/time bounds. Pools and batching alone do not select heterogeneous machines or concurrency by measured curves. |
| [vLLM/PagedAttention paper](https://arxiv.org/abs/2309.06180), [benchmark CLI](https://docs.vllm.ai/en/latest/benchmarking/cli/) | Efficient KV-memory management supports serving batches; load tests sweep concurrency/request rate and report throughput and latency. | Measure aggregate output, latency, prompt/output lengths, and KV memory together. Do not equate token throughput with end-to-end completion time. |
| [SGLang paper](https://papers.nips.cc/paper_files/paper/2024/hash/724be4472168f31ba1c9ac630f15dec8-Abstract-Conference.html), [model gateway](https://docs.sglang.io/docs/advanced_features/sgl_model_gateway) | RadixAttention reuses prefix state; gateway policies include cache-aware and load-aware routing. | Account for warm state and cache mode; let the engine implement continuous batching. Cache locality must not override owner policy. |
| [vLLM production stack](https://github.com/vllm-project/production-stack) | Kubernetes-native serving, routing, and metrics around vLLM engines. Its README marks prefix-aware routing as work in progress. | Reuse engine metrics where available; avoid another cluster control plane or assuming unfinished routing features exist. |
| [llama.cpp server](https://github.com/ggml-org/llama.cpp/blob/19ab5a5e65736a35437e6b1c0ccd6562fed01b1b/tools/server/README.md), [slot/batch implementation notes](https://github.com/ggml-org/llama.cpp/blob/19ab5a5e65736a35437e6b1c0ccd6562fed01b1b/tools/server/README-dev.md) | `--parallel` controls request slots; compatible slots share continuously formed batches. | Benchmark the exact slot/context settings; a slot is not a separate model copy. |
| [MLX LM server source](https://github.com/ml-explore/mlx-lm/blob/0d4d3d04f8ab7ce4a51e7209e8615755a48ba94d/mlx_lm/server.py) | Uses a batch generator with prompt/decode concurrency; batch eligibility excludes some request options and image handling is separate. | Qualify the exact version/configuration. Do not assume every MLX request can batch. |
| [exo source](https://github.com/exo-explore/exo) | Splits models across devices/machines using topology-aware execution and discovery. | Keep distributed-model research in #69; do not import its peer network or confuse it with batching independent jobs. |

### Workflows and windows

| Primary source | What it does | Copy / avoid |
| --- | --- | --- |
| [Ray DAG](https://docs.ray.io/en/latest/ray-core/ray-dag.html), [Workflows removal notice in source](https://github.com/ray-project/ray/blob/master/python/ray/workflow/__init__.py) | DAG nodes bind dependencies and runtime inputs. Experimental Workflows was deprecated in 2.44; 2.47 was its last release. | Copy explicit dependencies and reusable workers. DAG execution is not durable workflow recovery; do not depend on the removed library. |
| [Temporal workflows](https://docs.temporal.io/workflows) | Durable executions use recorded event histories and deterministic replay, including timers and signals. | Copy persisted transitions and timers; avoid a separate service, replay language, and SDK requirement. |
| [Prefect tasks](https://docs.prefect.io/v3/concepts/tasks) | Python task/flow calls, futures, mapping, tracked states, and retries allow dynamic computation. | Copy runtime information and observable dependencies; Mycelium should not execute arbitrary orchestration code. |
| [Dagster dynamic graphs](https://dagster.io/docs/guides/build/ops/dynamic-graphs) | Keyed dynamic outputs support `map` fan-out and `collect` fan-in. | Copy stable item keys and explicit collection completion. A complete collection differs from a timed partial batch. |
| [Argo DAG](https://argo-workflows.readthedocs.io/en/latest/walk-through/dag/) | Declares task dependencies, parallel branches, and failure behavior on Kubernetes. | Copy named edges and failure propagation; avoid a controller/container platform. |
| [Flink windows/triggers](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/) | Groups keyed streams by windows and fires through count/time/custom triggers. A count trigger replaces the default time trigger rather than automatically combining both. | Copy keyed accumulation and an explicit size **or** time rule. Avoid watermarks/event-time machinery until required. |
| [Kafka consumer configuration](https://kafka.apache.org/43/configuration/consumer-configs/) | `fetch.min.bytes` and `fetch.max.wait.ms` trade fetch latency for transport batching; `max.poll.records` limits records returned to a consumer. | Copy bounded accumulation intuition. Fetch/poll boundaries do not define an application’s semantic batch or its completion. |
| [Triton dynamic batcher](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/batcher.html) | For stateless models, combines compatible requests up to model maximum batch size; `max_queue_delay_microseconds` bounds intentional accumulation. Preferred sizes are optional and usually unnecessary. | Copy maximum size plus maximum wait, flushing partial batches. Keep this generic and distinct from backend token-level continuous batching. |

## Measurement contract

Represent a curve as sparse measured points, not a machine score:

| Part | Proposed contents |
| --- | --- |
| Identity | Existing work type; recipe/variant digest; correctness-equivalent workload/model digest; input-size/shape bucket; physical host and component ID; environment/backend fingerprint. |
| Configuration | Concurrent independent items `c`, vector batch size `b`, threads per process, cache mode, owner power/resource allowance. Initially sweep one axis with the others fixed. |
| Observation | Aggregate completed units/wall-second `R`, service latency summary, setup/load/transfer separately, peak host/device memory, sample count, timestamp, provenance, uncertainty/stale reason. |

Concurrency, vector batching, and threads are different controls. A command may
process eight inputs in one vector batch; an engine may interleave eight
requests; eight CPU processes may compete for cores. Measure the actual recipe’s
mode. Units might be simulations/s, input-audio-seconds/s, records/s, or tokens/s.
Never compare unlike units or unrelated reference workloads. LLM buckets include
model/quantization, prompt and output bounds, and context/cache settings; generic
shape keys carry these details without adding LLM fields to the planner.

**Declared by the extension/job:** existing work type and exact platform recipes;
size units/estimates and bounds; independent items or an explicit splitter and
merge command; compatible-input key; supported concurrency/batch values and
minimum useful batch; CPU/RAM/device floors and extra-slot memory bounds;
correctness reference; optional latency budget. Splittability defaults to false,
concurrency/batch to one. Generic batch commands receive a keyed input manifest
and return a keyed output manifest. The application owns semantic splitting and
merging; Mycelium creates and retries the declared pieces.

**Learned by Mycelium:** speed at each supported point, cold preparation and
transfer costs, observed memory envelope, variance, and prediction error. Begin
with `new = 0.8 × old + 0.2 × sample` for each matching bucket/point; this weight
is a proposed tunable. At stable occupancy, sample **aggregate accepted completed
units divided by the observation window**, never add overlapping request rates
or multiply the isolated rate by concurrency. Tag mixed-workload/unknown-occupancy
samples separately; initially use only clean samples to refine curves. Retain
partial progress for operations, but exclude failed, preempted, stale, duplicate,
or correctness-rejected attempts from successful speed samples.

Use declared floors plus a conservative measured high-water envelope: approximately
`base + c × extra_slot`, or measured `memory(c,b)` when nonlinear. Never lower
existing memory floors because an EWMA fell. An OOM makes a configuration unsafe,
not merely slow. Unified memory is charged once; two cards share one host ledger.
Unknown memory cannot authorize another slot.

## Placement rule

1. Run existing eligibility, qualification, owner availability, priority/fairness,
   project-cap, checkpoint, production-hold, and memory checks first. Keep the
   existing useful-victim/cleanup planner; speed does not justify same-tier
   preemption. Stopping allocations remain charged until release is confirmed.
2. For one piece or unequal pieces, rank feasible configurations by **expected
   completion**: admissible wait + transfer + cold setup + size-matched service.
   Include the slowdown imposed on already running work when adding a slot.
   Deterministic greedy earliest-finish placement is sufficient initially.
   If estimates are absent/incomparable, retain existing fallback and say why.
3. For an explicit homogeneous batch, use **discrete water-filling over measured
   points**. Test a completion horizon `H`. For component `m` and count `c`, let
   readiness be `a_m`, piece size `u`, and full-wave service time
   `S_m(c) = u × c / R_m(c)`. Conservative full-wave capacity is
   `K_m(H,c) = c × floor(max(0,H-a_m) / S_m(c))`.
   Enumerate the few measured counts (including zero), choose feasible component
   combinations per physical host under shared cores/RAM, and find the smallest
   horizon whose total capacity covers `N`. Assign integer waves, then evaluate
   smaller tails with measured smaller counts (or serial fallback). For true
   vector batches, use measured batch service time and allowed batch sizes.
4. Recompute on admission, completion, owner return, loss, or updated hints. Keep
   unstarted pieces in the shared queue; forecasts do not create machine-owned
   queues, reservations, or leases. Dispatch only what the current planner can
   admit. Do not wait indefinitely for a busy fast machine on a weak estimate.

For the 40-request example, a 40 s horizon fits the stated allocation. The fluid
`N × rate/sum(rates)` split is intuition, not a finite-batch ETA. Non-monotone
curves, indivisible work, transfer contention, and uncertain arrival times make
this a heuristic forecast rather than an optimality guarantee. Do not extrapolate
past measured safe points or assume independently measured host components can
all run at full rate together. Use conservative contention assumptions until
mixed-load observations exist. Long latency-sensitive calls may cap concurrency
below the throughput peak; strict owner policy always wins.

## What the dynamic benchmark runs

Reuse opt-in [onboarding benchmarking](../guide-benchmark-v3.md) and the existing
qualified-reference checks. Its current CPU hash, memory copy, cached disk read,
and tiny accelerator references are useful diagnostics, not a robotics, ASR, or
real naming-model performance curve. [#51](https://github.com/1kuna/mycelium/issues/51)
shipped that scalar baseline; [#33](https://github.com/1kuna/mycelium/issues/33)
and #107 cover the richer model. [#16](https://github.com/1kuna/mycelium/issues/16)
does not authorize runtime porting or silent GPU-to-CPU fallback.

For each opted-in work type, run **its own representative synthetic/public
reference inputs**, after its exact variant qualifies. Separate preparation,
warmup, and measured service. Try safe counts `1,2,4,8` up to recipe, memory,
and owner limits; a vector-batch recipe sweeps batch size instead. Record output
correctness, aggregate throughput, latency, and peak memory at every point.
Stop adding points when throughput flattens, latency breaches the declared
budget, signals change, or memory approaches its ceiling.

A proposed initial budget is roughly 8–10 measured seconds per point plus
bounded warmup, **at most 60 seconds per work-type sweep**. Slow references may
yield fewer points and an explicit partial result. Preserve the current nine-minute
onboarding ceiling, including waiting; downloading/building remains separate.
After a new extension qualifies, schedule its sweep only with standing benchmark
opt-in or an explicit request. Do not benchmark on every ordinary join.

Run only when the physical host is idle and existing fresh owner/power/heat
signals permit it, never alongside production, recovered work, or a busy daily
machine. Reuse the current five-idle-minute/external-power laptop policy and
cancel on owner return or unknown/unsafe signals. Serialize sweeps across host
components through existing admission/cleanup custody, not independent card
benchmarks that overload shared RAM. Native driver/runtime acceptance is a
separate authorized task; none was attempted for this research.

Invalidate on hardware, driver, OS/backend, recipe, reference/model, thread/cache,
or resource-allowance changes. Retain old evidence for explanation but mark it
incomparable. Age and sustained prediction error mark a curve stale and request
an idle refresh; a proposed 30-day review age is a policy default to evaluate,
not a physical expiry law. No automatic load-generating refresh on a daily host.
Expose unknown/stale/sample counts and prediction versus actual time. Real
accepted work can refresh matching points; bounded idle probes discover missing
points. Add regression or exploration algorithms only after observed error or
coverage problems justify them.

## Fit with the current implementation

| Existing seam | Incremental change proposed |
| --- | --- |
| [benchmark.go](../../internal/myce/benchmark.go), [capability.go](../../internal/myce/capability.go) | Extend scalar `Throughput`/`BenchmarkResult` with component/configuration/shape/provenance and sparse curve points; preserve old profiles as unknown richer measurements. |
| [extensions.go](../../internal/myce/extensions.go), [extension_composition.go](../../internal/myce/extension_composition.go) | Reuse `workType()` and exact variants. Important gap: `ExtensionVariant` has no benchmark member, while validation forbids a legacy top-level benchmark with variants. Add a per-variant reference/measurement contract before promising variant curves. |
| [capability_placement.go](../../internal/myce/capability_placement.go), [machine_placement.go](../../internal/myce/machine_placement.go) | Keep conservative learned `ProgramNeeds` floors distinct from speed learning; key curves to exact target components and retain the shared physical-host ledger. |
| [fairness.go](../../internal/myce/fairness.go), [sync.go](../../internal/myce/sync.go), [production_hold.go](../../internal/myce/production_hold.go) | Feed measured completion estimates into existing candidate/ranking paths, including `benchmarkBetter`/`capabilityPlacementBetter`, preferred placement, and production target choice. Preserve one planner and consistent status reasons. |
| [models.go](../../internal/myce/models.go), [placement_plan.go](../../internal/myce/placement_plan.go) | Model-call routing currently ranks ready/loading/cold and load cost. Add curve-aware ranking/admission here too; retain `roomPlan`/`callSlotPlan` and useful-victim costs. Configured `max_in_flight` is a hard bound (zero currently means unlimited), not a learned curve. |
| [worker_benchmark.go](../../internal/myce/worker_benchmark.go), [extension_local.go](../../internal/myce/extension_local.go) | Reuse queue-owned benchmark lifecycle, fingerprinting, heartbeats, and cleanup; do not create another coordinator. |

Retain one independent batch attempt per GPU. A higher concurrent GPU-process
limit would require a separate approved design change; batching inside one
attempt and existing model-engine calls do not require that change. All time-based
queue/scheduling decisions use the injected `Clock`. Snapshot for measurements
or artifact verification outside `q.mu`, then recheck attempt/session fences
before the small SQLite commit. No network or bulk I/O under that lock.

## Future: general workflows with bounded downstream batching

The declaration describes data dependencies and permitted execution choices, not
a machine timetable. Up-front information supports placement forecasts and
preparation; accepted outputs determine actual readiness. Runtime hints revise
estimates and enumerate work without granting new resource permissions.

### One generic declaration

Proposed vocabulary, **not supported current YAML**:

```yaml
workflow:
  version: 1
  stages:
    first:
      work_type: transform-v1
      recipe: transform@1
      fan_out: {manifest: items}       # app supplies stable keyed items
      batch: {max_items: 1, max_wait: 0s}
    second:
      work_type: combine-v1
      recipe: combine@1
      batch: {max_items: 8, max_wait: 5s}
  edges:
    - from: first.results
      to: second.inputs
      fan_in: {by: group_key, release: each}
  hints:
    allowed: [size, expected_items, input_closed, affinity, latency, route]
    max_pending_items: 256
    max_iterations: 10
```

Each stage references the same generic work/recipe contract above. Each input
envelope has run, stage, iteration, group/item keys, named artifact references,
and a compatibility/shape key. Fan-out reads an app-produced manifest; it never
splits arbitrary bytes. Edges carry named outputs to named input ports. Fan-in
supports **each** ready item, **join** of required ports for a key, and **all**
members of a closed group. Closure means no more members will be enumerated,
not that existing members have succeeded: all-member release also waits for every
enumerated member to succeed or reach its declared terminal policy. These are
generic dependency rules. `release: all`
passes a complete manifest as one semantic input; a batch timer cannot turn a
partial reduction or training dataset into a complete one.

For compatible ready inputs, dispatch at the chosen target count **up to**
`max_items`, or when the oldest ready input reaches `max_wait`, or when the input
group closes. Curves may choose a smaller target than the maximum; never wait
for eight if four finishes sooner under the declared latency budget. A zero
wait dispatches immediately. Preserve the oldest-ready timestamp across restart
and retries. Maximum wait bounds deliberate accumulation, not scheduling delay
behind higher tiers or an unavailable recipe. Report those waits separately.
Batch only within the same permitted run/group scope, project/tier, recipe,
compatibility key, and memory envelope; never blend unrelated policy classes.

Runtime updates are small, idempotent, versioned hints: revised sizes/counts,
group closure, locality/latency preferences, and selection among declared edges.
Hints cannot change a running attempt, invent a recipe, weaken memory/policy,
or assert successful outputs. Fan-out is bounded by pending-item limits, with
backpressure rather than eagerly materializing an unlimited queue.

For loops, declared stage templates may have a feedback edge: an accepted result
and route hint instantiate the next **iteration key**, capped by the declared
limit or an explicit stop. The materialized dependencies remain acyclic across
iterations. Generic conditional routes, fan-out, joins, and feedback cover changing
flows; application-specific decision code stays in ordinary stage commands.
This is not an arbitrary workflow-language interpreter.

### End-to-end examples using that same contract

| Flow | Declaration and runtime behavior | Same-schema check |
| --- | --- | --- |
| **Verso recordings** | Fan out 10 recording IDs. ASR → diarization → speaker ID → labeling, with artifact edges and per-ID joins wherever a stage needs multiple outputs. If diarization needs only source audio, declare parallel source-audio branches instead: ASR on B70 GPU0, diarization on GPU1, then join their outputs before speaker ID. Labeling declares `max_items: 8, max_wait: 5s`. Suppose eight compatible speaker-ID/transcript results are ready by t=20 s: materialize one eight-item labeling job for Spark if its curve wins. Two more arrive at t=23 s; an input-closed update flushes them then, or without closure the oldest-item timer flushes at t=28 s. Correlate all ten outputs by original ID and fan in a final manifest. Placements and times are hypothetical. | Stage names and artifact contents belong only to the app. The planner sees work types, each/join/all edges, keys, batch bounds, and hints. No transcript or speaker fields. |
| **Robotics sim → train → eval loop** | A parameter manifest fans out 48 keyed simulations, placed as in the CPU example. `release: all` gathers their accepted outputs for one training input after the simulation group closes; it cannot launch training on a timer with missing data. Training emits a checkpoint. Evaluation fans out keyed test scenarios, optionally `max_items: 4, max_wait: 2s`, then gathers metrics. An ordinary decision stage chooses a declared feedback edge with new parameters for iteration 2, or stops at the configured bound. Training minibatch size remains recipe semantics. | Same manifests, all/each edges, batch policy, and versioned route/count/closure hints. Checkpoints and parameter choices are artifacts. No robotics or training scheduler branch. |
| **CPU data processing: map → reduce** | A manifest fans out 100 partitions. Map runs on available CPU cores; a batch-capable map recipe can use `max_items: 4, max_wait: 1s`. Its keyed outputs fan in by reduction key, with `release: all` after the group closes. Large reductions use another declared fan-out/partial-reduce stage followed by final fan-in; Mycelium does not invent an associative merge. | Identical stage/edge schema; record shape is an opaque work bucket. Full reduction and timed execution batching stay distinct. No data-processing special case. |
| **Local agents: retrieve → batch → tools** | Retrieval fans out by request ID. A compatible generation stage uses `max_items: 8, max_wait: 200ms`; accepted outputs enumerate tool invocations through a manifest. Tools fan out and results join by request ID; a declared feedback edge permits another bounded pass. Side-effecting tools supply their own idempotency keys. | The generation and tool recipes are ordinary work types. Dynamic fan-out, keyed joins, feedback, and batching use the same fields; no agent runtime in Mycelium. |

Every example fits the same declaration; only stage names, recipes, artifacts,
group keys, and policy values change. None needs a planner branch for its domain.
The Verso parallel variant is legal only if its actual dependency contract allows
it; a strict ASR-dependent diarization stage must wait. Across all examples,
forecast arrivals can inform warmup and target batch size but cannot reserve a
machine or unlock a downstream stage before accepted inputs exist.

### One queue, durable state transitions

Persist run/stage/item dependencies, closed-group state, oldest-ready time, and
batch membership alongside normal jobs in the existing SQLite database. An
atomic transition claims ready input IDs and creates an ordinary queued batch
job. Freeze membership for its retries; only the accepted attempt’s keyed output
manifest unlocks downstream work. Use existing NAS-relative or verified
file-store handoff, not peer transfer. Upstream terminal failure/cancellation
follows a declared fail-dependent or skip-item policy; incomplete joins remain
visible, never silently successful.

Start with whole-group retry on failure and require idempotent stage commands;
per-item retry within a partially successful batch needs an explicit output
contract later. Attempt fences prevent duplicate downstream consumption, not
exactly-once external effects. Persisted model workflows need an ordinary durable
job wrapper for a batched model run: current in-flight model calls are not
replayed after queue restart. Backend continuous batching remains inside the
engine; do not introduce two independent batching delays without sharing the
stage’s latency budget.

Expose one run’s progress, blocked dependencies, timer/batch state, and final
outputs through existing status surfaces. Process readiness and timers through
the queue’s normal maintenance/dispatch path and injected `Clock`; there is no
new service, workflow engine, peer network, lease, or replicated registry.

## Incremental build plan

Each slice needs an approved amendment before product implementation. These are
independently useful increments; workflow support comes last.

| Slice | Deliverable | Acceptance on a GPU-free laptop |
| --- | --- | --- |
| **1. Comparable observations** | Per-variant reference contract; work/shape/component provenance and concurrency-1 timing from accepted attempts; prediction-versus-actual status. No placement change. | Synthetic CPU recipe yields a correctly keyed observation; stale/failed/duplicate attempts cannot update it. Old profiles still load as unknown. Variant references preserve qualification and fingerprint invalidation. This establishes whether meaningful measurements exist. |
| **2. Safe sparse curves** | Opt-in idle sweeps, aggregate throughput/latency and incremental memory at supported counts; EWMA refinement and stale reasons. | Injected measurements reproduce rising and falling curves; fake-clock owner return cancels a sweep, busy hosts never start one, and changed fingerprints cannot reuse rates. OOM forbids the point. Bounds and shared-host accounting hold with fake devices. No actual load test required in unit tests. |
| **3. Completion-aware placement** | Common estimate/ranking in command and model-call paths; size/setup/transfer costs; learned safe call cap within configured bounds. | Fake-clock cases choose cold-fast versus warm-slow correctly, preserve deterministic unknown fallback, memory floors, tiers/fairness/holds and cleanup fences, and expose the reason. Reuse existing victim selection. Concurrency remains one for unqualified modes. |
| **4. Declared fan-out and batch planning** | Explicit independent pieces, retries/gather, and discrete horizon planning for machine plus count; serial/smaller tail handling. | Synthetic naming and CPU curves reproduce the arithmetic above; a short batch chooses fewer slots, a busy daily host contributes none, and loss retries only affected pieces. `myce sim` must show a makespan gain with production p95 unchanged on the agreed scenario before shipping; compare identical arrivals/policy/seeds. |
| **5. Generic workflow batching** | Generic stages/edges, each/join/all fan-in, bounded fan-out, max-size/max-wait batching, durable input membership, runtime hints/feedback, and run status. | The same parser/planner runs synthetic versions of **all four** flows above with no domain-specific fields or branches. Fake-clock tests cover size/time/closure flush, complete-group fan-in, bounded iteration, backpressure, retry/restart without duplicate consumption or reset timers, cancellation/preemption, and exact-attempt cross-worker artifact handoff. Prove a standalone synthetic workflow before any app integration. |

The simulator gate is required by the [recorded 6–7 hour regression from naive
splitting](https://github.com/1kuna/mycelium/issues/107#issuecomment-6308396248).
Native CUDA/XPU/Metal correctness, mixed-load curves, and real-fleet gain remain
unverified and require separately authorized acceptance. This research changed
no product code, ran no hardware benchmarks, and accessed no fleet machines.
