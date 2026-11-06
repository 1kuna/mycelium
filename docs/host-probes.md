# Adaptive host observations (#809, next release)

Host probes protect the owner before maximizing background throughput. MY-34
requires measurements from the running host; a canary on one machine does not
establish a public default. This implementation separates host observation from
per-attempt resource sampling and from execution/readiness lifecycle policy.

## Timing and freshness

Owner, shared-pressure, host-health, foreign-consumer and accelerator-inventory
observations use measured service cost and variation. Kernel-log scans reuse the
same timing calculation. Quiet signals reduce scan frequency; observed changes
constrain it. Available CPU capacity changes the cost calculation. Disk and RAM
size are not proxies for latency: their actual effect on a probe is measured.

The observation period is the greatest of the existing control delivery period,
one probe plus its proportional CPU share, and the geometric mean of probe cost
and time without a change divided by CPU capacity. With measured changes, two
observations per change constrain the quiet-period term. A slower probe is never
scheduled concurrently with itself. An uncancellable kernel call retains that
one lane; it neither stalls heartbeat/control nor accumulates replacement calls.
The lifecycle context cancels subprocesses; there is no fixed two-second owner
or pressure deadline, five-second background inventory deadline, or two-second
kernel-journal deadline.

`probe.period_seconds`, `cost_seconds`, and `valid_seconds` describe the completed
observation. Validity is the period plus the measured service-cost prediction
(mean plus four absolute-deviation estimates). The prediction uses RFC 6298's
1/8 mean and 1/4 deviation smoothing. These dimensionless coefficients supply
statistical memory and a jitter allowance; they are not fleet latency thresholds.
They can cause conservative withholding after a cost outlier.

An observation keeps its original start time and its frozen validity horizon.
A slow pending scan never extends the previous observation. A completed failure
removes it immediately. Inventory retains stable device identity, but stale or
failed capacity cannot qualify work; partial driver failures preserve fresh
healthy siblings. Direct launch ownership/identity checks remain mandatory.

The queue translates worker-local age to its own injected clock. Pressure can
reuse matching-session check-in contact, bounded by the existing transport
contact policy, but even repeated contact cannot extend an observation beyond
its original validity plus that policy. Owner age is also carried explicitly;
worker wall-clock skew cannot renew it.

## Owner availability and interference

Daily-use eligibility learns pauses within the observed owner session. Idle must
exceed that pause prediction and the observation resolution. Returning input
withdraws availability immediately. A sampling gap or clock rollback requires
fresh awake evidence. Thermal recovery requires nominal readings spanning the
host's measured failed cooling/reheat spans (at least one observation period); unknown readings
break recovery. Power, battery and explicit desktop-coverage rules are unchanged.
No five-minute idle or one-minute cooling duration is added by this policy.

Shared-profile pressure uses each worker session's healthy wake/PSI baseline.
A value beyond its prediction band yields; recovery requires readings back at
the baseline mean. Adverse observations cannot train themselves into a healthy
reference. The first reference cannot prove that a host was historically idle;
OS warning/critical memory levels and measured memory stalls protect cold start.
No 20/5ms wake or 1/0.1% PSI bands remain in this path. The wake probe uses one
OS timer, with its wait proportional to measured sensor cost and CPU capacity,
instead of twenty fixed five-millisecond ticks.

The first owner-latency interruption creates a job/host recovery hold. The quiet
span must cover that attempt's measured time to interference and observation
horizon. Only distinct fresh healthy observations advance it. Missing, adverse or
gapped evidence breaks it; diagnostics and elapsed wall time do not clear it.
Other eligible hosts remain available. Exact-attempt release, planned-stop
accounting and cancellation precedence remain authoritative.

## Protocol and remaining boundaries

`host-probe-v1` is negotiated before additive observation fields are sent.
Strict older queues receive the legacy shape; a downgrade permits the existing
single legacy retry. Local owner cancellation still applies. Older workers retain
their legacy five-second cadence/fifteen-second freshness interpretation until
upgraded; their absence of timing evidence cannot be interpreted as a learned
horizon. Previously persisted fixed job cooldowns remain readable.

The 100ms lower delivery resolution comes from the existing local model-control
withdrawal lane, and the default one-second control cadence is the existing
worker heartbeat protocol. Sampling faster cannot improve those consumers'
resolution and costs owner CPU. One outstanding OS call prevents resource
exhaustion. Nanoseconds are `time.Duration`'s representation resolution. Float
and duration representation checks prevent overflow, never limit host capacity.
No new retention cardinality, retry count, byte cap or workload timeout is added.

The original #800/#809 inventory also includes per-attempt RSS/device telemetry
(1s/three readings/two periods, 200ms, 5/10s freshness and bounded helper output),
local model approval (250ms), native sidecar/listener/startup waits (200ms,
2/5/15s), and driver teardown attribution (1s/one minute/three per hour). Their
current owners are sampler #858 (attempt cadence/freshness and bounded probe
output), guard #855 (RSS confirmation/grace), readiness #862 (approval and
sidecar readiness), and lifecycle #832 (startup/control waits and driver
teardown attribution). They are **not** retired or qualified by this host-observation change.
Their existing incident measurements explain their purpose but do not establish
portable universal values. #809 stays open until those changes land and a maintainer reconciles every
remaining bound against live code and replacement evidence. This scoped delivery
was explicitly authorized by the maintainer on 2026-11-06.

## Evidence and qualification

Fake-clock fixtures cover one- and 128-core hosts, very different local latency
baselines, idle and busy owner sessions, slow/stuck/failed probes, age/clock skew,
partial inventories, and recovery interrupted by missing evidence. HTTP fixtures
exercise negotiation and strict downgrade. Existing offline owner-stop and
exact-release regressions retain real command/checkpoint behavior.

These are laptop proofs. Native daemon, GPU driver stress, thermal cycles,
Windows session bridge, OS hang reproduction and representative daily-owner
latency under load require a separately authorized maintenance run. No native
hang-cause attribution, fleet change or deployment is implied.
