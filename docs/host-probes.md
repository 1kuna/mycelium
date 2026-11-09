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
observations per change constrain the quiet-period term. Periods run from the
original start; timers wake at the actual due time, including between control
ticks. Inventory progresses independently of HTTP check-ins. A slower probe is never
scheduled concurrently with itself. An uncancellable kernel call retains that
one lane; it neither stalls heartbeat/control nor accumulates replacement calls.
The lifecycle context cancels subprocesses; there is no fixed two-second owner
or pressure deadline, five-second background inventory deadline, or two-second
kernel-journal deadline.

`probe.period_seconds`, `cost_seconds`, and `valid_seconds` describe the completed
observation. Validity is the period plus the measured service-cost prediction
(mean plus four absolute-deviation estimates). Host-health and shared-pressure reports also
include the same prediction of their observed control/report gap: the preceding
queue round trip plus the next report's preparation. Cheap OS sensing does not
imply cheap delivery to a launch fence. This gap trains before profile activation
and affects only newly completed probes; later slow reports cannot renew old
evidence. Both ages are captured at send time, after report preparation. Local
health admission reads do not train report cadence. During initial learning,
evidence can still expire: contact alone never proves that a sensor is healthy. A stopped sensor expires at its last completed
observation's frozen period, cost prediction and learned report-gap prediction,
even if the worker continues checking in.
The prediction uses RFC 6298's
1/8 mean and 1/4 deviation smoothing. These dimensionless coefficients supply
statistical memory and a jitter allowance; they are not fleet latency thresholds.
They can cause conservative withholding after a cost outlier.

An observation keeps its original start time and its frozen validity horizon.
A slow pending scan never extends the previous observation. A completed failure
removes it immediately. Inventory retains stable device identity, but stale or
failed capacity cannot qualify work; partial driver failures preserve fresh
healthy siblings. Direct launch ownership/identity checks remain mandatory.
Inventory compares hardware, topology, configuration and health, excluding
telemetry timestamps, activity counters, free memory and a unified device's
live uncapped budget. Workload activity cannot reset discovery backoff.
Unchanged failed-family classifications also back off; failed devices remain
unknown and partial observations retain their original expiry. Each
pass reuses its device enumeration for XPU qualification fingerprints and
shares identical fingerprint commands across cards; fingerprint bytes remain
compatible with existing qualification records.

Discrete XPU telemetry has its own observation lane. One host-wide `xpu-smi`
CSV query supplies PCI-keyed memory counters for all cards, at most once per
existing five-second device-consumer interval (or the slower control/probe-cost
cadence). Xe PMU activity remains per PCI device. A missing tool, unsupported
counter, missing row or duplicate PCI row supplies unknown memory. Results keep
their original sample start, and merge only into matching device/driver,
selector, topology and reserve observations. Missing or ten-second-old samples
cannot authorize telemetry-dependent sharing. A slow query never blocks
discovery, check-ins or starts another copy of itself. Per-device runtime
fallbacks use private device-affinity environments where supported; a full
inventory or host-wide telemetry query intentionally covers the host.

For an already assigned attempt, a fresh direct launch probe can establish
recovery from a cached `device_probe_unavailable` observation. The direct probe
must still succeed, identify exactly the assigned device and report healthy
capacity; saved identity and policy fences remain binding. This does not renew
the background observation or restore its advertised queue capacity.

The queue translates worker-local age at request receipt to its own injected
clock and includes preparation/transaction waiting time in diagnostic age.
For host health, admission and degradation use that original worker-local age
plus available queue service opportunity since acceptance. Incoming request
processing and queue mutex occupancy cannot consume the opportunity to accept
a replacement report. This uses the same mutex service clock as adaptive worker
contact, without treating contact as sensor evidence. This also covers the first
fresh report of a restarted worker before any report gap is learned: only actual
queue delay is excluded, and the remaining frozen validity bounds available
service after processing. Already stale or failed reports still refuse immediately. A newer fresh health report currently being
prepared outside the mutex pauses only its own session's prior service age at
receipt. It cannot revive already expired evidence, and stale or failed input
cannot obtain this credit. The marker spans preparation, reply hydration, HTTP
NAS instructions and response encoding, including the first report of a session. Successful return retains only that
receipt's actual queue processing credit; rejection or cancellation removes the
marker without retaining credit. After return a silent sensor has its remaining
frozen validity in available service time. Unrelated workers keep aging. Owner and pressure age rules are unchanged.

A silent sensor expires after its remaining frozen validity in available queue
service time. A reporting worker whose sensor stops expires when its growing
worker-local age reaches frozen validity and that report is processed. Under
normal service this is at most the frozen validity plus the next report delivery;
a measured queue stall adds its actual duration, not a new fixed timeout.
Continuous queue unavailability, including an indefinitely pending check-in
preparation for that session, has no finite trustworthy wall-time expiry bound. Queue restart discards service credit and uses conservative physical
wall age until another report arrives. Status snapshots freeze both clocks;
reported `age_seconds` remains physical diagnostic age, so it can exceed
`valid_seconds` while excluded queue service accounts for the difference.

Unified-CUDA physical host admission uses the accepted check-in's adaptive
worker freshness and the same frozen host-health evidence validity. It has no
separate fixed check-in age cutoff. Invalid physical RAM measurements, expired
or failed health observations and stale worker sessions still withhold room;
physical reserves and outstanding allocation growth are subtracted unchanged.
A completed failure and explicit drain still withhold immediately. Overlapping
check-ins retain the newest accepted queue receipt in the same worker session;
an older delayed success cannot clear a newer refusal or reset its age.

Owner age is carried explicitly; worker wall-clock skew cannot renew it. Older pressure
reports without timing evidence retain the existing bounded contact policy.

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
Missing observations retain session learning and the adverse-episode latch;
only fresh healthy observations can establish recovery.
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
single legacy retry. Unknown health and confirmed drains use reversible
availability withholding rather than the permanent worker leave signal. Local
owner cancellation still applies. Older workers retain
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

`TestInventoryHelperLaunchMeasurement` measures an instrumented two-card fake
inventory for one simulated minute, with a one-second heartbeat, 250ms scan
cost and two CPU cores. The former per-card fingerprint/query path launches
174 enumeration and 116 memory helpers; the corrected path launches 35 and 12.
This compares launch counts, not native driver overhead or throughput. No
production GPU or compute workload is exercised.

After an authorized deploy, the operator can compare equal-duration kernel-log
windows before and after worker activation: count peer-to-peer DMA warnings per
elapsed minute and record helper launches, observation cost/period, sample age,
workload occupancy and driver errors. Use existing logs and read-only process
observations; do not start GPU stress or extra driver probes on a production
host. Exclude the activation window from steady-state rates and compare similar
occupancy. Expected evidence is fewer enumeration/memory helpers and fewer
warnings without stale data granting sharing. A warning-rate reduction alone
does not establish the cause of compute timeouts; track those independently.
