# Adaptive host observations (#809, next release)

Host probes protect the owner before maximizing background throughput. MY-34
requires measurements from the running host; a canary on one machine does not
establish a public default. This implementation separates host observation from
per-attempt resource sampling and from execution/readiness lifecycle policy.

## Restart and launch diagnostics

Worker inventory reports its observation age independently of check-in contact.
Pending, failed and stale discovery cannot admit device work; CPU admission
continues. After a launch inventory refusal, cached discovery cannot re-admit
that card: a completed fresh scan must have started after the refusal. Healthy
families in a fresh partial scan remain eligible.

`inventory_probe.operation` names `nvidia_identity`, `nvidia_memory`,
`runtime_topology`, `kernel_identity` or `runtime_inventory`, with the elapsed
`pending_seconds`. `error_class` preserves deadline/cancellation and typed
runtime failures. Operation names contain no command arguments or private paths.
The direct prelaunch inventory probe uses exact-attempt cancellation rather than
a fixed five-second timeout; stop/release custody remains unchanged. Discovery
still retains only one outstanding scan and never stalls check-in/control.

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
even if the worker continues checking in. That validity predicts the worker's
next send, not that report's arrival, so the queue judges a measured
shared-pressure sample at each report and then keeps it until the next report
is due: the report's processing time plus frozen validity plus the session's
measured check-in delivery prediction (`delivery_seconds`, frozen at
acceptance). A stopped sensor fails at the report where its age reaches
validity; a missing or failed sample fails at once; a silent worker expires
when that next report is overdue. Health, alerts and admission read this one
predicate (#684).
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

Discrete XPU telemetry has its own demand-driven observation lane. Only active
opt-in fan-out sharing consumes physical card headroom. Idle, CPU-only and
exclusive-device heartbeats do not request these counters. Sharing reuses one
sample throughout its existing ten-second freshness window, without renewing
its timestamp; measured probe cost can slow subsequent observations. Missing or
expired evidence withholds additional sharing. Ordinary configured-budget
admission and exact-attempt DRM fdinfo sampling remain independent.

Memory comes from the optional read-only GPU telemetry broker's PCI-keyed
`vram` counters. The worker never falls back to `xpu-smi`: starting that tool can
initialize the vendor runtime on every card, including busy siblings. The
reference broker uses only `DRM_IOCTL_XE_DEVICE_QUERY` with `MEM_REGIONS`; it
creates no GPU objects or execution contexts. Older kernels silently censor usage to zero without `CAP_PERFMON` or
`CAP_SYS_ADMIN`; newer kernels expose it without privilege. The broker accepts
positive usage without extra privilege, but treats a zero as unknown unless
accounting capability is present. The template adds read-only DRM access with
no capability expansion.
A GTT-only broker remains valid for host-backed storage but supplies unknown
VRAM. Upgrade and qualify the broker separately to retain opportunistic sharing;
missing counters never become free device memory. Xe PMU activity remains per
PCI device. Samples merge only into matching device/driver, selector, topology
and reserve observations, with their original age.

Quarantine recovery is separate from inventory telemetry. A completed health
reading is reused until the recovery decision's existing quiet window: the greatest of
fault-to-reading latency, previously measured recovery, and the existing kernel
teardown-attribution resolution. Failed readings lengthen that interval from
observed failure duration. A new fault event starts a new decision. This avoids
repeated vendor initialization while a quiet-window decision cannot yet change;
the cost is later recovery detection within the measured window. No new fixed
retry rate is introduced. Health work remains asynchronous, so a stalled tool
does not block worker control or kernel scans. This pacing does not exclude
vendor health starts during accelerator/model custody; that guarantee requires
a separate design and review.

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
174 enumeration and 116 memory helpers; #1040 reduced this to 35 and 12, while
intentionally retaining the five-second host-memory lane. The demand regression
now simulates one minute each without demand, with sharing demand and after
withdrawal: zero, six and zero additional broker memory reads, and zero vendor
initializations throughout.
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

## Optional xe CCS preemption timeout

The optional [host template](../deploy/xe-preempt-timeout/README.md) persists an
explicitly configured CCS `preempt_timeout_us` across boot and xe device adds,
using a metadata-only udev rule that requests a bounded per-card systemd
oneshot, plus an independent boot oneshot. Udev launches no helper and does no
sysfs/state I/O; it does not wait for application to finish. The boot
unit is wanted by `multi-user.target`, has its own start timeout, and has no
ordering/dependency against the worker. Nothing is added to worker startup:
the setting matters when GPU jobs create execution queues, well after boot.
Failures/timeouts affect the optional operation; mismatches log an explicit
status for the operator. Uninterruptible kernel stalls cannot be cured by a
process timeout. The template is disabled by default (no writes), is installed
manually, and can restore recorded prior values and be removed. It never
changes job timeouts or timeslices.

Evidence status: raising 640000 to 1280000 microseconds is a **diagnostic trial**
for observed GuC preemption timeouts/engine resets on Intel Arc Pro B70 with xe
and Linux 7.0, not a vendor-validated setting or established fix. Confirmation
batch results and native boot/hotplug proof are still required before separate
authorization to apply it. A hung context can take up to an extra **0.64 seconds**
to reset, delaying recovery. Newly created GPU execution queues inherit the
value; existing queues retain their old value, including during rollback.

The template's README contains installation, direct sysfs/journal verification,
prior-value (or explicit 640000) rollback, uninstall and laptop fake-sysfs tests.
The live sysfs bound names and validation follow Linux 7.0's
[`xe_hw_engine_class_sysfs.c`](https://github.com/torvalds/linux/blob/v7.0/drivers/gpu/drm/xe/xe_hw_engine_class_sysfs.c).

## Quarantine health proof diagnostics (#1185)

XPU recovery reads the native `xpu-smi health` JSON fields `core_temperature`,
`power_health` and `memory_health`. Required components must report `OK`.
A component explicitly undeterminable on both the target and an otherwise
healthy same-model sibling in the same health batch is excluded from that proof
and published as `unsupported_on_device`. This is the orchestrator's 2026-11-09
agent default under MY-34, not a maintainer ruling. The existing nonempty
hardware/driver fingerprint identifies the model. Each batch obtains fresh
comparison evidence; new sibling kernel faults invalidate it. Each query keeps
its own start/completion time. A monitoring gap rejects any query begun before
the gap, and a subsequent kernel scan must follow both query completions. The
whole comparison is bounded by one control interval plus the prior measured
kernel service-cost prediction, frozen before starting. Slow comparison cannot
renew older evidence or grow its own allowance. All other
required components must be OK, and warnings on optional components still
prevent clearance. Missing/malformed readings and Unknown without a healthy
same-model sibling keep quarantine. The kernel observation window is unchanged.

The command runner keeps stdout JSON separate from stderr diagnostics. A MEI
initialization warning cannot replace component evidence, and successful exit
alone is insufficient. The supported Linux XPU installer/startup migration
grants the existing worker primary group access to GPU `mei_gsc` nodes (0660,
required for IGSC's read/write open) and Xe PMT telemetry attributes (0440,
read-only), via a root-owned udev rule and immediate application to matching
nodes. Unrelated MEI nodes are excluded. It needs noninteractive administrator
authority, installs no sudoers grant, runs no health command as root, and needs
no manual group/permission step. The captured health invocation opened MEI
`O_RDWR` for IGSC/HECI, which requires both read and write permission; PMT used
`O_RDONLY`; the [sanitized native excerpts](evidence/xpu-health-access.md)
retain the open flags and matching `mei_gsc` observation. Group `0660` permits
channel operations beyond health queries; the permission helper does not
restrict that API. A narrower health-only helper remains unqualified future
work. Every install/start retries the full provisioning sequence, rather
than trusting rule bytes left by an interrupted setup. The root-only node
journal also captures recreated nodes; last-owner uninstall restores unchanged
grants to the saved permissions and removes the rule/helper. Other installation
owners and administrator edits are preserved. An external root-only completion
record makes the final teardown resumable after ownership/validation metadata
is removed; it remains until the state directory is gone. See [installation and rollout
semantics](reference/accelerator-quarantine.md#accelerator-driver-fault-quarantine).

Rejected proofs publish sanitized `quarantine_recovery_blocked` status after
`device-health-v1` negotiation, with the failed component or command exit status.
It describes the last rejected observation, not a current health guarantee.
New healthy evidence removes it but still needs the existing uninterrupted
kernel observation and measured recovery window. Logs record changed rejection
classifications rather than raw vendor text. This does not change the probe
cadence; independent pacing is tracked in #1168/#1171.

The native diagnosis found unknown memory health even as root and both MEI and
PMT access denials for the worker. It does not establish an MEI-only dependency.
Earlier healthy-sibling evidence was supplied by the orchestrator; the fresh
comparison was skipped because that device had a running production attempt.
Native installation and recovery verification remain release-rollout work in
#1185. No live permission changes or extra device queries accompany this patch.
