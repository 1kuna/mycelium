# Proposed XPU recovery evidence without vendor process starts

Status: design proposal for #1181, split from #1168 / #1171. This does not
change the approved product contract or implement a new recovery source.
The release candidate separately removes periodic vendor memory polling and
paces health queries. It does not guarantee custody exclusion for health starts.

## Required behavior

The worker must never start an XPU vendor health command while accelerator or
model custody exists. Actor admission, model control and kernel-fault scans must
continue when process creation or a health source stalls indefinitely. Recovery
still requires positive evidence after the current fault and an uninterrupted
measured quiet window. Missing evidence must not become healthy evidence.

## Why process-start fencing is rejected

An idle check does not order a later process start against custody acquisition.
Checking before each card does not fence a batch already pending. A shared
mutex around custody and `cmd.Start` closes that race, but Go starts the
`CommandContext` cancellation watcher only after `os.StartProcess` returns.
Executable/interpreter filesystem I/O can therefore stall Start while holding
the mutex. Custody acquisition and a later control-loop health update would
then wait indefinitely. A deadline on the context cannot release that mutex.
Moving the lock into another goroutine leaves the late-start race or an
unbounded launch wait. A bounded handshake also needs proof that an abandoned
start cannot resume after custody proceeds; cancellation alone is not proof.

## Proposed structure

Remove automatic vendor commands from the worker's XPU quarantine-recovery
path, including the idle path. Obtain an optional positive recovery witness
through a read-only driver/broker source in the existing asynchronous observation
lane. The worker never asks this source to run a vendor tool, perform a GPU
self-test, or fall back to vendor health. CUDA and other backends remain outside
this proposal unless independently qualified.

There is no shared launch lock and no process-start phase to coordinate with
custody. Accelerator actors and model loads proceed through their ordinary
paths. A permanently stalled source leaves one outstanding observation and
unknown health. The worker keeps consuming kernel events and executing control
transactions; it does not launch repeated copies or wait for source completion.
The source must preserve the sample's original age and device identity.

Only a qualified positive witness may start or continue recovery proof. It must
identify the device, boot and reset generation, postdate the current fault, and
remain compatible with the current kernel-monitoring interval. A new fault,
identity change or monitoring gap invalidates it. Existing recovery timing and
positive-evidence checks remain authoritative.

## Source qualification is a prerequisite

No sufficient passive witness is claimed here. Audit supported driver ABIs and
their implementation before choosing the source. A successful MEM_REGIONS
query, readable memory counters, PCI presence or absence of new kernel errors
does not establish compute health. The current broker VRAM extension is not a
positive health witness and must not be reused as one.

For a supported witness, prove that a post-fault positive result plus the
existing measured quiet window eventually clears quarantine. For unsupported,
absent, stalled or ambiguous sources, retain quarantine and expose unknown
health. The existing explicit operator-clear route remains available. This
changes automatic recovery availability on unsupported drivers, so that cost
must be reviewed before replacing the current vendor-backed implementation.
Do not ship this proposal merely by disabling positive-evidence requirements.

## Failure analysis

| Failure | Required outcome |
| --- | --- |
| Custody arrives before, during or after source preparation | Zero automatic worker XPU vendor starts; no custody wait |
| A source or simulated executable start never returns | One outstanding observation; control, models and kernel scans progress |
| Old source work completes after a new fault or boot | Discard it; do not renew its timestamp or clear quarantine |
| Broker is missing, denies access or provides memory only | Unknown health; quarantine remains |
| A qualified positive witness completes late | Check event, boot, generation, original age and monitoring continuity before use |
| Existing quiet window has not elapsed | Reuse evidence only while valid; do not clear early |
| Native driver behavior differs from source assumptions | Withhold support until native qualification supplies evidence |

Read-only does not imply no hardware effect. The current Xe query path can
acquire runtime PM and wake/resume a device. Any new source needs its own source
audit and native measurements. Neither this design nor #1171 establishes a
causal fix for reported compute resets.

## Verification before implementation can merge

Preserve the three earlier review scenarios as behavioral regressions: custody
arrives after selection at the production factory boundary; a multi-card batch
is pending when custody arrives; process start stalls indefinitely. Replace
mutex-specific assertions with the required outcomes, including zero vendor
invocations, advancing control/kernel scans and no stale healthy release.

Use fake probes and the injected Clock. Count invocations over simulated time,
prove that pending work stays bounded, and demonstrate eventual recovery with
a qualified positive witness. Run the related race selection and exact-head
native/Linux gates. Separately qualify each supported native driver source;
laptop tests cannot establish its meaning or GPU effects.

Next step: independent design review and driver-ABI source investigation. This
is a draft design only, with no replacement implementation or deployment.
