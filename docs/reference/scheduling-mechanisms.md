# Scheduling mechanisms

Part of the [approved design](../design-proposal.md), moved verbatim apart
from relative links. Product contracts and maintainer rulings are unchanged.

### Adaptive worker loss
<a id="adaptive-worker-loss-661-my-31my-34"></a>

Loss detection is measured, not a fixed timeout (MY-31, MY-34).

Loss uses the current session's send interval and observed arrival/path variation,
measured with the injected queue Clock. Workers advertise their actual interval
through an optional header compatible with older strict JSON decoders. An older
worker without that header first establishes cadence from successive accepted
wall-time arrivals; queue occupancy cannot compress its expected send period.
The second accepted arrival learns cadence before admission, so a headerless
worker waits at most one observed send interval plus actual control-path delay
for its first offer after a queue cutover. Startup recovery reports can supply
these samples before ordinary admission. An empty keyed reply is idle, with no
assignment to launch; existing attempts continue reporting and retain custody.
Retained engines receive retention-only plans and their existing cleanup
deadlines during cold qualification; those plans cannot start a missing engine.
Until then automatic loss is unknown and capacity is suspect, even with a saved
operator minimum, so no new attempt can acquire an unbounded unknown loss
allowance. The allowance is at least two send intervals (the
expected arrival and one missed-arrival confirmation), extended by the shared
RFC 6298 mean/variation estimator. It has no class-specific default duration.
Explicit old timeout settings remain minimums, never permission to count queue
delay as silence; zero selects automatic timing. Existing saved settings survive.
Each accepted transaction also saves its session's frozen contact allowance for
offline conversion. That stopped-queue observation expires against the original
accepted wall timestamp; opening a queue does not renew it. An older database
without this evidence needs the existing verified-empty drain path. Running
queues still require live contact and startup reconciliation.

The queue accounts the union of all queue-mutex occupancy, including readers,
with constant-size local counters. This time is unavailable control service and
is excluded from silence. A newly slow transaction is protected immediately,
without first training a latency percentile. A current-session check-in protects
that worker while the server processes or writes its response, including work
outside the mutex; it cannot protect a dead peer. Rejection removes that credit.
Cancellation retains a reconnect opportunity following the worker's existing
retry schedule, without changing accepted LastSeen. Unsettled synthetic arrivals
expire after the measured allowance. Stale sessions cannot renew current contact.

For a normally responding queue and an ordinary one-second worker, detection
takes roughly two seconds of service opportunity, plus learned variation and the
next maintenance turn. An observed first one-second gap initially gives three
seconds under the estimator; further steady samples converge to the two-interval
floor. Under overload the wall duration additionally includes actual queue
occupancy. For example, a 31-second blocked transaction adds 31 seconds, not a
charged infrastructure failure. Suspicion uses wall time since the last current-session arrival, with an
allowance of at least two observed wall arrival intervals plus measured variation.
That wall cadence includes delivered queue service cost even for a worker that
advertises a shorter send interval. It remains separate from the opportunity
allowance for confirmed loss; otherwise a healthy slow control path could
withhold every new offer. Before the second arrival, the completed response's
measured wall cost extends the advertised interval. Recovery inference excludes
late pending arrivals but retains this completed delivery evidence, so normal
slow delivery can establish cadence instead of repeatedly resetting it.
Confirmed loss requires both a missed wall arrival and the opportunity
allowance: available service alone cannot make an expected arrival overdue.
Pending NAS inspection delivery uses the same wall
allowance, including in detached status/resolve views. Even during continuous mutex saturation, a dead sender
becomes **suspect** after that allowance plus the next maintenance/observation
turn. An old in-flight request cannot suppress suspicion. Suspect workers are
never shown as healthy, receive no new attempts or preparation launches, and
raise an owner-attention health alert. New arrival evidence clears suspicion.
Suspicion never interrupts an existing attempt or charges loss/retry.

A worker's check-in loop also does local durable work between check-ins, and
under host load that work can delay its next check-in past the allowance while
the process is alive (#1450). The worker's stop-event long poll runs beside
that loop, so a current-session poll also counts as contact for confirmed loss:
while it is open, and for the allowance after it ends normally. A dropped
connection gives no credit after its start, so a dead process is fenced at once
and a silently dead host after at most the server's existing poll window. The
poll proves the process, not its loop, so its credit is bounded: check-in
silence beyond the measured filesystem-operation stall backstop (30 s, #289),
or twice the session's longest completed check-in gap if that is longer, is
still confirmed loss, so a wedged loop's attempts are retried. Suspicion and
cadence learning still use check-ins only, so a stalled loop is still suspect
and receives no new work.

In steady state, continuous saturation or a wholly unreachable fleet has no
finite trustworthy confirmed-loss bound. Custody waits for evidence from
control opportunity and connectivity. Suspicion still ages with wall time.
Confirmed loss preserves exact attempt fencing and existing retry budgets.
Release requires its own proof.

#### Queue restart recovery

Before the queue accepts its first reconciliation check-in, only idle control
opportunity spends startup grace. A stalled startup can therefore retain a
worker's original attempt even when the configured grace has passed in wall
time. Once a check-in commits, an additional window starts. Its length is the
largest of the configured grace, two send intervals of the reporting worker
and the saved session-matched loss and wall-arrival allowances of workers
still due to report, plus the observed time to process that first check-in.
Older records can supply the saved loss allowance alone.

The first check-in fixes the deadline. Each subsequent committed check-in
advances the window to that request's arrival time. A request that arrived
before a stall cannot spend the stalled interval by completing late.
Maintenance can mark silent workers gone as soon as either the startup
opportunity or the check-in window expires. A current-session report still
being processed protects its worker. A cancelled request can protect that
session until its measured reconnect delay elapses in wall time. Exact
pending handovers retain their own custody protection. With neither usable
idle opportunity nor a check-in reaching the deadline, the queue needs
further evidence to expire the window.

The restart warning and startup network-recovery suppression use the same
timer. An unreported worker's stale health inputs and unknown-probe alert
use the same per-worker protection as loss marking. Planned maintenance
continues to exempt stale health alerts. The start-control refusal exception
ends with the timer and still requires exact proof of no execution.

Fleet, maintenance, placement, inventory and stale-host diagnostics use the same
confirmed-contact and suspicion calculations, frozen at status capture time. LastSeen remains an actual
accepted queue timestamp. Physical memory/disk/pressure/probe sample validity
still ages independently; liveness never manufactures fresh sensor evidence.

