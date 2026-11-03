# Shared-host pressure freshness

Shared interactive hosts protect their owner with measured reserves, polite CPU
execution and pressure-triggered yield. The canonical admission, thresholds and
native qualification guidance is in [operations](operations.md#pressure-response).

## Freshness during slow check-ins

Pressure sensors refresh every five seconds in an independent worker lane.
Check-in HTTP latency therefore cannot delay consuming a completed probe or
starting the next refresh. A completed probe failure removes its sample; a stuck
probe retains the previous sample with its original age. Late results retain
the probe start time, rather than becoming fresh when consumed.

The queue accepts only valid samples whose worker-local age is below 15 seconds.
It derives the observation time from its injected Clock minus that age, ignoring
worker-supplied receipt timestamps. Between reports, freshness is evaluated as:

```text
report_time = observation_time + reported_age
contact = max(report_time, existing_session_contact)
effective_age = reported_age + now - contact
fresh = effective_age < 15 seconds AND now - observation_time < 45 seconds
```

Existing session contact includes a matching in-flight check-in for at most
30 seconds from arrival, even if queue contention makes the client cancel.
This is the existing worker-silence protection, not a new retry allowance.
The total 45-second bound is the 15-second sample limit plus that contact
budget: repeated requests cannot renew one observation indefinitely. The cost
is retaining an otherwise valid low reading during bounded queue delay; the
benefit is avoiding false pressure loss during the observed 1–20-second waits
in [#661](https://github.com/1kuna/mycelium/issues/661). Longer queue stalls lose
this protection. Reported age still expires at 15 seconds, so a probe that keeps
failing or stalls cannot be hidden by successful heartbeats. Actual high readings
and the existing recovery hysteresis remain authoritative immediately.

Without new reports/contact, pressure expires at its original 15-second bound;
ordinary worker-loss health can become unhealthy sooner. Unknown pressure
withholds new work without treating it as measured interference or stopping
running work. The queue never renews probe age from a worker wall timestamp.
All queue freshness decisions use Clock, with no sensor or bulk I/O under q.mu.

## Health agreement

Host maintenance health includes pressure for shared-profile hosts and uses the
same bounded session contact as worker-loss detection. Fleet status copies those
contact facts into its detached observation and uses the same host classifier.
This fixes a host-status response being healthy despite unknown/stale pressure,
and a fleet snapshot losing in-flight contact and interpreting queue delay as
silence. The ordinary one-second public status cache remains an older snapshot;
compare snapshot times when checking separately issued commands. See
[status fields](status.md#shared-interactive-profile).

The implementation is queue-side and worker-side. Upgrading only the queue
fixes health/contact interpretation but does not make older workers refresh
pressure independently of slow requests. Native pressure calibration remains
separate from the fake-clock incident regressions.
