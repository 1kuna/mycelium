# Admission-only process-memory growth (#954)

Darwin RSS is sampled evidence, not a hard memory ceiling. Learned and bounded
exploratory reservations may grow while the host remains healthy (#855).
Clipping forecasts to those reservations would spend that growth twice.

The process observer pairs each positive RSS increment with the elapsed time
between the two readings that measured it. Its live allowance is that increment
and its live rate is increment / elapsed seconds. A flat or falling reading
reports zero allowance and zero rate. The attempt's peak merge independently
retains the largest measured increment for future sizing. A missing reading
breaks the pair; a first reading or nonpositive clock interval proves no rate.

The reading's timestamp is captured immediately after reading RSS, before guard
evidence persistence or signaling. Guard action timestamps and identity fences
are unchanged. A slow stop therefore ages the reading instead of assigning an
already-read footprint to a later time.

Admission retains the common footprint + growth + rate × sample age + measured
drift forecast. A positive slope still reserves predictable growth through the
entire existing freshness window; it is not saturated after one increment.
With measured Darwin physical inventory, the forecast charge is bounded by
physical RAM, while retaining actual footprint and known floors even when they
exceed physical RAM. Hitting this bound with RSS grants no resident credit and
consumes the host's admission room. Missing physical inventory leaves the
forecast unchanged. Linux's estimator and hard-cgroup path are unchanged.

The failure prevented is multiplication of unrelated historical extremes:
the original estimator retained both the maximum positive rate and longest
interval for the entire attempt. Synthetic injected-clock observations
`(seconds, GiB) = (0, 3.35), (1, 4.35), (108.6, 4.35)` produce 107.6 GiB of
growth under that estimator. The paired estimator reports zero live growth/rate
after the flat reading and retains 1 GiB of measured peak growth. Related traces
reproduce 19.9 and 15.5 GiB at 2.6 and 4.4 GiB peaks. These historical arithmetic traces use synthetic timestamps; attribution of
historical refusals remains unproven.

The cost is that a flat observation stops projecting a historical burst.
Future unseen bursts remain unknown, as with any sampled estimator. Existing
guard, pressure, stale/missing telemetry, stopping-attempt and exact-release
rules remain necessary. Sustained measured growth continues to reserve ahead:
the regression at 1 GiB with 0.1 GiB growth over 0.1 seconds still charges
5.1 GiB at age four seconds and refuses an unsafe neighbor on both queue and
worker. The physical bound is inventory evidence, not an operator-tuned cap or
a native containment/safety qualification.

## Replaceable bounded-auto forecasts (#1077)

For learned/exploratory native CPU auto attempts without a cold-probe floor,
forecast retention requires a fresh identified complete growth observation.
Expired or unidentified slopes cannot increase the durable charge. A newer fresh
sample can replace forecast-only uncertainty, while separately persisted observed
peaks never decrease. Missing/stale samples retain the last accepted charge and
grant no resident credit. Queue restart retains both the observed peak and the
last accepted prediction/identity; cached or older IDs cannot correct it.
Pre-correction opaque charges remain conservative until exact release. Cold
probes, declared containment and exact release custody retain their own rules.

## Cold-probe admission and containment (#1116)

A cold CPU probe without fresh complete Started telemetry keeps its full assigned
admission envelope. The adaptive execution ceiling may be wider so an unknown
command can grow under the physical-headroom/pressure guard; that containment
ceiling is not additional reserved demand. Substituting it for an offered residual
envelope can exceed the host allowance beside existing work and hold startup
before the first sample. Missing/stale samples grant no resident credit; fresh
Started observations and exact-release custody retain their existing rules.

Cold probes also retain exact-attempt measured high water across expiry, missing
reports and queue restart. Fresh complete Started samples can release unused
offer capacity, but cannot release an observed peak. Cold forecasts remain live
predictions rather than durable measured floors; a later flat sample can correct
them. Admission, worker checks and final launch use the same measured floor.
