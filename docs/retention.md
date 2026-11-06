# Adaptive evidence retention (#811)

This is next-release behavior under MY-34. It changes neither configured
retention policies nor the authority needed to release somebody else's data.
There is no deployment in this change.

The queue measures available host RAM and free bytes on its own database volume,
not the summed capacity of its workers. Its soft evidence target is the smaller
of available RAM minus the larger of the live Go heap and encoded queue state,
and free disk minus the encoded state. Those reserves buy a detached observation
and a durable rewrite. Missing measurements grant no optional capture or
pressure-driven compaction. Targets never authorize deletion of unique data.
An installation can remain above target because its evidence is protected.

`evidence_retention` reports measurement time, measured capacity, budget,
retained/protected bytes, last-pass reclamation, drifted rows, availability and
over-budget state. These are encoded logical queue bytes, not physical reclaimed
filesystem blocks or a guarantee of total process RSS. Status borrows this small
immutable summary; it does not measure capacity or run collection.

Committed record changes enqueue maintenance candidates. Each pass borrows one
immutable encoding at a time, decodes off the control mutex, and fences the edit
against the publication/dependency revision. Concurrent changes requeue it.
Maintenance yields to measured control pressure and after one ordinary measured
control-service quantum. A cold queue does one indivisible record to learn.
There is no scan/decode of all retained jobs on every tick. Idle file reclamation
releases lifecycle/control fences between objects and yields to waiting control
work; it has no numerical object batch limit. SQLite changes use normal undo and
FULL-synchronous commits.

A new episode of measured capacity pressure revisits unchanged history through
a resumable key cursor. Completed external-summary rewrites are fingerprinted
so their commit notifications cannot feed back into repeated resizing. An indexed
deadline per host expires buckets after their final public reader window even
when that host stops checking in; deadlines update in place rather than growing
with check-in count. Resized buckets keep their aligned identity so the next
sample adds to their retained counts.

## Evidence value and dependency rules

- Job intent, cancellation, wait/error attribution, individual attempt facts,
  custody ledgers, replay answers, accounting and incident delivery identities
  are unique evidence. Neither a file/log policy nor elapsed age deletes them.
  Under pressure, only byte-identical duplicate closed-attempt rows compact;
  distinct identities, placements, waits and prediction errors remain.
- Open jobs/calls conservatively pin learning inputs, including pooled and
  cross-target observations with no exact timing key. Ledger learning links pin
  their keys even after closure. Usable evidence, negative/constrained points,
  escalation floors and reversibly absent components remain. Only proven hard
  identity drift with no dependent reader retires a work observation. Storage
  failure floors stay protected; elapsed time alone says nothing about reuse.
- Input claims pin their exact bytes between requests until submission transfers
  custody. HEAD/upload lifecycle fences and reference revisions protect races;
  the longest observed claim-to-submission delay supplies additional publication
  protection. There is no universal six-hour promise. Unknown imports and
  unclaimed attempt directories have no age-based collection authority.
- External RAM telemetry's readers request 24-hour and seven-day windows. These
  existing public query horizons explain their coverage; they are not job-data
  expiry defaults. Capacity and measured serialization cost size time subdivisions.
  Aligned subdivisions aggregate sample counts, distributions and exact peaks;
  they never slide a stale peak's bucket forward indefinitely. The intersecting
  boundary bucket is included, so inspect first/last coverage, counts and peak
  time. Offline gaps create no samples. Records beyond every reader window expire.
  Percentiles are conservative observed bin maxima; new bins correspond to the
  bits of addressable byte counts, rather than a chosen GiB resolution. Legacy
  histograms remain readable with their original approximation.

Alert history and per-host external summaries use separate durable rows. Closed
alert history is not repeatedly serialized with base state. Status borrows
immutable generations and copies/projects their histories after unlocking.

## Retired numerical defaults and their replacements

- Results/logs/history seven days: omitted file/log retention is zero, retaining
  unique bytes until explicit release. Positive operator durations still expire
  their named payloads. They never expire job history. Recovered results without
  an explicit deadline remain protected. Seven-day defaults on an existing
  installation remain its saved policy until its owner changes them.
  Older installers that require positive retention refuse a new zero-retention
  saved config without changing it; omitting the zero to recover an old default
  would silently weaken protection. Explicit positive saved policies retain the
  supported installer/worker interoperability journey.
- Publication six hours and attempt/Docker 24 hours: claims, references, measured
  publication delay and exact acknowledgement replace elapsed default authority.
  Unknown folders remain. Docker's automatic sentinel is the maximum representable
  duration, a protected mode rather than an actual age; explicit Docker durations
  retain their existing operator meaning.
- Keyed submissions 30 days, usage 90 days and lossy downsampling: unique replay
  and exact accounting survive. A duration representation overflow remains a
  physical API limit, not a chosen lookback window.
- Memory/storage attempts 32, attempt history four, delivered alerts 256: unique
  facts and delivery custody survive. The older dropped counters remain readable;
  existing discarded data cannot be reconstructed.
- Prune/reclaim 64: one record is the indivisible transaction unit; idle I/O can
  continue between unlocked objects, yielding under measured control contention.
- Diagnostic tail 64 KiB, report 1 MiB and eight reports: authored report lists
  retain their meaning. A process-wide reservation accounts for concurrent
  failure/progress/Docker capture, raw/encoded strings, parser tokens, identifiers
  and backing-array growth using Go representation sizes. Unknown capacity
  withholds optional capture; it does not delete source reports/logs. A report
  outside its grant remains at its source and is reported unavailable/oversized.
- Docker stderr 64 KiB/256 lines and events 256/64 KiB/reload 128 KiB: measured,
  reserved byte allowances replace counts. Stderr streams through a byte tail;
  stdout stays excluded. Accepted event records never evict earlier unique facts.
  Exhaustion ends optional capture and preserves its existing archive; unreadable
  or over-budget recovery archives are preserved rather than overwritten.
- Progress 4 KiB/30 seconds: a reserved read checks the actual file size; ordered
  source updates teach cadence. Age stays visible. An unknown cadence cannot
  assert staleness, and unchanged file reports never renew freshness. Progress
  remains advisory and cannot prove execution, completion or release.
- Work evidence 30 days: hard identity drift and reader dependencies replace age.
- External history 15 minutes × 673 and quarter-GiB rounding: measured capacity
  and serialization cost determine subdivisions; returned sizes are unrounded.
  Suggested headroom uses authored reserve and measured peak-to-typical variation;
  one sample cannot invent variability or predict unseen future peaks.
- Request 256 samples/128 routes: a duration histogram uses one slot per bit of
  `time.Duration`; only finite registered mux patterns plus `unmatched` enter it.
  Counts cover all observations; percentiles are conservative bin maxima.
- Every 16th hold and fixed 250 ms/500 ms/one-second slow lines: all holds are
  observed; local service mean and adjacent variation identify unusual cost.
  Measured sink cost gates additional logging. Cold observations teach first.
- 64 client identities/minute summaries: sanitized identities share measured local
  diagnostic headroom, with finite per-kind overflow counters. Cumulative summaries
  add information on a binary traffic-magnitude change, subject to measured sink
  cost and one in-flight encoding. No peer, raw User-Agent or private path enters it.
- Status/job cache one/two seconds: measured capture/hit costs and mutation interval
  determine the lease, bounded by capture cost and observed writes. A nanosecond is
  the duration representation quantum for a cold injected clock. Terminal/release
  invalidation still takes precedence. Inspection and filtered views capture fresh.

These are synthetic laptop/fleet test mechanisms, not native hardware, real daemon
or production-load qualification. The consumer's acceptance checks and exact-attempt
custody rules remain independent.
