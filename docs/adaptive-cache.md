# Adaptive cache accounting (#805)

The worker shares a ledger by probed filesystem identity, rather than dividing
free disk independently among cache kinds. Cache inventories and filesystem
probes run outside the queue mutex. The ledger only serializes scalar accounting;
existing cache use/build/publication locks still fence actual data.

Retired assumptions and their replacements:

- Free/4, 4 GiB minimum, 80 GiB ceiling, 20 GiB reserve and 24 GiB startup minimum:
  actual available blocks, shared pending writes and external-consumption trend.
  Small disks initialize without an invented minimum; larger reused sets borrow
  beyond old ceilings. Explicit caps still apply.
- Model and model-snapshot 100 GiB defaults: the same shared volume ledger.
- Queue-store 100 GiB default: physical headroom plus existing tus and output
  commitments. Authoritative data keeps its existing custody/retention contract.
- Download-cache 16 GiB default: a separate demand domain on the same volume;
  standard package-manager pruning preserves installed hardlinks.
- Disk guard 1%/256 MiB: filesystem Bavail, an OS-page control-write bootstrap,
  and observed consumption over sampling/writer response intervals. Scheduling
  also accounts for the worker's reported free-space trend and sample age.
- Environment 24-hour minimum: actual inter-use/held-use observations and existing
  active locks. An unused entry has no presumed reuse period. Restart loses
  speculative reuse estimates; persisted use locks/pins/reference custody remain.
- Model-copy parallelism 1: one-stream measurement followed by an additional
  stream experiment when saturated; aggregate throughput decides whether to keep
  that increment. All worker cache kinds share the destination controller.
- The 24-hour project quota window remains an explicitly named authored protocol
  (`max_added_file_bytes_24h`), disabled by default. It is user intent, not an
  automatically imposed cache-size or traffic assumption.

Remaining bounds and costs:

- One OS page prevents promising every last available block before control-write
  evidence exists. It withholds one physical memory-page allocation, not a
  guessed installation reserve. Filesystem-reserved blocks already stay outside
  Bavail. Unknown external writers can still exhaust disk between samples.
- Pending copies are charged in full until their writers join. This prevents
  overlapping writers promising identical blocks, at the cost of double counting
  their already-written prefixes. Durable per-entry reservations retain their
  existing restart fences.
- Consumption is projected over the longest measured sampling/writer duration.
  This leaves time to observe/respond to another similar loss, at the cost of
  conservative headroom after an unusually slow writer. Recovery clears a
  declining trend; it does not create deletion authority.
- One initial copy stream is the minimum nonzero experiment. One extra stream
  bounds each experiment's disturbance. Explicit concurrency settings remain
  binding; no unmeasured automatic maximum is added.
- Verified sizes, expanded archive bounds, hashes, integer saturation and use,
  pin, authoritative-reference and exact-attempt fences retain their physical
  or protocol purposes. None is traded away to achieve a budget target.

Fake-clock and fake-volume tests cover small/multi-terabyte disks, shared pending
allocations, decline/recovery, a hot set above old defaults, cold-domain ordering,
explicit caps and cancellation, and saturated/scalable copy paths. Existing
cache/attempt tests cover pins, held use, corrupt bytes and restart custody.
These tests do not qualify native storage latency, OS responsiveness or hardware
power-loss behavior. No deployment or real-host collection is part of this change.
