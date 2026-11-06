# Adaptive cache accounting (#805)

Automatic byte limits stay zero: each admission remeasures its filesystem.
Explicit positive caps and project quota contracts remain operator intent.
Inventories and allocation scans run outside the queue mutex.
Saved model/environment ceilings encode automatic mode as the signed integer maximum
solely because the rollback release's strict reader rejects zero; current
readers restore automatic mode before admission. This representation sentinel
does not select a physical cache size. Rollback retains the older disk policy.

Cooperating queue and worker processes under the same account share pending
blocks through local kernel-locked records keyed by filesystem identity. A
writer retains its claim until it joins. Queue exit closes handles while accepted
upload commitments remain durable until publication or fenced reclamation.
Builder sessions are recorded before their payload gate opens; a parent crash
retains exploration until those sessions have no live members. There are no
timed leases or payloads in this accounting area. Inactive scalar
slots are reused. Other accounts and unrelated programs remain external writers.

Owned allocation changes use one inode union across writer paths, independently
of asynchronously refreshed cache inventories. Pending commitments stay charged
in full through completion, so overlapping paths cannot discharge another
writer's promise. This deliberately withholds already-written committed bytes
while a transfer remains active; completing the transfer restores that capacity.
Stable kernel-lock inodes are separate from atomically published, synced scalar
records with a second recovery copy. Release failures retain explicit retry
ownership in idle maintenance. External loss is the remaining
physical free-space decline. Its response horizon follows the latest completed sampling interval;
admission waits and long transfers do not become control-response deadlines. Demands make cold capacity reclaimable across cache kinds;
collection never creates authority to delete a protected or unknown entry.

Retired numerical assumptions:

- Environment free/4, 4 GiB minimum, 80 GiB ceiling, 20 GiB reserve and 24 GiB
  startup minimum: physical available allocations, shared commitments and trend.
- Model/model-snapshot and queue-store 100 GiB defaults: the same filesystem
  accounting. Authoritative store bytes retain their custody/retention contract.
- Download-cache 16 GiB: measured capacity, with standard package-manager pruning.
- Disk guard 1%/256 MiB: allocation rounding, protocol metadata, active control
  working sets and measured consumption. The scheduler's sector sanity check is
  followed by local write admission; it is not a control-space guarantee.
- Environment 24-hour minimum: observed inter-use and held-use intervals, plus
  existing use/build/reference protection. Unused entries acquire no fixed TTL.
- Copy parallelism 1: an initial stream measures the path; waiting demand can
  add one experimental stream. Each epoch spans the previous measured window
  and a contribution from every active stream. Keep the increment only when
  aggregate throughput improves without degrading measured metadata latency.
  Later completed epochs also retract established concurrency on control slowdown.
  Worker cache kinds share the volume controller; distinct processes independently
  observe contention while sharing physical capacity claims.
- The authored `max_added_file_bytes_24h` quota remains an explicit, disabled-by-
  default protocol contract, not an automatic cache policy.

Deletion requires a reproducible inactive entry. Shared inputs require a valid
source manifest, unchanged verification evidence, exact payload verification and
an independently verifiable source. File/model copies also retain unavailable
originals. This strengthens the earlier #720 policy: an expired original no
longer authorizes deleting its last verified copy. Malformed/legacy shared
records and unverified partials remain protected. Healthy admissions inspect
capacity before hashing selected eviction candidates and their originals. Pins, loaded models, readers, builders, queue
references and exact-attempt custody remain binding.

Unknown native installers explore alone on their destination volume: their
claim withholds the remaining available blocks from sibling writers. Declared
asset transfers finish first, under their own reservations. Installer/package
writes use the copy controller and existing pressure monitor. An unconfirmed
helper retains capacity and I/O custody until exact process-release evidence
allows maintenance to discharge it. This is admission coordination, not an OS
quota for an arbitrary executable.

Remaining bounds, basis and cost:

- The 512-byte allocation-counter sector and probed filesystem allocation unit
  prevent logical bytes from being treated as packed physical storage. Separate
  files and directory ancestors are charged separately. Tree manifests and
  archive inventories refine allocation demand before extraction; actual encoded
  publication/upload metadata is charged before writing it. This can withhold
  capacity on filesystems that pack, compress or clone small objects.
- Encoded scalar record sizes determine control allocations: two synced copies
  and one atomic-publication temporary for each state/owner record, their stable
  lock inodes and the parent directory. Per-copy metadata accounts for ready/
  verification records and publication directories. They prevent known metadata
  from consuming the payload's last reserved blocks, at the cost of withholding
  physical allocations for recoverable accounting.
- SQLite's 32 KiB WAL-index region, 32-byte WAL header, 24-byte frame header and
  database page geometry supply cold control evidence. Active WAL/SHM allocation
  and observed control growth replace that bootstrap when larger. They retain
  room for comparable control activity, at the cost of withheld cache capacity;
  they are not a bound on all future transactions or external writes.
- One initial stream and one added stream are the smallest nonzero measurement
  and concurrency experiment. Two metadata observations are the minimum pair
  that measures latency variation. The measured adjacent variation supplies
  tolerance; no universal rate, latency duration or automatic maximum is used.
- Full hashes, verified byte lengths, archive expansion declarations, explicit
  user caps, integer saturation and ownership fences retain their protocol,
  representation or physical purposes. Admission waits remain cancellable and
  are distinct from an active source's progress watchdog.

Fake-clock/fake-volume regressions cover tiny and multi-terabyte disks, separate
ledgers, interrupted owners, completed writes with stale inventories, shrinking
free space and recovery, hot sets above retired defaults, cross-cache demand,
small-file allocations, slow/scalable streams, established-control degradation,
admission waits, durable upload/helper custody, overlapping inodes, interrupted
record publication/release retries, and unknown partial preservation. These fixtures do not qualify native disk responsiveness, mount behavior,
OS write isolation or power-loss durability. Fleet gate acceptance is recorded
separately in the pull request; no deployment or real-host collection is part of
this change.
