# Queue-host worker bulk-write pacing

`myce install worker --pace-queue-io --config WORKER_CONFIG --dry-run` previews
an opt-in Linux worker policy. Use the normal release/installer workflow to
activate it; this option changes no cgroup, scheduler or system service policy.
Omitted settings survive upgrades. Explicit `--pace-queue-io=false` clears it.
It is not enabled by default pending native latency qualification (#527).

The worker resolves its authenticated queue endpoint against local interface
addresses. Only a local Linux endpoint enables pacing. Remote workers and macOS
keep their existing behavior; the installer rejects this option on macOS or a
queue service. Multiple local disks are not distinguished: all this worker's
owned bulk copies share one controller. An unresolved/proxied endpoint cannot
establish queue co-location. No token, database or spool is inspected to detect it.

The controller learns the mean and standard deviation of successful idle
check-in round trips. During bulk work, an allowance starts at one normal Go
copy buffer (32 KiB), then grows by the consumed allowance only after a check-in
within the observed idle mean plus one standard deviation. Slower replies reduce
the next allowance in proportion to baseline/observed latency. One copy buffer
per successful check-in preserves progress. This buffer bounds bootstrap work;
it is not a byte-per-second setting. The resulting throughput follows the
installation's control latency and heartbeat cadence. No fixed rate, device
bandwidth guess or fleet-tuned latency threshold is used.

Bulk writes pause when a check-in is failing, when its in-flight duration exceeds
the baseline, or when the current shared allowance is consumed. Successful
check-ins release the next allowance. Idle baseline statistics exclude windows
with writes, waiting writers or an active copy/sync, so bulk pressure cannot
teach itself to be healthy. Unknown/zero-resolution timing remains unknown.
Heartbeat/control-file writes never wait on this controller. Deliberate pacing
waits renew the existing staging-progress signal and remain cancellable; a
blocked source read, write or sync gets no synthetic progress.

Coverage includes shared-input and declared-model staging, archive extraction,
stored input downloads, private cache-to-attempt copies, NAS job-input copies,
and model-cache materialization/recovery copies. Clones that share blocks do not
consume byte-write allowances. This does not control arbitrary payload writes,
external package-manager/compiler writes, or another worker process's allowance.
It does not prioritize queue blob writes or remove queue write amplification.
Pacing on a remote worker cannot protect the queue's input-download reads and
is deliberately not inferred from network latency alone.

## Sync and page-cache choices

Existing per-file sync, directory durability, hash verification and atomic
publication remain in force. `fdatasync` still flushes file data and metadata
needed to retrieve it, including file size for a growing staged file. Replacing
`fsync` with it does not establish a lower data-write cost or avoid dirty-page
reclaim. More frequent sync batches can bound dirty data, but also add flushes
competing with SQLite's FULL-synchronous commits. Native comparison must measure
that tradeoff; no new batch size or sync policy is inferred from the incident's
ten-minute device averages.

`O_DIRECT` adds filesystem/device-specific buffer, size and offset alignment
requirements and may fail or fall back depending on the filesystem. Staged bytes
are immediately read for verification and then consumed by jobs: bypassing their
page cache can add another physical read. The retained incident does not isolate
dirty-page reclaim as the causal stall, so direct I/O is not adopted without a
matched native measurement. Pacing is the independent control being qualified.
See Linux `fsync(2)` and `open(2)` (`O_DIRECT`) for these syscall guarantees.

## Evidence and remaining qualification

`TestWorkerBulkIOBackoffAndRecovery` and the cancellation/shared-allowance tests
use an injected clock. `TestWorkerBulkIOSharedStagingControlFeedback` uses real
HTTP, SQLite, stored-file transfer, shared-cache staging and hash verification;
its induced control delays use fake time. This proves feedback and data/custody
behavior on a laptop, not reproduction of NVMe saturation or a native deadline
guarantee.

Native acceptance must compare the same bounded workload with pacing disabled
and enabled on the queue host after independent reservations and consumer work
have physically released. Record persist/commit duration, worker check-in
latency, PSI I/O, device await, throughput and losses in both phases. Measure
latency before calling a policy protective. If the unchanged check-in deadline
is still missed, report that result and retain the standard delegated `io`
controller/`io.max` path with the host-configuration owner. Do not add an
unmeasured default or change live fleet units/cgroups by hand.
