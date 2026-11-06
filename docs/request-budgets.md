# Capacity-aware API budgets (#804, next release)

Budgets use the queue host's current memory and disk headroom, the requested
payload's representation, measured control/projection cost, and negotiated peer
capacity. They do not use a fleet-size or inventory-size constant. The same
legacy full inventory shape can grow past both historical check-in ceilings.
Inventory hashing, validation, decoding, disk observations and page measurement
run outside the queue mutex. Existing inventory hash/version acknowledgements
still determine when a full refresh is necessary.

## Retired assumptions

| Previous assumption | Replacement and failure prevented | Cost |
| --- | --- | --- |
| Ordinary/extension/check-in JSON: 1/2/8 MiB | OS available RAM, container ancestor headroom and `GOMEMLIMIT`, minus measured GC growth and stack reserve; runnable CPUs plus a control lane share the pool. Wire buffers and typed map/slice/struct allocations are charged before decode. Prevents one body or simultaneous bodies exhausting queue memory. | Large requests can receive 413 under actual memory contention. |
| Wait: 256 IDs, 20 seconds | Scalar response storage and current headroom bound ID count. Nonnegative representable caller seconds, HTTP cancellation/deadline and negotiated transport deadline bound duration; measured check-in service cost leaves delivery time. | Large batches split at the advertised capacity; transport expiry does not cancel jobs. |
| Status: default 100 / maximum 1,000; interval and ledger maximum 256 | Go representation size and measured serialized row size determine memory cost. Status history additionally uses measured projection time per row against local check-in latency. Before timings exist it probes one history row per runnable lane. Positive explicit page lengths are upper bounds; cursors retain coverage. | Page length changes with pressure and cost; clients must follow returned cursors. Active/custody visibility and explicit full detail retain their existing meaning. |
| Usage: maximum 90 days | Positive representable caller duration and request cancellation. | An explicitly broad report may do more work. |
| Extension source: 1 MiB | Current host memory shared among source, clone, YAML/JSON, base64 and fingerprint representations. Folder reads enforce the remaining allowance even if a file grows after stat. | Source registration can refuse under memory pressure; large assets should use immutable file references. |
| Resolve NAS: 256 pending, 8 checks/heartbeat, 1 minute | Pending metadata uses measured queue headroom; freshness expires at the existing worker session freshness fence. One check per heartbeat matches the worker's single outstanding syscall slot. | Cold/stale evidence remains unknown. This is existence advice, never byte verification or launch authority. |
| Submission input verification: 30 seconds | Caller cancellation/deadline, retaining the existing single I/O slot until an uninterruptible syscall returns. | A caller without a deadline may wait for storage recovery; no additional I/O goroutines accumulate behind a stuck syscall. |
| Generic model body: 8 GiB; shared spool: 16 GiB | Local free disk minus unwritten commitments and a measured current control-file working set for WAL/DB growth. Written bytes already reduce free disk and are not charged twice. Positive operator budgets remain explicit intent. `--upload-spool-bytes=-1` selects automatic capacity. | Competing disk writers can change capacity during upload; writes still report filesystem failures. This is local admission, not a filesystem quota against unrelated processes. |
| OpenAI JSON body: 64 MiB | The same typed memory admission as other JSON bodies. | Requests exceeding available memory receive a typed size refusal. |

Six wire representations cover growing input and decoder buffers (up to two
copies each), token strings and decoded strings. Typed preflight adds Go object
sizes, spare slice capacity and map key/value/control storage, including sparse
objects with large zero-valued fields. Extension processing allows eight
coexisting source/transport representations. These are representation accounting
bounds, not deployment tuning. Page staging owns detached state, projection and
encoded output. The OS page size controls read chunks; it is not a body limit.
Duration and stream-length maxima only prevent integer overflow. Existing JSON
syntax/depth and hash widths remain protocol representation boundaries.

## Peer negotiation and refusals

The queue advertises `X-Myce-Request-Budgets: capacity-v1` and
`X-Myce-Max-Body-Bytes`. This is the current wire allowance; typed allocation
expansion can require a smaller body. HTTP 413 `request_too_large` explains the
memory cause and fitting wire bytes, rather than asking workers to upgrade.
Chunked bodies use the same accounting. Unavailable memory measurements return
503 `request_capacity_unavailable` with fitting bytes zero. Spool refusal names
required and fitting bytes; abandoned uploads release commitments only after
local file removal succeeds.

A supporting peer sends `X-Myce-Request-Budgets: capacity-v1` and may advertise
`X-Myce-Transport-Deadline` as an absolute RFC3339 timestamp for waits, and
`X-Myce-Max-Response-Bytes` as a positive page-planning allowance. Unknown or
absent protocols keep legacy semantics; no additive request JSON fields are
required. Clock synchronization is needed for an absolute peer deadline; an
HTTP request context deadline is always an upper bound. Wait replies also give
`X-Myce-Max-Wait-IDs`; physical-capacity refusals name the fitting count.

Request memory charges are ephemeral and released after each handler, including
failed decoding and cancellation. Restart measures fresh capacity; no learned
budget can bypass worker sessions, inventory versions, input verification,
submission keys or exact-attempt release fences. No deployment/configuration
migration is required. Existing saved positive spool limits remain operator
intent until explicitly changed.

The laptop regressions inject different queue CPU/RAM capacities for two-host
and 128-host inventories, cross 1 MiB and 8 MiB, refuse hostile known/chunked
bodies and sparse decoded expansion, cover concurrent accounting/recovery,
legacy and negotiated peers, injected-clock wait expiry, larger batch reads,
projection cost changes and disk commitments above 8 GiB. These are synthetic
API proofs, not native fleet capacity or filesystem-quota qualification.
