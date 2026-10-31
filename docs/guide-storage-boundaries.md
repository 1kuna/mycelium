# Backed storage admission integration

This implements laptop-testable parts of [#506](https://github.com/1kuna/mycelium/issues/506)
against [revision 3](design-learned-storage.md). It does **not** activate automatic
storage admission, qualify a native backend, or replace existing cap/reserve
settings. There is no installation/CLI switch. Default jobs retain their existing
contracts and the physical-storage status remains shadow evidence.

`Queue.ConfigureStorageProtection` and `Worker.StorageBoundary` are installer/runtime
integration seams. Production implementations of those interfaces remain necessary
before native qualification. Fake implementations prove accounting and lifecycle
behavior; their verification flags are not native qualification evidence.

## Control capacity and availability backing

`MeasureControlStorage` counts allocated blocks and inodes across the concurrently
live SQLite/WAL, log, sync staging, checkpoint copy and backend metadata working
sets of a control-operation rehearsal. Hardlinks and aliased roots count once.
Sparse logical lengths, the old percentage/fixed floor and historical bursts do
not supply headroom. Missing categories, inaccessible files and cancelled
measurements grant no measurement. Local paths never enter persisted status.

The installer must provide **the complete admitted control operation's working
set, including planned expansion**, rather than only the idle database. The
measurement is not a predictor or bound on arbitrary future queue/log growth.
New control workloads require a new rehearsal and backing; production control
profiling/provisioning remains part of backend integration.

The backend funds complete control and worker backing before observing shared
free space. Accepted availability mechanisms are independent storage, fully
allocated bounded backing and native reserves. Capacity includes measured backend
overhead. A project quota alone, sparse maximum, unverified/unconfined root or
unrelated unbounded writer cannot qualify this path. The backend must reconstruct
all existing data and boundaries before reporting usable budget capacity; unknown
or legacy custody cannot be silently treated as free capacity.

Canonical capacity-pool and backing identities deduplicate aliases, including
volumes sharing an APFS container. Backends call
`ObserveStorageBackingIdentity(root)` outside `q.mu` and supply its SHA-256
fingerprint as `native_identity`. It uses one open directory descriptor and
unprivileged native APIs:

| Platform | Identity fields | Observation |
| --- | --- | --- |
| macOS / APFS | Volume UUID | `fgetattrlist` with `ATTR_VOL_UUID` |
| Linux | Filesystem ID plus device | `fstatfs.f_fsid` and `fstat.st_dev` |

macOS `f_fsid` alone was reused when distinct APFS images occupied the same path
in native testing, so it does not provide the required replacement fence here.
Configured paths, directory inodes, mount-incarnation tokens, usage, free bytes
and unused reservations do not participate in identity. Reattaching the **same
volume** is valid; a **different volume** at the path must be fenced. This is the
narrow #506 identity requirement, not detection of every mount event.

Refresh/restart compares volume identity and installed capacity independently of
fresh funding/overhead/reservation observations, which are validated each time.
Filling and emptying an unchanged reserve preserve retained allocations and
production publication. Missing/unavailable native identity grants no protection
verification. The observer identifies a volume, not quota, confinement or
availability enforcement.
Allocated backing already reduced `statfs`
free space. Only unused native reservations still included in that free reading
are subtracted, once. Fresh file-store inventory/tus lengths, configured caps,
production reserve and the old disk floor continue to restrict actual uploads.
No capacity increase is activated by these seams.

## Attempt admission and worker custody

New protected submissions explicitly request `storage_protocol: worker-custody/1`.
Older/frozen policies are held in a configured protected pool rather than being
rewritten. A request for this protocol holds when no backend is configured.
This protocol is an integration contract; supported clients and final destination
results still need [#508](https://github.com/1kuna/mycelium/issues/508).

The scheduler persists exact execution/machine/session/backing byte and inode
allocations in the existing SQLite queue. Comparable accepted envelopes supply
limits without rounding up a zero-error four-unit prediction. Unknown demand uses
the backend's bounded exploration. After a constrained miss, a new attempt must
acquire a larger exploration independently; the accepted prediction is unchanged.
Admission is implemented here, rather than deferred to #507.

Each worker has independent experiment-custody and production-staging backing.
Retained experiment data cannot consume the production staging budget. Filling a
worker's custody budget holds its new experiments while another eligible worker
can continue. Restart preserves every allocation and custody manifest, and grants
zero new protected allowance until backing is verified again. Missing workers do
not release their retained bytes.

Protected worker attempts use separate directories. The runtime boundary must
install/verify aggregate byte/inode denial and confinement **before preparation**,
covering main and sidecar workspace/temp/output, immutable spool duplication,
checkpoint/publication and worker metadata/index writes. Shared inputs/caches are
read-only. Equivalent confinement is required for frozen unsandboxed policies;
omission of the kernel sandbox cannot authorize escaping the storage boundary.
A missing backend refuses launch. Workers must advertise `bounded_storage_v1`;
older workers cannot silently ignore a protected assignment. The assignment
records admitted bytes separately; `hard_limit` stays absent until installed
enforcement can be proved. Native denial observations classify contained
ENOSPC/EDQUOT as `disk_limit_exceeded`; ordinary host ENOSPC remains `storage_full`.

Experiment results retain the existing synced immutable worker spool even with a
healthy destination. `/api/workers/storage-custody` records a versioned exact
manifest receipt; replay is idempotent, conflicts/stale sessions fail. No checkpoint,
diagnostic or unique output is uploaded into shared storage by this path. A local
receipt grants neither task success, ordinary `output_refs`, nor NAS durability.
Partial/failure progress remains local and charged too. Root discovery records
and a scan of synced protected child state recover a crash before indexing.

Allocations conservatively retain their **entire** installed budget, even after
compute release/failure. Nothing recycles it or removes protected directories.
That costs placement capacity until #508 supplies verified delivery and legal
custody release. This deliberately avoids a sampled-usage release of unique data.

## Proof and remaining work

Focused tests use fake Clock, HTTP, real SQLite/WAL and small local files. They
cover control measurement/backing, pool aliases, denial semantics, worker budget
isolation, exact custody replay/conflicts, lost indexing, restart and an unchanged
four-unit miss followed by fresh six-unit admission beside 96 retained worker
units and 100 shared units. Existing #513 cap/reserve/tus accounting stays active.

`TestStorageProtectionNativeBoundedVolume` additionally accepts explicitly prepared
`MYCE_TEST_BOUNDED_VOLUME_ROOT` and `MYCE_TEST_BOUNDED_VOLUME_BACKING`. It requires a
small disposable filesystem with a fully allocated backing file. It fills that
filesystem to real ENOSPC while independent HTTP publication and SQLite WAL
checkpointing progress. This is bounded-volume evidence only: it does not qualify
per-attempt quotas, arbitrary process/sidecar confinement, inode denial, unrelated
OS writers, or any fleet filesystem. The ordinary gate skips this opt-in check.

Still required: production native backend implementations, complete operation
rehearsal/provisioning, block/inode/confinement qualification on each host, #508
source-local destination delivery/client contracts and verified release, and #509
legacy-custody drain/migration/activation. No fleet, quota/mount provisioning on
fleet hosts, service change, merge or deployment is part of this implementation.

### Native identity replay

The opt-in platform tests use two disposable filesystems at one mountpoint. They
fill the first to real ENOSPC, empty it, reattach it across SQLite restart, swap
in the second, then restore the first. Same-volume observations keep verification
and publication capacity; a different volume fails closed without dropping
retained allocations. macOS provisions APFS with `hdiutil` without root. Linux
uses disposable ext4 loop images only when loop devices and mount permissions
are already available; it never invokes sudo or changes host configuration.

```sh
mkdir -p .reports/storage-identity
MYCE_TEST_STORAGE_IDENTITY_NATIVE=1 \
MYCE_TEST_STORAGE_IDENTITY_SCRATCH="$PWD/.reports/storage-identity" \
go test -race ./internal/myce -run '^TestStorageIdentity' -count=1 -v
```

Ordinary gates skip native image/loop provisioning. Linux unit tests check actual
`f_fsid`/`st_dev` extraction and usage independence without loop devices. A skipped
loop test supplies no native ext4 qualification; XFS volume replacement, Linux
loop remount/device assignment, and all native quota/inode/confinement guarantees
remain separately unqualified. The APFS identity replay likewise does not qualify
quota enforcement or complete control backing. Provisioning tests remove their
owned images after detach, and retain scratch if detach cannot be verified.
