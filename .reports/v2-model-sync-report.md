# Mycelium v2 model sync implementation and independent review

Base: `365d1a552c03d57c7190d079d8a0f514fcee9665` (main).
Branch: `v2-model-sync`, created from main. Read `AGENTS.md`, the approved design,
the prior implementation report, and the affected source/tests before editing.
The current task authorizes the pre-copy extension recorded in the design.

## Review of 365d1a5

Real findings and fixes:

1. A fresh worker session unconditionally charged an infrastructure loss to an
   execution already stopping for intentional preemption. A non-resumable job
   with two prior losses could therefore fail rather than return to the queue
   if restart happened between production submission and stop reconciliation.
   Fresh-session and sudden-loss reconciliation now charge only running work;
   intentionally stopped copies are also not eligible to reclaim a later
   assignment. `TestPreemptedNonresumableRestartDoesNotChargeLoss` reproduces
   the production/restart ordering and verifies stale completion rejection.
   The same fresh-session path also requeued a job with cancellation pending;
   it now completes cancellation instead.
   `TestCancellationSurvivesFreshWorkerRestart` verifies this ordering.
2. Cache inventory accepted a matching filename and size without verifying its
   bytes, so status and scheduling could advertise a same-size corrupt file as
   cached. Engine startup did independently verify it; this was a false warm
   cache claim, not an observed unverified engine load. Startup inventory now
   verifies complete files; publication records verification in the worker
   session. A same-size corrupt file is not advertised, and Prepare still
   verifies before returning a local path. Covered by
   `TestCacheDoesNotTrustUnverifiedSameSizeCopy`.
3. The old copy loop wrote an entire buffer before checking the declared size.
   An oversized master could temporarily exceed its disk/cache allowance by a
   chunk despite eventual checksum failure. The copy now checks the boundary
   before writing, with `TestOversizedMasterNeverExceedsCopyReservation`.
4. Pin cache protection depended on a loaded/loading runtime entry. That was
   insufficient for the new standby caches and inactive reservation windows.
   Queue replies now protect every pin landing worker and active batch model,
   in addition to the existing resident/loading/draining protection. Pin
   standby, active batch, and eviction protection have regression coverage.

Reviewed without finding another reproducible defect: copies were already
serialized in the base commit, complete files were verified before engine load,
resident/loading/draining copies were excluded from eviction, pins prevented
idle and priority unloading, and attempt/session checks rejected stale results.
Existing fake-clock preemption/front-of-line tests and the actual command dev
suite continue to pass. An intentional resumable stop may exit successfully
while still needing resume, so a zero exit code alone was not changed into a
completed-job decision. No speculative scheduler rewrite was made.

The existing cross-filesystem Trash move retains a verified recovery backup
and the moved original. This is conservative and costs extra NAS I/O/storage;
it was retained rather than changing the recovery guarantee. Context-aware
copy/hash/move operations now let background eviction respond to cancellation.
No peer network, lease system, second queue, installation layer, or dependency
was introduced.

## Implementation

- `myce model sync MODEL --to MACHINE|PLATFORM|all`, and authenticated
  `POST /api/models/sync`, persist idempotent model/target requests in SQLite.
  `--cancel` suppresses that target's idle copying and keeps partial bytes;
  repeating the request resumes it. Pins override suppression. Machine targets
  must have joined once; platform/all requests include future workers.
- Pins warm and protect every compatible landing cache, even outside a time
  window. GPU memory and engine loading remain assigned to one worker.
- Idle plans use queued demand and recent model-use counts; counts expire after
  24 hours of no use. Pins go first, then demand count, then stable model name.
  Real batches, model loads and in-flight calls cancel background copying.
  The worker also cancels copying directly when an inference HTTP request arrives,
  without waiting for the next heartbeat.
- Partial files survive interruption/restart. Resume rehashes the durable local
  prefix, seeks the NAS source to its byte offset, and verifies the complete
  size/SHA-256 before sync and atomic rename. Corrupt/oversized prefixes or
  masters fail visibly; corrupt partials move to recoverable Trash.
- `--model-copy-parallel` defaults to one per worker. Higher configured values
  actually run independent copies together. Per-model admission prevents two
  writers to one file; full-size reservations prevent concurrent capacity and
  filesystem-space overspending. Disk admission conservatively reserves other
  active models in full. Partial files count toward the cache limit.
- Background eviction cannot displace pinned, loaded/loading/draining, active
  batch, active-copy or more-used entries. Protection/popularity are checked
  again before eviction. Admission/eviction errors appear in copy reports.
- Workers measure achieved bytes/s, including resumed transfers, report per-copy
  and last machine throughput, and persist the machine measurement locally.
  Queue placement uses remaining bytes / measured throughput, with a documented
  40 MB/s estimate until measurement. Cached/loaded models have zero NAS transfer
  cost. Engine startup and inference are separate costs.
- CLI, JSON status and the HTML page show persistent requests, copy progress,
  states/errors, speed and per-model estimated NAS copy seconds. Startup-verified
  complete files report ready; retained partials report paused.
- Operations, integration and production-test docs now require measuring live
  copy speed and free disk, setting bounded cache allowances, pre-copying every
  production model and checking verified caches before traffic. B70's historical
  roughly 100 GB free is not treated as room for a 100 GiB active cache.

## Verification

Final required checks, on the local laptop:

```text
$ go build ./...
exit 0 (no output)
$ go vet ./...
exit 0 (no output)
$ go test ./... -race
ok  mycelium/cmd/myce       18.243s
ok  mycelium/internal/myce  35.251s
$ PYTHONPATH=clients/python/src python3.13 -m unittest discover -s clients/python/tests -v
Ran 9 tests in 9.986s
OK (skipped=2)
$ git diff --check
exit 0 (no output)
```

The two Python skips are existing optional OpenAI/YAML extra tests; no packages
were installed. The executed Python dev test exercises real commands/API.

New laptop regressions cover target matching/cancellation/persistence, all pin
landing caches, most-needed idle copying, pause on real work, resume and measured
speed after interruption, corrupt prefixes and complete files, protected and
popular entries, cache/disk reservations under concurrency, actual parallel
copying, cached/speed-aware placement, recent long-job use, active batch cache
protection and the preempted non-resumable restart bug. CLI sync is tested against
the real API. A real Worker with fake GPU inventory copies a temporary NAS master
through HTTP scheduling, reports ready and measured throughput, and leaves cleanly.

During development, the old interrupted-copy cleanup expectation was updated to
assert the newly requested retained prefix. A fake catalog without byte metadata
exposed an affinity tie, which was corrected without weakening the existing
cached-placement test. Two new large-file tests initially inherited a 1000-byte
mock free-space budget; correcting their budgets resolved their failure/block.
The final full race suite passed after those fixes and additional regressions.

UI verification used the project-native T3 preview and a temporary local API
fixture with explicitly fake reports, not fleet observations. Inspected ready,
copying, paused, failed and empty-engine states at 1280x800 and 390x844, including
scrolling to requests and pins. At phone width, document scrollWidth was 390;
there was no horizontal page overflow. Screenshot evidence:

- Desktop: /Users/zach/.t3/userdata/browser-artifacts/browser-screenshot-127-0-0-1-mv0kepkv-8fba248e.png
- Phone bottom/errors: /Users/zach/.t3/userdata/browser-artifacts/browser-screenshot-127-0-0-1-mv0kf3u4-b63e1b11.png

The temporary preview process was stopped; its ignored fixture was not staged.
No real machine, SSH, install, LibreOffice, push or deployment was used.

## Limits

An outstanding NAS filesystem syscall obeys the OS mount's timeout. Cancellation
is checked between chunks and during hash/eviction operations; worker heartbeats
are not synchronously held behind that syscall. No actual fleet speed or 40 GB
production copy was measured in this task. Large-model engine readiness, NAS
failure behavior on real mounts and disk headroom need the documented future
hardware acceptance test. Cache eviction may later reclaim paused partials under
the same protection/popularity rules. Plain batch commands still choose their
own local model path; `needs.model` supplies placement/pre-copy metadata.

## DIGEST
Reviewed the prior commit and fixed restart, cancellation and cache safety gaps.
Added model sync, pin warmup, idle copying, cancellation and verified resume.
Cache limits and protected models survive concurrent copies.
Placement uses cached models, remaining bytes and measured copy speed.
Build, vet, Go race tests and Python tests pass; two optional Python tests skip.
Docs and the local status page are updated; fleet hardware was untouched.
