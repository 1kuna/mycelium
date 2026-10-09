# Independent final review of model sync

Date: 2026-10-09. Reviewed commit: `6cdfa53d398e1ba22420152b0ba1b07d90ea997d`,
with its cache/pin dependencies in `365d1a5`. Fresh review on `v2-prod-ready`,
created from clean `main` at the reviewed commit. Read `AGENTS.md` and
`docs/design-proposal.md` first; the latter remains the sole product spec.

## Confirmed bugs fixed

1. **Cached reuse bypassed the cache limit.** A worker restarted with a smaller
   allowance could load a verified cached model while its protected neighbors
   left the total over limit. Reuse now follows the same capacity/eviction gate
   as copying. It refuses when protected data prevents fitting; otherwise it
   evicts only eligible copies into recoverable Trash.
   `TestCacheReuseEnforcesLoweredLimit` covers both outcomes.
2. **Status advertised reserved bytes as free.** `Report` counted written files
   but not the unfinished part of active copies. It now subtracts unwritten
   reservations from both free disk and remaining allowance. A concurrent map
   permits this without taking the admission lock or blocking check-ins behind
   NAS eviction. `TestCacheReportIncludesUnwrittenReservations` holds a copy
   before its first write and checks zero free allowance.
3. **Batch placement ignored loaded-versus-cached affinity across workers.**
   Both had a zero transfer estimate, letting the cached worker take a job even
   when an idle fitting worker already had its model loaded. Affinity now
   precedes speed comparisons, retaining explicit platform preferences.
   `TestBatchPlacementPrefersLoadedOverCached` covers the tie.
4. **Disconnected workers kept background copies running.** With no queue
   connection, a worker could keep using NAS bandwidth without receiving new
   priority decisions. Failed queue interactions now cancel background copies
   without waiting on filesystem calls, dropping cache pins, or deleting partial
   bytes. The full-system copy fault test proves pause and verified resumption.
5. **A corrupt protected file retained its verified inventory flag.** Rechecking
   a same-size corrupt file correctly refused loading but returned before
   removing the stale flag. It now stops advertising that file as verified,
   preserving the protected file for diagnosis.
   `TestCorruptProtectedCopyStopsAdvertisingVerifiedCache` covers this. This was
   a reporting defect; the foreground checksum gate already rejected the load.

Initial corruption/oversize cleanup also uses the caller's cancellation context,
consistent with other eviction paths.

## Safety and scope reviewed

Existing size/SHA checks, bounded writes, prefix rehashing, fsync and atomic
rename remain in place. A fake engine auditing the exact file handed to `Load`
verifies the full local path, byte count and hash independently of status.
Partial and bad-master files never reach it. Existing regressions cover pinned,
loaded, more-used and active-copy protection, bounded parallel reservations,
free-disk refusal, restart prefixes and oversized masters. These pass with race
checking. Reuse-limit tests preserve protected data and verify eligible eviction.

No peer network, leases, registry, installer or new scheduling service was added.
Two package-private worker test seams expose the existing chunk boundary and
engine boundary for deterministic integration evidence. CLI/API behavior stays
small; no broad refactor or optional architecture cleanup was needed.

## Fault and verification evidence

`TestPreDeployFullSystem` passed in the uncached full race suite: actual HTTP,
SQLite, processes/signals, spools and NAS aliases exercise priority, restart,
network, NAS, migration and planned leave. Its terminal ledger, job-ID count,
attempt identities and single final log markers check retained results.

`TestPreDeployModelCopyFaults` adds queue restart mid-copy, worker partition
mid-copy, production arrival during pre-copy, eviction pressure beside a pin,
byte-prefix retention/resume, post-idle pin readiness, verified cached model
calls and corrupt-master rejection. The pin loads once throughout those faults;
the production batch starts/completes once. The new scenario passed three
consecutive race-enabled repetitions and then the final full race suite.

Final checks passed: `go build ./...`, `go vet ./...`,
`go test ./... -race -count=1`, and cgo-disabled Linux amd64/arm64 builds of all
packages. Python 3.13.11 with PyYAML 6.0.3 and OpenAI 3.26.1 passed all 9 tests,
zero skips, after the final code changes.

No SSH, fleet installations, real GPU/model execution, LibreOffice or push.
Cancellation is cooperative between chunks; an outstanding NAS syscall obeys
its mount timeout. Tests use a deterministic chunk gate and do not qualify real
mount timeout behavior. Partitioned command copies can overlap until they
reconnect: the approved reconciliation contract does not provide exactly-once
external effects. Hardware acceptance remains the founder checklist.
