# Mycelium v2 unified-memory implementation

Date: 2026-10-08. Branch: `v2-unified-memory`, created from local `main`.
Read `AGENTS.md`, approved design, fleet-discovery evidence and the final-check
production gate before implementation. Work remained local; no SSH, installation,
LibreOffice, fleet changes or push.

## Implementation

- CUDA numeric `nvidia-smi memory.free` remains dedicated-memory telemetry.
  Single-device `N/A`, `[N/A]` (GB10), `Not Supported` and `[Not Supported]`
  select unified memory automatically. Multiple-device output, malformed data,
  and command errors still fail rather than guessing a source.
- Linux unified memory uses `/proc/meminfo` `MemAvailable` in kB, converted to
  GiB. Metal defaults to unified memory. macOS uses `vm_stat` without cgo,
  reading its actual page size and summing free + inactive pages. Active,
  wired, compressed and speculative counts are not added. macOS accounting is
  a conservative reclaimable estimate, not a guarantee that an allocation fits.
- Every live read applies an optional positive `--memory-gb` cap, subtracts
  `--reserve-memory-gb`, and clamps at zero. OS reads observe allocations outside
  the queue, including Atlas. A suggested initial Spark reserve is 16 GiB;
  operator admission and pressure measurements may require more.
- `--memory-mode auto|unified|dedicated|fixed` selects detection, an OS override,
  numeric CUDA-only telemetry, or an explicit positive fixed allowance.
  Fixed mode cannot observe outside allocations and needs independently enforced
  headroom. A cap alone never replaces failed telemetry. XPU/fake retain their
  configured allowance by default; XPU may explicitly select unified accounting.
- Probe errors advertise zero memory with a diagnostic persisted through the
  status API and displayed by `myce status` and the HTML status page. Batch and
  model admission refuse workers with that diagnostic. Subsequent check-ins
  retry telemetry and clear failures on recovery.
- Existing immediate command/model-load rechecks use the new source. Live
  unified readings use measured-memory accounting to avoid double subtraction
  of resident allocations; not-yet-resident loading reservations remain held.
- Updated operations, production test plan, CLI/model references and deployment
  guidance. Added `deploy/myce-spark-worker.service` with auto detection, a live
  uncapped reading, 16 GiB reserve, and a checkpoint-root placeholder.

## Verification

All tests use fake inventory/OS data or local fake workers; no GPU is required.
Coverage includes GB10 markers, changing Linux availability, 4 KiB/16 KiB Mac
pages, malformed/missing sources, caps/reserve, fixed and detection overrides,
failed-probe status and recovery, blocked model admission, and an OS-memory
drop between scheduling and command/model start. Existing dedicated-memory,
reservation and bounded-probe tests remain in the suite.

Passed:

- `go build ./...`
- `go vet ./...`
- `go test ./... -race`
- `PYTHONPATH=clients/python/src python3.13 -m unittest discover -s clients/python/tests -v`
  (9 tests: 7 passed, 2 optional tests skipped because `openai`/PyYAML are absent;
  no packages installed).
- `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/myce-unified-memory-arm64 ./cmd/myce`
- `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/myce-unified-memory-amd64 ./cmd/myce`
- `git diff --check`

The first Python attempt used the system Python 3.9 (below the documented 3.10+
requirement) while other checks ran and hit a socket timeout; the supported
Python 3.13 rerun passed. The new integration test initially closed its HTTP
server before worker cleanup; cleanup ordering was corrected and it passed.
A later full race run hit the existing orphan-recovery test
`TestWorkerRestartCleansOrphanBeforeReassignment` four-second leave timeout;
the test passed in isolation, and the full suite was rerun without concurrent
checks. These were local validation failures, not changes to the Python client
or the orphan-recovery implementation.

## Remaining production boundaries

No hardware execution or production readiness claim. This resolves the missing
unified-memory telemetry path in software. Real Spark pressure/headroom, Atlas
owner admission/guards, an installed GPU-enabled engine, reviewed model contract,
and NAS identity still require the separate authorized production test.

## DIGEST
Unified-memory accounting added for Spark CUDA and Metal, with reserves and overrides.
Probe failures block work and explain why in status; starts and loads recheck OS memory.
Docs and Spark example recommend an initial 16 GiB reserve.
Go build/vet/race and Linux arm64/amd64 cross-builds passed.
Python passed: 7 tests, 2 optional dependency skips; no fleet changes or push.
