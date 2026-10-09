# Mycelium v2 final pre-production review

Date: 2026-10-09. Branch: `v2-prod-ready`, created from clean main at
`6cdfa53d398e1ba22420152b0ba1b07d90ea997d`. Independent review; no prior
implementation authorship. Repository guidance and approved design read first.
All changes are local; no push or fleet access.

## Implementation review

Five confirmed issues fixed: cached reuse bypassing a lowered cache allowance,
free-byte telemetry omitting copy reservations, batch loaded/cached placement
ties, background copying during queue disconnection, and stale verified flags
on protected corrupt files. Regression tests accompany every fix. Full details
and boundaries: [model-sync report](v2-model-sync-report.md).

Cache admission still protects pinned/loaded/active/more-used models and uses
recoverable Trash. Full size/SHA verification precedes publication and engine
load. Foreground commands never wait for background copy cancellation. Queue
and scheduling decisions still use injected clocks. No fleet token was read,
printed or staged; engine/job token stripping is unchanged.

## Full-system fault acceptance

Both full-system scenarios pass with race checking and uncached execution.
The original test retains actual HTTP/SQLite/worker/process/spool behavior while
covering whole-network loss, isolated loss/reconciliation, queue restart, NAS
outage, preemption and leave across fake CUDA/XPU/Metal inventories.

The extension covers an actual queue/database restart during a held partial
copy, queue-network loss during another copy, and production arrival during a
third copy. It asserts preserved sync/pin state, durable one-MiB prefixes,
verified resume/publication, no partial cache advertisement, one continuously
loaded pin, one production job/attempt/start/final marker, and rejection of a
bad-hash master before engine load. An audited fake engine independently checks
every actual local file it receives. Three repeated race runs additionally
checked stability of the deterministic fault gates.

These are laptop acceptance tests. Command copies may overlap during a partition
until connectivity permits reconciliation; authoritative completion/logs are
not duplicated, but external side effects must remain retry-safe. Real GPU
execution, kernel NAS timeouts, power-cut durability and real mount identity
still need the founder's hardware test.

## Python acceptance

Created `.venv-prod-ready-py313` using Python 3.13.11; installed the client in
editable mode with both `[yaml,openai]` extras. Observed PyYAML 6.0.3 and OpenAI
3.26.1. Final invocation:

```sh
.venv-prod-ready-py313/bin/python -m unittest discover -s clients/python/tests -v
```

Result: **9 tests passed, zero skips**, in 14.712 seconds. YAML submission and
standard SDK embeddings/chat/streaming all execute. The client README documents
the fresh virtualenv procedure and makes zero skips the full acceptance gate.
The default macOS `python3` is 3.9.6 and is unsuitable for this >=3.10 package;
the successful environment uses the already installed 3.13 interpreter. Local
virtualenvs/egg metadata are ignored and not staged; no system packages installed.

## Documentation consistency

Reviewed README, integrating, job-reference, operations, production-test-plan,
guide-model-calls and guide-resumable-jobs against current code and the approved
design. Corrected stale Metal/fixed-memory descriptions, stop-grace wording,
missing sync/parallel/status reference, and the old B70 v1-service statement.
The v1 removal date (2026-10-09) comes from the supplied operator instruction;
no live B70 verification was performed.

`docs/production-test-plan.md` is now the single ordered founder checklist:
prerequisites/cross-builds; private config and NAS identity; CPU-only B70 queue
on 7331 with GPU hiding; Spark CUDA unified memory with 16 GiB reserve;
measured cold copy and pre-copy; pin and genuine model call; a CUDA PyTorch batch;
copy/priority/network faults; loss and reboot recovery; recoverable undo.
Spark uses `/home/kuna/nas-dev`; B70 uses its verified `/mnt/verso-nas` or
`/home/kuna/nas-dev` mount. Removed the duplicate ordered preparation sequence
from operations and linked the checklist. General operations recipes remain.

Hardware prerequisites remain explicit: approved model/inference contract,
existing GPU engines/runtime, actual free disk and shared mount identity,
GPU admission and reboot windows. No installation or hardware readiness claim
comes from local tests. B70 host reboot remains separately gated by Verso.

## Final validation

Environment: Go 1.25.6, macOS arm64. All passed on the final code:

- `go build ./...`
- `go vet ./...`
- `go test ./... -race -count=1` (cmd 16.246s, internal 39.541s)
- `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ./...`
- `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...`
- Full Python unittest discovery: 9/9, zero skips
- `git diff --check`

Cross-builds prove compilation only; no Linux executable or real engine was run.
No SSH, fleet changes, LibreOffice, push, deployment or production cutover.

## DIGEST
Five model-sync bugs fixed with regression tests.
Cache limits, copy reservations and loaded-model placement now agree.
Background copies pause on queue loss and production arrival.
Full-system faults pass; pins stay loaded and bad files never load.
Python passes all 9 tests with zero skips in a fresh virtualenv.
Go build, vet, race tests and both Linux cross-builds pass.
Docs now have one ordered founder test and recoverable undo.
No fleet access, LibreOffice or push; hardware acceptance remains pending.
