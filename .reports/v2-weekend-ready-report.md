# Mycelium v2 weekend readiness

Date: 2026-10-09. Branch: `v2-weekend-ready`, created from local main
`3bfa1220b17a80e41b80ba3a5aeab1bc7b47ca66`. Local implementation only: no fleet
access, SSH, model loading on real hardware, LibreOffice, deployment or push.

## Status page and CLI

Dispatch and status share the candidate selector, fit/cap checks, fair-share
scores and scheduling comparator. Status projects eligible waiting work against
current machine inventory with batch reservations released; model calls participate
when their real model route is eligible. Within a priority, running/stopping work
comes first, then that scheduler order, then blocked work. Busy explanations
combine capacity with priority, fair share or the first queued job's precedence.
The UI consumes queue-provided order/reasons rather than implementing another policy.

This is the current dispatch order, not a guarantee about future arrivals,
changing usage or which of several workers becomes free first. Machine/model
eligibility, caps and environment blockers stay visible. Dispatch policy is unchanged.

IDs display eight characters with full IDs in native hover titles and
`myce status --full`. Assigned commands show `attempt 1` and later actual attempt
numbers; unstarted commands and model calls do not invent attempt zero. The
machine card's zero progress is no longer mistaken for an attempt. Running and
waiting ages use the injected queue clock; requeued jobs count from their latest
return to the queue without changing scheduler submission order. Running age uses acknowledged start,
with assignment time before acknowledgement. Model calls record dispatch time.
Completed command duration is saved when start/finish are known; older records
without a finish time do not receive fabricated durations. Existing command
errors/exit-status text stays visible; “No additional details” is removed.

Tests cover busy fair-share ordering matching actual subsequent dispatch,
shared affinity/FIFO order, eligible model calls, current-attempt durations,
HTML escaping, short/hover IDs, filler removal and CLI `--full`/one-based labels.

## Rendered evidence

`.reports/status-page-2.png` is a 1280×1500 headless Google Chrome capture of the
actual `myce dev` HTTP page with one fake 8 GiB worker. Real local shell commands
first completed a five-second Verso experiment to establish observed usage, then
ran a production-priority sleep while Atlas and Verso experiments waited. A
queued production example was cancelled before the final capture so the fair-share
reason remains visible alongside the running production job. No UI data was
injected into the HTML and no fleet token is present in the image.

The saved dark desktop capture was inspected. The collaborative browser also
verified the light page at 390 px, scrolled through the queue/outcomes, confirmed
no horizontal overflow, eight-character IDs with full hover titles, growing ages
and successful periodic HTTP refreshes. Closing the owned local demo produced
“Refresh paused”; retained content remained readable. Demo and owned headless
Chrome processes were stopped after capture. Scratch DBs/builds/profiles remain
outside staged changes.

## Weekend checklist

`docs/weekend-runbook.md` is the single weekend ordering with inputs/decisions
first and per-stage estimates, go/no-go checks, blockers and rollback. It links
to the real-machine acceptance plan and reviewed Atlas/Verso worktree runbooks
instead of duplicating commands. All local links resolve. The acceptance plan
now points back to the weekend checklist.

Order: Mycelium acceptance → immutable NAS catalogs/checksums and environments →
pre-copy → B70 GPU0 measurement/explicit capacity decision → Atlas judge/OCR →
Atlas known GPU batches → Verso GPU1 experiments → Verso GPU0 production last.
The current Verso six-model command includes Nemotron and `qwen3.8-27b`; the
older Mycelium measurement example's five-model/`verso-naming` list is explicitly
flagged so the founder does not qualify the wrong resident set.

Atlas's missing deployed `atlas_wiki` source blocks deployed-source qualification
in stage 2, wiki/loop cutover/certification in stage 5, loop-triggered batches and
search/loop integration in stage 6, and full deployed Atlas acceptance. It does
not block isolated known in-repo judge/OCR/batch canaries or Verso stages. Existing
CPU search/loop remains untouched pending source reconciliation. Source facts
come from the provided local runbooks and preparation report; no new remote or
fleet verification was attempted. Catalog placeholders, exact runtime/source
contracts, real measured fit/headroom/concurrency and authorized ownership remain
weekend gates; local test success is not hardware or migration acceptance.

## Verification

- Native Mac Go 1.25.6: `go build ./...` and `go vet ./...` passed.
- Linux/arm64 Go 1.25.6: build, vet and `go test ./... -race` passed in an
  isolated local Docker container. Final output: `ok mycelium/cmd/myce 25.463s`,
  `ok mycelium/internal/myce (cached)`; the same final-source runtime package
  actually passed in 59.546s on the preceding run. `GOFLAGS=-p=1`,
  `GOMAXPROCS=2`, four CPUs/4 GiB; no GPU, fleet config or credentials mounted.
  Current Go source/manifests snapshot SHA-256 (sorted path, NUL, file bytes):
  `b977ae7ebbae70b12f0e20a10b1232af96f86496859d82d1fb7b1c81a0f76823`.
  Base image digest:
  `sha256:d9c68c2c51161e12fd77e4c6320687c9cd86e1af1e3ad6e6cd63ff970641453c`;
  `GOTOOLCHAIN=go1.25.6` selected the same compiler version as the Mac.
- Python 3.13 with YAML/OpenAI extras: all nine unittest tests passed, no skips;
  tests build the actual CLI and run `myce dev` locally.
- Both `CGO_ENABLED=0 GOOS=linux` cross-builds passed. Binary SHA-256: amd64
  `69f484b206c9a83b1640f75e5c8864e19cf9fb2e3565bf96076edf73ba56a254`;
  arm64 `109fcd7ddc3172aed6234d828edfe8b8e5162ceb3f55e3d98880ea0e6abeab8a`.
- `git diff --check`, local documentation links and rendered desktop/mobile
  checks passed.

**Native Mac full-suite caveat:** repeated runs, including serialized packages,
hit production-start, server stop/permission, dev startup/shutdown and one-second
HTTP timing failures. The Mac had load averages around 41–48 on ten cores with
other workloads active. Focused status tests and a focused copy-fault rerun passed;
no data race was reported. The first Linux run also hit the copy-fault request
timeout, and the first final-source Linux run hit a one-second model-idle status
assertion; the unchanged final full command then passed. No test assertions,
timeouts, model-idle setting or production grace were weakened. Linux success
does not clear the observed Mac failures or certify fleet hardware. Local stopped
verification containers/toolchain caches are retained; no daemon settings or
unrelated workloads were changed.

## DIGEST
- Status order and combined wait reasons use the dispatch scheduler.
- Short/hover IDs, `status --full`, one-based attempts and elapsed times added.
- Saved `.reports/status-page-2.png`; desktop/mobile and refresh failure checked.
- Added ordered weekend checklist with decisions, gates, rollback and Atlas blocks.
- Build/vet, Linux race suite, Python suite and both Linux builds pass.
- Mac full-suite failures recorded; no fleet access, LibreOffice or push.
