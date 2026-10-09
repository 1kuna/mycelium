# How it works today (before this branch)

Inspected main at `dab0e6a79991319c1e619e065ca25ac688fc54cf` before implementation.
`Model.Path` was passed unchanged to `llama-server --model` in
`internal/myce/engine.go`. It meant a worker-visible file path, without NAS-root
translation or a local cache. A NAS-mounted path therefore caused llama-server
to read directly from the NAS. The worker checked free GPU memory before loading,
but not filesystem space. There was no verified copy, cache limit, disk eviction,
partial-copy cleanup, cache inventory, explicit model fetch, or production pin.
The adapter itself never downloaded weights, but unrestricted engine arguments
could request a Hugging Face/model-URL download. Production preemption only
stopped resumable batches, with the same two-minute grace as deadlines. Stops
were returned at the worker's next check-in.

# Implemented

Branch `v2-models-prod` was created from main with an initially clean worktree.

- Catalog paths now identify NAS masters, relative to each worker's `--model-root`
  or absolute mount paths. Real models require exact byte count and SHA-256.
  Explicit `myce model fetch` downloads a Hugging Face HTTPS file, checks it,
  publishes without overwriting an existing master, and preserves temporary
  material in NAS Trash. No fetch is invoked by loading. Remote-download engine
  arguments and related environment settings are rejected.
- Worker-local cache directory and size allowance are configurable. Copies are
  serialized, checked against actual free disk and the allowance, written to
  `.partial`, verified, synced, and atomically renamed. Reuse also verifies the
  cached file. LRU eviction excludes loading/resident/draining models. Startup
  cleans interrupted partial copies. Copy/verification is within load timeout.
  `direct_nas: true` explicitly enables verified direct reads for small models.
- Check-ins include cached names, available cache bytes, and refusal messages.
  Placement prefers ready models, then cached copies, then cold copies. CLI,
  status JSON, and the HTML status page expose cache and pin state.
- Production preempts lower-priority batches immediately, with 10s configurable
  checkpoint grace for resumable work and immediate kill for non-resumable work.
  Requeue preserves the infrastructure-loss retry budget and creates a fresh
  execution identity. Deadline preemption retains 2m and requires resumability.
  Production also shortens an existing deadline stop grace without extending it
  on later check-ins. An authenticated, session-scoped long-poll control channel
  delivers stops independently of the heartbeat, reconnecting after failure.
- SQLite stores project-owned machine/platform pins and optional weekday/time-zone
  windows. Pins warm the model, retain a healthy platform assignment, reserve GPU
  memory, and prevent idle/priority unloading. Loaded pins also protect their cache
  copy. Unpin releases protection. Load failures remain visible in status.
- Startup acknowledgement records production submission-to-start latency using
  queue time, avoiding worker clock skew. A started-process check-in can recover
  a missed acknowledgement. Status exposes the measurement. A real StartDev
  test (the implementation behind `myce dev`) additionally observes command logs
  after clearing an experiment that ignores SIGTERM, with a <15s assertion.
- Updated all five requested documents, plus the model/checkpoint guides and
  catalog template. Experiments are strongly advised to save frequent atomic
  checkpoints and implement resumability.

# Verification

- `go build ./...`: passed.
- `go vet ./...`: passed.
- `go test ./... -race`: passed; CLI package 16.459s, internal package 35.687s.
- `PYTHONPATH=clients/python/src python3.13 -m unittest discover -s
  clients/python/tests -v`: passed, 9 tests run, 2 optional tests skipped.
  OpenAI SDK and YAML extras are absent; nothing was installed. Python 3.13 is
  installed and meets the package's >=3.10 requirement. The system Python 3.9
  run is unsupported and failed its timeout exception test; it is not the
  acceptance runtime.
- Additional timing evidence (`go test ./... -race -run
  'TestDevCheckpointEndToEnd|TestDevProductionStartsUnder15Seconds' -v -count=1`):
  real `myce dev` binary, cooperative checkpoint save: **1.493 seconds** from
  submission to observed production command output; StartDev with a victim
  ignoring SIGTERM, full 10s grace: **10.426 seconds**. Both targets were <15s,
  with no model load. Later heartbeat-inventory changes also passed the full suite.
- Browser inspection: local fake dev status at 1280px and 390px, including ready
  pin, cache inventory, disk-refusal message, and empty jobs. Phone page scrolls
  through all tables; measured root width 390px and no horizontal body overflow.
  Read-only status has no form/hover/focus states or dark-mode control.
- `git diff --check`: passed. The temporary local dev server was shut down.

All tests use fake GPU inventory; command execution, HTTP, SQLite, cache files, and checksum checks are
real. Fetch tests use an in-memory HTTP transport, never an internet download.

Coverage includes LRU and disk-pressure eviction, disk-full and protected-cache
refusal, checksum/size mismatch, cancellation, interrupted-copy restart cleanup,
cache reuse/reporting, direct NAS and traversal refusal, explicit fetch success
and checksum failure, remote-download argument rejection, ready/cached placement,
pin protection/reservation/windows/persistence/reassignment/failure visibility,
CLI pin/unpin, inventory during a copy without blocking heartbeats, immediate stop delivery, production grace/configuration/shortening while preserving cancellation,
non-resumable restart without loss retries, stale results, and the real dev timing.

# Operational limits

No SSH, fleet hardware, installs, LibreOffice, push, or deployment occurred.
Only local temporary dev processes/files and repository changes were used.

Eviction is recoverable: with `--model-root`, files move to NAS Trash. On a
cross-filesystem move, a recovery copy is saved and checksum-verified before the
original is moved; this conservatively retains two copies in NAS Trash. Manage
NAS recovery retention explicitly. With no NAS root, user Trash is used and may
not release local filesystem blocks; the load refuses if physical space remains
insufficient. A failed fetch can leave unpublished `.partial` bytes for explicit
recovery; it never publishes a failed checksum.

Pins promise no load wait only after readiness, while a healthy eligible worker
is available. Window activation may require warmup. Overnight windows must be
split. Failed pinned loads retain a visible failed reservation until unpinning;
no silent retry or eviction makes that reservation disappear. Real GPU serving,
NAS cross-filesystem throughput, and hardware startup remain fleet acceptance
items, not claims from laptop tests. Startup metrics include acknowledgement
transport delay; the dev assertion uses observed command output as an upper bound.

# DIGEST
NAS masters now load through a verified local cache, with explicit fetch only.
Cache reports, space checks, eviction, and interrupted-copy cleanup are tested.
Production stops lower-priority work immediately and restarts experiments safely.
Machine/platform pins and weekday reservations protect model memory.
Real dev starts took 1.493s and 10.426s, excluding model load, below 15s.
Build, vet, and race tests passed; Python passed with two optional tests skipped.
Docs and this report are updated. Real GPU and NAS throughput remain untested.
No fleet access, installs, LibreOffice, or push occurred.
