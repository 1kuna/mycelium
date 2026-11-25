# Cut list (#1971)

A ranked list of what can leave this repository, written for the workers who
will execute it. Delete this file and `cut-list-tests.tsv` when the list is
done. Measured on main at `fd74bf1b5`, 2026-11-24.

## Verdict

The earlier conclusion, "every remaining line guards real behaviour", is half
right.

- **Runtime code: right.** Only 62 runtime functions (536 lines) are
  unreachable from the binary, and the three unify-then-delete lanes netted
  about zero. Runtime shrinks only by removing whole features.
- **Tests: wrong.** Run one at a time, 1,414 of 2,987 tests (58,033 of
  130,056 test-function lines) can be removed without losing a single covered
  statement. The suite repeats itself.
- **Docs and tooling: wrong.** One data file is 10,002 lines, one reference
  page is 3,444 lines of mechanism narrative, and a second gate runner exists
  for a path that is currently banned on the development host.

| Group | Cuts | Lines |
|---|---|---:|
| (a) The orchestrator can approve: no feature or guarantee lost | A1–A7 | about 23,000 |
| (b) Needs the maintainer's yes | B1–B6 | about 62,700 |

Group (b) is 64,600 before removing a 1,900-line overlap between B1 and B2.
With the smaller serving option in B2 it is about 51,000.

## How this was measured

- **Line counts** are physical lines from `git ls-files` at the commit above.
- **Coupling.** For each candidate, a type-checked pass counted every use of
  its declarations from files outside it ("seam references"), and every test
  function outside it that touches it.
- **Unreachable code.** `deadcode ./...` for Darwin and for Linux; a function
  counts only when both report it.
- **Test redundancy.** The `internal/myce` test binary was built with coverage
  on Darwin and each of its 2,995 tests was run alone. 2,987 passed and were
  compared by the set of statement blocks each executes. Eight failed or timed
  out alone and were kept (listed under [Not verified](#not-verified)).
- **Use.** The feature census in
  [#1971](https://github.com/1kuna/mycelium/issues/1971#issuecomment-7124037069)
  (10,833 retained jobs, 16 projects), plus one read of live status from a CLI
  built from tag `s088r6.36`.

Each cut says which numbers are measured and which are estimates. An estimate
is a seam that has to be cut by hand, sized from its reference count.

## Rules for the workers executing this

These exist so a cut is not re-argued line by line.

1. **The cut is decided.** Your job is to remove it and prove what remains.
   If you find the evidence here is false (a caller this list says does not
   exist, a job that uses the feature), stop that cut and report the fact.
   "This might guard something" is not a finding.
2. **Net deletion is the acceptance test.** A PR that executes a cut and does
   not shrink the repository by roughly the stated amount is not done.
3. **Delete the tests of a removed thing** (MY-53, and MY-41 for tooling).
   Each cut names the one check that proves the remaining behaviour. Do not
   keep a test "for safety", port it to the replacement, or add a parity
   matrix.
4. **Four guarantees are out of bounds:** exact custody (no duplicate or lost
   attempt), token secrecy, the shipped memory model, and verified outputs. No
   cut here removes one. If a seam runs through one of them, cut around it and
   keep its tests.
5. **Merge on quick plus the named check** (MY-41). The full gate runs once per
   release batch.
6. **Adjacent releases still interoperate** (MY-42). When a cut removes a wire
   field or a saved-state field, keep decoding it for one release, then delete
   the decoder.
7. **Update `skills/mycelium` in the same change** when a consumer-visible
   command, field or error changes, and tell the projects named under "Who
   notices".

## Group (a): the orchestrator can approve

Ranked by lines removed per unit of risk. Risk is 1 (cannot affect a job) to
5 (scheduler surgery).

| # | Cut | Lines | Risk |
|---|---|---:|:---:|
| A1 | Per-test timing file reduced to its heavy roots | 9,450 | 1 |
| A2 | Mechanism narrative and research records out of docs | about 4,700 | 1 |
| A3 | Orphaned fixtures, a harness nothing runs, a stale release fixture | 3,522 | 1 |
| A4 | Internal-detail tests already covered by other tests | 1,866 | 1 |
| A5 | Finished migrations and decode-only bridges | about 1,350 | 2 |
| A6 | Runtime functions only tests call | about 1,040 | 2 |
| A7 | Finish the one-file migration, then delete the converter | 1,081 or more | 4 |

### A1. Per-test timing file

- **What goes:** `scripts/gate-test-durations.json`, 10,002 lines, becomes one
  flat map of about 520 lines.
- **Why:** it stores a duration for each of 3,343 test roots three times
  (generic, Darwin, Linux). Only 516 roots take 2 seconds or more on either
  platform; `gate-shards.py` already gives an unlisted root 0.1 seconds. The
  hints order work and never select coverage (measured from the file and the
  loader).
- **Replaces it:** `{package: {root: seconds}}`, roots of 2 seconds or more,
  the larger platform value.
- **Who notices:** nobody outside this repository.
- **Execute:** rewrite the file; reduce the loader in `gate-shards.py` to one
  map; update `test-gate-shards.py`. Proof: that test, and one fleet quick.

### A2. Docs

- **What goes:**
  - `docs/reference/status-behavior.md`, 3,444 lines, to 700 or fewer. One
    section, "Automatic host health", is 1,475 lines.
  - `docs/research/` (3 files, 832 lines), `docs/design-learned-storage.md`
    (177, a parked proposal) and `docs/process-memory-growth.md` (73, no
    inbound link).
  - About 450 lines of records in `docs/development.md`: "Existing test-suite
    pruning record", "M1 keyed attempt wire revision", "Current worker
    protocol", "Queue scan shortcuts", "Placement scenario probes", "Backed
    storage admission integration", "Tool inventory check-in protocol", "CPU
    and I/O collector cost", "Fan-out burst acceptance", "Pressure and status
    agreement".
  - In `docs/design-memory-admission.md`, sections 1 and 3 to 7 (about 410
    lines), once #1972 closes. Section 2 is the model and stays.
- **Why:** consumers' agents read the skill and operators read
  `operations.md`. These pages describe how the code works inside (sample
  counts, smoothing constants, which lock is held), which the code already
  says, or record how a decision was reached, which git and the issue keep.
  The status field inventory is generated into `docs/status.md` and is not
  affected.
- **Replaces it:** nothing. Inbound links become permalinks to the last commit
  that has the file.
- **Who notices:** nobody; no consumer contract changes.
- **Execute:** for each paragraph ask one question: does it tell a consumer
  what a field, hold or error means, or tell an operator what to do? If yes,
  keep one statement of it. If it explains how the code achieves that, delete
  it. Proof: `scripts/check-doc-links.py` and the generated-status check.
  Estimate, not a measured count.

### A3. Orphaned fixtures and a harness nothing runs

- **What goes** (measured):
  - `internal/myce/testdata/unified-physical-history.json`,
    `unified-cuda-coverage-gap.json`, `unified-naming-history.json` and
    `unified-cuda-retained-bound.json`: 1,851 lines. No file in the repository
    names them.
  - `internal/myce/queue_install_cutover_linux_test.go` (1,030) with
    `testdata/queue_install_physical.py`, `queue_install_volume.py`,
    `queue_install_systemctl.py`, `run_queue_install_pair.sh` and
    `run_queue_install_cutover.sh`: 1,454 lines. The test skips unless
    `MYCE_NATIVE_QUEUE_INSTALL_TEST=1`, which no gate, profile or workflow
    sets.
  - `internal/myce/queue_r610_fixture_test.go` with
    `testdata/r6.10-queue.sql` and `r6.10-queue-seed_test.go.txt`: 217 lines.
    It claims to guard the adjacent release pair with a database from r6.10;
    the fleet is on r6.36.
- **Why:** a fixture nothing loads and a test nothing runs guard nothing. The
  cutover harness qualified a one-time queue install (inference from its own
  comments).
- **Replaces it:** `scripts/upgrade-compat.py` already exercises the real
  adjacent pair in the full gate.
- **Who notices:** nobody.
- **Execute:** delete the files and the `development.md` mention of the r6.10
  fixture. Proof: `go vet ./...` and quick.

### A4. Internal-detail tests already covered by other tests

- **What goes:** the 69 tests marked tier 1 in
  [`cut-list-tests.tsv`](cut-list-tests.tsv), 1,866 lines.
- **Why:** MY-53 allows deleting a test that checks only internal details when
  another test already exercises that code. Both halves are measured. Each
  listed test, directly or through its helpers, calls no exported function or
  method, no HTTP test server and no subprocess; and every statement it
  executes is also executed by tests that do. Tests that cite an issue number,
  or whose name or file touches a protected area, were left out.
- **Replaces it:** nothing.
- **Who notices:** nobody.
- **Execute:** delete the listed functions, then any helper left unused
  (`staticcheck -checks U1000`). Proof: quick on both platforms. Do not review
  the tests one by one; the list is the review.

### A5. Finished migrations and decode-only bridges

- **What goes:**
  - `storage_compat.go` (120), `storage_compat_test.go` (165),
    `storage_compat_review_test.go` (178) and
    `testdata/parked-storage-state.json` (143): 606 lines. The file says it
    is decode-only compatibility for a feature that is parked on a branch.
  - `worker_unit_migration.go` (153), its two tests (240),
    `cmd/myce/migrate.go` (38) and `migrate_test.go` (17): 448 lines.
  - The wire and config bridges the refactor plan in #1987 already
    inventoried: 110–132 runtime and 159–205 test lines.
- **Why:** MY-42 keeps only the adjacent release. Every worker in the live
  read reports one version (measured).
- **Replaces it:** nothing.
- **Who notices:** nobody, if the two checks below pass.
- **Execute:** first confirm from live status that no retained job record
  carries the parked storage fields, and that every enrolled worker already
  runs under the migrated unit. If either is false, finish that migration
  first. Then delete. Proof: `scripts/upgrade-compat.py` against the previous
  release.

### A6. Runtime functions only tests call

- **What goes:** 62 functions, 536 runtime lines, that the binary cannot reach
  on either platform, and the tests whose only purpose is to call them (about
  500 lines, estimated). The largest are `readQueueState` (88),
  `Queue.copyEnvironmentLogs` (34), `SetWorkerProbeDefaults` (30),
  `memoryReferenceKey` (30) and `Queue.productionParityKnown` (23).
- **Why:** a function production never calls is not behaviour. The earlier
  dead-code pass kept these because tests reference them.
- **Replaces it:** where a test needs the function as a fixture, move it into
  a `_test.go` file.
- **Who notices:** nobody.
- **Execute:** run `deadcode ./...` and `GOOS=linux deadcode ./...`; take the
  intersection; skip `internal/testenv` and `internal/testguard`. For each:
  move it to a test file if tests use it as a helper, otherwise delete it with
  the tests that exist only for it. Proof: quick.

### A7. Finish the one-file migration

- **What goes:** `cmd/myce/adapter_convert.go` (527), its two tests (442) and
  the converter chapter in `docs/adapter.md` (112): 1,081 measured. Then the
  old profile and extension authoring readers and their tests, estimated at
  another 500 to 1,500.
- **Why:** MY-22 and the approved design already chose one authored file and a
  one-shot converter, retired after consumers convert. The cutover never
  finished: this repository still carries six `.mycelium/profiles/*.yaml`, and
  seven projects submitted 3,042 profile-origin jobs in the retained window
  (census).
- **Replaces it:** `.mycelium/adapter.yaml` entries. The internal decoder for
  saved snapshots stays, as the design says.
- **Who notices:** the seven projects still submitting profiles, including
  this one. Each needs one converted file.
- **Execute:** convert this repository's own profiles first. Announce to the
  other projects with the converter command. Delete only when the job list
  shows no `origin: profile` submission for 14 days. Until then this cut is
  parked on that trigger. Proof: the design's cold-restart check for a job
  accepted before the cutover.

## Group (b): needs the maintainer's yes

| # | Cut | Lines | Risk |
|---|---|---:|:---:|
| B1 | Tests that only re-run what other tests run | 26,940 | 3 |
| B2 | Queue-managed model serving | about 25,000 (or 12,000) | 5 |
| B3 | The development host's own gate runner | about 8,300 | 2 |
| B4 | Background NAS delivery and local-first checkpoints | about 2,800 | 2 |
| B5 | The web status page | about 800 | 1 |
| B6 | The npm environment type | 768 | 2 |

### B1. Tests that only re-run what other tests run

- **What goes:** the 726 tests marked tier 2 in
  [`cut-list-tests.tsv`](cut-list-tests.tsv): 26,940 lines and 454 seconds of
  serial test time. 42 whole test files empty out.
- **Why:** with every tier 2 test deleted, the remaining tests execute exactly
  the same statements (measured, Darwin). The list was built with 1,599 tests
  held back untouched: every test whose name or file mentions custody,
  release, recovery, cancellation, tokens, isolation, security, memory,
  admission, outputs, uploads, checkpoints, files, wire formats or schemas;
  every API test and `myce dev` end-to-end journey; every test that cites an
  issue number; and the internal-detail tests A4 leaves in place. Without the
  hold-back the removable figure is 58,033 lines.
- **Which layer stays:** the end-to-end journeys and API tests stay, because
  they check what a consumer sees. The unit tests they and their neighbours
  already cover are the ones listed.
- **What is not proved:** equal statement coverage is not equal checking. A
  listed test may assert an outcome no surviving test asserts, on lines the
  survivors also run. That is why this needs a yes: MY-53 covers internal
  detail tests, and these reach exported entry points.
- **Replaces it:** nothing.
- **Who notices:** nobody directly. Gates get faster (MY-38).
- **Question for the maintainer:** *About 27,000 lines of tests only re-run
  code that other tests already run. None is in the custody, token, memory or
  output areas, none is an end-to-end or API test, and none names a past
  incident. May they be deleted? The risk
  is that a few check a specific outcome nothing else checks; if a bug slips
  through, one focused test goes back.*
- **Execute:** record the covered-block set on Linux and on Darwin
  (`go test ./internal/myce -coverprofile`). Delete the listed tests and the
  helpers left unused. Record the set again. For each block that is no longer
  covered, restore the smallest listed test that covers it, and stop there.
  Proof: quick on both platforms and the two block sets equal. Do not evaluate
  the tests one by one.

### B2. Queue-managed model serving

- **What goes (full):** the queue's own model servers: the `/v1` endpoint, the
  model catalog and server entries, engine load and unload, the worker model
  cache, pins and schedules, pre-copying, local-folder approvals,
  `myce model fetch`, resident-engine binding, the detached engine keeper,
  learned readiness, and the model interval ledger.
  - Measured: 9,137 runtime and 9,564 test lines in 54 dedicated files
    (`engine*.go`, `models.go`, `model_custodian.go`, `model_fetch.go`,
    `model_files.go`, `model_intervals.go`, `model_memory.go`,
    `model_readiness*.go`, `model_root_local*.go`, `model_store.go`,
    `server*.go`, `pins.go`, `readiness_*.go`, `resident_jobs.go`,
    `local_models.go`, `sync.go`, `worker_sync.go`, `cache.go`), plus
    `cmd/myce/model.go` and its test (203), the Python OpenAI test (109), five
    example files (330), the `llama-cpp` starter extension, and 1,311 lines of
    `docs/services.md`, `docs/readiness.md` and `docs/reference/models.md`.
  - Estimated: 1,000 to 1,500 runtime lines at the seam (1,057 references
    from 99 other files, mostly model slots in placement, preemption, memory
    pools, fairness, usage and status) and 2,000 to 4,000 lines of the 164
    outside test functions that touch it.
- **What stays:** per-job model snapshot import (`model_snapshots.go`,
  `model_hub.go`), which jobs use, and native sidecars.
- **Why:** the 20-day model ledger holds one engine load and one metered
  request (census). The live read shows no loaded model, no pin and no
  pre-copy request, and one model copy ever recorded on one machine. The
  paying production consumer does not use serving in production; it has one
  dormant opt-in resident path. This is the largest feature in the codebase by
  lines and the least used.
- **Replaces it:** a job starts its model server as a native sidecar, which is
  documented in `services.md` and which three projects did for 496 attempts in
  the retained window.
- **Who notices:** the paying production consumer (its dormant resident path
  goes) and any project with a server entry. Their callers get a typed error.
- **Smaller option:** keep on-demand load, call and idle unload; remove pins
  and schedules, pre-copying, local-folder approvals, `myce model fetch`,
  resident binding, the detached engine keeper, learned readiness and the
  ledger. Measured 4,253 runtime and 5,245 test lines in 29 files; about
  12,000 with seam and docs.
- **Question for the maintainer:** *In the last 20 days Mycelium started a
  model server once and answered one request; nothing is loaded now. Should
  Mycelium stop running model servers itself? A project that needs a model
  would start it inside its own job, which already works. Yes removes about
  25,000 lines. If you want Mycelium to keep serving models on demand and drop
  only the extras (pins, schedules, pre-copying, the download command,
  resident binding), that removes about 12,000.* This changes the approved
  design and MY-11, which names serving.
- **Execute, in three PRs:**
  1. *Surface.* `/v1/*`, the model routes, `myce model` and a job spec with a
     resident binding return `model_serving_retired` and point to the sidecar
     guide. A saved catalog with model entries logs a warning and is ignored;
     it must not stop the queue starting. Update the skill.
  2. *Worker.* Delete the engine runtime, model cache, copy planner and
     keeper. Keep decoding the model fields of a previous-release worker for
     one release.
  3. *Queue.* Delete model slots and calls from placement, preemption, memory
     pools, fairness, usage and status.
  Proof: one new test that a `/v1` call returns the typed error; the existing
  native sidecar group tests; the `myce dev` batch journey;
  `scripts/upgrade-compat.py`. Delete every test in the listed files.

### B3. The development host's own gate runner

- **What goes:**
  - The local VM path and its admission scheduler: `gate-local.py`,
    `gate-linux.py`, `gate-guest.py`, `gate-capacity.py`, `gate-cache.py`,
    `gate-default-cache.py`, `gate-measure.py`, `gate-source.py`,
    `gate-handles.py`, `gate-memory.py`, `gate-command.sh`, `gate-leg.sh`,
    `measure-shared-host.py`, their tests and `.mycelium/lima-gates.yaml`:
    4,756 lines (measured).
  - The supervisor that runs inside a gate job: `gate-budget.py`,
    `gate-scheduler.py`, `gate-progress.py`, `gate-shards.py`,
    `gate-receipt.py`, `gate-reconcile.py` and their tests: 2,956 lines
    (measured), less about 150 for what replaces it.
  - About 800 lines of "Laptop test gates" and "Local Linux VM" in
    `docs/development.md` (estimate).
- **Why:** full local gates and the VM are currently banned on the development
  host for disk space, and CI and `gate-fleet-quick.sh` already run gates as
  Mycelium jobs. The local runner re-implements admission, memory observation,
  disk floors and process custody in Python; the in-job supervisor repeats
  containment the worker already gives every job.
- **Replaces it:** fleet gates as the one gated path; `go test` for local
  targeted runs; `gate.sh` reduced to selecting tests and calling `go test`.
- **Who notices:** Mycelium's own agents. No consumer.
- **Question for the maintainer:** *Full test gates on the development Mac are
  banned for disk space, and gates already run on the fleet. Can the Mac's own
  gate runner (the Linux VM path and its scheduler) be deleted, so the fleet is
  the one place gates run and the Mac runs only quick targeted tests? This
  reverses MY-37, which said gates must not depend on the fleet being up.* The
  cost of yes: a fleet outage blocks gating its own fix until someone runs
  `go test` by hand.
- **Execute:** delete the first group and its docs in one PR; prove with one
  fleet quick and one fleet full. Replace the in-job supervisor in a second PR
  only after that.

### B4. Background NAS delivery and local-first checkpoints

- **What goes:** `output_delivery.go` (480), `checkpoint_local.go` (335) and
  their tests (769): 1,584 measured. Up to 393 runtime and 368 test lines in
  `output_delivery_capacity.go` and `output_delivery_spool.go`, once the
  helpers ordinary outputs share are moved out; a seam of 136 references in 28
  files; 13 outside test functions (up to 1,012 lines); the Python client's
  local-first tests; and about 190 doc lines. `output_delivery_patterns.go`
  stays: ordinary declared outputs use it.
- **Why:** none of the 10,833 retained jobs carries a delivery or local-first
  marker (census). Local-first checkpoints depend on the delivery publisher
  for their NAS mirror, so the two go together.
- **Replaces it:** the caller fetches its verified result and copies it where
  it wants; resumable jobs use ordinary NAS or file-store checkpoints.
- **Who notices:** no project in the retained history.
- **Question for the maintainer:** *Two opt-in file features have not been
  used by any job on record: copying finished outputs to the NAS in the
  background, and "local-first" checkpoints. Delete both? Jobs keep verified
  outputs through the queue and ordinary checkpoints.* Both are in the approved
  design.
- **Execute:** check live state for a pending delivery or mirror first; if one
  exists, let it finish. Refuse the two spec options at submission with a
  typed error, then delete. Keep the output verification and file-release
  tests untouched. Proof: the ordinary output publication and checkpoint
  resume tests.

### B5. The web status page

- **What goes:** `status_page.go` (421), `status_page_test.go` (231), the
  `GET /` route in `http.go`, and the page assertions in 16 outside test
  functions: about 800.
- **Why:** agents and operators read `myce status` and the JSON API. Whether
  anyone opens the page is not measured; there is no page-view counter.
- **Replaces it:** `myce status`.
- **Who notices:** anyone who opens the queue address in a browser.
- **Question for the maintainer:** *Do you ever open Mycelium's status page in
  a web browser? If not, it goes; `myce status` stays.* The approved design
  lists the page.
- **Execute:** delete the file and route; `GET /` returns 404. Remove the page
  assertions and keep the rest of each test. Proof: the status JSON tests.

### B6. The npm environment type

- **What goes:** `npm.go` (325), `npm_test.go` (309) and its documentation
  (134, per the census): 768.
- **Why:** none of the 122 environment names workers report contains "npm"
  (live read). That is weak evidence: the job API does not expose an
  environment's backend, so use is unmeasured.
- **Replaces it:** nothing; a Node job installs its packages in its own
  command.
- **Who notices:** any project with an npm environment.
- **Question for the maintainer:** *Does any project run Node.js (npm) jobs on
  Mycelium? If not, the npm environment type goes; Python environments stay.*
- **Execute:** refuse the backend at submission with a typed error; delete the
  two files and the doc section. Proof: the uv and pixi environment tests.

## Checked and kept

Do not reopen these without new evidence.

| Feature | Runtime / tests | Why it stays |
|---|---:|---|
| Production coverage leases | 239 / 543 | The paying production consumer plans to adopt them. |
| Strict ephemeral worker data | 672 / 1,330 | The paying production consumer needs it under a maintainer ruling on customer data. |
| Device-scoped production hold | 370 / 310 | Enabled on one host in live policy. |
| Shared interactive host profile | 898 / 1,811 | Active on one host in the live read. |
| Native sidecars | 1,518 / 1,469 | 496 attempts in 3 projects; the replacement for B2. |
| Completion observers and notifier | 1,226 / 2,254 | 1,188 deliveries in 8 projects. |
| Validation reservations | 533 / 1,051 | 37 records in the live read. |
| Scarce-host yield | 570 / 1,154 | MY-26; 72 jobs carry yield evidence. |
| Per-attempt wall time | 210 / 343 | 120 jobs in 4 projects. |
| Named-piece dependencies | 297 / 860 | One project uses the named-piece form; the rest of the file is heavily used. |
| Daily-use participation signals | 657 / 706 | No machine uses the mode (all four reporting are dedicated, none a laptop), but MY-11 names it and it shares its record with ordinary worker availability. Too small to be worth a ruling. |
| Per-job model snapshot import | 558 / 1,405 | Four of five machines hold snapshots. |

## Bigger than a deletion pass

These are large and probably over-built, but nothing here can be handed to a
worker as "delete these files". Each needs a short design first. None is in
the totals.

- **Install, onboarding and enrolment:** 6,421 runtime and 8,933 test lines in
  74 files, for a fleet of five machines.
- **Two ways to describe software:** registered extensions (4,284 runtime,
  4,811 test) next to adapter entries. A7 retires the old authoring files; the
  registry, composition and bundle store may be mergeable after it.
- **Self-tuning limits for diagnostics and polling:** 3,971 runtime and 4,739
  test lines in 31 files learn their own timeouts, budgets and polling pace,
  because MY-34 forbids fixed limits. A possible question for the maintainer,
  not asked here: *may diagnostics and polling use plain documented constants,
  while memory, disk and loss detection stay measured?*
- **Test helpers:** about 34,700 of the 165,233 test lines in `internal/myce`
  are helper and fixture code outside any test function.

## Not verified

- No gate was run. Nothing here was deleted to see what breaks.
- The test analysis is Darwin only. The 23 `_linux_test.go` files were not
  analysed and are not listed, and a listed test may cover Linux-only code.
  B1's execution step checks both platforms for that reason.
- `cmd/myce` tests (347 tests, 24,318 lines) and the Python client tests were
  not analysed.
- Eight tests failed or timed out when run alone on the development host and
  were kept: `TestAttemptPendingCheckpointScanDoesNotFenceHost`,
  `TestCandidateCostPlacementEquivalence`, `TestCheckpointProgressEventDriven`,
  `TestModelSnapshotPinCleanupPrecedesAttemptRetirement`,
  `TestScarceHostYieldAdvisoryDoesNotMaskMemoryRelease`,
  `TestStatusViewDisabledAndResourceConstraints`,
  `TestStatusViewFloorFreeAutoMemory` and
  `TestWorkerRetentionNativeStartFailureAcknowledgement`. Whether they depend
  on test order or on this host is not known.
- Seam sizes and every figure marked "about" or "estimated" are estimates from
  reference counts, not from a trial deletion.
- Use figures have the census's limits: full job records expire 14 days after
  a job finishes, and the job list omits several authored fields.
- The model catalog is a queue startup file and was not read; "nothing loaded
  or pinned" comes from live status.
