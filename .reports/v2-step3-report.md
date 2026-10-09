# Mycelium v2 step 3: ready for founder production testing

Worked locally in `/Users/zach/Developer/mycelium`, on `v2-step3` created from
`main` at `afabdcf499556d6c004d3d64508dd3c59f20db1b`. Read AGENTS.md, the approved
`docs/design-proposal.md`, all three prior step reports, and fleet discovery
before implementation. The approved design is unchanged; this task's explicit
founder decisions supersede its older loss-time and identical-mount details.
No SSH, fleet installation/start/stop, real GPU/model execution, LibreOffice,
production deployment, or push occurred. All servers and subprocesses were local.

## Part 1: independent step-2 review and real fixes

Reviewed model admission, transport/retry/stream behavior, runtime load/unload,
shared batch/model memory, priority, and attempt/session ownership against the
design. Bounded T3-owned child tasks handled model review, the Python client,
docs, memory-source tests, and the full-system test; their changes were integrated
and verified together here. Findings with regressions:

1. Completed production calls left their historical priority on a model,
   protecting experiment calls and idle models from rightful preemption.
   Recompute protection from currently admitted calls on finish/retry; idle
   models become experiment/reclaimable. Retry also updates the old slot's idle
   timestamp. Tests prove deadline/production batches can drain them.
2. Batch-versus-call precedence counted models that the competing work could not
   reclaim, blocking calls behind impossible batches and potentially batches
   behind impossible calls. Count reclaimable memory and use `callCanUse` in
   worker assignment precedence, considering affinity, platform, exclusions,
   readiness, in-flight use and resumable lower-priority work.
3. A drain arriving while an engine loaded could be forgotten when readiness
   won the cancellation race. Remember the drain and unload after readiness;
   never admit fresh calls to that draining engine. In-flight queue and local
   call counts still protect existing calls.
4. Failed load cleanup could release accounting despite a retained engine.
   Preserve `unload_failed` and its reservation when cleanup fails. Never count
   failed-unload memory as reclaimable or overwrite that failure with draining;
   runtime close surfaces unload errors. Successful llama cleanup clears its
   command/address, making a repeated cleanup safe.
5. A cancelled caller could trigger placement/reload on another worker.
   Cancellation now returns without scheduling a retry. Existing retry/loss,
   response buffering, stale execution, and mid-stream failure tests remain.
6. An empty upstream SSE body lost its upstream status. Preserve the status even
   when no first bytes arrive. Partial streams still fail explicitly and are
   never silently replayed after response bytes have been delivered.
7. Native measured free memory would have counted resident model memory twice
   through the old budget-only subtraction. Distinguish measured reports from
   configured budgets; subtract only not-yet-reflected loading reservations
   from measured readings, and all own reservations from configured budgets.
   Regression tests cover both the helper and actual Checkin accounting.

## Part 2: founder decisions

- Defaults: production loss 5s, deadline 20s, experiment 60s. Queue flags are
  `--production-loss-timeout`, `--deadline-loss-timeout`, and
  `--experiment-loss-timeout`; positive values are required. Worker
  `--heartbeat` defaults to 1s. The highest-priority active batch/call/load sets
  its worker's threshold; idle workers use experiment. Fake-clock tests cover
  just-before/exact boundaries, custom thresholds, production model loading,
  and strict-majority silence. If most workers are silent, assignments remain
  held; restart grace and newest-checkpoint duplicate reconciliation remain.
- `join --checkpoint-root` (or MYCE_CHECKPOINT_ROOT) sets the worker-local NAS
  root. Assignment checkpoint paths are relative; the worker resolves them
  before setting MYCE_CHECKPOINT_DIR. The queue keeps its own absolute paths for
  durable state/reconciliation. Legacy unspecified roots use the queue path.
  Linux/Mac aliases of one temp NAS prove checkpoint copying across roots, both
  in a focused test and actual workers in the full-system test. Relative path
  traversal is rejected. AGENTS.md and both existing guides now match this rule.
- CUDA queries `nvidia-smi --query-gpu=memory.free` every report/start/load;
  values are converted from MiB to GiB. An optional positive memory-gb caps the
  live reading, and reserve-memory-gb is then subtracted. Missing/malformed
  readings fail visibly, with no substitution of total GPU or host RAM. XPU and
  Metal use an explicit conservative configured budget minus reserve; fake
  uses its configurable budget. All sources validate finite/nonnegative values.
  Worker starts recheck memory and hold an assignment while insufficient;
  model loads recheck and report failure. Configured accounting subtracts own
  batch/model usage once; native accounting does not double-subtract resident
  usage. Sources/commands are injectable, so tests need no GPU.
- Install templates use ~/.local/bin/myce, ~/.local/share/myce and
  ~/.config/myce, with myce-queue.service / myce-worker.service and
  local.myce.queue / local.myce.worker launchd labels/files. These coexist with
  legacy Mycelium paths/units. Queue and worker templates retain private token
  placeholders; none were installed. Stop policy lets non-resumable work finish.

## Part 3: adoption

`clients/python` is a Python 3.10+ pip-installable package (`mycelium-client`,
import `mycelium`). The core uses only the standard library. Optional YAML and
OpenAI extras use PyYAML and the standard openai package. Client methods submit
mappings/YAML, wait for terminal results, follow paged logs with cursors, cancel,
list/get jobs and read status. OpenAI helpers set project/priority/deadline and
disable automatic SDK replay by default. Checkpoint/StopSignal helpers implement
newest complete JSON loading, temp/fsync/rename/directory-fsync saves, and
cooperative SIGTERM/SIGINT handling outside the signal handler. Failed saves
remain diagnosable and ignored temporary files remain recoverable.

Added integrating.md, job-reference.md and operations.md. README links to all
three plus client, model and checkpoint guides. Integration covers installation,
private configuration, priorities/status, all three patterns, loss/retry, complete
runnable fake examples, Atlas indexing/search-embedding shapes, and Verso
production model calls/resumable experiment shapes. Actual project GPU entry
points and approved models are explicit substitutions, not invented APIs or a
claim that Atlas/Verso have migrated. Operations covers Linux/Mac lifecycle,
adding/removing workers, private template rendering, upgrades and recoverable
uninstall. Existing model/checkpoint guides were updated to eliminate stale rules.

## Part 4: laptop acceptance

Rerun before every deploy:

```sh
go test ./internal/myce -race -run '^TestPreDeployFullSystem$' -v -count=1 -timeout 110s
```

The integrated test uses real HTTP/SQLite/spools/process groups/commands, fake
CUDA/XPU/Metal inventories and model engines, three different NAS aliases,
batches/model calls across all three priorities, and an injected advancing clock.
It restarts the queue under a stable API endpoint, disconnects every worker,
finishes/spools work offline, isolates one worker with healthy peers, makes the
NAS backing directory unreachable by reversible rename, and plans a worker leave.
Assertions cover priority/preemption/front-of-line ordering, seven durable job
identities, one terminal command marker per job, stale/contradictory result
rejection, no terminal regression, no new starts without network/NAS, four
checkpoint resumptions, shared progress across different roots, ignored .tmp
files, and one complete model answer or clear timeout/failure. No GPU required.

Observed dedicated output (routine intentional partition logs omitted):

```text
=== RUN   TestPreDeployFullSystem
real API/SQLite/runner; cuda+xpu+metal; 7 jobs succeeded once; experiment resumed 4 times across priority, restart, network, NAS and leave faults; model calls answered once or timed out clearly
--- PASS: TestPreDeployFullSystem (8.09s)
PASS
ok  mycelium/internal/myce  9.118s
```

The test also passed three consecutive race runs in the test owner's check
(32.6s total), and in the combined race-suite runs. Under two minutes even with
race instrumentation. Earlier intermediate runs exposed a stopped fake clock
in the new test and were corrected by advancing its injected clock on loop wakes;
regression fixtures were corrected to tick maintenance after batch submission.
No valid failing production requirements were removed.

## Verification

All following commands exited 0 locally:

```text
go build ./...                         [no output]
go vet ./...                           [no output]
go test ./... -race -count=1 -timeout 110s
ok  mycelium/cmd/myce       22.980s
ok  mycelium/internal/myce  21.119s

/tmp/myce-step3-python-verify/bin/python -m unittest discover -s clients/python/tests -v
Ran 9 tests in 27.850s
OK

plutil -lint deploy/local.myce.queue.plist deploy/local.myce.worker.plist
both OK
git diff --check                       [no output]
```

Python verification built/installed the wheel with YAML/OpenAI extras into an
isolated /tmp Python 3.11 environment and used real `myce dev`; all nine tests ran
without skips. The client owner also proved core installation with no runtime
dependencies (optional tests skipped there). Focused model/engine, priority,
memory, and worker race tests passed. The docs owner's copy-paste walkthrough
proved Atlas batch, embeddings, production batch and checkpoint resumption; local
links, embedded script syntax and plist syntax passed. No boot service acceptance
or real model quality/performance result is claimed.

## Deferred and limitations

- Founder hardware testing and actual Atlas/Verso integration remain next. In
  particular Spark unified-memory nvidia-smi support must be checked: invalid or
  unavailable data closes admission visibly. XPU/Metal configured allowances
  cannot observe changing external allocations; keep reserves current. Memory
  admission is not a replacement for Atlas/Verso's GPU ownership controls.
- Partitions cannot carry stop signals. Copies can overlap until reconnection;
  checkpoint winner selection and idempotent terminal acknowledgements do not
  promise exactly-once arbitrary external side effects. Projects must make
  outputs retry-safe. NAS timestamps, not semantic work units, select progress.
- HTTP model requests are not durable across queue process death; clients fail
  clearly and decide whether to resubmit. Started streams cannot safely replay.
- Mount probing checks access, not remote-share identity. Operators must prevent
  an unmounted NAS from becoming a writable local substitute.
- No vLLM, automatic environment installation, new registry/network architecture,
  production migration, real model download/evaluation, or spool retention policy
  was added. These existing boundaries remain explicit.

## Ready for production test checklist

- [x] Local build, vet, full race suite, Python dev suite and predeploy scenario pass.
- [x] v1-coexisting templates, integration/reference/operations docs and report exist.
- [ ] Founder chooses a bounded real-machine test and authorizes its installation.
- [ ] Verify NAS share identity/write/rename across all configured roots and keep
      SQLite/spools local; preserve v1 services/data and private credentials.
- [ ] Verify native CUDA free-memory readings or safe XPU/Metal reserve and agreed
      Atlas/Verso GPU ownership/admission; no bypass or CPU/model fallback.
- [ ] Verify actual model card/inference contract, engine GPU build, proxy/catalog
      matching, and installed batch commands/environments before test submission.
- [ ] Observe real result/checkpoint/leave/restart behavior before production use.

## DIGEST
Reviewed model calls and fixed priority, cancellation, cleanup and load races.
Added configurable 5s/20s/60s loss timing and one-second worker check-ins.
Workers resolve shared checkpoints through their own NAS mount paths.
CUDA measures free memory; XPU and Metal use explicit budgets and reserves.
Added the Python client, integration/reference/operations guides and v1-safe templates.
The laptop fault test completed seven jobs once and resumed checkpoints four times.
Build, vet, Go race tests and all nine Python tests passed.
Ready for founder production testing; no fleet access, LibreOffice or push.
