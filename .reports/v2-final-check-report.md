# Mycelium v2 final pre-production check

Reviewed main at `c6299d60e789cf547a7efdc507360b700c612cd0`; fixes on
`v2-final-check`. Read AGENTS.md and the approved design before implementation.
All execution stayed on the Mac: no SSH, fleet access, service installation,
LibreOffice, production inference, or push. Deployment commands below the docs
boundary were inspected as text and shell syntax, never executed remotely.

## Adopter walkthrough

Started from README.md and docs/integrating.md, built `/tmp/myce-step3`, ran the
real dev queue/command runner at loopback:7331 with one 8 GiB fake worker and a
5-second fake-model idle timeout. Executed the integration shell blocks,
including batch submission, embeddings, resumable experiment/production
preemption, logs, and status. Installed the Python distribution into a fresh
Python 3.13 virtualenv, submitted another batch through Client, and made a chat
call using the installed OpenAI package. Installed versions included
mycelium-client 0.1.0, OpenAI 3.26.1, and PyYAML 6.0.3.

Friction found and fixed:

- Python setup appeared after examples that already required it. Neither the
  quick start nor the integration path created a virtualenv. Added an early,
  copy-paste private environment/install block and checkout working directory.
- This Mac's `python3` is Apple Python 3.9, below the client's 3.10 minimum.
  The original generic install launched old pip dependency backtracking; it was
  stopped, and a new Python 3.13 environment installed successfully. Docs now
  state 3.10+ and show `python3.13`; no system Python changes were made.
- An inherited queue/token could point CLI examples at another queue or make
  dev use an unexpected token. The quick start now explicitly sets loopback
  MYCE_QUEUE and dev-token; integration dev explicitly selects dev-token too.
- The resumable walkthrough guessed startup readiness with `sleep 2` and only
  printed final saves. It now waits for a logged complete checkpoint and prints
  the inherited unit at startup. Observed attempt 1 saved unit 11, production
  completed, and attempt 2 resumed unit 11 and reached 100. Both jobs succeeded.
- Added a standalone standard-library checkpoint/stop diagnostic for the
  production checklist, instead of requiring an undocumented project command.
  Through the real dev runner it saved unit 5 on stop, then attempt 2 resumed
  unit 5 and checkpointed unit 6; explicit cancellation cleaned up that test.

The successful installation and executable demonstrations are comfortably
bounded for a 15-minute local adoption path with prerequisites installed. This
is not a measured cold-network/toolchain installation SLA. Model replies in dev
are explicitly fake; no real inference quality or GPU performance is claimed.

## Step 3 review findings and fixes

1. **CUDA admission combined separate devices into one memory pool.** Two
   devices with 8 and 4 GiB free advertised 12 GiB, despite no device-selection
   contract or model splitting. A 10 GiB job could be admitted while fitting
   neither device. Inventory now rejects multiple devices visibly. This avoids
   inventing device routing during the final check. Single-GPU Spark/4090
   inventory remains supported; multi-GPU support is an explicit limitation.
   Regression coverage rejects numeric multi-row output even with a budget cap.
2. **A hung CUDA probe could stall the worker indefinitely.** The heartbeat and
   pre-start paths use background contexts. nvidia-smi now gets a two-second
   deadline, so a stalled probe reports unavailable memory and returns to the
   worker loop. A regression waits on cancellation and requires a bounded error.
3. **The queue template omitted the discovery runbook's GPU hiding/resource
   boundary.** Added PrivateDevices, NoNewPrivileges, GOMAXPROCS=1, Nice=10,
   CPUQuota=25%, MemoryMax=512M and TasksMax=64. Operations/deploy docs describe
   this and require checking actual hiding, not merely the unit text. Linux
   user-manager support and native service acceptance remain untested.
4. **The fleet instructions could be mistaken for a runnable Spark deployment.**
   Discovery reports unsupported unified-memory telemetry and active Atlas
   ownership; step 3's cap never overrides a failed live query. Documented a
   hard stop, real-model/runtime prerequisites, and no fake or host-RAM fallback.
   Discovery's older myce-v2 namespace differs from current myce templates;
   the plan explicitly selects one namespace and warns against mixing them.

Inspected priority-loss maintenance, fresh/returning sessions, attempt-specific
results/log acknowledgements, worker-local checkpoint roots, model reservation/
drain changes, and the full-system fault scenario. No additional reproducible
lost-job or stale-result acceptance defect was found in that review. Existing
fake-clock tests cover exact 5/20/60-second boundaries. Worker paths are resolved
from NAS-relative assignments; the different-root regression and full-system
scenario verify Linux/Mac aliases of the same shared directory.

The full-system acceptance test uses real HTTP, SQLite, processes/signals,
spools and checkpoint copies with simulated inventories/model engines. It
covers preemption, model drain, fleet silence, queue restart, NAS loss,
individual loss, checkpoint conflict reconciliation, and terminal-state
retention. Its evidence is laptop recovery, not hardware certification.

No scheduler layers, leases, peer registry, or replacement architecture were
added. Existing queue/worker paths and myce service names are separate from the
old B70 .mycelium binary/state and mycelium-real-fleet.service.

## Production checklist and limits

Added docs/production-test-plan.md with ordered commands and success criteria
for CPU-only B70 queue, CUDA Spark worker, bidirectional NAS writes, smoke,
real model call, worker loss/cable loss, restart/reboot, and recoverable undo.
Long private preparation commands live in operations.md. The only host reboot
is a separately scheduled Spark reboot; B70 recovery restarts just the v2 queue
because rebooting B70 would interrupt Verso and v1.

A lone quiet Spark deliberately triggers the network-silence hold, not blind
5-second failover. The plan distinguishes that cable test from loss detection
with another healthy worker. It never proposes a B70 worker. Disconnected job
attempts can overlap until reconciliation; no exactly-once guarantee for
external writes is possible. Outputs must be idempotent or attempt-isolated.

The production plan is prepared, not executed. Live fleet GPU memory, admission,
GPU-enabled llama-server availability, model identity/settings, NAS identity/
writes, systemd sandbox support, boot behavior, and undo remain founder gates.
Spark's discovered vLLM installation alone cannot serve this llama.cpp adapter.
These blockers were surfaced, not worked around or repaired remotely.

## Validation output

Required final checks (all exit zero):

```text
$ go build ./...
(no output)
$ go vet ./...
(no output)
$ go test ./... -race
ok  mycelium/cmd/myce       (cached)
ok  mycelium/internal/myce  24.177s
```

The initial uncached race run also passed: cmd/myce 23.633s and internal/myce
22.738s. The final internal suite includes the new hung-probe/multi-GPU
regressions and TestPreDeployFullSystem. No failing tests were removed.

```text
$ /tmp/myce-final-check-py313/bin/python -m unittest discover -s clients/python/tests -v
9 tests: OK
Ran 9 tests in 24.875s
```

Python coverage includes real dev batch success/failure/logs, resumable
preemption/reload, OpenAI SDK, YAML, auth/cancel/timeouts, paginated terminal
log draining, atomic checkpoints and stop-signal restoration.

Additional checks passed: CGO-disabled Linux amd64 and arm64 cross-builds;
zsh syntax for all shell blocks in integration, operations and production plan;
both plist parses; git diff --check. Linux unit execution was not tested on
this Mac. The standalone diagnostic's local stop/resume test is described
above. Local dev was stopped cleanly after verification; ignored database,
checkpoint and spool evidence was retained, not deleted or staged.

## DIGEST
The Mac walkthrough works: batch, OpenAI call, logs, and checkpoint resume.
Fixed Python setup, local connection defaults, and readiness instructions.
CUDA rejects pooled multi-GPU memory and bounds hung inventory probes.
The B70 queue template now hides GPUs and limits CPU and memory.
Added the founder test checklist and recoverable undo commands.
Go build, vet, race tests, and all nine Python tests pass.
Linux amd64 and arm64 builds pass; no fleet machines were touched.
Spark telemetry, GPU admission, and model runtime still gate production.
Nothing was pushed, installed as a service, or run through LibreOffice.
