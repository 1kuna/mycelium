# Mycelium v2 general engines and measurement

Branch: `v2-engines`, created from clean local `main` at
`3c2afae6b90be95b0678c7f3f4e535cd61a81f43`.
Read AGENTS.md, the approved design, environments and model-call guides, the
previous environment report and the migration preparation gaps before editing.
The user authorized server/measurement extensions; the protected design and
AGENTS.md were not rewritten.

## Independent environment review

An independent T3 child reviewed the environment/install commit it did not write.
It fixed Linux NAS mount parsing, preserved injected doctor runners, made service
catalog paths absolute, failed missing service runners before install writes,
validated setup actions, and removed an unused probe field. Doctor now checks
Linux user lingering with `loginctl show-user UID --property=Linger --value`;
only `yes` passes. It explains `loginctl enable-linger UID` and login/logout/boot
behavior without enabling it or elevating. Tests cover injected yes/no/error,
unknown and missing-runner cases; Linux mounts and CLI catalog resolution.

The independent reviewer reproduced a build-only launcher with a stale partial
path in its shebang. The integration fix extends textual relocation to `bin`
as well as `venv/bin`; a real process regression executes its published launcher.
No broad environment architecture refactor was justified by reproduced defects.

## Implemented

- `server` catalog engine: literal argv templates for start/optional stop,
  loopback health URL, readiness/grace timeouts, declared memory, named software
  environment, device selector and supported per-process device environment.
- Shared owned-process supervision: gated startup identity records, readiness
  checks, log files, crash/timeout cleanup, TERM/KILL escalation, saved process
  groups and restart cleanup. Stop helpers receive their own gated durable
  identities and their descendant groups are always cleaned/verified.
- Environment platform recipes travel in queue load plans. Worker acquisition
  holds a shared use lock through load, calls, drain and unload. Failed unload
  retains the reservation/lease. Batch and model processes never inherit token.
- Authenticated `/models/NAME/PATH` forwarding with arbitrary method, body
  format, escaped path, query, end-to-end headers, response status and streaming.
  Routing credentials and hop-by-hop headers are consumed at both boundaries.
  Redirects pass through rather than being followed by the proxy.
- Existing OpenAI routes remain available. llama.cpp uses the general supervisor;
  the vLLM preset supplies local model directory, served name/catalog alias,
  maximum context, KV dtype, GPU utilization and exact extra parser arguments.
  Remote Hugging Face/transformers loading is disabled, without changing math.
- Verified immutable multi-file manifests for HF-style models. Bundles use
  aggregate identity, admission/reservations, protections, telemetry, pre-copy,
  resumable per-file prefixes, verified atomic directory publication and
  recoverable eviction. Missing/unlisted/colliding files, traversal and symlinks
  fail. No downloads or format/quantization conversion occurs during load.
- `join --device`: model eligibility respects catalog physical device; batch and
  engine selectors are per-process. CUDA selectors resolve to stable physical
  UUIDs, used by native inventory and execution. XPU admission remains a fixed
  reviewed allowance; no claim of live external-allocation arbitration.
- Local `myce measure`: exact join identity/spool lock, production refusal unless
  force, native selected-card CUDA/XPU used memory plus Linux host usage,
  simultaneous residency through sequential loads/optional warmups, per-model
  incremental peaks/load times and combined absolute/delta peaks. All attempted
  engines unload in reverse order, including failures; failed cleanup fails the
  report. Existing live engine identities/unfinished execution refuse admission.
  Completed records permit later attempts. Cache eviction is blocked during
  measurement. JSON and plain reports are written together, never overwritten.
- Updated model/general server, job, integration, operations and production
  guides. The separate B70 GPU0 maintenance checklist specifies exact model and
  runtime prerequisites, native counter/card identity, owner coordination,
  representative warmup, report interpretation, cleanup and restoration.
- Added an intentionally nondeployable Atlas Ornith FP8 vLLM example with the
  historical 262144 context, FP8 KV, 0.65 utilization, qwen3 reasoning and
  qwen3_coder tools. Immutable manifest and actual memory values remain placeholders.

## Verification

Final checks passed on the implementation tree:

- `go build ./...` and `go vet ./...`.
- `go test ./... -race`: both packages passed (cmd 24.077 s, internal 88.587 s).
- Full Python suite: 9 tests, zero skips, Python 3.12.14, OpenAI 3.26.1 and
  PyYAML 6.0.3 installed in a temporary virtual environment. Final run 32.535 s.
- `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build` and the corresponding arm64
  cross-build, binaries under `/tmp/myce-v2-engines-build`.
- `git diff --check` and explicit staged-path review.

One full race attempt exposed the stop-helper test's inherited 50 ms startup
race: cleanup occurred before the test could write its marker. The second
independent reviewer reproduced it 5/20 times. The descendant test now permits
2 seconds for startup, while a separate 150 ms test proves helper timeout/KILL
and zero-PID durable cleanup. Both helper regressions passed 10 repeated race
runs; the subsequent complete race suite passed. No valid failing test was
removed or weakened to conceal a runtime defect.

Focused laptop checks cover actual child HTTP servers, PATCH bodies/headers/query,
escaped filenames, flushed stream chunks, authentication without scheduling,
readiness timeout, load crash, device variables, ignoring TERM, stop helper
orphan cleanup, environment-backed startup, vLLM command contract and stable CUDA
identity resolution. Existing model scheduler fixtures now use a server catalog
entry, covering load/order/affinity, pins/windows, drain/idle, priority reclamation,
preemption, leave, loss, stale sessions, retry, wait and unload failures with fake
clocks. Measurement tests use injected native-like readings and fake engines,
cover joint peaks/fit, warmup, failure cleanup, retained failed-unload leases,
queue/production refusal, lock refusal and invalid readings. Bundle tests cover
resume/integrity/cache limits/protection/inventory and recoverable eviction.

The independent final review found five integration defects: missing authentication
on the new route, decoded-path corruption, measurement lease release on failed
unload, escaping stop-helper descendants, and numeric CUDA ordering mismatch.
Each was fixed with a regression. The second independent review confirmed the five implementation fixes, found no
additional concrete runtime blocker, and found the regression timing defect
resolved above. The final complete race result closes that validation finding.

## Limits and remaining migration gates

No SSH, fleet hardware, real service installation, LibreOffice, deployment or push.
Package dependencies and build outputs stayed in temporary storage. No real model
was configured/evaluated, so current official model/runtime contracts and actual
B70/Spark engine behavior remain founder-run qualification gates.

Request bodies are replay-buffered with an explicit 64 MiB limit; responses stream.
Request-body streaming and WebSockets are unsupported. Generic transport loss can
retry once before response bytes, so mutating endpoints require idempotency.
After bytes start, generic failure aborts the connection without injecting SSE.
Peak readings are system-level sampled usage, not allocator/per-process maxima;
100 ms is the nominal interval and native probes can be slower. Per-model deltas
are order-dependent; sequential warmup does not prove concurrent workload peaks.
Actual multi-file cross-filesystem recovery, driver counters/device isolation,
GPU memory fit, latency, host pressure and production rollback were not exercised.

Application-specific speech/face server wrappers, immutable runtime attestation,
exact encoder parity, native XPU admission guards and coordinated existing-owner
cutover remain application/hardware work. No immediate-start or GPU0 joint-fit
promise is made. Failure to fit requires a capacity/product decision, never a
silent GPU1 move, changed context or model fallback.

## DIGEST
Added general HTTP servers and a vLLM preset on v2-engines.
Kept server models on the existing cache, pin, priority and recovery lifecycle.
Added verified multi-file weights and per-worker device selection.
Added local joint-model measurement with JSON and plain reports and cleanup.
Fixed environment/install bugs and added Linux lingering checks and guidance.
Fixed five independent review findings with regression tests.
Updated guides, an unqualified Atlas example and the B70 weekend checklist.
Go build, vet, race tests, all Python tests and both Linux builds passed.
No real machines, services, LibreOffice or pushes were touched.
GPU0 joint fit and real model/runtime qualification remain untested.
