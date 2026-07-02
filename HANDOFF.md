# Mycelium Public Handoff

Last updated: 2026-07-02

This file is public repo continuity, not an operator journal. Do not record
client/project names, private hostnames, private IP addresses, token names,
local filesystem paths, artifact paths, or run narratives here. Put that
material in ignored local notes such as `ops/private/*.local.md`, or keep it in
an out-of-repo private handoff.

## Current Repo State

- Branch: `main`.
- Source-of-truth docs remain `01-project-spec.md`,
  `02-testing-architecture.md`, and `03-development-guide.md`.
- `04-engine-bootstrap.md` is a future/extension spec. It should guide future
  bootstrap work, but it is not part of the current MVP proof gate unless it is
  explicitly promoted.
- Use local gates; the default repo proof is `make ci`.

## Product Shape To Preserve

Mycelium is a peer-native, LAN-local inference control plane:

- `mycelium` is one peer daemon. There is no permanent server/node split.
- Any peer may coordinate a request.
- The selected resource owner commits leases and capacity locally.
- Registry replication is for visibility and rescue, not global authority.
- There is no fleet leader, consensus event log, SSH product transport, public
  WAN requirement, or model sharding across machines.
- Runtime placement must go through scheduler/coordinator plus owner admission.
  Do not reintroduce direct coordinator mutation of remote state.

## Implemented Public Surface

- Gateway: OpenAI-compatible `/v1/chat/completions`, route diagnostics headers,
  strict JSON/structured-output passthrough, tool-call diagnostics, streaming
  and non-streaming preflight evidence.
- Control CLI: `myce gateway preflight`, `myce gateway routes`,
  `myce route certify`, `myce route status`, `myce debug job`,
  `myce debug bundle`, `myce jobs admit`, `myce jobs status`,
  `myce jobs drain`, `myce jobs finish`,
  `myce telemetry samples`, `myce calibrate plan`, `myce calibrate run`,
  `myce calibrate status`, `myce calibrate report`,
  `myce calibrate apply-priors`, benchmark commands, model and recommendation
  inspection commands.
- Service operations: `mycelium service plan`, `install`, `status`,
  `uninstall`, and dry-run-only `upgrade` for binary backup/replacement,
  restart, verification, and rollback command evidence without mutating the
  host. Generic operator runbooks live under `docs/runbooks/`.
- Runtime proof surfaces: owner telemetry, run/session metrics, job registry,
  route headers, preflight artifacts, debug bundles, and optimization summaries.
- Scheduler/runtime: owner-authoritative admission, resource estimation,
  context/KV accounting, disk-headroom filtering, registry compaction,
  stale-job cleanup, peer diagnostics, and backend adapter coverage.
- Bootstrap/catalog: model catalog, engine readiness planning, compatibility
  reports, and dry-run engine install planning.

## Consumer Evidence Contract

Consumers should preserve secret-safe evidence, not private prompts or raw
completions:

- request route: gateway URL class, model or alias, project label if applicable,
  max tokens, streaming mode, structured-output mode, tool mode, and declared
  minimum context requirement;
- response headers: `X-Myc-Job`, `X-Myc-Node`, `X-Myc-Backend`,
  `X-Myc-Instance`, `X-Myc-Decision`, timing headers, structured-output/tool
  diagnostics, and retry/attempt fields;
- outcome: HTTP status, provider finish reason, content length, reasoning
  length, JSON parse status, tool-call count/names, textual-tool-call flag,
  token usage when available, consumer elapsed time, and sanitized consumer
  error class;
- quality: sanitized consumer validation result and quality gate, such as
  schema validity, parser success, or review status.

Use `myce gateway preflight --out <dir>` to write a redacted `preflight.json`.
Use `myce debug job --preflight <preflight.json> --owner-telemetry` or
`myce debug bundle --preflight <preflight.json> --owner-telemetry` to join
request-time evidence to owner-side timing and durable job state.
Use `myce route certify <contract-name>` and `myce route status` when the
operator wants a named contract with a default evidence home instead of a
one-off preflight command. Route contracts are generic JSON files that capture
model/alias, context, JSON/tool/streaming, finish/content, live-catalog, and
safe consumer-label gates; saved artifacts remain redacted preflight evidence.
Use `myce calibrate plan` for project-free synthetic calibration planning. It
reads inventory, writes a secret-safe fleet benchmark config and calibration
plan artifact, and validates the plan with the existing deterministic fleet
simulation. `myce calibrate run --simulate` executes that generated plan without
starting live engines or changing route policy. `myce calibrate status` and
`myce calibrate report` summarize saved artifacts and keep simulation distinct
from live calibration or optimizer defaults. `myce calibrate apply-priors`
refuses simulated artifacts and imports only live benchmark metric rows into
speed-class priors. The plan artifact also records the local queued-executor
smoke command plus the explicit `myce benchmark fleet` command for a later
operator-approved live pressure proof; running live fleet pressure remains a
live gate.

For thinking/tool-agent workloads, do not treat a low-context tool-call smoke as
a consumer-ready route. Use `--context-cap` and `--min-context-cap` so Mycelium
records `consumer_contract_context_too_small` when the route cannot honestly
fit the workload.

## Allocator Proof Boundary

Reachability canaries prove the transport, not the allocator. A real allocator
proof needs a workload with intent fields, concurrent pressure, queue or
preemption decisions, owner-side admission, periodic drain/reclaim, and durable
telemetry that shows which node owned each job.

The gateway constructs jobs with project, priority, speed, context,
concurrency, preemption, streaming, submitter, and handling intent. The
scheduler service supports queued work, owner-authoritative admission, and a
periodic queue drainer. Owner admission enforces the warm-instance
expected-concurrency envelope at offer and commit time, so a warm instance
sheds instead of accepting unbounded extra work. OpenAI-compatible chat
requests remain synchronous:
when the selected route cannot immediately acquire an owner, the gateway
returns `503 Service Unavailable` with `Retry-After` instead of accepting an
asynchronous workload.
Use the authenticated `/workloads` RPC surface,
`myce jobs admit/status/drain/finish/result`, and fleet benchmark surfaces for
pressure proofs that need queued admission without turning chat completions into
a hidden blocking queue. The workload surface admits durable job intent,
carries redacted payload/result metadata, classifies whether the queued payload
is registry-replayable or only runtime-local, and reports whether the payload
has an explicit executable gateway-request contract such as `openai_chat`.
It drains through the scheduler queue, marks terminal state through the
scheduler release path, and exposes terminal scheduler evidence through
status/debug surfaces. Workload responses also expose `execution_evidence`:
queued/running/terminal state, `payload_contract`, `payload_kind`,
`payload_executable`, whether provider output was captured automatically, and
whether an explicit stored result body is replayable.
Terminal workload evidence may carry a redacted result artifact manifest:
reference, media type, byte count, and digest. If a workload is finished with
explicit stored result bytes, `myce jobs result` can retrieve those bytes
without exposing them through status/default debug output. `myce jobs drain
--execute` is the first automatic queued provider-output path: it accepts only
non-streaming registry-replayable executable payloads, runs them through the
gateway/provider execution seam after scheduler/owner admission, stores the
provider response body as a workload result, and finishes/releases the job
through the scheduler. Workload admission now persists non-private payloads as
registry-replayable records when the local store supports the job registry, so
the normal `myce jobs admit --payload-kind ... --payload-file ...` operator path
feeds `myce jobs drain --execute` without hand-seeding registry state. The fast
test suite includes a repo-local HTTP workload proof for queued admission,
multi-owner drain, explicit provider execution, terminal success/failure,
result artifact manifest metadata, explicit stored result-body replay,
executable-payload contract classification, execution evidence, and lease
reclaim. `make smoke-workload-executor` adds a local loopback operator canary
over the real `myce` command path and a fake OpenAI-compatible provider; it does
not start real engines or mutate live services.
The live service-upgrade path has been exercised through the generic dry-run
upgrade packet, binary backup/replacement, restart wait, service status,
peer-diagnostics, and live catalog checks. Keep rollback-ready upgrade
discipline for future activations.
The project-free live benchmark path currently proves live cold placement and
warm reuse for one owner through the synchronous gateway. It does not yet prove
full allocator pressure: the current synchronous benchmark cannot safely express
hard-preemption intent on deployments that do not trust control headers, and
the explicit workload-lane pressure attempt can be blocked when an owner store
returns a persistent SQLite busy error during admission. File-backed SQLite
control stores now open with busy timeout, foreign keys, and WAL mode through
driver pragmas so owner admission, registry replication, telemetry, and
workload result capture do not run on the default rollback journal. Treat the
next live gate as: activate that store fix on the selected owner peers, verify
the owner admission path no longer returns persistent SQLite busy, then run an
explicit workload-based pressure proof that shows queued admission,
preemption/drain, provider execution, result capture, and release/reclaim.
Registry rescue is scoped to durable async work with replayable payload
references; foreground streams that die with their coordinator are interrupted
work, not transparently resumed streams. Streaming queued provider replay
remains a separate product gate.

## Public Hygiene Rules

- Public docs may say "strict JSON consumer", "tool-calling consumer",
  "project-scoped token", "example project", and "LAN peer".
- Public docs must not name private consumers, customer/project names, private
  token env vars, private hostnames/IPs, local user paths, artifact paths, or
  one-off run IDs.
- Test fixtures should use generic project/model/node names unless the name is
  a real public hardware or API term.
- Live-fleet facts are drift-prone. Verify live state before claiming a route,
  model, node, or backend is currently healthy.

## Next Public Work

- Keep improving generic route discovery, preflight, debug, and optimization
  evidence surfaces.
- Keep the Mac service activation path documented as generic commands with
  rollback/readiness checks, not host-specific narratives. The public runbook is
  `docs/runbooks/service-upgrade.md`.
- Keep service upgrade activation behind an operator maintenance window; the
  repo-local `service upgrade --dry-run` surface prints the command plan and
  binary hashes but does not switch a live service.
- Add a workload-based live pressure benchmark or run packet that uses the
  explicit workload lane for hard-preemption/queue/reclaim proof instead of
  relying on synchronous gateway control headers.
- Investigate and fix owner-store lock contention when admission returns
  persistent SQLite busy errors under live pressure.
- Move any private operational evidence into ignored `ops/private/*.local.md`
  notes or an out-of-repo private handoff, not tracked docs.
