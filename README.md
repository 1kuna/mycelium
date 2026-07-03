# Mycelium

Mycelium is a hardware-aware inference control plane for a personal/local fleet.
It lets one person run local models across heterogeneous machines such as a Mac,
DGX Spark, Intel Arc B70, or other GPU boxes without making every project know
which machine is free, which backend is warm, or which model should be loaded.

The product shape is intentionally simple:

- `mycelium` is the daemon. Every machine runs the same peer binary.
- A peer can be `compute: true` and own local accelerators/backends, or compute-off and only submit/coordinate work.
- Any peer can receive a request, gather a fleet snapshot, choose placement, and ask the selected resource owner to commit.
- The resource owner is authoritative for local leases and capacity.
- There is no permanent leader, no SSH product transport, and no model sharding across machines.
- `myce` is the operator/control CLI.

The goal for v0.1 is a robust personal compute cluster: local models, automatic placement, queueing, telemetry, and enough debug evidence to fix real issues quickly.

## Source Of Truth

Read these first when implementing or auditing:

1. `01-project-spec.md` - product/resource model and locked decisions.
2. `02-testing-architecture.md` - interfaces, mocks, conformance, and test tiers.
3. `03-development-guide.md` - phase order and gates.

Those three docs are the contract. Do not edit them casually. Implementation decisions and unresolved blockers live in `DECISIONS.md` and `BLOCKERS.md`.

## Mental Model

A request enters a gateway-compatible peer:

1. The peer resolves the requested model/preset and project defaults.
2. The coordinator gathers a fleet snapshot from known peers.
3. The scheduler estimates weights/KV, filters by fit/policy, scores candidates, and returns a `PlacementDecision`.
4. The selected owner commits the lease locally.
5. The owner reuses a warm instance or loads a backend.
6. The gateway proxies the request and returns `X-Myc-*` decision/debug headers.
7. The owner records run/session telemetry for later debugging and recommendations.

The important invariant: the coordinator decides, but the resource owner commits.

## Common Commands

Run a peer:

```bash
mycelium run --config ~/.mycelium/peer.json
```

Bootstrap a peer from a join URI:

```bash
mycelium bootstrap --join "$MYC_JOIN_URI" --rpc-token "$MYC_RPC_TOKEN" --compute auto --apply
```

Start from an existing config:

```bash
mycelium run
```

List known models/presets:

```bash
myce models list
```

Inspect telemetry samples:

```bash
myce telemetry samples --project <project-id> --limit 20
```

Check whether the local peer/fleet evidence looks usable:

```bash
myce doctor
myce status
```

Inspect one request/job and collect a redacted support bundle:

```bash
myce debug job <job-id>
myce debug bundle --job <job-id> --out /tmp/mycelium-debug
```

Plan a durable service install without changing the host:

```bash
mycelium service plan --config ~/.mycelium/peer.json --name real-fleet --user
```

Plan a binary upgrade without changing the host:

```bash
mycelium service upgrade \
  --config ~/.mycelium/peer.json \
  --binary /path/to/stable/mycelium \
  --target-binary /path/to/new/mycelium \
  --name real-fleet \
  --user \
  --dry-run
```

See [docs/runbooks/service-upgrade.md](docs/runbooks/service-upgrade.md) for
the full maintenance-window checklist, read-only macOS diagnostics, and
rollback proof path.

Generate and inspect recommendations:

```bash
myce recommendations generate --project <project-id>
myce recommendations list --project <project-id>
```

Run a quick gateway benchmark:

```bash
myce benchmark run \
  --url http://<gateway-host>:<port> \
  --prompt "Say hello." \
  --model <model-or-alias> \
  --out /tmp/mycelium-bench
```

Generate a secret-safe synthetic calibration plan from inventory before running
live fleet calibration:

```bash
myce calibrate plan \
  --config ~/.mycelium/peer.json \
  --out ~/.mycelium/calibration/plan

myce calibrate run \
  --plan ~/.mycelium/calibration/plan \
  --simulate \
  --out ~/.mycelium/calibration/runs

myce calibrate status \
  --plan ~/.mycelium/calibration/plan \
  --run ~/.mycelium/calibration/runs

myce calibrate report \
  --plan ~/.mycelium/calibration/plan \
  --run ~/.mycelium/calibration/runs \
  --out ~/.mycelium/calibration/report.json
```

After an operator-approved live fleet benchmark, import live metric rows as
optimizer speed-class priors:

```bash
myce calibrate apply-priors \
  --plan ~/.mycelium/calibration/plan \
  --run ~/.mycelium/calibration/live-runs
```

`calibrate plan` reads the peer config and control store inventory, emits a
`benchmark-config.json` plus `calibration-plan.json`, and proves the generated
plan through the existing fleet benchmark simulator. The first implementation
is deliberately simulation-first: it records candidate cold/warm/pressure
sweeps, ready nodes, selected presets, safety notes, the local queued-executor
smoke command, the simulation run command, and the explicit
`myce benchmark fleet` command for a later operator-approved live pressure
proof. It does not write token values, change routing policy, start engines, or
apply optimizer defaults. `calibrate run --simulate` executes that generated
plan through the existing scheduler/allocator benchmark path. `calibrate
status` and `calibrate report` summarize saved plan/run artifacts without
treating simulation as live calibration or applied optimizer facts. Run
`make smoke-workload-executor` before the live fleet proof to catch local
queued-executor wiring regressions without touching real engines. A live
pressure proof remains an explicit operator gate. `calibrate apply-priors`
refuses simulated runs and live runs with no metric rows; it imports only live
benchmark metric rows and updates speed-class priors through the existing
optimizer calibration path. See
[docs/runbooks/workload-pressure-proof.md](docs/runbooks/workload-pressure-proof.md)
for the full proof ladder.

For authenticated non-loopback gateways, `myce benchmark` resolves the gateway token in this order:

1. `--gateway-token`
2. `--gateway-token-env`
3. `MYCELIUM_GATEWAY_TOKEN`
4. `MYCELIUM_API_KEY`
5. `~/.mycelium/peer.json` field `gateway_token`

`OPENAI_API_KEY` is not used as a Mycelium gateway bearer token by default. If
an OpenAI-compatible consumer environment intentionally stores a Mycelium
gateway token in that variable, pass `--gateway-token-env OPENAI_API_KEY` so
the auth source is explicit in command output.

## Calling The Gateway

The gateway accepts OpenAI-style chat requests:

```bash
curl http://<gateway-host>:<port>/v1/chat/completions \
  -H "Authorization: Bearer $MYCELIUM_GATEWAY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "<model-or-alias>",
    "messages": [{"role": "user", "content": "Say hi."}],
    "max_tokens": 32
  }'
```

Project consumers should first run the secret-safe preflight. By default it
sends one non-streaming strict JSON-schema chat request, prints route/usage
evidence, and never prints the bearer token:

Before preflight, operators can inspect the route contract a peer config
advertises without exposing token values:

```bash
myce gateway routes --config ~/.mycelium/peer.json
myce gateway routes --config ~/.mycelium/peer.json --json
myce gateway routes --config ~/.mycelium/peer.json --live
myce gateway routes --config ~/.mycelium/peer.json --min-context-cap <minimum-for-this-workload>
myce gateway routes --config ~/.mycelium/peer.json --require-tools
```

The output joins projects to presets and reports the OpenAI base URL, model or
alias, node, backend, context length, project context cap, auth source class,
structured-output likelihood, streaming likelihood, and vLLM tool-parser flags.
It only reports whether a project token or global gateway token is configured;
it never prints token material. It also reports `tool_readiness` and
`readiness` so operators can distinguish a config-valid route from a route that
is actually ready for the requested workload class. For a shared model alias
with multiple owner envelopes, `group_envelopes` reports each preset's owner,
backend, and context cap so consumers can choose a request envelope without
per-node route lore. With `--live`, the command
performs an authenticated read-only `GET /v1/models` against the configured
gateway (or `--url`) and annotates each configured route with `live_model` /
`live_match`. Treat plain route output as catalog/config discovery, and `--live`
as live catalog exposure proof. A route is consumer-ready only after the
matching `myce gateway preflight` and, for post-hoc work, `myce debug job
--preflight` or `myce debug bundle --preflight` evidence exists.
Use `--min-context-cap` to classify routes against the actual consumer workload
before running it. For example, an 8192-token route can be valid transport/tool
smoke and still print `context_issue=undersized_context` for a reasoning-heavy
tool-agent workload. Add `--require-tools` or `--require-auto-tools` when the
consumer needs OpenAI-compatible tool-call support; routes without the required
vLLM tool-parser flags then report `readiness=tool_flags_missing` or
`readiness=auto_tool_flags_missing` instead of looking interchangeable with
plain chat routes.

External tools that run their own worker pools should not guess worker counts
from a project batch size. Ask Mycelium for a launch envelope first:

```bash
myce gateway allocate \
  --config ~/.mycelium/peer.json \
  --model <model-or-alias> \
  --context-cap <required-context> \
  --desired-workers <throughput-target> \
  --live \
  --out /tmp/mycelium-allocation
```

`gateway allocate` reads the same route catalog, optional live `/v1/models`
catalog, context caps, capabilities, and provider launch envelope evidence that
the gateway uses for routing. It returns the OpenAI-compatible base URL, model,
allocated worker count, accepted owner envelopes, refused envelopes with
reasons, and a generic external-tool contract. With `--out`, it also writes a
secret-free `allocation-plan.json` plus a shell-sourceable `runner.env` with
`MYCELIUM_OPENAI_BASE_URL`, `MYCELIUM_MODEL`,
`MYCELIUM_ALLOCATED_WORKERS`, `MYCELIUM_CONTEXT_HEADER`, and
`MYCELIUM_CONTEXT_CAP`. For a CLI-style tool, source the generated env file,
use the returned worker count for its worker argument, and send the returned
context cap as `X-Myc-Context-Cap`. If the synchronous gateway later returns
`503` with `Retry-After`, pause the affected worker and honor the backpressure;
do not spawn replacement requests above the allocation. The allocation plan has
a short `stale_after_seconds` hint; refresh it before launch and after repeated
backpressure. Immediately before launching an external tool, verify the saved
plan:

```bash
myce gateway allocate --verify-plan /tmp/mycelium-allocation/allocation-plan.json --live
```

Verification exits non-zero when the plan is expired, the live catalog no
longer satisfies it, or the current allocation no longer covers the planned
worker count. The allocation report is not a held lease or reservation.
Consumers that need Mycelium to hold capacity should use the explicit workload
lane. After a workload is admitted and running with an active lease, a
non-streaming OpenAI-compatible gateway request can present the held capacity
with:

```text
X-Myc-Workload: <workload-id>
X-Myc-Lease: <lease-id>
```

The gateway validates that the workload is still running, the lease is still
active, the project/model/context envelope matches, and the reserved instance
is visible. A valid request routes through the reserved owner/envelope and
finishes/releases the workload lease when the provider response completes.
Stale, mismatched, expired, released, or terminal workload leases fail loudly
instead of falling back to best-effort placement. Streaming leased gateway
execution remains a separate product gate.

Consumers may send `X-Myc-Context-Cap` with gateway requests to declare the
context envelope actually required by a chunk or job. Mycelium treats that as a
resource requirement for placement, not as a privileged control-plane override:
project, priority, speed, preemption, submitter, and handling headers still
require an explicitly trusted control path.

When a placement decision queues or rejects owners, inspect the scheduler
evidence as reconciliation work instead of reading trace strings by hand:

```bash
myce reconcile placement --decision-file <placement-or-job.json>
myce reconcile placement --decision-file <placement-or-job.json> --save --db <control.db>
myce reconcile drain --db <control.db> --job <job-id>
myce reconcile status --db <control.db> --job <job-id>
myce reconcile placement --decision-file <placement-or-job.json> --json
```

The command accepts a saved placement decision, job JSON, or wrapper containing
a `decision`, then turns filter/drop evidence into typed tasks such
as `wait_capacity`, `prewarm_replica`, `verify_engine_readiness`,
`stage_model_artifact`, `repair_owner`, `refresh_catalog`, or
`record_incompatibility`. Each task says whether it is an automatic candidate
under the default standing policy or whether it still needs operator approval.
Use `--save` to persist those tasks into the control store as `allowed` or
`blocked` records. The workload RPC also records the same task records
automatically when admission or queue-drain evidence includes a scheduler
decision. When the peer's control store supports the reconciliation executor it
also drains the safe store-local task classes immediately:
`record_incompatibility`, `stage_model_artifact`, and
`verify_engine_readiness`. Live-affecting work such as prewarming replicas,
catalog refresh, and owner repair remains explicit and must not be marked done
unless the lower-level action actually succeeds.
`myce reconcile drain` consumes only `allowed` records,
records `running` to terminal status transitions, and currently executes
`record_incompatibility` by writing a durable incompatibility fact and
`stage_model_artifact` by writing a planned `models locality` stage action plus
a planned model-locality record. The stage task does not copy artifacts itself;
it feeds the existing `myce models locality apply` peer-RPC path.
`verify_engine_readiness` runs the existing strict engine readiness checker
against saved engine profiles and fails if a ready compatibility-keyed profile
is missing or unready. `prewarm_replica` can execute through the existing
workload admission/finish path when `reconcile drain` is given an explicit peer
endpoint:

```bash
myce reconcile drain --db <control.db> --job <job-id> \
  --prewarm-url <peer-url> --rpc-token <rpc-token>
```

The prewarm task is marked done only when the lower-level workload reaches a
terminal done result; queued, failed, or unavailable admission leaves the
reconciliation task failed with the owner/admission reason. `refresh_catalog`
currently fails loudly because there is no registered catalog source/refresh
contract for the reconciler to call. Blocked records are left untouched.
For a stored workload or gateway job, `myce debug job <job-id> --reconcile`
adds the same report-only plan to the normal job/debug evidence so operators do
not have to extract the scheduler decision by hand.

Gateways built from the route-discovery revision expose `GET /v1/models` with
preset IDs and aliases only; host-local model paths and artifact refs are not
returned. If `myce gateway routes --live` reports an authenticated `404` for
`/v1/models`, the running gateway binary is stale for catalog discovery even if
chat/preflight routes still work.

For repeated route classes, store the gates in a local route-contract file and
certify the route before a consumer uses it:

```json
{
  "schema_version": "mycelium.route_contracts.v1",
  "evidence_root": "~/.mycelium/evidence",
  "contracts": [
    {
      "name": "strict-json-general",
      "url": "http://<gateway-host>:<port>",
      "model": "<model-or-alias>",
      "project": "example-project",
      "context_cap": 32768,
      "min_context_cap": 32768,
      "require_json": true,
      "require_live_model": true,
      "expect_finish_reason": "stop",
      "min_content_length": 1,
      "max_reasoning_length": 0
    },
    {
      "name": "tool-agent-high-context",
      "url": "http://<gateway-host>:<port>",
      "model": "<tool-capable-model-or-alias>",
      "project": "example-project",
      "context_cap": 65536,
      "min_context_cap": 65536,
      "tool_name": "echo",
      "tool_choice": "function",
      "require_tool_call": true,
      "reject_textual_tool_call": true,
      "require_live_model": true
    }
  ]
}
```

Then run:

```bash
myce route certify strict-json-general \
  --contracts ~/.mycelium/route-contracts.json \
  --gateway-token-file <local-token-file>

myce route status --contracts ~/.mycelium/route-contracts.json
```

`route certify` delegates to `gateway preflight` and writes the redacted
artifact to `~/.mycelium/evidence/<contract>/<timestamp>/preflight.json` unless
the contract or command supplies a different evidence root. `route status`
finds the latest saved evidence for each named contract and re-runs the
preflight verifier offline. The contract file may contain synthetic prompts for
certification, but saved certification artifacts do not include bearer tokens,
prompts, or raw completion content. Keep contract names and labels generic
enough to be safe in support bundles. Set `retention_days` when a certification
must expire instead of remaining valid until the next manual preflight.

```bash
myce gateway preflight \
  --url http://<gateway-host>:<port> \
  --model <model-or-alias> \
  --project <consumer-canary-id> \
  --consumer-run <batch-or-request-id> \
  --consumer-item <row-or-document-id> \
  --config <local-mycelium-peer.json> \
  --out /tmp/mycelium-preflight
```

Add `--require-live-model` when preflight should fail unless the authenticated
live gateway catalog exposes the requested model or alias before the chat
request runs. This distinguishes a route that exists only in local config from
one the running gateway binary can advertise through `/v1/models`.
If that catalog check fails before a gateway job is created, the redacted
artifact can still be inspected with `myce debug job --preflight <preflight.json>`;
the report classifies the failure as catalog-layer evidence instead of owner
inference evidence.
If the chat request itself returns an HTTP error and `--out` is set, preflight
still writes a redacted artifact with the status, whitelisted route headers
when present, and a bounded gateway error string. `debug job --preflight`
classifies common availability failures such as `queued: no instance available`
as route availability evidence, not as a consumer prompt or validation failure.
When a local peer config/control store is supplied, the debug report also prints
`route-availability` with the desired preset, owner node, backend/profile,
context cap, known warm/loading instances, unfinished jobs, and the next
operator check. This keeps "configured route exists but cannot place" from
turning into log archaeology.

Token resolution for `myce gateway preflight` is:

1. `--gateway-token`
2. `--gateway-token-env`
3. `--gateway-token-file`
4. `--config <peer.json>` field `gateway_token`
5. `MYCELIUM_GATEWAY_TOKEN`
6. `MYCELIUM_API_KEY`
7. `~/.mycelium/peer.json` field `gateway_token`

`OPENAI_API_KEY` is intentionally not part of the default Mycelium gateway
token chain. Use `--gateway-token-env OPENAI_API_KEY` only when that environment
variable deliberately contains a Mycelium gateway token, not a provider API key.

For an OpenAI-compatible consumer, set the base URL to the Mycelium gateway and
put the Mycelium gateway token in an env var or local token file that the
consumer already reads. Local Mycelium operator commands can use
`MYCELIUM_GATEWAY_TOKEN`. Do not store token values in repo docs or run logs.

Useful response headers:

- `X-Myc-Job` - job id to pass to `myce debug job <id>`.
- `X-Myc-Decision` - placement action, such as warm reuse or cold load.
- `X-Myc-Node` - owner node selected for the request.
- `X-Myc-Instance` - model instance used.
- `X-Myc-Backend` - backend type.
- `X-Myc-Attempts` - gateway attempts/failovers.
- `X-Myc-Trace` - JSON placement trace explaining estimate/filter/select/score/admit.
- `X-Myc-Cold-Load` - whether the request cold-loaded a new instance.
- `X-Myc-Load-Wall-Ms` - cold-load wall time when a cold load happened.
- `X-Myc-Queue-Wait-Ms` - gateway queue wait before placement; currently `0` for synchronous gateway requests that reject queued placement.
- `X-Myc-Upstream-Wall-Ms` - non-streaming upstream provider wall time.
- `X-Myc-Structured-Output` - structured-output request mode seen by Mycelium, such as `json_schema`, `json_object`, `structured_outputs`, or `none`.
- `X-Myc-Guided-Decoding-Requested` - whether Mycelium sent a structured-output control to an OpenAI-compatible vLLM route. This is request-path evidence, not provider schema-cache proof.
- `X-Myc-Tools-Requested` - whether the request carried OpenAI-compatible `tools`.
- `X-Myc-Tool-Choice` - the request's tool-choice mode, such as `auto`, `required`, `function:<name>`, `auto_default`, or `none`.
- `X-Myc-VLLM-Tool-Parser-Configured` - whether the selected vLLM preset declares a `--tool-call-parser` launch argument.
- `X-Myc-VLLM-Auto-Tool-Choice-Configured` - whether the selected vLLM preset declares `--enable-auto-tool-choice`.

When `--out` is set, `myce gateway preflight` writes
`preflight.json` with the gateway URL, model, project, auth source label,
the redacted request contract (`max_tokens`, streaming mode, structured-output
mode, tool name/choice when a tool canary is requested), any preflight
expectation gates, whitelisted `X-Myc-*` headers, finish reason,
content/reasoning lengths, JSON parse status, usage availability, usage counts
when available, and streaming completion status when `--stream` is used. It
also accepts optional
`--consumer-run`, `--consumer-item`, `--consumer-status`,
`--consumer-elapsed-ms`, `--consumer-error`, `--consumer-result`, and
`--consumer-quality-gate` labels so a project thread can preserve the consumer
batch/item/outcome join once at request time. It
intentionally does not write the bearer token, prompt, or raw completion
content. Use that artifact, plus `myce debug job --preflight <preflight.json>
--owner-telemetry --json`, as the standard consumer evidence packet for
optimization handoffs. The debug JSON includes an `optimization` object that
joins the request contract, consumer labels, route headers, owner timing,
usage, finish/tool/JSON outcome, diagnosis class, and failure layer into one
secret-safe summary. If the preflight artifact already has consumer labels,
`debug job` and `debug bundle` inherit them automatically; explicit
`--consumer-*` flags override the artifact when an operator needs to correct or
enrich the handoff:

```bash
myce debug job \
  --preflight /tmp/mycelium-preflight/preflight.json \
  --config <local-mycelium-peer.json> \
  --owner-telemetry \
  --consumer-run <batch-or-request-id> \
  --consumer-item <row-or-document-id> \
  --consumer-status <ok|timeout|error|canceled|partial> \
  --consumer-elapsed-ms <client-elapsed-ms> \
  --consumer-error <sanitized-error-class> \
  --consumer-result <pass|fail|unknown> \
  --consumer-quality-gate <sanitized-quality-gate> \
  --json
```

Use `--consumer-error` for a short sanitized class such as `client_timeout`,
`schema_validation_failed`, or `response_parse_error`; do not put bearer
tokens, raw prompts, raw completions, or private document text in that field.
Use `--consumer-result` and `--consumer-quality-gate` for deterministic
consumer validation outcomes such as schema validity, parser success, or manual
review gate status. A failed consumer result is classified as
`consumer_quality_failed` when the owner-side inference otherwise completed,
so later optimization can distinguish "served quickly" from "served usefully."
When `--db` is omitted, `debug job --config <peer.json>` reads that config's
`store_path`; use `--db` only when intentionally inspecting a different control
store.

To inspect many saved preflight/debug-bundle artifacts without hand-assembling
one row at a time, run:

```bash
myce debug summary --dir <artifact-dir>
myce debug summary --dir <artifact-dir> --json
```

The summary command reads `optimization.json` and standalone `preflight.json`
artifacts under the directory, prefers bundle optimization summaries when both
exist, and prints secret-safe rows for route, model, owner node, preset,
route availability/startability, context gate, JSON/tool/streaming mode,
finish reason, content/reasoning lengths, usage availability, timing, owner
telemetry, gateway attempts, queue wait, live-catalog agreement, and failure
class. It does not print bearer tokens, prompts, completions, or artifact
paths.
Use filters such as `--failure-layer catalog`, `--node <node-id>`,
`--model <model-or-alias>`, `--context undersized`, `--live-model missing`,
`--quality-gate <sanitized-gate>`, `--route-status no_matching_instance`,
`--startability operator_action_required`, `--usage missing`, or `--cold cold`
to narrow the rows. Use `--sort latency`, `--sort usage`, or `--sort context`
for the common optimization views, and `--group-by failure-layer`,
`--group-by quality-gate`, `--group-by route`, `--group-by node`,
`--group-by model`, `--group-by route-status`, `--group-by startability`, or
`--group-by live-model` for count summaries.
Use `--rollup-by route`, `--rollup-by route-status`, `--rollup-by model`, or
`--rollup-by failure-class` when comparing route classes across saved runs.
Rollups aggregate success/failure/unknown outcome counts, failure layers,
quality gates, usage availability, total tokens, context adequacy,
tool/textual-tool shape, JSON/reasoning boundaries, live-catalog availability,
warm/cold placement, attempt counts, queue-wait samples, latency samples, and
route-availability instance counts. They still report only sanitized fields
already present in preflight or debug-bundle artifacts.
Use `--recommend` for the first deterministic operator decision layer over
saved evidence. It defaults to the route-contract grain: model, owner node,
backend, and preset. Recommendations are mechanical classes such
as `route_healthy`, `insufficient_evidence`, `catalog_stale`,
`no_instance_start_owner`, `saturated_queue`, `context_too_small`,
`tool_call_route_unsuitable`, `final_content_route_unsuitable`,
`quality_gate_failing`, and `provider_failure`. They are not semantic model
judgments; semantic quality belongs in a sanitized consumer quality gate or a
future reviewed scorer artifact.
Use `--training-jsonl` to export one normalized optimizer row per saved
artifact row. The JSONL schema includes route contract, request expectations,
context, streaming/tool/JSON mode, timing, usage, failure class, quality gate,
payload replayability, redacted result-artifact
presence/type/size/digest-present flags, and the deterministic recommendation.
It does not include prompts, completions, tokens, artifact references, local
paths, or semantic quality labels unless a future reviewed artifact supplies
one explicitly. Consumers of this JSONL validate
evidence kind, owner-telemetry provenance, and deterministic quality-gate
source before selecting or proposing routes.
Use `myce debug select --training-jsonl <file>` to run a report-only route
selection dry run over that JSONL. Add workload gates such as
`--min-context-cap`, `--require-tools`, `--require-json`,
`--require-final-content`, `--require-usage`, or `--require-streaming`. The
selector ranks route contracts as `selected`, `candidate`,
`insufficient_evidence`, or `refused` with evidence counts and refusal reasons.
It does not mutate live routes or optimizer policy.
Use `myce debug propose-policy --training-jsonl <file>` when a selected route
needs an auditable operator approval packet before it can become policy. It
uses the same workload gates as `debug select`, then applies explicit promotion
gates such as `--min-successes`, `--require-owner-telemetry`,
`--require-live-model`, and `--require-quality-pass`. The output is always
report-only: proposed changes, candidates, refusals, missing evidence, rollback
notes, and the human approval boundary. It never writes route config, changes
optimizer policy, restarts peers, or treats preflight-only evidence as
equivalent to owner-side telemetry unless the operator chooses that threshold.
Use `myce debug verify-preflight --preflight <preflight.json>` to re-check a
saved preflight artifact offline. The verifier applies the artifact's recorded
expectation gates and can add stricter report-only gates for live-catalog proof,
context cap, JSON content, structured tool calls, textual tool-call rejection,
finish reason, content length, reasoning length, and prompt-token evidence. It
prints pass/fail reasons without reading tokens, prompts, or completions.

## Debugging Workflow

When a request fails or routes strangely, start with:

1. Capture response status, body, and all `X-Myc-*` headers.
2. Run `myce debug job <X-Myc-Job> --owner-telemetry`, or
   `myce debug job --preflight <preflight.json> --owner-telemetry` if the
   consumer preserved the preflight artifact.
3. Inspect `X-Myc-Trace` for fit/filter/score reasons.
4. Run `myce doctor` or `myce status` if the peer/fleet looks unhealthy.
5. Create `myce debug bundle --job <X-Myc-Job> --owner-telemetry --out /tmp/mycelium-debug`, or
   `myce debug bundle --preflight <preflight.json> --owner-telemetry --out /tmp/mycelium-debug`,
   before handing the issue to another agent.
6. Read `optimization.json` in the bundle first for project/run grouping and
   route/timing/outcome classification; read `job_debug.json` next when the
   summary points to owner/runtime/evidence ambiguity.
7. Check the selected owner peer logs and backend container/process logs.

Telemetry worth checking:

- `run_metrics`: per-run summary facts such as node, preset, backend, project, TTFT, tokens/sec, context used, and load wall-clock.
- `session_metrics`: phase timeline samples such as placed, load-ready, upstream-start, first-byte, stream-chunk, complete, and error.
- recommendation records: context-cap, consolidation, and engine/parameter recommendations derived from telemetry.

For explicit queued workload proofs, keep `/v1/chat/completions`
synchronous and use the workload lane:

```bash
myce jobs admit --url <peer-url> --rpc-token <token> --file <workload-intent.json>
myce jobs drain --url <peer-url> --rpc-token <token> --limit <n>
myce jobs drain --url <peer-url> --rpc-token <token> --limit <n> --execute
myce jobs finish \
  --url <peer-url> \
  --rpc-token <token> \
  --id <job-id> \
  --result-artifact <redacted-artifact-ref> \
  --result-media-type application/json \
  --result-bytes <byte-count> \
  --result-sha256 <sha256> \
  --result-file <file>
myce jobs result --url <peer-url> --rpc-token <token> --id <job-id> --out <file>
myce jobs status --url <peer-url> --rpc-token <token> --id <job-id> --json
```

When the workload is meant to carry an executable gateway request later, keep
the public-safe workload intent JSON separate from the private request body and
let the CLI wrap the payload envelope:

```bash
myce jobs admit \
  --url <peer-url> \
  --rpc-token <token> \
  --file <workload-intent-without-payload.json> \
  --payload-kind openai_chat \
  --payload-file <private-request-body.json>
```

The workload status surface records scheduler/owner/lease evidence plus
payload replayability classification, execution-state evidence, and a redacted
result artifact manifest: reference, media type, byte count, and digest.
`payload_state` distinguishes a registry-replayable payload from runtime-local,
redacted, missing, or unreadable payload evidence. `payload_contract`,
`payload_kind`, and `payload_executable` distinguish opaque evidence payloads
from explicit gateway-request envelopes such as `openai_chat`. `execution_state`
distinguishes queued, running, manifest-only terminal, and replayable terminal
work. `provider_output_capture=true` means `jobs drain --execute` ran a
non-streaming executable payload through the gateway/provider path and stored
the provider response body as the workload result. `provider_output_capture=false`
means the workload is still at the manual-finish boundary. The commands do not
print the queued payload or raw completion.
When an operator or runtime finishes a workload with `--result-file`, Mycelium
stores those result bytes in the private control store and `myce jobs result`
retrieves them explicitly after verifying the stored manifest. The first
automatic provider-output capture path is explicit and non-streaming:
`myce jobs drain --execute` requires a registry-replayable executable payload,
uses the same gateway/provider request semantics, stores a redacted result
manifest plus private result body, and finishes/releases the workload through
the scheduler. A separate non-streaming leased gateway request can consume a
running workload lease by sending `X-Myc-Workload` and `X-Myc-Lease`; this is
for external runners that already have a workload-held reservation and need the
ordinary OpenAI-compatible gateway path to use that reserved owner. Streaming
queued replay and streaming leased gateway execution remain separate product
gates.
Run `make smoke-workload-executor` before live fleet pressure work when you need
a local operator canary for this path. It uses a loopback peer, a fake
OpenAI-compatible provider, `myce jobs admit --payload-kind`, `myce jobs drain
--execute`, `myce jobs status`, and `myce jobs result` to prove admission,
provider execution, private result capture, release/reclaim, and redacted CLI
output without real engines or external hosts.

When a synchronous gateway request would queue instead of immediately acquiring
an owner, the gateway returns `503 Service Unavailable` with `Retry-After`
rather than silently accepting hidden async work. Consumers that need queued
admission should use the explicit workload lane above and preserve the failed
gateway response or preflight artifact as availability evidence.

To spread one model across every machine that can safely hold it (for external
OpenAI-compatible batch consumers such as OCR CLIs), plan and launch a replica
worker pool:

```bash
myce pool plan --model <model-or-alias> \
  --context <per-replica-context> --concurrency <per-replica-envelope> \
  --max-per-node <n> --out /tmp/pool-plan.json
myce pool up --plan /tmp/pool-plan.json --url <peer-url> --rpc-token <token> \
  --name <pool-name> --state /tmp/pool-state.json
myce pool status --state /tmp/pool-state.json --url <peer-url> --rpc-token <token>
myce pool down --state /tmp/pool-state.json --url <peer-url> --rpc-token <token>
```

`pool plan` mirrors the scheduler's hard compatibility rules and the
allocator's safe-fit math (`max_util`, oom-severity margins, existing
occupancy) to compute the maximum additional replicas per node. Refused nodes
report the exact setup gap: model artifact not staged, no compatible backend
runtime, preset context cap below the requested workload context, or
insufficient safe capacity. `pool up` launches the planned replicas through
normal workload admission and holds the owner leases so the capacity stays
reserved; catastrophic-OOM nodes flag `serial_cold_loads` and are launched one
at a time. It writes every planned worker into the state file as `pending`
BEFORE the first admission and rewrites the file after each worker, so a
killed or interrupted run always leaves cleanup-capable state for `pool down`.
Cold loads legitimately take minutes; `pool up` prints one progress line per
worker and bounds each admission with `--admit-timeout` (default 15m) so a
stuck admission fails loudly as `admit_error` instead of hanging. Workers whose
admission never reached the daemon are classified `not_admitted` by
`pool status`/`pool down` instead of failing cleanup. `pool down` finishes or cancels every worker to release the
reservations. External consumers then point their OpenAI-compatible base URL at
any peer's gateway; requests batch across the warm pool and shed to re-planning
when a replica's admission envelope is full.

For partial consumer batches, preserve one `preflight.json` or response-header
set per failed or timed-out row. `debug job --preflight --owner-telemetry`
stitches the request artifact to the durable job/registry surfaces and fetches
the selected job's authoritative run/session telemetry from the owner peer when
the local config or `--rpc-token` provides peer RPC auth. It prints the route,
owner, backend, warm/cold placement, structured-output mode, usage, terminal
job state, registry cleanup flag, owner TTFT/load/context/tokens-per-second,
owner session phases, and any explicit consumer run/item/outcome flags. If
owner telemetry fetch fails, the report keeps the request-time preflight
evidence and prints the exact owner telemetry error.
The `diagnosis` line is the quick operator handoff:

- `owner_completed_warm` / `owner_completed_cold_load`: the assigned owner
  reached the complete phase. If the consumer timed out or missed summary
  output, investigate the consumer timeout/artifact boundary rather than owner
  inference.
- `owner_failed`: Mycelium or the owner recorded a failed job or error phase.
- `cleanup_required` / `inflight_or_stale`: preserve a debug bundle and inspect
  owner state before retrying scale work.
- `owner_telemetry_unavailable`: fix peer RPC auth/address or keep relying on
  the request-time headers until owner telemetry can be fetched.

For OpenAI-compatible tool-calling consumers, treat both
vLLM tool headers as required for `tool_choice:"auto"`. If a response has
`X-Myc-Tools-Requested: true` but
`X-Myc-VLLM-Tool-Parser-Configured: false` or
`X-Myc-VLLM-Auto-Tool-Choice-Configured: false`, the selected route is not
tool-call ready even if plain chat or strict JSON works. Preserve the response
body too: vLLM usually states the exact missing launch flag when it rejects
auto tool choice. If the request returns `queued: no instance available`, the
tool-capable preset may exist but cannot currently be placed. Preserve the
preflight artifact and inspect `debug job --preflight` first; if it classifies
`failure_layer=availability` / `failure_class=route_no_instance_available`,
the `route-availability` line is the next handoff: route catalog, owner node
state, warm/loading instances, queue state, and whether the selected preset can
be started or repaired.

A route with both vLLM tool headers set is provider-configured for tools, but
that alone does not prove the model will choose tools automatically. Isolate the
layers with two tiny probes: first force a known function with
`tool_choice:{"type":"function","function":{"name":"echo"}}` and require an
OpenAI `tool_calls[0]` response; then repeat with `tool_choice:"auto"`. If the
forced probe returns structured `tool_calls` but auto emits a literal
`<tool_call>` block in final text, the remaining issue is model/template auto
tool behavior rather than Mycelium routing, auth, or missing vLLM flags.
`myce gateway preflight` can run that proof without printing secrets or storing
raw completions:

```bash
myce gateway preflight \
  --url http://<gateway-host>:<port> \
  --model <model-alias> \
  --project tool-canary \
  --prompt "Call the echo function with value ok. Do not answer directly." \
  --tool-name echo \
  --tool-choice function \
  --require-tool-call \
  --reject-textual-tool-call \
  --out /tmp/mycelium-tool-forced

myce gateway preflight \
  --url http://<gateway-host>:<port> \
  --model <model-alias> \
  --project tool-canary \
  --prompt "Call the echo function with value ok. Do not answer directly." \
  --tool-name echo \
  --tool-choice auto \
  --out /tmp/mycelium-tool-auto
```

The redacted `preflight.json` records `tool_call_count`, `tool_call_names`,
`textual_tool_call`, route headers, usage, content length, reasoning length, and
finish reason, but not the bearer token, prompt, raw content, or tool
arguments. Add `--stream` when the consumer path itself is streaming; the
artifact then records whether a terminal `[DONE]` event arrived and whether
streaming usage was actually supplied. Gateways built from this revision also
echo the effective
`X-Myc-Project` response header so later debug bundles can join route evidence
to consumer run labels without reading gateway internals.

Add expectation flags when the command is used as a consumer gate:

- `--expect-node <id>` fails if routing lands on the wrong owner.
- `--require-json` fails if strict-schema final content is not parseable JSON.
- `--require-tool-call` fails if no structured OpenAI `tool_calls` are present.
- `--reject-textual-tool-call` fails if final content contains a textual
  `<tool_call>` block.
- `--min-prompt-tokens <n>` fails if the canary did not exercise enough context
  for the consumer fixture.
- `--context-cap <n>` records the selected route's known context limit in the
  redacted artifact.
- `--min-context-cap <n>` fails if that selected route is too small for the
  declared consumer workload. Use this for thinking/tool-agent workloads so a
  route can pass transport/tool-call smoke while still failing the actual
  consumer contract.
- `--require-live-model` fails before the chat request unless authenticated
  `/v1/models` exposes the requested model or alias, and writes that catalog
  status into `preflight.json` when `--out` is set.
- `--expect-finish-reason <reason>` fails if the provider stopped for a
  different reason, such as `length`.
- `--min-content-length <n>` fails if the final answer channel is empty or too
  short for the gate.
- `--max-reasoning-length <n>` fails if reasoning output exceeds the allowed
  boundary for the gate. Use `0` when the canary should finish entirely in final
  content.

Strict JSON route gate:

```bash
myce gateway preflight \
  --url http://<gateway-host>:<port> \
  --model <strict-json-model-alias> \
  --project strict-json-canary \
  --expect-node <expected-owner-node> \
  --require-json \
  --expect-finish-reason stop \
  --min-content-length 2 \
  --out /tmp/mycelium-strict-json
```

Tool transport route gate:

- Gateway: `http://<gateway-host>:<port>`
- Model: `<tool-capable-model-alias>`
- Auth/project: use the operator-provided project-scoped token
  rather than the global fleet token when preserving optimization evidence.
- Tool choice: forced function choice, not `auto`
- Expected headers: `X-Myc-Node: <expected-owner-node>`, `X-Myc-Backend: vllm`,
  `X-Myc-Tool-Choice: function:<name>`,
  `X-Myc-VLLM-Tool-Parser-Configured: true`, and
  `X-Myc-VLLM-Auto-Tool-Choice-Configured: true`
- Expected evidence: `tool_call_count > 0`, `textual_tool_call=false`, and
  `usage.prompt_tokens` high enough for the consumer fixture.

Always leave headroom for the output cap, tool schema/template, reasoning, tool
calls, and any growing tool history. A low-context route can be useful
transport/tool-call smoke while still being unsuitable for a reasoning-heavy
tool-agent workload. Declare the workload minimum explicitly with
`--min-context-cap` so Mycelium records `consumer_contract_context_too_small`
instead of letting a too-small route look like a consumer-ready success.

For an OpenAI-compatible project runtime, the secret-safe handoff is:

- base URL: `http://<gateway-host>:<port>/v1`
- model: `<tool-capable-model-alias>`
- API key: the out-of-band project-scoped token, supplied through the
  consumer's normal API-key env var or a local token file
- request shape: forced function `tool_choice` until `tool_choice:"auto"` is
  proven for the selected model/template/parser route

```bash
myce gateway preflight \
  --url http://<gateway-host>:<port> \
  --model <tool-capable-model-alias> \
  --project <project-id> \
  --gateway-token-file <operator-provided-project-token-file> \
  --prompt "Call the echo function with value ok. Do not answer directly." \
  --max-tokens 160 \
  --context-cap <selected-route-context-cap> \
  --min-context-cap <minimum-for-this-workload> \
  --tool-name echo \
  --tool-choice function \
  --expect-node <expected-owner-node> \
  --expect-finish-reason tool_calls \
  --require-tool-call \
  --reject-textual-tool-call \
  --min-prompt-tokens <fixture-sized-prompt-token-floor> \
  --out /tmp/mycelium-tool-context-gate
```

When using a fixture-sized prompt, raise or lower `--min-prompt-tokens` to
match the fixture, and set `--min-context-cap` to the minimum context window
the workload actually needs. If a route is only meant to prove transport/tool
behavior, say that in the project-local handoff and do not promote it as a full
consumer route.

Streaming token accounting is weaker than non-streaming preflight unless the
provider stream includes usage. Use `--stream` to make that explicit in the
redacted artifact:

```bash
myce gateway preflight \
  --url http://<gateway-host>:<port> \
  --model <tool-capable-model-alias> \
  --project <project-id> \
  --gateway-token-file <operator-provided-project-token-file> \
  --prompt "Call the echo function with value ok. Do not answer directly." \
  --max-tokens 80 \
  --tool-name echo \
  --tool-choice function \
  --stream \
  --expect-node <expected-owner-node> \
  --require-tool-call \
  --reject-textual-tool-call \
  --out /tmp/mycelium-tool-stream
```

The streaming evidence to preserve for debug handoff is `stream_done`,
`usage_available`, `X-Myc-*` headers, structured `tool_calls`, and absence of
textual `<tool_call>` content. Pair any streaming run with at least one
non-streaming preflight on the same model/project when large-fixture context and
usage numbers matter for optimization.

### Prove cold-load cancellation cleanup after activation

Use this only after the gateway and owner peers are deliberately activated on a
binary that contains the cold-load cancellation cleanup fix. It is a proof
recipe, not an activation step. Pick a maintenance-safe model alias or preset
that is not already warm on the chosen owner; if the request reuses a warm
instance, it does not prove cold-load cleanup.

```bash
CANARY_ID="stale-cold-load-$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${TMPDIR:-/tmp}/mycelium-${CANARY_ID}"
GATEWAY="http://<gateway-host>:<port>"
MODEL="<unloaded-test-model-or-alias>"
DB="<gateway-control-store.sqlite>"
RPC_URL="$GATEWAY"

mkdir -p "$OUT"

set +e
curl --max-time 2 -sS -D "$OUT/headers.txt" -o "$OUT/body.json" \
  -H "Authorization: Bearer $MYCELIUM_GATEWAY_TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Myc-Project: $CANARY_ID" \
  "$GATEWAY/v1/chat/completions" \
  -d '{
    "model": "'"$MODEL"'",
    "messages": [{"role": "user", "content": "Return {\"ok\":true} as JSON."}],
    "max_tokens": 16,
    "response_format": {"type": "json_object"}
  }'
CURL_RC=$?
set -e
printf 'curl_rc=%s\n' "$CURL_RC" | tee "$OUT/curl-status.txt"
```

Expected client-side cancellation is usually `curl_rc=28`. If curl returns 0,
the request finished before the client timeout and the run did not exercise the
canceled cold-load path. Shorten `--max-time` or choose a colder test alias.

Find the job and inspect terminal state:

```bash
JOB_ID="$(
  myce jobs list --db "$DB" |
    awk -v project="$CANARY_ID" '$3 == project { id = $1 } END { print id }'
)"
test -n "$JOB_ID"

myce debug job "$JOB_ID" --db "$DB" --json > "$OUT/job.json"
jq -e '
  .job.status == "failed" and
  ((.registry == null) or
   (.registry.status == "failed" and (.registry.cleanup_required | not)))
' "$OUT/job.json"

myce debug bundle \
  --job "$JOB_ID" \
  --db "$DB" \
  --url "$RPC_URL" \
  --rpc-token "$MYCELIUM_RPC_TOKEN" \
  --out "$OUT/debug-bundle"
```

If the gateway is authenticated with `gateway_project_tokens`, the token pins
the project and ignores `X-Myc-Project`; use a dedicated canary project token or
locate the newest matching model row immediately after the run. Treat these
outcomes as evidence:

- `failed` job plus missing registry record or terminal `failed` registry with
  `cleanup_required=false`: cleanup proof passed.
- `placing`, `loading`, `running`, or registry `cleanup_required=true` after a
  bounded wait: stale-job lifecycle regression; preserve the debug bundle.
- no matching job row: the client timed out before the gateway accepted work;
  rerun with a slightly longer client timeout.
- `done`: the request completed; the run is valid gateway proof but not a
  cancellation cleanup proof.

## Test Gates

Fast local gates:

```bash
go build ./...
go vet ./...
test -z "$(gofmt -l .)"
go test ./...
go test ./... -race
```

Smoke gates touch real engines/hardware and are opt-in:

```bash
make smoke-local
make smoke-fleet
make smoke-spark-vllm
make smoke-b70
```

Phase 4/6 real-fleet smoke can use authenticated gateways directly:

```bash
MYCELIUM_GATEWAY_TOKEN=<token> \
MYCELIUM_JOIN_GATEWAY=http://<gateway>:<port> \
MYCELIUM_JOIN_MODEL=<model> \
go test -count=1 -tags smoke ./test/smoke/... -run TestPhase4JoinedNodeGatewaySmoke -timeout 20m -v

MYCELIUM_GATEWAY_TOKEN=<token> \
MYCELIUM_FEDERATION_GATEWAY_A=http://<gateway-a>:<port> \
MYCELIUM_FEDERATION_GATEWAY_B=http://<gateway-b>:<port> \
MYCELIUM_FEDERATION_MODEL=<model> \
go test -count=1 -tags smoke ./test/smoke/... -run TestPhase6FederationSubmitAnywhereSmoke -timeout 20m -v
```

The destructive dead-peer rescue smoke is disabled unless explicitly armed:

```bash
MYCELIUM_DEAD_PEER_RESCUE_ENABLE=1 \
MYCELIUM_DEAD_PEER_GATEWAY=http://<gateway-a>:<port> \
MYCELIUM_DEAD_PEER_MODEL=<model> \
MYCELIUM_DEAD_PEER_OWNER_NODE=<node-b> \
MYCELIUM_DEAD_PEER_KILL_COMMAND='<command that stops node-b peer>' \
MYCELIUM_DEAD_PEER_REGISTRY_URL=http://<registry-peer>:<port> \
MYCELIUM_DEAD_PEER_RPC_TOKEN=<rpc-token> \
go test -count=1 -tags smoke ./test/smoke/... -run TestPhase6DeadPeerRescueSmoke -timeout 20m -v
```
