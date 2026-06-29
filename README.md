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
mycelium service plan \
  --config ~/.mycelium/peer.json \
  --binary /path/to/mycelium \
  --name real-fleet \
  --system
```

On macOS, use `--system` when a user LaunchAgent cannot reach LAN peers because
of Local Network permission or binary-identity behavior. The plan command is
read-only: it prints the target unit path, generated launchd/systemd unit body,
install/status/reload commands, and rollback command. For a live gateway, first
keep a known-good binary backup, run `service status` against the current
service, inspect the `--system` plan, and install during a maintenance window:
the generated system LaunchDaemon runs as the invoking non-root user so it keeps
the existing user-owned config, logs, and control DB.

```bash
sudo /path/to/mycelium service install \
  --config /Users/user/.mycelium/bench/real-fleet/peer.json \
  --binary /path/to/mycelium \
  --name real-fleet \
  --system

sudo /path/to/mycelium service status \
  --config /Users/user/.mycelium/bench/real-fleet/peer.json \
  --binary /path/to/mycelium \
  --name real-fleet \
  --system
```

If seed diagnostics fail after activation, restore the known-good binary and
kick the service named by `service plan`; do not keep a Mac gateway live when
`service status` reports unreachable seeds.

Read-only checks for a Mac user LaunchAgent that loses LAN reachability after a
binary update:

```bash
launchctl print gui/$(id -u)/com.mycelium.real-fleet
codesign -dv --verbose=4 /Users/user/.mycelium/bin/mycelium 2>&1
codesign -dv --verbose=4 /path/to/new/mycelium 2>&1
spctl -a -vv /Users/user/.mycelium/bin/mycelium /path/to/new/mycelium
xattr -l /Users/user/.mycelium/bin/mycelium /path/to/new/mycelium
log show --style compact --last 2h \
  --predicate 'process == "mycelium" OR eventMessage CONTAINS[c] "mycelium"'
```

If the old LaunchAgent binary reaches LAN peers but the replacement reports
`no route to host` from the same config and launchd domain, compare the
CodeDirectory identifier and CDHash before blaming Mycelium routing. Ad-hoc
CLI signatures change identity whenever the binary changes; macOS Local Network
approval can therefore behave like a binary-identity gate. The clean proof path
is a stable signed bundle/helper identity or the system LaunchDaemon plan above,
followed by `service status` and the same seed diagnostics. Do not keep testing
by blind-swapping the live LaunchAgent binary.

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

For authenticated non-loopback gateways, `myce benchmark` resolves the gateway token in this order:

1. `--gateway-token`
2. `--gateway-token-env`
3. `MYCELIUM_GATEWAY_TOKEN`
4. `MYCELIUM_API_KEY`
5. `ATLAS_MYCELIUM_API_KEY`
6. `OPENAI_API_KEY`
7. `~/.mycelium/peer.json` field `gateway_token`

## Calling The Gateway

The gateway accepts OpenAI-style chat requests:

```bash
curl http://<gateway-host>:<port>/v1/chat/completions \
  -H "Authorization: Bearer $MYCELIUM_GATEWAY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "spark-qwen122b",
    "messages": [{"role": "user", "content": "Say hi."}],
    "max_tokens": 32
  }'
```

Project consumers should first run the secret-safe preflight. It sends one
non-streaming strict JSON-schema chat request, prints route/usage evidence, and
never prints the bearer token:

```bash
myce gateway preflight \
  --url http://<gateway-host>:<port> \
  --model <model-or-alias> \
  --project <consumer-canary-id> \
  --config <local-mycelium-peer.json> \
  --out /tmp/mycelium-preflight
```

Token resolution for `myce gateway preflight` is:

1. `--gateway-token`
2. `--gateway-token-env`
3. `--gateway-token-file`
4. `--config <peer.json>` field `gateway_token`
5. `MYCELIUM_GATEWAY_TOKEN`
6. `MYCELIUM_API_KEY`
7. `ATLAS_MYCELIUM_API_KEY`
8. `OPENAI_API_KEY`
9. `~/.mycelium/peer.json` field `gateway_token`

For an OpenAI-compatible consumer, set the base URL to the Mycelium gateway and
put the Mycelium gateway token in the env var that consumer already reads. For
example, Atlas can use `ATLAS_MYCELIUM_API_KEY` or `OPENAI_API_KEY`; local
Mycelium operator commands can use `MYCELIUM_GATEWAY_TOKEN`. Do not store token
values in repo docs or run logs.

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
whitelisted `X-Myc-*` headers, finish reason, content/reasoning lengths, JSON
parse status, and usage counts. It intentionally does not write the bearer
token, prompt, or raw completion content. Use that artifact, plus
`myce debug job --preflight <preflight.json> --owner-telemetry --json`, as the
standard consumer evidence packet for optimization handoffs. If the consumer
has a batch/run id, row/document id, timeout/error/cancel state, or client
elapsed time, attach it explicitly:

```bash
myce debug job \
  --preflight /tmp/mycelium-preflight/preflight.json \
  --owner-telemetry \
  --consumer-run <batch-or-request-id> \
  --consumer-item <row-or-document-id> \
  --consumer-status <ok|timeout|error|canceled|partial> \
  --consumer-elapsed-ms <client-elapsed-ms> \
  --consumer-error <sanitized-error-class> \
  --json
```

Use `--consumer-error` for a short sanitized class such as `client_timeout`,
`schema_validation_failed`, or `response_parse_error`; do not put bearer
tokens, raw prompts, raw completions, or private document text in that field.

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
6. Check the selected owner peer logs and backend container/process logs.

Telemetry worth checking:

- `run_metrics`: per-run summary facts such as node, preset, backend, project, TTFT, tokens/sec, context used, and load wall-clock.
- `session_metrics`: phase timeline samples such as placed, load-ready, upstream-start, first-byte, stream-chunk, complete, and error.
- recommendation records: context-cap, consolidation, and engine/parameter recommendations derived from telemetry.

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
The `diagnosis` line is the quick PM handoff:

- `owner_completed_warm` / `owner_completed_cold_load`: the assigned owner
  reached the complete phase. If the consumer timed out or missed summary
  output, investigate the consumer timeout/artifact boundary rather than owner
  inference.
- `owner_failed`: Mycelium or the owner recorded a failed job or error phase.
- `cleanup_required` / `inflight_or_stale`: preserve a debug bundle and inspect
  owner state before retrying scale work.
- `owner_telemetry_unavailable`: fix peer RPC auth/address or keep relying on
  the request-time headers until owner telemetry can be fetched.

For OpenAI-compatible tool-calling consumers such as Deep Agents, treat both
vLLM tool headers as required for `tool_choice:"auto"`. If a response has
`X-Myc-Tools-Requested: true` but
`X-Myc-VLLM-Tool-Parser-Configured: false` or
`X-Myc-VLLM-Auto-Tool-Choice-Configured: false`, the selected route is not
tool-call ready even if plain chat or strict JSON works. Preserve the response
body too: vLLM usually states the exact missing launch flag when it rejects
auto tool choice. If the request returns `queued: no instance available`, the
tool-capable preset may exist but cannot currently be placed without unloading
or changing live capacity.

A route with both vLLM tool headers set is provider-configured for tools, but
that alone does not prove the model will choose tools automatically. Isolate the
layers with two tiny probes: first force a known function with
`tool_choice:{"type":"function","function":{"name":"echo"}}` and require an
OpenAI `tool_calls[0]` response; then repeat with `tool_choice:"auto"`. If the
forced probe returns structured `tool_calls` but auto emits a literal
`<tool_call>` block in final text, the remaining issue is model/template auto
tool behavior rather than Mycelium routing, auth, or missing vLLM flags.

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
