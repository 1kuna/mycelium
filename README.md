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
4. `~/.mycelium/peer.json` field `gateway_token`

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

Useful response headers:

- `X-Myc-Job` - job id to pass to `myce debug job <id>`.
- `X-Myc-Decision` - placement action, such as warm reuse or cold load.
- `X-Myc-Node` - owner node selected for the request.
- `X-Myc-Instance` - model instance used.
- `X-Myc-Backend` - backend type.
- `X-Myc-Attempts` - gateway attempts/failovers.
- `X-Myc-Trace` - JSON placement trace explaining estimate/filter/select/score/admit.

## Debugging Workflow

When a request fails or routes strangely, start with:

1. Capture response status, body, and all `X-Myc-*` headers.
2. Run `myce debug job <X-Myc-Job>`.
3. Inspect `X-Myc-Trace` for fit/filter/score reasons.
4. Run `myce doctor` or `myce status` if the peer/fleet looks unhealthy.
5. Create `myce debug bundle --job <X-Myc-Job> --out /tmp/mycelium-debug` before handing the issue to another agent.
6. Check the selected owner peer logs and backend container/process logs.

Telemetry worth checking:

- `run_metrics`: per-run summary facts such as node, preset, backend, project, TTFT, tokens/sec, context used, and load wall-clock.
- `session_metrics`: phase timeline samples such as placed, load-ready, upstream-start, first-byte, stream-chunk, complete, and error.
- recommendation records: context-cap, consolidation, and engine/parameter recommendations derived from telemetry.

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
