# Mycelium

Mycelium is a hardware-aware inference control plane for a personal/local
fleet. Every machine runs the same `mycelium` peer binary, and any
OpenAI-compatible client can point at any peer's gateway endpoint while
Mycelium chooses the owner, warms workers under demand, and shrinks
auto-grown capacity when idle.

The product shape is intentionally simple:

- `mycelium` is the daemon. Every machine runs the same peer binary.
- A peer can be `compute: true` and own local accelerators/backends, or
  compute-off and only submit/coordinate work.
- Any peer can receive a request, gather a fleet snapshot, choose placement,
  and ask the selected resource owner to commit.
- The resource owner is authoritative for local capacity.
- There is no permanent leader, no SSH product transport, and no model
  sharding across machines.
- `myce` is the operator/control CLI.

The goal for v0.1 is a robust personal compute cluster: local models,
automatic placement, bounded cold-start waiting, telemetry, and enough
operator evidence to fix real issues quickly.

## Source Of Truth

Read these first when implementing or auditing:

1. `01-project-spec.md` - product/resource model and locked decisions.
2. `02-testing-architecture.md` - interfaces, mocks, conformance, and test
   tiers.
3. `03-development-guide.md` - phase order and gates.
4. `05-vision-alignment.md` - the autonomous endpoint alignment plan.

Those docs are the contract. Do not edit them casually. Implementation
decisions and unresolved blockers live in `DECISIONS.md` and `BLOCKERS.md`.

## Mental Model

A request enters a gateway-compatible peer:

1. The peer resolves the requested model/preset and project defaults.
2. The coordinator gathers a fleet snapshot from known peers.
3. The scheduler estimates weights/KV, filters by fit/policy, scores
   candidates, and returns a placement decision.
4. The selected owner commits capacity locally.
5. The owner reuses a warm instance or loads a backend.
6. The gateway proxies the request and returns `X-Myc-*` decision/debug
   headers.
7. The owner records run/session telemetry for later debugging and
   recommendations.

The important invariant: the coordinator decides, but the resource owner
commits.

## Quickstart

Run a peer:

```bash
mycelium run --config ~/.mycelium/peer.json
```

Bootstrap a peer from a join URI:

```bash
mycelium bootstrap --join "$MYC_JOIN_URI" --rpc-token "$MYC_RPC_TOKEN" --compute auto --apply
```

Add or register a model/preset:

```bash
myce add-model --id <preset-id> --model <model-or-alias> <source>
myce models list
```

Enable the autonomous behavior you want in `peer.json`:

```json
{
  "autonomy": {
    "auto_scale": true,
    "auto_stage": false,
    "max_replicas_per_model_per_node": 4,
    "max_replicas_per_model": 8,
    "scale_cooldown_ms": 60000,
    "idle_ttl_ms": 900000,
    "max_wait_default_ms": 0
  }
}
```

Point any OpenAI-compatible client at the peer:

```bash
export OPENAI_BASE_URL="http://<gateway-host>:<port>/v1"
export OPENAI_API_KEY="$MYCELIUM_GATEWAY_TOKEN"
```

Or call the gateway directly:

```bash
curl http://<gateway-host>:<port>/v1/chat/completions \
  -H "Authorization: Bearer $MYCELIUM_GATEWAY_TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Myc-Max-Wait-Ms: 600000" \
  -H "X-Myc-Priority: interactive" \
  -d '{
    "model": "<model-or-alias>",
    "messages": [{"role": "user", "content": "Say hi."}],
    "max_tokens": 32
  }'
```

`X-Myc-Max-Wait-Ms` is the explicit cold-start wait budget for synchronous
requests. `X-Myc-Priority` lets a trusted request declare intent such as
`interactive` or `background`. Header-free OpenAI clients still work; the
headers are optional intent.

## Autonomy Config

All autonomy is opt-in. Defaults are off/zero so a config with no `autonomy`
block preserves ordinary synchronous placement behavior.

- `auto_scale`: when true, durable demand signals can warm additional
  replicas through normal owner admission.
- `auto_stage`: when true, allowed staging demand can copy/model-stage
  artifacts through the established staging path before scale-up.
- `max_replicas_per_model_per_node`: cap on autonomous replicas for one model
  on one node.
- `max_replicas_per_model`: fleet-wide autonomous replica cap for one model.
- `scale_cooldown_ms`: cooldown after failed scale attempts; must be at least
  one second when `auto_scale` is enabled.
- `idle_ttl_ms`: idle age after which auto-grown instances may be unloaded.
- `max_wait_default_ms`: peer-level default wait budget when a request/project
  does not supply one.

Invalid negative values and unsafe cooldowns fail startup loudly.

## Gateway Tokens

Use a Mycelium gateway token, not a provider key, as the bearer token for
authenticated gateways. Operator commands that need to resolve a gateway token
share one source order documented in
[Reserved Capacity](docs/runbooks/reserved-capacity.md#gateway-token-resolution).
`OPENAI_API_KEY` is ignored by Mycelium operator commands unless explicitly
named with `--gateway-token-env OPENAI_API_KEY`.

## Runbooks

Operator workflows live under [docs/runbooks](docs/runbooks/README.md):

- [Service Upgrade](docs/runbooks/service-upgrade.md): dry-run upgrade packet,
  activation boundary, diagnostics, and rollback proof.
- [Gateway Evidence](docs/runbooks/gateway-evidence.md): route discovery,
  preflight, certification, debug bundles, allocation envelopes, and cold-load
  cleanup proof.
- [Operator Reference](docs/runbooks/operator-reference.md): relocated
  operator commands, evidence workflows, debug recipes, and route-gate details.
- [Reserved Capacity](docs/runbooks/reserved-capacity.md): explicit
  capacity-reservation operations and the local bridge for header-limited
  tools.
- [Workload And Pressure Proof](docs/runbooks/workload-pressure-proof.md):
  explicit queued workload proof, local executor smoke, calibration packet, and
  live pressure boundary.

## Test Gates

Fast local gates:

```bash
go build ./...
go vet ./...
test -z "$(gofmt -l .)"
go test ./...
go test ./... -race
```

Full local CI:

```bash
make ci
```

Smoke gates touch real engines/hardware and are opt-in:

```bash
make smoke-local
make smoke-fleet
make smoke-spark-vllm
make smoke-b70
make smoke-benchmark-fleet
make smoke-vision
```

The vision smoke is the end-to-end autonomous fleet acceptance check:

```bash
MYCELIUM_GATEWAY_TOKEN=<token> \
MYCELIUM_VISION_GATEWAY=http://<gateway>:<port> \
MYCELIUM_VISION_MODEL=<model> \
MYCELIUM_VISION_MIN_NODES=2 \
make smoke-vision
```

Optional knobs:

- `MYCELIUM_VISION_REQUESTS` (default `200`)
- `MYCELIUM_VISION_CONCURRENCY` (default `32`)
- `MYCELIUM_VISION_MAX_WAIT_MS` (default `600000`)
- `MYCELIUM_VISION_MAX_TOKENS` (default `8`)
- `MYCELIUM_VISION_EXPECT_REAP=1`
- `MYCELIUM_VISION_IDLE_TTL_MS` (default `900000`)
- `MYCELIUM_VISION_SNAPSHOT_URLS` (comma-separated peer URLs for reap proof)
- `MYCELIUM_VISION_RPC_TOKEN` (RPC token for snapshot polling)
