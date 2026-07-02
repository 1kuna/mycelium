# Workload And Pressure Proof Runbook

This runbook is public-safe. Do not add private hostnames, IP addresses, token
values, local artifact paths, prompts, completions, or project-specific run
narratives.

## Purpose

Reachability canaries prove transport. They do not prove fleet allocation under
pressure. Use this runbook when an operator needs to prove queued admission,
owner assignment, provider execution, result capture, release/reclaim, and
synthetic pressure behavior without using an application project as the test
harness.

## Local Executor Canary

Run the repo-local canary before live fleet pressure work:

```bash
make smoke-workload-executor
```

The canary uses a loopback peer, fake OpenAI-compatible provider, and the real
`myce` operator flow:

```bash
myce jobs admit --payload-kind openai_chat --payload-file <private-request-body.json>
myce jobs drain --execute
myce jobs status --json
myce jobs result --out <file>
```

It proves non-streaming executable queued payload handling, provider execution,
private result capture, scheduler finish/release, lease reclaim, and redacted
CLI output without starting real engines.

## Explicit Workload Lane

Keep `/v1/chat/completions` synchronous. When a request would queue, the
gateway should return `503 Service Unavailable` with `Retry-After`. Queued work
belongs on the authenticated workload lane:

```bash
myce jobs admit --url <peer-url> --rpc-token <token> --file <workload-intent.json>
myce jobs drain --url <peer-url> --rpc-token <token> --limit <n>
myce jobs drain --url <peer-url> --rpc-token <token> --limit <n> --execute
myce jobs status --url <peer-url> --rpc-token <token> --id <job-id> --json
myce jobs result --url <peer-url> --rpc-token <token> --id <job-id> --out <file>
```

For queued provider execution, the admitted payload must be registry-replayable
and executable:

```bash
myce jobs admit \
  --url <peer-url> \
  --rpc-token <token> \
  --file <workload-intent-without-payload.json> \
  --payload-kind openai_chat \
  --payload-file <private-request-body.json>
```

Status/debug output reports payload and result classification only. It must not
print payload bodies, prompts, or raw provider completions.

## Calibration And Live Pressure Packet

Generate a project-free calibration packet from inventory:

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

The plan artifact records:

- `local_proof_command`: the local queued-executor smoke command;
- `run_command`: the deterministic simulation command;
- `live_run_command`: the explicit `myce benchmark fleet` command for later
  operator-approved live pressure proof.

Simulation proves that the generated plan is structurally runnable through the
scheduler/allocator benchmark path. It does not start live engines, change
routing policy, apply optimizer defaults, or prove real hardware behavior.

## Live Proof Boundary

Run the generated live benchmark command only when a maintenance-safe operator
window exists. The live proof must capture:

- workload intent fields;
- concurrent pressure waves;
- queue/preemption/drain decisions;
- selected owner/node/backend/model/preset;
- warm/cold placement;
- release/reclaim evidence;
- route headers and timing;
- result/error evidence;
- benchmark artifacts and calibration report.

Do not claim full-fleet allocator proof from single-request chat canaries.

## Streaming Boundary

`jobs drain --execute` currently supports non-streaming executable payloads.
Streaming queued provider replay remains a separate product gate because it
needs an interruption/replay contract for partial streams and client
disconnects.

