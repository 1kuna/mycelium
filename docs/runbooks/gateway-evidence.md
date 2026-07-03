# Gateway Evidence Runbook

This runbook is public-safe. Do not add private hostnames, IP addresses, token
values, local artifact paths, prompts, completions, project/customer names, or
one-off run narratives.

## Purpose

Use these surfaces when an operator needs route proof, preflight evidence,
debug bundles, route certification, allocation envelopes, or cold-load
cancellation cleanup proof. These are diagnostics and operator workflows; they
are not required for the ordinary consumer happy path.

## Route Discovery

Inspect the route contract a peer config advertises without exposing token
values:

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
it never prints token material. With `--live`, the command performs an
authenticated read-only `GET /v1/models` against the configured gateway (or
`--url`) and annotates each configured route with `live_model` / `live_match`.

## Allocation Envelope

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
`MYCELIUM_CONTEXT_CAP`. The allocation report is not a held capacity
reservation; explicit reservations are covered in
[Reserved Capacity](reserved-capacity.md).

Immediately before launching an external tool, verify the saved plan:

```bash
myce gateway allocate --verify-plan /tmp/mycelium-allocation/allocation-plan.json --live
```

Verification exits non-zero when the plan is expired, the live catalog no
longer satisfies it, or the current allocation no longer covers the planned
worker count.

## Reconciliation Evidence

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
a `decision`, then turns filter/drop evidence into typed tasks such as
`wait_capacity`, `prewarm_replica`, `verify_engine_readiness`,
`stage_model_artifact`, `repair_owner`, `refresh_catalog`, or
`record_incompatibility`. `myce reconcile drain` consumes only `allowed`
records and records `running` to terminal status transitions. Blocked records
are left untouched.

## Route Certification And Preflight

For repeated route classes, store the gates in a local route-contract file and
certify the route before a consumer uses it:

```bash
myce route certify strict-json-general \
  --contracts ~/.mycelium/route-contracts.json \
  --gateway-token-file <local-token-file>

myce route status --contracts ~/.mycelium/route-contracts.json
```

`route certify` delegates to `gateway preflight` and writes the redacted
artifact to the configured evidence root. `route status` finds the latest saved
evidence for each named contract and re-runs the preflight verifier offline.
Saved certification artifacts do not include bearer tokens, prompts, or raw
completion content.

Run a direct preflight when a route needs a one-off canary:

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
request runs. If the chat request itself returns an HTTP error and `--out` is
set, preflight still writes a redacted artifact with the status, whitelisted
route headers when present, and a bounded gateway error string.

## Debug Reports

When a request fails or routes strangely, start with:

1. Capture response status, body, and all `X-Myc-*` headers.
2. Run `myce debug job <X-Myc-Job> --owner-telemetry`, or
   `myce debug job --preflight <preflight.json> --owner-telemetry` if the
   consumer preserved the preflight artifact.
3. Run `myce doctor` or `myce status` if the peer or fleet looks unhealthy.
4. Create a redacted support bundle before handing the issue to another agent:

```bash
myce debug bundle --job <X-Myc-Job> --owner-telemetry --out /tmp/mycelium-debug
myce debug bundle --preflight <preflight.json> --owner-telemetry --out /tmp/mycelium-debug
```

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

## Cold-Load Cancellation Cleanup

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

Treat these outcomes as evidence:

- `failed` job plus missing registry record or terminal `failed` registry with
  `cleanup_required=false`: cleanup proof passed.
- `placing`, `loading`, `running`, or registry `cleanup_required=true` after a
  bounded wait: stale-job lifecycle regression; preserve the debug bundle.
- no matching job row: the client timed out before the gateway accepted work;
  rerun with a slightly longer client timeout.
- `done`: the request completed; the run is valid gateway proof but not a
  cancellation cleanup proof.
