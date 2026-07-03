# Reserved Capacity Runbook

This runbook is public-safe. Do not add private hostnames, IP addresses, token
values, local operator paths, prompts, completions, project/customer names, or
one-off run narratives.

## Purpose

Use explicit reservations only when an operator needs guaranteed capacity,
ahead-of-time warming for a deadline, or a local bridge for a tool that cannot
send Mycelium reservation headers itself. Ordinary OpenAI-compatible consumers
should use the gateway directly; autonomous scale-up, bounded wait, and idle
reap are the default product path.

## Plan And Launch

To spread one model across every machine that can safely hold it for an
external batch window, plan and launch a replica worker pool:

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
stuck admission fails loudly as `admit_error` instead of hanging. If admission
returns queued while a cold load continues in the daemon, `pool up` refreshes
that worker until it becomes a held running lease or the admit timeout expires;
a successful `pool up` never means "queued-only, no usable lease." Workers
whose admission never reached the daemon are classified `not_admitted` by
`pool status`/`pool down` instead of failing cleanup. `pool down` finishes or
cancels every worker to release the reservations.

## Consume A Held Lease

A held worker's lease reserves its instance's admission envelope, so normal
gateway admission will refuse that capacity (`concurrency envelope is full`) —
held capacity is consumed through the leased path, not normal admission.
Header-aware clients send:

```text
X-Myc-Workload: <workload-id>
X-Myc-Lease: <lease-id>
X-Myc-Lease-Hold: true
```

With the hold header the gateway serves the request on the reserved lease and
keeps the workload running for reuse instead of finishing it. The header-less
leased call remains single-shot and auto-releases.

For header-unaware OpenAI-compatible consumers, run the bridge:

```bash
myce pool proxy --state /tmp/pool-state.json --url <gateway-url> \
  --gateway-token-env MYCELIUM_GATEWAY_TOKEN --listen 127.0.0.1:8600
```

Point the consumer's base URL at `http://127.0.0.1:8600/v1`. The proxy rotates
requests across held workers, injects the lease headers, overrides any
caller-supplied Mycelium reservation headers, and fails over to the next held
worker when a lease is refused. Leased gateway execution is non-streaming; batch
consumers should disable streaming. Release everything with `myce pool down`
when the run finishes.

## Gateway Token Resolution

`myce pool proxy`, `myce gateway preflight`, and `myce benchmark run` resolve a
Mycelium gateway token in one shared order:

1. `--gateway-token`
2. `--gateway-token-env <ENV_NAME>`
3. `--gateway-token-file <path>`
4. `MYCELIUM_GATEWAY_TOKEN`
5. `MYCELIUM_API_KEY`
6. peer config `gateway_token` (`--config <peer.json>` when the command accepts
   it, otherwise `~/.mycelium/peer.json`)

`OPENAI_API_KEY` is not used as a default Mycelium gateway bearer token. If an
OpenAI-compatible consumer environment intentionally stores a Mycelium gateway
token in that variable, pass `--gateway-token-env OPENAI_API_KEY` so the auth
source is explicit in command output.
