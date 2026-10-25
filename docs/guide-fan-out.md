# Declared independent pieces

The [slice 4 amendment](design-proposal.md#declared-fan-out-and-batch-planning-slice-4-107-approved-2026-10-24)
defines this contract. It uses ordinary extension jobs and existing artifact
transport. Applications supply homogeneous independent pieces and own semantic
splitting and reduction. Mycelium plans, retries and gathers them.

Declared calls fall back to serial admission when undeclared calls share the slot.

```yaml
name: parameter-sweep
project: research
priority: experiment
extension: simulator@1
workload: {input_shape: fixed-v1, units: 100}
files: {}                       # immutable common input manifest, or NAS inputs
outputs: {metrics.json: results}
fan_out:
  max_concurrency: 8             # ceiling, not a fixed batch size
  pieces:
    - {key: first, args: ["--seed", "1"]}
    - {key: second, args: ["--seed", "2"]}
```

Keys must be unique clean names; there are at most 256 pieces. `workload.units`
describes **one** piece, using the matching variant reference's unit and shape.
The existing `resumable` contract applies to each fixed wave. Arbitrary programs,
dynamic enumeration, stage graphs and timed input accumulation are outside this
slice. Common inputs are staged through the normal file-store/NAS path for each
wave; arguments select the relevant data. No private input paths go in curves.

A variant opts into measured wave execution by adding `run` to its reference
sweep, alongside `command`, `counts` and `max_concurrency`:

```yaml
sweeps:
  extension:simulator:
    command: [python3, "{extension}/reference.py"]
    run: [python3, "{extension}/pieces.py"]
    counts: [1, 2, 4, 8]
    max_concurrency: 8
```

The author promises that `run` uses the same concurrency mode and shape measured
by the correctness-checked reference. No measured point grants resource capacity.
The queue conservatively scales per-piece CPU/RAM/device floors, includes measured
high-water memory and charges shared hosts once. Existing priority, fairness,
placement and owner controls still apply. Short groups and tails use smaller
measured counts. Without a safe matching wave recipe/curve, each piece uses the
ordinary serial command and its own arguments.

For a measured wave the worker sets `MYCE_BATCH_CONCURRENCY` and
`MYCE_BATCH_MANIFEST=.myce-batch.json`. This small JSON file is in the attempt's
working directory and contains `parent`, `pieces` (key and args), `concurrency`,
common `args`, `inputs`/`files` names and keyed `outputs` names. Write each declared
output under `KEY/OUTPUT_NAME`, e.g. `first/metrics.json`. The worker publishes
these through the existing attempt-specific verified artifact path. The manifest
name is reserved. A wave owns one command slot, including on GPUs.

The returned parent job's `fan_out` has a dispatch cursor, child job IDs and
`results` keyed by original piece key. Each result names the accepted execution
and its output references. The parent succeeds only after every piece succeeds;
its final `output_refs`/`output_files` use `KEY/OUTPUT_NAME`. A failed wave fails
the parent. An OOM forbids its exact measured count and fails the wave; it does
not qualify the same occupancy for another attempt. Infrastructure loss and planned interruption use existing retry
policy on that wave alone. Membership and measured count stay fixed across
retries, preserving the recipe's vector-batch semantics. A retry waits if no
component can safely admit that count and its resource envelope. Late attempts
cannot replace accepted results. Parent cancellation stops its children and
reports outstanding cleanup. Persisted membership/results survive queue restart.

## Binding model-call curves

An extension-command sweep alone does not qualify server calls. Compute the
exact server configuration digest with the read-only command:

```sh
myce model measurement-digest MODEL --model-config models.yaml --environment-config environments.yaml
```

The reference sweep must declare that digest as `server_digest` and actually
exercise that exact model/server, shape, environment and cache/context settings.
The model catalog's `measurement` declares `extension_digest`, `work` (the exact
reference work type) and the same `server_digest`. Changing the model bytes,
command, settings, bounds or resolved environment invalidates the binding.
Existing variant correctness qualification and exact component/fingerprint,
allowance, freshness, OOM and measured-memory checks remain mandatory.

Requests opt in with generic headers `X-Myce-Fan-Out` (group key),
`X-Myce-Input-Shape` and `X-Myce-Work-Units`. Compatible pending calls in the same
project, class/tier, model, shape and size can use measured engine occupancy,
bounded by configured `max_in_flight`. A running wave's chosen occupancy is not
increased by later arrivals; a tail can use fewer slots. Unknown binding means
serial admission for declared calls. Requests without the declaration keep
their existing routing/caps, including zero-as-unlimited. Model calls retain
their existing ephemeral request/retry/streaming semantics; this is not a durable
workflow API or a second batching timer.

## Laptop simulator comparison

```sh
myce sim --fleet examples/sim/fleet.yaml --workload examples/sim/declared-fan-out/workload.yaml --what-if declared-batch --days 7 --seed 1 --json sim-results.json
```

This compares unsplit, naive split and declared execution using identical arrival
draws and policy. The added curves are explicitly synthetic work-units/second,
not measurements of any fleet. Execution timers use these rates while the real
queue chooses placement/count and handles preemption. The JSON records the
paired mean makespan gain, worst individual batch regression, production-p95
delta and gate result. A failed gate writes the JSON and exits nonzero. Each
comparison uses the same synthetic component inventory and shared host ledger;
virtual per-component workers retain the simulator's single-assignment transport.
Native qualification and
deployment require separate authorization.
