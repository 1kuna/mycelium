# Operator Surface Inventory

This inventory is public-safe. It intentionally avoids private node names,
host addresses, token labels, local artifact paths, and one-off run narratives.

## Product Direction

The normal consumer path is:

1. A consumer sends an OpenAI-compatible request to the gateway with a model,
   project/auth scope, priority, context envelope, and ordinary request body.
2. The gateway records demand for matching `ModelService` desired state when
   usable capacity is missing.
3. The daemon/reconciler performs allowed catalog, staging, readiness, prewarm,
   renewal, repair, and cleanup work under standing policy.
4. The gateway routes through gateway-native service-held capacity and records
   evidence.

Manual CLI surfaces remain only as debug, proof, emergency, or approval-gated
operator tools. They should not be the ordinary path for consumers.

## Surface Classification

| Surface | Current role | Target role |
| --- | --- | --- |
| `myce gateway preflight` | Consumer proof and request-shape validation. | Keep as debug/proof. Normal traffic should not require preflight rituals. |
| `myce gateway routes` | Route/catalog discovery and live-readiness proof. | Keep as debug/proof and status source. The daemon should act on stale or missing readiness. |
| `myce gateway allocate` | Non-reserving launch-envelope report for external tools. | Demote to debug/proof once gateway-native service capacity owns placement and worker counts. |
| `myce route certify/status` | Route contract proof ladder. | Keep as certification/debug. Certified route classes feed policy, not per-job operator choice. |
| `myce models service ensure/list/pause` | Desired-state serving control. | Keep as the product-facing model-service control plane until gateway demand fully owns creation. |
| `myce models prewarm/scale` | Manual supply creation. | Collapse into daemon/reconciler prewarm and replica reconciliation. Keep only as debug/emergency. |
| `myce models stage/locality` | Manual artifact/locality work. | Collapse into model ingestion and staging tasks under standing policy. Keep approval-gated apply paths. |
| `myce add-model` | Manual model source ingestion. | Replace as normal path with source-aware model demand. Keep as explicit import/debug. |
| `myce reconcile placement/status/drain` | Manual reconciliation proposal and executor. | Daemon owns automatic draining for allowed tasks. Keep status/debug and emergency drain only. |
| `myce pool plan/up/status/proxy/serve/down` | Transitional held-capacity bridge for header-unaware external tools. | Remove from normal path. Keep temporary debug/proof only until gateway-native service capacity covers those consumers. |
| `myce jobs admit/status/drain/execute/finish/cancel/result` | Explicit async workload control and result evidence. | Keep for durable async jobs and debug. Do not use as hidden sync gateway behavior. |
| `myce benchmark` and `myce calibrate` | Fleet measurement and calibration. | Keep as operator/proof tools. Calibration facts should feed optimizer priors only through documented import/apply policy. |
| `myce telemetry/recommendations/debug/doctor/status` | Evidence, diagnosis, and report surfaces. | Keep as observability. Avoid adding new report-only layers when an action path is missing. |
| `myce nodes/projects` | Membership and auth scope management. | Keep as explicit administrative control. Token and membership mutation remains operator-controlled. |
| `mycelium service/config/engines` | Daemon install, activation, and engine diagnostics. | Keep as install/debug/admin. Engine install/verify should become policy-driven where allowed. |

## Next Subtractions

- Gateway demand should create or bump `ModelService` desired state directly
  when policy allows it, instead of requiring `models service ensure` before a
  normal request can trigger supply.
- Catalog refresh, model staging, engine readiness, and prewarm tasks should be
  daemon-executed when policy allows, with audit evidence and loud blockers
  when live mutation or credentials are required.
- Pool/proxy and allocation-plan commands should stay out of consumer runbooks
  except as temporary compatibility tools for external clients that cannot yet
  use the gateway-native service path.
- Route and debug summaries should explain why supply is unavailable, but the
  reconciler should own the repair attempt whenever the action is safe.
