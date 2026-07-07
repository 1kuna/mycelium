# Mycelium Runbooks

Runbooks are public-safe operator procedures. Keep them generic: no private
hostnames, IP addresses, token values, local filesystem paths, prompts,
completions, project/customer names, or one-off run narratives.

- [Service Upgrade](service-upgrade.md): dry-run upgrade packet, activation
  boundary, diagnostics, and rollback proof.
- [Operator Surface Inventory](operator-surface-inventory.md): current manual
  CLI surfaces classified as product path, debug/proof, transitional, or
  collapse targets.
- [Workload And Pressure Proof](workload-pressure-proof.md): explicit queued
  workload proof, local executor smoke, calibration packet, and live pressure
  boundary.
