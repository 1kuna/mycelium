# Native execution

## Native payload isolation

Use [Native job isolation](../native-isolation.md) for platform guarantees,
limitations, credential handling and the optional AppArmor procedure.
The [approved isolation contract](../design-proposal.md#native-job-isolation)
covers stage exceptions and frozen registration behavior. Change a legacy
registration by publishing a new authored version. Keep database markers and
existing frozen bundle identities intact.

### Job environment

Every native command and sidecar, on every host, is told what it was given;
jobs need not set these themselves:

- `MYCE_CPUS`: the job's CPU allotment, the `needs.cpu_cores` the host ledger
  charged it (at least 1). The worker always sets it.
- `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `OPENBLAS_NUM_THREADS`,
  `VECLIB_MAXIMUM_THREADS`, `NUMEXPR_NUM_THREADS`, `RAYON_NUM_THREADS` and
  `GOMAXPROCS` default to `MYCE_CPUS` (OpenMP and PyTorch, MKL, OpenBLAS, Apple
  Accelerate, NumExpr, Rayon, Go). A value the job sets in `run.env`, a sidecar's
  `env` or its environment activation wins. On a shared interactive host only, a
  value above the allotment is lowered to it. The worker's own settings never
  pass through.
- `TMPDIR`, `TMP` and `TEMP`: the attempt's `{work}/.tmp`, and
  `MYCE_SCRATCH_DIR`: its `{work}/.scratch`. The worker owns both and they
  override authored values; retention and cleanup follow the attempt.
  `TMPDIR` is a short alias, `STATE/t/<12 hex>`, a link that resolves to
  `{work}/.tmp`. A Unix socket path must fit 108 bytes on Linux and 104 on
  Darwin, and the attempt tree alone can use most of that: servers that bind
  sockets in `TMPDIR`, such as vLLM's engine IPC, failed to start. With a
  default install's state path the alias leaves room for a UUID-named socket.
  Where links are unsupported the real path is used.
