# One entry on every accelerator

A small PyTorch training job that runs unchanged on CUDA, Intel Arc (XPU),
Apple Metal and CPU: one entry, one `uv.lock` covering Linux amd64, Linux arm64
and macOS arm64, and the device taken from `MYCE_DEVICE`.

- `.mycelium/adapter.yaml`: the entry. No targets, hosts or memory figure.
- `pyproject.toml`: torch from PyTorch's XPU index on Linux amd64, PyPI elsewhere.
- `uv.lock`: produced by `uv lock`; regenerate it after changing `pyproject.toml`.
- `train.py`: trains a small CNN on scikit-learn's bundled digits and writes
  `metrics.json`.

To try it, copy this folder into its own Git repository, commit, and run
`myce run --entry train --wait --out accepted`. The runs, the comparison with
variants and the known limits are in
[the research note](../../research/run-anywhere-entry-2026-11.md).
