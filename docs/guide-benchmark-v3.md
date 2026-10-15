# Onboarding benchmark (experimental v3)

Throughput measurement is opt-in. Pass `myce join --onboarding-benchmark` to
measure before admission, or use the explicit `myce benchmark` command below.
Default joins do not run or wait for a benchmark and leave its cache untouched.
They report hardware facts and use existing placement fallbacks when throughput
is unknown. Results from an enabled benchmark are in
`capability_profile.benchmark` in the status API, the CLI capability summary, and
an additive benchmark detail in the status page. Fake inventory in `myce dev`
does not load the host; tests inject measurements instead.

The benchmark uses at most **nine minutes**, including waiting for permission to
use a daily machine. Each resource gets at most 60 seconds and runs serially:

- CPU: single-thread SHA-256 on an 8 MiB buffer, for about 250 ms.
- Memory: copies between two 64 MiB buffers for about 250 ms.
- Disk: write, sync and read 16 MiB in the worker's own state folder. This is a
  short filesystem measurement; cached reads are included, not sustained media speed.
- Network: download and hash-verify an 8 MiB synthetic object from the actual
  file store. Upload prepares the object outside the download timer. One fixed
  content hash is reused, so it occupies at most 8 MiB in the store. This measures
  end-to-end HTTP delivery including hashing, not raw link capacity.
- Accelerator: only qualified extensions with a timed reference command run.
  PyTorch repeats its existing small pointwise native-reference computation for
  two seconds, synchronizing each result through the host and checking stability.
  This measures short reference work, not peak matmul FLOPS. llama.cpp replays its
  unchanged tiny-model prompt and reports its completed token-evaluation rate,
  excluding model loading and prompt evaluation. Both final outputs must still
  match their frozen native references. No CPU fallback, new baseline, or tolerance
  change is introduced.

Extension setup, downloads and builds are explicit preparation and are outside
this short measurement. The starter versions with timed references are
`pytorch-script@0.2.4` and `llama-cpp@0.1.1`. Existing frozen versions keep working;
without a timed command their throughput stays unknown. An optional extension
`benchmark` argv receives the same check environment plus `MYCE_BENCHMARK_OUTPUT`.
It writes the actual reference output and a small timing JSON document:

```json
{"units": 100, "seconds": 2, "unit": "references/s", "reference_units": 1}
```

`units` counts completed work; `reference_units` counts units in one reference.
Supported units are `references/s` and `tokens/s`. Both counts and duration must
be positive and finite. Mycelium validates the output against the extension's
bundled native baseline; a timing failure cannot qualify an extension or widen
its tolerance.

Daily use follows the existing owner, power, heat and fresh-signal policy:
five idle minutes, external power for laptops and known safe heat. Owner return
or unsafe/unknown signals cancel a running measurement. The benchmark runs before
job admission and model loads, never over a recovered attempt. Per-host benchmark
locks serialize resource workers sharing a host-control folder. A missing resource
or interruption is reported as partial, with a reason; retry on the next join with `--onboarding-benchmark`.
Complete results are reused when hardware/driver identity, CPU count, memory,
extension digests and file-store address are unchanged. Changed identities trigger
measurement on an opted-in rejoin, as with extension requalification; live driver replacement
while a worker runs is not supported.

Installed service configurations can set `"onboarding_benchmark": true`;
`service ... -- join` also accepts `--onboarding-benchmark` in its join argv.
Existing configurations default to false. On personal or polite hyphae, combine
the opt-in with `--daily-use` and the appropriate `--laptop` or `--desktop`
declaration so measurements respect owner activity, power and heat.

For an explicit isolated measurement, use your own state, prepared extensions
and test file-store queue. Add `--daily-use --laptop` on a daily laptop:

```sh
myce benchmark --gpu metal --state-dir ./benchmark-state \
  --extension-root ./extensions --queue http://localhost:18451 --daily-use --laptop
```

A complete explicit run exits zero. A partial run writes the profile and exits
nonzero. Missing throughput is never recorded as a zero-speed measurement.

Placement first applies all existing qualification, memory, availability, host,
project, checkpoint, priority, fairness, preemption, production-hold and start
authorization rules. Among free eligible hyphae it predicts one reference's
execution time plus known model/environment preparation and input-file delivery.
Measured predictions rank ahead of unknown ones. Comparisons within a frozen
reference group use predicted seconds; different references have a stable digest
order rather than an invented speed conversion. If neither candidate has a
prediction, first programs keep the memory-capacity fallback and other work keeps
its existing placement. Equal measured predictions use hypha ID.

This is a provisional workload estimate: there is no automatic model of arbitrary
program size or data-dependent execution time yet. CPU, memory and disk rates are
reported separately; they are not blended into an arbitrary accelerator score.
There is no queue for a busy faster hypha, automatic migration, or new service.
