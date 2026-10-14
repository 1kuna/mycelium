# Polite part-time hyphae (experimental v3)

Dedicated hyphae keep the existing default: offer resources whenever awake.
For a daily-use machine, add `--daily-use` to its usual `myce join` command.
No GPU, power, fan, sleep or screen settings are changed. No input is recorded.

```sh
myce join --daily-use --max-jobs 1
```

Daily-use hyphae take work after **five idle minutes**, on confirmed external
power when battery-equipped, with a known safe heat signal. Batteryless desktops
need a confirmed batteryless power report. Plugging in never overrides owner
activity. An unknown required signal means waiting with a reason in CLI/HTML
status and `capability_profile.availability` in the status API. Aggregate idle
seconds, power source and thermal state are reported; input contents aren't.

A detected laptop needs external power even in dedicated mode. `--laptop`
explicitly enforces that rule when automatic battery detection is unavailable.
Use dedicated mode for batteryless servers; daily-use mode for personal machines.
The battery-equipped classification is retained throughout a worker session
if a subsequent power read fails. This first slice configures foreground joining;
installed-service configuration for daily use is a future integration step.

Signals refresh every five seconds independently of queue connectivity.
Required readings older than 15 seconds withhold work. A sampling gap, including
sleep, starts a fresh five-minute awake window; the worker never prevents sleep.
Owner return, battery use, unsafe heat or unknown required signals stop admission
locally and through the queue. Resumable commands get SIGTERM and up to ten
seconds to checkpoint, then are killed if needed. Non-resumable commands stop
immediately and later restart from scratch. These planned stops don't spend the
infrastructure-loss retry budget. Checkpoints use the existing shared-root
contract; file movement without shared storage is the separate #42 slice.

Model loads and calls also stop when availability is withdrawn. Pins never
override owner availability. Engines stay reserved until cleanup confirms
release; queued callers may retry another hypha. Withholding remains temporary:
fresh eligible signals allow new work without rejoining.

`--max-jobs N` is the **one optional per-machine limit** (default zero: unlimited).
It counts simultaneous batch jobs plus in-flight model calls across all workers
with the same `--host-id`. A stopping batch continues to count until acknowledged.
Ready idle model servers aren't jobs. If workers report different positive
limits, the smallest is used; configure the same limit on every worker of a
physical machine. Lowering a limit blocks new work rather than killing existing
jobs. Memory capacity remains an independent admission constraint. This limits
concurrency, not watts, GPU percentage, fan speed or noise.

## OS signals and limits

- **macOS:** the binary links CoreGraphics' combined-session aggregate event age,
  IOKit's power-source snapshot, and Foundation's process thermal state. It needs
  no event tap, keylogging, screen capture or sensor permission. Daily work
  withholds at fair/serious/critical; nominal must persist for 60 seconds after
  a heat hold. Building on macOS needs cgo and the usual Xcode command-line SDK.
  A macOS binary built without cgo reports unknown signals and waits in daily mode.
  The aggregate query covers the worker's login session; fast user switching
  and coverage of other logged-in users remain unverified.
- **Linux:** logind, queried through `loginctl`, covers every listed user session.
  Work waits five minutes after every user session's idle hint, which is more
  conservative than claiming it is a last-input timer. Hint age uses the same
  OS monotonic clock as logind, rather than boot uptime that includes sleep. No user session, an
  uncovered session, denied access or missing desktop idle hints means waiting.
  Power comes from read-only kernel `power_supply` attributes. Heat comes from
  recognized CPU/GPU hwmon sensors with valid max/critical limits and no faults.
  All discovered relevant sensors must be usable. A max alarm/limit holds work;
  a critical-only sensor uses critical minus 10°C. Recovery needs readings at
  least 5°C below the guard (or a more conservative supplied hysteresis) for
  60 seconds. Unknown heat holds work. The recognized sensor list is intentionally
  small; unsensed accelerators and these margins are not hardware-qualified.
- **WSL2:** owner activity, power and physical heat must come from the **Windows
  host**, never the Linux guest. This slice detects WSL2 and reports unknown,
  withholding daily work. The proposed Windows executable/interop bridge is
  deferred: `GetLastInputInfo` covers only its interactive Windows session,
  unattended starts need verified session coverage, and Windows has no uniform
  nonprivileged live thermal API. Guest readings cannot safely substitute.
  Dedicated server behavior remains available; use `--daily-use --laptop` for a
  personal WSL machine so absent host signals cannot admit work.

Thresholds follow #25's research where compatible with #43. Unlike the research
note's proposed missing-heat exception, missing heat **withholds** daily work, as
required by the resolved unknown-signal rule. These OS observations aren't a
promise that a machine is silent, that a motionless owner is absent, or that all
physical components have thermal coverage. No quietness claim is made.

## Laptop tests

Injected `OwnerSignalSource` and `Clock` cover idle, battery, heat, unavailable
signals, cooling dwell, sampling gaps and stale readings. Queue tests cover
withholding, cross-worker checkpoint reuse, planned-stop retry accounting, and
shared-host batch/model-call limits. A real command test shows local owner
return saves a checkpoint even while a queue check-in is blocked. Existing
`myce dev` remains the real queue/command runner with fake GPU inventory.
