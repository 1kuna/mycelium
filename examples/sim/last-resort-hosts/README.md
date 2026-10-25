# Last-resort hosts

Compare identical synthetic inventory/work with and without `last_resort`:

```sh
myce sim --fleet examples/sim/last-resort-hosts/baseline.yaml --workload examples/sim/last-resort-hosts/workload.yaml --days 0.003 --seed 1
myce sim --fleet examples/sim/last-resort-hosts/fleet.yaml --workload examples/sim/last-resort-hosts/workload.yaml --days 0.003 --seed 1
```

The default MY-18 order puts the first CPU batch (200 seconds) on `small`.
With `small` marked last-resort, `ordinary` takes it instead. One second later,
the shorter batch (40 seconds) immediately uses the other host, including the
last-resort host when the ordinary host is busy. Both runs complete two batches
with no failures or unfinished work, zero production p95 wait, and a 200-second
makespan. `last_resort` on a simulator machine configures its physical host's
queue policy; it does not change the simulator's execution timers or planner.
Synthetic results do not qualify native hardware.
