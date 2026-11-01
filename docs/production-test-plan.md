# Native acceptance checklist

Use an explicitly authorized maintenance window. Laptop fixtures prove queue,
API and command mechanics. Native GPU, daemon, manager, memory and boot behavior
need separate evidence. Keep host identity, paths, payloads and secrets private.

> **Before queue cutover:** preservation checks must use
> `/api/status?detail=full` or complete `jobs=all` pagination, including private
> release-script copies. Default status (#439) can omit older completed jobs;
> omissions cannot prove preservation or release. Existing complete
> `/api/inspection` collectors remain supported. See
> [rolling fleet upgrades](operations.md#rolling-fleet-upgrades).

Record binary SHA/digest, config and secret references, device selection, manager
state and a recoverable rollback snapshot. Preserve current work and competing
owners. Verify NAS mount identity or verified file transport; qualify actual
runtime/model identity and simultaneous memory/headroom independently.

| Case | Required evidence |
| --- | --- |
| Install/redeploy | Saved settings, GPU environment and secret references preserved; inject failure and verify artifact/mode plus actual manager rollback. No local start gate or timer. |
| Queue maintenance | Both devices stop admission; drain preserves Started work; disable-stop retains exact custody until cleanup. Cancellation stays independent. |
| Freshness/outage | Missing/stale workers and queue outage cannot yield healthy quiescence or authorize new payloads. Reconnect never treats omission as release. |
| Reboot | Disabled policy survives; prior session/attempt evidence cannot authorize new work. Exact old-session cleanup and fresh worker reports precede resume. |
| Queue restart | Keyed lost-response replay returns the same job, changed specs conflict, cancellation reservations never run, stale protocols refuse and successful jobs do not rerun. |
| Runtime/resources | Verify native groups, Docker main/sidecar/network cleanup, pending daemon operations and RAM/cgroup limits. Timeout cannot settle custody. |

Bound each observation and record command exits and effects, release/rollback
receipts and unrun checks. Stop on the first failed native gate and preserve the
recovery snapshot. An actual reboot or service interruption needs its own task
authorization. Finish drained/disabled unless the task explicitly authorizes
resume. There is no offline rest certificate.

Use `myce measure` for the exact selected device/catalog, including representative
overlapping warmups. Qualification stays UNQUALIFIED until native simultaneous
peaks, runtime overhead, sidecars and OS headroom are measured. For private models
with `logs: none`, verify actual null stdout/stderr and Docker log-driver behavior,
including recovery; fake argv tests cannot qualify the daemon or journald.

## Pressure and status agreement

For #158, the laptop controlled-heartbeat fixture proves low live RAM holds
new work with `host_memory_headroom`, exposes live RAM separately from reserved
RAM, refuses the same need at worker launch, and admits it after headroom
recovers. It does not qualify OS PSI, macOS pressure, swap rate or live CPU use;
those remain unknown until implemented and measured.

Native follow-up owner: admission/status maintainer, in an explicitly authorized
maintenance window, one fleet check at a time. Confirm any operational holds
have been released before testing. This checklist does not schedule or authorize
a load run.

1. Confirm no competing native test or owner workload, current binary identity,
   free disk, memory, load and the worker's enforcement capability. Use a fresh
   explicit key; preserve current work and tokens privately.
2. Capture status JSON and the OS's available RAM/pressure readings at baseline.
   Use an existing bounded workload with an approved measured RAM need; avoid
   synthetic host exhaustion. A controlled memory-source fixture is sufficient
   for laptop proof but must not be presented as native telemetry evidence.
3. While available headroom is below that need, capture status, its typed queued
   hold, and evidence that no new payload starts. For a late change after
   assignment, capture the worker launch refusal and retry accounting. Compare
   declared need plus slack with the same measured signal at each fence.
4. After pressure clears, verify the same keyed job progresses, verify its
   accepted output and release, and preserve a short sanitized evidence ledger.
   Record unimplemented pressure signals explicitly as unknown. Keep #158 open
   for the OS-pressure/live-CPU/swap coverage and any unrun native checks.
