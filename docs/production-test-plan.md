# Native acceptance checklist

Use an explicitly authorized maintenance window. Laptop fixtures prove queue,
API and command mechanics. Native GPU, daemon, manager, memory and boot behavior
need separate evidence. Keep host identity, paths, payloads and secrets private.

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
