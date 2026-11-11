# Accelerator driver fault quarantine

Linux machine workers read the current boot's kernel journal from the worker's
start time, then follow the journal cursor every five seconds using the injected
worker clock. They never read the whole boot: a large boot journal can take far
longer than the two-second read budget. Faults logged while no worker was running
are not replayed; quarantine recorded before a restart persists. `journalctl -k -b`
runs as the worker account, without sudo; a readable `/dev/kmsg` is the fallback.
The account needs ordinary kernel-journal read permission (for example, membership
in `systemd-journal` where configured). If neither source is readable, the worker
logs `accelerator fault monitor unavailable` and reports `fault_monitor_error` in
`myce status` until a read succeeds; monitoring cannot detect faults in that
state. No cheap qualified Apple source is currently available, so Apple monitoring
is skipped.

A fault within one second of a stop request, delivered signal or exit of an
attempt on the same device is teardown, not a card fault. Payloads that exit while
device work is in flight can fault the driver (#154). The worker logs
`device fault attributed to teardown`, records the event in its local quarantine
state and leaves the device usable. Released attempts' timestamps are kept for one
minute. A fault seen less than a second after it was logged waits one scan, so an
exit recorded just after it still counts. A card fault that crashes its job also
falls in that window, so the third teardown incident on one device within an hour
quarantines it (`repeated teardown faults escalated to quarantine`); fault lines
within a second of each other count as one incident.

An xe/i915 fault or NVIDIA Xid matched to the device's PCI BDF sets its health
reason to `device_fault_xe`, `device_fault_i915`, or `device_fault_nvidia_xid` and
withholds its placement capacity. Other devices remain usable. The queue moves
affected running jobs directly to `queued` using the existing bounded
infrastructure-loss budget; their old exact attempts receive stop/kill directives
and retain custody until cleanup is acknowledged. Reassignment creates a new
attempt under the same job ID. This transition does not expose `stopping` or a
terminal job state when a retry remains available. Infrastructure exhaustion
still follows the existing terminal policy. Stale results cannot publish.

From the queue release containing #1142, a job's device-fault loss records
`attempt_history[].device_fault` (physical host, backend, stable device identities
and queue time). A retry cannot consume that host's last healthy accelerator of
the same backend. Occupied healthy cards still count; quarantined, unavailable
or administratively excluded cards do not. Another host or a card with a healthy
spare remains eligible. A group cannot consume all surviving cards. The queued
job reports `hold.kind: device_fault_last_healthy` and waits on the same job ID
and submission key. This protection is queue-side and works with older workers;
it does not require new recovery fields. Existing pre-upgrade fault history is
recognized when its retained copy identifies the backend on that worker.

The typed last-healthy hold pauses a relative production start or recovery
budget without spending more infrastructure retries. It saves the time remaining
at entry and resumes that amount when an eligible route returns; status keeps
`device_fault_last_healthy` and shows `paused for device recovery`. The original
`start_window` remains unchanged for a later started-attempt loss. Explicit
absolute `start_by` and queued-only `give_up_at` still bind.
A started infrastructure interruption renews the original first-start window,
including one derived from explicit `start_by`. That renewed cutoff keeps its
absolute semantics during the hold; an unstarted loss cannot renew it.
The saved amount survives any capacity wait after clearance and queue restart;
capacity recovery cannot refund startup time spent before the device-fault hold.

An unrecoverable quarantine must not strand a job forever. The hold expires one
`MaxProductionStartWindow` (the existing one-hour maximum availability window)
after the latest queue-recorded device-fault loss, including across queue
restarts. It fails with `diagnostic_code: device_fault_recovery_timeout` and a
launch timeout even when the first report after the bound clears quarantine;
an active hold expires before its budget can resume. The timeout is a sanitized
launch cause with the same predicate. This bounds withheld capacity and
caller latency when recovery never arrives; its cost is giving up on a recovery
that takes longer than the existing maximum start window. Earlier caller
cutoffs win. This bound applies to held jobs of every priority and never clears
quarantine or releases old attempt custody. If the route disappears altogether,
ordinary unavailable-route startup accounting resumes.

After an operator clear, fresh inventory reporting both cards usable releases
the two-card guard and either card can run the retry. An operator request alone
is not health evidence and cannot authorize a sole surviving card; that case
still requires new health-proven recovery or another healthy device/host.

From the worker release containing #1142, non-escalated quarantine also clears
automatically after positive vendor health evidence and uninterrupted kernel
observation. Intel uses `xpu-smi health -d PCI-BDF -j`: core temperature, power
and memory (native JSON keys `core_temperature`, `power_health`,
`memory_health`) must report `OK`, except for a component proven unsupported by
same-batch sibling comparison below. No supported component may report a warning
or critical state. NVIDIA uses UUID-selected `nvidia-smi -q -x`; the matching device
must explicitly report recovery action `None`. Missing tools, unsupported fields,
unknown required health and failed reads keep quarantine. Other backends can
supply the injectable health probe; unsupported backends do not auto-clear.

With `device-health-v1` negotiation, the last rejected proof appears in
`devices[].quarantine_recovery_blocked`: a typed `code`, optional `component`
or `exit_status`, and the worker's `observed_at`. Examples are
`health_component_unknown` with `component: memory`,
`health_component_missing` with `component: power`,
`health_probe_permission_denied`, and `health_probe_exit` with `exit_status`.
The worker logs a rejection when its classification or fault changes; repeated
identical rejections update status without flooding the log. Raw vendor output,
paths and error strings are never included. A new healthy reading removes the
last rejection; it does not bypass the recovery window. Older queues receive
no additive diagnostic field, while recovery-v1 clear receipts remain compatible.

XPU capability comparison is an orchestrator agent default (2026-11-09, under
MY-34), not a maintainer ruling; see [the decision](../DECISIONS.md). When a required
component is explicitly `Unknown` / “cannot be determined” on the quarantined
card and an otherwise healthy same-model sibling in the same health batch, that
component is excluded from this proof. `devices[].unsupported_on_device` lists
canonical component names such as `memory`. Model identity uses the existing
nonempty hardware/driver fingerprint. All other required components must be
`OK` on the target; the sibling must pass every determinable required component
and have no inventory exclusion, quarantine or new kernel fault. Missing,
malformed or failed readings cannot establish unsupported capability. Without a
healthy same-model sibling (including a single-card host), Unknown retains
quarantine. Each batch compares fresh readings; there is no cached model
allowlist. Each reading retains its query start and completion timestamps;
completing a sibling query never renews target evidence. Neither query may
begin before a kernel-monitor gap. The whole comparison must fit one control
interval plus the previously measured kernel service-cost prediction, frozen
before the batch starts. This bounds skew without allowing a stalled batch to
expand its own allowance; an overly slow comparison retains quarantine. A
kernel scan must start after both queries complete. Status reports
`health_probe_monitor_gap` or `health_probe_comparison_stale` when those fences
reject evidence. The exclusion accompanies the durable clear receipt in status
and never shortens the kernel observation window.

Linux XPU worker installation provisions health access automatically, before
service activation. Binary-only release rollout also performs this idempotent
prerequisite on service startup, before reading credentials or accepting work.
The root-owned `/etc/udev/rules.d/70-mycelium-xpu-health.rules` grants the worker's
existing primary group mode `0660` on GPU MEI nodes whose ancestor driver is
`mei_gsc`. The [retained sanitized trace and matching driver
observation](../evidence/xpu-health-access.md) show xpu-smi 2.2.0 attempting
`O_RDWR|O_CLOEXEC` on a GPU MEI node, denied for the worker; root's query had no
such denial. This
exchange opens a bidirectional HECI channel even though the requested operation
is a health read. Linux checks both read and write permission for `O_RDWR`, so a
read-only group grant cannot satisfy the observed invocation. `0660` supplies
that required open without world access; no read-only alternative has been
qualified for this tool. PMT was opened `O_RDONLY` and retains read-only access.
This is a device-access grant, not a health-only boundary: any process with the
worker's primary group can attempt channel operations beyond health queries,
subject to the driver's own controls. The installed helper manages permissions;
it does not restrict channel operations to health. A narrower helper exposing
only qualified health queries is unimplemented and unqualified future work.
Unrelated management-engine MEI nodes remain untouched. Xe-owned Intel PMT `telem` sysfs
attributes receive that group and mode `0440`; no world access is added. The
installer reloads the rule and applies the same narrow permissions to current
nodes, without probing the GPU or restarting a service. Existing group membership
is already effective, so no user-manager restart or manual group step is needed.
Installation needs noninteractive administrator authority (`sudo -n`); inability
to provision returns `xpu_health_access_unavailable` before activation. No
sudoers grant or elevated health command is installed. Each startup/install
reapplies the setup, even when rule bytes match: a rule published before a failed
reload or node update is not proof of completed access. Activation stays blocked
on failure and the next supported install/start repairs that partial application.

The root-only helper and baseline journal live under
`/var/lib/mycelium/xpu-health-access`. The udev helper records each node's
original group/mode and filesystem identity before granting access, including
new/recreated nodes. A stable root lock serializes udev, install and removal.
Owners are keyed by account and installation identity; several installations
using the same primary group share access. Different primary groups refuse
rather than replace another owner's grant.

Supported uninstall stops its worker, releases only that installation's owner
record, and retains grants while another installation still owns them. The last
owner removes the unchanged Mycelium rule, reloads udev and restores each
still-current grant to its recorded original group/mode. Node identity checks
prevent restoring an old device's receipt onto its replacement. Subsequent
administrator node edits are preserved; a changed/non-Mycelium rule refuses
cleanup instead of deleting it. Cleanup failure returns
`xpu_health_access_cleanup_failed` and retains the service configuration for a
supported uninstall retry. After grants are restored, final teardown publishes
a root-only completion record at
`/var/lib/mycelium/xpu-health-access.teardown` before deleting ownership or
validation metadata. The same owner can resume directory cleanup using that
record even if `gid`/`format` are already gone; install and udev application
refuse while teardown is pending. The completion record is removed only after
the state directory is gone. The root helper/journal are removed after successful
last-owner cleanup; the inert lock file remains for coordination. This requires
noninteractive administrator authority for uninstall as well as install; dry-run
uninstall makes no changes.

Read-only xpu-smi 2.2.0 observations found worker temperature Unknown and root
temperature OK, with GPU MEI and PMT access denied to the worker; this did not
isolate an MEI-only dependency. Memory health remained Unknown even as root.
The orchestrator supplied earlier healthy-sibling evidence of undeterminable
memory health; a fresh sibling query was skipped because a production attempt
was running. Installer application and worker-identity temperature/automatic-clear
verification happen through the release rollout, not manual host edits. Native
acceptance remains tracked in #1185; laptop fake probes do not establish it.

Vendor probes have a separate single-flight lane, so a slow tool cannot stop
kernel scanning or create a probe backlog.
Completed health readings remain pending until a subsequent kernel scan can
validate them; exceeding the heartbeat's collection budget does not discard proof.

The observation window is an **agent default**, implementing the relayed consumer
request, not a maintainer ruling. Following MY-34, it is the largest of the
observed fault-to-first-normal delay, the local control/probe gap, the existing
one-second teardown attribution interval, and that device's previous successful
clear window. At least two distinct normal observations must span it. The first
normal delay is an observed upper bound on recovery, not the exact instant of
hardware recovery. A matching new fault (including one awaiting teardown
classification), an unhealthy probe, a kernel-monitor gap or a worker restart
discards the current healthy streak. Kernel evidence must follow the health
reading. Repeated-teardown escalation still requires an operator clear; normal
vendor health alone cannot erase it. The cost of the window is temporarily
withheld capacity, in exchange for observing a complete local recovery interval.

Clear evidence persists in `devices[].quarantine_recovery`: `fault_event`,
`healthy_since`, `cleared_at`, `observation_seconds` and `probe`. The queue adds
`observed_at` when it observes the quarantined-to-healthy transition; replaying
the same proof does not refresh that queue timestamp, including across reports
that omit the optional field during negotiation or downgrade. A new worker-side
fault retires the previous clear proof; its historical observation window remains
calibration evidence and an operator clear does not restore the retired proof.
Retained queue receipts are historical: compare their queue time with the job's
latest fault loss, and check the current device reason. Such a transition after
the job's loss allows a retry even on a sole recovered card. Worker wall time
never orders queue admission. Capacity still requires a successful inventory
refresh, and old attempts still require exact cleanup. Auto-clear requires a
worker upgrade; a queue-only release provides the cascade protection alone.
Recovery evidence is negotiated with `device-recovery-v1`; newer workers omit
the additive field for older strict-decoding queues, including after downgrade.

Quarantine persists in the local worker spool across worker restarts. A changed
Linux boot ID clears old quarantine; faults in the new boot can quarantine again.
After repairing a device, an operator can request a clear without stopping the
worker (replace the example directory with the configured worker state directory):

```sh
myce device clear-quarantine --state-dir ./worker-state --device 0
```

The command prints `clear_requested: true`; this confirms the local request,
not healthy capacity. The worker applies it on the next successful log scan,
provided no newer fault has replaced the acknowledged fault. Verify the device
reason is empty and capacity is available in `myce status` before relying on it.
Historical fault records are acknowledged so clearing does not immediately
replay the same fault. Raw kernel text remains local and is neither published
in status nor retained in quarantine state. Fault sources and family matchers
are injectable for laptop verification.
