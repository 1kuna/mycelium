# Native acceptance checklist

Use this only in an explicitly authorized maintenance window. Laptop fixtures
prove API, scheduling and command mechanics; actual GPU, daemon, manager, account,
boot and consumer behavior need native evidence. Keep machine identity, addresses,
private paths, payloads and secret references in private evidence, not this public
checklist. The [approved design](design-proposal.md#testing) sets the bounds.

## Prepare

- Record the exact binary SHA-256 and version, config/secret references, selected
  physical host/device, manager state and a recoverable rollback snapshot.
- Verify no competing controller or other owner can launch on the selected host.
  Preserve current work; never stop an unrelated owner to make a check pass.
- Confirm the intended NAS mount identity or file-store transport and capacity.
  A writable mountpoint directory does not establish the remote filesystem.
- Check runtime/model identity and measured joint memory/headroom independently
  of catalog file sizes. Do not change precision, context or declarations to fit.
- Follow the [identity checklist](probe-s11-worker-identity.md) for the separate
  root-owned control copy, account isolation, ACLs and actual user-manager access.
- Inventory all state roots. Old or unmarked nonempty roots require
  [archive plus fresh install](guide-fresh-install.md), never an inserted marker.

## Component checks

Each case has one fixture, owner, total deadline and closed final state. Stop
on the first failure, preserve evidence, and use the same verified recovery path.
Do not stack deadlines or treat elapsed time as proof of release.

| Case | Evidence required |
| --- | --- |
| Install/upgrade | Explicit overlay preserves omitted config and secret references, CLOSED revision remains unchanged, all card units/start gates agree. Inject an installation failure and verify artifact/mode and actual service/timer rollback. Incomplete rollback fails. |
| Owner switch | Close fences every native/Docker launch; exact cancellation coverage and other-owner release precede revision-bound open; every required worker and the application's own health/identity pass. |
| Offline recovery | Local release includes all owned cgroups, GPU holders, endpoints and accepted late daemon operations. Queue outage can leave settlement pending; reconnect must settle exact custody before full rest. |
| Reboot resume | CLOSED survives; previous observations do not. Recover at the saved stop deadline without grace renewal, reconcile before admission and reject stale attempts. Real reboot requires its own authorization. |
| Queue stop/start | Durable keyed replay returns the same job after a lost reply; conflicting spec rejects, cancellation reservations never run, old protocols reject and no successful terminal job re-executes. |
| Rest verdict | Closed/ready checks cover positive violations, missing sources, FAIL over UNKNOWN, stale/future evidence, fenced work, unknown placement and terminal-but-unreleased attempts. Printed verdict and exit must agree. |

Allow at most 60 seconds for install/upgrade and queue stop/start, including at
most 30 seconds outage plus 30 seconds observation; 30 seconds for control/health
switches and recovery; 10 seconds per rest observation. Separately declare cold
model/application-delivery cases at no more than 300 seconds including teardown.
Declare reboot's total deadline including downtime and at most 120 seconds after
return. Capture final CLOSED, release and manager state on success or failure.

## Model and memory qualification

Use `myce measure` with the exact selected device, worker identity/spool and
approved catalog, optionally overlapping representative warm-ups. Record combined
native peaks, runtime overhead, sidecars and OS headroom. Sampling may miss brief
peaks. Preserve the conservative allowance as UNQUALIFIED until simultaneous
native evidence qualifies it; never rewrite the allowance on an owner switch.
See [RAM admission and ceilings](operations.md#host-ram-admission-and-ceilings).

For private model/server work use `logs: none` and verify actual null-device
stdout/stderr and Docker `none` logging behavior, including recovery. Check the
runtime's separate files/telemetry independently. Fake Docker argv tests do not
prove daemon log-driver or journald behavior. See [server log policy](guide-servers.md).

## Report

Record command exits, exact revision/attempt identities, observed effects,
release/rollback evidence and unrun native items privately. Public reports use
sanitized IDs, counts and status. Deployment is a separate decision after the
component checks; do not call a successful install or queue admission acceptance.
