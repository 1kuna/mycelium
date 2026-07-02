# Service Upgrade Runbook

This runbook is public-safe. Do not add private hostnames, IP addresses, token
values, local operator paths, or one-off fleet narratives.

## Current Boundary

`mycelium service upgrade` is a dry-run planning surface. It prints binary
hashes, backup/replace/restart/verify commands, and rollback commands, but it
does not replace a live service binary. This is deliberate: some platforms can
gate LAN reachability on service identity, signing, or user-session permission.

Live activation is an operator maintenance-window step. Use this runbook to
prepare the proof packet before that step.

## Plan The Service

```bash
mycelium service plan \
  --config ~/.mycelium/peer.json \
  --binary /path/to/mycelium \
  --name real-fleet \
  --user
```

For a system service:

```bash
mycelium service plan \
  --config ~/.mycelium/peer.json \
  --binary /path/to/mycelium \
  --name real-fleet \
  --system
```

On macOS, use the system-service plan when a user LaunchAgent cannot reach LAN
peers because of Local Network permission or binary-identity behavior. The
generated system LaunchDaemon runs as the selected non-root user so the service
keeps the existing user-owned config, logs, and control DB.

## Dry-Run An Upgrade

```bash
mycelium service upgrade \
  --config ~/.mycelium/peer.json \
  --binary /path/to/stable/mycelium \
  --target-binary /path/to/new/mycelium \
  --name real-fleet \
  --user \
  --dry-run
```

Use `--json` when another tool should validate or archive the upgrade packet:

```bash
mycelium service upgrade \
  --config ~/.mycelium/peer.json \
  --binary /path/to/stable/mycelium \
  --target-binary /path/to/new/mycelium \
  --name real-fleet \
  --user \
  --dry-run \
  --json
```

The dry-run output is the operator packet:

- current and target binary digests;
- backup path;
- current-service status command;
- backup command;
- replace command;
- restart command;
- post-restart status command;
- rollback command.

Run the printed status command before backup and after restart. If status or
peer diagnostics fail after restart, run the printed rollback command and keep
the old binary live.

## macOS User LaunchAgent Diagnostics

Use these read-only checks when the old service binary can reach LAN peers but
the candidate binary cannot:

```bash
launchctl print gui/$(id -u)/com.mycelium.real-fleet
codesign -dv --verbose=4 /path/to/stable/mycelium 2>&1
codesign -dv --verbose=4 /path/to/new/mycelium 2>&1
spctl -a -vv /path/to/stable/mycelium /path/to/new/mycelium
xattr -l /path/to/stable/mycelium /path/to/new/mycelium
log show --style compact --last 2h \
  --predicate 'process == "mycelium" OR eventMessage CONTAINS[c] "mycelium"'
```

Compare CodeDirectory identifiers and CDHashes before blaming Mycelium routing.
Ad-hoc CLI signatures can change identity whenever the binary changes; macOS
Local Network approval can therefore behave like a binary-identity gate.

## Success Criteria

A live upgrade is proven only when all of these are true after restart:

- `service status` succeeds;
- peer health succeeds;
- seed or peer diagnostics show expected LAN reachability;
- the gateway route/catalog proof expected for that deployment succeeds;
- rollback remains available until the new binary has survived the intended
  observation window.
