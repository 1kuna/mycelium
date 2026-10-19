# Archive old state and install fresh

Old formats are refused; there is no converter. Every queue/worker/cache root
has `.myce-format` containing `v3` and a newline. Startup validates every root
before writing. Nonempty unmarked roots fail with `legacy_state_present`;
contents, checkpoints and unique evidence are preserved for operator handling.

Stop old services and prevent their restart before archiving whole data roots
outside active lookup paths. Inventory saved database, worker state and cache
paths, including custom roots. Preserve old configuration, credentials and GPU
environment. An old shared host-control directory and reconcile timer are retired
assets: preserve the directory. A worker installer upgrade stops, disables and
retires the obsolete user reconcile service/timer and `_host-start-check`
drop-ins, retaining backups and restoring their prior state if the upgrade fails. Do not start old binaries against new data or fabricate format markers.

Default data lives under `~/.local/share/mycelium/queue/state/` and
`~/.local/share/mycelium/worker[-INSTANCE]/state/`. Keep checkpoints and archives;
a fresh queue cannot adopt old attempts. Render configuration in the current
schema, retaining runtime options and secret references and removing retired
local-control settings. Run `myce install ROLE --config CONFIG --dry-run`, inspect
the plan, then perform the authorized install for matching queue and workers.

Queue maintenance controls admission from the first report. To stage a host,
set `myce host disable HOST` on the queue before starting its workers; the policy
persists across their first registration and rejoin. Configure its queue allowance
while disabled, inspect `myce host status HOST`, then explicitly resume. A new
host without prior policy is enabled by default. No local close/open operation,
root copy or timer is installed. Native activation and qualification remain a
separate operator task.
