# In-place persisted-state conversion

Upgrade the queue and every worker together. The current binary refuses old
queue/spool formats and returns HTTP 426 for old or missing worker/report
protocols, including checkpoint publication.

Run conversion as the worker account on a stopped Linux/systemd installation:

```sh
"$NEW" host convert --local --config "$W0" --dry-run
"$NEW" host convert --local --config "$W0"
```

`NEW` is the reviewed new executable outside the installed binary paths. `W0`
is an existing worker config. Without `--config`, discovery uses
`$HOME/.local/share/mycelium/*/config.json`. All sibling queue and worker configs
must describe the same host. Configs select the existing DB, spools, host state,
credentials, environment custody, binaries and units. No target, path rebinding,
or hand-edited registry is needed. Both `myce-worker@gpu0.service` and
`myce-worker@gpu1.service` retain their names. External EnvironmentFile/joining
credentials are preserved; conversion does not read them or contact Docker.

Stop the queue, workers, reconcile service and reconcile timer before conversion,
and coordinate other restart sources with their owners. Conversion refuses active
units, held runtime/controller locks and a live SQLite writer. It does not stop
services. Saved native processes or Docker operations can still own executions;
stopped units and SQLite writers do not establish their release.

Dry-run opens SQLite read-only, includes saved WAL transactions in the job/copy
counts, lists the inspected file hashes and intended actions, and writes nothing.
It does not qualify local exclusion or promise successful reconciliation.
Operational JSON contains private installation paths: retain it privately.

Apply checksum-verifies an exact pre-conversion backup before modifying any
active file. SQLite checkpoints the verified private DB/WAL copy, then conversion
validates queue/spool formats and custody identities. Publication uses atomic
file replacement under a durable runtime fence. Only removing that fence commits
the complete installation. Jobs, attempts, copies, logs, auxiliary tables,
checkpoints and saved Docker endpoints stay intact. Only known installed
credential files can change from old JSON into EnvironmentFile syntax. Job and
checkpoint payloads, including files named `credentials.json`, `config.json` or
`state.json`, are opaque.

Backups, modes, timestamps, checksums, staged files and journals remain in
`<host-state>/.host-conversion/run-*`. Returned errors restore the original files;
a process crash keeps the installation fenced. Rerun the same command to verify
and restore its backup (`phase: restored`), then run it again to convert. A damaged
backup is refused and the fence remains. Preserve all evidence and the old binary.
An already converted installation returns `phase: unchanged` without writes.
A blocker is a stop condition: do not reset jobs, edit rows or delete custody to
bypass it.

Conversion preserves registry config, command, unit, spool, environment cache,
sessions and executions. RC registries omitted the current session: the exact
bound spool supplies it; receipt executions keep their own sessions. Conversion
never invents release. A provably idle installation starts CLOSED with confirmed
previous-close settlement. Saved execution, copy, model or daemon custody leaves
settlement pending; open returns `previous_close_unsettled` until normal CLOSED
reconciliation confirms release. An unresolved accepted Docker create remains
pending even if an empty daemon listing cannot find it.

After conversion, use preserving upgrades and start the existing units:

```sh
"$NEW" install worker --config "$W0"
"$NEW" install worker --config "$W1"
"$NEW" install queue --config "$QC"
systemctl --user start myce-queue.service
systemctl --user start myce-worker@gpu0.service myce-worker@gpu1.service
systemctl --user start myce-host-reconcile.timer
```

`QC`, `W0` and `W1` are the existing config paths. Worker installs enable the
reconcile timer; queue install starts/restarts the queue. Preserve the reviewed
configuration, including its secret mechanism and Docker endpoint. Workers
register while CLOSED and exit fenced successfully; they do not check in or
launch work. The queue serves while admission stays fenced. An explicit open
later starts those same units normally.

Use the separately installed, reviewed root-owned CLI (`ROOT_MYCE`) for privileged
exclusion observation and settlement. `WORKER_USER` is the installed account:

```sh
sudo -n "$ROOT_MYCE" host reconcile --local --worker-user "$WORKER_USER" --config "$W0" --timeout 30s
sudo -n "$ROOT_MYCE" host status --local --worker-user "$WORKER_USER" --config "$W0" --expect closed --verdict --wait --timeout 30s --json
```

Repeat reconcile/status until `host_settlement` is `confirmed`; a CLOSED exclusion
PASS alone does not imply settlement. Keep the installation CLOSED at the end of
the upgrade window. Opening requires its separate workload-owner authorization;
for a shared application host this follows the application's transition away
from its legacy GPU workload. With that authorization, take revision `R` from the
fresh CLOSED status and run:

```sh
sudo -n "$ROOT_MYCE" host open --local --worker-user "$WORKER_USER" --config "$W0" --expect-revision "$R"
```

Laptop tests use release-writer fixtures, fake services/process evidence and local
HTTP. Native systemd, Docker, privileged exclusion and consumer cutover acceptance
remain separate operational gates.
