# Start at boot

These are examples, not an installer. Nothing in this folder has been installed
on fleet machines. Build `myce` and install it at `/usr/local/bin/myce`. Use the
same absolute NAS mount path on queue and workers. Create the checkpoint root
on the mounted NAS first: Mycelium never creates a missing root, which could
otherwise hide an absent mount. A mount that exposes an existing local directory
must be configured by the operator to fail closed (for example an automount);
Mycelium verifies directory access, not the identity of a remote server.

On Linux, create a `myce` account and copy the appropriate unit into
`/etc/systemd/system/`. Edit the worker name, GPU platform, and lent memory.
Keep `/etc/myce/queue.env` and `/etc/myce/worker.env` private (mode 0600).
Both contain `MYCE_TOKEN=<your private token>`; worker.env also contains
`MYCE_QUEUE=http://<queue LAN address>:7331`. Never place a real token in this
repository. Then run `systemctl daemon-reload` and
`systemctl enable --now myce-queue` or `myce-worker`. The queue database and
worker spool must be on durable local storage. Keep the worker state directory
across restarts. One queue process owns a database; one worker owns a spool.
Only expose the queue to the trusted LAN.

On a Mac, edit every `REPLACE` field in `local.mycelium.worker.plist`, create its
log/state directories, and save a private copy (mode 0600) under
`~/Library/LaunchAgents/`. Load it with
`launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/local.mycelium.worker.plist`.
This LaunchAgent starts at login; for boot before login, an administrator can
install it as a LaunchDaemon under `/Library/LaunchDaemons/`, add `UserName`, and
use `launchctl bootstrap system`. Keep paths absolute. Credentials in the
private installed plist are readable by that account; jobs do not inherit them.

Queue recovery defaults to three minutes (`--restart-grace`). Workers keep
running during network loss and retry with capped exponential backoff. Missing
NAS access is shown in status and prevents new command starts. Running commands
must retry checkpoint saves themselves. Services cannot protect against NAS
hardware failure or a job that violates the checkpoint contract.
