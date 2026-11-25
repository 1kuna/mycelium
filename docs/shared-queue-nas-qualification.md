# Qualifying the NAS for the shared queue

This is the real-NAS half of [stage 2](design-queue-beyond-one-host.md#shortest-path-from-today)
of [MY-40](DECISIONS.md#my-40) ([#1176](https://github.com/1kuna/mycelium/issues/1176)).
It decides two things before any authority moves:

1. Whether the NAS can run the shared queue's PostgreSQL authority at all:
   durable commits, restarts, NAS OS updates, and independence from the file share.
2. Which volume holds the database: the busy data volume or the quiet backup volume.

Nothing here serves clients or changes the fleet. The single SQLite queue stays
authoritative until stage 3. **The storage owner runs or approves every NAS-side
step.** The orchestrator runs the harness from a fleet machine once the storage
owner agrees to the time window. Keep host names, addresses, share and volume
names, accounts and raw reports under the gitignored `.reports/` directory, never
in this repository or on GitHub.

## The harness

`scripts/queue-qualify` is a small separate Go module. Its static binary runs on
any fleet machine with no driver to install. It adds no qualification code or
PostgreSQL dependency to the `myce` binary and leaves stage 2's module file alone.
It uses the same pgx v5.13.0 driver as stage 2.

```sh
cd scripts/queue-qualify
CGO_ENABLED=0 GOOS=linux  GOARCH=amd64 go build -o queue-qualify-linux-amd64 .
CGO_ENABLED=0 GOOS=linux  GOARCH=arm64 go build -o queue-qualify-linux-arm64 .
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o queue-qualify-darwin-arm64 .
```

The connection string comes from the environment variable named by `--dsn-env`
(default `MYCE_QUAL_PG_DSN`) or from a `--dsn-file` with mode 600. The harness
never prints it. Errors are redacted, and reports record only the endpoint
class, such as `private-network`. Never put the fleet token in that variable or
file, and never point the harness at a queue or worker `config.json`.

| Command | What it measures |
| --- | --- |
| `info` | Version, durability settings, data-directory identity and `SELECT 1` round trip |
| `commit` | Durable coordinator-turn renewals and ordered change appends at the live rate. `--watch DIR` probes a file mount in the same process. |
| `durability-write`, `durability-verify` | A sequence-numbered durable writer with a synced local ledger, then a check of every acknowledged row after a restart, crash, reboot or OS update |
| `load` | Bounded bulk file I/O, written into one new run directory and never deleted |
| `catchup` | Snapshot creation into a new run directory, snapshot copy from the share with hashing, and change-log replay into a local SQLite copy |
| `summarize` | One comparison of saved reports |

Each run writes a JSON and a Markdown report to `--out` and exits nonzero when a
check fails. `--condition` and `--volume` take generic labels such as `idle`,
`bulk-same-volume`, `busy` or `quiet`.

**It never deletes.** The harness has no cleanup command and no delete,
truncate or drop anywhere. Agents never delete on the NAS; the storage owner
runs every NAS delete NAS-local. Each `load` run and each created snapshot
goes into a new directory of its own, and the run prints that directory's path
and size for the storage owner to remove. Database runs only add rows under
their own run ID or sequence; the storage owner removes the qualification
database or its container afterwards. The only files the harness removes are
its own scratch copies in `catchup --local-dir`, which it refuses to place on a
network mount.

**Where it writes.** Only the `myce_qualification` schema, which it creates
with an ownership comment, in a dedicated qualification database inside a
disposable container. It refuses, before writing:

- a database whose name does not start with `qualif`, in any case, such as
  `qualify` from the compose sketch below, so a production connection string
  fails even before the catalog checks;
- a database that holds any `myce_*` relation in any schema and in any case,
  because stage 2 creates its tables unqualified on the `search_path`, and a
  quoted name can be upper case;
- a `myce_qualification` schema it did not create;
- a server with `fsync` or `full_page_writes` off.

Each session pins `search_path` to `pg_catalog, pg_temp`, and every catalog
reference is written `pg_catalog.*`, so a schema placed ahead of the catalog by
a role or database setting cannot shadow `pg_class` or `current_database()`.
Every reconnect repeats all of these checks before it writes. It must also
reach the same server (system identifier) and database as the run's first
session, and it takes the run's advisory lock again, since a session lock ends
with its connection.

**Stopping.** Ctrl-C, SIGTERM or a run's own time limit stops the harness
within 10 seconds, even when a call into a hung SMB mount never returns. After
that grace it exits anyway and lists what it left for NAS-local removal,
including partly written files. The grace lets a writer finish syncing a file
on a healthy share.

**Write shape.** `commit` copies the stage 2 authority SQL from
`internal/myce/shared_queue_postgres.go` (`e5d9287a`, [#2107](https://github.com/1kuna/mycelium/pull/2107)).
Each append runs `BEGIN`, `SET LOCAL synchronous_commit = on`, a `FOR UPDATE`
turn read, a database clock read, `INSERT`, `UPDATE head` and `COMMIT`. That is
seven round trips. A second connection renews the turn every second against the
same row lock. Appends arrive as a Poisson process at **11.6/s**, the stage 0
rate. Payloads use stage 2's versioned-rows JSON with random bytes, which is the
worst case for compression. Sizes are 70% 3.9 kB, 25% 13.8 kB and 5% 96 kB. At
11.6/s this gives 127 kB/s, matching the 125.6 kB/s record payload measured in
[#2093](https://github.com/1kuna/mycelium/pull/2093). The report also gives
PostgreSQL's own WAL rate. For comparison, SQLite's WAL was about 300 kB/s after #2093.

## Container

| Item | Setting | Reason |
| --- | --- | --- |
| Image | `postgres:17.12-trixie@sha256:87c8b42f423b45d5247e6aa8fdb2e7762fec164c68a50fe3eb07e7bfcaf4dbcf` | Same major version as the stage 2 proof (17.10). 17.12 (2026-11-12) is the current minor release and fixes several index corruption bugs. The digest is the multi-architecture index, read from Docker Hub on 2026-11-25; confirm it when pulling. Never use `latest` or `17`, and never use an automatic image updater. |
| initdb | `POSTGRES_INITDB_ARGS="--data-checksums --locale-provider=builtin --builtin-locale=C.UTF-8 -E UTF8"` | Checksums detect silent page damage. The built-in collation doesn't depend on the image's C library. A later image therefore can't silently reorder the text index on `operation`. |
| Restart | `restart: always` | Under test. Docker restarts it after a crash and whenever the daemon starts, including at boot. A container stopped by hand comes back at the next daemon start. `unless-stopped` keeps a deliberate maintenance stop across reboots; the storage owner chooses. [Docker restart policies](https://docs.docker.com/engine/containers/start-containers-automatically/) |
| Stop grace | `stop_grace_period: 120s` | Docker sends SIGKILL after 10 s by default. The image's stop signal is a fast shutdown, but its shutdown checkpoint can take longer on a busy disk. Losing that race is safe, because recovery replays the WAL, but recovery is slower. [docker stop](https://docs.docker.com/reference/cli/docker/container/stop/) |
| Memory | `mem_limit: 1g`, `shm_size: 256m`, `shared_buffers=128MB` | The authority is a turn row and an append-only log. The working set is small, and the NAS keeps its RAM for file caching. The limit leaves room for about 20 backends. |
| CPU | `cpus: 1.0` | 11.6 small commits/s is disk-bound, not CPU-bound. The limit keeps a runaway query from slowing SMB. |
| Connections | `max_connections=50` | A handful per fleet machine, plus the harness |
| WAL | `wal_level=replica`, `max_wal_size=1GB`, `checkpoint_timeout=5min`; `fsync`, `full_page_writes` and `synchronous_commit` stay on | Defaults that permit `pg_basebackup`. The harness refuses `fsync` or `full_page_writes` off. |
| Network | Publish on the NAS's LAN address only. Use `scram-sha-256` in `pg_hba.conf` for the fleet subnet and firewall the port to that subnet. | The database is a separate TCP connection from SMB, which is the point of item 4 below. |
| Logs | `json-file`, `max-size: 10m`, `max-file: 3` | Bounds log growth on the NAS |
| Health | `pg_isready` every 10 s | Lets the NAS UI show readiness |

The qualification role can be the container's `POSTGRES_USER` superuser. With
`POSTGRES_USER: qualify` the image's default database is also `qualify`, which
meets the harness's database-name rule. The
cluster is disposable, and that role can read `pg_control_system()`, whose
system identifier proves that the same data directory came back. Take its
password from the storage owner's secret store into `POSTGRES_PASSWORD_FILE`. It
must never appear in a compose file in a repository. Production roles are a
stage 3 decision.

Compose sketch, with placeholders the storage owner fills in privately:

```yaml
services:
  myce-qual:
    image: postgres:17.12-trixie@sha256:87c8b42f423b45d5247e6aa8fdb2e7762fec164c68a50fe3eb07e7bfcaf4dbcf
    restart: always
    stop_grace_period: 120s
    mem_limit: 1g
    cpus: 1.0
    shm_size: 256m
    ports: ["<nas-lan-address>:<port>:5432"]
    environment:
      POSTGRES_USER: qualify
      POSTGRES_PASSWORD_FILE: /run/secrets/qualify
      POSTGRES_INITDB_ARGS: --data-checksums --locale-provider=builtin --builtin-locale=C.UTF-8 -E UTF8
    command: ["postgres", "-c", "shared_buffers=128MB", "-c", "max_connections=50",
              "-c", "max_wal_size=1GB", "-c", "checkpoint_timeout=5min"]
    volumes:
      - <dedicated-folder-on-chosen-volume>:/var/lib/postgresql/data
      - <secret-file>:/run/secrets/qualify:ro
    healthcheck: {test: ["CMD", "pg_isready", "-U", "qualify"], interval: 10s}
    logging: {driver: json-file, options: {max-size: 10m, max-file: "3"}}
```

**Admin access.** Using Docker is equivalent to root, and the fleet account has
none. The storage owner, or the maintainer through the NAS admin UI or an admin
session, creates, restarts and removes containers. Harness runs need only the
database port. They need no NAS login.

## Data path options

| Option | Placement | For | Against |
| --- | --- | --- | --- |
| 1 | Dedicated folder on the **busy data volume** | Same disks as job files. One volume to watch. | It runs 50–95% busy under bulk work and once hung under load. Commit tails must survive that. |
| 2 | Dedicated folder on the **quiet backup volume** | Mostly idle. Bulk job I/O doesn't compete. | Scheduled backup jobs may burst. A database there must have its own backups on the other volume. |
| 3 | Docker's own data root, as a named volume | Simplest to create | It lives on whichever volume the Docker app uses, so it is really 1 or 2. It is harder to find, back up and survive an app reinstall. Not recommended. |

The same rules apply to every option:

- Don't export the folder over SMB or NFS.
- Exclude it from indexing, antivirus and thumbnailing.
- Don't copy it with file-level snapshots or replication while it is live; use
  `pg_basebackup`.
- Make it owned by the image's `postgres` user (uid 999).
- If the volume's filesystem is copy-on-write, disable copy-on-write on the empty
  folder before the first start.
- Use a bind mount to a known folder, so the data survives a reinstall of the
  Docker app and the container can be recreated from the compose file and digest.

Run one cluster per folder. Measure only one cluster at a time.

## Bulk I/O load

The busy-volume condition needs realistic bulk traffic. Use either source.

- **Passive (preferred when available).** Run `commit` during a real bulk window
  that the storage owner identifies. Record the volume's busy percentage from the
  NAS monitor next to the report. This adds no load.
- **Synthetic, only with the storage owner's consent for that window.** Run
  `queue-qualify load` from one or two fleet machines into a dedicated folder on
  the target volume through SMB. Each run creates one new directory,
  `myce-qual-load-<time>-<random ID>`, with an exclusive mkdir, writes a
  `MYCE-QUALIFY-RUN` marker naming the run, and then writes random 512 MiB
  files inside it, fsyncing each and reading each back once. It writes nowhere
  else and deletes nothing, so the bytes on disk equal the bytes written. At the
  end it prints the directory's path and total bytes. The storage owner removes
  the directory NAS-local. It stops at the first of these limits:

| Bound | Default | Hard limit |
| --- | --- | --- |
| Bytes written, which is also what stays on disk | 16 GiB | exact: the last file is cut to fit, so a 1-byte cap writes 1 byte |
| Duration | 10 min | 1 h |
| Writers | 2 | 8 |
| Free-space floor | the larger of 100 GiB and 10% of the volume | refuses to start unless free space covers the floor plus twice the cap |
| Rate | none | `--rate-mib` caps it |

The free-space accounting counts every byte the run has written or reserved
against the floor, whether or not the share's free-space report shows it yet,
and never gives a reservation back. Each file needs `free >= floor + everything
reserved + this file`, read before the reservation is taken. A watcher reads
free space every 250 ms and stops the run once `free < floor + everything
reserved`, which only other work can cause. Neither holds a lock across a
filesystem call. Counting the run's own bytes twice once the share reports
them is why the start needs twice the cap.

That leaves one assumed lag: other clients' writes that the share has not yet
reported. This plan assumes it is at most a few seconds of other work, about
5 GiB at 1 GiB/s. The default floor of 100 GiB stays far above that; don't
lower it below 20 GiB.

Ramp in steps: `--rate-mib 50`, then `100`, then uncapped. Run `commit` during
each step. The storage owner watches the volume monitor and stops the run with
Ctrl-C if any of these happens:

- the volume sits at 100% busy for over a minute;
- the NAS UI or SMB stops responding;
- production work reports I/O errors.

Ctrl-C stops a run within 10 seconds and leaves its files, including any
partly written file, listed for the storage owner. A run killed outright
(`kill -9`) leaves its directory too; the marker names it. Running the Linux binary in a temporary container on the NAS loads
the disk without the network limit. That load can exceed what SMB clients
produce, so use it only if the storage owner wants a harsher test, starting at
`--rate-mib 50`.

## Steps

Run these in order. **SO** is the storage owner and **O** is the orchestrator.
Each step's report goes to `.reports/` on the measuring side.

| # | Who | Step | Pass |
| --- | --- | --- | --- |
| 0 | SO | Approve this plan and the time windows. Create the qualification secret. Privately record the NAS OS and Docker app versions. | |
| 0 | O | Build the binaries. Pick measuring machines on the wired LAN: one Linux and, for the mount test, one Mac. Put the connection string in a 600 file. | |
| 1 | SO | Start container **A** with its data on the busy volume. Run `docker exec <c> pg_test_fsync -s 5 -f /var/lib/postgresql/data/pg_test_fsync.tmp` and keep the output. | Container healthy |
| 2 | O | `info --volume busy` | Required settings pass |
| 3 | O | `commit --condition idle --volume busy` (10 min) | Commit checks |
| 4 | O, SO | `commit --condition bulk-same-volume --volume busy` during passive bulk work, or each synthetic `load` step | Commit checks |
| 5 | SO | Start container **B** with its data on the quiet volume, using the same image and settings on another port. Run `pg_test_fsync` again. | Container healthy |
| 6 | O | `info`, then `commit --condition idle --volume quiet` | Commit checks |
| 7 | O, SO | `commit --condition bulk-other-volume --volume quiet` while bulk load runs on the **busy** volume. This is the production pairing. If backups run on a schedule, also run once during a backup. | Commit checks |
| 8 | O, SO | `summarize` the commit reports and choose a volume (rule below). The SO stops the other container. | |
| 9 | O, SO | On the chosen volume: the SO records the container ID and its data mount source (`docker inspect --format '{{.Id}} {{range .Mounts}}{{.Source}}{{end}}'`). Then `durability-write --duration 15m`, `docker restart` mid-run, and `durability-verify`. The SO records the same two values again. | Durability checks, and the same container ID and mount source before and after. Verify fails unless at least one acknowledgement came before the restart it observes. |
| 10 | O, SO | Repeat with `docker kill`, which is a crash. | Durability checks |
| 11 | O, SO | Repeat across a NAS reboot. Use `--max-outage 30m` and `--duration 45m`. | Durability checks. The container returns with no admin action. |
| 12 | O, SO | At the next NAS OS update, run `durability-write --duration 2h --max-outage 60m` across it, then verify. Compare `docker inspect --format '{{.Image}}'` before and after. | Durability checks. Same image digest. No admin action except the update itself. |
| 13 | O, machine admin | **Mount isolation.** Drain a fleet machine, then run `commit --watch <dir in its SMB mount> --duration 10m`. Mid-run, that machine's admin drops TCP 445 to the NAS for at least 60 s, then lifts it (`pfctl` on macOS, `nft` or `iptables` on Linux). Do this once on the Mac and once on Linux. | Mount check |
| 14 | O | From machine X: `catchup --create-in <share dir> --create-bytes 8157110272 --create-only`, which prints the snapshot's path in its new run directory. From machine Y and the Mac: `catchup --snapshot <that path> --changes 6960`. | Replay check |
| 15 | O, SO | `summarize` all reports. Decide. The SO removes the printed run directories and the qualification containers NAS-local. | |

A drop rule makes SMB hang the way the observed stalls did. A reject rule would
make SMB fail fast instead. The harness probes the mount from the same process
that commits, as the queue process will. That tests the stronger claim: a thread
stuck in the kernel on SMB does not stall the process's database writes. If the
storage owner prefers not to block traffic, run a long `commit --watch` soak at
`--rate 1` instead and wait for a natural stall. That is slower and may never
exercise the check, which the report then marks `not-exercised`.

`pg_test_fsync` checks plausibility. If one disk reports more flushes per second
than its media can do, the write cache is volatile. Then only power-loss
protection, such as a UPS with a clean shutdown or a protected controller cache,
keeps acknowledged commits through a power cut. No restart test can show this.
The storage owner states what protection exists. Do not cut power to the NAS for
this qualification.

**Step 12 timing.** If no OS update arrives within the qualification window,
step 11 stands in. Step 12 stays open on #1176 as a stage 3 prerequisite, with
the revisit trigger "next NAS OS update".

### Choosing the volume

1. A volume qualifies only if both its idle and bulk commit runs pass every
   check, with its bulk run on the volume itself for option 1, or on the busy
   volume for option 2.
2. If both qualify, choose the one with the lower bulk-run p99 and maximum.
   Break ties toward the quiet volume, which keeps queue and job I/O on separate
   disks.
3. If neither qualifies, stop. The NAS cannot hold the authority under current
   bulk load. Report the numbers. The options for the orchestrator and maintainer
   are a dedicated SSD or volume for the database, bounding bulk I/O, a longer
   lease (see below), or design option D. Don't loosen a threshold to pass.

## Thresholds

The defaults judge against stage 2's timing: a **6 s lease**, a **1 s renewal**,
a **2 s per-call timeout** (lease/3), and the measured **11.6 commits/s**.
`commit --lease/--interval/--rate` scales every limit. If stage 3 picks a longer
lease from these measurements, as the design allows, the report states which
lease it judged against.

| Check | Limit at defaults | Failure it prevents | Basis |
| --- | --- | --- | --- |
| Append and renewal max | ≤ 1 s (call timeout / 2) | A commit outliving the per-call timeout. The append becomes uncertain and closes the writer, or the renewal fails and the coordinator retires. Either way, new starts pause. | Stage 2 retires on any failed renewal. The half leaves margin for network and scheduling outside the commit. One breach fails the volume, since the busy volume has already hung once. |
| Append response and renewal p99 | ≤ 250 ms (call timeout / 8) | A heavy tail that predicts rarer stalls past the timeout, outside the measured window | 6,960 appends per 10-minute run put the p99 on about 70 samples |
| Mean append service time | ≤ 43 ms (1 / (2 × 11.6/s)) | The single ordered writer saturating. Appends are serial, so a writer more than 50% busy builds a growing backlog during a 2× burst of check-ins. | Utilization at the measured rate |
| Failed appends or renewals | 0 | Each failure closes the writer turn or retires the coordinator | Stage 2 behavior |
| Commits while the mount is unresponsive | At least one window of 10 s or more. Every such window has appends, 0 errors and every operation within the max limit. | An SMB stall blocking queue authority, which is why the design uses a database connection | 10 s spans 10 renewals and about 116 appends |
| Every acknowledged write survives | 0 missing, 0 corrupt, no row from a transaction that failed before COMMIT | Losing an acknowledged assignment, start or result: duplicate execution or a lost result | The design's core promise |
| Acknowledged before a server restart | At least 1 acknowledgement written under an earlier server start time than verify's | A pass from a server that never went down, or went down before the first acknowledgement | Each ledger line records the `pg_postmaster_start_time()` it was acknowledged under |
| Same data directory | System identifier readable and unchanged; the writer stops rather than write to a different one | A container recreated on an empty directory, which starts a new, empty authority without notice | `pg_control_system()`; connect as the database owner |
| Not an older copy | WAL at verify at or past every acknowledged position and the writer's end marker; timeline unchanged | Verifying against a restored backup or an older copy of the data directory | Each acknowledgement records its WAL insert position; the end marker adds 1 MiB of WAL after the last one |
| Writer outlived every outage | Stopped by duration, not `--max-outage` | A server that never came back on its own | Restart policy under test |
| Required settings | `fsync`, `full_page_writes` on; a primary, not a standby | Acknowledged commits held only in memory, and torn pages | PostgreSQL WAL rules |
| Replay outruns live appends | ≥ 10× live in changes/s and bytes/s | A returning member that never converges, or a sole survivor waiting hours to coordinate | At 10×, an hour asleep costs six minutes |
| Advisory (warn) | `synchronous_commit` default on, checksums on, `commit_delay` 0, major version 17 | Configuration drift from what stage 2 proved | |
| Recorded, no pass/fail | WAL kB/s, outage seconds, snapshot transfer projected to 7.60 GiB, `pg_test_fsync` | Inputs for snapshot cadence, restart planning and disk sizing | |

Change-log replay applies each change in its own SQLite FULL transaction with
the cursor, as stage 2 does. It decodes the same JSON shape and upserts keyed
rows. It approximates the real table set, so the stage 2 laptop numbers remain
the reference for apply cost. The LAN measurement is what this step adds.

## Laptop sample

This run, from 2026-11-25, shows the shape of the reports. It says nothing
about the NAS. It used a disposable PostgreSQL 17.10 cluster on an Apple silicon
laptop's internal SSD, over loopback, with its own data directory and port.
Commit runs measured for 2 minutes after a 15 s warm-up, with the default
profile at 11.6/s.

| Run | Result |
| --- | --- |
| `commit`, idle | Pass. Append service p50/p95/p99/max 0.81 / 4.97 / 23.86 / 89.48 ms; renewal p99 23.57 ms; 11.49 appends/s; WAL 156 kB/s |
| `commit` during `load` on the same disk | Pass. Append service 1.03 / 8.06 / 27.20 / 49.11 ms; renewal p99 3.57 ms. Load wrote and read about 24 GiB each at about 1.2 GiB/s and stopped at its byte cap after 20 s. That earlier harness version, with a 4 GiB working set, then removed its directory; the harness no longer deletes. |
| `durability-write` across `pg_ctl restart -m immediate` | Pass. 1,076 acknowledged, 1 failed before commit, 0.5 s outage, 1 restart seen, same identifier |
| `durability-verify` | Pass. 0 of 1,076 missing, 0 corrupt |
| `catchup` | Pass. 6,960 changes (78 MB) replayed in 5.1 s, 117× the live rate. The 1 GiB snapshot copy ran from a warm local cache, so it is no network measure. |
| `commit --watch` with no fault | Incomplete, as designed: no unresponsive window, so the isolation check is `not-exercised` |

After the harness became write-only, a shorter rerun on the same laptop with a
fresh cluster gave these results:

- `commit` idle, 30 s, passed: append p50/p99/max 0.70 / 17.36 / 50.49 ms.
- `commit` during a four-writer `load` capped at 150 MiB/s, 25 s, passed:
  0.67 / 3.96 / 6.58 ms. The load wrote exactly its 4 GiB cap in eight
  files, deleted nothing, and printed its run directory and size.
- `durability-write` across `pg_ctl restart -m immediate`, then verify,
  passed: 200 acknowledged, 84 of them before the restart, none missing, WAL
  at verify past the end marker.
- `catchup --create-in` wrote a 1 GiB snapshot into its own run directory.
  Replay of 2,000 changes passed at 512× live.
- Ctrl-C on a running `load` exited in 0.02 s and listed its run directory
  with two partial files.
- `commit` against the cluster's `postgres` database was refused.

**Known limit: a byte-for-byte copy.** A copy of the data directory taken after
the writer's end marker holds the same rows, identifier, timeline and WAL as
the original, so nothing visible over SQL tells them apart. Verify detects a
copy or backup from any earlier point, through the WAL check, but not that one.
The procedural check in step 9 covers it: the same container ID and data mount
source before and after the restart. Steps 10 to 12 repeat it.

## Rollback and cleanup

Any step can stop at once. Ctrl-C stops the harness within 10 seconds, even
with the share hung, and lists what it left behind. `docker stop` stops a
container. No fleet component uses these databases, so stopping one has no
fleet effect.

The harness deletes nothing on the NAS. Every NAS delete is the storage owner's,
run NAS-local. Afterwards:

- **O:** Collect the run directories that `load` and `catchup --create-in`
  printed, with their sizes, and send the list to the SO. Delete the binaries,
  connection files and ledgers from the measuring machines.
- **SO:** Remove those run directories NAS-local, using the printed list and
  each directory's `MYCE-QUALIFY-RUN` marker. Remove both containers and their
  data folders on both volumes, which removes the qualification database. The
  harness never drops its tables, and each 10-minute `commit` run adds about
  80 MB of rows. Remove the qualification secret, any firewall rule, and
  optionally the image. Confirm the volumes' free space is back to its
  pre-test level.
- **Machine admin (step 13):** Confirm the drop rule is gone and the SMB mount
  is healthy before undraining.
- Keep the raw reports privately in `.reports/`. Post only sanitized numbers to
  #1176, using generic labels such as "busy" and "quiet".

Don't promote the qualification cluster. Stage 3 creates a fresh cluster on the
chosen volume with production roles, backups on the other volume, and the image
pin this plan qualified.

## What the storage owner decides

1. Whether and when the qualification may run on the NAS. This includes the
   container, the database port and firewall rule, and any synthetic bulk load
   and its ramp.
2. The dedicated folders on each volume and their filesystem settings, such as
   copy-on-write. Also whether the quiet volume's backup schedule needs its own run.
3. `restart: always` or `unless-stopped`, and who holds the admin access to
   recreate the container if an OS update needs it.
4. What power-loss protection the NAS has, read against `pg_test_fsync`.
5. The volume, using the rule above, together with the orchestrator.
6. Whether step 12 waits for the next scheduled OS update or one is scheduled
   for this purpose.
