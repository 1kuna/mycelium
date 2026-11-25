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
| `load` | Bounded bulk file I/O in one marked directory |
| `catchup` | Snapshot copy from the share with hashing, and change-log replay into a local SQLite copy |
| `summarize` | One comparison of saved reports |
| `cleanup` | Drops the qualification schema and removes marked leftovers |

Each run writes a JSON and a Markdown report to `--out` and exits nonzero when a
check fails. `--condition` and `--volume` take generic labels such as `idle`,
`bulk-same-volume`, `busy` or `quiet`.

**What it writes.** Only the `myce_qualification` schema. It refuses a database
that already has a shared queue authority (`public.myce_coordinator_v1`) and a
server with `fsync` or `full_page_writes` off.

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

The qualification role can be the container's `POSTGRES_USER` superuser. The
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
  the target volume through SMB. It creates `myce-qual-load-*` with a marker file
  and writes random 512 MiB files, fsyncing each. It reads each file back once
  and deletes the oldest files as it goes. It removes everything on exit,
  including after Ctrl-C or SIGTERM. It stops at the first of these limits:

| Bound | Default | Hard limit |
| --- | --- | --- |
| Total written | 32 GiB | any value you pass |
| On disk at once | 4 GiB | |
| Duration | 10 min | 1 h |
| Writers | 2 | 8 |
| Free-space floor | the larger of 100 GiB and 10% of the volume, checked before every file | refuses to start below floor + working set |
| Rate | none | `--rate-mib` caps it |

Ramp in steps: `--rate-mib 50`, then `100`, then uncapped. Run `commit` during
each step. The storage owner watches the volume monitor and stops the run with
Ctrl-C if any of these happens:

- the volume sits at 100% busy for over a minute;
- the NAS UI or SMB stops responding;
- production work reports I/O errors.

`queue-qualify cleanup --skip-db --load-dir DIR` removes marked leftovers after
a hard kill. Running the Linux binary in a temporary container on the NAS loads
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
| 9 | O, SO | On the chosen volume: `durability-write --duration 15m`, then `docker restart` mid-run, then `durability-verify` | Durability checks |
| 10 | O, SO | Repeat with `docker kill`, which is a crash. | Durability checks |
| 11 | O, SO | Repeat across a NAS reboot. Use `--max-outage 30m` and `--duration 45m`. | Durability checks. The container returns with no admin action. |
| 12 | O, SO | At the next NAS OS update, run `durability-write --duration 2h --max-outage 60m` across it, then verify. Compare `docker inspect --format '{{.Image}}'` before and after. | Durability checks. Same image digest. No admin action except the update itself. |
| 13 | O, machine admin | **Mount isolation.** Drain a fleet machine, then run `commit --watch <dir in its SMB mount> --duration 10m`. Mid-run, that machine's admin drops TCP 445 to the NAS for at least 60 s, then lifts it (`pfctl` on macOS, `nft` or `iptables` on Linux). Do this once on the Mac and once on Linux. | Mount check |
| 14 | O | From machine X: `catchup --snapshot <share>/myce-qual-snapshot.bin --create-bytes 8157110272 --create-only`. From machine Y and the Mac: `catchup --snapshot ... --changes 6960`. | Replay check |
| 15 | O, SO | `summarize` all reports. Decide, then clean up. | |

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
| Same data directory | System identifier unchanged | A container recreated on an empty directory, which starts a new, empty authority without notice | `pg_control_system()` |
| Writer outlived every outage | Stopped by duration, not `--max-outage` | A server that never came back on its own | Restart policy under test |
| Required settings | `fsync`, `full_page_writes` on; a primary, not a standby | Acknowledged commits held only in memory, and torn pages | PostgreSQL WAL rules |
| Replay outruns live appends | ≥ 10× live in changes/s and bytes/s | A returning member that never converges, or a sole survivor waiting hours to coordinate | At 10×, an hour asleep costs six minutes |
| Advisory (warn) | `synchronous_commit` default on, checksums on, `commit_delay` 0, major version 17 | Configuration drift from what stage 2 proved | |
| Recorded, no pass/fail | WAL kB/s, outage seconds, snapshot transfer projected to 7.60 GiB, `pg_test_fsync` | Inputs for snapshot cadence, restart planning and disk sizing | |

Change-log replay applies each change in its own SQLite FULL transaction with
the cursor, as stage 2 does. It decodes the same JSON shape and upserts keyed
rows. It approximates the real table set, so the stage 2 laptop numbers remain
the reference for apply cost. The LAN measurement is what this step adds.

## Rollback and cleanup

Any step can stop at once. Ctrl-C stops the harness, and the load generator
removes its files on the way out. `docker stop` stops a container. No fleet
component uses these databases, so stopping one has no fleet effect.

Afterwards:

- **O:** `queue-qualify cleanup --dsn-file F --load-dir <qualification folder>`
  on each cluster that still runs. This drops `myce_qualification` and removes
  marked load folders and snapshot test files. Delete the binaries, connection
  files and ledgers from the measuring machines.
- **SO:** Remove both containers and their data folders on both volumes. Remove
  the qualification secret, any firewall rule, and optionally the image. Confirm
  the volumes' free space is back to its pre-test level.
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
