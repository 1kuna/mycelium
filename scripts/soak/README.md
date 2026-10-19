# CPU-only Linux soak

`soak.py` uses Python 3 stdlib, the real Linux arm64 `myce`, SQLite, four
independent fake workers, and a shared **local fake NAS**. It never probes GPUs,
loads models, invokes Docker, installs packages, or accesses the real NAS.
Each worker reports 8 fake GiB; every command needs all 8, so each worker runs
one job. Commands only wait and atomically save small checkpoints every two
seconds. Three projects compete for experiment time alongside deadline and
production jobs. At most 16 unfinished jobs are admitted.

Build on the Mac (do not run the soak here):

```sh
GOOS=linux GOARCH=arm64 go build -o /tmp/myce-linux-arm64 ./cmd/myce
ssh TEST_HOST 'mkdir -p ~/myce-soak'
scp /tmp/myce-linux-arm64 TEST_HOST:myce-soak/myce
scp scripts/soak/soak.py TEST_HOST:myce-soak/soak.py
```

Before launch, copy the script into the new run folder and launch that immutable
copy (`/srv/example/myce-soak/run-RUN_ID/soak.py`). This prevents a later harness
edit from changing job commands in an already-running test.

Before each launch, check `uptime`, `free -g`, and `df -h ~`. Do not launch
when load is high or available memory is tight. The launch report records the
actual admission readings and the run/unit names. Run with nice 19 and idle I/O:

```sh
systemd-run --user --unit=myce-soak-RUN_ID \
  --property=Nice=19 --property=IOSchedulingClass=idle \
  --property=CPUWeight=1 --property=CPUQuota=25% \
  --property=MemoryMax=768M --property=TasksMax=128 \
  --property=TimeoutStopSec=45 --property=KillMode=mixed \
  /usr/bin/python3 /srv/example/myce-soak/run-RUN_ID/soak.py run \
  --run /srv/example/myce-soak/run-RUN_ID --duration 14400
```

For validation use a **fresh** folder and `--duration 300 --seed 20261010`.
Four hours means four hours of submissions/fault injection, followed by up to
15 minutes of draining. Default production/deadline/experiment loss timeouts
(5/20/60 seconds), production stop grace (10 seconds), heartbeat (1 second),
and queue restart grace (3 minutes) are preserved. Faults occur every 60–300
seconds: worker SIGKILL/restart, worker SIGSTOP/SIGCONT (10–90 seconds), queue
SIGKILL/restart, CLI leave/rejoin, or all workers paused (30–120 seconds).
The short validation cycles all five fault types at shorter intervals and uses
the minimum allowed pauses to fit five minutes. SIGSTOP pauses worker control,
while its command can keep running; this models a partition, not physical
network packet loss. Queue SIGKILL tests process crash/WAL recovery rather than
an actual host power cut.

The queue and audit relay bind only loopback on OS-selected high ports.
`state.json` stores those ports, the seed, pending submission intent, completed
phases, job IDs, terminal states, accepted success signatures, and violations.
A file lock prevents duplicate harnesses on one folder. Rerun the same command
and folder to resume interrupted work; completed runs are skipped. Unique job
names reconcile an uncertain submission before retrying it. Existing evidence
is retained. Never copy or print `.token`; it is private, mode 0600.

`ledger.jsonl` records progress, injected faults, restorations, accepted success
observations, process cleanup, and invariant checks every minute and during
drain. A local HTTP relay audits acknowledged worker completions against queue
state. A repeat acknowledgement of the **same** terminal execution is harmless;
a different successful attempt or terminal timestamp is a duplicate success.
Checks also detect lost jobs, terminal regressions, incomplete successful work,
saved progress ignored by a resumed successful attempt, and unexpected process
leftovers after restart. Attempt-specific JSON outputs record job, attempt,
start/end, resumed step, and PID; environment values are hashed, never printed.
Job environments and queue/worker logs are checked for the private token.
SQLite `PRAGMA integrity_check` runs after each queue crash and at completion.

Healthy-capacity production starts have a 30-second bound: 10-second stop
grace plus one-second check-in and queue ticks, with margin for low-priority CPU
scheduling and HTTP work. Production submissions during a fault or its recovery
are excluded, as are faults within 30 seconds of submission. This tests available
capacity, not availability during a partition. No real model load is included.
The test exercises sharing but does not claim a quantitative fairness proof.

All child processes use nice 19 and idle I/O. Systemd bounds aggregate CPU and
memory. The harness checks owned job PIDs every second and aborts above eight;
it checks the entire `~/myce-soak` tree every minute and aborts at 1.8 GiB,
leaving headroom below the 2 GB ceiling. There is no unbounded command output.
The concurrency count covers Python job commands; Mycelium's idle supervisor
shells are separately covered by the unit's task limit and cleanup.
On completion or SIGTERM it cleans only its owned processes. Stop early with:

```sh
ssh TEST_HOST 'systemctl --user stop myce-soak-RUN_ID'
```

`DONE` is JSON with counts and a clearly listed `violations` array. It means
the harness completed; it is a passing soak **only if that array is empty**.
`FAILED` means the harness broke; retained ledger/logs explain where. Read only
these evidence files, never the token. No Go changes are made by this harness.
An intentional early stop also writes `FAILED` with `interrupted: true` and can
be resumed on the same folder. A previous FAILED marker is renamed and retained.
