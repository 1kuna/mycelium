# Queue relocation and single-host resilience (#316)

Planning only, sampled 2026-10-29 at 12:28–12:31 EDT. No service, policy,
network, enrollment or fleet files were changed. The cutover belongs to the
maintainer's separately scheduled window. [#316](https://github.com/1kuna/mycelium/issues/316)
remains the execution ticket.

## Recommendation and limits

Recommend **m4-24**, with **5–10 minutes of queue/API unavailability** budgeted
for an idle, rehearsed cutover after an online file pre-copy. This is a planning
estimate, not a measured recovery promise. Allow another 10–20 minutes for
all-host checks before reopening submissions. Do not include bulk transfer in
that outage estimate: the present file store alone is 90.39 GiB.

The entire cluster cannot remain literally unaffected by every single machine
failure under the approved architecture. There is one queue and one SQLite
writer. Queue loss stops submissions, new starts, cancellation acknowledgements,
scheduling and queue-mediated model routing. Running commands on surviving
workers can continue; their inputs, checkpoint mode and external dependencies
still matter. Moving the queue removes GPU-host reboots from the control-plane
failure domain; it does not supply queue HA. No replication, peer registry,
automatic promotion or second live queue is proposed.

Binding rulings: [MY-18](DECISIONS.md) places ordinary control work off scarce
accelerators; MY-03 keeps one v3 runtime and fixes forward; MY-04 requires
proportionate qualification. The [approved design](design-proposal.md) remains
the sole product spec. This document records one deployment plan, not a generic
hardware requirement.

## Candidate evidence

Read-only evidence: SSH `uptime`, `sysctl`, `df`, `memory_pressure`, `mount`,
route/interface inventory and three-packet pings; authenticated `myce status
--json` for fresh worker facts, storage and queue release. Free disk is APFS
container availability, not the read-only system-volume usage percentage.
Memory percentages below are the OS pressure tool's free percentage, not an RSS
budget. CPU load averages are 1/5/15 minutes, not utilization percentages.

| Candidate | Current uptime | CPUs / RAM | Load averages | Local free disk | Memory free percentage | NAS / network | Assessment |
| --- | --- | --- | --- | --- | --- | --- | --- |
| mini (T3 host) | 12d 8h | 18 / 64 GiB | 7.94 / 8.74 / 8.26 | 206 GiB | 57% | Mounted SMB; default route en1 | Capacity fits, but T3 serves every agent thread; queue work, backup I/O and recovery would compete with orchestration. |
| m4-16 | 12d 13h | 10 / 16 GiB | 1.87 / 2.27 / 2.43 | 54 GiB | 89% | Mounted SMB; Wi-Fi en1 | Cannot fit the existing 90.39 GiB file store, even before DB, backups and growth. |
| **m4-24** | **7d 8h** | **10 / 24 GiB** | **1.30 / 1.41 / 1.44** | **303 GiB** | **67%** | Mounted SMB; Wi-Fi en1 | Best observed load and viable disk margin without sharing T3's failure domain. |
| b70 (current) | 21m after reboot | 16 / 31.2 GiB | 2.60 / 2.52 / 1.91 | 220 GiB | 25.8 GiB available | Wired default route; fresh shared-checkpoint report | XPU host; historical load about 18 and queue demand about 1.6 cores, per #316. Relocate. |
| spark | 18d 3h | 20 / 121.1 GiB | 2.65 / 3.36 / 4.46 | 1.7 TiB | 116.6 GiB available | Wired default, Wi-Fi fallback; fresh shared-checkpoint report | Sole CUDA host; moving control here recreates scarce-host coupling. |

The queue store reports 97,051,141,533 payload bytes, a 100 GiB configured
allowance and only 9.61 GiB allowance headroom. Its present disk has more space;
allowance exhaustion and relocation are separate concerns. One current store
copy leaves m4-24 about 213 GiB before DB/logs/other state; two such copies leave
about 122 GiB. Keep the durable backup on approved private storage rather than
turning that margin into accumulating local snapshots. A queue DB found on b70
is about 859 MiB; the cutover must identify the active config's exact DB and
measure every root, rather than assume the largest filename is authoritative.

m4-24 was not idle: one existing Python payload used about one core. A 1.6-core
queue would consume roughly 16% of ten logical CPUs if the old demand carried
over; this is a sizing approximation across different CPUs, not a benchmark.
Leave room for OS and queue CPU/RAM by reducing competing worker admission
through the supported host policy/config during the scheduled window. Measure
queue peak RSS under comparable fan-out before choosing a memory reserve.

Both M4s use Wi-Fi today, despite having Ethernet ports. From m4-24, average
RTT was 7.40 ms to the NAS and 14.74 ms to b70; m4-16 measured 10.32 and
13.68 ms respectively, with no loss in three packets. These tiny samples prove
reachability, not reliability or transfer throughput. The NAS directory was
readable on m4-24 and all five hosts reported fresh checkpoint-volume readings
(about 3.5 TiB free). Link speed, switch/AP/power independence, UPS coverage and
long-term outage history were not verified. Current uptime does not establish
m4-24 is historically more reliable than mini. Prefer a verified wired route for
m4-24 when the maintainer schedules physical/network work; do not silently assume
it exists or make cabling part of this read-only task. Reconfirm Wi-Fi stability
and power/sleep settings before committing the window. No stronger unused
non-GPU desktop was verified; a travel laptop is a worse always-on target.

## Supported tools and transfer contract

Use [operations](operations.md#install-and-upgrade),
[joining](guide-joining-v3.md), [signed releases](guide-onboarding-releases.md),
[history recovery](operations.md#queue-history-retention) and
[fresh-install guidance](guide-fresh-install.md). There is no `myce backup`,
`myce restore` or turnkey cross-OS relocation command in the inspected source.
The supported building blocks are the installer, ordinary service-manager stop,
a verified stopped-state filesystem backup, and SQLite integrity checking.
Do not copy the Linux service/config verbatim into macOS or run SQLite on a NAS.

Preserve this set as one stopped-state generation, privately:

- Active SQLite DB and any WAL/SHM companions; job/key/attempt/custody history,
  learned curves, live policy, host maintenance and release approvals live here.
- Queue file store (`DB.files` by default), implicit `DB.uploads` plus any
  separately configured upload spool, retained logs and every referenced root.
  `internal/myce/queue_roots.go:QueueStateRoots` enumerates startup roots.
- Joining identity (`DB.joining` by default), certificate/private key and
  enrollment authority; preserve the existing fleet credential privately.
- Saved config and referenced catalogs, policy/bootstrap files, publisher trust,
  credentials and service metadata. Resolve every path on macOS; preserve
  settings semantically, including timeouts, retention, limits and secret refs.
- Shared NAS checkpoints/models remain on the same share with their relative
  assignments. Configure the destination's local mount roots; never reinterpret
  NAS-relative assignments as b70 absolute paths. Preserve worker spools in place.

Use rsync pre-copy for large immutable payloads, followed by a stopped final
sync and a SHA-256 manifest comparison. Do not treat the online DB copy as a
backup. Copy the stopped DB and companions together; run `PRAGMA integrity_check`
on a local restored copy before activation. Do not vacuum during relocation.
A manifest must include the non-default roots and modes, not merely DB bytes.
The final sync must reconcile removals in the owned destination mirror so an
online pre-copy cannot resurrect reclaimed uploads. Keep independent verified
backup generations outside that mirror; no destructive sync against backups.

### Endpoint, joining authority and TLS

Inventory every authority type before the window: direct HTTP endpoint plus
private credential file, pinned joining JSON, environment-based endpoint,
explicit client overrides and consumers. Queue listen/advertise origin and TLS
joining origin are distinct: `--queue` is not a TLS listener flag. Preserve the
identity directory at its default beside the restored DB when using the ordinary
installer. Verify the advertised joining origin and actual TLS listener after
activation; do not invent installer flags for the foreground-only
`--discovery-listen` or `--identity-dir` options.

The pin is SHA-256 of certificate DER, not curl's SPKI pin. Preserving the joining
identity keeps trust valid across an address change. Discovery supplies candidate
addresses only; a matching pin and credential still authenticate the queue.
A newly generated identity would invalidate existing pins and require privately
provisioned authority on every worker/client; do not accept a fetched certificate
or disable pin checking to make the move pass.

Direct-endpoint workers can use the existing saved-config overlay:

```sh
myce install worker --config WORKER_CONFIG --queue NEW_QUEUE_ORIGIN --dry-run
myce install worker --config WORKER_CONFIG --queue NEW_QUEUE_ORIGIN
```

Environment-based authority must be inventoried and updated through its owning
configuration path too; stale environment values must not override the new URL.
Pinned workers need particular care: `ServiceConfig.Args` emits `--joining-file`
and omits `--queue`; `Doctor` uses the address inside that file. Changing only
saved `queue` is insufficient. `ResolveJoining` can discover the moved identity
on worker restart, then authenticate it with the existing pin. It does not
retarget the already-running client's URL on each check-in.

**Rehearse both existing pinned-host paths before scheduling:**

- With discovery, restart each settled worker through its existing manager,
  retaining identity, spool and joining file. Startup authenticates the discovered
  origin and writes it back to the joining file (`cmd/myce/main.go`), refreshing
  the address used by doctor. No new enrollment ticket is needed.
- Without discovery, export a new mode-0600 private invitation from the preserved
  queue identity with `myce invite --private-file --identity-dir IDENTITY_DIR
  --queue NEW_TLS_ORIGIN --out NEW_AUTHORITY`. Supply the credential through the
  private mechanism. Transfer the invitation privately to a **persistent** local
  path on each worker. Authenticate before changing installation:

  ```sh
  myce status --joining-file NEW_AUTHORITY --queue NEW_TLS_ORIGIN
  myce install worker --config WORKER_CONFIG --joining-file NEW_AUTHORITY --queue NEW_TLS_ORIGIN --dry-run
  myce install worker --config WORKER_CONFIG --joining-file NEW_AUTHORITY --queue NEW_TLS_ORIGIN
  ```

  The existing installer may select this new persistent authority path; it need
  not overwrite its old invitation. Preserve the original file until recovery is
  verified. This is authority transfer with the same pin/token, not trust rotation
  or worker re-enrollment. Do not manually rewrite JSON or delete worker state.

In both cases also overlay saved `ServiceConfig.Queue` for installed CLI clients
that prefer it, and update explicit consumer endpoints. Rediscovery updates the
joining file, not every stored endpoint. Re-running a spent bootstrap ticket is
not an endpoint migration procedure. The native rehearsal must prove both
paths, especially installer preflight and doctor, before booking the window.

## Scheduled procedure and rehearsal

1. Confirm deployed queue and chosen signed native macOS artifact include s053,
   including [#305 / PR #314](https://github.com/1kuna/mycelium/pull/314).
   Live API at sampling reported s053 commit `2bb3fb3`; this planning branch was
   based on `origin/main` `ef4c03c5` (s052). Use deployed/fixed release bytes,
   never the older planning base, for rehearsal or relocation. Live policy
   revision was 93. Capture fresh policy content and revision at cutover;
   startup YAML now seeds only unconfigured policy. Do not import stale YAML.
2. On a laptop, use an isolated sanitized queue fixture with history, curves,
   policy edited after bootstrap, files and joining identity. Rehearse stopped
   backup/restore through the real installer and native macOS service lifecycle.
   Keep it unreachable from production workers. Compare history/key answers,
   curve fields, policy content/revision, retention, grants and maintenance;
   prove stale result rejection, exact custody recovery and TLS pin rejection.
   Exercise direct and pinned clients, including mDNS unavailable, and record
   each unsupported path as a prerequisite rather than bypass it.
3. In the separately authorized native rehearsal, measure source/destination
   free space, NAS read access, sustained transfer throughput, final delta size,
   SHA verification time, startup and all-host rejoin time. Verify sleep/power,
   listener reachability from every worker and a single queue restart. Use an
   isolated fixture and distinct identity/endpoints; never advertise the cloned
   production identity while the production queue is live. Installer dry-run is
   useful but does not prove NAS write validation or daemon health.
4. Record before-move per-request spans under comparable fan-out as #316 requires.
   Capture route/count/window, p50/p95/p99, failures and host pressure; separate
   long-poll routes from control calls. Current bounded status samples showed
   check-in p95 199 ms, submit p95 532 ms and status p95 920 ms; these are not a
   matched baseline for the historical 1.3–2.4 s incident. Pre-copy payload files
   at measured spare I/O capacity without copying live DB state as authoritative.
5. Pause producers in the scheduled idle window. Drain all hosts through the
   queue and verify exact settlement, including preparations, engines, pending
   releases, buffered completions and uploads. Record which maintenance this
   operation created so existing drains/disables remain intact. Do not force-stop
   customer work to meet the estimate. Stop settled workers through their owning
   managers if endpoint changes require restart; keep spools and identities.
6. Record config/root/credential-reference hashes and API history/curve/policy
   baselines. Stop and disable the old queue's recorded service; verify process
   and listeners are gone. This is the start of API downtime. Take the final
   stopped generation, checksum-verify backup and destination, integrity-check
   the restored local DB, and retain proof the original cannot auto-restart.
7. On a **fresh queue installation root**, restore the complete generation
   before activation. Do not create a saved config by hand: `--config` selects an
   existing installation and macOS upgrade validation requires its installed
   signed binary. Use the approved signed native binary with the fresh path:

   ```sh
   myce install queue --root DEST_ROOT --db DEST_DB --nas DEST_NAS --checkpoints DEST_CHECKPOINTS --queue NEW_QUEUE_ORIGIN --listen NEW_BIND --dry-run
   ```

   Add explicit flags for **every preserved non-default setting** identified in
   the rehearsal (catalogs, policy files, timeouts, retention, upload spool and
   limits); these placeholders show the path-selection core, not a complete
   fleet-specific command. Provision the existing token privately in the
   installer's supported environment. Map restored roots to the fresh plan,
   including `DEST_DB.files` and `DEST_DB.joining`. Check the full dry-run,
   then repeat the exact command without `--dry-run`. This generates native
   macOS config/service files and starts the restored queue. Do not copy Linux
   manager metadata. The rehearsal must confirm all restored custom roots and
   publisher-trust startup settings are expressible by this installer/release;
   stop and report a specific gap if not. No empty DB/identity may be served.
   Never run two copies with the same identity, even briefly.

8. Check exact release, local storage, NAS, TLS fingerprint and authenticated API.
   Compare full live policy and revision, original history/key samples and all
   original curves/learning fields. Allow expected age-based history retention;
   distinguish newly added curves from changed original ones. Verify persisted
   release approval and publisher trust; a new invite must still work. If any
   preservation check fails, keep admission closed and repair forward.
9. Apply rehearsed endpoint/authority updates; start workers sequentially and
   verify all five exact host identities, fresh sessions, devices/protocols,
   runner smoke, stable manager health and empty unresolved custody. Update
   producer/client endpoints and credential references too. Resume only this
   operation's drains, then reopen producers. Record after-move spans under the
   same fan-out/input/release conditions and compare output count and quality.
10. Retain the verified pre-cutover generation privately until restore and
    post-move acceptance pass. Summarize the ledger and remove only verified
    rebuildable staging; keep unique history/checkpoints and backups required by
    the recovery policy. Keep b70's old queue service disabled.

The 5–10 minute estimate assumes the final delta and its verification take
1–3 minutes, restore/install/start 1–2 minutes, and endpoint changes plus first
all-host contact 3–5 minutes (some overlap). At 20–80 MiB/s, a full 90.39 GiB
payload copy alone takes about 19–77 minutes, excluding hashing and NAS effects.
If rehearsal cannot bound the final delta or prove both joining-address paths,
replace the estimate before booking the window. The three-minute restart grace
is not a deadline to squeeze an unsafe migration into: use an idle settled fleet.

## Single-host failures today and after relocation

| Lost host | Today | After moving the queue to m4-24 |
| --- | --- | --- |
| Queue host | b70 loss also removes both XPU devices and fleet control. Surviving Started commands continue locally; new control work waits. | m4-24 loss removes control and its local CPU/Metal capacity. b70/Spark commands already running continue, but no new launches or acknowledged cancellation until restore. |
| spark | Only CUDA host: CUDA-only jobs/engines have no replacement; surviving queue/XPU/CPU work can continue. | Same CUDA capability gap; queue itself stays available. |
| b70 | Both XPU devices and the queue disappear together. | Both XPU devices disappear; queue and other hosts remain usable. XPU-only work still waits. Two cards in one chassis are not host redundancy. |
| mini | T3 and agent orchestration stop, plus its worker capacity; queue remains on b70. | T3 still stops, but queue and GPU hosts remain independent. Keep recovery instructions/credentials accessible without T3. |
| m4-16 | Its CPU/Metal work is interrupted; other hosts and queue remain. | Same; portable compatible jobs may resume elsewhere. |
| m4-24 | Its CPU/Metal work is interrupted; queue remains on b70. | Becomes the queue-host case above. |

Implementation details matter: queue restart grants **180 s from startup** for
unreported workers (`RestartGrace`, configurable with `--restart-grace`). Normal
quiet-loss defaults are production **5 s**, deadline **20 s**, experiment **60 s**;
confirm saved effective values at the window. Pending valid check-ins can earn
bounded contact credit. A worker check-in transport failure retains running
actors and retries; it does not impose a 180 s local kill timer. Authentication,
protocol and conflict errors can terminate its loop. Queue restart grace does
not retarget old endpoint URLs, make unavailable blobs readable, or prove cleanup.

Fenced attempts, local result/spool replay and verified checkpoints preserve
progress. Genuine host loss can destroy uncheckpointed work; non-resumable jobs
restart only under applicable retry policy. Recovery from an older backup can
lose accepted jobs, cancellation/key state, newest curves/policy and custody.
Reconcile exact attempts before resuming; an old snapshot is not safe automatic
failover. NAS failure and shared power/AP/switch failure are separate common-mode
risks which this host move does not eliminate.

## Small follow-ups within one-queue design

- **Measured ride-through:** first rehearse a transport outage longer than the
  47 s reboot with a running command and a completed buffered result. Cover a
  >180 s outage plus reconnect, exact replay and stale-attempt fencing with fake
  time. Consider a 10 minute restart grace for recovery only after measuring the
  restore/rejoin path. It prevents premature loss during recovery but delays
  detection of truly dead workers by up to seven extra minutes. It does not fix
  live five-second production losses; do not relax those as a CPU-starvation
  workaround. Existing worker retry already outlives 180 s; improve missing
  behavior only where the rehearsal demonstrates a gap.
- **Cold standby restore:** use mini as an emergency destination with no running
  queue and no replication. Its 206 GiB free fits one current payload copy, but
  requires a new space/pressure check. Keep native signed bytes, installer
  recipe, root manifest and privately retrievable authority off m4-24. First
  establish a measured periodic verified-backup cadence: a proposed hourly
  stopped snapshot has up to one hour RPO and repeated queue interruption,
  so prefer an existing idle-window backup initially and record its actual age.
  If that RPO is unacceptable, separately design a consistent SQLite online
  backup plus referenced-file snapshot; rsync of a live DB is not that feature.
  Never silently promise zero data loss. Fence the old host before restoring;
  on partition with uncertain power state, promotion waits for positive fencing.
  Target 10–20 minutes recovery only with a pre-staged verified generation;
  fetching all payloads on demand adds the measured bulk-copy time.
- **Authority relocation rehearsal:** retain both existing address-refresh
  recipes above, including explicit pinned verification and durable invitation
  placement. File a narrow tooling defect only if the actual supported overlay
  or client path fails; no new retarget command is assumed necessary.
- **Capability redundancy through portable variants:** connect the pending
  run-anywhere design to [#16](https://github.com/1kuna/mycelium/issues/16),
  [#192](https://github.com/1kuna/mycelium/issues/192) and the existing
  [extension composition contract](design-proposal.md#extension-composition-e1-98).
  The local `design/run-anywhere` work had no published PR at sampling; these
  durable design issues are the link until its owner publishes one. Qualify
  equivalent CUDA/XPU/Metal/CPU variants and portable checkpoint formats on two
  independent hosts per important workload; apply MY-19 accuracy parity before
  routing. A platform label alone is not portability. Remove accidental machine
  restrictions only with workload-owner approval; require appropriate memory,
  environment and model availability. Truly CUDA-only or XPU-only software still
  has a sole-host dependency; physical redundancy is the maintainer's decision.

## Current state and next owner

Planning complete; no move or native rehearsal performed. #316 owns the scheduled
cutover, joining-address rehearsal and matched latency acceptance. The
maintainer schedules the window after reviewing this recommendation; the assigned
execution worker rechecks space/load, rehearses restore and joining, measures the
outage budget, then performs and verifies the move. Recovery/ride-through work
needs that measured evidence; portable-variant design stays with its existing
owner. No architecture ruling was changed by this plan.
