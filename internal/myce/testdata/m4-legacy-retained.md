# Legacy retained spool regression (#100, M4)

`m4-legacy-retained.json` is a sanitized projection of nine real retained Docker
attempts copied read-only from the production split-worker spools. Only execution
bindings, outcome flags, numeric Docker fields and retention acknowledgements
are retained. IDs, worker/session names, endpoints, times and error text are
synthetic. No production paths, hostnames, containers, commands, logs or secrets
are included. Raw source custody stays in private ignored evidence.

The stopped-window report counted 98 retained Docker records and results. The
later read-only copy contained 95 of each; all seven reported blockers remained:

- `record-kill-1` and `record-requeue-1`/`record-requeue-2` are released, forced,
  interrupted attempts without Docker retention acknowledgements.
- Those three results plus `record-restart-1` are superseded by successful
  attempts 2, 3 and 2 respectively. The restart result also carries a cleanup
  message and has a valid retention acknowledgement. Its exact-current-attempt
  check still fails.
- The three successful successor attempts and two ordinary successful attempts
  exercise normal retention alongside the blockers.

These are **seven rejected files across four attempts**. Both copied heartbeat
and process states contained no active execution. All present Docker acks use the
same `{worker, at}` format as the historical writers; this is not an ack-format
migration.

At `08816cf`, `Worker.Run` writes a Docker acknowledgement only after completion
returns nil. It clears job/result/identity on either nil or `ErrStale`.
At `57ed4e6`, `Queue.CompleteContext` commits exact release/copy settlement before
returning `ErrStale` for a superseded production result. Thus kill/requeue can
leave released Docker custody and a result on disk without the retention ack.
The fourth interrupted result was acknowledged and retained normally, then its
job ran a later attempt. Neither a missing ack nor an interrupted old result
alone proves live custody or release.

The fixture's **queue is synthetic**, modeling the report's terminal, settled
queue with exact copies for the observed attempt chains. It is not a production
queue snapshot or authorization to convert production. Conversion must cross-check
the actual exclusively locked queue: a terminal current attempt needs confirmed
release; a superseded attempt needs its exact confirmed-release copy. A larger
attempt number, a successful result or an old ack never substitutes for that
proof. Existing live-state, Docker, process, cgroup and lock checks still apply.
