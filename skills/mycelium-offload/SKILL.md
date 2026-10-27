---
name: mycelium-offload
description: Send heavy tests, builds, simulations, training and transcodes through Mycelium project profiles; wait for completion, inspect failures and fetch verified results.
---

# Mycelium offload

Run heavy commands from the project root with `myce run --profile NAME`.
Mycelium chooses placement. Small reads and a targeted test can stay local;
queue or capacity problems are holds with a next owner, never a local fallback.

1. Read `.mycelium/profiles/NAME.yaml`. If none fits, use `myce profile init`
   and [setup-project.md](setup-project.md). Review the command, source,
   targets and managed environment. `git: HEAD` sends committed bytes only;
   commit intended changes and ignore `.mycelium/receipts/` before submitting.
   Directory sources include ignored files: prepare them deliberately.
2. Submit once. Extra arguments append literally:

   ```sh
   myce run --profile NAME --key OPERATION
   myce run --profile NAME --key OPERATION -- EXTRA_ARGS
   ```

   Save the receipt's `key` and `job_id`. Same-key replay recovers the existing
   job; a changed intent needs a new key. To repeat identical work deliberately,
   use a new key. Read typed failures and receipt details in
   [agent-offload.md](https://github.com/1kuna/mycelium/blob/main/docs/agent-offload.md).
3. Wait with `myce wait JOB_ID`, or bind this thread's completion notifier
   using [t3-binding.md](t3-binding.md) and pass `--notify /path/to/notifier`
   on every submit and replay. End a turn waiting for a wake only when both
   `observation` and `delivery` are `armed`, or a hold records the next owner.
   `adopted_other_notifier` does not promise your thread a wake: wait in the
   foreground. A terminal receipt needs no further wake. For
   `observation_failed`, recover observation with the same key; the job itself
   has not failed. Use event completion, not repeated status reads.
4. Verify completion, read logs, and fetch the outputs the task needs:

   ```sh
   myce wait JOB_ID
   myce logs JOB_ID
   myce result JOB_ID --out RESULTS_DIR
   ```

   `wait` exits 0 only for success. A wake alone proves no result. `result`
   verifies SHA-256 and size; apply the project's own acceptance check too.
   Download within the queue's retention window (default seven days).
   Failed jobs have no accepted outputs. Once
   [#220](https://github.com/1kuna/mycelium/issues/220) is deployed, use
   `myce result JOB_ID --attempt N --out DIAGNOSTICS_DIR` for that failed
   attempt's diagnostics. These are failure details, not accepted success.
   Until then use logs; check installed `myce result --help` before using
   `--attempt`. Keep attempt/execution identity when reporting a failure.
5. If Mycelium or the adapter fails, search the Mycelium issue tracker for a
   duplicate, then update it or file a sanitized issue. Include expected versus
   observed behavior, version, target, job/attempt identity, minimal command and
   relevant redacted diagnostics. Add `Provenance: mycelium-offload agent`.
   Keep secrets, private source, raw local paths, personal data and webhook URLs
   out of public issues. Report a profile mistake to its project owner.

Commands use installed authority. Keep tokens out of commands, profiles,
`run.env`, logs and issues. A missing CLI or failed authority is a setup hold.
