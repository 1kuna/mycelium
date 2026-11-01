---
name: mycelium-offload
description: Send heavy tests, builds, simulations, training and transcodes through Mycelium project adapters or existing profiles; wait for completion, inspect failures and fetch verified results.
---

# Mycelium offload

Run heavy commands from the project root with `myce run --entry NAME`.
For an existing profile use `myce run --profile NAME`. New reusable integrations
use [the adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md).
Declare the software's platform, accelerator, memory and environment needs;
Mycelium chooses the host. Keep host/device selectors for a specific hardware
requirement, approved production parity or hardware qualification. Explain that
exception in the adapter's `requirement_reasons` (see the portability reference).
Report “Mycelium placed it on X because Y”, using the job's placement rationale;
distinguish an authored restriction from the assigned host or `production_host`.
Small reads and a targeted test can stay local;
queue or capacity problems are holds with a next owner, never a local fallback.

Before authoring a project adapter or changing targets, run `myce portability`
for live cluster targets, then
`myce portability --adapter .mycelium/adapter.yaml --entry NAME --json`.
Recheck when inventory or requirements change. For registered native evidence,
use `myce portability --extension NAME@VERSION --project PROJECT --json`.
Follow the report's per-target lock/build, portable API, preparation and native
qualification steps. Missing variants are gaps to investigate; authored
`requirement_reasons` explain legitimate hard limits. CPU variants keep their
own resources/checks; GPU checks must use their backend (`metal` selects `mps`
in the application). Obtain owner-accepted MY-19 output parity before splitting.
Advice is read-only; absent/stale inventory and unqualified recipes are not
support claims. [Portability reference](https://github.com/1kuna/mycelium/blob/main/docs/scheduling.md).

1. Read the selected entry in `.mycelium/adapter.yaml`, or the existing
   `.mycelium/profiles/NAME.yaml`. If none fits, author an adapter using the
   reference above; [setup-project.md](setup-project.md) covers compatibility profiles. Review the command, source,
   targets and managed environment. `git: HEAD` sends committed bytes only;
   commit intended changes and ignore `.mycelium/receipts/` before submitting.
   Directory sources include ignored files: prepare them deliberately.
2. Submit once. Extra arguments append literally:

   ```sh
   myce run --entry NAME --key OPERATION
   myce run --entry NAME --key OPERATION -- EXTRA_ARGS
   ```

   Save the receipt's `key` and `job_id`. Same-key replay recovers the existing
   job; a changed intent needs a new key. To repeat identical work deliberately,
   use a new key. Read typed failures and receipt details in
   [agent-offload.md](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md).
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
   Failed jobs have no accepted outputs. Supported clients provide
   `myce result JOB_ID --attempt N --out DIAGNOSTICS_DIR` for failed-attempt
   diagnostics. Use a compatible installed client/queue; `myce result --help`
   identifies the client interface. Diagnostics are not accepted success.
   Keep attempt/execution identity when reporting a failure.
5. If Mycelium or the adapter fails, search the Mycelium issue tracker for a
   duplicate, then update it or file a sanitized issue. Include expected versus
   observed behavior, version, target, job/attempt identity, minimal command and
   relevant redacted diagnostics. Add `Provenance: mycelium-offload agent`.
   Keep secrets, private source, raw local paths, personal data and webhook URLs
   out of public issues. Report a profile mistake to its project owner.

Commands use installed authority. Keep tokens out of commands, profiles,
`run.env`, logs and issues. A missing CLI or failed authority is a setup hold.

Shared orchestration/interactive hosts are usable within their configured limits.
Submit fitting portable native CPU tests, CI and small builds normally; do not
avoid or pin the host because it runs agents. The shared-host profile protects
measured RAM reserve and uses last-resort placement plus nice=10. Respect existing
drain/disable, storage and qualification fences. Core reservations and nice are
not hard CPU shares; do not claim pressure-triggered migration. See
[shared hosts](https://github.com/1kuna/mycelium/blob/main/docs/operations.md).
