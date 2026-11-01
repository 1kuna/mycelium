---
name: mycelium
description: "Use when work needs fleet compute (heavy tests, builds, sims, training, transcodes, model runs), when you submit, check, wait on or fetch a Mycelium job, when a job is waiting, or when someone asks you to hold or yield capacity."
---

<!-- Public consumer guide: no host names, fleet layout, private paths or project names. Update with consumer behavior changes; release qualifiers distinguish merged code from the deployed release. -->

# Mycelium

Mycelium runs CPU and GPU work for every project through one queue. It places each job among the currently eligible, qualified hosts using its implemented scheduling policy. You describe what the work **needs** and its **class**; you don't pick machines or negotiate with other projects for room.

## Submit

1. Use the project's `.mycelium/adapter.yaml` entry (`myce run --entry NAME --key KEY`) or an existing profile (`myce run --profile NAME --key KEY`). For a one-off command, `myce run --cpu N --mem 8G -- CMD` (add `--gpu cuda|xpu|metal`; `--mem` is then accelerator memory and `--host-mem` host memory); a one-off takes no `--key`, `--notify` or `--input`, so use an adapter entry when you need those. If nothing fits, author an adapter with [the adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md); [setup-project.md](setup-project.md) covers profiles.
2. **State needs, not machines:** platform, accelerator, memory, cores, environment. Use `memory: auto` where comparable accepted evidence exists; preserve measured bounds and real containment needs. A cold run is exploratory, not proof that a smaller reservation is safe. `myce portability` shows target, environment and qualification gaps; adding a variant still needs output parity before production use.

   Keep a host/device pin only for a hardware requirement, parity or validation, local-only data, or a shared host that can run only an already-prepared environment. Record the reason in the adapter's `requirement_reasons`. Replace filesystem affinity with managed `source`, `files` or immutable `shared_inputs`; use the [queue-visible environment library](https://github.com/1kuna/mycelium/blob/main/docs/environments.md#queue-visible-environment-library) (`myce env list`, `myce env get NAME`) for runtime dependencies. Verify preparation and qualification before removing a pin.

   Shared interactive hosts admit only native CPU commands with already-prepared environments: no Docker, extension jobs or new environment preparation. Use a plain command via `myce run --cpu N --mem SIZE -- CMD` or a native command adapter entry with a prepared environment; work needing other execution modes waits for an eligible target.
3. **Choose a class:**

   - `production`: work your project's rulings call production. Under the current policy it ranks first and can stop lower-tier work, including non-resumable commands.
   - `deadline` with `--deadline <RFC3339>`: work with a real date that Zach or a ruling set. It can stop lower-tier resumable work after a grace period; non-resumable commands keep running.
   - `experiment` (the default): everything else.

   Equal-tier work is never stopped to make room, a running job isn't migrated, and eligibility, resources and locks still apply, so a higher class doesn't guarantee immediate capacity. A higher class than the work deserves takes capacity from someone else's real deadline.
4. `git: HEAD` sends committed bytes only: commit first, and ignore `.mycelium/receipts/`. Save the receipt's `key` and `job_id`. The same key recovers the same job after a lost reply; changed intent needs a new key.
5. Make long experiments resumable: implement [atomic checkpoints](https://github.com/1kuna/mycelium/blob/main/docs/files.md) through `MYCE_CHECKPOINT_DIR`, restore progress on restart, then declare `resumable: true`. The flag alone does not preserve progress.

## Wait and check status

- For an adapter or profile submission, bind completion to **your own** thread with [t3-binding.md](t3-binding.md) and pass its notifier as `--notify`. One binding per submitting thread, never an orchestrator's or a shared one. For a one-off, wait with `myce wait JOB_ID` (exits 0 only on success). Check the installed CLI's usage line before relying on a flag.
- Read one job at a time: `myce status JOB_ID` (with its attempt history), `myce logs JOB_ID`. The full `myce status` snapshot is large and slows the queue for everyone, so keep it out of loops, checks and briefs.
- Give a long job one stall check at about twice its expected duration, bound to your thread, verifying one thing (the job's state or log growth), and delete it when done.

## Results

`myce result JOB_ID --out DIR` verifies each output's SHA-256 and size; then run your project's own acceptance check, since a wake or a zero exit proves no result. Download within the retention window (default seven days). Failed jobs have no accepted outputs; `myce result JOB_ID --attempt N --out DIR` fetches failed-attempt diagnostics. Report placement as "Mycelium placed it on X because Y", from the job's placement rationale.

## Urgency and holds

- **Urgency is the class.** When your work must go first, submit it with the class it truly has and read its waiting reason. A message asking another project to stop can't change queue order, and their queued jobs would take the freed slot anyway.
- **Existing holds and customer-window rulings still bind** until Mycelium explicitly replaces them; follow your project's rulings on where and when work may run.
- **Isolated checks:** use a job-bound reservation, not a higher class or a chat-negotiated hold. `myce extension check --reserve` reserves its selected device (the whole host for a CPU check); for an existing single validation job, use `myce reserve add SELECTOR --job JOB_ID --owner OWNER --reason TEXT`. Prefer `host=HOST;devices=DEVICE` over `host=HOST` when only one device needs isolation, leaving siblings eligible. Reservations drain conflicting work and release after the bound job finishes and cleanup is confirmed; they do not guarantee immediate capacity. See [validation reservations](https://github.com/1kuna/mycelium/blob/main/docs/operations.md#reserve-a-host-or-its-devices-for-an-existing-validation-job). Existing customer windows and broader policy changes stay with the queue owner.
- **When asked to hold**, stop submit, retry and callback paths; the hold covers queued jobs too. Leave running jobs alone. **Available from the next release:** `myce pause JOB_ID --reason TEXT` makes a queued job non-runnable; after clearance, `myce resume JOB_ID` restores it with the same key and queue age. Assigned/running attempts cannot be paused. **Until that release:** ask the queue owner to hold queued work; do not cancel and resubmit it.
- Queue policy (`myce policy`) is shared state: propose a change to the Mycelium thread rather than setting it.

## When a job waits

1. Read its waiting reason: `myce status JOB_ID`. Every waiting job has one: missing qualification, memory or resource needs, no available host, software preparation, or work ahead in the queue. GPU batch-slot contention can show up as a memory reason (mycelium#537).
2. Fix what's yours, in order: right-size needs using comparable accepted evidence, add portable targets, make the job resumable, and choose `deadline` plus a real date only when warranted. **Available from the next release:** submission/status/wait replies include deadline guidance explaining policy precedence, blockers and ETA uncertainty. This is advice, not a deadline guarantee or an edit to accepted job intent. **Until that release:** use the waiting reason and ask the queue owner about an existing job's urgency; do not cancel and resubmit it.
3. Otherwise it's a hold with a next owner: record it on your ticket and keep other work moving. Running heavy work locally on a shared host instead isn't a fallback.
4. If the reason looks wrong, or Mycelium itself failed, search the Mycelium issues for a duplicate, then update or file a sanitized one (expected versus observed, version, job and attempt id, minimal command). The repo is public: no secrets, private paths, host details or webhook URLs.

## Credentials

Never open, print, copy or pass fleet credentials, and never set the fleet token yourself. Use the installed-authority CLI or `Client.from_installed()`, which resolve credentials internally; prefer the CLI for read-only status and result calls. A missing CLI or failed authority is a setup hold, not a reason to look for the token.
