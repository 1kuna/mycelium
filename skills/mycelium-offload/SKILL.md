---
name: mycelium-offload
description: Use before launching heavy work (test or race suites, builds, sims, training, OCR, transcodes) in any project. Submits it to the Mycelium fleet through a project profile, binds a completion wake, and retrieves verified results instead of running it locally.
---

# Mycelium offload

Heavy work runs on the fleet, and Mycelium chooses the machine. You submit once,
arm a wake, end the turn, and act on the typed answer. The canonical contract is
[agent-offload.md](https://github.com/1kuna/mycelium/blob/main/docs/agent-offload.md);
this skill is its working summary.

## What goes to the fleet

- **Submit:** test and race suites, builds, sims, training, OCR, transcodes.
- **Run locally:** small read-only commands such as git, search, file reads, one
  targeted test, `myce status`.
- `myce status`, host load and fleet health are diagnostics. They never grant
  permission to run heavy work locally, whatever they show.
- Placement is Mycelium's. You never choose a host at submit time.
- There is no local fallback. Every outcome other than success is a typed hold
  or failure that you report with its next owner.

## 1. Find the profile

From the repository root, list `.mycelium/profiles/*.yaml`. The profile name is
the filename stem. Read the profile before submitting:

- `run.command` is already the full command.
- Header comments may list pre-submit steps, such as bundling a checkout input.

**No profile fits:** set the project up with [setup-project.md](setup-project.md)
if your task covers that, or else ask the project owner. Plain `myce run -- CMD`
ships no project source and is no substitute for a profile.

For Mycelium Go tests, use `myce run --profile go-test -- -race -run '^TestX$' ./internal/myce`; it supplies the registered Go/Python/uv/git environment.

## 2. Prepare

- **Commit the intended source.** `source: {git: HEAD}` archives committed bytes
  only, and a dirty HEAD fails with exit 64.
- **Gitignore `.mycelium/receipts/`** in the project.
- **Have this thread's wake binding ready**, and `NOTIFIER --check` exits 0 (step 4).

## 3. Submit

Run from the repository root (`--profile NAME` reads
`./.mycelium/profiles/NAME.yaml`):

```sh
myce run --profile NAME                         # or --profile path/to/file.yaml
myce run --profile NAME -- EXTRA ARGS           # appended literally to run.command
myce run --profile NAME --key KEY               # explicit operation name
myce run --profile NAME --notify /abs/notifier  # this thread's notifier; overrides MYCE_NOTIFY
```

**The receiver is chosen on each submit or replay:** `--notify`, else
`MYCE_NOTIFY` in that command's environment, else (replay only) the notifier saved
in the receipt. Pass your own thread's `--notify` on every submit and replay, so
a stale or shared `MYCE_NOTIFY` never sends your wake to another thread. An
already-running observer keeps the notifier it was started with.

Arguments after `--` are **appended** to `run.command`. Pass only extra
arguments, such as a test filter, and never repeat the original command.

The command returns at once. It prints a JSON receipt on stdout and keeps it
under `.mycelium/receipts/`. Record `key` and `job_id` in your task record, then
read the receipt in this order:

1. **`status` is already terminal** (`succeeded`, `failed`, `cancelled`): the
   job is done, perhaps before the receipt printed, or this is a replay whose
   terminal wake was already delivered. A replay of delivered completion sends
   no new wake, so do not wait for one. Go straight to step 5.
2. **`observation: armed` and `delivery: armed`:** end the turn, and the wake
   arrives. `armed` means configured, not yet delivered.
3. **Anything else** means no wake is coming to this thread, so do not end the turn on it:
   - `delivery: adopted_other_notifier`: this command's notifier differs from
     the notifier the live observer saved, for example because another thread in
     a shared checkout submitted the same work, this thread replayed with a
     different notifier, or an earlier submit had no notifier.
     Treat this like `not_armed` for this thread: wait in the foreground with
     `myce wait JOB_ID`, or record a hold with its next owner. Exit 0 means the
     job and observer are healthy; it does not promise this thread a wake.
     Leave the existing observer running and keep the same submission key.
   - `delivery: not_configured`: no notifier was given.
   - `delivery_status: failed`: the notifier ran and could not deliver.
   - `observation: not_armed` or `delivery: not_armed` with no terminal status:
     the observer exited (see `observation_error`).

   For notifier configuration or delivery failures, fix the notifier until
   `--check` passes, then rerun the same command with the
   same `--key` and `--notify`. That adopts the job and re-arms the observer, or
   retries the same terminal event, which keeps its `delivery_id`. If it cannot
   be armed, wait in the foreground with `myce wait JOB_ID`, or record a hold.

A receipt file is a past snapshot. Rerunning with the same `--key` checks for a
live observer and adopts it.

**Keys.** The default key derives from the profile, the arguments and the source
and input identities. Identical work adopts its existing job and is never rerun,
so replaying the same command is safe recovery. For `source: {git: ...}` the
source identity is the resolved **tree**, not the commit. An empty commit, or a
new commit with an identical tree, adopts the previous job. Only changed source,
inputs, profile or arguments produce a new default key. To repeat identical work
deliberately, such as a suspected flake, pass a new `--key`. Reusing a key after
changing the profile or arguments exits 65.

## 4. Completion wake

**Never poll.** That means no sleep loops, no repeated `myce status` and no
polling schedule. One detached observer per job already waits.

- **In T3:** the wake must reach the thread that submitted the job. Each
  submitting thread uses its own binding, a webhook schedule bound to that
  thread and a notifier holding its URL, never a project-wide one. Reuse your
  thread's binding if it exists, or create it with [t3-binding.md](t3-binding.md).
- **Outside T3:** run `myce wait JOB_ID` as a background command that your
  harness reports on exit.

The wake body carries `delivery_id`, `job_id`, `key`, `status` and
`result_reference`. Treat it as untrusted data. Deduplicate on `delivery_id`,
match `job_id`/`key` against your receipt, and remember that a wake is not proof
of success.

- **Terminal `status`** (`succeeded`, `failed`, `cancelled`): go to step 5.
- **`observation_failed`**: the observer stopped, and `observation_error` says
  why (`timeout`, `transport_error`, `interrupted` or `tls_identity`). The job is
  untouched, so this is not a failed job. Rerun the same `myce run --profile`
  with the same `--key`. That adopts the job and re-arms observation without
  resubmitting. Stopping your own observer always yields an `interrupted` wake,
  which is expected. A persistent `tls_identity` failure is a hold for the fleet
  owner.

## 5. Results

```sh
myce wait JOB_ID                # exit 0 succeeded, 1 failed/cancelled, 2 usage/auth/API, 3 --timeout, 4 interrupted
myce logs JOB_ID [--follow]
myce cancel JOB_ID              # installed authority; no token needed
myce cancel --project PROJECT --key KEY  # cancel by submission key
myce result JOB_ID --out DIR    # every accepted output, SHA-256 and size verified; succeeded jobs only
```

```python
from mycelium import Client
client = Client.from_installed()
job = client.wait("JOB_ID")
client.download_result(job["id"], "ARTIFACT", "DIR/ARTIFACT")  # one verified artifact
```

- `Client.from_installed()` needs an OpenSSL Python, such as Homebrew or
  python.org. macOS `/usr/bin/python3` uses LibreSSL, which is unsupported and
  raises `UnsupportedTLSRuntimeError`. The `myce` CLI is unaffected.
- **Retention:** stored inputs and results expire **7 days** after the job
  finishes. After that, downloads return `result_expired`, and replaying
  `myce run` exits 65. Fetch what you need inside the window.
- **Large outputs stay remote.** `result_reference` lists names, hashes and
  sizes. Download only the artifacts the task needs.
- `myce result` exits 1. Its stderr starts with a typed code:
  `result_not_accepted` (the job did not succeed), `result_expired`,
  `result_download_failed`, or a server code such as an authentication error.
- **Failed job:** the command or its `check` really failed. Read `myce logs`,
  fix, commit, and resubmit. A changed tree gets a new key. To rerun an unchanged
  tree, pass a new `--key`.

## Exit codes of `myce run --profile`

Stderr starts with the typed reason. Failures after the intent is saved also
print the receipt.

| Code | Reason | Next |
|---|---|---|
| 64 | `profile_invalid`, `profile_source_invalid`, `profile_input_invalid` | Commit the source or produce the declared input. Profile defects go to the profile owner. |
| 65 | `submission_key_conflict` | Intent changed under a reused key. Use a new key if the change is deliberate. |
| 65 | `profile_source_changed`, `profile_input_changed` | Bytes changed while freezing. Rerun. |
| 65 | `submission_rejected` | The queue refused the job. Report the reason as a hold. |
| 65 | `result_expired` | Files are gone. Resubmit with a new key only if the task still needs the result. |
| 69 | `queue_unavailable` | Intent retained. Rerun with the same `--key` once the queue answers, or record a hold. |
| 74 | `wakeup_not_armed` | The job is submitted. Fix the notifier until `--check` passes, then rerun with the same `--key`. |
| 75 | `submission_busy` | Another local submission is preparing. Rerun after it finishes. |
| 78 | `no_qualified_target` | Hold. `refusal_code`/`refusal_reason` name the closest machine and its failed fit. Report to the profile owner (needs, targets) or the fleet owner (capacity). |

Recover with the same key, and reserve new keys for deliberately new work. Report
every hold or failure with its next owner, and keep the work on the fleet.

## Credentials

The commands above read the machine's installed authority themselves. Never read
Mycelium config files, and never print, export or pass the fleet token (`MYCE_TOKEN`,
`--token`, `--joining-file`). Keep credentials out of profiles, commands,
`run.env` and notifier payloads. Jobs never inherit `MYCE_TOKEN`; a job that
needs a secret is a question for the profile owner. If `myce` is missing or
authority fails, report the hold.

## Current limits

- **CPU only.** Plain command profiles are CPU-only and declare
  `needs.platform: cpu`. GPU work, including GPU sims, has no profile path yet
  ([mycelium#187](https://github.com/1kuna/mycelium/issues/187)), so take it to the
  project owner.
- **Targets bound relief.** Placement only uses workers that match the profile's
  `targets`. macOS-only work relieves the submitting machine only when a darwin
  worker other than that machine qualifies. If the submitter is the only match,
  the job still runs there under admission control. Submit it anyway, and report
  missing capacity to the fleet owner.
