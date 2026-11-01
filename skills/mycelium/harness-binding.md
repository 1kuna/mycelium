# Harness command routing

For a Claude `PreToolUse` hook matching the shell tool, parse the structured tool
input and match known heavy launches to the project's profile. Return the
harness's deny decision with the exact `myce run --profile NAME` replacement, or
route through a supported tool adapter that runs it once and returns the receipt.
Permit small read-only commands and the adapter itself to avoid recursion. Do
not run both the original command and its replacement. Persist/reuse the key in
the harness operation record; only end the agent turn when observation and
delivery are both armed, completion is already verified, or an explicit typed
hold records its next owner.

For Codex, install the equivalent only through a supported pre-tool interception
interface in the host harness. Do not assume every Codex CLI or T3 provider
exposes Claude's `PreToolUse` event or JSON response schema. If it does not, expose
a wrapper/tool invoking this same CLI and record shell bypass as unsupported
coverage. No global configuration is installed by this change.

For T3, create/reuse an owning-thread webhook schedule and store its receiver
binding privately. Configure replay deduplication with `{{body.delivery_id}}`.
A local notifier's `--check` validates that private binding; its normal invocation
posts stdin JSON to it. The receiver wakes the owning task, deduplicates delivery
IDs, re-reads authoritative job state, downloads/verifies outputs and applies the
project's acceptance check. An `observation_failed` wake calls for observer
recovery, not a failed-job verdict. A wake is not a successful result or confirmed
resource release. Retry after ambiguous delivery can send the same ID again;
receivers must deduplicate. The existing positional `job-notify` protocol needs
a wrapper: an exit-zero missing-config path cannot satisfy this contract.

Command matching is cooperative routing, not an OS security boundary. Nested
shells, subprocesses, renamed binaries, SSH, containers and unhooked tools can
bypass it. Report coverage explicitly rather than claiming universal interception.
