# Set up project offload

For new reusable work, follow [the adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md):
one `.mycelium/adapter.yaml`, explicit targets, committed source, result checks,
and submit/wait/verified retrieval. Keep this package's [SKILL.md](SKILL.md) and
supporting files together when installing it.

## Existing profile compatibility

Keep existing `.mycelium/profiles/NAME.yaml` callers working. To initialize a
compatibility profile, use `myce profile init --name NAME --environment ENV -- CMD`,
review source/targets/resources/outputs, and commit the intended source. The
[profile reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#project-profiles)
explains source safety, automatic memory, per-item inputs, typed receipts and
recovery; [conversion](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#convert-profiles-and-extensions)
is explicit and never switches callers. Use a new key for an intentional repeat.

Before switching callers, prove a real queued run, inspect logs and retrieve its
verified accepted output; apply the project's own result check. Keep CI unchanged
unless its runner has authorized installed queue authority. Record the entry or
profile and its proof in the project's own integration records, following that
project's rules. A library/server uses the service contract; a native recipe needs
its recorded baseline and exact-target qualification. Neither follows from a
profile's successful command alone.

## Install the agent skill

From this repository, copy the skill directory, including its supporting files,
into your coding agent's skill directory. For Codex:

```sh
mkdir -p "${CODEX_HOME:-$HOME/.codex}/skills/mycelium-offload"
cp -R skills/mycelium-offload/. "${CODEX_HOME:-$HOME/.codex}/skills/mycelium-offload/"
```

For Claude Code, use `~/.claude/skills/mycelium-offload/` as the destination.
Reload skills or start a new session after installation. Other harnesses can
load `skills/mycelium-offload/SKILL.md` directly. This installs agent instructions;
shell interception depends on the harness integration linked below.


For harness interception, see [harness-binding.md](harness-binding.md).
