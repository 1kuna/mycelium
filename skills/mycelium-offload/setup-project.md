# Set up a project for Mycelium

Use this when the project has no profile that fits its heavy work. The result is
a committed profile, a locked environment handed to the queue operator, one
proven end-to-end run, and a recorded switch. Start with `myce profile init --name NAME` from the project root, then review
the generated starter. See [profile initialization](https://github.com/1kuna/mycelium/blob/main/docs/agent-offload.md#initialize-a-profile) for commands, environment prerequisites and source handling.

Canonical references: [agent-offload.md](https://github.com/1kuna/mycelium/blob/main/docs/agent-offload.md#project-profiles),
[job reference](https://github.com/1kuna/mycelium/blob/main/docs/job-reference.md#batch-yaml)
and [environments](https://github.com/1kuna/mycelium/blob/main/docs/environments.md#cpu-tools-with-pixi).
Profiles are strict YAML, so use only fields those docs define.

## 1. Inventory

Run `myce portability` before choosing targets. Declare what the software needs
and let Mycelium choose a host. Review adapter coverage with
`myce portability --adapter .mycelium/adapter.yaml --entry NAME` after authoring
and after inventory or requirements change. See the
[placement contract](https://github.com/1kuna/mycelium/blob/main/docs/portability.md#declare-requirements-let-mycelium-place).

List the project's heavy commands from its scripts, Makefile or task runner,
CI workflows, and README or AGENTS.md: test and race suites, builds, sims,
training, OCR and transcodes. For each one, note the exact argv, the tools and
versions it needs, the files it reads beyond the committed tree, what it
produces, and how success is observed.

Make one profile per command that agents or CI launch as a unit and that is
heavy enough to matter. A quick targeted test stays local. GPU work has no
profile path yet ([mycelium#187](https://github.com/1kuna/mycelium/issues/187)),
and long-lived servers belong in section 4.

## 2. Write the profile

Create `.mycelium/profiles/NAME.yaml`, where the filename stem is the profile
name:

```yaml
name: PROJECT-NAME            # also the memory-learning class
project: PROJECT
class: test                   # free-form job class
source:
  git: HEAD                   # committed tree; a dirty HEAD fails with exit 64
run:
  command: [bash, scripts/NAME-profile.sh]
needs:
  cpu_cores: 2
  memory: auto
  platform: cpu
targets:                      # exact os/arch/platform alternatives, and only ones that work
  - {os: linux, arch: amd64, platform: cpu}
  - {os: linux, arch: arm64, platform: cpu}
environment: PROJECT-tools    # locked recipe (section 3); {} means the worker's installed tools
outputs:
  NAME-result.txt: results    # required relative file -> result prefix
check: [grep, -qx, 'NAME: exit=0', NAME-result.txt]
```

- **Targets** list only platforms you have proven, and they must match the
  environment's locked platforms. Add missing platform recipes and prove them;
  a missing recipe is a portability gap, not a hardware requirement. A lock
  covering one platform limits platform coverage; it does not require a host pin.
- **`memory: auto`** learns the peak from successful runs. Match tool
  parallelism to `cpu_cores`, for example `GOMAXPROCS` in `run.env`.
- **`check`** must prove the work passed, not just that a file exists. The
  wrapper writes the marker only after the real command succeeds.
- **`inputs: {DEST: local/path}`** carries files that are not in the committed
  tree.
- Never put credentials in the profile, the command or `run.env`.

The wrapper runs in an isolated copy of the source. It checks prerequisites
first and names whatever is missing, so a bad machine fails with a reason:

```sh
#!/usr/bin/env bash
set -euo pipefail
for tool in TOOL1 TOOL2; do
  command -v "$tool" >/dev/null || { echo "missing prerequisite: $tool" >&2; exit 1; }
done
REAL COMMAND
printf 'NAME: exit=0\n' > NAME-result.txt
```

Add `.mycelium/receipts/` to `.gitignore`. Commit the profile and wrapper.

## 3. Lock the environment

Do not depend on whatever a worker happens to have installed. A profile with
`environment: {}` can qualify on a matching machine that lacks the toolchain and
then die without saying why
([mycelium#189](https://github.com/1kuna/mycelium/issues/189)). Pin the tools
in a pixi environment instead:

1. Write `env/PROJECT-tools/pixi.toml` with every tool the wrapper uses, and
   list exactly the platforms in `targets`: `linux-64` (linux/amd64),
   `linux-aarch64` (linux/arm64), `osx-arm64` (darwin/arm64).
2. Run `pixi lock --manifest-path env/PROJECT-tools/pixi.toml` and commit both
   files.
3. Hand off to the queue operator, who registers named recipes in the queue
   catalog. You cannot. Send the two files, their `shasum -a 256` hashes, and
   the catalog entry:

   ```yaml
   PROJECT-tools:
     cpu: {backend: pixi, lock_file: PROJECT-tools/pixi.lock, environment: default}
   ```

   The queue loads its catalog at startup. Until the operator restarts it, a
   submit naming `PROJECT-tools` is rejected as `unknown environment`. Record
   that as a hold with the operator as next owner.

Keep the prerequisite check after the switch. It turns any remaining toolchain
gap into a named failure.

## 4. When a profile is not enough

A profile wraps one finite command over a source tree. Choose something else
when the work is:

- **A long-lived model server** that callers reach over HTTP, such as an
  inference, speech or OCR API. Make it a catalog model with `engine: server`
  ([general servers](https://github.com/1kuna/mycelium/blob/main/docs/guide-servers.md)).
- **An accelerator stage, or a kind of work that needs qualification**:
  per-target variants, native baselines, benchmarks, model assets, sidecars, or
  reuse across projects. Write an extension
  ([write an extension](https://github.com/1kuna/mycelium/blob/main/docs/write-an-extension.md),
  [agent guide](https://github.com/1kuna/mycelium/blob/main/docs/extensions/AGENT-GUIDE.md)).
  Each machine must qualify the extension before it receives work.

If the work is a finite CPU command that only this project runs, keep it a
profile.

## 5. Prove it, then switch

1. Optionally rehearse on a laptop queue with `myce dev` (it accepts
   `--environment-config` for the catalog), following
   [Dogfood](https://github.com/1kuna/mycelium/blob/main/docs/agent-offload.md#dogfood).
2. Run one real job through the steps in [SKILL.md](SKILL.md): submit, take the
   wake, run `myce result JOB_ID --out DIR`, and confirm that the output holds
   the real command's result. Check its log with `myce logs JOB_ID` as well.
   Fix and repeat until it succeeds. A failure on a machine missing the
   toolchain must name the tool.
3. Only then switch the callers. Point the project's agents at
   `myce run --profile NAME` in place of the local command. Switch a CI step
   only where its runner is an enrolled machine with installed authority, and
   otherwise leave CI unchanged and say so.
4. Record each profile name, the command it replaces and the proof job in the
   project's AGENTS.md. Follow that project's own rules for editing AGENTS.md.
