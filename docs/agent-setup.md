# Agent setup runbook

You are a coding agent setting up Mycelium on a user's own computers. Follow
these steps in order. Commands are written for a POSIX shell. Text in CAPITALS
is a placeholder that you fill in. If a flag here is rejected, run
`myce COMMAND --help`, use the closest current option, and tell the user what
differed. Lines marked **Check:** are the least certain; confirm them with
`--help` before relying on them.

## Rules

- Run every `myce` command as the user's ordinary account. Never run `myce` with
  `sudo` or as root, because it refuses to start that way.
- The fleet token is a password for the whole cluster. Never print, echo, log,
  paste into chat or commit it, and never put it on a command line. Keep the
  files that hold it at mode `0600`.
- Run every `install` with `--dry-run` first, show the user the plan, then run
  it for real.
- Don't install drivers, mount drives, change firewalls or touch machines the
  user didn't name without asking first.
- Only the user's own trusted commands should run on the cluster. Jobs run with
  the installing account's permissions.

## 0. Ask the user

1. Which computer is the **hub**? Pick one that stays on, running Linux or an
   Apple Silicon Mac. It keeps the job list and can also run jobs.
2. Which other computers should join, and how can you reach each one (a terminal
   on it, SSH, or the user pasting commands)?
3. Where is the shared network drive (NAS) mounted on each machine?
4. Which machines does someone use every day, and which are laptops?

## 1. Check prerequisites on every machine

- **OS:** Linux on x86-64 or ARM64, or macOS on Apple Silicon.
- **Network:** the same local network, with the hub reachable from the others.
- **Shared drive:** the same NAS share (NFS, SMB or autofs) mounted on every
  machine. The defaults are `/mnt/nas` on Linux and `/Volumes/NAS` on macOS;
  otherwise pass `--nas PATH`. Mycelium never mounts a drive or falls back to
  local disk, so a missing mount stops the install with `nas_not_configured`.
  The dry run does not check the mount. Run `ls NAS_PATH` yourself and confirm
  it shows the shared drive's contents before installing.
- **GPU drivers already working:** for Nvidia, `nvidia-smi` lists the card. For
  Intel GPUs on Linux, the Level Zero/oneAPI runtime is installed. Apple GPUs
  need nothing. CPU-only machines are fine.
- **Hub only:** Git and Go 1.25.6 or newer (`go version`) to build `myce`.

## 2. Install the hub

On the hub, from a checkout of this repository:

```sh
go build -o myce ./cmd/myce
```

Find the hub's local network address (HUB_IP, for example from `ip -4 addr` or
`ipconfig getifaddr en0`). Then create the private fleet credential without
displaying it:

```sh
mkdir -p ~/.config/myce && umask 077
printf 'MYCE_TOKEN=%s\nMYCE_QUEUE=http://HUB_IP:7331\n' "$(openssl rand -hex 32)" \
  > ~/.config/myce/fleet.env
chmod 600 ~/.config/myce/fleet.env
```

Install the hub service, which listens on the local network, so other machines can reach it:

```sh
./myce install queue --listen HUB_IP:7331 --queue http://HUB_IP:7331 --nas NAS_PATH --dry-run
./myce install queue --listen HUB_IP:7331 --queue http://HUB_IP:7331 --nas NAS_PATH
```

Then run `./myce doctor` to check prerequisites. On Linux, if the installer
reports `lingering_permission_required`, ask the user to run
`sudo loginctl enable-linger USER` once, then repeat the install.

## 3. Create the first invitation

Load the credential into the environment without printing it, then write a
private invitation file. This bootstraps the first worker without requiring a
signed release; later workers can use the one-time invitations in step 4:

```sh
set -a; . ~/.config/myce/fleet.env; set +a
./myce invite --identity-dir ~/.local/share/mycelium/queue/state/myce.db.joining \
  --private-file --out invitation.json
```

**Check:** the identity directory is the hub database path (shown in the
`install queue` plan) plus `.joining`. `invitation.json` contains the fleet
token: keep it mode `0600`, move it only by private means such as `scp`, and
delete every copy once installs are done.

Install a worker on the hub with it, so the hub also runs jobs. Add
`--daily-use` if someone uses this computer, so jobs wait until they step away.

```sh
./myce install worker --joining-file invitation.json --nas NAS_PATH --dry-run
./myce install worker --joining-file invitation.json --nas NAS_PATH
```

The new worker finds the hub on the local network by itself. If it can't (for
example, across subnets), see [address and network behavior](guide-joining-v3.md#address-and-network-behavior).

## 4. Add each other machine

**Option A: one pasted line.** On the hub, or any machine that has joined, run:

```sh
./myce invite
```

It prints one line. Run that exact line in a terminal on the new machine. It
downloads and verifies `myce`, installs the worker and waits until the hub sees
it. The line has a one-time ticket that expires after 15 minutes
(`--expires` allows up to 1h). It needs no Go or Git on the new machine, and it
never contains the fleet token. If it fails with `release_unavailable`, the hub
has no approved signed release. Either set one up with the user
([publish and approve a release](guide-onboarding-releases.md)), or use option B.

**Option B: copy the invitation file.** This option needs no release. Copy
`invitation.json` and a `myce` binary for the new machine privately. Build the
binary on that machine, or cross-build it on the hub, for example
`GOOS=linux GOARCH=amd64 go build -o myce ./cmd/myce`. Build a Mac binary on a
Mac. On the new machine:

```sh
./myce install worker --joining-file invitation.json --nas NAS_PATH --dry-run
./myce install worker --joining-file invitation.json --nas NAS_PATH
rm invitation.json   # the installer keeps its own private copy
```

With either option, add `--daily-use` for computers someone uses, and
`--laptop` for laptops. With `--laptop`, the machine only works while plugged in.
**Check:** with option A, append these flags to the end of the pasted line only
if `myce install worker --help` on that machine lists them.

## 5. Verify

On the hub, with the credential loaded as in step 3:

```sh
./myce status
```

Every machine should appear with its GPUs, CPU and memory. A machine reporting
`ready` takes work now. `polite_waiting` means it's waiting for its user to step
away, which is also a success. `disabled` or `drained` means someone paused it
(`myce host resume HOST` turns it back on). See [status fields](guide-status-v3.md).

## 6. Run a first job

```sh
./myce run -- echo hello
./myce run --gpu cuda --mem 4G -- nvidia-smi
```

The first command runs on any free machine and prints `hello`. The second needs
an Nvidia GPU. Use `--gpu xpu` for Intel or `--gpu metal` for Apple, with a
command that exists there. `run` waits and streams the output. Add `--detach` to
get a job ID instead, then use `myce wait ID` and `myce logs ID`.

**Check:** `myce run` is new. If `myce run --help` is missing, use
`myce submit` with a YAML job as in the [laptop walkthrough](try-it-locally.md)
and the [job reference](job-reference.md).

## 7. Report back to the user

List each machine, its hardware and its state from `myce status`, plus the
output of the test jobs. Say where the credential file lives, but not its
contents. Mention anything you skipped or had to change. For upkeep (pausing a
machine, upgrades, uninstalling), point them to [operations](operations.md).
