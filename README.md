# Mycelium v3

One shared queue for GPU commands, OpenAI-format model calls and general HTTP
model servers. One Go binary, one SQLite database, plain-command workers and a
read-only status page. CPU callers and publication stay outside the queue.
The [approved design](docs/design-proposal.md) is the only product spec.

Start with [the integration guide](docs/START-HERE.md), [job/CLI reference](docs/job-reference.md)
or [operations](docs/operations.md). v3 includes authenticated joining, capability
profiles, qualified extensions, polite daily-use workers, measured placement,
verified file transport, software environments, pins and memory admission.
These are one runtime; no separate experimental branch is required.

## Five-minute laptop start

You need Go 1.25.6+ and a Unix shell. Fake GPUs provide inventory; commands really
execute. No GPU, Python, container or remote machine is needed. From the checkout:

```sh
go build -o myce ./cmd/myce
./myce dev --workers 2 --token dev-token
```

Dev is a long-running server at `http://127.0.0.1:7331`, with two 64-GiB fake
workers and a fake `dev-model`. Keep it running. In another terminal:

```sh
export MYCE_QUEUE=http://127.0.0.1:7331
export MYCE_TOKEN=dev-token
cat > job.yaml <<'YAML'
name: hello-queue
project: demo
priority: experiment
submission_key: demo-hello-1
run:
  command: [sh, -c, 'echo "job=$MYCE_JOB_ID attempt=$MYCE_ATTEMPT"; echo done']
needs:
  memory_gb: 1
  platform: any
resumable: false
YAML
id=$(./myce submit job.yaml)
./myce wait "$id" --timeout 30s
./myce logs "$id"
./myce status
```

Retrying this identical keyed submission returns the same job, including after
completion. A changed spec with that key returns `submission_key_conflict`.
Use a new key for new work. [Integration examples](docs/integrating.md) cover
lost replies, cancellation, retention and owner switches. Ctrl-C stops dev;
its database, checkpoints and worker spools persist. Use a fresh isolated
working directory for a new independent demo.

## Host ownership and installation

[Operations](docs/operations.md) is the canonical install/close/open/reconcile
runbook. `myce host close --local` persists whole-host CLOSED intent before
network access. `--wait-released` requires local exclusion PASS; full rest also
requires queue settlement. `host open --local --expect-revision REVISION`
requires the current confirmed close and allocates a new revision. An accepted
operation overtaken by another reports `revision_superseded`; a rejected
compare-and-set reports `revision_mismatch` with no accepted revision.

`host status --local --json` is read-only. Its [versioned envelope](docs/guide-status-v3.md#host-control-envelope)
separates desired state, current-boot observation, accepted revision, previous
close settlement and queue acknowledgement. `--expect closed|ready --verdict`
returns PASS, FAIL or UNKNOWN; exit zero means PASS only. Missing evidence stays
unknown and fresh contrary evidence wins. `host reconcile --local` retries the
same durable intent; the installed timer also runs while workers are stopped.

Root control uses a **separate root-owned** `/usr/local/libexec/myce/myce` copy
and `--worker-user WORKER_USER`. Never execute the worker-owned installed binary
as root. See [protected control](docs/operations.md#protected-root-owned-copy).

`myce install` reads saved config first and overlays only explicit flags, preserving
omitted settings and secret references. Workers install CLOSED without starting
or opening; failed publication rolls artifacts and manager state back, with an
explicit error if rollback is incomplete. Root control is deployed separately.
Every queue/worker/host state root carries `.myce-format` containing `v3` plus a
newline. Nonempty unmarked/old roots return `legacy_state_present`: stop old
services, archive whole data roots, preserve config/secrets, then install fresh.
There is no in-place conversion. See [fresh installation](docs/guide-fresh-install.md).

## Models, files and workers

Call the fake model in dev:

```sh
curl "$MYCE_QUEUE/v1/chat/completions" \
  -H "Authorization: Bearer $MYCE_TOKEN" -H 'Content-Type: application/json' \
  -H 'X-Myce-Project: demo' -H 'X-Myce-Priority: experiment' \
  -d '{"model":"dev-model","messages":[{"role":"user","content":"Hello"}]}'
```

Real models use explicitly provisioned, verified content and declared environments.
[Model calls](docs/guide-model-calls.md), [general servers](docs/guide-servers.md),
[environments](docs/environments.md), [files](docs/guide-files-v3.md) and
[resumable commands](docs/guide-resumable-jobs.md) document these paths.
[Joining](docs/guide-joining-v3.md) discovers addresses using a private pinned
invitation; discovery does not establish trust. Jobs never inherit `MYCE_TOKEN`.
Only run trusted commands: workers use the installed account's permissions.

## Build and verify

```sh
go build -o myce ./cmd/myce
go vet ./...
go test ./... -race -count=1 -timeout 30m
```

Serialize full race-suite runs. `./myce dev` starts the real API and command
runner with fake GPU inventory; it does not exit by itself. The executable
end-to-end tests exercise actual commands and checkpoint preemption/recovery.
Native GPU, daemon, systemd identity, boot and application checks remain separate
in the [native test plan](docs/production-test-plan.md). Laptop success is not
deployment or native qualification.
