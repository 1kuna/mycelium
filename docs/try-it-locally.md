# Try Mycelium on one laptop

`myce dev` runs the real Mycelium on a single machine with pretend GPUs, so you
can see how jobs flow before setting up real computers. To set up real machines,
see [agent setup](agent-setup.md); host maintenance is in [operations](operations.md).

## Five-minute start

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
Use a new key for new work. [Integration examples](integrating.md) cover
lost replies, cancellation, retention and owner switches. Ctrl-C stops dev;
its database, checkpoints and worker spools persist. Use a fresh isolated
working directory for a new independent demo.

## Models, files and workers

Call the fake model in dev:

```sh
curl "$MYCE_QUEUE/v1/chat/completions" \
  -H "Authorization: Bearer $MYCE_TOKEN" -H 'Content-Type: application/json' \
  -H 'X-Myce-Project: demo' -H 'X-Myce-Priority: experiment' \
  -d '{"model":"dev-model","messages":[{"role":"user","content":"Hello"}]}'
```

Real models use explicitly provisioned, verified content and declared environments.
[Model calls](guide-model-calls.md), [general servers](guide-servers.md),
[environments](environments.md), [files](guide-files-v3.md) and
[resumable commands](guide-resumable-jobs.md) document these paths.
[Joining](guide-joining-v3.md) discovers addresses using a private pinned
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
in the [native test plan](production-test-plan.md). Laptop success is not
deployment or native qualification.

To wrap an existing app, give your agent the [extension creation instructions](extensions/AGENT-GUIDE.md).
