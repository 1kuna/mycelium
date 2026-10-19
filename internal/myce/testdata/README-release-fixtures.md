# Release-format refusal fixtures

These are synthetic data, generated using the writers at `7285a88`. No fleet
state, credentials, host addresses, or private paths were read. The SQLite
schema and state/report JSON encodings come from that release's actual Go code.
`release-fixture-provenance.json` pins the commit, writer hashes and file hashes.

`release-7285a88` contains 103 jobs (102 queued, one terminal with outstanding
custody), 206 copies, a release charge, a loading model slot, a pin, usage/log
rows, a saved worker result, a pending Docker create, an engine process record,
a sequenced checkpoint and a check-in report. Nonzero retired enable/memory
counters and production-idle fields exercise their refusal. Host
receipts carry exact outstanding custody. The registry is written by the actual
RC `Worker.registerHost` method and deliberately has no Session field. Its bound
spool and the independently saved receipt supply session evidence.
`release-7285a88-native` retains the same queue/native custody without the pending
Docker create, to cover refusal of native custody.
`release-7285a88-idle` is written through the same RC writers with no saved
execution/copy/model custody; refusal must still reject its schema-less queue, worker and missing-Session registry.
The smaller `release-7285a88-dev` fixture has two jobs and no queue model/release
obligations, to verify that even small dev state is refused. Refusal never
rewrites fixture roots or other release values.

Reproduce in new, project-local ignored directories (Python 3.14 and Go):

```sh
python3.14 internal/myce/testdata/generate-release-fixtures.py \
  --scratch .reports/wp8/reproduce-writer --output .reports/wp8/reproduced
```

The generator uses `git archive 7285a88` for the isolated old package and verifies
its key writers against `git show 7285a88:<file>` before compiling the fixture
writer. The Go fixture source is retained as `release-writer_test.go.txt`, not
compiled into the current runtime. Logical state is deterministic; SQLite page
layout may vary with the toolchain. The checked-in file hashes are authoritative.

For an actual RC installer layout in a new disposable HOME, add
`--install-home .reports/wp8/reproduced-home`. The additional test-only input
`release-install-writer_test.go.txt` calls the real RC `PlanInstall`,
`WorkerInstance`, `Install`, and `registerHost` for queue plus gpu0/gpu1, using
synthetic credentials and a synthetic executable. The current CLI integration
test moves the whole data directories aside as the operator, then installs fresh.
The old runtime is never compiled into the current binary.
