# Release-format conversion fixtures

These are synthetic data, generated using the writers at `7285a88`. No fleet
state, credentials, host addresses, or private paths were read. The SQLite
schema and state/report JSON encodings come from that release's actual Go code.
`release-fixture-provenance.json` pins the commit, writer hashes and file hashes.

`release-7285a88` contains 103 jobs (102 queued, one terminal with outstanding
custody), 206 copies, a release charge, a loading model slot, a pin, usage/log
rows, a saved worker result, a pending Docker create, an engine process record,
a sequenced checkpoint and a check-in report. Nonzero retired enable/memory
counters and production-idle fields exercise their archival conversion. Host
receipts and registry records carry exact outstanding custody.
The smaller `release-7285a88-dev` fixture has two jobs and no queue model/release
obligations, so the real dev API/worker can execute its queued command and
restart without terminal replay. Its checkpoint-root placeholder is bound to
an isolated destination by the test before conversion. This changes fixture
values, never their release encoding.

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
