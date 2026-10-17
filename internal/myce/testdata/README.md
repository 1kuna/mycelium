`submission-legacy.db` was written by `Queue.Open` and `Queue.update` from
commit `8bc2a83acd08f9687d03a20a29e423cc1918f0b8`, before submission keys.
It contains one synthetic queued job, no machines, and no submission records.

To regenerate from the repository root (no branch change):

```sh
mkdir -p .reports/s02-legacy-source
git archive 8bc2a83acd08f9687d03a20a29e423cc1918f0b8 | tar -x -C .reports/s02-legacy-source
cp internal/myce/testdata/submission_fixture_test.go.txt .reports/s02-legacy-source/internal/myce/submission_fixture_test.go
(cd .reports/s02-legacy-source && go test ./internal/myce -run '^TestWriteSubmissionLegacyFixture$' -count=1)
```

The generator closes SQLite before copying the database, so the fixture needs
no WAL file. Its only checkpoint path is a synthetic relative path.
