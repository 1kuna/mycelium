//go:build !race

package myce

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

// The #1030 retained-history incident shape, with real SQLite FULL commits.
func BenchmarkCheckinPersistence(b *testing.B) {
	b.Run("retained", func(b *testing.B) { benchmarkCheckinPersistence(b, false) })
	b.Run("manifests", func(b *testing.B) { benchmarkCheckinPersistence(b, true) })
}

func benchmarkCheckinPersistence(b *testing.B, manifests bool) {
	q, workers := candidateCostFixture(b)
	admissionWaitBenchmarkSettle(q)
	clock := q.clock.(*workerFakeClock)
	if manifests {
		// The #661 fixture used 768 input references per job. Add them only to
		// occupied work here, isolating unchanged payload writes from queued planning.
		files := FileManifest{}
		for i := range 768 {
			files[fmt.Sprintf("input-%04d", i)] = FileRef{SHA256: fmt.Sprintf("%064x", i), SizeBytes: 1}
		}
		for i := range workers {
			q.s.Jobs[fmt.Sprintf("running-%d", i)].Spec.Files = files
		}
	}
	for i := 160; i < 6500; i++ {
		source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j := *source
		j.ID = fmt.Sprintf("retained-%04d", i)
		timing := *source.Timing
		timing.Execution.ID = j.ID
		j.Timing = &timing
		q.s.Jobs[j.ID] = &j
	}
	if err := q.update(func() error { return nil }); err != nil {
		b.Fatal(err)
	}
	admissionWaitBenchmarkSettle(q)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(q.persisted["state"], &fields); err != nil {
		b.Fatal(err)
	}
	for k, v := range fields {
		if len(v) >= 1024 {
			b.Logf("base field %s: %d bytes", k, len(v))
		}
	}
	// Prevent recycling/truncation while measuring WAL growth. This changes no
	// durability setting. Restoring/closing this private fixture checkpoints it.
	if _, err := q.db.Exec("PRAGMA wal_autocheckpoint=0; PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		b.Fatal(err)
	}
	var dbPath string
	var seq int
	var name string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &dbPath); err != nil {
		b.Fatal(err)
	}
	var sum queuePersistenceTiming
	var phases [5]time.Duration
	q.observePersistence = func(p queuePersistenceTiming) {
		sum.Marshal += p.Marshal
		sum.SQL += p.SQL
		sum.Commit += p.Commit
		sum.Bytes += p.Bytes
		sum.Records += p.Records
	}
	q.observePersistenceHold = func(p queueHoldPhases) {
		for i, v := range p.phases {
			phases[i] += v
		}
	}
	cpu := benchmarkCPU()
	b.ResetTimer()
	for i := range b.N {
		clock.Advance(time.Second)
		m := workers[i%len(workers)]
		_, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i%len(workers)), Attempt: 1}, Session: m.Session, Started: true}}})
		if err != nil {
			b.Fatal(err)
		}
	}
	admissionWaitBenchmarkSettle(q)
	b.StopTimer()
	usedCPU := benchmarkCPU() - cpu
	info, err := os.Stat(dbPath + "-wal")
	if err != nil {
		b.Fatal(err)
	}
	n := float64(b.N)
	b.ReportMetric(float64(usedCPU)/1e6/n, "CPU-ms/checkin")
	b.ReportMetric(float64(info.Size()-32)/n, "WAL-B/checkin")
	b.ReportMetric(float64(sum.Bytes)/n, "payload-B/checkin")
	b.ReportMetric(float64(sum.Records)/n, "records/checkin")
	b.ReportMetric(holdMS(sum.Marshal)/n, "marshal-ms/checkin")
	b.ReportMetric(holdMS(sum.SQL)/n, "SQL-ms/checkin")
	b.ReportMetric(holdMS(sum.Commit)/n, "commit-ms/checkin")
	var total time.Duration
	for _, v := range phases {
		total += v
	}
	b.ReportMetric(holdMS(total)/n, "hold-ms/checkin")
	b.ReportMetric(holdMS(phases[holdBody])/n, "body-ms/checkin")
	b.ReportMetric(holdMS(phases[holdRecords])/n, "snapshot-ms/checkin")
	b.ReportMetric(holdMS(phases[holdPersist])/n, "persist-ms/checkin")
}
