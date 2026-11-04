package myce

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"
)

// Use retained executions with real provenance, not empty terminal receipts.
// Queue decisions use the fixture's injected clock; SQLite commits are included.
func queueRecordsCostFixture(b testing.TB) (*Queue, []Machine) {
	b.Helper()
	q, workers := candidateCostFixture(b)
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
	return q, workers
}

func benchmarkCPU() int64 {
	var r syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return int64(r.Utime.Sec+r.Stime.Sec)*1e9 + int64(r.Utime.Usec+r.Stime.Usec)*1000
}

func BenchmarkQueueRecordsFleetCheckin(b *testing.B) {
	q, workers := queueRecordsCostFixture(b)
	var holds time.Duration
	q.observeUpdate = func(_, hold time.Duration) { holds += hold }
	b.ReportAllocs()
	b.ResetTimer()
	cpu := benchmarkCPU()
	for range b.N {
		m := workers[0]
		_, err := q.CheckinContext(context.Background(), Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: "running-0", Attempt: 1}, Session: m.Session, Started: true}}})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(benchmarkCPU()-cpu)/float64(b.N), "cpu-ns/op")
	b.ReportMetric(float64(holds.Nanoseconds())/float64(b.N), "hold-ns/op")
}

func BenchmarkQueueCatalogTransaction(b *testing.B) {
	q, _ := queueRecordsCostFixture(b)
	var holds time.Duration
	q.observeUpdate = func(_, hold time.Duration) { holds += hold }
	b.ReportAllocs()
	b.ResetTimer()
	cpu := benchmarkCPU()
	for range b.N {
		if err := q.update(func() error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(benchmarkCPU()-cpu)/float64(b.N), "cpu-ns/op")
	b.ReportMetric(float64(holds.Nanoseconds())/float64(b.N), "hold-ns/op")
}
