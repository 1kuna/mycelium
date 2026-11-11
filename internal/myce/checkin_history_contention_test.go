//go:build !race

package myce

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Unlike empty terminal receipts, retained constrained executions participate in
// cold-memory planning. Keep their real timing/configuration provenance.
// The test proves concurrent check-ins and job reads over retained history
// complete without errors or interrupting running attempts.
func TestCheckinRetainedHistoryContention(t *testing.T) {
	checkinRetainedHistoryContention(t)
}

// Hold-time acceptance: -run '^$' -bench CheckinRetainedHistoryContention
// -benchtime 1x on an otherwise idle host. Shared fleet load alone pushed the
// all-hold p99 to 835 ms on unchanged main, so the bound lives here.
func BenchmarkCheckinRetainedHistoryContention(b *testing.B) {
	for range b.N {
		a99 := checkinRetainedHistoryContention(b)
		b.ReportMetric(float64(a99.Microseconds())/1000, "all-hold-p99-ms")
		if a99 >= 100*time.Millisecond {
			b.Errorf("check-in all-hold p99 %s exceeds 100ms", a99)
		}
	}
}

func checkinRetainedHistoryContention(t testing.TB) time.Duration {
	t.Helper()
	q, workers := candidateCostFixture(t)
	clock := &workerFakeClock{now: q.clock.Now()}
	q.clock = clock
	m := cpuMachine("worker-5", "worker-5", "cpu", 16, 128)
	m.ExtensionFingerprint, m.CPUExtensionFingerprint = "synthetic-cpu", "synthetic-cpu"
	m.Devices = []MachineDevice{}
	m.LastSeen = clock.Now()
	q.s.Machines[m.ID] = &m
	workers = append(workers, m)
	now := clock.Now()
	q.s.Jobs["running-5"] = &Job{ID: "running-5", Spec: q.s.Jobs["running-1"].Spec, State: "running", Attempt: 1, Machine: m.ID, Session: m.Session, StartedAt: &now, Queued: now}
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
		t.Fatal(err)
	}
	var mu sync.Mutex
	var holds, allHolds, getWaits []time.Duration
	q.observeUpdate = func(_, hold time.Duration) {
		mu.Lock()
		holds = append(holds, hold)
		mu.Unlock()
	}
	var before, after syscall.Rusage
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		clock.Advance(time.Second)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i, worker := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				span := &requestSpan{}
				ctx := context.WithValue(context.Background(), requestSpanKey{}, span)
				_, err := q.CheckinContext(ctx, Checkin{Machine: worker, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: worker.Session, Started: true}}})
				if err != nil {
					t.Error(err)
				}
				mu.Lock()
				allHolds = append(allHolds, span.hold)
				mu.Unlock()
			}()
		}
		for i := range 60 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				span := &requestSpan{}
				ctx := context.WithValue(context.Background(), requestSpanKey{}, span)
				if _, err := q.jobSnapshot(fmt.Sprintf("queued-%03d", i%30), ctx); err != nil {
					t.Error(err)
				}
				mu.Lock()
				getWaits = append(getWaits, span.wait)
				mu.Unlock()
			}()
		}
		close(start)
		wg.Wait()
	}
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&memAfter)
	percentiles := func(values []time.Duration) (time.Duration, time.Duration) {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		return values[(len(values)-1)/2], values[(99*len(values)+99)/100-1]
	}
	p50, p99 := percentiles(holds)
	a50, a99 := percentiles(allHolds)
	g50, g99 := percentiles(getWaits)
	cpuUS := func(r syscall.Rusage) int64 {
		return int64(r.Utime.Sec+r.Stime.Sec)*1e6 + int64(r.Utime.Usec+r.Stime.Usec)
	}
	t.Logf("120 check-ins, six workers, 6500 retained executions, 110 queued, 1400 curves, 1200 concurrent job GETs: update hold p50/p99=%s/%s; all hold p50/p99=%s/%s; GET wait p50/p99=%s/%s; process CPU/check-in=%.3fms", p50, p99, a50, a99, g50, g99, float64(cpuUS(after)-cpuUS(before))/120/1000)
	t.Logf("process allocation/check-in=%.3f MB, %.0f allocations", float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/120/1e6, float64(memAfter.Mallocs-memBefore.Mallocs)/120)
	for i := range workers {
		j := q.s.Jobs[fmt.Sprintf("running-%d", i)]
		if j.State != "running" || j.Attempt != 1 || j.Losses != 0 {
			t.Errorf("contention interrupted running attempt: %s %s attempt=%d losses=%d", j.ID, j.State, j.Attempt, j.Losses)
		}
	}
	return a99
}
