//go:build !race

package myce

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Replay the observed mix: retained history, queued receipts, uncached status,
// and real SQLite check-ins. Queue decisions use a fixed injected clock, so the
// test proves the mix completes without errors or interrupting production.
func TestStatusReadCheckinContention(t *testing.T) {
	statusReadCheckinContention(t)
}

// Latency acceptance for the same mix: -run '^$' -bench StatusReadCheckinContention
// -benchtime 1x on an otherwise idle host. The bound lives here because shared
// fleet load alone exceeded it in coverage runs on unchanged main.
func BenchmarkStatusReadCheckinContention(b *testing.B) {
	for range b.N {
		p99 := statusReadCheckinContention(b)
		b.ReportMetric(float64(p99.Microseconds())/1000, "checkin-p99-ms")
		if p99 >= 500*time.Millisecond {
			b.Errorf("check-in p99 %s exceeds half the 1s heartbeat interval", p99)
		}
	}
}

func statusReadCheckinContention(t testing.TB) time.Duration {
	t.Helper()
	q, clock := fixture(t)
	m := machine("contact", "fake")
	check(t, q, m, nil)
	sentinel := submit(t, q, spec("production"))
	assignment := check(t, q, m, nil).Job
	if assignment == nil || assignment.ID != sentinel.ID {
		t.Fatal("production sentinel was not assigned")
	}
	if err := q.Started(result(assignment, true)); err != nil {
		t.Fatal(err)
	}
	// The running attempt occupies this worker's capacity while receipts queue.
	m.FreeMemoryGB = 0
	contact := Checkin{Machine: m, Running: &Execution{ID: sentinel.ID, Attempt: assignment.Attempt}}
	// Earlier transactions can still publish admission evidence in the
	// background. Protect the bulk fixture writes with the same queue mutex.
	q.mu.Lock()
	for i := range 5000 {
		id := fmt.Sprintf("history-%04d", i)
		j := &Job{ID: id, State: "succeeded", Queued: clock.Now().Add(-time.Hour), Attempt: 1, ReleaseConfirmed: true, Spec: spec("experiment")}
		if i < 400 {
			j.Queued = clock.Now().Add(-time.Minute)
			j.Spec.FanOut = &FanOut{}
			j.FanOut = &FanOutProgress{}
			for k := range 32 {
				j.Spec.FanOut.Pieces = append(j.Spec.FanOut.Pieces, FanOutPiece{Key: fmt.Sprint(k)})
				j.FanOut.Children = append(j.FanOut.Children, fmt.Sprintf("history-%04d", 400+i*8+k%8))
			}
		}
		q.s.Jobs[id] = j
	}
	for i := range 200 {
		id := fmt.Sprintf("queued-%04d", i)
		s := spec("experiment")
		s.Run.Command = []string{"echo", strings.Repeat("payload", 256)}
		j := &Job{ID: id, State: "queued", Queued: clock.Now(), Spec: s}
		q.s.Jobs[id] = j
	}
	q.s.WorkObservations = map[string]WorkObservation{}
	for i := range 1000 {
		q.s.WorkObservations[fmt.Sprint(i)] = WorkObservation{Key: ObservationKey{Work: "work", Hardware: strings.Repeat("hardware", 16), Configuration: strings.Repeat("config", 16)}, Samples: 10, LastSeconds: 20, RecordedAt: clock.Now()}
	}
	q.mu.Unlock()
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	const readers = 62
	errors := make(chan error, readers+1)
	var lockedReads atomic.Int64
	ready := make(chan struct{}, readers)
	for i := range readers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ready <- struct{}{}
			for ctx.Err() == nil {
				var err error
				if i == 0 {
					_, err = q.statusSnapshotOptions(&statusOptions{jobs: "recent", limit: 100})
				} else {
					span := &requestSpan{}
					_, err = q.jobSnapshot(fmt.Sprintf("queued-%04d", i), context.WithValue(context.Background(), requestSpanKey{}, span))
					span.mu.Lock()
					if span.measured {
						lockedReads.Add(1)
					}
					span.mu.Unlock()
				}
				if err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}
	for range readers {
		<-ready
	}
	latencies := make([]time.Duration, 100)
	for i := range latencies {
		start := time.Now()
		_, err := q.Checkin(contact)
		latencies[i] = time.Since(start)
		if err != nil {
			errors <- err
			break
		}
	}
	cancel()
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if got := q.s.Jobs[sentinel.ID]; got.State != "running" || got.Attempt != assignment.Attempt || got.Losses != 0 {
		t.Error("observation load interrupted the production sentinel")
	}
	// Structural, load-independent: job reads use the published snapshot and
	// never take a q.mu turn while check-ins commit (#80-class stalls).
	if n := lockedReads.Load(); n != 0 {
		t.Errorf("%d job reads took a queue-mutex turn during check-in contention", n)
	}
	p99 := latencies[98]
	t.Logf("100 check-ins with 62 concurrent observation readers: p99=%s max=%s", p99, latencies[99])
	return p99
}
