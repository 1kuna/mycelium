//go:build !race

package myce

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Replay the observed mix: retained history, queued receipts, uncached status,
// and real SQLite check-ins. Wall time measures latency only; queue decisions
// use a fixed injected clock. Run separately from race instrumentation.
func TestStatusReadCheckinContention(t *testing.T) {
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
		q.s.WorkObservations[fmt.Sprint(i)] = WorkObservation{Key: ObservationKey{Work: "work", Hardware: strings.Repeat("hardware", 16), Configuration: strings.Repeat("config", 16)}, Samples: 10, Units: 100, Seconds: 20, RecordedAt: clock.Now()}
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	const readers = 62
	errors := make(chan error, readers+1)
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
					_, err = q.jobSnapshot(fmt.Sprintf("queued-%04d", i))
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
	p99 := latencies[98]
	t.Logf("100 check-ins with 62 concurrent observation readers: p99=%s max=%s", p99, latencies[99])
	if p99 >= 500*time.Millisecond {
		t.Errorf("check-in p99 %s exceeds half the 1s heartbeat interval", p99)
	}
}
