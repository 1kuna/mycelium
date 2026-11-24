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
	statusReadCheckinContention(t, false)
}

// Latency acceptance for the same mix: -run '^$' -bench StatusReadCheckinContention
// -benchtime 1x on an otherwise idle host. The bound lives here because shared
// fleet load alone exceeded it in coverage runs on unchanged main.
func BenchmarkStatusReadCheckinContention(b *testing.B) {
	for range b.N {
		p99 := contentionP99(statusReadCheckinContention(b, false)["checkin"])
		b.ReportMetric(float64(p99.Microseconds())/1000, "checkin-p99-ms")
		if p99 >= 500*time.Millisecond {
			b.Errorf("check-in p99 %s exceeds half the 1s heartbeat interval", p99)
		}
	}
}

// Use the same fixture to compare check-ins, submits and both status read sizes
// on the same host and toolchain. This run adds a concurrent submitting client.
func BenchmarkStatusReadControlContention(b *testing.B) {
	for range b.N {
		samples := statusReadCheckinContention(b, true)
		for name, durations := range samples {
			if len(durations) == 0 {
				b.Errorf("no %s latency samples", name)
				continue
			}
			b.ReportMetric(float64(contentionP99(durations).Microseconds())/1000, name+"-p99-ms")
			b.ReportMetric(float64(len(durations)), name+"-samples")
		}
	}
}

func contentionP99(samples []time.Duration) time.Duration {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[(99*len(samples)+99)/100-1]
}

func statusReadCheckinContention(t testing.TB, measureSubmit bool) map[string][]time.Duration {
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
	errors := make(chan error, readers+2)
	var status, compact, submits []time.Duration
	var lockedReads atomic.Int64
	ready := make(chan struct{}, readers)
	for i := range readers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ready <- struct{}{}
			for sample := 0; ctx.Err() == nil; sample++ {
				var err error
				if i == 0 {
					start := time.Now()
					if measureSubmit && sample%2 == 1 {
						_, err = q.statusSnapshotOptions(&statusOptions{jobs: "active"})
						compact = append(compact, time.Since(start))
					} else {
						_, err = q.statusSnapshotOptions(&statusOptions{jobs: "recent", limit: 100})
						status = append(status, time.Since(start))
					}
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
	var submitDone sync.WaitGroup
	if measureSubmit {
		submitDone.Go(func() {
			for i := range 100 {
				s := spec("experiment")
				s.SubmissionKey = fmt.Sprintf("contention-submit-%d", i)
				start := time.Now()
				if _, err := q.Submit(s); err != nil {
					errors <- err
					return
				}
				submits = append(submits, time.Since(start))
			}
		})
	}
	checkins := make([]time.Duration, 0, 100)
	for range 100 {
		start := time.Now()
		_, err := q.Checkin(contact)
		if err != nil {
			errors <- err
			break
		}
		checkins = append(checkins, time.Since(start))
	}
	submitDone.Wait()
	cancel()
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if got := q.s.Jobs[sentinel.ID]; got.State != "running" || got.Attempt != assignment.Attempt || got.Losses != 0 {
		t.Error("observation load interrupted the production sentinel")
	}
	// Structural, load-independent: job reads use the published snapshot and
	// never take a q.mu turn while check-ins commit (#80-class stalls).
	if n := lockedReads.Load(); n != 0 {
		t.Errorf("%d job reads took a queue-mutex turn during check-in contention", n)
	}
	if len(checkins) > 0 {
		p99 := contentionP99(checkins)
		t.Logf("%d check-ins with 62 concurrent observation readers: p99=%s max=%s", len(checkins), p99, checkins[len(checkins)-1])
	}
	return map[string][]time.Duration{"checkin": checkins, "submit": submits, "status": status, "compact": compact}
}
