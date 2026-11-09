package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Compare cold HTTP reads, including public projection and JSON, with real
// SQLite check-ins at the ordinary one-second heartbeat cadence. The fixed
// injected clock keeps elapsed-time policy separate from wall-time measurement.
func BenchmarkStatusReadScale(b *testing.B) {
	for _, large := range []bool{false, true} {
		for _, path := range []string{"/api/status", "/api/status?detail=full", "/api/inspection"} {
			b.Run(fmt.Sprintf("large=%v%s", large, path), func(b *testing.B) {
				q, workers := candidateCostFixture(b)
				if large {
					for i := 160; i < 6500; i++ {
						source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
						j := cloneJob(source)
						j.ID = fmt.Sprintf("retained-%04d", i)
						j.Timing.Execution.ID = j.ID
						q.s.Jobs[j.ID] = &j
					}
				} else {
					workers = workers[:3]
					for id, j := range q.s.Jobs {
						if j.State == "succeeded" && id > "retained-015" || j.State == "queued" && id > "queued-003" || id == "running-3" || id == "running-4" {
							delete(q.s.Jobs, id)
						}
					}
					for id := range q.s.Machines {
						if id == "worker-3" || id == "worker-4" {
							delete(q.s.Machines, id)
						}
					}
					for id := range q.s.WorkObservations {
						n, _ := strconv.Atoi(id)
						if n >= 16 {
							delete(q.s.WorkObservations, id)
						}
					}
				}
				if err := q.update(func() error { return nil }); err != nil {
					b.Fatal(err)
				}
				var mu sync.Mutex
				var checkinHolds []time.Duration
				var holds []time.Duration
				q.observeRead = func(kind string, _, hold time.Duration) {
					if kind == "status" {
						holds = append(holds, hold)
					}
				}
				q.observeUpdate = func(_, hold time.Duration) {
					mu.Lock()
					checkinHolds = append(checkinHolds, hold)
					mu.Unlock()
				}
				ctx, cancel := context.WithCancel(context.Background())
				var wg sync.WaitGroup
				for i, worker := range workers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						ticker := time.NewTicker(time.Second)
						defer ticker.Stop()
						for {
							_, err := q.CheckinContext(ctx, Checkin{Machine: worker, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: worker.Session, Started: true}}})
							if err != nil && ctx.Err() == nil {
								b.Error(err)
								return
							}
							select {
							case <-ctx.Done():
								return
							case <-ticker.C:
							}
						}
					}()
				}
				handler := API{Queue: q, Token: "test"}.Handler()
				var latencies []time.Duration
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					// Each iteration measures a refresh, not an injected-clock cache
					// hit. The production lifetime/invalidation behavior is unchanged.
					q.statusCache.mu.Lock()
					q.statusCache.expires = time.Time{}
					q.statusCache.mu.Unlock()
					span := &requestSpan{}
					r := httptest.NewRequest("GET", path, nil).WithContext(context.WithValue(ctx, requestSpanKey{}, span))
					r.Header.Set("Authorization", "Bearer test")
					w := httptest.NewRecorder()
					started := time.Now()
					handler.ServeHTTP(w, r)
					latencies = append(latencies, time.Since(started))
					if w.Code != 200 {
						b.Fatal(w.Code, w.Body.String())
					}
				}
				b.StopTimer()
				cancel()
				wg.Wait()
				report := func(values []time.Duration, name string) {
					sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
					if len(values) > 0 {
						b.ReportMetric(float64(values[(len(values)-1)/2].Microseconds())/1000, name+"-p50-ms")
						b.ReportMetric(float64(values[(99*len(values)+99)/100-1].Microseconds())/1000, name+"-p99-ms")
					}
				}
				report(latencies, "read")
				report(holds, "read-hold")
				report(checkinHolds, "checkin-hold")
			})
		}
	}
}

// A diagnostic read must use the same occupied CPU ledger as admission. Four
// admitted cores with three reserved cannot place the next two-core job.

func TestStatusReadTerminalCommitDuringProjection(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(strconv.FormatBool(cached), func(t *testing.T) {
			q, clock := fixture(t)
			j := submit(t, q, spec("experiment"))
			a := check(t, q, machine("status-worker", "fake"), nil).Job
			if a == nil || a.ID != j.ID {
				t.Fatal("no assignment")
			}
			if err := q.Started(result(a, true)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			reader, err := q.usageReader.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			captured := make(chan struct{})
			var captures atomic.Int32
			q.observeRead = func(kind string, _, _ time.Duration) {
				if kind == "status" && captures.Add(1) == 1 {
					close(captured)
				}
			}
			done := make(chan struct{})
			var got Snapshot
			go func() {
				defer close(done)
				if cached {
					got, err = q.cachedPublicSnapshot(ctx)
				} else {
					got, err = q.publicSnapshot(ctx)
				}
			}()
			select {
			case <-captured:
			case <-ctx.Done():
				t.Fatal("status never captured")
			}
			finish(t, q, a, true)
			// Reproduce the incident's 41 seconds with the injected clock while
			// the detached observation waits for usage I/O, outside q.mu.
			clock.Advance(41 * time.Second)
			reader.Close()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("status did not finish")
			}
			if err != nil || len(got.Jobs) != 1 || got.Jobs[0].State != "succeeded" || !got.Jobs[0].ReleaseConfirmed {
				t.Fatalf("delayed status returned pre-completion state: %+v, %v", got.Jobs, err)
			}
		})
	}
}

// Receipt polling must remain independent of both the usage reader and writer
// connection, and must preserve private payload redaction and exact key echo.

// A coalesced refresh can wait for its isolated usage snapshot while both start
// gates and cancellation continue. Its stale observation never authorizes work.

func TestStatusReadCoalescesAndDoesNotAuthorize(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("production"))
	a := check(t, q, machine("reader-test", "fake"), nil).Job
	if a == nil || a.ID != j.ID {
		t.Fatal("no assignment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reader, err := q.usageReader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	captured := make(chan struct{})
	var captures atomic.Int32
	q.observeRead = func(kind string, _, _ time.Duration) {
		if kind == "status" && captures.Add(1) == 1 {
			close(captured)
		}
	}
	type answer struct {
		s   Snapshot
		err error
	}
	answers := make(chan answer, 8)
	for range 8 {
		go func() { s, err := q.cachedPublicSnapshot(); answers <- answer{s, err} }()
	}
	select {
	case <-captured:
	case <-ctx.Done():
		t.Fatal("status never captured")
	}
	launch := HostLaunch{Kind: "batch", Key: a.ID, Machine: a.Machine, Session: a.Session, Attempt: strconv.Itoa(a.Attempt)}
	controls := make(chan error, 1)
	go func() {
		err := q.Started(result(a, true))
		if err == nil {
			err = q.AuthorizeLaunch(launch)
		}
		if err == nil {
			_, err = q.CancelJob(a.ID)
		}
		controls <- err
	}()
	select {
	case err := <-controls:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("usage observation blocked a control transaction")
	}
	reader.Close()
	for range 8 {
		select {
		case got := <-answers:
			if got.err != nil || len(got.s.Jobs) != 1 || got.s.Jobs[0].CancelledAt != nil {
				t.Fatal("refresh was not a single captured generation", got.err)
			}
			got.s.Jobs[0].Spec.Needs.Platform.Any = false
			got.s.PriorityPolicy.Policy.Tiers[0].Selectors[0] = "changed"
		case <-ctx.Done():
			t.Fatal("status refresh did not finish")
		}
	}
	if captures.Load() != 1 {
		t.Fatal("concurrent status reads repeated capture", captures.Load())
	}
	if err := q.Started(result(a, true)); !errors.Is(err, ErrStale) {
		t.Fatal("stale status authorized Started", err)
	}
	if err := q.AuthorizeLaunch(launch); err == nil {
		t.Fatal("stale status authorized launch")
	}
	fresh, err := q.jobSnapshot(a.ID)
	if err != nil || fresh.CancelledAt == nil {
		t.Fatal("job polling reused stale global status", err)
	}
	// A cache hit must work even while the actual queue is exclusively locked.
	q.mu.Lock()
	hit := make(chan answer, 1)
	go func() { s, err := q.cachedPublicSnapshot(); hit <- answer{s, err} }()
	var got answer
	select {
	case got = <-hit:
	case <-ctx.Done():
		q.mu.Unlock()
		t.Fatal("cache hit acquired q.mu")
	}
	q.mu.Unlock()
	if got.err != nil || !got.s.Jobs[0].Spec.Needs.Platform.Any || got.s.PriorityPolicy.Policy.Tiers[0].Selectors[0] == "changed" {
		t.Fatal("caller mutated the cached generation")
	}
	c.Advance(time.Second)
	snap, err := q.cachedPublicSnapshot()
	if err != nil || snap.Jobs[0].CancelledAt == nil || captures.Load() != 2 {
		t.Fatal("injected-clock expiry did not refresh status", err)
	}
}
