package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"slices"
	"sync"
	"testing"
	"time"
)

// A synthetic 225-job load. Production is eligible on four occupied
// workers, and a previously preempted experiment checks for a production gap
// on the fifth. Relative and absolute windows exercise both deadline paths.
func checkinMaintenanceDeepBacklog(b *testing.B) (*Queue, []Machine) {
	q, workers := candidateCostFixture(b)
	clock := q.clock.(*fakeClock)
	if err := q.update(func() error {
		for i := range workers {
			m := q.touchMachine(workers[i].ID)
			m.Capabilities = append(m.Capabilities, pooledMemoryCapability, sampledMemoryCapability)
			workers[i].Capabilities = m.Capabilities
		}
		// A large same-tier holder leaves a small CPU/RAM margin. The queued
		// head can use the host when it releases and fences the margin meanwhile.
		holder := q.touchJob("running-0")
		holder.Spec.Needs.CPUCores = 12
		holder.Spec.Needs.HostMemoryGB = 8
		for i := 0; i < 220; i++ {
			id := fmt.Sprintf("queued-%03d", i)
			if i >= 110 {
				job := *q.s.Jobs[fmt.Sprintf("queued-%03d", i-110)]
				job.ID = id
				q.putJob(id, &job)
			}
			job := q.touchJob(id)
			if job.Spec.Priority == "production" {
				job.Spec.ExcludedMachines = []string{workers[4].ID}
				if i%4 == 0 {
					deadline, explicit := clock.Now().Add(time.Hour), true
					job.Spec.StartBy, job.StartByExplicit = &deadline, &explicit
				}
			} else if i == 95 {
				job.ProductionBackoff = map[string]ProductionBackoff{hostID(&workers[4]): {LastAt: clock.Now()}}
			}
		}
		head := q.touchJob("queued-000")
		head.Spec.Needs.HostMemoryGB = 28
		head.Spec.Needs.CPUCores = 8
		head.Spec.AllowedMachines = []string{workers[0].ID}
		deadline := clock.Now()
		head.Spec.Deadline = &deadline
		head.PreemptionPlacement = &PreemptionPlacement{Machine: workers[0].ID, Session: workers[0].Session, Device: "0"}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return q, workers
}

func backlogCheckin(i int, m Machine) Checkin {
	return Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol,
		Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true}}}
}

func BenchmarkCheckinMaintenanceDeepBacklog(b *testing.B) {
	q, workers := checkinMaintenanceDeepBacklog(b)
	clock := q.clock.(*fakeClock)
	checkin := func(ctx context.Context, i int) {
		r, err := q.CheckinContext(ctx, backlogCheckin(i, workers[i]))
		if err != nil || r.Job != nil || r.Stop {
			b.Fatalf("check-in changed custody on worker %d: job=%v stop=%v err=%v", i, r.Job, r.Stop, err)
		}
		for _, attempt := range r.Attempts {
			if attempt.Stop || attempt.Job != nil && attempt.ID != fmt.Sprintf("running-%d", i) {
				b.Fatalf("check-in changed custody on worker %d: %+v", i, attempt)
			}
		}
	}
	for i := range workers {
		checkin(context.Background(), i)
	}
	var maintenance time.Duration
	q.observeBodyPhase = func(name string, elapsed time.Duration) {
		if name == "maintain" {
			maintenance += elapsed
		}
	}
	holds := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	pprof.Do(context.Background(), pprof.Labels("work", "deep-backlog-checkin"), func(ctx context.Context) {
		for n := range b.N {
			i := n % len(workers)
			if i == 0 {
				clock.Advance(time.Second)
			}
			var span requestSpan
			checkin(context.WithValue(ctx, requestSpanKey{}, &span), i)
			holds = append(holds, span.hold)
		}
	})
	b.StopTimer()
	slices.Sort(holds)
	median := (holds[(len(holds)-1)/2] + holds[len(holds)/2]) / 2
	b.ReportMetric(holdMS(median), "median-hold-ms/checkin")
	b.ReportMetric(holdMS(maintenance)/float64(b.N), "mean-maintenance-ms/checkin")
}

// Every burst queues the same five heartbeats and four exact-attempt control
// requests behind one held mutex. SQLite and the HTTP handlers are real. The
// queue clock advances between bursts and remains fixed during each burst.
func BenchmarkQueueControlDeepBacklog(b *testing.B) {
	q, workers := checkinMaintenanceDeepBacklog(b)
	clock := q.clock.(*fakeClock)
	for i, worker := range workers {
		if _, err := q.CheckinContext(context.Background(), backlogCheckin(i, worker)); err != nil {
			b.Fatal(err)
		}
	}
	active := Result{Execution: Execution{ID: "running-1", Attempt: 1}, Machine: workers[1].ID, Session: workers[1].Session}
	released := active
	released.ID, released.ReleaseOnly = "publishing", true
	if err := q.update(func() error {
		j := *q.s.Jobs[active.ID]
		j.ID, j.ReleaseConfirmed = released.ID, true
		q.putJob(j.ID, &j)
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	type request struct {
		route string
		body  []byte
	}
	var requests []request
	add := func(route string, value any) {
		body, err := json.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		requests = append(requests, request{route, body})
	}
	for i, worker := range workers {
		add("checkin", backlogCheckin(i, worker))
	}
	add("launch", HostLaunch{Machine: active.Machine, Session: active.Session, Kind: "batch", Key: active.ID, Attempt: "1"})
	add("native-custody", NativeCustodyReport{Execution: active.Execution, Machine: active.Machine, Session: active.Session, ProcessID: "process", CustodyID: "custody", Phase: "reserved"})
	add("started", active)
	add("complete", released)
	handler := API{Queue: q, Token: "fixture"}.Handler()
	send := func(item request) error {
		r := httptest.NewRequest(http.MethodPost, "/api/workers/"+item.route, bytes.NewReader(item.body))
		r.Header.Set("Authorization", "Bearer fixture")
		r.Header.Set("X-Myce-Start-Gate", startGateProtocol)
		r.Header.Set(checkinPeriodHeader, time.Second.String())
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			return fmt.Errorf("%s returned %d: %s", item.route, w.Code, w.Body.String())
		}
		return nil
	}
	for _, item := range requests {
		if err := send(item); err != nil {
			b.Fatal(err)
		}
	}
	type samples struct{ wait, hold []time.Duration }
	byRoute := map[string]samples{}
	var mu sync.Mutex
	q.observeRequest = func(route string, _, wait, hold, _ time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		v := byRoute[route]
		v.wait = append(v.wait, wait)
		v.hold = append(v.hold, hold)
		byRoute[route] = v
	}
	b.ResetTimer()
	for range b.N {
		clock.Advance(time.Second)
		var burst sync.WaitGroup
		start := make(chan struct{})
		for _, item := range requests {
			burst.Go(func() {
				<-start
				if err := send(item); err != nil {
					b.Error(err)
				}
			})
		}
		func() {
			q.mu.Lock()
			defer q.mu.Unlock()
			close(start)
			await(b, func() bool { return q.mu.waiting.Load() >= int64(len(requests)) })
		}()
		burst.Wait()
	}
	b.StopTimer()
	for _, item := range requests[len(workers)-1:] {
		v := byRoute["POST /api/workers/"+item.route]
		if len(v.wait) == 0 {
			b.Fatalf("no mutex samples for %s", item.route)
		}
		reportRoutePercentiles(b, item.route+"-wait", v.wait)
		reportRoutePercentiles(b, item.route+"-hold", v.hold)
	}
}

func reportRoutePercentiles(b *testing.B, name string, samples []time.Duration) {
	slices.Sort(samples)
	b.ReportMetric(holdMS(samples[(len(samples)+1)/2-1]), name+"-p50-ms")
	b.ReportMetric(holdMS(samples[(len(samples)*95+99)/100-1]), name+"-p95-ms")
}
