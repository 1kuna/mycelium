package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStatusReadHistoryGeneration(t *testing.T) {
	q, clock := fixture(t)
	peak := 3.0
	j := &Job{ID: "retained", State: "succeeded", ReleaseConfirmed: true,
		Spec: spec("experiment"), Queued: clock.Now(), Attempt: 1,
		MemoryAttempts: []AttemptMemoryFacts{{MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}}}}
	if err := q.update(func() error { q.putJob(j.ID, j); return nil }); err != nil {
		t.Fatal(err)
	}
	indexed, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	// The incremental path must preserve every projected field of the original
	// fresh capture, not just the commonly displayed lifecycle fields.
	q.mu.Lock()
	history := q.records.statusHistory
	q.records.statusHistory = nil
	q.mu.Unlock()
	uncached, err := q.publicSnapshot()
	if err != nil || !reflect.DeepEqual(indexed, uncached) {
		t.Fatal("incremental history changed the full response", err)
	}
	q.mu.Lock()
	q.records.statusHistory = history
	q.mu.Unlock()
	// Commit after capture/unlock, before the borrowed immutable history is
	// copied. An in-flight generation must own the old nested memory sample.
	q.observeRead = func(_ string, _, _ time.Duration) {
		if err := q.update(func() error {
			live := q.touchJob(j.ID)
			*live.MemoryAttempts[0].MemoryPeak.HostGB = 9
			live.Reason = "committed change"
			return nil
		}); err != nil {
			t.Error(err)
		}
	}
	view, _, err := q.captureObservation("")
	q.observeRead = nil
	if err != nil || *view.s.Jobs[j.ID].MemoryAttempts[0].MemoryPeak.HostGB != 3 {
		t.Fatal("committed change leaked into an in-flight history generation", err)
	}
	current, err := q.publicSnapshot()
	if err != nil || current.Jobs[0].Reason != "committed change" || *current.Jobs[0].MemoryAttempts[0].MemoryPeak.HostGB != 9 {
		t.Fatal("inspection did not observe committed cold-history change", err)
	}
	rolledBack := errors.New("rollback")
	if err := q.update(func() error {
		q.touchJob(j.ID).Reason = "must roll back"
		return rolledBack
	}); !errors.Is(err, rolledBack) {
		t.Fatal(err)
	}
	current, err = q.publicSnapshot()
	if err != nil || current.Jobs[0].Reason != "committed change" {
		t.Fatal("rolled-back history escaped into inspection", err)
	}
	if err := q.update(func() error { q.deleteJob(j.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	current, err = q.publicSnapshot()
	if err != nil || len(current.Jobs) != 0 {
		t.Fatal("removed history remained in inspection", err)
	}
}

func TestStatusReadPlanningEquivalence(t *testing.T) {
	q, _ := candidateCostFixture(t)
	// Keep both CPU and accelerator demand, constrained execution history,
	// occupied workers and learned curves; a smaller backlog is sufficient to
	// compare every response field against the original unindexed projection.
	for id, job := range q.s.Jobs {
		if job.State == "queued" && id > "queued-009" {
			delete(q.s.Jobs, id)
		} else if job.State == "queued" {
			// Equivalent occupied workers otherwise produce arbitrary diagnostic
			// tie winners as the two captures iterate independent maps.
			job.Spec.AllowedMachines = []string{"worker-0"}
		}
	}
	before, _, err := q.captureObservation("")
	if err != nil {
		t.Fatal(err)
	}
	after, _, err := q.captureObservation("")
	if err != nil {
		t.Fatal(err)
	}
	before.observationLive = nil
	want := before.projectSnapshot(true)
	after.memoryPlanning = newMemoryPlanningCache()
	after.observationScan = after.placementScan()
	after.observationScan.observationOwner = after
	after.observationScan.memory = after.memoryPlanning
	got := after.projectSnapshot(true)
	if !reflect.DeepEqual(want, got) {
		for i := range want.Jobs {
			if len(want.Jobs) == len(got.Jobs) && !reflect.DeepEqual(want.Jobs[i], got.Jobs[i]) {
				w, _ := json.Marshal(want.Jobs[i])
				g, _ := json.Marshal(got.Jobs[i])
				t.Fatalf("immutable planning changed job %s:\nbefore: %s\nafter: %s", want.Jobs[i].ID, w, g)
			}
		}
		t.Fatal("immutable planning changed non-job response fields")
	}
}

func TestStatusReadFreshness(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	first := readStatusOptions(t, q, "")
	if first.Freshness == nil || first.Freshness.Cached || first.Freshness.AgeSeconds != 0 || !first.Freshness.CapturedAt.Equal(first.Now) {
		t.Fatalf("fresh capture metadata: %+v", first.Freshness)
	}
	clock.Advance(500 * time.Millisecond)
	cached := readStatusOptions(t, q, "")
	if cached.Freshness == nil || !cached.Freshness.Cached || cached.Freshness.AgeSeconds != 0.5 || !cached.Freshness.CapturedAt.Equal(first.Now) || !cached.Freshness.ServedAt.Equal(clock.Now()) {
		t.Fatalf("cached generation disguised as current: %+v", cached.Freshness)
	}
	q.observeRead = func(_ string, _, _ time.Duration) { clock.Advance(2 * time.Second) }
	for _, path := range []string{"/api/status?detail=full", "/api/status?jobs=active", "/api/inspection"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
		var got Snapshot
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &got) != nil {
			t.Fatal(w.Code, w.Body.String())
		}
		if got.Freshness == nil || got.Freshness.Cached || got.Freshness.AgeSeconds != 2 || !got.Freshness.CapturedAt.Equal(got.Now) || !got.Freshness.ServedAt.Equal(clock.Now()) {
			t.Fatalf("%s: delayed capture disguised as current: %+v", path, got.Freshness)
		}
	}
}

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
func TestStatusReadCPUReservation(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("cpu", "shared", "cpu", 4, 16)
	check(t, q, m, nil)
	first := submit(t, q, cpuSpec(3, 1))
	assignment := check(t, q, m, nil).Job
	if assignment == nil || assignment.ID != first.ID {
		t.Fatal("no assignment")
	}
	second := submit(t, q, cpuSpec(2, 1))
	if cpuFits(second, q.s.Machines[m.ID]) {
		t.Fatal("fixture must exhaust CPU capacity")
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	for _, path := range []string{"/api/jobs/" + second.ID, "/api/status", "/api/inspection"} {
		t.Run(path, func(t *testing.T) {
			r := httptest.NewRequest("GET", path, nil)
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			var got *Job
			if path == "/api/jobs/"+second.ID {
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
			} else {
				var snapshot Snapshot
				if err := json.Unmarshal(w.Body.Bytes(), &snapshot); err != nil {
					t.Fatal(err)
				}
				for _, j := range snapshot.Jobs {
					if j.ID == second.ID {
						got = j
					}
				}
			}
			if got == nil {
				t.Fatal("waiting job missing from response")
			}
			if !strings.Contains(got.Reason, "1 cores unreserved of 4 admitted") || got.QueueOrder != 0 {
				t.Fatalf("occupied CPU capacity lost: reason=%q queue_order=%d", got.Reason, got.QueueOrder)
			}
		})
	}
}

func TestStatusReadTerminalCommitInvalidatesCache(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	a := check(t, q, machine("status-worker", "fake"), nil).Job
	if a == nil || a.ID != j.ID {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(a, true)); err != nil {
		t.Fatal(err)
	}
	before, err := q.cachedPublicSnapshot()
	if err != nil || len(before.Jobs) != 1 || before.Jobs[0].State != "running" {
		t.Fatal("running generation missing", err)
	}
	finish(t, q, a, true)
	// No clock advance: even the ordinary one-second cache must not retain
	// a running projection after the terminal/release transaction commits.
	for _, query := range []string{"", "?jobs=all", "?detail=full"} {
		s := readStatusOptions(t, q, query)
		if len(s.Jobs) != 1 || s.Jobs[0].State != "succeeded" || !s.Jobs[0].ReleaseConfirmed {
			t.Errorf("%s: terminal released job projected as active: %+v", query, s.Jobs)
		}
	}
	page, err := q.cachedPublicSnapshot()
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].State != "succeeded" {
		t.Fatal("HTML snapshot retained running job", err)
	}
}

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
func TestJobReadAvoidsUsage(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "poll-key"
	s.Run.Command = []string{"echo", "private-poll-payload"}
	j := submit(t, q, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writer, err := q.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	reader, err := q.usageReader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	var got Job
	if err := client.Call(ctx, "GET", "/api/jobs/"+j.ID, nil, &got); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(got)
	if got.ID != j.ID || got.Spec.SubmissionKey != s.SubmissionKey || strings.Contains(string(data), "private-poll-payload") {
		t.Fatal("job receipt lost identity or exposed execution payload")
	}
}

func TestJobReadCommittedSubmissionIgnoresStatusCache(t *testing.T) {
	q, _ := fixture(t)
	// Keep the injected clock fixed, so every aggregate read stays on this
	// empty generation even after the submission transaction commits.
	before, err := q.cachedStatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	j, err := c.Submit(context.Background(), spec("experiment"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{j.ID, j.ID[:8]} {
		got, err := c.GetJob(context.Background(), id)
		if err != nil || got.ID != j.ID || got.State != "queued" {
			t.Fatalf("committed job missing for %s: %+v %v", id, got, err)
		}
	}
	after, err := q.cachedStatusJSON()
	if err != nil || before != after {
		t.Fatal("fixture did not retain stale aggregate generation", err)
	}
}

func TestJobReadEligibilityLifecycle(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("receipt-a", "fake", "host-a"), machine("receipt-b", "fake", "host-b")
	check(t, q, a, nil)
	check(t, q, b, nil)
	j := submit(t, q, spec("production"))
	attempt := check(t, q, a, nil).Job
	if attempt == nil || attempt.ID != j.ID {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(attempt, true)); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{b.ID, "unknown"}}}); err != nil {
		t.Fatal(err)
	}
	assertActive := func(state string) {
		t.Helper()
		got, err := q.jobSnapshot(j.ID[:8])
		if err != nil {
			t.Fatal(err)
		}
		if got.State != state || got.EligibleUnrestricted || got.EligibleHosts == nil || !slices.Equal(*got.EligibleHosts, []string{"host-a", "host-b"}) || !slices.Equal(got.EligibleUnknownMachines, []string{"unknown"}) {
			t.Fatalf("receipt lost current policy or execution host: %+v", got)
		}
	}
	assertActive("running")
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	assertActive("stopping")
	if err := q.Complete(result(attempt, false)); err != nil {
		t.Fatal(err)
	}
	got, err := q.jobSnapshot(j.ID)
	if err != nil || got.State != "cancelled" || got.EligibleHosts != nil || got.EligibleUnrestricted || len(got.EligibleUnknownMachines) != 0 {
		t.Fatal("terminal receipt retained active eligibility", err)
	}
}

func TestStatusReadPreservesDeadlineReason(t *testing.T) {
	q, c := fixture(t)
	s := spec("production")
	j := submit(t, q, s)
	c.Advance(ProductionStartTimeout + time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	receipt, err := q.jobSnapshot(j.ID)
	if err != nil || receipt.State != "failed" || receipt.Reason != "production start deadline exceeded" {
		t.Fatal("receipt lost deadline diagnostic", receipt, err)
	}
	status, err := q.cachedPublicSnapshot()
	if err != nil || len(status.Jobs) != 1 || status.Jobs[0].Reason != receipt.Reason {
		t.Fatal("global status lost deadline diagnostic", err)
	}
}

func TestStatusInspectionRemainsFresh(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Run.Command = []string{"echo", "private-inspection-payload"}
	j := submit(t, q, s)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var cached Snapshot
	if err := client.Call(ctx, "GET", "/api/status", nil, &cached); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	current, err := client.inspectionSnapshot(ctx)
	if err != nil || len(current.Jobs) != 1 || current.Jobs[0].State != "cancelled" || cached.Jobs[0].State != "queued" {
		t.Fatal("inspection reused stale status", err)
	}
	data, _ := json.Marshal(current)
	if strings.Contains(string(data), "private-inspection-payload") {
		t.Fatal("inspection exposed execution payload")
	}
}

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

func TestStatusReadRace(t *testing.T) {
	q, c := fixture(t)
	clock := &workerFakeClock{now: c.Now()}
	q.clock = clock
	s := spec("experiment")
	s.SubmissionKey = "race-key"
	s.Run.Env = map[string]string{"PRIVATE": "private-poll-payload"}
	j := submit(t, q, s)
	m := machine("race-reader", "fake")
	check(t, q, m, nil)
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 12 {
				snapshot, err := q.cachedPublicSnapshot()
				if err != nil {
					errs <- err
					return
				}
				data, _ := json.Marshal(snapshot)
				if strings.Contains(string(data), "private-poll-payload") {
					errs <- errors.New("status exposed private payload")
					return
				}
				got, err := q.jobSnapshot(j.ID)
				if err != nil || got.Spec.SubmissionKey != s.SubmissionKey {
					errs <- errors.New("job receipt lost its key")
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 12 {
			clock.Advance(time.Second)
			if _, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: j.ID, Attempt: 1}}); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestStatusReadTerminalCommitDuringJobProjection(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("experiment"))
	a := check(t, q, machine("status-worker", "fake", "status-host"), nil).Job
	if a == nil || a.ID != j.ID {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(a, true)); err != nil {
		t.Fatal(err)
	}
	captured := false
	q.observeRead = func(kind string, _, _ time.Duration) {
		if kind == "job" && !captured {
			captured = true
			finish(t, q, a, true)
			clock.Advance(41 * time.Second)
		}
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	r := httptest.NewRequest("GET", "/api/jobs/"+j.ID, nil)
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	var got Job
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || got.State != "succeeded" || !got.ReleaseConfirmed {
		t.Fatalf("delayed job GET returned pre-completion state: status=%d state=%s released=%t", w.Code, got.State, got.ReleaseConfirmed)
	}
	r = httptest.NewRequest("GET", "/api/hosts/status-host/maintenance", nil)
	r.Header.Set("Authorization", "Bearer test")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	var host HostMaintenanceStatus
	if err := json.Unmarshal(w.Body.Bytes(), &host); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(host.Attempts) != 0 || len(host.PendingReleases) != 0 {
		t.Fatalf("released job retained by host GET: status=%d %+v", w.Code, host)
	}
}
