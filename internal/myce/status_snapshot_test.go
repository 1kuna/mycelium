package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// statusSnapshotFixture commits retained history, one worker and queued work
// under a tight measured memory allowance.
func statusSnapshotFixture(t *testing.T) (*Queue, Machine, *Job) {
	t.Helper()
	q, clock := fixture(t)
	m := machine("contact", "fake")
	check(t, q, m, nil)
	queued := submit(t, q, spec("experiment"))
	history := 3000
	if s := os.Getenv("MYCE_STATUS_SNAPSHOT_HISTORY"); s != "" {
		history, _ = strconv.Atoi(s)
	}
	q.mu.Lock()
	for i := range history {
		id := fmt.Sprintf("history-%05d", i)
		s := spec("experiment")
		s.Name = id
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded", Attempt: 1, ReleaseConfirmed: true, Queued: clock.Now().Add(-time.Duration(i+1) * time.Second), Spec: s}
	}
	q.mu.Unlock()
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	memory := int64(48 << 20)
	if s := os.Getenv("MYCE_STATUS_SNAPSHOT_MEMORY"); s != "" {
		memory, _ = strconv.ParseInt(s, 10, 64)
	}
	budgetHost(q, memory, 3)
	// Steady state on a serving queue (Run enables the warmer): the published
	// records are decoded off q.mu before reads arrive.
	q.readWarm.enable(&q.publishedReads)
	q.readWarm.idle.Wait()
	return q, m, queued
}

type statusSnapshotRead struct {
	path string
	want int
}

func statusSnapshotReads(queued string) []statusSnapshotRead {
	return []statusSnapshotRead{
		{"/api/status", 200},
		{"/api/status?view=snapshot", 200},
		{"/api/status?jobs=recent&limit=20", 200},
		{"/api/status?sections=machines", 200},
		{"/api/jobs/" + queued, 200},
		{"/api/jobs/history-00001", 200},
		{"/api/jobs/unknown-job-identity", 404},
	}
}

func TestMachineStatusBoundedDuringWriterTurn(t *testing.T) {
	q, clock := fixture(t)
	q.mu.Lock()
	for i := range 5 {
		id := fmt.Sprintf("worker-%d", i)
		m := machine(id, "fake")
		m.LastSeen = clock.Now()
		m.EnvironmentReports = []EnvironmentReport{{Key: "env", State: "ready"}}
		if i == 0 {
			m.Devices = []MachineDevice{{ID: "0", Platform: "cuda", TotalMemoryGB: 24, BudgetGB: 24, FreeMemoryGB: 24}}
			m.HostHealth = &HostHealth{History: []HostHealthSample{{}, {}}}
		}
		q.s.Machines[id] = &m
	}
	for i := range 12400 {
		id := fmt.Sprintf("history-%05d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded", ReleaseConfirmed: true, Queued: clock.Now().Add(-time.Hour), Attempt: 1, Spec: spec("experiment")}
	}
	for _, group := range []struct {
		state string
		count int
	}{{"queued", 126}, {"paused", 56}, {"running", 30}} {
		for i := range group.count {
			id := fmt.Sprintf("%s-%03d", group.state, i)
			j := &Job{ID: id, State: group.state, Queued: clock.Now(), Spec: spec("experiment")}
			if group.state == "running" {
				j.Machine, j.Attempt, j.StartedAt = fmt.Sprintf("worker-%d", i%5), 1, statusTestTime(clock.Now())
				j.Spec.Needs.Platform.Any = false
				j.Spec.Needs.Platform.Required = []string{"cpu"}
				if i == 0 {
					j.Spec.Needs.Platform.Required = []string{"cuda"}
					j.AllocatedDevices = []string{"0"}
					q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{j.Machine: {jobKey(*j): {devicePeakGB: 10}}}
				}
			}
			q.s.Jobs[id] = j
		}
	}
	q.mu.Unlock()
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	publication := q.publishedReads.Load()
	history := readGet(publication.root, "jobs/history-00000")
	if history.decoded.Load() {
		t.Fatal("fixture must start with cold settled jobs")
	}
	handler := (API{Queue: q, Token: "test"}).Handler()
	q.mu.Lock()
	defer q.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := httptest.NewRequest("GET", "/api/status?sections=machines", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer test")
	res := httptest.NewRecorder()
	started := time.Now()
	handler.ServeHTTP(res, req)
	elapsed := time.Since(started)
	t.Logf("machine status: %s", elapsed)
	if res.Code != 200 || elapsed >= 2*time.Second {
		t.Fatalf("machine status returned HTTP %d in %s: %s", res.Code, elapsed, res.Body.String())
	}
	if history.decoded.Load() {
		t.Error("machine status decoded an unrelated retained job")
	}
	var s Snapshot
	if err := json.Unmarshal(res.Body.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	if len(s.Jobs) != 212 || len(s.Machines) != 5 || len(s.Machines[0].EnvironmentReports) != 1 || s.Summary != nil {
		t.Fatalf("machine status lost jobs or machine detail: %d jobs, %d machines, summary=%t", len(s.Jobs), len(s.Machines), s.Summary != nil)
	}
	m := s.Machines[0]
	if m.HostHealth == nil || len(m.HostHealth.History) != 2 || len(m.Devices) != 1 || m.Devices[0].FreeMemoryGB != 14 || len(m.Devices[0].Attempts) != 1 {
		t.Fatalf("machine status lost host history or exact device custody: health=%+v devices=%+v", m.HostHealth, m.Devices)
	}
}

// #1052: status and job GETs project the last committed publication. A writer
// that owns q.mu (a long control transaction) must not delay them: before this
// change every default-status miss, filtered status and unknown-ID job read
// waited for q.mu. Concurrent paged reads over retained history must also fit
// the request share instead of copying all history (projection_capacity_exceeded).
func TestStatusSnapshotReadsDuringWriterHold(t *testing.T) {
	q, m, queued := statusSnapshotFixture(t)
	handler := (API{Queue: q, Token: "test"}).Handler()
	read := func(ctx context.Context, r statusSnapshotRead) error {
		req := httptest.NewRequest("GET", r.path, nil).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != r.want {
			return fmt.Errorf("%s: %d %s", r.path, w.Code, w.Body.String())
		}
		return nil
	}
	reads := statusSnapshotReads(queued.ID)
	concurrently := func(phase string, ctx context.Context) {
		t.Helper()
		errs := make(chan error, 4*len(reads))
		var wg sync.WaitGroup
		for range 4 {
			for _, r := range reads {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := read(ctx, r); err != nil {
						errs <- err
					}
				}()
			}
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("%s: %v", phase, err)
		}
	}

	// Phase 0: a burst of concurrent status reads over retained history,
	// with no writer active, must fit the measured request shares.
	concurrently("concurrent reads", context.Background())
	if t.Failed() {
		return
	}

	// Phase 1: a writer holds q.mu until every concurrent read has returned.
	// The deadline only bounds a regression; passing reads never wait on it.
	q.mu.Lock()
	var unlock sync.Once
	release := func() { unlock.Do(q.mu.Unlock) }
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	concurrently("read while a writer held q.mu", ctx)
	cancel()
	release()
	if t.Failed() {
		return
	}

	// Phase 2: readers loop while real submits and check-ins commit. Every
	// read succeeds and every control transaction completes.
	stop := make(chan struct{})
	loopErrs := make(chan error, 64)
	var readers sync.WaitGroup
	for i := range 16 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := read(context.Background(), reads[(i+n)%len(reads)]); err != nil {
					select {
					case loopErrs <- err:
					default:
					}
					return
				}
			}
		}()
	}
	for i := range 10 {
		s := spec("experiment")
		s.Name = fmt.Sprintf("during-reads-%d", i)
		if _, err := q.Submit(s); err != nil {
			t.Error(err)
		}
		if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
			t.Error(err)
		}
	}
	close(stop)
	readers.Wait()
	close(loopErrs)
	for err := range loopErrs {
		t.Errorf("read during commits: %v", err)
	}
}

// A receipt reports the age of the committed publication it projects, and
// whether it reused a completed receipt.
func TestStatusSnapshotJobReadFreshness(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("experiment"))
	committed := clock.Now()
	clock.Advance(3 * time.Second)
	handler := (API{Queue: q, Token: "test"}).Handler()
	read := func() *SnapshotFreshness {
		r := httptest.NewRequest("GET", "/api/jobs/"+j.ID, nil)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		var got struct {
			Freshness *SnapshotFreshness `json:"freshness"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &got) != nil || got.Freshness == nil {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
		return got.Freshness
	}
	first := read()
	if !first.CapturedAt.Equal(committed) || first.AgeSeconds != 3 || first.Cached {
		t.Fatalf("cold receipt freshness: %+v", first)
	}
	if again := read(); !again.CapturedAt.Equal(committed) || !again.Cached {
		t.Fatalf("cached receipt freshness: %+v", again)
	}
}
