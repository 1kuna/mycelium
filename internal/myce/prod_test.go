package myce

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestProductionNonresumableRequeueNoLoss(t *testing.T) {
	q, c := fixture(t)
	s := spec("experiment")
	s.Resumable = false
	j := submit(t, q, s)
	m := machine("one", "fake")
	r := check(t, q, m, nil).Job
	prod := submit(t, q, spec("production"))
	stop := check(t, q, m, r)
	if !stop.Stop || !stop.Kill {
		t.Fatal("production did not immediately kill")
	}
	requeued := job(t, q, j.ID)
	if requeued.State != "queued" || requeued.Losses != 0 {
		t.Fatal("preemption consumed retry")
	}
	c.Advance(time.Second)
	p := check(t, q, m, nil).Job
	if p != nil {
		if e := q.Started(result(p, true)); e != nil {
			t.Fatal(e)
		}
		p = job(t, q, p.ID)
	}
	if p == nil || p.ID != prod.ID || p.StartLatencySeconds != 1 {
		t.Fatal("production latency", p)
	}
	finish(t, q, p, true)
	again := check(t, q, m, nil).Job
	if again == nil || again.ID != j.ID || again.Attempt != 2 || again.Losses != 0 {
		t.Fatal("experiment not restarted", again)
	}
	if e := q.Complete(result(r, true)); e != ErrStale {
		t.Fatal("stale result accepted", e)
	}
}
func TestProductionGraceConfiguration(t *testing.T) {
	q, c := fixture(t)
	if e := q.SetProductionGrace(4 * time.Second); e != nil {
		t.Fatal(e)
	}
	submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	r := check(t, q, m, nil).Job
	submit(t, q, spec("production"))
	if check(t, q, m, r).GraceSeconds != 4 {
		t.Fatal("grace override")
	}
	c.Advance(3 * time.Second)
	if check(t, q, m, r).Kill {
		t.Fatal("early kill")
	}
	c.Advance(time.Second)
	if !check(t, q, m, r).Kill {
		t.Fatal("missing grace kill")
	}
}
func TestPinRetentionReservationWindowAndUnpin(t *testing.T) {
	q, c, m := modelFixture(t)
	p := ModelPin{Project: "atlas", Model: "test", Platform: "fake", Start: "08:00", End: "20:00", Timezone: "UTC", Weekdays: []int{4}}
	if e := q.Pin(p); e != nil {
		t.Fatal(e)
	}
	reportModel(t, q, m, "ready")
	// Keep worker alive while fake time crosses idle expiration.
	c.Advance(11 * time.Minute)
	reportModel(t, q, m, "ready")
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	if s == nil || s.State != "ready" {
		t.Fatal("pin idle eviction")
	}
	submit(t, q, spec("production"))
	if s.State != "ready" {
		t.Fatal("pin memory reclaimed")
	}
	if check(t, q, m, nil).Job != nil {
		t.Fatal("batch used reserved memory")
	}
	if e := q.Unpin("atlas", "test"); e != nil {
		t.Fatal(e)
	}
	if q.slotPinned(s) {
		t.Fatal("unpin ineffective")
	}
	// Window boundaries follow injected clock.
	if p.active(time.Date(2026, 10, 8, 20, 0, 0, 0, time.UTC)) || !p.active(time.Date(2026, 10, 8, 8, 0, 0, 0, time.UTC)) || p.active(time.Date(2026, 10, 9, 9, 0, 0, 0, time.UTC)) {
		t.Fatal("window boundaries")
	}
}
func TestCachedPlacementAndPinPersistence(t *testing.T) {
	q, _, m := modelFixture(t)
	other := m
	other.ID = "z-cached"
	other.Session = "s-z"
	other.CachedModels = []string{"test"}
	check(t, q, other, nil)
	addModelCall(t, q, "production")
	if q.s.ModelSlots[slotKey(other.ID, "test")] == nil {
		t.Fatal("cache affinity ignored")
	}
	if e := q.Pin(ModelPin{Project: "atlas", Model: "test", Machine: other.ID}); e != nil {
		t.Fatal(e)
	}
	snap, e := q.Snapshot()
	if e != nil || len(snap.Pins) != 1 || snap.Pins[0].Assigned != other.ID {
		t.Fatal("pin status", snap.Pins, e)
	}
}
func TestImmediateStopNotification(t *testing.T) {
	q, _ := fixture(t)
	m := machine("one", "fake")
	submit(t, q, spec("experiment"))
	r := check(t, q, m, nil).Job
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan WorkerEvent, 1)
	go (Worker{Machine: m, Client: Client{URL: server.URL, Token: "test"}}).watchStops(ctx, events)
	submit(t, q, spec("production"))
	select {
	case e := <-events:
		if e.ID != r.ID || e.Attempt != r.Attempt || e.GraceSeconds != 10 {
			t.Fatal(e)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("stop waited for heartbeat")
	}
}

// StartDev is the implementation behind myce dev: real HTTP and subprocesses,
// fake GPU only. The experiment ignores TERM to exercise the full 10s grace.
func TestDevProductionStartsUnder15SecondsRealTime(t *testing.T) {
	if testing.Short() {
		t.Skip("real-time production start measurement (HTTP, subprocesses, 10s grace)")
	}
	dir := t.TempDir()
	d, e := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(dir, "queue.db"), Checkpoints: filepath.Join(dir, "nas"), Token: "test", Workers: 1, MemoryGB: 8, Interval: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	// Immediate delivery independently has a long-poll notification test.
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `if [ "$MYCE_ATTEMPT" != 1 ]; then exit 0; fi; trap '' TERM; echo ready; while :; do sleep 1; done`}
	j, e := d.Queue.Submit(s)
	if e != nil {
		t.Fatal(e)
	}
	until := time.Now().Add(testWaitTimeout)
	for {
		ls, e := d.Queue.Logs(j.ID, 0)
		if e != nil {
			t.Fatal(e)
		}
		if len(ls) > 0 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("experiment never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	p, e := d.Queue.Submit(spec("production"))
	if e != nil {
		t.Fatal(e)
	}
	until = time.Now().Add(testWaitTimeout)
	for {
		snap, e := d.Queue.Snapshot()
		if e != nil {
			t.Fatal(e)
		}
		for _, got := range snap.Jobs {
			if got.ID == p.ID && got.StartedAt != nil {
				logs, err := d.Queue.Logs(p.ID, 0)
				if err != nil {
					t.Fatal(err)
				}
				if len(logs) == 0 {
					continue
				}
				elapsed := got.StartedAt.Sub(got.Queued)
				t.Logf("queue-measured production submitted-to-start %.3fs (no model load)", elapsed.Seconds())
				if got.StartLatencySeconds != elapsed.Seconds() {
					t.Fatal("recorded start latency disagrees with queue timestamps", got)
				}
				if elapsed >= 15*time.Second {
					t.Fatal("production latency exceeds target")
				}
				return
			}
		}
		if time.Now().After(until) {
			t.Fatalf("production start/logs not observed within %s; snapshot=%+v", testWaitTimeout, snap)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPinPersistsAndPlatformReassigns(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	config := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{{Name: "m", Engine: "fake", Path: "fake", MemoryGB: 8, Platforms: []string{"fake"}}}}
	q, e := Open(filepath.Join(dir, "q.db"), dir, clock)
	if e != nil {
		t.Fatal(e)
	}
	if e = q.ConfigureModels(config); e != nil {
		t.Fatal(e)
	}
	m := machine("a", "fake")
	m.ModelURL = "http://127.0.0.1:1"
	m.ModelConfigHash = modelConfigHash(config)
	check(t, q, m, nil)
	if e = q.Pin(ModelPin{Project: "test", Model: "m", Platform: "fake"}); e != nil {
		t.Fatal(e)
	}
	if e = q.Close(); e != nil {
		t.Fatal(e)
	}
	q, e = Open(filepath.Join(dir, "q.db"), dir, clock)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	q.ConfigureModels(config)
	if len(q.s.Pins) != 1 {
		t.Fatal("pin lost on restart")
	}
	other := m
	other.ID = "b"
	other.Session = "session-b"
	check(t, q, other, nil)
	if q.s.Pins[0].Assigned != "b" {
		t.Fatal("pin not reassigned to available platform")
	}
}
func TestPinnedLoadFailureVisible(t *testing.T) {
	q, _, m := modelFixture(t)
	if e := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); e != nil {
		t.Fatal(e)
	}
	if _, e := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", State: "failed", Error: "disk full"}}}); e != nil {
		t.Fatal(e)
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	if s == nil || s.State != "failed" || s.Error != "disk full" {
		t.Fatal("pin failure hidden", s)
	}
}

func TestProductionShortensExistingDeadlineGrace(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	r := check(t, q, m, nil).Job
	submit(t, q, spec("deadline"))
	if check(t, q, m, r).GraceSeconds != 120 {
		t.Fatal("deadline grace")
	}
	c.Advance(time.Second)
	submit(t, q, spec("production"))
	if check(t, q, m, r).GraceSeconds != 10 {
		t.Fatal("production did not shorten existing grace")
	}
	c.Advance(time.Second)
	if check(t, q, m, r).GraceSeconds != 9 {
		t.Fatal("grace was extended on check-in")
	}
}

func TestProductionShorteningPreservesCancellation(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	r := check(t, q, m, nil).Job
	if e := q.Cancel(j.ID); e != nil {
		t.Fatal(e)
	}
	submit(t, q, spec("production"))
	if check(t, q, m, r).GraceSeconds != 10 {
		t.Fatal("cancel grace not shortened")
	}
	c.Advance(10 * time.Second)
	check(t, q, m, r)
	requireState(t, q, j.ID, "cancelled")
}

// Logic-level target: OS scheduling and log delivery cannot affect the queue's
// own measurement. The real-time counterpart above is skipped only with -short.
func TestProductionStartLatencyFakeClock(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	run := check(t, q, m, nil).Job
	prod := submit(t, q, spec("production"))
	if reply := check(t, q, m, run); !reply.Stop || reply.GraceSeconds != 10 {
		t.Fatal("production grace", reply)
	}
	c.Advance(9 * time.Second)
	if check(t, q, m, run).Kill {
		t.Fatal("killed before production grace")
	}
	c.Advance(time.Second)
	if !check(t, q, m, run).Kill {
		t.Fatal("did not kill at production grace")
	}
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != prod.ID {
		t.Fatal("production not assigned first", assigned)
	}
	c.Advance(time.Second) // command startup, after assignment
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, prod.ID)
	if got.StartedAt == nil || got.StartLatencySeconds != 11 || got.StartLatencySeconds != got.StartedAt.Sub(got.Queued).Seconds() || got.StartLatencySeconds >= 15 {
		t.Fatalf("incorrect queue-measured production latency: %+v", got)
	}
	c.Advance(time.Hour) // slow observer must not change the measured start
	got = job(t, q, prod.ID)
	if got.StartLatencySeconds != 11 {
		t.Fatal("observation changed start measurement", got)
	}
}

// Completion arrives before the worker's next memory report.
func TestProductionPreemptionAcknowledgementDoesNotStopSecondCard(t *testing.T) {
	q, c := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	submit(t, q, spec("experiment"))
	first := check(t, q, a, nil).Job
	submit(t, q, spec("experiment"))
	second := check(t, q, b, nil).Job
	prod := submit(t, q, spec("production"))
	if !check(t, q, a, first).Stop || check(t, q, b, second).Stop {
		t.Fatal("one production job must stop exactly one experiment")
	}
	c.Advance(time.Second)
	finish(t, q, first, false)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if check(t, q, b, second).Stop {
		t.Fatal("acknowledged release must count while its memory report is pending")
	}
	assigned := check(t, q, a, nil).Job
	if assigned == nil || assigned.ID != prod.ID {
		t.Fatal("production must start on the first free report", assigned)
	}
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, prod.ID); got.StartLatencySeconds != 1 {
		t.Fatal("production was delayed", got.StartLatencySeconds)
	}
	requireState(t, q, second.ID, "running")
}

func TestProductionPreemptionTwoJobsStopTwoCards(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	submit(t, q, spec("experiment"))
	first := check(t, q, a, nil).Job
	submit(t, q, spec("experiment"))
	second := check(t, q, b, nil).Job
	p1 := submit(t, q, spec("production"))
	p2 := submit(t, q, spec("production"))
	if !check(t, q, a, first).Stop || !check(t, q, b, second).Stop {
		t.Fatal("two production jobs need two experiment releases")
	}
	finish(t, q, first, false)
	finish(t, q, second, false)
	one, two := check(t, q, a, nil).Job, check(t, q, b, nil).Job
	if one == nil || two == nil || one.ID == two.ID ||
		(one.ID != p1.ID && one.ID != p2.ID) || (two.ID != p1.ID && two.ID != p2.ID) {
		t.Fatal("both production jobs must start on free reports", one, two)
	}
}

func TestProductionPreemptionReleaseGrace(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		name := "stop never completes"
		if acknowledged {
			name = "completion without free memory report"
		}
		t.Run(name, func(t *testing.T) {
			q, c := fixture(t)
			a, b := machine("a", "fake"), machine("b", "fake")
			submit(t, q, spec("experiment"))
			first := check(t, q, a, nil).Job
			submit(t, q, spec("experiment"))
			second := check(t, q, b, nil).Job
			submit(t, q, spec("production"))
			if !check(t, q, a, first).Stop {
				t.Fatal("first card must stop")
			}
			if acknowledged {
				c.Advance(time.Second)
				finish(t, q, first, false)
				c.Advance(8 * time.Second)
			} else {
				c.Advance(9 * time.Second)
				if check(t, q, a, first).Kill {
					t.Fatal("killed before grace")
				}
			}
			if check(t, q, b, second).Stop {
				t.Fatal("second card stopped before release grace")
			}
			c.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if !check(t, q, b, second).Stop {
				t.Fatal("second card must stop when first release grace expires")
			}
		})
	}
}

func TestProductionPreemptionFullReportSupersedesReleaseCredit(t *testing.T) {
	q, c := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	submit(t, q, spec("experiment"))
	first := check(t, q, a, nil).Job
	submit(t, q, spec("experiment"))
	second := check(t, q, b, nil).Job
	submit(t, q, spec("production"))
	c.Advance(time.Second)
	finish(t, q, first, false)
	a.FreeMemoryGB = 0 // The command exited, but the memory remains unavailable.
	if check(t, q, a, nil).Job != nil {
		t.Fatal("release credit cannot admit work")
	}
	if !check(t, q, b, second).Stop {
		t.Fatal("full memory report must allow another victim")
	}
}
