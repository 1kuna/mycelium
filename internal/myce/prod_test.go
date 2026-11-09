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
	s := cpuSpec(1, 1)
	s.Resumable = false
	j := submit(t, q, s)
	m := cpuMachine("one", "one", "fake", 1, 8)
	r := check(t, q, m, nil).Job
	prod := submit(t, q, spec("production"))
	stop := check(t, q, m, r)
	if !stop.Stop || !stop.Kill {
		t.Fatal("production did not immediately kill")
	}
	ack := result(r, false)
	ack.Interrupted = true
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
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
	if _, e := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: currentTestReportAttempt(q, m, "test"), State: "failed", Error: "disk full"}}}); e != nil {
		t.Fatal(e)
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	if s == nil || s.State != "failed" || s.Error != "model load or unload failed" {
		t.Fatal("pin failure hidden", s)
	}
}

func TestProductionShortensExistingDeadlineGrace(t *testing.T) {
	q, c := fixture(t)
	s := cpuSpec(1, 1)
	s.Resumable = true
	submit(t, q, s)
	m := cpuMachine("one", "one", "fake", 1, 8)
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

// Completion arrives before the worker's next memory report.

func TestProductionPreemptionTwoJobsStopTwoCards(t *testing.T) {
	for _, newcomerFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "selected demand first", true: "new demand first"}[newcomerFirst], func(t *testing.T) {
			q, c := fixture(t)
			a, b := machine("a", "fake"), machine("b", "fake")
			submit(t, q, spec("experiment"))
			first := check(t, q, a, nil).Job
			submit(t, q, spec("experiment"))
			second := check(t, q, b, nil).Job
			p1 := submit(t, q, spec("production"))
			c.Advance(time.Second)
			s := spec("production")
			if newcomerFirst {
				// A same-tier newcomer would normally win through its deadline. It
				// must not borrow the release already committed to the first demand.
				due := c.Now().Add(time.Hour)
				s.Deadline = &due
			}
			p2 := submit(t, q, s)
			if !check(t, q, a, first).Stop || !check(t, q, b, second).Stop {
				t.Fatal("two production jobs need two experiment releases")
			}
			if a, b := job(t, q, p1.ID).PreemptionPlacement, job(t, q, p2.ID).PreemptionPlacement; a == nil || b == nil || a.Machine == b.Machine {
				t.Fatal("production demands must retain separate targets", a, b)
			}
			finishInterrupted(t, q, first)
			finishInterrupted(t, q, second)
			one, two := check(t, q, a, nil).Job, check(t, q, b, nil).Job
			if one == nil || two == nil || one.ID == two.ID ||
				(one.ID != p1.ID && one.ID != p2.ID) || (two.ID != p1.ID && two.ID != p2.ID) {
				t.Fatal("both production jobs must start on free reports", one, two)
			}
			if one.ID != p1.ID || two.ID != p2.ID {
				t.Fatal("production demands did not use their selected targets", one.ID, two.ID)
			}
		})
	}
}

func TestProductionPreemptionReleaseGrace(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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
				prod := submit(t, q, spec("production"))
				if !check(t, q, a, first).Stop {
					t.Fatal("first card must stop")
				}
				if acknowledged {
					c.Advance(time.Second)
					finishInterrupted(t, q, first)
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
				if q.roomPlan(job(t, q, prod.ID), q.s.Machines[a.ID], true) != nil {
					t.Fatal("expired release credit still supplies a placement plan")
				}
				// #662 retains the selected host even when planning credit expires.
				// Grace authorizes killing the selected victim, never another stop
				// on a host this demand will not use, nor admission without release.
				if check(t, q, b, second).Stop {
					t.Fatal("release grace redirected demand to an unrelated card")
				}
				if !acknowledged {
					reply := check(t, q, a, first)
					if !reply.Kill || reply.Job != nil {
						t.Fatal("expired grace must kill without admitting replacement", reply)
					}
					requireState(t, q, prod.ID, "queued")
					finishInterrupted(t, q, first)
				}
				a.FreeMemoryGB = 0
				if next := check(t, q, a, nil).Job; next != nil {
					t.Fatal("exact release without free memory admitted work", next)
				}
				requireState(t, q, prod.ID, "queued")
				a.FreeMemoryGB = 8
				next := check(t, q, a, nil).Job
				if next == nil || next.ID != prod.ID || next.Machine != first.Machine {
					t.Fatal("production did not start on its selected released host", next)
				}
				if err := q.Started(result(next, false)); err != nil {
					t.Fatal(err)
				}
				requireState(t, q, second.ID, "running")
			})
		}

	})

	t.Run("ProductionPreemptionFullReportSupersedesReleaseCredit", func(t *testing.T) {
		q, c := fixture(t)
		a, b := machine("a", "fake"), machine("b", "fake")
		submit(t, q, spec("experiment"))
		first := check(t, q, a, nil).Job
		submit(t, q, spec("experiment"))
		second := check(t, q, b, nil).Job
		prod := submit(t, q, spec("production"))
		c.Advance(time.Second)
		finishInterrupted(t, q, first)
		a.FreeMemoryGB = 0 // The command exited, but the memory remains unavailable.
		if check(t, q, a, nil).Job != nil {
			t.Fatal("release credit cannot admit work")
		}
		// #662 forbids redirecting the demand after it caused this release.
		// A full report supersedes credit, but cannot authorize an unrelated stop.
		if check(t, q, b, second).Stop {
			t.Fatal("unavailable-memory report redirected demand to another victim")
		}
		requireState(t, q, prod.ID, "queued")
		a.FreeMemoryGB = 8
		next := check(t, q, a, nil).Job
		if next == nil || next.ID != prod.ID || next.Machine != first.Machine {
			t.Fatal("production did not use its selected host after memory recovered", next)
		}
		if err := q.Started(result(next, false)); err != nil {
			t.Fatal(err)
		}
		requireState(t, q, second.ID, "running")

	})
}
