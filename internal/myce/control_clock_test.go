package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func adaptiveLossFixture(t *testing.T, priority string) (*Queue, *fakeClock, Machine, Machine, *Job) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.SetLossTimeouts(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	worker, peer := machine("worker", "fake"), machine("peer", "fake")
	s := spec(priority)
	s.AllowedMachines = []string{worker.ID}
	submit(t, q, s)
	job := check(t, q, worker, nil).Job
	if job == nil {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(job, true)); err != nil {
		t.Fatal(err)
	}
	check(t, q, peer, nil)
	for range 4 {
		clock.Advance(time.Second)
		check(t, q, worker, job)
		check(t, q, peer, nil)
	}
	return q, clock, worker, peer, job
}

// The incident's new 31-second queue tail must be safe even when all preceding
// measurements were fast. No percentile pretraining or class exception is used.
func TestControlClockSlowQueuePreservesAttempts(t *testing.T) {
	for _, priority := range []string{"production", "deadline", "experiment"} {
		t.Run(priority, func(t *testing.T) {
			q, clock, worker, peer, job := adaptiveLossFixture(t, priority)
			for _, delay := range []time.Duration{31 * time.Second, 90 * time.Second} {
				q.mu.Lock()
				clock.Advance(delay)
				q.maintain() // A tick gets its turn before queued check-ins.
				gone := q.s.Machines[worker.ID].Gone
				q.mu.Unlock()
				if gone {
					t.Fatal("queue service time became worker silence")
				}
				got := q.s.Jobs[job.ID]
				if got.State != "running" || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Attempt != job.Attempt {
					t.Fatalf("false interruption: %+v", got)
				}
				status, err := q.HostMaintenanceStatus(hostID(&worker))
				if err != nil || len(status.Workers) != 1 || status.Workers[0].Fresh || !status.Workers[0].Machine.Suspect {
					t.Fatalf("maintenance freshness disagrees: %+v %v", status.Workers, err)
				}
				view, _, err := q.captureStatusObservation("", nil)
				if err != nil || view.workerFresh(view.s.Machines[worker.ID], view.clock.Now()) || !view.workerSuspect(view.s.Machines[worker.ID], view.clock.Now()) {
					t.Fatal("detached status lost the service-time fence", err)
				}
				check(t, q, peer, nil)
				check(t, q, worker, job)
			}
		})
	}
}

func TestControlClockDeadWorkerDetection(t *testing.T) {
	for _, overloaded := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "overloaded"}[overloaded], func(t *testing.T) {
			q, clock, worker, peer, job := adaptiveLossFixture(t, "production")
			allowance := q.machineLossTimeout(worker.ID)
			if overloaded {
				q.mu.Lock()
				clock.Advance(31 * time.Second)
				q.mu.Unlock()
			}
			clock.Advance(allowance - time.Nanosecond)
			check(t, q, peer, nil)
			if q.s.Machines[worker.ID].Gone {
				t.Fatal("lost before the measured opportunity allowance")
			}
			clock.Advance(time.Nanosecond)
			check(t, q, peer, nil)
			got := q.s.Jobs[job.ID]
			if !q.s.Machines[worker.ID].Gone || got.State != "queued" || got.Losses != 1 || got.InfrastructureRetries != 1 || got.ReleaseConfirmed {
				t.Fatalf("dead worker not fenced with exact retry/custody: %+v", got)
			}
		})
	}
}

func TestControlClockCancelledQueueWaitDoesNotChargeRetry(t *testing.T) {
	q, clock, worker, peer, job := adaptiveLossFixture(t, "production")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := q.CheckinContext(ctx, Checkin{Machine: worker, Running: &Execution{ID: job.ID, Attempt: job.Attempt}})
		done <- err
	}()
	await(t, func() bool {
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		return len(q.contacts[worker.ID].pending) != 0
	})
	clock.Advance(90 * time.Second)
	cancel()
	q.maintain()
	q.mu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	clock.Advance(time.Second) // The worker's first actual reconnect interval.
	check(t, q, peer, nil)
	check(t, q, worker, job)
	got := q.s.Jobs[job.ID]
	if got.State != "running" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("cancelled queue wait charged a retry: %+v", got)
	}
}

func TestControlClockPendingPeerCannotHideDeadWorker(t *testing.T) {
	q, clock, worker, peer, _ := adaptiveLossFixture(t, "production")
	settle := q.beginWorkerCheckin(peer.ID, peer.Session, time.Second, true)
	defer settle(false)
	clock.Advance(q.machineLossTimeout(worker.ID))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if !q.s.Machines[worker.ID].Gone || q.s.Machines[peer.ID].Gone {
		t.Fatal("one in-flight control path hid an independently dead worker")
	}
}

func TestControlClockWorkerAdvertisedCadence(t *testing.T) {
	q, clock := fixture(t)
	if err := q.SetLossTimeouts(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	m := machine("slow-cadence", "fake")
	if _, err := q.Checkin(Checkin{Machine: m, period: 12 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if got := q.machineLossTimeout(m.ID); got != 24*time.Second {
		t.Fatalf("lost advertised cadence: %s", got)
	}
	clock.Advance(12 * time.Second)
	if _, err := q.Checkin(Checkin{Machine: m, period: 12 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if got := q.machineLossTimeout(m.ID); got <= 24*time.Second {
		t.Fatalf("no learned arrival variation: %s", got)
	}
}

func TestControlClockHTTPAdvertisedCadence(t *testing.T) {
	q, _ := fixture(t)
	if err := q.SetLossTimeouts(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{Client: Client{URL: server.URL, Token: "test"}, Interval: 7 * time.Second}
	m := machine("worker", "fake")
	if _, err := w.checkin(context.Background(), Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	if got := q.machineLossTimeout(m.ID); got != 14*time.Second {
		t.Fatalf("HTTP lost the actual worker interval: %s", got)
	}
}

func TestControlClockDevQueueStall(t *testing.T) {
	d, _, ctx := devProbe(t)
	release := filepath.Join(t.TempDir(), "release")
	s := spec("production")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", `while [ ! -f "$1" ]; do sleep 0.05; done`, "fixture", release}
	client := d.Client()
	var submitted Job
	if err := client.Call(ctx, "POST", "/api/jobs", s, &submitted); err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool {
		d.Queue.mu.RLock()
		defer d.Queue.mu.RUnlock()
		return d.Queue.s.Jobs[submitted.ID].StartedAt != nil
	})
	d.Queue.mu.Lock()
	_, err := d.Queue.clock.(*devClock).advance(90)
	d.Queue.maintain()
	got := *d.Queue.s.Jobs[submitted.ID]
	d.Queue.mu.Unlock()
	if err != nil || got.State != "running" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("real command was interrupted by queue delay: %+v %v", got, err)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	finished, err := client.Wait(ctx, submitted.ID)
	if err != nil || finished.State != "succeeded" || finished.Attempt != got.Attempt || finished.Losses != 0 || finished.InfrastructureRetries != 0 {
		t.Fatalf("real command did not finish the same attempt: %+v %v", finished, err)
	}
}

// Even an uninterrupted queue transaction cannot indefinitely present a dead
// worker as healthy. Suspicion is bounded in wall time, confirmation in service
// opportunity time. The former withholds offers but never spends retry budget.
func TestControlClockDeadWorkerContinuousSaturation(t *testing.T) {
	q, clock, worker, _, job := adaptiveLossFixture(t, "production")
	s := spec("experiment")
	s.AllowedMachines = []string{worker.ID}
	submit(t, q, s)
	allowance := q.machineLossTimeout(worker.ID)
	q.mu.Lock()
	defer q.mu.Unlock()
	clock.Advance(allowance - time.Nanosecond)
	if q.workerSuspect(q.s.Machines[worker.ID], clock.Now()) {
		t.Fatal("suspect before the measured arrival allowance")
	}
	clock.Advance(time.Nanosecond)
	for range 3 {
		q.maintain()
		q.detectHealth()
		m := q.s.Machines[worker.ID]
		status := q.hostMaintenanceStatus(hostID(m))
		if m.Gone || !q.workerSuspect(m, clock.Now()) || q.workerFresh(m, clock.Now()) || status.Healthy || status.Workers[0].Fresh || !status.Workers[0].Machine.Suspect {
			t.Fatal("silent worker presented as healthy or falsely confirmed lost")
		}
		if q.machineLimitReason(m) == "" || q.placementLiveness(q.placementScan())[m.ID] {
			t.Fatal("suspect worker can receive new work")
		}
		got := q.s.Jobs[job.ID]
		if got.Losses != 0 || got.InfrastructureRetries != 0 || got.State != "running" || got.Attempt != job.Attempt {
			t.Fatalf("suspicion charged an interruption: %+v", got)
		}
		clock.Advance(31 * time.Second)
	}
	found := false
	for _, event := range q.s.Health.Events {
		found = found || event.Reason == "worker_suspect"
	}
	if !found {
		t.Fatal("suspect worker did not raise owner attention")
	}
}

func TestControlClockColdLegacyCannotStrandWork(t *testing.T) {
	q, clock := fixture(t)
	if err := q.SetLossTimeouts(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	m := machine("legacy", "fake")
	submit(t, q, spec("production"))
	reply, err := q.Checkin(Checkin{Machine: m})
	if err != nil || reply.Job != nil || !q.workerSuspect(q.s.Machines[m.ID], clock.Now()) {
		t.Fatal("unknown cadence received work with no finite loss allowance", err)
	}
	for range 2 {
		clock.Advance(time.Second)
		reply, err = q.Checkin(Checkin{Machine: m})
		if err != nil {
			t.Fatal(err)
		}
	}
	if reply.Job == nil || q.workerSuspect(q.s.Machines[m.ID], clock.Now()) {
		t.Fatal("observed legacy cadence did not qualify capacity")
	}
}

func TestControlClockRestartGraceExcludesQueueStall(t *testing.T) {
	q, clock, worker, _, job := adaptiveLossFixture(t, "production")
	q.unreported = map[string]bool{worker.ID: true}
	q.SetRestartGrace(RestartGrace)
	q.mu.Lock()
	clock.Advance(2 * RestartGrace)
	q.maintain()
	gone, retries := q.s.Machines[worker.ID].Gone, q.s.Jobs[job.ID].InfrastructureRetries
	q.mu.Unlock()
	if gone || retries != 0 || !q.inRestartGrace(clock.Now()) {
		t.Fatal("startup queue service consumed the reconciliation window")
	}
	check(t, q, worker, job)
	if q.unreported[worker.ID] || q.s.Jobs[job.ID].State != "running" {
		t.Fatal("reporting worker did not reconcile its original attempt")
	}
}

func TestControlClockOverlappingReadersCountOnce(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	c := &controlClock{clock: clock.Now, at: clock.Now()}
	clock.Advance(time.Second)
	c.begin()
	clock.Advance(time.Second)
	c.begin()
	clock.Advance(time.Second)
	c.end()
	clock.Advance(time.Second)
	c.end()
	clock.Advance(time.Second)
	if got := c.now(); got != 2*time.Second {
		t.Fatalf("overlapping occupancy counted twice: %s", got)
	}
}
