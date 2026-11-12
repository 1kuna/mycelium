package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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

// Keep the survivor reporting at the fixture's advertised interval. One jump
// with both workers silent would reproduce a fleet outage, not an isolated loss.
func advanceReportingPeer(t *testing.T, q *Queue, clock *fakeClock, peer Machine, elapsed time.Duration) {
	t.Helper()
	for elapsed > 0 {
		step := min(time.Second, elapsed)
		clock.Advance(step)
		check(t, q, peer, nil)
		elapsed -= step
	}
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
			advanceReportingPeer(t, q, clock, peer, allowance-time.Nanosecond)
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

// #1450: a worker's check-in loop can stall on local work (a slow fsync under
// host load) while the process is alive. Its stop-event poll runs separately;
// while that connection is open, the worker's running work and model streams
// are not charged a loss. Suspicion still withholds new work, and once the
// connection drops the measured allowance applies again.
func TestControlClockLiveEventPollIsNotWorkerLoss(t *testing.T) {
	q, clock, worker, peer, job := adaptiveLossFixture(t, "production")
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		// Re-poll as the worker's watchStops does after each bounded wait.
		client := Client{URL: server.URL, Token: "test"}
		for ctx.Err() == nil {
			var event WorkerEvent
			_ = client.Call(ctx, "GET", "/api/workers/"+worker.ID+"/events?session="+worker.Session, nil, &event)
		}
	}()
	polling := func() bool {
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		p := q.workerPolls[worker.ID][worker.Session]
		return p != nil && p.open > 0
	}
	await(t, polling)
	q.mu.Lock()
	allowance := q.machineLossTimeout(worker.ID)
	q.mu.Unlock()
	advanceReportingPeer(t, q, clock, peer, 3*allowance)
	q.mu.Lock()
	gone, suspect, got := q.s.Machines[worker.ID].Gone, q.workerSuspect(q.s.Machines[worker.ID], clock.Now()), *q.s.Jobs[job.ID]
	q.mu.Unlock()
	if gone || got.State != "running" || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Attempt != job.Attempt {
		t.Fatalf("live worker with stalled check-ins charged a loss: gone=%v state=%s losses=%d retries=%d attempt=%d", gone, got.State, got.Losses, got.InfrastructureRetries, got.Attempt)
	}
	if !suspect {
		t.Fatal("stalled check-ins must still withhold new work")
	}
	cancel()
	<-polled
	await(t, func() bool { return !polling() })
	advanceReportingPeer(t, q, clock, peer, allowance)
	q.mu.Lock()
	gone, got = q.s.Machines[worker.ID].Gone, *q.s.Jobs[job.ID]
	q.mu.Unlock()
	if !gone || got.Losses != 1 {
		t.Fatalf("closed connection did not restore loss detection: gone=%v losses=%d", gone, got.Losses)
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
	if _, err := w.fixtureCheckin(context.Background(), Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	if got := q.machineLossTimeout(m.ID); got != 14*time.Second {
		t.Fatalf("HTTP lost the actual worker interval: %s", got)
	}
}

// Successive arrivals establish the wall delivery cadence. Applying only an
// opportunity-clock allowance to wall age can otherwise withhold every offer
// when queue processing is slower than the sender's interval.
func TestControlClockSlowControlPathDoesNotStarveAdmission(t *testing.T) {
	q, clock := fixture(t)
	if err := q.SetLossTimeouts(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	m := machine("worker", "fake")
	j := submit(t, q, spec("production"))
	period, service := 5*time.Millisecond, 31*time.Millisecond
	var reply Reply
	for range 2 {
		contact, settle := q.trackWorkerCheckin(m.ID, m.Session, period, true)
		q.mu.Lock()
		clock.Advance(service)
		q.mu.Unlock()
		var err error
		c := keyedFixtureCheckin(Checkin{Machine: m, period: period, contact: contact})
		reply, err = q.checkinContext(context.Background(), c)
		reply = fixtureReply(c, reply)
		settle(err == nil)
		if err != nil {
			t.Fatal(err)
		}
		// Response decoding/local reconciliation also precedes the next send.
		// A second interval outside q.mu exercises cold recovery inference.
		clock.Advance(2 * period)
	}
	q.mu.Lock()
	fresh := q.workerFresh(q.s.Machines[m.ID], clock.Now())
	launchAllowed := q.hostLaunchAllowed(q.s.Machines[m.ID], m.Session)
	lossTimeout := q.machineLossTimeout(m.ID)
	q.contactMu.Lock()
	cadence := q.contacts[m.ID].cadence.Mean
	q.contactMu.Unlock()
	q.mu.Unlock()
	if reply.Job == nil || reply.Job.ID != j.ID || !fresh {
		t.Fatal("measured slow control path withheld a live worker")
	}
	if !launchAllowed || cadence == 0 {
		t.Fatal("cold control cost repeatedly discarded healthy arrival cadence")
	}
	if err := q.AuthorizeLaunch(HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(reply.Job.Attempt)}); err != nil {
		t.Fatal("accepted slow contact could not authorize its offer", err)
	}
	if got := lossTimeout; got >= service {
		t.Fatalf("wall service cost inflated confirmed-loss opportunities: %s", got)
	}
	// Opportunities alone cannot confirm loss before the actual delivered
	// cadence is missed. This is ordinary available service after a slow reply.
	clock.Advance(lossTimeout)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	prematureLoss := q.s.Machines[m.ID].Gone || q.s.Jobs[j.ID].Losses != 0 || !q.workerFresh(q.s.Machines[m.ID], clock.Now())
	q.mu.Unlock()
	if prematureLoss {
		t.Fatal("available service charged a worker before its expected arrival")
	}
	// Learning this completed path must still leave a finite wall-silence
	// bound for a sender that then dies during continuous queue saturation.
	q.mu.Lock()
	clock.Advance(4 * (service + period))
	q.maintain()
	suspect := q.workerSuspect(q.s.Machines[m.ID], clock.Now())
	q.mu.Unlock()
	got := job(t, q, j.ID)
	if !suspect || got.State != "running" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatal("dead sender did not become suspect without a false charge", got)
	}
}

func TestControlClockSuspectPreparationRetainsStartedCustody(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	live := q.s.Machines[m.ID]
	launch := q.issuePreparation(live, "environment", "started")
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	started := q.s.PreparationPlans[launch.Attempt].StartedAt
	clock.Advance(q.workerArrivalAllowance(live))
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if !q.workerSuspect(live, clock.Now()) || live.Gone {
		t.Fatal("fixture did not preserve suspect custody")
	}
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal("suspected contact interrupted started preparation", err)
	}
	if got := q.s.PreparationPlans[launch.Attempt]; !got.Started || got.StartedAt == nil || !got.StartedAt.Equal(*started) {
		t.Fatal("continuation renewed preparation custody", got)
	}
	cold := q.issuePreparation(live, "environment", "unstarted")
	if err := q.AuthorizeLaunch(cold); !errors.Is(err, ErrStale) {
		t.Fatal("suspect contact authorized a new preparation", err)
	}
}

func TestControlClockLegacyFirstWorkAfterOneInterval(t *testing.T) {
	q, clock := fixture(t)
	if err := q.SetLossTimeouts(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	api := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Del(checkinPeriodHeader) // Older workers never advertised cadence.
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	w := Worker{Machine: machine("legacy", "fake"), Interval: 12 * time.Second, Client: Client{URL: server.URL, Token: "test"}}
	j := submit(t, q, spec("production"))
	c := Checkin{Machine: w.Machine}
	first, err := w.fixtureCheckin(context.Background(), c)
	if err != nil || first.Job != nil || len(first.Attempts) != 0 {
		t.Fatalf("empty idle reply did not remain idle: %+v %v", first, err)
	}
	clock.Advance(w.Interval)
	second, err := w.fixtureCheckin(context.Background(), c)
	if err != nil || second.Job == nil || second.Job.ID != j.ID || len(second.Attempts) != 1 {
		t.Fatalf("legacy work waited beyond one observed interval: %+v %v", second, err)
	}
	if q.machineLossTimeout(w.Machine.ID) < 2*w.Interval || q.s.Jobs[j.ID].Losses != 0 || q.s.Jobs[j.ID].InfrastructureRetries != 0 {
		t.Fatal("legacy admission lacked its measured silence allowance")
	}
}

func TestControlClockLegacyRejoinRetainsEngine(t *testing.T) {
	for _, state := range []string{"loading", "ready"} {
		t.Run(state, func(t *testing.T) {
			q, clock, m := modelFixture(t)
			call := addModelCall(t, q, "production")
			reportModel(t, q, m, state)
			if state == "ready" {
				if err := q.finishCall(call.ID); err != nil {
					t.Fatal(err)
				}
			}
			slot := *q.s.ModelSlots[slotKey(m.ID, "test")]
			config, root := q.models, q.root
			var path string
			if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
				t.Fatal(err)
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			q, err = Open(path, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			if err = q.ConfigureModels(config); err != nil {
				t.Fatal(err)
			}
			// The first old-wire report after a queue-first upgrade has no
			// known cadence. It must preserve its live engine, without offering
			// a fresh engine start or another preparation.
			r, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: slot.Attempt, State: state}}})
			if err != nil || !q.workerSuspect(q.s.Machines[m.ID], clock.Now()) || len(r.ModelPlan) != 1 || r.ModelPlan[0].Attempt != slot.Attempt || r.ModelPlan[0].State != "ready" || r.Job != nil || len(r.EnvironmentPlan) != 0 {
				t.Fatalf("cold legacy rejoin interrupted custody or offered a start: %+v %v", r, err)
			}
		})
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
	c.end(func() {})
	clock.Advance(time.Second)
	c.end(func() {})
	clock.Advance(time.Second)
	if got := c.now(); got != 2*time.Second {
		t.Fatalf("overlapping occupancy counted twice: %s", got)
	}
}

type heldCheckinResponse struct {
	header  http.Header
	entered chan struct{}
	release chan struct{}
}

func (w *heldCheckinResponse) Header() http.Header { return w.header }
func (w *heldCheckinResponse) WriteHeader(int)     {}
func (w *heldCheckinResponse) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(p), nil
}

func TestControlClockFirstSessionResponseProtected(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(map[bool]string{false: "first", true: "replacement"}[replacement], func(t *testing.T) {
			q, clock := fixture(t)
			if err := q.SetLossTimeouts(0, 0, 0); err != nil {
				t.Fatal(err)
			}
			m, peer := machine("worker", "fake"), machine("peer", "fake")
			check(t, q, peer, nil)
			c := Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: m}
			if replacement {
				check(t, q, m, nil)
				c.Machine.Session = "replacement-session"
				c.Fresh, c.PreviousSession = true, m.Session
			}
			s := spec("production")
			s.AllowedMachines = []string{m.ID}
			j := submit(t, q, s)
			body, _ := json.Marshal(c)
			r := httptest.NewRequest("POST", "/api/workers/checkin", bytes.NewReader(body))
			r.Header.Set("Authorization", "Bearer test")
			r.Header.Set("X-Myce-Start-Gate", startGateProtocol)
			r.Header.Set(checkinPeriodHeader, "1s")
			w := &heldCheckinResponse{header: http.Header{}, entered: make(chan struct{}), release: make(chan struct{})}
			done := make(chan struct{})
			go func() { defer close(done); (API{Queue: q, Token: "test"}.Handler()).ServeHTTP(w, r) }()
			defer func() { close(w.release); <-done }()
			<-w.entered
			if q.s.Jobs[j.ID].State != "running" {
				t.Fatal("no initial assignment")
			}
			clock.Advance(3 * time.Second)
			check(t, q, peer, nil)
			got := q.s.Jobs[j.ID]
			if q.s.Machines[m.ID].Gone || got.Losses != 0 || got.InfrastructureRetries != 0 || got.State != "running" {
				t.Fatalf("first-session response became worker silence: %+v", got)
			}
		})
	}
}

func TestControlClockLegacyWallCadenceSurvivesContention(t *testing.T) {
	q, clock := fixture(t)
	if err := q.SetLossTimeouts(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	m := machine("legacy", "fake")
	for range 40 {
		if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		clock.Advance(900 * time.Millisecond)
		q.mu.Unlock()
		clock.Advance(100 * time.Millisecond)
	}
	if got := q.workerLossAllowance(q.s.Machines[m.ID]); got < 2*time.Second {
		t.Fatalf("cadence compressed to %s", got)
	}
	s := spec("production")
	s.AllowedMachines = []string{m.ID}
	submit(t, q, s)
	r, err := q.Checkin(Checkin{Machine: m})
	if err != nil || r.Job == nil {
		t.Fatal("no legacy assignment", err)
	}
	if err := q.Started(result(r.Job, true)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(500 * time.Millisecond)
	check(t, q, machine("peer", "fake"), nil)
	if q.s.Machines[m.ID].Gone || q.s.Jobs[r.Job.ID].Losses != 0 {
		t.Fatal("contention compressed the worker's send interval")
	}
}

func TestControlClockOperatorMinimumCannotQualifyLegacy(t *testing.T) {
	q, clock := fixture(t) // Historical positive operator minimums.
	m := machine("legacy", "fake")
	s := spec("production")
	s.AllowedMachines = []string{m.ID}
	submit(t, q, s)
	r, err := q.Checkin(Checkin{Machine: m})
	if err != nil || r.Job != nil {
		t.Fatal("operator minimum qualified unknown cadence", err)
	}
	clock.Advance(12 * time.Second)
	check(t, q, machine("peer", "fake"), nil)
	if q.s.Machines[m.ID].Gone {
		t.Fatal("operator minimum lost an unqualified slow sender")
	}
	if _, err = q.Checkin(Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(12 * time.Second)
	r, err = q.Checkin(Checkin{Machine: m})
	if err != nil || r.Job == nil {
		t.Fatal("measured slow sender did not qualify", err)
	}
	if q.machineLossTimeout(m.ID) < 24*time.Second {
		t.Fatal("operator minimum overrode measured cadence")
	}
}

func TestControlClockCachedPreparationCannotStartSuspect(t *testing.T) {
	q, clock, worker, _, job := adaptiveLossFixture(t, "production")
	q.mu.Lock()
	l := q.issuePreparation(q.s.Machines[worker.ID], "environment", "cached")
	q.mu.Unlock()
	clock.Advance(q.machineLossTimeout(worker.ID))
	if err := q.AuthorizeLaunch(l); err == nil {
		t.Fatal("cached preparation started while suspect")
	}
	batch := HostLaunch{Kind: "batch", Machine: worker.ID, Session: worker.Session, Key: job.ID, Attempt: strconv.Itoa(job.Attempt)}
	if err := q.AuthorizeLaunch(batch); err != nil {
		t.Fatal("suspicion revoked an existing assigned batch", err)
	}
}

func TestControlClockLastReconnectDoesNotLearnOutage(t *testing.T) {
	for _, tick := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-maintenance", true: "after-maintenance"}[tick], func(t *testing.T) {
			q, clock, worker, peer, _ := adaptiveLossFixture(t, "production")
			clock.Advance(time.Hour)
			if tick {
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if !q.networkQuiet {
					t.Fatal("fixture missed outage")
				}
			}
			check(t, q, worker, nil)
			check(t, q, peer, nil) // This report clears the recovery epoch.
			allowance := q.machineLossTimeout(peer.ID)
			if allowance > 3*time.Second || q.machineLossTimeout(worker.ID) > 3*time.Second {
				t.Fatalf("reconnect learned outage: first=%s last=%s", q.machineLossTimeout(worker.ID), allowance)
			}
			advanceReportingPeer(t, q, clock, worker, allowance)
			if !q.s.Machines[peer.ID].Gone {
				t.Fatal("last reconnected worker hid a new genuine loss")
			}
		})
	}
}

func TestControlClockFailedSessionReplacementRestoresLossDetection(t *testing.T) {
	q, clock, worker, peer, job := adaptiveLossFixture(t, "production")
	allowance := q.machineLossTimeout(worker.ID)
	if _, err := q.db.Exec(`CREATE TRIGGER contact_fail BEFORE UPDATE ON queue_state BEGIN SELECT RAISE(ABORT,'contact failure'); END`); err != nil {
		t.Fatal(err)
	}
	replacement := worker
	replacement.Session = "replacement-session"
	if _, err := q.Checkin(Checkin{Machine: replacement, Fresh: true, PreviousSession: worker.Session, period: time.Second}); err == nil {
		t.Fatal("failed commit accepted replacement")
	}
	if q.s.Machines[worker.ID].Session != worker.Session || q.machineLossTimeout(worker.ID) != allowance {
		t.Fatal("failed replacement disabled the restored session's loss detector")
	}
	if _, err := q.db.Exec(`DROP TRIGGER contact_fail`); err != nil {
		t.Fatal(err)
	}
	advanceReportingPeer(t, q, clock, peer, allowance)
	got := q.s.Jobs[job.ID]
	if !q.s.Machines[worker.ID].Gone || got.Losses != 1 || got.InfrastructureRetries != 1 || got.State != "queued" {
		t.Fatalf("restored dead session stranded its attempt: %+v", got)
	}
}

func TestControlClockConcurrentReconnectsDoNotLearnOutage(t *testing.T) {
	q, clock, worker, peer, job := adaptiveLossFixture(t, "production")
	allowance := q.machineLossTimeout(worker.ID)
	clock.Advance(time.Hour)
	ctx := context.Background()
	a, settleA := q.trackWorkerCheckin(worker.ID, worker.Session, time.Second, true, ctx)
	b, settleB := q.trackWorkerCheckin(peer.ID, peer.Session, time.Second, true, ctx)
	defer settleA(false)
	defer settleB(false)
	// Both arrivals exist before either serialized transaction publishes. A late
	// pending reconnect must not masquerade as a survivor from before the outage.
	c := keyedFixtureCheckin(Checkin{Machine: worker, Running: &Execution{ID: job.ID, Attempt: job.Attempt}, period: time.Second, contact: a})
	r, err := q.checkinContext(ctx, c)
	r = fixtureReply(c, r)
	if err != nil || r.Stop {
		t.Fatal("first returning attempt lost custody", err)
	}
	settleA(true)
	if _, err := q.checkinContext(ctx, keyedFixtureCheckin(Checkin{Machine: peer, period: time.Second, contact: b})); err != nil {
		t.Fatal(err)
	}
	settleB(true)
	if q.machineLossTimeout(worker.ID) > allowance || q.machineLossTimeout(peer.ID) > allowance {
		t.Fatal("concurrent reconnects trained outage as cadence")
	}
	clock.Advance(allowance - time.Nanosecond)
	if q.workerSuspect(q.s.Machines[worker.ID], clock.Now()) {
		t.Fatal("worker became suspect before its measured allowance")
	}
	// The survivor keeps reporting while the other worker genuinely stops.
	check(t, q, peer, nil)
	clock.Advance(time.Nanosecond)
	if !q.workerSuspect(q.s.Machines[worker.ID], clock.Now()) {
		t.Fatal("outage hid bounded suspicion of a new genuine failure")
	}
	check(t, q, peer, nil)
	got := q.s.Jobs[job.ID]
	if !q.s.Machines[worker.ID].Gone || got.Losses != 1 || got.InfrastructureRetries != 1 {
		t.Fatal("outage hid prompt confirmation of a new genuine failure")
	}
}

func TestControlClockNetworkRecoveryExcludesQueueStall(t *testing.T) {
	for _, delay := range []time.Duration{31 * time.Second, 90 * time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			q, clock, worker, peer, job := adaptiveLossFixture(t, "production")
			lastSeen := q.s.Machines[worker.ID].LastSeen
			clock.Advance(time.Hour)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			check(t, q, peer, nil)
			if q.s.NetworkRecoveryAt.IsZero() {
				t.Fatal("reconnect missed the recovery epoch")
			}
			q.mu.Lock()
			clock.Advance(delay)
			q.maintain()
			q.mu.Unlock()
			got := q.s.Jobs[job.ID]
			if q.s.Machines[worker.ID].Gone || got.State != "running" || got.Losses != 0 || got.InfrastructureRetries != 0 || !q.s.Machines[worker.ID].LastSeen.Equal(lastSeen) {
				t.Fatalf("blocked recovery charged false loss or invented a heartbeat: %+v", got)
			}
			if !q.workerSuspect(q.s.Machines[worker.ID], clock.Now()) {
				t.Fatal("recovery fabricated healthy status")
			}
			advanceReportingPeer(t, q, clock, peer, q.machineLossTimeout(worker.ID))
			got = q.s.Jobs[job.ID]
			if !q.s.Machines[worker.ID].Gone || got.Losses != 1 || got.InfrastructureRetries != 1 {
				t.Fatalf("recovery hid real loss after service resumed: %+v", got)
			}
		})
	}
}

func TestControlClockDetachedSourcesPreserveContact(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	capture := func() []*Queue {
		t.Helper()
		read, _, err := q.captureReadObservation("", nil)
		if err != nil {
			t.Fatal(err)
		}
		q.mu.Lock()
		advice := q.captureSubmissionAdvice()
		q.mu.Unlock()
		view, err := advice.planner(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return []*Queue{read, view}
	}
	if q.readGeneration == nil || !q.workerFresh(q.s.Machines[m.ID], clock.Now()) {
		t.Fatal("healthy committed worker required")
	}
	before := capture()
	for _, view := range before {
		if !view.workerFresh(view.s.Machines[m.ID], view.clock.Now()) {
			t.Fatal("detached source discarded healthy contact")
		}
	}
	q.mu.Lock()
	clock.Advance(q.workerArrivalAllowance(q.s.Machines[m.ID]))
	q.mu.Unlock()
	if !q.workerSuspect(q.s.Machines[m.ID], clock.Now()) {
		t.Fatal("wall suspicion required")
	}
	for _, view := range capture() {
		if !view.workerSuspect(view.s.Machines[m.ID], view.clock.Now()) {
			t.Fatal("detached source renewed old contact")
		}
	}
	for _, view := range before {
		if !view.workerFresh(view.s.Machines[m.ID], view.clock.Now()) {
			t.Fatal("later wall time changed frozen contact")
		}
	}
}

func TestControlClockDetachedSourcesPreserveRestartGrace(t *testing.T) {
	q, clock := fixture(t)
	check(t, q, machine("worker", "fake"), nil)
	q = reopenQueue(t, q, clock)
	q.mu.Lock()
	clock.Advance(2 * RestartGrace)
	q.mu.Unlock()
	if !q.inRestartGrace(clock.Now()) {
		t.Fatal("blocked service consumed live restart grace")
	}
	read, _, err := q.captureReadObservation("", nil)
	if err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	advice := q.captureSubmissionAdvice()
	q.mu.Unlock()
	view, err := advice.planner(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []*Queue{read, view} {
		if !v.inRestartGrace(v.clock.Now()) {
			t.Fatal("detached source lost remaining service grace")
		}
	}
}
