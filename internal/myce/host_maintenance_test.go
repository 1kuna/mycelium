package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func maintenanceMachines() (Machine, Machine) {
	a, b := machine("gpu0", "fake"), machine("gpu1", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "host"
		m.Device = m.ID
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 32
	}
	return a, b
}

func TestHostMaintenanceLastResortPersistsAcrossWorkerSessions(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	a, b := maintenanceMachines()
	check(t, q, a, nil)
	check(t, q, b, nil)
	lastResortPolicy(t, q, a.HostID, true)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	previous := a.Session
	a.Session = "new-session"
	if _, err := q.Checkin(Checkin{Machine: a, Fresh: true, PreviousSession: previous}); err != nil {
		t.Fatal(err)
	}
	check(t, q, b, nil)
	status, err := q.HostMaintenanceStatus(a.HostID)
	if err != nil || !status.Policy.LastResort || status.Policy.Mode != "enabled" {
		t.Fatal("restart or check-in erased queue-owned policy", status, err)
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range snap.Machines {
		if !m.LastResort {
			t.Fatal("physical host policy did not cover sibling worker", m.ID)
		}
	}
	lastResortPolicy(t, q, a.HostID, false)
	// Incoming status-only fields cannot promote or demote the physical host.
	a.LastResort = true
	check(t, q, a, nil)
	snap, err = q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range snap.Machines {
		if m.LastResort {
			t.Fatal("worker changed queue-owned placement policy", m.ID)
		}
	}
}
func TestHostMaintenancePlacementAndBudgetPreserveDrainOwner(t *testing.T) {
	q, clock := fixture(t)
	a, _ := maintenanceMachines()
	check(t, q, a, nil)
	drained, err := q.ChangeHostMaintenance(a.HostID, HostMaintenanceRequest{Action: "drain", DrainOwner: "upgrade"})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	gib := 12.0
	if _, err := q.ChangeHostMaintenance(a.HostID, HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &gib}); err != nil {
		t.Fatal(err)
	}
	// MY-45 roles: services is accepted without disturbing the drain; an
	// unknown role, a role riding on another action, and interactive without
	// the shared profile it implies are refused.
	for _, r := range []struct {
		request HostMaintenanceRequest
		ok      bool
	}{
		{HostMaintenanceRequest{Action: "role", Role: roleRef("services")}, true},
		{HostMaintenanceRequest{Action: "role", Role: roleRef("gpu")}, false},
		{HostMaintenanceRequest{Action: "role", Role: roleRef("interactive")}, false},
		{HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &gib, Role: roleRef("node")}, false},
	} {
		clock.Advance(time.Second)
		status, err := q.ChangeHostMaintenance(a.HostID, r.request)
		if (err == nil) != r.ok || r.ok && status.Policy.Role != *r.request.Role {
			t.Fatal("role request", *r.request.Role, r.request.Action, status.Policy, err)
		}
	}
	for _, enabled := range []bool{true, false} {
		clock.Advance(time.Second)
		lastResortPolicy(t, q, a.HostID, enabled)
		status, err := q.HostMaintenanceStatus(a.HostID)
		if err != nil || status.Policy.Mode != "drained" || status.Policy.DrainOwner != drained.Policy.DrainOwner || !status.Policy.UpdatedAt.Equal(drained.Policy.UpdatedAt) || status.Policy.LastResort != enabled {
			t.Fatal("placement or allowance edit invalidated drain receipt", status.Policy, err)
		}
	}
	if _, err := q.ChangeHostMaintenance(a.HostID, HostMaintenanceRequest{Action: "resume", IfMode: "drained", IfUpdatedAt: &drained.Policy.UpdatedAt, IfDrainOwner: drained.Policy.DrainOwner}); err != nil {
		t.Fatal("drain owner could not conditionally resume after placement edit", err)
	}
}

func maintenanceChange(t *testing.T, q *Queue, action string, stop bool) HostMaintenanceStatus {
	t.Helper()
	status, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: action, Stop: stop})
	if err != nil {
		t.Fatal(err)
	}
	return status
}
func maintenanceAssign(t *testing.T, q *Queue, m Machine, priority string) *Job {
	t.Helper()
	s := spec(priority)
	s.AllowedMachines = []string{m.ID}
	s.Needs.HostMemoryGB = 2
	submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("no assignment", m.ID)
	}
	return j
}
func TestMaintenanceDrainBothDevicesAndOtherHost(t *testing.T) {
	q, _ := fixture(t)
	a, b := maintenanceMachines()
	active := maintenanceAssign(t, q, a, "production")
	if err := q.Started(result(active, false)); err != nil {
		t.Fatal(err)
	}
	check(t, q, b, nil)
	status := maintenanceChange(t, q, "drain", false)
	if status.Policy.Mode != "drained" || len(status.Attempts) != 1 || status.Quiescent {
		t.Fatal(status)
	}
	r := check(t, q, a, active)
	if r.Stop || r.Leave || !r.AdmissionBlocked {
		t.Fatal("drain interrupted active attempt", r)
	}
	waiting := submit(t, q, spec("experiment"))
	if check(t, q, b, nil).Job != nil {
		t.Fatal("sibling admitted during drain")
	}
	other := machine("other", "fake")
	if r := check(t, q, other, nil); r.Job == nil || r.Job.ID != waiting.ID {
		t.Fatal("other host fenced", r)
	}
	finish(t, q, active, true)
	status, err := q.HostMaintenanceStatus("host")
	if err != nil || !status.Quiescent {
		t.Fatal(status, err)
	}
	maintenanceChange(t, q, "resume", false)
	submit(t, q, spec("experiment"))
	if check(t, q, b, nil).Job == nil {
		t.Fatal("resume did not restore sibling")
	}
}
func TestMaintenanceAllocationAndStartedRaces(t *testing.T) {
	for _, action := range []string{"drain", "disable"} {
		t.Run(action, func(t *testing.T) {
			q, _ := fixture(t)
			a, b := maintenanceMachines()
			check(t, q, a, nil)
			check(t, q, b, nil)
			for i := 0; i < 8; i++ {
				submit(t, q, spec("experiment"))
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			for _, m := range []Machine{a, b} {
				wg.Add(1)
				go func(m Machine) {
					defer wg.Done()
					<-start
					reply, err := q.Checkin(Checkin{Machine: m})
					if err != nil {
						t.Error(err)
						return
					}
					if reply.Job != nil {
						err = q.Started(result(reply.Job, false))
						if err != nil && !errors.Is(err, ErrStale) {
							t.Error(err)
						}
					}
				}(m)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: action, Stop: action == "disable"})
				if err != nil {
					t.Error(err)
				}
			}()
			close(start)
			wg.Wait()
			for _, m := range []Machine{a, b} {
				q.mu.Lock()
				for _, j := range q.s.Jobs {
					if j.Machine == m.ID && j.State == "running" && j.StartedAt == nil {
						t.Error("unstarted allocation escaped drain")
					}
				}
				q.mu.Unlock()
				if r := check(t, q, m, nil); r.Job != nil {
					t.Fatal("redelivered a fenced assignment", r.Job)
				}
			}
		})
	}
}
func TestMaintenanceCancelVersusStarted(t *testing.T) {
	for _, maintenance := range []bool{false, true} {
		t.Run(fmt.Sprint(maintenance), func(t *testing.T) {
			q, _ := fixture(t)
			a, _ := maintenanceMachines()
			j := maintenanceAssign(t, q, a, "production")
			var wg sync.WaitGroup
			start := make(chan struct{})
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				err := q.Started(result(j, false))
				if err != nil && !errors.Is(err, ErrStale) {
					t.Error(err)
				}
			}()
			go func() {
				defer wg.Done()
				<-start
				if _, err := q.CancelJob(j.ID); err != nil {
					t.Error(err)
				}
			}()
			if maintenance {
				maintenanceChange(t, q, "disable", true)
			}
			close(start)
			wg.Wait()
			if err := q.Started(result(j, false)); !errors.Is(err, ErrStale) {
				t.Fatal(err)
			}
			r := result(j, false)
			r.Interrupted = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.State != "cancelled" || got.InfrastructureRetries != 0 {
				t.Fatal(got)
			}
		})
	}
}
func TestMaintenancePersistenceRejoinAndMemoryDebt(t *testing.T) {
	q, c := fixture(t)
	a, b := maintenanceMachines()
	a.HostMemoryBudgetGB = 8
	b.HostMemoryBudgetGB = 8
	j := maintenanceAssign(t, q, a, "experiment")
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	check(t, q, b, nil)
	maintenanceChange(t, q, "drain", false)
	value := 1.0
	status, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &value})
	if err != nil || status.Policy.MemoryBudget.Bytes != 1<<30 || status.MemoryDebtGB != 1 || status.MemoryInUseGB != 2 {
		t.Fatal(status, err)
	}
	path, root := "", q.root
	if err = q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	status, err = q.HostMaintenanceStatus("host")
	if err != nil || status.Policy.Mode != "drained" || status.Policy.MemoryBudget.GiB != 1 || status.Quiescent || status.Healthy || len(status.Attempts) != 1 {
		t.Fatal(status, err)
	}
	old := a.Session
	a.Session = "rejoined"
	a.HostMemoryBudgetGB = 1
	b.HostMemoryBudgetGB = 1
	if r, err := q.Checkin(Checkin{Machine: a, Fresh: true, PreviousSession: old, period: time.Second}); err != nil || r.Job != nil || !r.AdmissionBlocked {
		t.Fatal(r, err)
	}
	check(t, q, b, nil)
	status, err = q.HostMaintenanceStatus("host")
	if err != nil || !status.Quiescent {
		t.Fatal(status, err)
	}
	if _, err = q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "resume", MemoryBudgetGB: &value}); err == nil {
		t.Fatal("changed allowance while enabled")
	}
	value = 8
	if _, err = q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &value}); err != nil {
		t.Fatal(err)
	}
	a.HostMemoryBudgetGB, b.HostMemoryBudgetGB = 8, 8
	check(t, q, a, nil)
	check(t, q, b, nil)
	maintenanceChange(t, q, "resume", false)
	if reply := check(t, q, a, nil); reply.Job == nil {
		t.Fatalf("rejoined host did not resume: reply=%+v job=%+v", reply, job(t, q, j.ID))
	}
}
func TestMaintenanceBudgetMismatchAndStarted(t *testing.T) {
	q, _ := fixture(t)
	a, b := maintenanceMachines()
	check(t, q, a, nil)
	check(t, q, b, nil)
	if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: new(float64)}); err == nil {
		t.Fatal("enabled allowance change")
	}
	maintenanceChange(t, q, "disable", false)
	value := 4.0
	if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &value}); err != nil {
		t.Fatal(err)
	}
	a.HostMemoryBudgetGB = 4
	check(t, q, a, nil)
	maintenanceChange(t, q, "resume", false)
	// b has not acknowledged yet. Each worker enforces its own acknowledged
	// allowance, so admission continues within the lower of the new policy and
	// every acknowledgement instead of pausing; readiness still reports it.
	status, err := q.HostMaintenanceStatus("host")
	if err != nil || status.Healthy || !status.Workers[0].Machine.HostMemoryPolicyMismatch {
		t.Fatal("pending acknowledgement not reported", status, err)
	}
	b.HostMemoryBudgetGB = 3 // A lower acknowledged allowance binds at once.
	check(t, q, b, nil)
	if got := q.s.Machines[a.ID].EffectiveHostMemoryBudgetGB; got != 3 {
		t.Fatal("effective allowance is not the lower acknowledgement", got)
	}
	s := spec("experiment")
	s.Needs.HostMemoryGB = 2
	submit(t, q, s)
	j := check(t, q, a, nil).Job
	if j == nil {
		j = check(t, q, b, nil).Job
	}
	if j == nil {
		t.Fatal("pending acknowledgement paused admission")
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal("pending acknowledgement refused a start within both allowances", err)
	}
	big := spec("experiment")
	big.Needs.HostMemoryGB = 1.5 // 2.125 + 1.625 > 3 acknowledged, < 4 policy.
	submit(t, q, big)
	owner, other := a, b
	if j.Machine == b.ID {
		owner, other = b, a
	}
	if check(t, q, owner, j).Job != nil || check(t, q, other, nil).Job != nil {
		t.Fatal("admitted above an acknowledged allowance", q.s.Machines[a.ID].FreeHostMemoryGB)
	}
	b.HostMemoryBudgetGB = 4
	var running *Job
	if j.Machine == b.ID {
		running = j
	}
	check(t, q, b, running)
	if got := q.s.Machines[a.ID].EffectiveHostMemoryBudgetGB; got != 4 || q.s.Machines[a.ID].HostMemoryPolicyMismatch {
		t.Fatal("acknowledged policy did not apply", got)
	}
	if status, err := q.HostMaintenanceStatus("host"); err != nil || status.Workers[0].Machine.HostMemoryPolicyMismatch {
		t.Fatal("acknowledged policy still pending", status, err)
	}
}
func TestMaintenancePlannedStopsDoNotConsumeRetries(t *testing.T) {
	for _, mode := range []string{"complete", "lost", "restart", "timeout", "pending"} {
		t.Run(mode, func(t *testing.T) {
			q, c := fixture(t)
			a, b := maintenanceMachines()
			if mode == "timeout" {
				if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
					t.Fatal(err)
				}
			}
			check(t, q, b, nil)
			for iteration := 0; iteration < 1; iteration++ {
				j := maintenanceAssign(t, q, a, "production")
				// Keep one job for each sequence; each undergoes repeated planned stops.
				for attempt := 0; attempt < 4; attempt++ {
					if err := q.Started(result(j, false)); err != nil {
						t.Fatal(err)
					}
					maintenanceChange(t, q, "disable", true)
					switch mode {
					case "lost":
						c.Advance(ProductionLossAfter)
						check(t, q, b, nil)
					case "restart":
						old := a.Session
						a.Session = ID()
						if _, err := q.Checkin(Checkin{Machine: a, Fresh: true, PreviousSession: old}); err != nil {
							t.Fatal(err)
						}
					case "timeout":
						c.Advance(StopGrace)
						check(t, q, b, nil)
					default:
						r := result(j, false)
						r.Interrupted = true
						if mode == "pending" {
							r.ReleasePending = true
						}
						if err := q.Complete(r); err != nil {
							t.Fatal(err)
						}
						if mode == "pending" {
							r.ReleasePending = false
							r.ReleaseOnly = true
							if err := q.Complete(r); err != nil {
								t.Fatal(err)
							}
						}
					}
					got := job(t, q, j.ID)
					if got.InfrastructureRetries != 0 || got.Losses != 0 || got.State != "queued" {
						t.Fatal("planned stop spent a retry", got)
					}
					// Exact cleanup precedes reuse; lateness never publishes a stale result.
					r := result(j, true)
					r.ReleaseOnly = true
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
					if err := q.Complete(result(j, true)); !errors.Is(err, ErrStale) {
						t.Fatal("old result accepted", err)
					}
					maintenanceChange(t, q, "resume", false)
					j = check(t, q, a, nil).Job
					if j == nil {
						t.Fatal("no retry")
					}
				}
				finish(t, q, j, true)
			}
		})
	}
}
func TestMaintenanceQueueOutageAndWithdrawnStart(t *testing.T) {
	q, _ := fixture(t)
	a, _ := maintenanceMachines()
	j := maintenanceAssign(t, q, a, "experiment")
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	w := Worker{Machine: a, Client: Client{URL: server.URL, Token: "test"}}
	server.Close()
	if err := w.authorizeStart(*j, a.Session); !errors.Is(err, errWorkerControlUnreachable) {
		t.Fatal("queue outage did not return its typed cause", err)
	}
	maintenanceChange(t, q, "disable", true)
	maintenanceChange(t, q, "resume", false)
	if err := q.Started(result(j, false)); !errors.Is(err, ErrStale) {
		t.Fatal("resume authorized withdrawn attempt", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "resume"}); err == nil {
		t.Fatal("failed durable write accepted")
	}
	if _, err := q.HostMaintenanceStatus("host"); err == nil {
		t.Fatal("closed queue certified health")
	}
}

func TestMaintenancePreparationSidecarsAndFreshness(t *testing.T) {
	q, c := fixture(t)
	a, b := maintenanceMachines()
	check(t, q, a, nil)
	check(t, q, b, nil)
	maintenanceChange(t, q, "drain", false)
	if err := q.update(func() error {
		q.touchMachine(a.ID).EnvironmentReports = []EnvironmentReport{{Key: "build", State: "building"}}
		q.touchMachine(b.ID).CopyReports = []CopyReport{{Model: "weights", State: "copying"}}
		q.s.ModelSlots["engine"] = &ModelSlot{Machine: a.ID, Session: a.Session, Model: "engine", Attempt: "load", State: "ready"}
		if q.s.HostReleases == nil {
			q.s.HostReleases = map[string]HostRelease{}
		}
		r := HostRelease{Host: "host", Machine: b.ID, Session: b.Session, Job: "preparation", Preparation: true}
		q.s.HostReleases[releaseKey(r)] = r
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, err := q.HostMaintenanceStatus("host")
	if err != nil || status.Preparations != 2 || status.Sidecars != 1 || len(status.PendingReleases) != 1 || status.Quiescent {
		t.Fatal(status, err)
	}
	c.Advance(q.machineLossTimeout(a.ID))
	status, err = q.HostMaintenanceStatus("host")
	if err != nil || status.Healthy || status.Workers[0].Fresh || status.Quiescent {
		t.Fatal("stale worker certified", status, err)
	}
	status, err = q.HostMaintenanceStatus("unknown")
	if err != nil || status.Healthy || status.Quiescent {
		t.Fatal("unknown host certified", status, err)
	}
}
func TestMaintenancePlannedCausePersistsAndStageFailureWins(t *testing.T) {
	for _, failure := range []string{"interrupted", "exit", "oom"} {
		t.Run(failure, func(t *testing.T) {
			q, c := fixture(t)
			a, _ := maintenanceMachines()
			j := maintenanceAssign(t, q, a, "production")
			if err := q.Started(result(j, false)); err != nil {
				t.Fatal(err)
			}
			maintenanceChange(t, q, "disable", true)
			path, root := "", q.root
			if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
				t.Fatal(err)
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			q, err = Open(path, root, c)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			got := job(t, q, j.ID)
			if !plannedMaintenanceStop(got) {
				t.Fatal("stop cause lost at restart")
			}
			r := result(j, false)
			r.Interrupted = failure == "interrupted"
			r.OutOfMemory = failure == "oom"
			if err = q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got = job(t, q, j.ID)
			want := "queued"
			if failure != "interrupted" {
				want = "failed"
			}
			if got.State != want || got.InfrastructureRetries != 0 || got.Losses != 0 {
				t.Fatal(got)
			}
		})
	}
}

func TestMaintenanceCauseDoesNotExemptNextAttempt(t *testing.T) {
	q, _ := fixture(t)
	a, _ := maintenanceMachines()
	j := maintenanceAssign(t, q, a, "production")
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	if err := q.SetProductionInfrastructureRetries(0); err != nil {
		t.Fatal(err)
	}
	maintenanceChange(t, q, "disable", true)
	r := result(j, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || got.InfrastructureRetries != 0 {
		t.Fatal("planned stop spent disabled budget", got)
	}
	maintenanceChange(t, q, "resume", false)
	next := check(t, q, a, nil).Job
	if next == nil || next.Attempt == j.Attempt {
		t.Fatal(next)
	}
	r = result(next, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got = job(t, q, j.ID)
	if got.State != "failed" || got.Reason != "infrastructure_retries_exhausted" {
		t.Fatal("planned cause leaked to later attempt", got)
	}
}

// Policy converges through ordinary checkins, even when local bootstrap input
// differs. Maintenance must not add a startup endpoint dependency.
func TestMaintenanceWorkerCheckinPolicy(t *testing.T) {
	q, _ := fixture(t)
	value := 4.0
	if _, err := q.ChangeHostMaintenance("test-host", HostMaintenanceRequest{Action: "drain", MemoryBudgetGB: &value}); err != nil {
		t.Fatal(err)
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/maintenance") {
			t.Error("unexpected startup policy GET")
			http.NotFound(rw, r)
			return
		}
		handler.ServeHTTP(rw, r)
	}))
	defer server.Close()
	w := recoveryWorker(t)
	w.Machine.HostID = "test-host"
	w.Client = Client{URL: server.URL, Token: "test"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	await(t, func() bool {
		snapshot, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range snapshot.Machines {
			if m.ID == w.Machine.ID && m.HostMemoryBudgetGB == value && !m.HostMemoryPolicyMismatch {
				return true
			}
		}
		return false
	})
	// One machine worker owns this host; observe policy through its registered
	// identity rather than introduce a legacy peer beside the running worker.
	snapshot, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var observer Machine
	for _, m := range snapshot.Machines {
		if m.ID == w.Machine.ID {
			observer = cloneMachine(m)
		}
	}
	r := check(t, q, observer, nil)
	if r.HostMaintenance != "drained" || !r.AdmissionBlocked || r.HostMemoryBudget == nil || r.HostMemoryBudget.GiB != value {
		t.Fatal(r)
	}
}

func TestMaintenanceFractionalBudgetDelivery(t *testing.T) {
	q, _ := fixture(t)
	m := machine("fractional", "fake", "fractional-host")
	m.HostMemoryMeasured = true
	m.HostAvailableMemoryGB = 1
	m.HostMemoryBudgetGB = 131071.5 / (1 << 30)
	reply := check(t, q, m, nil)
	if reply.HostMemoryBudget.GiB != m.HostMemoryBudgetGB || reply.HostMemoryBudget.Bytes != 131071 {
		t.Fatal(reply.HostMemoryBudget)
	}
	w := Worker{Machine: m}
	if err := w.checkHostMemoryBudget(0, m.HostMemoryBudgetGB); err != nil {
		t.Fatal("fitting fractional allowance refused", err)
	}
	if err := w.checkHostMemoryBudget(0, 131072.0/(1<<30)); !errors.Is(err, errWorkerMemory) {
		t.Fatal("over-budget allocation accepted", err)
	}
}

// #153: an isolated quarantine must not hide usable neighboring resources.
func TestMaintenanceQuarantinedDeviceDegraded(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.State != "healthy" || !status.Healthy {
		t.Fatal(status, err)
	}
	m.Devices[0].Reason = "device_fault_xe"
	m.Devices[0].FreeMemoryGB = 0
	machineCheck(t, q, m)
	status, err = q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.State != "degraded" || status.Healthy || status.Workers[0].Machine.Devices[0].Reason != "device_fault_xe" {
		t.Fatal(status, err)
	}
	snapshot, err := q.Snapshot()
	if err != nil || snapshot.HostStates[m.HostID] != "degraded" {
		t.Fatal(snapshot, err)
	}
	submit(t, q, spec("production"))
	assigned := assignedJob(machineCheck(t, q, m))
	if assigned == nil || len(assigned.AllocatedDevices) != 1 || assigned.AllocatedDevices[0] != "1" {
		t.Fatal("quarantine must preserve placement on the healthy neighbor", assigned)
	}
	clock.Advance(ProductionLossAfter)
	status, err = q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.State != "unhealthy" || status.Healthy {
		t.Fatal("stale host must remain unhealthy", status, err)
	}
}

// A drain waiting on a running attempt names it in compact status, one line per
// host, from the same Quiescent predicate; the line clears once the host is quiet.
func TestMaintenanceNotQuiescentInCompactStatus(t *testing.T) {
	q, _ := fixture(t)
	a, b := maintenanceMachines()
	active := maintenanceAssign(t, q, a, "experiment")
	if err := q.Started(result(active, false)); err != nil {
		t.Fatal(err)
	}
	check(t, q, b, nil)
	maintenanceChange(t, q, "drain", false)
	read := func() CompactStatus {
		q.statusCache.data, q.statusCache.compact = "", ""
		r := httptest.NewRequest("GET", "/api/status", nil)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
		var view CompactStatus
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &view) != nil {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
		return view
	}
	view := read()
	if len(view.NotQuiescent) != 1 || view.NotQuiescent[0].Host != "host" || view.NotQuiescent[0].Mode != "drained" ||
		len(view.NotQuiescent[0].Attempts) != 1 || view.NotQuiescent[0].Attempts[0].JobID != active.ID || view.NotQuiescent[0].Attempts[0].Attempt != 1 || view.NotQuiescent[0].Attempts[0].State != "running" {
		t.Fatalf("not quiescent: %+v", view.NotQuiescent)
	}
	want := "not quiescent host (drained): job " + ShortID(active.ID) + " " + active.Spec.Name + " attempt 1 running"
	if text := strings.Join(view.Lines(false), "\n"); !strings.Contains(text, want) {
		t.Fatalf("missing %q in:\n%s", want, text)
	}
	finish(t, q, active, true)
	if view = read(); len(view.NotQuiescent) != 0 {
		t.Fatalf("quiet host still listed: %+v", view.NotQuiescent)
	}
}

func roleRef(role string) *string { return &role }
