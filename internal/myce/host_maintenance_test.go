package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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
func TestHostMaintenancePlacementPreservesDrainOwner(t *testing.T) {
	q, clock := fixture(t)
	a, _ := maintenanceMachines()
	check(t, q, a, nil)
	drained, err := q.ChangeHostMaintenance(a.HostID, HostMaintenanceRequest{Action: "drain", DrainOwner: "upgrade"})
	if err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{true, false} {
		clock.Advance(time.Second)
		lastResortPolicy(t, q, a.HostID, enabled)
		status, err := q.HostMaintenanceStatus(a.HostID)
		if err != nil || status.Policy.Mode != "drained" || status.Policy.DrainOwner != drained.Policy.DrainOwner || !status.Policy.UpdatedAt.Equal(drained.Policy.UpdatedAt) || status.Policy.LastResort != enabled {
			t.Fatal("placement edit invalidated drain receipt", status.Policy, err)
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
	if r, err := q.Checkin(Checkin{Machine: a, Fresh: true, PreviousSession: old}); err != nil || r.Job != nil || !r.AdmissionBlocked {
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
	s := spec("experiment")
	s.Needs.HostMemoryGB = 2
	submit(t, q, s)
	if check(t, q, a, nil).Job != nil || check(t, q, b, nil).Job != nil {
		t.Fatal("mismatched cards admitted")
	}
	b.HostMemoryBudgetGB = 4
	j := check(t, q, b, nil).Job
	if j == nil {
		j = check(t, q, a, nil).Job
	}
	if j == nil {
		t.Fatal("matching cards refused")
	}
	b.HostMemoryBudgetGB = 3
	check(t, q, b, nil)
	if err := q.Started(result(j, false)); !errors.Is(err, ErrStale) {
		t.Fatal("Started ignored sibling policy mismatch", err)
	}
	status, err := q.HostMaintenanceStatus("host")
	if err != nil || status.Healthy || !status.Workers[0].Machine.HostMemoryPolicyMismatch {
		t.Fatal(status, err)
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
func TestMaintenancePendingDockerCreateRetainsExactCustody(t *testing.T) {
	q, c := fixture(t)
	a, b := maintenanceMachines()
	a.Capabilities = append(a.Capabilities, "docker")
	a.CapabilityProfile.OS = "linux"
	check(t, q, b, nil)
	s := spec("production")
	s.Run.Docker = true
	s.Run.Command = dockerJob().Spec.Run.Command
	s.Needs.HostMemoryGB = 2
	s.AllowedMachines = []string{a.ID}
	submit(t, q, s)
	j := check(t, q, a, nil).Job
	if j == nil {
		t.Fatal("no Docker assignment")
	}
	a.DockerCleanup = "create pending"
	check(t, q, a, j)
	status := maintenanceChange(t, q, "disable", true)
	if len(status.Attempts) != 1 || len(status.PendingReleases) != 1 || len(status.PendingCleanup) != 1 || status.Quiescent {
		t.Fatal(status)
	}
	if err := q.Started(result(j, false)); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	c.Advance(10 * time.Minute)
	check(t, q, observer(), nil)
	status, err := q.HostMaintenanceStatus("host")
	if err != nil || status.Quiescent || len(status.Attempts) != 1 || len(status.PendingReleases) != 1 || status.MemoryInUseGB != 2 {
		t.Fatal(status, err)
	}
	// Sibling reports and a wrong attempt cannot discharge a pending create.
	check(t, q, b, nil)
	r := result(j, false)
	r.ReleaseOnly = true
	r.Attempt++
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	status, _ = q.HostMaintenanceStatus("host")
	if len(status.PendingReleases) != 1 {
		t.Fatal("wrong attempt discharged create")
	}
	r.Attempt--
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	a.DockerCleanup = ""
	check(t, q, a, nil)
	check(t, q, b, nil)
	status, _ = q.HostMaintenanceStatus("host")
	if !status.Quiescent {
		t.Fatal(status)
	}
}
func TestMaintenanceQueueOutageAndWithdrawnStart(t *testing.T) {
	q, _ := fixture(t)
	a, _ := maintenanceMachines()
	j := maintenanceAssign(t, q, a, "experiment")
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	w := Worker{Machine: a, Client: Client{URL: server.URL, Token: "test"}}
	server.Close()
	if err := w.authorizeStart(*j, a.Session); err == nil {
		t.Fatal("queue outage authorized payload")
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
		q.s.Machines[a.ID].EnvironmentReports = []EnvironmentReport{{Key: "build", State: "building"}}
		q.s.Machines[b.ID].CopyReports = []CopyReport{{Model: "weights", State: "copying"}}
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
	c.Advance(ProductionLossAfter)
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

func TestMaintenanceLateDockerCreateAndPolicyDelivery(t *testing.T) {
	w, store, clock, root := dockerFixture(t)
	q, _ := fixture(t)
	q.clock = clock
	m := w.Machine
	m.HostID = "host"
	m.HostMemoryMeasured = true
	m.HostAvailableMemoryGB = 32
	check(t, q, m, nil)
	maintenanceChange(t, q, "drain", false)
	value := 4.0
	if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &value}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	w.Machine = m
	var policyReply Reply
	if err := w.Client.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: m}, &policyReply); err != nil {
		t.Fatal(err)
	}
	w.Machine.HostMemoryBudgetGB = policyReply.HostMemoryBudget.GiB
	if w.Machine.HostMemoryBudgetGB != 4 {
		t.Fatal("queue allowance not delivered")
	}
	if err := w.checkHostMemoryBudget(0, 5); !errors.Is(err, errWorkerMemory) {
		t.Fatal("queue allowance bypassed", err)
	}
	m = w.Machine
	check(t, q, m, nil)
	maintenanceChange(t, q, "resume", false)
	s := dockerJob().Spec
	s.Priority = "production"
	s.Needs.HostMemoryGB = 2
	submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("no assignment")
	}
	w.CheckpointRoot = q.root
	if err := os.WriteFile(filepath.Join(root, "mode"), []byte("late-create"), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := startDockerFixture(w, store, *j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed: create") })
	m.DockerCleanup = p.docker.status()
	check(t, q, m, j)
	status := maintenanceChange(t, q, "disable", true)
	if status.Quiescent || len(status.PendingReleases) != 1 {
		t.Fatal(status)
	}
	// A scan can be empty while an accepted Docker create is still outstanding.
	objects, err := p.docker.containers()
	if err != nil || len(objects) != 0 {
		t.Fatal(objects, err)
	}
	reply := check(t, q, m, j)
	if !reply.Stop {
		t.Fatal("pending create missed stop")
	}
	p.stop(clock.Now(), 0, true)
	expireDockerStop(t, p)
	status, _ = q.HostMaintenanceStatus("host")
	if status.Quiescent || status.MemoryInUseGB != 2 {
		t.Fatal("empty scan freed custody", status)
	}
	if err = os.WriteFile(filepath.Join(root, "release-late"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case finish := <-p.done:
		if finish.err != nil {
			t.Fatal(finish.err)
		}
		if err = q.Complete(finish.result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late create cleanup did not settle")
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || got.InfrastructureRetries != 0 {
		t.Fatal(got)
	}
	m.DockerCleanup = ""
	check(t, q, m, nil)
	status, _ = q.HostMaintenanceStatus("host")
	if !status.Quiescent {
		t.Fatal("exact create cleanup did not settle host", status)
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

func TestReviewMaintenanceDisableStopsActiveModelCall(t *testing.T) {
	for _, priority := range []string{"production", "experiment"} {
		t.Run(priority, func(t *testing.T) {
			q, clock, m := modelFixture(t, "host")
			if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
				t.Fatal(err)
			}
			active := addModelCall(t, q, priority)
			reportModel(t, q, m, "ready")
			if active.Machine != m.ID {
				t.Fatal("call was not admitted")
			}
			if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(2 * StopGrace)
			reportModel(t, q, m, "ready")
			slot := q.s.ModelSlots[slotKey(m.ID, "test")]
			if active.Error == "" && active.StopAt == nil && slot != nil && slot.InFlight == 1 && q.callAttemptLive(active) {
				t.Fatalf("disable --stop left exact model call live after 2*StopGrace: call.stop_at=nil, call.error=empty, slot.state=%s, slot.in_flight=%d", slot.State, slot.InFlight)
			}
		})
	}
}

func TestReviewMaintenanceFailedPinnedEngine(t *testing.T) {
	for _, action := range []string{"drain", "disable", "disable-stop"} {
		t.Run(action, func(t *testing.T) {
			q, clock, m := modelFixture(t, "host")
			if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
				t.Fatal(err)
			}
			if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
				t.Fatal(err)
			}
			s := q.s.ModelSlots[slotKey(m.ID, "test")]
			recoveryReport(t, q, m, s, "ready")
			recoveryReport(t, q, m, s, "failed")
			attempt := s.Attempt
			verb := action
			if verb == "disable-stop" {
				verb = "disable"
			}
			maintenanceChange(t, q, verb, action == "disable-stop")
			clock.Advance(2 * StopGrace)
			reply := recoveryReport(t, q, m, s, "failed")
			if s.State != "failed" || s.Attempt != attempt {
				t.Fatal("pin restarted during maintenance", s)
			}
			for i := 0; i < 3; i++ {
				clock.Advance(time.Second)
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if s.State != "failed" || s.Attempt != attempt {
					t.Fatal("tick restarted pin during maintenance", s)
				}
				recoveryReport(t, q, m, s, "failed")
			}
			status, err := q.HostMaintenanceStatus("host")
			if err != nil {
				t.Fatal(err)
			}
			count := len(q.hostObligations("host"))
			slotState, retryReserved := s.State, s.RetryReserved
			if !status.Quiescent {
				// Removing the configuration intent alone clears the otherwise permanent wait.
				if err := q.Unpin("test", "test"); err != nil {
					t.Fatal(err)
				}
				check(t, q, m, nil)
				cleared, err := q.HostMaintenanceStatus("host")
				if err != nil || !cleared.Quiescent {
					t.Fatal("unpin did not clear wait", cleared, err)
				}
				t.Fatalf("clean failed pin prevents wait: healthy=%v, quiescent=%v, engines=%d, state=%s, retry_reserved=%v, plan=%d, obligations=%d", status.Healthy, status.Quiescent, status.Sidecars, slotState, retryReserved, len(reply.ModelPlan), count)
			}
			if !status.Healthy || status.Sidecars != 0 || count != 0 {
				t.Fatal("cleaned engine retained custody", status, count)
			}
			if len(q.s.Pins) != 1 || q.s.Pins[0].Assigned != m.ID {
				t.Fatal("maintenance removed pin intent", q.s.Pins)
			}
			maintenanceChange(t, q, "resume", false)
			if s.State != "loading" || s.Attempt == attempt {
				t.Fatal("resume did not restart pin", s)
			}
		})
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
