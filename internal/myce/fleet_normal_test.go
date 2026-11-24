package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// MY-55 supersedes #1591's bounded checkpoint stops for ordinary updates.
// Persisted deadlines from the adjacent release are read, but never delivered.
func TestFleetUpgradeNormalNaturalFallback(t *testing.T) {
	for _, kind := range []string{"resumable", "non-resumable", "production", "wrong-attempt", "wrong-session", "unready"} {
		t.Run(kind, func(t *testing.T) {
			q, c, workers := fleetFixture(t, "q", "a")
			m := workers[0]
			s := spec("experiment")
			s.Resumable = kind != "non-resumable"
			if kind == "production" {
				s.Priority = "production"
			}
			run := fleetRun(t, q, m, s)
			if kind == "wrong-attempt" || kind == "wrong-session" || kind == "unready" {
				proof := &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{runExecution(run)}}
				if kind == "wrong-attempt" {
					proof.Attempts[0].Attempt++
				}
				if kind == "unready" {
					proof.Reason = "custodian process birth unconfirmed"
				}
				q.s.Machines[m.ID].Fleet.Adoption = proof
				if kind == "wrong-session" {
					q.s.Machines[m.ID].Session = "other-session"
				}
			}
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			h := q.s.FleetUpgrade.Hosts[0]
			// A production blocker can wait before acquiring the host drain.
			if kind != "production" {
				h = q.fleetActiveHost()
				if h == nil {
					t.Fatal("no active drain")
				}
				h.PlannedStops = []FleetPlannedStop{{StopAfter: c.Now().Add(-time.Hour)}}
			}
			// Real SQLite reopen preserves the fallback policy across a queue restart.
			var path string
			if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
				t.Fatal(err)
			}
			if err := q.update(func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			root, runtime := q.root, q.fleetQueue
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			q, err = Open(path, root, c)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			fakeCheckpointAvailability(t, q, true)
			q.fleetQueue = runtime
			for i := 0; i < 30; i++ {
				c.Advance(StopGrace)
				fleetRefresh(t, q, c)
			}
			got := job(t, q, run.ID)
			if got.State != "running" || got.StopAt != nil || got.Attempt != run.Attempt || got.Losses != 0 {
				t.Fatalf("ordinary update interrupted work: %+v", got)
			}
			h = q.s.FleetUpgrade.Hosts[0]
			if len(h.StopRequests) != 0 || len(h.PlannedStops) != 0 || h.Takeover {
				t.Fatalf("unproved adoption authorized interruption: %+v", h)
			}
			status, _ := json.Marshal(q.fleetUpgradeStatus())
			fallback := h.TakeoverWait
			if fallback == "" || !strings.Contains(string(status), "waiting") {
				t.Fatalf("missing visible fallback reason: %s", status)
			}
			finish(t, q, run, true)
			fleetRefresh(t, q, c)
			if h.Phase != "worker_update" {
				t.Fatalf("natural completion did not permit update: %+v", h)
			}
			if h.TakeoverWait != fallback {
				t.Fatalf("update lost earlier wait evidence: %+v", h)
			}
		})
	}
}

func runExecution(j *Job) Execution { return Execution{ID: j.ID, Attempt: j.Attempt} }

func TestFleetUpgradeLiveTakeover(t *testing.T) {
	for _, kind := range []string{"normal", "critical", "preparing", "finalizing"} {
		t.Run(kind, func(t *testing.T) {
			q, c, workers := fleetFixture(t, "q", "a")
			m := workers[0]
			s := spec("production")
			s.Needs.HostMemoryGB = 1
			s.Resumable = false
			run := fleetRun(t, q, m, s)
			urgency := kind
			if kind == "preparing" || kind == "finalizing" {
				urgency = "normal"
			}
			if kind == "preparing" {
				// The assignment exists but the payload has not started. The
				// custodian owns staging and the final launch gate throughout.
				q.s.Jobs[run.ID].StartedAt = nil
			}
			g := &attemptCustodian{observed: true, last: custodianSnapshot{At: c.Now(), Started: kind != "preparing"}}
			a := &attemptSupervisor{job: *run, custodian: g}
			var store *workerStore
			if kind == "finalizing" {
				// The process and helper have exited. The queue still owns the
				// attempt until its worker publishes the durable result.
				spool, id := t.TempDir(), ID()
				store = &workerStore{dir: spool, state: workerState{Job: run, Session: run.Session, Custodian: id}}
				g.dir, g.id = custodianDir(spool), id
				receipt := result(run, true)
				g.last = custodianSnapshot{Protocol: custodianProtocol, ID: id, At: c.Now().Add(-time.Hour), Started: true, Finished: true, State: workerState{Job: run, Session: run.Session, Result: &receipt}}
				a.released = true
				if err := PrepareStateRoots(g.dir); err != nil {
					t.Fatal(err)
				}
				if err := saveWorkerJSON(g.dir, custodianName(id, "snapshot"), g.last); err != nil {
					t.Fatal(err)
				}
			}
			q.s.Machines[m.ID].Fleet.Adoption = workerAdoptionProof(map[attemptKey]*attemptSupervisor{jobKey(*run): a}, c.Now(), time.Second)
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Urgency: urgency}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			h := q.fleetActiveHost()
			if h == nil || !h.Takeover || h.Phase != "worker_update" || h.Owned || q.maintenanceBlocked(m.HostID) {
				t.Fatalf("live takeover drained or waited: %+v", h)
			}
			if q.machineCapacityReason(q.s.Machines[m.ID]) == "" {
				t.Fatal("new admission not fenced during cutover")
			}
			if q.hostLaunchAllowed(q.s.Machines[m.ID], m.Session) {
				t.Fatal("new launch not fenced")
			}
			// Native-group member restarts belong to the retained attempt, so
			// the brief new-admission fence must leave their exact gates open.
			if err := q.AuthorizeLaunch(HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: run.ID, Attempt: fmt.Sprint(run.Attempt)}); err != nil {
				t.Fatalf("cutover refused retained attempt member launch: %v", err)
			}
			i := fleetInstructionFor(t, q)
			if len(h.StopRequests) != 0 || job(t, q, run.ID).State != "running" {
				t.Fatal("takeover stopped work")
			}
			// A replacement process must prove custody under the retained session.
			updated := cloneMachine(q.s.Machines[m.ID])
			// The current worker can admit the CPU probe beside retained GPU
			// work. The pre-upgrade legacy fixture had only one execution slot.
			updated.Devices = m3Machine().Devices[:1]
			updated.Fleet = fleetTestProof(c, m.Session, 777, true)
			updated.Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{runExecution(run)}}
			if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
				t.Fatal(err)
			}
			var final Result
			if store != nil {
				p, err := (Worker{Clock: c}).adoptCustodian(store)
				if err != nil {
					t.Fatal(err)
				}
				defer p.custodian.detach()
				replacement := &attemptSupervisor{job: *run, custodian: p.custodian, released: true}
				updated.Fleet.Adoption = workerAdoptionProof(map[attemptKey]*attemptSupervisor{jobKey(*run): replacement}, c.Now(), time.Second)
				select {
				case f := <-p.done:
					if f.err != nil {
						t.Fatal(f.err)
					}
					final = f.result
				case <-time.After(testWaitTimeout):
					t.Fatal("replacement did not recover result")
				}
			}
			if _, err := q.Checkin(Checkin{Machine: updated, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: runExecution(run), Session: run.Session, Started: kind != "preparing"}}}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			for i := 0; i <= int(fleetStableFor/time.Second); i++ {
				c.Advance(time.Second)
				fleetRefresh(t, q, c)
			}
			got := job(t, q, run.ID)
			if got.State != "running" || got.Attempt != run.Attempt || got.Session != run.Session || got.Losses != 0 || got.StopAt != nil {
				t.Fatalf("takeover changed running attempt: %+v", got)
			}
			if !h.WorkerDone {
				t.Fatalf("same-session replacement not verified: %+v", h)
			}
			if kind == "preparing" && got.StartedAt != nil {
				t.Fatal("upgrade waited for preparation to start the payload")
			}
			if store != nil {
				stale := final
				stale.Attempt++
				if err := q.Complete(stale); !errors.Is(err, ErrStale) {
					t.Fatalf("different attempt was accepted: %v", err)
				}
				for range 2 {
					if err := q.Complete(final); err != nil {
						t.Fatal(err)
					}
				}
				got := job(t, q, run.ID)
				if got.State != "succeeded" || !got.ReleaseConfirmed || got.Attempt != run.Attempt || len(got.AttemptHistory) != 1 {
					t.Fatalf("result was lost or settled more than once: %+v", got)
				}
			}
		})
	}
}

// A stale readiness view is a wait-and-recheck condition, not a requirement to
// finish the payload. Positive release is still required for pending cleanup.
func TestFleetUpgradeReadinessRecheck(t *testing.T) {
	for _, kind := range []string{"unobserved", "stale", "release-pending"} {
		t.Run(kind, func(t *testing.T) {
			q, c, workers := fleetFixture(t, "q", "a")
			m := workers[0]
			run := fleetRun(t, q, m, spec("experiment"))
			g := &attemptCustodian{observed: kind != "unobserved", last: custodianSnapshot{At: c.Now()}}
			if kind == "stale" {
				g.last.At = c.Now().Add(-time.Hour)
			}
			if kind == "release-pending" {
				g.last.State.Result = &Result{ReleasePending: true}
			}
			a := &attemptSupervisor{job: *run, custodian: g}
			actors := map[attemptKey]*attemptSupervisor{jobKey(*run): a}
			q.s.Machines[m.ID].Fleet.Adoption = workerAdoptionProof(actors, c.Now(), time.Second)
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			h := q.fleetActiveHost()
			if h == nil || h.Takeover || h.Phase != "draining" || !strings.Contains(h.Wait, "readiness or release") {
				t.Fatalf("unproved custody authorized swap: %+v", h)
			}
			c.Advance(time.Second)
			if kind == "release-pending" {
				// Cleanup must finish before release, rather than accepting a
				// newer observation of the same pending helper.
				g.last.At = c.Now()
				q.s.Machines[m.ID].Fleet.Adoption = workerAdoptionProof(actors, c.Now(), time.Second)
				fleetRefresh(t, q, c)
				if h.Takeover || h.Phase != "draining" {
					t.Fatal("fresh pending cleanup authorized swap", h)
				}
				finish(t, q, run, true)
				a.released = true
			} else {
				g.observed, g.last.At = true, c.Now()
			}
			q.s.Machines[m.ID].Fleet.Adoption = workerAdoptionProof(actors, c.Now(), time.Second)
			fleetRefresh(t, q, c)
			// Positive release can advance the ordinary quiet-host update
			// immediately, before another live-adoption proof is needed.
			if h.Phase != "worker_update" || kind != "release-pending" && !h.Takeover || len(h.StopRequests) != 0 {
				t.Fatalf("recheck failed to permit unmodified custody: %+v", h)
			}
			if kind != "release-pending" && job(t, q, run.ID).State != "running" {
				t.Fatal("readiness required natural completion")
			}
		})
	}
}

func TestFleetUpgradePreparationLaunchRace(t *testing.T) {
	q, _, workers := fleetFixture(t, "q", "a")
	m := workers[0]
	run := fleetRun(t, q, m, spec("experiment"))
	q.s.Machines[m.ID].Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{runExecution(run)}}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	// A delayed preparation offer appears after the instruction was fetched.
	// The cutover fence prevents it from acquiring custody, so it cannot
	// force an otherwise adoptable running attempt to drain.
	launch := q.issuePreparation(q.s.Machines[m.ID], "environment", "late-build")
	if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
		t.Fatalf("preparation raced cutover: %v", err)
	}
	if err := q.authorizeFleetUpdate(i.Operation, i.Host, i.Machine, i.Session); err != nil {
		t.Fatal("an unstarted offer blocked final authorization", err)
	}
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	if q.s.PreparationPlans[launch.Attempt].Started || !slices.Equal(q.s.WorkerHandovers[m.ID].Attempts, []Execution{runExecution(run)}) {
		t.Fatal("late offer acquired execution custody")
	}
	if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
		t.Fatalf("retained worker handover allowed a stale offer: %v", err)
	}
}

func TestFleetUpgradeStandalonePreparationFallback(t *testing.T) {
	for _, kind := range []string{"environment", "copy", "untracked", "cleanup"} {
		t.Run(kind, func(t *testing.T) {
			q, c, workers := fleetFixture(t, "q", "a")
			m := q.s.Machines[workers[0].ID]
			m.Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol}
			switch kind {
			case "environment", "copy":
				l := q.issuePreparation(m, kind, "background")
				if err := q.AuthorizeLaunch(l); err != nil {
					t.Fatal(err)
				}
			case "untracked":
				m.EnvironmentReports = []EnvironmentReport{{Key: "legacy", State: "building"}}
			case "cleanup":
				r := HostRelease{Host: m.HostID, Machine: m.ID, Session: m.Session, Job: "preparation", Preparation: true}
				q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
				m.EnvironmentReports = []EnvironmentReport{{Key: "unconfirmed-cleanup", State: "building"}}
			}
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			h := q.fleetActiveHost()
			c.Advance(StopGrace)
			fleetRefresh(t, q, c)
			if h == nil || h.Takeover || h.Phase != "draining" || len(h.StopRequests) != 0 || !strings.Contains(h.Wait, "preparation has no detached custodian") {
				t.Fatalf("unowned preparation lost drain/reason: %+v", h)
			}
		})
	}
}

func TestFleetUpgradeLostTakeoverProofFallsBack(t *testing.T) {
	q, c, workers := fleetFixture(t, "q", "a")
	m := workers[0]
	run := fleetRun(t, q, m, spec("experiment"))
	q.s.Machines[m.ID].Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{runExecution(run)}}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	h := q.fleetActiveHost()
	q.s.Machines[m.ID].Fleet.Adoption = nil
	fleetTick(t, q)
	c.Advance(StopGrace)
	fleetRefresh(t, q, c)
	if h.Takeover || !h.Owned || h.Phase != "draining" {
		t.Fatalf("did not acquire natural fallback drain: %+v", h)
	}
	instruction, err := q.fleetInstruction(h.Host, m.ID, m.Session)
	if err != nil || instruction.Operation != "" || job(t, q, run.ID).StopAt != nil {
		t.Fatalf("lost proof authorized update/stop: %+v %v", instruction, err)
	}
}

// Replacement is a planned custody transfer, even beyond health/loss timeouts
// and queue restart. A live peer prevents fleet-quiet suppression masking loss.
func TestFleetUpgradeHandoverRetainsExactCustody(t *testing.T) {
	q, c, workers := fleetFixture(t, "q", "a", "peer")
	m, peer := workers[0], workers[1]
	run := fleetRun(t, q, m, spec("experiment"))
	q.s.Machines[m.ID].Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{runExecution(run)}}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	h := q.fleetActiveHost()
	// The idle peer may precede the running host.
	for h != nil && h.Host != m.HostID {
		fleetActivate(t, q, c)
		fleetTick(t, q)
		h = q.fleetActiveHost()
	}
	i := fleetInstructionFor(t, q)
	ack := FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}
	if err := q.AckFleetUpgrade(ack); err != nil {
		t.Fatal(err)
	}
	oldPID := q.s.WorkerHandovers[m.ID].PID
	if oldPID == 0 {
		t.Fatal("missing durable handover")
	}
	var path string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root, runtime := q.root, q.fleetQueue
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	q.fleetQueue = runtime
	// Exceed the complete operation timeout, maintaining only the other host.
	for n := 0; n < 20; n++ {
		c.Advance(time.Minute)
		peer = cloneMachine(q.s.Machines[peer.ID])
		peer.Fleet = fleetTestProof(c, peer.Session, 301, true)
		check(t, q, peer, nil)
		fleetTick(t, q)
	}
	got := job(t, q, run.ID)
	if got.State != "running" || got.Attempt != run.Attempt || got.Losses != 0 || q.s.Machines[m.ID].Gone || q.networkQuiet {
		t.Fatalf("handover lost running work: %+v quiet=%v", got, q.networkQuiet)
	}
	if !q.fleetTakeoverFence(m.HostID) || !q.retainWorkerHandover(q.s.Machines[m.ID]) {
		t.Fatal("health timeout discarded unknown custody")
	}
	wrong := cloneMachine(q.s.Machines[m.ID])
	wrong.Session = "replacement-session"
	wrong.Fleet = fleetTestProof(c, wrong.Session, oldPID+1, true)
	if _, err := q.Checkin(Checkin{Machine: wrong, Fresh: true, PreviousSession: m.Session}); err == nil {
		t.Fatal("fresh session superseded unresolved custody")
	}
	replacement := cloneMachine(q.s.Machines[m.ID])
	replacement.Fleet = fleetTestProof(c, m.Session, oldPID+1, true)
	replacement.Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{{ID: run.ID, Attempt: run.Attempt + 1}}}
	report := Checkin{Machine: replacement, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: runExecution(run), Session: run.Session, Started: true}}}
	if _, err := q.Checkin(report); err != nil {
		t.Fatal(err)
	}
	if !q.retainWorkerHandover(q.s.Machines[m.ID]) {
		t.Fatal("wrong attempt proved adoption")
	}
	replacement.Fleet.Adoption.Attempts = []Execution{runExecution(run)}
	replacement.Fleet.ObservationAgeSeconds = (2 * fleetRuntimeFreshFor).Seconds()
	report.Machine = replacement
	if _, err := q.Checkin(report); err != nil {
		t.Fatal(err)
	}
	if !q.retainWorkerHandover(q.s.Machines[m.ID]) {
		t.Fatal("stale proof cleared custody")
	}
	replacement.Fleet.ObservedAt = c.Now()
	replacement.Fleet.ObservationAgeSeconds = 0
	report.Machine = replacement
	if _, err := q.Checkin(report); err != nil {
		t.Fatal(err)
	}
	if q.retainWorkerHandover(q.s.Machines[m.ID]) {
		t.Fatal("exact adoption did not clear handover")
	}
	got = job(t, q, run.ID)
	if got.Attempt != run.Attempt || got.Losses != 0 || got.State != "running" {
		t.Fatal("adoption changed attempt", got)
	}
}

func TestFleetUpgradeRestartAcknowledgementIsIdempotent(t *testing.T) {
	q, c, workers := fleetFixture(t, "q", "a")
	m := workers[0]
	run := fleetRun(t, q, m, spec("experiment"))
	q.s.Machines[m.ID].Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{runExecution(run)}}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	ack := FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}
	if err := q.AckFleetUpgrade(ack); err != nil {
		t.Fatal(err)
	}
	oldPID := q.s.WorkerHandovers[m.ID].PID
	replacement := cloneMachine(q.s.Machines[m.ID])
	replacement.Fleet = fleetTestProof(c, m.Session, oldPID+1, true)
	replacement.Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{runExecution(run)}}
	if _, err := q.Checkin(Checkin{Machine: replacement, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: runExecution(run), Session: run.Session, Started: true}}}); err != nil {
		t.Fatal(err)
	}
	if err := q.AckFleetUpgrade(ack); err != nil {
		t.Fatal(err)
	}
	if v, ok := q.s.WorkerHandovers[m.ID]; ok && v.PID != oldPID {
		t.Fatal("retry replaced original process identity", v)
	}
	if q.retainWorkerHandover(q.s.Machines[m.ID]) {
		t.Fatal("ack retry stranded adopted work")
	}
}
