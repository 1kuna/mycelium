package myce

import (
	"encoding/json"
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
			if !strings.Contains(string(status), "waiting") {
				t.Fatalf("missing visible fallback reason: %s", status)
			}
			finish(t, q, run, true)
			fleetRefresh(t, q, c)
			if h.Phase != "worker_update" {
				t.Fatalf("natural completion did not permit update: %+v", h)
			}
		})
	}
}

func runExecution(j *Job) Execution { return Execution{ID: j.ID, Attempt: j.Attempt} }

func TestFleetUpgradeLiveTakeover(t *testing.T) {
	for _, urgency := range []string{"normal", "critical"} {
		t.Run(urgency, func(t *testing.T) {
			q, c, workers := fleetFixture(t, "q", "a")
			m := workers[0]
			s := spec("production")
			s.Resumable = false
			run := fleetRun(t, q, m, s)
			q.s.Machines[m.ID].Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{runExecution(run)}}
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
			if q.hostSessionOpen(q.s.Machines[m.ID], m.Session) {
				t.Fatal("new launch not fenced")
			}
			i := fleetInstructionFor(t, q)
			if len(h.StopRequests) != 0 || job(t, q, run.ID).State != "running" {
				t.Fatal("takeover stopped work")
			}
			// A replacement process must prove custody under the retained session.
			updated := cloneMachine(q.s.Machines[m.ID])
			updated.Fleet = fleetTestProof(c, m.Session, 777, true)
			updated.Fleet.Adoption = &WorkerAdoptionProof{Protocol: custodianProtocol, Attempts: []Execution{runExecution(run)}}
			if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Checkin(Checkin{Machine: updated, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: runExecution(run), Session: run.Session, Started: true}}}); err != nil {
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
