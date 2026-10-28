package myce

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFleetUpgradeFixForwardPreservesNewerOperatorDrain(t *testing.T) {
	q, c, workers := fleetFixture(t, "host", "host")
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifest := q.s.EnrollmentRelease.Manifest
	sign := func() []byte {
		data, _ := json.Marshal(manifest)
		envelope, _ := json.Marshal(SignedRelease{Manifest: data, Signature: ed25519.Sign(private, data)})
		return envelope
	}
	if err = q.ConfigureEnrollmentRelease(sign(), pub, "https://publisher.invalid/release"); err != nil {
		t.Fatal(err)
	}
	// The queue was already installed; only the colocated worker helper fails.
	q.fleetQueue.Proof = fleetTestProof(c, "queue-current", 100, true)
	if _, err = q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	for n := 0; n < 32; n++ {
		fleetHeartbeatAdvance(t, q, c)
	}
	i := fleetInstructionFor(t, q)
	if i.Machine != workers[0].ID {
		t.Fatal("expected worker helper", i.Machine)
	}
	if err = q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed"}); err != nil {
		t.Fatal(err)
	}
	c.Advance(time.Second)
	if _, err = q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	cpu, ram, lastResort := 4, 8.0, true
	if _, err = q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "budget", CPUBudgetCores: &cpu, MemoryBudgetGB: &ram}); err != nil {
		t.Fatal(err)
	}
	if _, err = q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "placement", LastResort: &lastResort}); err != nil {
		t.Fatal(err)
	}
	operator := q.s.HostMaintenance["host"]
	// A same-target rerun must retain the stale receipt as a safe conflict.
	if _, err = q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if status := q.fleetUpgradeStatus(); status.State != "stopped" || status.Hosts[0].Failure != "maintenance_changed" || !strings.Contains(status.Hosts[0].Wait, "fleet upgrade") {
		t.Fatalf("stale receipt lost its actionable conflict: %+v", status)
	}
	manifest.Version = "v1.0.2"
	if err = q.approveEnrollmentRelease(EnrollmentReleaseApproval{SignedRelease: sign(), PublicKey: pub, Origin: "https://publisher.invalid/release"}); err != nil {
		t.Fatal(err)
	}
	var path string
	if err = q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	// Ordinary supported installation finished while stopped. Both services now
	// run the newly approved bytes; reconciliation must not restart either one.
	proof := fleetTestProof(c, "queue-newer", 101, true)
	proof.Version = manifest.Version
	q.fleetQueue = &fleetQueueRuntime{Host: "host", OS: "linux", Arch: "amd64", Proof: proof, ObservedAt: c.Now()}
	m := workers[0]
	m.Session = "worker-newer"
	m.Fleet = fleetTestProof(c, m.Session, 201, true)
	m.Fleet.Version = manifest.Version
	m.CPUBudgetCores, m.HostMemoryBudgetGB = cpu, ram
	if _, err = q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: i.Session}); err != nil {
		t.Fatal(err)
	}
	request := FleetUpgradeRequest{Release: manifest.Version}
	out, err := q.StartFleetUpgrade(request)
	if err != nil || out.Operation == i.Operation {
		t.Fatalf("explicit fix forward refused: %+v %v", out, err)
	}
	for n := 0; n < 70; n++ {
		fleetHeartbeatAdvance(t, q, c)
		for _, runtime := range []struct{ machine, session string }{{"@queue", proof.Session}, {m.ID, m.Session}} {
			instruction, e := q.fleetInstruction("host", runtime.machine, runtime.session)
			if e != nil || instruction.Operation != "" {
				t.Fatal("already-current runtime instructed to restart", e)
			}
		}
	}
	if q.s.FleetUpgrade.State != "complete" || !reflect.DeepEqual(operator, q.s.HostMaintenance["host"]) || !q.maintenanceBlocked("host") {
		t.Fatalf("operator policy not preserved through completion: %+v %+v", q.fleetUpgradeStatus(), q.s.HostMaintenance["host"])
	}
	if q.s.Machines[m.ID].Session != m.Session || q.fleetQueue.Proof.Session != proof.Session {
		t.Fatal("already-current process identity changed")
	}
}

func fleetTestProof(clock Clock, session string, pid int, target bool) *FleetRuntimeProof {
	version, hash := "v1.0.0", strings.Repeat("b", 64)
	if target {
		version, hash = "v1.0.1", strings.Repeat("a", 64)
	}
	return &FleetRuntimeProof{Version: version, SHA256: hash, Session: session, PID: pid, ServiceHealthy: true, RunnerSmoke: true, StartGate: startGateProtocol, AttemptProtocol: keyedAttemptProtocol, ObservedAt: clock.Now()}
}
func fleetFixture(t *testing.T, queueHost string, hosts ...string) (*Queue, *fakeClock, []Machine) {
	t.Helper()
	q, c := fixture(t)
	signed, key, _ := signedTestRelease(t)
	if err := q.ConfigureEnrollmentRelease(signed, key, "https://publisher.invalid/release"); err != nil {
		t.Fatal(err)
	}
	q.fleetQueue = &fleetQueueRuntime{Host: queueHost, OS: "linux", Arch: "amd64", Proof: fleetTestProof(c, "queue-old", 100, false), ObservedAt: c.Now()}
	var workers []Machine
	for n, host := range hosts {
		m := machine("worker-"+host, "fake", host)
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 32
		m.Fleet = fleetTestProof(c, m.Session, 200+n, false)
		check(t, q, m, nil)
		workers = append(workers, m)
	}
	return q, c, workers
}
func fleetTick(t *testing.T, q *Queue) {
	t.Helper()
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
}
func fleetInstructionFor(t *testing.T, q *Queue) FleetUpdateInstruction {
	t.Helper()
	h := q.fleetActiveHost()
	if h == nil {
		t.Fatal("no active host")
	}
	machine, session := h.Machine, ""
	if h.Before != nil {
		session = h.Before.Session
	}
	if h.Phase == "queue_update" {
		machine, session = "@queue", h.QueueBefore.Session
	}
	i, err := q.fleetInstruction(h.Host, machine, session)
	if err != nil || i.Operation == "" {
		t.Fatalf("instruction at %s: %+v %v", h.Phase, i, err)
	}
	return i
}
func fleetActivate(t *testing.T, q *Queue, c *fakeClock) string {
	t.Helper()
	i := fleetInstructionFor(t, q)
	h := q.fleetActiveHost()
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	if i.Machine == "@queue" {
		q.fleetQueue.Proof = fleetTestProof(c, "queue-new", 101, true)
	} else {
		m := cloneMachine(q.s.Machines[i.Machine])
		m.Session = "new-" + m.Session
		m.Fleet = fleetTestProof(c, m.Session, h.Before.Fleet.PID+1000, true)
		if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: i.Session}); err != nil {
			t.Fatal(err)
		}
	}
	fleetTick(t, q)
	for n := 0; n < 31; n++ {
		fleetHeartbeatAdvance(t, q, c)
	}
	return i.Host + "/" + i.Machine
}

func fleetHeartbeatAdvance(t *testing.T, q *Queue, c *fakeClock) {
	t.Helper()
	c.Advance(time.Second)
	for _, m := range q.s.Machines {
		m.LastSeen = c.Now()
		if m.Fleet != nil {
			m.Fleet.ObservedAt = c.Now()
		}
	}
	q.s.LastWorkerContact = c.Now()
	if q.fleetQueue != nil && q.fleetQueue.Proof != nil {
		q.fleetQueue.Proof.ObservedAt = c.Now()
	}
	fleetTick(t, q)
}

func TestFleetUpgradeSequentialProductionLastAndNaturalCompletion(t *testing.T) {
	q, c, workers := fleetFixture(t, "z", "a", "b", "z")
	s := spec("production")
	s.AllowedMachines = []string{workers[2].ID}
	j := submit(t, q, s)
	r := check(t, q, workers[2], nil).Job
	if r == nil {
		t.Fatal("production not assigned")
	}
	if err := q.Started(result(r, true)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if h := q.fleetActiveHost(); h.Host != "a" || h.Phase != "worker_update" {
		t.Fatalf("order: %+v", h)
	}
	if got := fleetActivate(t, q, c); got != "a/worker-a" {
		t.Fatal(got)
	}
	fleetTick(t, q)
	if got := fleetActivate(t, q, c); got != "b/worker-b" {
		t.Fatal(got)
	}
	fleetTick(t, q)
	h := q.fleetActiveHost()
	if h.Host != "z" || h.Phase != "draining" || h.Wait == "" {
		t.Fatalf("production must wait: %+v", h)
	}
	// No completion deadline. Fresh exact custody still blocks cutover hours later.
	c.Advance(2 * time.Hour)
	workers[2].Fleet.ObservedAt = c.Now()
	q.fleetQueue.Proof.ObservedAt = c.Now()
	if _, err := q.Checkin(Checkin{Machine: workers[2], Running: &Execution{ID: r.ID, Attempt: r.Attempt}, Started: true}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if got := job(t, q, j.ID); got.State != "running" || got.StopAt != nil || got.Attempt != 1 || got.InfrastructureRetries != 0 {
		t.Fatalf("upgrade interrupted Started work: %+v", got)
	}
	finish(t, q, r, true)
	check(t, q, workers[2], nil)
	fleetTick(t, q)
	if got := fleetActivate(t, q, c); got != "z/@queue" {
		t.Fatal(got)
	}
	fleetTick(t, q)
	if got := fleetActivate(t, q, c); got != "z/worker-z" {
		t.Fatal(got)
	}
	fleetTick(t, q)
	if q.s.FleetUpgrade.State != "complete" {
		t.Fatalf("not complete: %+v", q.fleetUpgradeStatus())
	}
	for _, host := range []string{"a", "b", "z"} {
		if q.s.HostMaintenance[host].Mode != "enabled" {
			t.Fatal("owned drain not restored", host)
		}
	}
	if got := job(t, q, j.ID); got.State != "succeeded" || got.Attempt != 1 || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("work lost: %+v", got)
	}
}

func TestFleetUpgradeQueueBeforeWorkersAndCallsWait(t *testing.T) {
	q, _, _ := fleetFixture(t, "queue", "a", "b")
	q.calls = map[string]*ModelCall{"stream": {ID: "stream", Priority: "production", Machine: "worker-a"}}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if h := q.fleetActiveHost(); h.Host != "queue" || h.Phase != "draining" || !strings.Contains(h.Wait, "model calls") {
		t.Fatalf("queue call cutover: %+v", h)
	}
	if !q.fleetPausesCalls() {
		t.Fatal("new model-call admission not paused")
	}
	delete(q.calls, "stream")
	fleetTick(t, q)
	if i := fleetInstructionFor(t, q); i.Machine != "@queue" {
		t.Fatal("queue must precede compatible worker activation")
	}
}

func TestFleetUpgradeResumeDurableIntentAndReverifyHash(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "a")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	var path string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	q = reopened
	fakeCheckpointAvailability(t, q, true)
	q.fleetQueue = &fleetQueueRuntime{Host: "queue", OS: "linux", Arch: "amd64", Proof: fleetTestProof(c, "queue-new", 101, true), ObservedAt: c.Now()}
	check(t, q, workers[0], nil)
	if out, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil || out.Operation != i.Operation {
		t.Fatalf("lost operation: %+v %v", out, err)
	}
	fleetTick(t, q)
	for n := 0; n < 31; n++ {
		fleetHeartbeatAdvance(t, q, c)
	}
	fleetTick(t, q)
	if h := q.s.FleetUpgrade.Hosts[0]; h.Phase != "complete" {
		t.Fatalf("new queue hash not reconciled: %+v", h)
	}
	if q.s.FleetUpgrade.Hosts[1].Phase == "pending" {
		fleetTick(t, q)
	}
	worker := fleetInstructionFor(t, q)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: worker.Operation, Revision: worker.Revision, Host: worker.Host, Machine: worker.Machine, Session: worker.Session, Phase: "installed"}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if q.fleetActiveHost().Phase != "worker_update" {
		t.Fatal("disk publication falsely proved running hash")
	}
	fleetActivate(t, q, c)
	fleetTick(t, q)
	if q.s.FleetUpgrade.State != "complete" {
		t.Fatal(q.fleetUpgradeStatus())
	}
}

func TestFleetUpgradeStopFirstFailureAndMaintenanceCAS(t *testing.T) {
	for _, maintenanceRace := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "operator-edit"}[maintenanceRace], func(t *testing.T) {
			q, _, _ := fleetFixture(t, "queue", "a", "b")
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			i := fleetInstructionFor(t, q)
			if maintenanceRace {
				if _, err := q.ChangeHostMaintenance(i.Host, HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: "secret must never be projected"}); err != nil {
					t.Fatal(err)
				}
			}
			fleetTick(t, q)
			if q.s.FleetUpgrade.State != "stopped" {
				t.Fatal("did not stop")
			}
			for _, h := range q.s.FleetUpgrade.Hosts[1:] {
				if h.Phase != "pending" || q.s.HostMaintenance[h.Host].Mode != "enabled" {
					t.Fatal("later host touched", h)
				}
			}
			if q.s.HostMaintenance[i.Host].Mode != "drained" {
				t.Fatal("failed host resumed")
			}
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed"}); err == nil {
				t.Fatal("stale revision accepted")
			}
			if maintenanceRace {
				fleetTick(t, q)
				if q.s.FleetUpgrade.State != "stopped" || q.s.HostMaintenance[i.Host].DrainOwner != "operator" {
					t.Fatal("resumed operator drain")
				}
			}
		})
	}
}

func TestFleetUpgradeApprovalEnvelopeReapprovalAndPersistence(t *testing.T) {
	q, _, _ := fleetFixture(t, "queue")
	approved := cloneFleetRelease(*q.s.EnrollmentRelease)
	q.s.EnrollmentRelease.SignedRelease = nil
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err == nil {
		t.Fatal("legacy approval without envelope accepted")
	}
	if err := q.approveEnrollmentRelease(EnrollmentReleaseApproval{SignedRelease: approved.SignedRelease, PublicKey: approved.PublisherKey, Origin: approved.Origin}); err != nil {
		t.Fatal("identical reapproval refused", err)
	}
	if len(q.s.EnrollmentRelease.SignedRelease) == 0 {
		t.Fatal("envelope not retained")
	}
	if _, err := VerifyRelease(q.s.EnrollmentRelease.SignedRelease, q.s.EnrollmentRelease.PublisherKey); err != nil {
		t.Fatal(err)
	}
}

func TestFleetUpgradeWorkerObservationAgeIgnoresClockSkew(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "a")
	for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		m := workers[0]
		m.Fleet = cloneFleetProof(m.Fleet)
		m.Fleet.ObservedAt = c.Now().Add(skew)
		m.Fleet.ObservationAgeSeconds = 1
		check(t, q, m, nil)
		stored := q.s.Machines[m.ID].Fleet
		if stored == nil || !stored.ObservedAt.Equal(c.Now().Add(-time.Second)) || !q.fleetProofFresh(stored) {
			t.Fatal("worker wall clock affected queue freshness", stored)
		}
	}
	m := workers[0]
	m.Fleet = cloneFleetProof(m.Fleet)
	m.Fleet.ObservedAt = c.Now().Add(time.Hour)
	m.Fleet.ObservationAgeSeconds = 6
	check(t, q, m, nil)
	if q.s.Machines[m.ID].Fleet != nil {
		t.Fatal("cached observation renewed by heartbeat")
	}
}

func TestFleetUpgradeAlreadyCurrentRequiresStableEnrollmentHealth(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "a")
	q.fleetQueue.Proof = fleetTestProof(c, "queue-current", 100, true)
	m := workers[0]
	m.Fleet = fleetTestProof(c, m.Session, m.Fleet.PID, true)
	m.HostMemoryError = "memory unavailable"
	check(t, q, m, nil)
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if q.s.FleetUpgrade.Hosts[0].Phase == "complete" {
		t.Fatal("no-op queue skipped stable health")
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	if i, err := q.fleetInstruction("queue", "@queue", "queue-current"); err != nil || i.Operation != "" {
		t.Fatal("rerun instructed already-current queue to restart")
	}
	for n := 0; n < 32; n++ {
		fleetHeartbeatAdvance(t, q, c)
	}
	h := q.fleetActiveHost()
	if h == nil || h.Host != "a" || h.Phase != "worker_health" {
		t.Fatalf("no-op worker health path: %+v", h)
	}
	if i, err := q.fleetInstruction(h.Host, m.ID, m.Session); err != nil || i.Operation != "" {
		t.Fatal("already-current worker was instructed to restart")
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	if i, err := q.fleetInstruction(h.Host, m.ID, m.Session); err != nil || i.Operation != "" || h.Phase != "worker_health" {
		t.Fatal("rerun instructed degraded current worker to restart")
	}
	for n := 0; n < 32; n++ {
		fleetHeartbeatAdvance(t, q, c)
	}
	if q.s.FleetUpgrade.State == "complete" || q.s.HostMaintenance["a"].Mode != "drained" {
		t.Fatal("unhealthy current host resumed")
	}
	m.HostMemoryError = ""
	m.Fleet.ObservedAt = c.Now()
	check(t, q, m, nil)
	for n := 0; n < 32; n++ {
		fleetHeartbeatAdvance(t, q, c)
	}
	if q.s.FleetUpgrade.State != "complete" || q.s.Machines[m.ID].Session != m.Session {
		t.Fatal("healthy current host not reconciled without restart")
	}
}

func TestFleetUpgradeRerunRetainsActiveHelperFailureUntilReplacement(t *testing.T) {
	for _, worker := range []bool{false, true} {
		for _, replace := range []bool{false, true} {
			t.Run(fmt.Sprintf("worker=%t/replacement=%t", worker, replace), func(t *testing.T) {
				q, c, _ := fleetFixture(t, "queue", "a", "b")
				if worker {
					q.fleetQueue.Proof = fleetTestProof(c, "queue-current", 100, true)
				}
				if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
					t.Fatal(err)
				}
				fleetTick(t, q)
				if worker {
					for n := 0; n < 32; n++ {
						fleetHeartbeatAdvance(t, q, c)
					}
				}
				i := fleetInstructionFor(t, q)
				ack := FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "prepared"}
				if err := q.AckFleetUpgrade(ack); err != nil {
					t.Fatal(err)
				}
				if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
					t.Fatal(err)
				}
				if err := q.authorizeFleetUpdate(i.Operation, i.Host, i.Machine, i.Session, i.Revision); err != nil {
					t.Fatal("rerun revoked active helper", err)
				}
				if replace {
					current := ack
					current.Revision = q.s.FleetUpgrade.Revision
					if err := q.AckFleetUpgrade(current); err != nil {
						t.Fatal(err)
					}
					for _, phase := range []string{"failed", "installed", "restarting", "prepared"} {
						ack.Phase = phase
						if err := q.AckFleetUpgrade(ack); err == nil {
							t.Fatalf("superseded helper %s accepted", phase)
						}
					}
					if err := q.authorizeFleetUpdate(i.Operation, i.Host, i.Machine, i.Session, i.Revision); err == nil {
						t.Fatal("superseded helper authorized")
					}
					ack = current
				}
				ack.Phase = "failed"
				if err := q.AckFleetUpgrade(ack); err != nil {
					t.Fatal("active helper failure hidden by rerun", err)
				}
				if q.s.FleetUpgrade.State != "stopped" || q.s.HostMaintenance[i.Host].Mode != "drained" {
					t.Fatal("failure did not stop with maintenance retained")
				}
				for _, h := range q.s.FleetUpgrade.Hosts {
					if h.Host != i.Host && h.Phase != "complete" && h.Phase != "pending" {
						t.Fatal("later host advanced after failure")
					}
				}
			})
		}
	}
}

func TestFleetUpgradeRunnerSmokeFailureStopsCurrentProcess(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "a")
	q.fleetQueue.Proof = fleetTestProof(c, "queue-current", 100, true)
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	for n := 0; n < 32; n++ {
		fleetHeartbeatAdvance(t, q, c)
	}
	i := fleetInstructionFor(t, q)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	m := workers[0]
	m.Session = "worker-failed-smoke"
	m.Fleet = fleetTestProof(c, m.Session, 999, true)
	m.Fleet.RunnerSmoke = false
	m.Fleet.Failure = "runner_smoke_failed"
	if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: i.Session}); err != nil {
		t.Fatal(err)
	}
	// An explicit rerun cannot erase a definitive failure by bumping revision.
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	ack := FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: "runner_smoke_failed", Runtime: m.Fleet}
	stale := ack
	stale.Runtime = cloneFleetProof(m.Fleet)
	stale.Runtime.Session = "obsolete-process"
	if err := q.AckFleetUpgrade(stale); err == nil {
		t.Fatal("obsolete runtime failure accepted")
	}
	if err := q.AckFleetUpgrade(ack); err != nil {
		t.Fatal(err)
	}
	if q.s.FleetUpgrade.State != "stopped" || q.fleetActiveHost().Failure != "runner_smoke_failed" || q.s.HostMaintenance[i.Host].Mode != "drained" {
		t.Fatal("cached smoke failure did not stop maintained host")
	}
}

func TestFleetUpgradeQueueOnlyKeepsWorkersAndResumes(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "queue", "other")
	// An unrelated old/unmanaged worker is not a queue-only preflight target.
	q.s.Machines[workers[1].ID].Fleet = nil
	request := FleetUpgradeRequest{QueueOnly: true, WaitProductionIdle: true}
	out, err := q.StartFleetUpgrade(request)
	if err != nil {
		t.Fatal(err)
	}
	if !out.QueueOnly || !out.WaitProductionIdle || len(out.Hosts) != 1 {
		t.Fatalf("scope: %+v", out)
	}
	fleetTick(t, q)
	if _, err = q.StartFleetUpgrade(FleetUpgradeRequest{}); err == nil {
		t.Fatal("running scope changed")
	}
	var path string
	if err = q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	q.fleetQueue = &fleetQueueRuntime{Host: "queue", OS: "linux", Arch: "amd64", Proof: fleetTestProof(c, "queue-old", 100, false), ObservedAt: c.Now()}
	check(t, q, workers[0], nil)
	resumed, err := q.StartFleetUpgrade(request)
	if err != nil || resumed.Operation != out.Operation || !resumed.QueueOnly || !resumed.WaitProductionIdle {
		t.Fatalf("resume: %+v %v", resumed, err)
	}
	if got := fleetActivate(t, q, c); got != "queue/@queue" {
		t.Fatal(got)
	}
	fleetTick(t, q)
	if q.s.FleetUpgrade.State != "complete" {
		t.Fatal(q.fleetUpgradeStatus())
	}
	for _, m := range q.s.Machines {
		if m.Fleet != nil && m.Fleet.Version != "v1.0.0" {
			t.Fatal("queue-only updated worker", m.ID)
		}
	}
	if q.s.HostMaintenance["other"].Mode == "drained" || q.s.HostMaintenance["queue"].Mode != "enabled" {
		t.Fatal("maintenance not preserved")
	}
	// A completed queue-only operation must not make a later same-release fleet
	// upgrade a no-op.
	check(t, q, workers[1], nil)
	full, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
	if err != nil || full.Operation == out.Operation || len(full.Hosts) != 2 {
		t.Fatalf("full after queue-only: %+v %v", full, err)
	}
}

func TestFleetUpgradeProductionIdleWaitsAcrossFleet(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "remote")
	s := spec("production")
	s.AllowedMachines = []string{workers[0].ID}
	j := submit(t, q, s)
	r := check(t, q, workers[0], nil).Job
	if r == nil {
		t.Fatal("production not assigned")
	}
	if err := q.Started(result(r, true)); err != nil {
		t.Fatal(err)
	}
	request := FleetUpgradeRequest{QueueOnly: true, WaitProductionIdle: true}
	if _, err := q.StartFleetUpgrade(request); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	h := q.fleetActiveHost()
	if h.Phase != "draining" || !strings.Contains(h.Wait, "production attempts") {
		t.Fatalf("remote production not waited: %+v", h)
	}
	queued := submit(t, q, s)
	if q.placementPolicyAllowed(q.s.Jobs[queued.ID], q.s.Machines[workers[0].ID], false) {
		t.Fatal("new production admitted during idle wait")
	}
	if !q.placementPolicyAllowed(q.s.Jobs[j.ID], q.s.Machines[workers[0].ID], false) {
		t.Fatal("existing execution blocked")
	}
	c.Advance(2 * time.Hour)
	workers[0].Fleet.ObservedAt = c.Now()
	q.fleetQueue.Proof.ObservedAt = c.Now()
	if _, err := q.Checkin(Checkin{Machine: workers[0], Running: &Execution{ID: r.ID, Attempt: r.Attempt}, Started: true}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if got := job(t, q, j.ID); got.StopAt != nil || got.State != "running" || got.Losses != 0 {
		t.Fatalf("running production interrupted: %+v", got)
	}
	completion := result(r, true)
	completion.ReleasePending = true
	if err := q.Complete(completion); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if q.fleetProductionIdle() {
		t.Fatal("completion mistaken for released remote custody")
	}
	release := result(r, true)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	check(t, q, workers[0], nil)
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	// Recheck at authorization too, even after the coordinator selected cutover.
	q.calls = map[string]*ModelCall{"late": {Priority: "production", Machine: workers[0].ID}}
	if instruction, err := q.fleetInstruction(i.Host, i.Machine, i.Session); err != nil || instruction.Operation != "" {
		t.Fatalf("late production bypassed fence: %+v %v", instruction, err)
	}
	delete(q.calls, "late")
	fleetActivate(t, q, c)
	fleetTick(t, q)
	if q.fleetPausesProduction() {
		t.Fatal("production admission not restored")
	}
	if got := job(t, q, j.ID); got.State != "succeeded" || got.Attempt != 1 || got.Losses != 0 {
		t.Fatalf("attempt changed: %+v", got)
	}
}

func TestFleetUpgradeReportsSanitizedStoragePreflightFailure(t *testing.T) {
	q, _, _ := fleetFixture(t, "queue", "remote")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{QueueOnly: true}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: "fleet_storage_preflight_failed"})
	if err != nil || q.fleetUpgradeStatus().Hosts[0].Failure != "storage_preflight_failed" {
		t.Fatalf("specific safe failure lost: %+v %v", q.fleetUpgradeStatus(), err)
	}
}
