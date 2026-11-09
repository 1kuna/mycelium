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
	if _, err = q.Checkin(Checkin{Machine: m, period: time.Second, Fresh: true, PreviousSession: i.Session}); err != nil {
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
		if _, err := q.Checkin(Checkin{Machine: m, period: time.Second, Fresh: true, PreviousSession: i.Session}); err != nil {
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
		if m.Fleet != nil {
			m.Fleet.ObservedAt = c.Now()
		}
		// Exercise a current-worker contact, not just a fresher inventory date.
		checkin := Checkin{Machine: *m, period: time.Second}
		for _, j := range q.liveJobs() {
			if j.Machine == m.ID && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") {
				checkin.AttemptProtocol = keyedAttemptProtocol
				checkin.Attempts = append(checkin.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: j.StartedAt != nil})
			}
		}
		if _, err := q.Checkin(checkin); err != nil {
			t.Fatal(err)
		}
	}
	q.s.LastWorkerContact = c.Now()
	if q.fleetQueue != nil && q.fleetQueue.Proof != nil {
		q.fleetQueue.Proof.ObservedAt = c.Now()
	}
	fleetTick(t, q)
}

func TestFleetUpgradeFreshQueueWithoutWorkers(t *testing.T) {
	q, c, _ := fleetFixture(t, "queue")
	upgrade, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// Retained custody must block even when no worker is currently enrolled.
	q.s.HostReleases = map[string]HostRelease{"retained": {
		Host: "queue", Job: "job", Machine: "former-worker", Session: "old", Attempt: 1,
	}}
	fleetTick(t, q)
	h := q.s.FleetUpgrade.Hosts[0]
	policy := q.s.HostMaintenance["queue"]
	if h.Phase != "pending" || h.Owned || policy.Mode == "drained" || h.Wait == "" {
		t.Fatalf("unreleased custody acquired a drain: %+v %+v", h, policy)
	}
	if i, err := q.fleetInstruction("queue", "@queue", "queue-old"); err != nil || i.Operation != "" {
		t.Fatalf("unreleased custody allowed cutover: %+v %v", i, err)
	}
	delete(q.s.HostReleases, "retained")
	c.Advance(time.Second)
	fleetTick(t, q)
	policy = q.s.HostMaintenance["queue"]
	if h.Phase != "queue_update" || !h.Owned || policy.Mode != "drained" || policy.DrainOwner != upgrade.Operation {
		t.Fatalf("queue did not progress after custody cleared: %+v", h)
	}
	if i := fleetInstructionFor(t, q); i.Machine != "@queue" || i.Operation != upgrade.Operation {
		t.Fatalf("wrong queue instruction: %+v", i)
	}
	// A newer operator drain must fence the previously authorized upgrade.
	c.Advance(time.Second)
	if _, err := q.ChangeHostMaintenance("queue", HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"}); err != nil {
		t.Fatal(err)
	}
	if i, err := q.fleetInstruction("queue", "@queue", "queue-old"); i.Operation != "" {
		t.Fatalf("replaced drain allowed cutover: %+v %v", i, err)
	}
	fleetTick(t, q)
	policy = q.s.HostMaintenance["queue"]
	if q.s.FleetUpgrade.State != "stopped" || h.Failure != "maintenance_changed" || policy.Mode != "drained" || policy.DrainOwner != "operator" {
		t.Fatalf("operator drain not preserved: %+v %+v", h, policy)
	}
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
	h := q.s.FleetUpgrade.Hosts[2]
	if h.Host != "z" || h.Phase != "pending" || h.Wait == "" || q.maintenanceBlocked("z") {
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

func TestFleetUpgradeDisabledHostRetainsQueueAndPolicyFences(t *testing.T) {
	for _, queueFailure := range []bool{false, true} {
		t.Run(fmt.Sprint("queue-failure=", queueFailure), func(t *testing.T) {
			q, c, _ := fleetFixture(t, "host", "host")
			if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "disable"}); err != nil {
				t.Fatal(err)
			}
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{WaitProductionIdle: true}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			if !queueFailure {
				fleetActivate(t, q, c)
				fleetTick(t, q)
			}
			i := fleetInstructionFor(t, q)
			if !queueFailure {
				c.Advance(time.Second)
				if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "disable"}); err != nil {
					t.Fatal(err)
				}
			}
			err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: "fleet_storage_preflight_failed"})
			// A maintenance edit already reconciles the operation to stopped,
			// so its formerly authorized helper must now be stale.
			if queueFailure && err != nil || !queueFailure && err == nil {
				t.Fatalf("unexpected acknowledgement result: %v", err)
			}
			want := "maintenance_changed"
			if queueFailure {
				want = "storage_preflight_failed"
			}
			if status := q.fleetUpgradeStatus(); status.State != "stopped" || status.Hosts[0].Failure != want || q.s.HostMaintenance["host"].Mode != "disabled" || queueFailure && !q.fleetPausesProduction() {
				t.Fatalf("disabled host lost safety fence: %+v", status)
			}
		})
	}
}

func fleetDisabledWorkerInstruction(t *testing.T) (*Queue, *fakeClock, FleetUpdateInstruction) {
	t.Helper()
	q, c, _ := fleetFixture(t, "queue", "disabled")
	if _, err := q.ChangeHostMaintenance("disabled", HostMaintenanceRequest{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	fleetActivate(t, q, c)
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	if i.Host != "disabled" {
		t.Fatal("expected disabled worker instruction", i.Host)
	}
	return q, c, i
}

func TestFleetUpgradeDisabledRetryPreservesMaintenanceReceipt(t *testing.T) {
	q, c, i := fleetDisabledWorkerInstruction(t)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: "fleet_storage_preflight_failed"}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	c.Advance(time.Second)
	if _, err := q.ChangeHostMaintenance(i.Host, HostMaintenanceRequest{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	operator := q.s.HostMaintenance[i.Host]
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if status := q.fleetUpgradeStatus(); status.State != "stopped" || status.Hosts[1].Failure != "maintenance_changed" || !reflect.DeepEqual(operator, q.s.HostMaintenance[i.Host]) {
		t.Fatalf("retry replaced the operator receipt: %+v", status)
	}
}

func TestFleetUpgradeDisabledRetryPreservesRuntimeMismatchFailure(t *testing.T) {
	q, c, i := fleetDisabledWorkerInstruction(t)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "restarting"}); err != nil {
		t.Fatal(err)
	}
	m := cloneMachine(q.s.Machines[i.Machine])
	m.Session = "unexpected-process"
	m.Fleet = fleetTestProof(c, m.Session, 999, false)
	if _, err := q.Checkin(Checkin{Machine: m, period: time.Second, Fresh: true, PreviousSession: i.Session}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if status := q.fleetUpgradeStatus(); status.State != "complete" || status.Hosts[1].Failure != "worker_running_hash_mismatch" {
		t.Fatalf("initial runtime mismatch not reported: %+v", status)
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	fleetTick(t, q)
	if status := q.fleetUpgradeStatus(); status.State != "complete" || status.Hosts[1].Phase != "failed" || status.Hosts[1].Failure != "worker_running_hash_mismatch" || status.Hosts[1].Wait == "" {
		t.Fatalf("retry became an indefinite wait: %+v", status)
	}
	if instruction, err := q.fleetInstruction(i.Host, i.Machine, i.Session); err != nil || instruction.Operation != "" {
		t.Fatalf("obsolete process authorized after retry: %+v %v", instruction, err)
	}
}
