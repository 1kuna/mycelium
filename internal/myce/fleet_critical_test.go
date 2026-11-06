package myce

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestFleetUpgradeCriticalFixForwardRetainsStopEvidence(t *testing.T) {
	q, c, _ := fleetFixture(t, "host")
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
	if err := q.ConfigureEnrollmentRelease(sign(), pub, "https://publisher.invalid/release"); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("worker", "host", "cpu", 8, 16)
	m.Fleet = fleetTestProof(c, m.Session, 200, false)
	check(t, q, m, nil)
	s := cpuSpec(1, 1)
	s.Resumable = true
	j := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("attempt not assigned")
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: run.ID, Attempt: run.Attempt, Progress: 7}, Started: true}); err != nil {
		t.Fatal(err)
	}
	request := FleetUpgradeRequest{Urgency: "critical", QueueOnly: true}
	first, err := q.StartFleetUpgrade(request)
	if err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	want := append([]FleetStopRequest(nil), q.fleetActiveHost().StopRequests...)
	if len(want) != 1 || want[0].ProgressBefore != 7 {
		t.Fatal("missing original stop evidence", want)
	}
	completion := result(run, false)
	completion.Interrupted, completion.ReleasePending = true, true
	if err := q.Complete(completion); err != nil {
		t.Fatal(err)
	}
	release := result(run, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	i := fleetInstructionFor(t, q)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: "fleet_storage_preflight_failed"}); err != nil {
		t.Fatal(err)
	}
	manifest.Version = "v1.0.2"
	if err := q.approveEnrollmentRelease(EnrollmentReleaseApproval{SignedRelease: sign(), PublicKey: pub, Origin: "https://publisher.invalid/release"}); err != nil {
		t.Fatal(err)
	}
	request.Release = manifest.Version
	next, err := q.StartFleetUpgrade(request)
	if err != nil || next.Operation == first.Operation {
		t.Fatal("newer release did not replace failed critical operation", next, err)
	}
	q = reopenCancellationQueue(t, q, c)
	if len(q.s.FleetSuperseded) != 1 {
		t.Fatal("replaced operation's evidence was not durable", q.s.FleetSuperseded)
	}
	record := q.s.FleetSuperseded[0]
	if record.Operation != first.Operation || record.Urgency != "critical" || record.State != "stopped" || len(record.Hosts) != 1 || !reflect.DeepEqual(record.Hosts[0].StopRequests, want) {
		t.Fatal("fix-forward lost stop identity, timestamp or accepted progress", record)
	}
	if policy := q.s.HostMaintenance["host"]; policy.Mode != "drained" || policy.DrainOwner != first.Operation {
		t.Fatal("archiving interruption evidence changed drain custody", policy)
	}
}

func TestFleetUpgradeCriticalCheckpointRequeueAndCustody(t *testing.T) {
	for _, priority := range []string{"experiment", "production"} {
		t.Run(priority, func(t *testing.T) {
			q, c, _ := fleetFixture(t, "host")
			m := cpuMachine("worker", "host", "cpu", 8, 16)
			m.Fleet = fleetTestProof(c, m.Session, 200, false)
			check(t, q, m, nil)
			s := cpuSpec(1, 1)
			s.Priority, s.Resumable = priority, true
			if priority == "production" {
				s.StartWindow = "10m" // Authored window covers stop grace and service-health verification.
			}
			j := submit(t, q, s)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != j.ID {
				t.Fatal("attempt not assigned")
			}
			if err := q.Started(result(run, true)); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: run.ID, Attempt: run.Attempt, Progress: 7}, Started: true}); err != nil {
				t.Fatal(err)
			}
			request := FleetUpgradeRequest{Urgency: "critical", QueueOnly: true}
			out, err := q.StartFleetUpgrade(request)
			if err != nil || out.Urgency != "critical" {
				t.Fatal(out, err)
			}
			fleetTick(t, q)
			h := q.fleetActiveHost()
			wantDeadline := c.Now().Add(StopGrace)
			if h == nil || h.Phase != "draining" || h.CriticalStopAt == nil || !h.CriticalStopAt.Equal(wantDeadline) {
				t.Fatal("critical update did not request immediate maintenance stop", h)
			}
			if got := job(t, q, j.ID); got.State != "stopping" || got.StopAt == nil || !got.StopAt.Equal(wantDeadline) || got.InfrastructureRetries != 0 || got.Losses != 0 {
				t.Fatalf("wrong stop/classifier state: state=%s stop=%v infra=%d loss=%d", got.State, got.StopAt, got.InfrastructureRetries, got.Losses)
			}
			if len(h.StopRequests) != 1 || !h.StopRequests[0].Resumable || h.StopRequests[0].ProgressBefore != 7 || h.StopRequests[0].Obligation.Result.Attempt != run.Attempt {
				t.Fatal("exact stop intent not reported", h.StopRequests)
			}
			reply, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: run.ID, Attempt: run.Attempt, Progress: 9}, Started: true})
			if err != nil || !reply.Stop || reply.Kill || reply.GraceSeconds != StopGrace.Seconds() {
				t.Fatal("checkpoint grace not delivered through existing stop protocol", reply, err)
			}
			dir := filepath.Join(q.root, run.Checkpoint, "attempt-1")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":9},"value":"saved-before-release"}`), 0600); err != nil {
				t.Fatal(err)
			}
			completion := result(run, false)
			completion.Interrupted, completion.ReleasePending, completion.Progress = true, true, 9
			if err := q.Complete(completion); err != nil {
				t.Fatal(err)
			}
			c.Advance(StopGrace + time.Second)
			fleetHeartbeatAdvance(t, q, c)
			if got := job(t, q, j.ID); got.State != "queued" || got.Progress != 9 || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Attempt != 1 {
				t.Fatal("planned checkpoint requeue lost progress or charged retry", got)
			}
			if !h.CriticalStopAt.Equal(wantDeadline) || len(h.StopRequests) != 1 {
				t.Fatal("maintenance observation renewed grace or duplicated stop intent")
			}
			if instruction, err := q.fleetInstruction("host", "@queue", "queue-old"); err != nil || instruction.Operation != "" {
				t.Fatal("elapsed stop grace bypassed release custody", instruction, err)
			}
			release := result(run, false)
			release.ReleaseOnly = true
			if err := q.Complete(release); err != nil {
				t.Fatal(err)
			}
			if instruction := fleetInstructionFor(t, q); instruction.Machine != "@queue" {
				t.Fatal("exact release did not permit queue cutover")
			}
			fleetActivate(t, q, c)
			fleetTick(t, q)
			resumed := check(t, q, m, nil).Job
			if resumed == nil || resumed.ID != j.ID || resumed.Attempt != 2 || q.s.FleetUpgrade.State != "complete" {
				t.Fatal("updated queue did not resume the same job", q.fleetUpgradeStatus())
			}
			resumed.Checkpoint = filepath.Join(q.root, resumed.Checkpoint)
			prepared, err := prepareCheckpoint(*resumed)
			if err != nil {
				t.Fatal(err)
			}
			if generation, err := checkpointProgress(prepared); err != nil || generation != 9 {
				t.Fatal("resumed attempt did not restore the saved checkpoint", generation, err)
			}
		})
	}
}

func TestFleetUpgradeCriticalScopeAndDurableIntent(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "remote")
	m := workers[0]
	job := submit(t, q, spec("experiment"))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != job.ID {
		t.Fatal("remote work not assigned")
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	q.calls = map[string]*ModelCall{"stream": {ID: "stream", Machine: m.ID, Session: m.Session, Model: "test", Attempt: "engine", Priority: "experiment"}}
	request := FleetUpgradeRequest{Urgency: "critical", QueueOnly: true}
	if _, err := q.StartFleetUpgrade(request); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	h := q.fleetActiveHost()
	deadline := *h.CriticalStopAt
	if q.calls["stream"].StopAt == nil || !q.calls["stream"].StopAt.Equal(deadline) || !q.fleetPausesCalls() || len(h.StopRequests) != 1 {
		t.Fatal("queue cutover did not stop its remote stream", h)
	}
	if q.s.Jobs[job.ID].State != "running" || q.s.Jobs[job.ID].StopAt != nil || q.maintenanceBlocked("remote") {
		t.Fatal("queue-only critical stop escaped its batch/host scope")
	}
	beforePolicy := q.s.HostMaintenance["remote"]
	q = reopenCancellationQueue(t, q, c)
	fakeCheckpointAvailability(t, q, true)
	q.fleetQueue = &fleetQueueRuntime{Host: "queue", OS: "linux", Arch: "amd64", Proof: fleetTestProof(c, "queue-old", 100, false)}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{QueueOnly: true}); err == nil {
		t.Fatal("normal invocation silently resumed a critical operation")
	}
	out, err := q.StartFleetUpgrade(request)
	if err != nil || out.Urgency != "critical" || len(out.Hosts[0].StopRequests) != 1 || !q.fleetActiveHost().CriticalStopAt.Equal(deadline) || !reflect.DeepEqual(beforePolicy, q.s.HostMaintenance["remote"]) {
		t.Fatal("restart/rerun changed urgency, grace, stop evidence or operator policy", out, err)
	}
}

func TestFleetUpgradeUrgencyRejectsUnknownWithoutMutation(t *testing.T) {
	q, _, _ := fleetFixture(t, "queue")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Urgency: "force"}); err == nil || q.s.FleetUpgrade != nil {
		t.Fatal("unknown urgency created an operation", err)
	}
	out, err := q.StartFleetUpgrade(FleetUpgradeRequest{})
	if err != nil || out.Urgency != "normal" {
		t.Fatal("legacy/default intent did not remain normal", out, err)
	}
}

func TestFleetUpgradeCriticalWaitsForFreshContactPreservingOperatorDrain(t *testing.T) {
	q, c, workers := fleetFixture(t, "host", "host")
	m := workers[0]
	j := submit(t, q, spec("experiment"))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("attempt not assigned")
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	operator, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{Urgency: "critical", QueueOnly: true}); err != nil {
		t.Fatal(err)
	}
	q.unreported[m.ID] = true
	fleetTick(t, q)
	h := q.fleetActiveHost()
	if h == nil || h.Owned || h.CriticalStopAt != nil || q.s.Jobs[j.ID].StopAt != nil || !reflect.DeepEqual(operator.Policy, q.s.HostMaintenance["host"]) {
		t.Fatal("unknown contact spent checkpoint grace or replaced operator policy")
	}
	check(t, q, m, run)
	if h.CriticalStopAt == nil || !h.CriticalStopAt.Equal(c.Now().Add(StopGrace)) || len(h.StopRequests) != 1 || !reflect.DeepEqual(operator.Policy, q.s.HostMaintenance["host"]) {
		t.Fatal("fresh contact failed to start the bounded stop without policy mutation")
	}
}
