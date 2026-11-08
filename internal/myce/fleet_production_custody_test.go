package myce

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestFleetProductionIdleLastCopyReleaseAfterRetry(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "remote")
	s := spec("production")
	s.AllowedMachines = []string{workers[0].ID}
	// The terminal-expiry custody check needs a caller-owned cutoff: generated
	// startup deadlines pause while the unreleased copy occupies capacity.
	deadline := c.Now().Add(ProductionStartTimeout)
	s.StartBy = &deadline
	submit(t, q, s)
	run := check(t, q, workers[0], nil).Job
	if run == nil {
		t.Fatal("no production attempt")
	}
	r := result(run, false)
	r.Interrupted, r.ReleasePending = true, true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, run.ID)
	if got.State != "queued" || got.Machine != "" || got.Session != "" || got.ReleaseConfirmed {
		t.Fatalf("retry did not clear current identity with custody retained: %+v", got)
	}
	if q.fleetProductionIdle() {
		t.Fatal("queued retry lost unreleased production custody")
	}
	c.Advance(ProductionStartTimeout + time.Second)
	q.fleetQueue.Proof.ObservedAt = c.Now()
	fleetTick(t, q)
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{QueueOnly: true, WaitProductionIdle: true}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	if q.fleetProductionIdle() || job(t, q, run.ID).State != "failed" {
		t.Fatal("failed attempt without release did not block")
	}
	r.ReleaseOnly, r.ReleasePending = true, false
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got = job(t, q, run.ID)
	if !got.Copies[0].ReleaseConfirmed || !got.ReleaseConfirmed || !q.fleetProductionIdle() {
		t.Fatalf("last exact release did not settle retried job: %+v", got)
	}
}

func TestFleetProductionIdleReconcilesPersistedCopyJobs(t *testing.T) {
	for _, recovery := range []string{"target", "newer"} {
		for _, pending := range []bool{false, true} {
			t.Run(recovery+"/pending-"+stringBool(pending), func(t *testing.T) {
				q, c, _ := fleetFixture(t, "queue")
				pub, private, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				manifest := q.s.EnrollmentRelease.Manifest
				sign := func(version string) []byte {
					manifest.Version = version
					data, _ := json.Marshal(manifest)
					envelope, _ := json.Marshal(SignedRelease{Manifest: data, Signature: ed25519.Sign(private, data)})
					return envelope
				}
				if err := q.ConfigureEnrollmentRelease(sign("v1.0.1"), pub, "https://publisher.invalid/release"); err != nil {
					t.Fatal(err)
				}
				// Two old failed jobs, one with one retained attempt and one with two.
				// Omitempty persists the unset aggregate exactly as older queues did.
				if err := q.update(func() error {
					now := c.Now()
					for n := 1; n <= 2; n++ {
						j := &Job{ID: fmt.Sprintf("failed-%d", n), Spec: spec("production"), State: "failed", Attempt: n, Queued: c.Now(), FinishedAt: &now}
						for attempt := 1; attempt <= n; attempt++ {
							copy := Copy{Machine: "remote", Session: "old", Attempt: attempt, ReleaseConfirmed: true, ReleaseReason: "execution_acknowledged"}
							if pending && n == 2 && attempt == 1 {
								copy.ReleaseConfirmed, copy.ReleaseReason = false, "awaiting_release"
							}
							j.Copies = append(j.Copies, copy)
						}
						q.putJob(j.ID, j)
					}
					if q.fleetProductionIdle() == pending || mutableJob(q.s.Jobs["failed-2"]) != pending {
						t.Fatal("guards did not use per-copy release evidence")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				// Persist an operation already waiting on the incident's stale aggregate.
				if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{QueueOnly: true, WaitProductionIdle: true}); err != nil {
					t.Fatal(err)
				}
				fleetTick(t, q)
				if err := q.update(func() error {
					h := q.s.FleetUpgrade.Hosts[0]
					if h.Phase == "pending" {
						if err := q.changeHostMaintenance(h.Host, HostMaintenanceRequest{Action: "drain", DrainOwner: q.s.FleetUpgrade.Operation}); err != nil {
							return err
						}
						h.Owned, h.Policy = true, q.s.HostMaintenance[h.Host]
					}
					h.Phase, h.Wait = "draining", "waiting for production attempts and release custody across the fleet"
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				operation := q.s.FleetUpgrade.Operation
				q = reopenCancellationQueue(t, q, c)
				fakeCheckpointAvailability(t, q, true)
				proof := fleetTestProof(c, "queue-restarted", 101, true)
				if recovery == "newer" {
					proof.Version = "v1.0.2"
				}
				q.fleetQueue = &fleetQueueRuntime{Session: proof.Session, Host: "queue", OS: "linux", Arch: "amd64", Proof: proof, ObservedAt: c.Now()}
				fleetTick(t, q)
				if q.s.FleetUpgrade.Operation != operation || q.fleetProductionIdle() == pending {
					t.Fatal("restart lost operation or misclassified copy custody", q.fleetUpgradeStatus())
				}
				if pending {
					if h := q.fleetActiveHost(); h.Phase != "draining" || h.Wait == "" || job(t, q, "failed-2").ReleaseConfirmed {
						t.Fatal("unconfirmed old copy did not retain wait", h)
					}
					r := Result{Execution: Execution{ID: "failed-2", Attempt: 1}, Machine: "remote", Session: "old", ReleaseOnly: true}
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
				}
				for _, id := range []string{"failed-1", "failed-2"} {
					if !job(t, q, id).ReleaseConfirmed || mutableJob(q.s.Jobs[id]) {
						t.Fatal("released failed job still carries mutable custody", id)
					}
				}
				fleetTick(t, q)
				if recovery == "newer" {
					if q.s.FleetUpgrade.State != "stopped" || q.fleetActiveHost().Failure != "queue_running_hash_mismatch" || !q.fleetPausesProduction() {
						t.Fatal("replacement queue did not expose supported fix-forward recovery", q.fleetUpgradeStatus())
					}

					if err := q.approveEnrollmentRelease(EnrollmentReleaseApproval{SignedRelease: sign(proof.Version), PublicKey: pub, Origin: "https://publisher.invalid/release"}); err != nil {
						t.Fatal(err)
					}
					out, err := q.StartFleetUpgrade(FleetUpgradeRequest{QueueOnly: true, WaitProductionIdle: true, Release: proof.Version})
					if err != nil || out.Operation == operation {
						t.Fatal("explicit newer approved release did not recover", out, err)
					}
				}
				// The restarted queue already runs the target bytes. Verify health
				// with its new session/PID; no second restart may be instructed.
				for n := 0; n < 34; n++ {
					fleetHeartbeatAdvance(t, q, c)
					instruction, err := q.fleetInstruction("queue", "@queue", proof.Session)
					if err != nil || instruction.Operation != "" {
						t.Fatal("already-current restarted queue instructed to restart", instruction, err)
					}
				}
				if q.s.FleetUpgrade.State != "complete" || q.fleetPausesProduction() {
					t.Fatal("persisted operation did not finish", q.fleetUpgradeStatus())
				}
			})
		}
	}
}

func TestFleetProductionIdleRetainsUnconfirmedCustody(t *testing.T) {
	q, c, _ := fleetFixture(t, "queue")
	j := &Job{ID: "failed", Spec: spec("production"), State: "failed", Attempt: 2, ReleaseConfirmed: true,
		Copies: []Copy{{Attempt: 1, ReleaseReason: "awaiting_release"}, {Attempt: 2, ReleaseConfirmed: true}}}
	if err := q.update(func() error { q.putJob(j.ID, j); return nil }); err != nil {
		t.Fatal(err)
	}
	c.Advance(24 * time.Hour)
	if q.fleetProductionIdle() || !mutableJob(j) {
		t.Fatal("current release, failure or age masked an unconfirmed older copy")
	}
	// Legacy records have no copy evidence and must retain the job-level fence.
	j.Copies, j.ReleaseConfirmed = nil, false
	if q.fleetProductionIdle() || !mutableJob(j) {
		t.Fatal("legacy attempt without release did not block")
	}
}
