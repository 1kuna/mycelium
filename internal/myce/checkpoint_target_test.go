package myce

import (
	"testing"
)

// #1437: a planned stop of checkpointed multi-target work resumes on the target
// that wrote the checkpoint, waiting while that host is busy. Declared tested
// resume_compatibility, or that target leaving the fleet, permits another.
func TestPlannedStopResumesOnCheckpointTarget(t *testing.T) {
	for _, mode := range []string{"bound", "portable", "left"} {
		t.Run(mode, func(t *testing.T) {
			q, _ := fixture(t)
			a, b := machine("a", "fake", "host-a"), machine("b", "fake", "host-b")
			a.CapabilityProfile.Arch = "arm64"
			s := spec("experiment")
			s.Targets = []ExecutionTarget{{OS: "linux", Arch: "arm64"}, {OS: "linux", Arch: "amd64"}}
			if mode == "portable" {
				s.ResumeCompatibility = &ResumeCompatibility{CheckpointFormat: "counter-v1", ResultContract: "counter-result-v1", Targets: []ResumeTarget{
					{Target: ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "fake"}, Environment: "none", ResultCheck: "counter-resume-test"},
					{Target: ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "fake"}, Environment: "none", ResultCheck: "counter-resume-test"},
				}}
			}
			submit(t, q, s)
			j := check(t, q, a, nil).Job
			if j == nil || j.Machine != a.ID {
				t.Fatal("not placed on A", j)
			}
			if err := q.Started(result(j, false)); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Checkin(Checkin{Machine: a, Running: &Execution{ID: j.ID, Attempt: j.Attempt, Progress: 3}, Started: true}); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID).CheckpointTarget; got == nil || got.Target != (ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "fake"}) || got.Generation != 3 {
				t.Fatal("checkpoint target not recorded", got)
			}

			// Planned maintenance stop, acknowledged and released.
			if _, err := q.ChangeHostMaintenance("host-a", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
				t.Fatal(err)
			}
			finishInterrupted(t, q, j)
			r := result(j, true)
			r.ReleaseOnly = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID); got.State != "queued" || got.InfrastructureRetries != 0 {
				t.Fatal("planned stop did not requeue", got.State, got.InfrastructureRetries)
			}

			switch mode {
			case "portable":
				if got := check(t, q, b, nil).Job; got == nil || got.ID != j.ID {
					t.Fatal("declared portable checkpoint did not resume on B", got)
				}
				return
			case "left":
				if err := q.Leave(a.ID, false); err != nil {
					t.Fatal(err)
				}
				if got := check(t, q, b, nil).Job; got == nil || got.ID != j.ID {
					t.Fatal("checkpoint target left the fleet but B did not resume it", got)
				}
				return
			}

			// A host of the checkpoint's target exists: B never takes it, during
			// maintenance or while A is busy with other work.
			if got := check(t, q, b, nil).Job; got != nil {
				t.Fatal("resumed across targets during maintenance", got.ID)
			}
			if h := waitFor(t, q, j.ID); h == nil || h.Kind != checkpointTargetHold {
				t.Fatal("status does not explain the checkpoint-target wait", h)
			}
			filler := spec("deadline")
			filler.Name, filler.AllowedMachines = "filler", []string{a.ID}
			submit(t, q, filler)
			if _, err := q.ChangeHostMaintenance("host-a", HostMaintenanceRequest{Action: "resume"}); err != nil {
				t.Fatal(err)
			}
			busy := check(t, q, a, nil).Job
			if busy == nil || busy.Spec.Name != "filler" {
				t.Fatal("A did not take the filler", busy)
			}
			if got := check(t, q, b, nil).Job; got != nil {
				t.Fatal("resumed across targets while A was busy", got.ID)
			}
			if h := waitFor(t, q, j.ID); h == nil || h.Kind != checkpointTargetHold {
				t.Fatal("busy wait not explained", h)
			}
			finish(t, q, busy, true)
			got := check(t, q, a, nil).Job
			if got == nil || got.ID != j.ID || got.Machine != a.ID {
				t.Fatal("did not resume on the checkpoint's target", got)
			}
		})
	}
}

func waitFor(t *testing.T, q *Queue, id string) *Hold {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.waitHold(q.s.Jobs[id])
}
