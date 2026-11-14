package myce

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Critical stops at once; normal waits before using planned leave. Both save a
// checkpoint, requeue without charge and resume it on the updated host. Exact
// release custody still gates the update, including past the stop deadline.
func TestFleetUpgradeCriticalCheckpointRequeueAndCustody(t *testing.T) {
	for _, tc := range []struct{ urgency, priority, backend string }{
		{"critical", "experiment", "cpu"}, {"critical", "production", "cpu"},
		{"normal", "experiment", "cpu"}, {"normal", "experiment", "fake"},
	} {
		t.Run(tc.urgency+"/"+tc.priority+"/"+tc.backend, func(t *testing.T) {
			priority := tc.priority
			q, c, _ := fleetFixture(t, "host")
			m := cpuMachine("worker", "host", tc.backend, 8, 16)
			m.Fleet = fleetTestProof(c, m.Session, 200, false)
			check(t, q, m, nil)
			s := cpuSpec(1, 1)
			s.Priority, s.Resumable = priority, true
			if tc.backend != "cpu" {
				s = spec(priority)
			}
			if priority == "production" {
				s.StartWindow = "10m" // Covers stop grace and two stability windows.
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
			out, err := q.StartFleetUpgrade(FleetUpgradeRequest{Urgency: tc.urgency})
			if err != nil || out.Urgency != tc.urgency {
				t.Fatal(out, err)
			}
			fleetTick(t, q)
			h := q.fleetActiveHost()
			if tc.urgency == "normal" {
				if got := job(t, q, j.ID); got.State != "running" || len(h.PlannedStops) != 1 || len(h.StopRequests) != 0 || !h.PlannedStops[0].StopAfter.Equal(c.Now().Add(StopGrace)) || h.PlannedStops[0].Reason == "" {
					t.Fatal("normal stop was not planned before signalling", h)
				}
				c.Advance(StopGrace - time.Nanosecond)
				fleetRefresh(t, q, c)
				if job(t, q, j.ID).State != "running" {
					t.Fatal("normal stop before the bounded wait")
				}
				c.Advance(time.Nanosecond)
				fleetRefresh(t, q, c)
			}
			wantDeadline := c.Now().Add(StopGrace)
			if h == nil || h.Phase != "draining" || tc.urgency == "critical" && (h.CriticalStopAt == nil || !h.CriticalStopAt.Equal(wantDeadline)) {
				t.Fatal("update did not request the maintenance stop", h)
			}
			if got := job(t, q, j.ID); got.State != "stopping" || got.StopAt == nil || !got.StopAt.Equal(wantDeadline) || got.InfrastructureRetries != 0 || got.Losses != 0 {
				t.Fatalf("wrong stop state: state=%s stop=%v infra=%d loss=%d", got.State, got.StopAt, got.InfrastructureRetries, got.Losses)
			}
			if len(h.StopRequests) != 1 || !h.StopRequests[0].Resumable || h.StopRequests[0].ProgressBefore != 7 || h.StopRequests[0].Obligation.Result.Attempt != run.Attempt {
				t.Fatal("stop not reported", h.StopRequests)
			}
			reply, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: run.ID, Attempt: run.Attempt, Progress: 9}, Started: true})
			if err != nil || !reply.Stop || reply.Kill || reply.GraceSeconds != StopGrace.Seconds() {
				t.Fatal("checkpoint grace not delivered through the stop protocol", reply, err)
			}
			dir := filepath.Join(run.Checkpoint, "attempt-1")
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
			fleetRefresh(t, q, c)
			if got := job(t, q, j.ID); got.State != "queued" || got.Progress != 9 || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Attempt != 1 || got.MaintenanceResume == nil {
				t.Fatal("planned checkpoint requeue lost progress or charged a retry", got)
			}
			if tc.urgency == "critical" && !h.CriticalStopAt.Equal(wantDeadline) || len(h.StopRequests) != 1 {
				t.Fatal("observation renewed grace or duplicated the stop")
			}
			if i, err := q.fleetInstruction("host", m.ID, m.Session); err != nil || i.Operation != "" {
				t.Fatal("elapsed stop grace bypassed release custody", i, err)
			}
			release := result(run, false)
			release.ReleaseOnly = true
			if err := q.Complete(release); err != nil {
				t.Fatal(err)
			}
			if got := fleetActivate(t, q, c); got != "host/worker" {
				t.Fatal("worker must update before its queue", got)
			}
			fleetTick(t, q)
			if got := fleetActivate(t, q, c); got != "host/@queue" {
				t.Fatal(got)
			}
			fleetTick(t, q)
			updated := cloneMachine(q.s.Machines[m.ID])
			resumed := check(t, q, updated, nil).Job
			if resumed == nil || resumed.ID != j.ID || resumed.Attempt != 2 || q.s.FleetUpgrade.State != "complete" {
				t.Fatal("updated host did not resume the same job", q.fleetUpgradeStatus())
			}
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
