package myce

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFleetUpgradeNormalWaitBasis(t *testing.T) {
	for _, cost := range []time.Duration{0, 700 * time.Millisecond, 5 * time.Minute} {
		t.Run(cost.String(), func(t *testing.T) {
			q, c, _ := fleetFixture(t, "q")
			m := m3Machine()
			m.Fleet = fleetTestProof(c, m.Session, 200, false)
			s := cpuSpec(1, 1)
			s.Resumable = true
			r := fleetRun(t, q, m, s)
			if cost > 0 {
				// A retained exact release from the preceding planned stop.
				released := c.Now().Add(-time.Second)
				stopped := released.Add(-cost)
				if err := q.update(func() error {
					j := q.touchJob(r.ID)
					j.Copies = append([]Copy{{Attempt: 0, StoppedAt: &stopped, ReleasedAt: &released, ReleaseConfirmed: true, StopCause: maintenanceStopCause}}, j.Copies...)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			h := q.fleetActiveHost()
			wait := StopGrace
			if cost > 0 {
				wait = min(cost, StopGrace)
			}
			deadline := c.Now().Add(wait)
			if len(h.PlannedStops) != 1 || !h.PlannedStops[0].StopAfter.Equal(deadline) {
				t.Fatal("wait did not use measured cost or the documented fallback", h)
			}
			// Exercise the persisted representation and public status projection.
			data, err := json.Marshal(q.s.FleetUpgrade)
			if err != nil {
				t.Fatal(err)
			}
			var saved fleetUpgrade
			if err := json.Unmarshal(data, &saved); err != nil || !saved.Hosts[0].PlannedStops[0].StopAfter.Equal(deadline) {
				t.Fatal("plan was not persisted", err)
			}
			status, err := json.Marshal(q.fleetUpgradeStatus())
			if err != nil || !strings.Contains(string(status), r.ID) || !strings.Contains(string(status), "stop_after") || !strings.Contains(string(status), "reason") {
				t.Fatal("status omitted planned job, reason or deadline", string(status), err)
			}
			c.Advance(wait / 2)
			fleetRefresh(t, q, c)
			// Reopen the real SQLite state mid-wait: a queue restart must not
			// grant a fresh window or lose the attempt-specific plan.
			var path string
			if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
				t.Fatal(err)
			}
			root, runtime := q.root, q.fleetQueue
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			q, err = Open(path, root, c)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			fakeCheckpointAvailability(t, q, true)
			q.fleetQueue = runtime
			fleetRefresh(t, q, c)
			h = q.fleetActiveHost()
			if !h.PlannedStops[0].StopAfter.Equal(deadline) {
				t.Fatal("observation renewed the bounded wait")
			}
			c.Advance(wait - wait/2)
			fleetRefresh(t, q, c)
			if got := job(t, q, r.ID); got.State != "stopping" || got.StopAt == nil || !got.StopAt.Equal(c.Now().Add(StopGrace)) {
				t.Fatal("measured wait did not lead to a planned stop", got.State, got.StopAt)
			}
		})
	}
}

func TestFleetUpgradeNormalMixedWork(t *testing.T) {
	for _, blocker := range []string{"non-resumable", "production", "missing-contract"} {
		t.Run(blocker, func(t *testing.T) {
			q, c, _ := fleetFixture(t, "q")
			m := m3Machine()
			m.Fleet = fleetTestProof(c, m.Session, 200, false)
			s := cpuSpec(1, 1)
			s.Resumable = true
			b := s
			b.Name = "blocker"
			if blocker == "non-resumable" {
				b.Resumable = false
			}
			if blocker == "production" {
				b.Priority, b.StartWindow = "production", "1h"
			}
			submit(t, q, b)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil {
				t.Fatal("blocker not assigned")
			}
			if err := q.Started(result(run, true)); err != nil {
				t.Fatal(err)
			}
			submit(t, q, s)
			r := assignedJob(machineCheck(t, q, m, run))
			if r == nil {
				t.Fatal("resumable concurrent attempt not assigned")
			}
			if err := q.Started(result(r, true)); err != nil {
				t.Fatal(err)
			}
			if blocker == "missing-contract" {
				if err := q.update(func() error { q.touchJob(run.ID).Checkpoint = ""; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			c.Advance(10 * StopGrace)
			fleetRefresh(t, q, c)
			h := q.fleetActiveHost()
			for _, id := range []string{r.ID, run.ID} {
				if got := job(t, q, id); got.State != "running" || got.StopAt != nil {
					t.Fatal("mixed work was stopped", id, got.State, got.StopAt)
				}
			}
			if len(h.PlannedStops) != 0 || len(h.StopRequests) != 0 {
				t.Fatal("planned a stop while protected work remained", h)
			}
			finish(t, q, run, true)
			fleetRefresh(t, q, c)
			if len(h.PlannedStops) != 1 || !h.PlannedStops[0].StopAfter.Equal(c.Now().Add(StopGrace)) {
				t.Fatal("remaining resumable work was not given a bounded wait", h)
			}
			// A near-finished job may finish naturally during that window.
			finish(t, q, r, true)
			fleetRefresh(t, q, c)
			if h.Phase != "worker_update" || len(h.StopRequests) != 0 || len(h.PlannedStops) != 0 {
				t.Fatal("natural completion did not cancel the plan", h)
			}
		})
	}
}
