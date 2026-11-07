package myce

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFleetUpgradeCheckpointHealthClassification(t *testing.T) {
	for _, scenario := range []string{"preexisting_probe", "new_probe", "unavailable_root", "recovered_probe"} {
		t.Run(scenario, func(t *testing.T) {
			q, c, workers := fleetFixture(t, "queue", "worker-host")
			q.fleetQueue.Proof = fleetTestProof(c, "queue-current", 100, true)
			m := workers[0]
			m.CheckpointUnavailable = scenario != "new_probe"
			check(t, q, m, nil)
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			for n := 0; n < 32; n++ {
				fleetHeartbeatAdvance(t, q, c)
			}
			h := q.fleetActiveHost()
			if h == nil || h.Host != "worker-host" {
				t.Fatal("worker was not selected", h)
			}
			m = cloneMachine(q.s.Machines[m.ID])
			if scenario == "recovered_probe" {
				m.CheckpointUnavailable = false
				check(t, q, m, nil)
			}
			m.CheckpointUnavailable = true
			if scenario == "unavailable_root" {
				m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "private probe detail"}}
			}
			check(t, q, m, nil)
			fleetActivate(t, q, c)
			state := "blocked"
			if scenario == "preexisting_probe" {
				state = "advisory"
				if q.s.FleetUpgrade.State != "complete" || q.maintenanceBlocked(h.Host) {
					t.Fatal("unchanged probe with no dependent work blocked verified rollout")
				}
			} else if q.s.FleetUpgrade.State == "complete" || !q.maintenanceBlocked(h.Host) {
				t.Fatal("new degradation or positive storage failure resumed admission")
			}
			status := q.fleetUpgradeStatus()
			found := false
			for _, check := range status.Hosts[1].Checks {
				if check.Check == "checkpoint_probe" {
					found = check.State == state && check.Capability == "nas_checkpoint"
				}
			}
			data, err := json.Marshal(status)
			if !found || err != nil || strings.Contains(string(data), "private probe detail") {
				t.Fatal("missing typed capability classification or raw probe disclosure", string(data), err)
			}
			if !q.s.Machines[m.ID].CheckpointUnavailable {
				t.Fatal("rollout health changed the worker's admission evidence")
			}
		})
	}
}

func TestFleetUpgradeCheckpointProbeStillBlocksDependentWork(t *testing.T) {
	q, _, workers := fleetFixture(t, "queue", "worker-host")
	m := workers[0]
	m.CheckpointUnavailable = true
	check(t, q, m, nil)
	j := submit(t, q, spec("experiment"))
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	h := q.s.FleetUpgrade.Hosts[1]
	if q.fleetCheckpointAdvisory(h, q.s.Machines[m.ID]) || q.s.Jobs[j.ID].State != "queued" {
		t.Fatal("checkpoint-dependent work treated a failed probe as advisory")
	}
}
