package myce

import "testing"

func TestFleetUpgradeCheckpointRecoveryWhileStoppedSurvivesRestart(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "worker-host")
	q.fleetQueue.Proof = fleetTestProof(c, "queue-current", 100, true)
	m := workers[0]
	m.CheckpointUnavailable = true
	check(t, q, m, nil)
	request := FleetUpgradeRequest{}
	if _, err := q.StartFleetUpgrade(request); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	for n := 0; n < 32; n++ {
		fleetHeartbeatAdvance(t, q, c)
	}
	i := fleetInstructionFor(t, q)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: "fleet_storage_preflight_failed"}); err != nil {
		t.Fatal(err)
	}
	// Positive recovery is accepted even though fleet runtime proof is absent
	// and the failed operation does not run its worker-health phase.
	m = cloneMachine(q.s.Machines[m.ID])
	m.CheckpointUnavailable, m.Fleet = false, nil
	check(t, q, m, nil)
	if !q.fleetActiveHost().CheckpointProbeRecovered {
		t.Fatal("accepted stopped-state recovery was discarded")
	}
	q = reopenCancellationQueue(t, q, c)
	fakeCheckpointAvailability(t, q, true)
	q.fleetQueue = &fleetQueueRuntime{Host: "queue", OS: "linux", Arch: "amd64", Proof: fleetTestProof(c, "queue-current", 100, true)}
	m.CheckpointUnavailable = true
	m.Fleet = fleetTestProof(c, m.Session, 200, false)
	check(t, q, m, nil)
	if _, err := q.StartFleetUpgrade(request); err != nil {
		t.Fatal(err)
	}
	fleetActivate(t, q, c)
	h := q.fleetActiveHost()
	if h == nil || !h.CheckpointProbeRecovered || q.fleetCheckpointAdvisory(h, q.s.Machines[m.ID]) || !q.maintenanceBlocked(h.Host) || q.s.FleetUpgrade.State == "complete" {
		t.Fatal("stopped/restart/retry forgot recovery and waived a new failure", q.fleetUpgradeStatus())
	}
}

func TestFleetUpgradeCheckpointDependencyUsesDeviceQualification(t *testing.T) {
	for _, scenario := range []string{"secondary_device", "cpu_excluded_by_platform"} {
		t.Run(scenario, func(t *testing.T) {
			q, c, _ := fleetFixture(t, "queue")
			m := m3Machine()
			m.Devices[1].Platform = "cuda"
			m.CapabilityProfile.Accelerators = append(m.CapabilityProfile.Accelerators, AcceleratorProfile{Type: "cuda", MemoryBytes: 8 << 30})
			if scenario == "cpu_excluded_by_platform" {
				m = cpuMachine("cpu-only", "cpu-only", "cpu", 8, 32)
				peer := cpuMachine("accelerated", "accelerated", "cuda", 8, 32)
				check(t, q, peer, nil)
			}
			m.CheckpointUnavailable = true
			m.Fleet = fleetTestProof(c, m.Session, 200, false)
			check(t, q, m, nil)
			s := spec("experiment")
			s.Needs.Platform = Platform{Required: []string{"cuda"}}
			if scenario == "secondary_device" {
				s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cuda"}}
			}
			j := submit(t, q, s)
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{QueueOnly: true}); err != nil {
				t.Fatal(err)
			}
			stored := q.s.Machines[m.ID]
			want := scenario == "secondary_device"
			if got := q.fleetCheckpointDependent(q.s.Jobs[j.ID], stored); got != want {
				q.evaluateTargetsOnMachines(q.s.Jobs[j.ID], false, map[string]*Machine{m.ID: stored}, func(f targetFit, _ *Job, target *Machine) bool {
					t.Log("qualification", target.GPU, target.Device, f.failed)
					return true
				})
				t.Fatal("dependency disagrees with supported device/platform qualification", got, want)
			}
		})
	}
}

func TestFleetUpgradeComponentChecksMatchEnrollmentBaseline(t *testing.T) {
	for _, scenario := range []string{"cpu_count", "ram_total", "cpu_budget"} {
		t.Run(scenario, func(t *testing.T) {
			q, _, workers := fleetFixture(t, "queue", "worker-host")
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			m := cloneMachine(q.s.Machines[workers[0].ID])
			switch scenario {
			case "cpu_count":
				m.CapabilityProfile.LogicalCPUs++
			case "ram_total":
				m.CapabilityProfile.SystemMemoryBytes += 1 << 30
			case "cpu_budget":
				m.CPUBudgetCores++
			}
			check(t, q, m, nil)
			want := "worker_capacity_mismatch"
			if scenario == "cpu_budget" {
				// Accepted check-in derives a positive CPU policy error before
				// enrollment's final acknowledged-budget comparison.
				want = "worker_component_unavailable"
			}
			found := false
			for _, check := range q.fleetUpgradeStatus().Hosts[1].Checks {
				if check.Check == "worker_components" {
					found = check.State != "ready" && check.Code == want
				}
			}
			if !found {
				t.Fatal("positive changed facts incorrectly reported ready", q.fleetUpgradeStatus())
			}
		})
	}
}
