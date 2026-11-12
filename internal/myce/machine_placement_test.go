package myce

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"
)

func m3Machine() Machine {
	m := cpuMachine("machine", "machine", "fake", 8, 32)
	m.Devices = []MachineDevice{
		{ID: "0", Platform: "fake", Identity: "fake-0", Fingerprint: "fake", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8},
		{ID: "1", Platform: "fake", Identity: "fake-1", Fingerprint: "fake", Selector: "1", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8},
	}
	return m
}

func TestMachinePlacementAllocationViewScope(t *testing.T) {
	m := m3Machine()
	m.Devices[0].CapabilityProfile = m.CapabilityProfile
	m.Devices[1].UnifiedMemory = true
	projected := m
	projected.FreeHostMemoryGB = 3
	legacy := m
	legacy.Devices = nil
	q := &Queue{memoryPlanning: newMemoryPlanningCache()}
	for _, owner := range []*Machine{nil, &legacy, &m, &projected} {
		for _, devices := range [][]string{nil, {}, {"0"}, {"1"}, {"0", "1"}, {"1", "0"}, {"missing"}, {"0", "missing"}} {
			want := allocationMachine(owner, devices)
			got := q.planningAllocationMachine(owner, devices)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("devices %v: cached view differs from fresh projection", devices)
			}
			if q.planningAllocationMachine(owner, devices) != got {
				t.Fatalf("devices %v: immutable pass rebuilt its view", devices)
			}
		}
	}
	// Neither a different capacity projection of the same worker nor a later
	// inventory pass may borrow the original worker's available room or identity.
	first := q.planningAllocationMachine(&m, []string{"0"})
	if got := q.planningAllocationMachine(&projected, []string{"0"}); got == first || got.FreeHostMemoryGB != 3 {
		t.Fatal("capacity projection reused another owner's view")
	}
	m.Devices[0].FreeMemoryGB = 2
	m.Devices[0].ExtensionFingerprint = "new-identity"
	q.memoryPlanning = newMemoryPlanningCache()
	next := q.planningAllocationMachine(&m, []string{"0"})
	if next == first || next.FreeMemoryGB != 2 || next.ExtensionFingerprint != "new-identity" || first.FreeMemoryGB != 8 {
		t.Fatal("new pass retained old inventory or mutated an earlier view")
	}
	q.memoryPlanning = nil
	m.Devices[0].FreeMemoryGB = 1
	if got := q.planningAllocationMachine(&m, []string{"0"}); got == next || got.FreeMemoryGB != 1 {
		t.Fatal("view escaped the immutable planning scope")
	}
}

func machineCheck(t *testing.T, q *Queue, m Machine, jobs ...*Job) Reply {
	t.Helper()
	m.Devices = slices.Clone(m.Devices)
	c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol} // Current worker timing header.
	for _, j := range jobs {
		// This helper already models each reservation as resident in the card's
		// free counter below. Supply the matching per-attempt sensor evidence;
		// missing/stale/uncertain telemetry tests construct raw reports instead.
		gb := j.Spec.Needs.MemoryGB
		c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{DeviceGB: &gb}})
		for i := range c.Machine.Devices {
			if slices.Contains(j.AllocatedDevices, c.Machine.Devices[i].ID) {
				c.Machine.Devices[i].FreeMemoryGB = max(0, c.Machine.Devices[i].FreeMemoryGB-j.Spec.Needs.MemoryGB)
			}
		}
	}
	r, err := q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func assignedJob(r Reply) *Job {
	for _, a := range r.Attempts {
		if a.Job != nil {
			return a.Job
		}
	}
	return nil
}

func TestM3ConcurrentDeviceSlotsAndCPU(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	first := submit(t, q, spec("experiment"))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != first.ID || !slices.Equal(a.AllocatedDevices, []string{"0"}) {
		t.Fatal("first device assignment", a)
	}
	second := submit(t, q, spec("experiment"))
	b := assignedJob(machineCheck(t, q, m3Machine(), a))
	if b == nil || b.ID != second.ID || !slices.Equal(b.AllocatedDevices, []string{"1"}) {
		t.Fatal("neighbor was not admitted on the free device", b)
	}
	third := submit(t, q, spec("experiment"))
	if got := assignedJob(machineCheck(t, q, m3Machine(), a, b)); got != nil {
		t.Fatal("same-card overlap", got.ID)
	}
	cpu := submit(t, q, cpuSpec(1, 1))
	c := assignedJob(machineCheck(t, q, m3Machine(), a, b))
	if c == nil || c.ID != cpu.ID || len(c.AllocatedDevices) != 0 {
		t.Fatal("CPU backfill beside two GPU jobs", c)
	}
	requireState(t, q, third.ID, "queued")
}

// Host RAM serializes these otherwise equal cards. History must survive queue
// reopen, worker sessions, inventory ordering and ordinal changes.
func TestM3SerializedDevicesAlternate(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.HostMemoryBudgetGB = 24
	s := spec("experiment")
	s.Needs.HostMemoryGB = 13
	for i := 0; i < 6; i++ {
		pending := submit(t, q, s)
		a := assignedJob(machineCheck(t, q, m))
		want := m.Devices[i%2].ID
		if a == nil || a.ID != pending.ID || !slices.Equal(a.AllocatedDevices, []string{want}) {
			t.Fatalf("placement %d: got %+v, want device %s", i, a, want)
		}
		next := submit(t, q, s)
		if got := assignedJob(machineCheck(t, q, m, a)); got != nil {
			t.Fatal("host RAM did not serialize devices", got)
		}
		if err := q.Cancel(next.ID); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		if err := q.Complete(Result{Machine: a.Machine, Session: a.Session, Execution: Execution{ID: a.ID, Attempt: a.Attempt}}); err != nil {
			t.Fatal(err)
		}
		if i == 2 {
			path, root := q.dbPathForTest(t), q.root
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			q, err = Open(path, root, clock, WithCheckpointAvailability(func() bool { return true }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { q.Close() })
			previous := m.Session
			m.Session = "new-session"
			m.Devices[0].ID, m.Devices[1].ID = "1", "0"
			m.Devices[0].Selector, m.Devices[1].Selector = "1", "0"
			if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: previous, AttemptProtocol: keyedAttemptProtocol, period: time.Second}); err != nil {
				t.Fatal(err)
			}
		}
	}
	status, err := q.HostMaintenanceStatus(hostID(&m))
	if err != nil || len(status.DeviceUse) != 2 {
		t.Fatalf("device usage status: %+v, %v", status, err)
	}
	for _, use := range status.DeviceUse {
		if use.Assignments != 3 || use.LastAssignedAt.IsZero() {
			t.Fatalf("usage lost: %+v", use)
		}
	}
}

func TestM3ReleaseCreditStaysOnDevice(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.Devices[1].FreeMemoryGB = 0
	s := spec("experiment")
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	first := submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != first.ID {
		t.Fatal("missing first assignment", a)
	}
	demand := spec("production")
	demand.Needs.GPU = &GPUSelection{Devices: []string{"1"}}
	winner := submit(t, q, demand)
	drainAdmissionWait(t, q)
	if err := q.update(func() error {
		q.stopBatchFor(q.touchJob(a.ID), q.s.Jobs[winner.ID])
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(Result{Machine: a.Machine, Session: a.Session, Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	drainAdmissionWait(t, q)

	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		if len(q.s.BatchReleases) != 1 {
			t.Fatal("missing confirmed release credit", q.s.BatchReleases)
		}
		for _, target := range q.machineTargets(q.s.Machines[m.ID]) {
			if target.Device == "1" && q.roomPlan(winner, target, true) != nil {
				t.Fatal("release on device 0 supplied GPU capacity on device 1")
			}
		}
	}()
	machineCheck(t, q, m)
	drainAdmissionWait(t, q)
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.s.BatchReleases) != 0 {
		t.Fatal("fresh inventory retained speculative release credit", q.s.BatchReleases)
	}
}

func TestM3ModelLoadWaitsForDeviceProbe(t *testing.T) {
	m := m3Machine()
	m.Devices[0].Reason = "device_probe_unavailable"
	w := Worker{Machine: m, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return m3Machine().Devices, nil }, MemorySource: func(context.Context) (float64, error) { return 8, nil }}
	rt := &modelRuntime{worker: &w, models: map[string]*workerModel{}, memoryMachine: m, session: m.Session, clock: RealClock{}, config: ModelConfig{Wait: time.Minute, Models: []Model{{Name: "probe-model", Engine: "fake", Path: "fake", MemoryGB: 8}}}}
	plan := []ModelSlot{{Model: "probe-model", Device: "0", Session: m.Session, Attempt: "same-offer", State: "loading"}}
	rt.sync(plan)
	rt.wg.Wait()
	if reports, _ := rt.report(); len(reports) != 0 {
		t.Fatalf("temporary probe withholding became a failed load: %+v", reports)
	}
	rt.memoryMachine = m3Machine()
	rt.sync(plan)
	rt.wg.Wait()
	reports, _ := rt.report()
	if len(reports) != 1 || reports[0].State != "ready" || reports[0].Attempt != "same-offer" {
		t.Fatalf("matching probe did not resume the same offer: %+v", reports)
	}
}

func TestM3LegacyCopyKeepsRecoveryExclusion(t *testing.T) {
	q, clock := fixture(t)
	m := machine("legacy-recovery", "fake")
	submit(t, q, spec("experiment"))
	a := check(t, q, m, nil).Job
	clock.Advance(LossAfter)
	check(t, q, observer(), nil)
	requireState(t, q, a.ID, "queued")
	m.Devices = m3Machine().Devices
	r := machineCheck(t, q, m, a)
	for _, d := range r.Attempts {
		if d.ID == a.ID && (d.Stop || d.Hold) {
			t.Fatal("legacy exact copy could not recover", d)
		}
	}
	if got := job(t, q, a.ID); got.State != "running" || got.Attempt != a.Attempt {
		t.Fatal("legacy custody changed", got)
	}
	for _, target := range q.machineTargets(q.s.Machines[m.ID]) {
		if target.GPU != "cpu" && q.workerBatchIdle(target) {
			t.Fatal("legacy copy without device evidence left a GPU slot free", target.Device)
		}
	}
}

func TestM3SettledReportsDrainBeforeNewAssignment(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	submit(t, q, spec("experiment"))
	a := assignedJob(machineCheck(t, q, m))
	if err := q.Complete(Result{Machine: a.Machine, Session: a.Session, Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	if next := assignedJob(machineCheck(t, q, m, a)); next != nil {
		t.Fatal("new assignment prevented recovered custody from draining", next.Attempt)
	}
	if next := assignedJob(machineCheck(t, q, m)); next == nil || next.Attempt != a.Attempt+1 {
		t.Fatal("fresh admission did not resume after settled report drained", next)
	}
}

func TestM3DeviceUseKeepsExclusions(t *testing.T) {
	for _, exclusion := range []string{"quarantine", "memory", "device-disabled", "host-maintenance", "selector"} {
		t.Run(exclusion, func(t *testing.T) {
			q, _ := fixture(t)
			m := m3Machine()
			machineCheck(t, q, m)
			// Device 0 is unused; the tie-break would prefer it over device 1.
			recent := m.Devices[1].machine(m)
			q.recordDeviceUse(&recent)
			s := spec("experiment")
			switch exclusion {
			case "quarantine":
				m.Devices[0].Reason = "device_fault_xe"
			case "memory":
				m.Devices[0].FreeMemoryGB = 1
			case "device-disabled":
				changeDevice(t, q, m.HostID, "0", "disable")
			case "host-maintenance":
				if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
					t.Fatal(err)
				}
			case "selector":
				s.Needs.GPU = &GPUSelection{Devices: []string{"1"}}
			}
			pending := submit(t, q, s)
			got := assignedJob(machineCheck(t, q, m))
			if exclusion == "host-maintenance" {
				if got != nil {
					t.Fatal("usage bypassed maintenance", got)
				}
			} else if got == nil || got.ID != pending.ID || !slices.Equal(got.AllocatedDevices, []string{"1"}) {
				t.Fatal("usage bypassed exclusion", got)
			}
		})
	}
}
