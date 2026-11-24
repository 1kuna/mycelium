package myce

import (
	"context"
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

func machineCheck(t *testing.T, q *Queue, m Machine, jobs ...*Job) Reply {
	t.Helper()
	m.Devices = slices.Clone(m.Devices)
	c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol} // Current worker timing header.
	for _, j := range jobs {
		// This helper already models each reservation as resident in the card's
		// free counter below. Supply the matching per-attempt sensor evidence;
		// missing/stale/uncertain telemetry tests construct raw reports instead.
		gb := deviceShareGB(j)
		c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt, Progress: j.Progress}, Session: j.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{DeviceGB: &gb}})
		for i := range c.Machine.Devices {
			if slices.Contains(j.AllocatedDevices, c.Machine.Devices[i].ID) {
				c.Machine.Devices[i].FreeMemoryGB = max(0, c.Machine.Devices[i].FreeMemoryGB-gb)
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
