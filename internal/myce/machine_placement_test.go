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
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
	for _, j := range jobs {
		c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true})
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
	q.stopBatchFor(q.s.Jobs[a.ID], winner)
	if err := q.Complete(Result{Machine: a.Machine, Session: a.Session, Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	if len(q.s.BatchReleases) != 1 {
		t.Fatal("missing confirmed release credit", q.s.BatchReleases)
	}
	for _, target := range q.machineTargets(q.s.Machines[m.ID]) {
		if target.Device == "1" && q.roomPlan(winner, target, true) != nil {
			t.Fatal("release on device 0 supplied GPU capacity on device 1")
		}
	}
	machineCheck(t, q, m)
	if len(q.s.BatchReleases) != 0 {
		t.Fatal("fresh inventory retained speculative release credit", q.s.BatchReleases)
	}
}

func TestM3DockerCreateKeepsLaunchAuthorization(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.Capabilities = []string{"docker"}
	s := dockerJob().Spec
	s.Needs.GPU = &GPUSelection{Devices: []string{"1"}}
	j := submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != j.ID {
		t.Fatal("missing Docker assignment", a)
	}
	m.DockerCleanup = "creating"
	machineCheck(t, q, m, a)
	launch := HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: "1"}
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatalf("own device create rejected: %v", err)
	}
	if q.s.Machines[m.ID].DockerCleanup != "creating" {
		t.Fatal("authorization changed persisted diagnostics")
	}
	m.DockerCleanup = "cleanup unconfirmed: inspect"
	machineCheck(t, q, m, a)
	if err := q.AuthorizeLaunch(launch); err == nil {
		t.Fatal("unconfirmed cleanup permitted launch")
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
