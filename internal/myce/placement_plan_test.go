package myce

import (
	"runtime"
	"testing"
	"time"
)

func TestPlacementPlanSucceededProducerRetainsDeviceSlot(t *testing.T) {
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Variants[0].Needs = ResourceNeeds{MemoryGB: 1, HostMemoryGB: 1}
	e.Variants[0].References = map[string]WorkReference{"extension:sample": {InputShape: "small", Unit: "records", Units: 1}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	m.Device = "0" // The real worker also carries a scalar compatibility target.
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
	for i := range m.Devices {
		m.Devices[i].ExtensionFingerprint = "release-device-" + m.Devices[i].ID
		target := m.Devices[i].machine(m)
		r := newExtensionReport(e, &target, "extension:sample")
		r.Setup, r.Qualified, r.CheckID = true, true, "release-check"
		m.Devices[i].ExtensionReports = []ExtensionReport{r}
	}
	cpu := submit(t, q, cpuSpec(1, 1))
	cpuAttempt := assignedJob(machineCheck(t, q, m))
	if cpuAttempt == nil || cpuAttempt.ID != cpu.ID || len(cpuAttempt.AllocatedDevices) != 0 {
		t.Fatal("missing CPU neighbor", cpuAttempt)
	}
	s := Spec{Name: "producer", Project: "test", Priority: "experiment", Extension: extensionKey(e), Workload: &Workload{InputShape: "small", Units: 1}, FanOut: &FanOut{Pieces: []FanOutPiece{{Key: "item-0"}}, MaxConcurrency: 1}, Outputs: map[string]string{"out": "results"}}
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	parent := submit(t, q, s)
	c := spec("experiment")
	c.Needs.MemoryGB, c.Needs.HostMemoryGB = 1, 1
	c.After = &PieceDependency{Job: parent.ID, Outputs: map[string]string{"input": "item-0/out"}}
	consumer := submit(t, q, c)
	producer := assignedJob(machineCheck(t, q, m, cpuAttempt))
	if producer == nil || producer.Batch == nil || producer.Batch.Parent != parent.ID || len(producer.AllocatedDevices) != 1 || producer.AllocatedDevices[0] != "0" {
		t.Fatal("missing producer on device 0", producer, job(t, q, parent.ID).Reason)
	}
	r := result(producer, true)
	r.ReleasePending = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, parent.ID, "succeeded")
	finish(t, q, cpuAttempt, true)
	if got := job(t, q, consumer.ID); !dependencyReady(got) || got.Attempt != 0 {
		t.Fatal("accepted outputs did not release dependency", got)
	}
	// Custody survives restart and elapsed time; spare memory is not a free slot.
	q = reopenCancellationQueue(t, q, clock)
	clock.Advance(2 * time.Minute)
	next := assignedJob(machineCheck(t, q, m))
	if next == nil || next.ID != consumer.ID || len(next.AllocatedDevices) != 1 || next.AllocatedDevices[0] != "1" {
		t.Fatal("dependent must use the free device, not the unreleased producer's slot", next)
	}
	finish(t, q, next, true)
	c.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	pinned := submit(t, q, c)
	if got := assignedJob(machineCheck(t, q, m)); got != nil {
		t.Fatal("unreleased device admitted pinned dependent", got)
	}
	// A retained legacy copy without a device identity fences the whole machine.
	if err := q.update(func() error {
		key := releaseKey(HostRelease{Job: producer.ID, Machine: producer.Machine, Session: producer.Session, Attempt: producer.Attempt})
		held := q.s.HostReleases[key]
		held.Device = ""
		q.s.HostReleases[key] = held
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c.Needs.GPU = nil
	unpinned := submit(t, q, c)
	if got := assignedJob(machineCheck(t, q, m)); got != nil {
		t.Fatal("missing device identity left a device slot free", got)
	}
	if err := q.Cancel(unpinned.ID); err != nil {
		t.Fatal(err)
	}
	r.ReleaseOnly, r.ReleasePending = true, false
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != pinned.ID || len(got.AllocatedDevices) != 1 || got.AllocatedDevices[0] != "0" {
		t.Fatal("exact release did not free the producer's slot", got)
	}
}
