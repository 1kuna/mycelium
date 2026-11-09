package myce

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestDeviceFaultRetryCapacity(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.Devices = append(m.Devices, MachineDevice{ID: "2", Platform: "fake", Identity: "fake-2", Selector: "2", BudgetGB: 8, TotalMemoryGB: 8, FreeMemoryGB: 8, MemoryMode: "fixed"})
	m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "device_fault_xe", 0
	machineCheck(t, q, m)
	s := spec("production")
	j := Job{Spec: s, AttemptHistory: []AttemptOutcome{{DeviceFault: &DeviceFaultLoss{Host: hostID(&m), Platform: "fake", Devices: []string{"fake-0"}, At: clock.Now()}}}}
	target := allocationMachine(&m, []string{"1"})
	if q.deviceFaultRetryBlocked(&j, target) {
		t.Fatal("two healthy devices must permit retry")
	}
	if !q.deviceFaultRetryBlocked(&j, allocationMachine(&m, []string{"1", "2"})) {
		t.Fatal("group retry would consume all surviving devices")
	}
	m.Devices = slices.Clone(m.Devices[:2])
	machineCheck(t, q, m)
	if !q.deviceFaultRetryBlocked(&j, allocationMachine(&m, []string{"1"})) {
		t.Fatal("last healthy device not protected")
	}
	// A spare may disappear between offer and launch. Recheck at Started;
	// an already-started attempt keeps its existing custody.
	j.Machine, j.Session, j.AllocatedDevices = m.ID, m.Session, []string{"1"}
	if got := q.startPlacementRejection(&j); got != deviceFaultRetryHold {
		t.Fatal("launch fence missed last healthy device", got)
	}
	now := clock.Now()
	j.StartedAt = &now
	if got := q.startPlacementRejection(&j); got == deviceFaultRetryHold {
		t.Fatal("capacity change fenced an already-started attempt")
	}
	j.StartedAt = nil
	clock.Advance(time.Second)
	m.Devices[1].Reason = "device_fault_xe"
	machineCheck(t, q, m)
	// Worker wall time is unrelated to queue time; receipt orders this proof.
	m.Devices[1].Reason = ""
	m.Devices[1].Recovery = &DeviceRecovery{FaultEvent: strings.Repeat("a", 64), HealthySince: clock.Now().Add(-25 * time.Hour), ClearedAt: clock.Now().Add(-24 * time.Hour), ObservationSeconds: 1, Probe: "vendor health"}
	machineCheck(t, q, m)
	if q.deviceFaultRetryBlocked(&j, allocationMachine(&m, []string{"1"})) {
		t.Fatal("health-proven sole device must permit retry")
	}
	clock.Advance(time.Second)
	j.AttemptHistory[0].DeviceFault.At = clock.Now()
	clock.Advance(time.Second)
	machineCheck(t, q, m)
	if !q.deviceFaultRetryBlocked(&j, allocationMachine(&m, []string{"1"})) {
		t.Fatal("replayed recovery proof renewed past a newer loss")
	}
	other := m3Machine()
	other.ID, other.HostID = "other", "other"
	machineCheck(t, q, other)
	if q.deviceFaultRetryBlocked(&j, allocationMachine(&other, []string{"0"})) {
		t.Fatal("fault history leaked to another host")
	}
}
