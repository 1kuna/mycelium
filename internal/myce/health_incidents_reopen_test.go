package myce

import (
	"context"
	"testing"
	"time"
)

func TestHealthIncidentXPUTeardownWorkerReopen(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.Devices[0].Platform = "xpu"
	root := &workerStore{dir: t.TempDir()}
	key := deviceFaultKey(m.Devices[0])
	if err := saveWorkerJSON(root.dir, deviceFaultFile, deviceQuarantine{BootID: "boot", Devices: map[string]deviceFault{
		key: {Reason: "device_fault_xe", Event: "fault", RepeatedTeardown: true},
	}}); err != nil {
		t.Fatal(err)
	}
	boot := "boot"
	source := func(context.Context, time.Time) (KernelLogSnapshot, error) {
		return KernelLogSnapshot{BootID: boot}, nil
	}
	for _, newBoot := range []bool{false, true} {
		if newBoot {
			boot = "new-boot"
		}
		w := Worker{Machine: m, Clock: clock, DeviceFaultSource: source}
		if err := w.initDeviceFaults(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		machineCheck(t, q, w.Machine)
		requireIncident(t, q, "xpu_repeated_teardown_faults", "teardown-faults/machine/"+key, 1, !newBoot)
	}
}
