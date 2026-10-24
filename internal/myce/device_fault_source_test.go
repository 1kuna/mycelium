package myce

import "testing"

func TestDeviceFaultSourceKernelInput(t *testing.T) {
	input := []byte(`{"__CURSOR":"a","__MONOTONIC_TIMESTAMP":"123","MESSAGE":"xe 0000:03:00.0: Engine memory CAT error. Fault response: Unsuccessful -EINVAL"}
{"__CURSOR":"b","__MONOTONIC_TIMESTAMP":"124","MESSAGE":"i915 0000:07:00.0: [drm] GPU HANG: ecode 12:0:85dffffb"}
{"__CURSOR":"c","__MONOTONIC_TIMESTAMP":"125","MESSAGE":"NVRM: Xid (PCI:0000:09:00): 79, GPU has fallen off the bus."}
`)
	lines, cursor, err := parseKernelJournal(input)
	if err != nil || cursor != "c" || len(lines) != 3 {
		t.Fatalf("kernel input: lines=%d cursor=%q err=%v", len(lines), cursor, err)
	}
	for i, tc := range []struct {
		pci, reason string
		match       DeviceFaultMatcher
	}{
		{"0000:03:00.0", "device_fault_xe", intelKernelFault},
		{"0000:07:00.0", "device_fault_i915", intelKernelFault},
		{"00000000:09:00.0", "device_fault_nvidia_xid", cudaKernelFault},
	} {
		if got := tc.match(MachineDevice{pciAddress: tc.pci}, lines[i].Message); got != tc.reason {
			t.Fatalf("family %d: %q", i, got)
		}
		if got := tc.match(MachineDevice{pciAddress: "0000:ff:00.0"}, lines[i].Message); got != "" {
			t.Fatalf("fault crossed PCI devices: %q", got)
		}
	}
}
