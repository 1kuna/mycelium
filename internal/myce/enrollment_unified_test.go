package myce

import (
	"context"
	"os/exec"
	"testing"
)

func TestEnrollmentReadinessUnifiedMemory(t *testing.T) {
	for _, platform := range []string{"metal", "cuda"} {
		t.Run(platform, func(t *testing.T) {
			// Auto-detected Metal and CUDA N/A inventory has no dedicated
			// total. Its measured allowance comes from the host's shared RAM.
			m := readyEnrollmentMachine()
			m.GPU = platform
			m.UnifiedMemory = true
			m.Devices = []MachineDevice{{ID: "0", Identity: "unified-device", Platform: platform, Selector: "0", MemoryMode: "unified", UnifiedMemory: true, MemoryMeasured: true, ReserveMemoryGB: 4, BudgetGB: 12, FreeMemoryGB: 12}}
			if platform == "cuda" {
				m.CapabilityProfile.OS = "linux"
				m.Devices[0].Identity = "GPU-unified"
				m.Devices[0].Selector = "GPU-unified"
				m.Devices[0].ReserveMemoryGB = 16
			}
			probe := MachineProbe{OS: m.CapabilityProfile.OS, Arch: "arm64", Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
				switch name {
				case "/usr/sbin/system_profiler":
					return []byte("Metal: Supported"), nil
				case "nvidia-smi":
					return []byte("NVIDIA GB10, N/A"), nil
				default:
					return nil, exec.ErrNotFound
				}
			}}
			detected, err := probe.DetectDevice(context.Background(), m.Devices[0].Selector)
			if err != nil || detected.MemoryGB != 0 || detected.MemoryMode != "unified" {
				t.Fatalf("unexpected automatic %s detection: %+v %v", platform, detected, err)
			}
			budget := 0
			receipt := EnrollmentReceipt{Session: m.Session, Version: BuildVersion, Devices: m.Devices, LogicalCPUs: m.CapabilityProfile.LogicalCPUs, SystemMemoryBytes: m.CapabilityProfile.SystemMemoryBytes}
			observation := EnrollmentObservation{Machine: m, Fresh: true, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol, Policy: HostMaintenance{Mode: "enabled", CPUBudgetCores: &budget, MemoryBudget: &HostMemoryBudget{}}}
			t.Run("healthy", func(t *testing.T) {
				if state, err := EvaluateEnrollment(receipt, observation); err != nil || state != "ready" {
					t.Fatalf("healthy zero-total unified inventory: state=%q err=%v", state, err)
				}
			})
			for _, tc := range []struct {
				name   string
				change func(*EnrollmentObservation)
			}{
				{"unmeasured-device", func(o *EnrollmentObservation) { o.Machine.Devices[0].MemoryMeasured = false }},
				{"zero-allowance", func(o *EnrollmentObservation) { o.Machine.Devices[0].BudgetGB = 0 }},
				{"withheld-device", func(o *EnrollmentObservation) { o.Machine.Devices[0].Reason = "device_memory_unavailable" }},
				{"unknown-discrete-memory", func(o *EnrollmentObservation) {
					o.Machine.Devices[0].UnifiedMemory = false
					o.Machine.Devices[0].MemoryMode = "dedicated"
					o.Machine.UnifiedMemory = false
				}},
				{"unmeasured-host", func(o *EnrollmentObservation) { o.Machine.HostMemoryMeasured = false }},
				{"unknown-host-total", func(o *EnrollmentObservation) { o.Machine.CapabilityProfile.SystemMemoryBytes = 0 }},
				{"host-memory-error", func(o *EnrollmentObservation) { o.Machine.HostMemoryError = "unavailable" }},
				{"zero-host-allowance", func(o *EnrollmentObservation) { o.Machine.HostAvailableMemoryGB = 0 }},
				{"inventory-error", func(o *EnrollmentObservation) { o.Machine.InventoryReason = "device_probe_unavailable" }},
				{"wrong-session", func(o *EnrollmentObservation) { o.Machine.Session = "stale-session" }},
				{"wrong-protocol", func(o *EnrollmentObservation) { o.AttemptProtocol = "old" }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					bad := observation
					bad.Machine.Devices = append([]MachineDevice(nil), m.Devices...)
					profile := *m.CapabilityProfile
					bad.Machine.CapabilityProfile = &profile
					tc.change(&bad)
					if state, err := EvaluateEnrollment(receipt, bad); err == nil || state == "ready" {
						t.Fatalf("broken inventory accepted: state=%q err=%v", state, err)
					}
				})
			}
		})
	}
}
