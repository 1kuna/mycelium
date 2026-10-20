package myce

import "testing"

func TestFixedAcceleratorMemorySizesUnprobedDevice(t *testing.T) {
	m := Machine{ID: "w", GPU: "xpu", FreeMemoryGB: 31, TotalMemoryGB: 31}
	unsized := func() CapabilityProfile {
		return CapabilityProfile{MemoryLayout: "separate", SystemMemoryBytes: 32 << 30, Accelerators: []AcceleratorProfile{{Type: "xpu"}}, Unavailable: []string{"accelerator facts"}}
	}
	p := unsized()
	m.CapabilityProfile = &p
	if !availabilityBlocked(&m) {
		t.Fatal("unsized accelerator must block without an operator budget")
	}
	applyFixedAcceleratorMemory(&p, m, false)
	if p.Accelerators[0].MemoryBytes != 0 {
		t.Fatal("non-fixed mode must not invent capacity")
	}
	applyFixedAcceleratorMemory(&p, m, true)
	if got := p.Accelerators[0].MemoryBytes; got != 31<<30 {
		t.Fatalf("fixed budget = %d", got)
	}
	if availabilityBlocked(&m) {
		t.Fatalf("fixed operator budget must make capacity known: %s", capabilityUnavailableReason(&m))
	}
	measured := CapabilityProfile{MemoryLayout: "separate", SystemMemoryBytes: 32 << 30, Accelerators: []AcceleratorProfile{{Type: "xpu", MemoryBytes: 24 << 30}}}
	applyFixedAcceleratorMemory(&measured, m, true)
	if measured.Accelerators[0].MemoryBytes != 24<<30 {
		t.Fatal("measured facts must win over the operator budget")
	}
	j := &Job{Spec: Spec{Project: "p", Priority: "production", AllowedMachines: []string{"w"}}}
	j.Spec.Needs.MemoryGB = 1
	j.Spec.Needs.Platform = Platform{Required: []string{"xpu"}}
	if !fitsWithoutHostMemory(j, &m, m.TotalMemoryGB) {
		t.Fatal("pinned xpu job must structurally fit the fixed-budget worker")
	}
}
