package myce

import "testing"

func TestFixedAcceleratorMemorySizesUnprobedDevice(t *testing.T) {
	m := Machine{ID: "w", GPU: "xpu", FreeMemoryGB: 31, TotalMemoryGB: 31}
	unsized := func() CapabilityProfile {
		// ProfileProbe records logical CPUs even when accelerator probing fails.
		return CapabilityProfile{LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 32 << 30, Accelerators: []AcceleratorProfile{{Type: "xpu"}}, Unavailable: []string{"accelerator facts"}}
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

func TestFixedAcceleratorMemoryExtensionPlacement(t *testing.T) {
	for _, mode := range []string{"qualified", "check", "prepare"} {
		t.Run(mode, func(t *testing.T) {
			q, _ := fixture(t)
			e := compositionExtension(t, false)
			e.Variants[0].Target.Platform = "xpu"
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			m := placementMachine("fixed-xpu", "separate", 64, 31, e)
			m.GPU = "xpu"
			m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "xpu"}}
			m.ExtensionReports = nil
			m.EnvironmentBuilder = true
			if mode != "prepare" {
				r := newExtensionReport(e, &m, "kind:sample")
				r.Setup, r.Qualified, r.CheckID = true, mode == "qualified", "check"
				m.ExtensionReports = []ExtensionReport{r}
			}
			s := Spec{Name: "fixed-xpu", Project: "test", Extension: extensionKey(e), Kind: "sample", AllowedMachines: []string{m.ID}}
			if mode == "check" {
				s.Kind = ""
				s.ExtensionDigest = e.Digest
				s.ExtensionCheck = &ExtensionCheckRequest{Work: "kind:sample", Directory: t.TempDir()}
			}
			j := submit(t, q, s)
			if reply := check(t, q, m, nil); reply.Job != nil || len(reply.EnvironmentPlan) != 0 {
				t.Fatal("unsized accelerator admitted work")
			}
			applyFixedAcceleratorMemory(m.CapabilityProfile, m, true)
			reply := check(t, q, m, nil)
			if mode == "prepare" {
				if len(reply.EnvironmentPlan) != 1 {
					t.Fatalf("fixed-budget preparation blocked: %+v", reply)
				}
			} else if reply.Job == nil || reply.Job.ID != j.ID {
				t.Fatalf("fixed-budget %s blocked: %s", mode, job(t, q, j.ID).Reason)
			}
		})
	}
}

func TestFixedAcceleratorMemoryPreservesOtherModes(t *testing.T) {
	for _, tc := range []struct {
		name, gpu, layout string
		unified           bool
	}{
		{"fake", "fake", "separate", false}, {"unified profile", "xpu", "unified", false}, {"unified worker", "xpu", "separate", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := CapabilityProfile{MemoryLayout: tc.layout, Accelerators: []AcceleratorProfile{{Type: tc.gpu}}}
			applyFixedAcceleratorMemory(&p, Machine{GPU: tc.gpu, UnifiedMemory: tc.unified, FreeMemoryGB: 31}, true)
			if p.Accelerators[0].MemoryBytes != 0 {
				t.Fatal("changed fake or unified capacity")
			}
		})
	}
}
