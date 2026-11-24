package myce

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

func placementMachine(id, layout string, ram, accelerator float64, e Extension) Machine {
	m := machine(id, "fake")
	m.TotalMemoryGB, m.FreeMemoryGB = accelerator, accelerator
	m.HostAvailableMemoryGB, m.HostMemoryMeasured = ram, true
	m.UnifiedMemory = layout == "unified"
	m.CapabilityProfile = &CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH, MemoryLayout: layout, SystemMemoryBytes: uint64(ram * (1 << 30)), LogicalCPUs: 8, Accelerators: []AcceleratorProfile{{Type: "fake", MemoryBytes: uint64(accelerator * (1 << 30))}}}
	m.ExtensionReports = []ExtensionReport{{Digest: e.Digest, Work: "kind:sample", Platform: "fake", Qualified: true}, {Digest: e.Digest, Work: "program:sample-script", Platform: "fake", Qualified: true}}
	if len(e.Variants) > 0 {
		m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
		m.ExtensionFingerprint = "test-fingerprint"
		for i := range m.ExtensionReports {
			old := m.ExtensionReports[i]
			r := newExtensionReport(e, &m, old.Work)
			r.Setup = true
			r.Qualified = true
			r.CheckID = "test-check"
			m.ExtensionReports[i] = r
		}
	}
	return m
}

func placementSpec(program bool) Spec {
	s := Spec{Name: "placement", Project: "test", Priority: "experiment", Kind: "sample"}
	if program {
		s.Kind = ""
		s.Program = &Program{Type: "sample-script", File: "script"}
		s.Inputs = map[string]string{"script": "immutable/script-v1"}
	}
	return s
}

func TestCapabilityMemoryLayoutAndHints(t *testing.T) {
	for _, tc := range []struct {
		name, layout                               string
		ram, accelerator, needRAM, needAccelerator float64
		fits                                       bool
	}{
		{"ram-heavy-separate", "separate", 64, 24, 48, 2, true},
		{"accelerator-heavy-separate", "separate", 64, 24, 2, 40, false},
		{"accelerator-heavy-unified", "unified", 128, 128, 2, 40, true},
		{"shared-pool-counted-once", "unified", 64, 64, 40, 40, false},
		{"profile-caps-overreported-free", "separate", 64, 24, 2, 40, false},
		{"unknown-system-size", "separate", 0, 24, 2, 1, false},
		{"wrong-accelerator-type", "separate", 64, 24, 2, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			e := fakeExtension(t)
			e.Files["extension.yaml"] = []byte(strings.Replace(string(e.Files["extension.yaml"]), "platforms: [fake]", "platforms: [cuda, metal]", 1))
			var err error
			e, err = extensionFromFiles(e.Files)
			if err != nil {
				t.Fatal(err)
			}
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			s := placementSpec(false)
			s.SizeHint = SizeHint{MemoryGB: tc.needAccelerator, HostMemoryGB: tc.needRAM}
			submit(t, q, s)
			m := placementMachine("hypha", tc.layout, tc.ram, tc.accelerator, e)
			m.GPU = "cuda"
			if tc.layout == "unified" {
				m.GPU = "metal"
			}
			m.CapabilityProfile.Accelerators[0].Type = m.GPU
			for i := range m.ExtensionReports {
				m.ExtensionReports[i].Platform = m.GPU
			}
			if tc.name == "wrong-accelerator-type" {
				m.CapabilityProfile.Accelerators[0].Type = "xpu"
			}
			if tc.name == "profile-caps-overreported-free" {
				m.TotalMemoryGB, m.FreeMemoryGB = 128, 128
			}
			clock.Advance(time.Second)
			if got := check(t, q, m, nil).Job != nil; got != tc.fits {
				t.Fatalf("placed=%v want=%v", got, tc.fits)
			}
		})
	}
}

func TestCapabilityOOMDoesNotOverrideCancellation(t *testing.T) {
	q, clock := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("hypha", "separate", 64, 24, e)
	check(t, q, m, nil)
	j := submit(t, q, placementSpec(true))
	run := check(t, q, m, nil).Job
	clock.Advance(time.Second)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	r := result(run, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "cancelled" || got.OOMRetries != 0 {
		t.Fatal("OOM bypassed cancellation")
	}
}
