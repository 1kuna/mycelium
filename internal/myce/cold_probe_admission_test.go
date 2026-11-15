package myce

import (
	"testing"
	"time"
)

func coldProbeAdmissionFixture(t *testing.T, cpu bool) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "fake", 16, 19.49)
	m.HostMemoryBudgetGB = 24
	m.CapabilityProfile.SystemMemoryBytes = 32 << 30
	m.Devices = []MachineDevice{}
	m.MemoryEnforcement = cgroupMemoryEnforcement
	s := cpuSpec(1, 0)
	if !cpu {
		e := compositionExtension(t, false)
		e.Kinds = nil
		v := &e.Variants[0]
		v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "fake"}
		v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
		delete(v.Baselines, "kind:sample")
		v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
		e = freezeComposition(t, e)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		m.ExtensionFingerprint = "synthetic-cold"
		m.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "card", Fingerprint: "card-v1", Selector: "0", ExtensionFingerprint: m.ExtensionFingerprint, MemoryMode: "dedicated", MemoryMeasured: true, TotalMemoryGB: 32, BudgetGB: 32, FreeMemoryGB: 32}}
		target := m.Devices[0].machine(m)
		r := newExtensionReport(e, &target, "extension:sample")
		r.Setup, r.Qualified, r.CheckID = true, true, "synthetic-check"
		m.Devices[0].ExtensionReports = []ExtensionReport{r}
		s = Spec{Name: "cold-check", Project: "synthetic", Priority: "experiment", Extension: extensionKey(e), ExtensionDigest: e.Digest, RequireTarget: true, AllowedMachines: []string{m.ID}, Targets: []ExecutionTarget{v.Target}, Needs: ResourceNeeds{Memory: "auto"}, ExtensionCheck: &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}}
	}
	machineCheck(t, q, m)
	return q, clock, m, s
}

// Used by admission timing tests that need distinct reads of the fake clock.
type coldProbeAdvancingClock struct{ *fakeClock }

func (c coldProbeAdvancingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Nanosecond)
	return c.now
}
