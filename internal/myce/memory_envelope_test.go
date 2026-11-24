package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// #519: an accepted Linux cgroup peak times 2.5 used to strand Darwin
// despite its only local peak being constrained (and therefore no upper bound).

func unifiedAutoFixture(t *testing.T, memberCap float64) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "cuda"}
	v.Needs = ResourceNeeds{MemoryGB: .0625, HostMemoryGB: .015625}
	v.Execution.Main.MemoryGB = memberCap
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("unified", "shared-host", "cuda", 8, 101)
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 64, .125
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.CapabilityProfile.Arch, m.CapabilityProfile.MemoryLayout = "arm64", "unified"
	m.CapabilityProfile.SystemMemoryBytes = 128 << 30
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 128}
	d := MachineDevice{ID: "0", Platform: "cuda", Identity: "synthetic-gb10", Selector: "0", MemoryMode: "unified", ReserveMemoryGB: 16, BudgetGB: 101, FreeMemoryGB: 101, UnifiedMemory: true, MemoryMeasured: true, ExtensionFingerprint: "unified-v1"}
	target := d.machine(m)
	report := newExtensionReport(e, &target, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "check"
	d.ExtensionReports = []ExtensionReport{report}
	m.Devices = []MachineDevice{d}
	machineCheck(t, q, m)
	s := Spec{Name: "memory", Project: "synthetic", RequireTarget: true, Extension: extensionKey(e), Needs: ResourceNeeds{Memory: "auto"}, Workload: &Workload{InputShape: "small-v1", Units: 10}}
	return q, clock, m, s
}

type envelopePeakFS struct {
	fakeMemoryFS
	oom bool
}

func (f envelopePeakFS) Mkdir(p string) error {
	if err := f.fakeMemoryFS.Mkdir(p); err != nil {
		return err
	}
	if strings.HasPrefix(filepath.Base(p), "attempt-") {
		if err := os.WriteFile(filepath.Join(p, "memory.peak"), []byte("2147483648"), 0600); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(p, "memory.stat"), []byte("anon 2147483648\nkernel 0\nshmem 0\n"), 0600); err != nil {
			return err
		}
		if f.oom {
			return os.WriteFile(filepath.Join(p, "memory.events"), []byte("oom 1\noom_kill 1\n"), 0600)
		}
	}
	return nil
}

func TestMemoryEnvelopeNativeGroupPeak(t *testing.T) {
	_, clock := fixture(t)
	w := recoveryWorker(t)
	w.Clock = clock
	w.memoryCeilings = fakeCeilings(t)
	w.memoryCeilings.fs = envelopePeakFS{}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"true"})
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 4
	j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
	j.Spec.Needs.MemoryGB = 0
	j.Spec.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sleep", "60"}}}
	var p *process
	await(t, func() bool {
		p, err = w.start(store, j)
		return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
	})
	if err != nil {
		t.Fatal(err)
	}
	r := d5Finish(t, p)
	data, _ := json.Marshal(r)
	if !r.Success || r.ReleasePending || !strings.Contains(string(data), `"host_gb":2`) {
		t.Fatalf("whole group peak missing: %s", data)
	}
}

// A previously accepted explicit-cap peak must stop seeding auto once a
// comparable explicit attempt fails at its ceiling.

func envelopeCeilingPeak(t *testing.T, capGB float64) *AttemptMemoryPeak {
	t.Helper()
	a, err := fakeCeilings(t).create("attempt-bound", int64(capGB*(1<<30)), 0)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"memory.stat":   "anon " + strconv.FormatInt(int64(capGB*(1<<30)), 10) + "\nkernel 0\nshmem 0\n",
		"memory.events": "max 9907\nhigh 0\noom 0\noom_kill 0\n",
	} {
		if err := os.WriteFile(filepath.Join(a.path, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	peak := a.peak()
	if peak == nil || !peak.Constrained {
		t.Fatal("resident ceiling pressure was not classified")
	}
	return peak
}

// A qualified floor-free variant with an accepted peak on the fake clock.
func floorFreeFitFixture(t *testing.T, allowance float64) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
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
	m := cpuMachine("sized", "sized-host", "fake", 8, 64)
	m.TotalMemoryGB = 8
	m.ExtensionFingerprint = "synthetic-fit-v1"
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.HostMemoryBudgetGB = allowance
	if allowance > 0 {
		m.MemoryEnforcement = ""
	}
	m.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "synthetic-card", Fingerprint: "synthetic-card-v1", ExtensionFingerprint: m.ExtensionFingerprint, Selector: "0", MemoryMode: "fixed", MemoryMeasured: true, TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
	target := m.Devices[0].machine(m)
	r := newExtensionReport(e, &target, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "synthetic-check"
	m.Devices[0].ExtensionReports = []ExtensionReport{r}
	machineCheck(t, q, m)
	s := Spec{Name: "floor-free", Project: "synthetic", Priority: "experiment", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	probe := &Job{State: "queued", Spec: s, Extensions: []Extension{e}, Machine: m.ID, Session: m.Session, AllocatedDevices: []string{"0"}}
	q.mu.Lock()
	defer q.mu.Unlock()
	target = m.Devices[0].machine(*q.s.Machines[m.ID])
	candidates := extensionJobs(probe, &target, true)
	if len(candidates) != 1 {
		t.Fatal("synthetic variant must qualify")
	}
	probe = candidates[0]
	key, why := q.workObservationKey(probe)
	if key == nil {
		t.Fatal(why)
	}
	host, device := 2.0, 3.0
	q.s.WorkObservations = map[string]WorkObservation{identityHash(key): {
		Key: *key, Source: "accepted work", Samples: 1, RecordedAt: clock.Now(), MemoryRecordedAt: clock.Now(), MemoryLayout: "separate",
		MemoryConfiguration: memoryConfiguration(probe.Spec.Needs, probe.Spec.Args, probe.Spec.Program), MemoryPeak: &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device},
	}}
	return q, clock, m, s
}

// #303: the enforced parent charges file-backed memory too. Resident-only
// headroom reserved less than an already measured total, then accepted the
// ceiling-bound successor because its resident peak stayed near 80% of the cap.

// Real-shaped resident/total pairs replayed on CPU work. GPU work separately
// retains total-peak sizing until driver host-memory accounting is qualified.

// A host that cannot satisfy the job's target or backend must not appear as a
// RAM-envelope candidate. Observed: an xpu-only job listed a macOS host.
func TestMemoryEnvelopeReasonSkipsIneligibleHosts(t *testing.T) {
	q, _ := fixture(t)
	xpu := cpuMachine("xpu-host", "xpu-host", "xpu", 8, 0) // No physical headroom.
	mac := cpuMachine("mac-host", "mac-host", "metal", 8, 15)
	mac.CapabilityProfile.OS, mac.CapabilityProfile.Arch = "darwin", "arm64"
	s := cpuSpec(2, 0)
	s.Needs.Memory = "auto"
	s.Needs.Platform = Platform{Required: []string{"xpu"}}
	s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "xpu"}}
	j := submit(t, q, s)
	if check(t, q, mac, nil).Job != nil || check(t, q, xpu, nil).Job != nil {
		t.Fatal("exploration admitted without headroom or on an ineligible host")
	}
	reason := job(t, q, j.ID).Reason
	if !strings.Contains(reason, "xpu-host") || strings.Contains(reason, "mac-host") {
		t.Fatalf("reason must name only eligible hosts: %s", reason)
	}
	mac.HostAvailableMemoryGB = 64
	if check(t, q, mac, nil).Job != nil {
		t.Fatal("xpu job placed on a host without xpu")
	}
}
