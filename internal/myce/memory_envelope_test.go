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
	"time"
)

func TestMemoryEnvelopeMeasuredAccepted(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(2 * time.Second)
	r := result(&first, true)
	if err := json.Unmarshal([]byte(`{"memory_peak":{"host_gb":2}}`), &r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// Omitted mode preserves explicit production/bootstrap declarations.
	auto := s
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &auto.Needs); err != nil {
		t.Fatal(err)
	}
	next := startObservation(t, q, m, auto)
	if next.Spec.Needs.HostMemoryGB != 2.5 {
		t.Fatalf("peak × 1.25 not reserved: %+v", next.Spec.Needs)
	}
}

func TestMemoryEnvelopeCPUOnGPUHostReusesAndRelearnsPeak(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	m.CPUExtensionFingerprint = m.ExtensionFingerprint
	m.GPU, m.ExtensionFingerprint = "fake", "synthetic-gpu-v1"
	m.Devices = m3Machine().Devices
	for i := range m.Devices {
		m.Devices[i].ReserveMemoryGB, m.Devices[i].BudgetGB = 1, 7
	}
	machineCheck(t, q, m)
	s.Needs.Memory = "auto"
	start := func(source string, hostGB float64) *Job {
		t.Helper()
		queued := submit(t, q, s)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("CPU job not assigned: %+v", job(t, q, queued.ID))
		}
		if len(j.AllocatedDevices) != 0 || j.MemoryEnvelope == nil || j.MemoryEnvelope.Source != source || hostGB > 0 && j.Spec.Needs.HostMemoryGB != hostGB {
			t.Fatalf("wrong CPU admission: devices=%v envelope=%+v", j.AllocatedDevices, j.MemoryEnvelope)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		return j
	}
	complete := func(j *Job, peak float64) {
		t.Helper()
		clock.Advance(time.Second)
		r := result(j, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, point := range q.s.WorkObservations {
			if point.Job == j.ID {
				found = true
				if point.StaleReason != "" || point.MemoryPeak == nil || point.MemoryPeak.HostGB == nil || *point.MemoryPeak.HostGB != peak || !point.MemoryRecordedAt.Equal(clock.Now()) {
					t.Fatalf("accepted CPU peak is not current: %+v", point)
				}
			}
		}
		if !found {
			t.Fatal("successful solo CPU run did not record a peak")
		}
	}
	first := start("exploratory first run", 0)
	if owner := q.s.Machines[m.ID]; len(owner.Devices) != 2 || owner.ReservedMemoryGB != 1 {
		t.Fatalf("missing multi-device GPU reserve: %+v", owner)
	}
	complete(first, 2)
	next := start("accepted peak × 1.25", 2.5)
	clock.Advance(time.Second)
	r := result(next, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	for _, point := range q.s.WorkObservations {
		if point.MemoryPeak != nil || !point.MemoryRecordedAt.IsZero() {
			t.Fatalf("OOM retained a memory peak: %+v", point)
		}
	}
	if err := q.Cancel(next.ID); err != nil {
		t.Fatal(err)
	}
	complete(start("exploratory first run", 0), 8)
	start("accepted peak × 1.25", 10)
}

func TestMemoryEnvelopeProfileIdentityAndLifecycle(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	m.CPUExtensionFingerprint = "profile-cpu-v1"
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 16, 0.1
	for i := range m.Devices {
		m.Devices[i].ReserveMemoryGB, m.Devices[i].BudgetGB = 1, 7
	}
	machineCheck(t, q, m)
	s := cpuSpec(1, 0)
	s.Name, s.Project, s.RequireTarget, s.Needs.Memory = "gate-full", "synthetic", true, "auto"
	s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}
	s.Run.Command = []string{"sh", "-ec", "tar -xf .myce-source.tar\nbash scripts/gate.sh full\ntest -f result"}
	setSource := func(data string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "source")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(q.files.blob(ref), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		s.Files = FileManifest{".myce-source.tar": ref}
	}
	start := func(source string, hostGB float64) *Job {
		t.Helper()
		queued := submit(t, q, s)
		if queued.State != "queued" {
			t.Fatal("profile did not queue", queued.State)
		}
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("profile not assigned: %+v", job(t, q, queued.ID))
		}
		if j.MemoryEnvelope.Source != source || hostGB > 0 && j.Spec.Needs.HostMemoryGB != hostGB {
			t.Fatalf("wrong profile estimate: %+v", j.MemoryEnvelope)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		return j
	}
	complete := func(j *Job, peak float64) {
		t.Helper()
		clock.Advance(time.Second)
		r := result(j, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if timing := q.s.Jobs[j.ID].Timing; timing != nil && (timing.PredictedSeconds != nil || timing.ActualSeconds != nil) {
			t.Fatalf("plain profile learned timing: %+v", timing)
		}
	}
	setSource("commit one")
	first := start("exploratory first run", 0)
	busy := submit(t, q, s)
	if busy.State != "queued" || assignedJob(machineCheck(t, q, m, first)) != nil {
		t.Fatal("busy auto profile must queue without dispatching", busy)
	}
	if err := q.Cancel(busy.ID); err != nil {
		t.Fatal(err)
	}
	complete(first, 2)
	complete(start("accepted peak × 1.25", 2.5), 2)
	setSource("commit two")
	oom := start("accepted peak × 1.25", 2.5)
	r := result(oom, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(oom.ID); err != nil {
		t.Fatal(err)
	}
	complete(start("exploratory first run", 0), 4)
	complete(start("accepted peak × 1.25", 5), 4)
	s.Run.Command = []string{"sh", "-ec", "tar -xf .myce-source.tar\nbash scripts/other-suite.sh"}
	complete(start("exploratory first run", 0), 1)
	for _, point := range q.s.WorkObservations {
		if point.StaleReason != "" || point.Rate != 0 || point.Seconds != 0 || point.Units != 0 || point.LatencySeconds != 0 {
			t.Fatalf("profile memory observation contains stale/timing data: %+v", point)
		}
	}
	s.Needs.Memory, s.Needs.HostMemoryGB = "", 3
	complete(start("declared", 3), 1)
	// A first run can fit while its learned peak plus safety margin cannot.
	s.Needs.Memory, s.Needs.HostMemoryGB = "auto", 0
	s.Run.Command = []string{"sh", "-ec", "bash scripts/memory-heavy-suite.sh"}
	complete(start("exploratory first run", 0), 13)
	for _, mode := range []string{"auto", ""} {
		s.Needs.Memory = mode
		s.Needs.HostMemoryGB = 17
		if mode == "auto" {
			s.Needs.HostMemoryGB = 0 // The rejection must come from the learned peak.
		}
		_, err := q.Submit(s)
		var api *HTTPError
		if !errors.As(err, &api) || api.Code != "no_qualified_target" {
			t.Fatalf("oversized %q memory must retain exit-78 rejection: %v", mode, err)
		}
	}
}

func TestMemoryEnvelopeCrossMachineEstimate(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&first, true)
	if err := json.Unmarshal([]byte(`{"memory_peak":{"host_gb":2}}`), &r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	m.ID, m.HostID = "other-machine", "other-host"
	m.Session = "session-other-machine"
	e := first.Extensions[0]
	report := newExtensionReport(e, &m, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "other-check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	s.AllowedMachines = []string{m.ID}
	// No bootstrap declaration; recipe defaults are not submitter hints.
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &s.Needs); err != nil {
		t.Fatal(err)
	}
	queued := submit(t, q, s)
	next := check(t, q, m, nil).Job
	if next == nil {
		t.Fatalf("cross-machine candidate refused: %s", job(t, q, queued.ID).Reason)
	}
	data, _ := json.Marshal(next)
	if next.Spec.Needs.HostMemoryGB != 3.75 || !strings.Contains(string(data), "cross-machine estimate") {
		t.Fatalf("same-family peak must use 1.25 × 1.5: %s", data)
	}
	// The same accepted recipe on a different family gets the larger margin.
	for id, point := range q.s.WorkObservations {
		point.Key.Target.Arch = "arm64"
		q.s.WorkObservations[id] = point
	}
	probe := *q.s.Jobs[next.ID]
	probe.State = "queued"
	estimate := q.memoryCandidate(&probe, q.s.Machines[m.ID])
	if estimate.Spec.Needs.HostMemoryGB != 5 {
		t.Fatalf("other-family peak must use 1.25 × 2: %+v", estimate.Spec.Needs)
	}
}

func TestMemoryEnvelopeExploratoryFirstRun(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	plain := cpuSpec(1, 0)
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &plain.Needs); err != nil {
		t.Fatal(err)
	}
	if err := plain.Validate(); err != nil {
		t.Fatal("auto must not require a bootstrap declaration:", err)
	}
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &s.Needs); err != nil {
		t.Fatal(err)
	}
	m.HostAvailableMemoryGB = 15 // Less than half the physical 64 GiB allowance.
	j := submit(t, q, s)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("exploration admitted without large free headroom")
	}
	if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "exploratory") {
		t.Fatalf("missing exploratory refusal: %s", reason)
	}
	// Old release custody remains protected even with half the host free.
	m.HostAvailableMemoryGB = 48
	q.s.HostReleases = map[string]HostRelease{"memory-heavy": {Host: m.HostID, Job: "old-attempt", HostChargeGB: 16}}
	if check(t, q, m, nil).Job != nil || !strings.Contains(job(t, q, j.ID).Reason, "unconfirmed memory-heavy release") {
		t.Fatal("exploration ignored unconfirmed memory-heavy custody")
	}
	delete(q.s.HostReleases, "memory-heavy")
	m.HostAvailableMemoryGB = 32 // This telemetry already excludes the host reserve.
	first := check(t, q, m, nil).Job
	data, _ := json.Marshal(first)
	if first == nil || first.Spec.Needs.HostMemoryGB != 31.875 || !strings.Contains(string(data), "exploratory first run") {
		t.Fatalf("no-hint first run must leave launch drift headroom: %+v; %s", first, job(t, q, j.ID).Reason)
	}
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(first, true)
	if err := json.Unmarshal([]byte(`{"memory_peak":{"host_gb":2}}`), &r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	next := startObservation(t, q, m, s)
	data, _ = json.Marshal(next)
	if next.Spec.Needs.HostMemoryGB != 2.5 || !strings.Contains(string(data), "accepted peak × 1.25") {
		t.Fatalf("exploration did not become a measurement: %s", data)
	}
}

func TestMemoryEnvelopeExploratoryLaunchWithRAMDrift(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("drift", "drift-host", "cpu", 8, 32)
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 64, .125
	check(t, q, m, nil)
	s := cpuSpec(1, 0)
	s.Needs.Memory = "auto"
	s.Run.Command = []string{"true"}
	queued := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != queued.ID || j.MemoryEnvelope.Source != "exploratory first run" {
		t.Fatal("exploratory job did not dispatch", j)
	}
	w := recoveryWorker(t)
	w.Machine, w.Clock = m, clock
	w.memoryCeilings = fakeCeilings(t)
	free := m.HostAvailableMemoryGB - .0625
	w.HostMemorySource = func(context.Context) (float64, error) { return free, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	p, err := w.start(store, *j)
	if err != nil {
		t.Fatalf("small RAM drift refused exploratory launch: %v", err)
	}
	if f := <-p.done; f.err != nil || !f.result.Success {
		t.Fatalf("exploratory payload failed: %+v", f)
	}
	// Keep the check strict, and retain the exact values for a sub-MiB gap.
	need := hostNeed(&m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	free = need - 1.0/(1<<20)
	err = w.checkHostRAM(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	if !errors.Is(err, errWorkerMemory) || !strings.Contains(err.Error(), "need "+strconv.FormatFloat(need, 'f', -1, 64)+" GiB, available "+strconv.FormatFloat(free, 'f', -1, 64)+" GiB") {
		t.Fatalf("genuine shortage lacks exact values: %v", err)
	}
}

func TestMemoryEnvelopeUnifiedExploratoryGPUDrift(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, .25)
	// Device room, rather than host headroom, limits the exploratory GPU share.
	m.MemoryCeilingSlackGB = 0
	m.Devices[0].BudgetGB, m.Devices[0].FreeMemoryGB = 59, 59
	queued := submit(t, q, s)
	j := assignedJob(machineCheck(t, q, m))
	if j == nil || j.ID != queued.ID {
		t.Fatalf("unified exploration not assigned: %s", job(t, q, queued.ID).Reason)
	}
	w := recoveryWorker(t)
	w.Machine, w.Clock = *allocationMachine(&m, j.AllocatedDevices), clock
	w.memoryCeilings = fakeCeilings(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 100, nil }
	w.MemorySource = func(context.Context) (float64, error) { return 59 - .0625, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	// Exercise the real strict GPU launch check with a small plain payload.
	payload := *j
	payload.Spec = Spec{Run: RunSpec{Command: []string{"true"}}, Needs: j.Spec.Needs}
	payload.Extensions, payload.Extension = nil, nil
	p, err := w.start(store, payload)
	if err != nil {
		t.Fatalf("small GPU drift refused exploratory launch: %v", err)
	}
	if f := <-p.done; f.err != nil || !f.result.Success {
		t.Fatalf("exploratory payload failed: %+v", f)
	}
}

func TestMemoryEnvelopeRefusalReason(t *testing.T) {
	q, clock := fixture(t)
	m := machine("small", "fake")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 2
	check(t, q, m, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 4
	j := submit(t, q, s)
	clock.Advance(time.Second)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("admitted above live host memory")
	}
	reason := job(t, q, j.ID).Reason
	if !strings.Contains(reason, "available") || !strings.Contains(reason, "4.0") || !strings.Contains(reason, "2.0") {
		t.Fatalf("missing envelope/live capacity diagnostic: %s", reason)
	}
}

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

func TestMemoryEnvelopeUnifiedCappedAutoStarts(t *testing.T) {
	q, _, m, s := unifiedAutoFixture(t, .25)
	queued := submit(t, q, s)
	j := assignedJob(machineCheck(t, q, m))
	if j == nil || j.ID != queued.ID {
		t.Fatalf("capped auto job did not qualify and dispatch: %s", job(t, q, queued.ID).Reason)
	}
	if j.MemoryEnvelope == nil || j.MemoryEnvelope.Refusal != "" || j.MemoryEnvelope.Source != "exploratory first run" || j.Spec.Needs.HostMemoryGB < .25 || j.Spec.Needs.MemoryGB <= 0 || j.MemoryEnvelope.HostGB > 64 {
		t.Fatalf("capped exploration exceeds shared allowance: %+v", j)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "running" || got.StartedAt == nil {
		t.Fatalf("capped auto job did not start: %+v", got)
	}
	// A member floor larger than all shared headroom must still refuse.
	finish(t, q, j, true)
	machineCheck(t, q, m)
	probe := *j
	probe.State = "queued"
	probe.Spec.Run.MemoryGB = 65
	target := allocationMachine(q.s.Machines[m.ID], j.AllocatedDevices)
	if candidate := q.memoryCandidate(&probe, target); candidate.MemoryEnvelope.Refusal == "" {
		t.Fatalf("impossible member floor explored: %+v", candidate.MemoryEnvelope)
	}
}

func TestMemoryEnvelopeUnifiedAutoReusesPeakAcrossLiveDrift(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, 0)
	var allowance string
	for run := 0; run < 2; run++ {
		queued := submit(t, q, s)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("solo CUDA run %d not assigned: %s", run, job(t, q, queued.ID).Reason)
		}
		if run == 1 && (j.MemoryEnvelope.Source != "accepted peak × 1.25" || j.Spec.Needs.HostMemoryGB != .15625 || j.Spec.Needs.MemoryGB != .3125 || j.MemoryEnvelope.HostGB != .59375) {
			t.Fatalf("second CUDA run did not sum learned host + GPU + slack: %+v", j.MemoryEnvelope)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		key := job(t, q, j.ID).Timing.Key
		if run == 0 {
			allowance = key.Allowance
		} else if key.Allowance != allowance {
			t.Fatal("live RAM drift changed the allowance hash")
		}
		// Uncapped unified inventory currently reports live room as BudgetGB.
		m.HostAvailableMemoryGB--
		m.Devices[0].BudgetGB, m.Devices[0].FreeMemoryGB = m.HostAvailableMemoryGB, m.HostAvailableMemoryGB
		machineCheck(t, q, m, j)
		clock.Advance(time.Second)
		host, device := .125, .25
		r := result(j, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if timing := job(t, q, j.ID).Timing; timing.Reason != "" {
			t.Fatalf("successful solo CUDA run lost comparability: %+v", timing)
		}
	}
	// Resolved auto reservations share one timing identity across live drift.
	if len(q.s.WorkObservations) != 1 {
		t.Fatalf("expected both solo CUDA runs to share one peak record, got %d", len(q.s.WorkObservations))
	}
	for _, point := range q.s.WorkObservations {
		if point.Samples != 2 || point.StaleReason != "" || point.MemoryPeak == nil || point.MemoryLayout != "unified" {
			t.Fatalf("CUDA peaks not accepted: %+v", point)
		}
	}
}

func TestMemoryEnvelopeUnifiedHeadroom(t *testing.T) {
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	e.Variants[0].Target.OS = "linux"
	e.Variants[0].Target.Arch = "amd64"
	e.Variants[0].Needs = ResourceNeeds{MemoryGB: 8, HostMemoryGB: 4}
	delete(e.Variants[0].Baselines, "kind:sample")
	e.Variants[0].References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := machine("unified", "fake", "shared-host")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 32, 32
	m.MemoryCeilingSlackGB = .125
	m.CapabilityProfile.MemoryLayout = "unified"
	d := MachineDevice{ID: "0", Platform: "fake", Identity: "shared-device", Selector: "0", MemoryMode: "unified", TotalMemoryGB: 64, BudgetGB: 64, FreeMemoryGB: 64, UnifiedMemory: true, MemoryMeasured: true, ExtensionFingerprint: "unified-v1"}
	target := m
	target.Device = "0"
	target.ExtensionFingerprint = d.ExtensionFingerprint
	report := newExtensionReport(e, &target, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "check"
	d.ExtensionReports = []ExtensionReport{report}
	m.Devices = []MachineDevice{d}
	machineCheck(t, q, m)
	s := Spec{Name: "memory", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	queuedFirst := submit(t, q, s)
	firstReply := machineCheck(t, q, m)
	if assignedJob(firstReply) == nil {
		t.Fatalf("not assigned: %s", job(t, q, queuedFirst.ID).Reason)
	}
	first := *assignedJob(firstReply)
	if err := q.Started(result(&first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(&first, true)
	if err := json.Unmarshal([]byte(`{"memory_peak":{"host_gb":10,"device_gb":8}}`), &r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	auto := s
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &auto.Needs); err != nil {
		t.Fatal(err)
	}
	m.HostAvailableMemoryGB = 22.5
	j := submit(t, q, auto)
	if assignedJob(machineCheck(t, q, m)) != nil {
		t.Fatal("unified peak admitted without existing slack")
	}
	m.HostAvailableMemoryGB = 22.625
	assigned := assignedJob(machineCheck(t, q, m))
	if assigned == nil || assigned.ID != j.ID {
		t.Fatalf("conservative host + GPU reservation did not fit: %s", job(t, q, j.ID).Reason)
	}
	if assigned.Spec.Needs.MemoryGB != 10 || assigned.Spec.Needs.HostMemoryGB != 12.5 {
		t.Fatalf("wrong unified vector: %+v", assigned.Spec.Needs)
	}
	// No auto opt-in: production keeps the existing 8 + 4 + slack declaration.
	finish(t, q, assigned, true)
	for id, point := range q.s.WorkObservations {
		point.MemoryPeak.DeviceGB = nil
		q.s.WorkObservations[id] = point
	}
	if peak, _, _ := q.memoryEstimate(assigned, &m); peak != nil {
		t.Fatal("unified exact estimate used an unknown device peak")
	}
	// On a separate-memory target, a host estimate is still useful, but the
	// retained GPU declaration must not masquerade as a measured GPU peak.
	separateTarget := allocationMachine(q.s.Machines[m.ID], assigned.AllocatedDevices)
	separateTarget.UnifiedMemory = false
	for id, point := range q.s.WorkObservations {
		point.MemoryLayout = "separate"
		q.s.WorkObservations[id] = point
	}
	probe := *assigned
	probe.State = "queued"
	if candidate := q.memoryCandidate(&probe, separateTarget); !strings.Contains(candidate.MemoryEnvelope.Source, "GPU recipe default") {
		t.Fatalf("unknown GPU peak has misleading source: %+v", candidate.MemoryEnvelope)
	}
	s.Priority = "production"
	productionID := submit(t, q, s)
	production := assignedJob(machineCheck(t, q, m))
	if production == nil || production.ID != productionID.ID {
		t.Fatal("explicit production refused")
	}
	if production.Spec.Needs.MemoryGB != 8 || production.Spec.Needs.HostMemoryGB != 4 {
		t.Fatal("production declarations tightened")
	}
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

func TestMemoryEnvelopeOOMIsJobCause(t *testing.T) {
	q, clock := fixture(t)
	w := recoveryWorker(t)
	w.Clock = clock
	w.memoryCeilings = fakeCeilings(t)
	w.memoryCeilings.fs = envelopePeakFS{oom: true}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	m := w.Machine
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	s := spec("production")
	s.Resumable = false
	s.Needs.HostMemoryGB = 4
	s.Run.Command = []string{"true"}
	queued := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("not assigned")
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	store.state.Session = j.Session
	p, err := w.start(store, *j)
	if err != nil {
		t.Fatal(err)
	}
	r := d5Finish(t, p)
	data, _ := json.Marshal(r)
	if !r.OutOfMemory || r.Interrupted || !strings.Contains(string(data), `"host_gb":2`) || !strings.Contains(r.Error, "memory ceiling") {
		t.Fatalf("lost whole-attempt OOM/peak: %s", data)
	}
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, queued.ID)
	if got.State != "failed" || got.InfrastructureRetries != 0 || !strings.Contains(got.Reason, "memory ceiling") {
		t.Fatalf("OOM not reported against job: %+v", got)
	}
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
	p, err := w.start(store, j)
	if err != nil {
		t.Fatal(err)
	}
	r := d5Finish(t, p)
	data, _ := json.Marshal(r)
	if !r.Success || r.ReleasePending || !strings.Contains(string(data), `"host_gb":2`) {
		t.Fatalf("whole group peak missing: %s", data)
	}
}
