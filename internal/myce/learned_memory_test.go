package myce

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const physicalMemoryProbe = "exploratory first run"

func setLearnedMemory(t *testing.T, q *Queue, on bool) {
	t.Helper()
	record := q.PriorityPolicy()
	p := record.Policy
	p.LearnedMemory = &on
	if _, err := q.SetPriorityPolicy(record.Revision, p); err != nil {
		t.Fatal(err)
	}
}

// One 32 GiB CPU host; the cold probe owns measured physical headroom.
func learnedMemoryFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock := fixture(t)
	m := cpuMachine("worker", "worker-host", "cpu", 8, 32)
	m.HostMemoryBudgetGB, m.ExtensionFingerprint = 32, "cpu-v1" // Workers report their backend probe.
	check(t, q, m, nil)
	return q, clock, m
}

func runMemory(t *testing.T, q *Queue, clock *fakeClock, m Machine, s Spec, peak float64, mutate func(*Result)) *Job {
	t.Helper()
	queued := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != queued.ID {
		t.Fatalf("not assigned: %s", job(t, q, queued.ID).Reason)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(j, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak}
	if mutate != nil {
		mutate(&r)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	return j
}

func reopenQueue(t *testing.T, q *Queue, clock *fakeClock) *Queue {
	t.Helper()
	var database string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(database, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	reopened.fileRetention, reopened.logRetention = q.fileRetention, q.logRetention
	reopened.rebuildRecords()
	t.Cleanup(func() { reopened.Close() })
	fakeCheckpointAvailability(t, reopened, true)
	return reopened
}

func TestLearnedMemoryOmittedFigureLearnsAcrossReopen(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	s := cpuSpec(1, 0) // No memory figure at all.
	if err := s.Validate(); err != nil {
		t.Fatal("omitted memory must pass static validation:", err)
	}
	first := runMemory(t, q, clock, m, s, 2, nil)
	if e := first.MemoryEnvelope; e.Source != physicalMemoryProbe || e.Basis != "exploratory" || first.Spec.Needs.HostMemoryGB != 32-minimumHostMemoryGB || first.Spec.Needs.Memory != "auto" {
		t.Fatalf("cold run not bounded: %+v needs=%+v", e, first.Spec.Needs)
	}
	second := runMemory(t, q, clock, m, s, 2, nil)
	if e := second.MemoryEnvelope; e.Source != "accepted peak + measured uncertainty" || e.Basis != "learned" || e.Evidence != 1 || second.Spec.Needs.HostMemoryGB != 2+minimumHostMemoryGB {
		t.Fatalf("second run did not learn: %+v", e)
	}

	q = reopenQueue(t, q, clock)
	check(t, q, m, nil)
	busy := cpuSpec(1, 30) // Explicit figure: kept exactly, holds the host.
	busy.Name = "busy"
	held := submit(t, q, busy)
	if j := check(t, q, m, nil).Job; j == nil || j.ID != held.ID {
		t.Fatal("declared job not admitted")
	} else if e := j.MemoryEnvelope; e.Basis != "declared" || e.Evidence != 0 || j.Spec.Needs.HostMemoryGB != 30 {
		t.Fatalf("explicit figure changed: %+v %+v", e, j.Spec.Needs)
	} else if !strings.Contains(JobTiming(j), "memory 30.") || !strings.Contains(JobTiming(j), "declared") {
		t.Fatalf("status hides the declared figure: %s", JobTiming(j))
	}
	waiting := submit(t, q, s)
	check(t, q, m, nil)
	reason := job(t, q, waiting.ID).Reason
	if !strings.Contains(reason, "learned from 2 comparable accepted attempts: accepted peak + measured uncertainty") {
		t.Fatalf("queued reason lacks source and evidence: %s", reason)
	}
}

func TestLearnedMemoryBoundsGrowAndNeverShrink(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	s := cpuSpec(1, 0)
	s.Name = "suite"
	constrained := func(r *Result) {
		r.Success = false
		r.MemoryPeak.Constrained = true
		r.MemoryPeak.Events = &MemoryEventCounts{Max: 1627}
	}
	runMemory(t, q, clock, m, s, 6, constrained)
	next := runMemory(t, q, clock, m, s, 3, nil)
	if next.Spec.Needs.HostMemoryGB != 32-minimumHostMemoryGB || next.MemoryEnvelope.Basis != "exploratory" {
		t.Fatal("constrained evidence was treated as an upper bound", next.MemoryEnvelope)
	}
	other := s
	other.Run.Command = []string{"echo", "other"}
	runMemory(t, q, clock, m, other, 1, constrained)
	j := runMemory(t, q, clock, m, other, 1, nil)
	if j.Spec.Needs.HostMemoryGB != 3+minimumHostMemoryGB || j.MemoryEnvelope.Evidence != 1 || !strings.Contains(j.MemoryEnvelope.Origin(), "bounded by 1 similar accepted attempts") {
		t.Fatal("family evidence lost", j.MemoryEnvelope)
	}
	// A family envelope that underestimates a new shape retains its failed
	// bound plus a physical page, without inventing a doubled demand floor.
	other.Run.Command = []string{"echo", "larger"}
	queued := submit(t, q, other)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != queued.ID {
		t.Fatal("not assigned")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(run, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, run.ID)
	if got.MemoryOOMFloor == nil || got.MemoryOOMFloor.HostMemoryGB != run.Spec.Needs.HostMemoryGB+minimumHostMemoryGB || got.OOMRetries != 1 {
		t.Fatal("failed envelope floor lost", got.MemoryOOMFloor)
	}
}

func TestLearnedMemoryPolicyOffRestoresDeclaredFigures(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	data, _ := json.Marshal(q.PriorityPolicy())
	if strings.Contains(string(data), "learned_memory") {
		t.Fatalf("default switch must stay off the wire for older CLIs: %s", data)
	}
	setLearnedMemory(t, q, false)
	// An older CLI's round trip omits the field and must not flip it back.
	record := q.PriorityPolicy()
	record.Policy.LearnedMemory = nil
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	check(t, q, m, nil)
	_, err := q.Submit(cpuSpec(1, 0))
	var refused *HTTPError
	if !errors.As(err, &refused) || refused.Code != "job_memory_invalid" {
		t.Fatalf("omitted memory accepted with learning off: %v", err)
	}
	s := cpuSpec(1, 0)
	s.Needs.Memory = "auto"
	j := runMemory(t, q, clock, m, s, 2, nil)
	if j.MemoryEnvelope.Source != "exploratory first run" || j.Spec.Needs.HostMemoryGB <= 16 {
		t.Fatalf("switch off did not restore host-wide exploration: %+v", j.MemoryEnvelope)
	}
}

// One unified CUDA machine worker (96 GiB allowance; 81 GiB physical room
// above the 25 GiB floor) owning its card and CPU targets.
func unifiedGPUFixture(t *testing.T, unified bool) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.ExtensionFingerprint = "cuda-v1"
	d := MachineDevice{ID: "0", Platform: "cuda", Identity: "synthetic-gb10", Selector: "0", MemoryMode: "unified", BudgetGB: 96, FreeMemoryGB: 96, ReserveMemoryGB: 25, UnifiedMemory: true, MemoryMeasured: true, ExtensionFingerprint: "cuda-v1"}
	if !unified {
		m.UnifiedMemory, m.CapabilityProfile.MemoryLayout = false, "separate"
		d.MemoryMode, d.UnifiedMemory = "separate", false
	}
	m.Devices = []MachineDevice{d}
	machineCheck(t, q, m)
	return q, clock, m
}

// `myce run --gpu cuda -- CMD` with no --mem or --host-mem.
func plainGPUSpec() Spec {
	return Spec{Name: "one-off", Project: "gpu-project", Priority: "experiment", Run: RunSpec{Command: []string{"echo", "done"}}, Needs: ResourceNeeds{CPUCores: 1, Platform: Platform{Required: []string{"cuda"}}}}
}

func assignGPU(t *testing.T, q *Queue, m Machine, want *Job, running ...*Job) *Job {
	t.Helper()
	j := assignedJob(machineCheck(t, q, m, running...))
	if j == nil || j.ID != want.ID {
		t.Fatalf("not assigned: %s", job(t, q, want.ID).Reason)
	}
	return j
}

func TestLearnedMemoryUnifiedGPUFirstRunIsBounded(t *testing.T) {
	q, clock, m := unifiedGPUFixture(t, true)
	j := assignGPU(t, q, m, submit(t, q, plainGPUSpec()))
	if e := j.MemoryEnvelope; e.DeviceGB != minimumHostMemoryGB || e.HostGB != 80.875 || e.Basis != "exploratory" || !strings.Contains(e.Source, "shared physical headroom") {
		t.Fatal("unknown shared pool was guessed", e)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	peer := submit(t, q, cpuSpec(1, 70))
	if next := assignedJob(machineCheck(t, q, m, j)); next != nil {
		t.Fatal("cold probe spent shared pool twice", next)
	}
	clock.Advance(time.Second)
	peak := 3.0
	r := result(j, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	p := assignGPU(t, q, m, peer)
	finish(t, q, p, true)
	// A host-only sample cannot turn unknown GPU use into accepted sharing.
	next := assignGPU(t, q, m, submit(t, q, plainGPUSpec()))
	if next.MemoryEnvelope.Basis != "exploratory" || next.MemoryEnvelope.Evidence != 0 || next.MemoryEnvelope.HostGB != 80.875 {
		t.Fatal("missing device sample became accepted", next.MemoryEnvelope)
	}
	// A complete host and device sample is exact evidence. The next run records
	// its accepted peaks, without uncertainty, for the by-use charge (#1044).
	if err := q.Started(result(next, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	device := 20.0
	r = result(next, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak, DeviceGB: &device}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	learned := assignGPU(t, q, m, submit(t, q, plainGPUSpec())).MemoryEnvelope.Learned
	if learned == nil || learned.PeakHostGB != peak || learned.PeakDeviceGB != device || learned.DeviceGB == nil || *learned.DeviceGB < device {
		t.Fatalf("accepted peaks not recorded: %+v", learned)
	}
}

func TestLearnedMemoryUnifiedGPUHostHintKeepsDeviceFigure(t *testing.T) {
	q, _, m := unifiedGPUFixture(t, true)
	s := plainGPUSpec()
	s.Needs.HostMemoryGB = 4 // `--host-mem 4G` without --mem.
	j := assignGPU(t, q, m, submit(t, q, s))
	if e := j.MemoryEnvelope; e.DeviceGB != 76.75 || e.HostGB != 80.875 || e.Basis != "hint" || !strings.Contains(e.Source, "shared physical headroom") {
		t.Fatalf("host-hint-only GPU job charged a 0 GiB device: %+v", e)
	}
}

func TestLearnedMemoryDiscreteGPUFirstRunIsBounded(t *testing.T) {
	q, clock, m := unifiedGPUFixture(t, false)
	s := plainGPUSpec()
	j := assignGPU(t, q, m, submit(t, q, s))
	if e := j.MemoryEnvelope; e.Source != physicalMemoryProbe || e.DeviceGB != 96 || e.HostGB != 89.875 {
		t.Fatal("cold discrete physical probe", e)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	peer := submit(t, q, cpuSpec(1, 50))
	if next := assignedJob(machineCheck(t, q, m, j)); next != nil {
		t.Fatal("unknown host demand shared", next)
	}
	clock.Advance(time.Second)
	peak := 14.0
	r := result(j, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	cpu := assignGPU(t, q, m, peer)
	finish(t, q, cpu, true)
	q = reopenQueue(t, q, clock)
	machineCheck(t, q, m)
	next := assignGPU(t, q, m, submit(t, q, s))
	if next.MemoryEnvelope.DeviceGB != 96 || next.Spec.Needs.HostMemoryGB != 89.75 || next.MemoryEnvelope.Basis != "exploratory" {
		t.Fatal("constrained peak became a guessed upper bound", next.MemoryEnvelope)
	}
}

func TestLearnedMemoryDiscreteGPUHostHints(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		device, host, wantDevice, wantHost float64
	}{
		{"device only", 8, 0, 8, 89.75},
		{"host only", 0, 4, 96, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _, m := unifiedGPUFixture(t, false)
			s := plainGPUSpec()
			s.Needs.Memory = "auto"
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = tc.device, tc.host
			j := assignGPU(t, q, m, submit(t, q, s))
			if j.Spec.Needs.MemoryGB != tc.wantDevice || j.Spec.Needs.HostMemoryGB != tc.wantHost {
				t.Fatalf("partial hint lost its stated vector or host bound: %+v", j.MemoryEnvelope)
			}
		})
	}
}

func TestLearnedMemoryDiscreteGPUSimilarHostAndOOM(t *testing.T) {
	q, clock, m := unifiedGPUFixture(t, false)
	s := plainGPUSpec()
	first := assignGPU(t, q, m, submit(t, q, s))
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	host, total := 3.0, 4.0
	r := result(first, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// An unrelated shape uses only family evidence. A larger unified-layout
	// family sample must not contaminate the discrete host vector.
	for _, point := range q.s.WorkObservations {
		point.MemoryLayout = "unified"
		other := 20.0
		point.MemoryPeak = &AttemptMemoryPeak{HostGB: &other}
		q.putWorkObservation("unified-family", point)
		break
	}
	s.Run.Command = append(s.Run.Command, "different-shape")
	next := assignGPU(t, q, m, submit(t, q, s))
	if e := next.MemoryEnvelope; next.Spec.Needs.HostMemoryGB != 4+minimumHostMemoryGB || e.DeviceGB != 96 || e.Evidence != 1 || !strings.Contains(e.Source, "accepted family peaks + measured uncertainty") {
		t.Fatalf("same-layout conservative host peak not used: %+v", e)
	}
	if err := q.Started(result(next, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r = result(next, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	retry := assignGPU(t, q, m, next)
	if retry.Spec.Needs.HostMemoryGB != (4+minimumHostMemoryGB)*2 || retry.MemoryEnvelope.DeviceGB != 96 || retry.OOMRetries != 0 {
		t.Fatalf("host OOM did not double only host vector: %+v", retry.MemoryEnvelope)
	}
}

func TestLearnedMemoryDiscreteGPUExtensionHostBound(t *testing.T) {
	q, _, m, s := gpuHostFixture(t)
	run := assignGPU(t, q, m, submit(t, q, s))
	if run.Spec.Needs.HostMemoryGB != 24-minimumHostMemoryGB || run.MemoryEnvelope.DeviceGB != 30 || run.MemoryEnvelope.Basis != "exploratory" {
		t.Fatalf("extension did not retain exclusive card with bounded host vector: %+v", run.MemoryEnvelope)
	}
}

// #631: accepted samples of the same plain discrete-GPU command must turn
// its next run into measured work, even when throughput occupancy is unknown.
func TestLearnedMemoryDiscreteGPUAcceptedIdentity(t *testing.T) {
	q, clock, m := unifiedGPUFixture(t, false)
	s := plainGPUSpec()
	for i := 0; i < 3; i++ {
		j := assignGPU(t, q, m, submit(t, q, s))
		if i > 0 && (j.MemoryEnvelope.Basis != "learned" || j.MemoryEnvelope.Evidence != i || j.Spec.Needs.HostMemoryGB != 8.6+minimumHostMemoryGB) {
			t.Fatalf("accepted identity stayed exploratory: %+v needs=%+v", j.MemoryEnvelope, j.Spec.Needs)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		q.s.Jobs[j.ID].Timing.Reason = "mixed or unknown host occupancy"
		clock.Advance(time.Second)
		host, total, device := 5.56, 8.6, 3.0
		r := result(j, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, DeviceGB: &device}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	q = reopenQueue(t, q, clock)
	machineCheck(t, q, m)
	j := assignGPU(t, q, m, submit(t, q, s))
	if e := j.MemoryEnvelope; e.Basis != "learned" || e.Evidence != 3 || j.Spec.Needs.HostMemoryGB != 8.6+minimumHostMemoryGB || e.DeviceGB != 3+minimumHostMemoryGB {
		t.Fatalf("restart lost learned envelope: %+v", e)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := q.Complete(result(j, true)); err != nil {
		t.Fatal(err)
	}
	// An arbitrary argument can change input size; it does not inherit the
	// accepted class just because the executable is the same (#631 decision).
	s.Run.Command = append(s.Run.Command, "different-input")
	next := assignGPU(t, q, m, submit(t, q, s))
	if e := next.MemoryEnvelope; e.Basis != "exploratory" || e.Evidence != 3 || next.Spec.Needs.HostMemoryGB != 8.6+minimumHostMemoryGB || !strings.Contains(e.Source, "accepted family peaks + measured uncertainty") {
		t.Fatalf("different arguments did not use only similar-family evidence: %+v", e)
	}
}

// A per-item entry with a stated hint never repeats its exact work. After two
// accepted family runs the family bound supersedes the hint; before, it stands.
func TestLearnedMemoryFamilySupersedesHintForPerItemWork(t *testing.T) {
	q, clock, m := learnedMemoryFixture(t)
	item := func(n string) Spec {
		s := cpuSpec(1, 20)
		s.Name, s.Needs.Memory = "render", "auto"
		s.Run.Command = []string{"render", n}
		return s
	}
	first := runMemory(t, q, clock, m, item("a"), 2, nil)
	if e := first.MemoryEnvelope; e.Basis != "hint" || first.Spec.Needs.HostMemoryGB != 20 {
		t.Fatalf("cold hinted run not on its hint: %+v", e)
	}
	second := runMemory(t, q, clock, m, item("b"), 3, nil)
	if e := second.MemoryEnvelope; e.Basis != "hint" || second.Spec.Needs.HostMemoryGB != 20 {
		t.Fatalf("one family run superseded the hint: %+v", e)
	}
	runMemory(t, q, clock, m, item("c"), 4, nil)
	fourth := runMemory(t, q, clock, m, item("d"), 4, nil)
	if e := fourth.MemoryEnvelope; e.Basis != "exploratory" || e.Evidence != 3 || fourth.Spec.Needs.HostMemoryGB != 6 || !strings.Contains(e.Source, "submitter hint superseded") || !strings.Contains(e.Source, boundedFirstRun) {
		t.Fatalf("family did not supersede the hint: %+v needs=%+v", e, fourth.Spec.Needs)
	}
}
