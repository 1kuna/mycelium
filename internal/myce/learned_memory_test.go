package myce

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const boundedQuarter = "exploratory first run (bounded: 25% of host allowance)"

func setLearnedMemory(t *testing.T, q *Queue, on bool) {
	t.Helper()
	record := q.PriorityPolicy()
	p := record.Policy
	p.LearnedMemory = &on
	if _, err := q.SetPriorityPolicy(record.Revision, p); err != nil {
		t.Fatal(err)
	}
}

// One 32 GiB CPU host; the cold bound is a quarter of its allowance.
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
	if e := first.MemoryEnvelope; e.Source != boundedQuarter || e.Basis != "exploratory" || first.Spec.Needs.HostMemoryGB != 8 || first.Spec.Needs.Memory != "auto" {
		t.Fatalf("cold run not bounded: %+v needs=%+v", e, first.Spec.Needs)
	}
	second := runMemory(t, q, clock, m, s, 2, nil)
	if e := second.MemoryEnvelope; e.Source != "accepted peak × 1.25" || e.Basis != "learned" || e.Evidence != 1 || second.Spec.Needs.HostMemoryGB != 2.5 {
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
	if !strings.Contains(reason, "learned from 2 comparable accepted attempts: accepted peak × 1.25") {
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
	// #559 replay: a failed ceiling-bound attempt is a lower bound only.
	runMemory(t, q, clock, m, s, 6, constrained)
	next := runMemory(t, q, clock, m, s, 3, nil)
	if e := next.MemoryEnvelope; next.Spec.Needs.HostMemoryGB != 12 || !strings.HasSuffix(e.Source, "(bounded: constrained peak × 2)") {
		t.Fatalf("constrained peak did not raise the bound: %+v", e)
	}
	// A small constrained peak never lowers the quarter-allowance bound.
	other := s
	other.Run.Command = []string{"echo", "other"}
	runMemory(t, q, clock, m, other, 1, constrained)
	if j := runMemory(t, q, clock, m, other, 1, nil); j.Spec.Needs.HostMemoryGB != 3*similarMemoryFactor {
		t.Fatalf("similar accepted peak did not bound the cold run: %+v", j.MemoryEnvelope)
	} else if e := j.MemoryEnvelope; e.Evidence != 1 || !strings.Contains(e.Origin(), "bounded by 1 similar accepted attempts") {
		t.Fatalf("similar evidence not reported: %s", e.Origin())
	}

	// OOM on a bounded first run doubles without spending the one retry; at
	// half the allowance, host-wide exploration and its retry take over.
	fresh := cpuSpec(1, 0)
	fresh.Name = "fresh"
	queued := submit(t, q, fresh)
	for _, want := range []string{boundedQuarter, "exploratory first run; OOM retry floor"} {
		j := check(t, q, m, nil).Job
		if j == nil || j.ID != queued.ID || j.MemoryEnvelope.Source != want || j.OOMRetries != 0 {
			t.Fatalf("want %q: %+v %s", want, j.MemoryEnvelope, job(t, q, queued.ID).Reason)
		}
		if want != boundedQuarter && j.Spec.Needs.HostMemoryGB < 16 {
			t.Fatalf("retry shrank below the doubled floor: %+v", j.Spec.Needs)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		r := result(j, false)
		r.OutOfMemory = true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	if got := job(t, q, queued.ID); got.State != "failed" || got.OOMRetries != 1 {
		t.Fatalf("host-wide OOM lost its one-retry limit: %s %s retries=%d", got.State, got.Reason, got.OOMRetries)
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
	d := MachineDevice{ID: "0", Platform: "cuda", Identity: "synthetic-gb10", Selector: "0", MemoryMode: "unified", BudgetGB: 96, FreeMemoryGB: 96, UnifiedMemory: true, MemoryMeasured: true, ExtensionFingerprint: "cuda-v1"}
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
	if e := j.MemoryEnvelope; e.DeviceGB != 1 || e.HostGB != 2.125 || e.Basis != "exploratory" || !strings.HasSuffix(e.Source, "(bounded: unified GPU: 1 GiB device default, 1 GiB host default)") {
		t.Fatalf("unified GPU first run not bounded: %+v", e)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	// Production fits beside it without stopping it; host-wide exploration
	// would have reserved almost all of the 81 GiB physical room.
	production := cpuSpec(1, 70)
	production.Name, production.Priority = "production", "production"
	p := assignGPU(t, q, m, submit(t, q, production), j)
	if held := job(t, q, j.ID); held.State != "running" || held.StopAt != nil {
		t.Fatalf("GPU job was stopped for room it never needed: %s %s", held.State, held.Reason)
	}
	finish(t, q, p, true)
	clock.Advance(time.Second)
	peak := 3.0 // Host side only: unified CUDA reports no device sample.
	r := result(j, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}

	// The accepted host peak sizes the next run's host side; the device keeps
	// its default, since no device sample exists.
	next := assignGPU(t, q, m, submit(t, q, plainGPUSpec()))
	if next.Spec.Needs.HostMemoryGB != 7.5 || next.Spec.Needs.MemoryGB != 1 || next.MemoryEnvelope.Evidence != 1 || !strings.Contains(next.MemoryEnvelope.Source, "similar work host peak × 2.5") {
		t.Fatalf("similar host peak not used: %+v %+v", next.MemoryEnvelope, next.Spec.Needs)
	}
	// A classified host OOM doubles the bound without spending the one retry.
	if err := q.Started(result(next, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	oom := result(next, false)
	oom.OutOfMemory = true
	if err := q.Complete(oom); err != nil {
		t.Fatal(err)
	}
	retry := assignGPU(t, q, m, next)
	if retry.OOMRetries != 0 || retry.Spec.Needs.HostMemoryGB != 15 || retry.Spec.Needs.MemoryGB != 1 {
		t.Fatalf("host OOM did not double the bound: %+v %+v", retry.MemoryEnvelope, retry.Spec.Needs)
	}
}

func TestLearnedMemoryUnifiedGPUHostHintKeepsDeviceFigure(t *testing.T) {
	q, _, m := unifiedGPUFixture(t, true)
	s := plainGPUSpec()
	s.Needs.HostMemoryGB = 4 // `--host-mem 4G` without --mem.
	j := assignGPU(t, q, m, submit(t, q, s))
	if e := j.MemoryEnvelope; e.DeviceGB != 1 || e.HostGB != 5.125 || e.Basis != "hint" || !strings.Contains(e.Source, "1 GiB device default, stated host") {
		t.Fatalf("host-hint-only GPU job charged a 0 GiB device: %+v", e)
	}
}

func TestLearnedMemoryDiscreteGPUFirstRunIsBounded(t *testing.T) {
	q, clock, m := unifiedGPUFixture(t, false)
	s := plainGPUSpec()
	j := assignGPU(t, q, m, submit(t, q, s))
	if e := j.MemoryEnvelope; e.Source != boundedQuarter || e.DeviceGB != 96 || e.HostGB != 24.125 {
		t.Fatalf("discrete GPU first-run host vector not bounded: %+v", e)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	cpu := assignGPU(t, q, m, submit(t, q, cpuSpec(1, 50)), j)
	if held := job(t, q, j.ID); held.State != "running" || held.StopAt != nil {
		t.Fatalf("CPU peer stopped GPU run: %+v", held)
	}
	finish(t, q, cpu, true)
	clock.Advance(time.Second)
	peak := 14.0
	r := result(j, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	q = reopenQueue(t, q, clock)
	machineCheck(t, q, m)
	next := assignGPU(t, q, m, submit(t, q, s))
	if e := next.MemoryEnvelope; e.DeviceGB != 96 || next.Spec.Needs.HostMemoryGB != 28 || !strings.Contains(e.Source, "constrained peak × 2") {
		t.Fatalf("constrained host peak did not grow independently: %+v", e)
	}
}

func TestLearnedMemoryDiscreteGPUHostHints(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		device, host, wantDevice, wantHost float64
	}{
		{"device only", 8, 0, 8, 24},
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
	if e := next.MemoryEnvelope; next.Spec.Needs.HostMemoryGB != 10 || e.DeviceGB != 96 || e.Evidence != 1 || !strings.Contains(e.Source, "similar work peak × 2.5") {
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
	if retry.Spec.Needs.HostMemoryGB != 20 || retry.MemoryEnvelope.DeviceGB != 96 || retry.OOMRetries != 0 {
		t.Fatalf("host OOM did not double only host vector: %+v", retry.MemoryEnvelope)
	}
}

func TestLearnedMemoryDiscreteGPUExtensionHostBound(t *testing.T) {
	q, _, m, s := gpuHostFixture(t)
	run := assignGPU(t, q, m, submit(t, q, s))
	if run.Spec.Needs.HostMemoryGB != 6 || run.MemoryEnvelope.DeviceGB != 30 || run.MemoryEnvelope.Basis != "exploratory" {
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
		if i > 0 && (j.MemoryEnvelope.Basis != "learned" || j.MemoryEnvelope.Evidence != i || j.Spec.Needs.HostMemoryGB != 8.6*1.25) {
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
	if e := j.MemoryEnvelope; e.Basis != "learned" || e.Evidence != 3 || j.Spec.Needs.HostMemoryGB != 8.6*1.25 || e.DeviceGB != 3*1.25 {
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
	if e := next.MemoryEnvelope; e.Basis != "exploratory" || e.Evidence != 3 || next.Spec.Needs.HostMemoryGB != 8.6*2.5 || !strings.Contains(e.Source, "similar work peak × 2.5") {
		t.Fatalf("different arguments did not use only similar-family evidence: %+v", e)
	}
}
