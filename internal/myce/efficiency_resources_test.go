package myce

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEfficiencyConsumesRetainedResourceFacts(t *testing.T) {
	q, c := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 32)
	m.IOPressure = &IOPressure{Some: &PressureStall{}, SampledAt: c.Now()}
	check(t, q, m, nil)
	oldCPU, currentCPU := 3.0, 0.0
	read, zero := uint64(128), uint64(0)
	j := &Job{ID: "resources", Attempt: 2, Machine: m.ID, Session: m.Session, State: "running", Spec: cpuSpec(2, 4),
		AttemptHistory: []AttemptOutcome{{Attempt: 1, Machine: m.ID, Session: "previous", Forced: true, Placement: &PlacementDecision{Host: "host"}}},
		MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, Machine: m.ID, Session: "previous", MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory"}}},
		ResourceAttempts: []AttemptResourceFacts{
			{Attempt: 1, Machine: m.ID, Session: "previous", Resources: &AttemptResources{CPU: AttemptCPUTime{UsageSeconds: &oldCPU, LowerBound: true}, IO: AttemptIO{ReadBytes: &read, WriteBytes: &zero, ReadOps: &zero, WriteOps: &zero}, ElapsedSeconds: 10}},
			{Attempt: 2, Machine: m.ID, Session: m.Session, Resources: &AttemptResources{CPU: AttemptCPUTime{UsageSeconds: &currentCPU}, IO: AttemptIO{Unavailable: "read_failed"}, ElapsedSeconds: 20}},
		}}
	if err := q.update(func() error { q.putJob(j.ID, j); return nil }); err != nil {
		t.Fatal(err)
	}
	r := q.efficiencyReport()
	if len(r.Hosts) != 1 || r.Hosts[0].IOPressure == nil || r.Hosts[0].IOStatus != "measured host pressure" {
		t.Fatalf("host pressure missing: %+v", r.Hosts)
	}
	h := r.Hosts[0]
	if h.MeasuredCPUSeconds == nil || *h.MeasuredCPUSeconds != 3 || h.CPUObservedElapsedSeconds != 30 || h.CPUObservedAttempts != 2 || !h.CPULowerBound || h.ReadBytes == nil || *h.ReadBytes != 128 || h.WriteBytes == nil || *h.WriteBytes != 0 {
		t.Fatalf("counter coverage: %+v", h)
	}
	if r.CPUStatus != "partial retained attempts" || len(r.Wasted) != 1 || r.Wasted[0].Resources == nil || *r.Wasted[0].Resources.CPU.UsageSeconds != 3 {
		t.Fatalf("attempt identity lost: %+v", r)
	}
	// A measured zero remains distinguishable from the other unavailable I/O.
	var declared *EfficiencyAllocation
	for i := range r.Programs {
		if r.Programs[i].Basis == "declared" {
			declared = &r.Programs[i]
		}
	}
	if declared == nil || declared.MeasuredCPUSeconds == nil || *declared.MeasuredCPUSeconds != 0 || declared.ReadBytes != nil {
		t.Fatalf("unknown became zero: %+v", declared)
	}
	before, _ := json.Marshal(r)
	again, _ := json.Marshal(q.efficiencyReport())
	if string(before) != string(again) {
		t.Fatal("repeated projection double-counted cumulative resources")
	}
	// The collector replaces a cumulative observation, not an incremental delta.
	currentCPU = 4
	j.ResourceAttemptsDropped = 1
	r = q.efficiencyReport()
	if *r.Hosts[0].MeasuredCPUSeconds != 7 || r.ResourceAttemptsDropped != 1 || !strings.Contains(strings.Join(r.Lines(), "\n"), "1 attempt records dropped") {
		t.Fatalf("replacement/retention accounting: %+v", r)
	}
	c.Advance(q.machineLossTimeout(m.ID) + time.Second)
	r = q.efficiencyReport()
	if r.Hosts[0].IOPressure.Unavailable != "stale" {
		t.Fatal("stale host PSI remained current")
	}
}

func TestEfficiencyIOPressureUsesLearnedAlerts(t *testing.T) {
	_, c := fixture(t)
	p := &IOPressure{Some: &PressureStall{Avg10: 1}}
	r := &EfficiencyReport{Hosts: []EfficiencyHost{{Host: "host", IOPressure: p}}}
	var baseline map[string]efficiencyBaseline
	step := func() {
		c.Advance(time.Second)
		r.At = c.Now()
		baseline, _ = efficiencySignals(r, baseline)
	}
	for range alertWarmup {
		step()
	}
	key := "host/io_pressure_some_percent"
	if baseline[key].Active {
		t.Fatal("healthy I/O alerted")
	}
	p.Some.Avg10 = 30
	for range alertConfirmation {
		step()
	}
	if !baseline[key].Active {
		t.Fatal("I/O regression did not reach the adaptive path")
	}
	p.Unavailable = "stale"
	step()
	if baseline[key].Observed || !baseline[key].Active {
		t.Fatal("missing I/O evidence proved recovery")
	}
}
