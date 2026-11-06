package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEfficiencyNovemberReplay(t *testing.T) {
	q, c := fixture(t)
	m := cpuMachine("large", "large", "cpu", 64, 121)
	m.HostMemoryBudgetGB = 121
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 12, MyceliumGB: 10}
	check(t, q, m, nil)
	idle := cpuMachine("idle", "idle", "cpu", 16, 64)
	idle.CapabilityProfile.Arch = "arm64"
	check(t, q, idle, nil)
	var running, waiting *Job
	if err := q.update(func() error {
		m := q.s.Machines["large"]
		s := cpuSpec(4, 84)
		s.Needs.MemoryGB = 0
		s.Needs.HostMemoryGB = 84
		running = &Job{ID: "running", Spec: s, State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, Queued: c.Now(), StartedAt: timePointer(c.Now()), MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory", HostGB: 84}}
		q.putJob(running.ID, running)
		s = cpuSpec(4, 40)
		s.Needs.MemoryGB = 0
		s.Needs.HostMemoryGB = 40
		s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}
		waiting = &Job{ID: "waiting", Spec: s, State: "queued", Queued: c.Now()}
		q.putJob(waiting.ID, waiting)
		q.refreshHostMemory()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.updateEfficiencyDiagnostics(context.Background()); err != nil {
		t.Fatal(err)
	}
	if waiting.Efficiency.Blocker != "memory_reservation" {
		t.Fatalf("blocker=%q reason=%q", waiting.Efficiency.Blocker, q.waitReason(waiting))
	}
	c.Advance(3 * time.Hour)
	if err := q.update(func() error {
		q.s.Machines["large"].LastSeen = c.Now()
		q.s.Machines["idle"].LastSeen = c.Now()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.updateEfficiencyDiagnostics(context.Background()); err != nil {
		t.Fatal(err)
	}
	report := q.efficiencyView
	var large *EfficiencyHost
	for i := range report.Hosts {
		if report.Hosts[i].Host == "large" {
			large = &report.Hosts[i]
		}
	}
	if large == nil || large.Rate.ReservedMemoryGB < 84 || large.PhysicalUsedMemoryGB == nil || *large.PhysicalUsedMemoryGB != 12 || large.ReservedMemoryGBSeconds < 84*3*3600 {
		t.Fatalf("host=%+v", large)
	}
	if report.Jobs[0].Seconds["memory_reservation"] != 3*3600 {
		t.Fatalf("waits=%+v", report.Jobs)
	}
	if len(report.Stranded) != 1 || report.Stranded[0].Host != "idle" || strings.Join(report.Stranded[0].Fields, ",") != "targets" {
		t.Fatalf("stranded=%+v", report.Stranded)
	}
	if len(report.Programs) != 1 || report.Programs[0].Basis != "exploratory" {
		t.Fatalf("programs=%+v", report.Programs)
	}
	if report.CPUStatus != "unknown" || large.MeasuredCPUSeconds != nil {
		t.Fatal("invented CPU use")
	}
	if !strings.Contains(strings.Join(report.Lines(), "\n"), "memory_reservation wait 3.0 hours") {
		t.Fatal(report.Lines())
	}
	// Read-only endpoint returns the same materialized generation without invoking
	// planning or the full snapshot path. The mutable state can subsequently change.
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	var got EfficiencyReport
	if err := client.Call(context.Background(), http.MethodGet, "/api/efficiency", nil, &got); err != nil {
		t.Fatal(err)
	}
	if !got.At.Equal(report.At) || len(got.Stranded) != 1 {
		t.Fatalf("endpoint=%+v", got)
	}
	// Capability failure is not authored stranding, even when targets exclude it.
	q.s.Machines["idle"].FreeCPUCores = 0
	if len(q.strandedDemand(waiting)) != 0 {
		t.Fatal("stranded despite insufficient cores")
	}
}
func timePointer(t time.Time) *time.Time { return &t }

func TestEfficiencyAccountingTransitionsAndCoverage(t *testing.T) {
	now := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)
	used := 2.0
	e := JobEfficiency{At: now, Since: now, Work: "opaque", Blocker: "memory_reservation", WaitSeconds: map[string]float64{}, Rate: efficiencyRate{Host: "host", Basis: "exploratory", ReservedMemoryGB: 8, ReservedCPUCores: 2, MeasuredMemoryGB: &used, ValidUntil: now.Add(time.Second)}}
	e.advance(now.Add(2 * time.Second))
	e.Blocker = "environment"
	e.Rate = efficiencyRate{}
	e.advance(now.Add(5 * time.Second))
	e.Blocker = ""
	e.advance(now.Add(10 * time.Second))
	if e.WaitSeconds["memory_reservation"] != 2 || e.WaitSeconds["environment"] != 3 {
		t.Fatal(e.WaitSeconds)
	}
	a := e.Allocations[0]
	if a.ReservedMemoryGBSeconds != 16 || a.MeasuredMemoryGBSeconds != 2 || a.MemoryCoveredSeconds != 1 || a.ReservedCPUSeconds != 4 || a.MeasuredCPUSeconds != nil {
		t.Fatalf("integral=%+v", a)
	}
	before, _ := json.Marshal(e)
	e.advance(now)
	after, _ := json.Marshal(e)
	if string(before) != string(after) {
		t.Fatal("clock regression rewrote totals")
	}
}
func TestEfficiencyPredictionAndSuspectAttempts(t *testing.T) {
	peak, reserved, predicted, actual, available := 2.0, 12.0, 20.0, 10.0, 40.0
	zero := uint64(0)
	j := &Job{ID: "job", MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, Machine: "m", Session: "s", LearningReason: "eligible", MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory", HostGB: reserved}, MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}}, {Attempt: 2, Machine: "m", Session: "s", LearningReason: "oom", MemoryPeak: &AttemptMemoryPeak{HostGB: &peak, Pressure: &MemoryPressureEvidence{ElapsedUS: 1e6, HostStallUS: &zero, CgroupStallUS: &zero, MinAvailableGB: &available}}}}, AttemptHistory: []AttemptOutcome{{Attempt: 2, Machine: "m", Session: "s", OutOfMemory: true}}, Timing: &WorkTiming{Execution: Execution{ID: "job", Attempt: 1}, PredictedSeconds: &predicted, ActualSeconds: &actual}}
	errors := efficiencyPredictions(j)
	if len(errors) != 1 || *errors[0].HostErrorGB != 10 || *errors[0].DurationErrorSeconds != 10 || errors[0].Basis != "exploratory" {
		t.Fatalf("errors=%+v", errors)
	}
	wasted := efficiencyWasted(j)
	if len(wasted) != 1 || wasted[0].Suspect == nil || !*wasted[0].Suspect || wasted[0].Kind != "oom" {
		t.Fatalf("wasted=%+v", wasted)
	}
	j.MemoryAttempts[1].MemoryPeak.PressureUnavailable = true
	if efficiencyWasted(j)[0].Suspect != nil {
		t.Fatal("unknown pressure called healthy")
	}
	j.AttemptHistory[0].OperatorStop = true
	if len(efficiencyWasted(j)) != 0 {
		t.Fatal("operator stop counted as waste")
	}
}
func TestEfficiencyLearnedAlerts(t *testing.T) {
	q, _ := fixture(t)
	used := 10.0
	r := &EfficiencyReport{Hosts: []EfficiencyHost{{Host: "host", Rate: efficiencyRate{ReservedMemoryGB: 12, MeasuredMemoryGB: &used}}}}
	var baseline map[string]efficiencyBaseline
	step := func() {
		var signals []EfficiencySignal
		baseline, signals = efficiencySignals(r, baseline)
		for _, s := range signals {
			q.observeCurrentHealth("efficiency/"+s.Subject+"/"+s.Metric, s.Active, HealthAlert{Kind: "efficiency_regression", Reason: s.Metric})
		}
	}
	for range alertWarmup {
		step()
	}
	if len(alertsOf(q, "efficiency_regression")) != 0 {
		t.Fatal("healthy fleet alerted")
	}
	r.Hosts[0].Rate.ReservedMemoryGB = 84
	for range alertConfirmation {
		step()
	}
	if len(alertsOf(q, "efficiency_regression")) != 1 {
		t.Fatal("reservation regression not alerted")
	}
	for range alertHistorySize {
		step()
	}
	if len(alertsOf(q, "efficiency_regression")) != 1 {
		t.Fatal("episode flooded delivery")
	}
	r.Hosts[0].Rate.ReservedMemoryGB = 12
	for range alertConfirmation {
		step()
	}
	if baseline["host/reservation_gap_gb"].Active {
		t.Fatal("recovery did not rearm")
	}
	r.Hosts[0].Rate.MeasuredMemoryGB = nil
	baseline, _ = efficiencySignals(r, baseline)
	if _, ok := baseline["host/reservation_gap_gb"]; ok {
		t.Fatal("unknown attribution trained as zero")
	}
}
