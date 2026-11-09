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
	t.Run("contract", func(t *testing.T) {
		q, c := fixture(t)
		m := cpuMachine("large", "large", "cpu", 64, 121)
		m.HostMemoryBudgetGB = 121
		m.CapabilityProfile.SystemMemoryBytes = 121 << 30
		m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 12, MyceliumGB: 10}
		check(t, q, m, nil)
		idle := cpuMachine("idle", "idle", "cpu", 16, 64)
		idle.CapabilityProfile.Arch = "arm64"
		check(t, q, idle, nil)
		var running, waiting *Job
		if err := q.update(func() error {
			m := q.s.Machines["large"]
			s := cpuSpec(4, 84)
			if err := normalizeResources(&s); err != nil {
				return err
			}
			running = &Job{ID: "running", Spec: s, State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, Queued: c.Now(), StartedAt: timePointer(c.Now()), MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory", HostGB: 84}}
			q.putJob(running.ID, running)
			s = cpuSpec(4, 40)
			if err := normalizeResources(&s); err != nil {
				return err
			}
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
		// Refresh accepted reports, including the original exact running custody.
		// Writing LastSeen alone is not evidence of a received check-in.
		check(t, q, m, running)
		check(t, q, idle, nil)
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
		q.observeRead = func(string, time.Duration, time.Duration) { t.Error("efficiency endpoint performed a full capture") }
		server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
		defer server.Close()
		client := Client{URL: server.URL, Token: "test"}
		var got EfficiencyReport
		if err := client.Call(context.Background(), http.MethodGet, "/api/efficiency", nil, &got); err != nil {
			t.Fatal(err)
		}
		q.observeRead = nil
		if !got.At.Equal(report.At) || len(got.Stranded) != 1 {
			t.Fatalf("endpoint=%+v", got)
		}
		for _, path := range []string{"/api/status", "/api/inspection"} {
			var snapshot Snapshot
			if err := client.Call(context.Background(), http.MethodGet, path, nil, &snapshot); err != nil {
				t.Fatal(err)
			}
			if snapshot.Efficiency == nil || !snapshot.Efficiency.At.Equal(report.At) || len(snapshot.Efficiency.Stranded) != 1 {
				t.Fatalf("%s omitted the maintenance report", path)
			}
		}
		jobView, _, err := q.captureObservation(waiting.ID)
		if err != nil {
			t.Fatal(err)
		}
		if jobView.efficiencyView != nil || jobView.s.EfficiencyHosts != nil || jobView.s.EfficiencyHealth != nil || jobView.s.StrandedDemand != nil {
			t.Fatal("job-only capture retained fleet diagnostics")
		}
		// Capability failure is not authored stranding, even when targets exclude it.
		q.s.Machines["idle"].FreeCPUCores = 0
		if len(q.strandedDemand(waiting)) != 0 {
			t.Fatal("stranded despite insufficient cores")
		}

	})

	t.Run("EfficiencyConsumesRetainedResourceFacts", func(t *testing.T) {
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

	})

	t.Run("EfficiencyPredictionAndSuspectAttempts", func(t *testing.T) {
		peak, reserved, predicted, actual, available := 2.0, 12.0, 20.0, 10.0, 40.0
		zero := uint64(0)
		j := &Job{ID: "job", MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, Machine: "m", Session: "s", LearningReason: "eligible", MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory", HostGB: reserved}, MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}}, {Attempt: 2, Machine: "m", Session: "s", LearningReason: "oom", MemoryPeak: &AttemptMemoryPeak{HostGB: &peak, Pressure: &MemoryPressureEvidence{ElapsedUS: 1e6, HostStallUS: &zero, CgroupStallUS: &zero, MinAvailableGB: &available}}}}, AttemptHistory: []AttemptOutcome{{Attempt: 2, Machine: "m", Session: "s", OutOfMemory: true}}, Timing: &WorkTiming{Execution: Execution{ID: "job", Attempt: 1}, PredictedSeconds: &predicted, ActualSeconds: &actual}}
		errors := efficiencyPredictions(j)
		if len(errors) != 1 || *errors[0].HostErrorGB != 10 || *errors[0].DurationErrorSeconds != 10 || errors[0].Basis != "exploratory" {
			t.Fatalf("errors=%+v", errors)
		}
		// A completed program can teach prediction error without another live job.
		_, signals := efficiencySignals(&EfficiencyReport{At: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC), Predictions: errors}, nil)
		if len(signals) != 2 || errors[0].Work == "" {
			t.Fatalf("completed prediction facts did not reach the adaptive path: %+v", signals)
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

	})

	t.Run("EfficiencyRequeuedCustodyRemainsCharged", func(t *testing.T) {
		q, c := fixture(t)
		m := cpuMachine("host", "host", "cpu", 8, 16)
		check(t, q, m, nil)
		s := cpuSpec(2, 8)
		if err := normalizeResources(&s); err != nil {
			t.Fatal(err)
		}
		j := &Job{ID: "work", Spec: s, State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory"}}
		if err := q.update(func() error { q.putJob(j.ID, j); q.refreshHostMemory(); return nil }); err != nil {
			t.Fatal(err)
		}
		c.Advance(5 * time.Second)
		if err := q.update(func() error { q.retainRelease(j); q.requeue(j, "lost", false); q.refreshHostMemory(); return nil }); err != nil {
			t.Fatal(err)
		}
		if j.Machine != "" || len(j.Efficiency.RetainedRates) != 1 {
			t.Fatal("fixture did not retain old execution custody")
		}
		c.Advance(5 * time.Second)
		if err := q.update(func() error {
			q.settleRelease(Result{Execution: Execution{ID: j.ID, Attempt: 1}, Machine: m.ID, Session: m.Session}, true)
			q.refreshHostMemory()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		c.Advance(5 * time.Second)
		if err := q.update(func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		if len(j.Efficiency.Allocations) != 1 {
			t.Fatal(j.Efficiency.Allocations)
		}
		a := j.Efficiency.Allocations[0]
		if a.Host != "host" || a.Basis != "exploratory" || a.ReservedMemoryGBSeconds != 80 || a.ReservedCPUSeconds != 20 {
			t.Fatalf("custody charge ended before exact release or continued afterward: %+v", a)
		}

	})
}
func timePointer(t time.Time) *time.Time { return &t }

func TestEfficiencyRollbackAndRevisionFence(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, c := fixture(t)
		if err := q.update(func() error {
			q.putJob("wait", &Job{ID: "wait", Spec: spec("experiment"), State: "queued", Queued: c.Now()})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		c.Advance(time.Second)
		if err := q.update(func() error { return context.Canceled }); err != context.Canceled {
			t.Fatal(err)
		}
		if len(q.s.Jobs["wait"].Efficiency.WaitSeconds) != 0 {
			t.Fatal("failed transaction advanced accounting")
		}
		c.Advance(time.Second)
		if err := q.update(func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		if q.s.Jobs["wait"].Efficiency.WaitSeconds["unknown"] != 2 {
			t.Fatal("rollback lost or doubled elapsed time")
		}
		d, err := q.prepareEfficiencyDiagnostics(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := q.update(func() error { q.s.Jobs["wait"].State = "paused"; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := q.update(func() error { q.applyEfficiencyDiagnostics(d); return nil }); err != nil {
			t.Fatal(err)
		}
		if q.s.Jobs["wait"].Efficiency.Blocker != "holds" {
			t.Fatal("stale diagnostic crossed job-state fence")
		}

	})

	t.Run("EfficiencyStopSnapshotSurvivesCleanup", func(t *testing.T) {
		q, c := fixture(t)
		m := cpuMachine("host", "host", "cpu", 8, 16)
		m.MemoryObservation = &HostMemoryObservation{TotalGB: 16, UsedGB: 3, MyceliumGB: 2}
		m.HostHealth = &HostHealth{History: []HostHealthSample{{At: c.Now(), HostHealthReading: HostHealthReading{Pressure: 0}}}}
		m.IOPressure = &IOPressure{Some: &PressureStall{Avg10: 2}, SampledAt: c.Now()}
		check(t, q, m, nil)
		var first time.Time
		if err := q.update(func() error {
			j := &Job{ID: "stopped", Spec: cpuSpec(1, 2), State: "running", Attempt: 1, Machine: m.ID, Session: m.Session}
			q.putJob(j.ID, j)
			q.recordAttemptOutcome(j, AttemptOutcome{Attempt: 1, Machine: m.ID, Session: m.Session, Interrupted: true, Forced: true})
			first = j.AttemptHistory[0].StopObservation.At
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		c.Advance(time.Second)
		if err := q.update(func() error {
			q.s.Machines[m.ID].MemoryObservation.UsedGB = 15
			q.s.Machines[m.ID].HostHealth.History[0].Pressure = 10
			q.s.Machines[m.ID].IOPressure.Some.Avg10 = 20
			j := q.s.Jobs["stopped"]
			q.recordAttemptOutcome(j, AttemptOutcome{Attempt: 1, Machine: m.ID, Session: m.Session, Interrupted: true, Forced: true})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		o := q.s.Jobs["stopped"].AttemptHistory[0].StopObservation
		if o == nil || !o.At.Equal(first) || o.HeadroomGB == nil || *o.HeadroomGB != 13 || o.MemoryPressure == nil || *o.MemoryPressure != 0 || o.IOPressure == nil || o.IOPressure.Some == nil || o.IOPressure.Some.Avg10 != 2 {
			t.Fatalf("cleanup rewrote original stop evidence: %+v", o)
		}
		wasted := efficiencyWasted(q.s.Jobs["stopped"])
		if len(wasted) != 1 || wasted[0].Suspect == nil || !*wasted[0].Suspect {
			t.Fatalf("fresh host headroom and normal pressure did not explain suspect stop: %+v", wasted)
		}
		// A late statistical projection cannot replace a newer committed observation.
		newer := &efficiencyDiagnostics{report: &EfficiencyReport{At: c.Now().Add(time.Second)}}
		older := &efficiencyDiagnostics{report: &EfficiencyReport{At: c.Now()}}
		q.applyEfficiencyDiagnostics(newer)
		q.applyEfficiencyDiagnostics(older)
		if older.applied {
			t.Fatal("out-of-order capture was committed")
		}

	})
}

func TestEfficiencyReservationUsesLiveAdmissionCharge(t *testing.T) {
	q, c := fixture(t)
	m := cpuMachine("host", "host", "cpu", 8, 32)
	check(t, q, m, nil)
	s := cpuSpec(2, 8)
	if err := normalizeResources(&s); err != nil {
		t.Fatal(err)
	}
	s.Needs.Memory = "auto"
	j := &Job{ID: "probe", State: "running", Spec: s, Machine: m.ID, Session: m.Session, Attempt: 1, MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory", HostGB: 8, ProbeFloorGB: minimumHostMemoryGB}}
	if err := q.update(func() error {
		q.putJob(j.ID, j)
		q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: {jobKey(*j): {at: c.Now(), gb: 1, footprintGB: 1, growthObserved: true}}}
		q.refreshHostMemory()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	charge := q.hostJobCharge(j)
	if charge >= 8 || j.Efficiency.Rate.ReservedMemoryGB != charge || q.s.Machines[m.ID].HostMemoryInUseGB != charge {
		t.Fatalf("diagnostics disagree with admission: charge=%g rate=%g host=%g", charge, j.Efficiency.Rate.ReservedMemoryGB, q.s.Machines[m.ID].HostMemoryInUseGB)
	}
	c.Advance(time.Second)
	if err := q.update(func() error { q.refreshHostMemory(); return nil }); err != nil {
		t.Fatal(err)
	}
	if got := j.Efficiency.Allocations[0].ReservedMemoryGBSeconds; got != charge {
		t.Fatalf("integrated ceiling instead of admission charge: got %g want %g", got, charge)
	}
}
