package myce

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A two-card host with one attempt on card 0 and RAM-refused compatible
// demand: card 1 is idle while work waits, the refusal is a memory wait, and
// the job fits once the holder is charged at its measured peak (#2183).
func TestUtilizationCountsIdleCardBesideBusyOne(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	holderSpec := spec("experiment")
	holderSpec.Needs.MemoryGB, holderSpec.Needs.HostMemoryGB = 4, 20
	holderSpec.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	submit(t, q, holderSpec)
	holder := assignedJob(machineCheck(t, q, m))
	if holder == nil {
		t.Fatal("holder not assigned")
	}
	if err := q.Started(result(holder, false)); err != nil {
		t.Fatal(err)
	}
	demandSpec := spec("experiment")
	demandSpec.Name = "demand"
	demandSpec.Needs.MemoryGB, demandSpec.Needs.HostMemoryGB = 4, 16
	demand := submit(t, q, demandSpec)
	// The holder has measurably used 3 GiB at its peak; the ledger still
	// reserves the rest of its 20 GiB charge as pending growth.
	m.HostAvailableMemoryGB = 29
	measure := func() {
		t.Helper()
		if got := assignedJob(machineCheck(t, q, m, holder)); got != nil {
			t.Fatal("demand admitted at its charge", got.ID)
		}
		if err := q.update(func() error {
			q.hostMemorySamples[m.ID][jobKey(*q.s.Jobs[holder.ID])] = hostMemorySample{id: clock.Now().UnixNano(), at: clock.Now(), receivedAt: clock.Now(), gb: 3, footprintGB: 3, peakGB: 3, growthObserved: true, started: true}
			q.refreshHostMemory()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := q.updateEfficiencyDiagnostics(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// Observations hold only while inventory is fresh; maintenance observes
	// far more often than the loss timeout.
	measure()
	clock.Advance(30 * time.Second)
	measure()
	const half = 30.0 / 3600

	if hold := job(t, q, demand.ID).Efficiency; hold == nil || hold.Blocker != "memory_reservation" {
		t.Fatalf("RAM refusal beside a busy card is not a memory wait: %+v reason=%q", hold, q.waitReason(q.s.Jobs[demand.ID]))
	}
	r := q.efficiencyView.Utilization
	if r == nil {
		t.Fatal("no utilization section")
	}
	var idle, busy *CardUtilization
	for i := range r.Cards {
		switch r.Cards[i].Device {
		case "0":
			busy = &r.Cards[i]
		case "1":
			idle = &r.Cards[i]
		}
	}
	if idle == nil || busy == nil {
		t.Fatalf("cards=%+v", r.Cards)
	}
	if got := idle.IdleByReason["would_fit:"+fitHolderGrowth]; got != half || idle.Hours[cardIdleWorkWaiting] != half || idle.Current != cardIdleWorkWaiting {
		t.Fatalf("idle card=%+v", idle)
	}
	if busy.Hours[cardBusyWorkWaiting] != half || busy.Hours[cardIdleWorkWaiting] != 0 {
		t.Fatalf("busy card=%+v", busy)
	}
	if r.IdleCardHoursWhileWait != half {
		t.Fatalf("idle card-hours=%v", r.IdleCardHoursWhileWait)
	}
	// Unknown coverage sits next to the number: the window began before the
	// first observation only on its own day, and observed time is subtracted.
	if idle.Hours[cardUnknown] < 0 || r.UnknownCardHours < idle.Hours[cardUnknown] {
		t.Fatalf("unknown=%v total=%v", idle.Hours[cardUnknown], r.UnknownCardHours)
	}
	var fit *WouldFitRow
	for i := range r.WouldFit {
		if r.WouldFit[i].Blocker == fitHolderGrowth {
			fit = &r.WouldFit[i]
		}
	}
	if fit == nil || fit.Episodes != 1 || fit.JobHours != half {
		t.Fatalf("would-fit=%+v", r.WouldFit)
	}
	text := strings.Join(q.efficiencyView.Lines(), "\n")
	for _, want := range []string{"0.0 idle card-hours while compatible work waited", "card machine/1 (fake, now idle_with_work_waiting)", "would_fit:holder_pending_growth 0.0", "blocked by holder_pending_growth: 1 job episodes"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}

// A demand that does not fit even with holders at their measured peaks is a
// real limit: it is counted idle-while-waiting but never as would-fit.
func TestUtilizationRealLimitIsNotWouldFit(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	holderSpec := spec("experiment")
	holderSpec.Needs.MemoryGB, holderSpec.Needs.HostMemoryGB = 4, 20
	holderSpec.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	submit(t, q, holderSpec)
	holder := assignedJob(machineCheck(t, q, m))
	if err := q.Started(result(holder, false)); err != nil {
		t.Fatal(err)
	}
	demandSpec := spec("experiment")
	demandSpec.Needs.MemoryGB, demandSpec.Needs.HostMemoryGB = 4, 30
	submit(t, q, demandSpec)
	m.HostAvailableMemoryGB = 13 // The holder really uses 19 GiB.
	for range 2 {
		machineCheck(t, q, m, holder)
		if err := q.update(func() error {
			q.hostMemorySamples[m.ID][jobKey(*q.s.Jobs[holder.ID])] = hostMemorySample{id: clock.Now().UnixNano(), at: clock.Now(), receivedAt: clock.Now(), gb: 19, footprintGB: 19, peakGB: 19, growthObserved: true, started: true}
			q.refreshHostMemory()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := q.updateEfficiencyDiagnostics(context.Background()); err != nil {
			t.Fatal(err)
		}
		clock.Advance(20 * time.Second)
	}
	r := q.efficiencyView.Utilization
	if r == nil || len(r.WouldFit) != 0 || r.IdleCardHoursWhileWait == 0 {
		t.Fatalf("real limit counted as would-fit: %+v", r)
	}
}

// Credit is split at UTC midnight and stops at the observation's inventory
// validity; the remainder is unknown, not the last state.
func TestUtilizationLedgerStaleObservationIsUnknown(t *testing.T) {
	start := time.Date(2026, 11, 20, 23, 0, 0, 0, time.UTC)
	card := CardObservation{Host: "h", Machine: "h", Device: "0", State: cardIdleWorkWaiting, Reason: "memory_reservation", ValidUntil: start.Add(2 * time.Hour)}
	var l *UtilizationLedger
	l = l.observe(utilizationObservation{at: start, cards: map[string]CardObservation{"h/h/0": card}, jobs: map[string]JobFit{"j": {Blocker: fitOwnCharge, ValidUntil: start.Add(time.Hour)}}})
	l = l.observe(utilizationObservation{at: start.Add(3 * time.Hour), cards: map[string]CardObservation{"h/h/0": card}})
	if len(l.Days) != 2 || l.Days[0].Cards["h/h/0"]["idle_with_work_waiting/memory_reservation"] != 3600 ||
		l.Days[1].Cards["h/h/0"]["idle_with_work_waiting/memory_reservation"] != 3600 || l.Days[1].Cards["h/h/0"][cardUnknown] != 3600 {
		t.Fatalf("days=%+v", l.Days)
	}
	if l.Days[0].WouldFit[fitOwnCharge].JobSeconds != 3600 || l.Days[0].WouldFit[fitOwnCharge].Episodes != 1 {
		t.Fatalf("would-fit=%+v", l.Days[0].WouldFit)
	}
	r := l.report(start.Add(3 * time.Hour))
	if r.Cards[0].Hours[cardIdleWorkWaiting] != 2 || r.Cards[0].Hours[cardUnknown] != 1 {
		t.Fatalf("report=%+v", r.Cards[0])
	}
}

// Every hold kind names its efficiency category; nothing passes for
// capacity by omission (#2184).
func TestHoldEfficiencyCategoriesCoverEveryKind(t *testing.T) {
	for kind := range holdKinds {
		if holdEfficiencyCategories[kind] == "" {
			t.Errorf("hold kind %q has no efficiency category", kind)
		}
	}
	for _, kind := range []string{memoryWaitReason, "memory", "host_memory_headroom", "memory_capacity"} {
		if got := newHold(kind, "").efficiencyCategory(); got != "memory_reservation" {
			t.Errorf("%s -> %s", kind, got)
		}
	}
	if got := (&Hold{Kind: "not-a-kind"}).efficiencyCategory(); got != "unknown" {
		t.Errorf("unlisted kind -> %s", got)
	}
}

// Stopped attempts split by what happened, inside stated windows, with burned
// command time (#2184).
func TestEfficiencyWastedAttemptsSplitByOutcome(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	started := now.Add(-time.Hour)
	j := &Job{ID: "j", AttemptHistory: []AttemptOutcome{
		{Attempt: 1, Machine: "m", Session: "s", EndedAt: now.Add(-10 * 24 * time.Hour), Interrupted: true, LaunchTimings: &LaunchTimings{CommandStartAt: timePointer(now.Add(-10*24*time.Hour - time.Hour))}},
		{Attempt: 2, Machine: "m", Session: "s", EndedAt: now.Add(-time.Minute), Interrupted: true, State: "cancelled", Reason: "cancel requested"},
		{Attempt: 3, Machine: "m", Session: "s", EndedAt: now.Add(-time.Minute), Interrupted: true, LaunchTimings: &LaunchTimings{}},
		{Attempt: 4, Machine: "m", Session: "s", EndedAt: now, Interrupted: true, LaunchTimings: &LaunchTimings{CommandStartAt: &started}},
		{Attempt: 5, Machine: "m", Session: "s", EndedAt: now, OutOfMemory: true, LaunchTimings: &LaunchTimings{CommandStartAt: &started}},
		{Attempt: 6, Machine: "m", Session: "s", EndedAt: now, Interrupted: true},
	}}
	rows := efficiencyWasted(j)
	kinds := []string{}
	for _, r := range rows {
		kinds = append(kinds, r.Kind)
	}
	if got := strings.Join(kinds, ","); got != "lost,cancelled,never_started,lost,oom,lost_start_unknown" {
		t.Fatal(got)
	}
	windows := wastedWindows(rows, now)
	week, retained := windows[0], windows[1]
	if week.Name != "7d" || week.Attempts["lost"] != 1 || week.Attempts["cancelled"] != 1 || week.Attempts["never_started"] != 1 || week.Attempts["oom"] != 1 || week.BurnedSeconds != 2*3600 || week.UnknownBurn != 2 {
		t.Fatalf("7d=%+v", week)
	}
	if retained.Attempts["lost"] != 2 || !retained.Since.Equal(now.Add(-10*24*time.Hour)) || retained.BurnedSeconds != 3*3600 {
		t.Fatalf("retained=%+v", retained)
	}
	if got := wastedWindows(nil, now)[1].words(); got != "none retained" {
		t.Fatal(got)
	}
	r := &EfficiencyReport{At: now, Wasted: rows, WastedWindows: windows}
	text := strings.Join(r.Lines(), "\n")
	if !strings.Contains(text, "3 stopped attempts that may have burned run time in the last 7 days") || !strings.Contains(text, "wasted 7d: 5 stopped attempts") || !strings.Contains(text, "2.0 attempt-hours burned") {
		t.Fatal(text)
	}
}

// Predictions compare against the peak the scheduler learns from; the cgroup
// figure, which counts page cache, is kept only as a diagnostic (#2184).
func TestEfficiencyPredictionIgnoresCgroupPageCache(t *testing.T) {
	host, cgroup := 3.98, 14.57
	j := &Job{ID: "j", MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, LearningReason: "eligible",
		MemoryEnvelope: &JobMemoryEnvelope{Basis: "declared", HostGB: 16},
		MemoryPeak:     &AttemptMemoryPeak{HostGB: &host, CgroupGB: &cgroup}}}}
	rows := efficiencyPredictions(j)
	if len(rows) != 1 || *rows[0].AcceptedHostPeakGB != host || *rows[0].HostErrorGB != 16-host || rows[0].CgroupPeakGB == nil || *rows[0].CgroupPeakGB != cgroup {
		t.Fatalf("rows=%+v", rows)
	}
}

// A host without an attributable memory observation says why, shows its
// retained attempt peaks, and states the window its integrals cover (#2184).
func TestEfficiencyHostStatesWindowAndUnknownReason(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("mac", "mac", "cpu", 8, 16)
	m.CapabilityProfile.OS = "darwin"
	check(t, q, m, nil)
	peak := 2.5
	if err := q.update(func() error {
		q.putJob("done", &Job{ID: "done", Spec: cpuSpec(1, 4), State: "succeeded", MemoryAttempts: []AttemptMemoryFacts{{Attempt: 1, Machine: "mac", LearningReason: "eligible", MemoryPeak: &AttemptMemoryPeak{HostGB: &peak}}}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Hour)
	check(t, q, m, nil)
	if err := q.updateEfficiencyDiagnostics(context.Background()); err != nil {
		t.Fatal(err)
	}
	var mac *EfficiencyHost
	for i := range q.efficiencyView.Hosts {
		if q.efficiencyView.Hosts[i].Host == "mac" {
			mac = &q.efficiencyView.Hosts[i]
		}
	}
	if mac == nil || mac.MeasuredMemoryUnknown == "" || mac.AttemptPeaks == nil || mac.AttemptPeaks.Attempts != 1 || mac.AttemptPeaks.MaxGB != peak {
		t.Fatalf("host=%+v", mac)
	}
	text := strings.Join(q.efficiencyView.Lines(), "\n")
	if !strings.Contains(text, "measured Mycelium unknown (the worker sends no host memory observation) (attempt host peaks: 1 retained, median 2.5 GiB, max 2.5 GiB)") || !strings.Contains(text, "integrals since "+mac.Since.UTC().Format(time.RFC3339)) {
		t.Fatal(text)
	}
}

// Project usage rows carry no occupancy: a project has no observed-time
// denominator, so the field was always zero (#2184).
func TestUsageProjectRowsHaveNoBusyPercent(t *testing.T) {
	data, err := json.Marshal(UsageReport{Projects: []ProjectUsage{{Name: "p", GPUHours: 1}}, Daily: []DailyUsage{{Date: "2026-11-26", Project: "p", GPUHours: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "busy_percent") {
		t.Fatal(string(data))
	}
}
