package myce

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func r5Machine(budget float64) Machine {
	m := cpuMachine("worker", "host", "cpu", 8, budget)
	m.Devices = []MachineDevice{}
	m.HostMemoryBudgetGB, m.HostMemoryBudgetMeasured = budget, true
	m.CapabilityProfile.SystemMemoryBytes = 100 << 30
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability, sampledMemoryCapability)
	return m
}

func r5Check(t *testing.T, q *Queue, m Machine, pool MemoryShadowPool, runs ...*Job) Reply {
	t.Helper()
	pool.sum()
	m.MemoryShadow = &MemoryShadowReport{Pools: []MemoryShadowPool{pool}, ReactionSeconds: 11, stopForecast: true}
	return machineCheck(t, q, m, runs...)
}

func r5RAM(budget, free float64) MemoryShadowPool {
	return MemoryShadowPool{Name: "ram", CapacityGB: budget, FreeGB: shadowGB(free)}
}

func r5Start(t *testing.T, q *Queue, reply Reply, id string) *Job {
	t.Helper()
	j := assignedJob(reply)
	if j == nil || j.ID != id {
		t.Fatalf("job not assigned: %+v; queued: %+v", j, job(t, q, id).Hold)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	return j
}

func r5Clean(t *testing.T, q *Queue, j *Job, ram, card memoryRunPool) {
	t.Helper()
	r := result(j, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: shadowGB(ram.PeakGB), Learned: &memoryRunPeaks{RAM: ram, Card: card}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
}

func r5Hold(t *testing.T, q *Queue, id string) *Hold {
	t.Helper()
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	j, err := q.jobSnapshot(id)
	if err != nil {
		t.Fatal(err)
	}
	return j.Hold
}

func r5Stop(t *testing.T, q *Queue, clock *fakeClock, m Machine, j *Job, p MemoryShadowPool, expected, peak float64, releaseOnly bool) {
	t.Helper()
	clock.Advance(time.Second)
	p.Attempts = []MemoryShadowAttempt{{ID: j.ID, Attempt: j.Attempt, ExpectedGB: expected, FootprintGB: shadowGB(peak), HighWaterGB: peak}}
	p.ForecastFreeGB = shadowGB(-1)
	r5Check(t, q, m, p, j)
	got := job(t, q, j.ID)
	if stop := priorityStop(got); got.State != "stopping" || stop == nil || stop.MemorySample == nil {
		t.Fatalf("expected R4 stop with R5 evidence: %s %s %+v", got.State, got.Reason, stop)
	}
	r := result(j, false)
	r.Interrupted, r.ReleaseOnly = true, releaseOnly
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "queued" || got.Pause != nil {
		t.Fatalf("R5 should queue for the pool, got %s %s, pause %+v", got.State, got.Reason, got.Pause)
	}
}

func TestMemoryExpected2091RetryCapsOnBothResultPaths(t *testing.T) {
	for _, releaseOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("release-only=%t", releaseOnly), func(t *testing.T) {
			q, clock := fixture(t)
			m := r5Machine(96)
			j := submit(t, q, cpuSpec(1, 60))
			first := r5Start(t, q, r5Check(t, q, m, r5RAM(96, 96)), j.ID)
			r5Stop(t, q, clock, m, first, r5RAM(96, 16.4), 60, 79.6, releaseOnly)
			second := r5Start(t, q, r5Check(t, q, m, r5RAM(96, 96)), j.ID)
			if second.Attempt != 2 || second.Memory.RAM.ChargeGB != 96 || second.Memory.RAM.Alone {
				t.Fatalf("expected numeric whole-pool retry: %+v", second.Memory)
			}
			r5Stop(t, q, clock, m, second, r5RAM(96, -1), 96, 97, releaseOnly)
			if next := assignedJob(r5Check(t, q, m, r5RAM(96, 96))); next != nil {
				t.Fatalf("third offer: %+v", next.Memory)
			}
			hold := r5Hold(t, q, j.ID)
			if hold == nil || hold.Kind != "memory_capacity" || !strings.Contains(hold.Detail, "97.000") || !strings.Contains(hold.Detail, "reach 96.000") {
				t.Fatalf("capacity wait = %+v", hold)
			}
		})
	}
}

func TestMemoryExpectedWindowAnd2086(t *testing.T) {
	q, clock := fixture(t)
	m := r5Machine(96)
	s := cpuSpec(1, 46.95)
	run := func(peak float64, complete bool) {
		clock.Advance(time.Second)
		j := submit(t, q, s)
		a := r5Start(t, q, r5Check(t, q, m, r5RAM(96, 96)), j.ID)
		r5Clean(t, q, a, memoryRunPool{PeakGB: peak, Complete: complete}, memoryRunPool{})
	}
	run(20.99, true)
	for range 19 {
		run(20.62, true)
	}
	run(70, false)
	key := pooledEvidenceKey(&Job{Spec: s}, &m)
	e := q.currentMemoryEvidence()
	if got := e.expected(key, 46.95, 96, true, false); got.ChargeGB != 20.99 || got.Evidence.Runs != 20 {
		t.Fatalf("#2086 max of last 20 clean runs: %+v", got)
	}
	j := submit(t, q, s)
	a := r5Start(t, q, r5Check(t, q, m, r5RAM(96, 96)), j.ID)
	if a.Memory.RAM.TypicalGB != 20.99 || a.Memory.RAM.ChargeGB != 20.99 {
		t.Fatalf("#2086 assignment: %+v", a.Memory.RAM)
	}
	r := result(a, true)
	r.MemoryPeak = &AttemptMemoryPeak{CombinedHostGB: shadowGB(47), HostGB: shadowGB(20.62),
		Learned: &memoryRunPeaks{RAM: memoryRunPool{PeakGB: 20.62, Complete: true}}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	predictions := efficiencyPredictions(job(t, q, a.ID))
	if len(predictions) != 1 || predictions[0].AcceptedHostPeakGB == nil || *predictions[0].AcceptedHostPeakGB != 20.62 {
		t.Fatalf("efficiency used a different counting rule: %+v", predictions)
	}
	if got := q.currentMemoryEvidence().expected(key, 46.95, 96, true, false); got.ChargeGB != 20.62 {
		t.Fatalf("old peak did not expire: %+v", got)
	}
}

func TestMemoryExpectedReachAndKernelKill(t *testing.T) {
	q, clock := fixture(t)
	m := r5Machine(96)
	m.HostAvailableMemoryGB = 87
	p := r5RAM(96, 87)
	p.KeepGB, p.OwnersGB = 4, 9
	j := submit(t, q, cpuSpec(1, 60))
	first := r5Start(t, q, r5Check(t, q, m, p), j.ID)
	r := result(first, false)
	r.OutOfMemory = true
	r.MemoryPeak = &AttemptMemoryPeak{Learned: &memoryRunPeaks{RAM: memoryRunPool{PeakGB: 59}}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	second := r5Start(t, q, r5Check(t, q, m, p), j.ID)
	if second.Memory.RAM.ChargeGB != 87 {
		t.Fatalf("100 GiB physical host with 9 GiB owners and 4 GiB keep: %+v", second.Memory)
	}
	r5Clean(t, q, second, memoryRunPool{PeakGB: 61, Complete: true}, memoryRunPool{})
	j = submit(t, q, cpuSpec(1, 60))
	third := r5Start(t, q, r5Check(t, q, m, p), j.ID)
	if third.Memory.RAM.ChargeGB != 61 {
		t.Fatalf("clean run did not clear the raise: %+v", third.Memory.RAM)
	}
	r5Stop(t, q, clock, m, third, r5RAM(96, -1), 61, 88, false)
	if next := assignedJob(r5Check(t, q, m, p)); next != nil {
		t.Fatalf("peak above RAM reach was reoffered at %.3f", next.Memory.RAM.ChargeGB)
	}
	hold := r5Hold(t, q, j.ID)
	if hold == nil || hold.Kind != "memory_capacity" || !strings.Contains(hold.Detail, "88.000") || !strings.Contains(hold.Detail, "reach 87.000") {
		t.Fatalf("capacity wait = %+v", hold)
	}
}

func TestMemoryExpectedCardReach(t *testing.T) {
	q, clock := fixture(t)
	m := r5Machine(87)
	m.Devices = m3Machine().Devices[:1]
	m.Devices[0].TotalMemoryGB, m.Devices[0].BudgetGB, m.Devices[0].FreeMemoryGB = 24, 24, 24
	s := spec("experiment")
	s.Needs.HostMemoryGB = 2
	s.Needs.Platform = Platform{Required: []string{"fake"}}
	p := MemoryShadowPool{Name: "card/0", CapacityGB: 24, FreeGB: shadowGB(24)}
	j := submit(t, q, s)
	first := r5Start(t, q, r5Check(t, q, m, p), j.ID)
	if first.Memory.Card.ChargeGB != 24 {
		t.Fatalf("cold card E: %+v", first.Memory.Card)
	}
	r5Clean(t, q, first, memoryRunPool{PeakGB: 1, Complete: true}, memoryRunPool{PeakGB: 12, Complete: true})
	j = submit(t, q, s)
	first = r5Start(t, q, r5Check(t, q, m, p), j.ID)
	if first.Memory.Card.ChargeGB != 12 {
		t.Fatalf("clean card E: %+v", first.Memory.Card)
	}
	stopping := p
	stopping.FreeGB = shadowGB(2)
	r5Stop(t, q, clock, m, first, stopping, 12, 22, false)
	second := r5Start(t, q, r5Check(t, q, m, p), j.ID)
	if second.Memory.Card.ChargeGB != 24 || second.Memory.RAM.ChargeGB != 1 {
		t.Fatalf("RAM reach incorrectly affected card raise, or card stop affected RAM: %+v", second.Memory)
	}
	stopping.FreeGB = shadowGB(-1)
	r5Stop(t, q, clock, m, second, stopping, 24, 25, true)
	if next := assignedJob(r5Check(t, q, m, p)); next != nil {
		t.Fatalf("card was offered again: %+v", next.Memory)
	}
	hold := r5Hold(t, q, j.ID)
	if hold == nil || hold.Kind != "memory_capacity" || !strings.Contains(hold.Detail, "card needs") || !strings.Contains(hold.Detail, "reach 24.000") {
		t.Fatalf("card capacity wait = %+v", hold)
	}
}

func TestMemoryExpectedOwnersBurst(t *testing.T) {
	q, clock := fixture(t)
	m := r5Machine(96)
	j := submit(t, q, cpuSpec(1, 80))
	sample := r5RAM(96, 86)
	sample.OwnersGB = 10
	first := r5Start(t, q, r5Check(t, q, m, sample), j.ID)
	burst := r5RAM(96, 6)
	burst.OwnersGB, burst.OwnersForecastGB = 10, 17
	r5Stop(t, q, clock, m, first, burst, 80, 80, false)
	if next := assignedJob(r5Check(t, q, m, sample)); next != nil {
		t.Fatal("readmitted into a recurring owners' shortage")
	}
	hold := r5Hold(t, q, j.ID)
	if hold == nil || hold.Kind != "memory_capacity" || !strings.Contains(hold.Detail, "owners' peak 17.000 GiB, reach 79.000") {
		t.Fatalf("owners capacity wait = %+v", hold)
	}
	other := submit(t, q, cpuSpec(1, 70))
	smaller := r5Start(t, q, r5Check(t, q, m, sample), other.ID)
	// 96 - owners' peak 17 - smaller attempt 70 stays above keep.
	peak := r5RAM(96, 9)
	peak.OwnersGB, peak.OwnersForecastGB = 17, 17
	peak.ForecastFreeGB = shadowGB(9)
	peak.Attempts = []MemoryShadowAttempt{{ID: smaller.ID, Attempt: smaller.Attempt, ExpectedGB: 70, HighWaterGB: 70, FootprintGB: shadowGB(70)}}
	r5Check(t, q, m, peak, smaller)
	if got := job(t, q, smaller.ID); got.State != "running" {
		t.Fatalf("owners hold changed the R4 stop threshold: %s", got.Reason)
	}
	finish(t, q, smaller, true)

	// Check the lapse transitions on the injected clock with consecutive
	// below-X observations. Periodic report gap handling is separate.
	persisted, err := readMemoryEvidence(q.db)
	if err != nil {
		t.Fatal(err)
	}
	h := persisted.owners[ownersKey(&m, "ram")]
	if h != q.currentMemoryEvidence().owners[ownersKey(&m, "ram")] {
		t.Fatalf("owners hold was lost from the result transaction: %+v", h)
	}
	h = h.observe(clock.Now(), 17, true)
	for _, hours := range []time.Duration{24, 48, 96} {
		h = h.observe(clock.Now(), 10, true)
		window := hours * time.Hour
		if h.PeakGB != 17 || h.Window != window {
			t.Fatalf("before lapse: %+v, want %s", h, window)
		}
		clock.Advance(window - time.Second)
		if got := h.observe(clock.Now(), 10, true); got.PeakGB != 17 {
			t.Fatalf("lapsed early: %+v", got)
		}
		clock.Advance(time.Second)
		h = h.observe(clock.Now(), 10, true)
		if h.PeakGB != 0 || !h.LapsedAt.Equal(clock.Now()) {
			t.Fatalf("did not lapse: %+v", h)
		}
		if hours == 96 {
			clock.Advance(window)
		}
		h = h.stopped(clock.Now(), &memoryStopSample{OwnersGB: 10, OwnersPeakGB: 17})
	}
	if h.Window != firstOwnersWindow {
		t.Fatalf("lapse back-off did not reset: %+v", h)
	}

	// An ordinary check-in clears the persisted hold and offers the same job.
	// A six-hour fake cadence crosses a day without thousands of heartbeats.
	check := func() Reply {
		p := sample
		p.sum()
		m.MemoryShadow = &MemoryShadowReport{Pools: []MemoryShadowPool{p}, stopForecast: true}
		reply, err := q.Checkin(Checkin{Machine: m, period: 6 * time.Hour, AttemptProtocol: keyedAttemptProtocol})
		if err != nil {
			t.Fatal(err)
		}
		return reply
	}
	if got := assignedJob(check()); got != nil {
		t.Fatalf("job offered before a fresh window: %s", got.ID)
	}
	refused := errors.New("fixture transaction abort")
	if err := q.update(func() error {
		q.touchMachine(m.ID).memoryShadowAt = time.Time{}
		return refused
	}); !errors.Is(err, refused) {
		t.Fatal(err)
	}
	for range 3 {
		clock.Advance(6 * time.Hour)
		if got := assignedJob(check()); got != nil {
			t.Fatalf("job offered before 24 hours: %s", got.ID)
		}
	}
	clock.Advance(6 * time.Hour)
	retry := r5Start(t, q, check(), j.ID)
	if retry.Memory.RAM.ChargeGB != 80 {
		t.Fatalf("owners hold changed expected peak: %+v", retry.Memory.RAM)
	}
}

func TestMemoryExpectedOverECoTenant(t *testing.T) {
	q, _ := fixture(t)
	for _, used := range []float64{70, 75} {
		p := r5RAM(96, 96-used)
		p.Attempts = []MemoryShadowAttempt{{FootprintGB: shadowGB(used), HighWaterGB: 75, ExpectedGB: 60}}
		p.sum()
		if *p.SlackGB != 21 || *p.Attempts[0].CommitmentGB != 75 {
			t.Fatalf("co-tenant commitment moved with footprint: %+v", p)
		}
	}
	s := cpuSpec(1, 60)
	m := r5Machine(96)
	j := submit(t, q, s)
	a := r5Start(t, q, r5Check(t, q, m, r5RAM(96, 96)), j.ID)
	r5Clean(t, q, a, memoryRunPool{PeakGB: 75, Complete: true}, memoryRunPool{})
	j = submit(t, q, s)
	a = r5Start(t, q, r5Check(t, q, m, r5RAM(96, 96)), j.ID)
	if a.Memory.RAM.ChargeGB != 75 {
		t.Fatalf("co-tenant clean E = %g", a.Memory.RAM.ChargeGB)
	}
	p := r5RAM(96, 6)
	p.Attempts = []MemoryShadowAttempt{{ExpectedGB: a.Memory.RAM.ChargeGB, FootprintGB: shadowGB(70), HighWaterGB: 75, Growth: 1},
		{ExpectedGB: 20, FootprintGB: shadowGB(20), HighWaterGB: 20}}
	p.sum()
	previous := p
	p.forecast(&previous, 1, 11)
	if p.shortage() != "" || math.Abs(*p.ForecastFreeGB-1) > 0.001 {
		t.Fatalf("clean E did not keep the victim running: %+v", p)
	}
}

func TestMemoryExpectedDRMUnitsAndPCI(t *testing.T) {
	const sample = "drm-pdev:\t00000000:03:00.0\ndrm-client-id:\t7\ndrm-resident-vram0:\t65116 KiB\ndrm-resident-vram1:\t1 GiB\ndrm-resident-system:\t1048576 B\n"
	key, card, host, err := drmAttemptMemory([]byte(sample), "03:00.0")
	if err != nil || key != "0000:03:00.0/7" || card != 1+65116.0/(1<<20) || host != 1.0/1024 {
		t.Fatalf("fdinfo %q: card %g host %g err %v", key, card, host, err)
	}
	if _, _, _, err := drmAttemptMemory([]byte(strings.ReplaceAll(sample, "KiB", "unknown")), "0000:07:00.0"); err != errAttemptMemoryPending {
		t.Fatalf("foreign card parser failure was attributed to selected card: %v", err)
	}
}

func TestMemoryExpectedFirstRunFallbacks(t *testing.T) {
	e := newMemoryEvidence()
	first := memoryEvidenceKey{Platform: "linux/cpu/host", Pool: "ram", Family: "a", Exact: "1"}
	if got := e.expected(first, 0, 96, true, false); got.ChargeGB != 96 || got.Evidence.Level != "unknown" {
		t.Fatalf("first platform run = %+v", got)
	}
	e.observeExpected(first, 7)
	second := memoryEvidenceKey{Platform: first.Platform, Pool: "ram", Family: "b", Exact: "2"}
	e.observeExpected(second, 5)
	e.observeExpected(second, 11) // Repeat is excluded from the first-run median.
	newFamily := memoryEvidenceKey{Platform: first.Platform, Pool: "ram", Family: "c", Exact: "3"}
	for _, tc := range []struct {
		key        memoryEvidenceKey
		hint, peak float64
		level      string
	}{
		{first, 20, 7, "exact"},
		{memoryEvidenceKey{Platform: first.Platform, Pool: "ram", Family: "b", Exact: "new"}, 20, 11, "family"},
		{newFamily, 8, 8, "hint"},
		{newFamily, 0, 6, "platform"},
		{memoryEvidenceKey{Platform: "darwin/cpu/host", Pool: "ram", Family: "b", Exact: "2"}, 0, 96, "unknown"},
		{memoryEvidenceKey{Platform: first.Platform, Pool: "card", Family: "a", Exact: "1"}, 8, 24, "unknown"},
	} {
		reach := 96.0
		if tc.key.Pool == "card" {
			reach = 24
		}
		if got := e.expected(tc.key, tc.hint, reach, true, false); got.ChargeGB != tc.peak || got.Evidence.Level != tc.level {
			t.Errorf("%+v with hint %.2f: got %+v, want %.2f from %s", tc.key, tc.hint, got, tc.peak, tc.level)
		}
	}
}

func TestMemoryExpectedXPUCardSharing(t *testing.T) {
	q, clock := fixture(t)
	const budget, peak = 30.046, 9.506
	m := sharedDiscreteMachine("xpu")
	d := &m.Devices[0]
	d.TotalMemoryGB, d.BudgetGB, d.FreeMemoryGB = budget, budget, budget
	pool := MemoryShadowPool{Name: "card/0", CapacityGB: budget, FreeGB: shadowGB(budget)}
	s := sharedSpec("experiment", budget)
	s.Needs.Platform = Platform{Required: []string{"xpu"}}
	check := func(runs ...*Job) Reply {
		clock.Advance(time.Second)
		return r5Check(t, q, m, pool, runs...)
	}
	ram := memoryRunPool{PeakGB: 1, Complete: true}
	for _, complete := range []bool{false, true} {
		cold := submit(t, q, s)
		run := r5Start(t, q, check(), cold.ID)
		if card := run.Memory.Card; card == nil || !card.Alone || card.ChargeGB != budget || card.Evidence.Runs != 0 {
			t.Fatalf("card estimate without a complete prior run: %+v", card)
		}
		r5Clean(t, q, run, ram, memoryRunPool{PeakGB: peak, Complete: complete})
	}

	var running []*Job
	for range 3 {
		j := submit(t, q, s)
		run := r5Start(t, q, check(running...), j.ID)
		card := run.Memory.Card
		if !slices.Equal(run.AllocatedDevices, []string{"0"}) || card == nil ||
			!card.Learned || card.Alone || card.ChargeGB != peak || card.Evidence.Runs != 1 {
			t.Fatalf("co-tenant %d: cards %v, estimate %+v", len(running)+1, run.AllocatedDevices, card)
		}
		running = append(running, run)
	}
	exclusive := s
	exclusive.Advanced = &Advanced{ExclusiveDevice: true}
	wantsAlone := submit(t, q, exclusive)
	for _, run := range running[1:] {
		finish(t, q, run, true)
	}
	if got := assignedJob(check(running[0])); got != nil {
		t.Fatalf("exclusive job joined a held card: %s", got.ID)
	}
	finish(t, q, running[0], true)
	alone := r5Start(t, q, check(), wantsAlone.ID)
	if !alone.Spec.ExclusiveDevice || alone.Memory.Card.Evidence.Runs != 1 {
		t.Fatalf("exclusive offer did not use the learned workload: %+v", alone.Memory)
	}
	peer := submit(t, q, s)
	if got := assignedJob(check(alone)); got != nil {
		t.Fatalf("job joined a card held by an exclusive job: %s", got.ID)
	}
	finish(t, q, alone, true)
	r5Start(t, q, check(), peer.ID)
}

func r5XPUCardWorkload(t *testing.T, budget float64, unit string) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	m := sharedDiscreteMachine("xpu")
	m.Devices[0].TotalMemoryGB, m.Devices[0].BudgetGB, m.Devices[0].FreeMemoryGB = budget, budget, budget
	e := compositionExtension(t, false)
	v := &e.Variants[0]
	v.Target = machineTarget(&m)
	v.Needs = ResourceNeeds{CPUCores: 4, MemoryGB: 7.22, HostMemoryGB: 1}
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "recording", Unit: unit, Units: 1}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m.Devices[0].ExtensionFingerprint = "synthetic-xpu"
	target := m.Devices[0].machine(m)
	report := newExtensionReport(e, &target, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "fixture-check"
	m.Devices[0].ExtensionReports = []ExtensionReport{report}

	s := Spec{Name: "measure", Project: "test", Class: "experiment", Extension: extensionKey(e),
		Workload: &Workload{InputShape: "recording", Units: 1},
		FanOut:   &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "item"}}}}
	return q, clock, m, s
}

func r5StartCardPiece(t *testing.T, q *Queue, reply Reply, parent *Job, charge float64, alone bool) *Job {
	t.Helper()
	run := assignedJob(reply)
	if run == nil || run.Batch == nil || run.Batch.Parent != parent.ID {
		t.Fatalf("piece not assigned: %+v; parent hold: %+v", run, job(t, q, parent.ID).Hold)
	}
	if card := run.Memory.Card; card == nil || math.Abs(card.ChargeGB-charge) > 1e-9 || card.Alone != alone {
		t.Fatalf("piece card estimate: %+v, want %g GiB, alone=%t", card, charge, alone)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestMemoryExpectedXPUCardRequiresMeasuredInputs(t *testing.T) {
	const budget, shortPeak, longPeak, peerPeak = 30.046, 7.416, 8.758, 22
	q, clock, m, s := r5XPUCardWorkload(t, budget, "records")
	pool := MemoryShadowPool{Name: "card/0", CapacityGB: budget, FreeGB: shadowGB(budget)}
	short, long, peer := s, s, s
	short.Files = FileManifest{"input": putPieceTestFile(t, q, "short input")}
	long.Files = FileManifest{"input": putPieceTestFile(t, q, strings.Repeat("long input ", 100))}
	peer.Files = short.Files
	peer.Args = []string{"peer"}
	check := func(runs ...*Job) Reply {
		clock.Advance(time.Second)
		return r5Check(t, q, m, pool, runs...)
	}
	start := func(parent *Job, charge float64, alone bool, running ...*Job) *Job {
		t.Helper()
		return r5StartCardPiece(t, q, check(running...), parent, charge, alone)
	}
	clean := func(run *Job, peak float64) {
		t.Helper()
		r5Clean(t, q, run, memoryRunPool{}, memoryRunPool{PeakGB: peak, Complete: true})
	}
	clean(start(submit(t, q, short), budget, true), shortPeak)
	clean(start(submit(t, q, peer), budget, true), peerPeak)
	holder := start(submit(t, q, peer), peerPeak, false)
	demand := submit(t, q, long)
	// With both inputs declared as one record, the previous E left enough
	// nominal room for the 22 GiB tenant. The real peak exceeds that room.
	if run := assignedJob(check(holder)); run != nil {
		t.Fatalf("unmeasured longer input joined a card with %.3f GiB room", budget-peerPeak)
	}
	if hold := r5Hold(t, q, demand.ID); hold == nil || !strings.Contains(hold.Detail, "requires an exclusive first run to measure card memory") {
		t.Fatalf("unmeasured input wait: %+v", hold)
	}
	finish(t, q, holder, true)

	first := start(demand, budget, true)
	waitingPeer := submit(t, q, peer)
	if run := assignedJob(check(first)); run != nil {
		t.Fatal("a learned peer joined while the longer input was measuring")
	}
	if hold := r5Hold(t, q, waitingPeer.ID); hold == nil || !strings.Contains(hold.Detail, first.ID) ||
		!strings.Contains(hold.Detail, "exclusive while measuring card memory") {
		t.Fatalf("exclusive card owner missing from partner hold: %+v", hold)
	}
	clock.Advance(345 * time.Second)
	clean(first, longPeak)
	evidence, err := readMemoryEvidence(q.db)
	if err != nil {
		t.Fatal(err)
	}
	target := m.Devices[0].machine(m)
	key, ref := pooledEvidenceReference(first, &target)
	key.Pool = "card"
	if card := evidence.expectedXPUCard(first, key, ref, budget, budget, true); card.ChargeGB != longPeak || card.Alone {
		t.Fatalf("complete longer run did not persist the larger shareable estimate: %+v", card)
	}
	// Peaks written before input coverage was recorded still load, but cannot
	// qualify an input. A clean input sample also ages out with its peak.
	for range memoryPeakWindow {
		evidence.observeExpected(key, shortPeak)
	}
	if card := evidence.expectedXPUCard(first, key, ref, budget, budget, true); card.ChargeGB != budget || !card.Alone {
		t.Fatalf("expired input coverage admitted sharing: %+v", card)
	}
	holder = start(waitingPeer, peerPeak, false)
	repeat := submit(t, q, long)
	if run := assignedJob(check(holder)); run != nil {
		t.Fatal("learned longer input was admitted beside a peer exceeding the card budget")
	}
	finish(t, q, holder, true)

	second := start(repeat, longPeak, false)
	start(submit(t, q, long), longPeak, false, second)
}

func TestMemoryExpectedXPUCardSharesNewInputsWithinMeasuredSeconds(t *testing.T) {
	const budget, middlePeak, shortLow, shortHigh = 30.046, 6.766, 4.570, 4.960
	const middleCharge = middlePeak + shortHigh - shortLow
	q, clock, m, spec := r5XPUCardWorkload(t, budget, "audio-seconds")
	m.CapabilityProfile.LogicalCPUs = 16
	pool := MemoryShadowPool{Name: "card/0", CapacityGB: budget, FreeGB: shadowGB(budget)}
	check := func(running ...*Job) Reply {
		clock.Advance(time.Second)
		return r5Check(t, q, m, pool, running...)
	}
	newInput := func(seconds float64) *Job {
		s := spec
		s.Workload = &Workload{InputShape: spec.Workload.InputShape, Units: seconds}
		s.Files = FileManifest{"input": putPieceTestFile(t, q, fmt.Sprintf("distinct input %g at %s", seconds, clock.Now()))}
		return submit(t, q, s)
	}
	for _, sample := range []struct{ seconds, peak float64 }{
		{120, shortLow}, {120, shortHigh}, {1944.661, 6.540}, {2210.304, middlePeak}, {2618.2, 7.105}, {4550.688, 8.758}, {6151.224, 10.121},
	} {
		parent := newInput(sample.seconds)
		run := r5StartCardPiece(t, q, check(), parent, budget, true)
		r5Clean(t, q, run, memoryRunPool{}, memoryRunPool{PeakGB: sample.peak, Complete: true})
	}
	q = reopenCancellationQueue(t, q, clock)
	fakeCheckpointAvailability(t, q, true)

	// Three distinct recordings in the measured interval can overlap. The
	// 6.766 GiB upper sample plus the 0.390 GiB short-run spread covers
	// the replay's 6.600 GiB interior peak.
	var running []*Job
	for _, seconds := range []float64{2000, 2050, 2100} {
		parent := newInput(seconds)
		run := r5StartCardPiece(t, q, check(running...), parent, middleCharge, false)
		if card := run.Memory.Card; card.ChargeGB < 6.600 || card.Evidence.Level != "size_band" || !slices.Equal(run.AllocatedDevices, []string{"0"}) {
			t.Fatalf("unseen in-range input was not safely sized for this card: %+v", card)
		}
		running = append(running, run)
	}
	uncovered := newInput(6600)
	if run := assignedJob(check(running...)); run != nil {
		t.Fatal("out-of-range input joined despite having no upper measurement")
	}
	if hold := r5Hold(t, q, uncovered.ID); hold == nil || !strings.Contains(hold.Detail, "requires an exclusive first run") {
		t.Fatalf("missing out-of-range card hold: %+v", hold)
	}
	for _, run := range running {
		r5Clean(t, q, run, memoryRunPool{}, memoryRunPool{PeakGB: 6.600, Complete: true})
	}
	r5StartCardPiece(t, q, check(), uncovered, budget, true)
}

func TestMemoryExpectedXPUCardStaleOwnersForecast(t *testing.T) {
	q, clock := fixture(t)
	const (
		capacity = 30.296092987060547
		keep     = 0.25
		budget   = capacity - keep
		peak     = 28.457469940185547
		resident = 1.5775222778320312
		rawFree  = 3.7036705017089844
		owners   = capacity - rawFree - resident
		forecast = 170.35562166409454
		want     = 8.215
	)
	m := sharedDiscreteMachine("xpu")
	d := &m.Devices[0]
	d.ID, d.TotalMemoryGB, d.BudgetGB, d.FreeMemoryGB = "1", capacity, budget, budget
	d.ReserveMemoryGB, d.rawFreeGB = keep, shadowGB(rawFree)
	card := MemoryShadowPool{Name: "card/1", CapacityGB: capacity, KeepGB: keep, FreeGB: shadowGB(budget)}
	check := func(pool MemoryShadowPool, runs ...*Job) Reply {
		clock.Advance(time.Second)
		return r5Check(t, q, m, pool, runs...)
	}
	s := sharedSpec("experiment", budget)
	s.Needs.Platform = Platform{Required: []string{"xpu"}}
	train := submit(t, q, s)
	r5Clean(t, q, r5Start(t, q, check(card), train.ID),
		memoryRunPool{PeakGB: 1, Complete: true}, memoryRunPool{PeakGB: want, Complete: true})

	// A different workload had no complete samples and occupied the whole card.
	cold := s
	cold.Name = "whole-card-workload"
	coldJob := submit(t, q, cold)
	run := r5Start(t, q, check(card), coldJob.ID)
	if run.Memory.Card.ChargeGB != budget || !run.Memory.Card.Alone {
		t.Fatalf("cold XPU allocation: %+v", run.Memory.Card)
	}
	skewed := card
	skewed.FreeGB = shadowGB(rawFree - keep)
	skewed.Attempts = []MemoryShadowAttempt{{ID: run.ID, Attempt: run.Attempt, ExpectedGB: budget,
		FootprintGB: shadowGB(resident), HighWaterGB: peak}}
	// Replay the retained report from an adjacent worker. The old residual
	// forecast used a falling process reading against a still-low driver free.
	skewed.OwnersGB, skewed.OwnersForecastGB = owners, forecast
	skewed.ForecastFreeGB = shadowGB(rawFree - keep - (forecast - owners))
	// Replay a falling per-process reading against the same driver reading
	// through the current worker. Adjacent queues receive no residual.
	worker := Worker{Machine: m, Interval: time.Second}
	var shadow workerMemoryShadow
	for _, used := range []float64{peak, resident} {
		reading := &shadowAttemptReading{Card: shadowMemoryReading{At: clock.Now(), GB: used, PeakGB: peak}}
		report := AttemptReport{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Session: run.Session, memoryShadow: reading}
		shadow.sample(worker, &m, 32, []Job{*run}, []AttemptReport{report}, clock.Now())
		clock.Advance(time.Second)
	}
	current := shadow.previous.Pools[1]
	if current.ForecastFreeGB == nil || *current.ForecastFreeGB != rawFree-keep ||
		current.FootprintGB != resident || current.OwnersGB != 0 || current.OwnersForecastGB != 0 {
		t.Fatalf("worker forecast with no attempt growth: %+v", current)
	}

	check(skewed, run)
	if got := job(t, q, run.ID); got.State != "running" {
		t.Errorf("unattributed forecast stopped an in-budget card owner: %s", got.State)
	}

	waitingSpec := s
	waitingSpec.Priority, waitingSpec.Class = "production", "production"
	waiting := submit(t, q, waitingSpec)
	// A genuine below-keep reading still stops and requeues the exact attempt.
	// Its released card is available even with the old persisted projection.
	r5Stop(t, q, clock, m, run, MemoryShadowPool{Name: card.Name, CapacityGB: capacity,
		KeepGB: keep, FreeGB: shadowGB(-0.125), OwnersGB: owners, OwnersForecastGB: forecast},
		budget, peak, true)
	if h := q.currentMemoryEvidence().owners[ownersKey(&m, card.Name)]; h.PeakGB != 0 {
		t.Errorf("new card pressure stop persisted an unattributed peak: %+v", h)
	}
	if err := q.update(func() error {
		q.stagedMemoryEvidence().writeOwners(ownersKey(&m, card.Name),
			memoryOwnersHold{PeakGB: forecast, Window: firstOwnersWindow})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The waiting job is ahead of the requeued owner. The old attempt and
	// the requeue owe zero.
	a := r5Start(t, q, check(card), waiting.ID)
	if a.Memory.Card.ChargeGB != want {
		t.Fatalf("card charge after owner release: %+v", a.Memory.Card)
	}
	running := []*Job{a}
	for range 2 {
		peer := submit(t, q, waitingSpec)
		running = append(running, r5Start(t, q, check(card, running...), peer.ID))
	}
	over := submit(t, q, waitingSpec)
	if got := assignedJob(check(card, running...)); got != nil {
		t.Fatalf("offered %s (%s) over the card budget", got.ID, over.ID)
	}
	target := q.machineTargets(q.s.Machines[m.ID])[1]
	if room := budget - 3*want; math.Abs(target.FreeMemoryGB-room) > 1e-9 {
		t.Fatalf("card room %.3f, want %.3f after three live card peaks", target.FreeMemoryGB, room)
	}
}

func TestMemoryExpected2240XPUHighWater(t *testing.T) {
	for _, tc := range []struct {
		name    string
		success bool
		learned bool
	}{
		{name: "adjacent worker successful cells", success: true},
		{name: "application failures with incomplete counters", learned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			const allowance = 44.83
			m := m3Machine()
			m.GPU, m.CapabilityProfile.LogicalCPUs = "xpu", 16
			m.CapabilityProfile.Accelerators[0].Type = "xpu"
			m.HostMemoryBudgetGB, m.HostMemoryBudgetMeasured, m.HostAvailableMemoryGB = allowance, true, 46
			for i := range m.Devices {
				m.Devices[i].Platform = "xpu"
			}
			cell := func(arg, card string) Spec {
				s := sharedSpec("experiment", 0)
				s.Run.Command = append(s.Run.Command, arg)
				s.Needs.CPUCores, s.Needs.HostMemoryGB, s.Needs.Memory = 2, 0, "auto"
				s.Needs.Platform = Platform{Required: []string{"xpu"}}
				s.Needs.GPU = &GPUSelection{Devices: []string{card}}
				return s
			}
			free := r5RAM(allowance, 46)
			history := state{Machines: map[string]*Machine{m.ID: &m}, Jobs: map[string]*Job{}}
			for i, peak := range []float64{7.8, 7.2, 7.4} {
				s := cell(fmt.Sprint(i%2), "0")
				j := submit(t, q, s)
				a := r5Start(t, q, r5Check(t, q, m, free), j.ID)
				if i == 0 && a.Memory.RAM.ChargeGB != allowance {
					t.Fatalf("a genuinely cold run reserved %.2f GiB", a.Memory.RAM.ChargeGB)
				}
				clock.Advance(time.Second)
				r := result(a, tc.success)
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: shadowGB(peak), CombinedHostGB: shadowGB(38)}
				if tc.learned {
					r.MemoryPeak.Learned = &memoryRunPeaks{RAM: memoryRunPool{PeakGB: peak}}
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				state := "failed"
				if tc.success {
					state = "succeeded"
				}
				requireState(t, q, j.ID, state)
				history.Jobs[j.ID] = job(t, q, j.ID)
			}

			// Simulate opening a database with retained attempt facts and no
			// R6 evidence rows, then reopen it without the underlying jobs.
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "evidence.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, snapshot := range []state{history, {}} {
				e, err := loadMemoryEvidence(db, snapshot)
				if err != nil {
					t.Fatal(err)
				}
				target := m.Devices[0].machine(m)
				for _, variant := range []struct{ arg, level string }{{"0", "exact"}, {"new sibling", "family"}} {
					key := pooledEvidenceKey(&Job{Spec: cell(variant.arg, "0")}, &target)
					got := e.expected(key, 0, allowance, true, true)
					if got.ChargeGB != 7.8 || got.Evidence.Level != variant.level || got.Evidence.Runs != 0 {
						t.Fatalf("retained %s evidence did not survive startup: %+v", variant.level, got)
					}
				}
			}

			m.HostAvailableMemoryGB = 43
			free = r5RAM(allowance, 43)
			one := submit(t, q, cell("0", "0"))
			first := r5Start(t, q, r5Check(t, q, m, free), one.ID)
			if first.Memory.RAM.ChargeGB != 7.8 || first.Memory.RAM.Evidence.Level != "exact" {
				t.Fatalf("repeat reserved beyond the exact high-water: %+v", first.Memory.RAM)
			}
			two := submit(t, q, cell("new sibling", "1"))
			second := r5Start(t, q, r5Check(t, q, m, free, first), two.ID)
			for i, a := range []*Job{first, second} {
				if !slices.Equal(a.AllocatedDevices, []string{fmt.Sprint(i)}) ||
					!a.Memory.RAM.Learned || a.Memory.RAM.ChargeGB != 7.8 || a.Memory.RAM.Evidence.Runs != 0 ||
					!a.Memory.Card.Alone {
					t.Fatalf("XPU offer %d: devices=%v memory=%+v", i, a.AllocatedDevices, a.Memory)
				}
			}
			cpu := submit(t, q, cpuSpec(2, 8))
			third := r5Start(t, q, r5Check(t, q, m, free, first, second), cpu.ID)
			if third.Memory.RAM.ChargeGB != 8 || len(third.AllocatedDevices) != 0 {
				t.Fatalf("CPU offer: devices=%v memory=%+v", third.AllocatedDevices, third.Memory)
			}
			w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) {
				return m.HostAvailableMemoryGB, nil
			}}
			if err := localAttemptAdmission(w, []Job{*first}, *second, nil); err != nil {
				t.Fatal(err)
			}
			if err := localAttemptAdmission(w, []Job{*first, *second}, *third, nil); err != nil {
				t.Fatal(err)
			}

			finish(t, q, second, true)
			finish(t, q, third, true)
			r5Stop(t, q, clock, m, first, r5RAM(allowance, 34), 7.8, 9, false)
			retry := r5Start(t, q, r5Check(t, q, m, free), one.ID)
			if got := retry.Memory.RAM; got.ChargeGB != 18 || got.Evidence.Level != "raised" {
				t.Fatalf("over-E stop did not double the measured peak: %+v", got)
			}
			r5Clean(t, q, retry, memoryRunPool{PeakGB: 8, Complete: true}, memoryRunPool{})
			repeat := submit(t, q, cell("0", "0"))
			clean := r5Start(t, q, r5Check(t, q, m, free), repeat.ID)
			if got := clean.Memory.RAM; got.ChargeGB != 8 || got.Evidence.Runs != 1 {
				t.Fatalf("qualified run did not restore the clean window: %+v", got)
			}
		})
	}
}

func TestMemoryExpected2240RetainedXPUExtension(t *testing.T) {
	q, clock := fixture(t)
	const allowance, card, room = 47.193, 30.046, 14.81
	m := r5Machine(allowance)
	m.HostAvailableMemoryGB, m.FreeHostMemoryGB = room, room
	m.Devices = []MachineDevice{{
		ID: "0", Platform: "xpu", Identity: "test-card", ExtensionFingerprint: "test-xpu",
		TotalMemoryGB: card, BudgetGB: card, FreeMemoryGB: card, MemoryMeasured: true,
	}}
	ram := r5RAM(allowance, allowance)
	ram.sum()
	m.MemoryShadow = &MemoryShadowReport{Pools: []MemoryShadowPool{ram}, stopForecast: true}

	e := compositionExtension(t, false)
	v := &e.Variants[0]
	target := m.Devices[0].machine(m)
	v.Target = machineTarget(&target)
	v.Needs = ResourceNeeds{CPUCores: 2, Memory: "auto"}
	work := "extension:" + e.Name
	ref := WorkReference{InputShape: "shape", Unit: "items", Units: 1}
	v.References = map[string]WorkReference{work: ref}
	e = freezeComposition(t, e)
	spec := Spec{
		Name: "extension-work", Project: "synthetic", Class: "production", SubmissionKey: "current-item",
		Extension: extensionKey(e), ExtensionDigest: e.Digest, Needs: ResourceNeeds{Memory: "auto"},
		Workload: &Workload{InputShape: ref.InputShape, Units: 2}, Args: []string{"current"},
	}
	r := newExtensionReport(e, &target, work)
	r.Setup, r.Qualified, r.CheckID = true, true, "check-fixture"
	m.Devices[0].ExtensionReports = []ExtensionReport{r}
	target = m.Devices[0].machine(m)
	candidate := &Job{Spec: spec, Extensions: []Extension{e}, State: "queued"}

	started := clock.Now()
	history := state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{m.ID: durableMachine(&m)}}
	history.Machines[m.ID].Devices[0].ExtensionReports = nil
	for i, peak := range []float64{7.4, 7.8, 7.6} {
		id := fmt.Sprintf("prior-%d", i)
		settled := *candidate
		settled.ID, settled.Attempt, settled.State, settled.StartedAt = id, 1, "succeeded", &started
		settled.Machine, settled.Session, settled.AllocatedDevices = m.ID, "prior-worker-session", []string{"0"}
		settled.Spec.SubmissionKey, settled.Spec.Args = id, []string{id}
		settled.Spec.Needs = e.Variants[0].Needs
		settled.Extension = &ExtensionSelection{Name: e.Name, Version: e.Version, Digest: e.Digest}
		settled.Timing = &WorkTiming{Execution: Execution{ID: id, Attempt: 1}, Machine: m.ID, Session: settled.Session, Key: &ObservationKey{
			ExtensionDigest: e.Digest, Target: machineTarget(&target), Component: target.Device,
			Hardware: m.Devices[0].Identity,
		}}
		settled.MemoryAttempts = []AttemptMemoryFacts{{
			Attempt: 1, Machine: m.ID, Session: settled.Session, LearningReason: "eligible",
			MemoryPeak: &AttemptMemoryPeak{HostGB: shadowGB(peak), CombinedHostGB: shadowGB(41), DeviceGB: shadowGB(5.4)},
		}}
		history.Jobs[id] = &settled
	}
	// A historical report from a different physical card must not train the
	// restored allocation, even if its worker reused the old device number.
	replaced := *history.Jobs["prior-1"]
	replaced.ID = "replaced"
	timing := *replaced.Timing
	identity := *timing.Key
	timing.Execution.ID, identity.Hardware, timing.Key = replaced.ID, "removed-card", &identity
	replaced.Timing = &timing
	replaced.MemoryAttempts = slices.Clone(replaced.MemoryAttempts)
	replaced.MemoryAttempts[0].MemoryPeak = &AttemptMemoryPeak{HostGB: shadowGB(35)}
	history.Jobs[replaced.ID] = &replaced

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "evidence.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// This disposable fixture starts with the markers from the rollout
	// that only imported plain commands.
	if _, err := db.Exec(memoryEvidenceSchema); err != nil {
		t.Fatal(err)
	}
	var markers queueWriteBatch
	markers.add(`INSERT INTO memory_evidence(kind,key,data) VALUES ('projection','v1','true'), ('projection','xpu-ram-v1','true')`)
	writer := queueSQLiteWriter{db: db}
	if err := writer.apply(context.Background(), &markers, nil, nil); err != nil {
		t.Fatal(err)
	}
	q.memoryEvidence, err = loadMemoryEvidence(db, history)
	if err != nil {
		t.Fatal(err)
	}
	sizes := q.memoryCandidates(candidate, &target)
	if len(sizes) != 1 {
		t.Fatalf("qualified candidate count = %d", len(sizes))
	}
	resolved := memoryPools(sizes[0])
	if resolved.RAM.ChargeGB != 7.8 || resolved.RAM.Evidence.Level != "family" || resolved.RAM.Evidence.Runs != 0 ||
		resolved.Card.ChargeGB != card || !resolved.Card.Alone {
		t.Fatalf("extension sizing at %.2f GiB room: RAM=%+v card=%+v", room, resolved.RAM, resolved.Card)
	}
	needs := sizes[0].Spec.Needs
	if reason := hostFitFailure(&target, needs.MemoryGB, needs.HostMemoryGB); reason != "" {
		t.Fatalf("queue RAM check refused the extension: %s", reason)
	}
	if err := (HostRAMGate{RawGB: room}).check(&target, resolved, true); err != nil {
		t.Fatalf("extension did not fit in signed room: %v", err)
	}
}

func TestMemoryExpectedColdCardGroup(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.HostMemoryBudgetGB = 32
	s := bigSpec("experiment", 12)
	j := submit(t, q, s)
	a := r5Start(t, q, r5Check(t, q, m, r5RAM(32, 32)), j.ID)
	if len(a.AllocatedDevices) != 2 || a.Spec.Needs.MemoryGB != 12 {
		t.Fatalf("typed demand that cannot fit one cold card: devices=%v memory=%g", a.AllocatedDevices, a.Spec.Needs.MemoryGB)
	}
}

func TestMemoryExpectedColdCardModelCoexistence(t *testing.T) {
	for _, platform := range []string{"fake", "cpu"} {
		t.Run(platform, func(t *testing.T) {
			q, _ := fixture(t)
			m := m3Machine()
			m.Devices = m.Devices[:1]
			m.HostMemoryBudgetGB = 32
			s := spec("production")
			s.Needs = ResourceNeeds{HostMemoryGB: 1, Platform: Platform{Required: []string{platform}}}
			if platform == "fake" {
				s.Needs.MemoryGB = 1
			}
			j := submit(t, q, s)
			batch := r5Start(t, q, r5Check(t, q, m, r5RAM(32, 32)), j.ID)
			r5Check(t, q, m, r5RAM(32, 32), batch)
			call := callJob(&ModelCall{Model: "test", Project: s.Project, Priority: s.Priority})
			q.mu.Lock()
			card := q.machineTargets(q.s.Machines[m.ID])[1]
			fits := q.cardRoom(call, card, 1, false, true, false, &placementPlan{})
			q.mu.Unlock()
			if platform == "fake" {
				if fits || batch.Memory.Card == nil || batch.Memory.Card.ChargeGB != 8 {
					t.Fatalf("cold job must retain the whole card against a same-tier load: fits=%t memory=%+v", fits, batch.Memory)
				}
				return
			}
			if !fits || batch.Memory.Card != nil {
				t.Fatalf("CPU job must leave the card available to a same-tier load: fits=%t memory=%+v", fits, batch.Memory)
			}
		})
	}
}
