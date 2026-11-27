package myce

import (
	"fmt"
	"math"
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
	if got := e.expected(key, 46.95, 96, true); got.ChargeGB != 20.99 || got.Evidence.Runs != 20 {
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
	if got := q.currentMemoryEvidence().expected(key, 46.95, 96, true); got.ChargeGB != 20.62 {
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
	if got := e.expected(first, 0, 96, true); got.ChargeGB != 96 || got.Evidence.Level != "unknown" {
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
		if got := e.expected(tc.key, tc.hint, reach, true); got.ChargeGB != tc.peak || got.Evidence.Level != tc.level {
			t.Errorf("%+v with hint %.2f: got %+v, want %.2f from %s", tc.key, tc.hint, got, tc.peak, tc.level)
		}
	}
}

func TestMemoryExpectedRetainsXPUProbeGate(t *testing.T) {
	q, _ := fixture(t)
	m := sharedDiscreteMachine("xpu")
	d := m.Devices[0]
	m.MemoryShadow = &MemoryShadowReport{stopForecast: true}
	target := d.machine(m)
	s := sharedSpec("experiment", d.BudgetGB)
	j := &Job{Spec: s}
	key := pooledEvidenceKey(j, &target)
	key.Pool = "card"
	q.memoryEvidence.observeExpected(key, 3)
	if got := q.resolvePooledSize(j, &target).card; got.Learned || !got.Alone || got.ChargeGB != d.BudgetGB {
		t.Fatalf("ungated XPU telemetry became an estimate before 0b-4: %+v", got)
	}
	j.Attempt = 1
	j.AttemptHistory = []AttemptOutcome{{Attempt: 1, PriorityStop: &PriorityPreemption{Cause: memoryPressureCause, DevicePool: &DevicePoolStop{}}}}
	if got := q.resolvePooledSize(j, &target).pools.RAM; !got.Alone {
		t.Fatalf("gated card stop lost its compatibility retry: %+v", got)
	}

}
