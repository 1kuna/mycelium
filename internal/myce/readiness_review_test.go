package myce

import (
	"context"

	"errors"
	"net/http"

	"strings"

	"sync/atomic"
	"testing"
	"time"
)

func TestAdaptiveReadinessUnknownColdLoad(t *testing.T) {
	clock := &manualWorkerClock{now: time.Unix(1, 0)}
	ctx, p, finish := superviseModelLoad(context.Background(), Model{Name: "cold"}, t.TempDir(), clock)
	defer finish(false)
	clock.Advance(24 * time.Hour)
	if budget, stalled := p.state(); budget != 0 || stalled {
		t.Fatal("unknown initialization acquired a guessed deadline")
	}
	p.advance()
	clock.Advance(24 * time.Hour)
	if budget, stalled := p.state(); budget != 0 || stalled {
		t.Fatal("first progress signal guessed a cold deadline")
	}
	if ctx.Err() != nil {
		t.Fatal(context.Cause(ctx))
	}
	parent, cancel := context.WithCancel(context.Background())
	explicit, _, stop := superviseModelLoad(parent, Model{}, t.TempDir(), clock)
	defer stop(false)
	cancel()
	if !errors.Is(explicit.Err(), context.Canceled) {
		t.Fatal("caller cancellation lost")
	}
}

func TestAdaptiveReadinessProbeRecoversSlowerHealthyResponse(t *testing.T) {
	var completed atomic.Bool
	server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if readinessPause(r.Context(), 40*time.Millisecond) == nil {
			completed.Store(true)
		}
	}))
	defer server.Close()
	l := &LlamaEngine{Clock: RealClock{}, exited: make(chan struct{}), probeLatency: readinessLatency{Mean: time.Millisecond}}
	l.startHealthMonitor(server.URL)
	defer func() { l.monitorCancel(); <-l.monitorDone }()
	await(t, func() bool { return completed.Load() })
	await(t, func() bool { _, _, degraded := l.runtimeHealth(); return !degraded })
	if err, _, _ := l.runtimeHealth(); err != nil {
		t.Fatalf("slower healthy response unloaded engine: %v", err)
	}
}

func TestAdaptiveReadinessIdleDemandIsLocal(t *testing.T) {
	q, clock, m := modelFixture(t)
	q.models.Idle = 0
	c := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	slot := q.s.ModelSlots[slotKey(m.ID, "test")]
	clock.Advance(time.Second)
	reportModel(t, q, m, "ready")
	if slot.State != "ready" {
		t.Fatal("assigned local demand lost residency")
	}
	if err := q.finishCall(c.ID); err != nil {
		t.Fatal(err)
	}
	q.calls["remote"] = &ModelCall{Model: "test", Machine: "other", Error: ""}
	q.calls["excluded"] = &ModelCall{Model: "test", Project: "excluded", Queued: clock.Now()}
	q.s.Policies = map[string]ProjectPolicy{"excluded": {AllowedMachines: []string{"other"}}}
	clock.Advance(time.Hour)
	reportModel(t, q, m, "ready")
	if slot.State != "draining" {
		t.Fatal("policy-excluded demand protected residency")
	}
}

func TestAdaptiveReadinessActivityAndContendedColdAlert(t *testing.T) {
	clock := &manualWorkerClock{now: time.Unix(1, 0)}
	p := newReadinessProgress(clock, readinessHistory{})
	cpu, reads, host, device := 0.0, 0.0, 0.0, 0.0
	sample := func() { p.observeActivity(&cpu, &reads, &host, &device) }
	sample()
	for _, counter := range []*float64{&cpu, &reads, &host, &device} {
		clock.Advance(time.Hour)
		*counter++
		sample()
		if p.coldReason() != coldLoadActive {
			t.Fatal("moving OS signal was ignored")
		}
		if _, stalled := p.state(); stalled {
			t.Fatal("cold OS activity killed")
		}
	}
	sample()
	if p.coldReason() != coldLoadInactive {
		t.Fatal("quiet observable samples remained active")
	}
	p.advance()
	if p.coldReason() != coldLoadActive {
		t.Fatal("model progress did not clear inactivity")
	}

	q, _, m := modelFixture(t)
	q.models.Wait = 0
	addModelCall(t, q, "experiment")
	slot := q.s.ModelSlots[slotKey(m.ID, "test")]
	loading := func(attempt, reason string) {
		t.Helper()
		if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: attempt, State: "loading", Error: reason}}}); err != nil {
			t.Fatal(err)
		}
	}
	loading(slot.Attempt, coldLoadInactive)
	count := func() int {
		n := 0
		for _, e := range q.s.Health.Events {
			if e.Kind == "model_load_needs_attention" {
				n++
			}
		}
		return n
	}
	if count() != 0 {
		t.Fatal("uncontended cold load alerted")
	}
	j := submit(t, q, spec("experiment"))
	loading("stale-attempt", coldLoadActive)
	if slot.Error == coldLoadActive {
		t.Fatal("stale activity replaced exact attempt status")
	}
	loading(slot.Attempt, coldLoadInactive)
	if snap, err := q.publicSnapshot(); err != nil || len(snap.LoadedModels) != 1 || snap.LoadedModels[0].Error != coldLoadContended {
		t.Fatalf("public status hid cold reason: %+v %v", snap.LoadedModels, err)
	}
	if count() != 1 || slot.State != "loading" || !strings.Contains(slot.Error, "owner attention needed") {
		t.Fatalf("cold contention not visible: slot=%+v events=%+v waiting=%+v", slot, q.s.Health.Events, j)
	}
	loading(slot.Attempt, coldLoadInactive)
	if count() != 1 {
		t.Fatal("one inactive episode alerted repeatedly")
	}
	loading(slot.Attempt, coldLoadActive)
	loading(slot.Attempt, coldLoadInactive)
	if count() != 2 {
		t.Fatal("observed recovery did not rearm cold alert")
	}
	// Policy-ineligible backlog does not keep the episode active.
	q.s.Policies = map[string]ProjectPolicy{"test": {AllowedMachines: []string{"elsewhere"}}}
	q.detectReadinessHealth()
	for key, c := range q.s.Health.Conditions {
		if strings.HasPrefix(key, "cold-model/") && c.Active {
			t.Fatal("ineligible demand kept alert active")
		}
	}
}

type readinessRecordingEngine struct {
	FakeEngine
	clock *manualWorkerClock
}

func (e *readinessRecordingEngine) Load(ctx context.Context, m Model) error {
	e.clock.Advance(time.Second)
	readinessFromContext(ctx).advance()
	return e.FakeEngine.Load(ctx, m)
}

func TestAdaptiveReadinessSharedSensorHorizon(t *testing.T) {
	clock := &manualWorkerClock{now: time.Unix(1, 0)}
	p := newReadinessProgress(clock, readinessHistory{Load: readinessLatency{Mean: time.Millisecond}})
	// Linux cgroup CPU samples are faster than device-memory samples. A load
	// whose only advancing signal is GPU allocation must survive until it arrives.
	p.activityPeriod = readinessSensorPeriod(true, "linux", true)
	device := 0.0
	p.observeActivity(nil, nil, nil, &device)
	for range 1 {
		clock.Advance(deviceMemorySamplePeriod)
		if _, stalled := p.state(); stalled {
			t.Fatal("learned cutoff beat the device sensor's next observation")
		}
		device++
		p.observeActivity(nil, nil, nil, &device)
	}
	clock.Advance(2 * deviceMemorySamplePeriod)
	if _, stalled := p.state(); !stalled {
		t.Fatal("learned silent stall was not caught after the observable horizon")
	}
}

func TestAdaptiveReadinessSingleProcessResidentGrowth(t *testing.T) {
	clock := &manualWorkerClock{now: time.Unix(1, 0)}
	p := newReadinessProgress(clock, readinessHistory{})
	kib := "1024"
	birth := "original-model"
	probe := &processMemoryProbe{Resident: func(groups map[int]bool) (uint64, int, error) {
		// Root 7 execs the model and has no children; other groups are excluded.
		return processGroupResident([]byte("7 7 "+kib+"\n8 8 999999\n"), groups)
	}}
	birthOf := func(int) (string, error) { return birth, nil }
	sample := func() {
		t.Helper()
		bytes, members, err := readinessProcessResident(probe, map[int]string{7: "original-model"}, birthOf)
		if err != nil || members != 1 {
			t.Fatalf("single-process model RSS unavailable: %d %v", members, err)
		}
		gb := float64(bytes) / (1 << 30)
		p.observeActivity(nil, nil, &gb)
	}
	sample()
	clock.Advance(time.Hour)
	sample()
	if p.coldReason() != coldLoadInactive {
		t.Fatal("unchanged root RSS credited as growth")
	}
	kib = "2048"
	clock.Advance(time.Hour)
	sample()
	if p.coldReason() != coldLoadActive {
		t.Fatal("single-process model growth was excluded")
	}
	probe.Resident = func(map[int]bool) (uint64, int, error) {
		birth = "reused"
		return 1 << 30, 1, nil
	}
	if _, _, err := readinessProcessResident(probe, map[int]string{7: "original-model"}, birthOf); err == nil {
		t.Fatal("PID reuse during RSS inventory supplied activity")
	}
}
