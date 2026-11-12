package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestAdaptiveReadinessHistoryConcurrentRecipeAndTarget(t *testing.T) {
	dir := t.TempDir()
	model := Model{Name: "same", readinessRecipe: "immutable-recipe-a", Device: "0"}
	path := modelReadinessFile(dir, model)
	var wg sync.WaitGroup
	for _, d := range []time.Duration{time.Second, 10 * time.Second} {
		wg.Add(1)
		go func(d time.Duration) {
			defer wg.Done()
			mergeReadinessHistory(path, readinessHistory{Load: readinessLatency{Mean: d}, Gap: d})
		}(d)
	}
	wg.Wait()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var h readinessHistory
	if err := json.Unmarshal(b, &h); err != nil {
		t.Fatal(err)
	}
	if h.Gap != 10*time.Second || h.Load.Mean <= time.Second || h.Load.Mean >= 10*time.Second {
		t.Fatalf("lost merged observations: %+v", h)
	}
	model.readinessRecipe = "immutable-recipe-b"
	if readReadinessHistory(modelReadinessFile(dir, model)).Load.Mean != 0 {
		t.Fatal("changed recipe inherited history")
	}
	model.readinessRecipe, model.Device = "immutable-recipe-a", "1"
	if readReadinessHistory(modelReadinessFile(dir, model)).Load.Mean != 0 {
		t.Fatal("changed device inherited history")
	}
}

func TestAdaptiveReadinessProbeRecoversSlowerHealthyResponse(t *testing.T) {
	t.Run("nonempty_response_reuses_connection", func(t *testing.T) {
		var mu sync.Mutex
		connections := map[string]bool{}
		var probes atomic.Int32
		server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			connections[r.RemoteAddr] = true
			mu.Unlock()
			_, _ = w.Write([]byte("healthy"))
			probes.Add(1)
		}))
		defer server.Close()
		client := server.Client()
		defer client.CloseIdleConnections()
		// Exercise the monitor's real response drain without adaptive deadlines:
		// cancelling a slow request legitimately discards its TCP connection.
		// The recovery case below covers that cancellation/retry behavior.
		for range 3 {
			if healthy, err := probeServerHealth(t.Context(), client, server.URL); err != nil || !healthy {
				t.Fatalf("healthy response: healthy=%v err=%v", healthy, err)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if len(connections) != 1 {
			t.Fatalf("healthy probes churned TCP connections: %d connections for %d probes", len(connections), probes.Load())
		}
	})
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
	slot := &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, Attempt: "local", State: "ready", LastUsed: clock.Now()}
	q.s.ModelSlots[slotKey(m.ID, "test")] = slot
	q.calls = map[string]*ModelCall{"remote": {Model: "test", Machine: "other", Error: ""}}
	clock.Advance(time.Second)
	if !q.modelIdleExpired(slot) {
		t.Fatal("remote call protected unused local residency")
	}
	q.calls["excluded"] = &ModelCall{Model: "test", Project: "excluded", Queued: clock.Now()}
	q.s.Policies = map[string]ProjectPolicy{"excluded": {AllowedMachines: []string{"other"}}}
	if !q.modelIdleExpired(slot) {
		t.Fatal("policy-excluded demand protected residency")
	}
	q.calls["local"] = &ModelCall{Model: "test", Machine: m.ID, Session: m.Session, Attempt: slot.Attempt}
	if q.modelIdleExpired(slot) {
		t.Fatal("assigned local demand lost residency")
	}
}

func TestAdaptiveReadinessDevOmission(t *testing.T) {
	root := t.TempDir()
	d, err := startTestDev(t, DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "q.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "fixture", Workers: 1, MemoryGB: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Queue.models.Idle != 0 || d.Queue.models.Wait != 0 {
		t.Fatal("dev startup restored retired timeouts")
	}
	// Authored values continue to round-trip, including explicit validation.
	if _, err := ReadModels(""); err != nil {
		t.Fatal(err)
	}
	if err := validateModels(ModelConfig{Wait: -time.Second}); err == nil {
		t.Fatal("negative author deadline accepted")
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
				n += e.Count
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
func TestAdaptiveReadinessRuntimeUsesResolvedDeviceHistory(t *testing.T) {
	clock := &manualWorkerClock{now: time.Unix(1, 0)}
	host := m3Machine()
	worker := Worker{Machine: host, Clock: clock, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return host.Devices, nil }}
	dir := t.TempDir()
	model := Model{Name: "binding", Engine: "fake", Platforms: []string{"fake"}, MemoryGB: 1}
	m := &workerModel{device: "1", model: model, bound: &worker, attempt: "attempt", state: "loading", engine: &readinessRecordingEngine{clock: clock}, cancel: func() {}}
	rt := &modelRuntime{worker: &worker, models: map[string]*workerModel{"binding": m}, memoryMachine: host, platform: "unresolved", dir: dir, clock: clock}
	rt.load(context.Background(), m)
	if m.state != "ready" {
		t.Fatalf("fixture did not load: %s %s", m.state, m.err)
	}
	resolved := model
	resolved.Device = "1"
	if h := readReadinessHistory(modelReadinessFile(dir, resolved)); h.Load.Mean == 0 {
		t.Fatal("resolved device did not receive successful load evidence")
	}
	if h := readReadinessHistory(modelReadinessFile(dir, model)); h.Load.Mean != 0 {
		t.Fatal("unresolved host inherited device load evidence")
	}
}

func TestAdaptiveReadinessSharedResourceActivity(t *testing.T) {
	clock := &manualWorkerClock{now: time.Unix(1, 0)}
	p := newReadinessProgress(clock, readinessHistory{})
	cpu, reads, host, device := 0.0, uint64(0), 0.0, 0.0
	available := true
	o := &resourceObserver{clock: clock, start: clock.Now(), read: func() *AttemptResources {
		if !available {
			return &AttemptResources{}
		}
		c, r := cpu, reads
		return &AttemptResources{CPU: AttemptCPUTime{UsageSeconds: &c}, IO: AttemptIO{ReadBytes: &r}}
	}}
	memoryReads := 0
	p.osObserver = true
	p.activitySource = readinessActivitySource(o.snapshot, func() (*float64, *float64) {
		memoryReads++
		if !available {
			return nil, nil
		}
		h, d := host, device
		return &h, &d
	})
	sample := func() {
		clock.Advance(time.Hour)
		o.sample()
		if _, stalled := p.state(); stalled {
			t.Fatal("cold shared observation acquired a deadline")
		}
	}
	sample()
	for _, move := range []func(){func() { cpu++ }, func() { reads++ }, func() { host++ }, func() { device++ }} {
		sample()
		if p.coldReason() != coldLoadInactive {
			t.Fatal("unchanged sample was credited as activity")
		}
		move()
		sample()
		if p.coldReason() != coldLoadActive {
			t.Fatal("shared resource growth was ignored")
		}
		before := memoryReads
		p.state()
		if memoryReads != before || p.coldReason() != coldLoadActive {
			t.Fatal("repeated status resampled or changed the observed episode")
		}
	}
	available = false
	sample()
	if p.coldReason() != coldLoadInactive {
		t.Fatal("missing counters invented activity")
	}
	available = true
	host = 0
	sample()
	host = 0.5
	sample()
	if p.coldReason() != coldLoadActive {
		t.Fatal("memory regrowth below its previous peak was ignored")
	}
}

func TestAdaptiveReadinessSharedSensorHorizon(t *testing.T) {
	clock := &manualWorkerClock{now: time.Unix(1, 0)}
	p := newReadinessProgress(clock, readinessHistory{Load: readinessLatency{Mean: time.Millisecond}})
	// Linux cgroup CPU samples are faster than device-memory samples. A load
	// whose only advancing signal is GPU allocation must survive until it arrives.
	p.activityPeriod = readinessSensorPeriod(true, "linux", true)
	device := 0.0
	p.observeActivity(nil, nil, nil, &device)
	for range 3 {
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
