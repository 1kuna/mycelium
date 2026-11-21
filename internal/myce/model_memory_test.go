package myce

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type measuredFakeEngine struct {
	FakeEngine
	memory *modelMemoryTracker
	sample func([]byte)
}

func (e *measuredFakeEngine) modelMemory() *modelMemoryTracker { return e.memory }

func (e *measuredFakeEngine) Call(ctx context.Context, path string, data []byte) (*http.Response, error) {
	e.sample(data)
	response, err := e.FakeEngine.Call(ctx, path, data)
	// A normal engine can send an explicit Content-Length. The worker must
	// still deliver the measurement after the last body byte.
	response.Header.Set("Content-Length", "10")
	return response, err
}

func TestModelMemoryPeaksThroughStatusAndIntervals(t *testing.T) {
	q, clock, machine := modelFixture(t)
	model, _ := q.model("test")
	memory := &modelMemoryTracker{clock: clock, deviceUnavailable: modelMemoryNoSample}
	engine := &measuredFakeEngine{FakeEngine: FakeEngine{Model: model.Name}, memory: memory}
	period := time.Second
	sample := func(host, device float64, constrained bool) {
		at := clock.Now()
		memory.observe(at, at, &AttemptMemoryPeak{HostGB: &host, Constrained: constrained}, period, false)
		if device >= 0 {
			memory.observe(at, at, &AttemptMemoryPeak{DeviceGB: &device}, period, true)
		}
		clock.Advance(period)
	}
	runtime := &modelRuntime{clock: clock, token: "fixture", session: machine.Session}
	worker := httptest.NewServer(http.HandlerFunc(runtime.serve))
	defer worker.Close()
	machine.ModelURL = worker.URL
	machine.Devices = m3Machine().Devices
	machineCheck(t, q, machine)
	cold := addModelCall(t, q, "experiment")
	slot := q.s.ModelSlots[slotKey(machine.ID, model.Name, "0")]
	if slot == nil {
		t.Fatal("model was not assigned")
	}
	m := &workerModel{model: model, device: slot.Device, attempt: slot.Attempt, engine: engine, state: "ready"}
	runtime.models = map[string]*workerModel{engineKey(model.Name, slot.Device): m}
	report := func(state string) {
		t.Helper()
		runtime.mu.Lock()
		m.state = state
		runtime.mu.Unlock()
		reports, _ := runtime.report()
		if _, err := q.Checkin(Checkin{Machine: machine, ModelReports: reports}); err != nil {
			t.Fatal(err)
		}
	}
	memory.begin()
	sample(8, 4, false)
	sample(5, 3, false)
	memory.ready()
	report("ready")
	if err := q.finishCall(cold.ID); err != nil {
		t.Fatal(err)
	}
	sample(2, 1, false)
	sample(3, 2, false)
	server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "fixture"}
	engine.sample = func(data []byte) {
		if strings.Contains(string(data), `"stream":true`) {
			sample(10, 6, false)
			sample(7, 5, false)
		} else {
			sample(9, -1, true)
		}
	}
	runCall := func(body string) string {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/v1/chat/completions", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer fixture")
		req.Header.Set("X-Myce-Project", "test")
		req.Header.Set("X-Myce-Priority", "experiment")
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || len(data) == 0 || response.Trailer.Get(modelMemoryTrailer) != "" {
			t.Fatalf("unexpected proxy response %d %s %v", response.StatusCode, data, err)
		}
		if id := response.Header.Get(modelMemoryRequestHeader); id != "" {
			return id
		}
		t.Fatal("missing request ID")
		return ""
	}
	first := runCall(`{"model":"test","stream":true}`)
	last := runCall(`{"model":"test"}`)
	// Both calls finished between check-ins. The live snapshot keeps the
	// latest call, while the ledger must be able to recover either call.
	report("ready")
	stale := ModelReport{Name: model.Name, Device: slot.Device, Attempt: ID(), State: "ready", MemoryPeaks: &ModelMemoryPeaks{}}
	if _, err := q.Checkin(Checkin{Machine: machine, ModelReports: []ModelReport{stale}}); err != nil {
		t.Fatal(err)
	}
	var status Snapshot
	if err := client.Call(t.Context(), "GET", "/api/status?jobs=recent", nil, &status); err != nil || len(status.LoadedModels) != 1 {
		t.Fatalf("reading engine status: %v %+v", err, status.LoadedModels)
	}
	peaks := status.LoadedModels[0].MemoryPeaks
	assertPeak := func(measurement ModelMemoryMeasurement, want float64, source string) {
		t.Helper()
		if measurement.PeakGB == nil || *measurement.PeakGB != want || measurement.Source != source || measurement.SampledAt == nil {
			t.Fatalf("want %.1f GiB from %s, got %+v", want, source, measurement)
		}
	}
	assertPeak(peaks.Load.Host, 8, modelMemoryHostCgroup)
	assertPeak(peaks.Load.Device, 4, modelMemoryDevicePID)
	if !peaks.Load.Host.Complete || !peaks.Load.Device.Complete || peaks.Load.EndedAt == nil {
		t.Fatal("lost load coverage", peaks.Load)
	}
	assertPeak(peaks.Steady.Host, 3, modelMemoryHostCgroup)
	assertPeak(peaks.Steady.Device, 2, modelMemoryDevicePID)
	assertPeak(peaks.Request.Host, 9, modelMemoryHostCgroup)
	if peaks.Request.RequestID != last || !peaks.Request.Host.Constrained || peaks.Request.Host.Complete ||
		peaks.Request.Device.PeakGB != nil || peaks.Request.Device.Unavailable != modelMemoryNoSample {
		t.Fatal("lost constrained or unavailable request evidence", peaks.Request)
	}

	clock.Advance(q.models.Idle)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	memory.stop()
	report("unloaded")
	var page ModelIntervalPage
	if err := client.Call(t.Context(), "GET", "/api/models/intervals", nil, &page); err != nil {
		t.Fatal(err)
	}
	requests := map[string]*ModelMemoryWindow{}
	for _, event := range page.Entries {
		if event.Kind == "memory_request" && event.Attempt == slot.Attempt {
			requests[event.MemoryPeaks.Request.RequestID] = event.MemoryPeaks.Request
		}
	}
	if len(requests) != 2 || requests[first] == nil || requests[last] == nil {
		t.Fatal("request windows lost between heartbeats", requests)
	}
	assertPeak(requests[first].Host, 10, modelMemoryHostCgroup)
	assertPeak(requests[first].Device, 6, modelMemoryDevicePID)
	release := page.Entries[len(page.Entries)-1]
	if release.Kind != "released" || release.Attempt != slot.Attempt || release.Device != slot.Device || release.ReleasedAt == nil ||
		release.MemoryPeaks.Request.RequestID != last || release.MemoryPeaks.Steady.EndedAt == nil {
		t.Fatal("unload did not retain interval evidence", release)
	}
	assertPeak(release.MemoryPeaks.Load.Host, 8, modelMemoryHostCgroup)
	assertPeak(release.MemoryPeaks.Steady.Device, 2, modelMemoryDevicePID)

	var sequence int
	var name, path string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	server.Close()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	durable := intervalPage(t, reopened, release.Sequence-1, page.HighWater, 1)
	if len(durable.Entries) != 1 || durable.Entries[0].MemoryPeaks.Request.RequestID != last {
		t.Fatal("engine measurement lost across queue reopen", durable)
	}
}

func TestModelMemoryDeviceSamplerWindowsAndOverlap(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1, 0)}
	memory := &modelMemoryTracker{clock: clock, deviceUnavailable: modelMemoryNoSample}
	memory.begin()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ticks := make(chan time.Time, 1)
	samples := make(chan struct{}, 1)
	w := Worker{Machine: Machine{GPU: "cuda", Device: "GPU-test"}, cudaMemoryQuery: func(context.Context) ([]byte, error) {
		return []byte("GPU-test,42,2048\nGPU-test,43,16384\nGPU-other,42,65536\n"), nil
	}}
	done := w.observeDeviceMemoryWith(ctx, func() (map[int]bool, map[int]bool, error) {
		return map[int]bool{42: true}, nil, nil
	}, nil, deviceMemorySamplePeriod, clock, nil, func(start, at time.Time, peak *AttemptMemoryPeak, period time.Duration, device bool) {
		memory.observe(start, at, peak, period, device)
		samples <- struct{}{}
	}, ticks)
	sample := func() {
		t.Helper()
		clock.Advance(deviceMemorySamplePeriod)
		ticks <- clock.Now()
		select {
		case <-samples:
		case <-time.After(testWaitTimeout):
			t.Fatal("device sample did not arrive")
		}
	}
	sample()
	sample()
	memory.ready()
	a, b := memory.startRequest(""), memory.startRequest("")
	sample()
	for _, finish := range []func() *ModelMemoryWindow{a, b} {
		peak := finish()
		if !peak.Overlap || peak.Device.PeakGB == nil || *peak.Device.PeakGB != 2 || !peak.Device.PressureUnavailable {
			t.Fatal("device peak lost ownership, overlap or pressure uncertainty", peak)
		}
	}
	peaks := memory.snapshot()
	if !peaks.Load.Device.Complete || peaks.Steady.Device.PeakGB != nil {
		t.Fatal("request sample leaked into idle or load is incomplete", peaks)
	}
	cancel()
	<-done
}

func TestModelMemoryHostSamplerReusesAttemptCounters(t *testing.T) {
	clock := &workerFakeClock{now: time.Unix(1, 0)}
	memory := &modelMemoryTracker{clock: clock}
	memory.begin()
	a, err := fakeCeilings(t).create("engine-fixture", 8<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	memoryFactsFile(t, a, "memory.stat", "anon 2147483648\nkernel 0\nshmem 0\nfile 6442450944\n")
	memoryFactsFile(t, a, "memory.events", "high 1\nmax 0\noom 0\n")
	clock.Advance(attemptMemorySamplePeriod)
	w := Worker{Clock: clock, Machine: Machine{GPU: "cpu"}}
	job := Job{Spec: Spec{Needs: ResourceNeeds{Platform: Platform{Required: []string{"cpu"}}}}}
	finish := w.observeAttemptMemoryWith(a, job, nil, deviceMemorySamplePeriod, memory.observe)
	defer finish()
	await(t, func() bool { return memory.snapshot().Load.Host.Samples != 0 })
	host := memory.snapshot().Load.Host
	if host.PeakGB == nil || *host.PeakGB != 2 || !host.Constrained || host.Source != modelMemoryHostCgroup {
		t.Fatal("resident host sampling lost cgroup provenance or counted file cache", host)
	}
}
