package myce

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type measurementEngine struct {
	load     func(context.Context, Model) error
	unloaded bool
	call     func(context.Context, string, []byte) (*http.Response, error)
}

func (e *measurementEngine) Load(c context.Context, m Model) error { return e.load(c, m) }
func (e *measurementEngine) Unload() error                         { e.unloaded = true; return nil }
func (e *measurementEngine) Call(c context.Context, p string, b []byte) (*http.Response, error) {
	return e.call(c, p, b)
}
func measureTestOptions(t *testing.T) MeasureOptions {
	return MeasureOptions{Worker: Worker{StateDir: t.TempDir(), Machine: Machine{ID: "local"}, Clock: RealClock{}}, Platform: "cuda", Device: "0", BudgetGB: 10, Models: []Model{{Name: "one", Platforms: []string{"cuda"}}, {Name: "two", Platforms: []string{"cuda"}}}, ReadMemory: func(context.Context) (MemoryReading, error) { return MemoryReading{1, 2}, nil }, Status: func(context.Context) (Snapshot, error) { return Snapshot{}, nil }}
}
func TestMeasureSimultaneousResidencyAndDeltas(t *testing.T) {
	o := measureTestOptions(t)
	usage := MemoryReading{1, 2}
	var engines []*measurementEngine
	o.ReadMemory = func(context.Context) (MemoryReading, error) { return usage, nil }
	o.NewEngine = func(m Model, _ string, _ Clock) Engine {
		e := &measurementEngine{}
		e.load = func(context.Context, Model) error {
			for _, previous := range engines[:len(engines)-1] {
				if previous.unloaded {
					t.Error("earlier model unloaded before combined measurement")
				}
			}
			usage.DeviceGB += 3
			usage.HostGB += 4
			return nil
		}
		engines = append(engines, e)
		return e
	}
	r, err := Measure(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if r.Baseline.DeviceGB != 1 || r.Peak.DeviceGB != 7 || r.Delta.DeviceGB != 6 || !r.Fits || len(r.Models) != 2 || r.Models[1].Delta.DeviceGB != 3 {
		t.Fatalf("unexpected report %+v", r)
	}
	for _, e := range engines {
		if !e.unloaded {
			t.Fatal("engine not unloaded")
		}
	}
}
func TestMeasureFailureUnloadsFailedAndEarlierEngines(t *testing.T) {
	o := measureTestOptions(t)
	var engines []*measurementEngine
	released := 0
	o.Prepare = func(_ context.Context, m Model, _ string) (Model, func(), error) {
		return m, func() { released++ }, nil
	}
	o.NewEngine = func(m Model, _ string, _ Clock) Engine {
		e := &measurementEngine{load: func(context.Context, Model) error {
			if m.Name == "two" {
				return errors.New("load failed")
			}
			return nil
		}}
		engines = append(engines, e)
		return e
	}
	r, err := Measure(context.Background(), o)
	if err == nil || r.Fits {
		t.Fatalf("expected failed measurement: %+v %v", r, err)
	}
	for _, e := range engines {
		if !e.unloaded {
			t.Fatal("cleanup missed engine")
		}
	}
	if released != 2 {
		t.Fatalf("released %d", released)
	}
}
func TestMeasureProductionRefusalAndQueueFailure(t *testing.T) {
	for _, scenario := range []string{"production", "queue", "pin", "calls"} {
		t.Run(scenario, func(t *testing.T) {
			o := measureTestOptions(t)
			o.Status = func(context.Context) (Snapshot, error) {
				switch scenario {
				case "queue":
					return Snapshot{}, errors.New("offline")
				case "pin":
					return Snapshot{Pins: []ModelPin{{Machine: "local"}}}, nil
				case "calls":
					return Snapshot{LoadedModels: []ModelSlot{{Machine: "local", InFlight: 1}}}, nil
				default:
					return Snapshot{Jobs: []*Job{{Spec: Spec{Priority: "production"}, State: "queued"}}}, nil
				}
			}
			o.NewEngine = func(Model, string, Clock) Engine { t.Fatal("engine started during refusal"); return nil }
			if _, e := Measure(context.Background(), o); e == nil {
				t.Fatal("expected refusal")
			}
		})
	}
}
func TestMeasureForceNeverBypassesJoinLock(t *testing.T) {
	o := measureTestOptions(t)
	s, e := o.Worker.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	o.Force = true
	if _, e = Measure(context.Background(), o); e == nil || !strings.Contains(e.Error(), "already in use") {
		t.Fatalf("lock error: %v", e)
	}
}
func TestMeasureSamplingFailureCancelsLoad(t *testing.T) {
	o := measureTestOptions(t)
	o.SampleInterval = time.Millisecond
	n := 0
	o.ReadMemory = func(context.Context) (MemoryReading, error) {
		n++
		if n >= 3 {
			return MemoryReading{}, errors.New("lost probe")
		}
		return MemoryReading{}, nil
	}
	e := &measurementEngine{load: func(c context.Context, _ Model) error { <-c.Done(); return c.Err() }}
	o.NewEngine = func(Model, string, Clock) Engine { return e }
	if _, err := Measure(context.Background(), o); err == nil || !e.unloaded {
		t.Fatalf("error=%v cleanup=%v", err, e.unloaded)
	}
}
func TestMeasureRejectsInventedMemory(t *testing.T) {
	if _, e := NewMeasurementSource("fake", "0"); e == nil {
		t.Fatal("fake budget accepted as measured")
	}
	if _, e := parseMeasureHost([]byte("MemTotal: 100 kB\nMemAvailable: 200 kB\n")); e == nil {
		t.Fatal("invalid counters accepted")
	}
}
func TestMeasureWarmupFailureUnloads(t *testing.T) {
	o := measureTestOptions(t)
	o.Models = o.Models[:1]
	o.Warmup = map[string]MeasureWarmup{"one": {Path: "/infer", Body: []byte(`{}`)}}
	e := &measurementEngine{load: func(context.Context, Model) error { return nil }, call: func(context.Context, string, []byte) (*http.Response, error) {
		return nil, errors.New("warmup failure")
	}}
	o.NewEngine = func(Model, string, Clock) Engine { return e }
	if _, err := Measure(context.Background(), o); err == nil || !e.unloaded {
		t.Fatal("warmup failure did not clean up")
	}
}

func TestMeasureXPURequiresCurrentSelectedDevice(t *testing.T) {
	good := []byte(`{"device_id":1,"device_level":[{"metrics_type":"XPUM_STATS_MEMORY_USED","value":4096,"max":8192}]}`)
	n, e := parseMeasureXPU(good, "1")
	if e != nil || n != 4 {
		t.Fatalf("used=%v err=%v", n, e)
	}
	for _, data := range []string{`{"device_id":0,"device_level":[{"metrics_type":"XPUM_STATS_MEMORY_USED","value":4096}]}`, `{"device_id":1,"device_level":[{"metrics_type":"XPUM_STATS_MEMORY_USED","max":4096}]}`, `{"device_id":1,"device_level":[{"metrics_type":"XPUM_STATS_MEMORY_USED","value":-1}]}`} {
		if _, e := parseMeasureXPU([]byte(data), "1"); e == nil {
			t.Fatal("accepted missing/incorrect native counter")
		}
	}
}
func TestMeasureDeviceBoundBeforeEngineConstruction(t *testing.T) {
	o := measureTestOptions(t)
	o.Models = o.Models[:1]
	o.Device = "2"
	o.NewEngine = func(m Model, _ string, _ Clock) Engine {
		if m.Device != "2" || m.DeviceEnv["CUDA_VISIBLE_DEVICES"] != "2" {
			t.Fatalf("wrong selected device %+v", m)
		}
		return &measurementEngine{load: func(context.Context, Model) error { return nil }}
	}
	if _, e := Measure(context.Background(), o); e != nil {
		t.Fatal(e)
	}
}
func TestMeasureBudgetUsesAbsolutePeak(t *testing.T) {
	o := measureTestOptions(t)
	o.Models = o.Models[:1]
	o.BudgetGB = 2
	o.ReadMemory = func(context.Context) (MemoryReading, error) { return MemoryReading{3, 4}, nil }
	o.NewEngine = func(Model, string, Clock) Engine {
		return &measurementEngine{load: func(context.Context, Model) error { return nil }}
	}
	r, e := Measure(context.Background(), o)
	if e != nil || r.Fits || r.Delta.DeviceGB != 0 {
		t.Fatalf("absolute usage was not used for fit: %+v %v", r, e)
	}
}

type measurementRequestEngine struct {
	measurementEngine
	method  string
	path    string
	headers http.Header
}

func (e *measurementRequestEngine) CallRequest(_ context.Context, method, path string, _ []byte, h http.Header) (*http.Response, error) {
	e.method = method
	e.path = path
	e.headers = h
	return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
}
func TestMeasureWarmupRequestMethodAndHeaders(t *testing.T) {
	o := measureTestOptions(t)
	o.Models = o.Models[:1]
	o.Warmup = map[string]MeasureWarmup{"one": {Method: "PUT", Path: "/audio", Headers: http.Header{"X-Test": []string{"yes"}}, Body: []byte(`{}`)}}
	e := &measurementRequestEngine{measurementEngine: measurementEngine{load: func(context.Context, Model) error { return nil }}}
	o.NewEngine = func(Model, string, Clock) Engine { return e }
	if _, err := Measure(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if e.method != "PUT" || e.path != "/audio" || e.headers.Get("X-Test") != "yes" || !e.unloaded {
		t.Fatalf("request was not preserved: %+v", e)
	}
}
func TestMeasureProbeIncludesTransientLoadPeak(t *testing.T) {
	o := measureTestOptions(t)
	o.Models = o.Models[:1]
	o.SampleInterval = time.Millisecond
	peak := make(chan struct{})
	observed := make(chan struct{})
	closed := false
	o.ReadMemory = func(context.Context) (MemoryReading, error) {
		select {
		case <-peak:
			if !closed {
				close(observed)
				closed = true
			}
			return MemoryReading{8, 12}, nil
		default:
			return MemoryReading{1, 2}, nil
		}
	}
	o.NewEngine = func(Model, string, Clock) Engine {
		return &measurementEngine{load: func(c context.Context, _ Model) error {
			close(peak)
			select {
			case <-observed:
				return nil
			case <-c.Done():
				return c.Err()
			}
		}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	r, err := Measure(ctx, o)
	if err != nil || r.Peak.DeviceGB != 8 || r.Models[0].Delta.DeviceGB != 7 {
		t.Fatalf("transient peak not sampled %+v %v", r, err)
	}
}

type failedMeasureUnload struct{ measurementEngine }

func (*failedMeasureUnload) Unload() error { return errors.New("owned group still present") }
func TestMeasureFailedUnloadKeepsEnvironmentLease(t *testing.T) {
	o := measureTestOptions(t)
	o.Models = o.Models[:1]
	released := false
	o.Prepare = func(_ context.Context, m Model, _ string) (Model, func(), error) {
		return m, func() { released = true }, nil
	}
	o.NewEngine = func(Model, string, Clock) Engine {
		return &failedMeasureUnload{measurementEngine{load: func(context.Context, Model) error { return nil }}}
	}
	r, err := Measure(context.Background(), o)
	if err == nil || released || r.Fits || r.Error == "" {
		t.Fatalf("unsafe failed cleanup lease: released=%t report=%+v err=%v", released, r, err)
	}
}

func TestMeasureOverlappingWarmups(t *testing.T) {
	o := measureTestOptions(t)
	o.WarmupParallel = 2
	o.Models = append(o.Models, Model{Name: "three", Platforms: []string{"cuda"}})
	o.SampleInterval = time.Millisecond
	var active atomic.Int32
	sampled := make(chan struct{})
	var once sync.Once
	o.ReadMemory = func(context.Context) (MemoryReading, error) {
		n := active.Load()
		if n == 2 {
			once.Do(func() { close(sampled) })
		}
		return MemoryReading{float64(n * 4), float64(n * 5)}, nil
	}
	o.Warmup = map[string]MeasureWarmup{"one": {Path: "/infer"}, "two": {Path: "/infer"}, "three": {Path: "/infer"}}
	var engines []*measurementEngine
	o.NewEngine = func(Model, string, Clock) Engine {
		e := &measurementEngine{load: func(context.Context, Model) error { return nil }, call: func(c context.Context, _ string, _ []byte) (*http.Response, error) {
			active.Add(1)
			defer active.Add(-1)
			select {
			case <-sampled:
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			case <-c.Done():
				return nil, c.Err()
			}
		}}
		engines = append(engines, e)
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	r, err := Measure(ctx, o)
	if err != nil || r.Peak.DeviceGB != 8 || r.Peak.HostGB != 10 || r.WarmupParallel != 2 {
		t.Fatalf("%+v %v", r, err)
	}
	for _, e := range engines {
		if !e.unloaded {
			t.Fatal("cleanup")
		}
	}
}
func TestMeasureLoadTimeoutOption(t *testing.T) {
	o := measureTestOptions(t)
	o.LoadTimeout = time.Millisecond
	o.Models = o.Models[:1]
	e := &measurementEngine{load: func(c context.Context, _ Model) error { <-c.Done(); return c.Err() }}
	o.NewEngine = func(Model, string, Clock) Engine { return e }
	_, err := Measure(context.Background(), o)
	if !errors.Is(err, context.DeadlineExceeded) || !e.unloaded {
		t.Fatalf("%v", err)
	}
}

func TestMeasureWarmupFailureCancelsPeers(t *testing.T) {
	o := measureTestOptions(t)
	o.WarmupParallel = 2
	o.Warmup = map[string]MeasureWarmup{"one": {Path: "/infer"}, "two": {Path: "/infer"}}
	started := make(chan struct{})
	cancelled := make(chan struct{})
	var engines []*measurementEngine
	o.NewEngine = func(m Model, _ string, _ Clock) Engine {
		e := &measurementEngine{load: func(context.Context, Model) error { return nil }, call: func(c context.Context, _ string, _ []byte) (*http.Response, error) {
			if m.Name == "one" {
				close(started)
				<-c.Done()
				close(cancelled)
				return nil, c.Err()
			}
			<-started
			return nil, errors.New("warm failure")
		}}
		engines = append(engines, e)
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	r, err := Measure(ctx, o)
	if err == nil || r.Fits {
		t.Fatalf("%+v %v", r, err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("peer not cancelled before return")
	}
	for _, e := range engines {
		if !e.unloaded {
			t.Fatal("engine not cleaned up")
		}
	}
}
func TestMeasureSeparatesWarmupTime(t *testing.T) {
	o := measureTestOptions(t)
	o.Models = o.Models[:1]
	clock := &fakeClock{now: time.Unix(0, 0)}
	o.Worker.Clock = clock
	o.Warmup = map[string]MeasureWarmup{"one": {Path: "/infer"}}
	o.NewEngine = func(Model, string, Clock) Engine {
		return &measurementEngine{load: func(context.Context, Model) error { clock.Advance(5 * time.Second); return nil }, call: func(context.Context, string, []byte) (*http.Response, error) {
			clock.Advance(2 * time.Second)
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		}}
	}
	r, err := Measure(context.Background(), o)
	if err != nil || r.Models[0].LoadSeconds != 5 || r.Models[0].WarmupSeconds != 2 || r.WarmupSeconds != 2 {
		t.Fatalf("%+v %v", r, err)
	}
}
