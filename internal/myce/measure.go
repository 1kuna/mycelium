package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/probeguard"
)

// MemoryReading is absolute system usage in GiB, not a configured allowance.
// Host usage includes reclaimable cache (MemTotal - MemAvailable on Linux).
type MemoryReading struct {
	DeviceGB float64 `json:"device_gb"`
	HostGB   float64 `json:"host_gb"`
}
type MeasureWarmup struct {
	Method  string          `json:"method"`
	Path    string          `json:"path"`
	Body    json.RawMessage `json:"body"`
	Headers http.Header     `json:"headers,omitempty"`
}
type ModelMeasurement struct {
	Model         string        `json:"model"`
	WarmupSeconds float64       `json:"warmup_seconds"`
	LoadSeconds   float64       `json:"load_seconds"`
	Before        MemoryReading `json:"before"`
	Peak          MemoryReading `json:"absolute_peak"`
	Delta         MemoryReading `json:"incremental_peak_delta"`
}
type Measurement struct {
	WarmupParallel     int                `json:"warmup_parallel"`
	WarmupSeconds      float64            `json:"warmup_seconds"`
	LoadTimeoutSeconds float64            `json:"load_timeout_seconds"`
	DeviceBinding      string             `json:"device_binding,omitempty"`
	Error              string             `json:"error,omitempty"`
	SampleIntervalMS   int64              `json:"sample_interval_ms"`
	CatalogHash        string             `json:"catalog_hash"`
	Worker             string             `json:"worker"`
	Platform           string             `json:"platform"`
	Device             string             `json:"device"`
	BudgetGB           float64            `json:"device_budget_gb"`
	Baseline           MemoryReading      `json:"baseline"`
	Peak               MemoryReading      `json:"combined_absolute_peak"`
	Delta              MemoryReading      `json:"combined_peak_delta_from_baseline"`
	Fits               bool               `json:"fits_device_budget"`
	Models             []ModelMeasurement `json:"models"`
}
type MeasureOptions struct {
	WarmupParallel   int
	LoadTimeout      time.Duration
	Worker           Worker
	Models           []Model
	Platform, Device string
	BudgetGB         float64
	Force            bool
	Warmup           map[string]MeasureWarmup
	ReadMemory       func(context.Context) (MemoryReading, error)
	Status           func(context.Context) (Snapshot, error)
	Prepare          func(context.Context, Model, string) (Model, func(), error)
	NewEngine        func(Model, string, Clock) Engine
	SampleInterval   time.Duration
}

func measureDelta(a, b MemoryReading) MemoryReading {
	return MemoryReading{max(0, a.DeviceGB-b.DeviceGB), max(0, a.HostGB-b.HostGB)}
}
func measurePeak(a, b MemoryReading) MemoryReading {
	return MemoryReading{max(a.DeviceGB, b.DeviceGB), max(a.HostGB, b.HostGB)}
}

// Measure holds the exact join spool lock throughout preparation, loading,
// sampling and cleanup. Force bypasses queue refusal only, never this lock.
func Measure(ctx context.Context, o MeasureOptions) (report Measurement, err error) {
	if len(o.Models) == 0 || o.Worker.Machine.ID == "" || o.Device == "" || !validMemoryValue(o.BudgetGB) || o.BudgetGB <= 0 {
		return report, errors.New("measure requires models, worker, device and positive finite device budget")
	}
	if o.LoadTimeout < 0 || o.WarmupParallel < 0 {
		return report, errors.New("load timeout and warmup parallelism must be positive")
	}
	if o.LoadTimeout == 0 {
		o.LoadTimeout = 5 * time.Minute
	}
	if o.WarmupParallel == 0 {
		o.WarmupParallel = 1
	}
	if o.Worker.Clock == nil {
		o.Worker.Clock = RealClock{}
	}
	if o.SampleInterval <= 0 {
		o.SampleInterval = 100 * time.Millisecond
	}
	binding := o.Device
	if o.ReadMemory == nil {
		if o.Platform == "cuda" {
			binding, err = ResolveCUDADevice(ctx, o.Device)
			if err != nil {
				return report, err
			}
		}
		o.ReadMemory, err = NewMeasurementSource(o.Platform, binding)
		if err != nil {
			return report, err
		}
	}
	if o.NewEngine == nil {
		o.NewEngine = NewModelEngine
	}
	s, err := o.Worker.openStore()
	if err != nil {
		return report, err
	}
	defer s.close()
	if s.state.Job != nil || s.state.Result != nil || s.state.PID != 0 || s.state.Pending != 0 {
		return report, errors.New("worker spool has unfinished execution; join must reconcile it before measurement")
	}
	// Do not kill or adopt saved engines: worker recovery owns those identities.
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return report, err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "engine-") && strings.HasSuffix(e.Name(), ".json") {
			data, err := os.ReadFile(filepath.Join(s.dir, e.Name()))
			if err != nil {
				return report, err
			}
			var saved workerState
			if err = decodeWorkerJSON(data, &saved); err != nil {
				return report, err
			}
			if saved.PID != 0 {
				return report, errors.New("worker spool has saved engines; join must reconcile them before measurement")
			}
		}
	}
	if !o.Force {
		status := o.Status
		if status == nil {
			status = func(c context.Context) (Snapshot, error) {
				var snap Snapshot
				e := o.Worker.Client.Call(c, "GET", "/api/status", nil, &snap)
				return snap, e
			}
		}
		snap, e := status(ctx)
		if e != nil {
			return report, fmt.Errorf("cannot establish production safety from queue: %w", e)
		}
		for _, j := range snap.Jobs {
			if j != nil && j.Spec.Priority == "production" && !terminal(j.State) {
				return report, errors.New("production work outstanding; measurement refused (use --force only during an approved maintenance window)")
			}
		}
		for _, p := range snap.Pins {
			if p.Machine == o.Worker.Machine.ID || p.Platform == o.Platform {
				return report, errors.New("production pin targets this worker/platform; measurement refused")
			}
		}
		for _, m := range snap.LoadedModels {
			if m.Machine == o.Worker.Machine.ID && (m.Priority == "production" || m.InFlight > 0) {
				return report, errors.New("production model or active calls on selected worker; measurement refused")
			}
		}
	}
	report = Measurement{WarmupParallel: o.WarmupParallel, LoadTimeoutSeconds: o.LoadTimeout.Seconds(), DeviceBinding: binding, SampleIntervalMS: o.SampleInterval.Milliseconds(), CatalogHash: modelConfigHash(ModelConfig{Models: o.Models}), Worker: o.Worker.Machine.ID, Platform: o.Platform, Device: o.Device, BudgetGB: o.BudgetGB}
	read := func(c context.Context) (MemoryReading, error) {
		r, e := o.ReadMemory(c)
		if e == nil && (!validMemoryValue(r.DeviceGB) || !validMemoryValue(r.HostGB)) {
			e = errors.New("invalid measured used memory")
		}
		return r, e
	}
	report.Baseline, err = read(ctx)
	if err != nil {
		return report, err
	}
	report.Peak = report.Baseline
	var engines []Engine
	var releases []func()
	var pendingRelease func()
	defer func() {
		for i := len(engines) - 1; i >= 0; i-- {
			unloadErr := engines[i].Unload()
			err = errors.Join(err, unloadErr)
			if unloadErr == nil && releases[i] != nil {
				releases[i]()
			}
		}
		if pendingRelease != nil {
			pendingRelease()
		}
		if err != nil {
			report.Error = err.Error()
		}
		report.Delta = measureDelta(report.Peak, report.Baseline)
		report.Fits = err == nil && len(report.Models) == len(o.Models) && report.Peak.DeviceGB <= o.BudgetGB
	}()
	for _, m := range o.Models {
		if !contains(m.Platforms, o.Platform) {
			return report, fmt.Errorf("model %s does not support %s", m.Name, o.Platform)
		}
		if e := ctx.Err(); e != nil {
			return report, e
		}
		m, err = WithModelDevice(m, o.Platform, o.Device)
		if err == nil && o.Platform == "cuda" {
			m.DeviceEnv["CUDA_VISIBLE_DEVICES"] = binding
		}
		if err != nil {
			return report, err
		}
		if o.Prepare != nil {
			var release func()
			m, release, err = o.Prepare(ctx, m, s.dir)
			if release != nil {
				pendingRelease = release
			}
			if err != nil {
				return report, err
			}
		}
		before, e := read(ctx)
		if e != nil {
			return report, e
		}
		report.Peak = measurePeak(report.Peak, before)
		engine := o.NewEngine(m, s.dir, o.Worker.Clock)
		if engine == nil {
			return report, errors.New("nil measurement engine")
		}
		engines = append(engines, engine)
		releases = append(releases, pendingRelease)
		pendingRelease = nil
		stage := ModelMeasurement{Model: m.Name, Before: before, Peak: before}
		sample := func(c context.Context) error {
			r, e := read(c)
			if e == nil {
				stage.Peak = measurePeak(stage.Peak, r)
				report.Peak = measurePeak(report.Peak, r)
			}
			return e
		}
		start := o.Worker.Clock.Now()
		e = sampleMeasureOperation(ctx, o.SampleInterval, sample, func(c context.Context) error {
			loadCtx, cancel := context.WithTimeout(c, o.LoadTimeout)
			defer cancel()
			return engine.Load(loadCtx, m)
		})
		stage.LoadSeconds = o.Worker.Clock.Now().Sub(start).Seconds()
		if e == nil && o.WarmupParallel == 1 {
			if w, ok := o.Warmup[m.Name]; ok {
				warmStart := o.Worker.Clock.Now()
				e = sampleMeasureOperation(ctx, o.SampleInterval, sample, func(c context.Context) error { return measureWarmup(c, engine, w) })
				stage.WarmupSeconds = o.Worker.Clock.Now().Sub(warmStart).Seconds()
				report.WarmupSeconds += stage.WarmupSeconds
			}
		}
		stage.Delta = measureDelta(stage.Peak, stage.Before)
		report.Models = append(report.Models, stage)
		if e != nil {
			return report, fmt.Errorf("measure %s: %w", m.Name, e)
		}
	}
	if o.WarmupParallel > 1 {
		start := o.Worker.Clock.Now()
		sample := func(c context.Context) error {
			r, e := read(c)
			if e == nil {
				report.Peak = measurePeak(report.Peak, r)
			}
			return e
		}
		e := sampleMeasureOperation(ctx, o.SampleInterval, sample, func(c context.Context) error {
			return concurrentMeasureWarmups(c, o, engines, report.Models)
		})
		report.WarmupSeconds = o.Worker.Clock.Now().Sub(start).Seconds()
		if e != nil {
			return report, fmt.Errorf("concurrent warmups: %w", e)
		}
	}
	return report, nil
}

// All goroutines join before engines unload; any error cancels every request.
func concurrentMeasureWarmups(ctx context.Context, o MeasureOptions, engines []Engine, stages []ModelMeasurement) error {
	c, cancel := context.WithCancel(ctx)
	defer cancel()
	tasks := make(chan int)
	n := min(o.WarmupParallel, len(engines))
	results := make(chan error, n)
	for range n {
		go func() {
			var err error
			for i := range tasks {
				if c.Err() != nil {
					break
				}
				start := o.Worker.Clock.Now()
				e := measureWarmup(c, engines[i], o.Warmup[stages[i].Model])
				stages[i].WarmupSeconds = o.Worker.Clock.Now().Sub(start).Seconds()
				if e != nil {
					err = fmt.Errorf("%s: %w", stages[i].Model, e)
					cancel()
					break
				}
			}
			results <- err
		}()
	}
send:
	for i := range engines {
		if _, ok := o.Warmup[stages[i].Model]; !ok {
			continue
		}
		select {
		case tasks <- i:
		case <-c.Done():
			break send
		}
	}
	close(tasks)
	var err error
	for range n {
		err = errors.Join(err, <-results)
	}
	return errors.Join(err, ctx.Err())
}

func sampleMeasureOperation(ctx context.Context, interval time.Duration, sample func(context.Context) error, op func(context.Context) error) error {
	c, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- op(c) }()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case e := <-done:
			return errors.Join(e, sample(ctx))
		case <-ctx.Done():
			cancel()
			return errors.Join(ctx.Err(), <-done)
		case <-tick.C:
			if e := sample(c); e != nil {
				cancel()
				return errors.Join(e, <-done)
			}
		}
	}
}

func measureWarmup(ctx context.Context, e Engine, w MeasureWarmup) error {
	method := w.Method
	if method == "" {
		method = "POST"
	}
	if w.Path == "" || !strings.HasPrefix(w.Path, "/") || strings.HasPrefix(w.Path, "//") {
		return errors.New("warmup requires a local absolute request path")
	}
	var r *http.Response
	var err error
	if req, ok := e.(interface {
		CallRequest(context.Context, string, string, []byte, http.Header) (*http.Response, error)
	}); ok {
		r, err = req.CallRequest(ctx, method, w.Path, w.Body, w.Headers)
	} else if method == "POST" && len(w.Headers) == 0 {
		r, err = e.Call(ctx, w.Path, w.Body)
	} else {
		return errors.New("engine does not support requested warmup method/headers")
	}
	if err != nil {
		return err
	}
	if r == nil || r.Body == nil {
		return errors.New("warmup returned no response body")
	}
	defer r.Body.Close()
	_, err = io.Copy(io.Discard, r.Body)
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return fmt.Errorf("warmup HTTP %d", r.StatusCode)
	}
	return err
}

// NewMeasurementSource never converts configured allowances to observations.
// Linux host memory and device counters are mandatory; unsupported probes fail.
func NewMeasurementSource(platform, device string) (func(context.Context) (MemoryReading, error), error) {
	if runtime.GOOS != "linux" {
		return nil, errors.New("native measurement currently requires Linux /proc and CUDA or XPU counters")
	}
	if platform != "cuda" && platform != "xpu" {
		return nil, errors.New("measurement requires native CUDA or XPU counters")
	}
	if device == "" || strings.ContainsAny(device, "\n\r,") {
		return nil, errors.New("one explicit device is required")
	}
	return func(ctx context.Context) (MemoryReading, error) {
		probeguard.Check()
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		host, e := os.ReadFile("/proc/meminfo")
		if e != nil {
			return MemoryReading{}, e
		}
		h, e := parseMeasureHost(host)
		if e != nil {
			return MemoryReading{}, e
		}
		var d float64
		if platform == "cuda" {
			out, e := exec.CommandContext(c, "nvidia-smi", "--id="+device, "--query-gpu=memory.used", "--format=csv,noheader,nounits").Output()
			if e != nil {
				return MemoryReading{}, fmt.Errorf("CUDA used memory: %w", e)
			}
			d, e = parseCUDAFreeMemory(out)
			if e != nil {
				return MemoryReading{}, e
			}
		} else {
			if _, e := strconv.ParseUint(device, 10, 32); e != nil {
				return MemoryReading{}, errors.New("XPU device must be a numeric native device index")
			}
			out, e := exec.CommandContext(c, "xpu-smi", "stats", "-d", device, "--json").Output()
			if e != nil {
				return MemoryReading{}, fmt.Errorf("native XPU used memory unavailable: %w", e)
			}
			d, e = parseMeasureXPU(out, device)
			if e != nil {
				return MemoryReading{}, e
			}

		}
		return MemoryReading{DeviceGB: d, HostGB: h}, nil
	}, nil
}
func parseMeasureHost(data []byte) (float64, error) {
	available, e := parseMemAvailable(data)
	if e != nil {
		return 0, e
	}
	var total float64
	found := false
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) > 0 && f[0] == "MemTotal:" {
			if found || len(f) != 3 || f[2] != "kB" {
				return 0, errors.New("invalid MemTotal")
			}
			n, e := strconv.ParseUint(f[1], 10, 64)
			if e != nil {
				return 0, e
			}
			total = float64(n) / (1024 * 1024)
			found = true
		}
	}
	if !found || total <= 0 || available > total {
		return 0, errors.New("invalid host total/available memory")
	}
	return total - available, nil
}

// xpu-smi reports XPUM_STATS_MEMORY_USED in MiB. Only current device-level
// usage is accepted; historical min/max and tile sums are not substitutions.
func parseMeasureXPU(data []byte, device string) (float64, error) {
	var report struct {
		DeviceID *int            `json:"device_id"`
		Error    json.RawMessage `json:"error"`
		Metrics  []struct {
			Type  string   `json:"metrics_type"`
			Value *float64 `json:"value"`
		} `json:"device_level"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		return 0, err
	}
	id, err := strconv.Atoi(device)
	if err != nil {
		return 0, err
	}
	if report.DeviceID == nil || *report.DeviceID != id || len(report.Error) > 0 {
		return 0, errors.New("XPU statistics missing selected device identity or reports an error")
	}
	var used *float64
	for _, metric := range report.Metrics {
		if metric.Type == "XPUM_STATS_MEMORY_USED" {
			if used != nil || metric.Value == nil || !validMemoryValue(*metric.Value) {
				return 0, errors.New("invalid XPU used-memory metric")
			}
			used = metric.Value
		}
	}
	if used == nil {
		return 0, errors.New("XPU statistics lack current device-level memory used")
	}
	return *used / 1024, nil
}
