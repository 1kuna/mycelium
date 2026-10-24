package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Memory measurements are the whole point's peaks and its idle baseline, in GiB.
// Unified host peaks include device memory already; never add those peaks twice.
type CurveMeasurement struct {
	Units                  float64 `json:"units"`
	Seconds                float64 `json:"seconds"`
	LatencySeconds         float64 `json:"latency_seconds"`
	BaselineHostMemoryGB   float64 `json:"baseline_host_memory_gb"`
	HostMemoryGB           float64 `json:"host_memory_gb"`
	BaselineDeviceMemoryGB float64 `json:"baseline_device_memory_gb"`
	DeviceMemoryGB         float64 `json:"device_memory_gb"`
	OutOfMemory            bool    `json:"out_of_memory"`
}

// Use the same normalized floors/host ledger as ordinary attempts. Serialization
// holds the complete physical host, including siblings, until exact cleanup.
func curveHostCapacity(m *Machine) float64 {
	capacity := m.FreeHostMemoryGB
	if m.EffectiveHostMemoryBudgetGB > 0 {
		capacity = min(capacity, m.EffectiveHostMemoryBudgetGB)
	}
	return capacity
}

func curveFits(m *Machine, needs ResourceNeeds, count int) bool {
	s := Spec{Needs: needs}
	s.Needs.Platform = Platform{Required: []string{m.GPU}}
	if normalizeResources(&s) != nil || count < 1 || count > 64 {
		return false
	}
	n := s.Needs
	if max(1, n.CPUCores)*count > m.EffectiveCPUCores || !m.HostMemoryMeasured || m.HostMemoryError != "" || m.HostMemoryPolicyMismatch {
		return false
	}
	gpu, host := n.MemoryGB*float64(count), n.HostMemoryGB*float64(count)
	if host <= 0 {
		return false
	} // unknown memory cannot authorize even the first point
	if m.GPU != "cpu" && (gpu > m.FreeMemoryGB || m.MemoryError != "") {
		return false
	}
	return hostNeed(m, gpu, host) <= curveHostCapacity(m)
}

// Wall timers wake checks; only the injected Clock decides the sweep budget.
func curveBudget(ctx context.Context, clock Clock, duration time.Duration, monitor *availabilityMonitor) (context.Context, func()) {
	work, cancel := context.WithCancel(ctx)
	stopOwner := context.AfterFunc(monitor.workContext(), cancel)
	deadline := clock.Now().Add(duration)
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			if !clock.Now().Before(deadline) || !monitor.snapshot().Available {
				cancel()
				return
			}
			select {
			case <-work.Done():
				return
			case <-tick.C:
			}
		}
	}()
	return work, func() { stopOwner(); cancel(); <-done }
}

func runReferenceSweep(ctx context.Context, c BenchmarkConfig, e Extension, m *Machine, work string, monitor *availabilityMonitor) []WorkObservation {
	v := e.variant(machineTarget(m))
	if v == nil || !monitor.snapshot().Available {
		return nil
	}
	sweep, ok := v.Sweeps[work]
	if !ok || !sweep.valid() || c.CurveMaxConcurrency <= 0 {
		return nil
	}
	key, _ := curveKey(e, m, work)
	if key == nil {
		return nil
	}
	ctx, stop := curveBudget(ctx, c.Clock, 60*time.Second, monitor)
	defer stop()
	started := c.Clock.Now()
	var out []WorkObservation
	previous := 0.0
	for _, count := range sweep.Counts {
		if count > min(sweep.MaxConcurrency, c.CurveMaxConcurrency) || !curveFits(m, v.Needs, count) || ctx.Err() != nil || !monitor.snapshot().Available || c.Clock.Now().Sub(started) >= 60*time.Second {
			break
		}
		if checkLaunchGate(ctx) != nil {
			break
		}
		k := *key
		k.Concurrency = count
		// Forbidden points are never retried with the same exact identity.
		forbidden := false
		for _, p := range c.PreviousCurves {
			if p.Key == k && p.Forbidden {
				forbidden = true
				break
			}
		}
		if forbidden {
			break
		}
		bounded, cancel := curveBudget(ctx, c.Clock, 10*time.Second, monitor)
		start := c.Clock.Now()
		measure := c.MeasureCurve
		if measure == nil {
			measure = measureCurveReference
		}
		sample, err := measure(bounded, c, e, m, work, count)
		elapsed := c.Clock.Now().Sub(start)
		cancelled := bounded.Err() != nil || ctx.Err() != nil || !monitor.snapshot().Available
		cancel()
		// An observed OOM remains unsafe even when cancellation races it.
		if cancelled && !sample.OutOfMemory {
			break
		}
		p := WorkObservation{Key: k, SampleID: ID(), RecordedAt: c.Clock.Now(), Source: "idle reference", Machine: m.ID, Session: m.Session}
		p.Forbidden = sample.OutOfMemory
		if err != nil && !p.Forbidden {
			break
		}
		if !p.Forbidden {
			if elapsed > 10*time.Second || sample.Units != key.Reference.Units*float64(count) || !validRate(sample.Seconds) || sample.Seconds > 10 || !validRate(sample.LatencySeconds) {
				break
			}
			memoryOK := true
			for _, n := range []float64{sample.BaselineHostMemoryGB, sample.HostMemoryGB, sample.BaselineDeviceMemoryGB, sample.DeviceMemoryGB} {
				memoryOK = memoryOK && validMemoryValue(n)
			}
			if !memoryOK || !validRate(sample.HostMemoryGB) || m.GPU != "cpu" && !validRate(sample.DeviceMemoryGB) || sample.HostMemoryGB < sample.BaselineHostMemoryGB || sample.DeviceMemoryGB < sample.BaselineDeviceMemoryGB {
				break
			}
			p.Units, p.Seconds = sample.Units, sample.Seconds
			p.Rate, p.LatencySeconds = sample.Units/sample.Seconds, sample.LatencySeconds
			p.HostMemoryGB, p.DeviceMemoryGB = sample.HostMemoryGB, sample.DeviceMemoryGB
			p.IncrementalHostMemoryGB = sample.HostMemoryGB - sample.BaselineHostMemoryGB
			p.IncrementalDeviceMemoryGB = sample.DeviceMemoryGB - sample.BaselineDeviceMemoryGB
			p.Forbidden = p.IncrementalHostMemoryGB > curveHostCapacity(m) || m.GPU != "cpu" && p.IncrementalDeviceMemoryGB > m.FreeMemoryGB
			p.Samples = 1
		}
		out = append(out, p)
		if p.Forbidden || previous > 0 && p.Rate <= previous {
			break
		}
		previous = p.Rate
	}
	return out
}

// All filesystem work and measured commands run on the worker, outside q.mu.
func measureCurveReference(ctx context.Context, c BenchmarkConfig, e Extension, m *Machine, work string, count int) (CurveMeasurement, error) {
	var sample CurveMeasurement
	unlock, err := extensionLock(c.ExtensionRoot, e, m.GPU, false)
	if err != nil {
		return sample, err
	}
	defer unlock()
	actual, err := ReadExtension(extensionFolder(c.ExtensionRoot, e))
	if err != nil || actual.Digest != e.Digest {
		return sample, errors.New("extension changed")
	}
	selector := m.Device
	for _, d := range m.Devices {
		if d.ID == m.Device {
			selector = d.Selector
		}
	}
	fingerprint, err := ExtensionFingerprint(ctx, m.GPU, selector)
	if err != nil || fingerprint != m.ExtensionFingerprint {
		return sample, errors.New("hardware/driver fingerprint changed")
	}
	current := *m
	current.ExtensionReports = LocalExtensionReports(c.ExtensionRoot, m.GPU, m.ExtensionFingerprint, m)
	if k, _ := curveKey(e, &current, work); k == nil {
		return sample, errors.New("qualification changed")
	}
	v := e.variant(machineTarget(m))
	baseline := v.Baselines[work]
	env, err := extensionEnvironment(c.ExtensionRoot, e, m.GPU, m.Device)
	if err != nil {
		return sample, err
	}
	// The inventory's stable ID is not necessarily the backend selector.
	for _, d := range m.Devices {
		if d.ID == m.Device {
			model, e := WithModelDevice(Model{}, m.GPU, d.Selector)
			if e != nil {
				return sample, e
			}
			for k, v := range model.DeviceEnv {
				env[k] = v
			}
		}
	}
	command := slices.Clone(v.Sweeps[work].Command)
	if recipe, ok := extensionRecipe(e, m); ok {
		if c.EnvironmentCache == nil {
			return sample, errors.New("prepared environment unavailable")
		}
		recipe.readyOnly = true
		root, release, err := c.EnvironmentCache.Acquire(ctx, recipe)
		if err != nil {
			return sample, err
		}
		defer release()
		bindEnvironment(env, root)
		command = expandArgs(command, executionReplacer(env["MYCE_EXTENSION_DIR"], root, v.Execution.Main.Environment, machineTarget(m)))
	}
	tmp, err := os.MkdirTemp(env["MYCE_EXTENSION_CACHE"], "curve-")
	if err != nil {
		return sample, err
	}
	defer os.RemoveAll(tmp)
	env["MYCE_CHECK_INPUT"] = filepath.Join(env["MYCE_EXTENSION_DIR"], baseline.Input)
	env["MYCE_CHECK_OUTPUT_DIR"] = filepath.Join(tmp, "outputs")
	if err = os.Mkdir(env["MYCE_CHECK_OUTPUT_DIR"], 0700); err != nil {
		return sample, err
	}
	env["MYCE_CHECK_OUTPUT"] = filepath.Join(env["MYCE_CHECK_OUTPUT_DIR"], "0")
	env["MYCE_CHECK_WORK"] = work
	env["MYCE_BENCHMARK_OUTPUT"] = filepath.Join(tmp, "measurement.json")
	env["MYCE_BENCHMARK_CONCURRENCY"] = fmt.Sprint(count)
	command = expandArgs(command, strings.NewReplacer("{extension}", env["MYCE_EXTENSION_DIR"], "{cache}", env["MYCE_EXTENSION_CACHE"]))
	runErr := runExtensionCommand(ctx, command, env["MYCE_EXTENSION_DIR"], env, io.Discard, commandScope{StateDir: c.StateDir, Clock: c.Clock})
	f, err := os.Open(env["MYCE_BENCHMARK_OUTPUT"])
	if err != nil {
		return sample, errors.Join(runErr, err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(data) > 4096 {
		return sample, errors.New("measurement unavailable")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&sample); err != nil || decoder.Decode(new(any)) != io.EOF {
		return CurveMeasurement{}, errors.New("invalid curve measurement")
	}
	if sample.OutOfMemory {
		return sample, runErr
	}
	if runErr != nil {
		return CurveMeasurement{}, runErr
	}
	for i := 0; i < count; i++ {
		output, err := os.Open(filepath.Join(env["MYCE_CHECK_OUTPUT_DIR"], fmt.Sprint(i)))
		if err != nil {
			return CurveMeasurement{}, err
		}
		data, err = io.ReadAll(io.LimitReader(output, extensionMaxBytes+1))
		output.Close()
		if err != nil || len(data) > extensionMaxBytes {
			return CurveMeasurement{}, errors.New("reference output unavailable")
		}
		if err = compareExtensionOutput(data, e.Files[baseline.Output], baseline); err != nil {
			return CurveMeasurement{}, err
		}
	}
	return sample, nil
}
