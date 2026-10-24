package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// Throughput names its exact reference and unit. Unknown/failed measurements
// have no rate; they never masquerade as a slow or CPU-backed accelerator.
type Throughput struct {
	ReferenceUnits  float64 `json:"reference_units,omitempty"`
	Resource        string  `json:"resource"`
	Method          string  `json:"method"`
	Work            string  `json:"work,omitempty"`
	ExtensionDigest string  `json:"extension_digest,omitempty"`
	Rate            float64 `json:"rate,omitempty"`
	Unit            string  `json:"unit"`
	Seconds         float64 `json:"seconds,omitempty"`
	Reason          string  `json:"reason,omitempty"`
}
type BenchmarkResult struct {
	Curves       []WorkObservation `json:"curves,omitempty"`
	Fingerprint  string            `json:"fingerprint"`
	RecordedAt   time.Time         `json:"recorded_at"`
	Seconds      float64           `json:"seconds"`
	State        string            `json:"state"`
	Measurements []Throughput      `json:"measurements"`
}

func (b *BenchmarkResult) Summary() string {
	if b == nil {
		return "throughput pending"
	}
	s := fmt.Sprintf("benchmark %s (%.2fs)", b.State, b.Seconds)
	for _, m := range b.Measurements {
		if validRate(m.Rate) {
			s += fmt.Sprintf("; %s %s %.2f %s", m.Resource, m.Work, m.Rate, m.Unit)
		} else {
			s += fmt.Sprintf("; %s unavailable: %s", m.Resource, m.Reason)
		}
	}
	return s
}
func validRate(v float64) bool { return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }

type BenchmarkConfig struct {
	CurveMaxConcurrency     int
	EnvironmentCache        *EnvironmentCache
	PreviousCurves          []WorkObservation
	MeasureCurve            func(context.Context, BenchmarkConfig, Extension, *Machine, string, int) (CurveMeasurement, error)
	Clock                   Clock
	Machine                 Machine
	StateDir, ExtensionRoot string

	Client                    Client
	DailyUse, Laptop, Desktop bool
	OwnerSignals              OwnerSignalSource
	// Measure is an injected resource runner for laptop tests. Calls are serial.
	Measure func(context.Context, string) ([]Throughput, error)
}

// RunOnboardingBenchmark runs before admission, never concurrently with jobs.
// Setup/download/build is an explicit extension operation, outside this budget.
// A repeat on unchanged hardware, drivers, extensions and store reuses results.
func RunOnboardingBenchmark(ctx context.Context, c BenchmarkConfig, p *CapabilityProfile) error {
	var err error
	if c.Clock == nil {
		c.Clock = RealClock{}
	}
	if c.OwnerSignals == nil {
		c.OwnerSignals = LocalOwnerSignals
	}
	if err := PrepareStateRoots(c.StateDir); err != nil {
		return err
	}
	lockDir := c.StateDir
	if err := os.MkdirAll(lockDir, 0700); err != nil {
		return err
	}
	unlock, err := commandLock(lockDir, "benchmark.lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(ctx, 9*time.Minute)
	defer cancel()
	identityCtx, identityCancel := context.WithTimeout(ctx, 15*time.Second)
	identity, identityErr := ExtensionFingerprint(identityCtx, c.Machine.GPU, c.Machine.Device)
	identityCancel()
	extensions, err := LocalExtensions(c.ExtensionRoot)
	if c.ExtensionRoot == "" {
		extensions = nil
		err = nil
	}
	if err != nil {
		return err
	}
	digests := []string{}
	for _, e := range extensions {
		digests = append(digests, e.Digest)
	}
	slices.Sort(digests)
	raw, _ := json.Marshal([]any{"onboarding-v2", c.CurveMaxConcurrency, observationAllowance(&c.Machine), c.Machine.Devices, identity, p.OS, p.Arch, p.LogicalCPUs, p.SystemMemoryBytes, p.Accelerators, p.DiskTotalBytes, c.Client.URL, digests})
	sum := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(sum[:])
	var saved CapabilityProfile
	if data, err := os.ReadFile(filepath.Join(c.StateDir, "capability-profile.json")); err == nil {
		_ = json.Unmarshal(data, &saved)
	}
	if saved.Benchmark != nil {
		for _, point := range saved.Benchmark.Curves {
			found := false
			for _, current := range c.PreviousCurves {
				if current.Key == point.Key {
					found = true
					break
				}
			}
			if !found {
				c.PreviousCurves = append(c.PreviousCurves, point)
			}
		}
	}
	// Retain historical local evidence, but never present an old identity as current.
	targets := (&Queue{}).machineTargets(&c.Machine)
	for i, point := range c.PreviousCurves {
		reason := "recipe or component changed"
		for _, target := range targets {
			component := "cpu"
			if target.GPU != "cpu" {
				component = target.Device
			}
			if point.Key.Host != hostID(target) || point.Key.Component != component {
				continue
			}
			for _, e := range extensions {
				if e.Digest == point.Key.ExtensionDigest {
					reason = curveStaleReason(point, e, target, c.Clock.Now())
					break
				}
			}
		}
		c.PreviousCurves[i].StaleReason = reason
	}
	if identityErr == nil && saved.Benchmark != nil && saved.Benchmark.Fingerprint == fingerprint && saved.Benchmark.State == "complete" && c.Clock.Now().Sub(saved.Benchmark.RecordedAt) <= 30*24*time.Hour {
		p.Benchmark = saved.Benchmark
		p.Benchmark.Curves = c.PreviousCurves
		p.Onboarding = "measured; qualified reference replay"
		return nil
	}

	start := c.Clock.Now()
	result := &BenchmarkResult{Fingerprint: fingerprint, RecordedAt: start, State: "partial", Curves: slices.Clone(c.PreviousCurves)}
	p.Benchmark = result
	p.Onboarding = "measured; qualified reference replay"
	monitor := newAvailabilityMonitor(c.Clock, c.DailyUse, c.Laptop, c.Desktop)
	sample := func() {
		probe, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		monitor.sample(c.OwnerSignals(probe))
	}
	sample()
	wake := make(chan struct{}, 1)
	watch, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	done := make(chan struct{})
	go func() { defer close(done); monitor.run(watch, c.OwnerSignals, wake) }()
	defer func() { stopWatch(); <-done }()
	// Owner activity is a wait, never permission to run a reduced benchmark.
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for !monitor.snapshot().Available {
		select {
		case <-ctx.Done():
			result.Measurements = append(result.Measurements, Throughput{Resource: "benchmark", Reason: monitor.snapshot().Reason})
			return saveBenchmark(c, p, start)
		case <-tick.C:
		}
	}
	work, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-work.Done():
				return
			case <-tick.C:
				if !monitor.snapshot().Available {
					cancelWork()
					return
				}
			}
		}
	}()
	defer func() { cancelWork(); <-stopped }()
	measure := c.Measure
	if measure == nil {
		measure = func(ctx context.Context, resource string) ([]Throughput, error) {
			return measureResource(ctx, c, extensions, identity, resource)
		}
	}
	for _, resource := range []string{"cpu", "memory", "disk", "network", "accelerator"} {
		if work.Err() != nil {
			result.Measurements = append(result.Measurements, Throughput{Resource: resource, Reason: "benchmark interrupted; retry on next join"})
			continue
		}
		if err := checkLaunchGate(work); err != nil {
			return err
		}
		bounded, stop := context.WithTimeout(work, 60*time.Second)
		measurements, e := measure(bounded, resource)
		stop()
		if e != nil {
			measurements = []Throughput{{Resource: resource, Reason: "measurement unavailable"}}
		}
		if len(measurements) == 0 {
			measurements = []Throughput{{Resource: resource, Reason: "no qualified reference work"}}
		}
		for i := range measurements {
			if !validRate(measurements[i].Rate) {
				measurements[i].Rate = 0
				if measurements[i].Reason == "" {
					measurements[i].Reason = "invalid measurement"
				}
			}
		}
		result.Measurements = append(result.Measurements, measurements...)
	}
	if work.Err() == nil && c.CurveMaxConcurrency > 0 {
		// Snapshot target views without queue mutation or extra host allowances.
		for _, target := range targets {
			for _, e := range extensions {
				v := e.variant(machineTarget(target))
				if v == nil {
					continue
				}
				for _, name := range slices.Sorted(maps.Keys(v.Sweeps)) {
					for _, point := range runReferenceSweep(work, c, e, target, name, monitor) {
						result.Curves = slices.DeleteFunc(result.Curves, func(old WorkObservation) bool { return old.Key == point.Key })
						result.Curves = append(result.Curves, point)
					}
				}
			}
		}
	}
	if work.Err() == nil {
		result.State = "complete"
		for _, m := range result.Measurements {
			if !validRate(m.Rate) {
				result.State = "partial"
			}
		}
	}
	return saveBenchmark(c, p, start)
}
func saveBenchmark(c BenchmarkConfig, p *CapabilityProfile, start time.Time) error {
	p.Benchmark.Seconds = max(0, c.Clock.Now().Sub(start).Seconds())
	return saveWorkerJSON(c.StateDir, "capability-profile.json", p)
}

func measureResource(ctx context.Context, c BenchmarkConfig, extensions []Extension, identity, resource string) ([]Throughput, error) {
	if resource == "accelerator" {
		return measureReferences(ctx, c, extensions, identity)
	}
	start := c.Clock.Now()
	n := int64(0)
	unit := "bytes/s"
	method := ""
	switch resource {
	case "cpu":
		method = "sha256-8MiB-single-thread-v1"
		buf := make([]byte, 8<<20)
		for c.Clock.Now().Sub(start) < 250*time.Millisecond && ctx.Err() == nil {
			sum := sha256.Sum256(buf)
			buf[0] = sum[0]
			n += int64(len(buf))
		}
	case "memory":
		method = "copy-64MiB-v1"
		a, b := make([]byte, 64<<20), make([]byte, 64<<20)
		for c.Clock.Now().Sub(start) < 250*time.Millisecond && ctx.Err() == nil {
			n += int64(copy(a, b))
			a, b = b, a
		}
		// Keep the copies observable to the compiler.
		if a[len(a)-1] != 0 {
			return nil, errors.New("memory copy mismatch")
		}
	case "disk":
		method = "write-sync-read-16MiB-v1"
		f, err := os.CreateTemp(c.StateDir, "benchmark-disk-")
		if err != nil {
			return nil, err
		}
		defer os.Remove(f.Name())
		defer f.Close()
		buf := make([]byte, 16<<20)
		written, err := f.Write(buf)
		if err != nil {
			return nil, err
		}
		if err = f.Sync(); err != nil {
			return nil, err
		}
		if _, err = f.Seek(0, 0); err != nil {
			return nil, err
		}
		read, err := io.Copy(io.Discard, f)
		if err != nil {
			return nil, err
		}
		n = int64(written) + read
	case "network":
		method = "file-store-download-8MiB-sha256-v1"
		// A fixed synthetic object is reused rather than accumulating benchmark data.
		f, err := os.CreateTemp(c.StateDir, "benchmark-network-")
		if err != nil {
			return nil, err
		}
		defer os.Remove(f.Name())
		_, err = f.Write(make([]byte, 8<<20))
		err = errors.Join(err, f.Close())
		if err != nil {
			return nil, err
		}
		ref, err := IdentifyFile(f.Name())
		if err != nil {
			return nil, err
		}
		if err = c.Client.UploadFile(ctx, f.Name(), ref); err != nil {
			return nil, err
		}
		start = c.Clock.Now()
		res, err := c.Client.fileRequest(ctx, "GET", "/api/files/blobs/"+ref.SHA256, nil, nil)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			return nil, errors.New("file store download failed")
		}
		h := sha256.New()
		n, err = io.Copy(h, io.LimitReader(res.Body, ref.SizeBytes+1))
		if err != nil || n != ref.SizeBytes || hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
			return nil, errors.New("file store hash mismatch")
		}
	default:
		return nil, errors.New("unknown resource")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	seconds := c.Clock.Now().Sub(start).Seconds()
	if seconds <= 0 {
		return nil, errors.New("clock did not advance")
	}
	return []Throughput{{Resource: resource, Method: method, Rate: float64(n) / seconds, Seconds: seconds, Unit: unit}}, nil
}

func measureReferences(ctx context.Context, c BenchmarkConfig, extensions []Extension, identity string) ([]Throughput, error) {
	out := []Throughput{}
	for _, e := range extensions {
		reports := LocalExtensionReports(c.ExtensionRoot, c.Machine.GPU, identity)
		for _, r := range reports {
			if r.Digest != e.Digest || !r.Qualified || len(e.Benchmark) == 0 {
				continue
			}
			m := Throughput{Resource: "accelerator", Method: "qualified-reference-v1", Work: r.Work, ExtensionDigest: e.Digest, Unit: "references/s"}
			// Reuse the frozen check and compare every timed output to its native reference.
			b, ok := e.baseline(r.Work, c.Machine.GPU)
			if !ok {
				continue
			}
			err := func() error {
				unlock, err := extensionLock(c.ExtensionRoot, e, c.Machine.GPU, false)
				if err != nil {
					return err
				}
				defer unlock()
				qualified := false
				for _, current := range LocalExtensionReports(c.ExtensionRoot, c.Machine.GPU, identity) {
					if current.Digest == e.Digest && current.Work == r.Work && current.Qualified {
						qualified = true
					}
				}
				if !qualified {
					return errors.New("qualification changed")
				}
				actual, err := ReadExtension(extensionFolder(c.ExtensionRoot, e))
				if err != nil || actual.Digest != e.Digest {
					return errors.New("extension changed")
				}
				env, err := extensionEnvironment(c.ExtensionRoot, e, c.Machine.GPU, c.Machine.Device)
				if err != nil {
					return err
				}
				tmp, err := os.MkdirTemp(env["MYCE_EXTENSION_CACHE"], "benchmark-")
				if err != nil {
					return err
				}
				defer os.RemoveAll(tmp)
				env["MYCE_CHECK_INPUT"] = filepath.Join(env["MYCE_EXTENSION_DIR"], b.Input)
				env["MYCE_CHECK_OUTPUT"] = filepath.Join(tmp, "actual")
				env["MYCE_CHECK_WORK"] = r.Work
				env["MYCE_BENCHMARK_OUTPUT"] = filepath.Join(tmp, "timing.json")
				if err = runExtensionCommand(ctx, e.Benchmark, env["MYCE_EXTENSION_DIR"], env, io.Discard, commandScope{StateDir: c.StateDir, Clock: c.Clock}); err != nil {
					return err
				}
				data, err := os.ReadFile(env["MYCE_CHECK_OUTPUT"])
				if err != nil || len(data) > extensionMaxBytes {
					return errors.New("reference output unavailable")
				}
				if err = compareExtensionOutput(data, e.Files[b.Output], b); err != nil {
					return err
				}
				data, err = os.ReadFile(env["MYCE_BENCHMARK_OUTPUT"])
				if err != nil || len(data) > 4096 {
					return errors.New("reference timing unavailable")
				}
				var timing struct {
					Units          float64 `json:"units"`
					Seconds        float64 `json:"seconds"`
					Unit           string  `json:"unit"`
					ReferenceUnits float64 `json:"reference_units"`
				}
				decoder := json.NewDecoder(bytes.NewReader(data))
				decoder.DisallowUnknownFields()
				if err = decoder.Decode(&timing); err != nil {
					return err
				}
				if decoder.Decode(new(any)) != io.EOF {
					return errors.New("invalid reference timing document")
				}
				if !validRate(timing.Units) || !validRate(timing.Seconds) || !validRate(timing.ReferenceUnits) || timing.Seconds > 60 || (timing.Unit != "references/s" && timing.Unit != "tokens/s") {
					return errors.New("invalid reference timing")
				}
				m.Seconds = timing.Seconds
				m.Rate = timing.Units / timing.Seconds
				m.Unit = timing.Unit
				m.ReferenceUnits = timing.ReferenceUnits
				return nil
			}()
			if err != nil {
				m.Reason = "qualified reference replay unavailable"
				m.Rate = 0
			}
			out = append(out, m)
		}
	}
	return out, nil
}

// Compare only the same frozen work/reference and units. Memory/eligibility,
// project fairness, priority and start authorization remain outside this score.
// With no program-size model yet, one reference is the provisional work estimate.
func (q *Queue) benchmarkBetter(j *Job, a, b *Machine) (better, known bool) {
	ae, be := qualifiedExtension(j, a), qualifiedExtension(j, b)
	if ae == nil || be == nil {
		return false, false
	}
	find := func(m *Machine, e *Extension) *Throughput {
		if m.CapabilityProfile == nil || m.CapabilityProfile.Benchmark == nil {
			return nil
		}
		for _, r := range m.CapabilityProfile.Benchmark.Measurements {
			if r.Resource == "accelerator" && r.Work == j.Spec.workType() && r.ExtensionDigest == e.Digest && r.Method == "qualified-reference-v1" && (r.Unit == "references/s" || r.Unit == "tokens/s") && validRate(r.Rate) {
				return &r
			}
		}
		return nil
	}
	ar, br := find(a, ae), find(b, be)
	predict := func(m *Machine, r *Throughput) (float64, bool) {
		seconds := max(1, r.ReferenceUnits)/r.Rate + q.loadSeconds(j.Spec.Needs.Model, m) + environmentSeconds(j, m)
		bytes := int64(0)
		for _, f := range j.Spec.Files {
			bytes += f.SizeBytes
		}
		if bytes > 0 {
			found := false
			for _, n := range m.CapabilityProfile.Benchmark.Measurements {
				if n.Resource == "network" && n.Method == "file-store-download-8MiB-sha256-v1" && n.Unit == "bytes/s" && validRate(n.Rate) {
					seconds += float64(bytes) / n.Rate
					found = true
					break
				}
			}
			if !found {
				return 0, false
			}
		}
		return seconds, true
	}
	as, bs := 0.0, 0.0
	aok, bok := false, false
	if ar != nil {
		as, aok = predict(a, ar)
	}
	if br != nil {
		bs, bok = predict(b, br)
	}
	// Keep a total order even when some hyphae have not measured this work.
	// Pairwise memory fallbacks mixed with speed comparisons can form a cycle.
	if aok != bok {
		return aok, true
	}
	if !aok {
		return false, false
	}
	ag := fmt.Sprintf("%s/%s/%g", ae.Digest, ar.Unit, ar.ReferenceUnits)
	bg := fmt.Sprintf("%s/%s/%g", be.Digest, br.Unit, br.ReferenceUnits)
	if ag != bg {
		return ag < bg, true
	} // Incomparable references have a stable group order.
	if as == bs {
		return a.ID < b.ID, true
	}
	return as < bs, true
}
func (q *Queue) capabilityPlacementBetter(j *Job, a, b *Machine) bool {
	if j.Spec.Needs.cpuOnly() && (a.GPU == "cpu") != (b.GPU == "cpu") {
		return a.GPU == "cpu"
	}
	if better, known := q.benchmarkBetter(j, a, b); known {
		return better
	}
	return q.unmeasuredProgram(j, a) && capabilityBetter(a, b)
}
