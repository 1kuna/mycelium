package myce

import (
	"context"
	"encoding/csv"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const attemptMemorySamplePeriod = 200 * time.Millisecond

func (a *memoryAttempt) peak() *AttemptMemoryPeak {
	if a == nil {
		return nil
	}
	peak := &AttemptMemoryPeak{}
	readGiB := func(name string) *float64 {
		data, err := a.fs.ReadFile(filepath.Join(a.path, name))
		n, parseErr := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err != nil || parseErr != nil {
			return nil
		}
		gb := float64(n) / (1 << 30)
		return &gb
	}
	peak.CgroupGB = readGiB("memory.peak")
	peak.SwapGB = readGiB("memory.swap.peak")
	if peak.SwapGB == nil {
		peak.SwapGB = readGiB("memory.swap.current")
	}
	if data, err := a.fs.ReadFile(filepath.Join(a.path, "memory.stat")); err == nil {
		stats := memoryCounters(data)
		if gb, ok := nonreclaimableMemoryGB(stats); ok {
			peak.HostGB = &gb
			if device := a.device.Load(); device != nil && device.pinnedGB > 0 {
				combined := gb + device.pinnedGB
				peak.CombinedHostGB = &combined
			}
		}
		if out := stats["pswpout"]; out > 0 {
			peak.Pressure = &MemoryPressureEvidence{CgroupSwapOutPages: out}
		}
	}
	if data, err := a.fs.ReadFile(filepath.Join(a.path, "memory.events")); err == nil {
		events := memoryCounters(data)
		peak.Events = &MemoryEventCounts{High: events["high"], Max: events["max"], OOM: events["oom"], OOMKill: events["oom_kill"], OOMGroupKill: events["oom_group_kill"]}
	}
	if peak.HostGB == nil && peak.CgroupGB == nil && peak.SwapGB == nil && peak.Events == nil {
		return nil
	}
	if a.availableAtLaunch != nil {
		if available, ok := a.hostAvailable(); ok {
			combined := max(memoryValue(peak.CombinedHostGB), memoryValue(peak.HostGB), *a.availableAtLaunch-available)
			peak.CombinedHostGB = &combined
		}
	}
	peak.Constrained = pooledSampleLimited(peak)
	return peak
}

func (a *memoryAttempt) hostAvailable() (float64, bool) {
	data, err := a.fs.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	available, err := parseMemAvailable(data)
	return available, err == nil
}

func memoryCounters(b []byte) map[string]uint64 {
	values := map[string]uint64{}
	fields := strings.Fields(string(b))
	for i := 0; i+1 < len(fields); i += 2 {
		if n, err := strconv.ParseUint(fields[i+1], 10, 64); err == nil {
			values[fields[i]] = n
		}
	}
	return values
}

func nonreclaimableMemoryGB(stats map[string]uint64) (float64, bool) {
	anon, aok := stats["anon"]
	shmem, sok := stats["shmem"]
	kernel, kok := stats["kernel"]
	return (float64(anon) + float64(shmem) + float64(kernel)) / (1 << 30), aok && sok && kok
}

// Keep pressure evidence across samples, rather than training from a later
// unconstrained-looking counter snapshot after memory has been reclaimed.
func sampledMemoryPeak(old, next *AttemptMemoryPeak) *AttemptMemoryPeak {
	if next == nil {
		return old
	}
	copy := *next
	copy.Constrained = false
	previous := old
	if old != nil {
		p := *old
		p.Constrained = false
		previous = &p
	}
	out := mergeMemoryPeaks(previous, &copy)
	if out == nil {
		out = &AttemptMemoryPeak{Events: mergeMemoryEvents(nil, next.Events)}
		if old != nil && old.Events != nil {
			out.Events = mergeMemoryEvents(old.Events, next.Events)
		}
	}
	for _, peak := range []*AttemptMemoryPeak{old, next} {
		if peak != nil && peak.SwapGB != nil && validMemoryValue(*peak.SwapGB) && (out.SwapGB == nil || *peak.SwapGB > *out.SwapGB) {
			gb := *peak.SwapGB
			out.SwapGB = &gb
		}
	}
	out.Constrained = next.Constrained || old != nil && old.Constrained
	out.PressureUnavailable = next.PressureUnavailable || old != nil && old.PressureUnavailable
	if next.Pressure != nil {
		e := *next.Pressure
		out.Pressure = &e
	}
	if old != nil && old.Pressure != nil {
		if out.Pressure == nil {
			e := *old.Pressure
			out.Pressure = &e
		} else {
			out.Pressure.CgroupRefaultAnon = max(out.Pressure.CgroupRefaultAnon, old.Pressure.CgroupRefaultAnon)
			out.Pressure.CgroupSwapOutPages = max(out.Pressure.CgroupSwapOutPages, old.Pressure.CgroupSwapOutPages)
			if old.Constrained {
				out.Pressure.Reason = old.Pressure.Reason
			}
		}
	}
	return out
}

func (a *memoryAttempt) processIDs(ids map[int]bool) error {
	if a == nil {
		return nil
	}
	b, err := a.fs.ReadFile(filepath.Join(a.path, "cgroup.procs"))
	if err != nil {
		return err
	}
	for _, s := range strings.Fields(string(b)) {
		pid, e := strconv.Atoi(s)
		if e != nil {
			return e
		}
		ids[pid] = true
	}
	entries, err := a.fs.ReadDir(a.path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			if err := (&memoryAttempt{fs: a.fs, path: filepath.Join(a.path, entry.Name())}).processIDs(ids); err != nil {
				return err
			}
		}
	}
	return nil
}

// processMemoryProbe samples attempts on hosts without cgroup accounting.
type processMemoryProbe struct {
	// OwnedResident uses launch birth identities and the cleanup session boundary.
	OwnedResident func(map[int]string) (uint64, int, error)
	// OwnedFootprint sums phys_footprint and RSS over the same owned tree in
	// one pass. Metal buffers appear in the footprint and not in RSS. Nil
	// where it cannot be read.
	OwnedFootprint func(map[int]string) (footprint, resident uint64, members int, err error)
	// Resident bytes summed over live members of the process groups, and how
	// many members were found.
	Resident func(groups map[int]bool) (uint64, int, error)
	Pressure func(context.Context) (*gpuHostState, error)
}

func processGroupResident(out []byte, groups map[int]bool) (uint64, int, error) {
	var total uint64
	members := 0
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		pgid, e1 := strconv.Atoi(fields[1])
		kib, e2 := strconv.ParseUint(fields[2], 10, 64)
		if e1 != nil || e2 != nil {
			return 0, 0, errors.New("invalid process inventory")
		}
		if groups[pgid] {
			total += kib << 10
			members++
		}
	}
	return total, members, nil
}

// Without cgroups (macOS), reuse the birth-fenced cleanup session boundary.
// Periodic RSS sums cover concurrent members; rusage reports only the largest
// waited process. Shared pages count once per process; compression, deliberate
// session escape and spikes between observations prevent a hard bound. Host pressure rejects this sample's learning, but cannot
// attribute the cause to this attempt or invalidate other hosts' learned peaks.
type attemptMemorySampleSink func(start, at time.Time, sample *AttemptMemoryPeak, period time.Duration, device bool)

func (w Worker) observeProcessMemoryWith(groups func() []int, sink attemptMemorySampleSink, attempts ...*process) func() *AttemptMemoryPeak {
	var attempt *process
	if len(attempts) > 0 {
		attempt = attempts[0]
	}
	probe := w.processMemory
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	var peak *AttemptMemoryPeak
	var growth processMemoryGrowth
	var guard rssGuard
	if attempt != nil {
		guard.limit, _ = w.jobCeilingBytes(attempt.job)
	}
	footprint := attempt != nil && probe.OwnedFootprint != nil
	sample := func(enforce bool) {
		started := clock.Now()
		var bytes uint64
		var members int
		var err error
		if attempt != nil && (footprint || probe.OwnedResident != nil) {
			roots := attempt.rssRoots()
			quality := "owned_session_rss"
			if footprint {
				bytes, _, members, err = probe.OwnedFootprint(roots)
				quality = footprintGuardQuality
			} else {
				bytes, members, err = probe.OwnedResident(roots)
			}
			if enforce {
				if e := guard.observe(clock.Now(), roots, bytes, members, err); e != nil {
					e.Quality = quality
					guard.exceeded = attempt.stopRSS(clock.Now(), roots, e)
				}
			}
		} else if probe.Resident != nil {
			if sink != nil && attempt != nil {
				bytes, members, err = verifiedProcessGroupResident(probe, attempt.rssRoots(), processBirth)
			} else {
				ids := map[int]bool{}
				for _, id := range groups() {
					if id > 0 {
						ids[id] = true
					}
				}
				bytes, members, err = probe.Resident(ids)
			}
		}
		if err != nil || members == 0 {
			growth = processMemoryGrowth{}
			if attempt != nil {
				attempt.rssMemory.Store(nil)
			}
			if sink != nil && enforce {
				sink(started, clock.Now(), &AttemptMemoryPeak{}, processMemorySamplePeriod, false)
			}
			return
		}
		at := clock.Now()
		gb := float64(bytes) / (1 << 30)
		margin := growth.observe(at, gb)
		if attempt != nil {
			attempt.rssMemory.Store(&processResidentSample{gb: gb, growth: margin, rate: growth.rate, at: at, observed: growth.horizon > 0})
		}
		next := &AttemptMemoryPeak{HostGB: &gb, Source: processGroupMemorySource, GrowthGB: &margin, Constrained: guard.exceeded}
		if footprint {
			next.DeviceSource = ownedFootprintSource
		}
		if sink != nil && enforce {
			sink(started, at, next, processMemorySamplePeriod, false)
		}
		peak = sampledMemoryPeak(peak, next)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(processMemorySamplePeriod)
		defer tick.Stop()
		for {
			sample(true)
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	var once sync.Once
	return func() *AttemptMemoryPeak {
		once.Do(func() {
			cancel()
			<-done
			sample(false)
		})
		return peak
	}
}

// Raise a process-group peak to the root's exit max RSS, which catches a
// spike between samples in the main process. A Metal footprint peak is left
// alone: its RSS and beyond-RSS shares sum to the sampled footprint, and
// raising one share alone would count the same pages twice.
func withExitMaxRSS(peak *AttemptMemoryPeak, state *os.ProcessState) *AttemptMemoryPeak {
	if peak == nil || peak.Source != processGroupMemorySource || (peak.DeviceSource == deviceFootprintSource || peak.DeviceSource == ownedFootprintSource) || state == nil {
		return peak
	}
	ru, ok := state.SysUsage().(*syscall.Rusage)
	if !ok {
		return peak
	}
	if gb := rusageMaxRSSBytes(ru) / (1 << 30); validRate(gb) && (peak.HostGB == nil || gb > *peak.HostGB) {
		peak.HostGB = &gb
	}
	return peak
}

// Sample only processes inside this exact attempt. Driver/device totals include
// other workloads and cannot serve as job peaks. Sampling is a lower bound.
// groups names the attempt's process groups for hosts without cgroups.
func (w Worker) observeAttemptMemory(a *memoryAttempt, j Job, groups func() []int, attempts ...*process) func() *AttemptMemoryPeak {
	return w.observeAttemptMemoryEvery(a, j, groups, deviceMemorySamplePeriod, attempts...)
}

func (w Worker) observeAttemptMemoryEvery(a *memoryAttempt, j Job, groups func() []int, devicePeriod time.Duration, attempts ...*process) func() *AttemptMemoryPeak {
	return w.observeAttemptMemoryWith(a, j, groups, devicePeriod, nil, attempts...)
}

func (w Worker) observeAttemptMemoryWith(a *memoryAttempt, j Job, groups func() []int, devicePeriod time.Duration, sink attemptMemorySampleSink, attempts ...*process) func() *AttemptMemoryPeak {
	deviceTelemetry := !j.Spec.Needs.cpuOnly() && backendFor(w.Machine.GPU).attemptMemory != nil
	if a == nil {
		if w.processMemory != nil && groups != nil {
			return w.observeProcessMemoryWith(groups, sink, attempts...)
		}
		if deviceTelemetry {
			return func() *AttemptMemoryPeak { return &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryNoCgroup} }
		}
		return func() *AttemptMemoryPeak { return nil }
	}
	ctx, cancel := context.WithCancel(context.Background())
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	peak := a.peak()
	hostDone := make(chan struct{})
	go func() {
		defer close(hostDone)
		tick := time.NewTicker(attemptMemorySamplePeriod)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				start := clock.Now()
				sample := a.peak()
				peak = sampledMemoryPeak(peak, sample)
				if sink != nil {
					if sample == nil {
						sample = &AttemptMemoryPeak{}
					}
					sink(start, clock.Now(), sample, attemptMemorySamplePeriod, false)
				}
			}
		}
	}()
	var device <-chan deviceMemoryResult
	if deviceTelemetry {
		device = w.observeDeviceMemoryWith(ctx, func() (map[int]bool, map[int]bool, error) {
			ids := map[int]bool{}
			return ids, nil, a.processIDs(ids)
		}, a.fs, devicePeriod, clock, a, sink)
	}
	var once sync.Once
	return func() *AttemptMemoryPeak {
		once.Do(func() {
			cancel()
			<-hostDone
			peak = sampledMemoryPeak(peak, a.peak())
			if device != nil {
				deadline := time.NewTimer(attemptMemorySamplePeriod)
				defer deadline.Stop()
				select {
				case r := <-device:
					peak = sampledMemoryPeak(peak, r.peak)
				case <-deadline.C:
					peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryTimeout})
				}
			}
		})
		return peak
	}
}

var (
	errAttemptMemoryPending     = errors.New("attempt GPU memory not available yet")
	errAttemptMemoryUnsupported = errors.New("attempt GPU memory accounting unsupported")
	errDeviceMemoryNotReported  = errors.New("attempt GPU processes listed without per-process memory")
)

const (
	deviceMemoryNoSample    = "no usable per-process sample"
	deviceMemoryUnsupported = "per-process accounting unsupported or invalid"
	deviceMemoryInventory   = "attempt process inventory unavailable"
	deviceMemoryTimeout     = "device sampler did not finish"
	deviceMemoryNoCgroup    = "attempt cgroup unavailable"
	deviceMemoryNotReported = "per-process memory not reported; host delta unavailable"
	deviceMemoryShared      = "per-process memory not reported; another process used the GPU"
	// DeviceSource for a unified CUDA figure taken from the host memory drop
	// outside every Mycelium cgroup. It also counts growth by non-Mycelium
	// processes, and misses use that coincides with their frees.
	deviceHostDeltaSource = "host memory drop outside Mycelium cgroups"
	// DeviceSource for a Metal figure: phys_footprint beyond RSS (#1044).
	deviceFootprintSource = "phys_footprint beyond RSS"
	ownedFootprintSource  = "owned phys_footprint"
)

// The bool reports another process on the attempt's CUDA device.
func (w Worker) attemptDeviceMemoryReading(ctx context.Context, ids map[int]bool, fs memoryFS, resident *float64, known ...map[int]bool) (float64, bool, error) {
	if len(w.selectedDevices) > 1 {
		return 0, false, errAttemptMemoryUnsupported // One card's sample cannot describe a multi-card attempt.
	}
	if sample := backendFor(w.Machine.GPU).attemptMemory; sample != nil {
		return sample(w, ctx, ids, fs, resident, known...)
	}
	return 0, false, errAttemptMemoryUnsupported
}

// N/A rows for the attempt's own processes mean "listed, not reported" (some
// unified-memory drivers); a partial sum would undercount, so none is used.
func cudaAttemptMemoryReading(b []byte, selector string, ids map[int]bool, resident *float64, known ...map[int]bool) (float64, bool, error) {
	rows, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
	if err != nil {
		return 0, false, errAttemptMemoryUnsupported
	}
	total, largest, found, unreported, foreign := 0.0, 0.0, false, false, false
	for _, row := range rows {
		if len(row) != 3 {
			return 0, false, errAttemptMemoryUnsupported
		}
		if strings.TrimSpace(row[0]) != selector {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(row[1]))
		if err != nil {
			return 0, false, errAttemptMemoryUnsupported
		}
		if !ids[pid] {
			foreign = foreign || len(known) == 0 || !known[0][pid]
			continue
		}
		value := strings.TrimSpace(row[2])
		switch strings.ToLower(value) {
		case "n/a", "[n/a]", "not supported", "[not supported]":
			unreported = true
			continue
		}
		mib, err := strconv.ParseFloat(value, 64)
		if err != nil || !validMemoryValue(mib) {
			return 0, false, errAttemptMemoryUnsupported
		}
		total += mib / 1024
		largest = max(largest, mib/1024)
		found = true
	}
	if resident != nil && found && !unreported {
		// A sum can count one CUDA allocation in several processes. A single
		// process's counter is a lower bound on their physical union. Keep this
		// separate from the summed charge and RSS/fallback estimates. A worker can
		// report this lower bound for host-wide union credit on the queue.
		*resident = largest
	}
	switch {
	case unreported:
		return 0, foreign, errDeviceMemoryNotReported
	case len(ids) == 0:
		return 0, foreign, errAttemptMemoryPending
	}
	// A successful complete enumeration before CUDA initialization has no row
	// for this attempt. That is measured zero, not a stalled sensor. Imports
	// and CPU-side loading can take arbitrarily longer than one sensor cycle.
	// Listed N/A counters and failed queries still take their unknown paths.
	return total, foreign, nil
}

func drmAttemptMemory(b []byte, pci string) (string, float64, float64, error) {
	client, pdev := "", ""
	var total, host float64
	found := false
	for _, line := range strings.Split(string(b), "\n") {
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		val = strings.TrimSpace(val)
		switch key {
		case "drm-client-id":
			client = val
		case "drm-pdev":
			pdev = val
		case "drm-resident-system", "drm-resident-gtt":
			if gb, ok := drmMemoryGB(val); ok {
				host = max(host, gb)
			}
		default:
			if strings.HasPrefix(key, "drm-resident-vram") {
				gb, ok := drmMemoryGB(val)
				if !ok {
					return "", 0, 0, errAttemptMemoryUnsupported
				}
				total += gb
				found = true
			}
		}
	}
	if !found || client == "" || normalizePCIAddress(pdev) != normalizePCIAddress(pci) {
		return "", 0, 0, errAttemptMemoryPending
	}
	return pdev + "/" + client, total, host, nil
}

func drmMemoryGB(value string) (float64, bool) {
	fields := strings.Fields(value)
	if len(fields) == 0 || len(fields) > 2 {
		return 0, false
	}
	scale := float64(1)
	if len(fields) == 2 {
		switch fields[1] {
		case "KiB":
			scale = 1 << 10
		case "MiB":
			scale = 1 << 20
		default:
			return 0, false
		}
	}
	n, err := strconv.ParseUint(fields[0], 10, 64)
	return float64(n) * scale / (1 << 30), err == nil
}

// memory.current includes reclaimable file cache that MemAvailable already
// counts as available. Credit anonymous resident pages only, not total/peak/RSS.
// Cgroup charging counts shared pages once across disjoint attempt subtrees.

func (a *memoryAttempt) currentHostMemory(clock Clock) *CurrentHostMemory {
	p := a.peak()
	if p == nil || p.HostGB == nil {
		return nil
	}
	at := clock.Now()
	sample := &CurrentHostMemory{NonreclaimableGB: *p.HostGB, sampledAt: at, deviceSample: &a.device}
	if device := a.device.Load(); device != nil {
		gb := device.gb
		sample.DeviceGB, sample.deviceSampledAt = &gb, device.at
		if age := at.Sub(device.at); age >= 0 && age < deviceMemorySampleFreshFor {
			sample.NonreclaimableGB += device.pinnedGB
		}
	}
	return sample
}

type deviceMemoryResult struct {
	peak               *AttemptMemoryPeak
	unreported, shared bool
}

// inventory returns exact attempt PIDs and other known attempts' PIDs. Only
// the former contribute to the peak. The latter are not foreign workload.
func (w Worker) observeDeviceMemory(ctx context.Context, inventory func() (map[int]bool, map[int]bool, error), fs memoryFS, period time.Duration, clock Clock, a *memoryAttempt, ticks ...<-chan time.Time) <-chan deviceMemoryResult {
	return w.observeDeviceMemoryWith(ctx, inventory, fs, period, clock, a, nil, ticks...)
}

func (w Worker) observeDeviceMemoryWith(ctx context.Context, inventory func() (map[int]bool, map[int]bool, error), fs memoryFS, period time.Duration, clock Clock, a *memoryAttempt, sink attemptMemorySampleSink, ticks ...<-chan time.Time) <-chan deviceMemoryResult {
	result := make(chan deviceMemoryResult, 1)
	started := clock.Now()
	go func() {
		peak := &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryNoSample}
		unreported := false
		var growth processMemoryGrowth
		sampling := deviceSampling{start: started}
		defer func() {
			peak.DeviceSampling = sampling.finish(clock.Now(), period)
			result <- deviceMemoryResult{peak: peak, unreported: unreported}
		}()
		var sampleTick <-chan time.Time
		if len(ticks) > 0 {
			// Inject sensor events independently of the observation Clock.
			sampleTick = ticks[0]
		} else {
			tick := time.NewTicker(period)
			defer tick.Stop()
			sampleTick = tick.C
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-sampleTick:
				started := clock.Now()
				report := func(sample *AttemptMemoryPeak) {
					if sink != nil && ctx.Err() == nil {
						sink(started, clock.Now(), sample, period, true)
					}
				}
				ids, known, err := inventory()
				if err != nil {
					sampling.incomplete = true
					report(&AttemptMemoryPeak{DeviceUnavailable: deviceMemoryInventory})
					peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryInventory})
					continue
				}
				if len(ids) == 0 {
					continue
				}

				var pinned float64
				var host *float64
				if w.Machine.GPU == "xpu" && !w.Machine.UnifiedMemory {
					host = &pinned
				}
				gb, _, err := w.attemptDeviceMemoryReading(ctx, ids, fs, host, known)
				if errors.Is(err, errDeviceMemoryNotReported) {
					unreported = true
					report(&AttemptMemoryPeak{DeviceUnavailable: deviceMemoryNotReported})
					continue
				}
				if err != nil {
					sampling.incomplete = true
					if !errors.Is(err, errAttemptMemoryPending) {
						report(&AttemptMemoryPeak{DeviceUnavailable: deviceMemoryUnsupported})
						peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryUnsupported})
						return
					}
					report(&AttemptMemoryPeak{DeviceUnavailable: deviceMemoryNoSample})
					continue
				}

				report(&AttemptMemoryPeak{DeviceGB: &gb})
				now := clock.Now()
				sampling.observe(now, gb, period)
				margin := growth.observe(now, gb)
				peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceGB: &gb, DeviceGrowthGB: &margin})
				if a != nil {
					a.device.Store(&hostMemorySample{gb: gb, pinnedGB: pinned, at: now})
				}

			}
		}
	}()
	return result
}

func cudaAttemptDeviceMemory(w Worker, ctx context.Context, ids map[int]bool, _ memoryFS, resident *float64, known ...map[int]bool) (float64, bool, error) {
	selector := w.Machine.Device
	if w.selectedDevice != nil {
		selector = w.selectedDevice.Selector
	}
	if !strings.HasPrefix(selector, "GPU-") {
		return 0, false, errAttemptMemoryUnsupported
	}
	query := w.cudaMemoryQuery
	if query == nil {
		query = queryCUDAAttemptMemory
	}
	b, err := query(ctx)
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return 0, false, errAttemptMemoryUnsupported
		}
		// Driver startup and query timeouts do not establish lack of support.
		return 0, false, errAttemptMemoryPending
	}
	return cudaAttemptMemoryReading(b, selector, ids, resident, known...)
}

func queryCUDAAttemptMemory(ctx context.Context) ([]byte, error) {
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probe, "nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_gpu_memory", "--format=csv,noheader,nounits")
	// A descendant can retain the query's pipes after it exits or is killed.
	// Bound that drain too, so it cannot stop future five-second samples.
	// Incomplete output remains unknown rather than a partial measurement.
	cmd.WaitDelay = attemptMemorySamplePeriod
	return cmd.Output()
}
func xpuAttemptDeviceMemory(w Worker, _ context.Context, ids map[int]bool, fs memoryFS, host *float64, _ ...map[int]bool) (float64, bool, error) {
	if w.selectedDevice == nil || w.selectedDevice.pciAddress == "" {
		return 0, false, errAttemptMemoryUnsupported
	}

	clients := map[string]float64{}
	system := map[string]float64{}
	for pid := range ids {
		entries, err := fs.ReadDir(filepath.Join("/proc", strconv.Itoa(pid), "fdinfo"))
		if err != nil {
			continue
		}
		for _, entry := range entries {
			b, err := fs.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "fdinfo", entry.Name()))
			if err != nil {
				continue
			}
			key, gb, pinned, err := drmAttemptMemory(b, w.selectedDevice.pciAddress)
			if errors.Is(err, errAttemptMemoryUnsupported) {
				return 0, false, err
			}
			if err == nil {
				clients[key] = max(clients[key], gb)
				if host != nil {
					system[key] = max(system[key], pinned)
				}
			}
		}
	}
	total := 0.0
	for _, gb := range clients {
		total += gb
	}
	for _, gb := range system {
		*host += gb
	}
	if len(clients) == 0 {
		return 0, false, errAttemptMemoryPending
	}
	return total, false, nil
}

const deviceUnifiedResidentSource = "unified RSS/cgroup upper estimate with host reconciliation"
const deviceUnifiedUncertain = "unified device attribution uncertain; exclusive execution required"
