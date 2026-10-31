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

func (a *memoryAttempt) peak() *AttemptMemoryPeak {
	if a == nil {
		return nil
	}
	p := &AttemptMemoryPeak{Pressure: &MemoryPressureEvidence{Reason: "eligible"}}
	// PSI some includes full stalls. Eligibility uses baselined cumulative
	// totals over the attempt window; avg10 is diagnostic only.
	b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.pressure"))
	psi, psiErr := sharedMemoryPSI(b)
	total, totalErr := memoryPSITotal(b)
	p.pressureTotalUS, p.pressureAvg10 = total, psi
	p.PressureUnavailable = err != nil || psiErr != nil || totalErr != nil
	if b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.peak")); err == nil {
		if n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil {
			gb := float64(n) / (1 << 30)
			p.CgroupGB = &gb
		}
	}
	if b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.stat")); err == nil {
		stats := memoryCounters(b)
		anon, aok := stats["anon"]
		kernel, kok := stats["kernel"]
		shmem, sok := stats["shmem"]
		if aok && kok && sok {
			gb := (float64(anon) + float64(kernel) + float64(shmem)) / (1 << 30)
			p.HostGB = &gb
			unevictable, uok := stats["unevictable"]
			dirty, dok := stats["file_dirty"]
			writeback, wok := stats["file_writeback"]
			if uok && dok && wok {
				// Conservatively retain all kernel and shmem pages. Unevictable
				// can overlap anon/shmem; double counting is safer than omitting
				// locked file pages. Dirty/writeback pages need I/O before reclaim;
				// sum them conservatively even where the counters overlap.
				// This is not a driver pin counter; GPU reduction also needs
				// complete independent xe host-object/whole-host evidence.
				nonreclaimable := gb + (float64(unevictable)+float64(dirty)+float64(writeback))/(1<<30)
				p.nonreclaimableSampleGB = &nonreclaimable
			}
		}
		for _, name := range []string{"workingset_refault_anon", "pswpout"} {
			p.Constrained = p.Constrained || stats[name] > 0
		}
		p.Pressure.CgroupRefaultAnon, p.Pressure.CgroupSwapOutPages = stats["workingset_refault_anon"], stats["pswpout"]
		if p.Constrained {
			p.Pressure.Reason = "cgroup_refault_or_swap_out"
		}
	}
	if b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.events")); err == nil {
		events := memoryCounters(b)
		p.Events = &MemoryEventCounts{High: events["high"], Max: events["max"], OOM: events["oom"], OOMKill: events["oom_kill"], OOMGroupKill: events["oom_group_kill"]}
		// File-backed working sets can bind while resident-only samples retain
		// apparent headroom. Any max/high pressure makes true need unknown.
		p.Constrained = p.constrained() || events["oom"] > 0 || events["oom_kill"] > 0 || events["oom_group_kill"] > 0
		if p.Constrained && p.Pressure.Reason == "eligible" {
			p.Pressure.Reason = "cgroup_events"
		}
	}
	for _, name := range []string{"memory.swap.peak", "memory.swap.current"} {
		if b, err := a.fs.ReadFile(filepath.Join(a.path, name)); err == nil {
			n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
			p.Constrained = p.Constrained || err != nil || n > 0
			if (err != nil || n > 0) && p.Pressure.Reason == "eligible" {
				p.Pressure.Reason = "cgroup_swap"
			}
			if name == "memory.swap.peak" && err == nil {
				gb := float64(n) / (1 << 30)
				p.SwapGB = &gb
			}
		}
	}
	// A member can bind well below the shared envelope. Check descendants
	// against their own enforced caps, retaining only their pressure evidence;
	// the parent's resident counters already include their memory.
	if entries, err := a.fs.ReadDir(a.path); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			child := &memoryAttempt{fs: a.fs, path: filepath.Join(a.path, entry.Name())}
			if b, err := a.fs.ReadFile(filepath.Join(child.path, "memory.max")); err == nil {
				child.bytes, _ = strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
			}
			if peak := child.peak(); peak != nil {
				if peak.Constrained && !p.Constrained {
					p.Pressure.Reason = "descendant_" + strings.TrimPrefix(peak.Pressure.Reason, "descendant_")
				}
				p.Constrained = p.Constrained || peak.Constrained
				p.PressureUnavailable = p.PressureUnavailable || peak.PressureUnavailable
			}
		}
	}
	if p.HostGB == nil && p.CgroupGB == nil && p.SwapGB == nil && p.Events == nil && !p.Constrained {
		return nil
	}
	if !p.Constrained {
		p.Pressure.Reason = "counter_snapshot_only" // No baselined observation window yet.
	}
	return p
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

// Without cgroups (macOS), the attempt boundary is its process groups, the
// same boundary the worker signals. Periodic RSS sums cover concurrent
// members; rusage max RSS reports only the largest waited process. Shared
// pages count once per process, but compressed pages, members that left the
// group and spikes shorter than the interval are missed, so it is no bound in
// either direction. Host pressure rejects this sample's learning, but cannot
// attribute the cause to this attempt or invalidate other hosts' learned peaks.
func (w Worker) observeProcessMemory(groups func() []int) func() *AttemptMemoryPeak {
	probe := w.processMemory
	var mu sync.Mutex
	var peak *AttemptMemoryPeak
	readPressure := func() (*gpuHostState, error) {
		if probe.Pressure == nil {
			return nil, errAttemptMemoryUnsupported
		}
		ctx, cancel := context.WithTimeout(context.Background(), attemptMemorySamplePeriod)
		defer cancel()
		return probe.Pressure(ctx)
	}
	baseline, err := readPressure()
	complete := err == nil && baseline != nil
	var worst gpuHostState
	if complete {
		worst = *baseline
	}
	sample := func() {
		host, err := readPressure()
		if err != nil || host == nil || baseline == nil || host.swapOut < baseline.swapOut {
			complete = false
		} else {
			worst.retainPressure(host)
		}
		ids := map[int]bool{}
		for _, g := range groups() {
			if g > 0 {
				ids[g] = true
			}
		}
		bytes, members, err := probe.Resident(ids)
		if err != nil || members == 0 {
			return
		}
		gb := float64(bytes) / (1 << 30)
		next := &AttemptMemoryPeak{HostGB: &gb, Source: processGroupMemorySource}
		mu.Lock()
		peak = sampledMemoryPeak(peak, next)
		peak.Source = processGroupMemorySource
		mu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			sample()
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
			sample() // Include teardown pressure even after the process groups exit.
		})
		mu.Lock()
		defer mu.Unlock()
		if peak != nil && peak.HostGB != nil {
			peak.Pressure = hostPressureEvidence(baseline, &worst, *peak.HostGB, w.HostMemoryReserveGB, os.Getpagesize())
			if !complete {
				peak.Pressure.Reason = "host_pressure_unavailable"
			}
			peak.Constrained = peak.Pressure.Reason != "eligible"
		}
		return peak
	}
}

// Raise a process-group peak to the root's exit max RSS, which catches a
// spike between samples in the main process.
func withExitMaxRSS(peak *AttemptMemoryPeak, state *os.ProcessState) *AttemptMemoryPeak {
	if peak == nil || peak.Source != processGroupMemorySource || state == nil {
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
func (w Worker) observeAttemptMemory(a *memoryAttempt, j Job, groups func() []int) func() *AttemptMemoryPeak {
	deviceTelemetry := !j.Spec.Needs.cpuOnly() && (w.Machine.GPU == "cuda" || w.Machine.GPU == "xpu")
	if a == nil && w.processMemory != nil && groups != nil {
		return w.observeProcessMemory(groups)
	}
	if a == nil {
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
	var mu sync.Mutex
	record := func(sample *AttemptMemoryPeak) {
		mu.Lock()
		peak = sampledMemoryPeak(peak, sample)
		mu.Unlock()
	}
	completeNonreclaimable := peak != nil && peak.nonreclaimableSampleGB != nil && !peak.PressureUnavailable
	// The empty-cgroup baseline precedes payload launch, including for plain
	// commands whose observer is installed just after exec. No baseline means
	// diagnostic counters only, never permission to shrink a GPU envelope.
	gpuHost := w.Machine.GPU == "xpu" && !w.Machine.UnifiedMemory && w.selectedDevice != nil && w.selectedDevice.pciAddress != "" && !j.Spec.Needs.cpuOnly()
	completeGPUHost := gpuHost && a.hostBaseline != nil
	var worstHost gpuHostState
	pressureKnown := a.hostBaseline != nil && a.pressureBaseline != nil && !a.pressureStart.IsZero()
	unknownReason := "pressure_baseline_unavailable"
	var groupAvg10 float64
	var groupTotal uint64
	if a.pressureBaseline != nil {
		groupTotal = *a.pressureBaseline
	}
	if a.hostBaseline != nil {
		worstHost = *a.hostBaseline
	}
	var hostDropGB, outsideGB float64
	sampleHost := func(component *AttemptMemoryPeak) {
		// Pressure eligibility applies to ordinary learning on every Linux
		// cgroup attempt, independently of complete DRM/component evidence.
		if component == nil || component.PressureUnavailable || component.pressureTotalUS < groupTotal {
			pressureKnown = false
			unknownReason = "cgroup_psi_unavailable_or_reset"
		} else {
			groupTotal = component.pressureTotalUS
		}
		if component != nil {
			groupAvg10 = max(groupAvg10, component.pressureAvg10)
		}
		host, err := readGPUHostState(a.fs)
		if err != nil || a.hostBaseline == nil || host.oom < worstHost.oom || host.swapOut < worstHost.swapOut || host.psiTotalUS < worstHost.psiTotalUS {
			pressureKnown = false
			unknownReason = "host_pressure_unavailable_or_reset"
			completeGPUHost = false
			return
		}
		worstHost.retainPressure(host)
		if !completeGPUHost || component == nil || component.nonreclaimableSampleGB == nil || component.PressureUnavailable {
			completeGPUHost = false
			return
		}
		hostDropGB = max(hostDropGB, a.hostBaseline.available-host.available)
		// Host and component reads stay together; DRM I/O never runs on this
		// sampler. Its independently retained driver floor is added at finish.
		outsideGB = max(outsideGB, a.hostBaseline.available-host.available-*component.nonreclaimableSampleGB)
	}
	sampleHost(peak)
	var driver <-chan gpuHostCollection
	if completeGPUHost {
		driver = a.collectGPUHost(ctx, clock, w.GPUTelemetryHelper, w.selectedDevice.pciAddress)
	}
	var samplers sync.WaitGroup
	hostSampled := false // Read only after joining the host sampler.
	samplers.Add(1)
	go func() {
		defer samplers.Done()
		// Resident memory can spike and disappear before process exit. Keep
		// cheap cgroup reads independent of the slower driver probe cadence.
		hostTick := time.NewTicker(attemptMemorySamplePeriod)
		defer hostTick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-hostTick.C:
				sample := a.peak()
				completeNonreclaimable = completeNonreclaimable && sample != nil && sample.nonreclaimableSampleGB != nil && !sample.PressureUnavailable
				hostSampled = hostSampled || sample != nil && sample.HostGB != nil
				record(sample)
				sampleHost(sample)
			}
		}
	}()
	var device <-chan *AttemptMemoryPeak
	// Attempt facts are independent of learning eligibility. Declared jobs,
	// extension checks and resumed attempts need device diagnostics too.
	if deviceTelemetry {
		result := make(chan *AttemptMemoryPeak, 1)
		device = result
		go func() {
			// Device fdinfo can block in the same driver as host collection.
			// Own the result locally so a late read cannot change published facts.
			devicePeak := &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryNoSample}
			defer func() { result <- devicePeak }()
			deviceTick := time.NewTicker(5 * time.Second)
			defer deviceTick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-deviceTick.C:
					ids := map[int]bool{}
					if a.processIDs(ids) != nil {
						devicePeak = sampledMemoryPeak(devicePeak, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryInventory})
						continue
					}
					if len(ids) == 0 {
						continue
					}
					gb, err := w.attemptDeviceMemory(ctx, ids, a.fs)
					if err != nil {
						if !errors.Is(err, errAttemptMemoryPending) {
							devicePeak = sampledMemoryPeak(devicePeak, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryUnsupported})
							return
						}
						continue
					}
					devicePeak = sampledMemoryPeak(devicePeak, &AttemptMemoryPeak{DeviceGB: &gb})
				}
			}
		}()
	}
	var once sync.Once
	return func() *AttemptMemoryPeak {
		once.Do(func() {
			cancel()
			wait, stopWaiting := context.WithTimeout(context.Background(), attemptMemorySamplePeriod)
			defer stopWaiting()
			samplers.Wait()
			// Timestamp before final I/O, so sensor latency cannot dilute the
			// pressure ratio. Counter resets and nonpositive windows are unknown.
			elapsed := clock.Now().Sub(a.pressureStart)
			final := a.peak()
			record(final)
			sampleHost(final)
			if !pressureKnown || elapsed <= 0 {
				record(&AttemptMemoryPeak{PressureUnavailable: true})
				completeGPUHost = false
			}
			if peak != nil {
				resident := 0.0
				if peak.HostGB != nil {
					resident = *peak.HostGB
				}
				e := hostPressureEvidence(a.hostBaseline, &worstHost, resident, w.HostMemoryReserveGB, a.pageSize)
				e.ElapsedUS, e.CgroupAvg10 = float64(elapsed)/float64(time.Microsecond), groupAvg10
				if peak.Pressure != nil {
					e.CgroupRefaultAnon, e.CgroupSwapOutPages = peak.Pressure.CgroupRefaultAnon, peak.Pressure.CgroupSwapOutPages
					if peak.Constrained {
						e.Reason = peak.Pressure.Reason
					}
				}
				if pressureKnown {
					hostDelta, groupDelta := worstHost.psiTotalUS-a.hostBaseline.psiTotalUS, groupTotal-*a.pressureBaseline
					e.HostStallUS, e.CgroupStallUS = &hostDelta, &groupDelta
					if e.Reason == "eligible" && memoryStallPressure(a.hostBaseline.psiTotalUS, worstHost.psiTotalUS, elapsed) {
						e.Reason = "host_psi_total"
					}
					if e.Reason == "eligible" && memoryStallPressure(*a.pressureBaseline, groupTotal, elapsed) {
						e.Reason = "cgroup_psi_total"
					}
				}
				if !pressureKnown {
					e.Reason = unknownReason
				} else if elapsed <= 0 {
					e.Reason = "invalid_pressure_window"
				}
				peak.Pressure = e
				if pressureKnown && elapsed > 0 && e.Reason != "eligible" {
					peak.Constrained = true
				}
			}
			if device != nil {
				select {
				case devicePeak := <-device:
					record(devicePeak)
				case <-wait.Done():
					record(&AttemptMemoryPeak{DeviceUnavailable: deviceMemoryTimeout})
					completeGPUHost = false
				}
			}
			var collected gpuHostCollection
			if driver != nil {
				// Proc/debugfs reads cannot be cancelled once inside the kernel.
				// A buffered result owns no peak state, so cleanup need not wait
				// indefinitely and a late return cannot mutate published facts.
				select {
				case collected = <-driver:
				case <-wait.Done():
				}
			}
			age := clock.Now().Sub(collected.last)
			completeGPUHost = completeGPUHost && collected.complete && collected.seen && age >= 0 && age <= 2*attemptMemorySamplePeriod
			if (j.Spec.Needs.cpuOnly() || completeGPUHost) && peak != nil && !peak.PressureUnavailable && hostSampled && completeNonreclaimable && final != nil && final.nonreclaimableSampleGB != nil && !final.PressureUnavailable {
				peak.NonreclaimableGB = peak.nonreclaimableSampleGB
				if gpuHost {
					outsideGB = max(outsideGB, collected.driverGB)
					peak.GPUHost = &GPUHostMemory{DriverGB: &collected.driverGB, HostDropGB: &hostDropGB, OutsideGB: &outsideGB}
				}
			}
			// Counter-only read zeros are diagnostic facts, not a new
			// incomplete resident measurement that invalidates learning.
			if !hostSampled && peak != nil && (peak.HostGB != nil || peak.CgroupGB != nil || peak.DeviceGB != nil || peak.Constrained) {
				peak.Constrained = true
				peak.Pressure.Reason = "resident_sampling_incomplete"
			}
		})
		return peak
	}
}

var (
	errAttemptMemoryPending     = errors.New("attempt GPU memory not available yet")
	errAttemptMemoryUnsupported = errors.New("attempt GPU memory accounting unsupported")
)

const (
	deviceMemoryNoSample    = "no usable per-process sample"
	deviceMemoryUnsupported = "per-process accounting unsupported or invalid"
	deviceMemoryInventory   = "attempt process inventory unavailable"
	deviceMemoryTimeout     = "device sampler did not finish"
	deviceMemoryNoCgroup    = "attempt cgroup unavailable"
)

func (w Worker) attemptDeviceMemory(ctx context.Context, ids map[int]bool, fs memoryFS) (float64, error) {
	if w.Machine.GPU == "cuda" {
		selector := w.Machine.Device
		if w.selectedDevice != nil {
			selector = w.selectedDevice.Selector
		}
		if !strings.HasPrefix(selector, "GPU-") {
			return 0, errAttemptMemoryUnsupported
		}
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		cmd := exec.CommandContext(probe, "nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_gpu_memory", "--format=csv,noheader,nounits")
		// A descendant can retain the query's pipes after it exits or is killed.
		// Bound that drain too, so it cannot stop future five-second samples.
		// Incomplete output remains unknown rather than a partial measurement.
		cmd.WaitDelay = attemptMemorySamplePeriod
		b, err := cmd.Output()
		if err != nil {
			if errors.Is(err, exec.ErrNotFound) {
				return 0, errAttemptMemoryUnsupported
			}
			// Driver startup and query timeouts do not establish lack of support.
			return 0, errAttemptMemoryPending
		}
		return cudaAttemptMemory(b, selector, ids)
	}
	if w.Machine.GPU == "xpu" && w.selectedDevice != nil && w.selectedDevice.pciAddress != "" {
		clients := map[string]float64{}
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
				key, gb, err := drmAttemptMemory(b, w.selectedDevice.pciAddress)
				if errors.Is(err, errAttemptMemoryUnsupported) {
					return 0, err
				}
				if err == nil {
					clients[key] = max(clients[key], gb)
				}
			}
		}
		total := 0.0
		for _, gb := range clients {
			total += gb
		}
		if len(clients) == 0 {
			return 0, errAttemptMemoryPending
		}
		return total, nil
	}
	return 0, errAttemptMemoryUnsupported
}

func cudaAttemptMemory(b []byte, selector string, ids map[int]bool) (float64, error) {
	rows, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
	if err != nil {
		return 0, errAttemptMemoryUnsupported
	}
	total, found := 0.0, false
	for _, row := range rows {
		if len(row) != 3 {
			return 0, errAttemptMemoryUnsupported
		}
		if strings.TrimSpace(row[0]) != selector {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(row[1]))
		if err != nil {
			return 0, errAttemptMemoryUnsupported
		}
		if !ids[pid] {
			continue
		}
		mib, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
		if err != nil || !validMemoryValue(mib) {
			return 0, errAttemptMemoryUnsupported
		}
		total += mib / 1024
		found = true
	}
	if !found {
		return 0, errAttemptMemoryPending
	}
	return total, nil
}

func drmAttemptMemory(b []byte, pci string) (string, float64, error) {
	client, pdev := "", ""
	total := 0.0
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
		default:
			if strings.HasPrefix(key, "drm-resident-vram") {
				f := strings.Fields(val)
				if len(f) == 0 || len(f) > 2 {
					return "", 0, errAttemptMemoryUnsupported
				}
				scale := float64(1)
				if len(f) == 2 {
					switch f[1] {
					case "KiB":
						scale = 1 << 10
					case "MiB":
						scale = 1 << 20
					default:
						return "", 0, errAttemptMemoryUnsupported
					}
				}
				n, err := strconv.ParseUint(f[0], 10, 64)
				if err != nil {
					return "", 0, errAttemptMemoryUnsupported
				}
				total += float64(n) * scale / (1 << 30)
				found = true
			}
		}
	}
	if !found || client == "" || normalizePCIAddress(pdev) != normalizePCIAddress(pci) {
		return "", 0, errAttemptMemoryPending
	}
	return pdev + "/" + client, total, nil
}

// memory.current includes reclaimable file cache that MemAvailable already
// counts as available. Credit anonymous resident pages only, not total/peak/RSS.
// Cgroup charging counts shared pages once across disjoint attempt subtrees.
func (a *memoryAttempt) currentHostMemory(clock Clock) *CurrentHostMemory {
	if a == nil {
		return nil
	}
	at := clock.Now()
	b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.stat"))
	if err != nil {
		return nil
	}
	anon, ok := memoryCounters(b)["anon"]
	if !ok {
		return nil
	}
	b, err = a.fs.ReadFile(filepath.Join(a.path, "memory.current"))
	if err != nil {
		return nil
	}
	current, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return nil
	}
	return &CurrentHostMemory{NonreclaimableGB: float64(min(anon, current)) / (1 << 30), sampledAt: at}
}
