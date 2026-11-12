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
func (w Worker) observeProcessMemory(groups func() []int, attempts ...*process) func() *AttemptMemoryPeak {
	var attempt *process
	if len(attempts) > 0 {
		attempt = attempts[0]
	}
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	guard := learnedMemoryGuard{}
	probe := w.processMemory
	footprint := false
	if attempt != nil && attempt.job.Spec.Needs.cpuOnly() {
		guard = w.learnedGuard(attempt.job)
	} else if attempt != nil && w.footprintGuarded(attempt.job) {
		footprint, guard = true, w.footprintGuard(attempt.job)
	}
	var mu sync.Mutex
	var peak *AttemptMemoryPeak
	var growth processMemoryGrowth
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
	sample := func(enforce bool) {
		host, pressureErr := readPressure()
		if pressureErr != nil {
			host = nil
		}
		var err error
		if pressureErr != nil || host == nil || baseline == nil || host.swapOut < baseline.swapOut {
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
		var bytes, resident uint64
		var members int
		var sampleAt time.Time
		if (probe.OwnedResident != nil || footprint) && attempt != nil {
			roots := attempt.rssRoots()
			quality := "owned_session_rss"
			if footprint {
				bytes, resident, members, err = probe.OwnedFootprint(roots)
				quality = footprintGuardQuality
			} else {
				bytes, members, err = probe.OwnedResident(roots)
			}
			sampleAt = clock.Now()
			if enforce {
				if evidence := guard.observeMemory(clock.Now(), roots, bytes, members, err, host); evidence != nil {
					evidence.Quality = quality
					guard.exceeded = attempt.stopRSS(clock.Now(), roots, evidence)
				}
			}
		} else if probe.Resident != nil {
			bytes, members, err = probe.Resident(ids)
			sampleAt = clock.Now()
		}
		if err != nil || members == 0 {
			growth = processMemoryGrowth{}
			if attempt != nil {
				attempt.rssMemory.Store(nil)
			}
			return
		}
		gb := float64(bytes) / (1 << 30)
		margin := growth.observe(sampleAt, gb)
		if attempt != nil {
			attempt.rssMemory.Store(&processResidentSample{gb: gb, growth: margin, rate: growth.rate, at: sampleAt, observed: growth.horizon > 0})
		}
		next := &AttemptMemoryPeak{HostGB: &gb, Source: processGroupMemorySource, GrowthGB: &margin}
		if footprint {
			// Unified sizing sums a host and a device share. RSS is the host
			// share; the footprint beyond it (Metal buffers, compressed pages)
			// is the device share. Their sum is the measured footprint, or RSS
			// when shared file pages make RSS the larger.
			host, device := float64(resident)/(1<<30), max(0, gb-float64(resident)/(1<<30))
			next.HostGB, next.DeviceGB, next.DeviceSource = &host, &device, deviceFootprintSource
		}
		mu.Lock()
		peak = sampledMemoryPeak(peak, next)
		peak.Source = processGroupMemorySource
		mu.Unlock()
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
			sample(false) // Include teardown pressure even after the process groups exit.
		})
		mu.Lock()
		defer mu.Unlock()
		if peak != nil && peak.HostGB != nil {
			peak.Pressure = hostPressureEvidence(baseline, &worst, *peak.HostGB, w.HostMemoryReserveGB, os.Getpagesize())
			if !complete {
				peak.Pressure.Reason = "host_pressure_unavailable"
			}
			peak.Constrained = peak.Pressure.Reason != "eligible" || guard.exceeded
		}
		return peak
	}
}

// Raise a process-group peak to the root's exit max RSS, which catches a
// spike between samples in the main process. A Metal footprint peak is left
// alone: its RSS and beyond-RSS shares sum to the sampled footprint, and
// raising one share alone would count the same pages twice.
func withExitMaxRSS(peak *AttemptMemoryPeak, state *os.ProcessState) *AttemptMemoryPeak {
	if peak == nil || peak.Source != processGroupMemorySource || peak.DeviceSource == deviceFootprintSource || state == nil {
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
	deviceTelemetry := !j.Spec.Needs.cpuOnly() && backendFor(w.Machine.GPU).attemptMemory != nil
	if a == nil && w.processMemory != nil && groups != nil {
		return w.observeProcessMemory(groups, attempts...)
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
	gpuHost := backendFor(w.Machine.GPU).deviceHostMemory && !w.Machine.UnifiedMemory && w.selectedDevice != nil && w.selectedDevice.pciAddress != "" && !j.Spec.Needs.cpuOnly() && len(w.selectedDevices) == 0
	completeGPUHost := gpuHost && a.hostBaseline != nil && a.rootBaseline != nil
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
	// Unified CUDA hosts that list processes without per-process memory:
	// track the host drop outside every Mycelium cgroup, attributable to this
	// attempt only while nvidia-smi shows no other process on its GPU.
	unifiedDelta := unifiedCUDA(&w.Machine) && deviceTelemetry && a.hostBaseline != nil && a.rootBaseline != nil
	unifiedOutsideGB := 0.0
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
			completeGPUHost, unifiedDelta = false, false
			return
		}
		worstHost.retainPressure(host)
		if unifiedDelta {
			root, ok := cgroupResidentGB(a.fs, filepath.Dir(a.path))
			unifiedDelta = ok
			// Unclamped: a sibling attempt that exits raises MemAvailable and
			// shrinks the root by the same pages, leaving the outside drop.
			unifiedOutsideGB = max(unifiedOutsideGB, a.hostBaseline.available-host.available-(root-*a.rootBaseline))
		}
		if !completeGPUHost || component == nil || component.nonreclaimableSampleGB == nil || component.PressureUnavailable {
			completeGPUHost = false
			return
		}
		root, ok := cgroupResidentGB(a.fs, filepath.Dir(a.path))
		if !ok || component.HostGB == nil {
			completeGPUHost = false
			return
		}
		// The root includes this attempt's resident counters, so remove only
		// siblings' growth. Keep negative growth: an exiting sibling raises
		// MemAvailable and must not hide this attempt's outside allocations.
		siblingGrowth := root - *a.rootBaseline - *component.HostGB
		drop := a.hostBaseline.available - host.available - siblingGrowth
		hostDropGB = max(hostDropGB, drop)
		// DRM I/O stays off this sampler. Its driver floor is added at finish.
		outsideGB = max(outsideGB, drop-*component.nonreclaimableSampleGB)
	}
	sampleHost(peak)
	var driver <-chan gpuHostCollection
	if completeGPUHost {
		driver = a.collectGPUHost(ctx, clock, w.GPUTelemetryHelper, w.selectedDevice.pciAddress)
	}
	var samplers sync.WaitGroup
	var growth memoryGrowth
	guard := w.learnedGuard(j)
	guard.hard = guard.ceiling // Linux's uniquely charged bytes have a physical bound.
	var guarded *process
	if len(attempts) > 0 && guard.ceiling > 0 {
		guarded = attempts[0]
	}
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
				if guarded != nil {
					data, currentErr := a.fs.ReadFile(filepath.Join(a.path, "memory.current"))
					bytes, parseErr := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
					host, hostErr := readGPUHostState(a.fs)
					if hostErr != nil {
						host = nil
					}
					roots := guarded.rssRoots()
					if evidence := guard.observeMemory(clock.Now(), roots, bytes, 1, errors.Join(currentErr, parseErr), host); evidence != nil {
						evidence.Quality = "cgroup_current"
						guard.exceeded = guarded.stopRSS(clock.Now(), roots, evidence)
					}
				}
				sample := a.peak()
				if sample != nil && sample.HostGB != nil {
					margin := growth.observe(clock.Now(), hostMemoryPeakGB(sample))
					sample.GrowthGB = &margin
				}
				completeNonreclaimable = completeNonreclaimable && sample != nil && sample.nonreclaimableSampleGB != nil && !sample.PressureUnavailable
				hostSampled = hostSampled || sample != nil && sample.HostGB != nil
				record(sample)
				sampleHost(sample)
			}
		}
	}()
	var device <-chan deviceMemoryResult
	if deviceTelemetry {
		device = w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) {
			ids := map[int]bool{}
			err := a.processIDs(ids)
			known := map[int]bool{}
			if err == nil && unifiedCUDA(&w.Machine) {
				err = (&memoryAttempt{fs: a.fs, path: filepath.Dir(a.path)}).processIDs(known)
				for pid := range ids {
					delete(known, pid)
				}
			}
			return ids, known, err
		}, a.fs, devicePeriod, clock, a)
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
				peak.Constrained = peak.Constrained || guard.exceeded
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
				case r := <-device:
					record(r.peak)
					if r.unreported && (r.peak == nil || r.peak.DeviceGB == nil) {
						record(unifiedDevicePeak(unifiedDelta, r.shared, unifiedOutsideGB))
					}
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
					peak.GPUHost = &GPUHostMemory{SiblingAdjusted: true, DriverGB: &collected.driverGB, HostDropGB: &hostDropGB, OutsideGB: &outsideGB}
					combined, _ := peak.gpuHostPeakGB()
					peak.CombinedHostGB = &combined
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
)

// The fallback for a unified CUDA attempt whose processes nvidia-smi listed
// without memory. Another process on the GPU makes the drop unattributable.
func unifiedDevicePeak(complete, shared bool, outsideGB float64) *AttemptMemoryPeak {
	switch {
	case shared:
		return &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryShared}
	case !complete || !validMemoryValue(outsideGB):
		return &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryNotReported}
	}
	gb := max(0, outsideGB)
	return &AttemptMemoryPeak{DeviceGB: &gb, DeviceSource: deviceHostDeltaSource}
}

// Anonymous, kernel and shmem pages charged to one cgroup subtree.
func cgroupResidentGB(fs memoryFS, path string) (float64, bool) {
	b, err := fs.ReadFile(filepath.Join(path, "memory.stat"))
	if err != nil {
		return 0, false
	}
	stats := memoryCounters(b)
	anon, aok := stats["anon"]
	kernel, kok := stats["kernel"]
	shmem, sok := stats["shmem"]
	return (float64(anon) + float64(kernel) + float64(shmem)) / (1 << 30), aok && kok && sok
}

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
		// worker-local: neither RSS/fallback estimates nor remote reports grant it.
		*resident = largest
	}
	switch {
	case unreported:
		return 0, foreign, errDeviceMemoryNotReported
	case !found:
		return 0, foreign, errAttemptMemoryPending
	}
	return total, foreign, nil
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
	stats := memoryCounters(b)
	anon, ok := stats["anon"]
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
	a.currentMu.Lock()
	var footprint *float64
	complete, bytes := true, 0.0
	for _, name := range []string{"anon", "kernel", "shmem", "unevictable", "file_dirty", "file_writeback"} {
		n, ok := stats[name]
		complete = complete && ok
		bytes += float64(n)
	}
	margin, observed := 0.0, false
	if complete {
		gb := bytes / (1 << 30)
		footprint = &gb
		// Check-ins arrive irregularly: report the largest growth measured
		// between two of them, not a burst's rate over the longest gap.
		a.currentGrowth.observe(at, gb)
		margin = a.currentGrowth.increment
		observed = a.currentGrowth.horizon > 0
	} else {
		a.currentGrowth = memoryGrowth{} // Never bridge a missing interval.
	}
	rate := a.currentGrowth.rate
	a.currentMu.Unlock()
	sample := &CurrentHostMemory{GrowthObserved: observed, GrowthRateGB: rate, FootprintGB: footprint, GrowthGB: margin, NonreclaimableGB: float64(min(anon, current)) / (1 << 30), sampledAt: at, deviceSample: &a.device}
	if device := a.device.Load(); device != nil {
		gb := device.gb
		sample.DeviceGB, sample.deviceSampledAt, sample.DeviceGrowthGB = &gb, device.at, device.growthGB
		sample.DeviceSource, sample.DeviceExclusive, sample.DeviceGrowthRateGB = device.source, device.exclusive, device.rateGB
		sample.deviceResidentGB = device.residentGB
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
	result := make(chan deviceMemoryResult, 1)
	go func() {
		peak := &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryNoSample}
		unreported, shared, incompleteFallback := false, false, false
		var growth memoryGrowth
		defer func() {
			if incompleteFallback {
				peak.DeviceGB, peak.DeviceGrowthGB, peak.DeviceSource = nil, nil, ""
				peak.DeviceUnavailable = deviceUnifiedUncertain
			}
			result <- deviceMemoryResult{peak, unreported, shared}
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
				ids, known, err := inventory()
				if err != nil {
					peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryInventory})
					continue
				}
				if len(ids) == 0 {
					continue
				}
				resident := 0.0
				gb, foreign, err := w.attemptDeviceMemoryReading(ctx, ids, fs, &resident, known)
				// Known co-tenants also make a whole-host delta ambiguous.
				shared = shared || foreign || len(known) > 0
				source, exclusive := "", false
				if errors.Is(err, errDeviceMemoryNotReported) {
					unreported = true
					if !unifiedCUDA(&w.Machine) || a == nil {
						continue
					}
					gb, exclusive, err = a.unifiedDeviceMemory(ids, foreign)
					incompleteFallback = incompleteFallback || exclusive || err != nil
					source = deviceUnifiedResidentSource
					if err != nil {
						a.device.Store(&hostMemorySample{at: clock.Now(), exclusive: true, source: source})
						peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceUnavailable: deviceUnifiedUncertain})
						continue
					}
				}
				if err != nil {
					if !errors.Is(err, errAttemptMemoryPending) {
						peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryUnsupported})
						return
					}
					continue
				}
				margin := growth.observe(clock.Now(), gb)
				peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceGB: &gb, DeviceGrowthGB: &margin, DeviceSource: source})
				if a != nil {
					a.device.Store(&hostMemorySample{gb: gb, residentGB: resident, at: clock.Now(), growthGB: margin, source: source, exclusive: exclusive, rateGB: growth.rate})
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
			return 0, false, errAttemptMemoryUnsupported
		}
		// Driver startup and query timeouts do not establish lack of support.
		return 0, false, errAttemptMemoryPending
	}
	return cudaAttemptMemoryReading(b, selector, ids, resident, known...)
}
func xpuAttemptDeviceMemory(w Worker, _ context.Context, ids map[int]bool, fs memoryFS, _ *float64, _ ...map[int]bool) (float64, bool, error) {
	if w.selectedDevice == nil || w.selectedDevice.pciAddress == "" {
		return 0, false, errAttemptMemoryUnsupported
	}

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
				return 0, false, err
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
		return 0, false, errAttemptMemoryPending
	}
	return total, false, nil
}
