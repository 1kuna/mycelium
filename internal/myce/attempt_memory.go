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
	p := &AttemptMemoryPeak{}
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
		}
		for _, name := range []string{"workingset_refault_anon", "pswpout"} {
			p.Constrained = p.Constrained || stats[name] > 0
		}
	}
	if b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.events")); err == nil {
		events := memoryCounters(b)
		p.Events = &MemoryEventCounts{High: events["high"], Max: events["max"], OOM: events["oom"], OOMKill: events["oom_kill"], OOMGroupKill: events["oom_group_kill"]}
		// Learned envelopes add 1.25x headroom, so healthy resident use is
		// about 80% of the cap. Use the midpoint to saturation (90%) to
		// distinguish binding resident pressure from ordinary file-cache reclaim.
		nearCeiling := a.bytes > 0 && p.HostGB != nil && *p.HostGB*(1<<30) >= .9*float64(a.bytes)
		p.Constrained = p.Constrained || nearCeiling && (events["max"] > 0 || events["high"] > 0) || events["oom"] > 0 || events["oom_kill"] > 0 || events["oom_group_kill"] > 0
	}
	for _, name := range []string{"memory.swap.peak", "memory.swap.current"} {
		if b, err := a.fs.ReadFile(filepath.Join(a.path, name)); err == nil {
			n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
			p.Constrained = p.Constrained || err != nil || n > 0
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
				p.Constrained = p.Constrained || peak.Constrained
			}
		}
	}
	if p.HostGB == nil && p.CgroupGB == nil && !p.Constrained {
		return nil
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
		out = &AttemptMemoryPeak{}
	}
	out.Constrained = next.Constrained || old != nil && old.Constrained
	return out
}

func estimateFeedingAttempt(j Job) bool {
	return j.Spec.Needs.Memory == "auto" || j.Spec.Workload != nil && j.Spec.ExtensionCheck == nil && (!j.Spec.Resumable || j.Attempt <= 1)
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
// either direction. Nothing enforces it, and host-wide swap or pressure cannot
// attribute pressure to this attempt, so these peaks are never constrained.
func (w Worker) observeProcessMemory(groups func() []int) func() *AttemptMemoryPeak {
	probe := w.processMemory
	var mu sync.Mutex
	var peak *AttemptMemoryPeak
	sample := func() {
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
		})
		mu.Lock()
		defer mu.Unlock()
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
	if a == nil && w.processMemory != nil && groups != nil {
		return w.observeProcessMemory(groups)
	}
	if a == nil {
		return func() *AttemptMemoryPeak { return nil }
	}
	ctx, cancel := context.WithCancel(context.Background())
	peak := a.peak()
	var mu sync.Mutex
	var samplers sync.WaitGroup
	hostSampled := false // Read only after joining the host sampler.
	record := func(sample *AttemptMemoryPeak) {
		mu.Lock()
		peak = sampledMemoryPeak(peak, sample)
		mu.Unlock()
	}
	samplers.Add(1)
	go func() {
		defer samplers.Done()
		// Resident memory can spike and disappear before process exit. Keep
		// cheap cgroup reads independent of the slower driver probe cadence.
		hostTick := time.NewTicker(200 * time.Millisecond)
		defer hostTick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-hostTick.C:
				sample := a.peak()
				hostSampled = hostSampled || sample != nil && sample.HostGB != nil
				record(sample)
			}
		}
	}()
	if estimateFeedingAttempt(j) && !j.Spec.Needs.cpuOnly() && (w.Machine.GPU == "cuda" || w.Machine.GPU == "xpu") {
		samplers.Add(1)
		go func() {
			defer samplers.Done()
			deviceTick := time.NewTicker(5 * time.Second)
			defer deviceTick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-deviceTick.C:
					ids := map[int]bool{}
					if a.processIDs(ids) != nil || len(ids) == 0 {
						continue
					}
					gb, err := w.attemptDeviceMemory(ctx, ids)
					if err != nil {
						if !errors.Is(err, errAttemptMemoryPending) {
							return
						}
						continue
					}
					record(&AttemptMemoryPeak{DeviceGB: &gb})
				}
			}
		}()
	}
	var once sync.Once
	return func() *AttemptMemoryPeak {
		once.Do(func() {
			cancel()
			samplers.Wait()
			record(a.peak())
			if !hostSampled && peak != nil {
				peak.Constrained = true
			}
		})
		return peak
	}
}

var (
	errAttemptMemoryPending     = errors.New("attempt GPU memory not available yet")
	errAttemptMemoryUnsupported = errors.New("attempt GPU memory accounting unsupported")
)

func (w Worker) attemptDeviceMemory(ctx context.Context, ids map[int]bool) (float64, error) {
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
		b, err := exec.CommandContext(probe, "nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_gpu_memory", "--format=csv,noheader,nounits").Output()
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
			entries, err := os.ReadDir(filepath.Join("/proc", strconv.Itoa(pid), "fdinfo"))
			if err != nil {
				continue
			}
			for _, entry := range entries {
				b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "fdinfo", entry.Name()))
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
