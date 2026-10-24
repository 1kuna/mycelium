package myce

import (
	"context"
	"encoding/csv"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
		for _, name := range []string{"pgscan_anon", "pgsteal_anon", "workingset_refault_anon", "pswpout"} {
			p.Constrained = p.Constrained || stats[name] > 0
		}
	}
	if b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.events")); err == nil {
		events := memoryCounters(b)
		p.Constrained = p.Constrained || events["oom"] > 0 || events["oom_kill"] > 0 || events["oom_group_kill"] > 0
	}
	for _, name := range []string{"memory.swap.peak", "memory.swap.current"} {
		if b, err := a.fs.ReadFile(filepath.Join(a.path, name)); err == nil {
			n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
			p.Constrained = p.Constrained || err != nil || n > 0
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

// Sample only processes inside this exact attempt. Driver/device totals include
// other workloads and cannot serve as job peaks. Sampling is a lower bound.
func (w Worker) observeAttemptMemory(a *memoryAttempt, j Job) func() *AttemptMemoryPeak {
	if a == nil {
		return func() *AttemptMemoryPeak { return nil }
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	peak := a.peak()
	deviceSupported := estimateFeedingAttempt(j) && !j.Spec.Needs.cpuOnly() && (w.Machine.GPU == "cuda" || w.Machine.GPU == "xpu")
	go func() {
		defer close(done)
		// Avoid a per-process nvidia-smi probe twice a second on production.
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				peak = sampledMemoryPeak(peak, a.peak())
				if !deviceSupported {
					continue
				}
				ids := map[int]bool{}
				if a.processIDs(ids) != nil || len(ids) == 0 {
					continue
				}
				gb, ok := w.attemptDeviceMemory(ctx, ids)
				if !ok {
					deviceSupported = false
					continue
				}
				peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceGB: &gb})
			}
		}
	}()
	var once sync.Once
	return func() *AttemptMemoryPeak {
		once.Do(func() {
			cancel()
			<-done
			peak = sampledMemoryPeak(peak, a.peak())
		})
		return peak
	}
}

func (w Worker) attemptDeviceMemory(ctx context.Context, ids map[int]bool) (float64, bool) {
	if w.Machine.GPU == "cuda" {
		selector := w.Machine.Device
		if w.selectedDevice != nil {
			selector = w.selectedDevice.Selector
		}
		if !strings.HasPrefix(selector, "GPU-") {
			return 0, false
		}
		probe, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		b, err := exec.CommandContext(probe, "nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_gpu_memory", "--format=csv,noheader,nounits").Output()
		if err != nil {
			return 0, false
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
				key, gb, ok := drmAttemptMemory(b, w.selectedDevice.pciAddress)
				if ok {
					clients[key] = max(clients[key], gb)
				}
			}
		}
		total := 0.0
		for _, gb := range clients {
			total += gb
		}
		return total, len(clients) > 0
	}
	return 0, false
}

func cudaAttemptMemory(b []byte, selector string, ids map[int]bool) (float64, bool) {
	rows, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
	if err != nil {
		return 0, false
	}
	total, found := 0.0, false
	for _, row := range rows {
		if len(row) != 3 {
			return 0, false
		}
		if strings.TrimSpace(row[0]) != selector {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(row[1]))
		if err != nil {
			return 0, false
		}
		if !ids[pid] {
			continue
		}
		mib, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
		if err != nil || !validMemoryValue(mib) {
			return 0, false
		}
		total += mib / 1024
		found = true
	}
	return total, found
}

func drmAttemptMemory(b []byte, pci string) (string, float64, bool) {
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
				if len(f) != 2 || f[1] != "KiB" {
					return "", 0, false
				}
				n, err := strconv.ParseUint(f[0], 10, 64)
				if err != nil {
					return "", 0, false
				}
				total += float64(n) / (1 << 20)
				found = true
			}
		}
	}
	return pdev + "/" + client, total, found && client != "" && normalizePCIAddress(pdev) == normalizePCIAddress(pci)
}
