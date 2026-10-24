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
			p.HostGB = &gb
		}
	}
	if b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.events")); err == nil {
		f := strings.Fields(string(b))
		for i := 0; i+1 < len(f); i += 2 {
			if f[i] == "max" {
				n, err := strconv.ParseUint(f[i+1], 10, 64)
				p.Constrained = err != nil || n > 0
			}
		}
	}
	if p.HostGB == nil {
		return nil
	}
	return p
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
func (w Worker) observeAttemptMemory(a *memoryAttempt) func() *AttemptMemoryPeak {
	if a == nil {
		return func() *AttemptMemoryPeak { return nil }
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var device *float64
	go func() {
		defer close(done)
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				ids := map[int]bool{}
				if a.processIDs(ids) != nil || len(ids) == 0 {
					continue
				}
				gb, ok := w.attemptDeviceMemory(ctx, ids)
				if ok && (device == nil || gb > *device) {
					n := gb
					device = &n
				}
			}
		}
	}()
	var once sync.Once
	var peak *AttemptMemoryPeak
	return func() *AttemptMemoryPeak {
		once.Do(func() {
			cancel()
			<-done
			peak = a.peak()
			if peak == nil && device != nil {
				peak = &AttemptMemoryPeak{}
			}
			if peak != nil {
				peak.DeviceGB = device
			}
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
