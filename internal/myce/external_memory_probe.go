package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/probeguard"
)

// ps is available on both supported platforms. Read the whole tree once so
// concurrent process groups and the worker itself are counted only once.
func workerTreeRSS(data []byte, root int) (float64, error) {
	type row struct {
		pid, ppid int
		rss       uint64
	}
	var rows []row
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if len(f) != 3 {
			return 0, errors.New("invalid process memory inventory")
		}
		pid, e1 := strconv.Atoi(f[0])
		parent, e2 := strconv.Atoi(f[1])
		rss, e3 := strconv.ParseUint(f[2], 10, 64)
		if e1 != nil || e2 != nil || e3 != nil {
			return 0, errors.New("invalid process memory inventory")
		}
		rows = append(rows, row{pid, parent, rss})
	}
	owned := map[int]bool{root: true}
	for changed := true; changed; {
		changed = false
		for _, r := range rows {
			if owned[r.ppid] && !owned[r.pid] {
				owned[r.pid] = true
				changed = true
			}
		}
	}
	var total uint64
	found := false
	for _, r := range rows {
		if owned[r.pid] {
			total += r.rss
			found = found || r.pid == root
		}
	}
	if !found {
		return 0, errors.New("worker missing from process inventory")
	}
	return float64(total) / (1 << 20), nil
}

var workerMemoryRSS MemorySource = currentWorkerRSS

func currentWorkerRSS(ctx context.Context) (float64, error) {
	probeguard.Check()
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	data, err := exec.CommandContext(c, "ps", "-ax", "-o", "pid=,ppid=,rss=").Output()
	if err != nil {
		return 0, err
	}
	return workerTreeRSS(data, os.Getpid())
}

func (w Worker) hostMemoryObservation(ctx context.Context, m Machine, available float64, probeErr error) *HostMemoryObservation {
	if probeErr != nil || m.CapabilityProfile == nil || m.CapabilityProfile.SystemMemoryBytes == 0 {
		return nil
	}
	// Raw availability is retained even when admission availability clamps to zero.
	if !validMemoryValue(available) {
		return nil
	}
	total := float64(m.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
	used := total - available
	if !validMemoryValue(used) {
		return nil
	}
	if workerMemoryRSS == nil {
		return nil
	}
	rss, err := workerMemoryRSS(ctx)
	if err != nil || rss > used {
		return nil
	}
	return &HostMemoryObservation{TotalGB: total, UsedGB: used, MyceliumGB: rss}
}

// Preserve raw reclaimable RAM before the admission-only reserve/clamp. Explicit
// legacy sources without raw evidence can be reconstructed only above the clamp.
func (w Worker) sampleHostMemory(ctx context.Context) (admission, raw float64, err error) {
	if w.hostRawMemorySource != nil {
		raw, err = w.hostRawMemorySource(ctx)
		return max(0, raw-w.HostMemoryReserveGB), raw, err
	}
	admission, err = w.HostMemorySource(ctx)
	if admission == 0 && w.HostMemoryReserveGB > 0 {
		return admission, -1, err
	}
	return admission, admission + w.HostMemoryReserveGB, err
}
