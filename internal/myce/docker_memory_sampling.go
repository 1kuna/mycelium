package myce

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Docker's processes belong to the daemon, not the worker's process group.
// Inspect exact labels before asking top for host PIDs (including descendants).
func (d *dockerAttempt) processIDs(ctx context.Context) (map[int]bool, error) {
	d.mu.Lock()
	endpoint, identity := d.state.Endpoint, d.state.Identity
	probe := &dockerAttempt{ctx: ctx, binary: d.binary, command: d.command, env: d.env, workdir: d.workdir, timeout: d.timeout, state: dockerExecution{Endpoint: endpoint}}
	d.mu.Unlock()
	if identity == "" {
		return nil, errors.New("Docker attempt identity unavailable")
	}
	containers, err := inspectOwnedDocker(ctx, endpoint, identity, func(_ context.Context, _ string, args ...string) ([]byte, error) {
		return probe.callWithTimeout(probe.timeout, args...)
	})
	if err != nil {
		return nil, err
	}
	ids := map[int]bool{}
	for _, c := range containers {
		if !dockerLive(c) {
			continue
		}
		b, err := probe.callWithTimeout(probe.timeout, "top", c.ID, "-eo", "pid")
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(string(b))
		if len(fields) < 2 || fields[0] != "PID" {
			return nil, errors.New("Docker process inventory unavailable")
		}
		for _, field := range fields[1:] {
			pid, err := strconv.Atoi(field)
			if err != nil || pid <= 0 {
				return nil, errors.New("invalid Docker process inventory")
			}
			ids[pid] = true
		}
	}
	return ids, nil
}

func (d *dockerAttempt) observeMemory(j Job, period time.Duration) func() *AttemptMemoryPeak {
	if d.memoryWorker == nil {
		return func() *AttemptMemoryPeak { return nil }
	}
	w := *d.memoryWorker
	// Shared-parent groups already have an exact recursive PID inventory and
	// host-pressure/delta accounting. Capped containers need the daemon path.
	if a := d.envelope(); a != nil {
		return w.observeAttemptMemory(a, j, nil)
	}
	if j.Spec.Needs.cpuOnly() || w.Machine.GPU != "cuda" && w.Machine.GPU != "xpu" {
		return func() *AttemptMemoryPeak { return nil }
	}
	ctx, cancel := context.WithCancel(context.Background())
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	device := w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) {
		ids, err := d.processIDs(ctx)
		if err != nil {
			return nil, nil, err
		}
		known := map[int]bool{}
		if w.control != nil {
			// Copy handles, then perform all daemon/filesystem I/O unlocked.
			w.control.mu.Lock()
			var peers []*process
			for _, p := range w.control.processes {
				if p.docker != d {
					peers = append(peers, p)
				}
			}
			w.control.mu.Unlock()
			for _, p := range peers {
				if p.docker != nil {
					if other, err := p.docker.processIDs(ctx); err == nil {
						for pid := range other {
							known[pid] = true
						}
					}
				} else if p.memory != nil {
					_ = p.memory.processIDs(known)
				}
			}
		}
		return ids, known, nil
	}, osMemoryFS{}, period, clock, d.envelope())
	var once sync.Once
	var peak *AttemptMemoryPeak
	return func() *AttemptMemoryPeak {
		once.Do(func() {
			cancel()
			timer := time.NewTimer(attemptMemorySamplePeriod)
			defer timer.Stop()
			select {
			case r := <-device:
				peak = sampledMemoryPeak(peak, r.peak)
				if r.unreported && r.peak.DeviceGB == nil {
					peak = sampledMemoryPeak(peak, unifiedDevicePeak(false, r.shared, 0))
				}
			case <-timer.C:
				peak = sampledMemoryPeak(peak, &AttemptMemoryPeak{DeviceUnavailable: deviceMemoryTimeout})
			}
		})
		return peak
	}
}
