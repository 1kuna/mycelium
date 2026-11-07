package myce

import (
	"context"
	"runtime"
	"sync"
	"time"
)

// This consumes #858's existing per-attempt observer. Memory uses the existing
// custody-fenced probes; there is no second CPU/I/O sampler or host-wide credit.
func (l *LlamaEngine) observeReadinessActivity(ctx context.Context, p *readinessProgress) func() {
	w := Worker{Machine: l.memoryMachine, Clock: p.clock}
	if l.readinessWorker != nil {
		w = *l.readinessWorker
		w.Clock = p.clock
	}
	if w.processMemory == nil && workerProcessMemory != nil {
		w.processMemory = workerProcessMemory()
	}
	memory := l.memoryAttempt
	owned := &process{cmd: l.cmd, memory: memory}
	owned.birth, _ = processBirth(l.cmd.Process.Pid)
	resources := w.observeResources(owned, memory)
	deviceCtx, cancelDevice := context.WithCancel(ctx)
	var deviceDone <-chan deviceMemoryResult
	if memory != nil && (w.Machine.GPU == "cuda" || w.Machine.GPU == "xpu") {
		a := memory
		deviceDone = w.observeDeviceMemory(deviceCtx, func() (map[int]bool, map[int]bool, error) {
			ids := map[int]bool{}
			err := a.processIDs(ids)
			return ids, nil, err
		}, a.fs, deviceMemorySamplePeriod, p.clock, a)
	}
	source := readinessActivitySource(resources.snapshot, func() (*float64, *float64) {
		if sample := memory.currentHostMemory(p.clock); sample != nil {
			return &sample.NonreclaimableGB, sample.DeviceGB
		}
		if w.processMemory != nil && w.processMemory.Resident != nil {
			bytes, members, err := readinessProcessResident(w.processMemory, owned.rssRoots(), processBirth)
			if err == nil && members > 0 {
				gb := float64(bytes) / (1 << 30)
				return &gb, nil
			}
		}
		return nil, nil
	})
	p.mu.Lock()
	// Silence cannot be established before the next shared sensor observation.
	// Two sampling periods allow one interval plus observation scheduling jitter;
	// this is an observability floor, never a deadline on an unknown cold load.
	p.activityPeriod = readinessSensorPeriod(memory != nil, runtime.GOOS, deviceDone != nil)
	p.osObserver, p.activitySource = true, source
	p.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancelDevice()
			resources.finish()
			if deviceDone != nil {
				<-deviceDone
			}
			p.mu.Lock()
			p.activitySource, p.osObserver, p.activityPeriod = nil, false, 0
			p.mu.Unlock()
		})
	}
}

// Consume each shared sample once. Cached values allow concurrent status and
// supervision readers to see the same observation without another OS read.
func readinessActivitySource(snapshot func() *AttemptResources, memory func() (*float64, *float64)) func() (time.Time, []*float64) {
	var mu sync.Mutex
	var previous time.Time
	var values []*float64
	return func() (time.Time, []*float64) {
		mu.Lock()
		defer mu.Unlock()
		r := snapshot()
		if r == nil {
			return time.Time{}, nil
		}
		if r.SampledAt.Equal(previous) {
			return previous, values
		}
		previous = r.SampledAt
		host, device := memory()
		values = []*float64{r.CPU.UsageSeconds, nil, host, device}
		if r.IO.ReadBytes != nil {
			bytes := float64(*r.IO.ReadBytes)
			values[1] = &bytes
		}
		return previous, values
	}
}

func readinessSensorPeriod(cgroup bool, goos string, device bool) time.Duration {
	period := attemptMemorySamplePeriod
	if !cgroup && goos == "darwin" {
		period = max(period, attemptStorageSamplePeriod)
	}
	if device {
		period = max(period, deviceMemorySamplePeriod)
	}
	return period
}

// The model execs in the launch root itself. Unlike control-anchor accounting,
// include that root's RSS via the existing group inventory, with birth checks
// on both sides so a recycled group identity cannot supply readiness activity.
func readinessProcessResident(probe *processMemoryProbe, roots map[int]string, birthOf func(int) (string, error)) (uint64, int, error) {
	groups := map[int]bool{}
	check := func() error {
		for pid, saved := range roots {
			birth, err := birthOf(pid)
			if err != nil || saved == "" || birth != saved {
				return errLivenessUnavailable
			}
			groups[pid] = true
		}
		return nil
	}
	if err := check(); err != nil {
		return 0, 0, err
	}
	bytes, members, err := probe.Resident(groups)
	if err != nil {
		return 0, 0, err
	}
	if err := check(); err != nil {
		return 0, 0, err
	}
	return bytes, members, nil
}
