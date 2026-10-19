package myce

import (
	"context"
	"errors"
	"time"
)

func (q *Queue) hostBenchmarkPending(host string) bool {
	for _, m := range q.s.Machines {
		if hostID(m) == host && m.BenchmarkPending {
			return true
		}
	}
	return false
}

// A benchmark owns the host while onboarding. Reuse preparation offers and the
// common launch gate, including a final check in each command supervisor.
func (q *Queue) benchmarkIdle(m *Machine) bool {
	for _, p := range q.hostObligations(hostID(m)) {
		if p.Launch == nil || p.Launch.Machine != m.ID || p.Launch.Session != m.Session || p.Launch.Kind != "benchmark" {
			return false
		}
	}
	return true
}

func (w *Worker) runBenchmark(stop context.Context, s *workerStore) error {
	// Fresh-session release is truthful only after every old child is cleaned.
	if err := cleanupEngines(s.dir, w.Clock); err != nil {
		return err
	}
	if err := cleanupRecordedProcesses(s.dir, "attempt-command-*.json", w.Clock); err != nil {
		return err
	}
	m := cloneMachine(&w.Machine)
	m.BenchmarkPending = true
	m.ModelURL = ""
	m.ModelConfigHash = modelConfigHash(w.ModelConfig)
	checkin := func(done string) (Reply, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		free, probeErr := w.HostMemorySource(ctx)
		m.HostAvailableMemoryGB, m.HostMemoryError = free, ""
		if probeErr != nil || !validMemoryValue(free) {
			m.HostAvailableMemoryGB, m.HostMemoryError = 0, "host RAM probe failed"
		}
		a := w.availability.snapshot()
		m.CapabilityProfile.Availability = &a
		var reply Reply
		err := w.Client.Call(ctx, "POST", "/api/workers/checkin", Checkin{Machine: m, Fresh: s.state.Fresh, PreviousSession: s.state.PreviousSession, BenchmarkDone: done}, &reply)
		if err == nil && s.state.Fresh {
			s.state.Fresh = false
			err = s.save()
		}
		return reply, err
	}
	reply, err := checkin("")
	if err != nil {
		return err
	}
	if reply.BenchmarkPlan == nil {
		// Maintenance or existing work withholds onboarding; ordinary worker
		// checkins continue, and a subsequent join may retry the benchmark.
		return nil
	}
	launch := *reply.BenchmarkPlan
	ctx, cancel := context.WithCancel(stop)
	defer cancel()
	ctx = withLaunchGate(ctx, func() error { return w.authorizeLaunch(launch) })
	if err := checkLaunchGate(ctx); err != nil {
		return err
	}
	profile := *w.Machine.CapabilityProfile
	done := make(chan error, 1)
	go func() {
		done <- w.Benchmark(ctx, BenchmarkConfig{Clock: w.Clock, Machine: w.Machine, StateDir: s.dir, ExtensionRoot: w.ExtensionRoot, Client: w.Client, DailyUse: w.DailyUse, Laptop: w.Laptop, Desktop: w.Desktop, OwnerSignals: w.OwnerSignals}, &profile)
	}()
	// Keep health fresh and receive disable-stop while the benchmark runs.
	tick := time.NewTicker(min(w.Interval, time.Second))
	defer tick.Stop()
	var heartbeatErr error
	stopCh := stop.Done()
	for {
		select {
		case err := <-done:
			if cleanupErr := cleanupRecordedProcesses(s.dir, "attempt-command-*.json", w.Clock); cleanupErr != nil {
				return errors.Join(err, heartbeatErr, cleanupErr) // Retain queue custody.
			}
			m.BenchmarkPending = false
			_, ackErr := checkin(launch.Attempt)
			w.Machine.CapabilityProfile = &profile
			return errors.Join(err, heartbeatErr, ackErr)
		case <-stopCh:
			cancel()
			stopCh = nil
		case <-tick.C:
			reply, err := checkin("")
			if err != nil {
				heartbeatErr = err
				cancel()
			} else if reply.PreparationStopAt != nil || reply.Leave {
				cancel()
			}
		}
	}
}
