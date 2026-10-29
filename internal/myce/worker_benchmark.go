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

// Onboarding is one host-wide attempt and measures every inventory device.
// Defer a new attempt as a whole; ordinary worker admission can still proceed.
func (q *Queue) benchmarkDeviceAdmissionBlocked(m *Machine) bool {
	for _, d := range m.Devices {
		if q.s.DeviceMaintenance[hostID(m)][deviceMaintenanceKey(d.Platform, d.Identity)].Disabled || q.componentExcluded(m, d.Platform, d.Identity) {
			return true
		}
	}
	return q.deviceAdmissionBlocked(m)
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
	var curves []WorkObservation
	checkin := func(done string) (Reply, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		m.DiskVolumes = w.sampleDisks(s.dir, s.state.CheckpointRoot)
		free, probeErr := w.HostMemorySource(ctx)
		m.HostAvailableMemoryGB, m.HostMemoryError = free, ""
		if probeErr != nil || !validMemoryValue(free) {
			m.HostAvailableMemoryGB, m.HostMemoryError = 0, "host RAM probe failed"
		}
		a := w.availability.snapshot()
		m.CapabilityProfile.Availability = &a
		reply, err := w.checkin(ctx, Checkin{Machine: m, Fresh: s.state.Fresh, PreviousSession: s.state.PreviousSession, BenchmarkDone: done, CurveReports: curves})
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
		var rejected *HTTPError
		if errors.As(err, &rejected) && rejected.Code == "request_stale" {
			// A policy edit can invalidate the cached offer before its first
			// launch. Release it and continue ordinary worker startup.
			m.BenchmarkPending = false
			_, ackErr := checkin(launch.Attempt)
			return ackErr
		}
		return err
	}
	profile := *w.Machine.CapabilityProfile
	benchmarkMachine := m
	if reply.BenchmarkMachine != nil {
		benchmarkMachine = *reply.BenchmarkMachine
	}
	done := make(chan error, 1)
	go func() {
		done <- w.Benchmark(ctx, BenchmarkConfig{PreviousCurves: reply.BenchmarkCurves, CurveMaxConcurrency: w.CurveMaxConcurrency, EnvironmentCache: w.environmentCache, Clock: w.Clock, Machine: benchmarkMachine, StateDir: s.dir, ExtensionRoot: w.ExtensionRoot, Client: w.Client, DailyUse: w.DailyUse, Laptop: w.Laptop, Desktop: w.Desktop, OwnerSignals: w.OwnerSignals}, &profile)
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
			m.CapabilityProfile = &profile
			if profile.Benchmark != nil {
				curves = profile.Benchmark.Curves
			}
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
