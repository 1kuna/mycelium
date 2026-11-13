package myce

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestHostHealthReportDeliveryAndFrozenExpiry(t *testing.T) {
	for _, interval := range []time.Duration{time.Second, 7 * time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			q, clock := fixture(t)
			monitor, err := openHostHealth(t.TempDir(), clock)
			if err != nil {
				t.Fatal(err)
			}
			monitor.persist = func(HostHealth) error { return nil }
			lane := probeTiming{base: interval, cores: 2}
			reading := HostHealthReading{Load: 1, Pressure: 1, WakeSeconds: .001}
			complete := func(sensorErr error) {
				start := clock.Now()
				clock.Advance(time.Millisecond)
				observation := lane.observe(start, clock.Now(), false)
				if err := monitor.record(context.Background(), start, reading, sensorErr, observation); err != nil {
					t.Fatal(err)
				}
			}
			legacy := machine("legacy-health", "fake")
			legacy.HostHealth = &HostHealth{History: []HostHealthSample{{At: clock.Now()}}}
			current := machine("measured-health", "fake")
			monitor.report(nil)
			clock.Advance(interval)
			monitor.report(nil) // Establish delivery before the next completion.
			for i := 0; i < 24; i++ {
				complete(nil)
				current.HostHealth = monitor.report(nil)
				check(t, q, legacy, nil)
				check(t, q, current, nil)
				// The queue sees sample age plus delivery jitter and time until
				// the next report. Cheap independent probes may complete meanwhile.
				clock.Advance(interval + time.Duration(i%4)*interval/8)
				for _, m := range []Machine{legacy, current} {
					if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "" {
						t.Fatalf("report %d worker %s: %s", i, m.ID, reason)
					}
				}
			}
			// A stopped sensor must expire even with continuing reports, local
			// admission reads and accepted queue heartbeats. Later slow delivery
			// trains future completions only.
			frozen := *current.HostHealth.Probe
			remaining := frozen.freshness() - time.Duration(current.HostHealth.AgeSeconds*float64(time.Second)) - clock.Now().Sub(q.s.Machines[current.ID].LastSeen)
			clock.Advance(remaining + time.Nanosecond)
			current.HostHealth = monitor.report(nil)
			monitor.snapshot(nil)
			check(t, q, current, nil)
			if *current.HostHealth.Probe != frozen || q.hostHygieneReason(q.s.Machines[current.ID]) != "host_probe_unknown" {
				t.Fatal("continuing contact renewed stopped sensor evidence")
			}
			t.Logf("stopped sensor expires at frozen measured age %s", frozen.freshness())
			complete(errors.New("sensor failed"))
			current.HostHealth = monitor.report(nil)
			check(t, q, current, nil)
			if q.hostHygieneReason(q.s.Machines[current.ID]) != "host_probe_unknown" {
				t.Fatal("delivery prediction hid a failed sensor")
			}
			complete(nil)
			current.HostHealth = monitor.report(nil)
			check(t, q, current, nil)
			if reason := q.hostHygieneReason(q.s.Machines[current.ID]); reason != "" {
				t.Fatal("new successful sensor evidence did not recover", reason)
			}
			for range 2 {
				submit(t, q, spec("experiment"))
			}
			for _, m := range []Machine{legacy, current} {
				if check(t, q, m, nil).Job == nil {
					t.Fatal("mixed-version worker could not admit work", m.ID)
				}
			}
		})
	}
}

// A real dev worker's health reports reach the queue over the real listener,
// keep advancing for one cadence cycle, and keep the host admitting work.
// Cadence timing and queue-delay validity are covered on a fake clock in
// host_health_queue_delay_test.go. Keep the production 1s cadence: shorter
// intervals shrink frozen validity below race-build check-in latency.
func TestDevHostHealthReportCadence(t *testing.T) {
	const interval = time.Second
	// Exercise the actual asynchronous health lane, report preparation, HTTP
	// check-in and launch fences. Other dev fixtures leave this sensor disabled.
	previous := workerHostHealthSource
	previousForeign := workerForeignConsumers
	workerForeignConsumers = nil
	workerHostHealthSource = func(context.Context) (HostHealthReading, error) {
		return HostHealthReading{Load: 1, Pressure: 1, WakeSeconds: .001}, nil
	}
	t.Cleanup(func() { workerHostHealthSource, workerForeignConsumers = previous, previousForeign })
	root := newTestStateRoot(t)
	c := DevConfig{EnvironmentCacheLimit: 1 << 30, Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "health-cadence-token", Workers: 1, MemoryGB: 8, Interval: interval}
	d, err := startTestDev(t, c, func(q *Queue) { fakeCheckpointAvailability(t, q, true) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var first time.Time
	probeWait(t, ctx, func() bool {
		d.Queue.mu.Lock()
		defer d.Queue.mu.Unlock()
		m := d.Queue.s.Machines["fake-1"]
		// Three samples warm the probe's measured validity before hygiene applies.
		if m == nil || m.HostHealth == nil || len(m.HostHealth.History) < 3 || !hostHealthEvidenceValid(m.HostHealth, 0) {
			return false
		}
		first = m.HostHealth.History[len(m.HostHealth.History)-1].At
		return true
	})
	s := cpuSpec(1, 1)
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", "printf healthy"}
	var job Job
	if err := d.Client().Call(ctx, "POST", "/api/jobs", s, &job); err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool {
		d.Queue.mu.Lock()
		defer d.Queue.mu.Unlock()
		m := d.Queue.s.Machines["fake-1"]
		if reason := d.Queue.hostHygieneReason(m); reason != "" {
			t.Fatal("reporting dev worker became unhealthy", reason)
		}
		j := d.Queue.s.Jobs[job.ID]
		completed := j != nil && j.State == "succeeded" && j.Attempt == 1 && j.Losses == 0 && j.ReleaseConfirmed
		return completed && m.HostHealth.History[len(m.HostHealth.History)-1].At.After(first)
	})
}
