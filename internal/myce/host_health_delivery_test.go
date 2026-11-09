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

func TestDevHostHealthReportCadence(t *testing.T) {
	// Exercise the actual asynchronous health lane, report preparation, HTTP
	// check-in and launch fences. Other dev fixtures leave this sensor disabled.
	previous := workerHostHealthSource
	previousForeign := workerForeignConsumers
	workerForeignConsumers = nil
	workerHostHealthSource = func(context.Context) (HostHealthReading, error) {
		return HostHealthReading{Load: 1, Pressure: 1, WakeSeconds: .001}, nil
	}
	t.Cleanup(func() { workerHostHealthSource, workerForeignConsumers = previous, previousForeign })
	for _, scenario := range []struct {
		name     string
		interval time.Duration
		slow     bool
	}{{"1s", time.Second, false}, {"2s", 2 * time.Second, false}, {"slow-checkins", time.Second, true}} {
		interval := scenario.interval
		t.Run(scenario.name, func(t *testing.T) {
			root := newTestStateRoot(t)
			c := DevConfig{EnvironmentCacheLimit: 1 << 30, Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "health-cadence-token", Workers: 1, MemoryGB: 8, Interval: interval}
			d, err := startDev(c, func(q *Queue) { fakeCheckpointAvailability(t, q, true) })
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
			probeWait(t, ctx, func() bool {
				d.Queue.mu.Lock()
				defer d.Queue.mu.Unlock()
				m := d.Queue.s.Machines["fake-1"]
				return m != nil && m.HostHealth != nil && len(m.HostHealth.History) >= 3 && hostHealthEvidenceValid(m.HostHealth, 0)
			})
			if scenario.slow {
				// Force actual HTTP check-ins to wait behind the real queue
				// mutex longer than the sensor's frozen validity. Independent
				// health probes continue while the request is blocked.
				for range 2 {
					d.Queue.mu.Lock()
					before := d.Queue.s.Machines["fake-1"]
					accepted, sensor := before.LastSeen, before.HostHealth.History[len(before.HostHealth.History)-1].At
					delay := before.HostHealth.Probe.freshness() + interval
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						d.Queue.mu.Unlock()
						t.Fatal(ctx.Err())
					}
					reason := d.Queue.hostHygieneReason(before)
					d.Queue.mu.Unlock()
					if reason != "" {
						t.Fatal("queue wait invalidated healthy dev sensor", reason)
					}
					probeWait(t, ctx, func() bool {
						d.Queue.mu.Lock()
						defer d.Queue.mu.Unlock()
						m := d.Queue.s.Machines["fake-1"]
						if !m.LastSeen.After(accepted) {
							return false
						}
						if reason := d.Queue.hostHygieneReason(m); reason != "" {
							t.Fatal("delayed check-in invalidated healthy dev sensor", reason)
						}
						return m.HostHealth.History[len(m.HostHealth.History)-1].At.After(sensor)
					})
				}
			}
			s := cpuSpec(1, 1)
			s.Resumable = false
			s.Run.Command = []string{"sh", "-c", "printf healthy"}
			var job Job
			if err := d.Client().Call(ctx, "POST", "/api/jobs", s, &job); err != nil {
				t.Fatal(err)
			}
			until := time.Now().Add(3 * interval)
			completed := false
			var first, last time.Time
			probeWait(t, ctx, func() bool {
				d.Queue.mu.Lock()
				defer d.Queue.mu.Unlock()
				m := d.Queue.s.Machines["fake-1"]
				if reason := d.Queue.hostHygieneReason(m); reason != "" {
					t.Fatal("reporting dev worker became unhealthy", reason)
				}
				last = m.HostHealth.History[len(m.HostHealth.History)-1].At
				if first.IsZero() {
					first = last
				}
				j := d.Queue.s.Jobs[job.ID]
				completed = j != nil && j.State == "succeeded" && j.Attempt == 1 && j.Losses == 0 && j.ReleaseConfirmed
				// Three reporting intervals prove continuing sensor delivery;
				// process launch/finalization must also finish within the outer
				// context, rather than an unrelated three-heartbeat deadline.
				return time.Now().After(until) && completed && last.After(first)
			})
			if !completed || !last.After(first) {
				t.Fatalf("healthy admission/completion=%v, sensor advanced=%v", completed, last.After(first))
			}
		})
	}
}
