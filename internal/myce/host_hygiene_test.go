package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Other worker fixtures keep the new optional hardware boundary synthetic.
// Health tests inject their observations explicitly.
func init() { workerHostHealthSource = nil }

func healthFixture(t *testing.T, scale float64) (*hostHealthMonitor, *fakeClock) {
	t.Helper()
	c := &fakeClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	m, err := openHostHealth(t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	m.source = func(context.Context) (HostHealthReading, error) {
		return HostHealthReading{Load: scale, Pressure: 1, SwapBytes: 0, WakeSeconds: .001}, nil
	}
	m.disks = map[string]float64{"disk": scale * 1e9}
	for i := 0; i < hostHealthWarmup; i++ {
		c.Advance(signalRefresh)
		if err := m.sample(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return m, c
}

func TestHostHygieneAdaptiveDrainRecoveryRestart(t *testing.T) {
	for _, scale := range []float64{1, 128} {
		t.Run(fmt.Sprint(scale), func(t *testing.T) {
			m, c := healthFixture(t, scale)
			swap := 0.0
			m.source = func(context.Context) (HostHealthReading, error) {
				swap += scale * 1e6
				return HostHealthReading{Load: scale, Pressure: 4, SwapBytes: swap, WakeSeconds: .001}, nil
			}
			for i := 0; i < hostHealthConfirmation; i++ {
				c.Advance(signalRefresh)
				if err := m.sample(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if m.health.DrainReason != "host_memory_pressure" {
				t.Fatal("did not drain", m.health)
			}
			reopened, err := openHostHealth(m.dir, c)
			if err != nil {
				t.Fatal(err)
			}
			if reopened.health.DrainReason != m.health.DrainReason || len(reopened.health.History) != len(m.health.History) {
				t.Fatal("lost pre-restart evidence")
			}
			reopened.source = func(context.Context) (HostHealthReading, error) {
				return HostHealthReading{Load: scale, Pressure: 1, SwapBytes: swap, WakeSeconds: .001}, nil
			}
			for i := 0; i < hostHealthConfirmation; i++ {
				c.Advance(signalRefresh)
				if err := reopened.sample(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if reopened.health.DrainReason != "" {
				t.Fatal("did not recover", reopened.health.DrainReason)
			}
			if len(reopened.health.Baseline) != hostHealthWarmup+1 {
				t.Fatal("degradation contaminated baseline", len(reopened.health.Baseline))
			}
		})
	}
}

func TestHostHygieneZeroPressureBaseline(t *testing.T) {
	for _, retainedDrain := range []bool{false, true} {
		t.Run(fmt.Sprint("retained_drain=", retainedDrain), func(t *testing.T) {
			c := &fakeClock{now: time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)}
			m, err := openHostHealth(t.TempDir(), c)
			if err != nil {
				t.Fatal(err)
			}
			sample := func(r HostHealthReading) {
				t.Helper()
				r.pressurePSI = true
				m.source = func(context.Context) (HostHealthReading, error) { return r, nil }
				c.Advance(signalRefresh)
				if err := m.sample(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			idle := HostHealthReading{Load: 1, WakeSeconds: .001}
			for range hostHealthHistorySize {
				sample(idle)
			}
			if retainedDrain {
				// Reproduce a drain persisted by the old zero-width detector.
				m.health.DrainReason = "host_memory_pressure"
				if err := m.persist(m.health); err != nil {
					t.Fatal(err)
				}
				m, err = openHostHealth(m.dir, c)
				if err != nil {
					t.Fatal(err)
				}
			}
			// A slow queue alone must not turn useful workload PSI into a
			// local memory-pressure diagnosis.
			m.heartbeat = 1
			for _, psi := range []float64{.06, .05, .04, .03} {
				sample(HostHealthReading{Load: 2, Pressure: psi, WakeSeconds: idle.WakeSeconds})
			}
			if m.health.DrainReason != "" {
				t.Fatal("ordinary pressure without swap/control degradation did not stay healthy or recover", m.health.DrainReason)
			}
			_, healthyPressure := healthBand(m.health.Baseline, func(s HostHealthSample) float64 { return s.Pressure })
			if healthyPressure <= 0 {
				t.Fatal("ordinary workload did not establish a measured pressure reference")
			}
			swap := 0.0
			for range hostHealthConfirmation {
				swap += 1 << 20
				sample(HostHealthReading{Load: 2, Pressure: 6, SwapBytes: swap, WakeSeconds: idle.WakeSeconds})
			}
			if m.health.DrainReason != "host_memory_pressure" {
				t.Fatal("sustained pressure and swap growth did not drain", m.health.DrainReason)
			}
			m, err = openHostHealth(m.dir, c)
			if err != nil {
				t.Fatal(err)
			}
			for range hostHealthConfirmation {
				sample(HostHealthReading{Load: 2, Pressure: .03, SwapBytes: swap, WakeSeconds: idle.WakeSeconds})
			}
			if m.health.DrainReason != "" {
				t.Fatal("settled workload did not recover after restart", m.health.DrainReason)
			}
			for range hostHealthConfirmation {
				sample(HostHealthReading{Load: 2, Pressure: 6, SwapBytes: swap, WakeSeconds: idle.WakeSeconds})
			}
			// #1110 establishes that a positive learned reference is no more
			// evidence of harm than a zero one: PSI alone must stay eligible.
			if m.health.DrainReason != "" {
				t.Fatal("uncorroborated pressure outlier drained", m.health.DrainReason)
			}
		})
	}
}

func TestHostHygieneColdPressureWithSwapGrowth(t *testing.T) {
	for _, learnedTail := range []bool{false, true} {
		t.Run(fmt.Sprint("learned_tail=", learnedTail), func(t *testing.T) {
			c := &fakeClock{now: time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)}
			h := HostHealth{}
			for range hostHealthWarmup {
				c.Advance(signalRefresh)
				h.observe(HostHealthSample{At: c.Now()})
			}
			if learnedTail {
				c.Advance(signalRefresh)
				h.observe(HostHealthSample{At: c.Now(), HostHealthReading: HostHealthReading{Pressure: 2.16, pressurePSI: true}})
			}
			baseline := len(h.Baseline)
			for i := range hostHealthConfirmation {
				c.Advance(signalRefresh)
				h.observe(HostHealthSample{At: c.Now(), HostHealthReading: HostHealthReading{Pressure: 6.17, SwapBytes: float64(i+1) * 38e6 * signalRefresh.Seconds(), pressurePSI: true}})
			}
			if h.DrainReason != "host_memory_pressure" || len(h.Baseline) != baseline {
				t.Fatal("corroborated pressure was accepted into the healthy baseline", h.DrainReason, len(h.Baseline), baseline)
			}
		})
	}
}

func TestHostHygieneDiskUsesMeasuredConsumption(t *testing.T) {
	for _, scale := range []float64{1, 10000} {
		t.Run(fmt.Sprint(scale), func(t *testing.T) {
			m, c := healthFixture(t, scale)
			m.disks = map[string]float64{"disk": scale * .99e9}
			for i := 0; i < hostHealthConfirmation; i++ {
				c.Advance(signalRefresh)
				if err := m.sample(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if m.health.DrainReason != "" {
				t.Fatal("harmless allocation drained large free capacity")
			}
			m.disks = map[string]float64{"disk": scale * .01e9}
			for i := 0; i < hostHealthConfirmation; i++ {
				c.Advance(signalRefresh)
				if err := m.sample(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if m.health.DrainReason != "host_disk_pressure" {
				t.Fatal("measured burst exhaustion did not drain")
			}
			for i := 0; i < hostHealthHistorySize+hostHealthConfirmation; i++ {
				c.Advance(signalRefresh)
				if err := m.sample(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if m.health.DrainReason != "host_disk_pressure" {
				t.Fatal("history expiry falsely recovered unchanged low free disk")
			}
			reopened, err := openHostHealth(m.dir, c)
			if err != nil {
				t.Fatal(err)
			}
			reopened.source = m.source
			reopened.disks = map[string]float64{"disk": scale * 1e9}
			for i := 0; i < hostHealthConfirmation; i++ {
				c.Advance(signalRefresh)
				if err := reopened.sample(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if reopened.health.DrainReason != "" {
				t.Fatal("affirmative disk recovery did not resume after restart")
			}
		})
	}
}

func TestHostHygieneBlockedPersistenceDoesNotBlockHeartbeat(t *testing.T) {
	m, c := healthFixture(t, 1)
	blocked, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	m.persist = func(HostHealth) error { close(blocked); <-release; return nil }
	c.Advance(signalRefresh)
	go func() { done <- m.sample(context.Background()) }()
	<-blocked
	snapshot := make(chan *HostHealth, 1)
	go func() { snapshot <- m.snapshot(nil) }()
	select {
	case h := <-snapshot:
		if len(h.History) != hostHealthWarmup+1 {
			t.Error("missing fresh in-memory evidence")
		}
	case <-time.After(time.Second):
		t.Error("blocked persistence stalled heartbeat snapshot")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(m.dir, "host-health.json")); err != nil {
		t.Fatal("lost previously persisted history", err)
	}
}
