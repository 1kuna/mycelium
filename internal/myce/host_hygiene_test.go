package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestHostHygieneBriefSpikeAndMissingSensor(t *testing.T) {
	m, c := healthFixture(t, 1)
	m.source = func(context.Context) (HostHealthReading, error) {
		return HostHealthReading{Load: 100, WakeSeconds: 1}, nil
	}
	c.Advance(signalRefresh)
	if err := m.sample(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.source = func(context.Context) (HostHealthReading, error) {
		return HostHealthReading{}, errors.New("private sensor detail")
	}
	c.Advance(signalRefresh)
	if err := m.sample(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.source = func(context.Context) (HostHealthReading, error) {
		return HostHealthReading{Load: 1, Pressure: 1, WakeSeconds: .001}, nil
	}
	c.Advance(signalRefresh)
	if err := m.sample(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.health.DrainReason != "" {
		t.Fatal("brief spike drained")
	}
	if strings.Contains(fmt.Sprint(m.health), "private sensor") {
		t.Fatal("raw probe error leaked")
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
