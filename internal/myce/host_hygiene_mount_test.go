package myce

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHostHygieneMountSlowHistoryDoesNotWidenBaseline(t *testing.T) {
	m, c := healthFixture(t, 1)
	for i := range m.health.Baseline {
		m.health.Baseline[i].Mounts = []HostMountHealth{{Root: "checkpoint", LatencySeconds: .001}}
	}
	p := &hostMountProbe{root: "checkpoint", path: "sentinel", measure: func(string) hostMountResult { return hostMountResult{latency: 100 * time.Millisecond} }}
	m.mounts["checkpoint"] = p
	for i := 0; i < hostHealthConfirmation+1; i++ {
		c.Advance(signalRefresh)
		if err := m.sample(context.Background()); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { return len(p.pending) == 1 })
	}
	if got := m.health.History[len(m.health.History)-1].Mounts[0].Reason; got != "mount_probe_slow" {
		t.Fatal("slow probes contaminated healthy baseline", got)
	}
	for _, s := range m.health.Baseline {
		for _, mount := range s.Mounts {
			if mount.LatencySeconds > .001 {
				t.Fatal("adverse probe entered baseline")
			}
		}
	}
}

func TestHostHygieneMountSingleOutstandingAndScopedRecovery(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	release := make(chan struct{})
	var calls atomic.Int32
	p := &hostMountProbe{root: "checkpoint", path: "sentinel", probe: func(string) error { calls.Add(1); <-release; return nil }}
	baseline := make([]HostHealthSample, hostHealthWarmup)
	for i := range baseline {
		baseline[i].Mounts = []HostMountHealth{{Root: "checkpoint", LatencySeconds: .001}}
	}
	p.sample(now, baseline)
	await(t, func() bool { return calls.Load() == 1 })
	for i := 0; i < hostHealthConfirmation; i++ {
		p.sample(now.Add(time.Hour), baseline)
	}
	for i := 0; i < 100; i++ {
		if got := p.sample(now.Add(time.Hour), baseline); got.Reason != "mount_probe_stalled" {
			t.Fatal(got)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("timeout created probe swarm")
	}
	m := Machine{HostHealth: &HostHealth{History: []HostHealthSample{{Mounts: []HostMountHealth{{Root: "checkpoint", Reason: "mount_probe_stalled"}}}}}}
	applyHostMountHealth(Worker{}, &m)
	local := &Job{Spec: cpuSpec(1, 1)}
	local.Spec.Resumable = false
	local.Spec.Priority = "production"
	if storageRootReason(local, &m) != "" {
		t.Fatal("local work blocked by remote failure")
	}
	dependent := &Job{Spec: cpuSpec(1, 1)}
	dependent.Spec.SharedInputs = map[string]SharedInput{"data": {NAS: "data", SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	if storageRootReason(dependent, &m) == "" {
		t.Fatal("dependent work not withheld")
	}
	close(release)
	await(t, func() bool {
		select {
		case <-p.pending:
			p.pending = nil
			return true
		default:
			return false
		}
	})
}
