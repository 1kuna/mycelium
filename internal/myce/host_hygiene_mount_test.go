package myce

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

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
