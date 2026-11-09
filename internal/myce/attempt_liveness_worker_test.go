package myce

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestAttemptLivenessSamplerDoesNotBlockHeartbeat(t *testing.T) {
	previous := workerLivenessCPU
	defer func() { workerLivenessCPU = previous }()
	entered := make(chan struct{})
	workerLivenessCPU = func(ctx context.Context, root int, birth string) (float64, string, error) {
		if root != 42 || birth != "owned-birth" {
			t.Errorf("wrong root %d", root)
		}
		close(entered)
		<-ctx.Done()
		return 0, "", ctx.Err()
	}
	clock := &fakeClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	p := &process{cmd: &exec.Cmd{Process: &os.Process{Pid: 42}}, birth: "owned-birth", job: Job{ID: "job", Attempt: 1}}
	sampler := startAttemptLiveness(context.Background(), clock, time.Second, p, t.TempDir())
	defer func() { sampler.cancel(); <-sampler.done }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("collector did not start")
	}
	// Snapshot remains available while the sole collector is in OS I/O.
	done := make(chan *LivenessReport, 1)
	go func() { done <- sampler.snapshot(clock.Now()) }()
	select {
	case r := <-done:
		if r.CPU.Value != nil {
			t.Fatal("invented a sample")
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat blocked on collector")
	}
}

func TestAttemptLivenessRejectsReusedAnchorPID(t *testing.T) {
	data := []byte("42 1 42 00:10.00 Tue Nov 3 12:00:00 2026\n")
	observedBirth := "owned-birth"
	inventoryCalls := 0
	birth := func(pid int) (string, error) {
		if pid != 42 {
			t.Fatalf("wrong anchor %d", pid)
		}
		return observedBirth, nil
	}
	inventory := func(context.Context) ([]byte, error) { inventoryCalls++; return data, nil }
	if n, _, err := sampleLivenessCPU(context.Background(), 42, "owned-birth", birth, inventory); err != nil || n != 10 {
		t.Fatal("owned anchor unavailable", n, err)
	}
	// The native supervisor can be reaped while output publication is pending.
	// Its reused PID is stable across each probe, but belongs to a different job.
	observedBirth = "foreign-birth"
	for i := 0; i < 2; i++ {
		if _, _, err := sampleLivenessCPU(context.Background(), 42, "owned-birth", birth, inventory); err == nil {
			t.Fatal("foreign reused anchor supplied CPU")
		}
	}
	if inventoryCalls != 1 {
		t.Fatal("foreign anchor reached process inventory", inventoryCalls)
	}
	observedBirth = "owned-birth"
	if _, _, err := sampleLivenessCPU(context.Background(), 42, "owned-birth", birth, func(context.Context) ([]byte, error) { observedBirth = "foreign-birth"; return data, nil }); err == nil {
		t.Fatal("PID reuse during inventory supplied CPU")
	}
}
