package myce

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAttemptLivenessWorkDirBoundAndSymlinks(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "out"), []byte("hello"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	n, err := scanLivenessWork(context.Background(), dir)
	if err != nil || n != 5 {
		t.Fatal(n, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "out"), []byte("hello world"), 0600); err != nil {
		t.Fatal(err)
	}
	n, err = scanLivenessWork(context.Background(), dir)
	if err != nil || n != 11 {
		t.Fatal(n, err)
	}
	for i := 0; i < livenessScanEntries; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprint(i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = scanLivenessWork(context.Background(), dir); err == nil {
		t.Fatal("partial directory total treated as known")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := livenessWorkBytes(ctx, dir); err == nil {
		t.Fatal("cancel ignored")
	}
}

func TestAttemptLivenessProcessTreeCPU(t *testing.T) {
	data := []byte("10 1 10 00:01.50 Tue Nov 3 12:00:00 2026\n11 1 10 00:02.00 Tue Nov 3 12:00:00 2026\n12 11 12 01:00.00 Tue Nov 3 12:00:00 2026\n20 1 20 99:00.00 Tue Nov 3 12:00:00 2026\n")
	n, members, err := livenessTreeCPU(data, 10)
	if err != nil || n != 63.5 || strings.Contains(members, "20:") || !strings.Contains(members, "12:") {
		t.Fatal(n, members, err)
	}
	if _, _, err := livenessTreeCPU(data, 99); err == nil {
		t.Fatal("missing root treated as alive")
	}
	n, err = parseLivenessCPUTime("2-03:04:05.25")
	if err != nil || n != 183845.25 {
		t.Fatal(n, err)
	}
}

func TestAttemptLivenessSamplerDoesNotBlockHeartbeat(t *testing.T) {
	previous := workerLivenessCPU
	defer func() { workerLivenessCPU = previous }()
	entered := make(chan struct{})
	workerLivenessCPU = func(ctx context.Context, root int) (float64, string, error) {
		if root != 42 {
			t.Errorf("wrong root %d", root)
		}
		close(entered)
		<-ctx.Done()
		return 0, "", ctx.Err()
	}
	clock := &fakeClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	p := &process{cmd: &exec.Cmd{Process: &os.Process{Pid: 42}}, job: Job{ID: "job", Attempt: 1}}
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

func TestAttemptLivenessCgroupCPU(t *testing.T) {
	dir := t.TempDir()
	memory := &memoryAttempt{fs: osMemoryFS{}, path: dir}
	for path, body := range map[string]string{"cpu.stat": "usage_usec 1250000\nuser_usec 1000000\n", "cgroup.events": "populated 1\nfrozen 0\n"} {
		if err := os.WriteFile(filepath.Join(dir, path), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	n, err := livenessCgroupCPU(memory)
	if err != nil || n != 1.25 {
		t.Fatal(n, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.events"), []byte("populated 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := livenessCgroupCPU(memory); err == nil {
		t.Fatal("empty cgroup reported alive")
	}
}
