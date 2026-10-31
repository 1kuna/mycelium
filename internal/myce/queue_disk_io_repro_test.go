//go:build !race

package myce

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Opt-in measurement, not an automatic latency acceptance or native I/O-policy
// qualification. Uses real HTTP, SQLite and a scheduler-launched CPU command on
// the same filesystem. Queue decisions retain the dev runtime's injected clock.
func TestQueueDiskIOReproduction(t *testing.T) {
	if os.Getenv("MYCE_TEST_QUEUE_DISK_IO") != "1" {
		t.Skip("opt-in: writes 2 GiB total, at most 256 MiB of payload files")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	root := newTestStateRoot(t)
	c := DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "disk-io-test", GPU: "cpu", Workers: 1, CPUBudgetCores: 4, Interval: 100 * time.Millisecond}
	d, err := StartDev(c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel() // Bounds a broken local experiment, not a fleet job's runtime.
	probeWait(t, ctx, func() bool { s, err := d.Queue.Snapshot(); return err == nil && len(s.Machines) == 1 })
	// Twenty heartbeat opportunities establish an unloaded comparison. This is
	// wall-time observation only, not scheduling or loss-policy time.
	<-time.After(2 * time.Second)
	for _, row := range d.Queue.requestLatencies.snapshot() {
		if row.Route == "POST /api/workers/checkin" {
			t.Logf("baseline check-ins: n=%d p50_ms=%.3f p99_ms=%.3f", row.Samples, row.P50MS, row.P99MS)
		}
	}
	d.Queue.requestLatencies.mu.Lock()
	d.Queue.requestLatencies.routes = nil
	d.Queue.requestLatencies.mu.Unlock()
	// Four finite writers: 64 MiB each, overwritten eight times; fsync every
	// four MiB. Random reusable buffers avoid filesystem zero compression.
	command := `import concurrent.futures, os, pathlib
root = pathlib.Path("disk-io-payload")
root.mkdir()
def write(n):
    block = os.urandom(1024*1024)
    with (root / str(n)).open("wb", buffering=0) as f:
        for turn in range(8):
            f.seek(0)
            for part in range(64):
                view = memoryview(block)
                while view:
                    view = view[f.write(view):]
                if part % 4 == 3:
                    os.fsync(f.fileno())
    return 64*8*len(block)
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    print("payload_bytes_written", sum(pool.map(write, range(4))))
`
	s := Spec{Name: "queue-disk-io-reproduction", Project: "disk-io", Class: "test", Targets: []ExecutionTarget{{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "cpu"}}, Needs: ResourceNeeds{CPUCores: 1, HostMemoryGB: .25, Platform: Platform{Required: []string{"cpu"}}}, Run: RunSpec{Command: []string{python, "-c", command}}}
	j, err := d.Client().Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	finished, err := d.Client().Wait(ctx, j.ID)
	if err != nil {
		t.Error(err)
	}
	if finished != nil && (finished.State != "succeeded" || finished.Losses != 0) {
		t.Errorf("disk job state=%s losses=%d reason=%s", finished.State, finished.Losses, finished.Reason)
	}
	for _, row := range d.Queue.requestLatencies.snapshot() {
		if row.Route == "POST /api/workers/checkin" {
			t.Logf("same-disk 2 GiB write job: elapsed=%s check-ins=%d p50_ms=%.3f p99_ms=%.3f", time.Since(started), row.Samples, row.P50MS, row.P99MS)
		}
	}
}
