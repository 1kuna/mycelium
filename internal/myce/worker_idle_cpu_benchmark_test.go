//go:build !race

package myce

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Profile a separate process so queue CPU and workers' job subprocesses do
// not inflate the worker's result. Hardware probes use the usual fakes.
func BenchmarkWorkerIdleTraffic(b *testing.B) {
	dir := b.TempDir()
	q, err := Open(filepath.Join(dir, "queue.db"), dir, RealClock{})
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	q.admissionWaitBuild.synchronous = false
	q.checkpointProbe = func() bool { return true }
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	ids := make([]string, 0, 2)
	for i := range 2 {
		spec := spec("experiment")
		spec.Needs.MemoryGB = 2
		spec.Name = fmt.Sprintf("idle-work-%d", i)
		spec.Run.Command = []string{"sleep", "120"}
		ids = append(ids, submit(b, q, spec).ID)
	}
	binary, err := os.Executable()
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-test.run", "^$", "-test.bench", "^BenchmarkWorkerIdleProcess$", "-test.benchtime=1x")
	cmd.Env = append(os.Environ(), "MYCE_BENCH_QUEUE="+server.URL, "MYCE_BENCH_JOBS="+strings.Join(ids, ","))
	output, err := cmd.CombinedOutput()
	if err != nil {
		b.Fatalf("worker process: %v\n%s", err, output)
	}
	b.Logf("worker process:\n%s", output)
}

func BenchmarkWorkerIdleProcess(b *testing.B) {
	address := os.Getenv("MYCE_BENCH_QUEUE")
	if address == "" {
		b.Skip("run BenchmarkWorkerIdleTraffic to provision the local queue")
	}
	ids := strings.Split(os.Getenv("MYCE_BENCH_JOBS"), ",")
	extensions := b.TempDir()
	template := fakeExtension(b)
	for i := range 24 {
		fixture := make(map[string][]byte, len(template.Files)+32)
		for path, data := range template.Files {
			fixture[path] = data
		}
		title := fmt.Sprintf("name: sample-%d", i)
		fixture["extension.yaml"] = bytes.Replace(fixture["extension.yaml"], []byte("name: sample"), []byte(title), 1)
		for asset := range 32 {
			fixture[fmt.Sprintf("assets/%02d.dat", asset)] = bytes.Repeat([]byte{byte(asset)}, 8192)
		}
		e, err := extensionFromFiles(fixture)
		if err != nil {
			b.Fatal(err)
		}
		if err := InstallExtension(extensions, e); err != nil {
			b.Fatal(err)
		}
	}
	client := Client{URL: address, Token: "test"}
	m := machine("worker", "fake", "fixture-worker-host")
	m.MaxJobs = 2
	for i := range 2 {
		id := fmt.Sprint(i)
		m.Devices = append(m.Devices, MachineDevice{ID: id, Platform: "fake", Identity: "fake-" + id, Selector: id,
			Fingerprint: "fake", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8})
	}
	w := Worker{EnvironmentLimit: 1 << 20, StateDir: b.TempDir(), ExtensionRoot: extensions, Client: client, Machine: m, Interval: time.Second, CPUIsolationProbe: func(context.Context) error { return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	stopWorker := func() {
		cleanup, stop := context.WithTimeout(context.Background(), 8*time.Second)
		defer stop()
		for _, id := range ids {
			_, _ = client.Cancel(cleanup, id)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				b.Error(err)
			}
		case <-cleanup.Done():
			b.Error("worker did not shut down")
		}
	}
	defer stopWorker()
	ready, finishReady := context.WithTimeout(context.Background(), 15*time.Second)
	defer finishReady()
	for _, id := range ids {
		for ready.Err() == nil {
			reply, err := client.WaitBatch(ready, []string{id}, nil, 0)
			if err != nil {
				b.Fatal(err)
			}
			job := reply.Jobs[0]
			if job.State == "running" && job.StartedAt != nil {
				break
			}
			if terminal(job.State) {
				b.Fatalf("fixture attempt ended early: %s", job.State)
			}
			time.Sleep(100 * time.Millisecond)
		}
		if ready.Err() != nil {
			job, err := client.GetJob(context.Background(), id)
			b.Fatalf("fixture never started: state=%s hold=%v error=%v", job.State, job.Hold, err)
		}
	}
	stopProfile := idleCPUProfile(b)
	defer stopProfile()
	b.ResetTimer()
	cpu := benchmarkCPU()
	start := time.Now()
	time.Sleep(time.Duration(b.N) * 30 * time.Second)
	elapsed := time.Since(start)
	used := benchmarkCPU() - cpu
	b.StopTimer()
	stopProfile()
	b.ReportMetric(float64(used)*float64(time.Minute)/float64(elapsed)/1e9, "CPU-s/min")
}
