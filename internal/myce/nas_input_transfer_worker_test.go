package myce

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestNASInputProductionRecoveryRetainsAttempt(t *testing.T) {
	q, clock := fixture(t)
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "input", "small verified input")
	s := spec("production")
	s.Resumable = false
	s.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1}
	s.SharedInputs = map[string]SharedInput{"data": {NAS: "input", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}
	s.Run.Command = []string{"sh", "-ec", `test "$(cat "$MYCE_INPUT_DATA")" = 'small verified input'; printf once > launched`}
	j := submit(t, q, s)
	m := cpuMachine("nas-input-worker", "", "cpu", 8, 64)
	m.Capabilities = append(m.Capabilities, "shared_inputs_v1")
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("input not assigned", job(t, q, j.ID))
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	client := Client{URL: server.URL, Token: "test", Clock: clock, Jitter: func() float64 { return 0 }, Pause: func(ctx context.Context, _ time.Duration) error {
		close(entered)
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	w := Worker{StateDir: newTestStateRoot(t), Clock: clock, Machine: m, Client: client, CheckpointRoot: nas, FileCacheRoot: t.TempDir(), FileCacheLimit: 1 << 20, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }, MemorySource: func(context.Context) (float64, error) { return 64, nil }}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session, store.state.Job = assigned.Session, assigned
	store.extensionFiles.timeout = testWaitTimeout
	if err := w.initFileTransfers(ctx, store); err != nil {
		t.Fatal(err)
	}
	reads, transferred := 0, 0
	w.sharedInputs.readNAS = func(f *os.File, p []byte) (int, error) {
		reads++
		if reads == 1 {
			n, _ := f.Read(p[:6])
			transferred += n
			return n, syscall.ECONNRESET
		}
		n, err := f.Read(p)
		transferred += n
		return n, err
	}
	type startResult struct {
		p   *process
		err error
	}
	done := make(chan startResult, 1)
	go func() { p, err := w.startContext(ctx, store, *assigned); done <- startResult{p, err} }()
	select {
	case <-entered:
	case got := <-done:
		t.Fatalf("ended before recovery: %v", got.err)
	case <-time.After(testWaitTimeout):
		t.Fatal("did not enter recovery")
	}
	work := filepath.Join(store.dir, "job-files", j.ID, fmt.Sprint(assigned.Attempt))
	got := job(t, q, j.ID)
	if got.StartedAt != nil || got.Attempt != assigned.Attempt || got.Losses != 0 || got.InfrastructureRetries != 0 || !store.inputTransferRecovering.Load() {
		t.Fatalf("recovery spent attempt or launched: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(work, "launched")); !os.IsNotExist(err) {
		t.Fatal("payload ran before verified stage", err)
	}
	unblock()
	var started startResult
	select {
	case started = <-done:
	case <-time.After(testWaitTimeout):
		t.Fatal("recovery did not settle")
	}
	if started.err != nil || started.p == nil {
		t.Fatal("verified stage did not launch", started.err)
	}
	select {
	case finished := <-started.p.done:
		if finished.err != nil || !finished.result.Success {
			t.Fatalf("payload failed: %+v", finished)
		}
		if err := q.Complete(finished.result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWaitTimeout):
		started.p.stop(clock.Now(), 0, true)
		t.Fatal("payload did not exit")
	}
	got = job(t, q, j.ID)
	if got.State != "succeeded" || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Attempt != assigned.Attempt || int64(transferred) != ref.SizeBytes {
		t.Fatalf("resume changed accounting or reread source: bytes=%d job=%+v", transferred, got)
	}
	if data, err := os.ReadFile(filepath.Join(work, "launched")); err != nil || string(data) != "once" {
		t.Fatal("payload not processed exactly once", err)
	}
}
