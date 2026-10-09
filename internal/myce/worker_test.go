package myce

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func workerFixture(t *testing.T, q *Queue) context.CancelFunc {
	t.Helper()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	ctx, cancel := context.WithCancel(context.Background())
	w := Worker{Client: Client{URL: server.URL, Token: "test"}, Machine: machine("worker", "fake"), Interval: 5 * time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		s, _ := q.Snapshot()
		for _, j := range s.Jobs {
			_ = q.Cancel(j.ID)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(4 * time.Second):
			t.Error("worker did not exit")
		}
		server.Close()
	})
	return cancel
}
func await(t *testing.T, f func() bool) {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		if f() {
			return
		}
		select {
		case <-timeout:
			t.Fatal("timed out waiting for worker")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func TestWorkerCancelledBeforeJoining(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := Worker{Client: Client{URL: server.URL, Token: "test"}, Machine: machine("worker", "fake"), Interval: time.Millisecond}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, j.ID, "queued")
	if job(t, q, j.ID).Attempt != 0 {
		t.Fatal("cancelled worker started a command")
	}
}
func TestWorkerGracefulStopAndCheckpoint(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `trap 'echo saved > "$MYCE_CHECKPOINT_DIR/saved"; exit 0' TERM; echo ready; while :; do sleep 0.01; done`}
	j := submit(t, q, s)
	cancel := workerFixture(t, q)
	await(t, func() bool {
		ls, _ := q.Logs(j.ID, 0)
		for _, l := range ls {
			if strings.Contains(l.Text, "ready") {
				return true
			}
		}
		return false
	})
	cancel()
	await(t, func() bool { return job(t, q, j.ID).State == "queued" })
	data, err := os.ReadFile(filepath.Join(j.Checkpoint, "saved"))
	if err != nil || strings.TrimSpace(string(data)) != "saved" {
		t.Fatalf("checkpoint %q: %v", data, err)
	}
}
func TestWorkerWaitsForNonresumableOnInterrupt(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", `echo ready; while [ ! -f "$MYCE_CHECKPOINT_DIR/finish" ]; do sleep 0.01; done; echo finished`}
	j := submit(t, q, s)
	cancel := workerFixture(t, q)
	await(t, func() bool {
		ls, _ := q.Logs(j.ID, 0)
		for _, l := range ls {
			if strings.Contains(l.Text, "ready") {
				return true
			}
		}
		return false
	})
	cancel()
	await(t, func() bool { sn, _ := q.Snapshot(); return len(sn.Machines) == 1 && sn.Machines[0].Leaving })
	requireState(t, q, j.ID, "running")
	if err := os.WriteFile(filepath.Join(j.Checkpoint, "finish"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return job(t, q, j.ID).State == "succeeded" })
}
func TestWorkerCommandStartFailure(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Run.Command = []string{"/does-not-exist/myce-test"}
	j := submit(t, q, s)
	workerFixture(t, q)
	await(t, func() bool { return job(t, q, j.ID).State == "failed" })
	if !strings.Contains(job(t, q, j.ID).Reason, "no such file") {
		t.Fatal("start failure was not explained")
	}
}
