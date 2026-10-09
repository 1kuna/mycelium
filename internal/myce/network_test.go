package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type manualWorkerClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualWorkerClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *manualWorkerClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type partitionTransport struct{ down atomic.Bool }

func (p *partitionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if p.down.Load() {
		return nil, errors.New("pretend network partition")
	}
	return http.DefaultTransport.RoundTrip(r)
}

func TestWorkerNetworkPartitionFakeClock(t *testing.T) {
	q, qc := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	gate := filepath.Join(t.TempDir(), "finish")
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `echo ready; while [ ! -f "$FINISH" ]; do sleep 0.01; done; echo offline-done`}
	s.Run.Env = map[string]string{"FINISH": gate}
	first := submit(t, q, s)
	transport := &partitionTransport{}
	wc := &manualWorkerClock{now: qc.Now()}
	dir := t.TempDir()
	w := Worker{StateDir: dir, Clock: wc, Interval: 5 * time.Millisecond, Machine: machine("partition-worker", "fake"), Client: Client{URL: server.URL, Token: "test", HTTP: &http.Client{Transport: transport}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	await(t, func() bool {
		wc.Advance(time.Second)
		logs, _ := q.Logs(first.ID, 0)
		for _, l := range logs {
			if strings.Contains(l.Text, "ready") {
				return true
			}
		}
		return false
	})
	transport.down.Store(true)
	second := submit(t, q, spec("experiment"))
	qc.Advance(10 * time.Minute)
	requireState(t, q, first.ID, "running")
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		wc.Advance(time.Minute)
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".result.json") {
				return true
			}
		}
		return false
	})
	requireState(t, q, second.ID, "queued") // No new start during disconnection, even after the command finishes.
	transport.down.Store(false)
	await(t, func() bool { wc.Advance(time.Minute); return job(t, q, first.ID).State == "succeeded" })
	logs, _ := q.Logs(first.ID, 0)
	var output string
	for _, l := range logs {
		output += l.Text
	}
	if !strings.Contains(output, "offline-done") {
		t.Fatalf("lost offline output %q", output)
	}
	cancel()
	await(t, func() bool {
		wc.Advance(time.Minute)
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return true
		default:
			return false
		}
	})
}

func TestNASLossDoesNotStopRunningJob(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("a", "fake")
	running := check(t, q, m, nil).Job
	root := q.root
	away := root + "-offline"
	if err := os.Rename(root, away); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(away, root)
	submit(t, q, spec("production"))
	c.Advance(time.Second)
	if reply := check(t, q, m, running); reply.Stop {
		t.Fatal("NAS loss preempted running command")
	}
	requireState(t, q, running.ID, "running")
}
