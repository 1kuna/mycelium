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

type partitionGate struct {
	mu    sync.RWMutex
	value atomic.Bool
}

func (g *partitionGate) Store(down bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value.Store(down)
}

func (g *partitionGate) Load() bool { return g.value.Load() }

type partitionTransport struct{ down partitionGate }

func (p *partitionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Store(true) closes admission and waits for accepted requests to finish
	// their server-side mutations before the harness advances queue time.
	p.down.mu.RLock()
	defer p.down.mu.RUnlock()
	if p.down.Load() {
		return nil, errors.New("pretend network partition")
	}
	return http.DefaultTransport.RoundTrip(r)
}

// Partition admission must also drain requests that already passed the gate.
func TestQCPartitionDrainsInFlight(t *testing.T) {
	t.Parallel()
	q, clock := fixture(t)
	ms := []Machine{machine("a", "fake", "ha"), machine("b", "fake", "hb"), machine("c", "fake", "hc")}
	for _, m := range ms {
		check(t, q, m, nil)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		check(t, q, ms[2], nil)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	transport := &partitionTransport{}
	done := make(chan error, 1)
	go func() {
		r, err := (&http.Client{Transport: transport}).Get(server.URL)
		if r != nil {
			r.Body.Close()
		}
		done <- err
	}()
	<-entered
	partitioned := make(chan struct{})
	go func() { transport.down.Store(true); close(partitioned) }()
	premature := false
	select {
	case <-partitioned:
		premature = true
	case <-time.After(20 * time.Millisecond):
	}
	if premature {
		clock.Advance(10 * time.Minute)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-partitioned
	if !premature {
		clock.Advance(10 * time.Minute)
	}
	s, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if premature || !strings.Contains(s.Warning, "network") {
		t.Fatalf("partition returned before in-flight check-in drained; warning=%q", s.Warning)
	}
}

func TestWorkerNetworkPartitionFakeClock(t *testing.T) {
	t.Parallel()
	q, initial := fixture(t)
	qc := &manualWorkerClock{now: initial.Now()}
	q.clock = qc
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
	t.Parallel()
	q, c := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("a", "fake")
	running := check(t, q, m, nil).Job
	fakeCheckpointAvailability(t, q, false)
	submit(t, q, spec("production"))
	c.Advance(time.Second)
	if reply := check(t, q, m, running); reply.Stop {
		t.Fatal("NAS loss preempted running command")
	}
	requireState(t, q, running.ID, "running")
}
