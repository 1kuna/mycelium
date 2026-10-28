package myce

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Unlike the queue-only fake clock, this clock is safe for concurrent worker I/O.
type workerFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *workerFakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *workerFakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestWorkerStopAndGraceDuringBlockedUpload(t *testing.T) {
	testWorkerBlockedNetworkStop(t, false)
}
func TestWorkerStopAndGraceDuringBlockedCheckin(t *testing.T) {
	testWorkerBlockedNetworkStop(t, true)
}
func testWorkerBlockedNetworkStop(t *testing.T, blockCheckin bool) {
	t.Helper()
	q, _ := fixture(t)
	clock := &workerFakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	q.clock = clock
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var uploaded atomic.Bool
	api := API{Queue: q, Token: "test"}.Handler()
	server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/log" {
			uploaded.Store(true)
			if !blockCheckin {
				once.Do(func() { close(entered); <-release })
			}
		}
		if blockCheckin && uploaded.Load() && r.URL.Path == "/api/workers/checkin" {
			once.Do(func() { close(entered); <-release })
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `mkfifo -m 600 "$WAIT_FIFO" "$SIGNAL_FIFO" || exit 1; exec 3<> "$WAIT_FIFO"; trap 'echo term > "$SIGNAL_FIFO"' TERM; echo ready; while :; do read line <&3; done`}
	s.Run.Env = map[string]string{"WAIT_FIFO": "{work}/wait", "SIGNAL_FIFO": "{work}/signal"}
	submit(t, q, s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	stateDir := t.TempDir()
	w := Worker{StateDir: stateDir, Clock: clock, Interval: time.Millisecond, Machine: machine("one", "fake"), Client: Client{URL: server.URL, Token: "test"}}
	go func() { done <- w.Run(ctx) }()
	defer func() {
		data, _ := readSingleAttemptState(filepath.Join(stateDir, "state.json"))
		var state workerState
		if json.Unmarshal(data, &state) == nil && state.PID > 0 {
			_ = syscall.Kill(-state.PID, syscall.SIGKILL)
		}
		close(release)
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("worker cleanup timed out")
		}
	}()
	bootstrap := time.NewTicker(time.Millisecond)
	defer bootstrap.Stop()
	timeout := time.After(3 * time.Second)
waitingUpload:
	for {
		select {
		case <-entered:
			break waitingUpload
		case <-bootstrap.C:
			clock.Advance(time.Millisecond)
		case <-timeout:
			t.Fatal("no upload")
		}
	}
	var state workerState
	data, err := readSingleAttemptState(filepath.Join(stateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	signal, err := os.OpenFile(filepath.Join(state.Job.Spec.Run.Workdir, "signal"), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer signal.Close()
	terms := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(signal)
		if scanner.Scan() {
			terms <- scanner.Text()
		}
	}()
	submit(t, q, spec("production"))
	select {
	case term := <-terms:
		if term != "term" {
			t.Fatal(term)
		}
	case <-time.After(time.Second):
		t.Fatal("SIGTERM blocked behind log upload")
	}
	// The command keeps running after TERM. The original 10-second deadline
	// must kill it while the same upload remains blocked and the queue is silent.
	clock.Advance(9 * time.Second)
	if _, err := os.Stat(filepath.Join(stateDir, state.Identity+".result.json")); err == nil {
		t.Fatal("killed before grace")
	}
	clock.Advance(time.Second)
	timeout = time.After(time.Second)
	for {
		if _, err := os.Stat(filepath.Join(stateDir, state.Identity+".result.json")); err == nil {
			break
		}
		select {
		case <-bootstrap.C:
		case <-timeout:
			t.Fatal("SIGKILL did not enforce fake-clock deadline during blocked network call")
		}
	}
}

func TestWorkerStopEventPreservesQueueDeadline(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	deadline := clock.Now().Add(10 * time.Second)
	event := WorkerEvent{Cursor: "stop", Execution: Execution{ID: "job", Attempt: 1}, GraceSeconds: 10, StopAt: &deadline}
	server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clock.Advance(9 * time.Second) // delay in transit must not grant another 10s
		_ = json.NewEncoder(w).Encode(event)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan WorkerEvent)
	w := Worker{Clock: clock, Machine: machine("one", "fake"), Client: Client{URL: server.URL}}
	go w.watchStops(ctx, events)
	select {
	case got := <-events:
		if !got.deadline.Equal(deadline) {
			t.Fatalf("deadline extended: %s, want %s", got.deadline, deadline)
		}
	case <-time.After(time.Second):
		t.Fatal("no event")
	}
}
