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

// Safe for concurrent queue observation and worker I/O, including time advances.
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
	j := submit(t, q, s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	stateDir := t.TempDir()
	w := Worker{EnvironmentLimit: 1 << 20, StateDir: stateDir, Clock: clock, Interval: time.Millisecond, Machine: machine("one", "fake"), Client: Client{URL: server.URL, Token: "test"}}
	go func() { done <- w.Run(ctx) }()
	defer func() {
		data, _ := readSingleAttemptState(filepath.Join(stateDir, "state.json"))
		var state workerState
		if json.Unmarshal(data, &state) == nil && state.PID > 0 {
			_ = syscall.Kill(-state.PID, syscall.SIGKILL)
		}
		close(release)
		cancel()
		await(t, func() bool {
			clock.Advance(time.Millisecond)
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
				return true
			default:
				return false
			}
		})
	}()
	// Launch and first upload are asynchronous I/O. Advance bootstrap time only
	// until the held request enters; all stop assertions below freeze time.
	await(t, func() bool {
		clock.Advance(time.Millisecond)
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
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
	await(t, func() bool {
		select {
		case term := <-terms:
			if term != "term" {
				t.Fatal(term)
			}
			return true
		default:
			return false
		}
	})
	// TERM must arrive while this same request remains held. Use the queue's
	// original absolute grace deadline, excluding launch and observation time.
	deadline := job(t, q, j.ID).StopAt
	if deadline == nil || !clock.Now().Before(*deadline) {
		t.Fatal("TERM not observed before the original stop deadline")
	}
	clock.Advance(deadline.Sub(clock.Now()) - time.Second)
	resultPath := filepath.Join(stateDir, state.Identity+".result.json")
	if _, err := os.Stat(resultPath); err == nil {
		t.Fatal("killed before grace")
	}
	clock.Advance(time.Second)
	// Keep fake time fixed at the deadline and network release held until the
	// forced result is durably published. Blocking stop behind upload/check-in
	// can never satisfy this fence, regardless of real scheduling latency.
	await(t, func() bool { _, err := os.Stat(resultPath); return err == nil })
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
	await(t, func() bool {
		select {
		case got := <-events:
			if !got.deadline.Equal(deadline) {
				t.Fatalf("deadline extended: %s, want %s", got.deadline, deadline)
			}
			return true
		default:
			return false
		}
	})
}
