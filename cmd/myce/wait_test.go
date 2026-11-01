package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/internal/myce"
)

type waitClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *waitClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *waitClock) advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }

func waitQueue(t *testing.T) (*myce.Queue, *waitClock, string, string) {
	t.Helper()
	clock := &waitClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
	db, checkpoints := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	return waitOpenQueue(t, db, checkpoints, clock), clock, db, checkpoints
}

func waitOpenQueue(t *testing.T, db, checkpoints string, clock *waitClock) *myce.Queue {
	t.Helper()
	// These fixtures exercise CLI protocols, not storage. Storage tests keep
	// the real availability default by calling myce.Open directly.
	q, err := myce.Open(db, checkpoints, clock, myce.WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

// Read the status API without running another mutation/probe that could hide
// the availability observed by the failed check-in.
func waitAssignmentDiagnostics(q *myce.Queue) string {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/api/status", nil)
	request.Header.Set("Authorization", "Bearer diagnostic-fixture")
	myce.API{Queue: q, Token: "diagnostic-fixture"}.Handler().ServeHTTP(recorder, request)
	var status myce.Snapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil || recorder.Code != http.StatusOK {
		return fmt.Sprintf("status_code=%d status_error=%v", recorder.Code, err)
	}
	var waiting, workers []string
	for _, j := range status.Jobs {
		waiting = append(waiting, fmt.Sprintf("id=%s state=%s reason=%q", j.ID, j.State, j.Reason))
	}
	for _, m := range status.Machines {
		workers = append(workers, fmt.Sprintf("id=%s admission=%q checkpoint_unavailable=%t", m.ID, m.AdmissionReason, m.CheckpointUnavailable))
	}
	return fmt.Sprintf("waiting=%v nas_status=%q workers=%v", waiting, status.Warning, workers)
}

func TestWaitQueueIgnoresCheckpointStorage(t *testing.T) {
	q, _, _, checkpoints := waitQueue(t)
	// CLI outcome fixtures do not prepare checkpoints. A missing real root
	// must not turn their synthetic assignment into a host I/O timing check.
	if err := os.Rename(checkpoints, checkpoints+".offline"); err != nil {
		t.Fatal(err)
	}
	waitSubmit(t, q)
	waitFinish(t, q, true)
}
func waitSubmit(t *testing.T, q *myce.Queue) *myce.Job {
	t.Helper()
	s := myce.Spec{Name: "wait-test", Project: "test", Priority: "experiment"}
	s.Run.Command = []string{"echo", "done"}
	s.Needs.MemoryGB = 1
	s.Needs.Platform.Any = true
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// A fake worker uses the real queue assignment/result protocol, without processes/GPU.
func waitFinish(t *testing.T, q *myce.Queue, success bool) {
	t.Helper()
	defer func() {
		if t.Failed() {
			t.Log(waitAssignmentDiagnostics(q))
		}
	}()
	// Observe assignment before completing it, just as a real worker does.
	// Keep the scheduling clock fixed while waiting.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var reply myce.Reply
	wait(t, ctx, func() bool {
		var err error
		reply, err = q.Checkin(myce.Checkin{Machine: myce.Machine{Capabilities: []string{"native_isolation_landlock"}, CapabilityProfile: &myce.CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", LogicalCPUs: 64, MemoryLayout: "separate", SystemMemoryBytes: 64 << 30, Accelerators: []myce.AcceleratorProfile{{Type: "fake", MemoryBytes: 64 << 30}}}, ID: "fake", Session: "session", GPU: "fake", FreeMemoryGB: 8}})
		if err != nil {
			t.Fatal(err)
		}
		return reply.Job != nil
	})
	if reply.Job == nil {
		t.Fatal("no assignment")
	}
	j := reply.Job
	err := q.Complete(myce.Result{Machine: j.Machine, Session: j.Session, Execution: myce.Execution{ID: j.ID, Attempt: j.Attempt}, Success: success, Error: "bad\ncommand"})
	if err != nil {
		t.Fatal(err)
	}
}
func TestWaitQueueOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		states []string
		code   int
	}{
		{"success", []string{"succeeded"}, 0}, {"failure", []string{"failed"}, 1}, {"cancel", []string{"cancelled"}, 1}, {"mixed", []string{"failed", "succeeded", "cancelled"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, _, _ := waitQueue(t)
			var ids []string
			for range tc.states {
				ids = append(ids, waitSubmit(t, q).ID)
				clock.advance(time.Millisecond)
			}
			server := httptest.NewServer(myce.API{Queue: q, Token: "test-only"}.Handler())
			defer server.Close()
			var out, diagnostics bytes.Buffer
			step := 0
			err := waitJobs(context.Background(), myce.Client{URL: server.URL, Token: "test-only"}, append(ids, ids[0]), 0, &out, &diagnostics, clock, func(_ context.Context, d time.Duration) error {
				if d != 3*time.Second || step >= len(ids) {
					t.Fatalf("unexpected pause %v, step %d", d, step)
				}
				clock.advance(d)
				if tc.states[step] == "cancelled" {
					if e := q.Cancel(ids[step]); e != nil {
						t.Fatal(e)
					}
				} else {
					waitFinish(t, q, tc.states[step] == "succeeded")
				}
				step++
				return nil
			})
			if exitCode(err) != tc.code || step != len(ids) {
				t.Fatalf("code=%d err=%v step=%d", exitCode(err), err, step)
			}
			if len(strings.Split(strings.TrimSpace(out.String()), "\n")) != len(ids) {
				t.Fatal(out.String())
			}
			for i, id := range ids {
				if strings.Count(out.String(), id+" "+tc.states[i]) != 1 {
					t.Fatal(out.String())
				}
			}
			if tc.name == "failure" && (!strings.Contains(out.String(), "execution failed") || strings.Contains(out.String(), "bad command")) {
				t.Fatal(out.String())
			}
		})
	}
}

func TestWaitImmediatelyAfterSubmitWithStaleStatus(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	api := myce.API{Queue: q, Token: "test-only"}.Handler()
	var aggregateReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			aggregateReads.Add(1)
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	c := myce.Client{URL: server.URL, Token: "test-only"}
	// Freeze the fake clock with an empty aggregate cache generation, then
	// commit a submission through the same authenticated API used in production.
	if _, err := status(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	s := myce.Spec{Name: "immediate-wait", Project: "test", Priority: "experiment", Run: myce.RunSpec{Command: []string{"true"}}, Needs: myce.ResourceNeeds{MemoryGB: 1, Platform: myce.Platform{Any: true}}}
	j, err := c.Submit(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	pauses := 0
	err = waitJobs(context.Background(), c, []string{j.ID[:8], j.ID}, 0, &out, &bytes.Buffer{}, clock, func(_ context.Context, _ time.Duration) error {
		pauses++
		if pauses > 1 {
			t.Fatal("wait did not observe committed cancellation")
		}
		return q.Cancel(j.ID)
	})
	if exitCode(err) != 1 || out.String() != j.ID+" cancelled cancel requested\n" || pauses != 1 {
		t.Fatalf("immediate wait: err=%v output=%q pauses=%d", err, out.String(), pauses)
	}
	if aggregateReads.Load() != 1 {
		t.Fatal("wait read aggregate status", aggregateReads.Load())
	}
}
func TestWaitTimeoutDoesNotCancel(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	j := waitSubmit(t, q)
	server := httptest.NewServer(myce.API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	var delays []time.Duration
	err := waitJobs(context.Background(), myce.Client{URL: server.URL, Token: "test-only"}, []string{j.ID}, 5*time.Second, &bytes.Buffer{}, &bytes.Buffer{}, clock, func(_ context.Context, d time.Duration) error {
		delays = append(delays, d)
		clock.advance(d)
		return nil
	})
	if exitCode(err) != 3 || fmt.Sprint(delays) != "[3s 2s]" {
		t.Fatalf("%v %v", err, delays)
	}
	s, e := q.Snapshot()
	if e != nil || s.Jobs[0].State != "queued" {
		t.Fatalf("%+v %v", s, e)
	}
}
func TestWaitQueueRestartAndNetworkRecovery(t *testing.T) {
	q, clock, db, checkpoints := waitQueue(t)
	j := waitSubmit(t, q)
	server := httptest.NewServer(myce.API{Queue: q, Token: "test-only"}.Handler())
	address := server.Listener.Addr().String()
	queueURL := server.URL
	step := 0
	var out, diagnostics bytes.Buffer
	err := waitJobs(context.Background(), myce.Client{URL: queueURL, Token: "test-only"}, []string{j.ID}, time.Minute, &out, &diagnostics, clock, func(_ context.Context, d time.Duration) error {
		clock.advance(d)
		step++
		switch step {
		case 1:
			server.Close()
			if e := q.Close(); e != nil {
				t.Fatal(e)
			}
		case 2:
			if d != 6*time.Second {
				t.Fatal(d)
			} // connection refused; leave offline another cycle
		case 3:
			if d != 10*time.Second {
				t.Fatal(d)
			}
			q = waitOpenQueue(t, db, checkpoints, clock)
			// Completion precedes restored HTTP availability: the original
			// waiter must discover it on its first successful retry, without
			// needing a fresh process or another job state transition (#278).
			waitFinish(t, q, true)
			listener, e := net.Listen("tcp", address)
			if e != nil {
				t.Fatal(e)
			}
			server = httptest.NewUnstartedServer(myce.API{Queue: q, Token: "test-only"}.Handler())
			server.Listener.Close()
			server.Listener = listener
			server.Start()
			t.Cleanup(server.Close)
		default:
			t.Fatal("unexpected retry")
		}
		return nil
	})
	if err != nil || step != 3 || out.String() != j.ID+" succeeded\n" || !strings.Contains(diagnostics.String(), "restored") {
		t.Fatalf("%v %d %s %s", err, step, &out, &diagnostics)
	}
}
func TestWaitHelpUsageAuthAndEnvironment(t *testing.T) {
	q, _, _, _ := waitQueue(t)
	j := waitSubmit(t, q)
	waitFinish(t, q, true)
	server := httptest.NewServer(myce.API{Queue: q, Token: "private-test-token"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_QUEUE", server.URL)
	t.Setenv("MYCE_TOKEN", "private-test-token")
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"--help"}, 0}, {nil, 2}, {[]string{j.ID, "--timeout", "0"}, 2}, {[]string{j.ID, "--timeout", "bad"}, 2}, {[]string{j.ID, "--unknown"}, 2}, {[]string{j.ID, "--queue", "invalid"}, 2}, {[]string{j.ID, "--token", "wrong"}, 2}, {[]string{"missing"}, 2}, {[]string{j.ID, "--timeout", "1m"}, 0}, {[]string{"--timeout=1m", j.ID, j.ID}, 0},
	} {
		var out, diag bytes.Buffer
		err := waitCommand(context.Background(), tc.args, &out, &diag)
		if exitCode(err) != tc.code {
			t.Fatalf("%v: %v", tc.args, err)
		}
		if strings.Contains(diag.String(), "private-test-token") {
			t.Fatal("help exposed token")
		}
		if len(tc.args) > 0 && tc.args[0] == "--help" && !strings.Contains(diag.String(), "3 timeout") {
			t.Fatal(diag.String())
		}
	}
	t.Setenv("MYCE_TOKEN", "")
	if exitCode(waitCommand(context.Background(), []string{j.ID}, &bytes.Buffer{}, &bytes.Buffer{})) != 2 {
		t.Fatal("missing token")
	}
}
func TestWaitTransientHTTPAndInterrupted(t *testing.T) {
	for _, code := range []int{408, 429, 503} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			clock := &waitClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(code)
					return
				}
				fmt.Fprint(w, `{"id":"job","state":"succeeded"}`)
			}))
			defer server.Close()
			err := waitJobs(context.Background(), myce.Client{URL: server.URL}, []string{"job"}, 0, &bytes.Buffer{}, &bytes.Buffer{}, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
			if err != nil || calls.Load() != 2 {
				t.Fatalf("%v %d", err, calls.Load())
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if exitCode(waitJobs(ctx, myce.Client{}, []string{"job"}, 0, &bytes.Buffer{}, &bytes.Buffer{}, myce.RealClock{}, waitPause)) != 4 {
		t.Fatal("interrupt code")
	}
}

// A request timeout is an outage, not the end of the user's overall wait.
type waitTransport func(*http.Request) (*http.Response, error)

func (f waitTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWaitOutageDiagnosticsAndTerminalRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, cause string
		err         error
	}{
		{"timeout", "timeout", context.DeadlineExceeded},
		{"truncated", "response_incomplete", io.ErrUnexpectedEOF},
		{"transport", "read_error", errors.New("private transport details")},
		{"http", "http_503", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &waitClock{now: time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)}
			started := clock.Now()
			calls := 0
			client := myce.Client{URL: "http://fixture.invalid/private-endpoint", Token: "private-token", HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				status, body := http.StatusOK, `{"id":"job","state":"running"}`
				if calls > 1 && clock.Now().Sub(started) < 75*time.Second {
					if tc.err != nil {
						return nil, tc.err
					}
					status, body = http.StatusServiceUnavailable, `{"error":"private response details"}`
				} else if calls > 1 {
					body = `{"id":"job","state":"succeeded"}`
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			var out, diagnostics bytes.Buffer
			err := waitJobs(context.Background(), client, []string{"job"}, 0, &out, &diagnostics, clock, func(_ context.Context, d time.Duration) error {
				clock.advance(d)
				if clock.Now().Sub(started) > 90*time.Second {
					t.Fatal("wait failed to observe terminal job after outage")
				}
				return nil
			})
			if err != nil || out.String() != "job succeeded\n" {
				t.Fatalf("wait err=%v output=%q", err, out.String())
			}
			lines := strings.Split(strings.TrimSpace(diagnostics.String()), "\n")
			if len(lines) != 3 || !strings.Contains(lines[0], "cause="+tc.cause+" attempts=1 elapsed=0s") || !strings.Contains(lines[1], "cause="+tc.cause+" attempts=8 elapsed=1m6s") || lines[2] != "wait: queue connection restored" {
				t.Fatalf("missing sanitized retry progress or recovery: %q", diagnostics.String())
			}
			if strings.Contains(diagnostics.String(), "private") || strings.Contains(diagnostics.String(), "fixture.invalid") {
				t.Fatal("retry diagnostics exposed private request or response data")
			}
		})
	}
}

func TestWaitRequestTimeoutRetriesUntilOverallDeadline(t *testing.T) {
	clock := &waitClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
	calls := 0
	client := myce.Client{URL: "http://test", HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return nil, context.DeadlineExceeded
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"job","state":"succeeded"}`))}, nil
	})}}
	err := waitJobs(context.Background(), client, []string{"job"}, time.Minute, &bytes.Buffer{}, &bytes.Buffer{}, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
	if err != nil || calls != 2 {
		t.Fatalf("%v calls=%d", err, calls)
	}
}
func TestWaitTimeoutWhileQueueOffline(t *testing.T) {
	clock := &waitClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
	calls := 0
	client := myce.Client{URL: "http://test", HTTP: &http.Client{Transport: waitTransport(func(*http.Request) (*http.Response, error) { calls++; return nil, io.EOF })}}
	var delays []time.Duration
	err := waitJobs(context.Background(), client, []string{"job"}, 15*time.Second, &bytes.Buffer{}, &bytes.Buffer{}, clock, func(_ context.Context, d time.Duration) error {
		delays = append(delays, d)
		clock.advance(d)
		return nil
	})
	if exitCode(err) != 3 || calls != 2 || fmt.Sprint(delays) != "[6s 9s]" {
		t.Fatalf("%v calls=%d delays=%v", err, calls, delays)
	}
}
func TestWaitDeadlineCancelsInFlightRequest(t *testing.T) {
	client := myce.Client{URL: "http://test", HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })}}
	err := waitJobs(context.Background(), client, []string{"job"}, time.Millisecond, &bytes.Buffer{}, &bytes.Buffer{}, myce.RealClock{}, waitPause)
	if exitCode(err) != 3 {
		t.Fatal(err)
	}
}

func TestWaitCancellationWaitsForWorkerAcknowledgement(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	j := waitSubmit(t, q)
	reply, err := q.Checkin(myce.Checkin{Machine: myce.Machine{Capabilities: []string{"native_isolation_landlock"}, CapabilityProfile: &myce.CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", LogicalCPUs: 64, MemoryLayout: "separate", SystemMemoryBytes: 64 << 30, Accelerators: []myce.AcceleratorProfile{{Type: "fake", MemoryBytes: 64 << 30}}}, ID: "fake", Session: "session", GPU: "fake", FreeMemoryGB: 8}})
	if err != nil || reply.Job == nil {
		t.Fatalf("assignment: %v %+v %s", err, reply, waitAssignmentDiagnostics(q))
	}
	result := myce.Result{Machine: reply.Job.Machine, Session: reply.Job.Session, Execution: myce.Execution{ID: j.ID, Attempt: reply.Job.Attempt}}
	if err = q.Started(result); err != nil {
		t.Fatal(err)
	}
	if err = q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	var out bytes.Buffer
	pauses := 0
	err = waitJobs(context.Background(), myce.Client{URL: server.URL, Token: "test-only"}, []string{j.ID}, 0, &out, &bytes.Buffer{}, clock, func(_ context.Context, d time.Duration) error {
		pauses++
		if pauses > 1 {
			t.Fatal("unexpected pause")
		}
		if out.Len() != 0 {
			t.Fatal("printed before cancellation acknowledgement")
		}
		clock.advance(d)
		return q.Complete(result)
	})
	if exitCode(err) != 1 || pauses != 1 || !strings.Contains(out.String(), j.ID+" cancelled") {
		t.Fatalf("%v pauses=%d output=%s", err, pauses, &out)
	}
}

func TestWaitProductionStartDeadline(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	s := myce.Spec{Name: "production", Project: "test", Priority: "production"}
	s.Run.Command = []string{"echo", "done"}
	s.Needs.MemoryGB = 1
	s.Needs.Platform = myce.Platform{Required: []string{"xpu"}}
	deadline := clock.Now().Add(3 * time.Second)
	s.StartBy = &deadline
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	var out, diagnostics bytes.Buffer
	err = waitJobs(context.Background(), myce.Client{URL: server.URL, Token: "test-only"}, []string{j.ID}, 0, &out, &diagnostics, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return q.Tick() })
	if exitCode(err) != 1 || !strings.Contains(out.String(), "production start deadline exceeded") || strings.Contains(out.String(), "needs xpu") {
		t.Fatalf("exit=%d output=%s err=%v", exitCode(err), out.String(), err)
	}
}

func TestWaitDeadlineGuidance(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	server := httptest.NewServer(myce.API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	client := myce.Client{URL: server.URL, Token: "test-only"}
	s := myce.Spec{Name: "deadline-advice", Project: "synthetic", Class: "experiment"}
	s.Run.Command = []string{"echo", "done"}
	s.Needs.CPUCores = 1
	s.Needs.MemoryGB = 1
	j, err := client.Submit(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	var out, diagnostics bytes.Buffer
	err = waitJobs(context.Background(), client, []string{j.ID}, 7*time.Second, &out, &diagnostics, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
	if exitCode(err) != 3 || out.Len() != 0 {
		t.Fatal(err, out.String())
	}
	text := diagnostics.String()
	for _, want := range []string{"tier experiment selector */* revision 0", "tier deadline selector */deadline revision 0", "higher tier precedence", "Blockers:", "ETA unknown", "class: deadline; deadline:", "does not preempt non-resumable"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q: %s", want, text)
		}
	}
	if strings.Count(text, "priority advisory:") != 1 {
		t.Fatal("unchanged guidance repeated", text)
	}
}
