package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
	q, err := myce.Open(db, checkpoints, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	return q, clock, db, checkpoints
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
	// A bounded NAS probe may defer a check-in on a loaded laptop. Observe
	// assignment before completing it, just as a real worker does. Keep the
	// scheduling clock fixed while waiting for filesystem work to finish.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var reply myce.Reply
	wait(t, ctx, func() bool {
		var err error
		reply, err = q.Checkin(myce.Checkin{Machine: myce.Machine{ID: "fake", Session: "session", GPU: "fake", FreeMemoryGB: 8}})
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
			var e error
			q, e = myce.Open(db, checkpoints, clock)
			if e != nil {
				t.Fatal(e)
			}
			deferClose := q
			t.Cleanup(func() { deferClose.Close() })
			listener, e := net.Listen("tcp", address)
			if e != nil {
				t.Fatal(e)
			}
			server = httptest.NewUnstartedServer(myce.API{Queue: q, Token: "test-only"}.Handler())
			server.Listener.Close()
			server.Listener = listener
			server.Start()
			t.Cleanup(server.Close)
		case 4:
			if d != 3*time.Second {
				t.Fatal(d)
			}
			waitFinish(t, q, true)
		default:
			t.Fatal("unexpected retry")
		}
		return nil
	})
	if err != nil || step != 4 || !strings.Contains(out.String(), j.ID+" succeeded") || !strings.Contains(diagnostics.String(), "restored") {
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
				fmt.Fprint(w, `{"jobs":[{"id":"job","state":"succeeded"}]}`)
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
func TestWaitRequestTimeoutRetriesUntilOverallDeadline(t *testing.T) {
	clock := &waitClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
	calls := 0
	client := myce.Client{URL: "http://test", HTTP: &http.Client{Transport: waitTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return nil, context.DeadlineExceeded
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"jobs":[{"id":"job","state":"succeeded"}]}`))}, nil
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
	reply, err := q.Checkin(myce.Checkin{Machine: myce.Machine{ID: "fake", Session: "session", GPU: "fake", FreeMemoryGB: 8}})
	if err != nil || reply.Job == nil {
		t.Fatalf("%v %+v", err, reply)
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
	err = waitJobs(context.Background(), myce.Client{URL: server.URL, Token: "test-only"}, []string{j.ID}, 0, &out, &diagnostics, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
	if exitCode(err) != 1 || !strings.Contains(out.String(), "production start deadline exceeded") || strings.Contains(out.String(), "needs xpu") {
		t.Fatalf("exit=%d output=%s err=%v", exitCode(err), out.String(), err)
	}
}
