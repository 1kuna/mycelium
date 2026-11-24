package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

type waitIndexResult struct {
	reply WaitReply
	err   error
}

func awaitWaitIndexBlocked(t *testing.T, ctx context.Context, index *waitStateIndex, request WaitRequest) <-chan waitIndexResult {
	t.Helper()
	done := make(chan waitIndexResult, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		reply, err := index.wait(ctx, request)
		done <- waitIndexResult{reply, err}
	}()
	t.Cleanup(func() {
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("waiter did not join after context cancellation")
		}
	})
	// The index notification, context and transport timer belong to this
	// synctest bubble. No socket or queue transaction is in the wait path.
	// Wait returns only after the waiter has durably blocked, not while its
	// select operands are still being evaluated.
	synctest.Wait()
	select {
	case result := <-done:
		t.Fatalf("unchanged revision returned before commit/cancel: %+v", result)
	default:
	}
	return done
}

func TestWaitIndexCancellation(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	// Create the notification channel inside the bubble; queue persistence
	// remains real, but the waiter only reads the in-memory committed index.
	synctest.Test(t, func(t *testing.T) {
		index := q.waitIndex()
		initial, _ := index.wait(context.Background(), WaitRequest{IDs: []string{j.ID}})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := awaitWaitIndexBlocked(t, ctx, index, WaitRequest{IDs: []string{j.ID}, After: map[string]uint64{j.ID: initial.Jobs[0].Revision}, Seconds: 20})
		cancel()
		select {
		case result := <-done:
			if !errors.Is(result.err, context.Canceled) {
				t.Fatal(result.reply, result.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancellation did not release in-flight waiter")
		}
	})
}

// One wave represents 200 concurrent waiters over 6,500 retained jobs. Idle
// long-polls consume one such wave per 20 seconds, independently of history.
// Real process CPU includes HTTP dispatch/encoding and waiter goroutines.
func BenchmarkWaitIndex200(b *testing.B)     { benchmarkWaitIndex200(b, false) }
func BenchmarkWaitIndex200Idle(b *testing.B) { benchmarkWaitIndex200(b, true) }
func benchmarkWaitIndex200(b *testing.B, idle bool) {
	clock := &fakeClock{now: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock, WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	for i := range 6500 {
		id := fmt.Sprintf("history-%08d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded"}
	}
	for i := range 200 {
		id := fmt.Sprintf("waiting-%08d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "queued"}
	}
	handler := (API{Queue: q, Token: "fixture"}).handler("", clock)
	var before, after syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	b.ResetTimer()
	for range b.N {
		clock.Advance(20 * time.Second)
		var wg sync.WaitGroup
		q.mu.Lock() // Requests cannot depend on or hold this mutex.
		for i := range 200 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("waiting-%08d", i)
				request := WaitRequest{IDs: []string{id}}
				if idle {
					request.Seconds = 1
					request.After = map[string]uint64{id: q.waitStates.jobs[id].Revision}
				}
				body, _ := json.Marshal(request)
				r := httptest.NewRequest("POST", "/api/wait", bytes.NewReader(body))
				r.RemoteAddr = fmt.Sprintf("192.0.2.%d:1234", i)
				r.Header.Set("Authorization", "Bearer fixture")
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				if w.Code != 200 {
					b.Errorf("status %d", w.Code)
				}
			}(i)
		}
		wg.Wait()
		q.mu.Unlock()
	}
	b.StopTimer()
	syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec)*1000 + float64(r.Utime.Usec+r.Stime.Usec)/1000
	}
	b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-ms/200")
	b.ReportMetric(0, "queue-lock-ns/200")
}
