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
	"time"
)

func TestWaitIndexCommitRollbackAndUnlockedReads(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("experiment"))
	index := q.waitIndex()
	first, err := index.wait(context.Background(), WaitRequest{IDs: []string{j.ID[:8]}, Seconds: 0})
	if err != nil || first.Jobs[0].State != "queued" {
		t.Fatal(first, err)
	}
	after := map[string]uint64{j.ID: first.Jobs[0].Revision}
	abort := errors.New("rollback")
	if e := q.update(func() error { q.touchJob(j.ID).State = "failed"; return abort }); !errors.Is(e, abort) {
		t.Fatal(e)
	}
	clock.Advance(20 * time.Second)
	snapshot, _, changed, err := index.read(WaitRequest{IDs: []string{j.ID}, After: after})
	if err != nil || changed || snapshot[0].State != "queued" {
		t.Fatal(snapshot, changed, err)
	}
	// All 200 waiters must answer even with the control mutex held. No queue
	// projection or lock acquisition is permitted on this path.
	q.mu.Lock()
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply, e := index.wait(context.Background(), WaitRequest{IDs: []string{j.ID}})
			if e != nil || reply.Jobs[0].State != "queued" {
				t.Error(reply, e)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		q.mu.Unlock()
		t.Fatal("waiters took queue mutex")
	}
	q.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	awakened := make(chan WaitReply, 1)
	go func() {
		r, e := index.wait(ctx, WaitRequest{IDs: []string{j.ID}, After: after, Seconds: 20})
		if e != nil {
			t.Error(e)
		}
		awakened <- r
	}()
	if err = q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-awakened:
		if reply.Jobs[0].State != "cancelled" || reply.Jobs[0].Revision == after[j.ID] {
			t.Fatal(reply)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("committed change did not wake waiter")
	}
	// Failed runtime text uses exactly the public sanitizer, outside both locks.
	if err = q.update(func() error {
		v := q.touchJob(j.ID)
		v.State = "failed"
		v.Reason = "command failed: private fixture data"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reply, err := index.wait(ctx, WaitRequest{IDs: []string{j.ID}})
	if err != nil || reply.Jobs[0].Reason != "execution failed" {
		t.Fatal(reply, err)
	}
}
func TestWaitIndexCancellation(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	index := q.waitIndex()
	initial, _ := index.wait(context.Background(), WaitRequest{IDs: []string{j.ID}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := index.wait(ctx, WaitRequest{IDs: []string{j.ID}, After: map[string]uint64{j.ID: initial.Jobs[0].Revision}, Seconds: 20})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
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
