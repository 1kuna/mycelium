package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitReadRequest(handler http.Handler, id, query string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/api/jobs/"+id+query, nil)
	// Peer identity does not determine admission; only measured queue pressure
	// can shed a nonproduction projection.
	r.RemoteAddr = net.JoinHostPort("fixture-"+id, "1234")
	r.Header.Set("Authorization", "Bearer fixture")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestWaitReadPendingReasonPrivacy(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	if err := q.update(func() error {
		stored := q.touchJob(j.ID)
		stored.State = "stopping"
		stored.Reason = "hypha waiting: synthetic-private-secret /private/fixture"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handler := (API{Queue: q, Token: "fixture"}).Handler()
	var ordinary Job
	full := waitReadRequest(handler, j.ID, "")
	if err := json.Unmarshal(full.Body.Bytes(), &ordinary); err != nil || full.Code != 200 {
		t.Fatal(full.Code, err)
	}
	var pending Job
	w := waitReadRequest(handler, j.ID, "?view=wait")
	if err := json.Unmarshal(w.Body.Bytes(), &pending); err != nil || w.Code != 200 {
		t.Fatal(w.Code, err)
	}
	if pending.Reason != ordinary.Reason || strings.Contains(w.Body.String(), "synthetic-private-secret") || strings.Contains(w.Body.String(), "/private/fixture") {
		t.Fatalf("pending reason bypassed public sanitization: pending=%q ordinary=%q", pending.Reason, ordinary.Reason)
	}
}

// Replay 35 independent waiters across twenty expired diagnostic-cache windows.
// Queue time advances only between concurrent rounds, without wall-clock sleeps.
func TestWaitReadConcurrentPolling(t *testing.T) {
	q, clock := fixture(t)
	var ids []string
	for range 35 {
		ids = append(ids, submit(t, q, spec("experiment")).ID)
	}
	var captures atomic.Int64
	q.observeRead = func(kind string, _, _ time.Duration) {
		if kind == "job" {
			captures.Add(1)
		}
	}
	handler := (API{Queue: q, Token: "fixture"}).handler("", clock)
	for _, query := range []string{"", "?view=wait"} {
		captures.Store(0)
		for range 20 {
			clock.Advance(3 * time.Second)
			var wg sync.WaitGroup
			for _, id := range ids {
				wg.Add(1)
				go func() {
					defer wg.Done()
					w := waitReadRequest(handler, id, query)
					var got Job
					if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != 200 || got.ID != id || got.State != "queued" {
						t.Errorf("pending reply: status=%d id=%s state=%s err=%v", w.Code, got.ID, got.State, err)
					}
				}()
			}
			wg.Wait()
		}
		want := int64(700)
		if query != "" {
			want = 0
		}
		if got := captures.Load(); got != want {
			t.Fatalf("query=%q diagnostic captures=%d want=%d", query, got, want)
		}
		t.Logf("query=%q: 700 requests, %d diagnostic captures", query, captures.Load())
	}
	// A rolled-back terminal transition must never be visible to a waiter.
	abort := errors.New("abort")
	if err := q.update(func() error { q.touchJob(ids[0]).State = "failed"; return abort }); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	w := waitReadRequest(handler, ids[0][:8], "?view=wait")
	var pending map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &pending); err != nil || pending["state"] != "queued" || pending["spec"] != nil {
		t.Fatal("rollback or pending projection", pending, err)
	}
	// No TTL delay after commit; terminal payload and reason match old clients.
	if err := q.Cancel(ids[0]); err != nil {
		t.Fatal(err)
	}
	full := waitReadRequest(handler, ids[0], "")
	fast := waitReadRequest(handler, ids[0][:8], "?view=wait")
	if full.Code != 200 || fast.Code != 200 || full.Body.String() != fast.Body.String() {
		t.Fatal("terminal wait reply differs from ordinary GET")
	}
	for _, query := range []string{"", "?view=wait"} {
		if w := waitReadRequest(handler, "unknown", query); w.Code != 404 {
			t.Fatal("unknown job status", w.Code)
		}
	}
}

// Compare the existing cached GET against the wait view at the CLI's actual
// cadence: each of 35 jobs is read once per three seconds, beyond the 1s TTL.
// Includes HTTP dispatch/encoding; excludes transport and fixture setup.
func BenchmarkWaitRead(b *testing.B) {
	for _, query := range []string{"", "?view=wait"} {
		b.Run(fmt.Sprintf("view=%s", query), func(b *testing.B) {
			clock := &fakeClock{now: time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)}
			q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock, WithCheckpointAvailability(func() bool { return true }))
			if err != nil {
				b.Fatal(err)
			}
			defer q.Close()
			for i := range 6500 {
				id := fmt.Sprintf("history-%04d", i)
				q.s.Jobs[id] = &Job{ID: id, Spec: spec("experiment"), State: "succeeded", FinishedAt: &clock.now, Queued: clock.Now()}
			}
			ids := make([]string, 35)
			for i := range ids {
				ids[i] = fmt.Sprintf("pending-%04d", i)
				q.s.Jobs[ids[i]] = &Job{ID: ids[i], Spec: spec("experiment"), State: "queued", Queued: clock.Now()}
			}
			handler := (API{Queue: q, Token: "fixture"}).handler("", clock)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%len(ids) == 0 {
					clock.Advance(3 * time.Second)
				}
				if w := waitReadRequest(handler, ids[i%len(ids)], query); w.Code != 200 {
					b.Fatal(w.Code)
				}
			}
		})
	}
}
