package myce

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
