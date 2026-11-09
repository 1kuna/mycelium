package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	t.Run("contract", func(t *testing.T) {
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

	})

	t.Run("WaitIndexScalarDeadlineMetadata", func(t *testing.T) {
		now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
		deadline, startBy := now.Add(time.Hour), now.Add(time.Minute)
		j := &Job{ID: "job", State: "running", StartedAt: &now, Spec: Spec{
			Deadline: &deadline, StartBy: &startBy, ExpectedMinutes: 10,
			Project: "private-project", Run: RunSpec{Command: []string{"private-command"}},
		}}
		q, _ := fixture(t)
		index := q.waitIndex()
		index.set(j.ID, j)
		rows, _, _, err := index.read(WaitRequest{IDs: []string{j.ID}})
		if err != nil {
			t.Fatal(err)
		}
		row := rows[0]
		projected := row.Job()
		if projected.Spec.Deadline == nil || !projected.Spec.Deadline.Equal(deadline) ||
			projected.Spec.StartBy == nil || !projected.Spec.StartBy.Equal(startBy) ||
			projected.Spec.ExpectedMinutes != 10 || projected.StartedAt == nil || !projected.StartedAt.Equal(now) {
			t.Fatal("missing observation metadata", projected)
		}
		wire, err := json.Marshal(row)
		if err != nil || bytes.Contains(wire, []byte("private")) {
			t.Fatal("execution data escaped", string(wire), err)
		}
		deadline = deadline.Add(time.Minute)
		if row.Spec.Deadline.Equal(deadline) {
			t.Fatal("snapshot aliased mutable job metadata")
		}
		index.set(j.ID, j)
		updated, _, changed, err := index.read(WaitRequest{IDs: []string{j.ID}, After: map[string]uint64{j.ID: row.Revision}})
		if err != nil || !changed || updated[0].Revision == row.Revision || !updated[0].Spec.Deadline.Equal(deadline) {
			t.Fatal("metadata change did not wake waiters", updated, changed, err)
		}

	})
}

// Replay 35 independent waiters across twenty expired diagnostic-cache windows.
// Queue time advances only between concurrent rounds, without wall-clock sleeps.
func TestWaitReadConcurrentPolling(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		var ids []string
		for range 1 {
			ids = append(ids, submit(t, q, spec("experiment")).ID)
		}
		handler := (API{Queue: q, Token: "fixture"}).handler("", clock)
		for _, query := range []string{"", "?view=wait"} {
			for range 1 {
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

	})

	t.Run("WaitIndexCommitRollbackAndUnlockedReads", func(t *testing.T) {
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

	})
}

// Compare the existing cached GET against the wait view at the CLI's actual
// cadence: each of 35 jobs is read once per three seconds, beyond the 1s TTL.
// Includes HTTP dispatch/encoding; excludes transport and fixture setup.
