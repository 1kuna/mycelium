package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Each completed notifier contributes one virtual service unit. Keep the real
// process/receipt path, but make pacing independent of shell startup and host load.
type observerDeliveryClock struct {
	waitClock
	events    string
	delivered int
}

func (c *observerDeliveryClock) Now() time.Time {
	data, err := os.ReadFile(c.events)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Discovery also measures local work; give it deterministic nonzero cost.
	c.now = c.now.Add(time.Millisecond)
	c.delivered = max(c.delivered, bytes.Count(data, []byte{'\n'}))
	return c.now.Add(time.Duration(c.delivered) * time.Second)
}

func TestObserverRecoveryPacesEachBinding(t *testing.T) {
	root := t.TempDir()
	events := filepath.Join(root, "events")
	clock := &observerDeliveryClock{waitClock: waitClock{now: time.Now()}, events: events}
	finished := clock.Now().Add(-time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/wait" {
			var req myce.WaitRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			reply := myce.WaitReply{}
			for _, id := range req.IDs {
				reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: "succeeded", Revision: 1})
			}
			_ = json.NewEncoder(w).Encode(reply)
			return
		}
		_ = json.NewEncoder(w).Encode(myce.Job{ID: filepath.Base(r.URL.Path), State: "succeeded", FinishedAt: &finished})
	}))
	defer server.Close()
	for _, binding := range []string{"a", "b"} {
		hook := filepath.Join(root, "notify-"+binding)
		// The completed event supplies the virtual service cost. Other bindings
		// must still finish this pass.
		if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat >> "+shellArg(events)+"\nprintf '\\n' >> "+shellArg(events)+"\n"), 0700); err != nil {
			t.Fatal(err)
		}
		count := 1
		if binding == "a" {
			count = 3
		}
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("job-%s-%d", binding, i)
			dir := filepath.Join(root, id)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			r := profileReceipt{JobID: id, Status: "queued", Observation: "armed", ArmNonce: id, Nonce: id}
			if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
				t.Fatal(err)
			}
			reg := sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: server.URL, Notify: hook}, Token: "fixture", Expires: clock.Now().Add(time.Hour)}
			if err := saveProfileJSON(filepath.Join(root, fmt.Sprintf("%s-%d.json", binding, i)), reg); err != nil {
				t.Fatal(err)
			}
		}
	}
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	ctx := t.Context()
	pauses := 0
	sawPacedDelivery := false
	err = runSharedProfileLoop(ctx, root, lock, clock, func(ctx context.Context, delay time.Duration) error {
		pauses++
		var state observerDiagnostic
		if err := readProfileJSON(filepath.Join(root, "observer.status"), &state); err != nil {
			t.Fatal(err)
		}
		if delay <= time.Nanosecond && state.NextDeliveryAt.After(clock.Now()) {
			t.Fatal("paced delivery kept an expired read timer and would busy-poll")
		}
		if pauses > 4 {
			t.Fatal("four completions did not retire after their scheduled delivery passes")
		}
		if state.NextDeliveryAt.After(clock.Now()) {
			sawPacedDelivery = true
		}
		if pauses == 1 {
			data, e := os.ReadFile(events)
			if e != nil {
				t.Fatal(e)
			}
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			if len(lines) != 2 {
				t.Fatalf("binding backlog burst: first-pass deliveries=%d", len(lines))
			}
			var second profileNotification
			if e = json.Unmarshal([]byte(lines[1]), &second); e != nil || second.JobID != "job-b-0" {
				t.Fatal("paced binding blocked independent delivery", e)
			}
		}
		// Reads are already terminal. Jump directly to the next delivery wake,
		// including when discovery would otherwise wake earlier.
		clock.advance(max(delay, state.NextDeliveryAt.Sub(clock.Now())))
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawPacedDelivery {
		t.Fatal("nonzero delivery service cost did not pace the backlog")
	}
	data, err := os.ReadFile(events)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var n profileNotification
		if err = json.Unmarshal([]byte(line), &n); err != nil {
			t.Fatal(err)
		}
		if seen[n.DeliveryID] {
			t.Fatal("repeated paced notification")
		}
		seen[n.DeliveryID] = true
	}
	if len(seen) != 4 {
		t.Fatal("paced delivery lost completions", len(seen))
	}
}

func TestObserverDiagnosticsRetainSafeFailureAndBoundLog(t *testing.T) {
	root := t.TempDir()
	clock := &waitClock{now: time.Now()}
	d := newObserverDiagnostics(root, clock)
	// A raw error message is explicitly hostile/private; only its typed cause
	// belongs in diagnostics. No transport target or token should be retained.
	err := d.record("batch_read_failed", "job", &myce.HTTPError{Status: 429, Code: "client_rate_limited", Message: "https://private.invalid/receiver secret-fixture"}, true)
	if err != nil {
		t.Fatal(err)
	}
	status, err := os.ReadFile(filepath.Join(root, "observer.status"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(status), "HTTP 429 code=client_rate_limited") || strings.Contains(string(status), "private.invalid") || strings.Contains(string(status), "secret-fixture") {
		t.Fatal("unsafe or uninformative observer cause")
	}
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = waitCommand(context.Background(), []string{"--observer-status", "--observer-dir", root}, &out, &out); err != nil {
		t.Fatal(err)
	}
	var live struct {
		OwnerLocked bool `json:"owner_locked"`
	}
	if err = json.Unmarshal(out.Bytes(), &live); err != nil || !live.OwnerLocked {
		t.Fatal("status missed live owner", err)
	}
	lock.Close()
	out.Reset()
	if err = observerStatusCommand(root, &out); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(out.Bytes(), &live); err != nil || live.OwnerLocked {
		t.Fatal("stale status claimed live owner", err)
	}
	for i := 0; i < 4; i++ {
		if _, err = (&d.log).Write(make([]byte, observerLogBytes)); err != nil {
			t.Fatal(err)
		}
	}
	// Fill with structured events to force rotation; each retained file is bounded.
	for i := 0; i < 300; i++ {
		if err = d.log.event(struct {
			Event string `json:"event"`
		}{strings.Repeat("safe", 2048)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"observer.log", "observer.log.1"} {
		p := filepath.Join(root, name)
		s, e := os.Stat(p)
		if e != nil || s.Size() > observerLogBytes || s.Mode().Perm() != 0600 {
			t.Fatalf("unbounded/nonprivate diagnostic %s: %v %v", name, s, e)
		}
	}
}
