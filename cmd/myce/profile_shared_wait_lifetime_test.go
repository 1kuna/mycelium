package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// The detached observer must not outlive its queue (a dev teardown) forever,
// yet a long-running job on a live queue keeps it, even across short outages.
func TestSharedProfileObserverLifetime(t *testing.T) {
	const (
		up = iota
		down
		terminal
	)
	var mode atomic.Int32
	var held atomic.Pointer[waitClock]
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := "running"
		switch mode.Load() {
		case down:
			// No HTTP answer at all, as when the queue process is gone.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		case terminal:
			state = "succeeded"
		}
		if r.URL.Path == "/api/wait" {
			// A live queue holds an unchanged long-poll; the fake clock must pass.
			if clock := held.Load(); clock != nil && state == "running" {
				clock.advance(time.Minute)
			}
			fmt.Fprintf(w, `{"jobs":[{"id":"job","state":%q,"revision":1}]}`, state)
			return
		}
		fmt.Fprintf(w, `{"id":"job","state":%q}`, state)
	}))
	defer server.Close()
	observe := func(t *testing.T, clock *waitClock, step func(elapsed time.Duration)) (profileReceipt, string, error) {
		root := t.TempDir()
		dir, entry, lock := sharedWatchFixture(t, root, server.URL, clock)
		// Profile submissions register without an expiry; only the queue bounds them.
		if err := saveProfileJSON(entry, sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: server.URL}, Token: "fixture"}); err != nil {
			t.Fatal(err)
		}
		start, pauses := clock.Now(), 0
		err := runSharedProfileLoop(context.Background(), root, lock, clock, func(_ context.Context, d time.Duration) error {
			if pauses++; pauses > 1000 { // Far beyond the 1 h bound
				return errors.New("observer never settled")
			}
			// Ten-minute samples still cover both 50-minute outages and the
			// three-hour live period without hundreds of identical disk/HTTP rounds.
			clock.advance(max(d, 10*time.Minute))
			step(clock.Now().Sub(start))
			return nil
		})
		var saved profileReceipt
		if e := readProfileJSON(filepath.Join(dir, "receipt.json"), &saved); e != nil {
			t.Fatal(e)
		}
		return saved, entry, err
	}

	t.Run("live-queue-keeps-observer", func(t *testing.T) {
		mode.Store(down)
		clock := &waitClock{now: time.Now()}
		held.Store(clock)
		defer held.Store(nil)
		saved, _, err := observe(t, clock, func(elapsed time.Duration) {
			switch {
			case elapsed < 50*time.Minute:
				mode.Store(down)
			case elapsed < 3*time.Hour:
				mode.Store(up) // a long job on a live queue; the answer resets the outage
			case elapsed < 3*time.Hour+50*time.Minute:
				mode.Store(down)
			default:
				mode.Store(terminal)
			}
		})
		if err != nil || saved.Status != "succeeded" || saved.ObservationError != "" {
			t.Fatalf("live observation ended early: %+v %v", saved, err)
		}
	})

	t.Run("unreachable-queue-ends-observation", func(t *testing.T) {
		mode.Store(down)
		clock := &waitClock{now: time.Now()}
		start := clock.Now()
		saved, entry, err := observe(t, clock, func(time.Duration) {})
		if err != nil || saved.Observation != "not_armed" || saved.ObservationError != "transport_error" || saved.Status != "running" {
			t.Fatalf("unreachable observation: %+v %v", saved, err)
		}
		if elapsed := clock.Now().Sub(start); elapsed < observerUnreachableLimit || elapsed > 2*observerUnreachableLimit {
			t.Fatalf("gave up after %s", elapsed)
		}
		if _, e := os.Stat(entry); !os.IsNotExist(e) {
			t.Fatal("registration retained", e)
		}
	})

	t.Run("removed-receipt-or-root-exits", func(t *testing.T) {
		mode.Store(up)
		for _, remove := range []string{"receipt", "root"} {
			root := t.TempDir()
			clock := &waitClock{now: time.Now()}
			dir, _, lock := sharedWatchFixture(t, root, server.URL, clock)
			pauses := 0
			err := runSharedProfileLoop(context.Background(), root, lock, clock, func(_ context.Context, d time.Duration) error {
				if pauses++; pauses > 3 {
					return errors.New(remove + " removal kept the observer alive")
				}
				target := dir
				if remove == "root" {
					target = root
				}
				clock.advance(d)
				return os.RemoveAll(target)
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}
