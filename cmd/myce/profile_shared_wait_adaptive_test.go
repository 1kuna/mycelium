package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func adaptiveReceipt(t *testing.T, root, address, name string, expires time.Time) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	receipt := profileReceipt{JobID: name, Status: "running", Observation: "armed", ArmNonce: name}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), receipt); err != nil {
		t.Fatal(err)
	}
	reg := sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: address}, Token: "fixture", Expires: expires}
	if err := saveProfileJSON(filepath.Join(root, name+".json"), reg); err != nil {
		t.Fatal(err)
	}
	return dir
}
func adaptiveReply(w http.ResponseWriter, r *http.Request, state string) {
	if r.Method == "GET" {
		fmt.Fprintf(w, `{"id":%q,"state":%q}`, filepath.Base(r.URL.Path), state)
		return
	}
	var request myce.WaitRequest
	_ = json.NewDecoder(r.Body).Decode(&request)
	reply := myce.WaitReply{}
	for _, id := range request.IDs {
		reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: state, Revision: 1})
	}
	_ = json.NewEncoder(w).Encode(reply)
}
func TestSharedProfileAdaptiveSilentAuthorityIsolation(t *testing.T) {
	silent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain the request so net/http can observe the client's cancellation
		// while this fixture deliberately withholds its response.
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer silent.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { adaptiveReply(w, r, "succeeded") }))
	defer healthy.Close()
	root := t.TempDir()
	adaptiveReceipt(t, root, silent.URL, "silent", time.Time{})
	done := adaptiveReceipt(t, root, healthy.URL, "healthy", time.Time{})
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err = runSharedProfileLoop(ctx, root, lock, myce.RealClock{}, waitPause)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	var receipt profileReceipt
	if err := readProfileJSON(filepath.Join(done, "receipt.json"), &receipt); err != nil || receipt.Status != "succeeded" || receipt.Observation != "not_armed" {
		t.Fatal("silent authority blocked healthy completion", receipt, err)
	}
}
func TestSharedProfileAdaptiveRetryAfterAcrossBatchesAndRegistration(t *testing.T) {
	for _, status := range []int{429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			clock := &waitClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
			var mu sync.Mutex
			var calls []time.Time
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				calls = append(calls, clock.Now())
				mu.Unlock()
				w.Header().Set("Retry-After", "7")
				w.WriteHeader(status)
			}))
			defer server.Close()
			root := t.TempDir()
			for i := 0; i < 257; i++ {
				adaptiveReceipt(t, root, server.URL, fmt.Sprintf("job-%03d", i), time.Time{})
			}
			lock, err := profileLock(filepath.Join(root, "observer.lock"))
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			added := false
			stop := errors.New("observed second paced read")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = runSharedProfileLoop(ctx, root, lock, clock, func(_ context.Context, d time.Duration) error {
				if !added {
					adaptiveReceipt(t, root, server.URL, "new-registration", time.Time{})
					added = true
				}
				mu.Lock()
				defer mu.Unlock()
				if len(calls) > 1 {
					return stop
				}
				clock.advance(d)
				return nil
			})
			if !errors.Is(err, stop) {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(calls) != 2 || calls[1].Sub(calls[0]) < 7*time.Second {
				t.Fatal("Retry-After bypassed by batch or registration", calls)
			}
		})
	}
}
func TestSharedProfileAdaptiveRetiresAuthorityPacing(t *testing.T) {
	var pendingReads atomic.Int32
	terminal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { adaptiveReply(w, r, "succeeded") }))
	defer terminal.Close()
	pending := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { pendingReads.Add(1); adaptiveReply(w, r, "running") }))
	defer pending.Close()
	root := t.TempDir()
	done := adaptiveReceipt(t, root, terminal.URL, "done", time.Time{})
	adaptiveReceipt(t, root, pending.URL, "pending", time.Time{})
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	clock := &waitClock{now: time.Now()}
	stop := errors.New("observed continued paced reads")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = runSharedProfileLoop(ctx, root, lock, clock, func(_ context.Context, d time.Duration) error {
		var receipt profileReceipt
		_ = readProfileJSON(filepath.Join(done, "receipt.json"), &receipt)
		if receipt.Observation == "not_armed" && d <= 0 {
			t.Fatal("retired authority forced busy polling")
		}
		clock.advance(d)
		if receipt.Observation == "not_armed" && pendingReads.Load() >= 5 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
}
