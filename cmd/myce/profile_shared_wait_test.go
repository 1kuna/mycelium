package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestProfileReceiptSnapshotClearsOmittedFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	old := profileReceipt{Status: "queued", ObservationError: "transport_error", DeliveryStatus: "delivered", ArmNonce: "old"}
	if err := saveProfileJSON(path, profileReceipt{Status: "queued", Observation: "armed", ArmNonce: "new"}); err != nil {
		t.Fatal(err)
	}
	if err := readProfileJSON(path, &old); err != nil {
		t.Fatal(err)
	}
	if old.ObservationError != "" || old.DeliveryStatus != "" || old.ArmNonce != "new" {
		t.Fatal("stale observation reused", old)
	}
}
func TestSharedProfileObserverBatchesAndRetiresTerminal(t *testing.T) {
	root := t.TempDir()
	var state atomic.Bool
	var batches atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/wait" {
			batches.Add(1)
			var request myce.WaitRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if len(request.IDs) != 2 {
				t.Errorf("not batched: %d", len(request.IDs))
			}
			reply := myce.WaitReply{}
			for _, id := range request.IDs {
				s := "running"
				if state.Load() {
					s = "succeeded"
				}
				reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: s, Revision: 1})
			}
			_ = json.NewEncoder(w).Encode(reply)
			return
		}
		if r.Method != "GET" {
			t.Errorf("unexpected method: %s", r.Method)
		}
		fmt.Fprintf(w, `{"id":%q,"state":"succeeded"}`, filepath.Base(r.URL.Path))
	}))
	defer server.Close()
	clock := &waitClock{now: time.Now()}
	for i := range 2 {
		dir := filepath.Join(root, fmt.Sprintf("receipt-%d", i))
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		receipt := profileReceipt{JobID: fmt.Sprintf("job-%d", i), Status: "queued", Observation: "armed", ArmNonce: "one"}
		if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), receipt); err != nil {
			t.Fatal(err)
		}
		registration := sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: server.URL}, Token: "fixture", Expires: clock.Now().Add(time.Hour)}
		if err := saveProfileJSON(filepath.Join(root, fmt.Sprintf("watch-%d.json", i)), registration); err != nil {
			t.Fatal(err)
		}
	}
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	// A second process cannot own this host/user observer.
	if duplicate, err := profileLock(filepath.Join(root, "observer.lock")); err == nil {
		duplicate.Close()
		t.Fatal("duplicate observer owner")
	}
	pauses := 0
	err = runSharedProfileLoop(context.Background(), root, lock, clock, func(_ context.Context, d time.Duration) error {
		pauses++
		clock.advance(d)
		state.Store(true)
		if pauses > 3 {
			return errors.New("terminal observers lingered")
		}
		return nil
	})
	if err != nil || batches.Load() != 2 {
		t.Fatal(err, "batch requests", batches.Load())
	}
	for i := range 2 {
		var receipt profileReceipt
		if err := readProfileJSON(filepath.Join(root, fmt.Sprintf("receipt-%d", i), "receipt.json"), &receipt); err != nil || receipt.Status != "succeeded" || receipt.Observation != "not_armed" {
			t.Fatal(receipt, err)
		}
	}
}

func sharedWatchFixture(t *testing.T, root, address string, clock *waitClock) (string, string, *os.File) {
	t.Helper()
	dir := filepath.Join(root, "receipt")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	receipt := profileReceipt{JobID: "job", Status: "running", Observation: "armed", ArmNonce: "arm"}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), receipt); err != nil {
		t.Fatal(err)
	}
	entry := filepath.Join(root, "watch.json")
	reg := sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: address}, Token: "fixture", Expires: clock.Now().Add(time.Hour)}
	if err := saveProfileJSON(entry, reg); err != nil {
		t.Fatal(err)
	}
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.Close() })
	return dir, entry, lock
}
func TestSharedProfilePermanentTerminalReadFailureRetires(t *testing.T) {
	for _, status := range []int{401, 404, 502} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var requests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.URL.Path == "/api/wait" {
					fmt.Fprint(w, `{"jobs":[{"id":"job","state":"succeeded","revision":1}]}`)
					return
				}
				w.WriteHeader(status)
				if status == 502 {
					fmt.Fprint(w, `{"code":"job_reply_invalid"}`)
				} else if status == 404 {
					fmt.Fprint(w, `{"code":"job_unknown"}`)
				}
			}))
			defer server.Close()
			clock := &waitClock{now: time.Now()}
			root := t.TempDir()
			dir, _, lock := sharedWatchFixture(t, root, server.URL, clock)
			if err := runSharedProfileLoop(context.Background(), root, lock, clock, func(context.Context, time.Duration) error { return nil }); err != nil {
				t.Fatal(err)
			}
			var saved profileReceipt
			if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &saved); err != nil || saved.Observation != "not_armed" || saved.ObservationError == "" || requests.Load() != 2 {
				t.Fatal(saved, requests.Load(), err)
			}
		})
	}
}
func TestSharedProfilePersistenceFailureRetainsOwnership(t *testing.T) {
	var terminal atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := "running"
		if terminal.Load() {
			state = "succeeded"
		}
		if r.URL.Path == "/api/wait" {
			fmt.Fprintf(w, `{"jobs":[{"id":"job","state":%q,"revision":1}]}`, state)
		} else {
			fmt.Fprintf(w, `{"id":"job","state":%q}`, state)
		}
	}))
	defer server.Close()
	root := t.TempDir()
	clock := &waitClock{now: time.Now()}
	dir, entry, lock := sharedWatchFixture(t, root, server.URL, clock)
	path := filepath.Join(dir, "receipt.json")
	pauses := 0
	err := runSharedProfileLoop(context.Background(), root, lock, clock, func(_ context.Context, d time.Duration) error {
		pauses++
		clock.advance(d)
		switch pauses {
		case 1:
			terminal.Store(true)
			if err := os.Rename(path, path+".saved"); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
		case 2:
			if _, err := os.Stat(entry); err != nil {
				t.Fatal("lost recovery registration", err)
			}
			if extra, err := profileLock(filepath.Join(dir, "watch.lock")); err == nil {
				extra.Close()
				t.Fatal("released failed persistence owner")
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path+".saved", path); err != nil {
				t.Fatal(err)
			}
		default:
			if pauses > 3 {
				return errors.New("persisted terminal observer lingered")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var saved profileReceipt
	if err := readProfileJSON(path, &saved); err != nil || saved.Status != "succeeded" {
		t.Fatal(saved, err)
	}

	// Re-arm the same receipt and registration for observation-only expiry.
	receipt := profileReceipt{JobID: "job", Status: "running", Observation: "armed"}
	if err := saveProfileJSON(path, receipt); err != nil {
		t.Fatal(err)
	}
	reg := sharedProfileRegistration{Directory: dir, Expires: clock.Now().Add(time.Second)}
	if err := saveProfileJSON(entry, reg); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Second)
	if err := runSharedProfileLoop(context.Background(), root, lock, clock, func(context.Context, time.Duration) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := readProfileJSON(path, &receipt); err != nil || receipt.ObservationError != "timeout" || receipt.Status != "running" {
		t.Fatal(receipt, err)
	}
}
func TestSharedProfileMissingReceiptRetires(t *testing.T) {
	root := t.TempDir()
	clock := &waitClock{now: time.Now()}
	dir, entry, lock := sharedWatchFixture(t, root, "", clock)
	if err := os.Remove(filepath.Join(dir, "receipt.json")); err != nil {
		t.Fatal(err)
	}
	if err := runSharedProfileLoop(context.Background(), root, lock, clock, func(context.Context, time.Duration) error { return errors.New("missing receipt kept observer alive") }); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(entry); !os.IsNotExist(err) {
		t.Fatal("stale registration retained", err)
	}
}
