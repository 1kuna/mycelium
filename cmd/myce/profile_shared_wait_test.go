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
			if len(request.IDs) != 170 {
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
	for i := range 170 {
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
	for i := range 170 {
		var receipt profileReceipt
		if err := readProfileJSON(filepath.Join(root, fmt.Sprintf("receipt-%d", i), "receipt.json"), &receipt); err != nil || receipt.Status != "succeeded" || receipt.Observation != "not_armed" {
			t.Fatal(receipt, err)
		}
	}
}

func TestSharedProfileObserverExpiryIsObservationFailure(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "receipt")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	clock := &waitClock{now: time.Now()}
	receipt := profileReceipt{JobID: "job", Status: "running", Observation: "armed"}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), receipt); err != nil {
		t.Fatal(err)
	}
	reg := sharedProfileRegistration{Directory: dir, Expires: clock.Now().Add(time.Second)}
	if err := saveProfileJSON(filepath.Join(root, "watch.json"), reg); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Second)
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := runSharedProfileLoop(context.Background(), root, lock, clock, func(context.Context, time.Duration) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &receipt); err != nil || receipt.ObservationError != "timeout" || receipt.Status != "running" {
		t.Fatal(receipt, err)
	}
}
