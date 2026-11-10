package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestObserverSupervisorOwnerDeath(t *testing.T) {
	// All registry, notifier and process state belongs to this isolated fixture.
	t.Setenv("MYCE_DEV_SESSION_ROOT", t.TempDir())
	root := t.TempDir()
	binary := devTestBinary(t)
	var terminal atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := "running"
		if terminal.Load() {
			state = "succeeded"
		}
		if r.URL.Path == "/api/wait" {
			var req myce.WaitRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			reply := myce.WaitReply{}
			for _, id := range req.IDs {
				reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: state, Revision: 1})
			}
			_ = json.NewEncoder(w).Encode(reply)
			return
		}
		fmt.Fprintf(w, `{"id":%q,"state":%q}`, filepath.Base(r.URL.Path), state)
	}))
	defer server.Close()
	events := filepath.Join(root, "events")
	hook := filepath.Join(root, "notify")
	// The first notifier accepts a delivery and kills its own isolated owner
	// before the receipt acknowledgement. Replays must carry identical bytes;
	// this fixture receiver deduplicates them by the stable job/delivery binding.
	script := "#!/bin/sh\ncat > " + shellArg(filepath.Join(root, "incoming")) + "\n" +
		"id=$(sed -n 's/.*\"job_id\":\"\\([^\"]*\\)\".*/\\1/p' " + shellArg(filepath.Join(root, "incoming")) + ")\n" +
		"accepted=" + shellArg(root) + "/accepted-$id\n" +
		"if [ -f \"$accepted\" ]; then cmp \"$accepted\" " + shellArg(filepath.Join(root, "incoming")) + " || exit 1\n" +
		"else cp " + shellArg(filepath.Join(root, "incoming")) + " \"$accepted\"; cat \"$accepted\" >> " + shellArg(events) + "; printf '\\n' >> " + shellArg(events) + "; fi\n" +
		"if [ ! -f " + shellArg(filepath.Join(root, "notifier-death")) + " ]; then touch " + shellArg(filepath.Join(root, "notifier-death")) + "; kill -KILL \"$PPID\"; fi\n"
	if err := os.WriteFile(hook, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	var dirs []string
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("job-%d", i)
		dir := filepath.Join(root, id)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, dir)
		r := profileReceipt{JobID: id, Key: id, Nonce: id, ArmNonce: id, Observation: "armed", Status: "running"}
		if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
			t.Fatal(err)
		}
		reg := sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: server.URL, Notify: hook}, Token: "fixture"}
		if err := saveProfileJSON(filepath.Join(root, id+".json"), reg); err != nil {
			t.Fatal(err)
		}
	}
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := startSharedObserver(ctx, binary, root, lock); err != nil {
		t.Fatal(err)
	}
	lock.Close()
	var initial observerDiagnostic
	if err := readProfileJSON(filepath.Join(root, "observer.status"), &initial); err != nil {
		t.Fatal(err)
	}
	var parent observerSupervision
	if err := readProfileJSON(filepath.Join(root, "observer.supervision"), &parent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(parent.PID, syscall.SIGTERM) })
	// No keyed command, registration write, or status-triggered recovery follows.
	if err := syscall.Kill(initial.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	var replacement observerDiagnostic
	wait(t, ctx, func() bool {
		return readProfileJSON(filepath.Join(root, "observer.status"), &replacement) == nil && replacement.PID != initial.PID && replacement.Registrations == 3
	})
	t.Cleanup(func() { _ = syscall.Kill(replacement.PID, syscall.SIGKILL) })
	if duplicate, err := profileLock(filepath.Join(root, "observer.lock")); err == nil {
		duplicate.Close()
		t.Fatal("replacement lost exclusive lifetime ownership")
	}
	for _, dir := range dirs {
		var r profileReceipt
		if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &r); err != nil || r.ArmNonce != filepath.Base(dir) || r.Observation != "armed" {
			t.Fatal("recovery changed original arm", err)
		}
	}
	var retained observerSupervision
	if err := readProfileJSON(filepath.Join(root, "observer.supervision"), &retained); err != nil || retained.LastRecovery == nil || retained.LastRecovery.Signal != int(syscall.SIGKILL) {
		t.Fatal("missing exact child exit diagnostics", err)
	}
	terminal.Store(true)
	wait(t, ctx, func() bool {
		for _, dir := range dirs {
			var r profileReceipt
			if readProfileJSON(filepath.Join(dir, "receipt.json"), &r) != nil || r.DeliveryStatus != "delivered" {
				return false
			}
		}
		owner, err := profileLock(filepath.Join(root, "observer.lock"))
		if err != nil {
			return false
		}
		owner.Close()
		return true
	})
	if err := readProfileJSON(filepath.Join(root, "observer.supervision"), &retained); err != nil || retained.Restarts != 2 || retained.LastRecovery.Diagnostic.Phase != "notifier" {
		t.Fatal("notifier-interrupted recovery was not retained", err)
	}
	data, err := os.ReadFile(events)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var n profileNotification
		if err := json.Unmarshal([]byte(line), &n); err != nil {
			t.Fatal(err)
		}
		if seen[n.DeliveryID] {
			t.Fatal("duplicate notification")
		}
		seen[n.DeliveryID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("observed %d of 3 completions", len(seen))
	}
}
