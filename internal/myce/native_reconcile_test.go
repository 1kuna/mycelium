package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func nativeReconcileFixture(t *testing.T, j Job) (*workerStore, Result) {
	t.Helper()
	dir := t.TempDir()
	if err := PrepareStateRoots(dir); err != nil {
		t.Fatal(err)
	}
	r := result(&j, false)
	r.ReleasePending = true
	r.Error = "inspect process groups: exec: WaitDelay expired before I/O complete"
	r.CleanupCause = &RuntimeCause{Operation: "process_group", Class: "io_error"}
	s := &workerStore{dir: dir, stateName: "state-attempt-fixture.json", state: workerState{Session: j.Session, Identity: "exact-native", PID: 2147483647, Job: &j, Result: &r}}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{
		"state.json":                workerState{Session: j.Session, Attempts: []string{s.stateName}},
		"attempt-exact-native.json": workerState{Identity: s.state.Identity, PID: s.state.PID, Groups: map[int]map[int]string{s.state.PID: {s.state.PID: "recorded-kernel-birth"}}},
		"exact-native.result.json":  r,
	} {
		if err := saveWorkerJSON(dir, name, value); err != nil {
			t.Fatal(err)
		}
	}
	return s, r
}

func readNativeReconciled(t *testing.T, s *workerStore) workerState {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(s.dir, s.stateName))
	if err != nil {
		t.Fatal(err)
	}
	var got workerState
	if err := decodeWorkerState(data, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// #269: an old worker cannot acquire its cleanup fix because upgrade preflight
// refuses its pending result, even after the command and process group are gone.
func TestReconcileNativeAttemptSettlesUpgrade(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	j := maintenanceAssign(t, q, m, "experiment")
	s, pending := nativeReconcileFixture(t, *j)
	if err := q.Complete(pending); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: s.dir}
	client := Client{URL: server.URL, Token: "test"}
	prepare := func() (HostMaintenanceStatus, error) {
		return PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil })
	}
	if _, err := prepare(); err == nil || !strings.Contains(err.Error(), "custody") {
		t.Fatal("old-worker pending release failed to reproduce upgrade refusal", err)
	}
	// The failed inspection must never be replaced by an operator assertion.
	err := reconcileNativeAttempt(context.Background(), s.dir, s.state.Identity, func(int) ([]int, error) { return nil, exec.ErrWaitDelay }, processBirth, nativeScratchEmpty)
	if !errors.Is(err, exec.ErrWaitDelay) || !readNativeReconciled(t, s).Result.ReleasePending {
		t.Fatal("inspection failure released custody", err)
	}
	rootBefore, _ := os.ReadFile(filepath.Join(s.dir, "state.json"))
	if err := ReconcileNativeAttempt(context.Background(), s.dir, s.state.Identity); err != nil {
		t.Fatal(err)
	}
	got := readNativeReconciled(t, s)
	want := pending
	want.ReleasePending = false
	if got.PID != 0 || !reflect.DeepEqual(*got.Result, want) {
		t.Fatal("reconciliation lost the failed outcome or exact result", got.Result)
	}
	var receipt Result
	data, _ := os.ReadFile(filepath.Join(s.dir, "exact-native.result.json"))
	if err := json.Unmarshal(data, &receipt); err != nil || !reflect.DeepEqual(receipt, want) {
		t.Fatal("recovery receipt retained stale pending release", receipt, err)
	}
	rootAfter, _ := os.ReadFile(filepath.Join(s.dir, "state.json"))
	if string(rootBefore) != string(rootAfter) {
		t.Fatal("reconciliation rewrote machine index")
	}
	// Local reconciliation alone cannot release queue custody. The unchanged
	// installed worker replays the exact saved result on reconnect.
	if _, err := prepare(); err == nil {
		t.Fatal("offline command bypassed queue release acknowledgement")
	}
	if err := q.Complete(receipt); err != nil {
		t.Fatal(err)
	}
	check(t, q, m, nil)
	upgrade, err := prepare()
	if err != nil {
		t.Fatal("verified release did not unblock normal upgrade", err)
	}
	if err := FinishEnrollmentUpgrade(context.Background(), c, client, upgrade); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "failed" || !got.ReleaseConfirmed || got.Attempt != j.Attempt {
		t.Fatal("reconciliation changed failure or reran the attempt", got)
	}
}

func TestReconcileNativeAttemptLiveProcessKeepsCustody(t *testing.T) {
	s, _ := nativeReconcileFixture(t, Job{ID: "job", Attempt: 1, Session: "session", Machine: "hypha"})
	pid := os.Getpid()
	start, err := processBirth(pid)
	if err != nil {
		t.Fatal(err)
	}
	s.state.PID = pid
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(s.dir, "attempt-exact-native.json", workerState{Identity: s.state.Identity, PID: pid, Groups: map[int]map[int]string{syscall.Getpgrp(): {pid: start}}}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(s.dir, s.stateName))
	if err := ReconcileNativeAttempt(context.Background(), s.dir, s.state.Identity); err == nil {
		t.Fatal("live process group lost custody")
	}
	// Even with an empty original group, a recorded survivor or reused PID
	// must block release; it may have changed its process group.
	if err := reconcileNativeAttempt(context.Background(), s.dir, s.state.Identity, func(int) ([]int, error) { return nil, nil }, processBirth, nativeScratchEmpty); err == nil {
		t.Fatal("recorded live PID outside its old group lost custody")
	}
	after, _ := os.ReadFile(filepath.Join(s.dir, s.stateName))
	if string(before) != string(after) {
		t.Fatal("refusal modified pending custody")
	}
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatal("reconciliation signalled live work", err)
	}
}

func TestReconcileNativeAttemptRequiresOfflineExactResultAndClosedScratch(t *testing.T) {
	s, _ := nativeReconcileFixture(t, Job{ID: "job", Attempt: 1, Session: "session", Machine: "hypha"})
	unlock, err := commandLock(s.dir, "worker.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := ReconcileNativeAttempt(context.Background(), s.dir, s.state.Identity); err == nil {
		t.Fatal("active spool accepted")
	}
	unlock()
	if err := ReconcileNativeAttempt(context.Background(), s.dir, "another-attempt"); err == nil {
		t.Fatal("unrelated identity accepted")
	}
	if err := reconcileNativeAttempt(context.Background(), s.dir, s.state.Identity, workerGroupMembers, processBirth, func(context.Context, string) error { return errors.New("open scratch handle") }); err == nil {
		t.Fatal("open scratch accepted")
	}
	stale := *s.state.Result
	stale.Attempt++
	if err := saveWorkerJSON(s.dir, "exact-native.result.json", stale); err != nil {
		t.Fatal(err)
	}
	if err := ReconcileNativeAttempt(context.Background(), s.dir, s.state.Identity); err == nil || !readNativeReconciled(t, s).Result.ReleasePending {
		t.Fatal("stale result discharged exact attempt", err)
	}
}

func TestNativeScratchEmptyInspectsOpenHandles(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "output"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := nativeScratchEmpty(context.Background(), dir); err == nil {
		t.Fatal("OS inspector missed open scratch handle")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := nativeScratchEmpty(context.Background(), dir); err != nil {
		t.Fatal("closed scratch did not settle", err)
	}
}
