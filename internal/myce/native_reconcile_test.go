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
	// External inspector fixture: host mounts must not decide fake-clock queue
	// recovery tests. OS descriptor behavior is checked separately below.
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "lsof"), []byte("#!/bin/sh\nprintf 'p1\\nn/dev/null\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
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
		"attempt-exact-native.json": workerState{Identity: s.state.Identity, PID: s.state.PID},
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
	other := s.state
	other.Identity = "unrelated"
	other.PID = os.Getpid()
	if err := saveWorkerJSON(s.dir, "state-attempt-other.json", other); err != nil {
		t.Fatal(err)
	}
	otherBefore, _ := os.ReadFile(filepath.Join(s.dir, "state-attempt-other.json"))
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
	otherAfter, _ := os.ReadFile(filepath.Join(s.dir, "state-attempt-other.json"))
	if string(rootBefore) != string(rootAfter) || string(otherBefore) != string(otherAfter) {
		t.Fatal("reconciliation rewrote machine index or unrelated attempt")
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

func TestReconcileNativeAttemptDeadGroupAndSidecars(t *testing.T) {
	s, _ := nativeReconcileFixture(t, Job{ID: "job", Attempt: 1, Session: "session", Machine: "hypha"})
	if err := saveWorkerJSON(s.dir, "attempt-exact-native.json", workerState{Identity: s.state.Identity, PID: s.state.PID, Groups: map[int]map[int]string{s.state.PID: {s.state.PID: "supervisor-birth", s.state.PID - 1: "sidecar-birth"}}}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(s.dir, "native-exact-native.json", struct {
		Identity string
		Members  []nativeMember
	}{s.state.Identity, []nativeMember{{Name: "main", PID: s.state.PID}, {Name: "helper", PID: s.state.PID - 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := ReconcileNativeAttempt(context.Background(), s.dir, s.state.Identity); err != nil || readNativeReconciled(t, s).Result.ReleasePending {
		t.Fatal("verified-dead native group and sidecars did not settle", err)
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

func TestReconcileNativeAttemptPIDOnlyRetainsPopulatedCgroup(t *testing.T) {
	s, _ := nativeReconcileFixture(t, Job{ID: "job", Attempt: 1, Session: "session", Machine: "hypha"})
	s.state.Cgroup = filepath.Join(t.TempDir(), "attempt-"+s.state.Identity)
	if err := os.Mkdir(s.state.Cgroup, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(s.state.Cgroup, "cgroup.events")
	if err := os.WriteFile(events, []byte("populated 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ReconcileNativeAttempt(context.Background(), s.dir, s.state.Identity); err == nil || !readNativeReconciled(t, s).Result.ReleasePending {
		t.Fatal("PID-only custody ignored populated saved cgroup", err)
	}
	if err := os.WriteFile(events, []byte("populated 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ReconcileNativeAttempt(context.Background(), s.dir, s.state.Identity); err != nil {
		t.Fatal("empty saved cgroup did not settle PID-only custody", err)
	}
}

func TestNativeScratchEmptyInspectsOpenHandles(t *testing.T) {
	// Incomplete inventories (including unavailable host mounts) must refuse.
	// Such hosts cannot supply positive OS proof for this integration check.
	if err := nativeScratchEmpty(context.Background(), t.TempDir()); err != nil {
		t.Skip("complete OS descriptor inventory unavailable: ", err)
	}
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "output"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := nativeScratchEmpty(context.Background(), dir); err == nil {
		t.Fatal("OS inspector missed open scratch handle")
	}
	if err := os.Remove(f.Name()); err != nil {
		t.Fatal(err)
	}
	if err := nativeScratchEmpty(context.Background(), dir); err == nil {
		t.Fatal("OS inspector missed unlinked open scratch handle")
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := nativeScratchEmpty(context.Background(), dir); err == nil {
		t.Fatal("OS inspector missed handle under unlinked scratch directory")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := nativeScratchEmpty(context.Background(), dir); err != nil {
		t.Fatal("closed scratch did not settle", err)
	}
}

func TestNativeScratchEmptyRetainsUnlinkedHandlesAndInspectionFailures(t *testing.T) {
	dir, bin := t.TempDir(), t.TempDir()
	canonical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	inspector := filepath.Join(bin, "lsof")
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MYCE_TEST_SCRATCH_HANDLE", canonical+"/unlinked-output (deleted)")
	if err := os.WriteFile(inspector, []byte("#!/bin/sh\nprintf 'p123\\nn%s\\n' \"$MYCE_TEST_SCRATCH_HANDLE\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := nativeScratchEmpty(context.Background(), dir); err == nil {
		t.Fatal("unlinked descriptor inventory discharged scratch custody")
	}
	t.Setenv("MYCE_TEST_SCRATCH_HANDLE", "/dev/null")
	if err := nativeScratchEmpty(context.Background(), dir); err != nil {
		t.Fatal("complete empty scratch inventory failed", err)
	}
	if err := os.WriteFile(inspector, []byte("#!/bin/sh\nprintf 'p123\\nn/dev/null\\n'\nprintf 'incomplete inventory' >&2\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := nativeScratchEmpty(context.Background(), dir); err == nil {
		t.Fatal("inspection warning discharged scratch custody")
	}
}
