package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestWorkerRetryFakeClockBoundaries(t *testing.T) {
	c := &fakeClock{now: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)}
	var r workerRetry
	if !r.ready(c.Now()) {
		t.Fatal("first request must run immediately")
	}
	for _, d := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		r.failed(c.Now(), time.Second)
		if r.delay != d {
			t.Fatalf("delay %s want %s", r.delay, d)
		}
		c.Advance(d - time.Nanosecond)
		if r.ready(c.Now()) {
			t.Fatal("retry before deadline")
		}
		c.Advance(time.Nanosecond)
		if !r.ready(c.Now()) {
			t.Fatal("retry not ready at deadline")
		}
	}
	r.succeeded(c.Now(), time.Second)
	c.Advance(time.Second)
	r.failed(c.Now(), time.Second)
	if r.delay != time.Second {
		t.Fatal("success did not reset backoff")
	}
}
func recoveryWorker(t *testing.T) Worker {
	t.Helper()
	return Worker{Machine: machine("recovery-worker", "fake"), StateDir: t.TempDir(), Interval: time.Millisecond, Clock: RealClock{}}
}
func recoveryJob(t *testing.T, command []string) Job {
	t.Helper()
	s := spec("experiment")
	s.Run.Command = command
	return Job{ID: ID(), Spec: s, Attempt: 1, Checkpoint: filepath.Join(t.TempDir(), "job")}
}
func TestWorkerStateLockAndCorruption(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.openStore(); err == nil {
		t.Fatal("second owner acquired worker state lock")
	}
	s.state.Session = "persisted"
	if err = s.save(); err != nil {
		t.Fatal(err)
	}
	s.close()
	s, err = w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	if s.state.Session != "persisted" {
		t.Fatal("state not durable")
	}
	s.close()
	if err = os.WriteFile(filepath.Join(w.StateDir, "state.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = w.openStore(); err == nil {
		t.Fatal("corrupt state silently discarded")
	}
}
func TestWorkerStateRejectsForeignDocuments(t *testing.T) {
	for _, data := range []string{
		`{"state":"failed","pid":null,"assigned_xpu":1,"preemption_count":0,"attempt_token":"synthetic-old-attempt"}`,
		`{"state":"running","pid":999999,"assigned_xpu":1,"preemption_count":2,"attempt_token":"synthetic-old-attempt"}`,
		`{}`, `null`, `{"pid":999999}`, `{"session":"old","fresh":false}`,
		`{"session":null,"fresh":false,"offset":0}`,
		`{"session":"old","fresh":false,"offset":0,"attempt_token":"foreign"}`,
		`{"session":"old","fresh":false,"offset":0} {}`,
	} {
		t.Run(data, func(t *testing.T) {
			w := recoveryWorker(t)
			path := filepath.Join(w.StateDir, "state.json")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				store, err := w.openStore()
				if err == nil {
					store.close()
					t.Fatal("foreign or incomplete state accepted")
				}
				if !strings.Contains(err.Error(), "read worker state") {
					t.Fatal(err)
				}
			}
			got, err := os.ReadFile(path)
			if err != nil || string(got) != data {
				t.Fatalf("rejected state changed: %v", err)
			}
		})
	}
}

func TestWorkerStateAcceptsExistingSchema(t *testing.T) {
	for _, data := range []string{
		`{"session":"old","fresh":false,"offset":0}`,
		`{"session":"old","previous_session":"previous","fresh":true,"offset":17,"pid":42,"identity":"owned","launch_pending":true,"birth":"birth","pending":2,"checkpoint_root":"/local","log":"owned.log"}`,
	} {
		w := recoveryWorker(t)
		if err := os.WriteFile(filepath.Join(w.StateDir, "state.json"), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		store, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		if store.state.Session != "old" {
			t.Fatal("existing session lost")
		}
		if err := store.save(); err != nil {
			t.Fatal(err)
		}
		store.close()
		store, err = w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		store.close()
	}
}

func TestWorkerOfflineLargeOutputIsDurable(t *testing.T) {
	w := recoveryWorker(t)
	w.Client.URL = "http://127.0.0.1:1"
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.state.Session = "offline-session"
	j := recoveryJob(t, []string{"sh", "-c", `head -c 4194304 /dev/zero; echo done`})
	w = workerWithPreparedCheckpoint(t, w, s, j)
	p, err := w.start(s, j)
	if err != nil {
		t.Fatal(err)
	}
	var finish workerFinish
	select {
	case finish = <-p.done:
	case <-time.After(testWaitTimeout):
		p.stop(w.Clock.Now(), 0, true)
		t.Fatal("offline output blocked the command")
	}
	if finish.err != nil || !finish.result.Success {
		t.Fatalf("finish: %+v", finish)
	}
	info, err := os.Stat(filepath.Join(s.dir, s.state.Log))
	if err != nil || info.Size() != 4194309 {
		t.Fatalf("spool size %v: %v", info, err)
	}
	data, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result Result
	if err = json.Unmarshal(data, &result); err != nil || result.Session != "offline-session" || !result.Success {
		t.Fatalf("persisted result %+v: %v", result, err)
	}
}
func TestWorkerAttemptCheckpointIsolation(t *testing.T) {
	j := recoveryJob(t, []string{"true"})
	for _, dir := range []string{"attempt-1", "attempt-2", "attempt-2/nested"} {
		if err := os.MkdirAll(filepath.Join(j.Checkpoint, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	first := filepath.Join(j.Checkpoint, "attempt-1", "saved")
	latest := filepath.Join(j.Checkpoint, "attempt-2", "nested", "saved")
	if err := os.WriteFile(first, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(latest, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Unix(100, 0)}
	older := clock.Now()
	clock.Advance(100*time.Second + 123*time.Nanosecond)
	newer := clock.Now()
	os.Chtimes(first, older, older)
	os.Chtimes(latest, newer, newer)
	os.WriteFile(filepath.Join(j.Checkpoint, "attempt-2", "partial.tmp"), []byte("incomplete"), 0600)
	j.Attempt = 3
	dest, err := prepareCheckpoint(j)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "nested", "saved"))
	if err != nil || string(data) != "new" {
		t.Fatalf("checkpoint %q: %v", data, err)
	}
	if _, err = os.Stat(filepath.Join(dest, "partial.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("copied incomplete checkpoint")
	}
	progress, err := checkpointProgress(dest)
	if err != nil || progress != 0 {
		t.Fatalf("progress %d: %v", progress, err)
	}
	if err = os.WriteFile(filepath.Join(dest, "nested", "saved"), []byte("attempt 3"), 0600); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(latest)
	if string(data) != "new" {
		t.Fatal("new attempt modified previous checkpoint")
	}
}
func TestWorkerMissingCheckpointRootNotCreated(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	root := filepath.Join(t.TempDir(), "missing-nas")
	j := recoveryJob(t, []string{"true"})
	j.Checkpoint = filepath.Join(root, "job")
	if _, err = w.start(s, j); !errors.Is(err, errWorkerCheckpoint) {
		t.Fatalf("missing root error: %v", err)
	}
	if _, err = os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("created absent NAS mount")
	}
}
func TestWorkerOrphanCleanupVerifiesIdentity(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.state.Session = "old"
	j := recoveryJob(t, []string{"sh", "-c", `sleep 30 & wait`})
	w = workerWithPreparedCheckpoint(t, w, s, j)
	p, err := w.start(s, j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.stop(w.Clock.Now(), 0, true)
	identity := s.state.Identity
	s.state.Identity = "wrong-identity"
	if err = s.cleanupOrphan(w.Clock); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
		t.Fatal("killed process with mismatching identity")
	default:
	}
	s.state.Identity = identity
	if err = s.cleanupOrphan(w.Clock); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(testWaitTimeout):
		t.Fatal("orphan group was not killed")
	}
}
func TestWorkerPersistedResultReconcilesThenReplaysBeforeFresh(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	s.state.Session = "old-session"
	j := recoveryJob(t, []string{"sh", "-c", "echo recovered"})
	w = workerWithPreparedCheckpoint(t, w, s, j)
	p, err := w.start(s, j)
	if err != nil {
		t.Fatal(err)
	}
	f := <-p.done
	if f.err != nil {
		t.Fatal(f.err)
	}
	// Leave state.json in its pre-completion form, as if Run died during HTTP.
	s.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var calls []string
	var total string
	var uploadedSession string
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/workers/checkin":
			var c Checkin
			json.NewDecoder(r.Body).Decode(&c)
			if c.Running != nil {
				calls = append(calls, "old-checkin")
				if c.Machine.Session != "old-session" || c.Fresh {
					t.Error("result reconciled under a fresh session")
				}
			} else {
				calls = append(calls, "fresh")
				if len(calls) == 4 && (!c.Fresh || c.Machine.Session == "old-session") {
					t.Error("did not switch to fresh session after replay")
				}
				cancel()
			}
			json.NewEncoder(rw).Encode(Reply{CheckpointRoot: filepath.Dir(j.Checkpoint)})
		case "/api/workers/log":
			var l LogChunk
			json.NewDecoder(r.Body).Decode(&l)
			total += l.Text
			calls = append(calls, "log")
			if l.Offset != 1 {
				t.Errorf("first dedup offset %d", l.Offset)
			}
			rw.Write([]byte(`{}`))
		case "/api/workers/complete":
			var result Result
			json.NewDecoder(r.Body).Decode(&result)
			uploadedSession = result.Session
			calls = append(calls, "complete")
			rw.Write([]byte(`{}`))
		default:
			rw.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	w.Client.URL = server.URL
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("worker did not replay saved completion")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) < 4 || strings.Join(calls[:4], ",") != "old-checkin,log,complete,fresh" {
		t.Fatalf("replay order: %v", calls)
	}
	if total != "recovered\n" || uploadedSession != "old-session" {
		t.Fatalf("replay text %q session %s", total, uploadedSession)
	}
}

func TestWorkerReplayRetriesIdenticalChunkAfterLostAck(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	j := recoveryJob(t, []string{"true"})
	s.state.Job = &j
	s.state.Session = "session"
	s.state.Log = "logs"
	os.WriteFile(filepath.Join(s.dir, s.state.Log), []byte("first"), 0600)
	var count atomic.Int32
	var chunks []LogChunk
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var l LogChunk
		json.NewDecoder(r.Body).Decode(&l)
		chunks = append(chunks, l)
		if count.Add(1) == 1 {
			http.Error(rw, "ack lost", 503)
			return
		}
		rw.Write([]byte(`{}`))
	}))
	defer server.Close()
	w.Client.URL = server.URL
	if err = w.replay(context.Background(), s); err == nil {
		t.Fatal("missing replay failure")
	}
	f, _ := os.OpenFile(filepath.Join(s.dir, s.state.Log), os.O_APPEND|os.O_WRONLY, 0600)
	io.WriteString(f, "second")
	f.Close()
	if err = w.replay(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 || chunks[0].Text != "first" || chunks[1].Text != "first" || chunks[1].Offset != chunks[0].Offset || chunks[2].Offset != 6 {
		t.Fatalf("chunks: %+v", chunks)
	}
}

func TestWorkerCleanupDoesNotKillReusedPID(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.state.PID = cmd.Process.Pid
	s.state.Identity = "previous-execution"
	if err = s.cleanupOrphan(w.Clock); err != nil {
		t.Fatal(err)
	}
	if err = syscall.Kill(cmd.Process.Pid, 0); err != nil {
		t.Fatal("cleanup killed unrelated reused PID")
	}
}

func TestWorkerRelativeCommandUsesJobWorkdir(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.state.Session = "relative"
	dir := t.TempDir()
	if err = os.WriteFile(filepath.Join(dir, "run.sh"), []byte("#!/bin/sh\necho relative-command\n"), 0700); err != nil {
		t.Fatal(err)
	}
	j := recoveryJob(t, []string{"./run.sh"})
	j.Spec.Run.Workdir = dir
	w = workerWithPreparedCheckpoint(t, w, s, j)
	p, err := w.start(s, j)
	if err != nil {
		t.Fatal(err)
	}
	finish := <-p.done
	if finish.err != nil || !finish.result.Success {
		t.Fatalf("finish %+v", finish)
	}
	data, err := os.ReadFile(filepath.Join(s.dir, s.state.Log))
	if err != nil || string(data) != "relative-command\n" {
		t.Fatalf("output %q: %v", data, err)
	}
}
func TestWorkerLegacyRootCheckpointAndSymlink(t *testing.T) {
	j := recoveryJob(t, []string{"true"})
	os.MkdirAll(j.Checkpoint, 0700)
	legacy := filepath.Join(j.Checkpoint, "saved")
	os.WriteFile(legacy, []byte("legacy"), 0600)
	os.WriteFile(filepath.Join(j.Checkpoint, "partial.tmp"), []byte("partial"), 0600)
	dest, err := prepareCheckpoint(j)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "saved"))
	if err != nil || string(data) != "legacy" {
		t.Fatalf("legacy checkpoint %q: %v", data, err)
	}
	if _, err = os.Stat(filepath.Join(dest, "attempt-1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("legacy copy recursively copied attempt directories")
	}
	if err = os.Symlink(legacy, filepath.Join(dest, "link")); err != nil {
		t.Fatal(err)
	}
	j.Attempt = 2
	if _, err = prepareCheckpoint(j); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink checkpoint should fail visibly: %v", err)
	}
}
func TestWorkerRecoveryCrashHelper(t *testing.T) {
	if os.Getenv("MYCE_RECOVERY_TEST_HELPER") != "1" {
		return
	}
	w := Worker{Client: Client{URL: os.Getenv("MYCE_RECOVERY_TEST_URL"), Token: "test"}, Machine: machine("crash-worker", "fake"), StateDir: os.Getenv("MYCE_RECOVERY_TEST_STATE"), Interval: 5 * time.Millisecond}
	if err := w.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestWorkerRestartCleansOrphanBeforeReassignment(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `trap 'exit 0' TERM; echo ready-$MYCE_ATTEMPT; while :; do sleep 0.05; done`}
	j := submit(t, q, s)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	stateDir := t.TempDir()
	helper := exec.Command(os.Args[0], "-test.run=^TestWorkerRecoveryCrashHelper$")
	helper.Env = append(os.Environ(), "MYCE_RECOVERY_TEST_HELPER=1", "MYCE_RECOVERY_TEST_URL="+server.URL, "MYCE_RECOVERY_TEST_STATE="+stateDir)
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = helper.Process.Kill()
		_ = helper.Wait()
		// A readiness assertion can fail after the helper has spawned its
		// command. Clean only its recorded, identity-verified owned group.
		store, err := (Worker{StateDir: stateDir}).openStore()
		if err != nil {
			t.Error("open crash-helper cleanup state:", err)
			return
		}
		defer store.close()
		if err := store.cleanupOrphan(RealClock{}); err != nil {
			t.Error("crash-helper orphan cleanup:", err)
		}
	}()
	await(t, func() bool {
		ls, _ := q.Logs(j.ID, 0)
		for _, l := range ls {
			if strings.Contains(l.Text, "ready-1") {
				return true
			}
		}
		return false
	})
	data, err := os.ReadFile(filepath.Join(stateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var before workerState
	json.Unmarshal(data, &before)
	if err = helper.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	helper.Wait()
	members, err := workerGroupMembers(before.PID)
	if err != nil || len(members) == 0 {
		t.Fatalf("test did not leave orphan group: %v %v", members, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := Worker{Client: Client{URL: server.URL, Token: "test"}, Machine: machine("crash-worker", "fake"), StateDir: stateDir, Interval: 5 * time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	await(t, func() bool { return job(t, q, j.ID).Attempt == 2 && job(t, q, j.ID).State == "running" })
	members, err = workerGroupMembers(before.PID)
	if err != nil || len(members) != 0 {
		t.Fatalf("new command assigned before old group stopped: %v %v", members, err)
	}
	// Assignment precedes payload startup; wait for its TERM handler before leaving.
	await(t, func() bool {
		logs, _ := q.Logs(j.ID, 0)
		for _, line := range logs {
			if line.Attempt == 2 && strings.Contains(line.Text, "ready-2") {
				return true
			}
		}
		return false
	})
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("restarted worker failed to leave")
	}
}

func TestSignalExitedGroupPermissionRace(t *testing.T) {
	for _, tc := range []struct {
		name       string
		signalErr  error
		members    []int
		inspectErr error
		wantErr    error
	}{
		{name: "success"},
		{name: "already gone", signalErr: syscall.ESRCH},
		{name: "exiting group permission race", signalErr: syscall.EPERM},
		{name: "live group permission refused", signalErr: syscall.EPERM, members: []int{42}, wantErr: syscall.EPERM},
		{name: "inventory failure refuses cleanup", signalErr: syscall.EPERM, inspectErr: errors.New("inventory unavailable"), wantErr: syscall.EPERM},
		{name: "other signal error", signalErr: syscall.EINVAL, wantErr: syscall.EINVAL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inspected := false
			err := signalWorkerGroupUsing(42, syscall.SIGKILL, func(group int, signal syscall.Signal) error {
				if group != -42 || signal != syscall.SIGKILL {
					t.Fatal("wrong group/signal", group, signal)
				}
				return tc.signalErr
			}, func(group int) ([]int, error) {
				inspected = true
				if group != 42 {
					t.Fatal("wrong inventory group", group)
				}
				return tc.members, tc.inspectErr
			})
			if !errors.Is(err, tc.wantErr) {
				t.Fatal("signal result", err, "want", tc.wantErr)
			}
			if inspected != errors.Is(tc.signalErr, syscall.EPERM) {
				t.Fatal("unexpected inventory decision")
			}
		})
	}
}
