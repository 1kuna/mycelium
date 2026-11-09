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

func recoveryWorker(t *testing.T) Worker {
	t.Helper()
	handler := workerTestHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "start_gate": startGateProtocol})
	}))
	// A real listener prevents synctest's virtual time from advancing.
	client := &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "worker-test.invalid" {
			return http.DefaultTransport.RoundTrip(r)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Result(), nil
	})}
	return Worker{EnvironmentLimit: 1 << 20, Client: Client{URL: "http://worker-test.invalid", HTTP: client}, Machine: machine("recovery-worker", "fake"), StateDir: newTestStateRoot(t), Interval: time.Millisecond, Clock: RealClock{}}
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
	s.close()
	if err = os.WriteFile(filepath.Join(w.StateDir, "state.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = w.openStore(); err == nil {
		t.Fatal("corrupt state silently discarded")
	}
	// A syntactically valid document from another state format is not ours
	// to initialize or replace, even when startup refuses it.
	data := `{"state":"failed","pid":null,"assigned_xpu":1,"preemption_count":0,"attempt_token":"synthetic-old-attempt"}`
	path := filepath.Join(w.StateDir, "state.json")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := w.openStore()
	if err == nil {
		store.close()
		t.Fatal("foreign or incomplete state accepted")
	}
	if !strings.Contains(err.Error(), "read worker state") {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != data {
		t.Fatalf("rejected state changed: %v", err)
	}
}

func TestWorkerOfflineLargeOutputIsDurable(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.state.Session = "offline-session"
	j := recoveryJob(t, []string{"sh", "-c", `head -c 4194304 /dev/zero; echo done`})
	w = workerWithPreparedCheckpoint(t, w, s, j)
	p, err := startAfterFixtureInputs(t, w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	// The current protocol requires an online Started acknowledgement. Losing
	// the queue after that handoff must not block output or result durability.
	w.Client.URL = "http://127.0.0.1:1"
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

func TestWorkerMissingCheckpointRootNotCreated(t *testing.T) {
	w := recoveryWorker(t)
	w.checkpointPrepare = prepareCheckpoint
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	root := filepath.Join(t.TempDir(), "missing-nas")
	j := recoveryJob(t, []string{"true"})
	j.Checkpoint = filepath.Join(root, "job")
	if _, err = startAfterFixtureInputs(t, w, s, j); !errors.Is(err, errWorkerCheckpoint) {
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
	p, err := startAfterFixtureInputs(t, w, s, j)
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
	p, err := startAfterFixtureInputs(t, w, s, j)
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
	var freshChecked bool
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/workers/checkin":
			var c Checkin
			json.NewDecoder(r.Body).Decode(&c)
			if c.AttemptProtocol != keyedAttemptProtocol || c.Running != nil {
				t.Error("worker did not use keyed checkin")
			}
			if len(c.Attempts) != 0 {
				calls = append(calls, "old-checkin")
				if len(c.Attempts) != 1 || c.Attempts[0].ID != j.ID || c.Attempts[0].Attempt != j.Attempt || c.Attempts[0].Session != "old-session" {
					t.Error("saved completion lost its execution key")
				}
				if c.Machine.Session != "old-session" || c.Fresh {
					t.Error("result reconciled under a fresh session")
				}
			} else {
				if !freshChecked {
					if !c.Fresh || c.Machine.Session == "old-session" {
						t.Error("did not switch to fresh session after replay")
					}
					freshChecked = true
				}
				calls = append(calls, "fresh")
				cancel()
			}
			json.NewEncoder(rw).Encode(Reply{AttemptProtocol: keyedAttemptProtocol, CheckpointRoot: filepath.Dir(j.Checkpoint)})
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
	// Uploads are independent of the machine heartbeat. Repeated reports keep
	// exact old-session custody; only the replay/session transition is ordered.
	ordered := []string{}
	for i, call := range calls {
		if i == 0 || call != "old-checkin" {
			ordered = append(ordered, call)
		}
	}
	if len(ordered) < 4 || strings.Join(ordered[:4], ",") != "old-checkin,log,complete,fresh" {
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
	w.JobFileRoot = dir
	j.Spec.Inputs = map[string]string{"run.sh": "run.sh"}
	if err = w.prepareExtensionFiles(&j, s.dir); err != nil {
		t.Fatal(err)
	}
	// Input transport deliberately does not preserve executable permissions.
	// Make the fixture executable before testing relative command resolution.
	if err = os.Chmod(filepath.Join(j.Spec.Run.Workdir, "run.sh"), 0700); err != nil {
		t.Fatal(err)
	}
	j.Spec.Run.Workdir = "{work}"
	w = workerWithPreparedCheckpoint(t, w, s, j)
	p, err := startAfterFixtureInputs(t, w, s, j)
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

func TestWorkerRestartCleansOrphanBeforeReassignment(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `trap 'exit 0' TERM; echo ready-$MYCE_ATTEMPT; while :; do sleep 0.05; done`}
	j := submit(t, q, s)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	stateDir := t.TempDir()
	helper := exec.Command(os.Args[0], "-test.run=^TestNativeGroupWorkerCrashHelper$")
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
	data, err := readSingleAttemptState(filepath.Join(stateDir, "state.json"))
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
	w := Worker{EnvironmentLimit: 1 << 20, Client: Client{URL: server.URL, Token: "test"}, Machine: machine("crash-worker", "fake"), StateDir: stateDir, Interval: 5 * time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		if t.Failed() {
			got := job(t, q, j.ID)
			t.Logf("restarted queue job: state=%s attempt=%d losses=%d machine=%q reason=%q release_confirmed=%t execution_cause=%+v", got.State, got.Attempt, got.Losses, got.Machine, got.Reason, got.ReleaseConfirmed, got.ExecutionCause)
		}
	})
	await(t, func() bool {
		select {
		case err := <-done:
			t.Fatalf("restarted worker exited before reassignment: %v", err)
		default:
		}
		got := job(t, q, j.ID)
		return got.Attempt == 2 && got.State == "running"
	})
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
