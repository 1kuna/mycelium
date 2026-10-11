package myce

import (
	"context"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDisableFindsMarkedDetachedGroupAfterLeaderExit(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	identity := ID()
	cmd := exec.Command("sleep", "60")
	cmd.Env = append(os.Environ(), "MYCE_PROCESS_ID="+identity)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	// A child used setsid; its original group no longer exists.
	if err := saveWorkerJSON(root, "worker-test.json", HostWorker{State: state}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(state, "attempt-test.json", workerState{PID: 99999999, Identity: identity}); err != nil {
		t.Fatal(err)
	}
	r, err := (HostControl{Dir: root}).Disable(context.Background(), "detached child", 0)
	if err != nil {
		t.Fatal(err)
	}
	birth, err := processBirth(cmd.Process.Pid)
	if err == nil && birth != "" {
		t.Fatalf("disable certified cleanup with detached child alive: %+v", r)
	}
}

func TestDisableRefusesActiveLegacySpool(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	store, err := (Worker{StateDir: state}).openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err = saveWorkerJSON(root, "worker-test.json", HostWorker{State: state}); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Unix(1000, 0)}
	r, err := (HostControl{Dir: root, Clock: clock, Processes: HostProcesses{Pause: func(d time.Duration) { clock.Advance(d) }}}).Disable(context.Background(), "old worker", 0)
	if err == nil || len(r.Unconfirmed) == 0 {
		t.Fatalf("active legacy worker certified: %+v %v", r, err)
	}
	if _, err := os.Stat(filepath.Join(root, "disable-receipt.json")); !os.IsNotExist(err) {
		t.Fatalf("unconfirmed cleanup got receipt: %v", err)
	}
	if reason, err := HostDisabled(root); err != nil || reason == "" {
		t.Fatalf("flag absent: %q %v", reason, err)
	}
}

func TestReconcileEnableCannotClearFreshDisable(t *testing.T) {
	root := t.TempDir()
	saveWorkerJSON(root, "worker-test.json", HostWorker{State: t.TempDir(), Host: "test"})
	if err := saveWorkerJSON(root, "enable-pending.json", struct {
		Workers []HostWorker `json:"workers"`
	}{[]HostWorker{{Host: "test"}}}); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Dir: root, Notify: func(_ context.Context, _ HostWorker, enable, cleared bool, _ string) error {
		if enable {
			t.Error("stale enable receipt cleared fresh disable")
		}
		return nil
	}}
	if _, err := h.Disable(context.Background(), "new disable", 0); err != nil {
		t.Fatal(err)
	}
	if err := h.ReconcileEnable(context.Background()); err == nil {
		t.Error("reconciled enable while disabled")
	}
}

func TestReconcileEnableDoesNotWaitOnBusyLock(t *testing.T) {
	root := t.TempDir()
	unlock, err := hostOperationLock(root, "reconcile.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	// Bounded context is not honored by blocking flock; use a subprocess-free
	// release to avoid hanging this regression on the broken implementation.
	done := make(chan error, 1)
	go func() { done <- (HostControl{Dir: root}).ReconcileEnable(context.Background()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("busy reconciliation succeeded")
		}
	case <-time.After(100 * time.Millisecond):
		unlock()
		<-done
		t.Fatal("reconciliation blocked on flock")
	}
}

func TestHostPSDeadlineIncludesInheritedOutputPipe(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ps"), []byte("#!/bin/sh\nsleep 2\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	start := time.Now()
	if _, err := hostPS(); err == nil {
		t.Fatal("hung ps succeeded")
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("ps deadline exceeded: %s", elapsed)
	}
}

func TestDisableRejectsRecordWithoutIdentityEvenWhenPIDIsGone(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	if err := saveWorkerJSON(root, "worker-test.json", HostWorker{State: state}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(state, "state.json", workerState{PID: 99999999}); err != nil {
		t.Fatal(err)
	}
	if _, err := (HostControl{Dir: root}).Disable(context.Background(), "old record", 0); err == nil {
		t.Fatal("identity-free legacy record certified")
	}
}

func TestHostSignalSkipsVanishedGroupsIndependently(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	birth, err := processBirth(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	st := workerState{Groups: map[int]map[int]string{cmd.Process.Pid: {cmd.Process.Pid: birth}, 99999999: {}}}
	var signaled []int
	if err := signalHostProcessUsing(st, syscall.SIGTERM, func(group int, _ syscall.Signal) error { signaled = append(signaled, group); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(signaled) != 1 || signaled[0] != cmd.Process.Pid {
		t.Fatalf("signal authorized by another group: %v", signaled)
	}
}

func TestDisableFencesLaunchRaceAndRepeatedControls(t *testing.T) {
	root := t.TempDir()
	saveWorkerJSON(root, "worker-test.json", HostWorker{State: t.TempDir(), Host: "test"})
	unlock, err := hostOperationLock(root, "launch.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	h := HostControl{Dir: root, Clock: clock, Processes: HostProcesses{Pause: func(d time.Duration) { clock.Advance(d) }}}
	if _, err := h.Disable(context.Background(), "assignment race", 0); err == nil {
		t.Fatal("held launch gate certified")
	}
	if err := h.Enable(context.Background()); err == nil {
		t.Fatal("enabled during unconfirmed launch")
	}
	unlock()
	if gate, err := hostLaunchGate(root); err == nil {
		gate()
		t.Fatal("assigned job admitted after disable")
	}
	for i := 0; i < 2; i++ {
		if _, err := h.Disable(context.Background(), "repeat", 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	gate, err := hostLaunchGate(root)
	if err != nil {
		t.Fatal(err)
	}
	gate()
}

func TestDisabledRuntimeRefusesAlreadyRoutedModelCall(t *testing.T) {
	root := t.TempDir()
	rt := &modelRuntime{hostStateDir: root, models: map[string]*workerModel{"test": {model: Model{Name: "test"}, engine: &FakeEngine{Model: "test"}, state: "ready"}}, session: "session", token: "test"}
	if err := saveWorkerJSON(root, "DISABLED", map[string]string{"reason": "disabled"}); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Session", "session")
	response := httptest.NewRecorder()
	rt.serve(response, req)
	if response.Code != 503 {
		t.Fatalf("disabled host admitted inference: %d", response.Code)
	}
}

func TestDisabledRuntimeRefusesCallWhenFlagAppearsDuringBodyRead(t *testing.T) {
	root := t.TempDir()
	rt := &modelRuntime{hostStateDir: root, models: map[string]*workerModel{"test": {model: Model{Name: "test"}, engine: &FakeEngine{Model: "test"}, state: "ready"}}, session: "session", token: "test"}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	req := httptest.NewRequest("POST", "/v1/chat/completions", reader)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Session", "session")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { rt.serve(response, req); close(done) }()
	// Write returns only after serve has passed its initial flag check.
	if _, err := writer.Write([]byte(`{"model":`)); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(root, "DISABLED", map[string]string{"reason": "disabled"}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte(`"test"}`)); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("model admission did not finish")
	}
	if response.Code != 503 {
		t.Fatalf("body-read race admitted inference: %d", response.Code)
	}
}
