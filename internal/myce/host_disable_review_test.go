package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDisableFindsMarkedDetachedGroupAfterLeaderExit(t *testing.T) {
	for _, helper := range []bool{false, true} {
		name := "system-command"
		if helper {
			name = "visible-environment"
		}
		t.Run(name, func(t *testing.T) { testDisableDetachedGroup(t, helper) })
	}
}

func TestDisableDetachedMarkerHelper(t *testing.T) {
	if os.Getenv("MYCE_DETACHED_TEST_HELPER") != "1" {
		return
	}
	fmt.Println("ready")
	time.Sleep(time.Minute)
}

func testDisableDetachedGroup(t *testing.T, helper bool) {
	t.Helper()
	root, state := t.TempDir(), t.TempDir()
	identity := ID()
	cmd := exec.Command("sleep", "60")
	if helper {
		cmd = exec.Command(os.Args[0], "-test.run=^TestDisableDetachedMarkerHelper$")
	}
	cmd.Env = []string{"MYCE_PROCESS_ID=" + identity}
	if helper {
		cmd.Env = append(cmd.Env, "MYCE_DETACHED_TEST_HELPER=1")
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	if helper {
		ready := make([]byte, len("ready\n"))
		if _, err := io.ReadFull(stdout, ready); err != nil || string(ready) != "ready\n" {
			t.Fatalf("helper readiness: %q %v", ready, err)
		}
	}
	// A child used setsid; its original group no longer exists.
	if err := saveWorkerJSON(root, "worker-test.json", HostWorker{State: state}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(state, "attempt-test.json", workerState{PID: 99999999, Identity: identity}); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Dir: root}
	if helper {
		groups, err := ownedRecordGroups(workerState{PID: 99999999, Identity: identity})
		if err != nil || len(groups) != 1 || groups[0] != cmd.Process.Pid {
			t.Fatalf("detached environment marker not discovered: %v %v", groups, err)
		}
	}
	r, err := h.Disable(context.Background(), "detached child", 0)
	requireNativeMarkerDisableReport(t, h, r, err)
	birth, err := processBirth(cmd.Process.Pid)
	if err == nil && birth != "" {
		// Restricted system binaries can have hidden environments on macOS.
		// They must be explicitly unconfirmed, never silently accepted.
		if helper || runtime.GOOS != "darwin" {
			t.Fatalf("disable left a discoverable detached child alive: %+v", r)
		}
	}
	if (helper || runtime.GOOS != "darwin") && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("detached child exit not verified: %q %v", birth, err)
	}
	if helper && r.Stopped != 1 {
		t.Fatalf("detached group was not stopped: %+v", r)
	}
}

func requireNativeMarkerDisableReport(t *testing.T, h HostControl, r DisableReport, err error) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if err == nil || r.Confirmed || !r.BestEffort || !strings.Contains(strings.Join(r.Unconfirmed, "; "), "process environment markers") {
		t.Fatalf("hidden macOS descendants not explicitly unconfirmed: %+v %v", r, err)
	}
	if reason, err := HostDisabled(h.Dir); err != nil || reason == "" {
		t.Fatalf("unconfirmed cleanup lost disable flag: %q %v", reason, err)
	}
	if _, err := os.Stat(filepath.Join(h.Dir, "disable-receipt.json")); !os.IsNotExist(err) {
		t.Fatalf("unconfirmed cleanup got receipt: %v", err)
	}
	if err := h.Enable(context.Background()); err == nil {
		t.Fatal("unconfirmed cleanup allowed enable")
	}
	if reason, err := HostDisabled(h.Dir); err != nil || reason == "" {
		t.Fatalf("refused enable lost disable flag: %q %v", reason, err)
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
	// Keep this signaling test independent of the shared host's ps latency.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ps"), []byte(fmt.Sprintf("#!/bin/sh\necho '%d %d S'\n", cmd.Process.Pid, cmd.Process.Pid)), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
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
