package processidentity

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/ports"
)

func TestExitedAndIsExited(t *testing.T) {
	exited, err := Exited(0)
	if err != nil || !exited {
		t.Fatalf("Exited(0) = %t %v", exited, err)
	}
	exited, err = ExitedBySignal(os.Getpid())
	if err != nil || exited {
		t.Fatalf("ExitedBySignal(live) = %t %v", exited, err)
	}
	if !IsExited(ErrExited) || !IsExited(errors.Join(errors.New("wrapped"), ErrExited)) {
		t.Fatal("ErrExited was not recognized")
	}
	if IsExited(errors.New("other")) {
		t.Fatal("unrelated error recognized as exited")
	}
}

func TestVerifyLiveChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process identity test uses POSIX process groups")
	}
	cmd := exec.Command("/bin/sleep", "10")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	pid := cmd.Process.Pid
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		t.Fatalf("Getpgid: %v", err)
	}
	err = Verify(ports.Handle{
		PID:       pid,
		PGID:      pgid,
		Kind:      "test",
		Ref:       "sleep",
		Binary:    "/bin/sleep",
		Args:      []string{"10"},
		StartedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if err := Verify(ports.Handle{
		PID:    pid,
		PGID:   pgid,
		Kind:   "test",
		Ref:    "sleep",
		Binary: "/bin/sleep",
		Args:   []string{"10"},
	}); err == nil || !strings.Contains(err.Error(), "missing start time") {
		t.Fatalf("missing start err = %v", err)
	}
}

func TestExitedDetectsReapedProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signal test uses POSIX processes")
	}
	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Wait()
	exited, err := Exited(pid)
	if err != nil || !exited {
		t.Fatalf("Exited(reaped) = %t %v", exited, err)
	}
	exited, err = ExitedBySignal(pid)
	if err != nil && !IsExited(err) {
		t.Fatalf("ExitedBySignal err = %v", err)
	}
	if !exited && err == nil {
		t.Fatal("reaped process was still reported live")
	}
	if _, _, err := psField(pid, "command="); err != nil && !IsExited(err) {
		t.Fatalf("psField reaped err = %v", err)
	}
	if err := Verify(ports.Handle{PID: pid, Binary: "/bin/sh", Args: []string{"-c", "exit 0"}, StartedAt: time.Now().UTC()}); err != nil && !IsExited(err) {
		t.Fatalf("Verify reaped err = %v", err)
	}
}

func TestExitedDetectsZombieProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signal test uses POSIX processes")
	}
	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	defer func() { _, _ = cmd.Process.Wait() }()
	pid := cmd.Process.Pid
	var exited bool
	var err error
	for i := 0; i < 1000; i++ {
		exited, err = Exited(pid)
		if err != nil || exited {
			break
		}
		runtime.Gosched()
	}
	if err != nil || !exited {
		t.Fatalf("Exited(zombie) = %t %v", exited, err)
	}
	if err := Verify(ports.Handle{PID: pid, Binary: "/bin/sh", Args: []string{"-c", "exit 0"}, StartedAt: time.Now().UTC()}); !IsExited(err) {
		t.Fatalf("Verify zombie err = %v", err)
	}
}

func TestVerifyRejectsMissingOrChangedIdentity(t *testing.T) {
	selfPID := os.Getpid()
	selfPGID, err := syscall.Getpgid(selfPID)
	if err != nil {
		t.Fatalf("Getpgid self: %v", err)
	}
	if err := Verify(ports.Handle{}); err == nil || !strings.Contains(err.Error(), "pid is required") {
		t.Fatalf("pid err = %v", err)
	}
	if err := Verify(ports.Handle{PID: selfPID}); err == nil || !strings.Contains(err.Error(), "missing binary") {
		t.Fatalf("missing binary err = %v", err)
	}
	if err := Verify(ports.Handle{PID: selfPID, PGID: selfPGID + 1}); err == nil || !strings.Contains(err.Error(), "pgid changed") {
		t.Fatalf("pgid err = %v", err)
	}
	if err := Verify(ports.Handle{PID: selfPID, PGID: selfPGID, Binary: "/definitely/not/codex-test", Args: []string{"nope"}, StartedAt: time.Now().UTC()}); err == nil || !strings.Contains(err.Error(), "binary mismatch") {
		t.Fatalf("binary err = %v", err)
	}
	if err := Verify(ports.Handle{PID: selfPID, PGID: selfPGID, Binary: os.Args[0], Args: []string{"definitely-not-present"}, StartedAt: time.Now().UTC()}); err == nil || !strings.Contains(err.Error(), "argv mismatch") {
		t.Fatalf("argv err = %v", err)
	}
}

func TestVerifyCommandRejectsMalformedStoredIdentity(t *testing.T) {
	if err := verifyCommand(ports.Handle{PID: 10, Binary: "/bin/sleep", Args: []string{"10"}}, ""); err == nil || !strings.Contains(err.Error(), "command line is empty") {
		t.Fatalf("empty command err = %v", err)
	}
	if err := verifyCommand(ports.Handle{PID: 10, Binary: "/bin/sleep"}, "/bin/sleep 10"); err == nil || !strings.Contains(err.Error(), "missing argv") {
		t.Fatalf("missing argv err = %v", err)
	}
	if err := verifyCommand(ports.Handle{PID: 10, Binary: "sleep", Args: []string{"10"}}, "/bin/sleep 10"); err != nil {
		t.Fatalf("base binary match err = %v", err)
	}
}

func TestArgsMatch(t *testing.T) {
	if !argsMatch([]string{"prefix", "--model", "m", "suffix"}, []string{"--model", "m"}) {
		t.Fatal("expected subsequence args match")
	}
	if argsMatch([]string{"--model"}, []string{"--model", "m"}) {
		t.Fatal("short live args matched")
	}
	if argsMatch([]string{"--ctx", "1"}, []string{"--model", "m"}) {
		t.Fatal("unrelated args matched")
	}
}

func TestVerifyStartedAt(t *testing.T) {
	live := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	if err := verifyStartedAt(ports.Handle{PID: 10}, live); err == nil || !strings.Contains(err.Error(), "missing start time") {
		t.Fatalf("missing start err = %v", err)
	}
	if err := verifyStartedAt(ports.Handle{PID: 10, StartedAt: live.Add(11 * time.Second)}, live); err == nil || !strings.Contains(err.Error(), "start time mismatch") {
		t.Fatalf("mismatch err = %v", err)
	}
	if err := verifyStartedAt(ports.Handle{PID: 10, StartedAt: live.Add(-2 * time.Second)}, live); err != nil {
		t.Fatalf("startedAt err = %v", err)
	}
}

func TestPSHelpersForLiveAndMissingProcesses(t *testing.T) {
	if value, ok, err := psField(os.Getpid(), "command="); err != nil || !ok || value == "" {
		t.Fatalf("psField live value=%q ok=%t err=%v", value, ok, err)
	}
	started, ok, err := psStartedAt(os.Getpid())
	if err != nil || !ok || started.IsZero() {
		t.Fatalf("psStartedAt live started=%s ok=%t err=%v", started, ok, err)
	}
	if value, ok, err := psField(99999999, "command="); err != nil && !IsExited(err) || ok || value != "" {
		t.Fatalf("psField missing value=%q ok=%t err=%v", value, ok, err)
	}
	if started, ok, err := psStartedAt(99999999); err != nil && !IsExited(err) || ok || !started.IsZero() {
		t.Fatalf("psStartedAt missing started=%s ok=%t err=%v", started, ok, err)
	}
	if err := Verify(ports.Handle{PID: 99999999}); !IsExited(err) {
		t.Fatalf("Verify missing process err = %v", err)
	}
	if exited, err := ExitedBySignal(-1); err == nil && exited {
		t.Fatalf("negative pid reported exited without error: %v", err)
	}
}
