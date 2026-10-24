package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type commandScope struct {
	StateDir string
	Clock    Clock
	Stderr   io.Writer // nil combines diagnostics with stdout
}

func (s *commandScope) prepare() error {
	if s.Clock == nil {
		s.Clock = RealClock{}
	}
	if s.StateDir == "" {
		return errors.New("command state directory required")
	}
	return os.MkdirAll(s.StateDir, 0700)
}

// The ordinary build/extension command supervisor persists custody before
// releasing its payload gate. Cancellation and recovery retain exact process-group ownership.
func runRecordedCommand(ctx context.Context, argv []string, dir string, env []string, out io.Writer, scope commandScope, record string) error {
	if scope.Clock == nil {
		scope.Clock = RealClock{}
	}
	identity := ID()
	var stateMu sync.Mutex
	state := workerState{Identity: identity, LaunchPending: true}
	for _, entry := range env {
		if platform, ok := strings.CutPrefix(entry, "MYCE_PLATFORM="); ok && platform != "" && platform != "cpu" {
			state.Accelerator = true
		}
	}
	if e := saveWorkerJSON(scope.StateDir, record, state); e != nil {
		return e
	}
	args := []string{"-c", `read gate || exit 125; "$@" < /dev/null & child=$!; wait "$child"; exit $?`, "myce-supervisor-" + identity}
	args = append(args, argv...)
	cmd := exec.CommandContext(ctx, "sh", args...)
	cmd.Dir = dir
	cmd.Env = append(env, "MYCE_PROCESS_ID="+identity, "ZE_ENABLE_PCI_ID_DEVICE_ORDER=1")
	cmd.Stdout = out
	cmd.Stderr = out
	if scope.Stderr != nil {
		cmd.Stderr = scope.Stderr
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		stateMu.Lock()
		defer stateMu.Unlock()
		if cmd.Process != nil {
			if state.Accelerator {
				if err := gracefulRecoveredGroup(cmd.Process.Pid, scope.Clock, &state.StopAt, state.StopGrace, func() error {
					return saveWorkerJSON(scope.StateDir, record, state)
				}); err != nil {
					return err
				}
			}
			return signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = time.Second
	reader, writer, e := os.Pipe()
	if e != nil {
		return e
	}
	defer reader.Close()
	defer writer.Close()
	cmd.Stdin = reader
	if e = cmd.Start(); e != nil {
		return e
	}
	stateMu.Lock()
	state.PID = cmd.Process.Pid
	state.LaunchPending = false
	e = saveWorkerJSON(scope.StateDir, record, state)
	stateMu.Unlock()
	if e != nil {
		_ = syscall.Kill(-state.PID, syscall.SIGKILL)
		_ = cmd.Wait()
		return e
	}
	e = checkLaunchGate(ctx)
	if e == nil {
		_, e = writer.Write([]byte("start\n"))
	}
	if e != nil {
		_ = cmd.Cancel()
	}
	waitErr := cmd.Wait()
	if state.Accelerator {
		if err := gracefulRecoveredGroup(state.PID, scope.Clock, &state.StopAt, state.StopGrace, func() error {
			return saveWorkerJSON(scope.StateDir, record, state)
		}); err != nil {
			return errors.Join(e, waitErr, err)
		}
	}
	_ = syscall.Kill(-state.PID, syscall.SIGKILL)
	groupErr := waitWorkerGroup(state.PID, scope.Clock)
	if groupErr == nil {
		if err := os.Remove(filepath.Join(scope.StateDir, record)); err != nil && !errors.Is(err, os.ErrNotExist) {
			groupErr = err
		} else {
			groupErr = syncWorkerDir(scope.StateDir)
		}
	}
	return errors.Join(e, waitErr, groupErr)
}
