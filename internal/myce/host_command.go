package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

type hostCommandScope struct {
	HostDir, StateDir string
	Revision          HostRevision
	Clock             Clock
}

func hostCommandDirectory(dir string) string { return filepath.Join(dir, "local-commands") }

func (s *hostCommandScope) prepare() error {
	var err error
	s.HostDir, err = hostRoot(s.HostDir)
	if err != nil {
		return err
	}
	if s.Clock == nil {
		s.Clock = RealClock{}
	}
	if err = bindHostRevision(s.HostDir, &s.Revision); err != nil {
		return err
	}
	// Standalone commands have host custody, not worker membership or a queue.
	s.StateDir, err = filepath.Abs(hostCommandDirectory(s.HostDir))
	if err != nil {
		return err
	}
	if err = os.MkdirAll(s.StateDir, 0700); err != nil {
		return err
	}
	return nil
}

// The ordinary build/extension command supervisor persists custody before
// releasing its payload gate. Disable can find the group even without systemd.
func runRecordedHostCommand(ctx context.Context, argv []string, dir string, env []string, out io.Writer, scope hostCommandScope, record string) error {
	if scope.Clock == nil {
		scope.Clock = RealClock{}
	}
	revision := scope.Revision
	if err := bindHostRevision(scope.HostDir, &revision); err != nil {
		return err
	}
	unlock, err := hostLaunchGate(scope.HostDir, revision)
	if err != nil {
		return err
	}
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	identity := ID()
	state := workerState{Identity: identity, LaunchPending: true}
	if e := saveWorkerJSON(scope.StateDir, record, state); e != nil {
		return e
	}
	args := []string{"-c", `read gate || exit 125; "$@" < /dev/null & child=$!; wait "$child"; exit $?`, "myce-supervisor-" + identity}
	args = append(args, argv...)
	cmd := exec.CommandContext(ctx, "sh", args...)
	cmd.Dir = dir
	cmd.Env = append(env, "MYCE_PROCESS_ID="+identity)
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
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
	if e = hostDispatch(scope.HostDir, revision, cmd.Start); e != nil {
		return e
	}
	state.PID = cmd.Process.Pid
	state.LaunchPending = false
	if e = saveWorkerJSON(scope.StateDir, record, state); e != nil {
		_ = syscall.Kill(-state.PID, syscall.SIGKILL)
		_ = cmd.Wait()
		return e
	}
	if e = hostDispatch(scope.HostDir, revision, func() error { _, e := writer.Write([]byte("start\n")); return e }); e != nil {
		_ = syscall.Kill(-state.PID, syscall.SIGKILL)
	}
	unlock()
	unlock = nil
	waitErr := cmd.Wait()
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
