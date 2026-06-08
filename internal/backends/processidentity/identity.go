package processidentity

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mycelium/internal/ports"
)

var ErrExited = errors.New("process already exited")

func Exited(pid int) (bool, error) {
	if pid <= 0 {
		return true, nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false, err
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		return false, err
	}
	stat, ok, err := psField(pid, "stat=")
	if err != nil {
		if IsExited(err) {
			return true, nil
		}
		return false, err
	}
	if ok && strings.Contains(stat, "Z") {
		return true, nil
	}
	return false, nil
}

func Verify(handle ports.Handle) error {
	if handle.PID <= 0 {
		return fmt.Errorf("process pid is required")
	}
	exited, err := Exited(handle.PID)
	if err != nil {
		return err
	}
	if exited {
		return ErrExited
	}

	var verified bool
	if handle.PGID != 0 {
		pgid, err := syscall.Getpgid(handle.PID)
		if err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return ErrExited
			}
			return err
		}
		if pgid != handle.PGID {
			return fmt.Errorf("process %d pgid changed from %d to %d", handle.PID, handle.PGID, pgid)
		}
		verified = true
	}

	command, commandOK, err := psField(handle.PID, "command=")
	if err != nil {
		return err
	}
	if commandOK {
		if err := verifyCommand(handle, command); err != nil {
			return err
		}
		verified = true
	}

	started, startedOK, err := psStartedAt(handle.PID)
	if err != nil {
		return err
	}
	if startedOK {
		if err := verifyStartedAt(handle, started); err != nil {
			return err
		}
		verified = true
	}

	if !verified {
		return fmt.Errorf("process %d has no verifiable identity evidence: kind=%q ref=%q binary=%q args=%q started_at=%s", handle.PID, handle.Kind, handle.Ref, handle.Binary, strings.Join(handle.Args, " "), handle.StartedAt.Format(time.RFC3339Nano))
	}
	return nil
}

func IsExited(err error) bool {
	return errors.Is(err, ErrExited)
}

func verifyCommand(handle ports.Handle, command string) error {
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return fmt.Errorf("process %d command line is empty", handle.PID)
	}
	if handle.Binary == "" {
		return fmt.Errorf("process %d stored ref missing binary; live command=%q", handle.PID, command)
	}
	actualBinary := fields[0]
	if filepath.Base(actualBinary) != filepath.Base(handle.Binary) && actualBinary != handle.Binary {
		return fmt.Errorf("process %d binary mismatch: stored=%q live=%q command=%q", handle.PID, handle.Binary, actualBinary, command)
	}
	if len(handle.Args) == 0 {
		return fmt.Errorf("process %d stored ref missing argv; live command=%q", handle.PID, command)
	}
	if !argsMatch(fields[1:], handle.Args) {
		return fmt.Errorf("process %d argv mismatch: stored=%q live=%q", handle.PID, strings.Join(handle.Args, " "), command)
	}
	return nil
}

func argsMatch(live, stored []string) bool {
	if len(live) < len(stored) {
		return false
	}
	for start := 0; start <= len(live)-len(stored); start++ {
		ok := true
		for i, want := range stored {
			if live[start+i] != want {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func verifyStartedAt(handle ports.Handle, live time.Time) error {
	if handle.StartedAt.IsZero() {
		return fmt.Errorf("process %d stored ref missing start time; live start=%s", handle.PID, live.Format(time.RFC3339))
	}
	delta := handle.StartedAt.Sub(live)
	if delta < 0 {
		delta = -delta
	}
	if delta > 10*time.Second {
		return fmt.Errorf("process %d start time mismatch: stored=%s live=%s", handle.PID, handle.StartedAt.Format(time.RFC3339Nano), live.Format(time.RFC3339Nano))
	}
	return nil
}

func psStartedAt(pid int) (time.Time, bool, error) {
	raw, ok, err := psField(pid, "lstart=")
	if err != nil || !ok {
		return time.Time{}, ok, err
	}
	started, err := time.ParseInLocation("Mon Jan _2 15:04:05 2006", strings.TrimSpace(raw), time.Local)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse process %d start time %q: %w", pid, raw, err)
	}
	return started.UTC(), true, nil
}

func psField(pid int, field string) (string, bool, error) {
	cmd := exec.Command("ps", "-ww", "-p", strconv.Itoa(pid), "-o", field)
	out, err := cmd.Output()
	if err != nil {
		if exited, exitErr := ExitedBySignal(pid); exited {
			return "", false, exitErr
		}
		return "", false, nil
	}
	value := strings.TrimSpace(string(out))
	return value, value != "", nil
}

func ExitedBySignal(pid int) (bool, error) {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false, err
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return true, ErrExited
		}
		return false, err
	}
	return false, nil
}
