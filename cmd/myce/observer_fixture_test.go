package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Only the suite owner calls this, before removing its fixture. Borrowing test
// helpers must never stop the owning suite's observer. Exact executable custody
// excludes installed observers and other concurrently running suites.
func stopCLIObserverFixtures(root string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	type identity struct{ executable, birth string }
	inventory := func() (map[int]identity, error) {
		out, err := exec.CommandContext(ctx, "ps", "-ax", "-o", "pid=,lstart=,comm=").Output()
		if err != nil {
			return nil, err
		}
		found := map[int]identity{}
		for _, line := range strings.Split(string(out), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 7 {
				continue
			}
			pid, err := strconv.Atoi(fields[0])
			if err != nil {
				return nil, err
			}
			executable := strings.TrimSpace(line[strings.Index(line, fields[6]):])
			if runtime.GOOS == "linux" {
				// Linux ps comm is a basename, not the executable path.
				if executable != "myce-dev-test" && executable != "myce-dev" {
					continue
				}
				executable, err = os.Readlink(filepath.Join("/proc", fields[0], "exe"))
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					return nil, err
				}
			}
			if !strings.HasPrefix(executable, root+string(os.PathSeparator)) {
				continue
			}
			if name := filepath.Base(executable); name != "myce-dev-test" && name != "myce-dev" {
				continue
			}
			args, err := exec.CommandContext(ctx, "ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
			if err != nil {
				continue
			} // The owned helper may have exited since inventory.
			if strings.Contains(string(args), " wait --observer-dir ") && strings.Contains(string(args), " --detached") || strings.Contains(string(args), " wait --receipt ") {
				found[pid] = identity{executable, strings.Join(fields[1:6], " ")}
			}
		}
		return found, nil
	}
	owned, err := inventory()
	if err != nil {
		return err
	}
	confirmed, err := inventory()
	if err != nil {
		return err
	}
	for pid, saved := range owned {
		if confirmed[pid] != saved {
			continue
		}
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
	}
	// The existing notifier can take 30s; the gate's five-second graceful-exit
	// allowance covers process exit and verification. Reads cancel on TERM.
	// Failure preserves the fixture rather than deleting active process state.
	for len(owned) > 0 {
		current, err := inventory()
		if err != nil {
			return err
		}
		for pid, executable := range owned {
			if current[pid] != executable {
				delete(owned, pid)
			}
		}
		if len(owned) == 0 {
			return nil
		}
		if err := waitPause(ctx, 10*time.Millisecond); err != nil {
			return fmt.Errorf("test observers still own fixture: %w", err)
		}
	}
	return nil
}

// Registration retirement and an unlocked observer.lock happen before the
// final status/supervisor writes. Only process exit permits fixture removal.
func waitCLIObserverFixture(t *testing.T, ctx context.Context, registry string) {
	t.Helper()
	seen := map[int]bool{}
	wait(t, ctx, func() bool {
		entries, err := filepath.Glob(filepath.Join(registry, "*.json"))
		if err != nil {
			t.Fatal(err)
		}
		var owner observerDiagnostic
		if err := readProfileJSON(filepath.Join(registry, "observer.status"), &owner); err == nil {
			if owner.PID > 0 {
				seen[owner.PID] = true
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		var supervisor observerSupervision
		if err := readProfileJSON(filepath.Join(registry, "observer.supervision"), &supervisor); err == nil {
			for _, pid := range []int{supervisor.PID, supervisor.ChildPID} {
				if pid > 0 {
					seen[pid] = true
				}
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			return false
		}
		for pid := range seen {
			state, err := exec.CommandContext(ctx, "ps", "-p", strconv.Itoa(pid), "-o", "stat=").Output()
			if err != nil {
				var exited *exec.ExitError
				if errors.As(err, &exited) && exited.ExitCode() == 1 && len(state) == 0 && len(exited.Stderr) == 0 {
					continue
				}
				t.Fatal(err)
			}
			if status := strings.TrimSpace(string(state)); status != "" && !strings.HasPrefix(status, "Z") {
				return false
			}
		}
		return true
	})
}
