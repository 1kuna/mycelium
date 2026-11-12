package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// #579: the nested supervisor has its own PGID and process identity. Exercise
// cancellation through the real quick-budget supervisor too, and normal exit.
func TestProcessTreeCancellation(t *testing.T) {
	if os.Getenv(processSessionVariable) != "" && os.Getenv("TREE_TEST_DRIVER") == "" {
		// This test qualifies an outer worker boundary. An already managed gate
		// would otherwise make its fixture worker a nested, non-session owner.
		cmd := exec.Command(os.Args[0], "-test.run=^TestProcessTreeCancellation$")
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, processSessionVariable+"=") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		cmd.Env = append(cmd.Env, "TREE_TEST_DRIVER=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("outer session fixture: %v\n%s", err, out)
		}
		return
	}

	for _, mode := range []string{"cancel", "complete", "gate-budget"} {
		t.Run(mode, func(t *testing.T) {
			w := recoveryWorker(t)
			w.Machine.GPU = "cpu"
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			dir := t.TempDir()
			command := []string{os.Args[0], "-test.run=^TestProcessTreeHelper$"}
			if mode == "gate-budget" {
				budget, err := filepath.Abs("../../scripts/gate-budget.py")
				if err != nil {
					t.Fatal(err)
				}
				script := filepath.Join(dir, "scripts", "fixture.sh")
				if err := os.MkdirAll(filepath.Dir(script), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(script, []byte("#!/bin/bash\nexec \"$@\"\n"), 0700); err != nil {
					t.Fatal(err)
				}
				command = append([]string{"python3", budget, script}, command...)
			}
			j := recoveryJob(t, command)
			j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
			j.Spec.Run.Env = map[string]string{"TREE_TEST_DIR": dir, "TREE_TEST_MODE": mode, "TREE_TEST_OWNER": strconv.Itoa(os.Getpid())}
			var p *process
			await(t, func() bool { p, err = w.start(store, j); return !errors.Is(err, errWorkerCheckpoint) })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.stopDirective(w.Clock.Now(), 0, true, true) })
			pid := processTreeFixturePID(t, dir)
			pgid, err := syscall.Getpgid(pid)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = syscall.Kill(-pgid, syscall.SIGKILL) })
			if pgid == p.cmd.Process.Pid {
				t.Fatal("fixture did not leave the attempt process group")
			}
			if mode != "complete" {
				p.stopDirective(w.Clock.Now(), 0, true, true)
			}
			result := d5Finish(t, p)
			if result.ReleasePending {
				t.Fatalf("release unconfirmed: %+v", result)
			}
			if members, err := workerGroupMembers(p.cmd.Process.Pid); err != nil || len(members) != 0 {
				t.Fatalf("confirmed release with live session: %v %v", members, err)
			}
			if _, err := processBirth(pid); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("descendant remains: %v", err)
			}
		})
	}
}

// #1322: a payload's setsid child leaves the attempt's group and session and is
// reparented once its parent dies. Yield (TERM, then KILL), preemption, user
// cancellation and a parent that exits normally must all end the escaper
// before release is confirmed.
func TestProcessTreeSetsidEscaper(t *testing.T) {
	if os.Getenv(processSessionVariable) != "" && os.Getenv("TREE_TEST_DRIVER") == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestProcessTreeSetsidEscaper$")
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, processSessionVariable+"=") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		cmd.Env = append(cmd.Env, "TREE_TEST_DRIVER=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("outer session fixture: %v\n%s", err, out)
		}
		return
	}
	for _, stop := range []string{"yield", "preempt", "cancel", "exit"} {
		t.Run(stop, func(t *testing.T) {
			w := recoveryWorker(t)
			w.Machine.GPU = "cpu"
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			dir := t.TempDir()
			mode := "setsid"
			if stop == "exit" {
				mode = "setsid-exit"
			}
			j := recoveryJob(t, []string{os.Args[0], "-test.run=^TestProcessTreeHelper$"})
			j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
			j.Spec.Run.Env = map[string]string{"TREE_TEST_DIR": dir, "TREE_TEST_MODE": mode, "TREE_TEST_OWNER": strconv.Itoa(os.Getpid())}
			var p *process
			await(t, func() bool { p, err = w.start(store, j); return !errors.Is(err, errWorkerCheckpoint) })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.stopDirective(w.Clock.Now(), 0, true, true) })
			pid := processTreeFixturePID(t, dir)
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
			if session, err := unix.Getsid(pid); err != nil || session == p.cmd.Process.Pid {
				t.Fatalf("fixture did not leave the attempt session: %d %v", session, err)
			}
			switch stop {
			case "yield":
				p.stopDirective(w.Clock.Now(), 50*time.Millisecond, false, false)
			case "preempt":
				p.stopDirective(w.Clock.Now(), 0, true, false)
			case "cancel":
				p.stopDirective(w.Clock.Now(), 0, true, true)
			case "exit":
				p.refreshEscapers(w.Clock.Now()) // The attempt actor's periodic custody refresh.
				if err := os.WriteFile(filepath.Join(dir, "go"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			result := d5Finish(t, p)
			if _, err := processBirth(pid); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("release confirmed=%v while escaper %d remains: %v", !result.ReleasePending, pid, err)
			}
			if result.ReleasePending {
				t.Fatalf("release unconfirmed after escaper exit: %+v", result)
			}
		})
	}
}

// A recorded escaper counts only while its birth matches: a reused PID is
// never treated as the attempt's process (#1322).
func TestProcessTreeEscaperBirthFence(t *testing.T) {
	other := exec.Command("sleep", "60")
	if err := other.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Process.Kill(); _ = other.Wait() }()
	birth, err := processBirth(other.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	group := 1 << 30 // No live process group or session has this ID.
	defer forgetGroupCustody(group)
	for _, recorded := range []string{"0:0", birth} {
		adoptGroupCustody(group, map[int]string{other.Process.Pid: recorded})
		members, err := workerGroupMembers(group)
		if err != nil {
			t.Fatal(err)
		}
		if owned := slices.Contains(members, other.Process.Pid); owned != (recorded == birth) {
			t.Fatalf("birth %q: member=%v", recorded, owned)
		}
	}
}

func processTreeFixturePID(t *testing.T, dir string) int {
	t.Helper()
	nativeFile(t, dir, "pid")
	data, err := os.ReadFile(filepath.Join(dir, "pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func TestProcessTreeHelper(t *testing.T) {
	dir := os.Getenv("TREE_TEST_DIR")
	if dir == "" {
		return
	}
	// Keep this deliberately independent outer-session fixture finite if its
	// test driver dies during a cancelled gate before test cleanup can run.
	if owner, err := strconv.Atoi(os.Getenv("TREE_TEST_OWNER")); err == nil {
		birth, err := processBirth(owner)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			tick := time.NewTicker(20 * time.Millisecond)
			defer tick.Stop()
			for range tick.C {
				current, err := processBirth(owner)
				if errors.Is(err, os.ErrNotExist) || err == nil && current != birth {
					session, err := unix.Getsid(0)
					if err == nil {
						_ = signalWorkerGroup(session, syscall.SIGKILL)
					}
					return
				}
			}
		}()
	}

	if os.Getenv("TREE_TEST_MODE") == "orphan" {
		if os.Getenv("TREE_TEST_IGNORE_TERM") != "" {
			signal.Ignore(syscall.SIGTERM) // Only KILL by exact PID ends this escaper.
		}
		// File existence is the readiness handshake. Publish only complete PID bytes
		// so all process-tree callers can parse the first observation.
		if err := os.WriteFile(filepath.Join(dir, "pid.tmp"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(dir, "pid.tmp"), filepath.Join(dir, "pid")); err != nil {
			t.Fatal(err)
		}
		select {} // The owner-death watcher or attempt cancellation terminates this helper.
	}
	command := []string{os.Args[0], "-test.run=^TestProcessTreeHelper$"}
	childEnv := []string{"PATH=" + os.Getenv("PATH"), "TREE_TEST_DIR=" + dir, "TREE_TEST_MODE=orphan", "TREE_TEST_OWNER=" + os.Getenv("TREE_TEST_OWNER")}
	if mode := os.Getenv("TREE_TEST_MODE"); strings.HasPrefix(mode, "setsid") {
		// #1322: the escaper leaves the attempt's process group and session.
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Env = append(childEnv, "TREE_TEST_IGNORE_TERM=1")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		nativeFile(t, dir, "pid")
		if mode == "setsid-exit" {
			nativeFile(t, dir, "go") // Exit only after the worker could see the tree.
			return
		}
		select {} // The attempt's stop ends this parent; the escaper outlives it.
	}
	if os.Getenv("TREE_TEST_MODE") == "complete" {
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Env = processSessionEnvironment(childEnv)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		nativeFile(t, dir, "pid")
		return // Leave the child for the outer attempt's normal release check.
	}
	err := runRecordedCommand(context.Background(), command, dir, childEnv, io.Discard, commandScope{StateDir: dir, Clock: RealClock{}}, "nested.json")
	if err != nil {
		t.Fatal(err)
	}
}

// Restart with only the separate-PGID helper alive: saved birth custody reaps
// it; missing birth evidence retains release rather than assuming the SID.
func TestProcessTreeRecovery(t *testing.T) {
	for _, recorded := range []bool{false, true} {
		t.Run(strconv.FormatBool(recorded), func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestProcessTreeHelper$")
			cmd.Env = processSessionEnvironment(append(os.Environ(), "TREE_TEST_DIR="+dir, "TREE_TEST_OWNER="+strconv.Itoa(os.Getpid())))
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() })
			pid := processTreeFixturePID(t, dir)
			pgid, err := syscall.Getpgid(pid)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = syscall.Kill(-pgid, syscall.SIGKILL) })
			j := recoveryJob(t, []string{"unused"})
			j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
			st := workerState{Identity: ID(), PID: cmd.Process.Pid, Job: &j}
			if err := recordWorkerGroup(&st, cmd.Process.Pid); err != nil {
				t.Fatal(err)
			}
			if !recorded {
				st.Groups[cmd.Process.Pid] = map[int]string{cmd.Process.Pid: st.Groups[cmd.Process.Pid][cmd.Process.Pid]}
			}
			if err := saveWorkerJSON(dir, "attempt-"+st.Identity+".json", st); err != nil {
				t.Fatal(err)
			}
			// Kill only the original group, reproducing the old worker's orphan shape.
			if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			if _, err := processBirth(pid); err != nil {
				t.Fatal("fixture did not survive outer group", err)
			}
			store := &workerStore{dir: dir, state: st}
			err = store.cleanupOrphan(RealClock{})
			if !recorded {
				if err == nil {
					t.Fatal("unknown surviving session incorrectly confirmed release")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := processBirth(pid); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("recovered descendant remains: %v", err)
			}
		})
	}
}

// A transient inspector/persistence failure must not consume the only KILL.
func TestProcessTreeCancellationRetriesCustody(t *testing.T) {
	cmd := exec.Command("sleep", "300")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	j := recoveryJob(t, []string{"unused"})
	j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
	p := &process{cmd: cmd, job: j, saveCustody: func() error { return errors.New("transient custody write") }}
	p.stopDirective(RealClock{}.Now(), 0, true, true)
	if p.killed {
		t.Fatal("failed custody snapshot consumed cancellation")
	}
	if _, err := processBirth(cmd.Process.Pid); err != nil {
		t.Fatal(err)
	}
	p.saveCustody = nil
	p.stopDirective(RealClock{}.Now(), 0, true, true)
	if !p.killed {
		t.Fatal("cancellation did not retry")
	}
	_ = cmd.Wait()
	if _, err := processBirth(cmd.Process.Pid); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retried cancellation left a process: %v", err)
	}
}
