package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// #579: the nested supervisor has its own PGID and process identity. Exercise
// cancellation through the real quick-budget supervisor too, and normal exit.
func TestProcessTreeCancellation(t *testing.T) {
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
			j.Spec.Run.Env = map[string]string{"TREE_TEST_DIR": dir, "TREE_TEST_MODE": mode}
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
	if os.Getenv("TREE_TEST_MODE") == "complete" {
		cmd := exec.Command("sleep", "300")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "pid"), []byte(strconv.Itoa(cmd.Process.Pid)), 0600); err != nil {
			t.Fatal(err)
		}
		return // Deliberately leave the child for the outer attempt's release check.
	}
	err := runRecordedCommand(context.Background(), []string{"sh", "-c", `echo $$ > "$1"; exec sleep 300`, "child", filepath.Join(dir, "pid")}, dir,
		[]string{"PATH=" + os.Getenv("PATH")}, io.Discard, commandScope{StateDir: dir, Clock: RealClock{}}, "nested.json")
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
			cmd.Env = processSessionEnvironment(append(os.Environ(), "TREE_TEST_DIR="+dir))
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
