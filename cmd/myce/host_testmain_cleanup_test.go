package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLITestMainNoHelperTempLeak(t *testing.T) {
	tmp := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestQueueBindStopIsSuccessful$")
	child.Env = append(os.Environ(), "TMPDIR="+tmp)
	if b, err := child.CombinedOutput(); err != nil {
		t.Fatalf("isolated suite: %v %s", err, b)
	}
	dirs, err := filepath.Glob(filepath.Join(tmp, "myce-cli-docker-tests-*"))
	if err != nil || len(dirs) != 0 {
		t.Fatalf("isolated suite leaked %d CLI shim roots: %v", len(dirs), err)
	}
}

// Each suite owns its fixture; exit/kill helpers borrow it and cannot leak a
// second fixture. Normal test failures must still return through cleanup.
func TestPackageFixtureLifecycle(t *testing.T) {
	if mode := os.Getenv("MYCE_FIXTURE_LIFECYCLE_HELPER"); mode != "" {
		switch mode {
		case "failure":
			t.Error("intentional child failure")
		case "exit":
			os.Exit(7)
		case "kill":
			fmt.Println("helper-ready")
			select {}
		}
		return
	}
	for _, mode := range []string{"success", "failure", "exit", "kill"} {
		t.Run(mode, func(t *testing.T) {
			tmp := t.TempDir()
			child := exec.Command(os.Args[0], "-test.run=^TestPackageFixtureLifecycle$", "-test.count=1")
			for _, entry := range os.Environ() {
				if (mode == "success" || mode == "failure") && strings.HasPrefix(entry, "MYCE_TEST_DOCKER_BIN=") {
					continue
				}
				child.Env = append(child.Env, entry)
			}
			child.Env = append(child.Env, "TMPDIR="+tmp, "MYCE_FIXTURE_LIFECYCLE_HELPER="+mode)
			want := 0
			if mode == "failure" {
				want = 1
			}
			if mode == "exit" {
				want = 7
			}
			if mode == "kill" {
				output, err := child.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err = child.Start(); err != nil {
					t.Fatal(err)
				}
				scanner := bufio.NewScanner(output)
				if !scanner.Scan() || scanner.Text() != "helper-ready" {
					_ = child.Process.Kill()
					_ = child.Wait()
					t.Fatal("child did not reach borrowed fixture")
				}
				if err = child.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				if err = child.Wait(); err == nil {
					t.Fatal("killed child returned success")
				}
			} else {
				output, err := child.CombinedOutput()
				if code := child.ProcessState.ExitCode(); code != want {
					t.Fatalf("exit=%d want=%d: %v %s", code, want, err, output)
				}
			}
			dirs, err := filepath.Glob(filepath.Join(tmp, "myce-*-tests-*"))
			if err != nil || len(dirs) != 0 {
				t.Fatalf("%s leaked %d fixture roots: %v", mode, len(dirs), err)
			}
		})
	}
}
