package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Exercise each real command call site without a fleet queue or built binary.
func TestQueueBindStopHelper(t *testing.T) {
	if os.Getenv("MYCE_TEST_BIND_STOP") != "1" {
		return
	}
	dir := os.Getenv("MYCE_TEST_BIND_DIR")
	args := []string{"queue", "--token", "bind-test-only", "--db", filepath.Join(dir, "queue.db"), "--checkpoints", filepath.Join(dir, "checkpoints"), "--identity-dir", filepath.Join(dir, "identity"), "--host-state-dir", filepath.Join(dir, "host-state")}
	if os.Getenv("MYCE_TEST_BIND_LISTENER") == "joining" {
		args = append(args, "--listen", "127.0.0.1:0", "--discovery-listen", "192.0.2.1:0")
	} else {
		args = append(args, "--listen", "192.0.2.1:0", "--discovery=false")
	}
	err := run(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(exitCode(err))
}

func TestQueueBindStopIsSuccessful(t *testing.T) {
	for _, listener := range []string{"queue", "joining"} {
		t.Run(listener, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQueueBindStopHelper$")
			cmd.Env = append(os.Environ(), "MYCE_TEST_BIND_STOP=1", "MYCE_TEST_BIND_LISTENER="+listener, "MYCE_TEST_BIND_DIR="+t.TempDir(), "MYCE_TOKEN=bind-test-only")
			stderr, err := cmd.StderrPipe()
			if err != nil {
				t.Fatal(err)
			}
			ready := make(chan bool, 1)
			go func() {
				scanner := bufio.NewScanner(stderr)
				sent := false
				for scanner.Scan() {
					if !sent && strings.Contains(scanner.Text(), "Listen address unavailable;") {
						ready <- true
						sent = true
					}
				}
				if !sent {
					ready <- false
				}
			}()
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			select {
			case started := <-ready:
				if !started {
					err = cmd.Wait()
					t.Fatalf("retry did not start: %v", err)
				}
			case <-ctx.Done():
				_ = cmd.Wait()
				t.Fatal("bind retry readiness timed out")
			}
			if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if err = cmd.Wait(); err != nil {
				t.Fatalf("systemd-style stop failed: %v", err)
			}
		})
	}
}
