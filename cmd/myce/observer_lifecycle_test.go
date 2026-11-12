package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestObserverFixtureLifecycle(t *testing.T) {
	if mode := os.Getenv("MYCE_OBSERVER_LIFECYCLE_HELPER"); mode != "" {
		binary := devTestBinary(t)
		registry, err := sharedProfileRoot()
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(registry, "retained-receipt")
		if err = os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err = saveProfileJSON(filepath.Join(dir, "receipt.json"), profileReceipt{JobID: "fixture-job", Status: "queued", Observation: "armed", ArmNonce: "arm"}); err != nil {
			t.Fatal(err)
		}
		cfg := profileObserver{Queue: os.Getenv("MYCE_OBSERVER_TEST_QUEUE")}
		if err = saveProfileJSON(filepath.Join(registry, "fixture.json"), sharedProfileRegistration{Directory: dir, Observer: cfg, Token: "fixture", Expires: time.Now().Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		lock, err := profileLock(filepath.Join(registry, "observer.lock"))
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		if err = startSharedObserver(context.Background(), binary, registry, lock); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var status observerDiagnostic
		wait(t, ctx, func() bool {
			return readProfileJSON(filepath.Join(registry, "observer.status"), &status) == nil && status.PID > 0
		})
		if err = saveProfileJSON(os.Getenv("MYCE_OBSERVER_TEST_REPORT"), status.PID); err != nil {
			t.Fatal(err)
		}
		if mode == "managed" {
			var b [1]byte
			_, _ = os.Stdin.Read(b[:])
		}
		return // Suite teardown, rather than this test, owns the detached observer.
	}
	binary := devTestBinary(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	for _, mode := range []string{"fixture", "managed"} {
		t.Run(mode, func(t *testing.T) {
			tmp := t.TempDir()
			report := filepath.Join(tmp, "observer-pid.json")
			cmd := exec.Command(os.Args[0], "-test.run=^TestObserverFixtureLifecycle$", "-test.count=1")
			for _, e := range os.Environ() {
				if !strings.HasPrefix(e, "MYCE_TEST_ROOT=") {
					cmd.Env = append(cmd.Env, e)
				}
			}
			// Exercise the same prebuilt-image seam as the sharded gate. The
			// borrowing child's suite still owns its detached observer teardown.
			cmd.Env = append(cmd.Env, "TMPDIR="+tmp, "MYCE_OBSERVER_LIFECYCLE_HELPER="+mode, "GATE_TEST_CLI_BINARY="+binary, "MYCE_OBSERVER_TEST_QUEUE="+server.URL, "MYCE_OBSERVER_TEST_REPORT="+report)
			// Receipts can outlive the executable fixture (ordinary consumers keep
			// them in a checkout). Deleting the fixture is not observer teardown.
			if mode == "fixture" {
				cmd.Env = append(cmd.Env, "MYCE_DEV_SESSION_ROOT="+filepath.Join(tmp, "registrations"))
			}
			var input io.WriteCloser
			if mode == "managed" {
				cmd.Env = append(cmd.Env, "MYCE_PROCESS_SESSION=1")
				cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
				var err error
				input, err = cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var pid int
			wait(t, ctx, func() bool { return readProfileJSON(report, &pid) == nil && pid > 0 })
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
			if mode == "managed" {
				sid, err := unix.Getsid(pid)
				if err != nil {
					t.Fatal(err)
				}
				if sid != cmd.Process.Pid {
					input.Close()
					_ = cmd.Wait()
					t.Fatalf("observer escaped worker session: sid=%d worker=%d", sid, cmd.Process.Pid)
				}
				// The worker's session/group signal must reach an observer even after its
				// submitting command has returned, on normal or cancelled attempt cleanup.
				if err = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
					t.Fatal(err)
				}
				_ = input.Close()
				_ = cmd.Wait()
			} else if err := cmd.Wait(); err != nil {
				t.Fatal("fixture owner failed teardown", err)
			}
			wait(t, ctx, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
			if mode == "fixture" {
				dirs, _ := filepath.Glob(filepath.Join(tmp, "myce-cli-tests-*"))
				if len(dirs) != 0 {
					t.Fatal("fixture retained after observer stopped", strconv.Itoa(pid))
				}
			}
		})
	}
}

func TestSharedObserverCancellationPreservesArms(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { cancel(); w.WriteHeader(http.StatusUnauthorized) }))
	defer server.Close()
	root := t.TempDir()
	clock := &waitClock{now: time.Now()}
	dir, entry, lock := sharedWatchFixture(t, root, server.URL, clock)
	if err := runSharedProfileLoop(ctx, root, lock, clock, waitPause); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var r profileReceipt
	if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &r); err != nil || r.Observation != "armed" || r.ObservationError != "" {
		t.Fatalf("shutdown disarmed durable receipt: %+v %v", r, err)
	}
	if _, err := os.Stat(entry); err != nil {
		t.Fatal("shutdown retired registration", err)
	}
}
