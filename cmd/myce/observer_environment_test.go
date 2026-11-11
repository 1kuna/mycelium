package main

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestObserverStarterCleanup(t *testing.T) {
	if root := os.Getenv("MYCE_OBSERVER_STARTER_FIXTURE"); root != "" {
		lock, err := profileLock(filepath.Join(root, "observer.lock"))
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		if err := startSharedObserver(context.Background(), filepath.Join(root, "owner"), root, lock); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("MYCE_OBSERVER_STARTER_KILL") == "1" {
			var b [1]byte
			_, _ = os.Stdin.Read(b[:])
		}
		return
	}
	for _, mode := range []string{"exit", "group-kill"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			// Fake owner retains the real inherited lock and startup protocol. Only
			// environment names leave the process; no variable values are inspected.
			script := "#!/bin/sh\n/usr/bin/env | /usr/bin/cut -d= -f1 > \"$3/environment.names\"\nprintf '%s' \"$$\" > \"$3/pid\"\nprintf 1 >&4\nexec /bin/sleep 60\n"
			if err := os.WriteFile(filepath.Join(root, "owner"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestObserverStarterCleanup$", "-test.count=1")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, "MYCE_") {
					cmd.Env = append(cmd.Env, entry)
				}
			}
			cmd.Env = append(cmd.Env, "MYCE_OBSERVER_STARTER_FIXTURE="+root, "RUNNER_TRACKING_ID=fixture-tracking", "GITHUB_RUN_ID=fixture-run", "CI_JOB_ID=fixture-job", "FUTURE_CI_SECRET=fixture-secret", "MYCE_TOKEN=fixture-token")
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			if mode == "group-kill" {
				cmd.Env = append(cmd.Env, "MYCE_OBSERVER_STARTER_KILL=1")
			}
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var pid int
			wait(t, ctx, func() bool {
				data, err := os.ReadFile(filepath.Join(root, "pid"))
				pid, _ = strconv.Atoi(string(data))
				return err == nil && pid > 0
			})
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
			names, err := os.ReadFile(filepath.Join(root, "environment.names"))
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"RUNNER_TRACKING_ID", "GITHUB_RUN_ID", "CI_JOB_ID", "FUTURE_CI_SECRET", "MYCE_TOKEN"} {
				if strings.Contains("\n"+string(names), "\n"+key+"\n") {
					t.Fatalf("owner inherited %s", key)
				}
			}
			sid, err := unix.Getsid(pid)
			if err != nil || sid != pid {
				t.Fatalf("shared owner lacks its own session: %v", err)
			}
			if mode == "group-kill" {
				if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
					t.Fatal(err)
				}
				_ = cmd.Wait()
			} else if err := cmd.Wait(); err != nil {
				t.Fatal(err)
			}
			if err := syscall.Kill(pid, 0); err != nil {
				t.Fatal("owner died with starter", err)
			}
			if lock, err := profileLock(filepath.Join(root, "observer.lock")); err == nil {
				lock.Close()
				t.Fatal("surviving owner lost its lifetime lock")
			}
		})
	}
}

func TestObserverLegacyBindingKeepsPathAndLocale(t *testing.T) {
	binary := devTestBinary(t)
	root := t.TempDir()
	tools := filepath.Join(root, "bin")
	if err := os.Mkdir(tools, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tools, "observer-legacy-tool"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tools+":"+os.Getenv("PATH"))
	t.Setenv("LANG", "C")
	t.Setenv("LC_ALL", "C")
	events := filepath.Join(root, "events")
	hook := filepath.Join(root, "notify")
	script := "#!/bin/sh\n[ \"$LANG\" = C ] && [ \"$LC_ALL\" = C ] || exit 1\nobserver-legacy-tool || exit 1\n[ \"${1:-}\" = --check ] && exit 0\ncat > " + shellArg(events) + "\n"
	if err := os.WriteFile(hook, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := profileNotifier(ctx, hook); err != nil {
		t.Fatal("notifier preflight failed", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/wait" {
			fmt.Fprint(w, `{"jobs":[{"id":"job","state":"succeeded","revision":1}]}`)
			return
		}
		fmt.Fprint(w, `{"id":"job","state":"succeeded"}`)
	}))
	defer server.Close()
	dir, entry, lock := sharedWatchFixture(t, root, server.URL, &waitClock{now: time.Now()})
	var reg sharedProfileRegistration
	if err := readProfileJSON(entry, &reg); err != nil {
		t.Fatal(err)
	}
	reg.Observer.Notify = hook
	if err := saveProfileJSON(entry, reg); err != nil { // notify_env deliberately absent, as in durable legacy registrations.
		t.Fatal(err)
	}
	lock.Close()
	owner, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := startSharedObserver(ctx, binary, root, owner); err != nil {
		t.Fatal(err)
	}
	var state observerDiagnostic
	wait(t, ctx, func() bool {
		return readProfileJSON(filepath.Join(root, "observer.status"), &state) == nil && state.PID > 0
	})
	defer syscall.Kill(state.PID, syscall.SIGKILL)
	var receipt profileReceipt
	wait(t, ctx, func() bool {
		return readProfileJSON(filepath.Join(dir, "receipt.json"), &receipt) == nil && (receipt.DeliveryStatus == "delivered" || receipt.DeliveryStatus == "failed")
	})
	if receipt.DeliveryStatus != "delivered" || receipt.Observation != "not_armed" {
		t.Fatalf("legacy binding adoption: delivery=%s observation=%s", receipt.DeliveryStatus, receipt.Observation)
	}
	var notification profileNotification
	if err := readProfileJSON(events, &notification); err != nil || notification.JobID != "job" {
		t.Fatal("legacy notification was not received", err)
	}
}

func TestObserverStartupFailureIsLogged(t *testing.T) {
	root := t.TempDir()
	socket, err := sharedProfileSocket(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(socket, 0700); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(socket)
	owner, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := startSharedObserver(ctx, devTestBinary(t), root, owner); err == nil {
		t.Fatal("invalid socket configuration started an owner")
	}
	data, err := os.ReadFile(filepath.Join(root, "observer.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		var event struct {
			Event string `json:"event"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Event == "observer_start_failed" {
			if strings.Contains(line, root) {
				t.Fatal("startup event contains raw private error text")
			}
			return
		}
	}
	t.Fatal("broken owner startup has no retained diagnostic")
}
