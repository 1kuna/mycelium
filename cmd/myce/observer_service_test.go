package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"mycelium/internal/myce"
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

func TestObserverServiceInstall(t *testing.T) {
	for _, platform := range []string{"darwin", "linux"} {
		t.Run(platform, func(t *testing.T) {
			home := filepath.Join(t.TempDir(), "home $literal% &")
			if err := os.Mkdir(home, 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
			var calls []string
			run := func(_ context.Context, name string, args ...string) error {
				calls = append(calls, name+" "+strings.Join(args, " "))
				if name == "launchctl" && len(args) == 2 && strings.HasSuffix(args[1], "/"+observerServiceLabel) {
					return errors.New("not loaded")
				}
				return nil
			}
			binary, root := filepath.Join(home, "myce $literal% &"), filepath.Join(home, "cache")
			if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := installObserverService(context.Background(), platform, home, binary, root, []string{"HOME=" + home}, run); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(home, "Library", "LaunchAgents", observerServiceLabel+".plist")
			if platform == "linux" {
				path = filepath.Join(home, "config", "systemd", "user", "myce-observer.service")
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(data)
			if platform == "darwin" {
				for _, part := range []string{"<key>KeepAlive</key><true/>", "<key>AbandonProcessGroup</key><true/>", "<key>StandardErrorPath</key>", "<string>/usr/bin/env</string><string>-i</string>", html.EscapeString(observerClientBinary(home)), "bootstrap gui/"} {
					if !strings.Contains(text+strings.Join(calls, "\n"), part) {
						t.Fatalf("missing service behavior %s", part)
					}
				}
			} else {
				for _, part := range []string{"Restart=always", "KillMode=process", "StandardError=journal", "ExecStart=:\"/usr/bin/env\" \"-i\"", "$literal%%", "enable --now myce-observer.service"} {
					if !strings.Contains(text+strings.Join(calls, "\n"), part) {
						t.Fatalf("missing service behavior %s", part)
					}
				}
			}
			info, _ := os.Stat(path)
			if info.Mode().Perm() != 0600 {
				t.Fatal("service definition is not private")
			}
			calls = nil
			err = installObserverService(context.Background(), platform, t.TempDir(), binary, root, nil, func(context.Context, string, ...string) error { return errors.New("unavailable") })
			if err == nil || !strings.Contains(err.Error(), "observer_supervision_unavailable") {
				t.Fatal("unavailable manager not diagnosed")
			}
		})
	}
}

func TestObserverServiceRestartAdoptsArms(t *testing.T) {
	binary := devTestBinary(t)
	root := t.TempDir()
	var terminal atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := "running"
		if terminal.Load() {
			state = "succeeded"
		}
		if r.URL.Path == "/api/wait" {
			var req myce.WaitRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			reply := myce.WaitReply{}
			for _, id := range req.IDs {
				reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: state, Revision: 1})
			}
			_ = json.NewEncoder(w).Encode(reply)
			return
		}
		_ = json.NewEncoder(w).Encode(myce.Job{ID: filepath.Base(r.URL.Path), State: state})
	}))
	defer server.Close()
	events := filepath.Join(root, "events")
	hook := filepath.Join(root, "notify")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat >> "+shellArg(events)+"\nprintf '\\n' >> "+shellArg(events)+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		dir := filepath.Join(root, fmt.Sprintf("receipt-%d", i))
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("job-%d", i)
		if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), profileReceipt{JobID: id, Status: "queued", Observation: "armed", ArmNonce: id, Nonce: id}); err != nil {
			t.Fatal(err)
		}
		if err := saveProfileJSON(filepath.Join(root, id+".json"), sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: server.URL, Notify: hook}, Token: "fixture"}); err != nil {
			t.Fatal(err)
		}
	}
	// A fake user manager restarts on child exit (no timers or keyed commands).
	// Its own session confines every fixture process for exact teardown.
	cmd := exec.Command("/bin/sh", "-c", "while :; do \"$1\" service observer run --root \"$2\"; done", "manager", binary, root)
	cmd.Env = append(observerEnvironment(), "RUNNER_TRACKING_ID=fixture", "CI_JOB_ID=fixture")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var first, replacement observerDiagnostic
	wait(t, ctx, func() bool {
		return readProfileJSON(filepath.Join(root, "observer.status"), &first) == nil && first.Registrations == 2
	})
	if err := syscall.Kill(first.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		return readProfileJSON(filepath.Join(root, "observer.status"), &replacement) == nil && replacement.PID != first.PID && replacement.Registrations == 2
	})
	if lock, err := profileLock(filepath.Join(root, "observer.lock")); err == nil {
		lock.Close()
		t.Fatal("replacement did not own lifetime lock")
	}
	terminal.Store(true)
	wait(t, ctx, func() bool {
		for i := range 2 {
			var receipt profileReceipt
			if readProfileJSON(filepath.Join(root, fmt.Sprintf("receipt-%d", i), "receipt.json"), &receipt) != nil || receipt.DeliveryStatus != "delivered" || receipt.Observation != "not_armed" {
				return false
			}
		}
		return true
	})
	// Another restart must not redeliver completed registrations. Idle service
	// ownership is retained rather than entering a restart loop.
	if err := syscall.Kill(replacement.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	var idle observerDiagnostic
	wait(t, ctx, func() bool {
		return readProfileJSON(filepath.Join(root, "observer.status"), &idle) == nil && idle.PID != replacement.PID && idle.Phase == "idle" && idle.Registrations == 0
	})
	data, err := os.ReadFile(events)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var n profileNotification
		if err := json.Unmarshal([]byte(line), &n); err != nil || seen[n.DeliveryID] {
			t.Fatal("invalid or duplicated completion", err)
		}
		seen[n.DeliveryID] = true
	}
	if len(seen) != 2 {
		t.Fatal("restart lost completions")
	}
	// A new durable arm wakes an idle owner through the submission protocol.
	dir := filepath.Join(root, "new-receipt")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), profileReceipt{JobID: "new-job", Status: "queued", Observation: "armed", ArmNonce: "new"}); err != nil {
		t.Fatal(err)
	}
	if err := saveProfileJSON(filepath.Join(root, "new-job.json"), sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: server.URL, Notify: hook}, Token: "fixture"}); err != nil {
		t.Fatal(err)
	}
	socket, err := sharedProfileSocket(root)
	if err != nil {
		t.Fatal(err)
	}
	wake, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: socket, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = wake.Write([]byte{1})
	wake.Close()
	if err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		var receipt profileReceipt
		return readProfileJSON(filepath.Join(dir, "receipt.json"), &receipt) == nil && receipt.DeliveryStatus == "delivered"
	})
}

func TestObserverServiceWaitsForExistingOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observer.lock")
	owner, err := profileLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acquired := make(chan *os.File)
	failure := make(chan error, 1)
	go func() {
		lock, err := supervisedObserverLock(ctx, path)
		if err != nil {
			failure <- err
			return
		}
		acquired <- lock
	}()
	owner.Close()
	select {
	case lock := <-acquired:
		defer lock.Close()
		if duplicate, err := profileLock(path); err == nil {
			duplicate.Close()
			t.Fatal("supervised takeover permitted a duplicate owner")
		}
	case err := <-failure:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not adopt a released lifetime lock")
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
	cmd := exec.CommandContext(ctx, binary, "service", "observer", "run", "--root", root)
	cmd.Env = observerEnvironment()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	var receipt profileReceipt
	wait(t, ctx, func() bool {
		return readProfileJSON(filepath.Join(dir, "receipt.json"), &receipt) == nil && receipt.DeliveryStatus != ""
	})
	if receipt.DeliveryStatus != "delivered" || receipt.Observation != "not_armed" {
		t.Fatal("legacy binding lost preflight PATH/locale during adoption")
	}
	var notification profileNotification
	if err := readProfileJSON(events, &notification); err != nil || notification.JobID != "job" {
		t.Fatal("legacy notification was not received", err)
	}
}

func TestObserverServiceBrokenStartupIsLogged(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "observer.lock"), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(devTestBinary(t), "service", "observer", "run", "--root", root)
	cmd.Env = observerEnvironment()
	if err := cmd.Run(); err == nil {
		t.Fatal("invalid lifetime-lock configuration started an owner")
	}
	data, err := os.ReadFile(filepath.Join(root, "observer.log"))
	if err != nil || !strings.Contains(string(data), `"event":"supervisor_start_failed"`) {
		t.Fatal("broken startup has no retained diagnostic", err)
	}
	if strings.Contains(string(data), root) {
		t.Fatal("startup log retained private raw error text")
	}
}
