package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestDockerCommandContract(t *testing.T) {
	for _, command := range [][]string{{"docker", "run", "image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "echo", "ok"}, {"docker", "container", "run", "--init", "image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}} {
		s := spec("experiment")
		s.Run.Docker = true
		s.Run.Command = command
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"sh", "-c", "docker run image"}, {"docker", "run", "-d", "image"}, {"docker", "run", "--rm", "image"}, {"docker", "run", "--label", "myce.attempt=wrong", "image"}, {"docker", "run", "--restart=always", "image"}, {"docker", "run", "--privileged", "image"}, {"docker", "run", "--name=x", "image"}, {"docker", "run", "--env", "MYCE_TOKEN=secret", "image"}, {"docker", "run", "--volume", "/var/run:/var/run", "image@sha256:" + strings.Repeat("a", 64)}, {"docker", "run", "--volume", "/tmp:/myce/checkpoint", "image@sha256:" + strings.Repeat("a", 64)}} {
		s := spec("experiment")
		s.Run.Docker = true
		s.Run.Command = args
		if err := s.Validate(); err == nil {
			t.Fatalf("accepted unsafe Docker command: %q", args)
		}
	}
}

// Fake CLI/daemon state is separate from CLI lifetime: late starts survive timeout.
func TestDockerCLIHelper(t *testing.T) {
	if os.Getenv("MYCE_TEST_DOCKER") != "1" {
		return
	}
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	args = args[1:]
	root := os.Getenv("MYCE_TEST_DOCKER_ROOT")
	if args[0] == "late-create" || args[0] == "late" {
		deadline := time.After(10 * time.Second)
		for {
			if _, err := os.Stat(filepath.Join(root, "release-late")); err == nil {
				break
			}
			select {
			case <-deadline:
				os.Exit(3)
			case <-time.After(5 * time.Millisecond):
			}
		}
		if args[0] == "late-create" {
			_ = os.WriteFile(filepath.Join(root, "created"), []byte("id1"), 0600)
		}
		_ = os.WriteFile(filepath.Join(root, "running"), []byte("yes"), 0600)
		os.Exit(0)
	}
	if args[0] == "--host" {
		args = args[2:]
	}
	if os.Getenv("MYCE_TEST_DOCKER_SLOW_STARTUP") == "1" {
		time.Sleep(300 * time.Millisecond)
	}
	f, _ := os.OpenFile(filepath.Join(root, "calls"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	_, _ = fmt.Fprintln(f, strings.Join(args, " "))
	_ = f.Close()
	if os.Getenv("MYCE_TOKEN") != "" {
		os.Exit(99)
	}
	mode, _ := os.ReadFile(filepath.Join(root, "mode"))
	if string(mode) == "down" {
		fmt.Fprintln(os.Stderr, "daemon refuses")
		os.Exit(1)
	}
	if string(mode) == "hang" || (string(mode) == "hang-inspect" && args[0] == "inspect") {
		time.Sleep(time.Minute)
		os.Exit(1)
	}
	if fakeDockerNetwork(root, args, string(mode)) {
		os.Exit(0)
	}
	switch args[0] {
	case "create":
		if _, e := os.Stat(filepath.Join(root, "network")); e == nil {
			fakeDockerGroupCreate(root, args)
			os.Exit(0)
		}
		if string(mode) == "refuse-create" {
			fmt.Fprintln(os.Stderr, "Error response from daemon: create refused")
			os.Exit(1)
		}
		for i, arg := range args {
			if (arg == "--memory" || arg == "--memory-swap" || arg == "--network" || arg == "--name") && i+1 < len(args) {
				_ = os.WriteFile(filepath.Join(root, arg), []byte(args[i+1]), 0600)
			}
			if arg == "--label" && i+1 < len(args) && strings.HasPrefix(args[i+1], "myce.attempt=") {
				_ = os.WriteFile(filepath.Join(root, "label"), []byte(strings.TrimPrefix(args[i+1], "myce.attempt=")), 0600)
			}
		}
		if string(mode) == "late-create" {
			c := exec.Command(os.Args[0], "-test.run=^TestDockerCLIHelper$", "--", "late-create")
			c.Env = os.Environ()
			c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			_ = c.Start()
			time.Sleep(time.Minute)
		}
		_ = os.WriteFile(filepath.Join(root, "created"), []byte("id1"), 0600)
		fmt.Println("id1")
	case "start":
		if strings.HasPrefix(args[1], "sidecar-") {
			_ = os.WriteFile(filepath.Join(root, "running-"+args[1]), nil, 0600)
			fmt.Println(args[1])
			os.Exit(0)
		}
		if string(mode) == "late" {
			c := exec.Command(os.Args[0], "-test.run=^TestDockerCLIHelper$", "--", "late")
			c.Env = os.Environ()
			c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			_ = c.Start()
			time.Sleep(time.Minute)
		}
		if string(mode) != "docker-oom" {
			_ = os.WriteFile(filepath.Join(root, "running"), []byte("yes"), 0600)
		}
		fmt.Println("id1")
	case "ps":
		paths, _ := filepath.Glob(filepath.Join(root, "object-sidecar-*.json"))
		for _, p := range paths {
			fmt.Println(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), "object-"), ".json"))
		}
		if !slices.Contains(args, "label=myce.attempt") && !strings.Contains(strings.Join(args, " "), "label=myce.attempt=") {
			if _, e := os.Stat(filepath.Join(root, "unlabeled")); e == nil {
				fmt.Println("legacy")
			}
		}
		if !slices.Contains(args, "--no-trunc") {
			fmt.Println("short-id")
			os.Exit(0)
		}
		if _, e := os.Stat(filepath.Join(root, "created")); e == nil {
			fmt.Println("id1")
			if _, e := os.Stat(filepath.Join(root, "extra")); e == nil {
				fmt.Println("id2")
			}
		}
	case "inspect":
		_, networkErr := os.Stat(filepath.Join(root, "network"))
		_, removedErr := os.Stat(filepath.Join(root, "network-removed"))
		if networkErr == nil || removedErr == nil {
			fakeDockerGroupInspect(root, args[1:], string(mode))
			os.Exit(0)
		}
		if string(mode) == "bad-inspect" {
			label, _ := os.ReadFile(filepath.Join(root, "label"))
			fmt.Printf(`[{"Id":"id1","Config":{"Labels":{"myce.attempt":%q}},"State":{}}]`, string(label))
			os.Exit(0)
		}
		label, _ := os.ReadFile(filepath.Join(root, "label"))
		objects := []map[string]any{}
		for _, id := range args[1:] {
			if id == "short-id" {
				id = "id1"
			}
			objectLabel := string(label)
			file := "running"
			if id == "legacy" {
				file = "unlabeled"
				objectLabel = ""
			}
			if id == "id2" {
				file = "extra"
			}
			_, e := os.Stat(filepath.Join(root, file))
			running := e == nil
			status := "exited"
			if running {
				status = "running"
			}
			limit, _ := os.ReadFile(filepath.Join(root, "--memory"))
			swap, _ := os.ReadFile(filepath.Join(root, "--memory-swap"))
			memory, _ := strconv.ParseInt(string(limit), 10, 64)
			memorySwap, _ := strconv.ParseInt(string(swap), 10, 64)
			if string(mode) == "missing-memory-limit" {
				memory = 0
			}
			name, _ := os.ReadFile(filepath.Join(root, "--name"))
			objects = append(objects, map[string]any{"Name": "/" + string(name), "HostConfig": map[string]any{"RestartPolicy": map[string]any{"Name": "no"}, "Memory": memory, "MemorySwap": memorySwap, "NetworkMode": "none"}, "Id": id, "Config": map[string]any{"Labels": map[string]string{"myce.attempt": objectLabel, "myce.worker": "docker-worker"}}, "State": map[string]any{"Running": running, "Status": status, "ExitCode": 0, "OOMKilled": string(mode) == "docker-oom"}})
		}
		_ = json.NewEncoder(os.Stdout).Encode(objects)
		if string(mode) == "late" {
			if _, err := os.Stat(filepath.Join(root, "running")); os.IsNotExist(err) {
				_ = os.WriteFile(filepath.Join(root, "inspected-stopped"), nil, 0600)
			}
		}
	case "stop":
		if string(mode) == "stop-error" || string(mode) == "delivered-stop-error" {
			if string(mode) == "delivered-stop-error" {
				_ = os.WriteFile(filepath.Join(root, "stop-delivered"), nil, 0600)
			}
			fmt.Fprintln(os.Stderr, "Error response from daemon: stop reply lost")
			os.Exit(1)
		}
		_ = os.Rename(filepath.Join(root, "running"), filepath.Join(root, "stopped"))
	case "kill":
		if strings.HasPrefix(args[2], "sidecar-") && (args[1] == "--signal=KILL" || string(mode) == "term") {
			_ = os.Rename(filepath.Join(root, "running-"+args[2]), filepath.Join(root, "stopped-"+args[2]))
			os.Exit(0)
		}
		if string(mode) == "delivered-error" {
			f, _ := os.OpenFile(filepath.Join(root, "delivered"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			fmt.Fprintln(f, args[1], args[2])
			f.Close()
			fmt.Fprintln(os.Stderr, "Error response from daemon: runtime Kill RPC reply lost")
			os.Exit(1)
		}
		if string(mode) == "unknown-kill" {
			fmt.Fprintln(os.Stderr, "connection closed before reply")
			os.Exit(1)
		}
		if string(mode) == "refuse-kill" {
			event := "refused-kill-"
			if args[1] == "--signal=TERM" {
				event = "refused-term-"
			}
			_ = os.WriteFile(filepath.Join(root, event+args[2]), nil, 0600)
			fmt.Fprintln(os.Stderr, "Error response from daemon: kill refused")
			os.Exit(1)
		}
		if args[1] == "--signal=KILL" || string(mode) == "term" {
			file, target := "running", "stopped"
			if args[2] == "legacy" {
				file, target = "unlabeled", "legacy-stopped"
			}
			if args[2] == "id2" {
				file, target = "extra", "extra-stopped"
			}
			_ = os.Rename(filepath.Join(root, file), filepath.Join(root, target))
		}
	case "rm":
		if len(args) != 2 || args[1] != "id1" {
			os.Exit(2)
		}
		_ = os.Rename(filepath.Join(root, "created"), filepath.Join(root, "removed"))
	case "logs":
		if string(mode) == "logs-error" {
			os.Exit(1)
		}
		fmt.Println("payload output")
		fmt.Fprintln(os.Stderr, "payload stderr")
	default:
		os.Exit(2)
	}
	switch args[0] {
	case "kill":
		_ = os.WriteFile(filepath.Join(root, "completed-"+args[1]+"-"+args[2]), nil, 0600)
	case "stop":
		_ = os.WriteFile(filepath.Join(root, "completed-stop"), nil, 0600)
	case "ps":
		_ = os.WriteFile(filepath.Join(root, "completed-inventory"), nil, 0600)
	case "inspect":
		for _, id := range args[1:] {
			_ = os.WriteFile(filepath.Join(root, "completed-inspect-"+id), nil, 0600)
		}
	}
	os.Exit(0)
}

type dockerTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *dockerTestClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *dockerTestClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
func dockerFixture(t *testing.T) (Worker, *workerStore, *dockerTestClock, string) {
	t.Helper()
	root := t.TempDir()
	binary, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	script := "#!/bin/sh\nexec '" + binary + "' -test.run='^TestDockerCLIHelper$' -- \"$@\"\n"
	if e = os.WriteFile(filepath.Join(root, "docker"), []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MYCE_TEST_DOCKER", "1")
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	t.Setenv("MYCE_TEST_DOCKER_ROOT", root)
	t.Setenv("MYCE_TOKEN", "must-not-leak")
	hostDir := t.TempDir()
	t.Setenv("MYCE_HOST_STATE_DIR", hostDir)
	clock := &dockerTestClock{now: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)}
	ensureTestHost(t, hostDir, clock)
	w := Worker{HostStateDir: hostDir, Machine: machine("docker-worker", "fake"), StateDir: t.TempDir(), Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 32, nil }}
	s, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	s.state.Session = w.Machine.Session
	t.Cleanup(s.close)
	return w, s, clock, root
}
func dockerJob() Job {
	s := spec("experiment")
	s.Resumable = false
	s.Run.Docker = true
	s.Run.Command = []string{"docker", "run", "image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "sleep", "100"}
	return Job{ID: "docker-job", Attempt: 1, Spec: s}
}
func TestDockerWorkerGraceAndIdentity(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoints")
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	data, _ := os.ReadFile(filepath.Join(root, "calls"))
	if !strings.Contains(string(data), "create --restart=no ") {
		t.Fatal("owned create is restartable", string(data))
	}
	if !strings.Contains(string(data), "--label myce.attempt="+s.state.Identity) || !strings.Contains(string(data), "--cidfile ") {
		t.Fatalf("identity missing: %s", data)
	}
	p.stop(c.Now(), time.Second, false)
	await(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(root, "calls"))
		return strings.Contains(string(data), "--signal=TERM")
	})
	select {
	case <-p.done:
		t.Fatal("released TERM-ignoring container before grace")
	default:
	}
	c.advance(time.Second)
	p.enforceGrace(c.Now())
	select {
	case f := <-p.done:
		if !f.result.Forced || !f.result.Interrupted || f.result.ReleasePending {
			t.Fatalf("bad result: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish")
	}
	if _, e = os.Stat(filepath.Join(root, "running")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("container survived")
	}
	logData, _ := os.ReadFile(filepath.Join(s.dir, s.state.Log))
	if !strings.Contains(string(logData), "payload stderr") {
		t.Fatal("Docker payload stderr lost")
	}
}

func TestDockerLossKeepsHostReservedWithoutHoldPolicy(t *testing.T) {
	q, c := fixture(t)
	a := machine("a", "fake")
	a.HostID = "shared"
	a.HostMemoryMeasured = true
	a.HostAvailableMemoryGB = 32
	b := machine("b", "fake")
	b.HostID = "shared"
	b.HostMemoryMeasured = true
	b.HostAvailableMemoryGB = 32
	other := machine("other", "fake")
	other.HostID = "other"
	other.HostMemoryMeasured = true
	other.HostAvailableMemoryGB = 32
	check(t, q, b, nil)
	check(t, q, other, nil)
	s := dockerJob().Spec
	s.Resumable = true
	s.Needs.HostMemoryGB = 6
	s.AllowedMachines = []string{"a", "b"}
	j := submit(t, q, s)
	running := check(t, q, a, nil).Job
	if running == nil {
		t.Fatal("not assigned")
	}
	for i := 0; i < 17; i++ {
		c.Advance(4 * time.Second)
		check(t, q, b, nil)
		check(t, q, other, nil)
	}
	if e := q.Tick(); e != nil {
		t.Fatal(e)
	}
	snap, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	for _, m := range snap.Machines {
		if m.ID == "b" && m.FreeHostMemoryGB != 26 {
			t.Fatalf("lost Docker reservation freed: %v", m.FreeHostMemoryGB)
		}
	}
	prod := submit(t, q, spec("production"))
	if r := check(t, q, b, nil); r.Job != nil {
		t.Fatalf("reused unresolved host: %s", r.Job.ID)
	}
	if r := check(t, q, other, nil); r.Job == nil || r.Job.ID != prod.ID {
		t.Fatal("production did not go elsewhere")
	}
	if e = q.Complete(Result{Machine: a.ID, Session: a.Session, Execution: Execution{ID: j.ID, Attempt: running.Attempt}}); e != nil {
		t.Fatal(e)
	}
	if r := check(t, q, b, nil); r.Job == nil {
		t.Fatal("confirmed release did not reopen host")
	}
}

func TestDockerDaemonRefusalRetriesWithoutRelease(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("down"), 0600)
	p.stop(c.Now(), 0, true)
	await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed") })
	select {
	case <-p.done:
		t.Fatal("refusal released reservation")
	default:
	}
	_ = os.WriteFile(filepath.Join(root, "mode"), nil, 0600)
	select {
	case f := <-p.done:
		if !f.result.Forced {
			t.Fatal("forced custody lost")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry did not recover")
	}
}
func TestDockerDelayedStartSurvivesClientTimeout(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("late"), 0600)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed: start") })
	p.stop(c.Now(), 0, true)
	select {
	case <-p.done:
		t.Fatal("empty scan settled a pending start")
	default:
	}
	data, e := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker.json"))
	if e != nil || !strings.Contains(string(data), `"pending":true`) {
		t.Fatalf("lost pending intent: %s %v", data, e)
	}
	if err := os.WriteFile(filepath.Join(root, "release-late"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "stopped")); return e == nil })
	select {
	case f := <-p.done:
		if !f.result.Forced {
			t.Fatal("delayed start force classification lost")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("accepted start not reconciled")
	}
}
func TestDockerHangingCallDoesNotBlockStopOrHeartbeat(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("hang"), 0600)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(root, "calls"))
		return strings.Contains(string(data), "create")
	})
	stopped := make(chan struct{})
	go func() { p.stop(c.Now(), 0, true); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop blocked behind dockerd")
	}
	await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed") })
	select {
	case <-p.done:
		t.Fatal("hang released reservation")
	default:
	}
}
func TestDockerRestartReconcilesContainers(t *testing.T) {
	w, s, _, root := dockerFixture(t)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	p.docker.close()
	// Wait for the old lifecycle's I/O to end before simulating a new worker.
	<-p.docker.closed
	restored, e := w.restoreDocker(s)
	if e != nil {
		t.Fatal(e)
	}
	if restored == nil {
		t.Fatal("restart assumed CLI death meant release")
	}
	t.Cleanup(restored.docker.close)
	select {
	case f := <-restored.done:
		if !f.result.Interrupted || !f.result.Forced {
			t.Fatalf("bad restart result: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restart did not release containers")
	}
}
func TestDockerOwnershipMismatchDoesNotKillUnrelatedContainer(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	_ = os.WriteFile(filepath.Join(root, "label"), []byte("unrelated"), 0600)
	p.stop(c.Now(), 0, true)
	await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed") })
	data, _ := os.ReadFile(filepath.Join(root, "calls"))
	if strings.Contains(string(data), "kill --signal=") {
		t.Fatal("signaled another attempt")
	}
}

func TestDockerIncompleteInspectionCannotProveRelease(t *testing.T) {
	w, s, _, root := dockerFixture(t)
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("bad-inspect"), 0600)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	// Wait until the incomplete inspection has actually been processed.
	await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed") })
	select {
	case <-p.done:
		t.Fatal("incomplete inspection released running container")
	default:
	}
	if !strings.Contains(p.docker.status(), "unconfirmed") {
		t.Fatal("incomplete inspection not visible")
	}
}

func TestDockerDefiniteRefusalReleasesWithoutStarting(t *testing.T) {
	w, s, _, root := dockerFixture(t)
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("refuse-create"), 0600)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	select {
	case f := <-p.done:
		if f.result.Success || f.result.ReleasePending {
			t.Fatalf("invalid refusal result: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("definite refusal stayed reserved")
	}
	data, _ := os.ReadFile(filepath.Join(root, "calls"))
	if strings.Contains(string(data), "start ") {
		t.Fatal("start sent after refused create")
	}
}

func TestDockerWorkerPublishesUnconfirmedCleanupWhileDaemonHangs(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	s.close()
	q, _ := fixture(t)
	q.clock = c
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	w.Interval = time.Millisecond
	w.Machine.HostID = "docker-host"
	testHostOpen(t, w.HostStateDir, c, "docker-host")
	w.MemorySource = func(context.Context) (float64, error) { return 8, nil }
	w.HostMemorySource = func(context.Context) (float64, error) { return 32, nil }
	w.CheckpointRoot = q.root
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		if !t.Failed() {
			return
		}
		calls, _ := os.ReadFile(filepath.Join(root, "calls"))
		t.Logf("Docker calls: %s", calls)
		paths, _ := filepath.Glob(filepath.Join(w.StateDir, "*.docker.json"))
		for _, path := range paths {
			data, _ := os.ReadFile(path)
			t.Logf("Docker custody: %s", data)
		}
	}()
	j := submit(t, q, dockerJob().Spec)
	await(t, func() bool {
		c.advance(time.Millisecond)
		if _, e := os.Stat(filepath.Join(root, "running")); e == nil {
			return true
		}
		select {
		case e := <-done:
			t.Fatalf("worker ended before Docker start: %v", e)
		default:
		}
		return false
	})
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("hang"), 0600)
	if e := q.Cancel(j.ID); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		c.advance(time.Millisecond)
		snap, _ := q.Snapshot()
		return len(snap.Machines) > 0 && strings.Contains(snap.Machines[0].DockerCleanup, "unconfirmed")
	})
	requireState(t, q, j.ID, "stopping")
	// Independent control and heartbeats continue despite timed-out daemon requests.
	_ = os.WriteFile(filepath.Join(root, "mode"), nil, 0600)
	c.advance(StopGrace)
	cancel()
	deadline := time.After(5 * time.Second)
	// Worker completion happens after the final queue acknowledgement.
	for {
		c.advance(time.Millisecond)
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			final := job(t, q, j.ID)
			if final.State != "cancelled" || !final.ReleaseConfirmed || final.LastStopOutcome != "forced" || final.Losses != 0 {
				t.Fatalf("worker exited without confirmed cancellation: %+v", final)
			}
			paths, err := filepath.Glob(filepath.Join(w.StateDir, "*.docker.json"))
			if err != nil || len(paths) != 1 {
				t.Fatalf("missing attempt custody: %v, %v", paths, err)
			}
			data, err := os.ReadFile(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			var custody dockerExecution
			if err := json.Unmarshal(data, &custody); err != nil {
				t.Fatal(err)
			}
			if !custody.Stopped || !custody.Forced || !custody.Released || custody.Pending {
				t.Fatalf("unsettled Docker custody: %+v", custody)
			}
			return
		case <-deadline:
			data, _ := os.ReadFile(filepath.Join(w.StateDir, "state.json"))
			t.Fatalf("cancellation stuck: job=%+v state=%s", job(t, q, j.ID), data)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Explicit opt-in precedes even daemon discovery: never runs Docker in the normal suite.
func TestDockerRealCheckpointResume(t *testing.T) {
	if os.Getenv("MYCE_REAL_DOCKER_TEST") != "1" {
		t.Skip("real Docker opt-in disabled")
	}
	image := os.Getenv("MYCE_REAL_DOCKER_IMAGE")
	if image == "" {
		t.Skip("set MYCE_REAL_DOCKER_IMAGE to an already-local digest-pinned shell image")
	}
	t.Setenv("PATH", dockerTestOriginalPath)
	if _, e := exec.LookPath("docker"); e != nil {
		t.Skip("Docker unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "image", "inspect", image)
	if e := cmd.Run(); e != nil {
		t.Skip("Docker daemon or local image unavailable; no pull")
	}
	w := Worker{Machine: machine("real-docker-test", "fake"), StateDir: t.TempDir(), Clock: RealClock{}}
	s, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	s.state.Session = w.Machine.Session
	j := dockerJob()
	j.Spec.Resumable = true
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	j.Spec.Run.Command = []string{"docker", "run", image, "sh", "-c", `i=$(cat "$MYCE_CHECKPOINT_DIR/count" 2>/dev/null || echo 0); trap 'echo "$i" > "$MYCE_CHECKPOINT_DIR/count"; exit 0' TERM; while [ "$i" -lt 20 ]; do i=$((i+1)); echo "$i" > "$MYCE_CHECKPOINT_DIR/count"; sleep 0.1; done`}
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	defer p.docker.close()
	await(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(attemptCheckpoint(j), "count"))
		n, _ := strconv.Atoi(strings.TrimSpace(string(data)))
		return n >= 2
	})
	p.stop(w.Clock.Now(), 2*time.Second, false)
	f := <-p.done
	if !f.result.Interrupted {
		t.Fatal("did not stop")
	}
	j.Attempt = 2
	p2, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	defer p2.docker.close()
	f = <-p2.done
	if !f.result.Success {
		t.Fatalf("resume failed: %+v", f)
	}
	data, _ := os.ReadFile(filepath.Join(attemptCheckpoint(j), "count"))
	if strings.TrimSpace(string(data)) != "20" {
		t.Fatal("resume did not finish counter")
	}
	j.Attempt = 3
	j.Spec.Run.Command = []string{"docker", "run", image, "sh", "-c", `trap '' TERM; while :; do sleep 0.1; done`}
	p3, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	defer p3.docker.close()
	await(t, func() bool { return p3.docker.status() == "running" })
	p3.stop(w.Clock.Now(), 0, true)
	f = <-p3.done
	if !f.result.Forced {
		t.Fatal("TERM-ignoring payload not forced")
	}
}

func TestDockerAcceptedStartObservationSettlesLostResponse(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("late"), 0600)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed: start") })
	p.stop(c.Now(), 0, true)
	if err := os.WriteFile(filepath.Join(root, "release-late"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-p.done:
		if !f.result.Forced || !f.result.Interrupted {
			t.Fatal("forced disposition not retained")
		}
	case <-time.After(3 * time.Second):
		data, _ := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker.json"))
		calls, _ := os.ReadFile(filepath.Join(root, "calls"))
		t.Fatalf("positively witnessed and killed start stayed unresolved: custody=%s calls=%s", data, calls)
	}
}

func TestDockerStopBeforeDispatchNeverStarts(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	w.control = &processControl{clock: c, pending: WorkerEvent{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, deadline: c.Now(), Kill: true}}
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
		t.Fatal("never-started stop did not settle")
	}
	data, _ := os.ReadFile(filepath.Join(root, "calls"))
	if strings.Contains(string(data), "create ") || strings.Contains(string(data), "start ") {
		t.Fatal("request sent after persisted stop")
	}
}
func TestDockerProductionRetargetPreservesOldHostObligation(t *testing.T) {
	q, _ := fixture(t)
	if e := q.ConfigureHosts(map[string]HostPolicy{"shared": {ProductionHold: true, ProductionIdle: "180s"}}); e != nil {
		t.Fatal(e)
	}
	a := machine("a", "fake")
	a.HostID = "shared"
	check(t, q, a, nil)
	j := submit(t, q, dockerJob().Spec)
	running := check(t, q, a, nil).Job
	if running == nil {
		t.Fatal("Docker job not started")
	}
	prod := submit(t, q, spec("production"))
	requireState(t, q, j.ID, "stopping")
	other := machine("other", "fake")
	other.HostID = "other"
	r := check(t, q, other, nil)
	if r.Job == nil || r.Job.ID != prod.ID {
		t.Fatal("production could not escape unconfirmed host")
	}
	snap, _ := q.Snapshot()
	found := false
	for _, h := range snap.HostHolds {
		if strings.Contains(h, "shared: Docker cleanup unconfirmed") {
			found = true
		}
	}
	if !found {
		t.Fatal("retargeting forgot old release")
	}
}
func TestDockerFreshWorkerAndStaleAckCannotClearCustody(t *testing.T) {
	q, c := fixture(t)
	a := machine("a", "fake")
	a.HostID = "shared"
	check(t, q, a, nil)
	j := submit(t, q, dockerJob().Spec)
	running := check(t, q, a, nil).Job
	if running == nil {
		t.Fatal("not assigned")
	}
	if e := q.Cancel(j.ID); e != nil {
		t.Fatal(e)
	}
	c.Advance(StopGrace)
	if e := q.Tick(); e != nil {
		t.Fatal(e)
	}
	requireState(t, q, j.ID, "stopping")
	fresh := a
	fresh.Session = "new-session"
	if _, e := q.Checkin(Checkin{Machine: fresh, Fresh: true, PreviousSession: a.Session}); e != nil {
		t.Fatal(e)
	}
	if e := q.Complete(Result{Machine: a.ID, Session: a.Session, Execution: Execution{ID: j.ID, Attempt: running.Attempt + 1}, ReleaseOnly: true}); e != nil {
		t.Fatal(e)
	}
	snap, _ := q.Snapshot()
	found := false
	for _, h := range snap.HostHolds {
		if strings.Contains(h, "Docker cleanup unconfirmed") {
			found = true
		}
	}
	if !found {
		t.Fatal("fresh session/stale acknowledgement erased custody")
	}
}

func TestDockerEveryLabeledContainerAndKillRefusalRetainsCustody(t *testing.T) {
	for _, kill := range []bool{false, true} {
		t.Run(fmt.Sprintf("kill=%v", kill), func(t *testing.T) {
			w, s, c, root := dockerFixture(t)
			j := dockerJob()
			j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
			p, e := startDockerFixture(w, s, j)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(p.docker.close)
			await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
			_ = os.WriteFile(filepath.Join(root, "extra"), []byte("yes"), 0600)
			_ = os.WriteFile(filepath.Join(root, "mode"), []byte("refuse-kill"), 0600)
			p.stop(c.Now(), StopGrace, kill)
			await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed") })
			select {
			case <-p.done:
				t.Fatal("kill refusal released resources")
			default:
			}
			// TERM is never retried, even after a daemon refusal. Deadline
			// KILL must automatically reclaim both without external intervention.
			_ = os.WriteFile(filepath.Join(root, "mode"), []byte("term"), 0600)
			if !kill {
				c.advance(StopGrace)
			}
			select {
			case <-p.done:
			case <-time.After(3 * time.Second):
				t.Fatal("definite refusal did not recover automatically")
			}

			if _, e = os.Stat(filepath.Join(root, "extra-stopped")); e != nil {
				t.Fatal("second owned container survived")
			}

		})
	}
}

func TestDockerQueueRestartRetainsCustody(t *testing.T) {
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	c := &fakeClock{now: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)}
	q, e := Open(path, root, c)
	if e != nil {
		t.Fatal(e)
	}
	q.checkpointProbe = func() bool { return true }
	await(t, func() bool { _, e := q.nasProbe.run(root, func() (bool, error) { return true, nil }); return e == nil })
	a := machine("a", "fake")
	a.HostID = "shared"
	a.UnifiedMemory = true
	a.HostMemoryMeasured = true
	a.HostAvailableMemoryGB = 32
	check(t, q, a, nil)
	s := dockerJob().Spec
	s.Needs.HostMemoryGB = 6
	j := submit(t, q, s)
	running := check(t, q, a, nil).Job
	if running == nil {
		t.Fatal("not assigned")
	}
	if e = q.Cancel(j.ID); e != nil {
		t.Fatal(e)
	}
	if e = q.Tick(); e != nil {
		t.Fatal(e)
	}
	c.Advance(StopGrace + 2*time.Minute)
	if e = q.Tick(); e != nil {
		t.Fatal(e)
	}
	if e = q.Close(); e != nil {
		t.Fatal(e)
	}
	q, e = Open(path, root, c)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	fresh := a
	fresh.Session = "new"
	fresh.UnifiedMemory = false
	if _, e = q.Checkin(Checkin{Machine: fresh, Fresh: true, PreviousSession: a.Session}); e != nil {
		t.Fatal(e)
	}
	snap, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	if len(snap.HostHolds) == 0 || !strings.Contains(snap.HostHolds[0], "Docker cleanup unconfirmed") {
		t.Fatal("queue restart/fresh session freed Docker")
	}
	if !strings.Contains(strings.Join(snap.HostHolds, " "), "operator action required") {
		t.Fatal("restart forgot escalation")
	}
	if snap.Machines[0].FreeHostMemoryGB != 18 {
		t.Fatalf("original unified RAM charge changed: %v", snap.Machines[0].FreeHostMemoryGB)
	}
	if e = q.Complete(Result{Machine: a.ID, Session: a.Session, Execution: Execution{ID: j.ID, Attempt: running.Attempt}, ReleaseOnly: true}); e != nil {
		t.Fatal(e)
	}
	snap, _ = q.Snapshot()
	if len(snap.HostHolds) != 0 {
		t.Fatal("exact old-attempt release did not settle custody")
	}
}

func TestDockerCleanupEscalatesDurably(t *testing.T) {
	q, c := fixture(t)
	m := machine("b70", "xpu")
	check(t, q, m, nil)
	j := submit(t, q, dockerJob().Spec)
	running := check(t, q, m, nil).Job
	if running == nil {
		t.Fatal("not assigned")
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	before, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(before.HostHolds, " "), "operator action required") {
		t.Fatal("early escalation")
	}
	c.Advance(StopGrace + 2*time.Minute)
	after, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(after.HostHolds, " "), "operator action required") {
		t.Fatal("stuck cleanup was not escalated")
	}
	for _, r := range q.s.HostReleases {
		if r.EscalatedAt == nil {
			t.Fatal("escalation not durable")
		}
	}
	if next := check(t, q, m, nil); next.Job != nil {
		t.Fatal("escalation released memory")
	}
}

func TestDockerOperatorResolutionIsOfflineAndExact(t *testing.T) {
	w, s, _, _ := dockerFixture(t)
	j := dockerJob()
	s.state.Job = &j
	s.state.Identity = "owned"
	s.state.Log = "owned.log"
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	state := dockerExecution{Identity: "owned", Endpoint: "unix:///old.sock", Operation: "start", Pending: true}
	if err := saveWorkerJSON(s.dir, "owned.docker.json", state); err != nil {
		t.Fatal(err)
	}
	if err := closeTestHost(w.HostStateDir); err != nil {
		t.Fatal(err)
	}
	if err := ReconcileDockerAttempt(context.Background(), w.HostStateDir, w.StateDir, "owned"); err == nil {
		t.Fatal("accepted live worker lock")
	}
	s.close()
	for _, id := range []string{"owned", "other", "../owned"} {
		if err := ReconcileDockerAttempt(context.Background(), w.HostStateDir, w.StateDir, id); err == nil {
			t.Fatal("accepted unknown or wrong attempt")
		}
	}
	b, err := os.ReadFile(filepath.Join(s.dir, "owned.docker.json"))
	var saved dockerExecution
	if err != nil || json.Unmarshal(b, &saved) != nil || !saved.Pending || saved.Released {
		t.Fatal("unknown custody changed", err, saved)
	}

}

func TestDockerDelayedCreateAfterEmptyScanRetainsCustody(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("late-create"), 0600)
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, err := startDockerFixture(w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed: create") })
	objects, err := p.docker.containers()
	if err != nil || len(objects) != 0 {
		t.Fatalf("first scan not empty: %v %v", objects, err)
	}
	p.stop(c.Now(), 0, true)
	if err := os.WriteFile(filepath.Join(root, "release-late"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { _, err := os.Stat(filepath.Join(root, "stopped")); return err == nil })
	await(t, func() bool { p.docker.mu.Lock(); defer p.docker.mu.Unlock(); return p.docker.state.Released })
	p.docker.mu.Lock()
	defer p.docker.mu.Unlock()
	if p.docker.state.Pending {
		t.Fatal("positive exact late-create evidence did not settle acceptance")
	}

}

func TestDockerCannotOverrideAssignedDevice(t *testing.T) {
	for _, name := range []string{"CUDA_VISIBLE_DEVICES", "ONEAPI_DEVICE_SELECTOR", "ZE_AFFINITY_MASK"} {
		for _, flag := range []bool{false, true} {
			s := dockerJob().Spec
			if flag {
				s.Run.Command = append([]string{"docker", "run", "--env", name + "=other"}, s.Run.Command[2:]...)
			} else {
				s.Run.Env = map[string]string{name: "other"}
			}
			if err := s.Validate(); err == nil {
				t.Fatalf("accepted worker device override %s (flag=%v)", name, flag)
			}
		}
	}
}

func TestDockerHealthyHelperStartupBudget(t *testing.T) {
	for _, short := range []bool{true, false} {
		t.Run(fmt.Sprint(short), func(t *testing.T) {
			w, s, _, root := dockerFixture(t)
			t.Setenv("MYCE_TEST_DOCKER_SLOW_STARTUP", "1")
			if short {
				w.dockerCallTimeout = 200 * time.Millisecond
			}
			j := dockerJob()
			j.Checkpoint = t.TempDir()
			p, err := startDockerFixture(w, s, j)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(p.docker.close)
			if short {
				await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed") })
				p.docker.mu.Lock()
				pending := p.docker.state.Pending && p.docker.state.Operation == "create"
				p.docker.mu.Unlock()
				if !pending {
					t.Fatal("short startup timeout did not preserve uncertain create")
				}
				if _, err := os.Stat(filepath.Join(root, "calls")); !os.IsNotExist(err) {
					t.Fatal("helper entered command before startup timeout")
				}
				return
			}
			await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
			p.stop(w.Clock.Now(), 0, true)
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				t.Fatal("production budget failed healthy delayed startup/cleanup")
			}
		})
	}
}

// Standalone Docker tests retry bounded checkpoint I/O as the real worker does.
func startDockerFixture(w Worker, s *workerStore, j Job) (*process, error) {
	deadline := time.After(5 * time.Second)
	for {
		p, err := w.start(s, j)
		if !errors.Is(err, errWorkerCheckpoint) {
			return p, err
		}
		select {
		case <-deadline:
			return nil, err
		case <-time.After(5 * time.Millisecond):
		}
	}
}
