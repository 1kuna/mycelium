package myce

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestQueueInstalledCutover runs only in an explicitly opted-in Linux test job.
// The CLI, generated definition, manager lifecycle, SQLite and client are real.
// The testdata adapter replaces only unit lookup/publication and persistent
// enablement. Five worker protocol loops use Worker.checkin and workerRetry;
// they are not five native worker/payload processes or adjacent-release proof.
func TestQueueInstalledCutover(t *testing.T) {
	if os.Getenv("MYCE_NATIVE_QUEUE_INSTALL_TEST") != "1" {
		t.Skip("requires an isolated fleet Linux test job and real user systemd")
	}
	if os.Getenv("MYCE_TOKEN") != "" {
		t.Fatal("test job inherited a prohibited credential variable")
	}
	binary, err := filepath.Abs(os.Getenv("MYCE_TEST_BINARY"))
	if err != nil || os.Getenv("MYCE_TEST_BINARY") == "" {
		t.Fatal("MYCE_TEST_BINARY must identify the built real CLI")
	}
	systemctl, err := exec.LookPath("systemctl")
	if err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	home := filepath.Join(root, "home")
	runtimeDir := fmt.Sprintf("/run/user/%d", os.Getuid())
	unit := fmt.Sprintf("myce-cutover-test-%d-%d.service", os.Getpid(), time.Now().UnixNano())
	env := append(os.Environ(), "HOME="+home, "CODEX_HOME="+filepath.Join(home, "codex"), "XDG_RUNTIME_DIR="+runtimeDir, "DBUS_SESSION_BUS_ADDRESS=unix:path="+runtimeDir+"/bus")
	manager := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, systemctl, append([]string{"--user"}, args...)...)
		cmd.Env = env
		return cmd.CombinedOutput()
	}
	if _, err := manager("show", "--property=Version"); err != nil {
		t.Fatal("user manager unavailable")
	}
	q, workers := candidateCostFixture(t)
	q.clock.(*fakeClock).Advance(time.Since(q.clock.Now()))
	now := q.clock.Now()
	for i := 160; i < 12400; i++ {
		j := *q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j.ID = fmt.Sprintf("retained-%05d", i)
		q.s.Jobs[j.ID] = &j
	}
	for _, j := range q.s.Jobs {
		j.Queued = now
		if j.StartedAt != nil {
			j.StartedAt = &now
		}
		if j.FinishedAt != nil {
			j.FinishedAt = &now
		}
	}
	for k, v := range q.s.WorkObservations {
		v.RecordedAt = now
		v.MemoryRecordedAt = now
		q.s.WorkObservations[k] = v
	}
	for i := range workers {
		workers[i].HostHealth = &HostHealth{DrainReason: "host_memory_pressure"}
		q.s.Machines[workers[i].ID].LastSeen = now
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	var seq int
	var dbname, db string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &dbname, &db); err != nil {
		t.Fatal(err)
	}
	jobs, history := len(q.s.Jobs), len(q.s.WorkObservations)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	c, err := PlanInstall("queue", home, filepath.Join(root, "installed"), filepath.Join(root, "nas"), "http://"+addr, "synthetic", Detection{OS: "linux", Arch: runtime.GOARCH})
	if err != nil {
		t.Fatal(err)
	}
	c.Listen, c.DB, c.Checkpoints = addr, db, q.root
	c.EnvironmentFile = filepath.Join(root, "synthetic.env")
	c.RequireMountedNAS = false
	c.ModelCatalog, c.EnvironmentCatalog = "", ""
	// Bound the manager-launched process as well as the harness. Fresh queues
	// have no CPU/memory limit; these explicit test limits fit the fleet job.
	c.CPUQuota, c.MemoryMax = "200%", "1G"
	if quota := os.Getenv("MYCE_TEST_QUEUE_CPU_QUOTA"); quota != "" {
		c.CPUQuota = quota
	}
	write := func(path string, data []byte, mode os.FileMode) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, mode); err != nil {
			t.Fatal(err)
		}
	}
	write(c.EnvironmentFile, []byte("MYCE_TOKEN=synthetic\nMYCE_QUEUE="+c.Queue+"\n"), 0600)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	write(c.Binary, data, 0700)
	data, err = json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	write(c.Config, data, 0600)
	write(c.Service, []byte(c.ServiceText()), 0600)
	// The service must not inherit the real manager account's agent homes.
	// A private runtime drop-in affects only this test's uniquely named unit.
	isolatedHome := filepath.Join(runtimeDir, "systemd/user", unit+".d/home.conf")
	write(isolatedHome, []byte("[Service]\nEnvironment="+unitQuote("HOME="+home)+" "+unitQuote("CODEX_HOME="+filepath.Join(home, "codex"))+"\n"), 0600)
	shim := filepath.Join(root, "bin/systemctl")
	adapter, err := os.ReadFile("testdata/queue_install_systemctl.py")
	if err != nil {
		t.Fatal(err)
	}
	write(shim, adapter, 0700)
	events := filepath.Join(root, "manager.jsonl")
	env = append(env, "PATH="+filepath.Dir(shim)+":"+os.Getenv("PATH"), "MYCE_TEST_QUEUE_UNIT="+unit, "MYCE_TEST_QUEUE_DEFINITION="+c.Service, "MYCE_TEST_SYSTEMCTL="+systemctl, "MYCE_TEST_QUEUE_EVENTS="+events)
	// Refuse foreign and wildcard units, including names passed through flags,
	// before any manager operation. The event file proves no command escaped.
	for _, args := range [][]string{{"--user", "restart", "foreign.service"}, {"--user", "stop", "myce-queue.service", "foreign.socket"}, {"--user", "show", "myce-queue.service", "--unit=foreign"}, {"--user", "restart", "myce-*.service"}} {
		cmd := exec.Command(shim, args...)
		cmd.Env = env
		if err := cmd.Run(); err == nil {
			t.Fatal("namespace adapter accepted a foreign unit")
		}
	}
	if _, err := os.Stat(events); !os.IsNotExist(err) {
		t.Fatal("refused namespace request reached manager")
	}
	install := func() error {
		cmd := exec.Command(binary, "install", "queue", "--config", c.Config)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("install: %w: %s", err, out)
		}
		return nil
	}
	// Register before any install, so even a failed activation removes this unit.
	t.Cleanup(func() {
		out, err := manager("disable", "--now", unit)
		if err != nil {
			t.Errorf("isolated unit cleanup: %v: %s", err, out)
		}
		if out, err := manager("show", unit, "--property=MainPID", "--value"); err != nil || strings.TrimSpace(string(out)) != "0" {
			t.Errorf("isolated process cleanup unproved: %q %v", out, err)
		}
		for _, path := range []string{filepath.Join(runtimeDir, "systemd/user", unit), filepath.Join(runtimeDir, "systemd/user/default.target.wants", unit)} {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				t.Error(err)
			}
		}
		if err := os.Remove(isolatedHome); err != nil && !os.IsNotExist(err) {
			t.Error(err)
		}
		if err := os.Remove(filepath.Dir(isolatedHome)); err != nil && !os.IsNotExist(err) {
			t.Error(err)
		}
		if _, err := manager("daemon-reload"); err != nil {
			t.Error(err)
		}
		t.Log("isolated unit disabled, process exited, runtime definition and enablement removed; temporary state removed by test cleanup")
	})
	if err := install(); err != nil {
		t.Fatal(err)
	} // Bootstrap is outside the measured trial.
	client := Client{URL: c.Queue, Token: "synthetic", HTTP: &http.Client{Timeout: time.Second}, singleAttempt: true}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	read := func() error { var j Job; return client.Call(ctx, "GET", "/api/jobs/running-0?view=wait", nil, &j) }
	for read() != nil {
		select {
		case <-ctx.Done():
			t.Fatal("initial queue did not become ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
	oldPIDBytes, err := manager("show", unit, "--property=MainPID", "--value")
	if err != nil {
		t.Fatal(err)
	}
	oldPID, err := strconv.Atoi(strings.TrimSpace(string(oldPIDBytes)))
	if err != nil || oldPID <= 0 {
		t.Fatal("no initial queue PID")
	}
	var mu sync.Mutex
	marks := map[string]int64{}
	mark := func(name string) {
		mu.Lock()
		if marks[name] == 0 {
			marks[name] = time.Now().UnixNano()
		}
		mu.Unlock()
	}
	var wg sync.WaitGroup
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer func() { stopWorkers(); wg.Wait() }()
	for i, m := range workers {
		wg.Add(1)
		go func(i int, m Machine) {
			defer wg.Done()
			retry := workerRetry{}
			w := Worker{Client: client, Interval: time.Second}
			for workerCtx.Err() == nil {
				now := time.Now()
				if retry.ready(now) {
					_, err := w.checkin(workerCtx, Checkin{Machine: m, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true}}})
					if err != nil {
						retry.failed(time.Now(), time.Second)
					} else {
						retry.succeeded(time.Now(), time.Second)
						mu.Lock()
						started := marks["first_request_served"] > 0
						mu.Unlock()
						mark(fmt.Sprintf("worker_initial_%d", i))
						if started {
							mark(fmt.Sprintf("worker_%d", i))
						}
					}
				}
				select {
				case <-workerCtx.Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		}(i, m)
	}
	for {
		mu.Lock()
		all := true
		for i := range workers {
			all = all && marks[fmt.Sprintf("worker_initial_%d", i)] > 0
		}
		mu.Unlock()
		if all {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("initial worker checkins failed")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// A real Python wait is in flight before invoking install. Instrument only
	// socket connection/request boundaries; use the shipped retry policy.
	py := exec.CommandContext(ctx, python, "-u", "-c", `import http.client,sys
from mycelium import Client
connect=http.client.HTTPConnection.connect
request=http.client.HTTPConnection.request
def connected(self):
    connect(self)
    print('connected',flush=True)
def sent(self,*args,**kwargs):
    request(self,*args,**kwargs)
    if args[1].startswith('/api/wait'): print('sent',flush=True)
http.client.HTTPConnection.connect=connected
http.client.HTTPConnection.request=sent
j=Client(sys.argv[1],token='synthetic',timeout=60).wait('running-0',timeout=60)
assert j['state']=='succeeded', j
print('finished',flush=True)
`, c.Queue)
	py.Env = append(env, "PYTHONPATH="+filepath.Join(mustRepoRoot(t), "clients/python/src"))
	var pyErr bytes.Buffer
	py.Stderr = &pyErr
	pipe, err := py.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := py.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if py.ProcessState == nil {
			py.Process.Kill()
			py.Wait()
		}
	}()
	sent := make(chan struct{}, 1)
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "sent" {
				select {
				case sent <- struct{}{}:
				default:
				}
			}
			mu.Lock()
			cut := marks["stop_request"] > 0
			mu.Unlock()
			if line == "connected" && cut {
				mark("client_reconnected")
			}
			if line == "finished" {
				mark("client_finished")
			}
		}
	}()
	select {
	case <-sent:
	case <-ctx.Done():
		t.Fatalf("client wait not in flight: %s", pyErr.String())
	}
	if err := os.WriteFile(events, nil, 0600); err != nil {
		t.Fatal(err)
	}
	mark("install_start")
	installDone := make(chan error, 1)
	go func() {
		err := install()
		mark("install_return")
		if err == nil && os.Getenv("MYCE_TEST_EXPECT_READY") == "1" {
			if err = read(); err != nil {
				err = fmt.Errorf("install returned before API readiness: %w", err)
			}
		}
		installDone <- err
	}()
	// Sample user-visible availability and old-process exit independently of
	// systemctl (Type=simple can return before the listener exists).
	failed := false
	for {
		if data, err := os.ReadFile(events); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				var e struct {
					Event, Action string
					At            int64 `json:"at_ns"`
				}
				if json.Unmarshal([]byte(line), &e) == nil && e.Event == "manager_start" && e.Action == "restart" {
					mu.Lock()
					marks["stop_request"] = e.At
					mu.Unlock()
				}
			}
		}
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", oldPID)); os.IsNotExist(err) {
			mark("old_process_exit")
		}
		err := read()
		mu.Lock()
		cut := marks["stop_request"] > 0
		mu.Unlock()
		if err != nil && cut {
			failed = true
			mark("first_unavailable")
		}
		if err == nil {
			mu.Lock()
			if !failed {
				marks["last_request_served"] = time.Now().UnixNano()
			}
			mu.Unlock()
			if failed {
				mark("first_request_served")
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("cutover never recovered")
		case <-time.After(10 * time.Millisecond):
		}
	}
	select {
	case err := <-installDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("installer did not return")
	}
	for {
		mu.Lock()
		all := marks["client_reconnected"] > 0
		for i := range workers {
			all = all && marks[fmt.Sprintf("worker_%d", i)] > 0
		}
		mu.Unlock()
		if all {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("reconnections incomplete")
		case <-time.After(10 * time.Millisecond):
		}
	}
	mark("all_reconnected")
	// Read only this test unit's journal. Its existing listener-ready message
	// identifies readiness independently of Type=simple manager activation.
	journal := exec.CommandContext(ctx, "journalctl", "--user", "-u", unit, "--output=json", "--no-pager")
	journal.Env = env
	journalData, err := journal.Output()
	if err != nil {
		t.Fatal("cannot measure queue readiness from test unit journal")
	}
	for _, line := range bytes.Split(journalData, []byte("\n")) {
		var row struct {
			Message string `json:"MESSAGE"`
			At      string `json:"__REALTIME_TIMESTAMP"`
		}
		if json.Unmarshal(line, &row) != nil {
			continue
		}
		at, err := strconv.ParseInt(row.At, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		if at*1000 > marks["stop_request"] && strings.HasPrefix(row.Message, "Mycelium queue: http://") {
			marks["new_process_ready"] = at * 1000
		}
		cut := marks["stop_request"]
		mu.Unlock()
		if at*1000 > cut && (strings.Contains(row.Message, "event=queue_startup_phase") || strings.Contains(row.Message, "event=slow_queue_mutex_hold")) {
			t.Logf("installed_queue_phase=%s", row.Message)
		}
	}
	mu.Lock()
	ready := marks["new_process_ready"]
	mu.Unlock()
	if ready == 0 {
		t.Fatal("replacement readiness message absent")
	}
	stopWorkers()
	wg.Wait()
	if err := client.Call(ctx, "POST", "/api/workers/complete", Result{Execution: Execution{ID: "running-0", Attempt: 1}, Machine: workers[0].ID, Session: workers[0].Session, Success: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := py.Wait(); err != nil {
		t.Fatalf("client failed: %v %s", err, pyErr.String())
	}
	<-scanDone
	if _, err := manager("stop", unit); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(db, q.root, RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if len(restored.s.Jobs) != jobs || len(restored.s.WorkObservations) != history {
		t.Fatalf("record loss/duplication: jobs=%d history=%d", len(restored.s.Jobs), len(restored.s.WorkObservations))
	}
	for i, m := range workers {
		j := restored.s.Jobs[fmt.Sprintf("running-%d", i)]
		want := "running"
		if i == 0 {
			want = "succeeded"
		}
		if j.State != want || j.Attempt != 1 || j.Session != m.Session || j.Losses != 0 {
			t.Fatalf("attempt changed: %+v", j)
		}
	}
	managerEvents, err := os.ReadFile(events)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	result := map[string]any{"marks_ns": marks, "jobs": jobs, "history": history, "workers": len(workers), "cpu_quota": c.CPUQuota, "preserved": true}
	data, err = json.Marshal(result)
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("installed_cutover=%s", data)
	for _, line := range strings.Split(strings.TrimSpace(string(managerEvents)), "\n") {
		t.Logf("installed_manager=%s", line)
	}
}

func mustRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}
