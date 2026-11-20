package myce

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	retainedShape := os.Getenv("MYCE_TEST_RETAINED_SHAPE") == "1"
	paddingKiB, err := strconv.Atoi(envTestDefault("MYCE_TEST_RECORD_PADDING_KIB", "0"))
	if err != nil || paddingKiB < 0 || paddingKiB > 8 {
		t.Fatal("record padding must be between 0 and 8 KiB")
	}
	if retainedShape {
		shapeRetainedInstallFixture(t, q, now, paddingKiB)
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
	jobs, history, submissions := len(q.s.Jobs), len(q.s.WorkObservations), len(q.s.Submissions)
	var stateBytes int64
	if err := q.db.QueryRow("SELECT COALESCE(SUM(length(data)),0) FROM queue_state").Scan(&stateBytes); err != nil {
		t.Fatal(err)
	}
	if retainedShape {
		seedRetainedInstallLogs(t, q)
	}
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
	if retainedShape {
		c.LogRetention = 7 * 24 * time.Hour
	}
	// Bound the manager-launched process as well as the harness. Fresh queues
	// have no CPU/memory limit; these explicit test limits fit the fleet job.
	c.CPUQuota, c.MemoryMax = "200%", "1G"
	if paddingKiB > 0 {
		c.MemoryMax = "4G"
	} // Explicit footprint sweep; its fleet profile must allow this.
	if quota := os.Getenv("MYCE_TEST_QUEUE_CPU_QUOTA"); quota != "" {
		c.CPUQuota = quota
	}
	fixture, err := json.Marshal(map[string]any{"jobs": jobs, "history": history, "submissions": submissions, "state_bytes": stateBytes, "retained_shape": retainedShape, "padding_kib": paddingKiB, "log_retention_hours": c.LogRetention.Hours(), "workers": len(workers), "cpu_quota": c.CPUQuota, "memory_max": c.MemoryMax})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("installed_fixture=%s", fixture)
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
	var mu sync.Mutex
	marks := map[string]int64{}
	workerErrors := map[int]string{}
	mark := func(name string) {
		mu.Lock()
		if marks[name] == 0 {
			marks[name] = time.Now().UnixNano()
		}
		mu.Unlock()
	}
	journalSeen := map[string]bool{}
	readJournal := func() error {
		// Diagnostics must survive expiry of the trial's cancellation budget.
		journalCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		journal := exec.CommandContext(journalCtx, "journalctl", "--user", "-u", unit, "--output=json", "--no-pager", "--grep=event=queue_startup_phase|Mycelium queue: http://")
		journal.Env = env
		journalData, err := journal.Output()
		if err != nil {
			return fmt.Errorf("cannot read isolated startup journal: %w", err)
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
				return err
			}
			mu.Lock()
			cut := marks["stop_request"]
			if cut > 0 && at*1000 > cut && strings.HasPrefix(row.Message, "Mycelium queue: http://") {
				marks["new_process_ready"] = at * 1000
			}
			mu.Unlock()
			key := row.At + row.Message
			if !journalSeen[key] && strings.Contains(row.Message, "event=queue_startup_phase") {
				journalSeen[key] = true
				t.Logf("installed_queue_phase=%s", row.Message)
			}
		}
		return nil
	}
	// LIFO cleanup preserves partial evidence before removing the test unit.
	// These observations never substitute for the reconnect/preservation checks.
	t.Cleanup(func() {
		if err := readJournal(); err != nil {
			t.Errorf("startup diagnostics: %v", err)
		}
		mu.Lock()
		missing := []string{}
		if marks["client_reconnected"] == 0 {
			missing = append(missing, "client")
		}
		for i := range workers {
			if marks[fmt.Sprintf("worker_%d", i)] == 0 {
				missing = append(missing, fmt.Sprintf("worker_%d", i))
			}
		}
		partial, err := json.Marshal(map[string]any{"marks_ns": marks, "worker_last_errors": workerErrors, "missing_reconnections": missing, "failed": t.Failed()})
		mu.Unlock()
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("installed_observation=%s", partial)
		}
		if data, err := os.ReadFile(events); err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				if line != "" {
					t.Logf("installed_manager=%s", line)
				}
			}
		}
	})
	mark("bootstrap_start")
	if err := install(); err != nil {
		t.Fatal(err)
	} // Bootstrap is outside the measured trial.
	mark("bootstrap_install_return")
	client := Client{URL: c.Queue, Token: "synthetic", HTTP: &http.Client{Timeout: time.Second}, singleAttempt: true}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	mark("bootstrap_ready")
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
						code := fmt.Sprintf("%T", err)
						var api *HTTPError
						if errors.As(err, &api) {
							code = fmt.Sprintf("http_%d:%s", api.Status, api.Code)
						}
						mu.Lock()
						workerErrors[i] = code
						mu.Unlock()
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
j=Client(sys.argv[1],token='synthetic',timeout=240).wait('running-0',timeout=240)
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
	pyDone := make(chan struct{})
	var pyResult error
	defer func() {
		select {
		case <-pyDone:
		default:
			_ = py.Process.Kill()
			<-pyDone
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
	// One waiter owns Wait and stderr. Drain stdout before Wait closes its pipe;
	// closing pyDone publishes the result without racing ProcessState/buffers.
	go func() {
		<-scanDone
		pyResult = py.Wait()
		close(pyDone)
	}()
	select {
	case <-sent:
	case <-pyDone:
		t.Fatalf("client exited before wait was in flight: %v %s", pyResult, pyErr.String())
	case <-ctx.Done():
		t.Fatal("client wait not in flight before deadline")
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
	// Capture readiness and startup spans before reconnect success is known.
	if err := readJournal(); err != nil {
		t.Fatal(err)
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
		case <-pyDone:
			t.Fatalf("client exited before reconnection: %v %s", pyResult, pyErr.String())
		case <-ctx.Done():
			t.Fatal("reconnections incomplete")
		case <-time.After(10 * time.Millisecond):
		}
	}
	mark("all_reconnected")
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
	select {
	case <-pyDone:
		if pyResult != nil {
			t.Fatalf("client failed: %v %s", pyResult, pyErr.String())
		}
	case <-ctx.Done():
		t.Fatal("client did not finish before deadline")
	}
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
	if len(restored.s.Submissions) != submissions {
		t.Fatal("keyed answer loss or duplication")
	}
	for _, r := range restored.s.Submissions {
		if r.Job != restored.s.Jobs[r.JobID] {
			t.Fatal("keyed answer identity changed")
		}
	}
	if retainedShape {
		var fresh, expired int
		if err := restored.db.QueryRow("SELECT count(*) FROM logs WHERE text='synthetic-fresh-cutover'").Scan(&fresh); err != nil {
			t.Fatal(err)
		}
		if err := restored.db.QueryRow("SELECT count(*) FROM logs WHERE text='synthetic-expired-cutover'").Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if fresh != 6201 || expired != 0 {
			t.Fatalf("log retention or preservation changed: fresh=%d expired=%d", fresh, expired)
		}
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
	mu.Lock()
	result := map[string]any{"marks_ns": marks, "jobs": jobs, "history": history, "submissions": submissions, "state_bytes": stateBytes, "retained_shape": retainedShape, "padding_kib": paddingKiB, "log_retention_hours": c.LogRetention.Hours(), "workers": len(workers), "cpu_quota": c.CPUQuota, "preserved": true}
	data, err = json.Marshal(result)
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("installed_cutover=%s", data)
}

func envTestDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// Match known production dimensions without reading production records. The
// aged fraction, keyed coverage and optional padding are explicit stress axes,
// not claims about production's unmeasured age/byte distributions.
func shapeRetainedInstallFixture(t *testing.T, q *Queue, now time.Time, paddingKiB int) {
	t.Helper()
	for i := 1400; i < 4464; i++ {
		v := q.s.WorkObservations[strconv.Itoa(i%1400)]
		v.Key.Configuration = fmt.Sprintf("synthetic-shape-%d", i)
		q.s.WorkObservations[strconv.Itoa(i)] = v
	}
	q.s.Submissions = map[string]*submissionRecord{}
	for _, j := range q.s.Jobs {
		if terminal(j.State) {
			i, err := strconv.Atoi(strings.TrimPrefix(j.ID, "retained-"))
			if err != nil {
				t.Fatal(err)
			}
			if i%2 == 0 {
				old := now.Add(-8 * 24 * time.Hour)
				j.FinishedAt = &old
				j.StartedAt = &old
			}
			if paddingKiB > 0 {
				j.Reason = "synthetic record-size sweep " + strings.Repeat("p", paddingKiB*1024)
			}
		}
		j.Spec.SubmissionKey = "cutover-" + j.ID
		fingerprint, err := submissionFingerprint(j.Spec)
		if err != nil {
			t.Fatal(err)
		}
		q.s.Submissions[submissionDigest(j.Spec.Project, j.Spec.SubmissionKey)] = &submissionRecord{Kind: submissionBinding, Project: j.Spec.Project, Key: j.Spec.SubmissionKey, Fingerprint: fingerprint, JobID: j.ID, Job: j, Created: &now}
	}
}

func seedRetainedInstallLogs(t *testing.T, q *Queue) {
	t.Helper()
	tx, err := q.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, j := range q.s.Jobs {
		if !terminal(j.State) {
			continue
		}
		text := "synthetic-fresh-cutover"
		if q.clock.Now().Sub(*j.FinishedAt) > 7*24*time.Hour {
			text = "synthetic-expired-cutover"
		}
		if _, err := tx.Exec("INSERT INTO logs(job,attempt,text) VALUES(?,?,?)", j.ID, j.Attempt, text); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec("INSERT INTO logs(job,attempt,text) VALUES('running-0',1,'synthetic-fresh-cutover')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
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
