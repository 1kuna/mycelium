package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestBatchLogPolicyValidation(t *testing.T) {
	for _, policy := range []string{"", "keep", "none", "invalid"} {
		t.Run(policy, func(t *testing.T) {
			s := spec("experiment")
			data, err := yaml.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			data = append(data, []byte("logs: "+policy+"\n")...)
			var parsed Spec
			decoder := yaml.NewDecoder(strings.NewReader(string(data)))
			decoder.KnownFields(true)
			if err := decoder.Decode(&parsed); err != nil {
				t.Fatal(err)
			}
			if err := parsed.Validate(); (err == nil) != (policy != "invalid") {
				t.Fatalf("logs %q: %v", policy, err)
			}
		})
	}
}

func TestBatchLogsNoneQueueDiscardsCurrentAttemptOnly(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	if err := json.Unmarshal([]byte(`{"logs":"none"}`), &s); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, s)
	assigned := check(t, q, logsNoneMachine(t, "worker"), nil).Job
	chunk := LogChunk{Result: result(assigned, true), Offset: 1, Text: "synthetic stdout/stderr"}
	for range 2 {
		if err := q.AppendLog(chunk); err != nil {
			t.Fatal(err)
		}
	}
	assertNoBatchLogs(t, q, j.ID, "")
	var offsets int
	if err := q.db.QueryRow("SELECT count(*) FROM log_offsets WHERE job=?", j.ID).Scan(&offsets); err != nil || offsets != 0 {
		t.Fatalf("discarded logs retained offsets: %d, %v", offsets, err)
	}
	chunk.Attempt++
	if err := q.AppendLog(chunk); !errors.Is(err, ErrStale) {
		t.Fatalf("stale output accepted: %v", err)
	}
}

func TestBatchLogsNoneLifecycle(t *testing.T) {
	for _, mode := range []string{"success", "failure", "cancel", "large-output"} {
		t.Run(mode, func(t *testing.T) {
			q, _ := fixture(t)
			clock := &workerFakeClock{now: q.clock.Now()}
			q.clock = clock
			s := spec("experiment")
			s.Resumable = false
			if err := json.Unmarshal([]byte(`{"logs":"none"}`), &s); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			s.Run.Env = map[string]string{"NULL_ASSERT": exe}
			script := nullDescriptorAssertion + "echo synthetic-stdout; echo synthetic-stderr >&2; "
			want := "succeeded"
			switch mode {
			case "failure":
				script += "exit 7"
				want = "failed"
			case "cancel":
				// The marker proves payload startup without relying on captured logs.
				s.Run.Env["START_MARKER"] = "{work}/started"
				script += `echo ready > "$START_MARKER"; exec sleep 60`
				want = "cancelled"
			case "large-output":
				// Over 1 MiB on each pipe would block an undrained subprocess.
				script += "dd if=/dev/zero bs=65536 count=32; dd if=/dev/zero bs=65536 count=32 >&2"
			default:
				script += "exit 0"
			}
			s.Run.Command = []string{"sh", "-c", script}
			j := submit(t, q, s)
			run := workerFixture(t, q)
			if mode == "cancel" {
				nativeFile(t, nativeWorkerWork(t, run.state), "started")
				if err := q.Cancel(j.ID); err != nil {
					t.Fatal(err)
				}
			}
			await(t, func() bool {
				clock.Advance(time.Millisecond)
				return job(t, q, j.ID).State == want
			})
			await(t, func() bool {
				files, _ := filepath.Glob(filepath.Join(run.state, "*.result.json"))
				return len(files) == 1
			})
			r := run.result(t)
			if mode == "failure" && (!strings.HasPrefix(r.Error, "exit status 7; ") || !strings.Contains(r.Error, "declare needs.network: true")) {
				t.Fatalf("exit code or offline-network remedy lost: %q", r.Error)
			}
			if mode == "failure" && job(t, q, j.ID).Reason != "command failed: "+r.Error {
				t.Fatal("queue lost command exit-status diagnostic")
			}
			if mode == "cancel" && !r.Interrupted {
				t.Fatal("cancelled command did not report interruption")
			}
			if mode == "success" || mode == "large-output" {
				if !r.Success || r.Error != "" {
					t.Fatalf("discarding output changed success: %+v", r)
				}
			}
			assertNoBatchLogs(t, q, j.ID, run.state)
			var snapshot Snapshot
			req := httptest.NewRequest("GET", "/api/status", nil)
			req.Header.Set("Authorization", "Bearer test")
			response := httptest.NewRecorder()
			API{Queue: q, Token: "test"}.Handler().ServeHTTP(response, req)
			if response.Code != 200 {
				t.Fatalf("status: %d", response.Code)
			}
			if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Jobs) != 1 || snapshot.Jobs[0].State != want || snapshot.Jobs[0].Starts != 1 || snapshot.Jobs[0].Attempt != 1 || snapshot.Jobs[0].StartedAt == nil || snapshot.Jobs[0].FinishedAt == nil {
				t.Fatalf("lifecycle metadata lost: %+v", snapshot.Jobs)
			}
		})
	}
}

func TestBatchLogsDefaultCapture(t *testing.T) {
	for _, policy := range []string{"", "keep"} {
		t.Run(policy, func(t *testing.T) {
			q, _ := fixture(t)
			q.clock = &workerFakeClock{now: q.clock.Now()}
			s := spec("experiment")
			s.Resumable = false
			s.Logs = policy
			s.Run.Command = []string{"sh", "-c", "echo synthetic-stdout; echo synthetic-stderr >&2"}
			j := submit(t, q, s)
			run := workerFixture(t, q)
			await(t, func() bool { return job(t, q, j.ID).State == "succeeded" })
			logs, err := q.Logs(j.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			var text strings.Builder
			for _, chunk := range logs {
				text.WriteString(chunk.Text)
			}
			if text.String() != "synthetic-stdout\nsynthetic-stderr\n" {
				t.Fatal("default queue capture changed")
			}
			files, err := filepath.Glob(filepath.Join(run.state, "*.log"))
			if err != nil || len(files) != 1 {
				t.Fatalf("default attempt log missing: %v", err)
			}
			data, err := os.ReadFile(files[0])
			if err != nil || string(data) != text.String() {
				t.Fatal("default worker capture changed")
			}
		})
	}
}

func TestServerLogPolicyOutput(t *testing.T) {
	for _, policy := range []string{"none", "", "keep"} {
		t.Run(policy, func(t *testing.T) {
			m := serverTestModel(t, "ready")
			m.Logs = policy
			m.StopTimeout = 3 * time.Second
			prefix := "echo synthetic-stdout; echo synthetic-stderr >&2; "
			if policy == "none" {
				exe, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				m.Env = map[string]string{"NULL_ASSERT": exe, "NULL_STOP_MARKER": filepath.Join(t.TempDir(), "checked")}
				prefix = nullDescriptorAssertion + prefix
				prefix += "dd if=/dev/zero bs=65536 count=32; dd if=/dev/zero bs=65536 count=32 >&2; "
			}
			m.StartCommand = append([]string{"sh", "-c", prefix + `exec "$@"`, "server"}, m.StartCommand...)
			stop := "echo synthetic-stop-stdout; echo synthetic-stop-stderr >&2"
			if policy == "none" {
				stop = nullDescriptorAssertion + `echo checked > "$NULL_STOP_MARKER"; ` + stop
			}
			m.StopCommand = []string{"sh", "-c", stop}
			dir := t.TempDir()
			e := NewModelEngine(m, dir, RealClock{})
			ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
			defer cancel()
			if err := e.Load(ctx, m); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Unload() })
			if err := e.Unload(); err != nil {
				t.Fatal(err)
			}
			files, err := filepath.Glob(filepath.Join(dir, "*.log"))
			if err != nil {
				t.Fatal(err)
			}
			if policy == "none" {
				if data, err := os.ReadFile(m.Env["NULL_STOP_MARKER"]); err != nil || string(data) != "checked\n" {
					t.Fatal("stop helper did not verify null descriptors", err)
				}
				if len(files) != 0 {
					t.Fatal("server output retained")
				}
			} else {
				if len(files) != 1 {
					t.Fatal("default server output missing")
				}
				data, err := os.ReadFile(files[0])
				if err != nil || !strings.Contains(string(data), "synthetic-stdout\nsynthetic-stderr\n") || !strings.Contains(string(data), "synthetic-stop-stdout\nsynthetic-stop-stderr\n") {
					t.Fatal("default server or stop output changed")
				}
			}
		})
	}
}

func TestDockerLogsNoneRecovery(t *testing.T) {
	for _, missingFlag := range []bool{false, true} {
		name := "runtime-flag"
		if missingFlag {
			name = "job-policy"
		}
		t.Run(name, func(t *testing.T) { testDockerLogsNoneRecovery(t, missingFlag) })
	}
}

func testDockerLogsNoneRecovery(t *testing.T, missingFlag bool) {
	w, s, clock, root := dockerFixture(t)
	j := dockerGroupJob(t)
	j.Spec.Logs = "none"
	p, err := startDockerFixture(w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
	if err := os.Remove(filepath.Join(root, "running")); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(root, "calls"))
		return strings.Count(string(data), "kill --signal=TERM sidecar-") == 2
	})
	p.docker.close()
	// Simulate saved custody lacking the redundant flag. The job policy must win.
	var saved dockerExecution
	custody := filepath.Join(s.dir, s.state.Identity+".docker.json")
	data, err := os.ReadFile(custody)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	saved.LogsNone = !missingFlag
	if err := saveWorkerJSON(s.dir, s.state.Identity+".docker.json", saved); err != nil {
		t.Fatal(err)
	}
	restored, err := w.restoreDocker(s)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.docker.close()
	clock.advance(StopGrace)
	select {
	case finished := <-restored.done:
		if finished.err != nil || !finished.result.Success || finished.result.ReleasePending {
			t.Fatalf("discarding logs changed Docker lifecycle: %+v", finished)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("Docker cleanup stuck")
	}
	data, err = os.ReadFile(filepath.Join(root, "calls"))
	if err != nil || strings.Contains(string(data), "logs ") || strings.Count(string(data), "--log-driver none") != 3 {
		t.Fatal("Docker capture not disabled for all containers")
	}
	data, err = os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence map[string]dockerMember
	if err := json.Unmarshal(data, &evidence); err != nil || len(evidence) != 3 {
		t.Fatalf("lost exit evidence: %v", err)
	}
	for _, member := range evidence {
		if member.ExitCode == nil || member.StderrTail != "" {
			t.Fatal("missing exit code or retained stderr")
		}
	}
	files, err := filepath.Glob(filepath.Join(s.dir, "*.log"))
	if err != nil || len(files) != 0 || s.state.Log != "" {
		t.Fatal("Docker attempt output retained")
	}
}

func assertNoBatchLogs(t *testing.T, q *Queue, id, dir string) {
	t.Helper()
	var count int
	if err := q.db.QueryRow("SELECT count(*) FROM logs WHERE job=?", id).Scan(&count); err != nil || count != 0 {
		t.Fatalf("captured DB output: %d rows, %v", count, err)
	}
	logs, err := q.Logs(id, 0)
	if err != nil || len(logs) != 0 {
		t.Fatalf("captured API output: %d chunks, %v", len(logs), err)
	}
	if dir != "" {
		files, err := filepath.Glob(filepath.Join(dir, "*.log"))
		if err != nil || len(files) != 0 {
			t.Fatalf("created attempt logs: %d files, %v", len(files), err)
		}
	}
}

// This runs inside the payload, not in the parent test's output plumbing.
func TestLogsNoneChild(t *testing.T) {
	if os.Args[len(os.Args)-1] != "assert-null" {
		return
	}
	null, err := os.Stat(os.DevNull)
	if err != nil {
		os.Exit(90)
	}
	for _, f := range []*os.File{os.Stdout, os.Stderr} {
		st, err := f.Stat()
		if err != nil || !os.SameFile(null, st) {
			os.Exit(91)
		}
	}
}

const nullDescriptorAssertion = `"$NULL_ASSERT" -test.run=^TestLogsNoneChild$ -- assert-null || exit 91; `

func logsNoneMachine(t *testing.T, id string) Machine {
	t.Helper()
	m := machine(id, "fake")
	if err := json.Unmarshal([]byte(`{"capabilities":["native_isolation_landlock","logs_none"]}`), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestLogsNoneRequiresWorkerCapability(t *testing.T) {
	for _, policy := range []string{"", "keep", "none"} {
		t.Run(policy, func(t *testing.T) {
			q, clock := fixture(t)
			s := spec("experiment")
			s.Logs = policy
			j := submit(t, q, s)
			old := machine("old", "fake")
			r := check(t, q, old, nil)
			if policy != "none" {
				if r.Job == nil {
					t.Fatal("default/keep no longer works with old workers")
				}
				return
			}
			if r.Job != nil {
				t.Fatal("old worker received logs: none")
			}
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID); got.State != "queued" || !strings.Contains(got.Reason, "logs_none") {
				t.Fatalf("missing waiting reason: %+v", got)
			}
			capable := logsNoneMachine(t, "new")
			if check(t, q, capable, nil).Job == nil {
				t.Fatal("capable worker did not receive job")
			}
			// A lost-response redelivery must also respect current advertisement.
			if check(t, q, machine("new", "fake"), nil).Job != nil {
				t.Fatal("redelivered to incompatible worker")
			}
		})
	}
}

func TestLogsNoneServerRequiresWorkerCapability(t *testing.T) {
	q, clock, old := modelFixture(t)
	q.models.Models[0].Logs = "none"
	old.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, old, nil)
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Platform: "fake"}); err != nil {
		t.Fatal(err)
	}
	c := addModelCall(t, q, "experiment")
	if q.s.Pins[0].Assigned != "" {
		t.Fatal("pin assigned to old worker")
	}
	if len(q.s.ModelSlots) != 0 || c.Machine != "" {
		t.Fatal("old worker received server")
	}
	clock.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, c.ID); !strings.Contains(got.Reason, "logs_none") {
		t.Fatal(got.Reason)
	}
	capable := logsNoneMachine(t, "new")
	capable.ModelURL, capable.ModelConfigHash = old.ModelURL, old.ModelConfigHash
	if len(check(t, q, capable, nil).ModelPlan) != 1 {
		t.Fatal("capable worker missing server plan")
	}
	reportModel(t, q, capable, "ready")
	if c.Machine != capable.ID {
		t.Fatal("call not routed")
	}
	if err := q.finishCall(c.ID); err != nil {
		t.Fatal(err)
	}
	// A previously ready copy cannot be reused after capability disappears.
	capable = machine("new", "fake")
	capable.ModelURL, capable.ModelConfigHash = old.ModelURL, old.ModelConfigHash
	reportModel(t, q, capable, "ready")
	next := addModelCall(t, q, "experiment")
	if next.Machine != "" {
		t.Fatal("incompatible ready server reused")
	}
}

func TestLogsNoneEnvironmentOutput(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"broken": {"fake": r}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name, s.Logs = "broken", "none"
	j := submit(t, q, s)
	m := logsNoneMachine(t, "builder")
	m.EnvironmentBuilder = true
	check(t, q, m, nil)
	if err := q.AppendEnvironmentLog(EnvironmentLog{Machine: m.ID, Session: m.Session, Key: r.Key, Offset: 1, Text: "synthetic-build-output"}); err != nil {
		t.Fatal(err)
	}
	assertNoBatchLogs(t, q, j.ID, "")
	if err := q.copyEnvironmentLogs(q.s.Jobs[j.ID], &m, r.Key); err != nil {
		t.Fatal(err)
	}
	assertNoBatchLogs(t, q, j.ID, "")
	m.EnvironmentReports = []EnvironmentReport{{Key: r.Key, State: "failed", Error: "build exit 9", Log: "synthetic-failure-output"}}
	check(t, q, m, nil)
	if got := job(t, q, j.ID); got.State != "failed" || got.EnvironmentFailureLog != "" {
		t.Fatal("failure retained job output or lost lifecycle")
	}
	// Old persisted fallback text must not escape through Logs either.
	q.s.Jobs[j.ID].EnvironmentFailureLog = "saved-build-output"
	assertNoBatchLogs(t, q, j.ID, "")
	var count int
	if err := q.db.QueryRow("SELECT count(*) FROM environment_build_logs WHERE key=?", r.Key).Scan(&count); err != nil || count != 1 {
		t.Fatal("environment's independent log was lost", err)
	}
}

func TestLogsNoneServerFailureDiagnostic(t *testing.T) {
	for _, policy := range []string{"none", "keep"} {
		m := serverTestModel(t, "crash")
		m.Logs = policy
		e := NewModelEngine(m, t.TempDir(), RealClock{})
		ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
		err := e.Load(ctx, m)
		cancel()
		if err == nil || !strings.Contains(err.Error(), "exit status 7") {
			t.Fatalf("missing exit diagnostic: %v", err)
		}
		if strings.Contains(err.Error(), "see engine-") != (policy == "keep") {
			t.Fatalf("incorrect log advice: %v", err)
		}
	}
}
