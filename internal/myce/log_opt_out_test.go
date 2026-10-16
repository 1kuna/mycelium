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
	assigned := check(t, q, machine("worker", "fake"), nil).Job
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
			script := "echo synthetic-stdout; echo synthetic-stderr >&2; "
			want := "succeeded"
			switch mode {
			case "failure":
				script += "exit 7"
				want = "failed"
			case "cancel":
				// The marker proves payload startup without relying on captured logs.
				marker := filepath.Join(t.TempDir(), "started")
				s.Run.Env = map[string]string{"START_MARKER": marker}
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
				await(t, func() bool { _, err := os.Stat(s.Run.Env["START_MARKER"]); return err == nil })
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
			if mode == "failure" && r.Error != "exit status 7" {
				t.Fatalf("exit code lost: %q", r.Error)
			}
			if mode == "failure" && job(t, q, j.ID).Reason != "command failed: exit status 7" {
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
				prefix += "dd if=/dev/zero bs=65536 count=32; dd if=/dev/zero bs=65536 count=32 >&2; "
			}
			m.StartCommand = append([]string{"sh", "-c", prefix + `exec "$@"`, "server"}, m.StartCommand...)
			m.StopCommand = []string{"sh", "-c", "echo synthetic-stop-stdout; echo synthetic-stop-stderr >&2"}
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
	data, err := os.ReadFile(filepath.Join(root, "calls"))
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
