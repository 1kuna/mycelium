package main

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Redaction rebuilds the error text; it must keep the exit code a caller
// branches on (the command's own, typed refusal or runInfraExit) and never
// repeat the token.
func TestRunRedactionKeepsExitCode(t *testing.T) {
	for _, code := range []int{7, 78, runInfraExit, runDetachedExit} {
		got := redactRunError(waitError(code, "job x: secret-token"), "secret-token")
		if exitCode(got) != code || strings.Contains(got.Error(), "secret-token") {
			t.Fatalf("code %d: %d %v", code, exitCode(got), got)
		}
	}
}

// Exercise OS signals against the CLI, a real queue and verified file store.
// Queued/running jobs and an interrupted reattachment all retain the same key.
func TestRunWaitSignalsDetachAndReattach(t *testing.T) {
	binary := devTestBinary(t)
	for _, sig := range []os.Signal{syscall.SIGTERM, os.Interrupt} {
		for _, state := range []string{"queued", "running"} {
			t.Run(sig.String()+"/"+state, func(t *testing.T) {
				q, clock, db, _ := waitQueue(t)
				if err := q.ConfigureFileStore(db+".files", 1<<20, true); err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(myce.API{Queue: q, Token: "signal-fixture"}.Handler())
				defer server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
				defer cancel()
				client := myce.Client{URL: server.URL, Token: "signal-fixture"}
				root := t.TempDir()
				args := []string{"run", "--queue", server.URL, "--project", "signals", "--key", "same-key", "--wait", "--out", filepath.Join(root, "accepted"), "--output", "result.txt", "--", "sh", "-c", "printf accepted > result.txt"}
				command := func() *exec.Cmd {
					c := exec.CommandContext(ctx, binary, args...)
					c.Dir = root
					c.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + root, "MYCE_TOKEN=signal-fixture"}
					return c
				}
				var assigned *myce.Job
				assign := func() {
					clock.advance(time.Second)
					m := maintenanceCLIWorker("signal-worker")
					m.CPUIsolation = "select-none"
					reply, err := currentCLICheckin(q, clock, myce.Checkin{Machine: m})
					if err != nil || reply.Job == nil {
						t.Fatalf("assign: %+v %v %s", reply, err, waitAssignmentDiagnostics(q))
					}
					assigned = reply.Job
				}
				var id string
				for attempt := 0; attempt < 2; attempt++ {
					c := command()
					var logs enrollmentLogBuffer
					c.Stdout, c.Stderr = &logs, &logs
					if err := c.Start(); err != nil {
						t.Fatal(err)
					}
					done := make(chan error, 1)
					go func() { done <- c.Wait() }()
					wait(t, ctx, func() bool {
						snap, err := q.Snapshot()
						if err != nil || len(snap.Jobs) != 1 {
							return false
						}
						id = snap.Jobs[0].ID
						return strings.Contains(logs.String(), id+" ")
					})
					if state == "running" && attempt == 0 {
						assign()
					}
					if state == "running" {
						wait(t, ctx, func() bool { return strings.Contains(logs.String(), id+" running") })
					}
					if err := c.Process.Signal(sig); err != nil {
						t.Fatal(err)
					}
					select {
					case err := <-done:
						var exit *exec.ExitError
						if !errors.As(err, &exit) || exit.ExitCode() != runDetachedExit {
							t.Fatalf("detach: %v %s", err, logs.String())
						}
					case <-ctx.Done():
						_ = c.Process.Kill()
						<-done
						t.Fatal("signal did not detach")
					}
					if !strings.Contains(logs.String(), "job "+id+" continues") || !strings.Contains(logs.String(), "same --key") || !strings.Contains(logs.String(), "myce cancel") {
						t.Fatal(logs.String())
					}
					j, err := client.GetJob(ctx, id)
					if err != nil || j.State != state || j.CancelledAt != nil {
						t.Fatalf("waiter mutated job: %+v %v", j, err)
					}
				}
				if assigned == nil {
					assign()
				}
				file := filepath.Join(root, "result.txt")
				if err := os.WriteFile(file, []byte("accepted"), 0600); err != nil {
					t.Fatal(err)
				}
				ref, err := myce.IdentifyFile(file)
				if err != nil {
					t.Fatal(err)
				}
				if err = client.UploadFile(ctx, file, ref); err != nil {
					t.Fatal(err)
				}
				if err = q.Complete(myce.Result{Machine: assigned.Machine, Session: assigned.Session, Execution: myce.Execution{ID: id, Attempt: assigned.Attempt}, Success: true, OutputRefs: myce.FileManifest{"result.txt": ref}}); err != nil {
					t.Fatal(err)
				}
				if output, err := command().CombinedOutput(); err != nil {
					t.Fatalf("reattach: %v %s", err, output)
				}
				if data, err := os.ReadFile(filepath.Join(root, "accepted", "result.txt")); err != nil || string(data) != "accepted" {
					t.Fatalf("outputs: %q %v", data, err)
				}
				snap, err := q.Snapshot()
				if err != nil || len(snap.Jobs) != 1 || snap.Jobs[0].ID != id {
					t.Fatal("reattach duplicated job", err)
				}
			})
		}
	}
}

func TestRunReceiptInterruptedSubmissionDetaches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, receipt := range []string{`{"job_id":"accepted-job","project":"demo","key":"same-key"}`, ""} {
		err := runReceiptWait(ctx, func(out io.Writer) error { _, _ = io.WriteString(out, receipt); return ctx.Err() }, 0, "", "", "", "", "", io.Discard, io.Discard)
		if exitCode(err) != runDetachedExit || !strings.Contains(err.Error(), "same") || !strings.Contains(err.Error(), "myce cancel") {
			t.Fatalf("%q: %v", receipt, err)
		}
		if receipt != "" && !strings.Contains(err.Error(), "accepted-job") {
			t.Fatal(err)
		}
	}
}
