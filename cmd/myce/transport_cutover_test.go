package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Exercise the shipped CLI and Python transports against real SQLite/API
// replacement. The first request commits, then loses its response mid-flight;
// the next request reaches a reopened queue at the same address.
func TestTransportQueueCutover(t *testing.T) {
	binary := devTestBinary(t)
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	for _, consumer := range []string{"cli", "python"} {
		for _, operation := range []string{"status", "submit", "wait"} {
			t.Run(consumer+"/"+operation, func(t *testing.T) {
				q, clock, db, root := waitQueue(t)
				defer func() { q.Close() }()
				var id string
				if operation != "submit" {
					id = waitSubmit(t, q).ID
					waitFinish(t, q, clock, true)
				}
				var mu sync.Mutex
				cut := false
				capacityRefused := false
				var cutErr error
				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					target := operation == "submit" && r.Method == "POST" && r.URL.Path == "/api/jobs" || operation == "status" && r.URL.Path == "/api/status" || operation == "wait" && (r.URL.Path == "/api/jobs/"+id || r.URL.Path == "/api/wait")
					// The replacement can accept connections before transient GC/host
					// pressure clears. Preserve the real typed capacity signal here;
					// request-budget tests exercise its zero-headroom origin.
					if target && cut && !capacityRefused {
						capacityRefused = true
						w.Header().Set("Content-Type", "application/json")
						w.Header().Set("Retry-After", "1")
						w.WriteHeader(http.StatusServiceUnavailable)
						w.Write([]byte(`{"code":"request_capacity_unavailable","error":"queue has no measured request memory capacity; fitting wire bytes=0; retry after headroom recovers"}`))
						return
					}
					reply := httptest.NewRecorder()
					myce.API{Queue: q, Token: "synthetic"}.Handler().ServeHTTP(reply, r)
					if target && !cut && reply.Code == 200 {
						cut = true
						q.BeginShutdown()
						cutErr = q.Close()
						if cutErr == nil {
							q, cutErr = myce.Open(db, root, clock, myce.WithCheckpointAvailability(func() bool { return true }))
						}
						// Reads lose a framed body; submit loses its committed answer.
						if operation != "submit" {
							w.Header().Set("Content-Length", strconv.Itoa(reply.Body.Len()))
							w.WriteHeader(http.StatusOK)
							w.Write(reply.Body.Bytes()[:1])
						}
						conn, buffer, err := w.(http.Hijacker).Hijack()
						if err == nil {
							buffer.Flush()
							conn.Close()
						}
						return
					}
					for k, v := range reply.Header() {
						w.Header()[k] = v
					}
					w.WriteHeader(reply.Code)
					w.Write(reply.Body.Bytes())
				}))
				defer s.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				var cmd *exec.Cmd
				if consumer == "cli" {
					args := []string{operation, "--queue", s.URL, "--token", "synthetic"}
					if operation == "status" {
						args = append(args, "--json")
					}
					if operation == "wait" {
						args = append(args, id)
					}
					if operation == "submit" {
						path := filepath.Join(t.TempDir(), "job.yaml")
						if err := os.WriteFile(path, []byte("name: cutover\nproject: test\nsubmission_key: same-key\nrun:\n  command: [echo, done]\nneeds:\n  memory_gb: 1\n  platform: {any: true}\n"), 0600); err != nil {
							t.Fatal(err)
						}
						args = append(args, path)
					}
					cmd = exec.CommandContext(ctx, binary, args...)
				} else {
					script := `import sys
from mycelium import Client
c=Client(sys.argv[1],"synthetic",timeout=20)
op=sys.argv[2]
if op=="status": print(c.status()["retained_jobs"])
elif op=="submit": print(c.submit({"name":"cutover","project":"test","submission_key":"same-key","run":{"command":["echo","done"]},"needs":{"memory_gb":1,"platform":{"any":True}}})["id"])
else: assert c.wait(sys.argv[3],timeout=20)["state"]=="succeeded"
`
					cmd = exec.CommandContext(ctx, python, "-c", script, s.URL, operation, id)
					pythonPath, err := filepath.Abs("../../clients/python/src")
					if err != nil {
						t.Fatal(err)
					}
					cmd.Env = append(os.Environ(), "PYTHONPATH="+pythonPath)
				}
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%v: %s", err, output)
				}
				mu.Lock()
				defer mu.Unlock()
				if !cut || !capacityRefused || cutErr != nil {
					t.Fatalf("cutover=%t capacity refusal=%t error=%v", cut, capacityRefused, cutErr)
				}
				snapshot, err := q.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				if len(snapshot.Jobs) != 1 {
					t.Fatalf("lost/duplicated work: %d jobs", len(snapshot.Jobs))
				}
				job := snapshot.Jobs[0]
				if operation == "submit" {
					if !bytes.Contains(output, []byte(job.ID)) {
						t.Fatalf("did not return original committed job: %s", output)
					}
					if job.Spec.SubmissionKey != "same-key" {
						t.Fatal("submission binding lost")
					}
				} else if job.ID != id || job.State != "succeeded" || job.Attempt != 1 || job.Losses != 0 {
					t.Fatalf("restart changed terminal attempt: %+v", job)
				}
				if strings.Contains(string(output), "Traceback") {
					t.Fatalf("user-visible failure: %s", output)
				}
			})
		}
	}
}
