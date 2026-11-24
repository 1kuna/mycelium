package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The API/SQLite/command smoke retains the cross-mount resume assertion.
// Preemption, offline replay, restart and stale attempts have focused coverage in
// priority_preemption_test.go, network_test.go, recovery_test.go and checkpoint_nas_test.go.
func TestPreDeployFullSystem(t *testing.T) {
	t.Parallel()
	base := newTestStateRoot(t)
	nas := filepath.Join(base, "nas")
	if err := os.Mkdir(nas, 0700); err != nil {
		t.Fatal(err)
	}
	queueRoot := filepath.Join(base, "queue-mount")
	if err := os.Symlink(nas, queueRoot); err != nil {
		t.Fatal(err)
	}
	q, err := Open(filepath.Join(base, "queue.db"), queueRoot, RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	server := workerTestServer(API{Queue: q, Token: "predeploy-token"}.Handler())
	t.Cleanup(server.Close)
	client := Client{URL: server.URL, Token: "predeploy-token"}
	ctx, cancel := context.WithTimeout(t.Context(), testWaitTimeout)
	defer cancel()
	call := func(method, path string, in, out any) {
		t.Helper()
		if err := client.Call(ctx, method, path, in, out); err != nil {
			t.Fatal(err)
		}
	}
	logs := func(id string) string {
		t.Helper()
		var chunks []LogChunk
		call("GET", "/api/jobs/"+id+"/logs", nil, &chunks)
		var text strings.Builder
		for _, chunk := range chunks {
			text.WriteString(chunk.Text)
		}
		return text.String()
	}
	start := func(id string) (string, <-chan error) {
		t.Helper()
		alias := filepath.Join(base, id+"-mount")
		if err := os.Symlink(nas, alias); err != nil {
			t.Fatal(err)
		}
		workerCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		w := Worker{EnvironmentLimit: 1 << 30, checkpointPrepare: prepareCheckpoint, CheckpointRoot: alias,
			StateDir: filepath.Join(base, id+"-state"), Clock: RealClock{}, Interval: 5 * time.Millisecond,
			Machine: machine(id, "fake"), Client: client}
		go func() { done <- w.Run(workerCtx); close(done) }()
		t.Cleanup(func() {
			stop()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(testWaitTimeout):
				t.Error("worker cleanup did not finish")
			}
		})
		return alias, done
	}
	firstAlias, firstDone := start("first")
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `set -eu
[ -z "${MYCE_TOKEN:-}" ] || exit 90
if [ "$MYCE_ATTEMPT" -gt 1 ]; then
 grep -q '"value":42' "$MYCE_CHECKPOINT_DIR/progress.json"
 test ! -e "$MYCE_CHECKPOINT_DIR/unfinished.tmp"
 echo "RESUMED root=$MYCE_CHECKPOINT_DIR"
 exit 0
fi
trap 'printf '\''{"myce_checkpoint":{"generation":1},"value":42}'\'' > "$MYCE_CHECKPOINT_DIR/progress.json"; exit 0' TERM
printf partial > "$MYCE_CHECKPOINT_DIR/unfinished.tmp"
echo "START root=$MYCE_CHECKPOINT_DIR"
while :; do sleep .01; done`}
	var submitted Job
	call("POST", "/api/jobs", s, &submitted)
	await(t, func() bool { return strings.Contains(logs(submitted.ID), "START root="+firstAlias) })
	call("POST", "/api/workers/first/leave", map[string]bool{"now": false}, nil)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first worker did not leave", ctx.Err())
	}
	secondAlias, secondDone := start("second")
	completed, err := client.Wait(ctx, submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != "succeeded" || completed.Attempt != 2 || completed.Losses != 0 || completed.InfrastructureRetries != 0 {
		t.Fatalf("cross-mount resume: %+v", completed)
	}
	if text := logs(submitted.ID); !strings.Contains(text, "RESUMED root="+secondAlias) {
		t.Fatal("checkpoint did not resume through the second mount", text)
	}
	call("POST", "/api/workers/second/leave", map[string]bool{"now": false}, nil)
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("second worker did not leave", ctx.Err())
	}
}

// The engine observes the exact file handed to Load, rather than merely trusting
// cache telemetry. It is the only fake boundary besides GPU inventory.
type auditedModelEngine struct {
	FakeEngine
	root    string
	loads   *sync.Map
	invalid *sync.Map
}

func (e *auditedModelEngine) Load(ctx context.Context, m Model) error {
	if filepath.Dir(m.Path) != e.root || strings.HasSuffix(m.Path, ".partial") {
		e.invalid.Store(m.Name, "engine received a non-cache or partial path")
		return errors.New("unverified model path")
	}
	if err := verifyModel(m.Path, m); err != nil {
		e.invalid.Store(m.Name, err.Error())
		return err
	}
	old, _ := e.loads.LoadOrStore(m.Name, new(atomic.Int32))
	old.(*atomic.Int32).Add(1)
	return e.FakeEngine.Load(ctx, m)
}

// Environment restart fault extends the acceptance suite with real binary
// builder process groups, HTTP, SQLite, worker cache and command execution.
func TestPreDeployEnvironmentBuildRestart(t *testing.T) {
	t.Parallel()
	base := newTestStateRoot(t)
	nas := filepath.Join(base, "nas")
	if e := os.Mkdir(nas, 0700); e != nil {
		t.Fatal(e)
	}
	q, e := Open(filepath.Join(base, "queue.db"), nas, RealClock{})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	started, gate := filepath.Join(base, "building"), filepath.Join(base, "finish-build")
	build := `set -eu; echo half > "$1/interrupted-input"; touch "$2"; while [ ! -f "$3" ]; do sleep 0.01; done; printf '#!/bin/sh\necho FINISHED-environment\n' > "$1/bin/env-task"; chmod +x "$1/bin/env-task"`
	r, e := snapshotEnvironment(EnvironmentRecipe{Build: []string{"sh", "-c", build, "build", "{root}", started, gate}}, "fake")
	if e != nil {
		t.Fatal(e)
	}
	q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"restart-env": {"fake": r}}})
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	s := spec("experiment")
	s.Environment.Name = "restart-env"
	s.Run.Command = []string{"env-task"}
	j, e := q.Submit(s)
	if e != nil {
		t.Fatal(e)
	}
	state := filepath.Join(base, "worker")
	w := Worker{EnvironmentLimit: 1 << 30, StateDir: state, Clock: RealClock{}, Interval: 5 * time.Millisecond, Client: Client{URL: server.URL, Token: "test"}, Machine: Machine{ID: "restart-worker", GPU: "fake", FreeMemoryGB: 64}}
	run := func() (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- w.Run(ctx) }()
		return cancel, done
	}
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(testWaitTimeout)
		for !condition() {
			if time.Now().After(deadline) {
				t.Fatal(label + " timed out")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel, done := run()
	wait("builder starts", func() bool { _, e := os.Stat(started); return e == nil })
	cancel()
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	retired, err := os.ReadDir(filepath.Join(state, "environment-cache", ".Trash"))
	if err != nil || len(retired) != 0 {
		t.Fatal("cancelled build retained retired bytes", len(retired), err)
	}
	if _, err := os.Stat(filepath.Join(state, "environment-cache", r.Key, "ready.json")); !os.IsNotExist(err) {
		t.Fatal("cancelled prefix published", err)
	}

	if job(t, q, j.ID).State != "queued" {
		t.Fatal("interrupted build consumed execution")
	}
	if e = os.WriteFile(gate, nil, 0600); e != nil {
		t.Fatal(e)
	}
	cancel, done = run()
	defer func() {
		cancel()
		if e := <-done; e != nil {
			t.Error(e)
		}
	}()
	wait("rebuilt job finishes", func() bool { return job(t, q, j.ID).State == "succeeded" })
	trash, err := os.ReadDir(filepath.Join(state, "environment-cache", ".Trash"))
	if err != nil || len(trash) != 0 {
		t.Fatal("worker restart retained interrupted build", len(trash), err)
	}
	if logs, e := q.Logs(j.ID, 0); e != nil || !strings.Contains(fmt.Sprint(logs), "FINISHED-environment") {
		t.Fatal(logs, e)
	}
	if j := job(t, q, j.ID); j.Attempt != 1 || j.Losses != 0 {
		t.Fatal("build restart counted as an execution loss", j)
	}
}

// eventPollWriter records whether the event handler answered before its poll ended.
type eventPollWriter struct {
	http.ResponseWriter
	wrote bool
}

func (w *eventPollWriter) WriteHeader(code int) { w.wrote = true; w.ResponseWriter.WriteHeader(code) }
func (w *eventPollWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}
