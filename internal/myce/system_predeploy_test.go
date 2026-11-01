package myce

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
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
		w := Worker{checkpointPrepare: prepareCheckpoint, CheckpointRoot: alias,
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

// Keep the copy-specific boundaries here: durable unpublished prefixes across
// restart/partition, pin custody, production progress during blocked NAS I/O and
// verification before Engine.Load. These are not covered by the command smoke.
func TestPreDeployModelCopyFaults(t *testing.T) {
	t.Parallel()
	base := newTestStateRoot(t)
	nas, state := filepath.Join(base, "nas"), filepath.Join(base, "worker")
	checkpoints := filepath.Join(base, "checkpoints")
	cache := filepath.Join(state, "model-cache")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.Mkdir(nas, 0700))
	must(os.Mkdir(checkpoints, 0700))
	makeModel := func(name string, bytes int) Model {
		data := []byte(strings.Repeat(name[:1], bytes))
		must(os.WriteFile(filepath.Join(nas, name), data, 0600))
		return Model{Name: name, Path: name, Engine: "fake", Platforms: []string{"fake"}, MemoryGB: 1,
			SizeBytes: int64(bytes), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	}
	pin := makeModel("pin", 1024)
	restart := makeModel("restart", 3*1024*1024)
	network := makeModel("network", 3*1024*1024)
	urgent := makeModel("urgent", 3*1024*1024)
	bad := makeModel("bad", 1024)
	bad.SHA256 = strings.Repeat("0", 64)
	cfg := ModelConfig{Idle: time.Second, Wait: testWaitTimeout, Models: []Model{pin, restart, network, urgent, bad}}
	clock := &manualWorkerClock{now: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	open := func() *Queue {
		q, err := Open(filepath.Join(base, "queue.db"), nas, clock)
		must(err)
		must(q.ConfigureModels(cfg))
		q.SetRestartGrace(time.Minute)
		return q
	}
	q := open()
	var apiMu sync.RWMutex
	handler := API{Queue: q, Token: "copy-fault-test"}.Handler()
	server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiMu.RLock()
		defer apiMu.RUnlock()
		handler.ServeHTTP(w, r)
	}))
	client := Client{URL: server.URL, Token: "copy-fault-test", HTTP: &http.Client{Timeout: testWaitTimeout}}
	call := func(method, path string, in, out any) {
		t.Helper()
		must(client.Call(context.Background(), method, path, in, out))
	}
	snapshot := func() Snapshot { t.Helper(); var s Snapshot; call("GET", "/api/inspection", nil, &s); return s }
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.NewTimer(testWaitTimeout)
		defer deadline.Stop()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			if condition() {
				return
			}
			select {
			case <-deadline.C:
				t.Fatalf("%s: timed out: %+v", label, snapshot())
			case <-tick.C:
				clock.Advance(5 * time.Millisecond)
			}
		}
	}
	var gateMu sync.Mutex
	var entered, release, cancelled chan struct{}
	chunks := 0
	arm := func() {
		gateMu.Lock()
		defer gateMu.Unlock()
		entered, release, cancelled = make(chan struct{}), make(chan struct{}), make(chan struct{})
		chunks = 0
	}
	hook := func(ctx context.Context) {
		gateMu.Lock()
		chunks++
		if entered == nil || chunks != 2 {
			gateMu.Unlock()
			return
		}
		a, b, c := entered, release, cancelled
		gateMu.Unlock()
		close(a)
		select {
		case <-b:
		case <-ctx.Done():
			close(c)
		}
	}
	waitChannel := func(label string, ch <-chan struct{}) {
		t.Helper()
		wait(label, func() bool {
			select {
			case <-ch:
				return true
			default:
				return false
			}
		})
	}
	var loads, invalid sync.Map
	transport := &partitionTransport{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := Worker{Client: Client{URL: server.URL, Token: client.Token, HTTP: &http.Client{Transport: transport, Timeout: testWaitTimeout}},
		Machine: machine("copy-worker", "fake"), CheckpointRoot: checkpoints, ModelRoot: nas, StateDir: state,
		CacheLimit: 4 * 1024 * 1024, Clock: RealClock{}, Interval: 5 * time.Millisecond, ModelConfig: cfg,
		beforeModelCopy: hook, modelEngine: func(Model) Engine { return &auditedModelEngine{root: cache, loads: &loads, invalid: &invalid} }}
	go func() { done <- w.Run(ctx) }()
	finish := filepath.Join(base, "finish")
	t.Cleanup(func() {
		transport.down.Store(false)
		gateMu.Lock()
		if release != nil {
			select {
			case <-release:
			default:
				close(release)
			}
		}
		gateMu.Unlock()
		_ = os.WriteFile(finish, nil, 0600)
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("copy worker did not stop")
		}
		server.Close()
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	call("POST", "/api/models/pin", ModelPin{Model: pin.Name, Project: "fault", Machine: w.Machine.ID}, nil)
	pinReady := func() bool {
		for _, s := range snapshot().LoadedModels {
			if s.Model == pin.Name && s.State == "ready" {
				return true
			}
		}
		return false
	}
	wait("verified pin warmup", pinReady)
	assertPin := func() {
		t.Helper()
		if !pinReady() {
			t.Fatal("pin lost its loaded engine")
		}
		count, ok := loads.Load(pin.Name)
		if !ok || count.(*atomic.Int32).Load() != 1 {
			t.Fatal("pin was unloaded/reloaded")
		}
		must(verifyModel(filepath.Join(cache, cacheKey(pin)), pin))
	}
	syncModel := func(m Model, stop bool) {
		call("POST", "/api/models/sync", SyncRequest{Model: m.Name, To: "all", Cancel: stop}, nil)
	}
	cached := func(m Model) bool {
		for _, worker := range snapshot().Machines {
			if contains(worker.CachedModels, m.Name) {
				return true
			}
		}
		return false
	}
	assertPartial := func(m Model) {
		t.Helper()
		if cached(m) {
			t.Fatal("partial reported as cached")
		}
		st, err := os.Stat(filepath.Join(cache, cacheKey(m)+".partial"))
		must(err)
		if st.Size() != 1024*1024 {
			t.Fatalf("durable prefix=%d", st.Size())
		}
		if _, err := os.Stat(filepath.Join(cache, cacheKey(m))); !os.IsNotExist(err) {
			t.Fatal("partial published", err)
		}
		if _, ok := loads.Load(m.Name); ok {
			t.Fatal("engine loaded partial copy")
		}
	}
	// Queue restart mid-copy keeps the same pin process and persistent sync.
	arm()
	syncModel(restart, false)
	waitChannel("queue restart copy gate", entered)
	assertPartial(restart)
	apiMu.Lock()
	must(q.Close())
	q = open()
	handler = API{Queue: q, Token: client.Token}.Handler()
	apiMu.Unlock()
	if len(snapshot().SyncRequests) != 1 || len(snapshot().Pins) != 1 {
		t.Fatal("restart lost sync/pin")
	}
	close(release)
	wait("copy across queue restart", func() bool { return cached(restart) })
	must(verifyModel(filepath.Join(cache, cacheKey(restart)), restart))
	assertPin()
	clock.Advance(2 * time.Minute)
	wait("pin survives idle expiry", pinReady)
	assertPin()
	// Isolated API network loss pauses the copy even while the NAS is readable.
	arm()
	syncModel(network, false)
	waitChannel("network copy gate", entered)
	assertPartial(network)
	syncModel(restart, true) // avoid recopying an explicitly evicted completed target
	transport.down.Store(true)
	waitChannel("network failure cancels background I/O", cancelled)
	assertPartial(network)
	assertPin()
	syncModel(network, true)
	transport.down.Store(false)
	wait("reconnect with paused prefix", func() bool {
		for _, m := range snapshot().Machines {
			for _, r := range m.CopyReports {
				if r.Model == network.Name && r.State == "paused" && r.Bytes == 1024*1024 {
					return true
				}
			}
		}
		return false
	})
	close(release)
	syncModel(network, false)
	wait("network copy resumes", func() bool { return cached(network) })
	must(verifyModel(filepath.Join(cache, cacheKey(network)), network))
	assertPin()
	// Production starts while an unrelated NAS syscall is still at the gate.
	arm()
	syncModel(network, true)
	syncModel(urgent, false)
	waitChannel("production copy gate", entered)
	assertPartial(urgent)
	s := spec("production")
	s.Needs.MemoryGB = 1
	s.AllowedMachines = []string{w.Machine.ID}
	s.Run.Command = []string{"sh", "-c", `echo START; while [ ! -f "$FINISH" ]; do sleep 0.01; done; echo FINISHED`}
	s.Run.Env = map[string]string{"FINISH": finish}
	var job Job
	call("POST", "/api/jobs", s, &job)
	waitChannel("production pauses copy", cancelled)
	wait("production starts once", func() bool {
		var logs []LogChunk
		call("GET", "/api/jobs/"+job.ID+"/logs", nil, &logs)
		for _, l := range logs {
			if strings.Contains(l.Text, "START") {
				return true
			}
		}
		return false
	})
	assertPartial(urgent)
	assertPin()
	must(os.WriteFile(finish, nil, 0600))
	wait("production succeeds once", func() bool {
		for _, j := range snapshot().Jobs {
			if j.ID == job.ID {
				return j.State == "succeeded" && j.Attempt == 1 && j.Starts == 1
			}
		}
		return false
	})
	close(release)
	wait("idle copy resumes after production", func() bool { return cached(urgent) })
	must(verifyModel(filepath.Join(cache, cacheKey(urgent)), urgent))
	assertPin()
	// A real call loads only the verified local publication. Bad master bytes
	// must fail before Engine.Load, rather than merely returning fake success.
	modelCall := func(m Model) int {
		request, err := http.NewRequest("POST", server.URL+"/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"test"}]}`, m.Name)))
		must(err)
		request.Header.Set("Authorization", "Bearer "+client.Token)
		request.Header.Set("X-Myce-Project", "fault")
		request.Header.Set("X-Myce-Priority", "production")
		response, err := (&http.Client{Timeout: testWaitTimeout}).Do(request)
		must(err)
		defer response.Body.Close()
		_, err = io.Copy(io.Discard, response.Body)
		must(err)
		return response.StatusCode
	}
	if code := modelCall(urgent); code != 200 {
		t.Fatal("verified model call failed", code)
	}
	if code := modelCall(bad); code < 400 {
		t.Fatal("corrupt model call succeeded", code)
	}
	if _, ok := loads.Load(bad.Name); ok {
		t.Fatal("unverified master reached Engine.Load")
	}
	invalid.Range(func(name, err any) bool { t.Errorf("invalid engine load %s: %s", name, err); return true })
	assertPin()
	var logs []LogChunk
	call("GET", "/api/jobs/"+job.ID+"/logs", nil, &logs)
	var text strings.Builder
	for _, l := range logs {
		text.WriteString(l.Text)
	}
	if strings.Count(text.String(), "START") != 1 || strings.Count(text.String(), "FINISHED") != 1 || len(snapshot().Jobs) != 1 {
		t.Fatal("lost/duplicate job", text.String())
	}
	t.Log("queue restart mid-copy, worker partition mid-copy, production during pre-copy; pin loaded once; verified local engine paths; one job completed once; bad master never loaded")
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
	w := Worker{StateDir: state, Clock: RealClock{}, Interval: 5 * time.Millisecond, Client: Client{URL: server.URL, Token: "test"}, Machine: Machine{ID: "restart-worker", GPU: "fake", FreeMemoryGB: 64}}
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
