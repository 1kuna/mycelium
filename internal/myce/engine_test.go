package myce

import (
	"context"
	"errors"
	"fmt"
	"io"

	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// This child is a pretend llama-server, not a model or GPU implementation.
func TestLlamaServerCommand(t *testing.T) {
	if os.Getenv("MYCE_LLAMA_CHILD") != "1" {
		return
	}
	if os.Getenv("MYCE_TOKEN") != "" {
		t.Fatal("engine inherited fleet token")
	}
	args := os.Args
	port := ""
	for i, a := range args {
		if a == "--port" && i+1 < len(args) {
			port = args[i+1]
		}
	}
	if port == "" {
		t.Fatal("missing server port")
	}
	h := http.NewServeMux()
	h.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"status":"ok"}`) })
	h.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		data, _ := io.ReadAll(r.Body)
		w.Write(data)
	})
	if e := http.ListenAndServe("127.0.0.1:"+port, h); e != nil {
		t.Fatal(e)
	}
}
func TestLlamaAdapterAndOrphanCleanup(t *testing.T) {
	dir := t.TempDir()
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	launcher := filepath.Join(dir, "llama-server")
	script := fmt.Sprintf("#!/bin/sh\nexport MYCE_LLAMA_CHILD=1\nexec '%s' -test.run=^TestLlamaServerCommand$ -- \"$@\"\n", strings.ReplaceAll(exe, "'", "'\\''"))
	if e = os.WriteFile(launcher, []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MYCE_TOKEN", "must-not-inherit")
	engine := &LlamaEngine{Dir: dir, Clock: RealClock{}}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if e = engine.Load(ctx, Model{Name: "test", Path: "pretend.gguf"}); e != nil {
		t.Fatal(e)
	}
	defer engine.Unload()
	res, e := engine.Call(ctx, "/v1/chat/completions", []byte(`{"model":"test","messages":[],"temperature":0.7}`))
	if e != nil {
		t.Fatal(e)
	}
	data, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if string(data) != `{"model":"test","messages":[],"temperature":0.7}` {
		t.Fatal("adapter changed OpenAI request")
	}
	if e = cleanupEngines(dir, RealClock{}); e != nil {
		t.Fatal(e)
	}
	members, e := workerGroupMembers(engine.cmd.Process.Pid)
	if e != nil || len(members) != 0 {
		t.Fatalf("orphan engine survived: %v %v", members, e)
	}
	if err := engine.Unload(); err != nil {
		t.Fatal(err)
	}
	if engine.cmd != nil {
		t.Fatal("completed cleanup retained a reusable process id")
	}
	if err := engine.Unload(); err != nil {
		t.Fatal(err)
	}
}
func TestRuntimeKeepsAdmittedCallsWhileDraining(t *testing.T) {
	rt := &modelRuntime{models: map[string]*workerModel{"test": {model: Model{Name: "test"}, engine: &FakeEngine{Model: "test"}, state: "ready"}}, session: "session", token: "test"}
	rt.sync([]ModelSlot{{Attempt: "test-engine", Model: "test", Session: "session", State: "draining", InFlight: 1}})
	rt.mu.Lock()
	state := rt.models["test"].state
	rt.mu.Unlock()
	if state != "draining" {
		t.Fatal("engine unloaded before admitted call arrived")
	}
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("X-Myce-Session", "session")
	w := httptest.NewRecorder()
	rt.serve(w, req)
	if w.Code != 200 {
		t.Fatal("admitted call rejected while draining")
	}
	rt.sync([]ModelSlot{{Attempt: "test-engine", Model: "test", Session: "session", State: "draining", InFlight: 0}})
	rt.wg.Wait()
	rt.mu.Lock()
	state = rt.models["test"].state
	rt.mu.Unlock()
	if state != "unloaded" {
		t.Fatal("drained model not unloaded")
	}
}

type lifecycleEngine struct {
	started, release   chan struct{}
	loadErr, unloadErr error
	unloads            atomic.Int32
}

func (e *lifecycleEngine) Load(context.Context, Model) error {
	if e.started != nil {
		close(e.started)
	}
	if e.release != nil {
		<-e.release
	}
	return e.loadErr
}
func (e *lifecycleEngine) Call(context.Context, string, []byte) (*http.Response, error) {
	return nil, errors.New("unused")
}
func (e *lifecycleEngine) Unload() error { e.unloads.Add(1); return e.unloadErr }

func runtimeLoadFixture(t *testing.T, engine Engine) (*modelRuntime, *workerModel, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := &workerModel{model: Model{Name: "test", MemoryGB: 8}, engine: engine, state: "loading", cancel: cancel}
	rt := &modelRuntime{models: map[string]*workerModel{"test": m}, session: "session", memorySource: func(context.Context) (float64, error) { return 8, nil }}
	return rt, m, ctx
}

func TestRuntimeLoadMemoryErrorNeverStartsEngine(t *testing.T) {
	e := &lifecycleEngine{started: make(chan struct{})}
	rt, m, ctx := runtimeLoadFixture(t, e)
	rt.memorySource = func(context.Context) (float64, error) { return 0, errors.New("inventory unavailable") }
	rt.load(ctx, m)
	select {
	case <-e.started:
		t.Fatal("engine started without memory evidence")
	default:
	}
	if m.state != "failed" || !strings.Contains(m.err, "inventory unavailable") {
		t.Fatalf("memory error hidden: %+v", m)
	}
}
