package myce

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
}
func TestRuntimeKeepsAdmittedCallsWhileDraining(t *testing.T) {
	rt := &modelRuntime{models: map[string]*workerModel{"test": {model: Model{Name: "test"}, engine: &FakeEngine{Model: "test"}, state: "ready"}}, session: "session", token: "test"}
	rt.sync([]ModelSlot{{Model: "test", Session: "session", State: "draining", InFlight: 1}})
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
	rt.sync([]ModelSlot{{Model: "test", Session: "session", State: "draining", InFlight: 0}})
	rt.wg.Wait()
	rt.mu.Lock()
	state = rt.models["test"].state
	rt.mu.Unlock()
	if state != "unloaded" {
		t.Fatal("drained model not unloaded")
	}
}
