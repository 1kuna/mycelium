package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestGeneralServerChild(t *testing.T) {
	if os.Getenv("MYCE_SERVER_CHILD") != "1" {
		return
	}
	if os.Getenv("MYCE_TOKEN") != "" {
		os.Exit(9)
	}
	mode := os.Getenv("MYCE_SERVER_MODE")
	if mode == "crash" {
		os.Exit(7)
	}
	if mode == "ignore" {
		signal.Ignore(syscall.SIGTERM)
	}
	port := os.Args[len(os.Args)-1]
	streamRelease := make(chan struct{})
	var releaseOnce sync.Once
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if mode == "timeout" {
				w.WriteHeader(503)
			}
			if mode == "redirect" {
				http.Redirect(w, r, "/ready-somewhere-else", http.StatusFound)
			}
			return
		}
		w.Header().Set("X-Engine-Header", "preserved")
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.URL.Path == "/release-stream" {
			releaseOnce.Do(func() { close(streamRelease) })
			return
		}
		if r.URL.Path == "/stream" {
			fmt.Fprint(w, "first\n")
			w.(http.Flusher).Flush()
			select {
			case <-streamRelease:
			case <-r.Context().Done():
				return
			}
			fmt.Fprint(w, "last\n")
			return
		}
		data, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "%s|%s|%s|%s|%s|%s", r.Method, r.URL.RequestURI(), r.Header.Get("X-Custom"), os.Getenv("CUDA_VISIBLE_DEVICES"), os.Getenv("MYCE_TOKEN"), data)
	})
	if err := http.ListenAndServe("127.0.0.1:"+port, h); err != nil {
		os.Exit(8)
	}
	os.Exit(0)
}
func serverTestModel(t *testing.T, mode string) Model {
	t.Helper()
	exe, _ := os.Executable()
	dir := t.TempDir()
	launcher := filepath.Join(dir, "test-server")
	script := fmt.Sprintf("#!/bin/sh\nexport MYCE_SERVER_CHILD=1 MYCE_SERVER_MODE='%s'\nexec '%s' -test.run=^TestGeneralServerChild$ -- \"$@\"\n", mode, strings.ReplaceAll(exe, "'", "'\\''"))
	if err := os.WriteFile(launcher, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return Model{Name: "general", Engine: "server", Path: "unused", MemoryGB: 1, Platforms: []string{"fake"}, StartCommand: []string{launcher, "{port}"}, HealthURL: "http://127.0.0.1:{port}/health", ReadyTimeout: testWaitTimeout, StopTimeout: 50 * time.Millisecond, Device: "2", DeviceEnv: map[string]string{"CUDA_VISIBLE_DEVICES": "{device}"}}
}
func TestGeneralServerLifecycle(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "secret")
	for _, mode := range []string{"ready", "timeout", "redirect", "crash", "ignore"} {
		t.Run(mode, func(t *testing.T) {
			m := serverTestModel(t, mode)
			if mode == "timeout" || mode == "redirect" {
				m.ReadyTimeout = 160 * time.Millisecond
			}
			e := NewModelEngine(m, t.TempDir(), RealClock{})
			ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
			defer cancel()
			err := e.Load(ctx, m)
			if mode == "crash" || mode == "timeout" || mode == "redirect" {
				if err == nil {
					t.Fatal("load unexpectedly ready")
				}
				if err = e.Unload(); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			res, err := e.(CallRequestEngine).CallRequest(ctx, "PATCH", "/speech?x=1", []byte("audio"), http.Header{"X-Custom": []string{"value"}, "Authorization": []string{"secret"}})
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if string(b) != "PATCH|/speech?x=1|value|2||audio" {
				t.Fatalf("passthrough changed: %s", b)
			}
			unloadServer(t, e)
		})
	}
}
func TestGeneralServerQueuePassthroughAndStreaming(t *testing.T) {
	q, _, machine := modelFixture(t)
	model := serverTestModel(t, "ready")
	engine := NewModelEngine(model, t.TempDir(), RealClock{})
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if e := engine.Load(ctx, model); e != nil {
		t.Fatal(e)
	}
	defer engine.Unload()
	rt := &modelRuntime{models: map[string]*workerModel{"test": {engine: engine, state: "ready"}}, session: machine.Session, token: "token"}
	worker := httptest.NewServer(http.HandlerFunc(rt.serve))
	defer worker.Close()
	machine.ModelURL = worker.URL
	check(t, q, machine, nil)
	call := addModelCall(t, q, "production")
	reportModel(t, q, machine, "ready")
	q.finishCall(call.ID)
	api := httptest.NewServer(API{Queue: q, Token: "token"}.Handler())
	defer api.Close()
	for _, path := range []string{"/speech?x=1", "/stream", "/files/a%3Fb%2F%25?x=1"} {
		req, _ := http.NewRequestWithContext(ctx, "PATCH", api.URL+"/models/test"+path, strings.NewReader("audio"))
		req.Header.Set("Authorization", "Bearer token")
		req.Header.Set("X-Myce-Project", "verso")
		req.Header.Set("X-Myce-Priority", "production")
		req.Header.Set("X-Custom", "value")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != 200 || res.Header.Get("X-Engine-Header") != "preserved" {
			t.Fatal(res.Status, res.Header)
		}
		if path == "/stream" {
			b := make([]byte, 6)
			if _, err = io.ReadFull(res.Body, b); err != nil || string(b) != "first\n" {
				t.Fatal("first stream chunk", string(b), err)
			}
			// Release the remainder only after observing the first chunk: proves
			// forwarding without depending on a short wall-clock sleep.
			release, err := engine.(CallRequestEngine).CallRequest(ctx, "POST", "/release-stream", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			release.Body.Close()
		}
		b, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if path == "/stream" && string(b) != "last\n" {
			t.Fatal("last stream chunk", string(b))
		}
		if path != "/stream" && string(b) != "PATCH|"+path+"|value|2||audio" {
			t.Fatal(string(b))
		}
	}
}
func TestVLLMPresetContract(t *testing.T) {
	m := Model{Name: "judge", Engine: "vllm", Path: "/local/weights", VLLM: &VLLMPreset{ServedName: "atlas-ornith35-fp8-production", MaxContext: 262144, KVCacheDType: "fp8", GPUMemoryUtilization: .65}, Args: []string{"--reasoning-parser", "qwen3", "--tool-call-parser", "qwen3_coder"}}
	argv, _, _, env, err := serverCommand(m, 9123)
	if err != nil {
		t.Fatal(err)
	}
	command := strings.Join(argv, " ")
	for _, want := range []string{"vllm serve /local/weights", "--max-model-len 262144", "--kv-cache-dtype fp8", "--gpu-memory-utilization 0.65", "--reasoning-parser qwen3", "--tool-call-parser qwen3_coder"} {
		if !strings.Contains(command, want) {
			t.Fatal(command)
		}
	}
	if !strings.Contains(strings.Join(env, "\n"), "HF_HUB_OFFLINE=1") {
		t.Fatal("missing offline enforcement")
	}
}
func TestServerDeviceEligibility(t *testing.T) {
	model := Model{Platforms: []string{"xpu"}, Device: "0"}
	if modelFitsMachine(model, &Machine{GPU: "xpu", Device: "1"}) || !modelFitsMachine(model, &Machine{GPU: "xpu", Device: "0"}) {
		t.Fatal("device binding ignored")
	}
}

func TestGeneralServerRouteRequiresToken(t *testing.T) {
	q, _, _ := modelFixture(t)
	for _, token := range []string{"", "wrong"} {
		req := httptest.NewRequest("POST", "/models/test/infer", strings.NewReader("input"))
		req.Header.Set("X-Myce-Project", "attacker")
		req.Header.Set("X-Myce-Priority", "production")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		API{Queue: q, Token: "token"}.Handler().ServeHTTP(w, req)
		if w.Code != 401 || len(q.calls) != 0 || len(q.s.ModelSlots) != 0 {
			t.Fatalf("unauthorized model work: %d", w.Code)
		}
	}
}
func TestStopCommandDescendantsAreCleaned(t *testing.T) {
	m := serverTestModel(t, "ready")
	dir := t.TempDir()
	marker := filepath.Join(dir, "helper-group")
	m.StopTimeout = testWaitTimeout
	m.StopCommand = []string{"sh", "-c", "echo $$ > '" + marker + "'; sleep 60 &"}
	e := NewModelEngine(m, dir, RealClock{})
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := e.Load(ctx, m); err != nil {
		t.Fatal(err)
	}
	defer e.Unload()
	if err := e.Unload(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	members, err := workerGroupMembers(pid)
	if err != nil || len(members) != 0 {
		t.Fatal("stop helper survived", members, err)
	}
}
func TestResolveCUDAUsesStableUUID(t *testing.T) {
	seen := ""
	uuid, err := resolveCUDADevice(context.Background(), "0", func(_ context.Context, name string, args ...string) ([]byte, error) {
		seen = name + " " + strings.Join(args, " ")
		return []byte("GPU-1234-abcd\n"), nil
	})
	if err != nil || uuid != "GPU-1234-abcd" || !strings.Contains(seen, "--id=0 --query-gpu=uuid") {
		t.Fatal(uuid, seen, err)
	}
	for _, bad := range []string{"", "GPU-one\nGPU-two", "[N/A]"} {
		if _, err = resolveCUDADevice(context.Background(), "0", func(context.Context, string, ...string) ([]byte, error) { return []byte(bad), nil }); err == nil {
			t.Fatal("accepted invalid physical identity")
		}
	}
}

func TestGeneralServerUsesCatalogEnvironment(t *testing.T) {
	m := serverTestModel(t, "ready")
	recipe, err := snapshotEnvironment(EnvironmentRecipe{Binary: m.StartCommand[0]}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	m.StartCommand = []string{filepath.Base(m.StartCommand[0]), "{port}"}
	m.Environment = "speech-runtime"
	source := filepath.Join(t.TempDir(), "weights")
	data := []byte("immutable weights")
	if err = os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	m.Path = source
	m.SizeBytes = int64(len(data))
	m.SHA256 = fmt.Sprintf("%x", sha256.Sum256(data))
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}}
	w := Worker{Machine: Machine{Session: "session", GPU: "fake"}, Client: Client{Token: "token"}, ModelConfig: ModelConfig{Models: []Model{m}, Wait: testWaitTimeout}, Clock: RealClock{}, environmentCache: cache, MemorySource: func(context.Context) (float64, error) { return 10, nil }}
	rt, err := startModels(w, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer rt.close()
	rt.sync([]ModelSlot{{Model: m.Name, Session: "session", State: "loading", EnvironmentRecipe: &recipe}})
	deadline := time.Now().Add(testWaitTimeout)
	for {
		reports, _ := rt.report()
		if len(reports) > 0 {
			if reports[0].State == "ready" {
				break
			}
			if reports[0].State == "failed" {
				t.Fatal(reports[0].Error)
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("environment server did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	req := httptest.NewRequest("PUT", "/models/general/environment", strings.NewReader("input"))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("X-Myce-Session", "session")
	res := httptest.NewRecorder()
	rt.serve(res, req)
	if res.Code != 200 || !strings.Contains(res.Body.String(), "PUT|/environment") {
		t.Fatal(res.Code, res.Body.String())
	}
}

func TestStopCommandTimeoutEscalates(t *testing.T) {
	m := serverTestModel(t, "ready")
	m.StopCommand = []string{"sh", "-c", "trap '' TERM; sleep 60"}
	m.StopTimeout = 150 * time.Millisecond
	dir := t.TempDir()
	e := NewModelEngine(m, dir, RealClock{})
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := e.Load(ctx, m); err != nil {
		t.Fatal(err)
	}
	defer e.Unload()
	unloadServer(t, e)
	records, err := filepath.Glob(filepath.Join(dir, "engine-*.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range records {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var st workerState
		if err = decodeWorkerJSON(data, &st); err != nil || st.PID != 0 {
			t.Fatal("cleanup retained live group record", st, err)
		}
	}
}

// The stop grace is a product setting; observing exit includes OS scheduling and
// process inventory. A deadline prevents hangs without timing the observer.
func unloadServer(t *testing.T, e Engine) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- e.Unload() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatalf("server process group did not stop within %s", testWaitTimeout)
	}
}

func TestStopCommandExpiredGraceStillCleansServer(t *testing.T) {
	m := serverTestModel(t, "ignore")
	m.StopCommand = []string{"sh", "-c", "sleep 60"}
	engine := NewModelEngine(m, t.TempDir(), RealClock{}).(*LlamaEngine)
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := engine.Load(ctx, m); err != nil {
		t.Fatal(err)
	}
	defer engine.Unload()
	group := engine.cmd.Process.Pid
	// Deterministically expire grace before the stop helper can launch. This is
	// the same path slow fsync/startup took in the loaded-Mac repetition.
	engine.stopTimeout = time.Nanosecond
	unloadServer(t, engine)
	members, err := workerGroupMembers(group)
	if err != nil || len(members) != 0 || engine.cmd != nil {
		t.Fatal("expired grace left server alive", members, err)
	}
}
