package myce

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Engine owns one model process. The OpenAI body and endpoint pass through intact.
type Engine interface {
	Load(context.Context, Model) error
	Call(context.Context, string, []byte) (*http.Response, error)
	Unload() error
}
type FakeEngine struct{ Model string }

func (f *FakeEngine) Load(_ context.Context, m Model) error { f.Model = m.Name; return nil }
func (f *FakeEngine) Unload() error                         { return nil }
func (f *FakeEngine) Call(_ context.Context, path string, data []byte) (*http.Response, error) {
	var req struct {
		Stream bool `json:"stream"`
	}
	_ = json.Unmarshal(data, &req)
	content := "application/json"
	var body string
	switch path {
	case "/v1/embeddings":
		body = fmt.Sprintf(`{"object":"list","model":%q,"data":[{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}],"usage":{"prompt_tokens":1,"total_tokens":1}}`, f.Model)
	case "/v1/completions":
		body = fmt.Sprintf(`{"id":"fake","object":"text_completion","model":%q,"choices":[{"index":0,"text":"fake answer","finish_reason":"stop"}]}`, f.Model)
	default:
		if req.Stream {
			content = "text/event-stream"
			body = fmt.Sprintf("data: {\"id\":\"fake\",\"object\":\"chat.completion.chunk\",\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fake answer\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n", f.Model)
		} else {
			body = fmt.Sprintf(`{"id":"fake","object":"chat.completion","model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"fake answer"},"finish_reason":"stop"}]}`, f.Model)
		}
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{content}}, Body: io.NopCloser(strings.NewReader(body))}, nil
}

// LlamaEngine starts an installed llama-server, with an explicit inference contract
// in Model.Args. It never downloads models or changes the model's generation settings.
type LlamaEngine struct {
	Dir      string
	Clock    Clock
	cmd      *exec.Cmd
	done     chan error
	url      string
	identity string
	log      *os.File
}

func (l *LlamaEngine) Load(ctx context.Context, m Model) error {
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "LLAMA_ARG_HF_") {
			return errors.New("remote llama-server model environment is forbidden; use myce model fetch")
		}
		for _, key := range []string{"LLAMA_ARG_HF_REPO=", "LLAMA_ARG_HF_FILE=", "LLAMA_ARG_MODEL_URL=", "LLAMA_ARG_MMPROJ_URL="} {
			if strings.HasPrefix(v, key) && strings.TrimPrefix(v, key) != "" {
				return errors.New("remote llama-server model environment is forbidden; use myce model fetch")
			}
		}
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return e
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	args := []string{"--model", m.Path, "--alias", m.Name, "--host", "127.0.0.1", "--port", strconv.Itoa(port), "--n-gpu-layers", "99"}
	args = append(args, m.Args...)
	l.identity = ID()
	name := "engine-" + l.identity
	shell := []string{"-c", `read gate || exit 125; exec "$@"`, "myce-supervisor-" + l.identity, "llama-server"}
	shell = append(shell, args...)
	l.cmd = exec.Command("sh", shell...)
	l.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "MYCE_TOKEN=") {
			l.cmd.Env = append(l.cmd.Env, v)
		}
	}
	l.cmd.Env = append(l.cmd.Env, "MYCE_PROCESS_ID="+l.identity)
	l.log, e = os.OpenFile(filepath.Join(l.Dir, name+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	l.cmd.Stdout = l.log
	l.cmd.Stderr = l.log
	reader, writer, e := os.Pipe()
	if e != nil {
		l.log.Close()
		return e
	}
	defer reader.Close()
	defer writer.Close()
	l.cmd.Stdin = reader
	if e = saveWorkerJSON(l.Dir, name+".json", workerState{Identity: l.identity}); e != nil {
		l.log.Close()
		return e
	}
	if e = l.cmd.Start(); e != nil {
		l.log.Close()
		return e
	}
	l.done = make(chan error, 1)
	go func() { l.done <- l.cmd.Wait() }()
	if e = saveWorkerJSON(l.Dir, name+".json", workerState{Identity: l.identity, PID: l.cmd.Process.Pid}); e != nil {
		_ = l.Unload()
		return e
	}
	if _, e = writer.Write([]byte("start\n")); e != nil {
		_ = l.Unload()
		return e
	}
	l.url = "http://127.0.0.1:" + strconv.Itoa(port)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	hc := &http.Client{Timeout: time.Second}
	for {
		req, _ := http.NewRequestWithContext(ctx, "GET", l.url+"/health", nil)
		res, err := hc.Do(req)
		if err == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			_ = l.Unload()
			return ctx.Err()
		case e := <-l.done:
			l.done = nil
			_ = l.Unload()
			return fmt.Errorf("llama-server exited during load: %v; see %s.log", e, name)
		case <-tick.C:
		}
	}
}
func (l *LlamaEngine) Call(ctx context.Context, path string, data []byte) (*http.Response, error) {
	req, e := http.NewRequestWithContext(ctx, "POST", l.url+path, bytes.NewReader(data))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}
func (l *LlamaEngine) Unload() error {
	if l.cmd == nil || l.cmd.Process == nil {
		return nil
	}
	e := syscall.Kill(-l.cmd.Process.Pid, syscall.SIGKILL)
	if e == syscall.ESRCH {
		e = nil
	}
	if l.done != nil {
		<-l.done
		l.done = nil
	}
	e = errors.Join(e, waitWorkerGroup(l.cmd.Process.Pid, l.Clock))
	if l.log != nil {
		e = errors.Join(e, l.log.Close())
		l.log = nil
	}
	if e == nil {
		e = saveWorkerJSON(l.Dir, "engine-"+l.identity+".json", workerState{Identity: l.identity})
	}
	if e == nil {
		l.cmd = nil
		l.url = ""
	}
	return e
}
func cleanupEngines(dir string, clock Clock) error {
	files, e := filepath.Glob(filepath.Join(dir, "engine-*.json"))
	if e != nil {
		return e
	}
	for _, p := range files {
		data, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		var st workerState
		if e = decodeWorkerJSON(data, &st); e != nil {
			return e
		}
		s := workerStore{dir: dir, state: st}
		if e = s.cleanupOrphan(clock); e != nil {
			return e
		}
		st.PID = 0
		if e = saveWorkerJSON(dir, filepath.Base(p), st); e != nil {
			return e
		}
	}
	return nil
}

type workerModel struct {
	model          Model
	engine         Engine
	state, err     string
	inflight       int
	queueFlight    int
	cancel         context.CancelFunc
	drainRequested bool
}
type modelRuntime struct {
	lastSavedSpeed      float64
	copies              map[string]*workerCopy
	copyParallel        int
	cachePins           map[string]bool
	cacheUsage          map[string]int
	cache               *ModelCache
	mu                  sync.Mutex
	models              map[string]*workerModel
	config              ModelConfig
	session, token, dir string
	clock               Clock
	server              *http.Server
	wg                  sync.WaitGroup
	url                 string
	closing             bool
	engineFactory       func(Model) Engine
	memorySource        MemorySource
	memoryMeasured      bool
	batchMemory         *atomic.Uint64
}

func startModels(w Worker, dir string) (*modelRuntime, error) {
	if e := cleanupEngines(dir, w.Clock); e != nil {
		return nil, e
	}
	address := w.ModelListen
	if address == "" {
		address = "127.0.0.1:0"
	}
	ln, e := net.Listen("tcp", address)
	if e != nil {
		return nil, e
	}
	rt := &modelRuntime{copies: map[string]*workerCopy{}, copyParallel: max(1, w.CopyParallel), models: map[string]*workerModel{}, config: w.ModelConfig, session: w.Machine.Session, token: w.Client.Token, dir: dir, clock: w.Clock, url: "http://" + ln.Addr().String()}
	root := w.CacheRoot
	if root == "" {
		root = filepath.Join(dir, "model-cache")
	}
	limit := w.CacheLimit
	if limit == 0 {
		limit = 100 * 1024 * 1024 * 1024
	}
	rt.cache = &ModelCache{Root: root, NASRoot: w.ModelRoot, Limit: limit, Clock: w.Clock}
	rt.engineFactory = w.modelEngine
	rt.cache.beforeChunk = w.beforeModelCopy
	rt.cache.Parallel = max(1, w.CopyParallel)
	rt.cache.Protected = rt.cacheProtected
	rt.cache.Usage = rt.cachePopularity
	if e = rt.cache.init(); e != nil {
		ln.Close()
		return nil, e
	}
	rt.cache.ValidateInventory(w.ModelConfig.Models)
	if data, e := os.ReadFile(filepath.Join(dir, "copy-speed.json")); e == nil {
		if e := decodeWorkerJSON(data, &rt.cache.speed); e != nil {
			log.Printf("restore model copy speed: %v", e)
		}
	}
	rt.memorySource = w.MemorySource
	rt.memoryMeasured = w.MemoryMeasured
	rt.batchMemory = w.batchMemory
	if w.ModelAdvertise != "" {
		rt.url = w.ModelAdvertise
	}
	rt.server = &http.Server{Handler: http.HandlerFunc(rt.serve), ReadHeaderTimeout: 5 * time.Second}
	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		if e := rt.server.Serve(ln); e != nil && !errors.Is(e, http.ErrServerClosed) {
			log.Printf("worker model server: %v", e)
		}
	}()
	return rt, nil
}
func (rt *modelRuntime) report() ([]ModelReport, float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := []ModelReport{}
	memory := 0.0
	for name, m := range rt.models {
		out = append(out, ModelReport{Name: name, State: m.state, Error: m.err})
		if m.state != "unloaded" && m.state != "failed" {
			memory += m.model.MemoryGB
		}
	}
	return out, memory
}
func (rt *modelRuntime) sync(plan []ModelSlot) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	desired := map[string]ModelSlot{}
	for _, s := range plan {
		desired[s.Model] = s
	}
	for name, m := range rt.models {
		s, ok := desired[name]
		m.queueFlight = s.InFlight
		if !ok && m.state == "failed" {
			m.state = "unloaded"
		}
		if !ok || s.State == "draining" {
			m.drainRequested = true
			if m.state == "loading" {
				m.cancel()
			}
			if m.state == "ready" {
				m.state = "draining"
			}
			if m.state == "draining" && m.inflight == 0 && (m.queueFlight == 0 || rt.closing) {
				rt.unload(m)
			}
		}
	}
	for _, s := range plan {
		if s.State != "loading" || s.Session != rt.session {
			continue
		}
		if m := rt.models[s.Model]; m != nil && m.state != "unloaded" {
			continue
		}
		var model Model
		for _, m := range rt.config.Models {
			if m.Name == s.Model {
				model = m
			}
		}
		if model.Name == "" {
			rt.models[s.Model] = &workerModel{state: "failed", err: "model absent from worker config"}
			continue
		}
		var engine Engine
		if rt.engineFactory != nil {
			engine = rt.engineFactory(model)
		} else if model.Engine == "fake" {
			engine = &FakeEngine{}
		} else {
			engine = &LlamaEngine{Dir: rt.dir, Clock: rt.clock}
		}
		ctx, cancel := context.WithTimeout(context.Background(), rt.config.Wait)
		m := &workerModel{model: model, engine: engine, state: "loading", cancel: cancel}
		rt.models[s.Model] = m
		rt.wg.Add(1)
		go func() {
			defer rt.wg.Done()
			rt.load(ctx, m)
		}()
	}
}

func (rt *modelRuntime) load(ctx context.Context, m *workerModel) {
	if e := rt.checkLoadMemory(ctx, m); e != nil {
		m.cancel()
		rt.mu.Lock()
		m.state = "failed"
		m.err = e.Error()
		rt.mu.Unlock()
		return
	}
	model := m.model
	var e error
	if model.Engine == "llama.cpp" || model.SizeBytes > 0 {
		model.Path, e = rt.cache.Prepare(ctx, model)
	}
	if e == nil {
		e = m.engine.Load(ctx, model)
	}
	m.cancel()
	// A failed load may have started a process. Keep its memory reserved unless
	// cleanup actually succeeds, even when the engine's own load cleanup failed.
	var cleanupErr error
	if e != nil {
		cleanupErr = m.engine.Unload()
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if cleanupErr != nil {
		m.state = "unload_failed"
		m.err = errors.Join(e, fmt.Errorf("unload: %w", cleanupErr)).Error()
	} else if e != nil {
		m.state = "failed"
		m.err = e.Error()
	} else {
		m.state = "ready"
		// Cancellation can race with successful readiness. Remember the drain
		// request instead of exposing that engine as ready until another heartbeat.
		if rt.closing || m.drainRequested {
			m.state = "draining"
			if m.inflight == 0 && (m.queueFlight == 0 || rt.closing) {
				rt.unload(m)
			}
		}
	}
}

func (rt *modelRuntime) checkLoadMemory(ctx context.Context, loading *workerModel) error {
	if rt.memorySource == nil {
		return errors.New("model load requires a free-memory source")
	}
	// The source may inspect worker/runtime state. Never invoke it under rt.mu.
	free, e := rt.memorySource(ctx)
	if e != nil {
		return fmt.Errorf("model free-memory check: %w", e)
	}
	if math.IsNaN(free) || math.IsInf(free, 0) || free < 0 {
		return errors.New("model free-memory source returned invalid memory")
	}
	if !rt.memoryMeasured && rt.batchMemory != nil {
		free -= math.Float64frombits(rt.batchMemory.Load())
	}
	rt.mu.Lock()
	for _, other := range rt.models {
		if other == loading || other.state == "unloaded" || other.state == "failed" {
			continue
		}
		if !rt.memoryMeasured || other.state == "loading" {
			free -= other.model.MemoryGB
		}
	}
	rt.mu.Unlock()
	if free < loading.model.MemoryGB {
		return fmt.Errorf("insufficient free GPU memory for model %s: need %.1f GB, available %.1f GB", loading.model.Name, loading.model.MemoryGB, max(0, free))
	}
	return nil
}
func (rt *modelRuntime) unload(m *workerModel) {
	m.state = "unloading"
	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		e := m.engine.Unload()
		rt.mu.Lock()
		defer rt.mu.Unlock()
		if e != nil {
			m.state = "unload_failed"
			m.err = "unload: " + e.Error()
		} else {
			m.state = "unloaded"
		}
	}()
}
func (rt *modelRuntime) close() error {
	rt.pauseCopies()
	rt.mu.Lock()
	rt.closing = true
	for _, m := range rt.models {
		if m.cancel != nil {
			m.cancel()
		}
		if m.state == "ready" || m.state == "draining" {
			m.state = "draining"
			if m.inflight == 0 {
				rt.unload(m)
			}
		}
	}
	rt.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e := rt.server.Shutdown(ctx)
	rt.wg.Wait()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for name, m := range rt.models {
		if m.state == "unload_failed" {
			e = errors.Join(e, fmt.Errorf("model %s: %s", name, m.err))
		}
	}
	return e
}
func (rt *modelRuntime) serve(w http.ResponseWriter, r *http.Request) {
	rt.mu.Lock()
	session := rt.session
	rt.mu.Unlock()
	if subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(rt.token)) != 1 || r.Header.Get("X-Myce-Session") != session {
		modelError(w, 401, "invalid worker session or token")
		return
	}
	if r.Method != "POST" || !contains([]string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"}, r.URL.Path) {
		modelError(w, 404, "unknown model endpoint")
		return
	}
	data, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if e != nil {
		modelError(w, 400, e.Error())
		return
	}
	var req struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(data, &req) != nil {
		modelError(w, 400, "invalid JSON")
		return
	}
	rt.mu.Lock()
	m := rt.models[req.Model]
	if m == nil || (m.state != "ready" && !(m.state == "draining" && m.queueFlight > 0)) {
		rt.mu.Unlock()
		modelError(w, 503, "model is not ready")
		return
	}
	m.inflight++
	// A routed call can arrive before the next queue check-in. Pause any idle
	// copy immediately when the worker receives actual inference work.
	for _, copy := range rt.copies {
		copy.cancel()
	}
	rt.mu.Unlock()
	defer func() {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		m.inflight--
		if m.state == "draining" && m.inflight == 0 && (m.queueFlight == 0 || rt.closing) {
			rt.unload(m)
		}
	}()
	res, e := m.engine.Call(r.Context(), r.URL.Path, data)
	if e != nil {
		modelError(w, 503, e.Error())
		return
	}
	defer res.Body.Close()
	w.Header().Set("Content-Type", res.Header.Get("Content-Type"))
	w.WriteHeader(res.StatusCode)
	buf := make([]byte, 16384)
	for {
		n, e := res.Body.Read(buf)
		if n > 0 {
			if _, err := w.Write(buf[:n]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		if e != nil {
			if e != io.EOF {
				panic(http.ErrAbortHandler)
			}
			return
		}
	}
}
