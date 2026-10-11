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
	"net/url"
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

// ServerEngine shares process supervision with the llama.cpp preset.
type ServerEngine = LlamaEngine

func NewModelEngine(m Model, dir string, clock Clock) Engine {
	if m.Engine == "fake" {
		return &FakeEngine{}
	}
	return &ServerEngine{Dir: dir, Clock: clock}
}

type LlamaEngine struct {
	HostStateDir string
	stopCommand  []string
	stopTimeout  time.Duration
	env          []string

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
	if contains(m.Platforms, "cuda") {
		selector := m.DeviceEnv["CUDA_VISIBLE_DEVICES"]
		selector = strings.ReplaceAll(selector, "{device}", m.Device)
		if selector != "" {
			uuid, err := ResolveCUDADevice(ctx, selector)
			if err != nil {
				return err
			}
			env := map[string]string{}
			for k, v := range m.DeviceEnv {
				env[k] = v
			}
			m.DeviceEnv = env
			m.DeviceEnv["CUDA_VISIBLE_DEVICES"] = uuid
		}
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return e
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	argv, health, stop, env, err := serverCommand(m, port)
	if err != nil {
		return err
	}
	l.stopCommand, l.stopTimeout, l.env = stop, m.StopTimeout, env
	if l.stopTimeout == 0 {
		l.stopTimeout = 10 * time.Second
	}
	if m.ReadyTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.ReadyTimeout)
		defer cancel()
	}
	unlock, gateErr := hostLaunchGate(l.HostStateDir)
	if gateErr != nil {
		return gateErr
	}
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	l.identity = ID()
	name := "engine-" + l.identity
	shell := []string{"-c", `read gate || exit 125; exec "$@"`, "myce-supervisor-" + l.identity}
	shell = append(shell, argv...)
	l.cmd = exec.Command("sh", shell...)
	l.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	l.cmd.Env = env
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
	if e = saveWorkerJSON(l.Dir, name+".json", workerState{Identity: l.identity, LaunchPending: true}); e != nil {
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
	unlock()
	unlock = nil
	l.url = "http://127.0.0.1:" + strconv.Itoa(port)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	hc := &http.Client{
		Timeout:       time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	for {
		req, _ := http.NewRequestWithContext(ctx, "GET", health, nil)
		res, err := hc.Do(req)
		if err == nil {
			res.Body.Close()
			if res.StatusCode >= 200 && res.StatusCode < 300 {
				select {
				case e := <-l.done:
					l.done = nil
					_ = l.Unload()
					return fmt.Errorf("server exited as health became ready: %v; see %s.log", e, name)
				default:
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			_ = l.Unload()
			return ctx.Err()
		case e := <-l.done:
			l.done = nil
			_ = l.Unload()
			return fmt.Errorf("server exited during load: %v; see %s.log", e, name)
		case <-tick.C:
		}
	}
}
func (l *LlamaEngine) Call(ctx context.Context, path string, data []byte) (*http.Response, error) {
	return l.Request(ctx, "POST", path, http.Header{"Content-Type": []string{"application/json"}}, bytes.NewReader(data), int64(len(data)))
}
func (l *LlamaEngine) Request(ctx context.Context, method, path string, headers http.Header, body io.Reader, contentLength int64) (*http.Response, error) {
	req, e := http.NewRequestWithContext(ctx, method, l.url+path, body)
	if e != nil {
		return nil, e
	}
	req.Header = proxyHeaders(headers)
	req.ContentLength = contentLength
	return modelHTTPClient.Do(req)
}

func (l *LlamaEngine) Unload() error {
	if l.cmd == nil || l.cmd.Process == nil {
		return nil
	}
	// Give the optional control command and process group one bounded grace period.
	grace, cancel := context.WithTimeout(context.Background(), l.stopTimeout)
	defer cancel()
	stopErr := l.runStopCommand(grace)
	_ = syscall.Kill(-l.cmd.Process.Pid, syscall.SIGTERM)
	if l.done != nil {
		select {
		case <-l.done:
			l.done = nil
		case <-grace.Done():
		}
	}
	e := signalWorkerGroup(l.cmd.Process.Pid, syscall.SIGKILL)
	if l.done != nil {
		<-l.done
		l.done = nil
	}
	e = errors.Join(e, stopErr, waitWorkerGroup(l.cmd.Process.Pid, l.Clock))
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
func (l *LlamaEngine) runStopCommand(grace context.Context) error {
	if reason, _ := HostDisabled(l.HostStateDir); reason != "" {
		return nil
	}
	if len(l.stopCommand) > 0 {
		identity := ID()
		name := "engine-stop-" + identity
		argv := append([]string{"-c", `read gate || exit 125; exec "$@"`, "myce-supervisor-" + identity}, l.stopCommand...)
		cmd := exec.CommandContext(grace, "sh", argv...)
		cmd.Env = append(append([]string{}, l.env...), "MYCE_PROCESS_ID="+identity)
		reader, writer, pipeErr := os.Pipe()
		if pipeErr != nil {
			return pipeErr
		}
		defer reader.Close()
		defer writer.Close()
		cmd.Stdin = reader
		cmd.Stdout = l.log
		cmd.Stderr = l.log
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		cmd.WaitDelay = time.Second
		if err := saveWorkerJSON(l.Dir, name+".json", workerState{Identity: identity}); err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			// The shared stop grace may expire while persisting the helper's
			// recovery record, before Start. That is normal escalation: no
			// helper started, and Unload must still stop the server group.
			if grace.Err() != nil && errors.Is(err, grace.Err()) {
				return nil
			}
			return err
		}
		if err := saveWorkerJSON(l.Dir, name+".json", workerState{Identity: identity, PID: cmd.Process.Pid}); err != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
			return err
		}
		_, gateErr := writer.Write([]byte("start\n"))
		// A helper killed at the grace deadline can close its gate first.
		// Still inspect its group below; a broken pipe alone is expected.
		if grace.Err() != nil && errors.Is(gateErr, syscall.EPIPE) {
			gateErr = nil
		}
		if gateErr != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		_ = cmd.Wait()
		killErr := signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL)
		if err := errors.Join(gateErr, killErr, waitWorkerGroup(cmd.Process.Pid, l.Clock)); err != nil {
			return err
		}
		if err := saveWorkerJSON(l.Dir, name+".json", workerState{Identity: identity}); err != nil {
			return err
		}
	}
	return nil
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
	model              Model
	engine             Engine
	state, err         string
	inflight           int
	queueFlight        int
	recipe             *EnvironmentRecipe
	releaseEnvironment func()
	cancel             context.CancelFunc
	drainRequested     bool
}
type modelRuntime struct {
	hostStateDir          string
	lastSavedSpeed        float64
	copies                map[string]*workerCopy
	copyParallel          int
	cachePins             map[string]bool
	cacheUsage            map[string]int
	cache                 *ModelCache
	mu                    sync.Mutex
	models                map[string]*workerModel
	config                ModelConfig
	session, token, dir   string
	clock                 Clock
	server                *http.Server
	wg                    sync.WaitGroup
	url                   string
	closing               bool
	engineFactory         func(Model) Engine
	hostMemorySource      MemorySource
	unifiedMemory         bool
	memorySource          MemorySource
	memoryMeasured        bool
	batchMemory           *atomic.Uint64
	environmentCache      *EnvironmentCache
	environmentSourceRoot string
	device, platform      string
	machine               string
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
	rt := &modelRuntime{hostStateDir: w.HostStateDir, copies: map[string]*workerCopy{}, copyParallel: max(1, w.CopyParallel), models: map[string]*workerModel{}, config: w.ModelConfig, session: w.Machine.Session, token: w.Client.Token, dir: dir, clock: w.Clock, url: "http://" + ln.Addr().String()}
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
	rt.device, rt.platform = w.Machine.Device, w.Machine.GPU
	rt.machine = w.Machine.ID
	rt.environmentCache = w.environmentCache
	rt.environmentSourceRoot = w.EnvironmentSourceRoot
	rt.hostMemorySource = w.HostMemorySource
	rt.unifiedMemory = w.Machine.UnifiedMemory
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
		out = append(out, ModelReport{Name: name, State: m.state, Error: m.err, InFlight: m.inflight, CallsMeasured: true})
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
			engine = NewModelEngine(model, rt.dir, rt.clock)
			if owned, ok := engine.(*LlamaEngine); ok {
				owned.HostStateDir = rt.hostStateDir
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), rt.config.Wait)
		m := &workerModel{model: model, engine: engine, state: "loading", cancel: cancel, recipe: s.EnvironmentRecipe}
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
	model := withPlacementEnvironment(m.model, Machine{ID: rt.machine, GPU: rt.platform, Device: rt.device})
	var e error
	if rt.device != "" {
		model, e = WithModelDevice(model, rt.platform, rt.device)
	}
	if e == nil && model.Environment != "" {
		if m.recipe == nil {
			e = errors.New("model environment recipe unavailable")
		} else {
			recipe := *m.recipe
			if recipe.BinaryRelative != "" && rt.environmentSourceRoot != "" {
				recipe.Binary = filepath.Join(rt.environmentSourceRoot, recipe.BinaryRelative)
			}
			model, m.releaseEnvironment, e = PrepareModelEnvironment(ctx, model, recipe.Platform, EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{model.Environment: {recipe.Platform: recipe}}}, rt.environmentCache)
		}
	}
	if e == nil && (model.Engine == "llama.cpp" || model.SizeBytes > 0) {
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
		if m.releaseEnvironment != nil {
			m.releaseEnvironment()
			m.releaseEnvironment = nil
		}
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
	if loading.model.HostMemoryGB > 0 {
		if rt.hostMemorySource == nil {
			return errors.New("model load requires host RAM source")
		}
		free, e := rt.hostMemorySource(ctx)
		need := loading.model.HostMemoryGB
		if rt.unifiedMemory {
			need += loading.model.MemoryGB
		}
		if e != nil || !validMemoryValue(free) || free < need {
			return fmt.Errorf("insufficient host RAM for model %s: need %.1f GiB, available %.1f GiB: %v", loading.model.Name, need, free, e)
		}
	}
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
			if m.releaseEnvironment != nil {
				m.releaseEnvironment()
				m.releaseEnvironment = nil
			}
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
	if reason, err := HostDisabled(rt.hostStateDir); err != nil || reason != "" {
		modelError(w, 503, "host disabled or local admission unavailable")
		return
	}
	generic := strings.HasPrefix(r.URL.Path, "/models/")
	endpoint := r.URL.RequestURI()
	if !generic && (r.Method != "POST" || !contains([]string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"}, r.URL.Path)) {
		modelError(w, 404, "unknown model endpoint")
		return
	}
	var req struct {
		Model string `json:"model"`
	}
	var data []byte
	var e error
	if generic {
		parts := strings.SplitN(strings.TrimPrefix(r.URL.EscapedPath(), "/models/"), "/", 2)
		if len(parts) != 2 {
			modelError(w, 400, "expected model and path")
			return
		}
		req.Model, e = url.PathUnescape(parts[0])
		if e != nil {
			modelError(w, 400, e.Error())
			return
		}
		endpoint = "/" + parts[1]
		if r.URL.RawQuery != "" {
			endpoint += "?" + r.URL.RawQuery
		}
	} else {
		data, e = io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
		if e != nil {
			modelError(w, 400, e.Error())
			return
		}
	}
	if !generic && json.Unmarshal(data, &req) != nil {
		modelError(w, 400, "invalid JSON")
		return
	}
	unlock, err := hostLaunchGate(rt.hostStateDir)
	if err != nil {
		modelError(w, 503, "host disabled or local admission unavailable")
		return
	}
	rt.mu.Lock()
	m := rt.models[req.Model]
	if m == nil || (m.state != "ready" && !(m.state == "draining" && m.queueFlight > 0)) {
		rt.mu.Unlock()
		unlock()
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
	unlock()
	defer func() {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		m.inflight--
		if m.state == "draining" && m.inflight == 0 && (m.queueFlight == 0 || rt.closing) {
			rt.unload(m)
		}
	}()
	var res *http.Response
	if generic {
		engine, ok := m.engine.(StreamRequestEngine)
		if !ok {
			modelError(w, 503, "engine does not support streaming request bodies")
			return
		}
		res, e = engine.StreamRequest(r.Context(), r.Method, endpoint, r.Body, r.ContentLength, proxyHeaders(r.Header))
	} else if engine, ok := m.engine.(CallRequestEngine); ok {
		res, e = engine.CallRequest(r.Context(), r.Method, endpoint, data, proxyHeaders(r.Header))
	} else {
		res, e = m.engine.Call(r.Context(), endpoint, data)
	}
	if e != nil {
		modelError(w, 503, e.Error())
		return
	}
	defer res.Body.Close()
	copyProxyHeaders(w.Header(), res.Header)
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
