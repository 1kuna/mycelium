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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	model       Model
	engine      Engine
	state, err  string
	inflight    int
	queueFlight int
	cancel      context.CancelFunc
}
type modelRuntime struct {
	mu                  sync.Mutex
	models              map[string]*workerModel
	config              ModelConfig
	session, token, dir string
	clock               Clock
	server              *http.Server
	wg                  sync.WaitGroup
	url                 string
	closing             bool
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
	rt := &modelRuntime{models: map[string]*workerModel{}, config: w.ModelConfig, session: w.Machine.Session, token: w.Client.Token, dir: dir, clock: w.Clock, url: "http://" + ln.Addr().String()}
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
		if model.Engine == "fake" {
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
			e := engine.Load(ctx, model)
			cancel()
			rt.mu.Lock()
			defer rt.mu.Unlock()
			if e != nil {
				m.state = "failed"
				m.err = e.Error()
			} else {
				m.state = "ready"
				if rt.closing {
					m.state = "draining"
					rt.unload(m)
				}
			}
		}()
	}
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
