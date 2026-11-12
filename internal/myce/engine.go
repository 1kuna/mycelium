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
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
	readinessWorker    *Worker
	memoryCeilings     *memoryCeilings
	memoryAttempt      *memoryAttempt
	memoryMachine      Machine
	memorySwapMaxBytes int64

	custody     workerState
	stopCommand []string
	stopTimeout time.Duration
	env         []string

	Dir      string
	Clock    Clock
	cmd      *exec.Cmd
	url      string
	identity string
	log      *os.File
	// Completion is broadcast so readiness, monitoring and cleanup never steal
	// each other's Wait result. waitErr is published by closing exited.
	exited         chan struct{}
	waitErr        error
	monitorCancel  context.CancelFunc
	monitorDone    chan struct{}
	healthMu       sync.Mutex
	healthErr      error
	healthRevision uint64
	healthDegraded bool
	probeLatency   readinessLatency
	// healthPause replaces the wall-clock wait between health probes; nil uses readinessPause.
	healthPause func(context.Context, time.Duration) error
}

func (l *LlamaEngine) Load(ctx context.Context, m Model) error {
	progress := readinessFromContext(ctx)
	loaded := false
	if progress == nil {
		var finish func(bool)
		ctx, progress, finish = superviseModelLoad(ctx, m, l.Dir, l.Clock)
		defer func() { finish(loaded) }()
	}
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
	if err := (Worker{Machine: l.memoryMachine}).checkHostMemoryBudget(m.MemoryGB, m.HostMemoryGB); err != nil {
		return err
	}
	l.identity = ID()
	l.custody = workerState{Identity: l.identity, LaunchPending: true, Accelerator: !(len(m.Platforms) == 1 && m.Platforms[0] == "cpu"), StopGrace: acceleratorStopGrace(l.stopTimeout)}
	if l.custody.Accelerator {
		l.stopTimeout = l.custody.StopGrace
	}
	name := "engine-" + l.identity
	logHint := "; logs disabled"
	if m.Logs != "none" {
		logHint = "; see " + name + ".log"
	}
	shell := []string{"-c", `read gate || exit 125; exec "$@"`, "myce-supervisor-" + l.identity}
	shell = append(shell, argv...)
	l.cmd = exec.Command("sh", shell...)
	l.cmd.SysProcAttr = commandProcessAttributes()
	l.cmd.Env = env
	l.cmd.Env = processSessionEnvironment(append(l.cmd.Env, "MYCE_PROCESS_ID="+l.identity))
	l.log, e = openCommandOutput(l.Dir, name+".log", m.Logs, os.O_EXCL)
	if e != nil {
		return e
	}
	output := &modelReadinessOutput{output: l.log, progress: progress, line: diagnosticLine{tail: diagnosticTail{dir: l.Dir}}}
	if m.Logs == "none" {
		// Preserve null OS descriptors. OS activity still supplies progress;
		// stdout progress records require captured output.
		l.cmd.Stdout, l.cmd.Stderr = l.log, l.log
	} else {
		l.cmd.Stdout, l.cmd.Stderr = output, output
	}
	reader, writer, e := os.Pipe()
	if e != nil {
		l.log.Close()
		return e
	}
	defer reader.Close()
	defer writer.Close()
	l.cmd.Stdin = reader
	if e = saveWorkerJSON(l.Dir, name+".json", l.custody); e != nil {
		l.log.Close()
		return e
	}
	bytes, limitErr := (Worker{Machine: l.memoryMachine}).ceilingBytes(m.MemoryGB, m.HostMemoryGB)
	if limitErr != nil {
		l.log.Close()
		return limitErr
	}
	l.memoryAttempt, e = l.memoryCeilings.create("engine-"+l.identity, bytes, l.memorySwapMaxBytes)
	if e != nil {
		l.log.Close()
		return e
	}
	if e = l.cmd.Start(); e != nil {
		_ = l.memoryAttempt.cleanup(l.Clock)
		l.log.Close()
		return e
	}
	if l.memoryAttempt != nil {
		l.custody.Cgroup = l.memoryAttempt.path
	}
	l.exited = make(chan struct{})
	go func() { l.waitErr = l.cmd.Wait(); output.close(); close(l.exited) }()
	if e = l.memoryAttempt.attach(l.cmd.Process.Pid); e != nil {
		_ = l.Unload()
		return fmt.Errorf("memory ceiling attach: %w", e)
	}
	l.custody.PID, l.custody.LaunchPending = l.cmd.Process.Pid, false
	if e = saveWorkerJSON(l.Dir, name+".json", l.custody); e != nil {
		_ = l.Unload()
		return e
	}
	e = checkLaunchGate(ctx)
	if e == nil {
		_, e = writer.Write([]byte("start\n"))
	}
	if e != nil {
		_ = l.Unload()
		return e
	}
	stopActivity := l.observeReadinessActivity(ctx, progress)
	defer stopActivity()
	l.url = "http://127.0.0.1:" + strconv.Itoa(port)

	hc := &http.Client{
		Timeout:       l.probeLatency.budget(),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	lastStatus := 0
	probe := readinessProbe{latency: l.probeLatency}
	for {
		started := progress.clock.Now()
		realStarted := RealClock{}.Now()
		hc.Timeout = probe.budget()
		req, _ := http.NewRequestWithContext(ctx, "GET", health, nil)
		res, err := hc.Do(req)
		if err == nil {
			probe.observe(max(progress.clock.Now().Sub(started), RealClock{}.Now().Sub(realStarted)), false)
			l.probeLatency = probe.latency
			if res.StatusCode != lastStatus {
				progress.advance()
				lastStatus = res.StatusCode
			}
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode >= 200 && res.StatusCode < 300 && ctx.Err() == nil {
				select {
				case <-l.exited:
					oomErr := l.memoryAttempt.failure()
					_ = l.Unload()
					if oomErr != nil {
						return oomErr
					}
					return fmt.Errorf("server exited as health became ready: %v%s", l.waitErr, logHint)
				default:
					loaded = true
					l.startHealthMonitor(health)
					return nil
				}
			}
		}
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			probe.observe(max(progress.clock.Now().Sub(started), RealClock{}.Now().Sub(realStarted)), true)
		}

		select {
		case <-ctx.Done():
			oomErr := l.memoryAttempt.failure()
			_ = l.Unload()
			if oomErr != nil {
				return oomErr
			}
			return context.Cause(ctx)
		case <-l.exited:
			oomErr := l.memoryAttempt.failure()
			_ = l.Unload()
			if oomErr != nil {
				return oomErr
			}
			return fmt.Errorf("server exited during load: %v%s", l.waitErr, logHint)
		case <-time.After(l.probeLatency.cadence()):
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
	if l.monitorCancel != nil {
		l.monitorCancel()
		<-l.monitorDone
		l.monitorCancel = nil
	}
	if l.cmd == nil || l.cmd.Process == nil {
		return nil
	}
	// Give the optional control command and every process one shared deadline.
	// Main exiting never shortens a GPU descendant's opportunity to drain.
	persist := func() error { return saveWorkerJSON(l.Dir, "engine-"+l.identity+".json", l.custody) }
	if l.custody.Accelerator && l.custody.StopAt == nil {
		at := l.Clock.Now().Add(acceleratorStopGrace(l.stopTimeout))
		l.custody.StopAt = &at
		if err := persist(); err != nil {
			return err
		}
		_ = signalWorkerGroup(l.cmd.Process.Pid, syscall.SIGTERM)
		_ = l.memoryAttempt.termOutsideGroups(map[int]bool{l.cmd.Process.Pid: true})
	}
	remaining := l.stopTimeout
	if l.custody.StopAt != nil {
		remaining = max(0, l.custody.StopAt.Sub(l.Clock.Now()))
	}
	grace, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	stopErr := l.runStopCommand(grace)
	if l.custody.Accelerator {
		if err := gracefulRecoveredGroup(l.cmd.Process.Pid, l.Clock, &l.custody.StopAt, l.stopTimeout, persist, l.memoryAttempt); err != nil {
			return errors.Join(stopErr, err)
		}
	} else {
		_ = signalWorkerGroup(l.cmd.Process.Pid, syscall.SIGTERM)
	}
	if l.exited != nil && !l.custody.Accelerator {
		select {
		case <-l.exited:
		case <-grace.Done():
		}
	}
	e := signalWorkerGroup(l.cmd.Process.Pid, syscall.SIGKILL)
	if l.exited != nil {
		<-l.exited
	}
	e = errors.Join(e, stopErr, waitWorkerGroup(l.cmd.Process.Pid, l.Clock), l.memoryAttempt.cleanup(l.Clock))
	if e == nil {
		l.memoryAttempt = nil
	}
	if l.log != nil {
		e = errors.Join(e, l.log.Close())
		l.log = nil
	}
	if e == nil {
		e = saveWorkerJSON(l.Dir, "engine-"+l.identity+".json", workerState{Identity: l.identity})
	}
	if e == nil {
		l.cmd = nil
	}
	return e
}
func (l *LlamaEngine) runStopCommand(grace context.Context) error {
	if len(l.stopCommand) > 0 {
		identity := ID()
		name := "engine-stop-" + identity
		argv := append([]string{"-c", `read gate || exit 125; exec "$@"`, "myce-supervisor-" + identity}, l.stopCommand...)
		cmd := exec.CommandContext(grace, "sh", argv...)
		cmd.Env = processSessionEnvironment(append(append([]string{}, l.env...), "MYCE_PROCESS_ID="+identity))
		reader, writer, pipeErr := os.Pipe()
		if pipeErr != nil {
			return pipeErr
		}
		defer reader.Close()
		defer writer.Close()
		cmd.Stdin = reader
		cmd.Stdout = l.log
		cmd.Stderr = l.log
		cmd.SysProcAttr = commandProcessAttributes()
		cmd.Cancel = func() error { return signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL) }
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
			_ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
			return err
		}
		if err := l.memoryAttempt.attach(cmd.Process.Pid); err != nil {
			_ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
			return fmt.Errorf("stop helper memory ceiling attach: %w", err)
		}
		_, gateErr := writer.Write([]byte("start\n"))
		// A helper killed at the grace deadline can close its gate first.
		// Still inspect its group below; a broken pipe alone is expected.
		if grace.Err() != nil && errors.Is(gateErr, syscall.EPIPE) {
			gateErr = nil
		}
		if gateErr != nil {
			_ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL)
		}
		_ = cmd.Wait()
		if l.custody.Accelerator {
			_ = signalWorkerGroup(cmd.Process.Pid, syscall.SIGTERM)
			deadline := l.custody.StopAt
			if err := gracefulRecoveredGroup(cmd.Process.Pid, l.Clock, &deadline, l.stopTimeout, func() error { return nil }); err != nil {
				return err
			}
		}
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
	return cleanupRecordedProcesses(dir, "engine-*.json", clock)
}

func cleanupRecordedProcesses(dir, pattern string, clock Clock) error {
	files, e := filepath.Glob(filepath.Join(dir, pattern))
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
		// Legacy command/engine records do not identify their device. Drain
		// conservatively; new records explicitly preserve known CPU custody.
		var fields map[string]json.RawMessage
		if e = json.Unmarshal(data, &fields); e != nil {
			return e
		}
		if _, known := fields["accelerator"]; !known {
			st.Accelerator = true
		}
		s := workerStore{dir: dir, stateName: filepath.Base(p), state: st}
		if e = s.cleanupOrphan(clock); e != nil {
			return e
		}
		st = s.state
		st.PID = 0
		st.LaunchPending = false
		if e = saveWorkerJSON(dir, filepath.Base(p), st); e != nil {
			return e
		}
	}
	return nil
}

type workerModel struct {
	progress           *readinessProgress
	device             string
	bound              *Worker
	runtimeFailed      bool
	attempt            string
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
	worker                 *Worker
	batchJobs              []Job
	batchSupervisors       map[attemptKey]*attemptSupervisor
	admissionMu            sync.Mutex // Coherent stop publication and final accounting; no I/O.
	batchHostMemorySamples map[attemptKey]hostMemorySample
	hostLedger             atomic.Pointer[hostLedger]
	verifyDevice           func() error
	deviceEnv              map[string]string
	authorize              func(context.Context, HostLaunch) error
	copyResults            map[string]CopyReport
	localApprovals         map[string]string
	localApprovalStates    map[string]ModelApprovalState
	availability           *availabilityMonitor
	admissionBlocked       bool
	requireMemoryCeilings  bool
	memoryCeilings         *memoryCeilings
	memoryMachine          Machine
	memorySwapMaxBytes     int64

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
	secretRoot            string
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
	rt := &modelRuntime{copyResults: map[string]CopyReport{}, availability: w.availability, copies: map[string]*workerCopy{}, copyParallel: max(1, w.CopyParallel), models: map[string]*workerModel{}, config: w.ModelConfig, session: w.Machine.Session, token: w.Client.Token, dir: dir, clock: w.Clock, url: "http://" + ln.Addr().String()}
	root := w.CacheRoot
	if root == "" {
		root = filepath.Join(dir, "model-cache")
	}
	limit := w.CacheLimit
	if limit == 0 {
		limit = defaultModelCacheBytes
	}
	rt.worker = &w
	rt.secretRoot = w.SecretRoot
	rt.cache = &ModelCache{ApprovalRoot: w.ModelApprovalRoot, FileClient: &w.Client, Root: root, NASRoot: w.ModelRoot, Limit: limit, Clock: w.Clock}
	rt.cache.budget = w.diskBudget
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
	for _, model := range w.ModelConfig.Models {
		rt.cache.knownModels.Store(cacheKey(model), model)
	}
	if w.diskBudget != nil {
		w.diskBudget.add(diskBudgetRoot{name: "models", path: root, candidates: rt.cache.budgetCandidates})
	}
	if data, e := os.ReadFile(filepath.Join(dir, "copy-speed.json")); e == nil {
		if e := decodeWorkerJSON(data, &rt.cache.speed); e != nil {
			log.Printf("restore model copy speed: %v", e)
		}
	}
	rt.device, rt.platform = w.Machine.Device, w.Machine.GPU
	if w.Machine.GPU == "cpu" {
		bound := w
		bound.cpuIsolation = true
		rt.verifyDevice = bound.verifyDevice
		rt.deviceEnv, _ = bound.deviceEnvironment()
	}
	for _, d := range w.Machine.Devices {
		if d.ID == w.Machine.Device {
			bound := w
			bound.selectedDevice = &d
			bound.DeviceBinding = d.Selector
			rt.verifyDevice = bound.verifyDevice
			rt.deviceEnv, _ = bound.deviceEnvironment()
			break
		}
	}
	rt.machine = w.Machine.ID
	rt.environmentCache = w.environmentCache
	rt.environmentSourceRoot = w.EnvironmentSourceRoot
	rt.memoryCeilings, rt.memoryMachine, rt.memorySwapMaxBytes = w.memoryCeilings, w.Machine, w.MemorySwapMaxBytes
	rt.requireMemoryCeilings = w.RequireMemoryCeilings
	if w.HostMemorySource != nil {
		rt.hostMemorySource = func(ctx context.Context) (float64, error) {
			rt.mu.Lock()
			machine := rt.memoryMachine
			rt.mu.Unlock()
			bound := w
			bound.Machine = machine
			return bound.sharedHostLiveMemory(ctx)
		}
	}
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
func (rt *modelRuntime) setLocalApprovals(approved map[string]string, states map[string]ModelApprovalState) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.localApprovals = approved
	rt.localApprovalStates = states
}

// Called under the runtime lock; withholding readiness never releases custody.
func (rt *modelRuntime) localApprovalAvailable(m Model) bool {
	return m.LocalSource == "" || rt.localApprovalStates[m.LocalSource] == approvalVerified && rt.localApprovals[m.LocalSource] == m.SHA256
}

func (rt *modelRuntime) report() ([]ModelReport, float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := []ModelReport{}
	memory := 0.0
	for _, m := range rt.models {
		rt.checkEngineHealth(m)
		report := ModelReport{Name: m.model.Name, Device: m.device, Attempt: m.attempt, RuntimeFailed: m.runtimeFailed, State: m.state, Error: ceilingDiagnostic(m.err, "model load or unload failed"), InFlight: m.inflight, CallsMeasured: true}
		if m.state == "loading" {
			report.Error = m.progress.coldReason()
		}
		if m.state == "ready" {
			if owned, ok := m.engine.(*LlamaEngine); ok {
				_, report.HealthRevision, report.HealthDegraded = owned.runtimeHealth()
			}
		}
		out = append(out, report)
		if m.state != "unloaded" && m.state != "failed" {
			memory += m.model.MemoryGB
		}
	}
	return out, memory
}

// Caller holds rt.mu. Both heartbeat reporting and request admission must see
// process/health failure before granting the engine any new in-flight work.
func (rt *modelRuntime) checkEngineHealth(m *workerModel) {
	if m == nil || m.state != "ready" && m.state != "draining" {
		return
	}
	if owned, ok := m.engine.(*LlamaEngine); ok {
		err, _, _ := owned.runtimeHealth()
		if ceiling := owned.memoryAttempt.failure(); ceiling != nil {
			err = ceiling
		}
		if err != nil {
			m.err, m.runtimeFailed = err.Error(), true
			rt.unload(m)
		}
	}
}

func (rt *modelRuntime) sync(plan []ModelSlot) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	desired := map[string]ModelSlot{}
	for _, s := range plan {
		desired[engineKey(s.Model, s.Device)] = s
	}
	for name, m := range rt.models {
		s, ok := desired[name]
		m.queueFlight = s.InFlight
		replaced := ok && s.Attempt != "" && m.attempt != "" && s.Attempt != m.attempt
		if replaced {
			m.queueFlight = 0 // B's assignments cannot keep A alive.
			if m.state == "unload_failed" {
				// Transfer the cleanup failure receipt to the desired attempt,
				// retaining A's engine and custody. B must never start over it.
				m.attempt = s.Attempt
			}
		}
		if !ok && m.state == "failed" {
			m.state = "unloaded"
		}
		if ok && s.Attempt != "" && s.Attempt == m.attempt && s.Session == rt.session && s.StopAt != nil && expired(s.StopAt, rt.clock.Now()) && (m.state == "ready" || m.state == "draining") {
			rt.unload(m)
		}
		if !ok || replaced || s.State == "draining" || s.State == "unloading" || !s.Placement.allows(rt.machine) {
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
		if s.Session != rt.session || s.Attempt == "" {
			continue
		}
		if s.State == "unloading" {
			if m := rt.models[engineKey(s.Model, s.Device)]; m == nil || m.state == "unloaded" || m.state == "failed" {
				// The load deadline can expire before B starts. A has been
				// cleaned (or never existed), so acknowledge absence under B.
				rt.models[engineKey(s.Model, s.Device)] = &workerModel{device: s.Device, model: Model{Name: s.Model}, attempt: s.Attempt, state: "unloaded"}
			}
			continue
		}
		if s.State != "loading" || s.Session != rt.session {
			continue
		}
		if m := rt.models[engineKey(s.Model, s.Device)]; m != nil {
			// A delayed loading reply may race with local failure/cleanup.
			// Only a fresh queue attempt can authorize another engine load.
			if m.attempt == s.Attempt || m.state != "unloaded" && m.state != "failed" {
				continue
			}
		}
		var model Model
		for _, m := range rt.config.Models {
			if m.Name == s.Model {
				model = m
			}
		}
		if s.Placement != nil {
			model.Project = s.Placement.Project
		}
		if model.Name != "" && (len(model.AllowedMachines) > 0 && !contains(model.AllowedMachines, rt.machine) || !s.Placement.allows(rt.machine)) {
			rt.models[engineKey(s.Model, s.Device)] = &workerModel{device: s.Device, model: model, attempt: s.Attempt, state: "failed", err: "model placement excludes this worker"}
			continue
		}
		if model.Name == "" {
			rt.models[engineKey(s.Model, s.Device)] = &workerModel{device: s.Device, model: Model{Name: s.Model}, attempt: s.Attempt, state: "failed", err: "model absent from worker config"}
			continue
		}
		if !rt.localApprovalAvailable(model) || rt.admissionBlocked || rt.availability != nil && !rt.availability.snapshot().Available {
			continue
		}
		var bound *Worker
		if s.Device != "" && rt.worker != nil {
			device := slices.IndexFunc(rt.memoryMachine.Devices, func(d MachineDevice) bool { return d.ID == s.Device })
			if device < 0 || rt.memoryMachine.Devices[device].Reason != "" {
				// Keep the queue's offer pending while this component is
				// withheld. A matching probe can resume the same load attempt.
				continue
			}
			snapshot := *rt.worker
			snapshot.Machine = cloneMachine(&rt.memoryMachine)
			bound = &snapshot
		}
		var engine Engine
		if rt.engineFactory != nil {
			engine = rt.engineFactory(model)
		} else if model.Engine == "fake" {
			engine = &FakeEngine{}
		} else {
			engine = NewModelEngine(model, rt.dir, rt.clock)
			if owned, ok := engine.(*LlamaEngine); ok {
				owned.memoryCeilings, owned.memoryMachine, owned.memorySwapMaxBytes = rt.memoryCeilings, rt.memoryMachine, rt.memorySwapMaxBytes
			}
		}
		parent := context.Background()
		if rt.availability != nil {
			parent = rt.availability.workContext()
		}
		ctx, cancel := context.WithCancel(parent)
		if authorize := rt.authorize; authorize != nil {
			// Capture the exact plan under rt.mu; heartbeats update rt.session.
			launch := HostLaunch{Machine: rt.machine, Session: s.Session, Kind: "engine", Key: s.Model, Device: s.Device, Attempt: s.Attempt}
			rootWorker := rt.worker
			ctx = withLaunchGate(ctx, func(ctx context.Context) error {
				if rootWorker != nil {
					probe := *rootWorker
					probe.Machine = probe.storageMachine()
					probe.Machine.UnavailableRoots = probe.sampleStorageRoots(ctx, probe.CheckpointRoot)
					dependency := &Job{Spec: Spec{Priority: "production", Needs: ResourceNeeds{Model: model.Name}}}
					if recipe := s.EnvironmentRecipe; recipe != nil {
						dependency.ModelEnvironmentKey = recipe.Key
						dependency.ModelEnvironmentNeedsSource = recipe.Binary != "" || recipe.BinaryRelative != ""
					}
					if reason := probe.storageRootReason(dependency); reason != "" {
						return errors.New(reason)
					}
				}
				if s.Device == "" && rt.verifyDevice != nil {
					if err := rt.verifyDevice(); err != nil {
						return err
					}
				}
				return authorize(ctx, launch)
			})
		}
		m := &workerModel{device: s.Device, bound: bound, model: model, engine: engine, attempt: s.Attempt, state: "loading", cancel: cancel, recipe: s.EnvironmentRecipe}
		rt.models[engineKey(s.Model, s.Device)] = m
		rt.wg.Add(1)
		go func() {
			defer rt.wg.Done()
			rt.load(ctx, m)
		}()
	}
}

func (rt *modelRuntime) load(ctx context.Context, m *workerModel) {
	if m.device != "" && rt.worker != nil {
		bound := *rt.worker
		if m.bound != nil {
			bound = *m.bound
		} else {
			rt.mu.Lock()
			bound.Machine = cloneMachine(&rt.memoryMachine)
			rt.mu.Unlock()
		}
		var err error
		bound, err = bound.forAttempt(Job{AllocatedDevices: []string{m.device}})
		if err != nil {
			m.cancel()
			rt.mu.Lock()
			m.state, m.err = "failed", err.Error()
			rt.mu.Unlock()
			return
		}
		m.bound = &bound
		if owned, ok := m.engine.(*LlamaEngine); ok {
			owned.memoryMachine = bound.Machine
		}
		parent := ctx
		priorGate := launchGateFrom(parent)
		ctx = withLaunchGate(parent, func(ctx context.Context) error {
			if err := bound.verifyDevice(); err != nil {
				return err
			}
			return priorGate(ctx)
		})
	}
	if owned, ok := m.engine.(*LlamaEngine); ok {
		owned.readinessWorker = rt.worker
		if m.bound != nil {
			owned.readinessWorker = m.bound
		}
	}
	identity := m.model
	if m.recipe != nil {
		identity.readinessRecipe = m.recipe.Key
	}
	platform, device := rt.platform, rt.device
	if m.bound != nil {
		platform, device = m.bound.Machine.GPU, m.bound.Machine.Device
	}
	identity.Platforms, identity.Device = []string{platform}, device
	ctx, progress, finishReadiness := superviseModelLoadPhase(ctx, identity, rt.dir, rt.clock, true)
	rt.mu.Lock()
	m.progress = progress
	rt.mu.Unlock()
	if rt.cache != nil {
		progress.mu.Lock()
		progress.source = func() int64 {
			if v, ok := rt.cache.progress.Load(cacheKey(m.model)); ok {
				return v.(CopyReport).Bytes
			}
			return 0
		}
		progress.mu.Unlock()
	}
	loadSucceeded := false
	defer func() { finishReadiness(loadSucceeded) }()
	if e := rt.checkLoadMemory(ctx, m); e != nil {
		m.cancel()
		rt.mu.Lock()
		m.state = "failed"
		m.err = e.Error()
		rt.mu.Unlock()
		return
	}
	// Authorization and preparation may block while a batch receives a stop.
	// Recheck current admission after authorization at every engine launch gate.
	parent := ctx
	priorGate := launchGateFrom(parent)
	ctx = withLaunchGate(parent, func(ctx context.Context) error {
		resume := progress.pause()
		defer resume()
		if err := priorGate(ctx); err != nil {
			return err
		}
		return rt.checkLoadMemory(ctx, m)
	})
	deviceEnv := rt.deviceEnv
	platform, device = rt.platform, rt.device
	if m.bound != nil {
		platform, device = m.bound.Machine.GPU, m.bound.Machine.Device
		deviceEnv, _ = m.bound.deviceEnvironment()
	}
	model := withPlacementEnvironment(m.model, Machine{ID: rt.machine, GPU: platform, Device: device})
	model.secretRoot, model.fleetToken = rt.secretRoot, rt.token
	var e error
	if device != "" || platform == "cpu" {
		model, e = WithModelDevice(model, platform, device)
		if e == nil {
			for k, v := range deviceEnv {
				model.DeviceEnv[k] = v
			}
		}
	}
	if e == nil {
		e = checkLaunchGate(ctx)
	}
	if e == nil && model.Environment != "" {
		if m.recipe == nil {
			e = errors.New("model environment recipe unavailable")
		} else {
			recipe := *m.recipe
			if recipe.BinaryRelative != "" && rt.environmentSourceRoot != "" {
				recipe.Binary = filepath.Join(rt.environmentSourceRoot, recipe.BinaryRelative)
			}
			// Environment preparation has its own progress/ownership protocol;
			// a model-load timer cannot classify a queued builder as stalled.
			model, m.releaseEnvironment, e = PrepareModelEnvironment(ctx, model, recipe.Platform, EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{model.Environment: {recipe.Platform: recipe}}}, rt.environmentCache)
		}
	}
	// Only cache/model work can be classified by model-load progress. Start a
	// fresh silence interval after admission and environment preparation.
	progress.mu.Lock()
	progress.last, progress.realLast = progress.clock.Now(), RealClock{}.Now()
	progress.started, progress.realStarted = progress.last, progress.realLast
	progress.suspended = false
	progress.mu.Unlock()
	if e == nil && (model.Engine == "llama.cpp" || model.SizeBytes > 0) {
		model.Path, e = rt.cache.Prepare(ctx, model)
	}
	if e == nil {
		e = checkLaunchGate(ctx)
	}
	if e == nil {
		e = m.engine.Load(ctx, model)
	}
	if e == nil && ctx.Err() != nil {
		rt.mu.Lock()
		draining := rt.closing || m.drainRequested
		rt.mu.Unlock()
		if !draining {
			e = context.Cause(ctx)
		}
	}
	loadSucceeded = e == nil
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

// Capture actor samples before reading physical availability: crediting a newer
// resident allocation against an older free-RAM read would spend it twice.
// Actor publications are small in-memory snapshots, with no process/sensor I/O.
func (rt *modelRuntime) admissionMemorySnapshot() map[attemptKey]hostMemorySample {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	now := time.Time{}
	if rt.clock != nil {
		now = rt.clock.Now()
	}
	samples := maps.Clone(rt.batchHostMemorySamples)
	if samples == nil {
		samples = map[attemptKey]hostMemorySample{}
	}
	for key, actor := range rt.batchSupervisors {
		actor.mu.Lock()
		report := actor.report
		actor.mu.Unlock()
		if report.ID != key.job || report.Attempt != key.attempt || report.Session != key.session {
			continue
		}
		report.CurrentHostMemory = report.CurrentHostMemory.aged(now)
		for key, sample := range updateHostMemorySamples([]AttemptReport{report}, now, samples) {
			samples[key] = sample
		}
	}
	rt.batchHostMemorySamples = maps.Clone(samples)
	rt.publishHostLedgerLocked()
	return samples
}

// All blocking sensors run before this exact-attempt accounting snapshot.
// Call with rt.mu and rt.admissionMu held. Stop publication shares this ledger;
// actor reads never acquire process/control locks or perform receipt I/O.
func (rt *modelRuntime) checkHostAdmissionLocked(hostFree float64, hostMeasured bool, samples map[attemptKey]hostMemorySample, launching ...attemptKey) error {
	cores, ram, growth := 0, 0.0, 0.0
	anon, ownResident, ownCharge, ownAnon, ownForecast, ownPending := 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
	ownDevicePending := 0.0
	var contributors []HostGrowthContributor
	ownContributor := -1
	now := time.Time{}
	if rt.clock != nil {
		now = rt.clock.Now()
	}
	for _, j := range rt.batchJobs {
		// Preparation can outlast a heartbeat or receive an out-of-band stop.
		// Consult the exact actor at launch time, not its original running job.
		if a := rt.batchSupervisors[jobKey(j)]; a != nil {
			j = a.admissionJob()
		}
		if j.ReleaseConfirmed {
			continue
		}
		helper := 0.0
		cores += j.Spec.Needs.cores()
		owner := allocationMachine(&rt.memoryMachine, j.AllocatedDevices)
		charge, pending := memoryCharge(&j, owner, hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), samples[jobKey(j)], now)
		if slices.Contains(launching, jobKey(j)) && adaptiveMemoryCeilingGB(&j, owner) > 0 && j.MemoryEnvelope.ProbeFloorGB > 0 {
			// This cold command has not executed yet. Its offered reservation
			// must fit. Bounded estimates already use the offer plus retained
			// growth; launch must not discard their observed helper occupancy.
			charge = hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
			pending = charge
		}
		sample := samples[jobKey(j)]
		forecast, fresh := sample.hostGrowth(now)
		if fresh {
			anon += sample.gb
		}
		if slices.Contains(launching, jobKey(j)) {
			// The exact launch owns this complete reservation (#1033). Helpers
			// cannot demand more unallocated RAM than that bound.
			pending = min(pending, charge)
			if len(launching) == 1 {
				expanded := helperHostCharge(&j, owner, sample, now, charge)
				ownResident = helperDeviceResident(owner, sample, now)
				// Anonymous occupancy does not prove that future CUDA demand is
				// already allocated. Preserve the device component, including its
				// forecast/overshoot, after crediting only direct device occupancy.
				ownDevicePending = max(0, helperDeviceCharge(&j, sample, now)-ownResident)
				pending += expanded - charge
				helper = expanded - charge
				charge = expanded
				ownCharge, ownPending = charge, pending
				if fresh {
					ownAnon, ownForecast = sample.gb, forecast
				}
			}
		}
		ram += charge
		growth += pending
		c := HostGrowthContributor{ID: j.ID, Attempt: j.Attempt, Kind: "attempt", Launching: slices.Contains(launching, jobKey(j)), ChargeGB: charge, PendingGB: pending, HelperGB: helper, SampleFresh: fresh}
		if !sample.at.IsZero() && !now.Before(sample.at) {
			age := now.Sub(sample.at).Seconds()
			c.SampleAgeSeconds = &age
		}
		if c.Launching && len(launching) == 1 {
			ownContributor = len(contributors)
		}
		contributors = append(contributors, c)
	}
	for _, other := range rt.models {
		if other.state != "unloaded" && other.state != "failed" {
			cores++
			need := hostNeed(allocationMachine(&rt.memoryMachine, []string{other.device}), other.model.MemoryGB, other.model.HostMemoryGB)
			ram += need
			growth += need
			contributors = append(contributors, HostGrowthContributor{Kind: "model", ChargeGB: need, PendingGB: need})
		}
	}
	if ownResident > 0 {
		// Host anon is uniquely charged across attempt cgroups, but can also
		// contain CUDA pages. Credit only the union lower bound max(all anon,
		// one process's CUDA counter), never their sum or two process counters.
		additional := max(0, ownResident-anon)
		pending := max(0, ownCharge-ownAnon-additional, ownForecast, ownDevicePending)
		growth += pending - ownPending
		if ownContributor >= 0 {
			contributors[ownContributor].PendingGB = pending
		}
	}
	capacity, reason := cpuCapacity(&rt.memoryMachine)
	if reason == "" && cores > capacity {
		return fmt.Errorf("aggregate engine CPU/RAM allowance: %w", refuseAdmission("aggregate_cpu_allowance", "cores", float64(cores), float64(capacity), false))
	}
	if (rt.memoryMachine.HostMemoryBudgetGB > 0 || rt.memoryMachine.HostMemoryBudgetMeasured) && ram > rt.memoryMachine.HostMemoryBudgetGB {
		return fmt.Errorf("aggregate engine CPU/RAM allowance: %w", refuseAdmission("aggregate_host_allowance", "GiB", ram, rt.memoryMachine.HostMemoryBudgetGB, false))
	}
	if hostMeasured && hostFree < growth {
		return fmt.Errorf("insufficient host RAM for launch and pending growth: %w", refuseAdmission("live_host_growth", "GiB", growth, hostFree, false, contributors...))
	}
	return nil
}

// hostLedger is this worker's ledger as launch admission computes it: what
// each live attempt and loaded model is charged, and the part of that not yet
// resident. Published under rt.mu wherever its inputs change, and read without
// locks by attempt samplers (firstRunFloorRoom).
type hostLedger struct {
	charge, pending float64
	attempts        map[attemptKey][2]float64
}

// Call with rt.mu held.
func (rt *modelRuntime) publishHostLedgerLocked() {
	now := time.Time{}
	if rt.clock != nil {
		now = rt.clock.Now()
	}
	l := &hostLedger{attempts: map[attemptKey][2]float64{}}
	for _, j := range rt.batchJobs {
		if a := rt.batchSupervisors[jobKey(j)]; a != nil {
			j = a.admissionJob()
		}
		if j.ReleaseConfirmed {
			continue
		}
		owner := allocationMachine(&rt.memoryMachine, j.AllocatedDevices)
		c, p := memoryCharge(&j, owner, hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB), rt.batchHostMemorySamples[jobKey(j)], now)
		l.charge, l.pending, l.attempts[jobKey(j)] = l.charge+c, l.pending+p, [2]float64{c, p}
	}
	for _, other := range rt.models {
		if other.state != "unloaded" && other.state != "failed" {
			need := hostNeed(allocationMachine(&rt.memoryMachine, []string{other.device}), other.model.MemoryGB, other.model.HostMemoryGB)
			l.charge, l.pending = l.charge+need, l.pending+need
		}
	}
	rt.hostLedger.Store(l)
}

// hostChargesExcept is the published ledger without one attempt: what every
// other attempt and model is charged, and the part of that not yet resident.
func (rt *modelRuntime) hostChargesExcept(key attemptKey) (charge, pending float64) {
	l := rt.hostLedger.Load()
	if l == nil {
		return 0, 0
	}
	own := l.attempts[key]
	return max(0, l.charge-own[0]), max(0, l.pending-own[1])
}

// Queue authorization can outlast preparation or any sensor read. Revalidate
// the exact local reservation after those reads and before releasing a batch.
func (rt *modelRuntime) checkBatchLaunchMemory(ctx context.Context, launch HostLaunch) error {
	samples := rt.admissionMemorySnapshot()
	hostFree, measured := 0.0, rt.hostMemorySource != nil
	if measured {
		var err error
		hostFree, err = rt.hostMemorySource(ctx)
		if err != nil || !validMemoryValue(hostFree) {
			return fmt.Errorf("%w: host RAM observation unavailable", errWorkerMemory)
		}
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.admissionMu.Lock()
	defer rt.admissionMu.Unlock()
	attempt, err := strconv.Atoi(launch.Attempt)
	actor := rt.batchSupervisors[attemptKey{launch.Key, attempt, launch.Session}]
	if err != nil || actor == nil || launch.Machine != rt.machine || actor.admissionJob().State == "stopping" || actor.admissionJob().ReleaseConfirmed {
		return fmt.Errorf("%w: batch admission custody changed", errWorkerMemory)
	}
	return rt.checkHostAdmissionLocked(hostFree, measured, samples, jobKey(actor.job))
}

func (rt *modelRuntime) checkLoadMemory(ctx context.Context, loading *workerModel) error {
	if rt.availability != nil && !rt.availability.snapshot().Available {
		return errWorkerUnavailable
	}
	rt.mu.Lock()
	machine := rt.memoryMachine
	rt.mu.Unlock()
	source, measured := rt.memorySource, rt.memoryMeasured
	if loading.bound != nil {
		machine, source, measured = loading.bound.Machine, loading.bound.MemorySource, loading.bound.MemoryMeasured
	}
	if err := (Worker{Machine: machine}).checkHostMemoryBudget(loading.model.MemoryGB, loading.model.HostMemoryGB); err != nil {
		return err
	}
	if rt.requireMemoryCeilings && loading.model.HostMemoryGB <= 0 {
		return errors.New("worker requires positive host_memory_gb")
	}

	hostMeasured, hostFree := false, 0.0
	samples := rt.admissionMemorySnapshot()
	if loading.model.HostMemoryGB > 0 || unifiedCUDA(&machine) && loading.model.MemoryGB > 0 {
		if rt.hostMemorySource == nil {
			return errors.New("model load requires host RAM source")
		}
		free, e := rt.hostMemorySource(ctx)
		hostMeasured, hostFree = true, free
		machine.HostMemoryMeasured = true
		need := hostNeed(&machine, loading.model.MemoryGB, loading.model.HostMemoryGB)
		if e != nil || !validMemoryValue(free) || free < need {
			return fmt.Errorf("insufficient host RAM for model %s: need %.1f GiB, available %.1f GiB: %v", loading.model.Name, need, free, e)
		}
	}
	if source == nil {
		return errors.New("model load requires a free-memory source")
	}
	// The source may inspect worker/runtime state. Never invoke it under rt.mu.
	free, e := source(ctx)
	if e != nil {
		return fmt.Errorf("model free-memory check: %w", e)
	}
	if math.IsNaN(free) || math.IsInf(free, 0) || free < 0 {
		return errors.New("model free-memory source returned invalid memory")
	}
	if loading.bound == nil && !measured && rt.batchMemory != nil {
		free -= math.Float64frombits(rt.batchMemory.Load())
	}
	rt.mu.Lock()
	if loading.bound != nil && !measured {
		for _, j := range rt.batchJobs {
			if slices.Contains(j.AllocatedDevices, loading.device) {
				free -= j.Spec.Needs.MemoryGB
			}
		}
	}
	for _, other := range rt.models {
		if other.device != loading.device || other == loading || other.state == "unloaded" || other.state == "failed" {
			continue
		}
		if !measured || other.state == "loading" {
			free -= other.model.MemoryGB
		}
	}
	rt.admissionMu.Lock()
	hostErr := rt.checkHostAdmissionLocked(hostFree, hostMeasured, samples)
	rt.admissionMu.Unlock()
	rt.mu.Unlock()
	if hostErr != nil {
		return hostErr
	}
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
			if m.runtimeFailed {
				m.state = "failed"
			}
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
	generic := strings.HasPrefix(r.URL.Path, "/models/")
	endpoint := r.URL.RequestURI()
	if !generic && (r.Method != "POST" || !contains([]string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"}, r.URL.Path)) {
		modelError(w, 404, "unknown model endpoint")
		return
	}
	var req struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
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
	rt.mu.Lock()
	m := rt.models[req.Model]
	if attempt := r.Header.Get("X-Myce-Engine-Attempt"); attempt != "" {
		// Preserve the bounded legacy runtime's unkeyed model entry. Inventory
		// engines always carry an attempt and route only to an exact match.
		if m != nil && m.attempt != "" {
			m = nil
		}
		for _, candidate := range rt.models {
			if candidate.model.Name == req.Model && candidate.attempt == attempt {
				m = candidate
				break
			}
		}
	}
	if attempt := r.Header.Get("X-Myce-Engine-Attempt"); attempt != "" && m != nil && m.attempt != "" && attempt != m.attempt {
		rt.mu.Unlock()
		w.Header().Set("X-Myce-Engine-Rejected", attempt)
		modelError(w, 503, "engine attempt is no longer ready")
		return
	}
	if rt.availability != nil && !rt.availability.snapshot().Available {
		rt.mu.Unlock()
		modelError(w, 503, "hypha availability withdrawn")
		return
	}
	rt.checkEngineHealth(m)
	if m == nil || m.runtimeFailed || !(rt.localApprovalAvailable(m.model) || rt.localApprovalStates[m.model.LocalSource] == approvalUnknown) || (m.state != "ready" && !(m.state == "draining" && m.queueFlight > 0)) {
		w.Header().Set("X-Myce-Engine-Rejected", r.Header.Get("X-Myce-Engine-Attempt"))
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
	callCtx, cancel := context.WithTimeout(r.Context(), m.model.requestTimeout())
	defer cancel()
	if rt.availability != nil {
		stopCancel := context.AfterFunc(rt.availability.workContext(), cancel)
		defer stopCancel()
	}
	var res *http.Response
	if generic {
		engine, ok := m.engine.(StreamRequestEngine)
		if !ok {
			modelError(w, 503, "engine does not support streaming request bodies")
			return
		}
		res, e = engine.StreamRequest(callCtx, r.Method, endpoint, r.Body, r.ContentLength, proxyHeaders(r.Header))
	} else if engine, ok := m.engine.(CallRequestEngine); ok {
		res, e = engine.CallRequest(callCtx, r.Method, endpoint, data, proxyHeaders(r.Header))
	} else {
		res, e = m.engine.Call(callCtx, endpoint, data)
	}
	requestFailed := func() {
		// A failed worker->engine transport cannot prove the engine did no
		// work. Bind this control receipt to the attempted engine even if Wait
		// has not completed yet; retry_safe:false must never replay a crash.
		if callCtx.Err() == nil {
			w.Header().Set("X-Myce-Engine-Failed", m.attempt)
		}
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			modelError(w, 504, errModelRequestTimeout.Error())
			return
		}
		modelError(w, 503, "model request failed")
	}
	if e != nil {
		requestFailed()
		return
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		modelError(w, res.StatusCode, "upstream model request failed")
		return
	}
	if !generic && !req.Stream {
		// OpenAI non-stream responses are already buffered at the queue. Finish
		// reading here too so an engine body failure carries the attempt-bound
		// failure receipt before any response headers are committed.
		var response []byte
		response, e = io.ReadAll(res.Body)
		if e != nil {
			requestFailed()
			return
		}
		copyProxyHeaders(w.Header(), res.Header)
		w.WriteHeader(res.StatusCode)
		_, _ = w.Write(response)
		return
	}
	buf := make([]byte, 16384)
	n, e := res.Body.Read(buf)
	if n == 0 && e != nil && e != io.EOF {
		requestFailed()
		return
	}
	copyProxyHeaders(w.Header(), res.Header)
	w.WriteHeader(res.StatusCode)
	for {
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
		n, e = res.Body.Read(buf)
	}
}

// Withholding also drains local servers during a queue/network outage. Contexts
// cancel active loads/calls; memory remains reserved until the engine releases.
func (rt *modelRuntime) withdrawLocalAvailability() {
	if rt.availability == nil || rt.availability.snapshot().Available {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, m := range rt.models {
		m.drainRequested = true
		m.queueFlight = 0
		if m.state == "loading" && m.cancel != nil {
			m.cancel()
		}
		if m.state == "ready" {
			m.state = "draining"
		}
		if m.state == "draining" && m.inflight == 0 {
			rt.unload(m)
		}
	}
}

func engineKey(model, device string) string {
	if device == "" {
		return model
	}
	return model + "/" + device
}
