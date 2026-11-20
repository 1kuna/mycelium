package myce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// ModelCustody contains no local process details. The worker verifies process
// birth and independent service membership before reporting this exact owner.
type ModelCustody struct {
	Protocol int    `json:"protocol"`
	ID       string `json:"id"`
	// Queue-stamped coordinator identity, already public in FleetRuntimeProof.
	// A replacement must present its own exact model report before activation.
	WorkerPID int `json:"worker_pid,omitempty"`
}

var (
	workerModelCustodians    = true
	modelCustodianMembership = verifyCustodianIsolation
)

func validModelCustody(v *ModelCustody) bool {
	return v != nil && v.Protocol == custodianProtocol && validCustodianID(v.ID)
}

func validModelBridgeURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "http" && u.Host != "" && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}

type modelCustodianRequest struct {
	Protocol        int                 `json:"protocol"`
	ID              string              `json:"id"`
	Execution       HostEngineExecution `json:"execution"`
	Machine         Machine             `json:"machine"`
	Original        Model               `json:"original"`
	Prepared        Model               `json:"prepared"`
	EnvironmentPath string              `json:"environment_path,omitempty"`
	EnvironmentKey  string              `json:"environment_key,omitempty"`
	EnvironmentRoot string              `json:"environment_root,omitempty"`
	OOMScore        int                 `json:"oom_score"`
}

type modelCustodianSnapshot struct {
	custodianSnapshot
	Execution HostEngineExecution `json:"execution"`
	URL       string              `json:"url"`
	Report    ModelReport         `json:"report"`
	OOMScore  int                 `json:"oom_score"`
}

type modelCustodianPolicy struct {
	ID        string              `json:"id"`
	Execution HostEngineExecution `json:"execution"`
	OOMScore  int                 `json:"oom_score"`
}

type modelCustodianStop struct {
	ID        string              `json:"id"`
	Execution HostEngineExecution `json:"execution"`
}

// The coordinator is a proxy for control and accounting only. Response sockets,
// engine Wait, logs, health monitoring and memory containment live in the helper.
type modelCustodian struct {
	mu            sync.Mutex
	dir           string
	request       modelCustodianRequest
	clock         Clock
	interval      time.Duration
	service       ServiceConfig
	token         string
	last          modelCustodianSnapshot
	verified      bool
	policyWritten bool
	policyScore   int
	launch        func(context.Context, ServiceConfig, string, string) error
	verify        func(string, int, string) error
}

func (rt *modelRuntime) detachedModelSupported(m Model) bool {
	// Local approvals and owner-withdrawal currently gate the worker bridge.
	// Preserve those gates until they also have independent custody. The Linux
	// shared-host aggregate must not be split between worker and helper scopes.
	return workerModelCustodians && rt.worker != nil && rt.worker.ServiceConfig != nil && rt.worker.Custodian == "" && rt.engineFactory == nil && m.LocalSource == "" && !rt.worker.DailyUse && !rt.worker.Laptop && !rt.worker.Desktop && !(runtime.GOOS == "linux" && rt.memoryMachine.SharedHost != nil)
}

func (rt *modelRuntime) newModelCustodian(m *workerModel, prepared Model) *modelCustodian {
	r := modelCustodianRequest{Protocol: custodianProtocol, ID: ID(), Execution: HostEngineExecution{Machine: rt.machine, Session: rt.session, Model: m.model.Name, Device: m.device, Attempt: m.attempt}, Machine: rt.memoryMachine, Original: m.model, Prepared: prepared, EnvironmentPath: prepared.EnvironmentPath}
	if owned, ok := m.engine.(*LlamaEngine); ok {
		r.OOMScore = int(owned.memoryOOMScore.Load())
	}
	if m.bound != nil {
		r.Machine = m.bound.Machine
	}
	if m.recipe != nil {
		r.EnvironmentKey = m.recipe.Key
		r.EnvironmentRoot = rt.environmentCache.Root
	}
	return &modelCustodian{dir: custodianDir(rt.dir), request: r, clock: rt.clock, interval: rt.worker.Interval, service: *rt.worker.ServiceConfig, token: rt.token, launch: launchCustodian, verify: modelCustodianMembership}
}

func (g *modelCustodian) read() error {
	var v modelCustodianSnapshot
	if err := readCustodianJSON(g.dir, custodianName(g.request.ID, "snapshot"), &v); err != nil {
		return err
	}
	if v.Protocol != custodianProtocol || v.ID != g.request.ID || v.Execution != g.request.Execution || v.Report.Name != v.Execution.Model || v.Report.Attempt != v.Execution.Attempt || v.Report.Device != v.Execution.Device {
		return errors.New("detached model identity mismatch")
	}
	if !v.Finished {
		now := g.clock.Now()
		if now.Before(v.At) || now.Sub(v.At) > max(3*g.interval, attemptLivenessMaxAge) {
			return errors.New("detached model observation is stale")
		}
		birth, err := processBirth(v.PID)
		if err != nil || birth != v.Birth || v.Birth == "" {
			return errors.New("detached model process birth unconfirmed")
		}
		if err := g.verify(v.ID, v.PID, v.Cgroup); err != nil {
			return err
		}
		if !validModelBridgeURL(v.URL) {
			return errors.New("detached model bridge unavailable")
		}
	} else if v.Report.State != "unloaded" && v.Report.State != "failed" {
		return errors.New("detached model release unconfirmed")
	}
	g.last, g.verified = v, true
	return nil
}

func (g *modelCustodian) Load(ctx context.Context, _ Model) error {
	// The helper observes the engine's real load activity. Coordinator silence
	// cannot classify work progressing inside that independently owned process.
	if progress := readinessFromContext(ctx); progress != nil {
		resume := progress.pause()
		defer resume()
	}
	if err := PrepareStateRoots(g.dir); err != nil {
		return err
	}
	name := custodianName(g.request.ID, "model-request")
	// The immutable request is the launch intent. Never repeat an uncertain
	// launch, including one whose first snapshot has not appeared yet.
	if _, err := os.Lstat(filepath.Join(g.dir, name)); !errors.Is(err, os.ErrNotExist) {
		return errors.New("detached model launch already recorded")
	}
	if err := saveWorkerJSON(g.dir, name, g.request); err != nil {
		return err
	}
	if err := g.launch(ctx, g.service, g.dir, g.request.ID); err != nil {
		return fmt.Errorf("detached model launch unconfirmed: %w", err)
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := g.serveGate(ctx); err != nil {
			return err
		}
		g.mu.Lock()
		err := g.read()
		v := g.last
		g.mu.Unlock()
		if err == nil {
			if v.Report.State == "ready" {
				return nil
			}
			if v.Finished || v.Report.State == "unload_failed" {
				return fmt.Errorf("detached model load failed: %s", v.Report.Error)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (g *modelCustodian) serveGate(ctx context.Context) error {
	var r custodianGate
	if readCustodianJSON(g.dir, custodianName(g.request.ID, "gate"), &r) != nil {
		return nil
	}
	e := g.request.Execution
	want := HostLaunch{Machine: e.Machine, Session: e.Session, Kind: "engine", Key: e.Model, Device: e.Device, Attempt: e.Attempt}
	if r.Nonce == "" || r.Launch != want {
		return errors.New("detached model launch gate identity mismatch")
	}
	var previous custodianGateReceipt
	if readCustodianJSON(g.dir, custodianName(g.request.ID, "gate-receipt"), &previous) == nil && previous.Nonce == r.Nonce {
		return nil
	}
	receipt := custodianGateReceipt{Nonce: r.Nonce}
	if err := checkLaunchGate(ctx); err != nil {
		receipt.Error = err.Error()
	}
	return saveWorkerJSON(g.dir, custodianName(g.request.ID, "gate-receipt"), receipt)
}

func (g *modelCustodian) Call(ctx context.Context, path string, data []byte) (*http.Response, error) {
	return g.CallRequest(ctx, "POST", path, data, http.Header{"Content-Type": []string{"application/json"}})
}

// The adjacent queue release still uses the worker bridge. Preserve that path;
// it already refuses live model takeover. Supporting queues route directly.
func (g *modelCustodian) CallRequest(ctx context.Context, method, path string, data []byte, headers http.Header) (*http.Response, error) {
	return g.StreamRequest(ctx, method, path, bytes.NewReader(data), int64(len(data)), headers)
}

func (g *modelCustodian) StreamRequest(ctx context.Context, method, path string, body io.Reader, length int64, headers http.Header) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, method, g.bridgeURL()+"/models/"+url.PathEscape(g.request.Execution.Model)+path, body)
	if err != nil {
		return nil, err
	}
	r.Header = proxyHeaders(headers)
	r.Header.Set("Authorization", "Bearer "+g.token)
	r.Header.Set("X-Myce-Session", g.request.Execution.Session)
	r.Header.Set("X-Myce-Engine-Attempt", g.request.Execution.Attempt)
	r.ContentLength = length
	return modelHTTPClient.Do(r)
}

func (g *modelCustodian) Unload() error {
	if err := saveWorkerJSON(g.dir, custodianName(g.request.ID, "model-stop"), modelCustodianStop{g.request.ID, g.request.Execution}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), max(g.request.Prepared.StopTimeout, 10*time.Second)+processPSTimeout)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		g.mu.Lock()
		err := g.read()
		v := g.last
		g.mu.Unlock()
		if err == nil && v.Finished {
			retireCustodian(g.request.ID)
			return nil
		}
		if err == nil && v.Report.State == "unload_failed" {
			return errors.New(v.Report.Error)
		}
		select {
		case <-ctx.Done():
			return errors.New("detached model cleanup unconfirmed; custody retained")
		case <-tick.C:
		}
	}
}

// Called with rt.mu held. A read failure withholds proof and measured calls;
// it never releases the reservation or invents a replacement engine.
func (g *modelCustodian) refreshModel(m *workerModel) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.verified = false
	if g.read() != nil {
		return
	}
	r := g.last.Report
	m.inflight, m.runtimeFailed = r.InFlight, r.RuntimeFailed
	if m.state != "loading" && m.state != "draining" && m.state != "unloading" || g.last.Finished || r.RuntimeFailed || r.State == "unload_failed" {
		m.state, m.err = r.State, r.Error
	}
}

func (g *modelCustodian) proof() *ModelCustody {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.verified || g.last.Finished || g.last.Report.State != "ready" {
		return nil
	}
	return &ModelCustody{Protocol: custodianProtocol, ID: g.request.ID}
}

func (g *modelCustodian) bridgeURL() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.last.URL
}

func (g *modelCustodian) updateOOMScore(score int) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	// Compare the latest requested policy, not a possibly delayed snapshot.
	// A new coordinator always writes its own first exact policy after adoption.
	if g.policyWritten && g.policyScore == score {
		return nil
	}
	if err := PrepareStateRoots(g.dir); err != nil {
		return err
	}
	v := modelCustodianPolicy{g.request.ID, g.request.Execution, score}
	if err := saveWorkerJSON(g.dir, custodianName(g.request.ID, "model-policy"), v); err != nil {
		return err
	}
	g.policyWritten, g.policyScore = true, score
	return nil
}

func modelRequests(dir string) ([]modelCustodianRequest, error) {
	paths, err := filepath.Glob(filepath.Join(custodianDir(dir), "custodian-*.model-request.json"))
	if err != nil {
		return nil, err
	}
	var requests []modelCustodianRequest
	for _, path := range paths {
		var r modelCustodianRequest
		if err := readCustodianJSON(filepath.Dir(path), filepath.Base(path), &r); err != nil {
			return nil, err
		}
		if r.Protocol != custodianProtocol || !validCustodianID(r.ID) || filepath.Base(path) != custodianName(r.ID, "model-request") || r.Execution.Session == "" || r.Execution.Attempt == "" || r.Execution.Model != r.Original.Name {
			return nil, errors.New("invalid detached model record")
		}
		var v modelCustodianSnapshot
		if readCustodianJSON(filepath.Dir(path), custodianName(r.ID, "snapshot"), &v) == nil && v.Protocol == custodianProtocol && v.ID == r.ID && v.Execution == r.Execution && v.Report.Name == r.Execution.Model && v.Report.Device == r.Execution.Device && v.Report.Attempt == r.Execution.Attempt && v.Finished && (v.Report.State == "failed" || v.Report.State == "unloaded") {
			continue
		}
		requests = append(requests, r)
	}
	return requests, nil
}

func retainedModelSession(dir, machine string) (string, error) {
	requests, err := modelRequests(dir)
	session := ""
	for _, r := range requests {
		if r.Execution.Machine != machine || session != "" && session != r.Execution.Session {
			return "", errors.New("detached models retain a different worker identity")
		}
		session = r.Execution.Session
	}
	return session, err
}

func (rt *modelRuntime) restoreModelCustodians() error {
	return rt.restoreModelCustodiansWith(modelCustodianMembership)
}

func (rt *modelRuntime) restoreModelCustodiansWith(verify func(string, int, string) error) error {
	requests, err := modelRequests(rt.dir)
	if err != nil {
		return err
	}
	for _, r := range requests {
		if r.Execution.Machine != rt.machine || r.Execution.Session != rt.session {
			return errors.New("detached model worker/session mismatch")
		}
		g := &modelCustodian{dir: custodianDir(rt.dir), request: r, clock: rt.clock, interval: time.Second, token: rt.token, verify: verify}
		if err := g.read(); err != nil {
			return fmt.Errorf("detached model adoption unconfirmed; custody retained: %w", err)
		}
		key := engineKey(r.Execution.Model, r.Execution.Device)
		if rt.models[key] != nil {
			return errors.New("multiple detached owners retain one model slot")
		}
		m := &workerModel{model: r.Original, device: r.Execution.Device, attempt: r.Execution.Attempt, engine: g, state: g.last.Report.State, inflight: g.last.Report.InFlight, runtimeFailed: g.last.Report.RuntimeFailed, err: g.last.Report.Error}
		if m.state == "loading" {
			return errors.New("detached model load is not ready for adoption")
		}
		rt.models[key] = m
	}
	return nil
}

// Reuse the installed service's existing detached entry and native isolation.
// No new public command, token-bearing argument or credential copy is needed.
func (w Worker) runExecutionCustodian(stop context.Context) error {
	roots, err := w.StateRoots()
	if err != nil {
		return err
	}
	if _, err := os.Lstat(filepath.Join(custodianDir(roots[0]), custodianName(w.Custodian, "model-request"))); err == nil {
		return w.runModelCustodian(stop, roots[0])
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return w.runCustodian(stop)
}

func (w Worker) runModelCustodian(stop context.Context, spool string) error {
	id, dir := w.Custodian, custodianDir(spool)
	if w.ServiceConfig == nil || !validCustodianID(id) {
		return errors.New("model custodian requires installed worker identity")
	}
	unlock, err := commandLock(dir, "custodian-"+id+".lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := os.Lstat(filepath.Join(dir, custodianName(id, "snapshot"))); !errors.Is(err, os.ErrNotExist) {
		return errors.New("model custodian already has custody")
	}
	var r modelCustodianRequest
	if err := readCustodianJSON(dir, custodianName(id, "model-request"), &r); err != nil {
		return err
	}
	if r.ID != id || r.Protocol != custodianProtocol || r.Execution.Machine != w.Machine.ID || r.Machine.ID != w.Machine.ID || r.Execution.Session == "" || r.Execution.Attempt == "" || r.Original.Name != r.Execution.Model || r.Prepared.Name != r.Execution.Model {
		return errors.New("model custodian request identity mismatch")
	}
	if r.OOMScore != 0 && r.OOMScore != 1000 {
		return errors.New("invalid model custodian OOM policy")
	}
	w.Machine = r.Machine
	if w.Clock == nil {
		w.Clock = RealClock{}
	}
	w.initMemoryCeilings()
	if err := w.requireMemoryCeilings(); err != nil {
		return err
	}
	isolation := w.custodianIsolation
	if isolation == nil {
		isolation = ownCustodianIsolation
	}
	cgroup, err := isolation(id)
	if err != nil {
		return err
	}
	birth, err := processBirth(os.Getpid())
	if err != nil {
		return err
	}
	engineDir := filepath.Join(dir, "model-"+id)
	if err := os.MkdirAll(engineDir, 0700); err != nil {
		return err
	}
	if r.EnvironmentKey != "" {
		cache := &EnvironmentCache{Root: r.EnvironmentRoot, Clock: w.Clock}
		if err := cache.initStorage(); err != nil {
			return err
		}
		use, err := cache.lock(stop, r.EnvironmentKey+".use", syscall.LOCK_SH)
		if err != nil {
			return err
		}
		defer unlockEnvironment(use)
	}
	address := w.ModelListen
	if address == "" {
		address = "127.0.0.1:0"
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return err
	}
	defer ln.Close()
	bridge := "http://" + ln.Addr().String()
	if w.ModelAdvertise != "" {
		u, err := url.Parse(w.ModelAdvertise)
		if err != nil || u.Hostname() == "" {
			return errors.New("model advertise host unavailable")
		}
		bridge = "http://" + net.JoinHostPort(u.Hostname(), fmt.Sprint(ln.Addr().(*net.TCPAddr).Port))
	}
	engine := NewModelEngine(r.Prepared, engineDir, w.Clock)
	if owned, ok := engine.(*LlamaEngine); ok {
		owned.memoryCeilings, owned.memoryMachine, owned.memorySwapMaxBytes = w.memoryCeilings, w.Machine, w.MemorySwapMaxBytes
		owned.memoryOOMScore.Store(int32(r.OOMScore))
	}
	ctx, cancel := context.WithCancel(stop)
	defer cancel()
	m := &workerModel{model: r.Original, device: r.Execution.Device, attempt: r.Execution.Attempt, engine: engine, state: "loading", cancel: cancel}
	rt := &modelRuntime{models: map[string]*workerModel{engineKey(r.Original.Name, r.Execution.Device): m}, token: w.Client.Token, session: r.Execution.Session, clock: w.Clock, dir: engineDir}
	server := &http.Server{Handler: http.HandlerFunc(rt.serve), ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	go server.Serve(ln)
	v := modelCustodianSnapshot{custodianSnapshot: custodianSnapshot{Protocol: custodianProtocol, ID: id, PID: os.Getpid(), Birth: birth, Cgroup: cgroup}, Execution: r.Execution, URL: bridge}
	oomScore := r.OOMScore
	publish := func() error {
		rt.mu.Lock()
		rt.checkEngineHealth(m)
		v.Report = ModelReport{Name: r.Original.Name, Device: m.device, Attempt: m.attempt, State: m.state, Error: m.err, InFlight: m.inflight, CallsMeasured: true, RuntimeFailed: m.runtimeFailed}
		v.OOMScore = oomScore
		if owned, ok := engine.(*LlamaEngine); ok {
			v.OOMScore = int(owned.memoryOOMScore.Load())
			if m.state == "ready" {
				_, v.Report.HealthRevision, v.Report.HealthDegraded = owned.runtimeHealth()
			}
		}
		v.Started, v.Finished = m.state == "ready", m.state == "failed" || m.state == "unloaded"
		rt.mu.Unlock()
		v.At = w.Clock.Now()
		return saveWorkerJSON(dir, custodianName(id, "snapshot"), v)
	}
	if err := publish(); err != nil {
		return err
	}
	loaded := make(chan error, 1)
	r.Prepared.EnvironmentPath = r.EnvironmentPath
	r.Prepared.secretRoot, r.Prepared.fleetToken = w.SecretRoot, w.Client.Token
	launch := HostLaunch{Machine: r.Execution.Machine, Session: r.Execution.Session, Kind: "engine", Key: r.Execution.Model, Device: r.Execution.Device, Attempt: r.Execution.Attempt}
	go func() {
		loaded <- engine.Load(withLaunchGate(ctx, func(ctx context.Context) error { return custodianLaunchGate(ctx, dir, id, launch) }), r.Prepared)
	}()
	tick := time.NewTicker(custodianInterval(w.Interval))
	defer tick.Stop()
	loading, stopping := true, false
	stopDone := stop.Done()
	for {
		select {
		case err := <-loaded:
			loading = false
			rt.mu.Lock()
			if err != nil {
				m.err, m.runtimeFailed = err.Error(), true
				rt.unload(m)
			} else if stopping {
				rt.unload(m)
			} else {
				m.state = "ready"
			}
			rt.mu.Unlock()
		case <-stopDone:
			stopDone = nil
			cancel()
			stopping = true
		case <-tick.C:
		}
		var policy modelCustodianPolicy
		if !stopping && readCustodianJSON(dir, custodianName(id, "model-policy"), &policy) == nil && policy.ID == id && policy.Execution == r.Execution && (policy.OOMScore == 0 || policy.OOMScore == 1000) {
			rt.mu.Lock()
			if err := updateModelOOMScore(m, policy.OOMScore); err != nil {
				m.err, m.runtimeFailed = err.Error(), true
				rt.unload(m)
			} else {
				oomScore = policy.OOMScore
			}
			rt.mu.Unlock()
		}
		var control modelCustodianStop
		if !stopping && readCustodianJSON(dir, custodianName(id, "model-stop"), &control) == nil && control.ID == id && control.Execution == r.Execution {
			stopping = true
			cancel()
		}
		if stopping && !loading {
			rt.mu.Lock()
			if m.state == "ready" || m.state == "draining" {
				rt.unload(m)
			}
			rt.mu.Unlock()
		}
		if err := publish(); err != nil {
			// A failed publication cannot abandon a live engine.
			cancel()
			stopping = true
		} else if v.Finished {
			rt.wg.Wait()
			return nil
		}
	}
}

func (q *Queue) modelAdoptionReason(m *Machine, p HostObligation) string {
	e := p.Engine
	if p.Call != nil {
		e = &p.Call.HostEngineExecution
	}
	if e == nil || e.Machine != m.ID || e.Session != m.Session {
		return "an older or different worker session retains model custody"
	}
	s := q.s.ModelSlots[slotKey(e.Machine, e.Model, e.Device)]
	if s == nil || s.Session != e.Session || s.Attempt != e.Attempt || !validModelCustody(s.Custody) || !validModelBridgeURL(s.URL) {
		return "model engine or response bridge has no detached custody"
	}
	if m.Fleet == nil || m.Fleet.PID == 0 || s.Custody.WorkerPID != m.Fleet.PID {
		return "current worker has not proved adoption of the detached model"
	}
	if s.State != "ready" && s.State != "draining" {
		return "detached model readiness or release is unconfirmed"
	}
	if p.Call != nil && q.obligationCall(p.Call) == nil {
		return "model call no longer belongs to the retained engine"
	}
	return ""
}
