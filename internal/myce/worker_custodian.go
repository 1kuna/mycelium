package myce

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// A custodian owns one execution, including its existing launch gates, process
// waits, cache pins, memory guards and result writer. It never registers another
// worker or changes the queue's attempt/session. Only the replaceable worker
// uploads logs/results and acknowledges release.
const custodianProtocol = 1

var workerCustodians = true // Test binaries can model an adjacent release.

// Linux shared-host jobs and resident engines currently share one hard CPU
// aggregate beneath the worker. Detaching only jobs would split that cap.
// Preserve its established isolation and use natural upgrade drain until the
// whole aggregate can be qualified outside worker custody.
func (w Worker) detachedAttemptSupported() bool {
	return workerCustodians && !(runtime.GOOS == "linux" && w.Machine.SharedHost != nil)
}

func (w Worker) detachesAssignedAttempts() bool {
	return w.detachedAttemptSupported() && w.ServiceConfig != nil && w.ServiceConfig.Config != "" && w.ServiceConfig.Binary != "" && w.Custodian == ""
}

type custodianRequest struct {
	CompactNativeLaunch bool        `json:"compact_native_launch,omitempty"`
	Protocol            int         `json:"protocol"`
	ID                  string      `json:"id"`
	Machine             Machine     `json:"machine"`
	State               workerState `json:"state"`
	AccountingRoots     []string    `json:"accounting_roots,omitempty"`
}

func (r custodianRequest) unstartedFor(id string, a attemptRecord) bool {
	return r.Protocol == custodianProtocol && r.ID == id && r.Machine.ID == a.machine && a.ownsState(r.State) &&
		r.State.Custodian == "" && custodianUnstarted(r.State)
}

type custodianSnapshot struct {
	Protocol       int                `json:"protocol"`
	ID             string             `json:"id"`
	PID            int                `json:"pid"`
	Birth          string             `json:"birth"`
	Cgroup         string             `json:"cgroup,omitempty"`
	At             time.Time          `json:"at"`
	Started        bool               `json:"started"`
	Finished       bool               `json:"finished"`
	State          workerState        `json:"state"`
	Memory         *CurrentHostMemory `json:"memory,omitempty"`
	Resources      *AttemptResources  `json:"resources,omitempty"`
	MemoryAt       time.Time          `json:"memory_at,omitempty"`
	DeviceMemoryAt time.Time          `json:"device_memory_at,omitempty"`
}

func (v custodianSnapshot) belongsTo(id string, a attemptRecord) bool {
	return v.Protocol == custodianProtocol && v.ID == id && a.ownsState(v.State)
}

type custodianControl struct {
	ID           string              `json:"id"`
	Execution    Execution           `json:"execution"`
	Session      string              `json:"session"`
	StopAt       *time.Time          `json:"stop_at,omitempty"`
	Kill         bool                `json:"kill,omitempty"`
	UserCancel   bool                `json:"user_cancel,omitempty"`
	PriorityStop *PriorityPreemption `json:"priority_stop,omitempty"`
}

func attemptControl(dir, id string, key attemptKey) (custodianControl, bool) {
	expected := custodianControl{ID: id, Execution: Execution{ID: key.job, Attempt: key.attempt}, Session: key.session}
	var saved custodianControl
	if readCustodianJSON(dir, custodianName(id, "control"), &saved) == nil && saved.ID == id && saved.Execution == expected.Execution && saved.Session == key.session {
		return saved, true
	}
	return expected, false
}

func (c *custodianControl) requestStop(deadline time.Time, kill, userCancel bool, priority *PriorityPreemption) {
	if c.StopAt == nil || deadline.Before(*c.StopAt) {
		c.StopAt = &deadline
	}
	c.Kill, c.UserCancel = c.Kill || kill, c.UserCancel || userCancel
	c.PriorityStop = priority
}

type attemptCustodian struct {
	dir, id      string
	mu           sync.Mutex
	last         custodianSnapshot
	observed     bool
	control      custodianControl
	controlSaved bool
	cancel       context.CancelFunc
}

func validCustodianID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16 && hex.EncodeToString(b) == id
}

func custodianName(id, kind string) string { return "custodian-" + id + "." + kind + ".json" }
func custodianUnit(id string) string       { return "myce-attempt-" + id + ".scope" }

// A sibling of the payload spool: plain jobs retain spool access, but may not
// read another execution's control, result or adoption authority.
func custodianDir(spool string) string { return spool + ".custody" }

var errCustodianStartup = errors.New("custodian_startup_failed: detached owner exited before startup")

// RecordCustodianStartupFailure is called by the exiting service entry point,
// or after its launcher has been reaped. runCustodian commits its first snapshot
// before any input writer or payload can run. Missing/corrupt evidence otherwise
// remains uncertain custody; a log line or missing PID never proves release.
func RecordCustodianStartupFailure(spool, id string) error {
	if !validCustodianID(id) {
		return errors.New("invalid detached attempt identity")
	}
	dir := custodianDir(spool)
	if _, err := os.Lstat(filepath.Join(dir, custodianName(id, "snapshot"))); !errors.Is(err, os.ErrNotExist) {
		return errors.New("detached startup is not proven unstarted")
	}
	var req custodianRequest
	if err := readCustodianJSON(dir, custodianName(id, "request"), &req); err != nil {
		return err
	}
	if !req.unstartedFor(id, attemptRecordFor(req.State.Job, req.Machine.ID)) {
		return errors.New("custodian request identity or prelaunch custody unconfirmed")
	}
	j := *req.State.Job
	s := &workerStore{dir: spool, state: req.State}
	clock := RealClock{}
	at := clock.Now()
	cause := s.startFailureAt(j, launchRefused, errCustodianStartup, at)
	// A refusal without its durable launch cause must not become a finished
	// result: ordinary replay could otherwise charge/lose a resumable experiment.
	var recorded *recordedStartError
	if !errors.As(cause, &recorded) {
		return cause
	}
	r := startFailureOutcome(Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Error: errCustodianStartup.Error()}, &executionEvidenceError{cause, unstartedExecution(j.Attempt)})
	timing := s.launchRecorder(clock)
	timing.finish("")
	r.LaunchTimings = timing.snapshot()
	s.state.Result = &r
	return saveWorkerJSON(dir, custodianName(id, "snapshot"), custodianSnapshot{Protocol: custodianProtocol, ID: id, Finished: true, State: s.state})
}

func waitCustodian(ctx context.Context, dir, id string) error {
	// Same one-minute activation allowance as an installed service. Expiry
	// leaves durable custody; it never kills or repeats the accepted launch.
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var v custodianSnapshot
		if err := readCustodianJSON(dir, custodianName(id, "snapshot"), &v); err == nil && v.ID == id {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func readCustodianJSON(dir, name string, out any) error {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	return decodeWorkerJSON(b, out)
}

func (w Worker) startCustodian(ctx context.Context, s *workerStore, j Job) (*process, error) {
	if s.state.Custodian != "" {
		p, err := w.adoptCustodian(s) // An uncertain launch is never repeated.
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errWorkerCheckpoint, err)
		}
		return p, nil
	}
	timing := s.launchRecorder(w.Clock)
	timing.enter(launchCustodianSetup)
	id := ID()
	dir := custodianDir(s.dir)
	if err := PrepareStateRoots(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	j.LaunchTimings = timing.snapshot()
	state := s.state
	state.Job, state.Custodian = &j, ""
	r := custodianRequest{Protocol: custodianProtocol, ID: id, Machine: w.Machine, State: state, CompactNativeLaunch: w.compactNativeLaunch}
	if w.memoryCeilings != nil {
		r.AccountingRoots = []string{w.memoryCeilings.root}
	}
	if err := saveWorkerJSON(dir, custodianName(id, "request"), r); err != nil {
		return nil, err
	}
	s.state.Job, s.state.Custodian = &j, id
	if err := s.save(); err != nil {
		return nil, err
	}
	// No token in arguments, environment or durable launch records. The helper
	// reads the same private installed configuration as the ordinary service.
	if err := launchAttemptCustodian(ctx, *w.ServiceConfig, dir, id, Execution{ID: j.ID, Attempt: j.Attempt}); err != nil {
		return nil, fmt.Errorf("%w: detached attempt launch unconfirmed: %v", errWorkerCheckpoint, err)
	}
	p, err := w.adoptCustodian(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errWorkerCheckpoint, err)
	}
	return p, nil
}

// A queue stop is authority to interrupt an unadopted helper; cancellation of
// the worker's own context is not (service replacement must leave it running).
func persistUnadoptedCustodianStop(s *workerStore, j Job, d AttemptDirective, now time.Time) error {
	if !d.Stop || s.state.Custodian == "" {
		return nil
	}
	dir, id := custodianDir(s.dir), s.state.Custodian
	control, _ := attemptControl(dir, id, jobKey(j))
	deadline := now.Add(time.Duration(max(0, d.GraceSeconds) * float64(time.Second)))
	control.requestStop(deadline, d.Kill, d.UserCancel, d.PriorityStop)
	return saveWorkerJSON(dir, custodianName(id, "control"), control)
}

func (w Worker) adoptCustodian(s *workerStore) (*process, error) {
	id := s.state.Custodian
	if !validCustodianID(id) || s.state.Job == nil {
		return nil, errors.New("invalid detached attempt custody")
	}
	a := attemptRecordFor(s.state.Job, s.state.Job.Machine)
	if !a.ownsState(s.state) {
		return nil, errors.New("invalid detached attempt custody")
	}
	g := &attemptCustodian{dir: custodianDir(s.dir), id: id}
	if err := g.read(*s.state.Job); err != nil {
		if recoveryErr := w.recoverUnstartedCustodian(s); recoveryErr != nil {
			return nil, fmt.Errorf("detached attempt adoption unconfirmed; custody retained: %w", errors.Join(err, recoveryErr))
		}
		if err := g.read(*s.state.Job); err != nil {
			return nil, fmt.Errorf("detached attempt recovery unconfirmed; custody retained: %w", err)
		}
	}
	if s.state.Result != nil {
		v := g.snapshot()
		if !v.Finished || !a.ownsResult(s.state.Result) || v.State.Identity != s.state.Identity {
			return nil, errors.New("saved publication conflicts with detached custody")
		}
		// The parent owns delivery once it records the exact exit. Its saved
		// offsets and publication result are newer than the helper snapshot.
		// Upgrading a pending release still requires the durable cleanup receipt.
		if v.State.Result.ReleasePending && !s.state.Result.ReleasePending {
			r, err := s.resultReceipt(a)
			if err != nil || !a.readyToRelease(r, false) {
				return nil, fmt.Errorf("detached cleanup receipt unconfirmed: %w", errors.Join(errWorkerReleasePending, err))
			}
		}
		return nil, nil
	}
	g.updateStore(s)
	p := &process{job: *s.state.Job, custodian: g, done: make(chan workerFinish, 1), savePriorityStop: s.priorityStopRecorder()}
	g.control, g.controlSaved = attemptControl(g.dir, id, jobKey(p.job))
	// Use the same proxy after launch and restart. Reading a result always checks
	// its original attempt/session; a live PID is never inferred from its number.
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	go func() {
		tick := time.NewTicker(min(custodianInterval(w.Interval), 100*time.Millisecond))
		defer tick.Stop()
		defer cancel()
		for {
			w.serveCustodianGate(ctx, g, p.job)
			v := g.snapshot()
			if v.Finished && v.State.Result != nil {
				p.done <- workerFinish{result: *v.State.Result}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if err := g.read(p.job); err != nil {
				// Never turn failed observation into a signal or a release claim.
				g.mu.Lock()
				g.observed = false
				g.mu.Unlock()
			}
		}
	}()
	return p, nil
}

func (g *attemptCustodian) read(j Job) error {
	var v custodianSnapshot
	if err := readCustodianJSON(g.dir, custodianName(g.id, "snapshot"), &v); err != nil {
		return err
	}
	if v.Finished {
		return g.acceptFinished(v, j)
	}
	if !v.belongsTo(g.id, attemptRecordFor(&j, j.Machine)) {
		return errors.New("detached attempt identity mismatch")
	}
	birth, err := processBirth(v.PID)
	if err != nil || birth == "" || birth != v.Birth {
		// Completion is committed before exit. The read may have raced
		// that atomic rename; require the exact finished result on reread.
		var final custodianSnapshot
		if readCustodianJSON(g.dir, custodianName(g.id, "snapshot"), &final) == nil && final.Finished {
			return g.acceptFinished(final, j)
		}
		return errors.New("detached custodian process birth unconfirmed")
	}
	previous := g.snapshot()
	if previous.PID != v.PID || previous.Birth != v.Birth || previous.Cgroup != v.Cgroup {
		if err := verifyCustodianIsolation(g.id, v.PID, v.Cgroup); err != nil {
			return err
		}
	}
	g.observe(v)
	return nil
}

func (g *attemptCustodian) acceptFinished(v custodianSnapshot, j Job) error {
	a := attemptRecordFor(&j, j.Machine)
	if !v.Finished || !v.belongsTo(g.id, a) || !a.ownsResult(v.State.Result) {
		return errors.New("detached result identity mismatch")
	}
	g.observe(v)
	return nil
}

func (g *attemptCustodian) observe(v custodianSnapshot) {
	if v.Memory != nil {
		v.Memory.sampledAt, v.Memory.deviceSampledAt = v.MemoryAt, v.DeviceMemoryAt
	}
	g.mu.Lock()
	g.last, g.observed = v, true
	g.mu.Unlock()
}

func custodianInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return time.Second
	}
	return interval
}

// Keep the independently owned preparation observable while its runner blocks
// in staging/build I/O. Freeze the last committed snapshot instead of reading
// workerStore concurrently with that writer. Join before the next publication,
// so a preparation heartbeat can never overwrite Started or a final result.
func (w Worker) keepCustodianPreparationLive(dir, id string) func() {
	var v custodianSnapshot
	if readCustodianJSON(dir, custodianName(id, "snapshot"), &v) != nil {
		return func() {} // Failed observation expires normally; never invent proof.
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(custodianInterval(w.Interval))
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				v.At = w.Clock.Now()
				_ = saveWorkerJSON(dir, custodianName(id, "snapshot"), v)
			}
		}
	}()
	return func() { close(stop); <-done }
}

func (g *attemptCustodian) snapshot() custodianSnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.last
}

func (g *attemptCustodian) updateStore(s *workerStore) {
	v := g.snapshot().State
	// The parent exclusively owns upload offsets and publication custody.
	s.state.Job, s.state.Identity, s.state.Log = v.Job, v.Identity, v.Log
	s.state.PID, s.state.Birth, s.state.Cgroup, s.state.Groups = v.PID, v.Birth, v.Cgroup, v.Groups
	s.state.LaunchPending, s.state.InputWriterPending = v.LaunchPending, v.InputWriterPending
	// A failed preparation can retain a builder without any payload PID. Carry
	// its durable receipt into the existing exact-cleanup recovery path.
	s.state.Preparation, s.state.CustodyID = v.Preparation, v.CustodyID
	s.state.Accelerator, s.state.StopGrace = v.Accelerator, v.StopGrace
	s.state.LaunchOutcome = v.LaunchOutcome
}

func (g *attemptCustodian) detach() {
	if g.cancel != nil {
		g.cancel()
	}
}

// Called under the proxy process mutex. The custodian owns actual delivery,
// records its existing signal receipts and enforces this original deadline.
func (g *attemptCustodian) stopLocked(p *process, now time.Time, grace time.Duration, kill bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	previous := g.control
	g.control.requestStop(now.Add(grace), kill, p.userCancel, p.priorityStop)
	// Failed persistence retains stop intent for the next control-loop retry.
	if g.controlSaved && reflect.DeepEqual(previous, g.control) {
		return
	}
	g.controlSaved = saveWorkerJSON(g.dir, custodianName(g.id, "control"), g.control) == nil
}

func (p *process) started() bool {
	if p.custodian != nil {
		return p.custodian.snapshot().Started
	}
	return p.native == nil || p.native.started()
}

func custodiansDetached(actors map[attemptKey]*attemptSupervisor) bool {
	for _, a := range actors {
		a.mu.Lock()
		released := a.released || a.finished
		detached := a.detached
		a.mu.Unlock()
		if !released && !detached {
			return false
		}
	}
	return true
}

// initAttemptRuntime is shared by the coordinator and detached entry. Keep
// launch dependencies here; coordinator-only loops and retention stay in Run.
func (w *Worker) initAttemptRuntime(stop context.Context, s *workerStore) error {
	if w.ExtensionRoot == "" {
		w.ExtensionRoot = filepath.Join(s.dir, "extensions")
	}
	w.diskBudget = newWorkerDiskBudget(w.Clock, w.DiskProbe)
	w.diskBudget.inventoryDir = s.dir
	w.diskBudget.add(diskBudgetRoot{name: "attempts", path: s.dir})
	w.compilerCache = &workerCompilerCache{cache: EnvironmentCache{Root: filepath.Join(s.dir, "compiler-cache"), Clock: w.Clock}}
	if err := w.compilerCache.cache.initStorage(); err != nil {
		return err
	}
	w.diskBudget.add(diskBudgetRoot{name: "compiler", path: w.compilerCache.cache.Root, candidates: w.compilerCache.budgetCandidates})
	if err := w.initFileTransfers(stop, s); err != nil {
		return err
	}
	if w.EnvironmentRoot == "" {
		w.EnvironmentRoot = filepath.Join(s.dir, "environment-cache")
	}
	w.environmentCache = &EnvironmentCache{Root: w.EnvironmentRoot, Limit: w.EnvironmentLimit, Clock: w.Clock, Fetch: w.fileCache.copyTo, Run: w.environmentRun, sizeLimit: w.environmentSizeLimit, growToNeed: w.devEnvironmentSizing}
	w.environmentCache.budget = w.diskBudget
	w.environmentCache.references = w.environmentReferenced
	if err := w.environmentCache.initStorage(); err != nil {
		return err
	}
	w.diskBudget.add(diskBudgetRoot{name: "environments", path: w.EnvironmentRoot, candidates: w.environmentCache.budgetCandidates})
	return nil
}

func (w Worker) runCustodian(stop context.Context) error {
	if w.Clock == nil {
		w.Clock = RealClock{}
	}
	entered := w.Clock.Now()
	if w.ServiceConfig == nil || !validCustodianID(w.Custodian) {
		return errors.New("custodian requires installed worker configuration and exact identity")
	}
	roots, err := w.StateRoots()
	if err != nil {
		return err
	}
	spool, id := roots[0], w.Custodian
	dir := custodianDir(spool)
	unlock, err := commandLock(dir, "custodian-"+id+".lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := os.Lstat(filepath.Join(dir, custodianName(id, "snapshot"))); !errors.Is(err, os.ErrNotExist) {
		return errors.New("detached attempt already has custody; refusing repeated launch")
	}
	var req custodianRequest
	if err := readCustodianJSON(dir, custodianName(id, "request"), &req); err != nil {
		return err
	}
	if !req.unstartedFor(id, attemptRecordFor(req.State.Job, w.Machine.ID)) {
		return errors.New("custodian request identity mismatch")
	}
	w.Machine = req.Machine
	w.compactNativeLaunch = req.CompactNativeLaunch
	if w.Interval <= 0 {
		w.Interval = time.Second
	}
	w.initInventoryProbe()
	w.Client.Clock = w.Clock
	w.Client.launchTimingWire = &atomic.Bool{}
	w.rootMonitor = &workerRootMonitor{expected: map[string]string{}}
	// Linux adoption requires the delegated worker leaf. Refuse before the first
	// snapshot if setup fails, so a helper can never wait at an unserviceable gate.
	if runtime.GOOS == "linux" {
		w.RequireMemoryCeilings = true
	}
	w.initMemoryCeilings()
	if err := w.requireMemoryCeilings(); err != nil {
		return err
	}
	if w.HostMemorySource == nil {
		w.hostRawMemorySource, err = workerHostMemorySource(0)
		if err != nil {
			return err
		}
		raw := w.hostRawMemorySource
		w.HostMemorySource = func(ctx context.Context) (float64, error) {
			n, e := raw(ctx)
			return max(0, n-w.HostMemoryReserveGB), e
		}
	}
	s := &workerStore{dir: spool, stateName: custodianName(id, "state"), state: req.State, liveEvidence: unstartedExecution(req.State.Job.Attempt), liveEvidenceKey: jobKey(*req.State.Job)}
	timing := s.launchRecorder(w.Clock)
	// The detached process starts with a serialized same-host timestamp.
	// Reattach the local monotonic clock at the first available observation.
	if timing != nil && timing.value.CurrentPhase != "" {
		timing.advance(entered)
		timing.value.CurrentPhase = launchCustodianInit
	}
	w.checkpointExpiry = newWorkerCheckpointExpiry()
	w.checkNativeSeqpacketStartup()
	if w.memoryCeilings != nil {
		w.memoryCeilings.accountingRoots = append(append([]string{}, req.AccountingRoots...), filepath.Dir(w.memoryCeilings.root))
	}
	w.launchCtx = stop
	w.batchAdmission = func(ctx context.Context, launch HostLaunch) error { return custodianLaunchGate(ctx, dir, id, launch) }
	w.hostChargesExcept = custodianHostCharges(dir, id)
	if err := w.initAttemptRuntime(stop, s); err != nil {
		return err
	}
	pid := os.Getpid()
	birth, err := processBirth(pid)
	if err != nil {
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
	v := custodianSnapshot{Protocol: custodianProtocol, ID: id, PID: pid, Birth: birth, Cgroup: cgroup}
	publish := func(p *process, finished bool) error {
		v.At, v.State, v.Finished = w.Clock.Now(), s.state, finished
		if timing := s.launch.snapshot(); timing != nil {
			job := *s.state.Job
			job.LaunchTimings = timing
			v.State.Job = &job
		}
		if finished && v.State.Result != nil && v.State.Result.LaunchTimings == nil {
			r := *v.State.Result
			r.LaunchTimings = v.State.Job.LaunchTimings
			v.State.Result = &r
		}
		if p != nil {
			v.Started, v.Resources = p.started(), p.resourceSnapshot()
			v.Memory = p.currentProcessHostMemory()
			if p.memory != nil {
				v.Memory = p.memory.currentHostMemory(w.Clock)
			} else if p.native != nil && p.native.memory != nil {
				v.Memory = p.native.memory.currentHostMemory(w.Clock)
			}
			if v.Memory != nil {
				v.Memory = v.Memory.aged(w.Clock.Now())
				v.MemoryAt, v.DeviceMemoryAt = v.Memory.sampledAt, v.Memory.deviceSampledAt
				v.Memory.deviceSample = nil
			}
			p.mu.Lock()
			if p.native != nil {
				v.State.Groups = make(map[int]map[int]string, len(p.native.custody.Groups))
				for group, births := range p.native.custody.Groups {
					v.State.Groups[group] = maps.Clone(births)
				}
				v.State.Cgroup = p.native.custody.Cgroup
			} else {
				var custody workerState
				if readCustodianJSON(spool, "attempt-"+v.State.Identity+".json", &custody) == nil {
					v.State.Groups, v.State.Cgroup = custody.Groups, custody.Cgroup
				}
			}
			v.State.Birth = v.State.Groups[v.State.PID][v.State.PID]
			p.mu.Unlock()
		}
		return saveWorkerJSON(dir, custodianName(id, "snapshot"), v)
	}
	if err := publish(nil, false); err != nil {
		return err
	}
	// Control observes files, never queue availability. Cancellation interrupts
	// preparation; a running process retains the ordinary graceful-stop path.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prepareCtx, cancelPreparation := context.WithCancel(ctx)
	defer cancelPreparation()
	var mu sync.Mutex
	var running *process
	controlDone := make(chan struct{})
	key := jobKey(*req.State.Job) // Preparation can update the job while control watches.
	go func() {
		defer close(controlDone)
		tick := time.NewTicker(10 * time.Millisecond) // Existing process-control cadence.
		defer tick.Stop()
		for {
			c, _ := attemptControl(dir, id, key)
			if c.StopAt != nil || stop.Err() != nil {
				cancelPreparation()
			}
			mu.Lock()
			p := running
			mu.Unlock()
			if p != nil {
				now := w.Clock.Now()
				if c.StopAt != nil && p.recordPriorityStop(nil, c.PriorityStop) == nil {
					p.stopDirective(now, max(0, c.StopAt.Sub(now)), c.Kill, c.UserCancel)
				}
				if stop.Err() != nil {
					p.stopService(now, StopGrace)
				}
				p.enforceGrace(now)
				p.refreshEscapers(now)
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	defer func() { cancel(); <-controlDone }()
	tick := time.NewTicker(custodianInterval(w.Interval))
	defer tick.Stop()
	var p *process
	for p == nil && s.state.Result == nil {
		stopHeartbeat := w.keepCustodianPreparationLive(dir, id)
		p, err = w.startAssignedAttempt(prepareCtx, s, *req.State.Job)
		stopHeartbeat()
		if err != nil && !retryableAttemptStart(err) {
			r := startFailureOutcome(Result{Machine: w.Machine.ID, Session: req.State.Session, Execution: Execution{ID: req.State.Job.ID, Attempt: req.State.Job.Attempt}, Error: err.Error(), ExtensionUnavailable: errors.Is(err, errExtensionUnavailable), ReleasePending: errors.Is(err, errWorkerReleasePending)}, err)
			if cause := safeRuntimeCause(r.ExecutionCause); cause != nil {
				log.Printf("event=attempt_start_failed job=%s attempt=%d cause=%s", r.ID, r.Attempt, cause.String())
			}
			s.state.Result = &r
		}
		if p == nil && s.state.Result == nil {
			if e := publish(nil, false); e != nil {
				return e
			}
			<-tick.C
		}
	}
	mu.Lock()
	running = p
	mu.Unlock()
	if p != nil {
		for {
			if err := publish(p, false); err != nil {
				// Do not discard the sole process waiter when a metadata write
				// fails. Continue guarding and retry on the existing cadence.
				<-tick.C
				continue
			}
			select {
			case f := <-p.done:
				if f.err != nil {
					return f.err
				}
				s.state.Result = &f.result
				for {
					if err := publish(p, true); err == nil {
						return nil
					}
					<-tick.C
				}
			case <-tick.C:
			}
		}
	}
	return publish(nil, true)
}

// Only the publishing worker retires control records, after exact release and
// result acknowledgement. Ordinary spool receipts retain the attempt history.
func cleanupCustodian(dir, id string, result Result) {
	if !validCustodianID(id) {
		return
	}
	keepLogs := retireAttemptCustodian(dir, id, result)
	for _, kind := range []string{"request", "snapshot", "state", "control", "ledger", "gate", "gate-receipt"} {
		_ = os.Remove(filepath.Join(dir, custodianName(id, kind)))
	}
	suffixes := []string{".plist", ".lock"}
	if !keepLogs {
		suffixes = append(suffixes, ".log", ".stderr")
	}
	for _, suffix := range suffixes {
		_ = os.Remove(filepath.Join(dir, "custodian-"+id+suffix))
	}
}
