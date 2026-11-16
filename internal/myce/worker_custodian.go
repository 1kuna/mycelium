package myce

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
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

type custodianRequest struct {
	Protocol        int         `json:"protocol"`
	ID              string      `json:"id"`
	Machine         Machine     `json:"machine"`
	State           workerState `json:"state"`
	AccountingRoots []string    `json:"accounting_roots,omitempty"`
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

type custodianControl struct {
	ID           string              `json:"id"`
	Execution    Execution           `json:"execution"`
	Session      string              `json:"session"`
	StopAt       *time.Time          `json:"stop_at,omitempty"`
	Kill         bool                `json:"kill,omitempty"`
	UserCancel   bool                `json:"user_cancel,omitempty"`
	PriorityStop *PriorityPreemption `json:"priority_stop,omitempty"`
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
	id := ID()
	dir := custodianDir(s.dir)
	if err := PrepareStateRoots(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	state := s.state
	state.Job, state.Custodian = &j, ""
	r := custodianRequest{Protocol: custodianProtocol, ID: id, Machine: w.Machine, State: state}
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
	if err := launchCustodian(ctx, *w.ServiceConfig, dir, id); err != nil {
		return nil, fmt.Errorf("%w: detached attempt launch unconfirmed: %v", errWorkerCheckpoint, err)
	}
	p, err := w.adoptCustodian(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errWorkerCheckpoint, err)
	}
	return p, nil
}

func (w Worker) adoptCustodian(s *workerStore) (*process, error) {
	id := s.state.Custodian
	if !validCustodianID(id) || s.state.Job == nil {
		return nil, errors.New("invalid detached attempt custody")
	}
	g := &attemptCustodian{dir: custodianDir(s.dir), id: id}
	if err := g.read(*s.state.Job); err != nil {
		return nil, fmt.Errorf("detached attempt adoption unconfirmed; custody retained: %w", err)
	}
	g.updateStore(s)
	p := &process{job: *s.state.Job, custodian: g, done: make(chan workerFinish, 1), savePriorityStop: s.priorityStopRecorder()}
	g.control = custodianControl{ID: id, Execution: Execution{ID: p.job.ID, Attempt: p.job.Attempt}, Session: p.job.Session}
	var control custodianControl
	if readCustodianJSON(g.dir, custodianName(id, "control"), &control) == nil && control.ID == id && control.Execution == g.control.Execution && control.Session == g.control.Session {
		g.control = control
		g.controlSaved = true
	}
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
	if v.Protocol != custodianProtocol || v.ID != g.id || v.State.Job == nil || jobKey(*v.State.Job) != jobKey(j) || v.State.Session != j.Session {
		return errors.New("detached attempt identity mismatch")
	}
	if v.Finished {
		r := v.State.Result
		if r == nil || r.ID != j.ID || r.Attempt != j.Attempt || r.Session != j.Session || r.Machine != j.Machine {
			return errors.New("detached result identity mismatch")
		}
	} else {
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
		g.mu.Lock()
		previous := g.last
		g.mu.Unlock()
		if previous.PID != v.PID || previous.Birth != v.Birth || previous.Cgroup != v.Cgroup {
			if err := verifyCustodianIsolation(g.id, v.PID, v.Cgroup); err != nil {
				return err
			}
		}
	}
	g.mu.Lock()
	if v.Memory != nil {
		v.Memory.sampledAt, v.Memory.deviceSampledAt = v.MemoryAt, v.DeviceMemoryAt
	}
	g.last = v
	g.observed = true
	g.mu.Unlock()
	return nil
}

func (g *attemptCustodian) acceptFinished(v custodianSnapshot, j Job) error {
	r := v.State.Result
	if v.Protocol != custodianProtocol || v.ID != g.id || v.State.Job == nil || jobKey(*v.State.Job) != jobKey(j) || v.State.Session != j.Session || r == nil || r.ID != j.ID || r.Attempt != j.Attempt || r.Session != j.Session || r.Machine != j.Machine {
		return errors.New("detached result identity mismatch")
	}
	g.mu.Lock()
	g.last, g.observed = v, true
	g.mu.Unlock()
	return nil
}

func custodianInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return time.Second
	}
	return interval
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
	deadline := now.Add(grace)
	if g.control.StopAt == nil || deadline.Before(*g.control.StopAt) {
		g.control.StopAt = &deadline
	}
	g.control.Kill = g.control.Kill || kill
	g.control.UserCancel = g.control.UserCancel || p.userCancel
	g.control.PriorityStop = p.priorityStop
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

func (w Worker) runCustodian(stop context.Context) error {
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
	var req custodianRequest
	if err := readCustodianJSON(dir, custodianName(id, "request"), &req); err != nil {
		return err
	}
	if req.Protocol != custodianProtocol || req.ID != id || req.State.Job == nil || req.Machine.ID != w.Machine.ID || req.State.Job.Machine != w.Machine.ID || req.State.Session != req.State.Job.Session {
		return errors.New("custodian request identity mismatch")
	}
	w.Machine, w.Clock = req.Machine, RealClock{}
	w.Client.Clock = w.Clock
	w.rootMonitor = &workerRootMonitor{expected: map[string]string{}}
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
	s := &workerStore{dir: spool, stateName: custodianName(id, "state"), state: req.State}
	w.checkpointExpiry = newWorkerCheckpointExpiry()
	w.checkNativeSeqpacketStartup()
	if w.memoryCeilings != nil {
		w.memoryCeilings.accountingRoots = append(append([]string{}, req.AccountingRoots...), filepath.Dir(w.memoryCeilings.root))
	}
	w.launchCtx = stop
	w.batchAdmission = func(ctx context.Context, launch HostLaunch) error { return custodianLaunchGate(ctx, dir, id, launch) }
	w.hostChargesExcept = func(attemptKey) (float64, float64) {
		var ledger custodianLedger
		if readCustodianJSON(dir, custodianName(id, "ledger"), &ledger) != nil {
			return w.Machine.HostMemoryBudgetGB, w.Machine.HostMemoryBudgetGB
		}
		return ledger.Charge, ledger.Pending
	}
	w.diskBudget = newWorkerDiskBudget(w.Clock, w.DiskProbe)
	w.diskBudget.inventoryDir = spool
	w.diskBudget.add(diskBudgetRoot{name: "attempts", path: spool})
	w.compilerCache = &workerCompilerCache{cache: EnvironmentCache{Root: filepath.Join(spool, "compiler-cache"), Clock: w.Clock}}
	if err := w.compilerCache.cache.initStorage(); err != nil {
		return err
	}
	w.diskBudget.add(diskBudgetRoot{name: "compiler", path: w.compilerCache.cache.Root, candidates: w.compilerCache.budgetCandidates})
	if err := w.initFileTransfers(stop, s); err != nil {
		return err
	}
	if w.EnvironmentRoot == "" {
		w.EnvironmentRoot = filepath.Join(spool, "environment-cache")
	}
	w.environmentCache = &EnvironmentCache{Root: w.EnvironmentRoot, Limit: w.EnvironmentLimit, Clock: w.Clock}
	w.environmentCache.budget = w.diskBudget
	w.environmentCache.references = w.environmentReferenced
	if err := w.environmentCache.initStorage(); err != nil {
		return err
	}
	w.diskBudget.add(diskBudgetRoot{name: "environments", path: w.EnvironmentRoot, candidates: w.environmentCache.budgetCandidates})
	pid := os.Getpid()
	birth, err := processBirth(pid)
	if err != nil {
		return err
	}
	cgroup, err := ownCustodianIsolation(id)
	if err != nil {
		return err
	}
	v := custodianSnapshot{Protocol: custodianProtocol, ID: id, PID: pid, Birth: birth, Cgroup: cgroup}
	publish := func(p *process, finished bool) error {
		v.At, v.State, v.Finished = w.Clock.Now(), s.state, finished
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
	go func() {
		defer close(controlDone)
		tick := time.NewTicker(10 * time.Millisecond) // Existing process-control cadence.
		defer tick.Stop()
		for {
			var c custodianControl
			e := readCustodianJSON(dir, custodianName(id, "control"), &c)
			valid := e == nil && c.ID == id && c.Execution.ID == req.State.Job.ID && c.Execution.Attempt == req.State.Job.Attempt && c.Session == req.State.Session
			if valid && c.StopAt != nil || stop.Err() != nil {
				cancelPreparation()
			}
			mu.Lock()
			p := running
			mu.Unlock()
			if p != nil {
				now := w.Clock.Now()
				if valid && c.StopAt != nil && p.recordPriorityStop(nil, c.PriorityStop) == nil {
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
		p, err = w.startAssignedAttempt(prepareCtx, s, *req.State.Job)
		if err != nil && !errors.Is(err, errWorkerCheckpoint) && !errors.Is(err, errWorkerUnavailable) {
			r := startFailureOutcome(Result{Machine: w.Machine.ID, Session: req.State.Session, Execution: Execution{ID: req.State.Job.ID, Attempt: req.State.Job.Attempt}, Error: err.Error(), ReleasePending: errors.Is(err, errWorkerReleasePending)}, err)
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
func cleanupCustodian(dir, id string) {
	if !validCustodianID(id) {
		return
	}
	retireCustodian(id)
	for _, kind := range []string{"request", "snapshot", "state", "control", "ledger", "gate", "gate-receipt"} {
		_ = os.Remove(filepath.Join(dir, custodianName(id, kind)))
	}
	for _, suffix := range []string{".plist", ".log", ".lock"} {
		_ = os.Remove(filepath.Join(dir, "custodian-"+id+suffix))
	}
}
