package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

// Each actor exclusively owns an existing workerStore and its bulk I/O. The
// machine loop only exchanges small snapshots and never waits for preparation,
// checkpoint transfer, log upload or a result acknowledgement.
type attemptSupervisor struct {
	store       *workerStore
	job         Job
	mu          sync.Mutex
	report      AttemptReport
	diagnostics Machine
	directive   AttemptDirective
	worker      Worker
	connected   bool
	finished    bool
	err         error
	wake        chan struct{}
	done        chan struct{}
	recovering  *process
}

func jobKey(j Job) attemptKey { return attemptKey{j.ID, j.Attempt, j.Session} }
func (a *attemptSupervisor) snapshot() (AttemptReport, Machine, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.report, a.diagnostics, a.finished, a.err
}
func (a *attemptSupervisor) update(w Worker, d AttemptDirective, connected bool) {
	a.mu.Lock()
	a.worker, a.directive, a.connected = w, d, connected
	a.mu.Unlock()
	select {
	case a.wake <- struct{}{}:
	default:
	}
}
func (a *attemptSupervisor) run(ctx context.Context) {
	defer close(a.done)
	a.mu.Lock()
	initial := a.worker
	a.mu.Unlock()
	defer initial.control.remove(a.job)
	s := a.store
	p := a.recovering
	defer func() {
		if p != nil {
			p.stop(initial.Clock.Now(), 0, true)
			if p.docker != nil {
				p.docker.close()
			} else {
				f := <-p.done
				if f.err != nil {
					a.mu.Lock()
					a.err = errors.Join(a.err, f.err)
					a.mu.Unlock()
				}
			}
		}
	}()
	tick := time.NewTicker(min(initial.Interval, 100*time.Millisecond))
	defer tick.Stop()
	finish := func(err error) { a.mu.Lock(); a.err, a.finished = err, true; a.mu.Unlock() }
	for ctx.Err() == nil {
		a.mu.Lock()
		w, d, connected := a.worker, a.directive, a.connected
		a.mu.Unlock()
		now := w.Clock.Now()
		if p != nil {
			p.enforceGrace(now)
			select {
			case f := <-p.done:
				w.control.remove(p.job)
				p = nil
				if f.err != nil {
					finish(fmt.Errorf("persist command result: %w", f.err))
					return
				}
				if s.state.OperatorStopped {
					f.result = operatorOutcome(f.result, true)
				}
				s.state.Result, s.state.PID = &f.result, 0
				if err := s.save(); err != nil {
					finish(err)
					return
				}
			default:
			}
		}
		if connected {
			if p == nil && s.state.Result == nil {
				if d.Stop {
					cause := s.startFailure(a.job, launchRefused, errors.New("assignment withdrawn before command started"))
					r := startFailureOutcome(Result{Machine: w.Machine.ID, Session: a.job.Session, Execution: Execution{ID: a.job.ID, Attempt: a.job.Attempt}, Error: cause.Error()}, cause)
					s.state.Result = &r
				} else if !d.Hold {
					bound, err := w.forAttempt(a.job)
					if err == nil {
						p, err = bound.start(s, a.job)
					} else {
						err = s.startFailure(a.job, launchRefused, err)
					}
					if err != nil && !errors.Is(err, errWorkerCheckpoint) && !errors.Is(err, errWorkerMemory) && !errors.Is(err, errWorkerUnavailable) {
						r := startFailureOutcome(Result{Machine: w.Machine.ID, Session: a.job.Session, Execution: Execution{ID: a.job.ID, Attempt: a.job.Attempt}, Error: err.Error(), ExtensionUnavailable: errors.Is(err, errExtensionUnavailable), ReleasePending: errors.Is(err, errWorkerReleasePending)}, err)
						s.state.Result = &r
					}
				}
				if err := s.save(); err != nil {
					finish(err)
					return
				}
			}
			if p != nil && d.Stop {
				if err := p.recordPriorityStop(s, d.PriorityStop); err != nil {
					finish(err)
					return
				}
				p.stop(now, time.Duration(max(0, d.GraceSeconds)*float64(time.Second)), d.Kill)
			}
		}
		report := AttemptReport{Execution: Execution{ID: a.job.ID, Attempt: a.job.Attempt}, Session: a.job.Session, Started: p != nil && (p.docker == nil || p.docker.status() == "running") && (p.native == nil || p.native.started())}
		diagnostics := Machine{CheckpointUnavailable: s.checkpointUnavailable, FileTransferError: s.fileError}
		if p != nil && p.docker != nil {
			status := p.docker.status()
			p.docker.mu.Lock()
			diagnostics.DockerCause = safeRuntimeCause(p.docker.state.CleanupCause)
			p.docker.mu.Unlock()
			if status != "running" {
				diagnostics.DockerCleanup = status
			}
		}
		if needsCheckpoint(&a.job) {
			path := attemptCheckpoint(a.job)
			var err error
			report.Progress, err = s.nasProgress.run(path, func() (int64, error) { return checkpointProgress(path) })
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				diagnostics.CheckpointUnavailable = true
			}
		}
		a.mu.Lock()
		a.report, a.diagnostics = report, diagnostics
		a.mu.Unlock()
		if connected && !d.Hold {
			var err error
			if d.Stop && d.Kill {
				if s.state.Result != nil && !a.job.Spec.Run.Docker {
					released := *s.state.Result
					released.ReleaseOnly = !released.ReleasePending
					err = w.Client.Call(ctx, "POST", "/api/workers/complete", released, nil)
					if err == nil {
						err = &HTTPError{Status: 409}
					}
				}
			} else {
				err = w.replay(ctx, s)
			}
			if err == nil && a.job.FileTransport {
				if s.state.Result != nil {
					if !s.state.Result.ReleasePending {
						released := *s.state.Result
						released.ReleaseOnly = true
						released.OutputRefs = nil
						err = w.Client.Call(ctx, "POST", "/api/workers/complete", released, nil)
						if stale(err) {
							err = nil
						}
					}
					if err == nil {
						value, result := *s.state.Job, *s.state.Result
						finished, e := s.fileFinish.run(attemptCheckpoint(value), func() (Result, error) { return w.finishFileResult(value, result, s.dir) })
						err = e
						if e == nil {
							s.fileError = ""
							s.state.Result = &finished
							err = s.save()
						} else {
							s.fileError = "file publication pending; local results retained"
						}
					}
				} else if report.Progress > s.fileGeneration {
					value := *s.state.Job
					published, e := s.fileProgress.run(attemptCheckpoint(value), func() (int64, error) { return w.publishFileCheckpoint(value, s.dir) })
					if e == nil {
						s.fileGeneration, s.fileError = published, ""
					} else {
						s.fileError = "checkpoint transfer pending; only published progress can resume elsewhere"
					}
				}
			}
			if err == nil && s.state.Result != nil {
				err = w.Client.Call(ctx, "POST", "/api/workers/complete", s.state.Result, nil)
			}
			if s.state.Result != nil && (err == nil || stale(err)) && !s.state.Result.ReleasePending {
				if a.job.Spec.Run.Docker {
					if e := acknowledgeDocker(s, w.Machine.ID, now); e != nil {
						finish(e)
						return
					}
				}
				s.state.Job, s.state.Result = nil, nil
				if e := s.save(); e != nil {
					finish(e)
					return
				}
				finish(nil)
				return
			}
			if stale(err) && p != nil {
				p.stop(now, 0, true)
			}
		}
		select {
		case <-ctx.Done():
		case <-a.wake:
		case <-tick.C:
		}
	}
}

func (w Worker) recoverAttempt(s *workerStore) (*process, error) {
	var fs memoryFS = osMemoryFS{}
	if w.memoryCeilings != nil {
		fs = w.memoryCeilings.fs
	}
	if !s.state.Job.Spec.Run.Docker {
		if err := s.recoverResult(w.Machine.ID); err != nil {
			return nil, err
		}
		if err := s.recoverNativeCause(w.Machine.ID, fs); err != nil {
			return nil, err
		}
	}
	if err := s.cleanupOrphan(w.Clock, fs); err != nil {
		return nil, err
	}
	var p *process
	var err error
	if s.state.Job.Spec.Run.Docker && s.state.Identity != "" {
		p, err = w.restoreDocker(s)
		if err != nil {
			return nil, err
		}
	}
	if p != nil {
		s.state.Result = nil
		return p, nil
	}
	if err = s.recoverResult(w.Machine.ID); err != nil {
		return nil, err
	}
	if s.state.Result == nil {
		facts := memberOutcome{ObservationLost: true}
		if err = saveWorkerJSON(s.dir, s.state.Identity+".observation-loss.json", facts); err != nil {
			return nil, err
		}
		r := classifyAttemptOutcome(Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, Error: "worker restarted; previous process group cleaned up"}, attemptCause{memberOutcome: facts, PriorityStop: s.state.PriorityStop})
		s.state.Result = &r
	}
	if !s.state.Job.Spec.Run.Docker {
		s.state.Result.ReleasePending = false
	}
	if s.state.OperatorStopped {
		*s.state.Result = operatorOutcome(*s.state.Result, true)
	}
	s.state.PID = 0
	return nil, s.save()
}

// The root contains one session/index; attempts use independent ordinary stores.
// Persist child state before indexing. Reopen scans children too, so a crash
// between those writes cannot discard accepted custody.
func (s *workerStore) attemptStores() ([]*workerStore, error) {
	stores := []*workerStore{}
	for _, name := range s.state.Attempts {
		if filepath.Base(name) != name || name != "legacy-state.json" && (!strings.HasPrefix(name, "state-attempt-") || !strings.HasSuffix(name, ".json")) {
			return nil, errors.New("invalid saved attempt index")
		}
		if _, err := os.Stat(filepath.Join(s.dir, name)); err != nil {
			return nil, fmt.Errorf("saved attempt custody unavailable: %w", err)
		}
	}
	if s.state.Job != nil { // Adopt the bounded old single-attempt spool in place.
		child := &workerStore{dir: s.dir, state: s.state, stateName: "legacy-state.json"}
		child.state.Attempts = nil
		// The singular spool predates assignment session fields. Only this
		// bounded adoption may recover the absent field from saved custody.
		if child.state.Job.Session == "" {
			child.state.Job.Session = child.state.Session
		}
		if err := child.save(); err != nil {
			return nil, err
		}
		stores = append(stores, child)
		s.state.Job, s.state.Result = nil, nil
		s.state.Identity, s.state.Log = "", ""
		s.state.PID = 0
		if err := s.save(); err != nil {
			return nil, err
		}
	} else {
		data, err := os.ReadFile(filepath.Join(s.dir, "legacy-state.json"))
		if err == nil {
			child := &workerStore{dir: s.dir, stateName: "legacy-state.json"}
			if err = decodeWorkerState(data, &child.state); err != nil {
				return nil, err
			}
			if child.state.Job != nil {
				stores = append(stores, child)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	paths, err := filepath.Glob(filepath.Join(s.dir, "state-attempt-*.json"))
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		child := &workerStore{dir: s.dir, stateName: filepath.Base(path)}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err = decodeWorkerState(data, &child.state); err != nil {
			return nil, err
		}
		if child.state.Job != nil {
			stores = append(stores, child)
		}
	}
	seen := map[attemptKey]bool{}
	for _, child := range stores {
		key := jobKey(*child.state.Job)
		if key.session != s.state.Session || key.session != child.state.Session || seen[key] {
			return nil, errors.New("mismatched or duplicate saved attempt custody")
		}
		seen[key] = true
		child.nasProbe.timeout, child.nasPrepare.timeout = 250*time.Millisecond, 250*time.Millisecond
		if err := child.readOperatorStop(); err != nil {
			return nil, err
		}
	}
	return stores, nil
}

func newAttemptStore(root *workerStore, j Job) (*workerStore, error) {
	s := &workerStore{dir: root.dir, stateName: "state-attempt-" + ID() + ".json", state: workerState{Session: j.Session, Job: &j, CheckpointRoot: root.state.CheckpointRoot, Identity: ID()}}
	s.nasPrepare.timeout = 250 * time.Millisecond
	return s, s.save()
}

func localAttemptAdmission(w Worker, jobs []Job, next Job, models []ModelReport) error {
	cores, why := cpuCapacity(&w.Machine)
	usedCores, usedRAM := next.Spec.Needs.cores(), localJobHostNeed(w, next)
	for _, j := range jobs {
		usedCores += j.Spec.Needs.cores()
		usedRAM += localJobHostNeed(w, j)
		if !next.Spec.Needs.cpuOnly() && slices.Contains(j.AllocatedDevices, firstDevice(next, w.Machine)) {
			return fmt.Errorf("%w: device already has a batch attempt", errWorkerMemory)
		}
	}
	if w.Machine.MaxJobs > 0 && len(jobs) >= w.Machine.MaxJobs {
		return fmt.Errorf("%w: max-jobs", errWorkerMemory)
	}
	for _, r := range models {
		if r.State == "ready" || r.State == "draining" || r.State == "loading" {
			usedCores++
			for _, m := range w.ModelConfig.Models {
				if m.Name == r.Name {
					usedRAM += hostNeed(&w.Machine, m.MemoryGB, m.HostMemoryGB)
				}
			}
		}
	}
	if !next.Spec.Needs.cpuOnly() {
		id := firstDevice(next, w.Machine)
		for _, d := range w.Machine.Devices {
			if d.ID != id {
				continue
			}
			usedGPU := next.Spec.Needs.MemoryGB
			for _, r := range models {
				if r.State == "ready" || r.State == "draining" || r.State == "loading" {
					for _, m := range w.ModelConfig.Models {
						if m.Name == r.Name && (m.Device == id || m.Device == "" && id == w.Machine.Device) {
							usedGPU += m.MemoryGB
						}
					}
				}
			}
			if usedGPU > d.BudgetGB {
				return fmt.Errorf("%w: selected device budget", errWorkerMemory)
			}
		}
	}
	if why != "" || usedCores > cores || w.Machine.HostMemoryBudgetGB > 0 && usedRAM > w.Machine.HostMemoryBudgetGB {
		return fmt.Errorf("%w: aggregate CPU/RAM allowance", errWorkerMemory)
	}
	return w.checkHostRAM(next.Spec.Needs.MemoryGB, next.Spec.Needs.HostMemoryGB)
}
func localJobHostNeed(w Worker, j Job) float64 {
	m := w.Machine
	if !j.Spec.Needs.cpuOnly() {
		for _, d := range m.Devices {
			if d.ID == firstDevice(j, m) {
				m = d.machine(m)
				break
			}
		}
	}
	return hostNeed(&m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
}
func firstDevice(j Job, m Machine) string {
	if len(j.AllocatedDevices) > 0 {
		return j.AllocatedDevices[0]
	}
	if m.Device != "" {
		return m.Device
	}
	if len(m.Devices) > 0 {
		return m.Devices[0].ID
	}
	return ""
}

func (w Worker) runAttempts(stop context.Context, root *workerStore, stores []*workerStore, rt *modelRuntime, environments *environmentRuntime, rawMemory MemorySource, requestRetention func(string, time.Time)) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan WorkerEvent, 16)
	w.control = &processControl{clock: w.Clock, wake: make(chan struct{}, 1), availability: w.availability}
	controlDone := make(chan struct{})
	go func() { defer close(controlDone); w.control.run(ctx, stop, events) }()
	defer func() { cancel(); <-controlDone }()
	startEvents := func() context.CancelFunc {
		eventCtx, cancel := context.WithCancel(ctx)
		go w.watchStops(eventCtx, events)
		return cancel
	}
	eventCancel := startEvents()
	defer func() { eventCancel() }()
	actors := map[attemptKey]*attemptSupervisor{}
	recovering := len(stores) > 0
	spawn := func(s *workerStore, p *process) {
		j := *s.state.Job
		a := &attemptSupervisor{store: s, job: j, worker: w, report: AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session}, wake: make(chan struct{}, 1), done: make(chan struct{}), recovering: p}
		actors[jobKey(j)] = a
		if p != nil {
			w.control.set(p)
		}
		go a.run(ctx)
	}
	defer func() {
		cancel()
		for _, a := range actors {
			<-a.done
		}
	}()
	for _, s := range stores {
		p, err := w.recoverAttempt(s)
		if err != nil {
			return err
		}
		spawn(s, p)
	}
	tick := time.NewTicker(min(w.Interval, 100*time.Millisecond))
	defer tick.Stop()
	var retry workerRetry
	var nextRetention time.Time
	leaving, leaveSent := stop.Err() != nil, false
	stopCh := stop.Done()
	base := w.Machine.FreeMemoryGB
	fresh := root.state.Fresh && !recovering
	persistIndex := func() error {
		root.state.Attempts = nil
		for _, a := range actors {
			root.state.Attempts = append(root.state.Attempts, a.store.stateName)
		}
		slices.Sort(root.state.Attempts)
		return root.save()
	}
	if err := persistIndex(); err != nil {
		return err
	}
	for {
		now := w.Clock.Now()
		if !now.Before(nextRetention) {
			requestRetention("", now)
			nextRetention = now.Add(time.Minute)
		}
		select {
		case <-stopCh:
			leaving = true
			stopCh = nil
		default:
		}
		jobs := []Job{}
		reports := []AttemptReport{}
		batch := 0.0
		diagnostics := Machine{}
		for key, a := range actors {
			report, status, finished, err := a.snapshot()
			if finished {
				<-a.done
				if err != nil {
					return err
				}
				delete(actors, key)
				if err = persistIndex(); err != nil {
					return err
				}
				retry = workerRetry{}
				continue
			}
			reports = append(reports, report)
			diagnostics.CheckpointUnavailable = diagnostics.CheckpointUnavailable || status.CheckpointUnavailable
			if status.FileTransferError != "" {
				diagnostics.FileTransferError = status.FileTransferError
			}
			if status.DockerCleanup != "" && (diagnostics.DockerCleanup == "" || status.DockerCleanup < diagnostics.DockerCleanup) {
				diagnostics.DockerCleanup, diagnostics.DockerCause = status.DockerCleanup, status.DockerCause
			}
			jobs = append(jobs, a.job)
			batch += a.job.Spec.Needs.MemoryGB
		}
		w.batchMemory.Store(math.Float64bits(batch))
		if recovering && len(actors) == 0 {
			recovering = false
			root.state.PreviousSession, root.state.Session, root.state.Fresh = root.state.Session, ID(), true
			w.Machine.Session = root.state.Session
			if err := root.save(); err != nil {
				return err
			}
			eventCancel()
			eventCancel = startEvents()
			fresh = true
		}
		if retry.ready(now) {
			m := cloneMachine(&w.Machine)
			m.DockerCleanup, m.DockerCause = diagnostics.DockerCleanup, diagnostics.DockerCause
			m.FileTransferError = diagnostics.FileTransferError
			probeInventory := w.InventoryProbe
			observed, probeErr := root.inventory.run("devices", func() ([]MachineDevice, error) { return probeInventory(ctx) })
			if err := w.refreshInventory(observed, probeErr, root); err != nil {
				return err
			}
			m.Devices = slices.Clone(w.Machine.Devices)
			m.Leaving = leaving
			m.FreeMemoryGB = base
			m.TotalMemoryGB = base
			if rawMemory != nil {
				free, err := rawMemory(ctx)
				if err != nil || !validMemoryValue(free) {
					free = 0
					m.MemoryError = "memory probe failed"
					if err != nil {
						m.MemoryError = err.Error()
					}
				}
				m.FreeMemoryGB = free
				m.MemoryMeasured = w.MemoryMeasured
			}
			// The legacy fake runtime can inject measured telemetry. Carry it
			// into its sole inventory entry so projection cannot hide failure.
			if len(m.Devices) == 1 && m.Devices[0].Platform == "fake" {
				m.Devices[0].FreeMemoryGB = m.FreeMemoryGB
				m.Devices[0].MemoryMeasured = m.MemoryMeasured
				if m.Devices[0].Reason == "" || m.Devices[0].Reason == m.MemoryError {
					m.Devices[0].Reason = m.MemoryError
					if m.MemoryMeasured {
						m.Devices[0].BudgetGB, m.Devices[0].TotalMemoryGB = m.FreeMemoryGB, m.FreeMemoryGB
					}
				}
			}
			if len(m.Devices) == 0 && w.inventoryReason != "" {
				m.MemoryError = w.inventoryReason
			}
			if !w.MemoryMeasured {
				m.FreeMemoryGB = max(0, m.FreeMemoryGB-batch)
			}
			for i := range m.Devices {
				d := &m.Devices[i]
				d.FreeMemoryGB = min(d.FreeMemoryGB, d.BudgetGB)
				if !d.MemoryMeasured {
					for _, j := range jobs {
						if slices.Contains(j.AllocatedDevices, d.ID) {
							d.FreeMemoryGB = max(0, d.FreeMemoryGB-j.Spec.Needs.MemoryGB)
						}
					}
				}
			}
			hostFree, err := w.HostMemorySource(ctx)
			m.HostAvailableMemoryGB = hostFree
			if err != nil || !validMemoryValue(hostFree) {
				m.HostAvailableMemoryGB = 0
				m.HostMemoryError = "host RAM probe failed"
			}
			available, _ := root.nasProbe.run(root.state.CheckpointRoot, func() (bool, error) {
				err := workerCheckpointAvailable(root.state.CheckpointRoot)
				return err == nil, err
			})
			m.CheckpointUnavailable = !available || diagnostics.CheckpointUnavailable
			if m.CapabilityProfile != nil {
				profile := *m.CapabilityProfile
				a := w.availability.snapshot()
				profile.Availability = &a
				m.CapabilityProfile = &profile
			}
			if w.ExtensionRoot != "" {
				m.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, m.GPU, w.extensionFingerprint, &m)
				w.Machine.ExtensionReports = m.ExtensionReports
			}
			uploadMachine, uploadClient := m, w.Client
			_, _ = root.environmentLogs.run("environment-logs", func() (bool, error) { return true, environments.upload(ctx, uploadClient, uploadMachine) })
			ready, envReports, envTimes := environments.report()
			m.Environments = append(slices.Clone(w.Machine.Environments), ready...)
			m.EnvironmentBuilder = true
			m.EnvironmentReports, m.EnvironmentBuildSeconds = envReports, envTimes
			if !recovering {
				m.ModelURL = rt.url
			}
			m.ModelConfigHash = modelConfigHash(w.ModelConfig)
			m.Capabilities = append(slices.DeleteFunc(slices.Clone(w.Machine.Capabilities), func(c string) bool { return c == "docker" }), "logs_none")
			if w.DockerEndpoint != "" {
				m.Capabilities = append(m.Capabilities, "docker")
			}
			m.ApprovedModels, m.ModelApprovalStates = rt.cache.modelApprovals(w.ModelConfig.Models)
			rt.setLocalApprovals(m.ApprovedModels, m.ModelApprovalStates)
			modelReports, _ := rt.report()
			for _, r := range modelReports {
				if r.State == "failed" {
					m.CacheError = r.Error
				}
			}
			if rt.cache != nil {
				m.CachedModels, m.CacheFreeBytes, err = rt.cache.report(w.ModelConfig.Models, m.ApprovedModels)
				m.CopyReports, m.CopyBytesPerSecond = rt.copyTelemetry()
				if err != nil {
					m.CacheError = "cache report failed"
				}
			}
			rt.mu.Lock()
			rt.memoryMachine, rt.session = w.Machine, w.Machine.Session
			rt.mu.Unlock()
			reply, err := w.checkin(ctx, Checkin{Machine: m, Attempts: reports, Fresh: fresh, PreviousSession: root.state.PreviousSession, ModelReports: modelReports})
			if err == nil {
				if reply.CPUBudgetCores != nil {
					w.Machine.CPUBudgetCores = *reply.CPUBudgetCores
				}
				if reply.HostMemoryBudget != nil {
					w.Machine.HostMemoryBudgetGB = reply.HostMemoryBudget.GiB
				}
				fresh = false
				root.state.Fresh = false
				if reply.FileCheckpoints {
					if w.CheckpointRoot != "" {
						return errors.New("NAS checkpoint-root cannot be combined with file-store checkpoints")
					}
					w.fileCheckpoints = true
					root.state.CheckpointRoot = filepath.Join(root.dir, "file-checkpoints")
					if err = os.MkdirAll(root.state.CheckpointRoot, 0700); err != nil {
						return err
					}
				}
				if !w.fileCheckpoints && w.CheckpointRoot == "" && reply.CheckpointRoot != "" {
					root.state.CheckpointRoot = reply.CheckpointRoot
				}
				if err = root.save(); err != nil {
					return err
				}
				if reply.Leave && !recovering {
					leaving, leaveSent = true, true
				}
				for _, d := range reply.Attempts {
					key := attemptKey{d.ID, d.Attempt, d.Session}
					if d.Job != nil {
						j := *d.Job
						if !j.Spec.Needs.cpuOnly() && len(j.AllocatedDevices) == 0 {
							j.AllocatedDevices = []string{firstDevice(j, w.Machine)}
						}
						if j.FileTransport {
							j.Checkpoint = filepath.Join(root.dir, "file-checkpoints", j.ID)
						} else if !filepath.IsAbs(j.Checkpoint) {
							rel := filepath.Clean(j.Checkpoint)
							if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
								return errors.New("invalid relative checkpoint path")
							}
							j.Checkpoint = filepath.Join(root.state.CheckpointRoot, rel)
						}
						s, e := newAttemptStore(root, j)
						if e != nil {
							return e
						}
						if leaving || recovering {
							r := Result{Machine: m.ID, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Interrupted: true, Error: "worker leaving before command started"}
							s.state.Result = &r
						}
						if e = localAttemptAdmission(w, jobs, j, modelReports); e != nil {
							e = s.startFailure(j, launchRefused, e)
							r := startFailureOutcome(Result{Machine: m.ID, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Error: e.Error()}, e)
							s.state.Result = &r
						}
						if e = s.save(); e != nil {
							return e
						}
						spawn(s, nil)
						jobs = append(jobs, j)
						if e = persistIndex(); e != nil {
							return e
						}
					}
					if a := actors[key]; a != nil {
						if reply.AdmissionBlocked && !d.Stop {
							d.Hold = true
						}
						a.update(w, d, true)
					}
				}
				for key, a := range actors {
					found := false
					for _, d := range reply.Attempts {
						if key == (attemptKey{d.ID, d.Attempt, d.Session}) {
							found = true
						}
					}
					if !found {
						a.update(w, AttemptDirective{Hold: reply.AdmissionBlocked}, true)
					}
				}
				busy := len(actors) > 0 || leaving || recovering || w.checkAvailability() != nil
				for _, slot := range reply.ModelPlan {
					if slot.State == "loading" || slot.InFlight > 0 {
						busy = true
					}
				}
				for i := range reply.EnvironmentPlan {
					r := &reply.EnvironmentPlan[i]
					if r.BinaryRelative != "" && w.EnvironmentSourceRoot != "" {
						r.Binary = filepath.Join(w.EnvironmentSourceRoot, r.BinaryRelative)
					}
				}
				keep := (reply.HostMaintenance == "drained" || reply.HostMaintenance == "disabled") && reply.PreparationStopAt == nil
				prepareWorker := w
				environments.mu.Lock()
				environments.prepare = prepareWorker.prepareRegisteredExtension
				environments.mu.Unlock()
				environments.sync(reply.EnvironmentPlan, busy, keep)
				rt.syncCopies(reply.CopyPlan, reply.CacheUsage, reply.CachePins, busy, keep)
				rt.mu.Lock()
				rt.admissionBlocked = reply.AdmissionBlocked
				rt.mu.Unlock()
				rt.sync(reply.ModelPlan)
				if leaving && !leaveSent {
					err = w.Client.Call(ctx, "POST", "/api/workers/"+m.ID+"/leave", map[string]bool{"now": false}, nil)
					leaveSent = err == nil
				}
				if err == nil && leaving && len(actors) == 0 && !recovering && len(reply.ModelPlan) == 0 {
					return nil
				}
			}
			if err != nil {
				environments.sync(nil, true)
				rt.cancelCopies()
				for _, a := range actors {
					a.update(w, AttemptDirective{}, false)
				}
				var he *HTTPError
				if errors.As(err, &he) && (he.Status == 400 || he.Status == 401 || he.Status == 426 || he.Code == "machine_worker_conflict") {
					return err
				}
				log.Printf("worker %s reconnecting: %v", m.ID, err)
				retry.failed(w.Clock.Now(), w.Interval)
			} else {
				retry.succeeded(w.Clock.Now(), w.Interval)
			}
		}
		select {
		case <-stopCh:
			leaving = true
			stopCh = nil
		case <-w.control.wake:
			retry = workerRetry{}
		case <-tick.C:
		}
	}
}
