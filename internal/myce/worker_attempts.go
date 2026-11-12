package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	released    bool
	finished    bool
	stopping    atomic.Bool // Latched until exact-attempt release, including reconnects.
	admissionMu *sync.Mutex // Shared host ledger; never held for process I/O.
	err         error
	liveness    *attemptLivenessSampler
	wake        chan struct{}
	done        chan struct{}
	recovering  *process
	preparing   context.CancelFunc // Stops preparation, never abandons a started process.

	admissionBlocked bool
	// Confirmed local cleanup; independent of result delivery.
	admissionReleased atomic.Bool
}

// Observation retries preserve running status. Their exact cause must still
// reach the queue; successful recovery clears ObservationRetry and the report.
func dockerHeartbeatDiagnostic(d *dockerAttempt) (string, *RuntimeCause) {
	status := d.status()
	d.mu.Lock()
	defer d.mu.Unlock()
	var cause *RuntimeCause
	if status != "running" || d.state.ObservationRetry != "" {
		cause = safeRuntimeCause(d.state.CleanupCause)
	}
	if status == "running" {
		status = ""
	}
	return status, cause
}

func jobKey(j Job) attemptKey { return attemptKey{j.ID, j.Attempt, j.Session} }
func (a *attemptSupervisor) admissionJob() Job {
	j := a.job // Immutable execution identity; never inspect a process mutex here.
	if a.stopping.Load() {
		j.State = "stopping"
	}
	j.ReleaseConfirmed = a.admissionReleased.Load()
	return j
}
func (a *attemptSupervisor) releaseAdmission() {
	if a.admissionMu != nil {
		a.admissionMu.Lock()
		defer a.admissionMu.Unlock()
	}
	a.admissionReleased.Store(true)
}
func (a *attemptSupervisor) stopAdmission() {
	if a.admissionMu != nil {
		a.admissionMu.Lock()
		defer a.admissionMu.Unlock()
	}
	a.stopping.Store(true)
}
func (a *attemptSupervisor) snapshot() (AttemptReport, Machine, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.report
	if a.liveness != nil {
		r.Liveness = a.liveness.snapshot(a.worker.Clock.Now())
	}
	return r, a.diagnostics, a.finished, a.err
}

func (w Worker) sampleHeartbeatMemory(ctx context.Context, m *Machine, jobs []Job, reports []AttemptReport, actors map[attemptKey]*attemptSupervisor) {
	for i := range reports {
		// Inventory probes can outlast the original report while the actor
		// keeps sampling. Read its latest exact-attempt memory snapshot, with
		// no process or sensor I/O, before applying the original sample age.
		memory := (*CurrentHostMemory)(nil)
		key := attemptKey{reports[i].ID, reports[i].Attempt, reports[i].Session}
		if a := actors[key]; a != nil {
			a.mu.Lock()
			latest := a.report
			if latest.ID == key.job && latest.Attempt == key.attempt && latest.Session == key.session {
				memory = latest.CurrentHostMemory
			}
			a.mu.Unlock()
		}
		// Freeze the independently advancing device sampler before the physical
		// headroom read. Later aging must not fetch newer allocation counters.
		memory = memory.aged(w.Clock.Now())
		if memory != nil {
			memory.deviceSample = nil
		}
		reports[i].CurrentHostMemory = memory
	}
	// Sample physical headroom after the attempt counters. Crediting newer
	// resident memory against older availability would spend its growth twice.
	hostFree, rawAvailable, err := w.sampleHostMemory(ctx)
	m.HostAvailableMemoryGB = hostFree
	if err != nil || !validMemoryValue(hostFree) {
		m.HostAvailableMemoryGB = 0
		m.HostMemoryError = "host RAM probe failed"
	}
	m.MemoryObservation = w.hostMemoryObservation(ctx, *m, rawAvailable, err)
	for i := range reports {
		reports[i].Liveness = reports[i].Liveness.aged(w.Clock.Now())
		reports[i].ApplicationProgress = reports[i].ApplicationProgress.aged(w.Clock.Now())
		reports[i].CurrentHostMemory = reports[i].CurrentHostMemory.aged(w.Clock.Now())
	}
	m.MemoryObservation = creditDockerObservation(m.MemoryObservation, jobs, reports, w.Clock.Now())
}
func (a *attemptSupervisor) update(w Worker, d AttemptDirective, connected, admissionBlocked bool) {
	// Scarce-host yield is provisional until its delivery authorization. It
	// may be withdrawn while the exact process continues running normally.
	if d.Stop && (d.UserCancel || !scarceYieldStop(d.PriorityStop)) {
		a.stopAdmission()
	}
	w.stopAdmission = a.stopAdmission
	a.mu.Lock()
	a.worker, a.directive, a.connected = w, d, connected
	a.admissionBlocked = admissionBlocked
	if d.Stop && a.preparing != nil {
		a.preparing()
	}
	a.mu.Unlock()
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *attemptSupervisor) authorizeYieldStop(ctx context.Context, w Worker) (bool, error) {
	var authorization struct {
		Allowed bool `json:"allowed"`
	}
	identity := Result{Machine: w.Machine.ID, Session: a.job.Session, Execution: Execution{ID: a.job.ID, Attempt: a.job.Attempt}}
	err := w.Client.Call(ctx, "POST", "/api/workers/yield-stop", identity, &authorization)
	if err == nil && authorization.Allowed {
		// Successful authorization is durable stop custody, before persistence
		// or signal delivery can block. Reconnect never clears this latch.
		a.stopAdmission()
	}
	return err == nil && authorization.Allowed, err
}
func (a *attemptSupervisor) run(ctx context.Context) {
	defer close(a.done)
	a.mu.Lock()
	initial := a.worker
	a.mu.Unlock()
	ioCtx, cancelIO := context.WithCancel(ctx)
	defer cancelIO()
	if initial.serviceStop != nil {
		stopIO := context.AfterFunc(initial.serviceStop, cancelIO)
		defer stopIO()
	}
	defer initial.control.remove(a.job)
	s := a.store
	// A short observation retry must not cancel the operation it will consume.
	// Stop cancels this attempt context directly, including between start retries.
	prepareCtx, cancelPreparation := context.WithCancel(ctx)
	defer cancelPreparation()
	if initial.serviceStop != nil {
		stopPreparation := context.AfterFunc(initial.serviceStop, cancelPreparation)
		defer stopPreparation()
	}
	a.mu.Lock()
	a.preparing = cancelPreparation
	if a.directive.Stop {
		cancelPreparation()
	}
	a.mu.Unlock()
	p := a.recovering
	defer func() {
		a.mu.Lock()
		if a.liveness != nil {
			a.liveness.cancel()
		}
		a.mu.Unlock()
	}()
	defer func() {
		if p != nil {
			p.stop(initial.Clock.Now(), 0, true)
			if p.docker != nil {
				p.docker.waitShutdown(initial.Clock)
				p.docker.close()
			} else {
				wake := time.NewTicker(10 * time.Millisecond)
				defer wake.Stop()
				var f workerFinish
				waiting := true
				for waiting {
					p.enforceGrace(initial.Clock.Now())
					select {
					case f = <-p.done:
						waiting = false
					case <-wake.C:
					}
				}
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
		w, d, connected, admissionBlocked := a.worker, a.directive, a.connected, a.admissionBlocked
		a.mu.Unlock()
		now := w.Clock.Now()
		if p != nil {
			p.enforceGrace(now)
			p.refreshEscapers(now)
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
				f.result, f.err = applyWorkerWallTime(s.dir, a.job, f.result)
				if f.err != nil {
					finish(f.err)
					return
				}
				s.state.Result = &f.result
				if !f.result.ReleasePending {
					s.state.PID = 0
				}
				if err := s.save(); err != nil {
					finish(err)
					return
				}
			default:
			}
		}
		// A failed inventory/cleanup keeps custody, but must not freeze the
		// finished actor until a worker restart. Reuse recovery's exact process
		// identity checks; only confirmed cleanup can clear pending release.
		if p == nil && s.state.Result != nil && s.state.Result.ReleasePending && !s.extensionFiles.inFlight() && (!a.job.Spec.Run.Docker || s.state.Preparation != "" || s.inputWriterOnly()) {
			var fs memoryFS = osMemoryFS{}
			if w.memoryCeilings != nil {
				fs = w.memoryCeilings.fs
			}
			cleanupErr := s.cleanupOrphan(w.Clock, fs)
			if cleanupErr == nil {
				released := *s.state.Result
				released.ReleasePending = false
				// Recovery reloads this receipt before state. Record confirmed
				// release there before discarding the ordinary PID custody.
				if s.state.Identity != "" {
					if err := saveWorkerJSON(s.dir, s.state.Identity+".result.json", released); err != nil {
						finish(err)
						return
					}
				}
				s.state.Result = &released
				s.state.InputWriterPending = false
				s.state.PID = 0
				if err := s.save(); err != nil {
					finish(err)
					return
				}
			}
		}
		if initial.serviceStop != nil && initial.serviceStop.Err() != nil {
			// Local process cleanup and durable result custody survive queue
			// loss. An unstarted assignment must also remain replayable.
			connected = false
			if p == nil && s.state.Result == nil {
				s.state.InputWriterPending = s.extensionFiles.inFlight()
				cause := s.startFailureAt(a.job, launchRefused, context.Canceled, now)
				result := startFailureOutcome(Result{Machine: w.Machine.ID, Session: a.job.Session, Execution: Execution{ID: a.job.ID, Attempt: a.job.Attempt}, ReleasePending: s.state.InputWriterPending, Error: "worker service stopping before command started"}, cause)
				s.state.Result = &result
				if err := s.save(); err != nil {
					finish(err)
					return
				}
			}
		}
		if connected {
			if p == nil && s.state.Result == nil {
				if d.Stop {
					cause := s.startFailure(a.job, launchRefused, errors.New("assignment withdrawn before command started"))
					r := startFailureOutcome(Result{Machine: w.Machine.ID, Session: a.job.Session, Execution: Execution{ID: a.job.ID, Attempt: a.job.Attempt}, Error: cause.Error()}, cause)
					r = withStopProvenance(r, d.PriorityStop)
					s.state.Result = &r
				} else if !d.Hold && !admissionBlocked {
					bound, err := w.forAttempt(a.job)
					if err == nil {
						p, err = bound.startContext(prepareCtx, s, a.job)
					} else {
						err = s.startFailure(a.job, launchRefused, err)
					}
					a.mu.Lock()
					d = a.directive
					a.mu.Unlock()
					if p == nil && d.Stop && !errors.Is(err, errWorkerReleasePending) {
						err = s.startFailure(a.job, launchRefused, errors.New("assignment withdrawn before command started"))
					}
					if errors.Is(err, errWorkerMemory) {
						// RAM may disappear after assignment or during preparation.
						// Release unstarted custody so the queue can wait for capacity.
						err = s.startFailure(a.job, launchRefused, err)
					}
					if err != nil && !errors.Is(err, errWorkerCheckpoint) && !errors.Is(err, errWorkerUnavailable) {
						if initial.serviceStop != nil && initial.serviceStop.Err() != nil && errors.Is(err, context.Canceled) {
							// Cancellation of staging/launch belongs to installed
							// shutdown, not a payload failure. Preserve exact helper
							// and detached-writer custody until cleanup is confirmed.
							err = s.startFailureAt(a.job, launchRefused, err, now)
							if s.extensionFiles.inFlight() {
								s.state.InputWriterPending = true
								err = errors.Join(err, errWorkerReleasePending)
							}
						}
						r := startFailureOutcome(Result{Machine: w.Machine.ID, Session: a.job.Session, Execution: Execution{ID: a.job.ID, Attempt: a.job.Attempt}, Error: err.Error(), ExtensionUnavailable: errors.Is(err, errExtensionUnavailable), ReleasePending: errors.Is(err, errWorkerReleasePending)}, err)
						r = withStopProvenance(r, d.PriorityStop)
						s.state.Result = &r
					}
				}
				if err := s.save(); err != nil {
					finish(err)
					return
				}
			}
			if p != nil && d.Stop && !d.UserCancel && scarceYieldStop(d.PriorityStop) && s.state.PriorityStop == nil {
				if allowed, e := a.authorizeYieldStop(ioCtx, w); e != nil || !allowed {
					d.Stop = false
				}
			}
			if p != nil && d.Stop {
				if err := p.recordPriorityStop(s, d.PriorityStop); err != nil {
					finish(err)
					return
				}
				p.stopDirective(now, time.Duration(max(0, d.GraceSeconds)*float64(time.Second)), d.Kill, d.UserCancel)
			}
		}
		if err := enforceWorkerWallTime(s.dir, a.job, p, now); err != nil {
			finish(err)
			return
		}
		if s.state.Result != nil {
			r, err := applyWorkerWallTime(s.dir, a.job, *s.state.Result)
			if err != nil {
				finish(err)
				return
			}
			s.state.Result = &r
		}
		report := AttemptReport{Execution: Execution{ID: a.job.ID, Attempt: a.job.Attempt}, Session: a.job.Session, Started: p != nil && (p.docker == nil || p.docker.status() == "running") && (p.native == nil || p.native.started())}

		if report.Started {
			a.mu.Lock()
			if a.liveness == nil {
				a.liveness = startAttemptLiveness(ctx, w.Clock, w.Interval, p, s.dir)
			}
			a.mu.Unlock()
		}
		if p != nil && (report.Started || p.native != nil) {
			var memory *memoryAttempt
			switch {
			case p.native != nil:
				memory = p.native.memory
			case p.docker != nil:
				var fs memoryFS = osMemoryFS{}
				if w.memoryCeilings != nil {
					fs = w.memoryCeilings.fs
				}
				report.CurrentHostMemory = p.docker.currentHostMemory(w.Clock, fs)
			case p.memory != nil:
				memory = p.memory
			case s.state.Cgroup != "":
				var fs memoryFS = osMemoryFS{}
				if w.memoryCeilings != nil {
					fs = w.memoryCeilings.fs
				}
				memory = &memoryAttempt{fs: fs, path: s.state.Cgroup}
			}
			if p.docker == nil {
				report.CurrentHostMemory = memory.currentHostMemory(w.Clock)
				if memory == nil && w.reportsProcessSample(a.job) {
					report.CurrentHostMemory = p.currentProcessHostMemory()
				}
			}
		}
		if s.state.Job != nil {
			report.InputsStagedAt = s.state.Job.InputsStagedAt
			if s.state.Result != nil {
				s.state.Result.InputsStagedAt = s.state.Job.InputsStagedAt
			}
		}
		if p != nil {
			report.Resources = p.resourceSnapshot()
		}
		if s.state.Result != nil && s.state.Result.Resources != nil {
			report.Resources = s.state.Result.Resources
		}
		report.ApplicationProgress = readApplicationProgress(s.dir, a.job, w.Clock.Now())
		diagnostics := Machine{CheckpointUnavailable: s.checkpointUnavailable, FileTransferError: s.fileError}
		if diagnostics.FileTransferError == "" && s.inputTransferRecovering.Load() {
			diagnostics.FileTransferError = inputTransferRecoveringReason
		}
		if p != nil && p.docker != nil {
			diagnostics.DockerCleanup, diagnostics.DockerCause = dockerHeartbeatDiagnostic(p.docker)
			report.DockerCause = diagnostics.DockerCause
		}
		if needsCheckpoint(&a.job) {
			path := attemptCheckpoint(a.job)
			var err error
			report.Progress, err = s.nasProgress.run(path, func() (int64, error) {
				progress, err := checkpointProgress(path)
				w.observeStorageError(err)
				return progress, err
			})
			// A scan still waiting on NAS I/O is not a completed storage
			// failure. The independent root probe fences actual root stalls.
			if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errCheckpointIOPending) {
				diagnostics.CheckpointUnavailable = true
			}
		}
		a.mu.Lock()
		a.report, a.diagnostics = report, diagnostics
		a.released = p == nil && s.state.Result != nil && !s.state.Result.ReleasePending
		releasedLocally := a.released
		a.mu.Unlock()
		if releasedLocally {
			a.releaseAdmission()
		}
		if connected && !d.Hold {
			var err error
			releaseAcknowledged := false
			if d.Stop && d.Kill && !scarceYieldStop(s.state.PriorityStop) {
				if s.state.Result != nil && (!a.job.Spec.Run.Docker || s.state.Preparation != "") {
					released := *s.state.Result
					released.ReleaseOnly = !released.ReleasePending
					err = w.Client.Call(ioCtx, "POST", "/api/workers/complete", released, nil)
					if err == nil {
						releaseAcknowledged = true
						// Skip obsolete publication without losing the successful
						// release acknowledgement or sending another completion.
						err = &HTTPError{Status: 409}
					}
				}
			} else {
				err = w.replay(ioCtx, s)
			}
			if err == nil && (a.job.storedFileResults() || s.state.Result != nil && scarceYieldStop(s.state.Result.PriorityStop)) {
				w.storageObserver = s.storageObserver
				if s.state.Result != nil {
					if !s.state.Result.ReleasePending {
						released := *s.state.Result
						released.ReleaseOnly = true
						released.OutputRefs = nil
						err = w.Client.Call(ioCtx, "POST", "/api/workers/complete", released, nil)
						if stale(err) {
							err = nil
						}
					}
					if err == nil && s.fileProgress.inFlight() {
						err = errCheckpointIOPending
						s.fileError = "checkpoint publication pending; local results retained"
					}
					if err == nil {
						value, result := *s.state.Job, *s.state.Result
						metadata := []string{workerStorageMetadata(s), s.state.Identity + ".result.json"}
						logPath := ""
						if s.state.Log != "" {
							logPath = filepath.Join(s.dir, s.state.Log)
						}
						finished, e := s.fileFinish.run(attemptCheckpoint(value), func() (Result, error) {
							finished, err := w.finishFileResult(value, result, s.dir, metadata...)
							if err != nil {
								return finished, err
							}
							return w.finishFailureDiagnostics(value, finished, s.dir, logPath)
						})
						err = e
						if e == nil {
							s.fileError = ""
							s.state.Result = &finished
							err = s.save()
						} else {
							s.fileError = "file publication pending; local results retained"
							if finished.Execution == result.Execution && finished.Session == result.Session && finished.ExecutionCause != nil && finished.ExecutionCause.Finalization != nil {
								s.state.Result = &finished
								if saveErr := s.save(); saveErr != nil {
									finish(saveErr)
									return
								}
								if !finished.ReleasePending {
									released := finished
									released.ReleaseOnly, released.OutputRefs = true, nil
									// Best effort: the saved cause replays with the next release.
									_ = w.Client.Call(ioCtx, "POST", "/api/workers/complete", released, nil)
								}
							}
						}
					}
				} else if a.job.FileTransport && report.Progress > s.fileGeneration {
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
				err = w.Client.Call(ioCtx, "POST", "/api/workers/complete", s.state.Result, nil)
			}
			releaseAcknowledged = releaseAcknowledged || err == nil
			if !releaseAcknowledged && stale(err) && s.state.Result != nil && !s.state.Result.ReleasePending {
				// A stale log/publication reply says nothing about release. The
				// queue may have lost this copy while staging was still running.
				// Retain its only sender until an exact release is acknowledged.
				released := *s.state.Result
				released.ReleaseOnly, released.OutputRefs = true, nil
				releaseErr := w.Client.Call(ioCtx, "POST", "/api/workers/complete", released, nil)
				releaseAcknowledged = releaseErr == nil
				if releaseErr != nil {
					err = releaseErr
				}
			}
			if s.state.Result != nil && releaseAcknowledged && !s.state.Result.ReleasePending {
				// Release callbacks are best effort and may race result delivery.
				// Never retire the durable owner until every cache pin is removed.
				if e := s.releaseSharedInputPins(); e != nil {
					s.fileError = "cache pin cleanup pending; attempt custody retained"
					select {
					case <-ctx.Done():
					case <-a.wake:
					case <-tick.C:
					}
					continue
				}
				if err == nil {
					published, e := w.publishedAttemptFiles(ioCtx, s.state)
					if e != nil {
						s.fileError = "result confirmation pending; local files retained"
						// Keep custody and replay until the queue can confirm the result.
						select {
						case <-ctx.Done():
						case <-a.wake:
						case <-tick.C:
						}
						continue
					}
					s.state.PublishedAttempt = published
				}
				if a.job.Spec.Run.Docker {
					if e := acknowledgeDocker(s, w.Machine.ID, now); e != nil {
						finish(e)
						return
					}
				}
				preparation := s.state.Preparation
				s.state.Job, s.state.Result, s.state.Preparation = nil, nil, ""
				// This branch requires confirmed release and queue acknowledgement.
				// Failed launches may still carry intent or a previously reaped PID.
				s.state.PID, s.state.LaunchPending = 0, false
				if e := s.save(); e != nil {
					finish(e)
					return
				}
				if preparation != "" {
					if e := os.Remove(filepath.Join(s.dir, preparation)); e != nil && !errors.Is(e, os.ErrNotExist) {
						finish(e)
						return
					}
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
	if s.state.StorageDirectory != "" || parkedStorage(s.state.Job) {
		return nil, errors.New(parkedStorageReason)
	}
	if err := s.restorePriorityStop(); err != nil {
		return nil, err
	}
	var fs memoryFS = osMemoryFS{}
	if w.memoryCeilings != nil {
		fs = w.memoryCeilings.fs
	}
	if !s.state.Job.Spec.Run.Docker {
		if err := s.recoverResult(w.Machine.ID); err != nil {
			return nil, err
		}
		if err := s.recoverRSSCause(w.Machine.ID); err != nil {
			return nil, err
		}
		if err := s.recoverNativeCause(w.Machine.ID, fs); err != nil {
			return nil, err
		}
	}
	inputWriterOnly := s.inputWriterOnly()
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
	released := *s.state.Result
	if !s.state.Job.Spec.Run.Docker || inputWriterOnly {
		released.ReleasePending = false
		s.state.InputWriterPending = false
	}
	if s.state.OperatorStopped {
		released = operatorOutcome(released, true)
	}
	if !s.state.Job.Spec.Run.Docker {
		if err := saveWorkerJSON(s.dir, s.state.Identity+".result.json", released); err != nil {
			return nil, err
		}
	}
	s.state.Result = &released
	s.state.PID = 0
	return nil, s.save()
}

// The root contains one session/index; attempts use independent ordinary stores.
// Persist child state before indexing. Reopen scans children too, so a crash
// between those writes cannot discard accepted custody.
func (s *workerStore) attemptStores() ([]*workerStore, error) {
	if s.state.StorageDirectory != "" || parkedStorage(s.state.Job) {
		return nil, errors.New(parkedStorageReason)
	}
	cleared, err := clearedParkedStorageChildren(s.dir)
	if err != nil {
		return nil, err
	}
	stores := []*workerStore{}
	for _, name := range s.state.Attempts {
		if filepath.Base(name) != name || name != "legacy-state.json" && (!strings.HasPrefix(name, "state-attempt-") || !strings.HasSuffix(name, ".json")) {
			return nil, errors.New("invalid saved attempt index")
		}
		if _, err := os.Stat(filepath.Join(s.dir, name)); err != nil {
			return nil, fmt.Errorf("saved attempt custody unavailable: %w", err)
		}
	}
	if s.state.Job != nil {
		return nil, errors.New("retired single-attempt root spool; archive it and install a current worker")
	}
	// r6.8 may retain a child already adopted before the release floor. Its
	// exact process/result custody must still be recovered, never imported anew.
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
		if id := child.state.StorageDirectory; id != "" {
			session, found := cleared[id+"/"+child.stateName]
			if !found || session != child.state.Session || child.state.Job != nil || child.state.Result != nil || child.state.PID != 0 || child.state.LaunchPending {
				return nil, errors.New(parkedStorageReason)
			}
			continue // Exact child state already cleared; retain its historical index.
		}
		if parkedStorage(child.state.Job) {
			return nil, errors.New(parkedStorageReason)
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
		child.nasProbe.timeout, child.nasPrepare.timeout = heartbeatFilesystemBudget, checkpointPreparationBudget
		child.nasProgress.timeout = heartbeatFilesystemBudget
		if err := child.readOperatorStop(); err != nil {
			return nil, err
		}
	}
	return stores, nil
}

func newAttemptStore(root *workerStore, j Job) (*workerStore, error) {
	s := &workerStore{liveEvidence: unstartedExecution(j.Attempt), liveEvidenceKey: jobKey(j), inputStaging: root.inputStaging, dir: root.dir, stateName: "state-attempt-" + ID() + ".json", state: workerState{Session: j.Session, Job: &j, CheckpointRoot: root.state.CheckpointRoot, Identity: ID()}}
	// Docker identity denotes durable Docker custody, not mere assignment.
	// Refusals before startDocker must not request container recovery (#166).
	if j.Spec.Run.Docker {
		s.state.Identity = ""
	}
	s.nasPrepare.timeout = checkpointPreparationBudget
	s.nasProgress.timeout = heartbeatFilesystemBudget
	return s, s.save()
}

func localEnvironmentPreparationFits(w Worker, jobs []Job, models []ModelSlot, cpuOnly bool) bool {
	if w.Machine.SharedHost != nil {
		return false
	}
	cores, why := cpuCapacity(&w.Machine)
	used := 1 // The single environment builder, independently of CPU actors.
	for _, j := range jobs {
		if !j.Spec.Needs.cpuOnly() && !cpuOnly {
			return false
		}
		used += j.Spec.Needs.cores()
	}
	for _, s := range models {
		if s.State == "loading" || s.InFlight > 0 {
			return false
		}
		if slotReservesMemory(&s) {
			used++
		}
	}
	return why == "" && used <= cores
}

func localAttemptAdmission(w Worker, jobs []Job, next Job, models []ModelReport, preparationCores ...int) error {
	if reason := w.storageRootReason(&next); reason != "" {
		return errors.New(reason)
	}
	if w.Machine.SharedHost != nil && runtime.GOOS == "linux" && !w.sharedCPUDev && w.sharedExecutionCeilings == nil {
		return refuseAdmission("shared_cpu_controller", "controllers", 1, 0, true)
	}
	if w.Machine.SharedHost != nil && !sharedHostJobFits(&next, &w.Machine) {
		return fmt.Errorf("shared host requires a native CPU command with a prepared environment")
	}
	cores, why := cpuCapacity(&w.Machine)
	usedCores, usedRAM := next.Spec.Needs.cores(), localJobHostNeed(w, next)
	for _, n := range preparationCores {
		usedCores += n
	}
	growth := usedRAM
	for _, j := range jobs {
		usedCores += j.Spec.Needs.cores()
		need := localJobHostNeed(w, j)
		charge, pending := memoryCharge(&j, allocationMachine(&w.Machine, j.AllocatedDevices), need, w.hostMemorySamples[jobKey(j)], w.Clock.Now())
		usedRAM += charge
		growth += pending
		sharedCard := !next.Spec.Needs.cpuOnly() && !j.Spec.Needs.cpuOnly() && slices.ContainsFunc(attemptCards(next, w.Machine), func(id string) bool { return slices.Contains(j.AllocatedDevices, id) })
		if sharedCard && unifiedCUDA(allocationMachine(&w.Machine, j.AllocatedDevices)) && !sharedDeviceSampleReady(w.hostMemorySamples[jobKey(j)], w.Clock.Now()) {
			return errors.New(deviceUnifiedUncertain)
		}
		if sharedCard && fanOutDeviceSharing(&next) != fanOutDeviceSharing(&j) {
			return errors.New("XPU fan-out sharing requires every tenant to opt in")
		}
		if sharedCard && (fanOutDeviceSharing(&next) || fanOutDeviceSharing(&j)) {
			card := allocationMachine(&w.Machine, j.AllocatedDevices)
			sample, telemetry := w.hostMemorySamples[jobKey(j)], cardTelemetry(card)
			if !backendFor(card.GPU).deviceHostMemory || card.UnifiedMemory || !deviceTelemetryReady(telemetry, w.Clock.Now()) || *telemetry.ActivePercent >= 100 ||
				!fanOutDeviceSampleReady(sample, w.Clock.Now()) {
				return errors.New("fan-out sharing requires fresh discrete XPU headroom evidence")
			}
		}
		if !next.Spec.Needs.cpuOnly() && slices.ContainsFunc(attemptCards(next, w.Machine), func(id string) bool { return slices.Contains(j.AllocatedDevices, id) }) && !(sharesDevice(&next) && sharesDevice(&j)) {
			return refuseAdmission("local_device_reservation", "slots", 1, 0, false)
		}
	}
	if w.Machine.MaxJobs > 0 && len(jobs) >= w.Machine.MaxJobs {
		return refuseAdmission("local_max_jobs", "jobs", float64(len(jobs)+1), float64(w.Machine.MaxJobs), false)
	}
	for _, r := range models {
		if r.State == "ready" || r.State == "draining" || r.State == "loading" {
			usedCores++
			for _, m := range w.ModelConfig.Models {
				if m.Name == r.Name {
					need := hostNeed(allocationMachine(&w.Machine, []string{r.Device}), m.MemoryGB, m.HostMemoryGB)
					usedRAM += need
					growth += need
				}
			}
		}
	}
	// Each assigned card carries an equal share beside its resident models.
	for _, d := range w.Machine.Devices {
		id := d.ID
		if next.Spec.Needs.cpuOnly() || !slices.Contains(attemptCards(next, w.Machine), id) {
			continue
		}
		usedGPU := deviceShareGB(&next)
		fanOut, deviceGrowth := fanOutDeviceSharing(&next), deviceShareGB(&next)
		for _, j := range jobs {
			if !j.Spec.Needs.cpuOnly() && slices.Contains(j.AllocatedDevices, id) {
				card := d.machine(w.Machine)
				charge := sampledDeviceCharge(&j, &card, w.hostMemorySamples[jobKey(j)], w.Clock.Now())
				usedGPU += charge // A co-tenant sharing this card (#538).
				fanOut = fanOut || fanOutDeviceSharing(&j)
				s := w.hostMemorySamples[jobKey(j)]
				deviceGrowth += max(0, deviceShareGB(&j)-s.deviceGB) + s.deviceGrowthGB + s.deviceRateGB*w.Clock.Now().Sub(s.deviceAt).Seconds()
			}
		}
		for _, r := range models {
			if r.State == "ready" || r.State == "draining" || r.State == "loading" {
				for _, m := range w.ModelConfig.Models {
					if m.Name == r.Name && (r.Device == id || r.Device == "" && (m.Device == id || m.Device == "" && id == w.Machine.Device)) {
						usedGPU += m.MemoryGB
					}
				}
			}
		}
		if usedGPU > d.BudgetGB {
			return refuseAdmission("device_budget", "GiB", usedGPU, d.BudgetGB, false)
		}
		if fanOut && usedGPU > deviceShareGB(&next) && (d.Telemetry == nil || d.Telemetry.FreeGB == nil || *d.Telemetry.FreeGB < deviceGrowth) {
			return refuseAdmission("live_device_growth", "GiB", deviceGrowth, 0, true)
		}
	}
	if why != "" || usedCores > cores {
		return refuseAdmission("aggregate_cpu_allowance", "cores", float64(usedCores), float64(cores), why != "")
	}
	if (w.Machine.HostMemoryBudgetGB > 0 || w.Machine.HostMemoryBudgetMeasured) && usedRAM > w.Machine.HostMemoryBudgetGB {
		return refuseAdmission("aggregate_host_allowance", "GiB", usedRAM, w.Machine.HostMemoryBudgetGB, false)
	}
	if err := w.checkHostMemoryBudget(next.Spec.Needs.MemoryGB, next.Spec.Needs.HostMemoryGB); err != nil {
		return err
	}
	if next.Spec.Needs.HostMemoryGB > 0 || unifiedCUDA(&w.Machine) && next.Spec.Needs.MemoryGB > 0 {
		if w.HostMemorySource == nil {
			return refuseAdmission("live_host_growth", "GiB", growth, 0, true)
		}
		free, err := w.sharedHostLiveMemory(context.Background())
		if err != nil || !validMemoryValue(free) || free < growth {
			return refuseAdmission("live_host_growth", "GiB", growth, free, err != nil)
		}
	}
	return nil
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
func attemptCards(j Job, m Machine) []string {
	if len(j.AllocatedDevices) > 1 {
		return j.AllocatedDevices
	}
	return []string{firstDevice(j, m)}
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
	w.batchAdmission = rt.checkBatchLaunchMemory
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Interactive leave can finish work. Installed TERM must not depend on a
	// queue reply to exit once local custody is durably released.
	observeCtx, cancelObservations := context.WithCancel(ctx)
	defer cancelObservations()
	if w.serviceStop != nil {
		stopObservations := context.AfterFunc(w.serviceStop, cancelObservations)
		defer stopObservations()
	}
	// Worker.Run starts the shared health owner before model/launch capture.
	health := w.storageHealth
	var err error
	var qualification checkpointIO[bool]
	var ioProbe checkpointIO[*IOPressure]
	source := w.SharedPressureSource
	if source == nil {
		source = localSharedHostPressure
	}
	pressureProbe := &sharedPressureMonitor{clock: w.Clock, source: source, enabled: w.Machine.SharedHost != nil}
	pressureProbe.lane.timing = probeTiming{base: hostProbeControlPeriod(w.Interval), cores: w.Machine.EffectiveCPUCores}
	go pressureProbe.run(observeCtx)
	inventory := hostInventoryObserver{lane: hostProbeLane[[]MachineDevice]{clock: w.Clock, timing: probeTiming{base: hostProbeControlPeriod(w.Interval), cores: w.Machine.EffectiveCPUCores}}}
	// Telemetry shares the injectable OS/driver source used by fingerprints.
	telemetryProbe := extensionIdentityProbe()
	telemetryWorker := Worker{Clock: w.Clock, GPUTelemetryHelper: w.GPUTelemetryHelper}
	inventory.sampleTelemetry = func(ctx context.Context, devices []MachineDevice) ([]MachineDevice, error) {
		telemetryWorker.sampleDeviceTelemetry(ctx, telemetryProbe, devices)
		return devices, nil
	}
	inventoryDone := make(chan struct{})
	inventorySource := w.inventorySource()
	go func() { defer close(inventoryDone); inventory.run(observeCtx, inventorySource) }()
	defer func() { cancelObservations(); <-inventoryDone }()
	var cpuProbe checkpointIO[*memoryCeilings]
	sharedCPUAllowance := 0
	events := make(chan WorkerEvent, 16)
	w.control = &processControl{clock: w.Clock, wake: make(chan struct{}, 1), availability: w.availability, service: w.ServiceConfig != nil}
	controlDone := make(chan struct{})
	go func() { defer close(controlDone); w.control.run(ctx, stop, events) }()
	defer func() { cancel(); <-controlDone }()
	startEvents := func() context.CancelFunc {
		eventCtx, cancel := context.WithCancel(observeCtx)
		go w.watchStops(eventCtx, events)
		return cancel
	}
	eventCancel := startEvents()
	defer func() { eventCancel() }()
	actors := map[attemptKey]*attemptSupervisor{}
	activeJobs := func() []Job {
		jobs := []Job{}
		for _, actor := range actors {
			actor.mu.Lock()
			released := actor.released || actor.finished
			actor.mu.Unlock()
			if !released {
				jobs = append(jobs, actor.admissionJob())
			}
		}
		return jobs
	}
	recovering := len(stores) > 0
	spawn := func(s *workerStore, p *process) {
		j := *s.state.Job
		a := &attemptSupervisor{store: s, job: j, worker: w, report: AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session}, wake: make(chan struct{}, 1), done: make(chan struct{}), recovering: p}
		a.admissionMu, a.worker.stopAdmission = &rt.admissionMu, a.stopAdmission
		actors[jobKey(j)] = a
		// Publish reservations before the actor can reach its final launch gate.
		rt.mu.Lock()
		rt.batchJobs, rt.batchSupervisors = activeJobs(), maps.Clone(actors)
		rt.mu.Unlock()
		if p != nil {
			p.stopAdmission = a.stopAdmission
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
	var cpuQualification checkpointIO[string]
	var dockerQualification checkpointIO[bool]
	toolInventory := map[string][]string{"": executableTools(jobBaseEnvironment()["PATH"])}
	w.Machine.CPUIsolation = cpuIsolationUnavailable
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
				// Use the existing serialized collector immediately after release;
				// bulk deletion never blocks a command actor or machine heartbeat.
				requestRetention("", now)
				retry = workerRetry{}
				continue
			}

			if report.CurrentHostMemory != nil {
				sample := *report.CurrentHostMemory
				report.CurrentHostMemory = &sample
			}
			reports = append(reports, report)
			diagnostics.CheckpointUnavailable = diagnostics.CheckpointUnavailable || status.CheckpointUnavailable
			diagnostics.FileTransferError = preferTransferError(diagnostics.FileTransferError, status.FileTransferError)
			if status.DockerCleanup != "" && (diagnostics.DockerCleanup == "" || status.DockerCleanup < diagnostics.DockerCleanup) {
				diagnostics.DockerCleanup, diagnostics.DockerCause = status.DockerCleanup, status.DockerCause
			}
			a.mu.Lock()
			released := a.released
			a.mu.Unlock()
			if !released {
				jobs = append(jobs, a.admissionJob())
				batch += a.job.Spec.Needs.MemoryGB
			}
		}
		w.batchMemory.Store(math.Float64bits(batch))
		if w.serviceStop != nil && w.serviceStop.Err() != nil {
			// Actors retain unacknowledged results for startup replay. Waiting
			// for remote leave/model plans here kept an idle worker's inventory
			// alive until systemd's SIGKILL, even with no local custody.
			if len(jobs) == 0 {
				return nil
			}
			select {
			case <-tick.C:
			case <-w.control.wake:
			}
			continue
		}
		if recovering && len(actors) == 0 {
			recovering = false
			if w.sharedInputs != nil {
				w.sharedInputs.refreshCapacity()
			}
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
			ctx := observeCtx
			inventory.requestTelemetry(slices.ContainsFunc(jobs, func(j Job) bool { return !j.Spec.Needs.cpuOnly() && fanOutDeviceSharing(&j) }))
			m := cloneMachine(&w.Machine)
			m.HygieneCollection = w.hygieneCollector.snapshot()
			if w.sharedInputs != nil {
				m.CachedSharedInputs = w.sharedInputs.readyKeys()
				m.SharedInputCache = w.sharedInputs.capacityReport()
				evidence := w.sharedInputs.evidence()
				m.SharedInputEvidence = &evidence
			}
			release := CurrentRelease()
			m.Release = &release
			m.Fleet = w.fleet.observe(ctx, m, !recovering && !leaving)
			m.UnavailableRoots = w.sampleStorageRoots(ctx, root.state.CheckpointRoot)
			w.Machine.UnavailableRoots = slices.Clone(m.UnavailableRoots)
			m.DiskVolumes = w.sampleDisks(root.dir, root.state.CheckpointRoot)
			m.DockerCleanup, m.DockerCause = diagnostics.DockerCleanup, diagnostics.DockerCause
			m.FileTransferError = diagnostics.FileTransferError
			observed, probeErr := inventory.snapshot()
			if err := w.refreshInventory(ctx, observed, probeErr, root); err != nil {
				return err
			}
			if root.deviceFaults.inFlight() || !now.Before(root.faultTiming.next) {
				if err := w.refreshDeviceFaults(ctx, root); err != nil {
					return err
				}
			}
			m.Devices = slices.Clone(w.Machine.Devices)
			m.FaultMonitorError = w.Machine.FaultMonitorError
			// Slow runtime enumeration must not block heartbeats or stop delivery.
			probeWorker := w
			isolation, isolationErr := cpuQualification.run("cpu-isolation", func() (string, error) {
				return probeWorker.cpuIsolationStatus(), nil
			})
			if isolationErr == nil {
				w.Machine.CPUIsolation = isolation
			}
			m.CPUIsolation = w.Machine.CPUIsolation

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
			m.InventoryReason = w.inventoryReason
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
							d.FreeMemoryGB = max(0, d.FreeMemoryGB-deviceShareGB(&j))
						}
					}
				}
			}
			m.SharedHostPressure = nil
			if w.Machine.SharedHost != nil {
				switch runtime.GOOS {
				case "darwin":
					m.SharedCPUEnforcement = "background task policy; declared-core cap; CPU time unenforced"
				case "linux":
					m.SharedCPUEnforcement = "CPU time unenforced: delegated CPU controller unavailable"
					ceilings, cores := w.memoryCeilings, w.Machine.CPUBudgetCores
					configured, configErr := cpuProbe.run(fmt.Sprint(cores), func() (*memoryCeilings, error) { return sharedCPUCeilings(ceilings, cores) })
					if configErr == nil {
						w.sharedExecutionCeilings, sharedCPUAllowance = configured, cores
					} else if !errors.Is(configErr, errCheckpointIOPending) {
						w.sharedExecutionCeilings, sharedCPUAllowance = nil, 0
					}
					if w.sharedExecutionCeilings != nil && sharedCPUAllowance == w.Machine.CPUBudgetCores {
						m.SharedCPUEnforcement = fmt.Sprintf("cgroup v2 cpu.weight=10 cpu.max=%d/100000", sharedCPUAllowance*100000)
					}
					if w.sharedCPUDev {
						m.SharedCPUEnforcement = "CPU time unenforced: dev admission-only"
					}
				}
				w.Machine.SharedCPUEnforcement = m.SharedCPUEnforcement
			} else {
				w.sharedExecutionCeilings, sharedCPUAllowance = nil, 0
				m.SharedCPUEnforcement = ""
			}

			checkpointRoot := root.state.CheckpointRoot
			available, _ := root.nasProbe.runProgress(checkpointRoot, func(progress func()) (bool, error) {
				err := workerCheckpointAvailable(checkpointRoot, progress)
				probeWorker.observeStorageError(err)
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
				pruneUnusedExtensions(w.ExtensionRoot, w.Clock.Now(), w.ExtensionRetention)
				cpu := m
				cpu.GPU, cpu.Device = "cpu", ""
				cpu.ExtensionFingerprint = m.CPUExtensionFingerprint
				m.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "cpu", cpu.ExtensionFingerprint, &cpu)
				for i := range m.Devices {
					target := m.Devices[i].machine(m)
					m.Devices[i].ExtensionReports = LocalExtensionReports(w.ExtensionRoot, target.GPU, target.ExtensionFingerprint, &target)
				}
				for _, d := range m.Devices {
					if d.ID == m.Device {
						m.ExtensionReports = append(m.ExtensionReports, d.ExtensionReports...)
						m.ExtensionFingerprint = d.ExtensionFingerprint
					}
				}
				w.Machine.ExtensionReports = m.ExtensionReports
				w.Machine.Devices = slices.Clone(m.Devices)
			}
			uploadMachine, uploadClient := m, w.Client
			_, _ = root.environmentLogs.run("environment-logs", func() (bool, error) { return true, environments.upload(ctx, uploadClient, uploadMachine) })
			ready, envReports, envTimes, envBytes := environments.report()
			m.Environments = append(slices.Clone(w.Machine.Environments), ready...)
			m.ToolInventory = preparedToolInventory(toolInventory, m.Environments, w.EnvironmentRoot)
			m.EnvironmentBuilder = true
			m.EnvironmentReports, m.EnvironmentBuildSeconds, m.EnvironmentBytes = envReports, envTimes, envBytes
			if !recovering {
				m.ModelURL = rt.url
			}
			m.ModelConfigHash = modelConfigHash(w.ModelConfig)
			m.Capabilities = append(slices.DeleteFunc(slices.Clone(w.Machine.Capabilities), func(c string) bool { return c == "docker" || c == "docker_envelope" || c == dockerFilesCapability }), "logs_none", "shared_inputs_v1", "extension_shared_inputs_v1", "nas_inspection_v1", scarceHostYieldCapability)
			if w.ModelRoot != "" && w.modelSnapshots != nil {
				m.Capabilities = append(m.Capabilities, modelSnapshotsCapability)
			}
			m.Capabilities = slices.DeleteFunc(m.Capabilities, func(c string) bool {
				return c == "native_network" || c == "native_sidecars" || c == "native_network_partial" || strings.HasPrefix(c, "native_isolation_")
			})
			m.Capabilities = append(m.Capabilities, w.nativeIsolationCapabilities()...)
			if w.memoryCeilings != nil || w.processMemory != nil && w.processMemory.OwnedResident != nil {
				m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
			}
			if w.processMemory != nil && w.processMemory.OwnedFootprint != nil && backendFor(w.Machine.GPU).processFootprint {
				m.Capabilities = append(m.Capabilities, metalFootprintGuardCapability)
			}
			if w.DockerEndpoint != "" {
				// Refresh without allowing a slow daemon to block heartbeats.
				if supported, err := dockerQualification.run("docker-envelope", func() (bool, error) { return probeWorker.dockerEnvelopeSupported(), nil }); err == nil {
					w.dockerEnvelope = supported
				}
				m.Capabilities = append(m.Capabilities, "docker", dockerFilesCapability)
				if w.dockerEnvelope {
					m.Capabilities = append(m.Capabilities, "docker_envelope")
				}
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
			w.publishStorageReadiness(m)
			rt.mu.Lock()
			rt.memoryMachine, rt.session = w.Machine, w.Machine.Session
			rt.batchJobs = slices.Clone(jobs)
			rt.batchSupervisors = maps.Clone(actors)
			rt.mu.Unlock()

			// Refresh memory together after inventory, then age through the reads.
			w.sampleHeartbeatMemory(ctx, &m, jobs, reports, actors)
			if m.MemoryObservation != nil {
				m.MemoryObservation.Quiescent = !recovering && !leaving && len(actors) == 0 && modelReportsQuiescent(modelReports) && environments.cores() == 0 && !hostPreparing(&m)
			}
			rt.mu.Lock()
			w.hostMemorySamples = updateHostMemorySamples(reports, w.Clock.Now(), rt.batchHostMemorySamples)
			// Replaced, never mutated: model loads share this exact-attempt
			// snapshot and age it with their own launch-time clock read.
			rt.batchHostMemorySamples = w.hostMemorySamples
			rt.mu.Unlock()
			probeClock := w.Clock
			m.IOPressure, _ = ioProbe.run("host-io-pressure", func() (*IOPressure, error) {
				if workerIOPressure == nil {
					return &IOPressure{SampledAt: probeClock.Now(), Unavailable: "unsupported_os"}, nil
				}
				return workerIOPressure(probeClock), nil
			})
			if m.IOPressure != nil {
				m.IOPressure.AgeSeconds = max(0, w.Clock.Now().Sub(m.IOPressure.SampledAt).Seconds())
			}
			if m.CapabilityProfile != nil {
				a := w.availability.snapshot()
				m.CapabilityProfile.Availability = &a
			}
			// Snapshot at send time: preparation must not disappear from the
			// sensor age, and report-gap learning includes the entire control path.
			m.HostHealth = health.report(m.DiskVolumes)
			applyHostMountHealth(w, &m)
			w.Machine.UnavailableRoots = slices.Clone(m.UnavailableRoots)
			m.SharedHostPressure = pressureProbe.snapshot(w.Machine.SharedHost != nil)
			w.Machine.SharedHostPressure = m.SharedHostPressure
			heartbeatStarted := w.Clock.Now()
			reply, err := w.checkin(observeCtx, Checkin{NASPathReports: w.nasPathReports(), Machine: m, Attempts: reports, Recovering: recovering, Fresh: fresh, PreviousSession: root.state.PreviousSession, ModelReports: modelReports})
			health.noteHeartbeat(w.Clock.Now().Sub(heartbeatStarted))
			if err == nil {
				if receiptErr := saveEnrollmentReceipt(root.dir, m); receiptErr != nil {
					// Enrollment is an observer, never authority to terminate live work.
					// Retry next successful check-in; no filesystem paths or errors in logs.
					log.Printf("enrollment receipt unavailable; will retry")
				}
				w.fleet.noteReply(reply)
				w.inspectNASPaths(reply.NASPathChecks, root.state.CheckpointRoot)
				w.expireCheckpoints(reply.CheckpointExpiries, root.state.CheckpointRoot)
				if reply.SharedHost != nil {
					w.Machine.SharedHost = cloneSharedHost(reply.SharedHost)
					if reply.SharedHost.InteractiveReserveGB == 0 {
						w.Machine.SharedHost = nil
					}
				}
				if reply.CPUBudgetCores != nil {
					w.Machine.CPUBudgetCores = *reply.CPUBudgetCores
				}
				if reply.HostMemoryBudget != nil {
					w.Machine.HostMemoryBudgetGB = reply.HostMemoryBudget.GiB
					w.Machine.HostMemoryBudgetMeasured = reply.HostMemoryBudget.Measured
				}
				fresh = false
				qualificationWorker := w
				qualificationPlans := slices.Clone(reply.ExtensionQualificationPlan)
				_, _ = qualification.run("extension-qualification", func() (bool, error) {
					var errs []error
					for _, plan := range qualificationPlans {
						errs = append(errs, qualificationWorker.reconcileExtensionQualification(ctx, plan))
					}
					return true, errors.Join(errs...)
				})
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
					w.publishCheckpointRoot(reply.CheckpointRoot)
				}
				if err = root.save(); err != nil {
					return err
				}
				if reply.Leave && !recovering {
					leaving, leaveSent = true, true
				}
				// Admit against the readiness advertised in this check-in. Keep
				// cache discoveries out of the configured environment list.
				admissionWorker := w
				admissionWorker.Machine.Environments = m.Environments
				admissionWorker.Machine.UnavailableRoots = w.sampleStorageRoots(ctx, root.state.CheckpointRoot)
				admissionWorker.Machine.HostHealth = health.snapshot(m.DiskVolumes)
				applyHostMountHealth(w, &admissionWorker.Machine)
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
						if leaving {
							r := Result{Machine: m.ID, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Interrupted: true, Error: "worker leaving before command started"}
							s.state.Result = &r
						}
						if e = localAttemptAdmission(admissionWorker, activeJobs(), j, modelReports, environments.cores()); e != nil {
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
						// Maintenance fences new starts, not log/result replay.
						a.update(w, d, true, reply.AdmissionBlocked)
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
						a.update(w, AttemptDirective{}, true, reply.AdmissionBlocked)
					}
				}
				unavailable := leaving || recovering || reply.AdmissionBlocked || w.checkAvailability() != nil
				busy := len(actors) > 0 || unavailable
				cpuPreparation := len(reply.EnvironmentPlan) > 0 && reply.EnvironmentPlan[0].Platform == "cpu"
				environmentBusy := unavailable || !localEnvironmentPreparationFits(w, activeJobs(), reply.ModelPlan, cpuPreparation)
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
				environments.sync(reply.EnvironmentPlan, environmentBusy, keep)
				rt.syncCopies(reply.CopyPlan, reply.CacheUsage, reply.CachePins, busy, keep)
				// A result can finish while check-in is in flight. Refresh the
				// same exact local reservations before applying a new engine load.
				currentJobs := activeJobs()
				rt.mu.Lock()
				rt.batchJobs = currentJobs
				rt.batchSupervisors = maps.Clone(actors)
				rt.admissionBlocked = reply.AdmissionBlocked
				rt.mu.Unlock()
				rt.sync(reply.ModelPlan)
				if leaving && !leaveSent {
					err = w.Client.Call(observeCtx, "POST", "/api/workers/"+m.ID+"/leave", map[string]bool{"now": false}, nil)
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
					a.update(w, AttemptDirective{}, false, false)
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
