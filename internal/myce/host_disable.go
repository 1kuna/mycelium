package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var errHostDisabled = errors.New("host disabled by operator")

// All workers run by this OS user share this local directory, including per-card workers.
func HostStateDirectory() (string, error) {
	if p := os.Getenv("MYCE_HOST_STATE_DIR"); p != "" {
		return filepath.Abs(p)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".local", "share", "myce"), nil
}

type HostWorker struct {
	Session      string   `json:"session,omitempty"`
	Config       string   `json:"config,omitempty"`
	Unit         string   `json:"unit,omitempty"`
	Executions   []Result `json:"executions,omitempty"`
	Command      []string `json:"command,omitempty"`
	State        string   `json:"state_dir"`
	Environments string   `json:"environment_cache,omitempty"`
	Host         string   `json:"host_id"`
	Name         string   `json:"name"`
	Queue        string   `json:"queue"`
	PID          int      `json:"pid,omitempty"`
	Birth        string   `json:"birth,omitempty"`
}

func (w *Worker) registerHost(s *workerStore) error {
	if w.HostStateDir == "" {
		var err error
		w.HostStateDir, err = HostStateDirectory()
		if err != nil {
			return err
		}
	}
	if err := os.MkdirAll(w.HostStateDir, 0700); err != nil {
		return err
	}
	controlUnlock, err := hostOperationLock(w.HostStateDir, "intake.lock", true)
	if err != nil {
		return err
	}
	defer controlUnlock()
	if err := CheckHostRevision(w.HostStateDir, w.hostRevision); err != nil {
		return err
	}
	intent, err := ReadHostIntent(w.HostStateDir)
	if err != nil {
		return err
	}
	if w.Machine.HostID == "" {
		w.Machine.HostID = intent.Host
	}
	if w.Machine.HostID != intent.Host {
		return errors.New("worker host identity does not match local host intent")
	}
	state, err := filepath.Abs(s.dir)
	if err != nil {
		return err
	}
	env, err := filepath.Abs(w.EnvironmentRoot)
	if err != nil {
		return err
	}
	r := HostWorker{Session: s.state.Session, Config: w.ServiceConfig, Unit: w.Unit, State: state, Environments: env, Host: w.Machine.HostID, Name: w.Machine.ID, Queue: w.Client.URL, Command: w.ResumeCommand}
	if w.SuperviseProcess {
		r.PID = os.Getpid()
		r.Birth, err = processBirth(r.PID)
		if err != nil {
			return err
		}
	}
	sum := sha256.Sum256([]byte(state))
	return saveWorkerJSON(w.HostStateDir, "worker-"+hex.EncodeToString(sum[:16])+".json", r)
}

// HostProcesses is the OS boundary for fake-process/fake-clock laptop drills.
type HostProcesses struct {
	native bool
	Owned  func(workerState) (bool, error)
	Signal func(workerState, syscall.Signal) error
	Pause  func(time.Duration)
}
type HostControl struct {
	ContainerRetention *time.Duration
	DockerEndpoint     string
	dockerConfigPath   string                                                // Primary endpoint already selected by the caller.
	Observe            func(context.Context, HostIntent) HostExclusionReport // Injected full observer for laptop fixtures.
	Docker             bool
	dockerProgress     *dockerDisableProgress
	ResolveServices    func() (map[string]HostService, error)
	Services           map[string]HostService
	ResolveService     func() HostService
	Service            HostService
	Expected           HostRevision // Open only.
	FreshClose         bool
	CloseExpected      *HostRevision // Explicit close CAS; nil always accepts close.
	Settled            func(HostIntent) error
	accepted           HostRevision
	Timeout            time.Duration
	StopSecondary      func(context.Context) error
	ctx                context.Context
	Prepare            func() error // Local discovery runs only after the durable disable flag.
	Dir                string
	Clock              Clock
	Processes          HostProcesses
}
type DisableReport struct {
	cleanupComplete bool
	Exclusion       *HostExclusionReport `json:"exclusion,omitempty"`
	HostRevision
	Units       map[string]string
	Docker      string
	Confirmed   bool
	BestEffort  bool
	Stopped     int
	Unconfirmed []string
}

func (h *HostControl) defaults() error {
	if h.Dir == "" {
		var err error
		h.Dir, err = HostStateDirectory()
		if err != nil {
			return err
		}
	}
	if h.Clock == nil {
		h.Clock = RealClock{}
	}
	h.Processes.native = h.Processes.Owned == nil && h.Processes.Signal == nil
	if h.Processes.Owned == nil {
		h.Processes.Owned = ownedHostProcess
	}
	if h.Processes.Signal == nil {
		h.Processes.Signal = signalHostProcess
	}
	originalSignal := h.Processes.Signal
	h.Processes.Signal = func(st workerState, sig syscall.Signal) error {
		if h.ctx != nil && h.ctx.Err() != nil {
			return h.ctx.Err()
		}
		return h.closedAction(func() error { return originalSignal(st, sig) })
	}
	if h.Processes.Pause == nil {
		h.Processes.Pause = func(d time.Duration) { t := time.NewTimer(d); defer t.Stop(); <-t.C }
	}
	pause := h.Processes.Pause
	h.Processes.Pause = func(d time.Duration) {
		if h.ctx != nil && h.ctx.Err() != nil {
			return
		}
		pause(d)
	}
	return os.MkdirAll(h.Dir, 0700)
}
func (h HostControl) workers() ([]HostWorker, error) {
	paths, err := filepath.Glob(filepath.Join(h.Dir, "worker-*.json"))
	if err != nil {
		return nil, err
	}
	var out []HostWorker
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var r HostWorker
		if err = json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("worker registry %s: %w", p, err)
		}
		if b, e := os.ReadFile(filepath.Join(r.State, "state.json")); e == nil {
			var st workerState
			if e = json.Unmarshal(b, &st); e != nil {
				return nil, e
			}
			if r.Session == "" {
				r.Session = st.Session
			}
			if st.Job != nil {
				r.Executions = []Result{{Machine: r.Name, Session: st.Session, Execution: Execution{ID: st.Job.ID, Attempt: st.Job.Attempt}}}
			}
		}
		out = append(out, r)
	}
	return out, nil
}
func (h HostControl) Disable(ctx context.Context, reason string, grace time.Duration) (DisableReport, error) {
	timeout := h.Timeout
	if timeout == 0 {
		timeout = grace + 20*time.Second
	}
	if timeout < 0 {
		return DisableReport{}, errors.New("timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	h.ctx = ctx
	h.dockerProgress = &dockerDisableProgress{}
	if h.Docker {
		h.dockerProgress.set("unconfirmed", []string{"Docker cleanup not yet confirmed"})
	}
	type outcome struct {
		report DisableReport
		err    error
	}
	done := make(chan outcome, 1)
	go func() { r, e := h.disable(ctx, reason, grace); done <- outcome{r, e} }()
	select {
	case result := <-done:
		return result.report, result.err
	case <-ctx.Done():
		r := DisableReport{Units: h.dockerProgress.unitReport(), Unconfirmed: []string{"overall deadline: cleanup, receipt and queue acknowledgement unconfirmed"}}
		r.Docker, r.Unconfirmed = h.dockerProgress.report(r.Unconfirmed)
		return r, ctx.Err()
	}
}
func (h HostControl) disable(ctx context.Context, reason string, grace time.Duration) (r DisableReport, retErr error) {
	if grace < 0 || grace > time.Minute {
		return r, errors.New("grace must be between 0 and 1m")
	}
	if err := h.defaults(); err != nil {
		return r, err
	}
	if reason == "" {
		reason = "disabled by operator"
	}
	intent, err := changeHostIntentContext(ctx, h.Dir, h.Clock, hostIntentChange{Desired: "closed", FreshClose: h.FreshClose, CloseExpected: h.CloseExpected, Reason: reason, Grace: grace})
	if err != nil {
		return r, err
	}
	r.HostRevision = intent.HostRevision
	unlock, err := hostOperationLock(h.Dir, "reconcile.lock", false)
	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			boot, _ := HostBootID()
			r.Confirmed = localExclusionConfirmed(intent, boot)
			return r, nil
		}
		return r, err
	}
	defer unlock()
	current, e := ReadHostIntent(h.Dir)
	if e != nil {
		return r, e
	}
	if current.HostRevision != intent.HostRevision || current.Desired != "closed" {
		return r, ErrHostRevision
	}
	h.accepted = intent.HostRevision
	boot, e := HostBootID()
	if e != nil {
		return r, e
	}
	if localExclusionConfirmed(current, boot) {
		r.Confirmed = true
		return r, nil
	}
	// Persist both failed and successful exclusion attempts for this revision.
	// Re-read after Stop: a worker can save a result or a new attempt during stop.
	var initial []HostObligation
	defer func() {
		workers, e := h.workers()
		final, results, holds, custodyErr := hostCustody(workers)
		if len(results) > 0 {
			holds = append(holds, "diagnostic:undelivered_result")
		}
		boot, bootErr := HostBootID()
		evidenceErrors := append([]string(nil), r.Unconfirmed...)
		if retErr != nil {
			evidenceErrors = append(evidenceErrors, retErr.Error())
		}
		for _, failure := range []error{e, custodyErr, bootErr} {
			if failure != nil {
				evidenceErrors = append(evidenceErrors, failure.Error())
				retErr = errors.Join(retErr, failure)
			}
		}
		r.Confirmed = len(evidenceErrors) == 0
		r.cleanupComplete = r.Confirmed
		report := HostExclusionReport{HostRevision: intent.HostRevision, Boot: boot, Verdict: "UNKNOWN", At: h.Clock.Now().UTC(), Errors: []ExclusionError{{Component: "kernel", Code: "privileged_observation_required", Message: "Cleanup alone cannot prove exclusion"}}}
		if h.Observe != nil {
			report = h.Observe(ctx, intent)
		}
		if report.Verdict == "PASS" && !report.completePass() {
			report.problem("kernel", "exclusion_incomplete", false)
		}
		if report.Verdict != "PASS" || report.HostRevision != intent.HostRevision || report.Boot != boot {
			r.Confirmed = false
			retErr = errors.Join(retErr, errors.New("local_exclusion_unconfirmed"))
			evidenceErrors = append(evidenceErrors, "local_exclusion_unconfirmed")
		}
		r.Exclusion = &report
		r.Unconfirmed = evidenceErrors
		released := mergeObligations(initial, final)
		saveErr := mutateHostIntent(h.Dir, intent.HostRevision, func(s *HostIntent) error {
			for _, hold := range holds {
				if !slices.Contains(s.Holds, hold) {
					s.Holds = append(s.Holds, hold)
				}
			}
			var sessions []HostSession
			if r.Confirmed {
				for _, w := range workers {
					if w.Host == intent.Host && w.Name != "" && w.Session != "" {
						sessions = append(sessions, HostSession{Machine: w.Name, Session: w.Session})
					}
				}
			}
			s.LocalExclusion = &HostLocalExclusion{HostObservation: HostObservation{HostRevision: intent.HostRevision, Boot: boot, State: "closed", At: h.Clock.Now()}, Evidence: r.Exclusion, Confirmed: r.Confirmed, Errors: evidenceErrors, Released: released, Sessions: sessions}
			if s.LastClose == nil {
				s.LastClose = &HostClose{HostRevision: intent.HostRevision, Settlement: "pending"}
			}
			if !r.Confirmed {
				s.LastClose.Settlement = "pending"
				s.LastClose.SettledAt = nil
			}
			return nil
		})
		if saveErr == nil && r.Confirmed {
			saveErr = PublishHostObservation(h.Dir, intent.HostRevision, boot, "closed", h.Clock)
		}
		if saveErr != nil {
			r.Confirmed = false
			retErr = errors.Join(retErr, saveErr)
		}
	}()

	// A retry retains the original deadline, including after reboot.
	grace = max(0, intent.StopDeadline.Sub(h.Clock.Now()))
	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	if h.Prepare != nil {
		if err := h.Prepare(); err != nil {
			return r, err
		}
	}
	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	if h.ResolveService != nil {
		h.Service = h.ResolveService()
	}
	if h.ResolveServices != nil {
		var e error
		h.Services, e = h.ResolveServices()
		if e != nil {
			return r, e
		}
	}
	if h.Service != nil {
		h.Services = map[string]HostService{"worker": h.Service}
	}
	workers, err := h.workers()
	if err == nil {
		var custodyErr error
		initial, _, _, custodyErr = hostCustody(workers)
		if custodyErr != nil {
			r.Unconfirmed = append(r.Unconfirmed, "initial custody unreadable: "+custodyErr.Error())
		}
	}

	if err != nil {
		if len(h.Services) == 0 {
			return r, err
		}
		r.Unconfirmed = append(r.Unconfirmed, "worker registry unavailable; queue reconciliation incomplete")
		if h.Docker {
			r.Unconfirmed = append(r.Unconfirmed, "Docker pending-operation custody unconfirmed: worker registry unavailable")
		}
	}

	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	if len(h.Services) > 0 {
		for _, w := range workers {
			if !ValidWorkerUnit(w.Unit) && h.Observe == nil {
				r.Unconfirmed = append(r.Unconfirmed, "registry_worker_without_unit")
			}
		}
		r.Units = map[string]string{}
		pending := map[string]HostService{}
		for unit := range h.Services {
			r.Units[unit] = "unconfirmed"
			h.dockerProgress.unit(unit, r.Units[unit])
		}
		for unit, service := range h.Services {
			if e := h.closedAction(func() error { return service.Stop(ctx, max(0, intent.StopDeadline.Sub(h.Clock.Now()))) }); e != nil {
				r.Units[unit] = "unconfirmed: " + e.Error()
				h.dockerProgress.unit(unit, r.Units[unit])
				r.Unconfirmed = append(r.Unconfirmed, unit+": "+e.Error())
			} else {
				pending[unit] = service
			}
		}
		for len(pending) > 0 {
			for unit, service := range pending {
				empty, e := service.Empty(ctx)
				if e != nil || empty {
					delete(pending, unit)
					if e != nil {
						r.Units[unit] = "unconfirmed: " + e.Error()
						h.dockerProgress.unit(unit, r.Units[unit])
						r.Unconfirmed = append(r.Unconfirmed, unit+": "+e.Error())
					} else {
						r.Units[unit] = "confirmed empty cgroup"
						h.dockerProgress.unit(unit, r.Units[unit])
					}
				}
			}
			if len(pending) == 0 {
				break
			}
			select {
			case <-ctx.Done():
				for unit := range pending {
					r.Unconfirmed = append(r.Unconfirmed, unit+": "+ctx.Err().Error())
				}
				return r, ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
		// CLI commands are outside worker service cgroups and the worker registry.
		unlock, e := h.waitHostLaunches()
		if e != nil {
			r.Unconfirmed = append(r.Unconfirmed, "launch gate not confirmed: "+e.Error())
		} else {
			defer unlock()
		}
		records, readErr := hostProcessRecords(nil, hostCommandDirectory(h.Dir))
		if readErr != nil {
			r.Unconfirmed = append(r.Unconfirmed, readErr.Error())
		}
		r.Stopped, err = h.stopOwned(records, max(0, intent.StopDeadline.Sub(h.Clock.Now())))
		if err != nil {
			r.Unconfirmed = append(r.Unconfirmed, err.Error())
		}
		if runtime.GOOS == "darwin" && h.Processes.native && len(records) > 0 {
			r.Unconfirmed = append(r.Unconfirmed, "macOS can hide process environment markers; detached descendants unconfirmed")
		}
		r.Confirmed = len(r.Unconfirmed) == 0
	} else {
		r.BestEffort = true
		if len(workers) == 0 {
			r.Unconfirmed = append(r.Unconfirmed, "not running under the systemd service; ownership is best-effort; empty registry cannot confirm cleanup")
		}
		// Stop recorded worker lifetimes. Children have independent groups and get their own grace.
		for _, w := range workers {
			if w.PID == 0 {
				continue
			}
			st := workerState{PID: w.PID, Birth: w.Birth}
			owned, e := h.Processes.Owned(st)
			if e == nil && owned {
				e = h.Processes.Signal(st, syscall.SIGKILL)
			}
			if e != nil {
				r.Unconfirmed = append(r.Unconfirmed, "worker "+w.Name+": "+e.Error())
			}
		}
		unlock, e := h.waitHostLaunches()
		if e != nil {
			r.Unconfirmed = append(r.Unconfirmed, "launch gate not confirmed: "+e.Error())
		} else {
			defer unlock()
		}
		records, readErr := hostProcessRecords(workers, hostCommandDirectory(h.Dir))
		if readErr != nil {
			r.Unconfirmed = append(r.Unconfirmed, readErr.Error())
		}
		r.Stopped, err = h.stopOwned(records, max(0, intent.StopDeadline.Sub(h.Clock.Now())))
		if err != nil {
			r.Unconfirmed = append(r.Unconfirmed, err.Error())
		}
		if runtime.GOOS == "darwin" && h.Processes.native && len(records) > 0 {
			// macOS can hide restricted executables' environments even from ps -E.
			// Stopping visible groups cannot rule out a detached, hidden marker.
			r.Unconfirmed = append(r.Unconfirmed, "macOS can hide process environment markers; detached descendants unconfirmed")
		}
		// Docker containment extends this confirmation boundary with labeled-container cleanup.
		for _, w := range workers {
			if w.PID == 0 {
				continue
			}
			deadline := h.Clock.Now().Add(5 * time.Second)
			var live bool
			var e error
			for {
				live, e = h.Processes.Owned(workerState{PID: w.PID, Birth: w.Birth})
				if e != nil || !live || (!h.Clock.Now().Before(deadline) || h.contextDone()) {
					break
				}
				h.Processes.Pause(10 * time.Millisecond)
			}
			if e != nil || live {
				r.Unconfirmed = append(r.Unconfirmed, "worker "+w.Name+" exit not confirmed")
			}
		}
		// A legacy or in-process worker has no signalable supervisor identity.
		// Its spool lock must be released before cleanup can be certified.
		spoolDeadline := h.Clock.Now().Add(5 * time.Second)
		for _, w := range workers {
			f, e := os.OpenFile(filepath.Join(w.State, "worker.lock"), os.O_RDWR, 0)
			if errors.Is(e, os.ErrNotExist) {
				continue
			}
			if e == nil {
				for {
					e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
					if e == nil || (!h.Clock.Now().Before(spoolDeadline) || h.contextDone()) {
						break
					}
					h.Processes.Pause(10 * time.Millisecond)
				}
				if e == nil {
					_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				}
				_ = f.Close()
			}
			if e != nil {
				r.Unconfirmed = append(r.Unconfirmed, "worker "+w.Name+" spool still in use or unreadable: "+e.Error())
			}
		}
	}
	if len(h.Services) == 0 && h.StopSecondary != nil {
		if e := h.closedAction(func() error { return h.StopSecondary(ctx) }); e != nil {
			r.Unconfirmed = append(r.Unconfirmed, e.Error())
		}
	}
	// Docker cleanup shares the native cleanup deadline and must precede the receipt.
	if h.Docker {
		if e := h.closedAction(func() error { return h.confirmDocker(ctx, workers, *intent.StopDeadline) }); e != nil {
			r.Unconfirmed = append(r.Unconfirmed, e.Error())
		}
		r.Docker, _ = h.dockerProgress.report(nil)
	}

	if len(r.Unconfirmed) > 0 || ctx.Err() != nil {
		r.Confirmed = false
	}
	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	// Cleanup is local evidence only. S05 will reconcile exact custody with
	// the queue; neither an old cleared receipt nor this report opens admission.
	if current, e := ReadHostIntent(h.Dir); e != nil || current.HostRevision != intent.HostRevision {
		r.Confirmed = false
		r.Unconfirmed = append(r.Unconfirmed, "host revision changed during cleanup")
	}
	if len(r.Unconfirmed) > 0 {
		return r, fmt.Errorf("disabled; cleanup not confirmed: %s", strings.Join(r.Unconfirmed, "; "))
	}
	return r, nil
}

// Wait for durable process custody from any launch accepted before close.
func (h HostControl) waitHostLaunches() (func(), error) {
	lock, err := os.OpenFile(filepath.Join(h.Dir, "launch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	deadline := h.Clock.Now().Add(time.Second)
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); _ = lock.Close() }, nil
		}
		if !h.Clock.Now().Before(deadline) || h.contextDone() {
			_ = lock.Close()
			return nil, err
		}
		h.Processes.Pause(10 * time.Millisecond)
	}
}

func hostProcessRecords(workers []HostWorker, commandDirs ...string) ([]workerState, error) {
	var records []workerState
	var errs []error
	seen := map[string]bool{}
	add := func(path string) {
		if seen[path] {
			return
		}
		seen[path] = true
		b, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		if err != nil {
			errs = append(errs, err)
			return
		}
		var st workerState
		if err = json.Unmarshal(b, &st); err != nil {
			errs = append(errs, fmt.Errorf("process record %s: %w", path, err))
			return
		}
		if st.Identity != "" || st.PID > 0 {
			records = append(records, st)
		}
	}
	for _, dir := range commandDirs {
		paths, err := filepath.Glob(filepath.Join(dir, "attempt-command-*.json"))
		if err != nil {
			errs = append(errs, err)
		}
		for _, path := range paths {
			add(path)
		}
	}
	for _, w := range workers {
		add(filepath.Join(w.State, "state.json"))
		for _, pattern := range []string{"engine-*.json", "attempt-*.json"} {
			paths, err := filepath.Glob(filepath.Join(w.State, pattern))
			if err != nil {
				errs = append(errs, err)
			}
			for _, p := range paths {
				add(p)
			}
		}
		if w.Environments != "" {
			err := filepath.WalkDir(w.Environments, func(p string, d os.DirEntry, err error) error {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				if err != nil {
					return err
				}
				if !d.IsDir() && d.Name() == "build-state.json" {
					add(p)
				}
				return nil
			})
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return records, errors.Join(errs...)
}

func (h HostControl) stopOwned(records []workerState, grace time.Duration) (int, error) {
	live := []workerState{}
	seen := map[string]bool{}
	var errs []error
	for _, st := range records {
		if seen[st.Identity] {
			continue
		}
		seen[st.Identity] = true
		if h.Processes.native {
			groups, err := ownedRecordGroups(st)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			st.Groups = map[int]map[int]string{}
			for _, g := range groups {
				members, e := workerGroupMembers(g)
				if e != nil {
					errs = append(errs, e)
					continue
				}
				births := map[int]string{}
				for _, pid := range members {
					birth, e := processBirth(pid)
					if e == nil {
						births[pid] = birth
					} else if !errors.Is(e, os.ErrNotExist) && !errors.Is(e, syscall.ESRCH) {
						errs = append(errs, e)
					}
				}
				if len(births) > 0 {
					st.Groups[g] = births
				}
			}
		}
		owned, err := h.Processes.Owned(st)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !owned {
			continue
		}
		sig := syscall.SIGTERM
		if grace == 0 {
			sig = syscall.SIGKILL
		}
		if err = h.Processes.Signal(st, sig); err != nil {
			errs = append(errs, err)
		}
		live = append(live, st)
	}
	count := len(live)
	deadline := h.Clock.Now().Add(grace)
	killDeadline := deadline.Add(5 * time.Second)
	for len(live) > 0 {
		remaining := live[:0]
		for _, st := range live {
			owned, err := h.Processes.Owned(st)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !owned {
				continue
			}
			if !h.Clock.Now().Before(deadline) || h.contextDone() {
				if err = h.Processes.Signal(st, syscall.SIGKILL); err != nil {
					errs = append(errs, err)
				}
			}
			remaining = append(remaining, st)
		}
		live = remaining
		if len(live) == 0 {
			break
		}
		if !h.Clock.Now().Before(killDeadline) || h.contextDone() {
			errs = append(errs, fmt.Errorf("%d recorded process groups still live", len(live)))
			break
		}
		h.Processes.Pause(10 * time.Millisecond)
	}
	return count, errors.Join(errs...)
}

func (h HostControl) Enable(ctx context.Context) error {
	if err := h.defaults(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock, err := hostOperationLockContext(ctx, h.Dir, "reconcile.lock")
	if err != nil {
		return err
	}
	defer unlock()
	_, err = changeHostIntentContext(ctx, h.Dir, h.Clock, hostIntentChange{Desired: "open", Expected: h.Expected, Settled: h.Settled})
	return err
}

func AdoptHostWorker(dir string, w HostWorker) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	state, err := filepath.Abs(w.State)
	if err != nil {
		return err
	}
	w.State = state
	sum := sha256.Sum256([]byte(state))
	name := "worker-" + hex.EncodeToString(sum[:16]) + ".json"
	if _, err = os.Stat(filepath.Join(dir, name)); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return saveWorkerJSON(dir, name, w)
}
func HostResumeCommands(dir string) ([][]string, error) {
	workers, err := (HostControl{Dir: dir}).workers()
	if err != nil {
		return nil, err
	}
	var out [][]string
	for _, w := range workers {
		if len(w.Command) > 0 {
			out = append(out, w.Command)
		}
	}
	return out, nil
}

func ownedHostProcess(st workerState) (bool, error) {
	if st.Groups != nil {
		live := false
		for group, births := range st.Groups {
			members, err := workerGroupMembers(group)
			if err != nil {
				return false, err
			}
			if len(members) == 0 {
				continue
			}
			authorized := false
			for _, pid := range members {
				birth, e := processBirth(pid)
				if e == nil && births[pid] == birth {
					authorized = true
					break
				}
			}
			if !authorized {
				return false, fmt.Errorf("process group %d ownership changed; exit unconfirmed", group)
			}
			live = true
		}
		return live, nil
	}

	if st.Birth != "" {
		birth, err := processBirth(st.PID)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			return false, nil
		}
		return birth == st.Birth, err
	}
	groups, err := ownedRecordGroups(st)
	return len(groups) > 0, err
}
func signalHostProcess(st workerState, sig syscall.Signal) error {
	return signalHostProcessUsing(st, sig, signalWorkerGroup)
}
func signalHostProcessUsing(st workerState, sig syscall.Signal, signalGroup func(int, syscall.Signal) error) error {
	if st.Groups != nil {
		var errs []error
		for group, births := range st.Groups {
			owned, err := ownedHostProcess(workerState{Groups: map[int]map[int]string{group: births}})
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if owned {
				errs = append(errs, signalGroup(group, sig))
			}
		}
		return errors.Join(errs...)
	}

	if st.Birth != "" {
		owned, err := ownedHostProcess(st)
		if err != nil || !owned {
			return err
		}
		err = syscall.Kill(st.PID, sig)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	groups, err := ownedRecordGroups(st)
	if err != nil {
		return err
	}
	var errs []error
	for _, g := range groups {
		errs = append(errs, signalGroup(g, sig))
	}
	return errors.Join(errs...)
}
func ownedRecordGroups(st workerState) ([]int, error) {
	if st.PID <= 0 && !st.LaunchPending {
		return nil, nil
	}
	if st.Identity == "" {
		if st.PID > 0 {
			return nil, errors.New("process record has PID without ownership identity")
		}
		return nil, nil
	}
	// Durable intent covers a supervisor killed between Start and recording PID.
	out, err := hostPS("axeww", "-o", "pid=,pgid=,stat=,command=")
	if err != nil {
		return nil, err
	}
	groups := map[int]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 4 || strings.HasPrefix(f[2], "Z") {
			continue
		}
		_, e1 := strconv.Atoi(f[0])
		group, e2 := strconv.Atoi(f[1])
		if e1 != nil || e2 != nil {
			return nil, errors.New("invalid process inventory")
		}
		// Descendants may call setsid/setpgid; the inherited marker still owns them.
		for _, field := range f[3:] {
			if field == "myce-supervisor-"+st.Identity || field == "MYCE_PROCESS_ID="+st.Identity {
				groups[group] = true
			}
		}
	}
	var outGroups []int
	for group := range groups {
		outGroups = append(outGroups, group)
	}
	return outGroups, nil
}

func hostDisableGrace(dir string, clock Clock) time.Duration {
	s, err := ReadHostIntent(dir)
	if err != nil || s.StopDeadline == nil {
		return 0
	}
	return max(0, s.StopDeadline.Sub(clock.Now()))
}

func hostPS(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ps", args...)
	cmd.WaitDelay = 100 * time.Millisecond
	return cmd.Output()
}

func hostOperationLock(dir, name string, wait bool) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	mode := syscall.LOCK_EX
	if !wait {
		mode |= syscall.LOCK_NB
	}
	if err = syscall.Flock(int(f.Fd()), mode); err != nil {
		f.Close()
		return nil, fmt.Errorf("host control already in progress: %w", err)
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}
func (h HostControl) contextDone() bool { return h.ctx != nil && h.ctx.Err() != nil }

// S08 open uses the same exclusion lock as cleanup and Docker-hold clearance.
func HostOpenReconcileLock(dir string) (func(), error) {
	return hostOperationLock(dir, "reconcile.lock", true)
}
func (h HostControl) closedAction(fn func() error) error {
	if h.accepted.Revision == 0 {
		return fn()
	}
	unlock, err := hostOperationLock(h.Dir, "intake.lock", true)
	if err != nil {
		return err
	}
	s, err := ReadHostIntent(h.Dir)
	unlock()
	if err != nil {
		return err
	}
	if s.HostRevision != h.accepted || s.Desired != "closed" {
		return ErrHostRevision
	}
	// Open holds reconcile.lock, so discovery/stop cannot straddle an accepted open.
	// Do not hold intake.lock across systemctl, dockerd or process waits: repeated
	// close must still return the accepted revision while cleanup is in progress.
	return fn()
}
func hostOperationLockContext(ctx context.Context, dir, name string) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		unlock, err := hostOperationLock(dir, name, false)
		if err == nil {
			if err := ctx.Err(); err != nil {
				unlock()
				return nil, err
			}
			return unlock, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
