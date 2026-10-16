package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
func HostDisabled(dir string) (string, error) {
	if parent := unitParent(dir); parent != "" {
		if reason, err := HostDisabled(parent); err != nil || reason != "" {
			return reason, err
		}
	}
	if dir == "" {
		var err error
		dir, err = HostStateDirectory()
		if err != nil {
			return "", err
		}
	}
	b, err := os.ReadFile(filepath.Join(dir, "DISABLED"))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var flag struct {
		Reason string `json:"reason"`
	}
	if json.Unmarshal(b, &flag) != nil || flag.Reason == "" {
		return "disabled by operator (flag present)", nil
	}
	return flag.Reason, nil
}

// launchGate covers only durable process creation, never network calls or preparation.
func hostLaunchGate(dir string) (func(), error) {
	if dir == "" {
		return func() {}, nil
	}
	parentUnlock := func() {}
	if parent := unitParent(dir); parent != "" {
		var err error
		parentUnlock, err = hostLaunchGate(parent)
		if err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, "launch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		parentUnlock()
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		parentUnlock()
		return nil, fmt.Errorf("host launch gate busy: %w", err)
	}
	unlock := func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close(); parentUnlock() }
	reason, err := HostDisabled(dir)
	if err != nil || reason != "" {
		unlock()
		return nil, errors.Join(err, errHostDisabled)
	}
	return unlock, nil
}

type HostWorker struct {
	Session      string   `json:"session,omitempty"`
	Config       string   `json:"config,omitempty"`
	Unit         string   `json:"unit,omitempty"`
	Generation   uint64   `json:"generation,omitempty"`
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
	registryDir := w.HostStateDir
	if parent := unitParent(registryDir); parent != "" {
		registryDir = parent
	}
	controlUnlock, err := hostOperationLock(registryDir, "control.lock", true)
	if err != nil {
		return err
	}
	defer controlUnlock()
	unlock, err := hostLaunchGate(w.HostStateDir)
	if err != nil {
		return err
	}
	defer unlock()
	if w.Machine.HostID == "" {
		w.Machine.HostID, err = os.Hostname()
		if err != nil {
			return err
		}
	}
	state, err := filepath.Abs(s.dir)
	if err != nil {
		return err
	}
	env, err := filepath.Abs(w.EnvironmentRoot)
	if err != nil {
		return err
	}
	r := HostWorker{Config: w.ServiceConfig, Unit: w.Unit, State: state, Environments: env, Host: w.Machine.HostID, Name: w.Machine.ID, Queue: w.Client.URL, Command: w.ResumeCommand}
	if w.SuperviseProcess {
		r.PID = os.Getpid()
		r.Birth, err = processBirth(r.PID)
		if err != nil {
			return err
		}
	}
	forgotten, err := readForgottenQueues(registryDir)
	if err != nil {
		return err
	}
	if _, ok := forgotten.Queues[r.Queue]; ok {
		r.Generation, err = nextHostGeneration(registryDir)
		if err != nil {
			return err
		}
	}
	sum := sha256.Sum256([]byte(state))
	if parent := unitParent(w.HostStateDir); parent != "" {
		if err := saveWorkerJSON(parent, "worker-"+hex.EncodeToString(sum[:16])+".json", r); err != nil {
			return err
		}
	}
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
	Docker            bool
	dockerProgress    *dockerDisableProgress
	ResolveServices   func() (map[string]HostService, error)
	Services          map[string]HostService
	ResolveService    func() HostService
	Service           HostService
	Timeout           time.Duration
	StopSecondary     func(context.Context) error
	ConfirmContainers func(context.Context) error
	ctx               context.Context
	Prepare           func() error // Local discovery runs only after the durable disable flag.
	Dir               string
	Clock             Clock
	Processes         HostProcesses
	Notify            func(context.Context, HostWorker, bool, bool, string) error
}
type DisableReport struct {
	Units       map[string]string
	Docker      string
	Confirmed   bool
	BestEffort  bool
	Stopped     int
	Unconfirmed []string
	QueueErrors []string
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
		return originalSignal(st, sig)
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
	forgottenDir := h.Dir
	if parent := unitParent(forgottenDir); parent != "" {
		forgottenDir = parent
	}
	forgotten, err := readForgottenQueues(forgottenDir)
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
			r.Session = st.Session
			if st.Job != nil {
				r.Executions = []Result{{Machine: r.Name, Session: st.Session, Execution: Execution{ID: st.Job.ID, Attempt: st.Job.Attempt}}}
			}
		}
		if generation, ok := forgotten.Queues[r.Queue]; ok && r.Generation <= generation {
			r.Queue = ""
		}
		out = append(out, r)
	}
	return out, nil
}
func (h HostControl) notify(ctx context.Context, workers []HostWorker, enable, cleared bool, reason string) []string {
	if h.Notify == nil {
		return nil
	}
	forgottenDir := h.Dir
	if parent := unitParent(forgottenDir); parent != "" {
		forgottenDir = parent
	}
	forgotten, err := readForgottenQueues(forgottenDir)
	if err != nil {
		return []string{"queue registry unreadable"}
	}
	// One shared deadline, regardless of the number of workers/queues.
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	seen := map[string]bool{}
	results := make(chan error, len(workers))
	n := 0
	for _, w := range workers {
		if w.Queue == "" {
			continue
		}
		if generation, ok := forgotten.Queues[w.Queue]; ok && w.Generation <= generation {
			continue
		}
		key := w.Queue + "\x00" + w.Host
		if unitParent(h.Dir) != "" {
			key += "\x00" + w.Name + "\x00" + w.Session
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		n++
		w.Executions = append([]Result(nil), w.Executions...)
		for _, sibling := range workers {
			if sibling.State != w.State && sibling.Queue == w.Queue && sibling.Host == w.Host && (unitParent(h.Dir) == "" || sibling.Name == w.Name && sibling.Session == w.Session) {
				w.Executions = append(w.Executions, sibling.Executions...)
			}
		}
		go func(w HostWorker) {
			err := h.Notify(ctx, w, enable, cleared, reason)
			if err != nil {
				detail := "unavailable"
				var httpErr *HTTPError
				if errors.As(err, &httpErr) {
					detail = fmt.Sprintf("HTTP %d", httpErr.Status)
				}
				if errors.Is(err, context.DeadlineExceeded) {
					detail = "timed out"
				}
				endpoint := "invalid address"
				if u, e := url.Parse(w.Queue); e == nil {
					endpoint = u.Scheme + "://" + u.Host
				}
				err = fmt.Errorf("host %q queue %s: %s", w.Host, endpoint, detail)
			}
			results <- err
		}(w)
	}
	var out []string
	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			if err != nil {
				out = append(out, err.Error())
			}
		case <-ctx.Done():
			return append(out, "queue notification timed out")
		}
	}
	return out
}
func NotifyHost(client Client) func(context.Context, HostWorker, bool, bool, string) error {
	return func(ctx context.Context, w HostWorker, enable, cleared bool, reason string) error {
		c := client
		c.URL = w.Queue
		action := "disable"
		if enable {
			action = "enable"
		}
		// Confirmed physical cleanup can acknowledge only the saved executions.
		// Send before host disable requeues them and clears their current keys.
		if !enable && cleared {
			for _, r := range w.Executions {
				r.ReleaseOnly = true
				r.ReleasePending = false
				if err := c.Call(ctx, "POST", "/api/workers/complete", r, nil); err != nil {
					return err
				}
			}
		}
		return c.Call(ctx, "POST", "/api/hosts/"+url.PathEscape(w.Host)+"/"+action, map[string]any{"reason": reason, "cleared": cleared, "executions": w.Executions, "generation": w.Generation}, nil)
	}
}

// NotifyUnit uses the host API with an explicit machine scope. Cleanup and
// generation acceptance are atomic, so a stale notification cannot release custody.
func NotifyUnit(client Client) func(context.Context, HostWorker, bool, bool, string) error {
	return func(ctx context.Context, w HostWorker, enable, cleared bool, reason string) error {
		if w.Name == "" {
			return errors.New("unit notification requires machine name")
		}
		if w.Session == "" && len(w.Executions) > 0 {
			w.Session = w.Executions[0].Session
		}
		c := client
		c.URL = w.Queue
		action := "disable"
		if enable {
			action = "enable"
		}
		return c.Call(ctx, "POST", "/api/hosts/"+url.PathEscape(w.Host)+"/"+action, map[string]any{"machine": w.Name, "session": w.Session, "generation": w.Generation, "reason": reason, "cleared": cleared, "executions": w.Executions}, nil)
	}
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
func (h HostControl) disable(ctx context.Context, reason string, grace time.Duration) (DisableReport, error) {
	var r DisableReport
	if grace < 0 || grace > time.Minute {
		return r, errors.New("grace must be between 0 and 1m")
	}
	if err := h.defaults(); err != nil {
		return r, err
	}
	if reason == "" {
		reason = "disabled by operator"
	}
	unlock, opErr := hostOperationLock(h.Dir, "control.lock", false)
	if opErr != nil {
		return r, opErr
	}
	defer unlock()
	// The flag is first: neither queue reachability nor a hung worker can delay it.
	generation, err := readNextHostGeneration(h.Dir)
	generationErr := err
	flag := struct {
		Generation   uint64    `json:"generation"`
		Who          string    `json:"who"`
		When         time.Time `json:"when"`
		Reason       string    `json:"reason"`
		GraceSeconds float64   `json:"grace_seconds"`
	}{generation, strconv.Itoa(os.Getuid()), h.Clock.Now(), reason, grace.Seconds()}
	if err := saveWorkerJSON(h.Dir, "DISABLED", flag); err != nil {
		return r, err
	}
	if err := archiveHostFile(h.Dir, "disable-receipt.json"); err != nil {
		return r, err
	}
	if err := archiveHostFile(h.Dir, "enable-pending.json"); err != nil {
		return r, err
	}
	if generationErr != nil {
		return r, generationErr
	}
	if err := saveWorkerJSON(h.Dir, "control-generation.json", generation); err != nil {
		return r, err
	}
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
	if err != nil {
		if len(h.Services) == 0 {
			return r, err
		}
		r.QueueErrors = append(r.QueueErrors, "worker registry unavailable; queue reconciliation incomplete")
		if h.Docker {
			r.Unconfirmed = append(r.Unconfirmed, "Docker pending-operation custody unconfirmed: worker registry unavailable")
		}
	}

	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	for i := range workers {
		workers[i].Generation = generation
	}
	notifications := make(chan []string, 1)
	go func() { notifications <- h.notify(context.WithoutCancel(ctx), workers, false, false, reason) }()
	if len(h.Services) > 0 {
		r.Units = map[string]string{}
		pending := map[string]HostService{}
		for unit := range h.Services {
			r.Units[unit] = "unconfirmed"
			h.dockerProgress.unit(unit, r.Units[unit])
		}
		for unit, service := range h.Services {
			if e := service.Stop(ctx, grace); e != nil {
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
		lock, e := os.OpenFile(filepath.Join(h.Dir, "launch.lock"), os.O_CREATE|os.O_RDWR, 0600)
		if e == nil {
			deadline := h.Clock.Now().Add(time.Second)
			for {
				e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
				if e == nil || (!h.Clock.Now().Before(deadline) || h.contextDone()) {
					break
				}
				h.Processes.Pause(10 * time.Millisecond)
			}
		}
		if e != nil {
			if lock != nil {
				lock.Close()
			}
			r.Unconfirmed = append(r.Unconfirmed, "launch gate not confirmed: "+e.Error())
		} else {
			defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); _ = lock.Close() }()
		}
		records, readErr := hostProcessRecords(workers)
		if readErr != nil {
			r.Unconfirmed = append(r.Unconfirmed, readErr.Error())
		}
		r.Stopped, err = h.stopOwned(records, grace)
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
		if e := h.StopSecondary(ctx); e != nil {
			r.Unconfirmed = append(r.Unconfirmed, e.Error())
		}
	}
	// Docker cleanup shares the native cleanup deadline and must precede the receipt.
	if h.Docker {
		if e := h.confirmDocker(ctx, workers, flag.When.Add(grace)); e != nil {
			r.Unconfirmed = append(r.Unconfirmed, e.Error())
		}
		r.Docker, _ = h.dockerProgress.report(nil)
	}
	if h.ConfirmContainers != nil {
		if e := h.ConfirmContainers(ctx); e != nil {
			r.Unconfirmed = append(r.Unconfirmed, "Docker containers: "+e.Error())
		}
	}
	if len(r.Unconfirmed) > 0 || ctx.Err() != nil {
		r.Confirmed = false
	}
	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	select {
	case failures := <-notifications:
		r.QueueErrors = append(r.QueueErrors, failures...)
	default:
		r.QueueErrors = append(r.QueueErrors, "queue notification pending; durable receipt will reconcile")
	}
	if len(r.Unconfirmed) == 0 {
		if ctx.Err() != nil {
			return r, ctx.Err()
		}
		if err = saveWorkerJSON(h.Dir, "disable-receipt.json", hostReceipt{Workers: workers, Generation: generation}); err != nil {
			r.Unconfirmed = append(r.Unconfirmed, err.Error())
		} else {
			if ctx.Err() != nil {
				return r, ctx.Err()
			}
			// Keep offline disable below its existing 400 ms responsiveness bound.
			notifyCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			r.QueueErrors = append(r.QueueErrors, h.notify(notifyCtx, workers, false, true, reason)...)
			cancel()
		}
	}
	if len(r.Unconfirmed) > 0 {
		return r, fmt.Errorf("disabled; cleanup not confirmed: %s", strings.Join(r.Unconfirmed, "; "))
	}
	return r, nil
}

func hostProcessRecords(workers []HostWorker) ([]workerState, error) {
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
	unlock, err := hostOperationLock(h.Dir, "control.lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	if reason, e := HostDisabled(h.Dir); e != nil {
		return e
	} else if reason != "" {
		b, e := os.ReadFile(filepath.Join(h.Dir, "DISABLED"))
		if e != nil {
			return e
		}
		var flag struct {
			Generation uint64 `json:"generation"`
		}
		if e = json.Unmarshal(b, &flag); e != nil {
			return e
		}
		b, e = os.ReadFile(filepath.Join(h.Dir, "disable-receipt.json"))
		if e != nil {
			return errors.New("cleanup unconfirmed; rerun myce disable before enable")
		}
		var receipt hostReceipt
		if e = json.Unmarshal(b, &receipt); e != nil {
			return e
		}
		if flag.Generation == 0 || receipt.Generation != flag.Generation {
			return errors.New("cleanup receipt does not match current disable generation; rerun disable")
		}

	}
	_, err = h.workers()
	if err != nil {
		return err
	}
	// Persist the enable generation before removing the flag; reconciliation retries reuse it.
	if b, e := os.ReadFile(filepath.Join(h.Dir, "disable-receipt.json")); e == nil {
		var receipt hostReceipt
		if err = json.Unmarshal(b, &receipt); err != nil {
			return err
		}
		if receipt.EnableGeneration == 0 {
			receipt.EnableGeneration, err = nextHostGeneration(h.Dir)
			if err != nil {
				return err
			}
		}
		if err = saveWorkerJSON(h.Dir, "disable-receipt.json", receipt); err != nil {
			return err
		}
	}
	// Preserve an audit copy in recoverable local Trash.
	if _, e := os.Stat(filepath.Join(h.Dir, "disable-receipt.json")); e == nil {
		if e = os.Rename(filepath.Join(h.Dir, "disable-receipt.json"), filepath.Join(h.Dir, "enable-pending.json")); e != nil {
			return e
		}
	}
	for _, name := range []string{"DISABLED"} {
		path := filepath.Join(h.Dir, name)
		if _, err = os.Stat(path); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		trash := filepath.Join(h.Dir, ".Trash")
		if err = os.MkdirAll(trash, 0700); err != nil {
			return err
		}
		if err = os.Rename(path, filepath.Join(trash, ID()+"-"+name)); err != nil {
			return err
		}
	}
	if err = h.reconcileEnable(ctx, "", ""); err != nil {
		log.Printf("enabled locally; queue acknowledgements pending; workers reconcile their own queue on rejoin: %v", err)
	}
	return syncWorkerDir(h.Dir)
}

// Reconcile saved cleanup before clearing queue fencing, including an offline disable.
func (h HostControl) ReconcileEnable(ctx context.Context) error {
	if err := h.defaults(); err != nil {
		return err
	}
	unlock, err := hostOperationLock(h.Dir, "control.lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	return h.reconcileEnable(ctx, "", "")
}

// ReconcileWorkerEnable cannot let an unrelated queue veto this worker's rejoin.
func (h HostControl) ReconcileWorkerEnable(ctx context.Context, queue, host string) error {
	if queue == "" || host == "" {
		return errors.New("worker queue and host required")
	}
	if err := h.defaults(); err != nil {
		return err
	}
	unlock, err := hostOperationLock(h.Dir, "control.lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	return h.reconcileEnable(ctx, queue, host)
}

func (h HostControl) reconcileEnable(ctx context.Context, queue, host string) error {
	if reason, err := HostDisabled(h.Dir); err != nil || reason != "" {
		return errors.Join(err, errHostDisabled)
	}
	unlock, err := hostOperationLock(h.Dir, "reconcile.lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	path := filepath.Join(h.Dir, "enable-pending.json")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var receipt hostReceipt
	if err = json.Unmarshal(b, &receipt); err != nil {
		return err
	}
	if receipt.EnableGeneration == 0 {
		return errors.New("legacy receipt has no ordered enable; rerun disable and enable")
	}
	forgottenDir := h.Dir
	if parent := unitParent(forgottenDir); parent != "" {
		forgottenDir = parent
	}
	forgotten, err := readForgottenQueues(forgottenDir)
	if err != nil {
		return err
	}
	remaining := []HostWorker{}
	failures := []string{}
	seen := map[string]bool{}
	for _, target := range receipt.Workers {
		if generation, ok := forgotten.Queues[target.Queue]; ok && receipt.Generation <= generation {
			continue
		}
		if target.Queue == "" {
			continue
		}
		key := target.Queue + "\x00" + target.Host
		if seen[key] {
			continue
		}
		seen[key] = true
		group := []HostWorker{}
		for _, worker := range receipt.Workers {
			if worker.Queue == target.Queue && worker.Host == target.Host {
				group = append(group, worker)
			}
		}
		if queue != "" && (target.Queue != queue || target.Host != host) {
			remaining = append(remaining, group...)
			continue
		}
		// A lost enable response can make the old disable stale; still retry enable.
		_ = h.notify(ctx, group, false, true, "disabled by operator")
		enabled := append([]HostWorker(nil), group...)
		for i := range enabled {
			enabled[i].Generation = receipt.EnableGeneration
		}
		if errs := h.notify(ctx, enabled, true, false, ""); len(errs) > 0 {
			failures = append(failures, errs...)
			remaining = append(remaining, group...)
		}
	}
	if len(remaining) > 0 {
		receipt.Workers = remaining
		if err := saveWorkerJSON(h.Dir, "enable-pending.json", receipt); err != nil {
			return err
		}
		if len(failures) > 0 {
			return fmt.Errorf("enable not yet acknowledged: %s", strings.Join(failures, "; "))
		}
		return nil
	}
	trash := filepath.Join(h.Dir, ".Trash")
	if err = os.MkdirAll(trash, 0700); err != nil {
		return err
	}
	return os.Rename(path, filepath.Join(trash, ID()+"-enable-pending.json"))
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

func hostDisableGrace(dir string) time.Duration {
	if parent := unitParent(dir); parent != "" {
		if reason, _ := HostDisabled(parent); reason != "" {
			return hostDisableGrace(parent)
		}
	}
	b, err := os.ReadFile(filepath.Join(dir, "DISABLED"))
	if err != nil {
		return 0
	}
	var flag struct {
		GraceSeconds float64 `json:"grace_seconds"`
	}
	if json.Unmarshal(b, &flag) != nil {
		return 0
	}
	return time.Duration(max(0, min(60, flag.GraceSeconds)) * float64(time.Second))
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
func archiveHostFile(dir, name string) error {
	path := filepath.Join(dir, name)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	trash := filepath.Join(dir, ".Trash")
	if err := os.MkdirAll(trash, 0700); err != nil {
		return err
	}
	return os.Rename(path, filepath.Join(trash, ID()+"-"+name))
}

type hostReceipt struct {
	Generation       uint64       `json:"generation"`
	Workers          []HostWorker `json:"workers"`
	EnableGeneration uint64       `json:"enable_generation,omitempty"`
}

func (h HostControl) contextDone() bool { return h.ctx != nil && h.ctx.Err() != nil }

// Serialized by control.lock; retained across enable and reboot.
func nextHostGeneration(dir string) (uint64, error) {
	n, e := readNextHostGeneration(dir)
	if e != nil {
		return 0, e
	}
	return n, saveWorkerJSON(dir, "control-generation.json", n)
}
func readNextHostGeneration(dir string) (uint64, error) {
	var last uint64
	b, e := os.ReadFile(filepath.Join(dir, "control-generation.json"))
	if e == nil {
		if e = json.Unmarshal(b, &last); e != nil {
			return 0, e
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return 0, e
	}
	// The flag covers a crash after flag fsync but before counter fsync.
	if b, e := os.ReadFile(filepath.Join(dir, "DISABLED")); e == nil {
		var flag struct {
			Generation uint64 `json:"generation"`
		}
		if e = json.Unmarshal(b, &flag); e != nil {
			return 0, e
		}
		last = max(last, flag.Generation)
	} else if !errors.Is(e, os.ErrNotExist) {
		return 0, e
	}
	if last == ^uint64(0) {
		return 0, errors.New("host generation exhausted")
	}
	last++
	return last, nil
}
