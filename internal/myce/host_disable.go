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
	f, err := os.OpenFile(filepath.Join(dir, "launch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("host launch gate busy: %w", err)
	}
	unlock := func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }
	reason, err := HostDisabled(dir)
	if err != nil || reason != "" {
		unlock()
		return nil, errors.Join(err, errHostDisabled)
	}
	return unlock, nil
}

type HostWorker struct {
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
	r := HostWorker{State: state, Environments: env, Host: w.Machine.HostID, Name: w.Machine.ID, Queue: w.Client.URL, Command: w.ResumeCommand}
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
	Dir       string
	Clock     Clock
	Processes HostProcesses
	Notify    func(context.Context, HostWorker, bool, bool, string) error
}
type DisableReport struct {
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
	if h.Processes.Pause == nil {
		h.Processes.Pause = func(d time.Duration) { t := time.NewTimer(d); defer t.Stop(); <-t.C }
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
			if st.Job != nil {
				r.Executions = []Result{{Machine: r.Name, Session: st.Session, Execution: Execution{ID: st.Job.ID, Attempt: st.Job.Attempt}}}
			}
		}
		out = append(out, r)
	}
	return out, nil
}
func (h HostControl) notify(ctx context.Context, workers []HostWorker, enable, cleared bool, reason string) []string {
	if h.Notify == nil {
		return nil
	}
	// One shared deadline, regardless of the number of workers/queues.
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	seen := map[string]bool{}
	results := make(chan error, len(workers))
	n := 0
	for _, w := range workers {
		key := w.Queue + "\x00" + w.Host
		if seen[key] {
			continue
		}
		seen[key] = true
		n++
		for _, sibling := range workers {
			if sibling.State != w.State && sibling.Queue == w.Queue && sibling.Host == w.Host {
				w.Executions = append(w.Executions, sibling.Executions...)
			}
		}
		go func(w HostWorker) { results <- h.Notify(ctx, w, enable, cleared, reason) }(w)
	}
	var out []string
	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			if err != nil {
				out = append(out, "queue notification unavailable")
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
		return c.Call(ctx, "POST", "/api/hosts/"+url.PathEscape(w.Host)+"/"+action, map[string]any{"reason": reason, "cleared": cleared, "executions": w.Executions}, nil)
	}
}

func (h HostControl) Disable(ctx context.Context, reason string, grace time.Duration) (DisableReport, error) {
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
	flag := struct {
		Who          string    `json:"who"`
		When         time.Time `json:"when"`
		Reason       string    `json:"reason"`
		GraceSeconds float64   `json:"grace_seconds"`
	}{strconv.Itoa(os.Getuid()), h.Clock.Now(), reason, grace.Seconds()}
	if err := saveWorkerJSON(h.Dir, "DISABLED", flag); err != nil {
		return r, err
	}
	if err := archiveHostFile(h.Dir, "disable-receipt.json"); err != nil {
		return r, err
	}
	workers, err := h.workers()
	if err != nil {
		return r, err
	}
	notifications := make(chan []string, 1)
	go func() { notifications <- h.notify(ctx, workers, false, false, reason) }()
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
			if e == nil || !h.Clock.Now().Before(deadline) {
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
			if e != nil || !live || !h.Clock.Now().Before(deadline) {
				break
			}
			h.Processes.Pause(10 * time.Millisecond)
		}
		if e != nil || live {
			r.Unconfirmed = append(r.Unconfirmed, "worker "+w.Name+" exit not confirmed")
		}
	}
	r.QueueErrors = <-notifications
	if len(r.Unconfirmed) == 0 {
		if err = saveWorkerJSON(h.Dir, "disable-receipt.json", struct {
			Workers []HostWorker `json:"workers"`
		}{workers}); err != nil {
			r.Unconfirmed = append(r.Unconfirmed, err.Error())
		} else {
			r.QueueErrors = append(r.QueueErrors, h.notify(ctx, workers, false, true, reason)...)
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
	var activeGroups map[int]bool
	if h.Processes.native {
		var err error
		activeGroups, err = liveHostGroups()
		if err != nil {
			return 0, err
		}
	}
	for _, st := range records {
		if seen[st.Identity] {
			continue
		}
		seen[st.Identity] = true
		if h.Processes.native && st.PID > 0 && !activeGroups[st.PID] {
			continue
		}
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
				st.Groups[g] = births
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
			if !h.Clock.Now().Before(deadline) {
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
		if !h.Clock.Now().Before(killDeadline) {
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
		if _, e = os.Stat(filepath.Join(h.Dir, "disable-receipt.json")); e != nil {
			return errors.New("cleanup unconfirmed; rerun myce disable before enable")
		}
	}
	_, err = h.workers()
	if err != nil {
		return err
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
	if err = h.ReconcileEnable(ctx); err != nil {
		log.Print("enabled locally; queue unavailable; worker will reconcile on rejoin")
	}
	return syncWorkerDir(h.Dir)
}

// Reconcile saved cleanup before clearing queue fencing, including an offline disable.
func (h HostControl) ReconcileEnable(ctx context.Context) error {
	if err := h.defaults(); err != nil {
		return err
	}
	unlock, err := hostOperationLock(h.Dir, "reconcile.lock", true)
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
	var receipt struct {
		Workers []HostWorker `json:"workers"`
	}
	if err = json.Unmarshal(b, &receipt); err != nil {
		return err
	}
	workers := receipt.Workers
	if failures := h.notify(ctx, workers, false, true, "disabled by operator"); len(failures) > 0 {
		return errors.New("disable receipt not yet acknowledged")
	}
	if failures := h.notify(ctx, workers, true, false, ""); len(failures) > 0 {
		return errors.New("enable not yet acknowledged")
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
	if st.Groups != nil {
		owned, err := ownedHostProcess(st)
		if err != nil || !owned {
			return err
		}
		var errs []error
		for group := range st.Groups {
			errs = append(errs, signalWorkerGroup(group, sig))
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
		errs = append(errs, signalWorkerGroup(g, sig))
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
	out, err := hostPS("-ax", "-o", "pid=,pgid=,stat=")
	if err != nil {
		return nil, err
	}
	groups := map[int]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) != 3 || strings.HasPrefix(f[2], "Z") {
			continue
		}
		pid, e1 := strconv.Atoi(f[0])
		group, e2 := strconv.Atoi(f[1])
		if e1 != nil || e2 != nil {
			return nil, errors.New("invalid process inventory")
		}
		if st.PID > 0 && group != st.PID {
			continue
		}
		b, e := hostPS("eww", "-p", strconv.Itoa(pid), "-o", "command=")
		if e != nil {
			var exit *exec.ExitError
			if errors.As(e, &exit) && exit.ExitCode() == 1 {
				continue
			}
			return nil, e
		}
		for _, field := range strings.Fields(string(b)) {
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
	return exec.CommandContext(ctx, "ps", args...).Output()
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

func liveHostGroups() (map[int]bool, error) {
	b, err := hostPS("-ax", "-o", "pgid=,stat=")
	if err != nil {
		return nil, err
	}
	out := map[int]bool{}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || strings.HasPrefix(fields[1], "Z") {
			continue
		}
		group, e := strconv.Atoi(fields[0])
		if e != nil {
			return nil, e
		}
		out[group] = true
	}
	return out, nil
}
