package myce

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// workerRetry makes reconnect decisions independent of timer delivery. Timers
// merely wake Run; all deadlines, including process grace, use the Clock.
type workerRetry struct {
	next  time.Time
	delay time.Duration
}

func (r *workerRetry) ready(now time.Time) bool { return !now.Before(r.next) }
func (r *workerRetry) failed(now time.Time, base time.Duration) {
	if r.delay == 0 {
		r.delay = base
	} else {
		r.delay = min(r.delay*2, 30*time.Second)
	}
	r.next = now.Add(r.delay)
}
func (r *workerRetry) succeeded(now time.Time, interval time.Duration) {
	r.delay = 0
	r.next = now.Add(interval)
}

type workerState struct {
	Preparation      string     `json:"preparation_custody,omitempty"` // Attempt-local copy of an unconfirmed builder receipt.
	CustodyID        string     `json:"custody_id,omitempty"`
	IsolationWarning *Execution `json:"isolation_warning,omitempty"`
	// Written with cleared custody only after the queue confirms terminal,
	// released publication. Survives a restart before scratch retention runs.
	PublishedAttempt *Execution `json:"published_attempt,omitempty"`

	LaunchOutcome   string                 `json:"launch_outcome,omitempty"`
	Attempts        []string               `json:"attempts,omitempty"`
	PriorityStop    *PriorityPreemption    `json:"priority_stop,omitempty"`
	Accelerator     bool                   `json:"accelerator"`
	StopGrace       time.Duration          `json:"stop_grace,omitempty"`
	UserCancel      bool                   `json:"user_cancel,omitempty"`
	StopAt          *time.Time             `json:"stop_at,omitempty"`
	SidecarStopAt   *time.Time             `json:"sidecar_stop_at,omitempty"`
	Schema          int                    `json:"schema_version"`
	OperatorStopped bool                   `json:"operator_stopped,omitempty"`
	Groups          map[int]map[int]string `json:"groups,omitempty"`
	Cgroup          string                 `json:"cgroup,omitempty"`
	LaunchPending   bool                   `json:"launch_pending,omitempty"`
	Birth           string                 `json:"birth,omitempty"`
	Session         string                 `json:"session"`
	PreviousSession string                 `json:"previous_session,omitempty"`
	Fresh           bool                   `json:"fresh"`
	Job             *Job                   `json:"job,omitempty"`
	Result          *Result                `json:"result,omitempty"`
	PID             int                    `json:"pid,omitempty"`
	Identity        string                 `json:"identity,omitempty"`
	Offset          int64                  `json:"offset"`
	Pending         int                    `json:"pending,omitempty"`
	CheckpointRoot  string                 `json:"checkpoint_root,omitempty"`
	Log             string                 `json:"log,omitempty"`
}
type workerStore struct {
	inputStaging          *atomic.Bool // Shared by all attempt stores; bounds detached ordinary staging.
	sharedRelease         func()       // Idempotent; retained until exact process cleanup.
	deviceFaults          checkpointIO[KernelLogSnapshot]
	stateName             string
	environmentLogs       checkpointIO[bool]
	inventory             checkpointIO[[]MachineDevice]
	fileError             string
	fileProgress          checkpointIO[int64]
	fileFinish            checkpointIO[Result]
	fileGeneration        int64
	extensionFiles        checkpointIO[stagedInputs]
	checkpointUnavailable bool
	nasProbe              checkpointAvailability
	nasProgress           checkpointIO[int64]
	nasPrepare            checkpointIO[string]
	dir                   string
	lock                  *os.File
	state                 workerState
}

// StateRoots resolves the worker spool and all effective local cache roots without writing.
func (w Worker) StateRoots() ([]string, error) {
	dir := w.StateDir
	if dir == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256([]byte(w.Client.URL + "\x00" + w.Machine.ID))
		dir = filepath.Join(cache, "mycelium", "workers", hex.EncodeToString(sum[:16]))
	}
	// Jobs change cwd to their private input folder. All paths derived from the
	// worker-owned spool (usage reports, checkpoints and staging) must retain
	// their meaning there, including when --state-dir is relative.
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	environment := w.EnvironmentRoot
	if environment == "" {
		environment = filepath.Join(dir, "environment-cache")
	}
	modelCache := w.CacheRoot
	if modelCache == "" {
		modelCache = filepath.Join(dir, "model-cache")
	}
	fileCache := w.FileCacheRoot
	if fileCache == "" {
		fileCache = filepath.Join(dir, "file-cache")
	}
	return []string{dir, environment, modelCache, fileCache}, nil
}

func (w Worker) openStore() (*workerStore, error) {
	roots, err := w.StateRoots()
	if err != nil {
		return nil, err
	}
	dir := roots[0]
	if err := PrepareStateRoots(roots...); err != nil {
		return nil, fmt.Errorf("read worker state: %w", err)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("worker state already in use: %w", err)
	}
	s := &workerStore{dir: dir, lock: lock, inputStaging: new(atomic.Bool)}
	s.nasProbe.timeout = heartbeatFilesystemBudget
	s.nasProgress.timeout = heartbeatFilesystemBudget
	s.nasPrepare.timeout = checkpointPreparationBudget
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err == nil {
		err = decodeWorkerState(data, &s.state)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.close()
		return nil, fmt.Errorf("read worker state: %w", err)
	}
	if err = s.readOperatorStop(); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

// The binary-free marker is an atomic snapshot of state.json taken before unit stop.
func (s *workerStore) readOperatorStop() error {
	data, err := os.ReadFile(filepath.Join(s.dir, "operator-stop.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var marker workerState
	if err = decodeWorkerState(data, &marker); err != nil {
		return fmt.Errorf("read operator-stop marker: %w", err)
	}
	st := &s.state
	if s.stateName != "" && marker.Session == st.Session && slices.Contains(marker.Attempts, s.stateName) && st.Job != nil && st.Result == nil {
		st.OperatorStopped = true
		return s.save()
	}
	if marker.Job != nil && marker.Result == nil && st.Job != nil && marker.Identity != "" && marker.Identity == st.Identity && marker.Session == st.Session && marker.Job.ID == st.Job.ID && marker.Job.Attempt == st.Job.Attempt {
		st.OperatorStopped = true
		return s.save()
	}
	return nil
}

// Existing spools always include these fields, even when their values are zero.
func decodeWorkerState(data []byte, state *workerState) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(state); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"session", "fresh", "offset"} {
		if value, ok := fields[name]; !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("not Mycelium worker state: missing %s", name)
		}
	}
	return nil
}

// Results from a different execution must never be adopted from a spool file.
func (s *workerStore) matchesResult(r Result, machine string) bool {
	return s.state.Job != nil && r.ID == s.state.Job.ID && r.Attempt == s.state.Job.Attempt && r.Machine == machine && r.Session == s.state.Session
}

func (s *workerStore) recoverResult(machine string) error {
	if s.state.Result != nil && !s.matchesResult(*s.state.Result, machine) {
		return errors.New("mismatched saved worker result")
	}
	var r Result
	data, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".result.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = decodeWorkerJSON(data, &r); err != nil {
		return err
	}
	if !s.matchesResult(r, machine) {
		return errors.New("mismatched recovered worker result")
	}
	s.state.Result = &r
	return nil
}

// Event delivery must persist intent without racing the attempt actor's state.
// The execution identity gives this receipt the same custody as its outcome.
func (s *workerStore) priorityStopRecorder() func(*PriorityPreemption) error {
	dir, identity := s.dir, s.state.Identity
	return func(stop *PriorityPreemption) error {
		return saveWorkerJSON(dir, identity+".priority-stop.json", stop)
	}
}

func (s *workerStore) restorePriorityStop() error {
	data, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".priority-stop.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var stop PriorityPreemption
	if err := decodeWorkerJSON(data, &stop); err != nil {
		return err
	}
	s.state.PriorityStop = &stop
	return nil
}

func (s *workerStore) close() {
	_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	_ = s.lock.Close()
}
func (s *workerStore) save() error {
	name := s.stateName
	if name == "" {
		name = "state.json"
	}
	return saveWorkerJSON(s.dir, name, s.state)
}
func decodeWorkerJSON(data []byte, v any) error { return json.Unmarshal(data, v) }
func saveWorkerJSON(dir, name string, v any) error {
	switch value := v.(type) {
	case workerState:
		value.Schema = persistedSchema
		v = value
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, name+".tmp"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(filepath.Join(dir, name+".tmp"), filepath.Join(dir, name)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Inspect both the leader and surviving descendants. The per-execution
// marker prevents PID/PGID reuse from targeting an unrelated process group.
func workerGroupMembers(group int) ([]int, error) {
	out, err := processPS("-ax", "-o", "pid=,pgid=,stat=")
	if err != nil {
		return nil, fmt.Errorf("inspect process groups: %w", err)
	}
	var members []int
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		pid, e1 := strconv.Atoi(fields[0])
		pgid, e2 := strconv.Atoi(fields[1])
		if e1 != nil || e2 != nil {
			return nil, errors.New("invalid process inventory")
		}
		if pgid == group && !strings.HasPrefix(fields[2], "Z") {
			members = append(members, pid)
		}
	}
	return members, nil
}

// macOS can return EPERM for a group whose last process is exiting. Treat
// that as disappearance only after a successful inventory proves no live
// members remain; permission errors for surviving processes stay fail-closed.
func signalWorkerGroup(group int, signal syscall.Signal) error {
	return signalWorkerGroupUsing(group, signal, syscall.Kill, workerGroupMembers)
}
func signalWorkerGroupUsing(group int, signal syscall.Signal, kill func(int, syscall.Signal) error, inventory func(int) ([]int, error)) error {
	err := kill(-group, signal)
	if err == nil || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if errors.Is(err, syscall.EPERM) {
		members, inspectErr := inventory(group)
		if inspectErr != nil {
			return errors.Join(err, inspectErr)
		}
		if len(members) == 0 {
			return nil
		}
	}
	return fmt.Errorf("signal process group %d: %w", group, err)
}

func waitWorkerGroup(group int, clock Clock) error {
	deadline := clock.Now().Add(5 * time.Second)
	wake := time.NewTicker(5 * time.Millisecond)
	defer wake.Stop()
	for {
		members, err := workerGroupMembers(group)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return nil
		}
		if !clock.Now().Before(deadline) {
			return errors.New("job process group survived SIGKILL")
		}
		<-wake.C
	}
}
func (s *workerStore) cleanupOrphan(clock Clock, memoryFiles ...memoryFS) (cleanupErr error) {
	defer func() {
		if cleanupErr == nil {
			s.releaseSharedInputs()
		}
	}()
	if s.state.Preparation != "" {
		return s.cleanupPreparation(clock)
	}
	var stopIntent *workerState
	if s.state.Job != nil && s.state.Identity != "" {
		data, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".stop.json"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			var intent workerState
			if err := decodeWorkerJSON(data, &intent); err != nil {
				return err
			}
			if intent.Identity != s.state.Identity {
				return errors.New("mismatched stop custody")
			}
			stopIntent = &intent
			s.state.StopAt, s.state.UserCancel = intent.StopAt, intent.UserCancel
		}
	}
	// Native members use the ordinary attempt record, written before each exec
	// gate opens. argv/environment markers may disappear (including env -i),
	// so authenticate surviving PIDs by their kernel birth identity instead.
	if s.state.Job != nil && s.state.Identity != "" {
		data, err := os.ReadFile(filepath.Join(s.dir, "attempt-"+s.state.Identity+".json"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			var st workerState
			if err := decodeWorkerJSON(data, &st); err != nil {
				return err
			}
			if st.Identity != s.state.Identity || s.state.PID != 0 && st.PID != s.state.PID {
				return errors.New("mismatched attempt process custody")
			}
			if st.Groups != nil || st.Cgroup != "" {
				if st.StopGrace == 0 {
					st.StopGrace = s.state.StopGrace
				}
				// Ordinary commands record stop delivery separately; read it before
				// group recovery so a restart cannot renew grace or forget cancel.
				if stopIntent != nil {
					st.StopAt, st.UserCancel = stopIntent.StopAt, stopIntent.UserCancel
				}
				if !s.state.Job.Spec.Needs.cpuOnly() {
					if st.UserCancel {
						if err := s.drainCancelledNativeMain(&st, clock, memoryFiles...); err != nil {
							return err
						}
					}
					// Only native cancellation establishes a separate sidecar phase.
					// An ordinary command's delivered cancel must not gain another
					// TERM or wait, even when shared inputs gave it group custody.
					if !st.UserCancel || st.SidecarStopAt != nil {
						if err := gracefulRecoveredGroups(&st, clock, func() error { return saveWorkerJSON(s.dir, "attempt-"+s.state.Identity+".json", st) }, memoryFiles...); err != nil {
							return err
						}
					}
				}
				return cleanupAttemptGroups(st, clock, memoryFiles...)
			}
		}
	}
	if s.state.PID <= 0 || s.state.Identity == "" {
		if (s.state.Job == nil || !s.state.Job.Spec.Run.Docker) && s.state.Result != nil && s.state.Result.ReleasePending {
			return errors.New("attempt cleanup custody unavailable; release pending")
		}
		return nil
	}
	members, err := workerGroupMembers(s.state.PID)
	if err != nil {
		return err
	}
	owned := false
	for _, pid := range members {
		out, err := exec.Command("ps", "eww", "-p", strconv.Itoa(pid), "-o", "command=").Output()
		if err != nil {
			var e *exec.ExitError
			if errors.As(err, &e) && e.ExitCode() == 1 {
				continue
			}
			return fmt.Errorf("inspect orphan identity: %w", err)
		}
		if strings.Contains(string(out), "myce-supervisor-"+s.state.Identity) || strings.Contains(string(out), "MYCE_PROCESS_ID="+s.state.Identity) {
			owned = true
			break
		}
	}
	if s.state.Job != nil && len(members) > 0 && !owned {
		if !s.state.Job.Spec.Run.Docker && s.state.Result != nil && s.state.Result.ReleasePending {
			return errors.New("attempt process group identity unconfirmed; release pending")
		}
		return nil // Preserve a live command whose identity does not match.
	}
	var memory *memoryAttempt
	if s.state.Cgroup != "" {
		prefix := "engine-"
		if s.state.Job != nil {
			prefix = "attempt-"
		}
		if filepath.Base(s.state.Cgroup) != prefix+s.state.Identity {
			return errors.New("mismatched cgroup custody")
		}
		if _, err := os.Stat(s.state.Cgroup); err == nil {
			var fs memoryFS = osMemoryFS{}
			if len(memoryFiles) > 0 {
				fs = memoryFiles[0]
			}
			memory = &memoryAttempt{fs: fs, path: s.state.Cgroup}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if !owned && memory == nil {
		if len(members) > 0 {
			return errors.New("helper process group identity unconfirmed; release pending")
		}
		return nil
	}
	if (s.state.Accelerator || s.state.Job != nil && !s.state.Job.Spec.Needs.cpuOnly()) && !s.state.UserCancel {
		group := s.state.PID
		if !owned {
			group = 0 // Only the cgroup is authenticated, never signal a reused PGID.
		}
		if err := gracefulRecoveredGroup(group, clock, &s.state.StopAt, s.state.StopGrace, s.save, memory); err != nil {
			return err
		}
	}
	if owned {
		if err = signalWorkerGroup(s.state.PID, syscall.SIGKILL); err != nil {
			return err
		}
	}
	if !owned {
		return memory.cleanup(clock)
	}
	return errors.Join(waitWorkerGroup(s.state.PID, clock), memory.cleanup(clock))
}

func (s *workerStore) releaseSharedInputs() {
	if s.sharedRelease != nil {
		s.sharedRelease()
	}
}

func cleanupAttemptGroups(st workerState, clock Clock, memoryFiles ...memoryFS) error {

	if st.Cgroup != "" {
		if filepath.Base(st.Cgroup) != "attempt-"+st.Identity {
			return errors.New("mismatched attempt cgroup custody")
		}
		if _, err := os.Stat(st.Cgroup); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return err
			}
			var fs memoryFS = osMemoryFS{}
			if len(memoryFiles) > 0 {
				fs = memoryFiles[0]
			}
			if err := (&memoryAttempt{fs: fs, path: st.Cgroup}).cleanup(clock); err != nil {
				return err
			}
		}
	}
	for group, births := range st.Groups {
		members, err := workerGroupMembers(group)
		if err != nil {
			return err
		}
		owned := false
		for _, pid := range members {
			if births[pid] == "" {
				continue
			}
			birth, err := processBirth(pid)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			owned = owned || birth == births[pid]
		}
		if len(members) != 0 && !owned {
			return errors.New("attempt process group identity unconfirmed; release pending")
		}
		if owned {
			if err := signalWorkerGroup(group, syscall.SIGKILL); err != nil {
				return err
			}
		}
		if err := waitWorkerGroup(group, clock); err != nil {
			return err
		}
	}
	return nil
}
func attemptCheckpoint(j Job) string {
	return filepath.Join(j.Checkpoint, fmt.Sprintf("attempt-%d", j.Attempt))
}
func checkpointProgress(dir string) (int64, error) {
	return scanCheckpointProgress(dir, false)
}
func scanCheckpointProgress(dir string, jobRoot bool) (int64, error) {
	latest := int64(-1)
	err := filepath.WalkDir(dir, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if jobRoot && filepath.Dir(path) == dir && e.IsDir() && strings.HasPrefix(e.Name(), "attempt-") {
			return filepath.SkipDir
		}
		if strings.HasSuffix(e.Name(), ".tmp") {
			if e.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if e.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("checkpoint symlink is unsupported: %s", path)
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			latest = max(latest, 0) // Unsequenced legacy saves rank below every generation.
			if strings.HasSuffix(e.Name(), ".json") {
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				var save map[string]json.RawMessage
				if json.Unmarshal(data, &save) == nil && save["myce_checkpoint"] != nil {
					var metadata map[string]json.RawMessage
					if json.Unmarshal(save["myce_checkpoint"], &metadata) == nil && metadata["generation"] != nil {
						var generation int64
						if err := json.Unmarshal(metadata["generation"], &generation); err != nil || generation <= 0 {
							return fmt.Errorf("invalid checkpoint generation: %s", path)
						}
						latest = max(latest, generation)
					}
				}
			}
		}
		return nil
	})
	return latest, err
}
func prepareCheckpoint(j Job) (string, error) {
	return prepareCheckpointCopy(j, io.Copy)
}
func prepareCheckpointCopy(j Job, copyFile func(io.Writer, io.Reader) (int64, error)) (string, error) {
	return prepareCheckpointCopySync(j, copyFile, syncWorkerDir)
}
func prepareCheckpointCopySync(j Job, copyFile func(io.Writer, io.Reader) (int64, error), syncDir func(string) error) (string, error) {
	if _, err := os.Stat(filepath.Dir(j.Checkpoint)); err != nil {
		return "", fmt.Errorf("checkpoint root unavailable: %w", err)
	}
	if err := os.MkdirAll(j.Checkpoint, 0700); err != nil {
		return "", err
	}
	dest := attemptCheckpoint(j)
	if _, err := os.Stat(dest); err == nil {
		progress, err := checkpointProgress(dest)
		if err != nil || progress >= 0 {
			return dest, err // A prepared retry must not overwrite its own durable saves.
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	entries, err := os.ReadDir(j.Checkpoint)
	if err != nil {
		return "", err
	}
	source := j.Checkpoint
	newest, err := scanCheckpointProgress(j.Checkpoint, true)
	if err != nil {
		return "", err
	}
	newestAttempt := 0
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "attempt-") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(e.Name(), "attempt-"))
		if err != nil || n >= j.Attempt {
			continue
		}
		dir := filepath.Join(j.Checkpoint, e.Name())
		progress, err := checkpointProgress(dir)
		if err != nil {
			return "", err
		}
		if progress >= 0 && (progress > newest || (progress == newest && n > newestAttempt)) {
			newest = progress
			newestAttempt = n
			source = dir
		}
	}
	// Publish the whole copy together: a manifest must not outrank missing payloads.
	staging, err := os.MkdirTemp(j.Checkpoint, ".myce-prepare-*.tmp")
	if err != nil {
		return "", err
	}
	directories := []string{}
	if source != "" {
		err = filepath.WalkDir(source, func(path string, e os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if strings.HasSuffix(e.Name(), ".tmp") {
				if e.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if source == j.Checkpoint && e.IsDir() && path != source && strings.HasPrefix(e.Name(), "attempt-") {
				return filepath.SkipDir
			}
			rel, err := filepath.Rel(source, path)
			if err != nil {
				return err
			}
			target := filepath.Join(staging, rel)
			if e.IsDir() {
				directories = append(directories, target)
				return os.MkdirAll(target, 0700)
			}
			if e.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("checkpoint symlink is unsupported: %s", path)
			}
			info, err := e.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("checkpoint is not a regular file: %s", path)
			}
			in, err := os.Open(path)
			if err != nil {
				return err
			}
			defer in.Close()
			out, err := os.OpenFile(target+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			_, err = copyFile(out, in)
			if err == nil {
				err = out.Sync()
			}
			ce := out.Close()
			if err == nil {
				err = ce
			}
			if err != nil {
				return err
			}
			if err = os.Rename(target+".tmp", target); err != nil {
				return err
			}
			return nil // Staging is unpublished; sync each directory once below.
		})
	}
	if err == nil {
		progress, readErr := scanCheckpointProgress(source, source == j.Checkpoint)
		if readErr != nil {
			err = readErr
		} else if progress != newest {
			err = errors.New("checkpoint changed during preparation; retry")
		}
	}
	if err == nil {
		// Children before parents; every directory entry is durable before publication.
		sort.Slice(directories, func(i, k int) bool { return len(directories[i]) > len(directories[k]) })
		for _, dir := range directories {
			if err = syncDir(dir); err != nil {
				break
			}
		}
		if len(directories) == 0 {
			err = syncDir(staging)
		}
	}
	if err == nil {
		err = os.Rename(staging, dest)
	}
	if err == nil {
		err = syncDir(j.Checkpoint)
	}
	return dest, err
}

func workerCheckpointAvailable(root string, progress func()) error {
	if root == "" {
		return nil
	}
	info, err := os.Stat(root)
	progress()
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("checkpoint root is not a directory")
	}
	f, err := os.CreateTemp(root, ".myce-probe-*.tmp")
	progress()
	if err != nil {
		return err
	}
	name := f.Name()
	err = f.Close()
	progress()
	// Probe files are worker-owned transient output, not user data.
	if removeErr := os.Remove(name); err == nil {
		err = removeErr
	}
	progress()
	return err
}

func syncWorkerDir(dir string) error {
	return syncCheckpointDirectory(dir, checkpointCIFS, (*os.File).Sync)
}

func syncCheckpointDirectory(dir string, filesystem func(*os.File) (bool, error), syncFile func(*os.File) error) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	// Linux CIFS directory fsync is a dummy operation; file flushes remain required.
	if cifs, err := filesystem(f); err == nil && cifs {
		return nil
	}
	return syncFile(f)
}

// Recovery authenticates every surviving group before sending any signal. A
// persisted deadline prevents worker restarts from renewing or resending TERM.
func gracefulRecoveredGroups(st *workerState, clock Clock, persist func() error, memoryFiles ...memoryFS) error {
	var memory *memoryAttempt
	if st.Cgroup != "" {
		if filepath.Base(st.Cgroup) != "attempt-"+st.Identity {
			return errors.New("mismatched attempt cgroup custody")
		}
		if _, err := os.Stat(st.Cgroup); err == nil {
			var fs memoryFS = osMemoryFS{}
			if len(memoryFiles) > 0 {
				fs = memoryFiles[0]
			}
			memory = &memoryAttempt{fs: fs, path: st.Cgroup}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for group, births := range st.Groups {
		members, err := workerGroupMembers(group)
		if err != nil {
			return err
		}
		owned := len(members) == 0
		for _, pid := range members {
			if births[pid] == "" {
				continue
			}
			birth, err := processBirth(pid)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			owned = owned || birth == births[pid]
		}
		if !owned {
			return errors.New("attempt process group identity unconfirmed; release pending")
		}
	}
	first := st.StopAt == nil
	if first {
		deadline := clock.Now().Add(acceleratorStopGrace(st.StopGrace))
		st.StopAt = &deadline
		if err := persist(); err != nil {
			return err
		}
	}
	// TERM is idempotent. A crash after deadline persistence but before signal
	// dispatch must not let cancellation recovery skip helper TERM entirely.
	if first || st.UserCancel {
		groups := map[int]bool{}
		for group := range st.Groups {
			groups[group] = true
			if err := signalWorkerGroup(group, syscall.SIGTERM); err != nil {
				return err
			}
		}
		if err := memory.termOutsideGroups(groups); err != nil {
			return err
		}
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for clock.Now().Before(*st.StopAt) {
		alive, err := memory.populated()
		if err != nil {
			return err
		}
		for group := range st.Groups {
			members, err := workerGroupMembers(group)
			if err != nil {
				return err
			}
			alive = alive || len(members) > 0
		}
		if !alive {
			return nil
		}
		<-tick.C
	}
	return nil
}

func gracefulRecoveredGroup(group int, clock Clock, deadline **time.Time, grace time.Duration, persist func() error, memories ...*memoryAttempt) error {
	var memory *memoryAttempt
	if len(memories) > 0 {
		memory = memories[0]
	}
	groups := map[int]bool{}
	if group > 0 {
		groups[group] = true
	}
	if *deadline == nil {
		at := clock.Now().Add(acceleratorStopGrace(grace))
		*deadline = &at
		if err := persist(); err != nil {
			return err
		}
		if group > 0 {
			if err := signalWorkerGroup(group, syscall.SIGTERM); err != nil {
				return err
			}
		}
		if err := memory.termOutsideGroups(groups); err != nil {
			return err
		}
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for clock.Now().Before(**deadline) {
		var members []int
		if group > 0 {
			var err error
			members, err = workerGroupMembers(group)
			if err != nil {
				return err
			}
		}
		outside, err := memory.outsideGroups(groups, false)
		if err != nil {
			return err
		}
		if len(members) == 0 && !outside {
			return nil
		}
		<-tick.C
	}
	return nil
}
