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
	"sort"
	"strconv"
	"strings"
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
	Schema          int                    `json:"schema_version"`
	OperatorStopped bool                   `json:"operator_stopped,omitempty"`
	Groups          map[int]map[int]string `json:"-"`
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
	fileError             string
	fileProgress          checkpointIO[int64]
	fileFinish            checkpointIO[Result]
	fileGeneration        int64
	extensionFiles        checkpointIO[string]
	checkpointUnavailable bool
	nasProbe              checkpointIO[bool]
	nasProgress           checkpointIO[int64]
	nasPrepare            checkpointIO[string]
	dir                   string
	lock                  *os.File
	state                 workerState
}

func (w Worker) openStore() (*workerStore, error) {
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
	s := &workerStore{dir: dir, lock: lock}
	// CIFS metadata round trips can exceed the queue's 25 ms I/O budget.
	s.nasProbe.timeout = 250 * time.Millisecond
	// Preparation also crosses CIFS metadata/automount boundaries. Keep its
	// wait bounded, but allow the same network-filesystem latency as the probe.
	s.nasPrepare.timeout = 250 * time.Millisecond
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
	if marker.Job != nil && marker.Result == nil && st.Job != nil && marker.Identity != "" && marker.Identity == st.Identity && marker.Session == st.Session && marker.Job.ID == st.Job.ID && marker.Job.Attempt == st.Job.Attempt {
		st.OperatorStopped = true
		return s.save()
	}
	return nil
}

// Existing spools always include these fields, even when their values are zero.
func decodeWorkerState(data []byte, state *workerState) error {
	var header struct {
		Schema int `json:"schema_version"`
	}
	if json.Unmarshal(data, &header) != nil || header.Schema != persistedSchema {
		return errPersistedFormat
	}
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

func (s *workerStore) close() {
	_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	_ = s.lock.Close()
}
func (s *workerStore) save() error              { return saveWorkerJSON(s.dir, "state.json", s.state) }
func decodeWorkerJSON(data []byte, v any) error { return json.Unmarshal(data, v) }
func saveWorkerJSON(dir, name string, v any) error {
	if state, ok := v.(workerState); ok {
		state.Schema = persistedSchema
		v = state
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
	out, err := hostPS("-ax", "-o", "pid=,pgid=,stat=")
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
func (s *workerStore) cleanupOrphan(clock Clock) error {
	if s.state.PID <= 0 || s.state.Identity == "" {
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
	if !owned {
		return nil
	}
	if err = syscall.Kill(-s.state.PID, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	return waitWorkerGroup(s.state.PID, clock)
}
func attemptCheckpoint(j Job) string {
	return filepath.Join(j.Checkpoint, fmt.Sprintf("attempt-%d", j.Attempt))
}
func checkpointProgress(dir string) (int64, error) {
	return scanCheckpointProgress(dir, false)
}
func scanCheckpointProgress(dir string, jobRoot bool) (int64, error) {
	latest := int64(-1)
	hasData := false
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
			hasData = true
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
	if err == nil && hasData && latest < 0 {
		err = errors.New("unsequenced checkpoint requires offline conversion or an inert hold")
	}
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

func workerCheckpointAvailable(root string) error {
	if root == "" {
		return nil
	}
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("checkpoint root is not a directory")
	}
	f, err := os.CreateTemp(root, ".myce-probe-*.tmp")
	if err != nil {
		return err
	}
	name := f.Name()
	err = f.Close()
	// Probe files are worker-owned transient output, not user data.
	if removeErr := os.Remove(name); err == nil {
		err = removeErr
	}
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
