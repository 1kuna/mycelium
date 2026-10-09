package myce

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
	Session         string  `json:"session"`
	PreviousSession string  `json:"previous_session,omitempty"`
	Fresh           bool    `json:"fresh"`
	Job             *Job    `json:"job,omitempty"`
	Result          *Result `json:"result,omitempty"`
	PID             int     `json:"pid,omitempty"`
	Identity        string  `json:"identity,omitempty"`
	Offset          int64   `json:"offset"`
	Pending         int     `json:"pending,omitempty"`
	CheckpointRoot  string  `json:"checkpoint_root,omitempty"`
	Log             string  `json:"log,omitempty"`
}
type workerStore struct {
	dir   string
	lock  *os.File
	state workerState
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
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err == nil {
		err = json.Unmarshal(data, &s.state)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		s.close()
		return nil, fmt.Errorf("read worker state: %w", err)
	}
	return s, nil
}
func (s *workerStore) close() {
	_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	_ = s.lock.Close()
}
func (s *workerStore) save() error              { return saveWorkerJSON(s.dir, "state.json", s.state) }
func decodeWorkerJSON(data []byte, v any) error { return json.Unmarshal(data, v) }
func saveWorkerJSON(dir, name string, v any) error {
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
	out, err := exec.Command("ps", "-ax", "-o", "pid=,pgid=,stat=").Output()
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
	var latest int64
	err := filepath.WalkDir(dir, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
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
			latest = max(latest, info.ModTime().UnixNano())
		}
		return nil
	})
	return latest, err
}
func prepareCheckpoint(j Job) (string, error) {
	if _, err := os.Stat(filepath.Dir(j.Checkpoint)); err != nil {
		return "", fmt.Errorf("checkpoint root unavailable: %w", err)
	}
	if err := os.MkdirAll(j.Checkpoint, 0700); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(j.Checkpoint)
	if err != nil {
		return "", err
	}
	source := j.Checkpoint
	var newest int64
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
		if progress > newest {
			newest = progress
			source = dir
		}
	}
	dest := attemptCheckpoint(j)
	if err = os.MkdirAll(dest, 0700); err != nil {
		return "", err
	}
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
			target := filepath.Join(dest, rel)
			if e.IsDir() {
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
			_, err = io.Copy(out, in)
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
			if err = os.Chtimes(target+".tmp", info.ModTime(), info.ModTime()); err != nil {
				return err
			}
			if err = os.Rename(target+".tmp", target); err != nil {
				return err
			}
			return syncWorkerDir(filepath.Dir(target))
		})
	}
	if err == nil {
		err = syncWorkerDir(dest)
	}
	if err == nil {
		err = syncWorkerDir(j.Checkpoint)
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
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
