package myce

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// EnvironmentRecipe is an immutable submission-time snapshot. Only the cache
// destination is substituted in binary build arguments; no implicit shell.
type EnvironmentRecipe struct {
	Python         string            `json:"python,omitempty" yaml:"python,omitempty"`
	LockFile       string            `json:"lock_file,omitempty" yaml:"lock_file,omitempty"`
	Binary         string            `json:"binary,omitempty" yaml:"binary,omitempty"`
	BinaryRelative string            `json:"binary_relative,omitempty" yaml:"-"`
	Build          []string          `json:"build,omitempty" yaml:"build,omitempty"`
	Files          map[string][]byte `json:"files,omitempty" yaml:"-"`
	Key            string            `json:"key" yaml:"-"`
	Platform       string            `json:"platform" yaml:"-"`
}
type EnvironmentCatalog struct {
	Environments map[string]map[string]EnvironmentRecipe `json:"environments" yaml:"environments"`
}

func ReadEnvironments(path string) (EnvironmentCatalog, error) {
	var c EnvironmentCatalog
	if path == "" {
		return c, nil
	}
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := yaml.NewDecoder(io.LimitReader(f, 1<<20))
	d.KnownFields(true)
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return c, errors.New("expected one environment catalog")
	}
	for name, platforms := range c.Environments {
		if name == "" || len(platforms) == 0 {
			return c, errors.New("environment requires a name and platform recipes")
		}
		for platform, r := range platforms {
			if !validGPU(platform) {
				return c, fmt.Errorf("invalid environment platform %s", platform)
			}
			if r.LockFile != "" && !filepath.IsAbs(r.LockFile) {
				r.LockFile = filepath.Join(filepath.Dir(path), r.LockFile)
			}
			if r.Binary != "" && !filepath.IsAbs(r.Binary) {
				r.BinaryRelative = r.Binary
				r.Binary = filepath.Join(filepath.Dir(path), r.Binary)
			}
			r, e = snapshotEnvironment(r, platform)
			if e != nil {
				return c, fmt.Errorf("environment %s/%s: %w", name, platform, e)
			}
			platforms[platform] = r
		}
	}
	return c, nil
}
func snapshotEnvironment(r EnvironmentRecipe, platform string) (EnvironmentRecipe, error) {
	r.Platform = platform
	r.Key = ""
	if (r.LockFile != "") == (r.Binary != "" || len(r.Build) > 0) {
		return r, errors.New("recipe requires either a Python lock file or a binary/build")
	}
	r.Files = map[string][]byte{}
	if r.LockFile != "" {
		if r.Python == "" {
			return r, errors.New("Python lock file requires python version")
		}
		name := filepath.Base(r.LockFile)
		if name != "uv.lock" && name != "pyproject.toml" && name != "requirements.txt" {
			return r, errors.New("lock_file must be uv.lock, pyproject.toml, or requirements.txt")
		}
		names := []string{"requirements.txt"}
		if name != "requirements.txt" {
			names = []string{"uv.lock", "pyproject.toml"}
		}
		for _, n := range names {
			b, e := os.ReadFile(filepath.Join(filepath.Dir(r.LockFile), n))
			if e != nil {
				return r, e
			}
			if len(b) > 1<<20 {
				return r, errors.New("environment input exceeds 1 MiB")
			}
			r.Files[n] = b
		}
	} else if r.Binary != "" {
		// Binaries stay on NAS; include verified content identity, not its mount path.
		f, e := os.Open(r.Binary)
		if e != nil {
			return r, e
		}
		h := sha256.New()
		_, e = io.Copy(h, f)
		f.Close()
		if e != nil {
			return r, e
		}
		r.Files["binary.name"] = []byte(filepath.Base(r.Binary))
		r.Files["binary.sha256"] = []byte(hex.EncodeToString(h.Sum(nil)))
	}
	identity := r
	identity.LockFile = ""
	identity.Binary = ""
	identity.BinaryRelative = ""
	identity.Key = ""
	b, e := json.Marshal(identity)
	if e != nil {
		return r, e
	}
	h := sha256.Sum256(b)
	r.Key = hex.EncodeToString(h[:])
	return r, nil
}
func (q *Queue) ConfigureEnvironments(c EnvironmentCatalog) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.environments = c
	return nil
}
func (q *Queue) resolveEnvironment(e Environment) (map[string]EnvironmentRecipe, error) {
	if e.Key() == "" {
		return nil, nil
	}
	if e.Name != "" {
		p, ok := q.environments.Environments[e.Name]
		if !ok { // Existing explicit --environments remains supported only without a catalog.
			if len(q.environments.Environments) == 0 {
				return nil, nil
			}
			return nil, fmt.Errorf("unknown environment %q", e.Name)
		}
		return p, nil
	}
	out := map[string]EnvironmentRecipe{}
	for _, p := range []string{"cuda", "xpu", "metal", "fake"} {
		if len(e.Platforms) > 0 && !slices.Contains(e.Platforms, p) {
			continue
		}
		r, err := snapshotEnvironment(EnvironmentRecipe{LockFile: e.LockFile, Python: e.Python}, p)
		if err != nil {
			return nil, err
		}
		out[p] = r
	}
	return out, nil
}
func environmentKey(j *Job, m *Machine) string {
	if len(j.EnvironmentRecipes) > 0 {
		return j.EnvironmentRecipes[m.GPU].Key
	}
	return j.Spec.Environment.Key()
}
func environmentReady(j *Job, m *Machine) bool {
	k := environmentKey(j, m)
	return j.Spec.Environment.Key() == "" || k != "" && slices.Contains(m.Environments, k)
}
func environmentCanRun(j *Job, m *Machine) bool {
	if j.Spec.Environment.Key() == "" {
		return true
	}
	k := environmentKey(j, m)
	if k == "" {
		return false
	}
	return slices.Contains(m.Environments, k) || len(j.EnvironmentRecipes) > 0 && m.EnvironmentBuilder
}

type EnvironmentReport struct {
	Key          string  `json:"key"`
	State        string  `json:"state"`
	Error        string  `json:"error,omitempty"`
	Log          string  `json:"log,omitempty"`
	BuildSeconds float64 `json:"build_seconds,omitempty"`
}

func (q *Queue) environmentPlan(m *Machine) []EnvironmentRecipe {
	if !q.workerIdle(m) || !m.EnvironmentBuilder {
		return nil
	}
	jobs := []*Job{}
	for _, j := range q.s.Jobs {
		if j.State == "queued" && q.placementAllowed(j, m) && fits(j, m, m.FreeMemoryGB) && !environmentReady(j, m) {
			jobs = append(jobs, j)
		}
	}
	sort.Slice(jobs, func(i, k int) bool { return before(jobs[i], jobs[k], m) })
	if len(jobs) == 0 {
		return nil
	}
	j := jobs[0]
	for _, other := range q.s.Machines {
		if other.ID == m.ID || !q.workerIdle(other) || !q.placementAllowed(j, other) || !fits(j, other, other.FreeMemoryGB) {
			continue
		}
		if q.placementBetter(j, other, m) {
			return nil
		}
	}
	return []EnvironmentRecipe{j.EnvironmentRecipes[m.GPU]}
}
func environmentSeconds(j *Job, m *Machine) float64 {
	if environmentReady(j, m) {
		return 0
	}
	if v := m.EnvironmentBuildSeconds[environmentKey(j, m)]; v > 0 {
		return v
	}
	total := 0.0
	count := 0
	for _, v := range m.EnvironmentBuildSeconds {
		if v > 0 {
			total += v
			count++
		}
	}
	if count > 0 {
		return total / float64(count)
	}
	return 60
}

// EnvironmentCache uses OS file locks, atomic publication and shared use leases.
// Partial builds and evictions are moved to recoverable local Trash, never deleted.
type EnvironmentCache struct {
	once    sync.Once
	initErr error
	Root    string
	Limit   int64
	Clock   Clock
	Run     func(context.Context, []string, string, []string, io.Writer) error
}

func (c *EnvironmentCache) init() error {
	c.once.Do(func() {
		if c.Clock == nil {
			c.Clock = RealClock{}
		}
		if c.Limit <= 0 {
			c.initErr = errors.New("environment-cache-bytes must be positive")
			return
		}
		if c.Root == "" {
			c.initErr = errors.New("environment cache root is required")
			return
		}
		if e := os.MkdirAll(c.Root, 0700); e != nil {
			c.initErr = e
			return
		}
		root, e := filepath.EvalSymlinks(c.Root)
		if e != nil {
			c.initErr = e
			return
		}
		root, e = filepath.Abs(root)
		if e != nil {
			c.initErr = e
			return
		}
		c.Root = root
		for _, p := range []string{filepath.Join(c.Root, "locks"), filepath.Join(c.Root, ".Trash")} {
			if e := os.MkdirAll(p, 0700); e != nil {
				c.initErr = e
				return
			}
		}
	})
	return c.initErr
}

func (c *EnvironmentCache) lock(ctx context.Context, key string, op int) (*os.File, error) {
	f, e := os.OpenFile(filepath.Join(c.Root, "locks", key), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	for {
		e = syscall.Flock(int(f.Fd()), op|syscall.LOCK_NB)
		if e == nil {
			return f, nil
		}
		if e != syscall.EWOULDBLOCK && e != syscall.EAGAIN {
			f.Close()
			return nil, e
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func unlockEnvironment(f *os.File) {
	if f != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}
}
func (c *EnvironmentCache) trash(path string) error {
	if _, e := os.Stat(path); errors.Is(e, os.ErrNotExist) {
		return nil
	}
	dst := filepath.Join(c.Root, ".Trash", ID()+"-"+filepath.Base(path))
	if e := os.Rename(path, dst); e != nil {
		return e
	}
	_, e := os.Stat(dst)
	return e
}
func environmentSize(path string) (int64, error) {
	var size int64
	e := filepath.WalkDir(path, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			s, e := d.Info()
			if e != nil {
				return e
			}
			size += s.Size()
		}
		return nil
	})
	return size, e
}
func (c *EnvironmentCache) ready(key string) bool {
	_, e := os.Stat(filepath.Join(c.Root, key, "ready.json"))
	return e == nil
}
func (c *EnvironmentCache) Cleanup(ctx context.Context) error {
	if e := c.init(); e != nil {
		return e
	}
	entries, e := os.ReadDir(c.Root)
	if e != nil {
		return e
	}
	for _, d := range entries {
		if !strings.HasSuffix(d.Name(), ".partial") {
			continue
		}
		key := strings.TrimSuffix(d.Name(), ".partial")
		f, e := os.OpenFile(filepath.Join(c.Root, "locks", key+".build"), os.O_CREATE|os.O_RDWR, 0600)
		if e != nil {
			return e
		}
		if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
			e = c.cleanupBuild(filepath.Join(c.Root, d.Name()))
			if e == nil {
				e = c.trash(filepath.Join(c.Root, d.Name()))
			}
			unlockEnvironment(f)
			if e != nil {
				return e
			}
		} else {
			f.Close()
		}
	}
	return ctx.Err()
}
func (c *EnvironmentCache) admit(key string, size int64) error {
	entries, e := os.ReadDir(c.Root)
	if e != nil {
		return e
	}
	type entry struct {
		key  string
		size int64
		at   time.Time
	}
	candidates := []entry{}
	total := size
	for _, d := range entries {
		if d.IsDir() && strings.HasSuffix(d.Name(), ".partial") && d.Name() != key+".partial" {
			n, e := environmentSize(filepath.Join(c.Root, d.Name()))
			if e != nil && !errors.Is(e, os.ErrNotExist) {
				return e
			}
			total += n
			continue
		}
		if !d.IsDir() || len(d.Name()) != 64 || d.Name() == key {
			continue
		}
		path := filepath.Join(c.Root, d.Name())
		n, e := environmentSize(path)
		if e != nil {
			return e
		}
		s, e := os.Stat(filepath.Join(path, "ready.json"))
		if e != nil {
			return e
		}
		total += n
		candidates = append(candidates, entry{d.Name(), n, s.ModTime()})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].at.Before(candidates[j].at) })
	for _, v := range candidates {
		if total <= c.Limit {
			break
		}
		f, e := os.OpenFile(filepath.Join(c.Root, "locks", v.key+".use"), os.O_CREATE|os.O_RDWR, 0600)
		if e != nil {
			return e
		}
		if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
			e = c.trash(filepath.Join(c.Root, v.key))
			unlockEnvironment(f)
			if e != nil {
				return e
			}
			total -= v.size
		} else {
			f.Close()
		}
	}
	if total > c.Limit {
		return fmt.Errorf("environment cache needs %d bytes; limit %d (in-use entries protected)", total, c.Limit)
	}
	return nil
}
func (c *EnvironmentCache) Acquire(ctx context.Context, r EnvironmentRecipe) (string, func(), error) {
	if len(r.Key) != 64 {
		return "", nil, errors.New("invalid environment hash")
	}
	if e := c.init(); e != nil {
		return "", nil, e
	}
	// Shared use lock spans build and execution, preventing eviction races.
	use, e := c.lock(ctx, r.Key+".use", syscall.LOCK_SH)
	if e != nil {
		return "", nil, e
	}
	release := func() { unlockEnvironment(use) }
	if e = c.ensure(ctx, r); e != nil {
		release()
		return "", nil, e
	}
	path := filepath.Join(c.Root, r.Key)
	at := c.Clock.Now()
	if e = os.Chtimes(filepath.Join(path, "ready.json"), at, at); e != nil {
		release()
		return "", nil, e
	}
	return path, release, nil
}
func (c *EnvironmentCache) ensure(ctx context.Context, r EnvironmentRecipe) error {
	build, e := c.lock(ctx, r.Key+".build", syscall.LOCK_EX)
	if e != nil {
		return e
	}
	defer unlockEnvironment(build)
	if c.ready(r.Key) {
		return nil
	}
	partial := filepath.Join(c.Root, r.Key+".partial")
	if e = c.cleanupBuild(partial); e != nil {
		return e
	}
	if e = c.trash(partial); e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Join(partial, "bin"), 0700); e != nil {
		return e
	}
	output, e := os.OpenFile(filepath.Join(c.Root, r.Key+".build.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer func() { _ = output.Sync(); _ = output.Close() }()
	start := c.Clock.Now()
	env := []string{}
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "MYCE_TOKEN=") && !strings.HasPrefix(v, "UV_CACHE_DIR=") && !strings.HasPrefix(v, "UV_PYTHON_INSTALL_DIR=") && !strings.HasPrefix(v, "UV_PROJECT_ENVIRONMENT=") {
			env = append(env, v)
		}
	}
	env = append(env, "UV_CACHE_DIR="+filepath.Join(partial, "uv-cache"), "UV_PYTHON_INSTALL_DIR="+filepath.Join(partial, "python"), "UV_PROJECT_ENVIRONMENT="+filepath.Join(partial, "venv"))
	run := c.Run
	if run == nil {
		run = c.runBuild
	}

	buildCtx, cancelBuild := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	monitorStop := make(chan struct{})
	var budgetErr error
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-monitorStop:
				return
			case <-ticker.C:
				size, e := environmentSize(partial)
				if errors.Is(e, os.ErrNotExist) {
					continue
				}
				if e == nil {
					var gate *os.File
					gate, e = c.lock(buildCtx, "admission", syscall.LOCK_EX)
					if e == nil {
						e = c.admit(r.Key, size)
						unlockEnvironment(gate)
					}
				}
				if e == nil {
					var free int64
					free, e = diskFree(c.Root)
					if e == nil && free < 64*1024*1024 {
						e = errors.New("environment build disk space below 64 MiB")
					}
				}
				if e != nil {
					budgetErr = e
					cancelBuild()
					return
				}
			}
		}
	}()
	monitoring := true
	stopMonitor := func() {
		if monitoring {
			close(monitorStop)
			<-monitorDone
			monitoring = false
		}
		cancelBuild()
	}
	defer stopMonitor()
	if r.LockFile != "" {
		for n, b := range r.Files {
			if n != "uv.lock" && n != "pyproject.toml" && n != "requirements.txt" {
				return errors.New("invalid environment input name")
			}
			if e = os.WriteFile(filepath.Join(partial, n), b, 0600); e != nil {
				return e
			}
		}
		if _, ok := r.Files["uv.lock"]; ok {
			e = run(buildCtx, []string{"uv", "sync", "--frozen", "--no-dev", "--no-install-project", "--no-editable", "--python", r.Python}, partial, env, output)
		} else {
			e = run(buildCtx, []string{"uv", "venv", "--python", r.Python, filepath.Join(partial, "venv")}, partial, env, output)
			if e == nil {
				e = run(buildCtx, []string{"uv", "pip", "sync", "--python", filepath.Join(partial, "venv", "bin", "python"), "requirements.txt"}, partial, env, output)
			}
		}
	} else if r.Binary != "" {
		var b []byte
		b, e = os.ReadFile(r.Binary)
		if e == nil {
			h := sha256.Sum256(b)
			if hex.EncodeToString(h[:]) != string(r.Files["binary.sha256"]) {
				e = errors.New("NAS binary changed since submission")
			} else {
				e = os.WriteFile(filepath.Join(partial, "bin", filepath.Base(r.Binary)), b, 0700)
			}
		}
	}
	if e == nil && len(r.Build) > 0 {
		argv := slices.Clone(r.Build)
		for i, a := range argv {
			argv[i] = strings.ReplaceAll(a, "{root}", partial)
		}
		e = run(buildCtx, argv, partial, env, output)
	}
	stopMonitor()
	e = errors.Join(e, budgetErr)
	_ = output.Sync()
	if e != nil {
		fmt.Fprintln(output, "environment build failed:", e)
		return fmt.Errorf("environment %s build failed: %w (log %s)", r.Key, e, output.Name())
	}
	// Virtualenv scripts contain absolute paths. Build in a stable path via a
	// temporary symlink is unsafe; rewrite textual launchers before publication.
	final := filepath.Join(c.Root, r.Key)
	if e = relocateEnvironment(partial, final); e != nil {
		return e
	}
	report := EnvironmentReport{Key: r.Key, State: "ready", BuildSeconds: c.Clock.Now().Sub(start).Seconds()}
	if e = saveWorkerJSON(partial, "ready.json", report); e != nil {
		return e
	}
	size, e := environmentSize(partial)
	if e != nil {
		return e
	}
	gate, e := c.lock(ctx, "admission", syscall.LOCK_EX)
	if e != nil {
		return e
	}
	defer unlockEnvironment(gate)
	if e = c.admit(r.Key, size); e != nil {
		return e
	}
	if e = syncEnvironmentFiles(partial); e != nil {
		return e
	}
	if e = os.Rename(partial, final); e != nil {
		return e
	}
	dir, e := os.Open(c.Root)
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
func relocateEnvironment(from, to string) error {
	return filepath.WalkDir(from, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			target, e := os.Readlink(p)
			if e != nil {
				return e
			}
			if strings.HasPrefix(target, from+string(filepath.Separator)) { // preserve original link in Trash before replacing it
				trash := filepath.Join(from, "link-trash")
				if e = os.MkdirAll(trash, 0700); e != nil {
					return e
				}
				if e = os.Rename(p, filepath.Join(trash, ID())); e != nil {
					return e
				}
				return os.Symlink(to+strings.TrimPrefix(target, from), p)
			}
			return nil
		}
		rel, e := filepath.Rel(from, p)
		if e != nil {
			return e
		}
		if !strings.HasPrefix(rel, "venv"+string(filepath.Separator)+"bin"+string(filepath.Separator)) && !strings.HasPrefix(rel, "bin"+string(filepath.Separator)) && rel != "venv/pyvenv.cfg" {
			return nil
		}
		st, e := d.Info()
		if e != nil {
			return e
		}
		if st.Size() > 1<<20 {
			return nil
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		if strings.Contains(string(b), from) && !strings.ContainsRune(string(b), '\x00') {
			return os.WriteFile(p, []byte(strings.ReplaceAll(string(b), from, to)), st.Mode())
		}
		return nil
	})
}
func (c *EnvironmentCache) cleanupBuild(path string) error {
	b, e := os.ReadFile(filepath.Join(path, "build-state.json"))
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	var state workerState
	if e = json.Unmarshal(b, &state); e != nil {
		return e
	}
	store := workerStore{state: state}
	return store.cleanupOrphan(c.Clock)
}
func (c *EnvironmentCache) runBuild(ctx context.Context, argv []string, dir string, env []string, out io.Writer) error {
	identity := ID()
	state := workerState{Identity: identity}
	if e := saveWorkerJSON(dir, "build-state.json", state); e != nil {
		return e
	}
	args := []string{"-c", `read gate || exit 125; "$@" < /dev/null & child=$!; wait "$child"; exit $?`, "myce-supervisor-" + identity}
	args = append(args, argv...)
	cmd := exec.CommandContext(ctx, "sh", args...)
	cmd.Dir = dir
	cmd.Env = append(env, "MYCE_PROCESS_ID="+identity)
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = time.Second
	reader, writer, e := os.Pipe()
	if e != nil {
		return e
	}
	defer reader.Close()
	defer writer.Close()
	cmd.Stdin = reader
	if e = cmd.Start(); e != nil {
		return e
	}
	state.PID = cmd.Process.Pid
	if e = saveWorkerJSON(dir, "build-state.json", state); e != nil {
		_ = syscall.Kill(-state.PID, syscall.SIGKILL)
		_ = cmd.Wait()
		return e
	}
	if _, e = writer.Write([]byte("start\n")); e != nil {
		_ = syscall.Kill(-state.PID, syscall.SIGKILL)
	}
	waitErr := cmd.Wait()
	_ = syscall.Kill(-state.PID, syscall.SIGKILL)
	groupErr := waitWorkerGroup(state.PID, c.Clock)
	return errors.Join(e, waitErr, groupErr)
}

type environmentRuntime struct {
	dir     string
	cache   *EnvironmentCache
	mu      sync.Mutex
	reports map[string]EnvironmentReport
	offsets map[string]int64
	cancel  context.CancelFunc
	done    chan struct{}
	key     string
}

func (rt *environmentRuntime) sync(plan []EnvironmentRecipe, busy bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	wanted := ""
	if !busy && len(plan) > 0 {
		wanted = plan[0].Key
	}
	if rt.cancel != nil {
		if wanted != rt.key {
			rt.cancel()
		}
		return
	}
	if wanted == "" {
		return
	}
	if r := rt.reports[wanted]; r.State == "failed" {
		return
	}
	r := plan[0]
	ctx, cancel := context.WithCancel(context.Background())
	rt.cancel = cancel
	rt.done = make(chan struct{})
	rt.key = r.Key
	rt.reports[r.Key] = EnvironmentReport{Key: r.Key, State: "building"}
	if e := saveWorkerJSON(rt.dir, "environment-reports.json", rt.reports); e != nil {
		log.Printf("persist environment build intent: %v", e)
	}
	go func() {
		_, release, e := rt.cache.Acquire(ctx, r)
		if release != nil {
			release()
		}
		report := EnvironmentReport{Key: r.Key, State: "ready"}
		if e != nil {
			report.State = "failed"
			report.Error = e.Error()
		}
		b, _ := os.ReadFile(filepath.Join(rt.cache.Root, r.Key+".build.log"))
		if len(b) > 32768 {
			b = b[len(b)-32768:]
		}
		report.Log = string(b)
		if ctx.Err() != nil {
			report.State = "paused"
			report.Error = ""
		}
		if e == nil {
			b, _ := os.ReadFile(filepath.Join(rt.cache.Root, r.Key, "ready.json"))
			_ = json.Unmarshal(b, &report)
		}
		rt.mu.Lock()
		rt.reports[r.Key] = report
		if e := saveWorkerJSON(rt.dir, "environment-reports.json", rt.reports); e != nil {
			log.Printf("persist environment build result: %v", e)
		}
		rt.cancel = nil
		close(rt.done)
		rt.mu.Unlock()
	}()
}
func (rt *environmentRuntime) stop() {
	rt.mu.Lock()
	cancel, done := rt.cancel, rt.done
	rt.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}
func (rt *environmentRuntime) report() ([]string, []EnvironmentReport, map[string]float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	ready := []string{}
	reports := []EnvironmentReport{}
	times := map[string]float64{}
	entries, _ := os.ReadDir(rt.cache.Root)
	for _, d := range entries {
		if len(d.Name()) != 64 || !rt.cache.ready(d.Name()) {
			continue
		}
		ready = append(ready, d.Name())
		var r EnvironmentReport
		b, _ := os.ReadFile(filepath.Join(rt.cache.Root, d.Name(), "ready.json"))
		_ = json.Unmarshal(b, &r)
		times[r.Key] = r.BuildSeconds
	}
	for _, r := range rt.reports {
		if r.State == "failed" {
			if st, e := os.Stat(filepath.Join(rt.cache.Root, r.Key+".build.log")); e == nil && rt.offsets[r.Key] < st.Size() {
				r.State = "uploading"
			}
		}
		r.Log = ""
		reports = append(reports, r)
	}
	return ready, reports, times
}

func (q *Queue) reconcileEnvironments(m *Machine) error {
	for _, r := range m.EnvironmentReports {
		if r.State != "failed" {
			continue
		}
		for _, j := range q.s.Jobs {
			if j.State == "queued" && q.placementAllowed(j, m) && fits(j, m, m.FreeMemoryGB) && environmentKey(j, m) == r.Key {
				j.State = "failed"
				j.Reason = "environment build failed on " + m.ID + ": " + r.Error
				if e := q.copyEnvironmentLogs(j, m, r.Key); e != nil {
					return e
				}
				j.EnvironmentFailureLog = r.Log + "\n" + j.Reason + "\n"
			}
		}
	}
	return nil
}

type EnvironmentLog struct {
	Machine string `json:"machine"`
	Session string `json:"session"`
	Key     string `json:"key"`
	Offset  int64  `json:"offset"`
	Text    string `json:"text"`
}

func (rt *environmentRuntime) upload(ctx context.Context, client Client, m Machine) error {
	rt.mu.Lock()
	keys := []string{}
	for k := range rt.reports {
		keys = append(keys, k)
	}
	rt.mu.Unlock()
	for _, key := range keys {
		rt.mu.Lock()
		offset := rt.offsets[key]
		rt.mu.Unlock()
		f, e := os.Open(filepath.Join(rt.cache.Root, key+".build.log"))
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		for range 4 {
			b := make([]byte, 16384)
			n, e := f.ReadAt(b, offset)
			if e != nil && e != io.EOF {
				f.Close()
				return e
			}
			if n == 0 {
				break
			}
			chunk := EnvironmentLog{Machine: m.ID, Session: m.Session, Key: key, Offset: offset + 1, Text: string(b[:n])}
			if e = client.Call(ctx, "POST", "/api/workers/environment-log", chunk, nil); e != nil {
				f.Close()
				return e
			}
			offset += int64(n)
			rt.mu.Lock()
			if rt.offsets == nil {
				rt.offsets = map[string]int64{}
			}
			rt.offsets[key] = offset
			persistErr := saveWorkerJSON(rt.dir, "environment-offsets.json", rt.offsets)
			rt.mu.Unlock()
			if persistErr != nil {
				f.Close()
				return persistErr
			}
		}
		f.Close()
	}
	return nil
}
func (q *Queue) AppendEnvironmentLog(l EnvironmentLog) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	m := q.s.Machines[l.Machine]
	if m == nil || m.Session != l.Session || m.Gone {
		return errors.New("stale environment builder")
	}
	if len(l.Key) != 64 || l.Offset <= 0 || len(l.Text) > 16384 {
		return errors.New("invalid environment log chunk")
	}
	tx, e := q.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.Exec("INSERT OR IGNORE INTO environment_build_logs(machine,session,key,offset,text) VALUES(?,?,?,?,?)", l.Machine, l.Session, l.Key, l.Offset, l.Text); e != nil {
		return e
	}
	for _, j := range q.s.Jobs {
		if j.State == "queued" && environmentKey(j, m) == l.Key {
			if e = insertEnvironmentJobLog(tx, j.ID, l); e != nil {
				return e
			}
		}
	}
	return tx.Commit()
}
func insertEnvironmentJobLog(tx *sql.Tx, id string, l EnvironmentLog) error {
	res, e := tx.Exec("INSERT OR IGNORE INTO environment_log_offsets(job,machine,session,key,offset) VALUES(?,?,?,?,?)", id, l.Machine, l.Session, l.Key, l.Offset)
	if e != nil {
		return e
	}
	n, e := res.RowsAffected()
	if e != nil {
		return e
	}
	if n > 0 {
		_, e = tx.Exec("INSERT INTO logs(job,attempt,text) VALUES(?,0,?)", id, l.Text)
	}
	return e
}
func (q *Queue) copyEnvironmentLogs(j *Job, m *Machine, key string) error {
	rows, e := q.db.Query("SELECT offset,text FROM environment_build_logs WHERE machine=? AND session=? AND key=? ORDER BY offset", m.ID, m.Session, key)
	if e != nil {
		return e
	}
	chunks := []EnvironmentLog{}
	for rows.Next() {
		l := EnvironmentLog{Machine: m.ID, Session: m.Session, Key: key}
		if e = rows.Scan(&l.Offset, &l.Text); e != nil {
			rows.Close()
			return e
		}
		chunks = append(chunks, l)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	tx, e := q.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, l := range chunks {
		if e = insertEnvironmentJobLog(tx, j.ID, l); e != nil {
			return e
		}
	}
	return tx.Commit()
}

func syncEnvironmentFiles(root string) error {
	dirs := []string{}
	e := filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			dirs = append(dirs, p)
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		f, e := os.Open(p)
		if e != nil {
			return e
		}
		return errors.Join(f.Sync(), f.Close())
	})
	if e != nil {
		return e
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		f, e := os.Open(dirs[i])
		if e != nil {
			return e
		}
		e = errors.Join(f.Sync(), f.Close())
		if e != nil {
			return e
		}
	}
	return nil
}

func (rt *environmentRuntime) restore() error {
	for _, item := range []struct {
		name  string
		value any
	}{{"environment-reports.json", &rt.reports}, {"environment-offsets.json", &rt.offsets}} {
		b, e := os.ReadFile(filepath.Join(rt.dir, item.name))
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, item.value); e != nil {
			return e
		}
	}
	for key, r := range rt.reports {
		if r.State == "building" || r.State == "failed" || r.State == "uploading" {
			r.State = "paused"
			r.Error = ""
			rt.reports[key] = r
		}
	}
	return nil
}
