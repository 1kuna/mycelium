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
	"path/filepath"
	"runtime"
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
	FilesRef       *FileRef `json:"files_ref,omitempty" yaml:"-"`
	prepare        func(context.Context, string, io.Writer) error
	readyOnly      bool
	Launch         *HostLaunch         `json:"launch,omitempty" yaml:"-"`
	Preparation    *ExtensionSelection `json:"preparation,omitempty" yaml:"-"`
	Assets         FileManifest        `json:"assets,omitempty" yaml:"-"`
	InputDigest    string              `json:"input_digest,omitempty" yaml:"-"`
	Manifest       string              `json:"manifest,omitempty" yaml:"manifest,omitempty"`
	Environment    string              `json:"environment,omitempty" yaml:"environment,omitempty"`
	Backend        string              `json:"backend,omitempty" yaml:"backend,omitempty"`
	Target         *ExecutionTarget    `json:"target,omitempty" yaml:"-"`
	Python         string              `json:"python,omitempty" yaml:"python,omitempty"`
	LockFile       string              `json:"lock_file,omitempty" yaml:"lock_file,omitempty"`
	Binary         string              `json:"binary,omitempty" yaml:"binary,omitempty"`
	BinaryRelative string              `json:"binary_relative,omitempty" yaml:"-"`
	Build          []string            `json:"build,omitempty" yaml:"build,omitempty"`
	Files          map[string][]byte   `json:"files,omitempty" yaml:"-"`
	Key            string              `json:"key" yaml:"-"`
	Platform       string              `json:"platform" yaml:"-"`
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
			if r.Manifest != "" && !filepath.IsAbs(r.Manifest) {
				r.Manifest = filepath.Join(filepath.Dir(path), r.Manifest)
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
	if r.Backend != "" && r.Backend != "uv" && r.Backend != "pixi" {
		return r, errors.New("unsupported environment backend")
	}
	r.Platform = platform
	r.Key = ""
	if (r.LockFile != "") == (r.Binary != "" || len(r.Build) > 0) {
		return r, errors.New("recipe requires either a Python lock file or a binary/build")
	}
	r.Files = map[string][]byte{}
	if r.LockFile != "" {
		if r.Backend != "pixi" && r.Python == "" {
			return r, errors.New("Python lock file requires python version")
		}
		name := filepath.Base(r.LockFile)
		if r.Backend == "pixi" {
			if name != "pixi.lock" {
				return r, errors.New("pixi requires pixi.lock")
			}
		} else if name != "uv.lock" && name != "pyproject.toml" && name != "requirements.txt" {
			return r, errors.New("lock_file must be uv.lock, pyproject.toml, or requirements.txt")
		}
		names := []string{"requirements.txt"}
		if name != "requirements.txt" {
			names = []string{"uv.lock", "pyproject.toml"}
		}
		if r.Backend == "pixi" {
			names = []string{"pixi.lock", "pixi.toml"}
			if r.Manifest != "" && filepath.Clean(r.Manifest) != filepath.Join(filepath.Dir(r.LockFile), "pixi.toml") {
				return r, errors.New("pixi manifest must be pixi.toml beside pixi.lock")
			}
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
	identity.Manifest = ""
	identity.Binary = ""
	identity.BinaryRelative = ""
	identity.Key = ""
	identity.Launch = nil
	b, e := json.Marshal(identity)
	if e != nil {
		return r, e
	}
	h := sha256.Sum256(b)
	r.Key = hex.EncodeToString(h[:])
	return r, nil
}
func (q *Queue) ConfigureEnvironments(c EnvironmentCatalog) error {
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	return q.update(func() error {
		q.environmentRevision++
		q.environments = c
		return nil
	})
}
func (q *Queue) resolveEnvironment(e Environment) (map[string]EnvironmentRecipe, error) {
	return q.environments.resolveEnvironment(e)
}

// The catalog is immutable after configuration. Resolve on a captured catalog
// outside q.mu: inline recipes read and hash local files.
func (c EnvironmentCatalog) resolveEnvironment(e Environment) (map[string]EnvironmentRecipe, error) {
	if e.Key() == "" {
		return nil, nil
	}
	if e.Name != "" {
		p, ok := c.Environments[e.Name]
		if !ok { // Existing explicit --environments remains supported only without a catalog.
			if len(c.Environments) == 0 {
				return nil, nil
			}
			return nil, rejectFieldRule("job_environment_invalid", fmt.Errorf("unknown environment %q", e.Name))
		}
		out := make(map[string]EnvironmentRecipe, len(p))
		for platform, r := range p {
			out[platform] = r
		}
		return out, nil
	}
	out := map[string]EnvironmentRecipe{}
	for _, p := range []string{"cuda", "xpu", "metal", "fake", "cpu"} {
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
func environmentPlatformsMatch(needs ResourceNeeds, recipes map[string]EnvironmentRecipe) bool {
	if len(recipes) == 0 { // No recipe constraint in legacy preinstalled mode.
		return true
	}
	if needs.cpuOnly() {
		_, ok := recipes["cpu"]
		return ok
	}
	// As in placement, any/preferred permit GPU platforms only. CPU execution
	// requires an explicit CPU request after resource normalization.
	for platform := range recipes {
		if platform != "cpu" && (len(needs.Platform.Required) == 0 || slices.Contains(needs.Platform.Required, platform)) {
			return true
		}
	}
	return false
}
func environmentKey(j *Job, m *Machine) string {
	if j.Spec.extensionJob() {
		if e := qualifiedExtension(j, m); e != nil {
			if r, ok := extensionRecipe(*e, m); ok {
				return r.Key
			}
		}
	}
	if len(j.EnvironmentRecipes) > 0 {
		return j.EnvironmentRecipes[m.GPU].Key
	}
	return j.Spec.Environment.Key()
}
func extensionEnvironmentRecipes(j *Job, m *Machine) []EnvironmentRecipe {
	e := qualifiedExtension(j, m)
	if e == nil {
		return nil
	}
	v := e.variant(machineTarget(m))
	if v == nil {
		if j.Spec.ExtensionCheck != nil && (len(e.Assets) > 0 || len(e.Setup) > 0) {
			return []EnvironmentRecipe{{Key: extensionPreparationKey(*e, m)}}
		}
		return nil
	}
	names := make([]string, 0, len(v.Environments))
	for n := range v.Environments {
		names = append(names, n)
	}
	slices.Sort(names)
	out := []EnvironmentRecipe{}
	for _, n := range names {
		out = append(out, v.Environments[n])
	}
	if len(e.Assets) > 0 || j.Spec.ExtensionCheck != nil && len(v.Setup) > 0 {
		out = append(out, EnvironmentRecipe{Key: extensionPreparationKey(*e, m)})
	}
	return out
}
func environmentReady(j *Job, m *Machine) bool {
	// Checks can run before qualification, but never before worker preparation.
	// A no-environment/no-setup variant still needs its installed frozen files.
	if e := qualifiedExtension(j, m); e != nil && j.Spec.ExtensionCheck != nil {
		prepared := false
		for _, r := range m.ExtensionReports {
			if r.Digest == e.Digest && r.Setup && (len(e.Variants) == 0 || r.Host == hostID(m) && r.Fingerprint == m.ExtensionFingerprint && r.EnvironmentIdentity == extensionEnvironmentIdentity(*e, m)) {
				prepared = true
				break
			}
		}
		if !prepared {
			return false
		}
	}
	for _, r := range extensionEnvironmentRecipes(j, m) {
		if !slices.Contains(m.Environments, r.Key) {
			return false
		}
	}
	k := environmentKey(j, m)
	return k == "" && j.Spec.Environment.Key() == "" || k != "" && slices.Contains(m.Environments, k)
}
func environmentCanRun(j *Job, m *Machine) bool {
	if j.Spec.Environment.Key() == "" && environmentKey(j, m) == "" {
		return true
	}
	k := environmentKey(j, m)
	if k == "" {
		return false
	}
	if slices.Contains(m.Environments, k) {
		return true
	}
	if e := qualifiedExtension(j, m); e != nil {
		if _, ok := extensionRecipe(*e, m); ok {
			return m.EnvironmentBuilder
		}
	}
	return len(j.EnvironmentRecipes) > 0 && m.EnvironmentBuilder
}

type EnvironmentReport struct {
	Kind         string  `json:"kind,omitempty"`
	Attempt      string  `json:"attempt,omitempty"`
	Key          string  `json:"key"`
	State        string  `json:"state"`
	Error        string  `json:"error,omitempty"`
	Log          string  `json:"log,omitempty"`
	BuildSeconds float64 `json:"build_seconds,omitempty"`
}

func (q *Queue) environmentPlan(m *Machine) []EnvironmentRecipe {
	if q.s.HostMaintenance[hostID(m)].SharedHost != nil {
		return nil
	}
	if !q.hostLaunchAllowed(m, m.Session) || !q.workerBatchIdle(m) || !m.EnvironmentBuilder || q.preparationStopping(m) {
		return nil
	}
	// Preserve a started same-tier/protected build even if another queued job
	// wins admission. Only a planned stop may interrupt its preparation.
	for _, l := range q.s.PreparationPlans {
		if l.Kind != "environment" || l.Machine != m.ID || l.Session != m.Session || l.Device != targetDevice(m) || !l.Started {
			continue
		}
		if j := q.s.Jobs[l.Demand]; j != nil && j.State == "queued" && !cancelled(j) {
			if e := qualifiedExtension(j, m); e != nil {
				return q.extensionPreparationOffer(*e, m)
			}
			r := j.EnvironmentRecipes[m.GPU]
			if r.Key == l.Key {
				copy := l
				r.Launch = &copy
				return []EnvironmentRecipe{r}
			}
		}
	}
	scan := q.placementScan()
	j := q.candidate(m, nil, scan)
	if j != nil && j.Spec.extensionJob() && !environmentReady(j, m) {
		if e := qualifiedExtension(j, m); e != nil {
			return q.extensionPreparationOffer(*e, m)
		}
	}
	if j == nil || environmentReady(j, m) {
		return q.extensionPreparationPlan(m)
	}
	for _, other := range q.plannerMachines() {
		if sameTarget(other, m) || q.preparationStopping(other) || !q.workerIdle(other, j) || !q.placementAllowed(j, other) || !fits(j, other, other.FreeMemoryGB) {
			continue
		}
		// workerIdle permits a shared started build. Preserve a strict
		// preference for it, but never defer on an identity tie to a busy peer.
		if winner := q.candidate(other, nil, scan); winner != nil && winner.ID == j.ID && q.placementBetterWithTie(j, other, m, !q.preparationActive(other), scan) {
			return nil
		}
	}
	r := j.EnvironmentRecipes[m.GPU]
	l := q.issuePreparation(m, "environment", r.Key)
	r.Launch = &l
	return []EnvironmentRecipe{r}
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
// Partial builds and evictions are retired, then purged once use/build locks allow it.
type EnvironmentCache struct {
	Tools   *EnvironmentToolSource
	once    sync.Once
	initErr error
	Root    string
	Limit   int64
	Clock   Clock
	Fetch   func(context.Context, FileRef, string) error
	Run     func(context.Context, []string, string, []string, io.Writer, io.Writer) error
	// DownloadLimit is separate from the prepared-environment limit.
	DownloadLimit int64
}

const environmentRecentWindow = 24 * time.Hour

func (c *EnvironmentCache) recent(path string) bool {
	st, err := os.Stat(filepath.Join(path, "ready.json"))
	return err == nil && c.Clock.Now().Sub(st.ModTime()) < environmentRecentWindow
}

func (c *EnvironmentCache) init() error {
	c.once.Do(func() {
		if c.Clock == nil {
			c.Clock = RealClock{}
		}
		if c.Limit == 0 && c.Root != "" {
			c.Limit, c.initErr = AutoEnvironmentCacheLimit(c.Root)
			if c.initErr != nil {
				return
			}
		}
		if c.Limit <= 0 {
			c.initErr = errors.New("environment-cache-bytes must be positive")
			return
		}
		if c.Root == "" {
			c.initErr = errors.New("environment cache root is required")
			return
		}
		if e := PrepareStateRoots(c.Root); e != nil {
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
		if c.DownloadLimit == 0 {
			c.DownloadLimit = 16 << 30
		}
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

// tryLock skips live attempts/preparations without waiting for them to finish.
func (c *EnvironmentCache) tryLock(key string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(c.Root, "locks", key), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return nil, nil
		}
		return nil, err
	}
	return f, nil
}

// purgeRetired runs under admission. The remaining bytes still count toward
// the limit, including entries protected by an attempt or preparation.
func (c *EnvironmentCache) purgeRetired() (int64, error) {
	root := filepath.Join(c.Root, ".Trash")
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}
	for _, d := range entries {
		_, key, ok := strings.Cut(d.Name(), "-")
		key = strings.TrimSuffix(key, ".partial")
		if !ok || (FileRef{SHA256: key}).validate() != nil {
			continue
		}
		use, err := c.tryLock(key + ".use")
		if err != nil {
			return 0, err
		}
		if use == nil {
			continue
		}
		build, err := c.tryLock(key + ".build")
		if err == nil && build != nil {
			path := filepath.Join(root, d.Name())
			// Old interrupted builds may still have a recorded child to reap.
			if !c.recent(path) {
				if err = c.cleanupBuild(path); err == nil {
					err = os.RemoveAll(path)
				}
			}
		}
		unlockEnvironment(build)
		unlockEnvironment(use)
		if err != nil {
			return 0, err
		}
	}
	return environmentSize(root)
}

func (c *EnvironmentCache) purgeReleased() {
	gate, err := c.lock(context.Background(), "admission", syscall.LOCK_EX)
	if err == nil {
		_, err = c.purgeRetired()
		unlockEnvironment(gate)
	}
	if err != nil {
		log.Printf("purge retired environments: %v", err)
	}
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

// environmentDiskUsage follows du: allocated blocks, one count per device/inode
// across all supplied prefixes. Symlinks are counted without following targets.
func environmentDiskUsage(paths ...string) (int64, error) {
	seen := map[[2]uint64]bool{}
	var size int64
	for _, root := range paths {
		err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			info, err := d.Info()
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return errors.New("environment disk accounting unavailable")
			}
			id := [2]uint64{uint64(stat.Dev), uint64(stat.Ino)}
			if !seen[id] {
				size += stat.Blocks * 512
				seen[id] = true
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

// DiskUsage reports allocated bytes across stable, building and retired prefixes.
// Download objects have their own bounded cache. Shared inodes count once.
func (c *EnvironmentCache) DiskUsage() (int64, error) {
	entries, err := os.ReadDir(c.Root)
	if err != nil {
		return 0, err
	}
	paths := []string{filepath.Join(c.Root, ".Trash")}
	for _, d := range entries {
		if d.IsDir() && (len(d.Name()) == 64 || strings.HasSuffix(d.Name(), ".partial")) {
			paths = append(paths, filepath.Join(c.Root, d.Name()))
		}
	}
	return environmentDiskUsage(paths...)
}
func (c *EnvironmentCache) ready(key string) bool {
	var r EnvironmentReport
	b, e := os.ReadFile(filepath.Join(c.Root, key, "ready.json"))
	return e == nil && json.Unmarshal(b, &r) == nil && r.Key == key && r.State == "ready"
}
func (c *EnvironmentCache) Cleanup(ctx context.Context) error {
	if e := c.init(); e != nil {
		return e
	}
	gate, e := c.lock(ctx, "admission", syscall.LOCK_EX)
	if e != nil {
		return e
	}
	defer unlockEnvironment(gate)
	entries, e := os.ReadDir(c.Root)
	if e != nil {
		return e
	}
	for _, d := range entries {
		if !strings.HasSuffix(d.Name(), ".partial") && (len(d.Name()) != 64 || c.ready(d.Name())) {
			continue
		}
		key := strings.TrimSuffix(d.Name(), ".partial")
		use, e := c.tryLock(key + ".use")
		if e != nil {
			return e
		}
		if use == nil {
			continue
		}
		build, e := c.tryLock(key + ".build")
		if e == nil && build != nil {
			e = c.cleanupBuild(filepath.Join(c.Root, d.Name()))
			if e == nil {
				e = c.trash(filepath.Join(c.Root, d.Name()))
			}
		}
		unlockEnvironment(build)
		unlockEnvironment(use)
		if e != nil {
			return e
		}
	}
	if _, e := c.purgeRetired(); e != nil {
		return e
	}
	return ctx.Err()
}
func (c *EnvironmentCache) admit(key string, size int64) error {
	_, e := c.purgeRetired()
	if e != nil {
		return e
	}
	entries, e := os.ReadDir(c.Root)
	if e != nil {
		return e
	}
	type entry struct {
		key string
		at  time.Time
	}
	candidates := []entry{}
	total, e := c.DiskUsage()
	if e != nil {
		return e
	}
	total += size
	for _, d := range entries {
		if !d.IsDir() || len(d.Name()) != 64 || d.Name() == key {
			continue
		}
		path := filepath.Join(c.Root, d.Name())
		s, e := os.Stat(filepath.Join(path, "ready.json"))
		if errors.Is(e, os.ErrNotExist) {
			// Unready stable prefixes are builds, never eviction candidates.
			continue
		}
		if e != nil {
			return e
		}
		candidates = append(candidates, entry{d.Name(), s.ModTime()})
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
			// Recheck after locking: a just-finished attempt may have refreshed use.
			if c.recent(filepath.Join(c.Root, v.key)) {
				unlockEnvironment(f)
				continue
			}
			log.Printf("environment eviction key=%s reason=cache_limit", v.key)
			e = c.trash(filepath.Join(c.Root, v.key))
			unlockEnvironment(f)
			if e != nil {
				return e
			}
			if _, e = c.purgeRetired(); e != nil {
				return e
			}
			// Removing one link may free no blocks: measure the remaining inode union.
			total, e = c.DiskUsage()
			if e != nil {
				return e
			}
			total += size
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
	if r.Backend != "" && r.Backend != "uv" && r.Backend != "pixi" {
		return "", nil, errors.New("unsupported environment backend")
	}
	if r.Target != nil && (r.Target.OS != runtime.GOOS || r.Target.Arch != runtime.GOARCH) {
		return "", nil, errors.New("environment target does not match this host")
	}
	if (FileRef{SHA256: r.Key}).validate() != nil {
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
	var released sync.Once
	release := func() {
		released.Do(func() {
			at := c.Clock.Now()
			_ = os.Chtimes(filepath.Join(c.Root, r.Key, "ready.json"), at, at)
			unlockEnvironment(use)
			c.purgeReleased()
		})
	}
	if r.readyOnly && !c.ready(r.Key) {
		release()
		return "", nil, fmt.Errorf("%w: environment preparation required", errExtensionUnavailable)
	}
	for attempt := 0; attempt < 2; attempt++ {
		e = c.ensure(ctx, r)
		if !errors.Is(e, errEnvironmentBuildStalled) || errors.Is(e, errWorkerReleasePending) || ctx.Err() != nil {
			break
		}
		if attempt == 0 {
			if f, err := os.OpenFile(filepath.Join(c.Root, r.Key+".build.log"), os.O_APPEND|os.O_WRONLY, 0600); err == nil {
				fmt.Fprintln(f, "environment build watchdog: retry 1/1 using shared downloads")
				_ = f.Close()
			}
		}
	}
	if e != nil {
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
func (c *EnvironmentCache) ensure(ctx context.Context, r EnvironmentRecipe) (ensureErr error) {
	build, e := c.lock(ctx, r.Key+".build", syscall.LOCK_EX)
	if e != nil {
		return e
	}
	defer unlockEnvironment(build)
	if c.ready(r.Key) {
		return nil
	}
	// Install at the final prefix: conda and managed Python can embed it in binaries.
	// Readiness alone is atomically published after every build step succeeds.
	partial := filepath.Join(c.Root, r.Key)
	if e = c.cleanupBuild(partial); e != nil {
		return e
	}
	if e = c.trash(partial); e != nil {
		return e
	}
	defer func() {
		if !c.ready(r.Key) {
			if err := c.cleanupBuild(partial); err != nil {
				// Transfer unconfirmed helper custody while .build still fences
				// this receipt. An attempt must not release merely because its
				// cancelled builder returned an ordinary command error.
				if retain, ok := ctx.Value(preparationCustodyKey{}).(func(string) error); ok {
					ensureErr = errors.Join(ensureErr, errWorkerReleasePending, err, retain(filepath.Join(partial, "build-state.json")))
				}
				return
			}
			if errors.Is(e, errEnvironmentBuildStalled) {
				// Only this just-created, never-published build is disposable.
				// Its recorded process has been reaped and .build is still held;
				// retain download objects, not a duplicate prefix charged to retry.
				_ = os.RemoveAll(partial)
			} else {
				_ = c.trash(partial)
			}
		}
	}()
	if e = os.MkdirAll(filepath.Join(partial, "bin"), 0700); e != nil {
		return e
	}
	output, e := os.OpenFile(filepath.Join(c.Root, r.Key+".build.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer func() { _ = output.Sync(); _ = output.Close() }()
	start := c.Clock.Now()
	// Package installers also honor the operator's safe uv settings, such as the
	// documented UV_LINK_MODE, but never uv credentials (buildUVVariables).
	env := environmentList(buildEnvironmentFrom(os.Environ()))
	env = append(env, "PIXI_CACHE_DIR="+c.downloadCache("pixi"), "PIXI_NO_PROGRESS=true", "UV_CACHE_DIR="+c.downloadCache("uv"), "UV_HTTP_TIMEOUT=60", "UV_HTTP_RETRIES=3", "UV_PYTHON_INSTALL_DIR="+filepath.Join(partial, "python"), "UV_PROJECT_ENVIRONMENT="+filepath.Join(partial, "venv"))
	run := c.Run
	if run == nil {
		run = c.runBuild
	} else {
		custom := run
		run = func(ctx context.Context, argv []string, dir string, env []string, stdout, stderr io.Writer) error {
			if err := checkLaunchGate(ctx); err != nil {
				return err
			}
			return custom(ctx, argv, dir, env, stdout, stderr)
		}
	}
	if e = c.pruneDownloads(ctx, partial, env, output, run); e != nil {
		return e
	}
	downloads, e := c.lock(ctx, "downloads", syscall.LOCK_SH)
	if e != nil {
		return e
	}
	defer func() { unlockEnvironment(downloads) }()

	buildCtx, cancelBuild := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	monitorStop := make(chan struct{})
	var budgetErr error
	lastProgress := c.Clock.Now()
	lastSample := lastProgress
	logSize, cacheSize := int64(0), int64(0)
	if st, err := output.Stat(); err == nil {
		logSize = st.Size()
	}
	cacheSize, _ = environmentSize(filepath.Join(c.Root, ".downloads"))
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-monitorStop:
				return
			case <-ticker.C:
				now := c.Clock.Now()
				if now.Sub(lastSample) >= 10*time.Second {
					lastSample = now
					st, statErr := output.Stat()
					n, sizeErr := environmentSize(filepath.Join(c.Root, ".downloads"))
					if errors.Is(sizeErr, os.ErrNotExist) {
						continue // uv atomically renames cache objects while installing.
					}
					if statErr != nil || sizeErr != nil {
						budgetErr = errors.Join(statErr, sizeErr)
					} else if st.Size() > logSize || n > cacheSize {
						lastProgress = now
					} else if now.Sub(lastProgress) >= environmentBuildStallWindow {
						budgetErr = errEnvironmentBuildStalled
					}
					if budgetErr != nil {
						cancelBuild()
						return
					}
					logSize, cacheSize = st.Size(), n
				}
				_, e := os.Stat(partial)
				if errors.Is(e, os.ErrNotExist) {
					continue
				}
				if e == nil {
					var gate *os.File
					gate, e = c.lock(buildCtx, "admission", syscall.LOCK_EX)
					if e == nil {
						e = c.admit(r.Key, 0)
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
	for name, ref := range r.Assets {
		if !safeRelative(name) || c.Fetch == nil {
			return errors.New("asset transport unavailable or invalid asset name")
		}
		if e = checkLaunchGate(buildCtx); e != nil {
			return e
		}
		if e = c.Fetch(buildCtx, ref, filepath.Join(partial, "assets", name)); e != nil {
			return e
		}
	}
	{
		for n, b := range r.Files {
			if r.Binary != "" {
				continue
			}
			if !safeRelative(n) || (n != "uv.lock" && n != "pyproject.toml" && n != "requirements.txt" && n != "pixi.toml" && n != "pixi.lock" && !strings.HasPrefix(n, "extension/")) {
				return errors.New("invalid environment input name")
			}
			if e = os.MkdirAll(filepath.Dir(filepath.Join(partial, n)), 0700); e != nil {
				return e
			}
			if e = os.WriteFile(filepath.Join(partial, n), b, 0600); e != nil {
				return e
			}
		}
		if r.LockFile == "" {
			// Binary/build-only and asset recipes have no package installer.
		} else if r.Backend == "pixi" {
			e = c.installPixi(buildCtx, r, partial, env, output, run)
		} else if _, ok := r.Files["uv.lock"]; ok {
			e = run(buildCtx, []string{"uv", "sync", "--verbose", "--frozen", "--no-dev", "--no-install-project", "--no-editable", "--python", r.Python}, partial, env, output, output)
		} else {
			e = run(buildCtx, []string{"uv", "venv", "--verbose", "--python", r.Python, filepath.Join(partial, "venv")}, partial, env, output, output)
			if e == nil {
				e = run(buildCtx, []string{"uv", "pip", "sync", "--verbose", "--python", filepath.Join(partial, "venv", "bin", "python"), "requirements.txt"}, partial, env, output, output)
			}
		}
	}
	if r.Binary != "" {
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
			argv[i] = strings.NewReplacer("{root}", partial, "{extension}", filepath.Join(partial, "extension")).Replace(a)
		}
		buildEnv := map[string]string{}
		for _, entry := range env {
			k, v, _ := strings.Cut(entry, "=")
			buildEnv[k] = v
		}
		bindEnvironment(buildEnv, partial)
		e = run(buildCtx, argv, partial, cleanExtensionEnv(buildEnv), output, output)
	}
	if e == nil && r.prepare != nil {
		e = r.prepare(buildCtx, partial, output)
	}
	stopMonitor()
	e = errors.Join(e, budgetErr)
	unlockEnvironment(downloads)
	downloads = nil
	if ctx.Err() == nil {
		e = errors.Join(e, c.pruneDownloads(ctx, partial, env, output, run))
	}
	_ = output.Sync()
	if e != nil {
		fmt.Fprintln(output, "environment build failed:", e)
		return fmt.Errorf("environment %s build failed: %w (log %s)", r.Key, e, output.Name())
	}
	report := EnvironmentReport{Key: r.Key, State: "ready", BuildSeconds: c.Clock.Now().Sub(start).Seconds()}
	// Reserve one filesystem block for ready.json before publishing readiness.
	var fs syscall.Statfs_t
	if e = syscall.Statfs(partial, &fs); e != nil {
		return e
	}
	size := int64(fs.Bsize)
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
	if e = checkLaunchGate(ctx); e != nil {
		return e
	}
	if e = saveWorkerJSON(partial, "ready.json", report); e != nil {
		return e
	}
	dir, e := os.Open(c.Root)
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
func (c *EnvironmentCache) cleanupBuild(path string) error {
	if err := CheckStateRoots(c.Root); err != nil {
		return err
	}
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
	return store.cleanupOrphanCustody(c.Clock, true)
}
func (c *EnvironmentCache) runBuild(ctx context.Context, argv []string, dir string, env []string, stdout, stderr io.Writer) error {
	if len(argv) > 0 && (argv[0] == "uv" || argv[0] == "pixi") {
		if err := checkLaunchGate(ctx); err != nil {
			return err
		}
		tool, err := c.acquireEnvironmentTool(ctx, argv[0])
		if err != nil {
			return err
		}
		argv = append([]string(nil), argv...)
		argv[0] = tool
	}
	return runRecordedCommand(ctx, argv, dir, env, stdout, commandScope{StateDir: dir, Clock: c.Clock, Stderr: stderr}, "build-state.json")
}

type environmentRuntime struct {
	authorize func(HostLaunch) error
	dir       string
	cache     *EnvironmentCache
	prepare   func(context.Context, EnvironmentRecipe) error
	mu        sync.Mutex
	reports   map[string]EnvironmentReport
	offsets   map[string]int64
	cancel    context.CancelFunc
	done      chan struct{}
	key       string
}

func (rt *environmentRuntime) sync(plan []EnvironmentRecipe, busy bool, keepActive ...bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	wanted := ""
	if !busy && len(plan) > 0 {
		wanted = plan[0].Key
	}
	if rt.cancel != nil {
		if len(keepActive) > 0 && keepActive[0] {
			return
		}
		if wanted != rt.key {
			rt.cancel()
		}
		return
	}
	if wanted == "" {
		return
	}
	if r := rt.reports[wanted]; r.State == "failed" || (r.State == "ready" && r.Attempt == launchAttempt(plan[0].Launch)) {
		// Keep an exact completion visible until the queue withdraws its offer.
		// A stale heartbeat response must not restart completed preparation.
		return
	}
	r := plan[0]
	prepare := rt.prepare
	ctx, cancel := context.WithCancel(context.Background())
	if rt.authorize != nil {
		ctx = withLaunchGate(ctx, func() error {
			if r.Launch == nil {
				return ErrStale
			}
			return rt.authorize(*r.Launch)
		})
	}
	rt.cancel = cancel
	rt.done = make(chan struct{})
	rt.key = r.Key
	kind := ""
	if r.Preparation != nil {
		kind = "extension-setup"
	}
	rt.reports[r.Key] = EnvironmentReport{Key: r.Key, State: "building", Kind: kind, Attempt: launchAttempt(r.Launch)}
	if e := saveWorkerJSON(rt.dir, "environment-reports.json", rt.reports); e != nil {
		log.Printf("persist environment build intent: %v", e)
	}
	go func() {
		var release func()
		e := checkLaunchGate(ctx)
		if e == nil {
			if r.Preparation != nil {
				if prepare == nil {
					e = errors.New("extension preparation unavailable")
				} else {
					e = prepare(ctx, r)
				}
			} else {
				_, release, e = rt.cache.Acquire(ctx, r)
			}
		}
		if release != nil {
			release()
		}
		report := EnvironmentReport{Key: r.Key, State: "ready", Kind: kind}
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
		report.Attempt = launchAttempt(r.Launch)
		if errors.Is(e, errWorkerStartAuthorization) {
			report.State, report.Error = "paused", ""
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
		for _, j := range q.liveJobs() {
			if j.State != "queued" || !q.placementAllowed(j, m) || environmentKey(j, m) != r.Key {
				continue
			}
			for _, candidate := range q.memoryCandidates(j, m) {
				if !hostFits(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) || !fitsResources(candidate, m, m.FreeMemoryGB) {
					continue
				}
				j.State = "failed"
				j.Reason = "environment build failed on " + m.ID + ": " + r.Error
				if e := q.copyEnvironmentLogs(j, m, r.Key); e != nil {
					return e
				}
				if j.Spec.Logs != "none" {
					j.EnvironmentFailureLog = r.Log + "\n" + j.Reason + "\n"
				}
				break
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
func (q *Queue) AppendEnvironmentLog(l EnvironmentLog, requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestLock(requestCtx, &q.mu)
	defer requestUnlock1()
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
	for _, j := range q.liveJobs() {
		if j.State == "queued" && environmentKey(j, m) == l.Key {
			if e = insertEnvironmentJobLog(tx, j, l); e != nil {
				return e
			}
		}
	}
	return tx.Commit()
}
func insertEnvironmentJobLog(tx *sql.Tx, j *Job, l EnvironmentLog) error {
	if j.Spec.Logs == "none" {
		return nil
	}
	id := j.ID
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
	if j.Spec.Logs == "none" {
		return nil
	}
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
		if e = insertEnvironmentJobLog(tx, j, l); e != nil {
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
		// s007 and older stored only the diagnostic. Migrate that exact capacity
		// refusal too; other failures remain negatively cached across restarts.
		var needed, limit int64
		// Acquire wraps admission failures with the build error and log path.
		if at := strings.Index(r.Error, "environment cache needs "); at >= 0 {
			if n, err := fmt.Sscanf(r.Error[at:], "environment cache needs %d bytes; limit %d (in-use entries protected)", &needed, &limit); n != 2 || err != nil {
				limit = 0
			}
		}
		if r.State == "failed" && limit > 0 && limit != rt.cache.Limit {
			r.State, r.Error = "paused", ""
			rt.reports[key] = r
		}
		if r.State == "building" {
			r.State = "paused"
			r.Error = ""
			rt.reports[key] = r
		}
	}
	return nil
}
