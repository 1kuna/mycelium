package myce

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

// ModelSnapshot declares one exact Hugging Face snapshot. Workers resolve it in
// the hub cache under their configured shared model root; authors never supply
// host paths, and the job receives a verified read-only copy.
type ModelSnapshot struct {
	Repo     string   `json:"repo" yaml:"repo"`
	Revision string   `json:"revision" yaml:"revision"`
	Files    []string `json:"files,omitempty" yaml:"files,omitempty"`
}

// ModelPolicy is local queue policy: which repositories may be used and how
// large a snapshot copy may be. Absent source restrictions permit every repository.
type ModelPolicy struct {
	Sources  []string `json:"sources,omitempty" yaml:"sources,omitempty"`
	MaxBytes int64    `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
	// Zero derives the reserve from the worker cache volume (diskFreeFloor).
	FreeFloorBytes int64 `json:"free_floor_bytes,omitempty" yaml:"free_floor_bytes,omitempty"`
}

// ModelLimits freezes the policy that admitted a job, so a worker enforces the
// same limits the submitter was shown.
type ModelLimits struct {
	PolicyRevision uint64 `json:"policy_revision"`
	MaxBytes       int64  `json:"max_bytes,omitempty"`
	FreeFloorBytes int64  `json:"free_floor_bytes,omitempty"`
}

const (
	modelSnapshotsCapability = "model_snapshots_v1"
	modelHubDirectory        = "hub"
	defaultModelCacheBytes   = 100 << 30
)

var (
	modelRepoPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$`)
	modelCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sha256Name         = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Fixed, path-free refusal reasons; status may publish them verbatim.
var modelSnapshotMessages = map[string]string{
	"model_snapshot_missing":   "declared model repository was found neither in this host's model cache nor on the model hub",
	"model_snapshot_revision":  "the model hub has no such repository revision; declare an existing full commit (private repositories need the hub token on workers)",
	"model_snapshot_access":    "the model hub refused access: the repository is gated, private or does not exist; gated and private repositories need the hub token provisioned as local secret hf_token on workers",
	"model_snapshot_mismatch":  "declared model snapshot files are missing or failed size or hash verification",
	"model_snapshot_escape":    "declared model snapshot links outside its repository blob store",
	"model_snapshot_too_large": "declared model snapshot exceeds the local model size limit",
	"model_snapshot_policy":    "declared model snapshot has no admitting local model policy",
}

func modelSnapshotError(predicate, class string, cause error) error {
	// A failed filesystem operation says nothing about content identity. Keep
	// its errno/deadline and staging trace instead of manufacturing a mismatch.
	if modelSnapshotIOFailure(cause) {
		return runtimeErrorFor("input_staging", withStagingCause("", FileRef{}, "model_snapshot", cause))
	}
	if cause == nil {
		cause = errors.New(modelSnapshotMessages[predicate])
	}
	if predicate == "model_snapshot_mismatch" {
		class = "invalid_input"
	}
	return &runtimeError{diagnostic: RuntimeCause{Operation: "input_staging", Class: class, Predicate: predicate}, cause: cause}
}

func modelSnapshotIOFailure(err error) bool {
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		return false
	}
	var errno syscall.Errno
	var network net.Error
	return errors.As(err, &errno) || errors.As(err, &network) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, errCheckpointIOTimeout) || errors.Is(err, errFileTransferStalled) ||
		errors.Is(err, errInputTransferUnreachable)
}

func sharedReaders(s Spec) bool { return len(s.SharedInputs)+len(s.ModelSnapshots) > 0 }

func validateModelSnapshots(s Spec) error {
	if len(s.ModelSnapshots) == 0 {
		return nil
	}
	if s.extensionJob() {
		return errors.New("model snapshots require a plain batch command")
	}
	for name, m := range s.ModelSnapshots {
		if name == "" || strings.Trim(name, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_") != "" {
			return errors.New("model snapshot names must use letters, digits and underscores")
		}
		for other := range s.ModelSnapshots {
			if other != name && strings.EqualFold(name, other) {
				return errors.New("model snapshot environment names collide")
			}
		}
		if !modelRepoPattern.MatchString(m.Repo) || strings.Contains(m.Repo, "--") || strings.Contains(m.Repo, "..") {
			return fmt.Errorf("model snapshot %s: repo must be OWNER/NAME", name)
		}
		if !modelCommitPattern.MatchString(m.Revision) {
			return fmt.Errorf("model snapshot %s: revision must be the full 40-character lowercase commit; branch and tag names can move", name)
		}
		seen := map[string]bool{}
		for _, f := range m.Files {
			if !safeRelative(f) || seen[f] {
				return fmt.Errorf("model snapshot %s: files must be unique clean relative paths", name)
			}
			seen[f] = true
		}
	}
	return nil
}

func (p *ModelPolicy) validate() error {
	if p == nil {
		return nil
	}
	if p.MaxBytes < 0 || p.FreeFloorBytes < 0 {
		return errors.New("models policy requires nonnegative byte limits")
	}
	for _, source := range p.Sources {
		pattern := strings.TrimPrefix(source, "!")
		if _, err := path.Match(pattern, ""); err != nil || strings.Count(pattern, "/") != 1 || strings.HasPrefix(pattern, "/") || strings.HasSuffix(pattern, "/") {
			return fmt.Errorf("models source %q must be an OWNER/NAME pattern", source)
		}
	}
	return nil
}

// modelLimits applies local policy at submission, inside the queue transaction.
// It reads only in-memory policy; workers do every filesystem check.
func (q *Queue) modelLimits(s Spec) (*ModelLimits, error) {
	if len(s.ModelSnapshots) == 0 {
		return nil, nil
	}
	p := q.effectivePriorityPolicy().Models
	if p == nil {
		p = &ModelPolicy{}
	}
	names := make([]string, 0, len(s.ModelSnapshots))
	for name := range s.ModelSnapshots {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		repo := s.ModelSnapshots[name].Repo
		restricted, permitted, denied := false, false, false
		for _, source := range p.Sources {
			pattern, deny := strings.CutPrefix(source, "!")
			ok, _ := path.Match(pattern, repo)
			if deny {
				denied = denied || ok
			} else {
				restricted = true
				permitted = permitted || ok
			}
		}
		permitted = !denied && (!restricted || permitted)
		if !permitted {
			return nil, &HTTPError{Status: 409, Code: "model_snapshot_not_permitted", Message: fmt.Sprintf("model snapshot %s: repository %s is not permitted by explicit models.sources restrictions", name, repo)}
		}
	}
	return &ModelLimits{PolicyRevision: q.policyRevision(), MaxBytes: p.MaxBytes, FreeFloorBytes: p.FreeFloorBytes}, nil
}

// resolveModelSnapshot runs on the worker, outside any queue lock. It returns a
// tree input whose manifest names every declared file and its content hash.
// Hub snapshot files are links into the repository's blob store; any other link
// target, non-regular file or changed blob withholds the job.
func resolveModelSnapshot(ctx context.Context, modelRoot string, m ModelSnapshot) (SharedInput, string, error) {
	if modelRoot == "" {
		return SharedInput{}, "", modelSnapshotError("model_snapshot_missing", "not_found", nil)
	}
	repo, err := filepath.EvalSymlinks(filepath.Join(modelRoot, modelHubDirectory, "models--"+strings.ReplaceAll(m.Repo, "/", "--")))
	if err != nil {
		return SharedInput{}, "", modelSnapshotError("model_snapshot_missing", "not_found", err)
	}
	snapshot := filepath.Join(repo, "snapshots", m.Revision)
	if info, err := os.Lstat(snapshot); err != nil || !info.IsDir() {
		return SharedInput{}, "", modelSnapshotError("model_snapshot_revision", "not_found", err)
	}
	blobs := filepath.Join(repo, "blobs")
	allowed, missing := map[string]bool{}, map[string]bool{}
	for _, f := range m.Files {
		allowed[f], missing[f] = true, true
	}
	manifest := FileManifest{}
	sources := map[string]string{}
	err = filepath.WalkDir(snapshot, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(snapshot, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if len(allowed) > 0 && !allowed[rel] {
			return nil
		}
		if d.Type()&fs.ModeSymlink == 0 && !d.Type().IsRegular() {
			return modelSnapshotError("model_snapshot_mismatch", "io_error", nil)
		}
		source, err := modelSnapshotSource(ctx, p, blobs)
		if err != nil {
			return err
		}
		ref, err := modelBlobIdentity(ctx, source)
		if err != nil {
			return err
		}
		manifest[rel] = ref
		sources[rel] = source
		delete(missing, rel)
		return nil
	})
	if err != nil {
		var typed *runtimeError
		if errors.As(err, &typed) || ctx.Err() != nil {
			return SharedInput{}, "", err
		}
		return SharedInput{}, "", modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	if len(missing) > 0 || len(manifest) == 0 {
		return SharedInput{}, "", modelSnapshotError("model_snapshot_mismatch", "not_found", nil)
	}
	return SharedInput{NAS: "snapshots/" + m.Revision, Manifest: manifest, sources: sources}, repo, nil
}

// Hub blobs are named by their identity: SHA-256 for LFS files and the Git blob
// SHA-1 for small files. LFS blobs are verified while copying; other files are
// hashed here and must match their Git name when they have one.
func modelBlobIdentity(ctx context.Context, source string) (FileRef, error) {
	info, err := os.Stat(source)
	if err != nil || !info.Mode().IsRegular() {
		return FileRef{}, modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	name := filepath.Base(source)
	if sha256Name.MatchString(name) {
		return FileRef{SHA256: name, SizeBytes: info.Size()}, nil
	}
	f, err := os.Open(source)
	if err != nil {
		return FileRef{}, modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	defer f.Close()
	content, git := sha256.New(), sha1.New()
	io.WriteString(git, "blob "+strconv.FormatInt(info.Size(), 10)+"\x00")
	n, err := io.Copy(io.MultiWriter(content, git), contextReader{ctx, f})
	if err != nil || n != info.Size() {
		return FileRef{}, modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	if modelCommitPattern.MatchString(name) && hex.EncodeToString(git.Sum(nil)) != name {
		return FileRef{}, modelSnapshotError("model_snapshot_mismatch", "io_error", nil)
	}
	return FileRef{SHA256: hex.EncodeToString(content.Sum(nil)), SizeBytes: n}, nil
}

// acquireModelSnapshots copies (or clones, on a copy-on-write volume) each
// declared snapshot into the model snapshot cache, which verifies every byte,
// makes the copy read-only and holds a use lock until exact attempt cleanup.
// Paths are keyed by the job's environment names.
func (w Worker) acquireModelSnapshots(ctx context.Context, j Job) (map[string]string, []func(), error) {
	paths := map[string]string{}
	var releases []func()
	fail := func(err error) (map[string]string, []func(), error) {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
		return nil, nil, err
	}
	if len(j.Spec.ModelSnapshots) == 0 {
		return paths, nil, nil
	}
	if w.modelSnapshots == nil {
		return fail(errors.New("model snapshot cache unavailable"))
	}
	if j.ModelLimits == nil {
		return fail(modelSnapshotError("model_snapshot_policy", "permission_denied", nil))
	}
	// Concurrent acquisitions inspect Root for capacity before Acquire. Finish
	// its once-only canonicalization before any reader observes that field.
	if err := w.modelSnapshots.cache.init(); err != nil {
		return fail(modelSnapshotError("model_snapshot_mismatch", "io_error", err))
	}
	for name, m := range j.Spec.ModelSnapshots {
		path, done, err := w.acquireModelSnapshot(ctx, j, m)
		if err != nil {
			return fail(err)
		}
		releases = append(releases, done)
		paths["MYCE_MODEL_"+strings.ToUpper(name)] = path
	}
	return paths, releases, nil
}

// acquireModelSnapshot makes one declared snapshot present in this host's
// model snapshot cache: an entry it already holds (no download, no source
// read), else the local model root's hub, else the model hub. One lock per
// declaration makes concurrent jobs share a single download.
func (w Worker) acquireModelSnapshot(ctx context.Context, j Job, m ModelSnapshot) (path string, done func(), err error) {
	c := w.modelSnapshots
	id := snapshotIdentity(m)
	err = c.withHubLock(ctx, id, func(scratch string) error {
		var held bool
		if path, done, held, err = w.acquireHeldSnapshot(ctx, j, id, m); err != nil || held {
			return err
		}
		var input SharedInput
		var root string
		var resolveErr error = modelSnapshotError("model_snapshot_missing", "not_found", nil)
		if w.ModelRoot != "" && !slices.ContainsFunc(w.Machine.UnavailableRoots, func(r UnavailableRoot) bool { return r.Root == "model" }) {
			input, root, resolveErr = resolveModelSnapshot(ctx, w.ModelRoot, m)
		}
		if missingLocally(resolveErr) {
			hub, e := w.modelHub()
			if e != nil {
				return modelHubHostFault("io_error", e)
			}
			root = ""
			input, resolveErr = w.resolveHubSnapshot(ctx, hub, m, scratch)
		}
		if resolveErr != nil {
			return resolveErr
		}
		input.model = &m
		// Learned before admission, so a host that refuses for disk reports
		// the size and the queue charges it on the next placement.
		w.modelIndex.learn(id, snapshotBytes(input))
		if path, done, err = w.acquireResolvedSnapshot(ctx, j, input, root); err == nil {
			w.modelIndex.record(c.cache.Root, m, input)
		}
		return err
	})
	return path, done, err
}

func missingLocally(err error) bool {
	var typed *runtimeError
	return errors.As(err, &typed) && (typed.diagnostic.Predicate == "model_snapshot_missing" || typed.diagnostic.Predicate == "model_snapshot_revision")
}

// acquireHeldSnapshot reuses an entry this worker recorded for the same
// declaration. Its saved recovery source keeps eviction able to re-obtain it;
// if the entry was evicted meanwhile, the caller resolves it again.
func (w Worker) acquireHeldSnapshot(ctx context.Context, j Job, id string, m ModelSnapshot) (string, func(), bool, error) {
	stored, ok := w.modelIndex.lookup(id)
	if !ok {
		return "", nil, false, nil
	}
	key := stored.key()
	var entry sharedEntry
	b, err := os.ReadFile(filepath.Join(w.modelSnapshots.cache.Root, key, "ready.json"))
	if err != nil || json.Unmarshal(b, &entry) != nil || entry.Recovery == nil || entry.Recovery.Input.key() != key {
		return "", nil, false, nil
	}
	r := entry.Recovery
	s := r.Input
	s.sources, s.model, s.hub = r.Sources, &m, r.Hub
	if s.hub {
		s.fetch = func(context.Context, string, string, FileRef) error { return errModelIndexStale }
	}
	path, done, err := w.acquireResolvedSnapshot(ctx, j, s, r.Root)
	if errors.Is(err, errModelIndexStale) {
		return "", nil, false, nil
	}
	return path, done, err == nil, err
}

// acquireResolvedSnapshot admits and copies (or reuses) one resolved snapshot.
// The size limit is policy and fails the job; a disk shortage is this host's,
// so it requeues the job without a predicate and the queue, which has now
// learned the size, places it on a host with room.
func (w Worker) acquireResolvedSnapshot(ctx context.Context, j Job, input SharedInput, root string) (string, func(), error) {
	need := input.bytes()
	if limit := j.ModelLimits.MaxBytes; limit > 0 && need > limit {
		return "", nil, modelSnapshotError("model_snapshot_too_large", "storage_full", nil)
	}
	// Zero uses the cache's volume-derived disk floor during admission,
	// after inactive entries have had a chance to be evicted.
	if _, err := os.Stat(filepath.Join(w.modelSnapshots.cache.Root, input.key())); err != nil && j.ModelLimits.FreeFloorBytes > 0 && w.modelSnapshots.budget == nil {
		disk, e := workerDiskProbe(w.modelSnapshots.cache.Root)
		if e != nil || disk.FreeBytes < uint64(need) || disk.FreeBytes-uint64(need) < uint64(j.ModelLimits.FreeFloorBytes) {
			return "", nil, modelSnapshotDiskFault(e)
		}
	}
	path, done, err := w.modelSnapshots.Acquire(context.WithValue(ctx, diskBudgetFloorKey{}, uint64(max(0, j.ModelLimits.FreeFloorBytes))), input, root)
	if err != nil {
		var typed *runtimeError
		if errors.Is(err, errCheckpointIOPending) || errors.Is(err, errModelIndexStale) || ctx.Err() != nil || errors.As(err, &typed) {
			return "", nil, err
		}
		var capacity *stagingCapacityError
		if errors.As(err, &capacity) || strings.HasPrefix(err.Error(), "disk_headroom") || errors.Is(err, syscall.ENOSPC) {
			return "", nil, modelSnapshotDiskFault(err)
		}
		return "", nil, modelSnapshotError("model_snapshot_mismatch", "io_error", err)
	}
	return path, done, nil
}

func modelSnapshotDiskFault(cause error) error {
	if cause == nil {
		cause = errors.New("model snapshot cache lacks room above the free-space floor")
	}
	return &runtimeError{diagnostic: RuntimeCause{Operation: "input_staging", Class: "storage_full"}, cause: cause}
}

// nativeInputPaths keeps model data separate from executable shared runtimes.
// Only acquired paths get grants; run.env cannot expand the boundary.
func (w *Worker) nativeInputPaths(j Job, paths map[string]string) {
	w.nativeSharedInputs, w.nativeModelInputs = nil, nil
	for name := range j.Spec.SharedInputs {
		if path := paths["MYCE_INPUT_"+strings.ToUpper(name)]; path != "" {
			w.nativeSharedInputs = append(w.nativeSharedInputs, path)
		}
	}
	for name := range j.Spec.ModelSnapshots {
		if path := paths["MYCE_MODEL_"+strings.ToUpper(name)]; path != "" {
			w.nativeModelInputs = append(w.nativeModelInputs, path)
		}
	}
	slices.Sort(w.nativeSharedInputs)
	slices.Sort(w.nativeModelInputs)
}

func init() {
	rejectionMessages["model_snapshot_not_permitted"] = "declared model repository is not permitted by local model policy"
	for predicate, message := range modelSnapshotMessages {
		launchPredicateMessages[predicate] = message
	}
}
