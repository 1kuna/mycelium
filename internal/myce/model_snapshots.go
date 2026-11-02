package myce

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
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
// large a snapshot copy may be. Values are deployment settings, never defaults.
type ModelPolicy struct {
	Sources        []string `json:"sources" yaml:"sources"`
	MaxBytes       int64    `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
	FreeFloorBytes int64    `json:"free_floor_bytes,omitempty" yaml:"free_floor_bytes,omitempty"`
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
	dockerModelRoot          = "/myce/models"
)

var (
	modelRepoPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$`)
	modelCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sha256Name         = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Fixed, path-free refusal reasons; status may publish them verbatim.
var modelSnapshotMessages = map[string]string{
	"model_snapshot_missing":   "declared model repository is not in the shared model root; acquire it with a CPU download job (see the mycelium skill), then resubmit",
	"model_snapshot_revision":  "the shared model root has this repository but not the declared revision; download that exact commit with a CPU download job, then resubmit",
	"model_snapshot_mismatch":  "declared model snapshot files are missing or failed size or hash verification",
	"model_snapshot_escape":    "declared model snapshot links outside its repository blob store",
	"model_snapshot_too_large": "declared model snapshot exceeds the local model size limit or free-space floor",
	"model_snapshot_policy":    "declared model snapshot has no admitting local model policy",
}

func modelSnapshotError(predicate, class string, cause error) error {
	if cause == nil {
		cause = errors.New(modelSnapshotMessages[predicate])
	}
	return &runtimeError{diagnostic: RuntimeCause{Operation: "input_staging", Class: class, Predicate: predicate}, cause: cause}
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
	if len(p.Sources) == 0 || p.MaxBytes < 0 || p.FreeFloorBytes < 0 {
		return errors.New("models policy requires source patterns and nonnegative byte limits")
	}
	for _, source := range p.Sources {
		if _, err := path.Match(source, ""); err != nil || strings.Count(source, "/") != 1 {
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
	names := make([]string, 0, len(s.ModelSnapshots))
	for name := range s.ModelSnapshots {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		repo := s.ModelSnapshots[name].Repo
		permitted := false
		for _, source := range sourcesOf(p) {
			ok, _ := path.Match(source, repo)
			permitted = permitted || ok
		}
		if !permitted {
			return nil, &HTTPError{Status: 409, Code: "model_snapshot_not_permitted", Message: fmt.Sprintf("model snapshot %s: repository %s is not permitted by local model policy; the queue owner adds a models.sources pattern with myce policy set", name, repo)}
		}
	}
	return &ModelLimits{PolicyRevision: q.policyRevision(), MaxBytes: p.MaxBytes, FreeFloorBytes: p.FreeFloorBytes}, nil
}

func sourcesOf(p *ModelPolicy) []string {
	if p == nil {
		return nil
	}
	return p.Sources
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
		source := p
		if d.Type()&fs.ModeSymlink != 0 {
			if source, err = filepath.EvalSymlinks(p); err != nil || !insideLocalRoot(blobs, source) {
				return modelSnapshotError("model_snapshot_escape", "permission_denied", err)
			}
		} else if !d.Type().IsRegular() {
			return modelSnapshotError("model_snapshot_mismatch", "io_error", nil)
		}
		ref, err := modelBlobIdentity(ctx, source)
		if err != nil {
			return err
		}
		manifest[rel] = ref
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
	return SharedInput{NAS: "snapshots/" + m.Revision, Manifest: manifest}, repo, nil
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

// acquireModelSnapshots copies each declared snapshot into the shared-input
// cache, which verifies every byte, makes the copy read-only and holds a use lock
// until exact attempt cleanup. Paths are keyed by the job's environment names.
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
	if w.sharedInputs == nil {
		return fail(errors.New("shared input cache unavailable"))
	}
	if j.ModelLimits == nil {
		return fail(modelSnapshotError("model_snapshot_policy", "permission_denied", nil))
	}
	for name, m := range j.Spec.ModelSnapshots {
		input, repo, err := resolveModelSnapshot(ctx, w.ModelRoot, m)
		if err != nil {
			return fail(err)
		}
		need := input.bytes()
		if limit := j.ModelLimits.MaxBytes; limit > 0 && need > limit {
			return fail(modelSnapshotError("model_snapshot_too_large", "storage_full", nil))
		}
		if _, err := os.Stat(filepath.Join(w.sharedInputs.cache.Root, input.key())); err != nil && j.ModelLimits.FreeFloorBytes > 0 {
			disk, e := workerDiskProbe(w.sharedInputs.cache.Root)
			if e != nil || disk.FreeBytes < uint64(need) || disk.FreeBytes-uint64(need) < uint64(j.ModelLimits.FreeFloorBytes) {
				return fail(modelSnapshotError("model_snapshot_too_large", "storage_full", e))
			}
		}
		path, done, err := w.sharedInputs.Acquire(ctx, input, repo)
		if err != nil {
			if errors.Is(err, errCheckpointIOPending) || ctx.Err() != nil {
				return fail(err)
			}
			if strings.HasPrefix(err.Error(), "disk_headroom") {
				return fail(modelSnapshotError("model_snapshot_too_large", "storage_full", err))
			}
			return fail(modelSnapshotError("model_snapshot_mismatch", "io_error", err))
		}
		releases = append(releases, done)
		paths["MYCE_MODEL_"+strings.ToUpper(name)] = path
	}
	return paths, releases, nil
}

// Docker sees the same verified directory at a fixed container path.
func dockerModelMounts(j Job, paths map[string]string) []dockerFileMount {
	var mounts []dockerFileMount
	for name := range j.Spec.ModelSnapshots {
		key := "MYCE_MODEL_" + strings.ToUpper(name)
		if source := paths[key]; source != "" {
			mounts = append(mounts, dockerFileMount{Type: "bind", Source: source, Destination: dockerModelRoot + "/" + name})
		}
	}
	slices.SortFunc(mounts, func(a, b dockerFileMount) int { return strings.Compare(a.Destination, b.Destination) })
	return mounts
}

func init() {
	rejectionMessages["model_snapshot_not_permitted"] = "declared model repository is not permitted by local model policy"
	for predicate, message := range modelSnapshotMessages {
		launchPredicateMessages[predicate] = message
	}
}
