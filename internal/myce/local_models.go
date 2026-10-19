package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"
)

type localModelMetadata struct {
	Path         string
	Target       string
	Size         int64
	Mtime        int64
	Mode         uint32
	Device       uint64
	Inode        uint64
	TargetMode   uint32
	TargetSize   int64
	TargetMtime  int64
	TargetDevice uint64
	TargetInode  uint64
}
type localModelApproval struct {
	Host     string
	Path     string
	Root     string
	Files    []ModelFile
	Metadata []localModelMetadata
}

var errLocalApproval = errors.New("local model unavailable or changed; explicit approval on this host required")

func insideLocalRoot(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !filepath.IsAbs(rel) && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
func localMetadata(ctx context.Context, path, root string) ([]localModelMetadata, error) {
	var out []localModelMetadata
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path || !insideLocalRoot(root, path) {
		return nil, errLocalApproval
	}
	err = filepath.WalkDir(path, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errLocalApproval
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		st, err := os.Lstat(p)
		if err != nil {
			return errLocalApproval
		}
		sys, ok := st.Sys().(*syscall.Stat_t)
		if !ok {
			return errLocalApproval
		}
		rel, err := filepath.Rel(path, p)
		if err != nil {
			return errLocalApproval
		}
		meta := localModelMetadata{Path: filepath.ToSlash(rel), Size: st.Size(), Mtime: st.ModTime().UnixNano(), Mode: uint32(st.Mode()), Device: uint64(sys.Dev), Inode: uint64(sys.Ino)}
		if st.Mode()&os.ModeSymlink != 0 {
			target, err := filepath.EvalSymlinks(p)
			if err != nil || !insideLocalRoot(root, target) {
				return errLocalApproval
			}
			targetInfo, err := os.Stat(target)
			if err != nil || !targetInfo.Mode().IsRegular() {
				return errLocalApproval
			}
			targetSys, ok := targetInfo.Sys().(*syscall.Stat_t)
			if !ok {
				return errLocalApproval
			}
			meta.Target, meta.TargetSize, meta.TargetMtime = target, targetInfo.Size(), targetInfo.ModTime().UnixNano()
			meta.TargetDevice, meta.TargetInode = uint64(targetSys.Dev), uint64(targetSys.Ino)
			meta.TargetMode = uint32(targetInfo.Mode())
		} else if !st.IsDir() && !st.Mode().IsRegular() {
			return errLocalApproval
		}
		out = append(out, meta)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ApproveLocalModel is an explicit local operator action; it hashes once and
// records the original path. root bounds symlink targets (e.g. HF blobs).
// The returned Model has only a portable manifest, never local paths/hostnames.
func ApproveLocalModel(ctx context.Context, approvals, name, path, root string) (Model, error) {
	if approvals == "" || !localName(name) || !filepath.IsAbs(path) || !filepath.IsAbs(root) {
		return Model{}, errLocalApproval
	}
	path, err := filepath.EvalSymlinks(path)
	if err != nil {
		return Model{}, errLocalApproval
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return Model{}, errLocalApproval
	}
	st, err := os.Stat(path)
	if err != nil || !st.IsDir() {
		return Model{}, errLocalApproval
	}
	before, err := localMetadata(ctx, path, root)
	if err != nil {
		return Model{}, err
	}
	var files []ModelFile
	for _, meta := range before {
		if os.FileMode(meta.Mode).IsDir() {
			continue
		}
		f, err := os.Open(filepath.Join(path, filepath.FromSlash(meta.Path)))
		if err != nil {
			return Model{}, errLocalApproval
		}
		hash := sha256.New()
		n, err := io.Copy(hash, contextReader{ctx, f})
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			return Model{}, errLocalApproval
		}
		files = append(files, ModelFile{Path: meta.Path, SizeBytes: n, SHA256: hex.EncodeToString(hash.Sum(nil))})
	}
	after, err := localMetadata(ctx, path, root)
	if err != nil || !reflect.DeepEqual(before, after) {
		return Model{}, errLocalApproval
	}
	size, hash, err := ModelFilesIdentity(files)
	if err != nil {
		return Model{}, err
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		return Model{}, errLocalApproval
	}
	approval := localModelApproval{Host: host, Path: path, Root: root, Files: files, Metadata: after}
	if err := os.MkdirAll(approvals, 0700); err != nil {
		return Model{}, errLocalApproval
	}
	if !privateLocalDirectory(approvals) {
		return Model{}, errLocalApproval
	}
	if err := saveWorkerJSON(approvals, name+".json", approval); err != nil {
		return Model{}, errLocalApproval
	}
	return Model{LocalSource: name, Files: files, SizeBytes: size, SHA256: hash}, nil
}
func (c *ModelCache) prepareLocal(ctx context.Context, m Model) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	approval, err := c.readLocalApproval(m)
	if err != nil {
		return "", err
	}
	if err := c.checkLocalMetadata(ctx, approval); err != nil {
		return "", err
	}
	return approval.Path, nil
}

// Local records are checked for every source before any directory traversal.
func (c *ModelCache) readLocalApproval(m Model) (localModelApproval, error) {
	if !privateLocalDirectory(c.ApprovalRoot) || !localName(m.LocalSource) || m.FileStore || m.DirectNAS || m.Path != "" {
		return localModelApproval{}, errLocalApproval
	}
	record := filepath.Join(c.ApprovalRoot, m.LocalSource+".json")
	info, err := os.Lstat(record)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return localModelApproval{}, errLocalApproval
	}
	data, err := os.ReadFile(record)
	if err != nil {
		return localModelApproval{}, errLocalApproval
	}
	var approval localModelApproval
	if json.Unmarshal(data, &approval) != nil {
		return localModelApproval{}, errLocalApproval
	}
	host, err := os.Hostname()
	if err != nil || host != approval.Host {
		return localModelApproval{}, errLocalApproval
	}
	size, hash, err := ModelFilesIdentity(approval.Files)
	if err != nil || size != m.SizeBytes || hash != m.SHA256 || validateModelFiles(m) != nil {
		return localModelApproval{}, errLocalApproval
	}
	return approval, nil
}

func (c *ModelCache) checkLocalMetadata(ctx context.Context, approval localModelApproval) error {
	metadata := localMetadata
	if c.localMetadataProbe != nil {
		metadata = c.localMetadataProbe
	}
	current, err := metadata(ctx, approval.Path, approval.Root)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || !reflect.DeepEqual(approval.Metadata, current) {
		return errLocalApproval
	}
	return nil
}

// Metadata syscalls can outlive context cancellation. Reuse the bounded I/O
// helper to retain at most one outstanding scan, including after a timeout.
const localApprovalTimeout = 250 * time.Millisecond

// ModelApprovalState describes the last bounded check of a local source.
// Omitted states cannot authorize local-source use.
type ModelApprovalState string

const (
	approvalVerified ModelApprovalState = "verified"
	approvalInvalid  ModelApprovalState = "invalid"
	approvalUnknown  ModelApprovalState = "unknown"
)

type localApprovalSnapshot struct {
	ctx context.Context
}

type localApprovalRecords struct {
	approvals map[string]localModelApproval
	states    map[string]ModelApprovalState
	ctx       context.Context
}

type localApprovalResult struct {
	round uint64
	state ModelApprovalState
	hash  string
}

func (c *ModelCache) ApprovedModels(models []Model) map[string]string {
	approved, _ := c.modelApprovals(models)
	return approved
}

func (c *ModelCache) modelApprovals(models []Model) (map[string]string, map[string]ModelApprovalState) {
	c.localCheckMu.Lock()
	defer c.localCheckMu.Unlock()
	states := map[string]ModelApprovalState{}
	for _, m := range models {
		if m.LocalSource != "" {
			states[m.LocalSource] = approvalUnknown
		}
	}
	if len(states) == 0 {
		return nil, nil
	}
	c.localProbeInit.Do(func() { c.localProbe.timeout = localApprovalTimeout; c.localRecordProbe.timeout = localApprovalTimeout })
	c.localApprovalRound++
	round := c.localApprovalRound
	key := modelConfigHash(ModelConfig{Models: models})
	// This bounded phase runs every heartbeat, even while an older walk is stuck.
	recordCtx, cancelRecords := context.WithTimeout(context.Background(), localApprovalTimeout)
	defer cancelRecords()
	records, err := c.localRecordProbe.run(key, func() (localApprovalRecords, error) {
		out := localApprovalRecords{approvals: map[string]localModelApproval{}, states: map[string]ModelApprovalState{}, ctx: recordCtx}
		for _, m := range models {
			if m.LocalSource == "" {
				continue
			}
			if err := recordCtx.Err(); err != nil {
				return out, err
			}
			identity := m.LocalSource + ":" + m.SHA256
			if _, ok := out.approvals[identity]; ok {
				continue
			}
			approval, err := c.readLocalApproval(m)
			if err != nil {
				out.states[m.LocalSource] = approvalInvalid
			} else {
				out.approvals[identity] = approval
				if out.states[m.LocalSource] != approvalInvalid {
					out.states[m.LocalSource] = approvalUnknown
				}
			}
		}
		return out, recordCtx.Err()
	})
	if err != nil || records.ctx == nil || records.ctx.Err() != nil {
		c.warnLocalApproval()
		return nil, states
	}
	states = maps.Clone(records.states)
	needsWalk := false
	for _, state := range states {
		needsWalk = needsWalk || state == approvalUnknown
	}
	if !needsWalk {
		return nil, states
	}
	ctx, cancel := context.WithTimeout(context.Background(), localApprovalTimeout)
	defer cancel()
	scan, err := c.localProbe.run(key, func() (localApprovalSnapshot, error) {
		checked := map[string]bool{}
		for _, m := range models {
			if m.LocalSource == "" || records.states[m.LocalSource] == approvalInvalid {
				continue
			}
			identity := m.LocalSource + ":" + m.SHA256
			if checked[identity] {
				continue
			}
			checked[identity] = true
			if err := ctx.Err(); err != nil {
				return localApprovalSnapshot{ctx: ctx}, err
			}
			err := c.checkLocalMetadata(ctx, records.approvals[identity])
			if ctx.Err() != nil {
				return localApprovalSnapshot{ctx: ctx}, ctx.Err()
			}
			state := approvalVerified
			if err != nil {
				state = approvalInvalid
			}
			c.localResults.Store(m.LocalSource, localApprovalResult{round: round, state: state, hash: m.SHA256})
		}
		return localApprovalSnapshot{ctx: ctx}, nil
	})
	approved := map[string]string{}
	// Completed checks survive a sibling timeout. Consume them only once: replaying
	// an old valid result while a sibling stays stuck would reset the queue's grace.
	c.localResults.Range(func(key, value any) bool {
		result := value.(localApprovalResult)
		c.localResults.Delete(key)
		source := key.(string)
		if result.round == round && states[source] == approvalUnknown {
			states[source] = result.state
			if result.state == approvalVerified {
				approved[source] = result.hash
			}
		}
		return true
	})
	if err != nil || scan.ctx == nil || scan.ctx.Err() != nil {
		c.warnLocalApproval()
	}
	return approved, states
}

func (c *ModelCache) warnLocalApproval() {
	clock := c.Clock
	if clock == nil {
		clock = RealClock{}
	}
	c.localWarningMu.Lock()
	defer c.localWarningMu.Unlock()
	now := clock.Now()
	if !now.Before(c.nextApprovalWarning) {
		log.Print("local model approval not currently verifiable; withholding new loads; ready servers have a bounded grace (will retry)")
		c.nextApprovalWarning = now.Add(time.Minute)
	}
}
