package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"
)

var errCheckpointBacking = errors.New("checkpoint backing changed")

func (j Job) nasCheckpoint() bool {
	return needsCheckpoint(&j) && !j.FileTransport && !j.Spec.localFirst()
}

func checkpointRootUnavailable(m *Machine) bool {
	return m.CheckpointUnavailable || slices.ContainsFunc(m.UnavailableRoots, func(root UnavailableRoot) bool {
		return root.Root == "checkpoint"
	})
}

func requireCheckpointFloor(j Job, generation int64) error {
	if origin := j.CheckpointTarget; origin != nil && origin.Generation > 0 && generation < origin.Generation {
		return fmt.Errorf("checkpoint generation %d is below accepted generation %d", generation, origin.Generation)
	}
	return nil
}

// These identities stay in the local worker spool. A replacement directory at
// the same path cannot inherit the attempt's authority to publish progress.
type checkpointDirectoryID struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

type checkpointBacking struct {
	Root    checkpointDirectoryID `json:"root"`
	Attempt checkpointDirectoryID `json:"attempt"`
}

func checkpointDirectoryIdentity(info fs.FileInfo, err error) (checkpointDirectoryID, error) {
	if err != nil {
		return checkpointDirectoryID{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() {
		return checkpointDirectoryID{}, errors.New("checkpoint directory identity unavailable")
	}
	return checkpointDirectoryID{Device: uint64(st.Dev), Inode: st.Ino}, nil
}

type preparedCheckpoint struct {
	path    string
	backing *checkpointBacking
}

func (w Worker) prepareBoundCheckpoint(ctx context.Context, j Job, root string, prior *checkpointBacking, prepare func(Job) (string, error)) (preparedCheckpoint, error) {
	if !j.nasCheckpoint() {
		path, err := prepare(j)
		return preparedCheckpoint{path: path}, err
	}
	if root == "" {
		root = filepath.Dir(j.Checkpoint)
	}
	if !filepath.IsAbs(root) || !withinNAS(root, j.Checkpoint) {
		return preparedCheckpoint{}, errCheckpointBacking
	}
	before, err := checkpointDirectoryIdentity(os.Stat(root))
	if err != nil {
		return preparedCheckpoint{}, err
	}
	// Confirm the mount after observing the directory. A writable mountpoint
	// left behind after preflight is not the storage that passed preflight.
	m := Machine{UnavailableRoots: w.sampleStorageRoots(ctx, root)}
	if checkpointRootUnavailable(&m) || prior != nil && prior.Root != before {
		return preparedCheckpoint{}, errCheckpointBacking
	}
	if prior != nil {
		path := attemptCheckpoint(j)
		attempt, err := openBoundCheckpoint(path, prior.Attempt)
		if err != nil {
			return preparedCheckpoint{}, err
		}
		defer attempt.Close()
		// Preparation may be retried after a later launch step stalls. Reuse
		// the prepared empty directory too, so its receipt stays valid.
		if j.CheckpointTarget != nil && j.CheckpointTarget.Generation > 0 {
			generation, err := scanCheckpointProgressFS(attempt.FS(), ".", false)
			if err == nil {
				err = requireCheckpointFloor(j, generation)
			}
			if err != nil {
				return preparedCheckpoint{}, checkpointPathError(path, err)
			}
		}
		return preparedCheckpoint{path: path, backing: prior}, nil
	}
	path, err := prepare(j)
	if err != nil {
		return preparedCheckpoint{}, err
	}
	attempt, err := checkpointDirectoryIdentity(os.Stat(path))
	if err != nil {
		return preparedCheckpoint{}, err
	}
	after, err := checkpointDirectoryIdentity(os.Stat(root))
	if err != nil {
		return preparedCheckpoint{}, err
	}
	if before != after || attempt.Device != before.Device {
		return preparedCheckpoint{}, errCheckpointBacking
	}
	return preparedCheckpoint{path: path, backing: &checkpointBacking{Root: before, Attempt: attempt}}, nil
}

func openBoundCheckpoint(path string, expected checkpointDirectoryID) (*os.Root, error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	id, err := checkpointDirectoryIdentity(root.Stat("."))
	if err != nil || id != expected {
		root.Close()
		return nil, errors.Join(errCheckpointBacking, err)
	}
	return root, nil
}

// os.Root reports paths relative to the descriptor. Keep storage fault
// attribution on the configured absolute path and the originating syscall.
func checkpointPathError(root string, err error) error {
	switch e := err.(type) {
	case *os.PathError:
		if !filepath.IsAbs(e.Path) {
			return &os.PathError{Op: e.Op, Path: filepath.Join(root, e.Path), Err: e.Err}
		}
	case *os.LinkError:
		if !filepath.IsAbs(e.Old) && !filepath.IsAbs(e.New) {
			return &os.LinkError{Op: e.Op, Old: filepath.Join(root, e.Old), New: filepath.Join(root, e.New), Err: e.Err}
		}
	}
	return err
}

// The queue must independently fence its own lifecycle scans. Keep one receipt
// on the verified root so a remount of that same storage can be recognized
// even if the OS assigned it a new device or directory identity.
type queueCheckpointBacking struct {
	directory checkpointDirectoryID
	receipt   string
}

func checkpointBackingReceipt(root string) string {
	return filepath.Base(storageProbePath(root, ".myce-probe-", "queue-backing"))
}

func (b *queueCheckpointBacking) matches(root *os.Root, current checkpointDirectoryID) bool {
	if current == b.directory {
		return true
	}
	f, err := root.OpenFile(checkpointBackingReceipt(root.Name()), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, int64(len(b.receipt)+1)))
	return err == nil && string(data) == b.receipt
}

func (q *Queue) probeCheckpointBacking(path string, progress func()) bool {
	root, err := os.OpenRoot(path)
	progress()
	if err != nil {
		return false
	}
	defer root.Close()
	id, err := checkpointDirectoryIdentity(root.Stat("."))
	progress()
	if err != nil {
		return false
	}
	prior := q.nasBacking.Load()
	if prior != nil && !prior.matches(root, id) {
		return false
	}
	progress()
	if !checkpointAvailable(path, "queue", progress) {
		return false
	}
	current, err := checkpointDirectoryIdentity(os.Stat(path))
	progress()
	if err != nil || current != id {
		return false
	}
	if prior == nil {
		receipt := ID()
		file, err := root.OpenFile(checkpointBackingReceipt(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
		progress()
		if err != nil {
			return false
		}
		_, err = file.WriteString(receipt)
		progress()
		if err == nil {
			err = file.Sync()
			progress()
		}
		err = errors.Join(err, file.Close())
		progress()
		if err != nil {
			return false
		}
		prior = &queueCheckpointBacking{receipt: receipt}
	}
	q.nasBacking.Store(&queueCheckpointBacking{directory: id, receipt: prior.receipt})
	return true
}

func (q *Queue) scanBoundCheckpoint(path string, scan func(string) (int64, error)) (int64, error) {
	if scan != nil {
		return scan(path)
	}
	known := q.nasBacking.Load()
	if known == nil {
		if q.simulation || q.checkpointProbe != nil {
			return checkpointProgress(path) // Synthetic queues may replace the probe.
		}
		return -1, errCheckpointBacking
	}
	root, err := os.OpenRoot(q.root)
	if err != nil {
		return -1, errors.Join(errCheckpointBacking, err)
	}
	defer root.Close()
	id, err := checkpointDirectoryIdentity(root.Stat("."))
	if err != nil || !known.matches(root, id) {
		return -1, errors.Join(errCheckpointBacking, err)
	}
	rel, err := filepath.Rel(q.root, path)
	if err != nil || !filepath.IsLocal(rel) || rel == "." {
		return -1, errCheckpointBacking
	}
	dir, err := root.OpenRoot(rel)
	if err != nil {
		return -1, err
	}
	defer dir.Close()
	attempt, err := checkpointDirectoryIdentity(dir.Stat("."))
	if err != nil {
		return -1, err
	}
	if attempt.Device != id.Device {
		return -1, errCheckpointBacking
	}
	return scanCheckpointProgressFS(dir.FS(), ".", false)
}
