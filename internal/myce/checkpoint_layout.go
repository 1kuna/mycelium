package myce

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Checkpoint layout (#894). One folder per job directly under a shared root
// made every macOS SMB enumeration and metadata operation in that root slow on
// every host once it held thousands of entries. New NAS jobs use
// ROOT/<first two ID hex digits>/<ID>: 256 shards keep each folder at a few
// hundred entries even at tens of thousands of retained jobs, and settled-job
// expiry (checkpoint_expiry.go) keeps the total bounded. Existing jobs keep the
// absolute path recorded at submission, so flat folders stay readable and
// resumable; expiry drains them as their jobs settle.
//
// The queue creates all shards, then this marker, before assigning any sharded
// path. Predecessor workers require a job folder's parent to exist already, so
// they resume and run sharded jobs unchanged. Workers also require the marker
// before expiry, so an empty local mountpoint is never mistaken for the share.
const checkpointShardMarker = ".myce-checkpoint-shards-v1"

// A failed preparation (share down, permission refusal) is retried on a later
// check-in. One attempt is 257 metadata operations; once a minute bounds that
// repeated load while the share is unhealthy.
const checkpointShardRetry = time.Minute

type checkpointShards struct {
	enabled bool
	ready   atomic.Bool
	mu      sync.Mutex
	running bool
	next    time.Time
	done    chan struct{} // Closed when the in-flight preparation finishes; tests wait on it.
}

// WithCheckpointShards enables the sharded NAS checkpoint layout and settled
// checkpoint expiry. The queue command and dev enable it; synthetic unit
// queues keep the flat layout so they do no background root I/O.
func WithCheckpointShards() QueueOption {
	return func(q *Queue) { q.shards.enabled = true }
}

// checkpointShard returns the shard folder for a queue-generated job ID, or ""
// for IDs that do not start with two lowercase hex digits (synthetic IDs).
func checkpointShard(id string) string {
	if len(id) < 3 || !lowerHex(id[0]) || !lowerHex(id[1]) {
		return ""
	}
	return id[:2]
}
func lowerHex(b byte) bool { return '0' <= b && b <= '9' || 'a' <= b && b <= 'f' }

// checkpointPath runs under q.mu and performs no I/O.
func (q *Queue) checkpointPath(id string) string {
	if q.shards.ready.Load() && !q.fileCheckpoints && !q.simulation {
		if shard := checkpointShard(id); shard != "" {
			return filepath.Join(q.root, shard, id)
		}
	}
	return filepath.Join(q.root, id)
}

// kickCheckpointShards starts at most one background preparation. Callers
// must not hold q.mu; the work itself never takes it.
func (q *Queue) kickCheckpointShards() <-chan struct{} {
	s := &q.shards
	if !s.enabled || s.ready.Load() || q.fileCheckpoints || q.simulation {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return s.done
	}
	if q.clock.Now().Before(s.next) {
		return nil
	}
	s.running, s.done = true, make(chan struct{})
	done, root := s.done, q.root
	go func() {
		err := prepareCheckpointShards(root)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.running = false
		if err != nil {
			s.next = q.clock.Now().Add(checkpointShardRetry)
			log.Printf("checkpoint shard preparation unavailable; new jobs keep flat folders and preparation retries")
		} else {
			s.ready.Store(true)
		}
		close(done)
	}()
	return done
}

func prepareCheckpointShards(root string) error {
	marker := filepath.Join(root, checkpointShardMarker)
	if _, err := os.Stat(marker); err == nil {
		return nil
	}
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("checkpoint root is not a directory")
	}
	// Never create the root: a missing mount must not become local folders.
	// Shards inherit the root's permission bits so other workers can write.
	for i := range 256 {
		if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("%02x", i)), info.Mode().Perm()); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
	}
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, err = f.WriteString("v1\n")
	return errors.Join(err, f.Sync(), f.Close())
}

// ensureCheckpointParent keeps the existing "root unavailable" refusal for a
// missing root, and recreates only a missing shard beneath an existing root.
func ensureCheckpointParent(path string) error {
	parent := filepath.Dir(path)
	_, err := os.Stat(parent)
	if err == nil || !errors.Is(err, fs.ErrNotExist) || checkpointShard(filepath.Base(path)) != filepath.Base(parent) {
		return err
	}
	info, err := os.Stat(filepath.Dir(parent))
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("checkpoint root is not a directory")
	}
	if err := os.Mkdir(parent, info.Mode().Perm()); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	return nil
}
