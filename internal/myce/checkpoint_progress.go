package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Running NAS attempts use local notifications only. NAS observations are
// keyed to recovery/finalization events, with single-flight late completion.
// The time-based reader below is used only for local file-transport checkpoints.
type checkpointProgressReader struct {
	checkpointIO[int64]
	readMu     sync.Mutex // Never q.mu; serializes observation publication, not queue state.
	recent     map[string]checkpointProgressObservation
	scan       func(string) (int64, error) // Defaults to checkpointProgress.
	signalInfo os.FileInfo
	signal     *checkpointSave
	readSignal func(string, any) error // Local marker counting seam.
}

type checkpointProgressObservation struct {
	event   string
	until   time.Time
	value   int64
	err     error
	pending bool
}

// A payload publishes this on local storage only after the named atomic save
// is durable. It is a notification, not trusted evidence of NAS durability.
type checkpointSave struct {
	Name       string `json:"name"`
	Generation int64  `json:"generation"`
}

func (s *checkpointSave) valid() bool {
	return s != nil && s.Generation > 0 && filepath.Base(s.Name) == s.Name && !strings.ContainsAny(s.Name, `/\\`) && strings.HasSuffix(s.Name, ".json")
}

func checkpointSignalFile(spool string, j Job) string {
	return filepath.Join(attemptStorageRoot(j, spool), ".myce-checkpoint-"+identityHash(j.Session)+".json")
}

// Open one regular file, without following a symlink or blocking on a FIFO.
// The same size bound as the checkpoint JSON contract applies to notifications.
func readCheckpointJSON(path string, value any) error {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return decodeCheckpointJSON(f, value)
}

func decodeCheckpointJSON(f *os.File, value any) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return errors.New("invalid checkpoint marker")
	}
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return errors.New("checkpoint marker too large")
	}
	return json.Unmarshal(data, value)
}

func verifyCheckpointSignal(root *os.Root, save checkpointSave) (int64, error) {
	f, err := root.OpenFile(save.Name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return -1, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return -1, err
	}
	if !info.Mode().IsRegular() {
		return -1, nil
	}
	// Verify only the generation metadata. The writer has already attested
	// to an atomic save, which can contain a much larger application value.
	decoder := json.NewDecoder(io.LimitReader(f, 1<<20))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return -1, nil
	}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			break
		}
		if key == "myce_checkpoint" {
			var metadata struct{ Generation int64 }
			if decoder.Decode(&metadata) == nil && metadata.Generation == save.Generation {
				return save.Generation, nil
			}
			break
		}
		var skip json.RawMessage
		if decoder.Decode(&skip) != nil {
			break
		}
	}
	return -1, nil
}

// Cache by event identity, not elapsed time. Unchanged heartbeats never start
// I/O, including after an error. A new save/recovery/finalization is fresh work.
func (n *checkpointProgressReader) event(clock Clock, event, path string, scan func() (int64, error)) (int64, error) {
	n.readMu.Lock()
	defer n.readMu.Unlock()
	old, exists := n.recent[path]
	if exists && old.event == event && !old.pending {
		return old.value, old.err
	}
	if old.pending && n.inFlight() {
		return old.value, errCheckpointIOPending
	}
	value, err := n.run(path+":"+event, scan)
	pending := errors.Is(err, errCheckpointIOPending)
	if pending {
		value = -1
		if exists {
			value = old.value
		}
	}
	if n.recent == nil {
		n.recent = map[string]checkpointProgressObservation{}
	}

	n.recent[path] = checkpointProgressObservation{event: event, value: value, err: err, pending: pending, until: clock.Now()}
	return value, err
}

// Terminal jobs cannot be recovered. Drop their event observations without
// expiring a live event into a new heartbeat scan. Caller must not hold q.mu.
func (q *Queue) forgetSettledCheckpointProgress(id string) {
	q.mu.Lock()
	root := ""
	if j := q.s.Jobs[id]; j != nil && terminal(j.State) {
		root = j.Checkpoint
	}
	q.mu.Unlock()
	if root == "" {
		return
	}
	q.nasProgress.readMu.Lock()
	defer q.nasProgress.readMu.Unlock()
	for path, observation := range q.nasProgress.recent {
		if filepath.Dir(path) == root && !observation.pending {
			delete(q.nasProgress.recent, path)
		}
	}
}

// Notifications live in the exact attempt's managed local work directory.
// Stat local identity each actor tick; read a newly published marker once.
func (n *checkpointProgressReader) localSignal(path string) *checkpointSave {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	if n.signalInfo != nil && os.SameFile(n.signalInfo, info) && n.signalInfo.ModTime() == info.ModTime() && n.signalInfo.Size() == info.Size() {
		return n.signal
	}
	read := n.readSignal
	if read == nil {
		read = readCheckpointJSON
	}
	var save checkpointSave
	n.signalInfo, n.signal = info, nil
	if read(path, &save) == nil && save.valid() {
		n.signal = &save
	}
	return n.signal
}

func (s *workerStore) checkpointReport(w Worker, j Job, recovery, final bool) (int64, error) {
	path := attemptCheckpoint(j)
	if j.Spec.localFirst() {
		return s.nasProgress.run(path, func() (int64, error) { return localFirstProgress(path) })
	}
	if j.FileTransport { // Local disk; retain existing portable capture behavior.
		return s.nasProgress.read(w.Clock, w.Interval, path)
	}
	if s.state.CheckpointBacking == nil {
		return -1, nil // Older launches have no proof of the prepared backing.
	}
	backing := s.state.CheckpointBacking.Attempt
	event := "recovery"
	var save *checkpointSave
	if final {
		event = "final"
	} else if !recovery {
		save = s.nasProgress.localSignal(checkpointSignalFile(s.dir, j))
		if save == nil {
			return -1, nil // Arbitrary writes: unknown until a lifecycle event.
		}
		event = fmt.Sprintf("save:%s:%d", save.Name, save.Generation)
	}
	generation, err := s.nasProgress.event(w.Clock, event, path, func() (int64, error) {
		root, err := openBoundCheckpoint(path, backing)
		if err != nil {
			return -1, err
		}
		defer root.Close()
		if save != nil {
			return verifyCheckpointSignal(root, *save)
		}
		if s.nasProgress.scan != nil {
			return s.nasProgress.scan(path)
		}
		return scanCheckpointProgressFS(root.FS(), ".", false)
	})
	if err != nil {
		if errors.Is(err, errCheckpointBacking) {
			return -1, nil // Fence only this attempt. The host may have remounted.
		}
		return -1, checkpointPathError(path, err)
	}
	return generation, nil
}

func scanCheckpointProgressFS(files fs.FS, dir string, jobRoot bool) (int64, error) {
	return scanCheckpointProgressFiles(dir, jobRoot, func(name string, visit fs.WalkDirFunc) error {
		return fs.WalkDir(files, name, visit)
	}, func(name string) ([]byte, error) { return fs.ReadFile(files, name) })
}

func (n *checkpointProgressReader) read(clock Clock, period time.Duration, path string) (int64, error) {
	n.readMu.Lock()
	defer n.readMu.Unlock()
	now := clock.Now()
	observation, exists := n.recent[path]
	if exists && !observation.pending && now.Before(observation.until) {
		return observation.value, observation.err
	}
	if exists && observation.pending && n.inFlight() {
		// Joining an owned syscall must not spend another observation budget
		// on every actor tick or serialize duplicate queue reports behind it.
		return observation.value, errCheckpointIOPending
	}
	// Keep only this interval's observations, not the queue's retained history.
	for key, old := range n.recent {
		if !now.Before(old.until) && key != path {
			delete(n.recent, key)
		}
	}
	if !exists || !observation.pending {
		observation.until = now.Add(period)
	}
	scan := n.scan
	if scan == nil {
		scan = checkpointProgress
	}
	value, err := n.run(path, func() (int64, error) { return scan(path) })
	observation.pending = errors.Is(err, errCheckpointIOPending)
	if !observation.pending {
		observation.value, observation.err = value, err
		// A late completion must be consumed before another scan can start.
		observation.until = clock.Now().Add(period)
	} else if exists {
		value = observation.value // A slow refresh must not reset reported progress.
	}
	if n.recent == nil {
		n.recent = map[string]checkpointProgressObservation{}
	}
	n.recent[path] = observation
	return value, err
}
