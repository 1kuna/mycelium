package myce

import (
	"encoding/json"
	"errors"
	"io"
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
	if !recovery && !final {
		if save := s.nasProgress.localSignal(checkpointSignalFile(s.dir, j)); save != nil {
			return save.Generation, nil
		}
		return -1, nil // Arbitrary writes: unknown until an event.
	}
	event := "recovery"
	if final {
		event = "final"
	}
	return s.nasProgress.event(w.Clock, event, path, func() (int64, error) {
		if s.nasProgress.scan != nil {
			return s.nasProgress.scan(path)
		}
		return checkpointProgress(path)
	})
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
