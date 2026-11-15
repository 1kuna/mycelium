package myce

import (
	"errors"
	"sync"
	"time"
)

// Progress has one consumer per check-in. Reading it on the 100ms actor tick
// produced ~104 CIFS directory reads per attempt in 5s (#1670). Reuse each
// observation for the consumer's interval instead: the cost is at most one
// report interval of progress latency, with no new fixed polling interval.
// Do not suppress legacy scans using the root mtime: nested/in-place saves do
// not change it. Local-first's published marker is read on local disk instead.
type checkpointProgressReader struct {
	checkpointIO[int64]
	readMu sync.Mutex // Never q.mu; serializes observation publication, not queue state.
	recent map[string]checkpointProgressObservation
	scan   func(string) (int64, error) // Defaults to checkpointProgress.
}

type checkpointProgressObservation struct {
	until   time.Time
	value   int64
	err     error
	pending bool
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

// Use this sender's advertised or learned cadence, including legacy clients.
// Without cadence evidence, retain the existing scan-on-check-in behavior.
func (q *Queue) checkpointProgressPeriod(c Checkin) time.Duration {
	if c.period > 0 {
		return c.period
	}
	q.contactMu.Lock()
	defer q.contactMu.Unlock()
	if contact := q.contacts[c.Machine.ID]; contact != nil && contact.session == c.Machine.Session {
		if contact.period > 0 {
			return contact.period
		}
		return time.Duration(contact.cadence.Mean * float64(time.Second))
	}
	return 0
}
