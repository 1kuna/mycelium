package myce

import (
	"sync"
	"sync/atomic"
	"time"
)

// queueMutex keeps observation at the mutex boundary, including direct locks
// and requestLock sections outside updateRequest. Writer turns are always fair;
// without a pressure sensor or hold callback, telemetry needs no wall-clock reads.
type queueMutex struct {
	mu       sync.RWMutex
	writers  queueWriters
	pressure atomic.Pointer[queuePressure]
	control  *controlClock // Set before the queue is published; injected decision clock.

	// Configure only while the queue is idle. The callback must not reenter
	// the queue. Concurrent readers count as one occupied interval, since a
	// writer waits for all of them, not the sum of their overlapping holds.
	observeHold func(time.Duration)
	observation sync.Mutex
	occupants   int
	occupiedAt  time.Time

	// Liveness evidence for waiting clients: releases only advance, and waiting
	// counts callers blocked in Lock/RLock. Two atomics; no lock, no history.
	releases atomic.Uint64
	waiting  atomic.Int64
}

// QueueProgress lets a client tell a slow queue from a stalled one. Releases
// that advance, or nobody waiting, is progress; a constant count while callers
// wait means one holder has kept the queue for the whole interval.
type QueueProgress struct {
	Releases uint64  `json:"releases"`
	Waiting  int64   `json:"waiting"`
	P99MS    float64 `json:"p99_ms"` // completed requests, excluding ping
}

func (m *queueMutex) progress() QueueProgress {
	return QueueProgress{Releases: m.releases.Load(), Waiting: max(0, m.waiting.Load())}
}

func (m *queueMutex) Lock() {
	m.lock(false)
}

func (m *queueMutex) lock(submission bool) {
	m.waiting.Add(1)
	p, w := m.waitPressure()
	m.writers.acquire(submission)
	m.mu.Lock()
	m.waiting.Add(-1)
	if m.control != nil {
		m.control.begin()
	}
	m.acquirePressure(p, w)
	m.enter()
}

func (m *queueMutex) Unlock() {
	m.releases.Add(1)
	m.leave()
	if p := m.pressure.Load(); p != nil {
		p.release()
	}
	if m.control != nil {
		m.control.end(m.mu.Unlock)
	} else {
		m.mu.Unlock()
	}
	m.writers.release()
}

func (m *queueMutex) RLock() {
	m.waiting.Add(1)
	p, w := m.waitPressure()
	m.mu.RLock()
	m.waiting.Add(-1)
	if m.control != nil {
		m.control.begin()
	}
	m.acquirePressure(p, w)
	m.enter()
}

func (m *queueMutex) RUnlock() {
	m.releases.Add(1)
	m.leave()
	if p := m.pressure.Load(); p != nil {
		p.release()
	}
	if m.control != nil {
		m.control.end(m.mu.RUnlock)
	} else {
		m.mu.RUnlock()
	}
}

func (m *queueMutex) TryLock() bool {
	if !m.writers.tryAcquire() {
		return false
	}
	if !m.mu.TryLock() {
		m.writers.release()
		return false
	}
	if m.control != nil {
		m.control.begin()
	}
	m.acquirePressure(m.pressure.Load(), nil)
	m.enter()
	return true
}

// Writers take turns by class, FIFO within each class. The first queued submit
// waits for the current writer and at most one control turn, rather than every
// queued heartbeat. Alternating when both classes are waiting
// also preserves worker progress during a continuous stream of submissions.
// Readers retain RWMutex's writer exclusion and concurrent-read behavior.
type queueWriters struct {
	mu         sync.Mutex
	active     bool
	submission bool
	head, tail [2]*queueWriter
}

type queueWriter struct {
	ready chan struct{}
	next  *queueWriter
}

func (w *queueWriters) acquire(submission bool) {
	w.mu.Lock()
	if !w.active {
		w.active, w.submission = true, submission
		w.mu.Unlock()
		return
	}
	class := 0
	if submission {
		class = 1
	}
	waiter := &queueWriter{ready: make(chan struct{})}
	if w.tail[class] == nil {
		w.head[class] = waiter
	} else {
		w.tail[class].next = waiter
	}
	w.tail[class] = waiter
	w.mu.Unlock()
	<-waiter.ready
}

func (w *queueWriters) tryAcquire() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active {
		return false
	}
	w.active, w.submission = true, false
	return true
}

func (w *queueWriters) release() {
	w.mu.Lock()
	defer w.mu.Unlock()
	class := 1
	if w.submission {
		class = 0
	}
	if w.head[class] == nil {
		class = 1 - class
	}
	if next := w.head[class]; next != nil {
		w.head[class] = next.next
		if next.next == nil {
			w.tail[class] = nil
		}
		w.submission = class == 1
		close(next.ready)
	} else {
		w.active = false
	}
}

type submissionMutex struct{ *queueMutex }

func (m submissionMutex) Lock() { m.queueMutex.lock(true) }

type submissionTurnKey struct{}

func (m *queueMutex) TryRLock() bool {
	if !m.mu.TryRLock() {
		return false
	}
	if m.control != nil {
		m.control.begin()
	}
	m.acquirePressure(m.pressure.Load(), nil)
	m.enter()
	return true
}

// Installation is fenced by the underlying writer lock. A caller that began
// before installation rechecks after acquiring, so every release has an enter.
func (m *queueMutex) enablePressure(clock Clock) *queuePressure {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p := m.pressure.Load(); p != nil {
		return p
	}
	p := &queuePressure{clock: clock, waiting: make(map[*queuePressureWait]time.Time)}
	m.pressure.Store(p)
	return p
}

func (m *queueMutex) waitPressure() (*queuePressure, *queuePressureWait) {
	p := m.pressure.Load()
	if p == nil {
		return nil, nil
	}
	return p, p.wait()
}

func (m *queueMutex) acquirePressure(p *queuePressure, w *queuePressureWait) {
	if p == nil {
		p = m.pressure.Load()
	}
	if p != nil {
		p.acquired(w)
	}
}

func (m *queueMutex) enter() {
	if m.observeHold == nil {
		return
	}
	m.observation.Lock()
	defer m.observation.Unlock()
	if m.occupants == 0 {
		m.occupiedAt = time.Now()
	}
	m.occupants++
}

func (m *queueMutex) leave() {
	if m.observeHold == nil {
		return
	}
	m.observation.Lock()
	defer m.observation.Unlock()
	m.occupants--
	if m.occupants == 0 {
		m.observeHold(time.Since(m.occupiedAt))
	}
}
