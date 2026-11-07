package myce

import (
	"sync"
	"sync/atomic"
	"time"
)

// queueMutex keeps observation at the mutex boundary, including direct locks
// and requestLock sections outside updateRequest. Without a pressure sensor or
// hold callback it delegates without clock reads or additional synchronization.
type queueMutex struct {
	mu       sync.RWMutex
	control  *controlRecorder
	pressure atomic.Pointer[queuePressure]

	// Configure only while the queue is idle. The callback must not reenter
	// the queue. Concurrent readers count as one occupied interval, since a
	// writer waits for all of them, not the sum of their overlapping holds.
	observeHold func(time.Duration)
	observation sync.Mutex
	occupants   int
	occupiedAt  time.Time
}

func (m *queueMutex) Lock() {
	p, w := m.waitPressure()
	m.lockPressure(p, w)
}

// Named control acquisitions start pressure observation before reserving the
// SQLite writer, so that reservation never hides control-path queueing.
func (m *queueMutex) lockPressure(p *queuePressure, w *queuePressureWait) {
	m.mu.Lock()
	m.acquirePressure(p, w)
	m.enter()
}

func (m *queueMutex) Unlock() {
	m.leave()
	if p := m.pressure.Load(); p != nil {
		p.release()
	}
	m.mu.Unlock()
}

func (m *queueMutex) RLock() {
	p, w := m.waitPressure()
	m.mu.RLock()
	m.acquirePressure(p, w)
	m.enter()
}

func (m *queueMutex) RUnlock() {
	m.leave()
	if p := m.pressure.Load(); p != nil {
		p.release()
	}
	m.mu.RUnlock()
}

func (m *queueMutex) TryLock() bool {
	if !m.mu.TryLock() {
		return false
	}
	m.acquirePressure(m.pressure.Load(), nil)
	m.enter()
	return true
}

func (m *queueMutex) TryRLock() bool {
	if !m.mu.TryRLock() {
		return false
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
