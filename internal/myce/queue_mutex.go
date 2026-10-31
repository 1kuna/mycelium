package myce

import (
	"sync"
	"time"
)

// queueMutex keeps observation at the mutex boundary, including direct locks
// and requestLock sections outside updateRequest. With observation disabled it
// delegates to sync.RWMutex without clock reads or additional synchronization.
type queueMutex struct {
	mu sync.RWMutex

	// Configure only while the queue is idle. The callback must not reenter
	// the queue. Concurrent readers count as one occupied interval, since a
	// writer waits for all of them, not the sum of their overlapping holds.
	observeHold func(time.Duration)
	observation sync.Mutex
	occupants   int
	occupiedAt  time.Time
}

func (m *queueMutex) Lock() {
	m.mu.Lock()
	m.enter()
}

func (m *queueMutex) Unlock() {
	m.leave()
	m.mu.Unlock()
}

func (m *queueMutex) RLock() {
	m.mu.RLock()
	m.enter()
}

func (m *queueMutex) RUnlock() {
	m.leave()
	m.mu.RUnlock()
}

func (m *queueMutex) TryLock() bool {
	if !m.mu.TryLock() {
		return false
	}
	m.enter()
	return true
}

func (m *queueMutex) TryRLock() bool {
	if !m.mu.TryRLock() {
		return false
	}
	m.enter()
	return true
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
