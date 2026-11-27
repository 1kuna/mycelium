package myce

import (
	"math"
	"os"
	"sync"
	"time"
)

// Average over the same short horizon as Linux CPU pressure's avg10. This
// spans multiple queue ticks, so one expensive check-in doesn't move an
// integer admission boundary for a single tick.
const controlCPUWindow = 10 * time.Second

type controlCPUCounter struct {
	file  os.FileInfo
	usecs uint64
}

type controlCPUInterval struct {
	start, end time.Time
	usecs      uint64
}

type controlCPUSampler struct {
	mu       sync.Mutex // Never held with Queue.mu.
	read     func() (map[string]controlCPUCounter, error)
	at       time.Time
	previous map[string]controlCPUCounter
	window   []controlCPUInterval
}

type controlCPUObservation struct {
	at    time.Time
	cores float64
}

func (s *controlCPUSampler) sample(clock Clock) controlCPUObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	counters, err := s.read()
	now := clock.Now()
	if err != nil || !s.at.IsZero() && now.Sub(s.at) > controlCPUWindow {
		s.previous, s.window = nil, nil
	}
	if err != nil {
		s.at = time.Time{}
		return controlCPUObservation{}
	}
	if s.previous != nil && !now.After(s.at) {
		return controlCPUObservation{}
	}
	if s.previous != nil {
		var delta uint64
		for path, current := range counters {
			if old, ok := s.previous[path]; ok && os.SameFile(old.file, current.file) && current.usecs >= old.usecs {
				delta += current.usecs - old.usecs
			}
		}
		s.window = append(s.window, controlCPUInterval{s.at, now, delta})
	}
	s.at, s.previous = now, counters
	since := now.Add(-controlCPUWindow)
	for len(s.window) > 0 && !s.window[0].end.After(since) {
		s.window = s.window[1:]
	}
	var usecs float64
	for _, interval := range s.window {
		covered := interval.end.Sub(interval.start)
		if interval.start.Before(since) {
			covered = interval.end.Sub(since)
		}
		usecs += float64(interval.usecs) * float64(covered) / float64(interval.end.Sub(interval.start))
	}
	result := controlCPUObservation{}
	if len(s.window) > 0 {
		start := s.window[0].start
		if start.Before(since) {
			start = since
		}
		result = controlCPUObservation{at: now, cores: usecs / (now.Sub(start).Seconds() * 1e6)}
	}
	return result
}

func controlCPUJobCapacity(m *Machine, capacity int, cores float64) int {
	if capacity == 0 || cores <= 0 {
		return capacity
	}
	// A single remaining logical core must still be able to make forward
	// progress. Such a host shares that core with its control plane.
	logical := m.CapabilityProfile.LogicalCPUs - m.CPUReserveCores
	return min(capacity, max(1, logical-int(math.Ceil(cores))))
}
