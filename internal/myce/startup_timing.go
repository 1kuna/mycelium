package myce

import (
	"slices"
	"sync"
	"time"
)

// StartupPhase is observational worker-clock evidence, never admission, Started,
// custody or parity authority. Intervals can overlap (native_member_launch
// contains launch_authorization); consumers must not sum nested phases.
type StartupPhase struct {
	Phase      string    `json:"phase"`
	Sidecar    int       `json:"sidecar,omitempty"` // One-based declared order; zero is main/attempt.
	Calls      uint64    `json:"calls"`
	Seconds    float64   `json:"seconds"` // Cumulative completed calls, including failed calls.
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

type startupTimings struct {
	mu     sync.Mutex
	clock  Clock
	phases []StartupPhase
}

func (s *startupTimings) begin(phase string, sidecar int) func() {
	if s == nil {
		return func() {}
	}
	start := s.clock.Now()
	return func() {
		end := s.clock.Now()
		s.mu.Lock()
		defer s.mu.Unlock()
		for i := range s.phases {
			p := &s.phases[i]
			if p.Phase == phase && p.Sidecar == sidecar {
				p.Calls++
				p.Seconds += max(0, end.Sub(start).Seconds())
				if start.Before(p.StartedAt) {
					p.StartedAt = start
				}
				if end.After(p.FinishedAt) {
					p.FinishedAt = end
				}
				return
			}
		}
		s.phases = append(s.phases, StartupPhase{Phase: phase, Sidecar: sidecar, Calls: 1, Seconds: max(0, end.Sub(start).Seconds()), StartedAt: start, FinishedAt: end})
	}
}

func (s *startupTimings) snapshot() []StartupPhase {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.phases)
}

// Caller has already checked the exact execution/session fence. Accept only
// fixed labels and declared sidecar ordinals: no paths or worker error text.
// One record per phase/component bounds this by the authored execution topology.
func retainStartupTiming(j *Job, phases []StartupPhase) {
	if len(phases) == 0 {
		return
	}
	sidecars := nativeSidecarCount(j, nil)
	// Queue jobs retain the frozen variant, not the worker-lowered RunSpec.
	if j.Extension != nil {
		for _, e := range j.Extensions {
			if e.Digest == j.Extension.Digest {
				for _, v := range e.Variants {
					sidecars = max(sidecars, len(v.Execution.Sidecars))
				}
			}
		}
	}
	j.StartupTiming = slices.Clone(j.StartupTiming)

	for _, p := range phases {
		switch p.Phase {
		case "extension_preparation", "shared_input_staging", "input_staging", "checkpoint_preparation", "environment_acquire", "native_supervisor", "native_member_launch", "process_start", "launch_authorization", "start_authorization", "sidecar_readiness":
		default:
			continue
		}
		if p.Sidecar < 0 || p.Sidecar > sidecars || p.Calls == 0 || !validMemoryValue(p.Seconds) || p.StartedAt.IsZero() || p.FinishedAt.Before(p.StartedAt) {
			continue
		}
		found := false
		for i, old := range j.StartupTiming {
			if old.Phase == p.Phase && old.Sidecar == p.Sidecar {
				found = true
				// A delayed heartbeat cannot erase newer completed work.
				if p.Calls >= old.Calls && p.Seconds >= old.Seconds && !p.FinishedAt.Before(old.FinishedAt) {
					j.StartupTiming[i] = p
				}
				break
			}
		}
		if !found {
			j.StartupTiming = append(j.StartupTiming, p)
		}
	}
}
