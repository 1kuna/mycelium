package myce

import (
	"fmt"
	"sort"
	"time"
)

const engineLoadSlack = 5 * time.Second

func slotReservesMemory(s *ModelSlot) bool { return s.State != "failed" || s.RetryReserved }

func (q *Queue) engineLoadDeadline() *time.Time {
	// Match the worker's entire load context, including copies, environments
	// and admission waits. ready_timeout starts only at engine launch.
	until := q.clock.Now().Add(q.models.Wait + engineLoadSlack)
	return &until
}

func (q *Queue) failModelLoadCalls(model string) {
	for _, c := range q.calls {
		if c.Model == model && c.Machine == "" {
			c.Error = "model load failed"
		}
	}
}

func (q *Queue) expireEngineLoad(s *ModelSlot) {
	if s.State != "loading" {
		return
	}
	if s.LoadUntil == nil {
		// Upgrade persisted slots conservatively: initialize once, never extend
		// this deadline on subsequent heartbeats or queue restarts.
		s.LoadUntil = q.engineLoadDeadline()
	}
	if q.clock.Now().Before(*s.LoadUntil) {
		return
	}
	q.noteEngineFailure(s)
	s.LoadTimedOut = true
	s.RuntimeFailed = false
	s.State = "unloading"
	s.Error = "model load timed out; cleanup pending"
	q.failModelLoadCalls(s.Model)
}

// A slot's attempt identifies an engine, independently of the worker session.
// Empty attempts preserve compatibility with reports from older workers.
func (q *Queue) callExcludes(c *ModelCall, machine string) bool {
	if machine != c.Exclude {
		return false
	}
	if c.ExcludeAttempt == "" {
		return true
	}
	s := q.s.ModelSlots[slotKey(machine, c.Model)]
	// A cleaned failed slot can only receive a fresh load, never a dispatch.
	return s != nil && s.State != "failed" && s.Attempt == c.ExcludeAttempt
}

func (q *Queue) callAttemptLive(c *ModelCall) bool {
	s := q.s.ModelSlots[slotKey(c.Machine, c.Model)]
	return s != nil && s.Session == c.Session && s.Attempt == c.Attempt && (s.State == "ready" || s.State == "draining")
}

func (q *Queue) engineAttemptLost(c *ModelCall) bool {
	m := q.s.Machines[c.Machine]
	return m != nil && !m.Gone && m.Session == c.Session && !q.callAttemptLive(c)
}

func (q *Queue) noteEngineFailure(s *ModelSlot) {
	if s.FailureCounted {
		return
	}
	s.FailureCounted = true
	s.CrashCount++
	if s.RetryDelay == 0 {
		s.RetryDelay = time.Second
	} else {
		s.RetryDelay = min(s.RetryDelay*2, 30*time.Second)
	}
	next := q.clock.Now().Add(s.RetryDelay)
	s.RetryAt = &next
	s.HealthySince = nil
}

func (q *Queue) applyEngineReport(key string, s *ModelSlot, r ModelReport) {
	q.expireEngineLoad(s)
	if s.LoadTimedOut && r.State == "unloaded" {
		r.State, r.RuntimeFailed = "failed", false
	}
	if r.RuntimeFailed && s.State != "failed" && s.State != "unloading" && s.State != "unload_failed" {
		q.noteEngineFailure(s)
		s.State = "unloading"
	}
	switch r.State {
	case "failed":
		// Preserve prompt failure of an initial load (for example corrupt
		// weights). Runtime recovery keeps queued demand through its backoff;
		// it must not turn initial verification errors into silent retry waits.
		if !r.RuntimeFailed {
			q.failModelLoadCalls(s.Model)
		}
		if s.State != "failed" && !(r.RuntimeFailed && s.RetryAt != nil) {
			q.noteEngineFailure(s)
		}
		if s.State != "failed" {
			s.RetryReserved = q.slotPinned(s)
		}
		s.RuntimeFailed = r.RuntimeFailed
		s.State = "failed"
		s.Error = ceilingDiagnostic(r.Error, "model load or unload failed")
		// Reports confirm process/group cleanup; old call completions must not
		// decrement a replacement attempt's accounting.
		s.InFlight = 0
		s.ReleasingCalls = false
	case "unload_failed":
		s.State = "unload_failed"
		s.RetryAt = nil
		s.HealthySince = nil
		s.Error = ceilingDiagnostic(r.Error, "model load or unload failed")
	case "unloading":
		if r.RuntimeFailed {
			s.State = "unloading"
			s.Error = "engine stopped; cleanup pending"
		}
	case "ready":
		if s.State == "loading" {
			s.State = "ready"
			s.Error = ""
			s.RetryAt = nil
			s.LastUsed = q.clock.Now()
			s.LoadUntil = nil
		}
		if s.State != "ready" {
			return
		}
		if r.HealthDegraded || s.HealthRevision != r.HealthRevision {
			s.HealthySince = nil
		}
		s.HealthRevision = r.HealthRevision
		if !r.HealthDegraded {
			if s.HealthySince == nil {
				now := q.clock.Now()
				s.HealthySince = &now
			}
			if q.clock.Now().Sub(*s.HealthySince) >= time.Minute {
				s.CrashCount = 0
				s.RetryDelay = 0
			}
		}
	case "unloaded":
		if s.State == "draining" && s.InFlight == 0 {
			delete(q.s.ModelSlots, key)
		}
	}
}

func (q *Queue) engineDemand(s *ModelSlot, m *Machine) *ModelCall {
	var best *ModelCall
	for _, c := range q.calls {
		if c.Model != s.Model || c.Machine != "" || c.Error != "" || q.clock.Now().Sub(c.Queued) >= q.models.Wait || !q.callPlacementAllowed(c, m) || q.capReasonFor(callJob(c), m) != "" {
			continue
		}
		// The excluded attempt has died; a new attempt can satisfy this demand.
		if c.Exclude == m.ID && c.ExcludeAttempt == "" {
			continue
		}
		if best == nil || q.fairBefore(callJob(c), callJob(best), m) {
			best = c
		}
	}
	return best
}

func (q *Queue) maintainEngineRetry(key string, s *ModelSlot, m *Machine, model Model) {
	pinned := q.slotPinned(s)
	if !pinned && s.RetryReserved {
		s.RetryReserved = false
		debt := min(model.MemoryGB, m.modelReservationDebtGB)
		m.modelReservationDebtGB -= debt
		m.FreeMemoryGB += model.MemoryGB - debt
		q.refreshHostMemory()
	}
	demand := q.engineDemand(s, m)
	if !pinned && !s.RuntimeFailed {
		// A late transport retry can arrive after the failed load report.
		// It must fail too, rather than turn a warm-up failure into a loop.
		q.failModelLoadCalls(s.Model)
		demand = nil
	}
	if !pinned && demand == nil {
		// An HTTP handler may still be converting a lost attempt into its
		// permitted retry. Keep history until it retries or finishes; no memory
		// is reserved by a cleaned failed slot.
		for _, c := range q.calls {
			if c.Model == s.Model && c.Machine == m.ID && c.Session == s.Session && c.Attempt == s.Attempt {
				return
			}
		}
		delete(q.s.ModelSlots, key)
		return
	}
	// Demand-only recovery goes through ordinary ordered call placement below,
	// including fairness, batch precedence, fences and admission.
	if !pinned {
		return
	}
	if s.RetryAt == nil || m.Gone || m.Leaving || q.networkQuiet || q.unreported[m.ID] || m.MemoryError != "" || m.ModelURL == "" || !q.modelUsableMachine(model, m) {
		return
	}
	priority := "production"
	project := model.Project
	if pinned {
		for _, pin := range q.s.Pins {
			if pin.Model == s.Model && pin.Assigned == m.ID && pin.active(q.clock.Now()) {
				project = pin.Project
				break
			}
		}
	}

	job := &Job{Spec: Spec{Project: project, Priority: priority}}
	job.Spec.Needs.Model = s.Model
	if !q.placementAllowed(job, m) {
		return
	}
	room := *m
	if s.RetryReserved {
		// The pin alone can spend its planning reservation. It is never
		// returned to the batch scheduler during crash backoff.
		room.FreeMemoryGB = max(0, room.FreeMemoryGB+model.MemoryGB-m.modelReservationDebtGB)
		room.FreeHostMemoryGB = max(0, room.FreeHostMemoryGB+hostNeed(m, model.MemoryGB, model.HostMemoryGB)-m.hostReservationDebtGB)
	}
	if room.FreeMemoryGB < model.MemoryGB || !hostFits(&room, model.MemoryGB, model.HostMemoryGB) {
		// A newly added pin can still need ordinary pin reclamation.
		if q.reclaimableGPUMemory(m, priority) < model.MemoryGB || !q.reclaimHost(m, model.MemoryGB, model.HostMemoryGB, priority, "pin") {
			return
		}
		q.makeModelRoom(m, model.MemoryGB, priority)
		for _, batch := range q.s.Jobs {
			if batch.Machine == m.ID && (batch.State == "running" || batch.State == "stopping") && rank(batch.Spec.Priority) > 0 && m.FreeMemoryGB+batch.Spec.Needs.MemoryGB >= model.MemoryGB {
				q.stopFor(batch, priority, "pin")
			}
		}
		return
	}
	if !s.RetryReserved {
		s.RetryReserved = true
		m.FreeMemoryGB -= model.MemoryGB
	}
	q.refreshHostMemory()
	if !q.clock.Now().Before(*s.RetryAt) {
		q.beginEngineAttempt(s, priority)
	}
}

func (q *Queue) beginEngineAttempt(s *ModelSlot, priority string) {
	s.State = "loading"
	s.RetryReserved = false
	s.LoadTimedOut = false
	s.RuntimeFailed = false
	s.LoadUntil = q.engineLoadDeadline()
	s.FailureCounted = false
	s.Attempt = ID()
	s.InFlight = 0
	s.Priority = priority
	s.ProductionUsed = priority == "production"
	s.StopAt = nil
	s.Recovering = false
	s.RetryAt = nil
	s.ApprovalUnknownSince = nil
	s.HealthRevision = 0
	s.HealthySince = nil
}

func (q *Queue) engineWaitReason(c *ModelCall) string {
	if c.Machine != "" {
		return ""
	}
	keys := make([]string, 0, len(q.s.ModelSlots))
	for key := range q.s.ModelSlots {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		s := q.s.ModelSlots[key]
		m := q.s.Machines[s.Machine]
		model, _ := q.model(c.Model)
		if s.Model != c.Model || m == nil || m.Gone || m.Leaving || !q.callPlacementAllowed(c, m) || !q.modelUsableMachine(model, m) {
			continue
		}
		if s.State == "unload_failed" {
			return "engine cleanup failed on " + m.ID + "; memory remains reserved"
		}
		if s.CrashCount > 0 && (s.State == "failed" || s.State == "loading" || s.State == "unloading") {
			if s.State == "unloading" {
				return "engine stopped on " + m.ID + "; cleanup pending"
			}
			if s.RetryAt != nil && s.State == "failed" {
				return fmt.Sprintf("engine restarting on %s, retry at %s", m.ID, s.RetryAt.UTC().Format(time.RFC3339Nano))
			}
			return "engine restarting on " + m.ID + "; loading"
		}
	}
	if c.Exclude != "" {
		return "waiting for another engine attempt; failed attempt excluded on " + c.Exclude
	}
	return ""
}
