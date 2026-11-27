package myce

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

const shadowWeek = 7 * 24 * time.Hour

type MemoryShadowWeek struct {
	Since               time.Time      `json:"since"`
	Through             time.Time      `json:"through"`
	OverE               int            `json:"r4_over_e"`
	Victim              int            `json:"r4_victim"`
	Repeat              map[string]int `json:"repeat_by_cause"`
	AttemptHours        float64        `json:"r4_attempt_hours"`
	Raises              int            `json:"raises"`
	RaiseGiBHours       float64        `json:"raise_gib_hours"`
	OwnersHolds         int            `json:"owners_holds"`
	OwnersGiBHours      float64        `json:"owners_hold_gib_hours"`
	LendingVictims      int            `json:"victims_after_lending"`
	CurrentStops        int            `json:"current_memory_pressure_stops"`
	CurrentAttemptHours float64        `json:"current_memory_pressure_attempt_hours"`
}

type memoryShadowHost struct {
	Worker string              `json:"worker"`
	Report *MemoryShadowReport `json:"report"`
}

type shadowCandidate struct {
	priority  int
	resumable bool
	started   time.Time
}

type shadowObservation struct {
	worker, host, session string
	order                 uint64
	at                    time.Time
	report                *MemoryShadowReport
	candidates            map[Execution]shadowCandidate
}

func (q *Queue) shadowObservation(m *Machine, at time.Time) shadowObservation {
	o := shadowObservation{worker: m.ID, host: hostID(m), session: m.Session, order: m.memoryShadowOrder, at: at, report: m.MemoryShadow}
	if o.report == nil {
		return o
	}
	o.candidates = map[Execution]shadowCandidate{}
	for _, pool := range o.report.Pools {
		for _, a := range pool.Attempts {
			j := q.s.Jobs[a.ID]
			if j != nil && j.Attempt == a.Attempt && j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") && j.StartedAt != nil && !j.ReleaseConfirmed {
				o.candidates[Execution{ID: a.ID, Attempt: a.Attempt}] = shadowCandidate{q.priorityMatch(j.Spec).Index, j.Spec.Resumable, *j.StartedAt}
			}
		}
	}
	return o
}

func (o shadowObservation) victim(pool MemoryShadowPool) (MemoryShadowAttempt, shadowCandidate, bool) {
	var victim MemoryShadowAttempt
	var rank shadowCandidate
	var overshoot float64
	for _, a := range pool.Attempts {
		candidate, ok := o.candidates[Execution{ID: a.ID, Attempt: a.Attempt}]
		if !ok || a.FootprintGB == nil {
			continue
		}
		over := max(0, *a.FootprintGB-a.ExpectedGB)
		if victim.ID == "" || candidate.before(rank, over, overshoot) {
			victim, rank, overshoot = a, candidate, over
		}
	}
	return victim, rank, victim.ID != ""
}

func (a shadowCandidate) before(b shadowCandidate, aOver, bOver float64) bool {
	if a.priority != b.priority {
		return a.priority > b.priority
	}
	if aOver != bOver {
		return aOver > bOver
	}
	if a.resumable != b.resumable {
		return a.resumable
	}
	return a.started.After(b.started)
}

type shadowOwnersHold struct {
	peak               float64
	belowSince, lapsed time.Time
	window             time.Duration
}

func (h *shadowOwnersHold) observe(at time.Time, gb float64) {
	if h.peak == 0 {
		return
	}
	if gb >= h.peak {
		h.peak, h.belowSince = gb, time.Time{}
		return
	}
	if h.belowSince.IsZero() {
		h.belowSince = at
	}
	if !at.Before(h.belowSince.Add(h.window)) {
		h.peak, h.lapsed = 0, h.belowSince.Add(h.window)
		h.belowSince = time.Time{}
	}
}

func (h *shadowOwnersHold) raise(at time.Time, peak float64) {
	if h.peak == 0 {
		if !h.lapsed.IsZero() && at.Sub(h.lapsed) < h.window {
			h.window *= 2
		} else {
			h.window = 24 * time.Hour
		}
	}
	h.peak, h.belowSince = peak, time.Time{}
}

type shadowPoolHistory struct {
	lentAt                map[Execution]time.Time
	lentSession           string
	owners                shadowOwnersHold
	blocked               Execution // One hypothetical victim until its real attempt leaves the trace.
	blockedSession        string
	lastStop              map[string]time.Time
	raises                map[string]shadowRaise
	at                    time.Time
	ownersHoldGB, raiseGB float64
}

type shadowRaise struct {
	from, to float64
}

type shadowPoolKey struct {
	host, pool string
}

type shadowQueueMonitor struct {
	mu        sync.Mutex // Acquired only after queue commit or outside q.mu.
	pools     map[shadowPoolKey]*shadowPoolHistory
	orders    map[string]uint64
	hours     map[time.Time]*MemoryShadowWeek
	first     time.Time
	lastPrune time.Time
}

func (s *shadowQueueMonitor) hour(at time.Time) *MemoryShadowWeek {
	key := at.UTC().Truncate(time.Hour)
	h := s.hours[key]
	if h == nil {
		h = &MemoryShadowWeek{Repeat: map[string]int{}}
		s.hours[key] = h
	}
	return h
}

func (s *shadowQueueMonitor) observe(o shadowObservation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hours == nil {
		s.hours = map[time.Time]*MemoryShadowWeek{}
		s.orders = map[string]uint64{}
		s.pools = map[shadowPoolKey]*shadowPoolHistory{}
		s.first = o.at
	}
	if o.order <= s.orders[o.worker] {
		return
	}
	s.orders[o.worker] = o.order
	if o.at.Sub(s.lastPrune) >= time.Hour {
		cutoff := o.at.Add(-shadowWeek).UTC().Truncate(time.Hour)
		for at := range s.hours {
			if at.Before(cutoff) {
				delete(s.hours, at)
			}
		}
		for key, p := range s.pools {
			for id, at := range p.lastStop {
				if at.Before(cutoff) {
					delete(p.lastStop, id)
					delete(p.raises, id)
				}
			}
			if p.at.Before(cutoff) && p.owners.peak == 0 {
				delete(s.pools, key)
			}
		}
		s.lastPrune = o.at
	}
	for _, pool := range o.report.Pools {
		key := shadowPoolKey{o.host, pool.Name}
		h := s.pools[key]
		if h == nil {
			h = &shadowPoolHistory{lastStop: map[string]time.Time{}, raises: map[string]shadowRaise{}}
			s.pools[key] = h
		}
		s.observePool(o, pool, h)
	}
}

func (s *shadowQueueMonitor) observePool(o shadowObservation, p MemoryShadowPool, h *shadowPoolHistory) {
	if !h.at.IsZero() && !o.at.After(h.at) {
		return
	}
	if p.SlackGB == nil || p.ReachGB == nil || p.FreeGB == nil {
		h.at, h.ownersHoldGB, h.raiseGB = time.Time{}, 0, 0
		h.owners.belowSince = time.Time{}
		return
	}
	gap := o.at.Sub(h.at)
	if !h.at.IsZero() && gap.Seconds() <= 2*o.report.ReactionSeconds {
		for from := h.at; from.Before(o.at); {
			to := minTime(o.at, from.UTC().Truncate(time.Hour).Add(time.Hour))
			row := s.hour(from)
			hours := to.Sub(from).Hours()
			row.OwnersGiBHours += h.ownersHoldGB * hours
			row.RaiseGiBHours += h.raiseGB * hours
			from = to
		}
	} else {
		// A missing observation cannot prove that owners stayed below their peak.
		h.owners.belowSince = time.Time{}
	}
	h.at = o.at
	priorLent := h.lentAt
	if h.lentSession != o.session {
		priorLent = nil
	}
	h.lentAt, h.lentSession = make(map[Execution]time.Time), o.session
	for _, a := range p.Attempts {
		key := Execution{ID: a.ID, Attempt: a.Attempt}
		if at := priorLent[key]; !at.IsZero() {
			h.lentAt[key] = at
		} else if a.Lent {
			h.lentAt[key] = o.at
		}
	}
	h.owners.observe(o.at, p.OwnersGB)
	defer func() {
		h.ownersHoldGB = max(0, h.owners.peak-p.OwnersGB)
		h.raiseGB = 0
		for _, a := range p.Attempts {
			if r, ok := h.raises[a.ID]; ok && a.FootprintGB != nil {
				// Charge the extra reservation while this raised attempt is
				// present, above what its normal E and high-water already hold.
				h.raiseGB += max(0, r.to-max(r.from, a.HighWaterGB, *a.FootprintGB))
			}
		}
	}()
	if h.blockedSession == o.session && slices.ContainsFunc(p.Attempts, func(a MemoryShadowAttempt) bool {
		return a.ID == h.blocked.ID && a.Attempt == h.blocked.Attempt
	}) {
		return
	}
	h.blocked, h.blockedSession = Execution{}, ""
	if !p.PressureWarning && *p.FreeGB >= 0 && (p.ForecastFreeGB == nil || *p.ForecastFreeGB >= 0) {
		return
	}
	a, rank, ok := o.victim(p)
	if !ok {
		return
	}
	row := s.hour(o.at)
	h.blocked, h.blockedSession = Execution{ID: a.ID, Attempt: a.Attempt}, o.session
	row.AttemptHours += max(0, o.at.Sub(rank.started).Hours())
	cause := "unclassified"
	if *a.FootprintGB > a.ExpectedGB {
		row.OverE++
		cause = "own_overshoot"
		to := min(*p.ReachGB, 2*max(a.HighWaterGB, a.ExpectedGB))
		if to > max(a.HighWaterGB, a.ExpectedGB) {
			row.Raises++
			h.raises[a.ID] = shadowRaise{a.ExpectedGB, to}
		}
	} else {
		row.Victim++
		if slices.ContainsFunc(p.Attempts, func(b MemoryShadowAttempt) bool {
			return b.ID != a.ID && b.FootprintGB != nil && *b.FootprintGB > b.ExpectedGB && b.Growth > 0
		}) {
			cause = "co_tenant_over_e"
		}
		if p.OwnersForecastGB > max(p.OwnersGB, h.owners.peak) {
			if !h.owners.lapsed.IsZero() && h.owners.peak == 0 {
				cause = "lapsed_owners_hold"
			} else if cause == "unclassified" {
				cause = "owners_burst"
			}
			row.OwnersHolds++
			h.owners.raise(o.at, p.OwnersForecastGB)
		}
		if slices.ContainsFunc(p.Attempts, func(b MemoryShadowAttempt) bool {
			lentAt := h.lentAt[Execution{ID: b.ID, Attempt: b.Attempt}]
			return b.ID != a.ID && b.Growth > 0 && !lentAt.IsZero() && rank.started.After(lentAt)
		}) {
			row.LendingVictims++
		}
	}
	if prior := h.lastStop[a.ID]; !prior.IsZero() && o.at.Sub(prior) <= shadowWeek {
		row.Repeat[cause]++
	}
	h.lastStop[a.ID] = o.at
}

func (s *shadowQueueMonitor) week(at time.Time) *MemoryShadowWeek {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.first.IsZero() {
		return nil
	}
	out := &MemoryShadowWeek{Since: maxTime(s.first, at.Add(-shadowWeek).UTC().Truncate(time.Hour)), Through: at, Repeat: map[string]int{}}
	for when, h := range s.hours {
		if when.Before(out.Since.UTC().Truncate(time.Hour)) || when.After(at) {
			continue
		}
		out.OverE += h.OverE
		out.Victim += h.Victim
		out.Raises += h.Raises
		out.OwnersHolds += h.OwnersHolds
		out.LendingVictims += h.LendingVictims
		out.AttemptHours += h.AttemptHours
		out.RaiseGiBHours += h.RaiseGiBHours
		out.OwnersGiBHours += h.OwnersGiBHours
		for k, n := range h.Repeat {
			out.Repeat[k] += n
		}
	}
	return out
}

func (w MemoryShadowWeek) line() string {
	repeats := []string{}
	for _, key := range slices.Sorted(maps.Keys(w.Repeat)) {
		repeats = append(repeats, fmt.Sprintf("%s %d", key, w.Repeat[key]))
	}
	if len(repeats) == 0 {
		repeats = append(repeats, "0")
	}
	return fmt.Sprintf("memory v2 shadow (last week, observed since %s): R4 over-E %d, victim %d (%.2f attempt-hours); current memory_pressure %d (%.2f attempt-hours); repeat %s; raises %d (%.3f GiB-hours), owners' holds %d (%.3f GiB-hours); victims after lending %d",
		w.Since.Format(time.RFC3339), w.OverE, w.Victim, w.AttemptHours, w.CurrentStops, w.CurrentAttemptHours, strings.Join(repeats, ", "), w.Raises, w.RaiseGiBHours, w.OwnersHolds, w.OwnersGiBHours, w.LendingVictims)
}

func (q *Queue) observeCurrentMemoryStops(week *MemoryShadowWeek) {
	for _, j := range q.s.Jobs {
		for _, a := range j.AttemptHistory {
			if a.EndedAt.Before(week.Since) || a.EndedAt.After(week.Through) {
				continue
			}
			if !(a.PriorityStop != nil && a.PriorityStop.Cause == memoryPressureCause || a.ExecutionCause != nil && a.ExecutionCause.Class == memoryPressureCause) {
				continue
			}
			week.CurrentStops++
			var resources *AttemptResources
			for _, f := range j.ResourceAttempts {
				if f.Attempt == a.Attempt && f.Session == a.Session && f.Machine == a.Machine {
					resources = f.Resources
					break
				}
			}
			if _, burn := attemptBurn(j, a, resources); burn != nil {
				week.CurrentAttemptHours += *burn / 3600
			}
		}
	}
}
