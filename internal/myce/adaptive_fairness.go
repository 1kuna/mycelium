package myce

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"time"
)

type fairScope [2]string

type fairInterval struct {
	key, allocation    string
	age, covered, rate float64
}

type fairCycle struct {
	wait, waitRate, quantum float64
	entries                 map[string]FairUsage
	intervals               []fairInterval
}

func fairAllocationKey(m *Machine, work, attempt string) string {
	key, _ := json.Marshal([5]string{m.ID, m.Session, m.Device, work, attempt})
	return string(key)
}

// A cycle spans a complete project round: oldest wait age once per demanding
// project, the longest covered active allocation age, or the latest completed
// allocation's covered duration. Completion supplies a measured positive scale;
// before that first sample, preserve history and account service without decay.
// Active observations are attempt/session-specific and survive unknown gaps.
func (q *Queue) accountFairHistory(from, now time.Time) {
	groups := map[fairScope]*fairCycle{}
	group := func(scope fairScope) *fairCycle {
		if groups[scope] == nil {
			groups[scope] = &fairCycle{entries: map[string]FairUsage{}}
		}
		return groups[scope]
	}
	for key, u := range q.s.FairUsage {
		var parts [3]string
		if json.Unmarshal([]byte(key), &parts) != nil {
			continue
		}
		g := group(fairScope{parts[0], parts[1]})
		g.entries[key] = u
		g.quantum = max(g.quantum, u.QuantumSeconds)
	}
	waits := map[string]float64{}
	observeWait := func(j *Job) {
		if j.State != "queued" || !dependencyReady(j) || cancelled(j) || q.capReason(j) != "" {
			return
		}
		since := j.WaitingSince
		if since.IsZero() {
			since = j.Queued
		}
		if since.IsZero() {
			return
		}
		key := q.fairKey(j.Spec)
		age := max(0, from.Sub(since).Seconds())
		g := group(fairScope{q.priorityMatch(j.Spec).Tier, fairResourceFamily(j.Spec)})
		if old, exists := waits[key]; exists {
			g.wait += max(0, age-old)
		} else {
			g.wait += age
			g.waitRate++
		}
		waits[key] = max(waits[key], age)
		if _, exists := g.entries[key]; !exists {
			g.entries[key] = FairUsage{}
		}
	}
	for _, j := range q.liveJobs() {
		observeWait(j)
	}
	for _, c := range q.calls {
		if c.Machine == "" && c.Error == "" {
			j := callJob(c)
			j.State = "queued"
			observeWait(j)
		}
	}
	span := now.Sub(from).Seconds()
	for _, m := range q.plannerMachines() {
		covered := 0.0
		if !m.Gone && !q.unreported[m.ID] && m.MemoryError == "" {
			end := minTime(now, m.LastSeen.Add(q.machineLossTimeout(m.ID)))
			covered = max(0, end.Sub(from).Seconds())
		}
		// Retain a still-custodied allocation's covered age even when its
		// worker is unreported. Unknown time adds neither age nor usage.
		for _, r := range q.deviceUsage(m) {
			if r.Fair <= 0 || r.Project == "" {
				continue
			}
			key := fairUsageKey(r.Tier, r.Family, r.Project)
			g := group(fairScope{r.Tier, r.Family})
			u := g.entries[key]
			g.entries[key] = u
			g.intervals = append(g.intervals, fairInterval{key, r.FairAllocation, u.Active[r.FairAllocation], covered, r.Fair})
		}
	}
	for _, g := range groups {
		active := map[string]map[string]float64{}
		for _, r := range g.intervals {
			if active[r.key] == nil {
				active[r.key] = map[string]float64{}
			}
			active[r.key][r.allocation] = r.age + r.covered
		}
		completed := 0.0
		for key, u := range g.entries {
			for allocation, age := range u.Active {
				if _, exists := active[key][allocation]; !exists {
					completed = max(completed, age)
				}
			}
		}
		if completed > 0 {
			g.quantum = completed
		}
		// Coverage expiry changes rates and service-age slopes. Between these
		// boundaries the horizon is the maximum of affine functions of time.
		boundaries := []float64{0, span}
		for _, r := range g.intervals {
			if r.covered > 0 && r.covered < span {
				boundaries = append(boundaries, r.covered)
			}
		}
		slices.Sort(boundaries)
		boundaries = slices.Compact(boundaries)
		for i := 1; i < len(boundaries); i++ {
			start, end := boundaries[i-1], boundaries[i]
			lines := []fairLine{{g.quantum, 0}, {g.wait, g.waitRate}}
			fixed, growing := g.quantum, -1.0
			rates := map[string]float64{}
			for _, r := range g.intervals {
				if r.covered > start {
					growing = max(growing, r.age)
					rates[r.key] += r.rate
				} else {
					fixed = max(fixed, r.age+r.covered)
				}
			}
			lines[0].base = fixed
			if growing >= 0 {
				lines = append(lines, fairLine{growing, 1})
			}
			cuts := []float64{start, end}
			for a := range lines {
				for b := 0; b < a; b++ {
					if lines[a].slope == lines[b].slope {
						continue
					}
					at := (lines[b].base - lines[a].base) / (lines[a].slope - lines[b].slope)
					if at > start && at < end {
						cuts = append(cuts, at)
					}
				}
			}
			slices.Sort(cuts)
			cuts = slices.Compact(cuts)
			for k := 1; k < len(cuts); k++ {
				lo, hi := cuts[k-1], cuts[k]
				chosen := lines[0]
				for _, line := range lines[1:] {
					if line.at((lo+hi)/2) > chosen.at((lo+hi)/2) {
						chosen = line
					}
				}
				factor, service := 1.0, hi-lo
				if g.quantum > 0 {
					factor, service = chosen.integral(lo, hi)
				}
				for key, u := range g.entries {
					u.Seconds = u.Seconds*factor + rates[key]*service
					g.entries[key] = u
				}
			}
		}
		serviceAge := 0.0
		for _, r := range g.intervals {
			serviceAge = max(serviceAge, r.age+r.covered)
		}
		for key, u := range g.entries {
			u.WaitSeconds, u.ServiceSeconds = g.wait+g.waitRate*span, serviceAge
			u.QuantumSeconds = g.quantum
			u.HorizonSeconds = max(g.quantum, u.WaitSeconds, serviceAge)
			if u.HorizonSeconds == 0 {
				u.HorizonSeconds = q.s.FairUsage[key].HorizonSeconds
			}
			u.Active, u.At = active[key], now
			q.s.FairUsage[key] = u
		}
	}
}

type fairLine struct{ base, slope float64 }

func (l fairLine) at(t float64) float64 { return l.base + l.slope*t }

// Solve dU/dt = rate - ln(2)*U/H(t) analytically. This composes exactly
// when an unchanged interval is split by incidental reads or check-ins.
func (l fairLine) integral(from, until float64) (factor, service float64) {
	h0 := l.at(from)
	if l.slope == 0 {
		exponent := -math.Ln2 * (until - from) / h0
		return math.Exp(exponent), -math.Expm1(exponent) * h0 / math.Ln2
	}
	logRatio := math.Log1p(l.slope * (until - from) / h0)
	factor = math.Exp(-math.Ln2 / l.slope * logRatio)
	service = (l.slope*(until-from) - h0*math.Expm1(-math.Ln2/l.slope*logRatio)) / (l.slope + math.Ln2)
	return
}

func (s *candidateSet) fairExplanation(j *Job) string {
	q := s.q
	projects := s.fairProjects(j)
	total, weights, largest := 0.0, 0.0, 1.0
	for p := range projects {
		largest = max(largest, q.weight(p))
	}
	for p := range projects {
		total += decayed(q.s.FairUsage[fairUsageKey(q.priorityMatch(j.Spec).Tier, fairResourceFamily(j.Spec), p)], q.clock.Now())
		weights += q.weight(p) / largest
	}
	u := q.s.FairUsage[q.fairKey(j.Spec)]
	usage := decayed(u, q.clock.Now())
	share := total * (q.weight(j.Spec.Project) / largest) / weights
	return fmt.Sprintf("fair share %s/%s: retained %.6gs, weighted share %.6gs, debt %+.6gs (positive ahead); horizon %.6gs, wait %.6gs, observed service age %.6gs, completed service quantum %.6gs; %d competing projects", q.priorityMatch(j.Spec).Tier, fairResourceFamily(j.Spec), usage, share, usage-share, u.HorizonSeconds, u.WaitSeconds, u.ServiceSeconds, u.QuantumSeconds, len(projects))
}
