package myce

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

type fairScope [2]string // stable tier name, resource family
// wait is the sum of oldest waiting ages, once per demanding project.
// This retains a full project round as the number of contenders grows.
type fairCycle struct{ wait, service float64 }
type fairInterval struct {
	key         string
	scope       fairScope
	from, until time.Time
	rate        float64
}

// Remember one current demand/service cycle, rather than a fleet's daily rhythm.
// Long waits retain the service received by competitors; short bursts forget old
// debt on their own timescale. An active allocation's observed age keeps a long
// run from being treated as a burst. Idle ledgers reuse their last measured cycle.
// There is no timer floor, ceiling, or reporting-retention cutoff on fair usage.
func (q *Queue) accountFairHistory(from, now time.Time) {
	cycles := map[fairScope]fairCycle{}
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
		key := fairScope{q.priorityMatch(j.Spec).Tier, fairResourceFamily(j.Spec)}
		cycle := cycles[key]
		projectKey := q.fairKey(j.Spec)
		age := max(0, now.Sub(since).Seconds())
		cycle.wait += max(0, age-waits[projectKey])
		waits[projectKey] = max(waits[projectKey], age)
		cycles[key] = cycle
		if _, ok := q.s.FairUsage[projectKey]; !ok {
			q.s.FairUsage[projectKey] = FairUsage{}
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
	intervals := []fairInterval{}
	for _, m := range q.plannerMachines() {
		if m.Gone || q.unreported[m.ID] || m.MemoryError != "" {
			continue
		}
		end := minTime(now, m.LastSeen.Add(q.machineLossTimeout(m.ID)))
		if !end.After(from) {
			continue
		}
		for _, r := range q.deviceUsage(m) {
			if r.Fair <= 0 || r.Project == "" {
				continue
			}
			scope := fairScope{r.Tier, r.Family}
			cycle := cycles[scope]
			cycle.service = max(cycle.service, end.Sub(from).Seconds())
			if !r.FairSince.IsZero() {
				cycle.service = max(cycle.service, end.Sub(r.FairSince).Seconds())
			}
			cycles[scope] = cycle
			intervals = append(intervals, fairInterval{fairUsageKey(r.Tier, r.Family, r.Project), scope, from, end, r.Fair})
		}
	}
	// Apply the same horizon to all projects in a tier/family, including absent
	// projects. Otherwise switching demand could resurrect selectively aged debt.
	for key, u := range q.s.FairUsage {
		var parts [3]string
		if json.Unmarshal([]byte(key), &parts) != nil {
			continue
		}
		cycle := cycles[fairScope{parts[0], parts[1]}]
		if horizon := max(cycle.wait, cycle.service); horizon > 0 {
			u.HorizonSeconds = horizon
		}
		u.WaitSeconds, u.ServiceSeconds = cycle.wait, cycle.service
		u.Seconds, u.At = decayed(u, now), now
		q.s.FairUsage[key] = u
	}
	for _, r := range intervals {
		u := q.s.FairUsage[r.key]
		cycle := cycles[r.scope]
		u.HorizonSeconds = max(cycle.wait, cycle.service)
		u.WaitSeconds, u.ServiceSeconds = cycle.wait, cycle.service
		k := math.Ln2 / u.HorizonSeconds
		// Exact constant-horizon integral over covered service, independent of
		// the minute buckets used by the historical reporting API.
		u.Seconds += -math.Expm1(-k*r.until.Sub(r.from).Seconds()) / k * math.Exp(-k*now.Sub(r.until).Seconds()) * r.rate
		u.At = now
		q.s.FairUsage[r.key] = u
	}
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
	return fmt.Sprintf("fair share %s/%s: retained %.6gs, weighted share %.6gs, debt %+.6gs (positive ahead); horizon %.6gs, wait %.6gs, observed service age %.6gs; %d competing projects", q.priorityMatch(j.Spec).Tier, fairResourceFamily(j.Spec), usage, share, usage-share, u.HorizonSeconds, u.WaitSeconds, u.ServiceSeconds, len(projects))
}
