package myce

import (
	"fmt"
	"time"
)

// ResourceAttempts already retains one cumulative observation per exact attempt.
// Reduce that bounded history only on the detached maintenance observation: no
// second raw history, no counter deltas across sessions, and no work on GET.
// These are full observed attempt windows, distinct from queue-clock reservation
// integrals that begin when efficiency accounting first observes custody.
func (q *Queue) efficiencyResourceAllocation(j *Job, f AttemptResourceFacts) EfficiencyAllocation {
	a := EfficiencyAllocation{Host: f.Machine, Work: profileMemoryIdentity(j), Basis: "unknown"}
	if j.Efficiency != nil {
		a.Work = j.Efficiency.Work
	}
	if m := q.s.Machines[f.Machine]; m != nil {
		a.Host = hostID(m)
	}
	if f.Attempt == j.Attempt && f.Machine == j.Machine && f.Session == j.Session {
		a.Basis = efficiencyBasis(j)
		if j.Placement != nil && j.Placement.Attempt == f.Attempt {
			a.Host = j.Placement.Host
		}
	}
	for _, o := range j.AttemptHistory {
		if o.Attempt == f.Attempt && o.Machine == f.Machine && o.Session == f.Session && o.Placement != nil {
			a.Host = o.Placement.Host
		}
	}
	for _, m := range j.MemoryAttempts {
		if m.Attempt == f.Attempt && m.Machine == f.Machine && m.Session == f.Session && m.MemoryEnvelope != nil {
			a.Basis = m.MemoryEnvelope.Basis
		}
	}
	r := f.Resources
	if r == nil {
		return a
	}
	if r.CPU.UsageSeconds != nil {
		n := *r.CPU.UsageSeconds
		a.MeasuredCPUSeconds = &n
		a.CPUObservedElapsedSeconds = r.ElapsedSeconds
		a.CPUObservedAttempts = 1
		a.CPULowerBound = r.CPU.LowerBound || r.CPU.Unavailable != "" ||
			f.Attempt == j.Attempt && f.Machine == j.Machine && f.Session == j.Session && !terminal(j.State)
	}
	a.ReadBytes, a.WriteBytes, a.ReadOps, a.WriteOps = r.IO.ReadBytes, r.IO.WriteBytes, r.IO.ReadOps, r.IO.WriteOps
	if a.ReadBytes != nil || a.WriteBytes != nil || a.ReadOps != nil || a.WriteOps != nil {
		a.IOObservedAttempts = 1
		a.IOLowerBound = r.IO.Unavailable != "" || a.ReadBytes == nil || a.WriteBytes == nil || a.ReadOps == nil || a.WriteOps == nil ||
			f.Attempt == j.Attempt && f.Machine == j.Machine && f.Session == j.Session && !terminal(j.State)
	}
	return a
}

func attemptIOStatus(r *AttemptResources) string {
	if r == nil {
		return "unknown"
	}
	if r.IO.Unavailable != "" {
		return "unknown: " + r.IO.Unavailable
	}
	if r.IO.ReadBytes != nil || r.IO.WriteBytes != nil || r.IO.ReadOps != nil || r.IO.WriteOps != nil {
		return "observed attempt counters"
	}
	return "unknown"
}

func (q *Queue) efficiencyIOPressure(now time.Time) map[string]*IOPressure {
	byHost := map[string]*IOPressure{}
	for _, m := range q.s.Machines {
		if m.IOPressure == nil || m.Replacement != nil || q.migrationHistoricalMachine(m) {
			continue
		}
		host := hostID(m)
		if old := byHost[host]; old != nil && !m.IOPressure.ReceivedAt.After(old.ReceivedAt) {
			continue
		}
		p := *m.IOPressure
		p.AgeSeconds += max(0, now.Sub(p.ReceivedAt).Seconds())
		if p.Unavailable == "" && (m.Gone || p.ReceivedAt.IsZero() || now.Before(p.ReceivedAt) ||
			p.AgeSeconds >= q.machineLossTimeout(m.ID).Seconds() || !now.Before(m.LastSeen.Add(q.machineLossTimeout(m.ID)))) {
			p.Unavailable = "stale"
		}
		byHost[host] = &p
	}
	return byHost
}

func efficiencyCPUWords(a ResourceIntegral) string {
	if a.MeasuredCPUSeconds == nil {
		return "CPU use unknown"
	}
	bound := ""
	if a.CPULowerBound {
		bound = " (lower bound)"
	}
	return fmt.Sprintf("CPU %.1f core-s%s over %.1fs in %d observed attempt windows; reservation %.1f core-s since accounting began",
		*a.MeasuredCPUSeconds, bound, a.CPUObservedElapsedSeconds, a.CPUObservedAttempts, a.ReservedCPUSeconds)
}

func efficiencyIOWords(a ResourceIntegral) string {
	read, write := "unknown", "unknown"
	if a.ReadBytes != nil {
		read = fmt.Sprintf("%d B", *a.ReadBytes)
	}
	if a.WriteBytes != nil {
		write = fmt.Sprintf("%d B", *a.WriteBytes)
	}
	bound := ""
	if a.IOLowerBound {
		bound = " (lower bound)"
	}
	return fmt.Sprintf("I/O read %s, write %s%s", read, write, bound)
}
