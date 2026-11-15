package myce

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// ProjectRestrictions shows one project what its own hand-steering costs it
// (MY-31: pins are optional and situational; agents state needs, not
// placement). Counts cover retained jobs only, from fields the queue already
// keeps. Advice names platforms, never hosts.
type ProjectRestrictions struct {
	Project string `json:"project"`
	Jobs    int    `json:"jobs"`
	// Jobs naming hosts (allowed_machines or excluded_machines).
	HostRestricted int `json:"host_restricted"`
	// Union of host lists and targets, counting each retained job once.
	Restricted int `json:"restricted"`
	// Plain queue wait for restricted jobs, not a causal cost estimate.
	RestrictedWaitSeconds float64 `json:"restricted_wait_seconds"`
	// Distinct currently waiting restricted jobs with an unrestricted fit.
	StrandedJobs int `json:"stranded_jobs"`
	// Jobs with targets, and those of them below the production tier.
	Targeted              int `json:"targeted"`
	NonProductionTargeted int `json:"non_production_targeted"`
	// Queue wait while a host the job's own host lists or targets excluded
	// had room for it, and the platforms of those hosts right now.
	StrandedSeconds   float64  `json:"stranded_seconds"`
	StrandedPlatforms []string `json:"stranded_platforms,omitempty"`
	// Typed figures, and the mean typed minus learned peak where both exist.
	TypedMemory        int      `json:"typed_memory"`
	MemoryCompared     int      `json:"memory_compared"`
	MemoryOverPeakGB   *float64 `json:"memory_over_peak_gb" jsonschema:"nullable"`
	TypedCores         int      `json:"typed_cores"`
	CoresCompared      int      `json:"cores_compared"`
	CoresOverPeakCores *float64 `json:"cores_over_peak" jsonschema:"nullable"`
	Advice             []string `json:"advice,omitempty"`
}

func (q *Queue) projectRestrictions(now time.Time, stranded []StrandedDemand) []ProjectRestrictions {
	byProject := map[string]*ProjectRestrictions{}
	memorySum, coreSum := map[string]float64{}, map[string]float64{}
	row := func(project string) *ProjectRestrictions {
		p := byProject[project]
		if p == nil {
			p = &ProjectRestrictions{Project: project}
			byProject[project] = p
		}
		return p
	}
	for _, j := range q.s.Jobs {
		s := j.Spec
		p := row(s.Project)
		p.Jobs++
		if len(s.AllowedMachines)+len(s.ExcludedMachines)+len(s.Targets) > 0 {
			p.Restricted++
			p.RestrictedWaitSeconds += restrictionQueueWait(j, now)
		}
		if len(s.AllowedMachines) > 0 || len(s.ExcludedMachines) > 0 {
			p.HostRestricted++
		}
		if len(s.Targets) > 0 {
			p.Targeted++
			if s.Priority != "production" {
				p.NonProductionTargeted++
			}
		}
		if e := j.Efficiency; e != nil {
			seconds := e.StrandedSeconds
			if e.Stranded && mutableJob(j) && now.After(e.At) {
				seconds += now.Sub(e.At).Seconds()
			}
			p.StrandedSeconds += seconds
		}
		for _, r := range jobResources(j) {
			switch r.Dimension {
			case "host_memory_gb":
				if r.Declared != nil {
					p.TypedMemory++
					if r.Learned != nil {
						p.MemoryCompared++
						memorySum[s.Project] += *r.Declared - *r.Learned
					}
				}
			case "cpu_cores":
				if r.Declared != nil {
					p.TypedCores++
					if r.Learned != nil {
						p.CoresCompared++
						coreSum[s.Project] += *r.Declared - *r.Learned
					}
				}
			}
		}
	}
	platforms := map[string]map[string]bool{}
	strandedJobs := map[string]bool{}
	for _, s := range stranded {
		if platforms[s.Project] == nil {
			platforms[s.Project] = map[string]bool{}
		}
		platforms[s.Project][s.Platform] = true
		if !strandedJobs[s.JobID] {
			j := q.s.Jobs[s.JobID]
			if j != nil && len(j.Spec.AllowedMachines)+len(j.Spec.ExcludedMachines)+len(j.Spec.Targets) > 0 {
				row(s.Project).StrandedJobs++
				strandedJobs[s.JobID] = true
			}
		}
	}
	var out []ProjectRestrictions
	for _, project := range slices.Sorted(maps.Keys(byProject)) {
		p := byProject[project]
		p.StrandedPlatforms = slices.Sorted(maps.Keys(platforms[project]))
		if p.MemoryCompared > 0 {
			v := memorySum[project] / float64(p.MemoryCompared)
			p.MemoryOverPeakGB = &v
		}
		if p.CoresCompared > 0 {
			v := coreSum[project] / float64(p.CoresCompared)
			p.CoresOverPeakCores = &v
		}
		p.Advice = p.advice()
		out = append(out, *p)
	}
	return out
}

func restrictionQueueWait(j *Job, now time.Time) float64 {
	if e := j.Efficiency; e != nil && e.WaitSeconds != nil {
		var seconds float64
		for _, wait := range e.WaitSeconds {
			seconds += wait
		}
		// Only project the wait clock. Advancing all telemetry would also
		// update resource integrals, whose slices belong to the recorded job.
		if mutableJob(j) && e.Blocker != "" && now.After(e.At) {
			seconds += now.Sub(e.At).Seconds()
		}
		return seconds
	}
	// Older jobs can lack sampled wait telemetry. Bound the current queue
	// episode by assignment/start/finish rather than counting execution as wait.
	end := now
	for _, at := range []*time.Time{j.AssignedAt, j.StartedAt, j.FinishedAt} {
		if at != nil && at.Before(end) {
			end = *at
		}
	}
	if j.Queued.IsZero() || !end.After(j.Queued) {
		return 0
	}
	return end.Sub(j.Queued).Seconds()
}

func (p *ProjectRestrictions) advice() []string {
	var advice []string
	if p.HostRestricted > 0 {
		line := "drop allowed_machines and excluded_machines; state the need (platform, memory, tools) and Mycelium places the work"
		if p.StrandedSeconds > 0 {
			line = fmt.Sprintf("your restrictions cost %.1f queue hours while an excluded host had room; ", p.StrandedSeconds/3600) + line
		}
		advice = append(advice, line)
	} else if p.StrandedSeconds > 0 {
		advice = append(advice, fmt.Sprintf("your targets cost %.1f queue hours while an excluded host had room; widen or drop them", p.StrandedSeconds/3600))
	}
	if len(p.StrandedPlatforms) > 0 {
		advice = append(advice, "waiting now although a "+strings.Join(p.StrandedPlatforms, " or ")+" host has room")
	}
	if p.NonProductionTargeted > 0 {
		advice = append(advice, "drop targets on non-production entries; keep them only where output parity matters")
	}
	if p.TypedMemory > 0 {
		advice = append(advice, "stop typing host_memory_gb; use needs.memory: auto and Mycelium learns it")
	}
	// Cores are charged whole, so a mean gap under one core changes nothing.
	if p.CoresOverPeakCores != nil && *p.CoresOverPeakCores >= 1 {
		advice = append(advice, "lower cpu_cores to what the work uses; typed figures sit above the observed peak")
	}
	return advice
}

func (p ProjectRestrictions) lines(indent string) []string {
	if p.Jobs == 0 {
		return nil
	}
	share := func(n int) string { return fmt.Sprintf("%d/%d (%.0f%%)", n, p.Jobs, 100*float64(n)/float64(p.Jobs)) }
	memory, cores := "no learned peak yet", "no observed peak yet"
	if p.MemoryOverPeakGB != nil {
		memory = fmt.Sprintf("%+.1f GiB from learned peak over %d", *p.MemoryOverPeakGB, p.MemoryCompared)
	}
	if p.CoresOverPeakCores != nil {
		cores = fmt.Sprintf("%+.1f from observed peak over %d", *p.CoresOverPeakCores, p.CoresCompared)
	}
	lines := []string{
		fmt.Sprintf("%sproject %s restrictions: %s retained jobs; plain queue wait %.1f hours; %d stranded jobs now (unrestricted capacity fits)", indent, p.Project, share(p.Restricted), p.RestrictedWaitSeconds/3600, p.StrandedJobs),
		fmt.Sprintf("%sproject %s steering: hosts named %s; targets %s (%d non-production); %.1f queue hours waited while an excluded host had room", indent, p.Project, share(p.HostRestricted), share(p.Targeted), p.NonProductionTargeted, p.StrandedSeconds/3600),
		fmt.Sprintf("%sproject %s typed sizing: host_memory_gb %s, %s; cpu_cores %s, %s", indent, p.Project, share(p.TypedMemory), memory, share(p.TypedCores), cores),
	}
	if p.StrandedSeconds == 0 {
		lines = append(lines, indent+"  unrestricted counterfactual unavailable; plain wait does not establish restriction cost")
	} else {
		lines = append(lines, indent+"  unrestricted capacity was observed during stranded wait; this is not a predicted start time or an exact wait saving")
	}
	for _, a := range p.Advice {
		lines = append(lines, indent+"  advice: "+a)
	}
	return lines
}

// ProjectLines is the compact per-project view: waits, steering cost and
// advice, without host rows.
func (r *EfficiencyReport) ProjectLines(project string) []string {
	if r == nil {
		return nil
	}
	lines := []string{fmt.Sprintf("efficiency for project %s at %s", project, r.At.UTC().Format(time.RFC3339))}
	for _, p := range r.Projects {
		if p.Project != project {
			continue
		}
		for _, category := range slices.Sorted(maps.Keys(p.Seconds)) {
			lines = append(lines, fmt.Sprintf("  %s wait %.1f hours", category, p.Seconds[category]/3600))
		}
	}
	found := false
	for _, p := range r.Restrictions {
		if p.Project == project {
			found = true
			lines = append(lines, p.lines("  ")...)
		}
	}
	if !found {
		lines = append(lines, "  no retained jobs for this project")
	}
	return lines
}

// A platform label for advice: operating system, architecture and
// accelerator kind, never the host name.
func machinePlatformLabel(m *Machine) string {
	label := "unknown platform"
	if p := m.CapabilityProfile; p != nil && p.OS != "" {
		label = p.OS
		if p.Arch != "" {
			label += "/" + p.Arch
		}
	}
	if m.GPU != "" && m.GPU != "cpu" {
		label += " " + m.GPU
	}
	return label
}
