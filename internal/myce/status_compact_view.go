package myce

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// CompactStatus is the default GET /api/status document, also rendered by
// `myce status` text and encoded by `--json`: what runs, what waits and why, what is held, one row per
// hypha and alerts grouped by kind. Full job and machine objects stay behind
// view=snapshot, sections, job pages, detail=full and per-job reads.
type CompactStatus struct {
	Now           time.Time            `json:"now"`
	QueueRelease  RunningRelease       `json:"queue_release"`
	RetainedJobs  int                  `json:"retained_jobs"`
	Counts        map[string]int       `json:"counts"`
	Alerts        []StatusAlertGroup   `json:"alert_groups,omitempty"`
	Leases        []StatusLease        `json:"production_leases,omitempty"`
	NotQuiescent  []StatusQuiescence   `json:"not_quiescent,omitempty"`
	AlertDelivery *HealthAlertDelivery `json:"health_alert_delivery,omitempty"`
	FleetUpgrade  []string             `json:"fleet_upgrade,omitempty"`
	HostStates    map[string]string    `json:"host_states,omitempty"`
	Holds         []Hold               `json:"holds,omitempty"`
	Warning       string               `json:"warning,omitempty"`
	Hyphas        []CompactHypha       `json:"hyphas"`
	Models        []string             `json:"models,omitempty"`
	Jobs          []CompactJob         `json:"jobs,omitempty"`
	Paused        []CompactPaused      `json:"paused,omitempty"`
	Detail        string               `json:"detail"`
	Freshness     *SnapshotFreshness   `json:"freshness,omitempty"`
}
type CompactHypha struct {
	ID           string   `json:"id"`
	GPU          string   `json:"gpu"`
	FreeMemoryGB float64  `json:"free_memory_gb"`
	Availability string   `json:"availability"`
	HostRAMGB    *float64 `json:"host_ram_for_admission_gb,omitempty"`
	FreeCPU      *int     `json:"free_cpu_cores,omitempty"`
	CPU          *int     `json:"cpu_cores,omitempty"`
	Running      int      `json:"running,omitempty"`
	// Release is set only when it differs from the queue's, or is "unknown".
	Release            string              `json:"release,omitempty"`
	Preparing          []string            `json:"preparing,omitempty"`
	Copying            []string            `json:"copying,omitempty"`
	ResultPublications []ResultPublication `json:"result_publications,omitempty"`
}
type CompactJob struct {
	ID      string     `json:"id"`
	Name    string     `json:"name"`
	Project string     `json:"project"`
	Class   string     `json:"class,omitempty"`
	State   string     `json:"state"`
	Attempt int        `json:"attempt,omitempty"`
	Machine string     `json:"machine,omitempty"`
	Since   *time.Time `json:"since,omitempty"`
	Reason  string     `json:"reason,omitempty"`
	// Next is the waiting or paused job's one next action, from its hold.
	Next     string `json:"next,omitempty"`
	Delivery string `json:"delivery,omitempty"`
	// CommandStarted is false only when no attempt's command began.
	CommandStarted bool `json:"command_started"`
	// Finding is the failed or requeued job's plain-language cause and action.
	Finding *JobFinding `json:"finding,omitempty"`
}
type CompactPaused struct {
	Project string   `json:"project"`
	Reason  string   `json:"reason"`
	Next    string   `json:"next,omitempty"`
	Count   int      `json:"count"`
	IDs     []string `json:"ids"`
}

const compactStatusDetail = "status JOB_ID for one job, --sections jobs,machines for detail, --history for everything"

func compactJobGroup(j *Job) string {
	switch j.State {
	case "running", "stopping":
		return "running"
	case "queued":
		return "waiting"
	case "paused":
		return "paused"
	}
	return "other"
}

// NewCompactStatus projects a default (summary) snapshot. Reasons are whole;
// the text view bounds them to one line.
func NewCompactStatus(s Snapshot) CompactStatus {
	c := CompactStatus{Now: s.Now, QueueRelease: s.QueueRelease, Counts: map[string]int{}, Hyphas: []CompactHypha{}, Detail: compactStatusDetail, Freshness: s.Freshness}
	if s.Summary != nil {
		c.RetainedJobs, c.Alerts, c.Leases, c.NotQuiescent = s.Summary.RetainedJobs, s.Summary.Alerts, s.Summary.Leases, s.Summary.NotQuiescent
	}
	if d := s.HealthAlertDelivery; d != nil && d.Pending > 0 {
		c.AlertDelivery = d
	}
	if u := s.FleetUpgrade; u != nil && u.State != "complete" {
		c.FleetUpgrade = append(c.FleetUpgrade, fmt.Sprintf("fleet upgrade %s: release=%s state=%s", u.Operation, u.Release, u.State))
		for _, h := range u.Hosts {
			c.FleetUpgrade = append(c.FleetUpgrade, fmt.Sprintf("host %s: phase=%s wait=%s failure=%s", h.Host, h.Phase, h.Wait, h.Failure))
		}
	}
	for host, state := range s.HostStates {
		if state != "healthy" {
			if c.HostStates == nil {
				c.HostStates = map[string]string{}
			}
			c.HostStates[host] = state
		}
	}
	c.Holds = s.Holds
	c.Warning = s.Warning
	running := map[string]int{}
	for _, j := range s.Jobs {
		group := compactJobGroup(j)
		c.Counts[group]++
		if j.Machine != "" && group != "paused" {
			running[j.Machine]++
		}
	}
	for _, m := range s.Machines {
		h := CompactHypha{ID: m.ID, GPU: m.GPU, FreeMemoryGB: m.FreeMemoryGB, Availability: HyphaAvailability(m), Running: running[m.ID]}
		h.ResultPublications = m.ResultPublications
		if m.HostMemoryMeasured {
			ram := m.FreeHostMemoryGB
			h.HostRAMGB = &ram
		}
		if m.CapabilityProfile != nil {
			free, total := m.FreeCPUCores, m.EffectiveCPUCores
			h.FreeCPU, h.CPU = &free, &total
		}
		if m.Release == nil {
			h.Release = "unknown"
		} else if s.QueueRelease.Version != "" && m.Release.Version != s.QueueRelease.Version {
			h.Release = m.Release.Version
		}
		for _, r := range m.EnvironmentReports {
			h.Preparing = append(h.Preparing, r.Key+": "+r.State)
		}
		for _, r := range m.CopyReports {
			h.Copying = append(h.Copying, fmt.Sprintf("%s: %d/%d bytes", r.Model, r.Bytes, r.Total))
		}
		c.Hyphas = append(c.Hyphas, h)
	}
	for _, m := range s.LoadedModels {
		c.Models = append(c.Models, strings.TrimSpace(fmt.Sprintf("model %s on %s: %s, calls in flight %d %s", m.Model, m.Machine, m.State, m.InFlight, m.Error)))
	}
	for _, j := range s.Jobs {
		reason, next := jobReasonText(j), ""
		if h := JobHold(j); h != nil {
			next = h.Next
		}
		if compactJobGroup(j) == "paused" {
			i := slices.IndexFunc(c.Paused, func(p CompactPaused) bool { return p.Project == j.Spec.Project && p.Reason == reason })
			if i < 0 {
				c.Paused = append(c.Paused, CompactPaused{Project: j.Spec.Project, Reason: reason, Next: strings.ReplaceAll(next, j.ID, "JOB_ID")})
				i = len(c.Paused) - 1
			}
			c.Paused[i].Count++
			c.Paused[i].IDs = append(c.Paused[i].IDs, j.ID)
			continue
		}
		if reason == "running now" {
			reason = ""
		}
		row := CompactJob{ID: j.ID, Name: j.Spec.Name, Project: j.Spec.Project, Class: j.Spec.Class, State: j.State, Attempt: j.Attempt, Machine: j.Machine, Reason: reason, Next: next, Finding: j.Finding, CommandStarted: commandStarted(j)}
		if j.State == "queued" {
			since := j.WaitingSince
			if since.IsZero() {
				since = j.Queued
			}
			if !since.IsZero() {
				row.Since = &since
			}
		} else if j.StartedAt != nil {
			row.Since = j.StartedAt
		} else if j.AssignedAt != nil {
			row.Since = j.AssignedAt
		}
		if d := j.Delivery; d != nil {
			row.Delivery = strings.TrimSpace(fmt.Sprintf("attempt=%d: %s bytes=%d attempts=%d %s", d.Attempt, d.State, d.Bytes, d.Attempts, d.Cause))
		}
		c.Jobs = append(c.Jobs, row)
	}
	slices.SortStableFunc(c.Paused, func(a, b CompactPaused) int { return b.Count - a.Count })
	return c
}

// Lines renders the text view: the same fields, reasons bounded to one line.
func (c CompactStatus) Lines(full bool) []string {
	var out []string
	header := fmt.Sprintf("jobs: %d current / %d retained", c.Counts["running"]+c.Counts["waiting"]+c.Counts["paused"]+c.Counts["other"], c.RetainedJobs)
	for _, group := range []string{"running", "waiting", "paused", "other"} {
		if c.Counts[group] > 0 {
			header += fmt.Sprintf(" · %d %s", c.Counts[group], group)
		}
	}
	out = append(out, header+"; "+c.Detail)
	if len(c.Alerts) > 0 {
		total := 0
		for _, g := range c.Alerts {
			total += g.Count
		}
		out = append(out, fmt.Sprintf("alerts: %d active in %d kinds; members: myce efficiency, status --sections machines,jobs", total, len(c.Alerts)))
		for _, g := range c.Alerts {
			line := "  " + g.Kind
			if g.Count > 1 {
				line += fmt.Sprintf(" ×%d", g.Count)
			}
			if len(g.Scopes) > 0 {
				var scopes []string
				for _, scope := range slices.Sorted(maps.Keys(g.Scopes)) {
					if n := g.Scopes[scope]; n > 1 {
						scopes = append(scopes, fmt.Sprintf("%s %d", scope, n))
					} else {
						scopes = append(scopes, scope)
					}
				}
				line += " (" + strings.Join(scopes, ", ") + ")"
			}
			out = append(out, line+" since "+g.Since.UTC().Format(time.RFC3339))
		}
	}
	if c.AlertDelivery != nil {
		out = append(out, c.AlertDelivery.Summary())
	}
	if c.QueueRelease.Version != "" {
		out = append(out, fmt.Sprintf("queue release: %s (%s)", c.QueueRelease.Version, c.QueueRelease.Commit))
	}
	out = append(out, c.FleetUpgrade...)
	for _, host := range slices.Sorted(maps.Keys(c.HostStates)) {
		out = append(out, fmt.Sprintf("host %s: %s", host, c.HostStates[host]))
	}
	for _, l := range c.Leases {
		hosts := "no host can start it"
		if len(l.Hosts) > 0 {
			hosts = "holds " + strings.Join(l.Hosts, ", ")
		}
		out = append(out, fmt.Sprintf("production lease %s/%s: expires %s; %s", l.Project, l.Key, l.ExpiresAt.UTC().Format(time.RFC3339), hosts))
	}
	for _, h := range c.NotQuiescent {
		var parts []string
		for _, a := range h.Attempts {
			part := fmt.Sprintf("job %s", ShortID(a.JobID))
			if a.Name != "" {
				part += " " + a.Name
			}
			part += fmt.Sprintf(" attempt %d %s", a.Attempt, a.State)
			if a.Reason != "" {
				part += " (" + a.Reason + ")"
			}
			parts = append(parts, part)
		}
		parts = append(parts, h.Other...)
		out = append(out, fmt.Sprintf("not quiescent %s (%s): %s", h.Host, h.Mode, strings.Join(parts, "; ")))
	}
	for _, h := range c.Holds {
		out = append(out, h.Line())
	}
	if c.Warning != "" {
		out = append(out, c.Warning)
	}
	for _, h := range c.Hyphas {
		line := fmt.Sprintf("hypha %s: %s, %.1f GiB free, %s", h.ID, h.GPU, h.FreeMemoryGB, h.Availability)
		if h.HostRAMGB != nil {
			line += fmt.Sprintf("; host RAM %.1f GiB for admission", *h.HostRAMGB)
		}
		if h.CPU != nil {
			line += fmt.Sprintf("; CPU %d/%d cores free", *h.FreeCPU, *h.CPU)
		}
		if h.Running > 0 {
			line += fmt.Sprintf("; running %d", h.Running)
		}
		if h.Release != "" {
			line += "; release " + h.Release
		}
		out = append(out, line)
		for _, p := range h.ResultPublications {
			out = append(out, "  "+p.String())
		}
		for _, p := range h.Preparing {
			out = append(out, "  preparing "+p)
		}
		for _, p := range h.Copying {
			out = append(out, "  copying "+p)
		}
	}
	out = append(out, c.Models...)
	for _, j := range c.Jobs {
		id := ShortID(j.ID)
		if full {
			id = j.ID
		}
		// Derived from the encoded fields, so a decoded server view renders alike.
		display := displayState(j.State)
		if j.Attempt > 0 {
			display += fmt.Sprintf(" · attempt %d", j.Attempt)
		}
		line := fmt.Sprintf("%s %s [%s/%s] %s", id, j.Name, j.Project, j.Class, display)
		if j.Machine != "" {
			line += " on " + j.Machine
		}
		if j.Since != nil && j.State == "queued" {
			line += " waited " + elapsed(*j.Since, c.Now)
		} else if j.Since != nil && (j.State == "running" || j.State == "stopping") {
			line += " running for " + elapsed(*j.Since, c.Now)
		}
		if j.Reason != "" {
			line += "; " + compactLine(j.Reason)
		}
		out = append(out, line)
		if j.Next != "" {
			// Never clipped: the one action for this wait.
			out = append(out, "  next: "+j.Next)
		}
		if j.Finding != nil {
			// Never clipped: the action is the part an agent needs.
			out = append(out, "  "+j.Finding.Text())
		}
		if j.Delivery != "" {
			out = append(out, "  delivery "+j.Delivery)
		}
	}
	for _, p := range c.Paused {
		out = append(out, fmt.Sprintf("paused %d %s: %s", p.Count, p.Project, compactLine(p.Reason)))
		if p.Next != "" {
			out = append(out, "  next: "+p.Next)
		}
	}
	if len(c.Jobs) == 0 && len(c.Paused) == 0 {
		out = append(out, "No jobs yet.")
	}
	return out
}

// compactLine bounds one free-form reason to a line; status JOB_ID has it whole.
func compactLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > 160 {
		return string(r[:157]) + "..."
	}
	return s
}
