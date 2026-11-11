package main

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"mycelium/internal/myce"
)

// printCompactStatus renders the default summary: what runs, what waits and
// why, what is held, one line per hypha and alerts grouped by kind. Paused
// jobs fold into one line per project and reason. Unknown estimates, capability
// profiles, caches and replaced worker identities stay in --sections/--history.
func printCompactStatus(w io.Writer, s myce.Snapshot, full bool) {
	summary := s.Summary
	counts := map[string]int{}
	for _, j := range s.Jobs {
		counts[compactJobGroup(j)]++
	}
	fmt.Fprintf(w, "jobs: %d current / %d retained", len(s.Jobs), summary.RetainedJobs)
	for _, group := range []string{"running", "waiting", "paused", "other"} {
		if counts[group] > 0 {
			fmt.Fprintf(w, " · %d %s", counts[group], group)
		}
	}
	fmt.Fprintln(w, "; status JOB_ID for one job, --sections jobs,machines for detail, --history for everything")
	if len(summary.Alerts) > 0 {
		total := 0
		for _, g := range summary.Alerts {
			total += g.Count
		}
		fmt.Fprintf(w, "alerts: %d active in %d kinds; members: myce efficiency, status --sections machines,jobs\n", total, len(summary.Alerts))
		for _, g := range summary.Alerts {
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
			fmt.Fprintf(w, "%s since %s\n", line, g.Since.UTC().Format(time.RFC3339))
		}
	}
	if s.HealthAlertDelivery != nil && s.HealthAlertDelivery.Pending > 0 {
		fmt.Fprintln(w, s.HealthAlertDelivery.Summary())
	}
	if s.QueueRelease.Version != "" {
		fmt.Fprintf(w, "queue release: %s (%s)\n", s.QueueRelease.Version, s.QueueRelease.Commit)
	}
	if u := s.FleetUpgrade; u != nil && u.State != "complete" {
		fmt.Fprintf(w, "fleet upgrade %s: release=%s state=%s\n", u.Operation, u.Release, u.State)
		for _, h := range u.Hosts {
			fmt.Fprintf(w, "  host %s: phase=%s wait=%s failure=%s\n", h.Host, h.Phase, h.Wait, h.Failure)
		}
	}
	for _, host := range slices.Sorted(maps.Keys(s.HostStates)) {
		if state := s.HostStates[host]; state != "healthy" {
			fmt.Fprintf(w, "host %s: %s\n", host, state)
		}
	}
	for _, r := range s.Reservations {
		fmt.Fprintf(w, "reservation %s %s: %s; state=%s job=%s expires=%s\n", r.ID, r.Selector, r.Text, r.State, myce.ShortID(r.Job), r.ExpiresAt.UTC().Format(time.RFC3339))
	}
	for _, p := range s.ComponentExclusions {
		if p.Active {
			fmt.Fprintf(w, "exclusion %s: set by %s since %s; reason=%q\n", p.Selector, p.SetBy, p.UpdatedAt.UTC().Format(time.RFC3339), p.Reason)
		}
	}
	for _, d := range s.DeviceMaintenance {
		if d.Disabled {
			fmt.Fprintf(w, "host %s device %s (%s): disabled by operator since %s; reason=%q\n", d.Host, d.Identity, d.Platform, d.UpdatedAt.UTC().Format(time.RFC3339), d.Reason)
		}
	}
	for _, hold := range s.HostHolds {
		fmt.Fprintln(w, hold)
	}
	if s.Warning != "" {
		fmt.Fprintln(w, s.Warning)
	}
	running := map[string]int{}
	for _, j := range s.Jobs {
		if j.Machine != "" && compactJobGroup(j) != "paused" {
			running[j.Machine]++
		}
	}
	for _, m := range s.Machines {
		line := fmt.Sprintf("hypha %s: %s, %.1f GiB free, %s", m.ID, m.GPU, m.FreeMemoryGB, myce.HyphaAvailability(m))
		if m.HostMemoryMeasured {
			line += fmt.Sprintf("; host RAM %.1f GiB for admission", m.FreeHostMemoryGB)
		}
		if m.CapabilityProfile != nil {
			line += fmt.Sprintf("; CPU %d/%d cores free", m.FreeCPUCores, m.EffectiveCPUCores)
		}
		if n := running[m.ID]; n > 0 {
			line += fmt.Sprintf("; running %d", n)
		}
		if m.Release == nil {
			line += "; release unknown"
		} else if s.QueueRelease.Version != "" && m.Release.Version != s.QueueRelease.Version {
			line += "; release " + m.Release.Version
		}
		fmt.Fprintln(w, line)
		for _, r := range m.EnvironmentReports {
			fmt.Fprintf(w, "  preparing %s: %s\n", r.Key, r.State)
		}
		for _, r := range m.CopyReports {
			fmt.Fprintf(w, "  copying %s: %d/%d bytes\n", r.Model, r.Bytes, r.Total)
		}
	}
	for _, m := range s.LoadedModels {
		fmt.Fprintf(w, "model %s on %s: %s, calls in flight %d %s\n", m.Model, m.Machine, m.State, m.InFlight, m.Error)
	}
	type pausedGroup struct {
		project, reason string
		count           int
	}
	var paused []*pausedGroup
	for _, j := range s.Jobs {
		if compactJobGroup(j) == "paused" {
			reason := compactText(myce.JobReason(j))
			i := slices.IndexFunc(paused, func(g *pausedGroup) bool { return g.project == j.Spec.Project && g.reason == reason })
			if i < 0 {
				paused = append(paused, &pausedGroup{project: j.Spec.Project, reason: reason})
				i = len(paused) - 1
			}
			paused[i].count++
			continue
		}
		id := myce.ShortID(j.ID)
		if full {
			id = j.ID
		}
		line := fmt.Sprintf("%s %s [%s/%s] %s", id, j.Spec.Name, j.Spec.Project, j.Spec.Class, myce.JobState(j))
		if j.Machine != "" {
			line += " on " + j.Machine
		}
		if age := myce.JobAge(j, s.Now); age != "" {
			line += " " + age
		}
		if reason := myce.JobReason(j); reason != "" && reason != "running now" {
			line += "; " + compactText(reason)
		}
		fmt.Fprintln(w, line)
		if d := j.Delivery; d != nil {
			fmt.Fprintf(w, "  delivery attempt=%d: %s bytes=%d attempts=%d %s\n", d.Attempt, d.State, d.Bytes, d.Attempts, d.Cause)
		}
	}
	slices.SortStableFunc(paused, func(a, b *pausedGroup) int { return b.count - a.count })
	for _, g := range paused {
		fmt.Fprintf(w, "paused %d %s: %s\n", g.count, g.project, g.reason)
	}
	if len(s.Jobs) == 0 {
		fmt.Fprintln(w, "No jobs yet.")
	}
}

func compactJobGroup(j *myce.Job) string {
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

// compactText bounds one free-form reason to a line; status JOB_ID has it whole.
func compactText(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > 160 {
		return string(r[:157]) + "..."
	}
	return s
}
