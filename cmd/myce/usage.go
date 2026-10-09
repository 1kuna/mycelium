package main

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/myce"
)

func usageDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		days, e := strconv.ParseFloat(strings.TrimSuffix(s, "d"), 64)
		if e != nil || math.IsNaN(days) || math.IsInf(days, 0) || days <= 0 || days > 90 {
			return 0, fmt.Errorf("since must be between 0 and 90 days")
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	d, e := time.ParseDuration(s)
	if e != nil || d <= 0 || d > 90*24*time.Hour {
		return 0, fmt.Errorf("since must be a positive duration up to 90d")
	}
	return d, nil
}
func printUsage(w io.Writer, r myce.UsageReport, by string) error {
	if by != "project" && by != "machine" {
		return fmt.Errorf("by must be project or machine")
	}
	fmt.Fprintf(w, "GPU usage %s to %s (observed online time)\n", r.Since.Format(time.RFC3339), r.Until.Format(time.RFC3339))
	if by == "project" {
		for _, p := range r.Projects {
			fmt.Fprintf(w, "project %s: %.3f GPU-hours; %.3f pinned hours\n", p.Name, p.GPUHours, p.PinnedHours)
		}
	}
	// Machine occupancy is always shown: it answers whether pooling fills idle time.
	for _, m := range r.Machines {
		fmt.Fprintf(w, "machine %s: %.3f GPU-hours; %.1f%% busy; %.3f idle hours; %.3f pinned hours\n", m.Name, m.GPUHours, m.BusyPercent, m.IdleHours, m.PinnedHours)
	}
	if len(r.Machines) == 0 {
		fmt.Fprintln(w, "No observed usage yet.")
	}
	fmt.Fprintf(w, "Preemptions: %d\n", len(r.Preemptions))
	for _, p := range r.Preemptions {
		fmt.Fprintf(w, "%s %s/%s stopped for %s\n", p.At.Format(time.RFC3339), p.Project, p.Job, p.For)
	}
	for _, s := range r.ProductionStarts {
		fmt.Fprintf(w, "production %s: %.3fs time-to-start at %s\n", s.Job, s.Seconds, s.At.Format(time.RFC3339))
	}
	return nil
}
