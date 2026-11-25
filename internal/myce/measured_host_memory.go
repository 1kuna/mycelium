package myce

import "errors"

type HostBudgetSuggestion struct {
	HostMemoryDerivation
	SuggestedGB   float64 `json:"suggested_gb"`
	Worker        string  `json:"worker"`
	QueueOverhead string  `json:"queue_overhead,omitempty"`
	DryRun        bool    `json:"dry_run"`
}

// MeasuredBudgetSuggestion previews the lowest allowance the queue would
// send to any fresh worker, before worker acknowledgement and live RAM limits.
func (s HostMaintenanceStatus) MeasuredBudgetSuggestion() (HostBudgetSuggestion, error) {
	if s.Policy.MemoryBudget == nil || s.Policy.MemoryBudget.GiB <= 0 {
		return HostBudgetSuggestion{}, errors.New("measured memory mode requires a positive authored fallback")
	}
	policy := *s.Policy.MemoryBudget
	policy.Measured = true
	var suggestion HostBudgetSuggestion
	for _, worker := range s.Workers {
		if !worker.Fresh {
			continue
		}
		budget, d := deriveMeasuredHostMemoryBudget(&worker.Machine, policy, s.Policy.role(), s.ExternalMemory)
		if !suggestion.DryRun || budget.GiB < suggestion.SuggestedGB {
			suggestion = HostBudgetSuggestion{
				HostMemoryDerivation: *d,
				SuggestedGB:          budget.GiB,
				Worker:               worker.Machine.ID,
				DryRun:               true,
			}
		}
	}
	if !suggestion.DryRun {
		return suggestion, errors.New("no fresh worker reports")
	}
	if suggestion.Source == "quiescent_v1" {
		suggestion.QueueOverhead = "included_in_idle_baseline"
	}
	return suggestion, nil
}

func modelReportsQuiescent(reports []ModelReport) bool {
	for _, r := range reports {
		// Failed is published only after successful cleanup. Failed unloads
		// keep their separate custody state, as do all live/unknown states.
		if r.InFlight != 0 || r.State != "unloaded" && r.State != "failed" {
			return false
		}
	}
	return true
}

// Whole-host quiescence is independent of process RSS attribution. A worker
// asserts local zero custody; the queue excludes every outstanding owner and
// legacy sibling. Never infer idle from a reservation or an absent RSS sample.
func (q *Queue) hostMemoryQuiescent(m *Machine) bool {
	o := m.MemoryObservation
	if !validHostMemoryObservation(o) || o.BaselineVersion != 1 || !o.Quiescent || !m.HostMemoryMeasured || m.HostMemoryError != "" || hostPreparing(m) {
		return false
	}
	for _, other := range q.s.Machines {
		if other.ID != m.ID && hostID(other) == hostID(m) && !other.Gone && other.Replacement == nil {
			return false
		}
	}
	return len(q.hostObligations(hostID(m))) == 0
}

// GiB remains the authored fallback, not a learned value written back to policy.
// Legacy zero retains its uncapped meaning; opting in requires a positive fallback.
func (q *Queue) measuredHostMemoryBudget(m *Machine, policy HostMemoryBudget) (HostMemoryBudget, *HostMemoryDerivation) {
	if !policy.Measured {
		return policy, nil
	}
	host := hostID(m)
	return deriveMeasuredHostMemoryBudget(m, policy, q.s.HostMaintenance[host].role(), q.externalMemoryStatus(host))
}

func deriveMeasuredHostMemoryBudget(m *Machine, policy HostMemoryBudget, role string, h *ExternalMemoryHistory) (HostMemoryBudget, *HostMemoryDerivation) {
	d := &HostMemoryDerivation{Source: "authored_fallback", FallbackGB: policy.GiB, AllowanceGB: policy.GiB, Reason: "no qualified quiescent memory history", Role: role}
	o := m.MemoryObservation
	if h == nil || h.Quiescent == nil || h.Quiescent.Week.Samples == 0 || !validHostMemoryObservation(o) || o.BaselineVersion != 1 {
		return policy, d
	}
	d.Source, d.Reason = "quiescent_v1", ""
	d.History = h.Quiescent.Week
	d.TotalGB, d.BaselineMaxGB = o.TotalGB, d.History.MaxGB
	if m.CapabilityProfile != nil && m.CapabilityProfile.SystemMemoryBytes > 0 {
		d.TotalGB = min(d.TotalGB, float64(m.CapabilityProfile.SystemMemoryBytes)/(1<<30))
	}
	// Raw availability already excludes unavailable OS pages (Linux's kernel
	// watermarks, or Darwin's active/wired/compressor pages). Keep the operator
	// reserve floor, plus observed idle peak-to-typical variation for new bursts.
	// The baseline includes idle Mycelium overhead; nothing is subtracted via RSS.
	d.ReserveGB = o.ReserveGB
	if d.Role != hostRoleInteractive && !ownerGated(m) {
		// MY-45: capacity is live availability minus one measured margin. The
		// idle footprint (OS, services, drivers and the idle worker, all in the
		// physical baseline) is kept back once: its p50 on an on-demand node,
		// its p95 on the services host, whose always-on services are persistent
		// external demand. The margin is the larger of the measured burst above
		// that baseline and the explicit reserve, which remains the operator's
		// named override for overshoot not yet measured. Never below the
		// observed idle maximum; neither burst nor reserve is stacked twice.
		d.Dedicated, d.BaselineQuantileGB = true, d.History.P50GB
		if d.Role == hostRoleServices {
			d.BaselineQuantileGB = d.History.P95GB
		}
		burst := d.History.MaxGB - d.BaselineQuantileGB
		d.ReserveSource = "explicit_reserve"
		if burst > d.ReserveGB {
			d.ReserveGB, d.ReserveSource = burst, "idle_burst"
		}
		d.AllowanceGB = max(0, d.TotalGB-d.BaselineQuantileGB-d.ReserveGB)
	} else {
		if d.History.Samples > 1 {
			d.ReserveGB = max(d.ReserveGB, d.History.MaxGB-d.History.P50GB)
		}
		d.AllowanceGB = max(0, d.TotalGB-d.BaselineMaxGB-d.ReserveGB)
	}
	budget, _ := hostMemoryAllowance(d.AllowanceGB)
	budget.Measured = true
	return budget, d
}
