package myce

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
	d := &HostMemoryDerivation{Source: "authored_fallback", FallbackGB: policy.GiB, AllowanceGB: policy.GiB, Reason: "no qualified quiescent memory history"}
	h := q.externalMemoryStatus(hostID(m))
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
	policyRole := q.s.HostMaintenance[hostID(m)]
	d.Role = policyRole.role()
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
