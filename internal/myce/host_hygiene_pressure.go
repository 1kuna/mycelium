package myce

// HostPressureEvaluation explains the queue's Linux PSI admission decision.
// PressureLimit is in the worker's raw PSI percent units. No fleet cutoff is
// used: all three bands come from this worker's retained healthy observations.
type HostPressureEvaluation struct {
	PressureLimit       float64  `json:"pressure_limit"`
	BaselineSamples     int      `json:"baseline_samples"`
	ReportedDrainReason string   `json:"reported_drain_reason,omitempty"`
	Corroboration       []string `json:"corroboration"`
	RecoverySamples     int      `json:"recovery_samples"`
}

func evaluateHostPressure(m *Machine) {
	h := m.HostHealth
	// Older workers do not serialize the local pressurePSI source flag.
	// The already-negotiated OS profile establishes its units instead.
	if h == nil || m.CapabilityProfile == nil || m.CapabilityProfile.OS != "linux" {
		return
	}
	e := &HostPressureEvaluation{BaselineSamples: len(h.Baseline), ReportedDrainReason: h.DrainReason, Corroboration: []string{}}
	h.PressureEvaluation = e
	if len(h.Baseline) < hostHealthWarmup {
		return // Unknown calibration never clears a reported drain.
	}
	_, e.PressureLimit = healthBand(h.Baseline, func(s HostHealthSample) float64 { return s.Pressure })
	_, swapLimit := healthBand(h.Baseline, func(s HostHealthSample) float64 { return s.SwapGrowth })
	_, wakeLimit := healthBand(h.Baseline, func(s HostHealthSample) float64 { return s.WakeSeconds })
	// Recheck the confirmation horizon from distinct sensor completions, not
	// check-in count. Keep a reported drain if any corroborating harm remains;
	// three healthy samples let a legacy idle-baseline latch recover while
	// ordinary work continues. Errors/duplicate timestamps cannot prove recovery.
	for i := len(h.History) - 1; i >= 0 && len(h.History)-i <= hostHealthConfirmation; i-- {
		s := h.History[i]
		if s.Error != "" || s.At.IsZero() || i+1 < len(h.History) && !h.History[i+1].At.After(s.At) {
			break
		}
		if s.SwapGrowth > swapLimit {
			e.Corroboration = append(e.Corroboration, "swap_growth")
		}
		if s.WakeSeconds > wakeLimit {
			e.Corroboration = append(e.Corroboration, "wake_latency")
		}
		if len(e.Corroboration) != 0 {
			break
		}
		e.RecoverySamples++
	}
	if h.DrainReason == "host_memory_pressure" && e.RecoverySamples == hostHealthConfirmation {
		h.DrainReason = ""
	}
}
