package myce

import (
	"encoding/hex"
	"errors"
	"fmt"
)

// AdmissionDetail is a small snapshot of the failed worker-side comparison.
// Available is absent when the probe/configuration was unavailable, never a
// fabricated zero. Memory quantities are GiB; counts use the indicated unit.
type AdmissionDetail struct {
	Subcheck         string                  `json:"subcheck"`
	Unit             string                  `json:"unit"`
	Required         float64                 `json:"required"`
	Available        *float64                `json:"available,omitempty"`
	Unavailable      bool                    `json:"unavailable,omitempty"`
	KeepGB           float64                 `json:"keep_gb,omitempty"`
	HeldGB           float64                 `json:"held_gb,omitempty"`
	StartupGB        float64                 `json:"startup_gb,omitempty"`
	EnvelopeSource   string                  `json:"envelope_source,omitempty"`
	EnvelopeHostGB   float64                 `json:"envelope_host_gb"`
	EnvelopeDeviceGB float64                 `json:"envelope_device_gb"`
	Contributors     []HostGrowthContributor `json:"contributors,omitempty"`
	MemoryRoom       *MemoryRoomDecision     `json:"memory_room,omitempty"`
}

// These are the terms of the failed physical-growth sum, not memory attributed
// to the launching job. Model terms have no execution or resident sample.
type HostGrowthContributor struct {
	ID        string  `json:"id,omitempty"`
	Attempt   int     `json:"attempt,omitempty"`
	Kind      string  `json:"kind"`
	Launching bool    `json:"launching,omitempty"`
	ChargeGB  float64 `json:"charge_gb"`
	// HelperGB is the launching attempt's measured helper charge beyond its
	// reservation, already included in ChargeGB. Older workers omit it.
	HelperGB         float64  `json:"helper_gb,omitempty"`
	PendingGB        float64  `json:"pending_gb"`
	SampleAgeSeconds *float64 `json:"sample_age_seconds,omitempty"`
	SampleFresh      bool     `json:"sample_fresh"`
}

type admissionError struct{ detail AdmissionDetail }

func (e *admissionError) Error() string {
	check := e.detail.Subcheck
	if check == "live_host_ram" {
		check = "host RAM"
	}
	available := "unavailable"
	if e.detail.Available != nil {
		if check == memoryWaitReason {
			return fmt.Sprintf("%s: %s needs %.3f GiB, %.3f GiB net room (%.3f available, %.3f reserve, %.3f pending)",
				errWorkerMemory, check, e.detail.Required, *e.detail.Available-e.detail.KeepGB-e.detail.HeldGB,
				*e.detail.Available, e.detail.KeepGB, e.detail.HeldGB)
		}
		available = fmt.Sprintf("%g %s", *e.detail.Available, e.detail.Unit)
	}
	return fmt.Sprintf("%s: %s need %g %s, available %s", errWorkerMemory, check, e.detail.Required, e.detail.Unit, available)
}
func (e *admissionError) Unwrap() error { return errWorkerMemory }

func refuseAdmission(subcheck, unit string, required, available float64, unavailable bool, contributors ...HostGrowthContributor) error {
	d := AdmissionDetail{Subcheck: subcheck, Unit: unit, Required: required, Unavailable: unavailable || !validMemoryValue(available), Contributors: contributors}
	if !d.Unavailable {
		d.Available = &available
	}
	return &admissionError{detail: d}
}

func admissionDetail(err error, j Job) *AdmissionDetail {
	var refusal *admissionError
	if !errors.As(err, &refusal) {
		return nil
	}
	d := refusal.detail
	d.EnvelopeHostGB, d.EnvelopeDeviceGB = j.Spec.Needs.HostMemoryGB, j.Spec.Needs.MemoryGB
	d.EnvelopeSource = "declared"
	if j.MemoryEnvelope != nil {
		d.EnvelopeSource = j.MemoryEnvelope.Source
		d.EnvelopeHostGB, d.EnvelopeDeviceGB = j.MemoryEnvelope.HostGB, j.MemoryEnvelope.DeviceGB
	}
	return safeAdmissionDetail(&d)
}

func safeAdmissionDetail(d *AdmissionDetail) *AdmissionDetail {
	if d == nil {
		return nil
	}
	units := map[string]string{
		"local_device_reservation": "slots", "local_max_jobs": "jobs",
		"device_budget": "GiB", "aggregate_cpu_allowance": "cores",
		"aggregate_host_allowance": "GiB", "configured_host_allowance": "GiB",
		"live_host_ram": "GiB", "live_host_growth": "GiB", "live_gpu_memory": "GiB",
		memoryWaitReason:                   "GiB",
		"shared_cpu_controller":            "controllers",
		"exploratory_capacity_unavailable": "GiB", "custodian_ledger_read_failed": "GiB",
		"exploratory_host_memory_unavailable": "GiB", "exploratory_memory_room": "GiB",
	}
	if units[d.Subcheck] == "" || units[d.Subcheck] != d.Unit || !validMemoryValue(d.Required) || !validMemoryValue(d.EnvelopeHostGB) || !validMemoryValue(d.EnvelopeDeviceGB) {
		return nil
	}
	out := *d
	if d.Subcheck != memoryWaitReason || !validMemoryValue(d.KeepGB) || !validMemoryValue(d.HeldGB) {
		out.KeepGB, out.HeldGB = 0, 0
	}
	if d.Subcheck != memoryWaitReason || !validMemoryValue(d.StartupGB) {
		out.StartupGB = 0
	}
	out.MemoryRoom = safeMemoryRoom(d.MemoryRoom)
	out.Contributors = nil
	if d.Subcheck == "live_host_growth" {
		for _, c := range d.Contributors {
			if !validMemoryValue(c.ChargeGB) || !validMemoryValue(c.PendingGB) || !validMemoryValue(c.HelperGB) {
				continue
			}
			if c.Kind == "attempt" {
				if len(c.ID) != 32 || c.Attempt < 1 {
					continue
				}
				if _, err := hex.DecodeString(c.ID); err != nil {
					continue
				}
			} else if c.Kind != "model" || c.ID != "" || c.Attempt != 0 {
				continue
			}
			if c.SampleAgeSeconds != nil {
				age := *c.SampleAgeSeconds
				if !validMemoryValue(age) {
					continue
				}
				c.SampleAgeSeconds = &age
			}
			out.Contributors = append(out.Contributors, c)
		}
	}
	out.Available = nil
	if d.Available != nil && !d.Unavailable && validMemoryValue(*d.Available) {
		v := *d.Available
		out.Available = &v
	} else {
		out.Unavailable = true
	}
	out.EnvelopeSource = ""
	if d.EnvelopeSource == "memory pool estimate" || d.EnvelopeSource == "declared" {
		out.EnvelopeSource = d.EnvelopeSource
	}
	return &out
}
