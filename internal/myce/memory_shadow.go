package myce

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

const memoryShadowProtocol = "memory-shadow-v1"
const memoryShadowHeader = "X-Myce-Memory-Shadow"

const (
	shadowLinuxCPU      = "linux-cpu"
	shadowLinuxDiscrete = "linux-discrete"
	shadowLinuxUnified  = "linux-unified-cuda"
	shadowDarwin        = "macos"
	shadowSynthetic     = "synthetic"
)

type shadowMemoryCounters struct {
	host, device, driverSystem *float64
}

// A nil result represents a missing counter, including an unqualified
// attribution. Callers must never substitute a host-wide reading.
func shadowFootprint(platform string, c shadowMemoryCounters) (ram, card *float64) {
	sum := func(a, b *float64) *float64 {
		if a == nil || b == nil {
			return nil
		}
		n := *a + *b
		return &n
	}
	switch platform {
	case shadowLinuxCPU, shadowDarwin, shadowSynthetic:
		return c.host, nil
	case shadowLinuxDiscrete:
		return sum(c.host, c.driverSystem), c.device
	case shadowLinuxUnified:
		return sum(c.host, c.device), nil
	default:
		return nil, nil
	}
}

type shadowMemoryReading struct {
	At         time.Time `json:"at"`
	GB         float64   `json:"gb"`
	PeakGB     float64   `json:"peak_gb"`
	Rate       float64   `json:"growth_gb_per_second"`
	Fastest    float64   `json:"fastest_growth_gb_per_second"`
	LastGrowth time.Time `json:"last_growth"`
}

func (s shadowMemoryReading) observe(at time.Time, gb *float64, period time.Duration) shadowMemoryReading {
	if gb == nil || !validMemoryValue(*gb) {
		s.At, s.Rate = time.Time{}, 0
		return s
	}
	if dt := at.Sub(s.At); !s.At.IsZero() && dt <= 0 {
		return s
	} else if !s.At.IsZero() && dt < 2*period {
		s.Rate = max(0, *gb-s.GB) / dt.Seconds()
		if s.Rate > 0 {
			s.LastGrowth = at
		}
		s.Fastest = max(s.Fastest, s.Rate)
	} else {
		s.Rate, s.LastGrowth = 0, at
	}
	s.At, s.GB, s.PeakGB = at, *gb, max(s.PeakGB, *gb)
	return s
}

func (s shadowMemoryReading) fresh(at time.Time, period time.Duration) bool {
	return !s.At.IsZero() && !at.Before(s.At) && at.Sub(s.At) < 2*period
}

type shadowAttemptReading struct {
	RAM  shadowMemoryReading `json:"ram"`
	Card shadowMemoryReading `json:"card"`
}

type MemoryShadowReport struct {
	Pools         []MemoryShadowPool `json:"pools"`
	FastestGrowth map[string]float64 `json:"fastest_growth_gb_per_second,omitempty"`
	// The worker's reaction horizon uses the queue's existing stop grace.
	ReactionSeconds float64 `json:"reaction_seconds"`
}

type MemoryShadowPool struct {
	Name             string                `json:"pool"`
	CapacityGB       float64               `json:"capacity_gb"`
	KeepGB           float64               `json:"keep_gb"`
	FreeGB           *float64              `json:"free_gb,omitempty"`
	FootprintGB      float64               `json:"footprint_gb"`
	HeldGB           float64               `json:"held_gb"`
	CommitmentGB     float64               `json:"commitment_gb"`
	SlackGB          *float64              `json:"slack_gb,omitempty"`
	ReachGB          *float64              `json:"reach_gb,omitempty"`
	ForecastFreeGB   *float64              `json:"forecast_free_gb,omitempty"`
	OwnersGB         float64               `json:"owners_gb"`
	OwnersForecastGB float64               `json:"owners_forecast_gb"`
	PressureWarning  bool                  `json:"pressure_warning,omitempty"`
	Attempts         []MemoryShadowAttempt `json:"attempts,omitempty"`
}

type MemoryShadowAttempt struct {
	ID           string   `json:"id"`
	Attempt      int      `json:"attempt"`
	ExpectedGB   float64  `json:"expected_gb"`
	FootprintGB  *float64 `json:"footprint_gb,omitempty"`
	HighWaterGB  float64  `json:"high_water_gb"`
	HoldGB       *float64 `json:"hold_gb,omitempty"`
	CommitmentGB *float64 `json:"commitment_gb,omitempty"`
	Growth       float64  `json:"growth_gb_per_second"`
	Lent         bool     `json:"lent,omitempty"`
}

func (p *MemoryShadowPool) sum() {
	complete := p.FreeGB != nil
	for i := range p.Attempts {
		a := &p.Attempts[i]
		if a.FootprintGB == nil {
			complete = false
			continue
		}
		footprint := *a.FootprintGB
		commitment := max(a.ExpectedGB, a.HighWaterGB, footprint)
		if a.Lent {
			commitment = max(a.HighWaterGB, footprint)
		}
		hold := commitment - footprint
		a.HoldGB, a.CommitmentGB = &hold, &commitment
		p.FootprintGB += footprint
		p.HeldGB += hold
		p.CommitmentGB += commitment
	}
	if complete {
		s := *p.FreeGB - p.HeldGB
		if p.Name == "ram" {
			s = min(s, p.CapacityGB-p.CommitmentGB)
		}
		reach := s + p.CommitmentGB
		p.SlackGB, p.ReachGB = &s, &reach
	}
}

func (p *MemoryShadowPool) forecast(previous *MemoryShadowPool, elapsed, reaction float64) {
	if p.SlackGB == nil || previous == nil || previous.SlackGB == nil || elapsed <= 0 {
		return
	}
	// Compare matching sets. A start/end between readings cannot establish
	// the other owners' slope from the difference in aggregate footprints.
	if len(p.Attempts) != len(previous.Attempts) {
		return
	}
	loss := 0.0
	for i, a := range p.Attempts {
		b := previous.Attempts[i]
		if a.ID != b.ID || a.Attempt != b.Attempt {
			return
		}
		growth := a.Growth * reaction
		if *a.FootprintGB <= a.ExpectedGB {
			growth = min(growth, *a.HoldGB)
		}
		loss += growth
	}
	ownersLoss := max(0, p.OwnersGB-previous.OwnersGB) / elapsed * reaction
	p.OwnersForecastGB = p.OwnersGB + ownersLoss
	free := *p.FreeGB - loss - ownersLoss
	p.ForecastFreeGB = &free
}

func (r *MemoryShadowReport) lines() []string {
	if r == nil {
		return nil
	}
	var out []string
	for _, p := range r.Pools {
		if p.SlackGB == nil {
			out = append(out, fmt.Sprintf("memory v2 shadow %s: insufficient footprint or free-memory samples", p.Name))
			continue
		}
		out = append(out, fmt.Sprintf("memory v2 shadow %s: footprint %.3f GiB, held %.3f GiB, S %.3f GiB, reach %.3f GiB", p.Name, p.FootprintGB, p.HeldGB, *p.SlackGB, *p.ReachGB))
	}
	if len(r.FastestGrowth) > 0 {
		var rates []string
		for _, k := range slices.Sorted(maps.Keys(r.FastestGrowth)) {
			rates = append(rates, fmt.Sprintf("%s %.3f GiB/s", k, r.FastestGrowth[k]))
		}
		out = append(out, "memory v2 shadow fastest growth: "+strings.Join(rates, ", "))
	}
	return out
}

type memoryShadowSettings struct {
	KeepGB       float64 `json:"keep_gb"`
	GraceSeconds float64 `json:"grace_seconds"`
}
