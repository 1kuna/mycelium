package myce

import "time"

// DeviceMemorySampling belongs to one exact attempt, not an aggregate learning
// row. Preserve measured variation and coverage alongside the existing growth
// forecast: a startup slope alone cannot describe the device-memory tail.
// Complete coverage uses the guard's existing one-missed-cycle tolerance.
// This diagnostic does not discount DeviceGrowthGB or authorize GPU sharing.
type DeviceMemorySampling struct {
	Samples            int     `json:"samples"`
	Complete           bool    `json:"complete"`
	MinGB              float64 `json:"min_gb"`
	MaxGB              float64 `json:"max_gb"`
	LastGB             float64 `json:"last_gb"`
	MaxStepGB          float64 `json:"max_step_gb"`
	LastStepGB         float64 `json:"last_step_gb"`
	FirstAtSeconds     float64 `json:"first_at_seconds"`
	PeakAtSeconds      float64 `json:"peak_at_seconds"`
	ElapsedSeconds     float64 `json:"elapsed_seconds"`
	MaxIntervalSeconds float64 `json:"max_interval_seconds"`
}

type deviceSampling struct {
	DeviceMemorySampling
	start, last time.Time
	incomplete  bool
}

func (s *deviceSampling) observe(at time.Time, gb float64, period time.Duration) {
	if s.Samples == 0 {
		s.MinGB = gb
		s.FirstAtSeconds = at.Sub(s.start).Seconds()
	} else {
		gap := at.Sub(s.last)
		s.MaxIntervalSeconds = max(s.MaxIntervalSeconds, gap.Seconds())
		s.incomplete = s.incomplete || gap <= 0 || gap > 2*period
		s.LastStepGB = gb - s.LastGB
		s.MaxStepGB = max(s.MaxStepGB, s.LastStepGB)
	}
	if s.Samples == 0 || gb > s.MaxGB {
		s.PeakAtSeconds = at.Sub(s.start).Seconds()
	}
	s.MinGB, s.MaxGB, s.LastGB = min(s.MinGB, gb), max(s.MaxGB, gb), gb
	s.last = at
	s.Samples++
}

func (s *deviceSampling) finish(at time.Time, period time.Duration) *DeviceMemorySampling {
	if s.Samples == 0 {
		return nil
	}
	out := s.DeviceMemorySampling
	out.ElapsedSeconds = at.Sub(s.start).Seconds()
	out.Complete = !s.incomplete && s.Samples >= 2 && out.FirstAtSeconds >= 0 && out.FirstAtSeconds <= (2*period).Seconds() && out.ElapsedSeconds > 0 && !at.Before(s.last) && at.Sub(s.last) <= 2*period
	return &out
}
