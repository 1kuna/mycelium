package myce

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"sort"
	"time"
)

// HostMemoryObservation is simultaneous best-effort host used RAM and current
// worker/descendant RSS plus managed Docker cgroup memory. It never uses
// reservations or historical attempt peaks. Unknown attribution does not
// invalidate the independent physical-memory measurement.
type HostMemoryObservation struct {
	AttributionUnknown bool    `json:"attribution_unknown,omitempty"`
	TotalGB            float64 `json:"total_gb"`
	UsedGB             float64 `json:"used_gb"`
	MyceliumGB         float64 `json:"mycelium_gb"`
}
type externalMemoryBucket struct {
	ByteCounts map[uint8]uint64  `json:"byte_counts,omitempty"`
	ByteMax    map[uint8]float64 `json:"byte_max,omitempty"`
	Values     map[uint64]uint64 `json:"values,omitempty"`
	End        time.Time         `json:"end,omitempty"`
	Start      time.Time         `json:"start"`
	Counts     map[uint8]uint32  `json:"counts"`
	MaxGB      float64           `json:"max_gb"`
	PeakAt     time.Time         `json:"peak_at"`
}
type externalMemoryRing struct {
	Slots   int                           `json:"slots,omitempty"`
	LastAt  time.Time                     `json:"last_at,omitempty"`
	Cadence time.Duration                 `json:"cadence,omitempty"`
	Buckets map[int]*externalMemoryBucket `json:"buckets"`
}
type ExternalMemoryWindow struct {
	Samples uint64    `json:"samples"`
	Buckets int       `json:"buckets"`
	P50GB   float64   `json:"p50_gb"`
	P95GB   float64   `json:"p95_gb"`
	MaxGB   float64   `json:"max_gb"`
	PeakAt  time.Time `json:"peak_at"`
	FirstAt time.Time `json:"first_bucket_at"`
	LastAt  time.Time `json:"last_bucket_at"`
}

// Summary keeps missing observations visibly unknown in text status.
func (w ExternalMemoryWindow) Summary() string {
	if w.Samples == 0 {
		return "unknown (no samples)"
	}
	return fmt.Sprintf("%.2f/%.2f/%.2f GiB (peak %s; %d samples)", w.P50GB, w.P95GB, w.MaxGB, w.PeakAt.Format(time.RFC3339), w.Samples)
}

type ExternalMemoryHistory struct {
	Day  ExternalMemoryWindow `json:"24h"`
	Week ExternalMemoryWindow `json:"7d"`
}

func externalBin(v float64) int {
	if v <= 0 {
		return 0
	}
	return min(63, max(1, int(math.Ceil(math.Log(v/.125)/math.Log(1.2)))+1))
}
func externalBinUpper(i int) float64 {
	if i == 0 {
		return 0
	}
	return .125 * math.Pow(1.2, float64(i-1))
}
func (q *Queue) recordExternalMemory(m *Machine) {
	o := m.MemoryObservation
	if !validHostMemoryObservation(o) || o.AttributionUnknown {
		return
	}
	if q.s.ExternalMemory == nil {
		q.s.ExternalMemory = map[string]*externalMemoryRing{}
	}
	host := hostID(m)
	r := q.s.ExternalMemory[host]
	if r == nil {
		r = &externalMemoryRing{Buckets: map[int]*externalMemoryBucket{}}
		q.s.ExternalMemory[host] = r
	}
	// Published rings/buckets are immutable generations, borrowed by status and
	// undo without copying history while control is locked.
	copy := *r
	r = &copy
	r.Buckets = maps.Clone(r.Buckets)
	q.s.ExternalMemory[host] = r
	now := q.clock.Now()
	r.LastAt = now
	width := externalBucketSpan(r.Slots)
	start := now.Truncate(width)
	index := int(start.UnixNano() / int64(width) % int64(max(1, r.Slots)+1))
	b := r.Buckets[index]
	if b == nil || !b.Start.Equal(start) {
		b = &externalMemoryBucket{Start: start}
	} else {
		cloned := *b
		b = &cloned
	}
	b.ByteCounts, b.ByteMax = maps.Clone(b.ByteCounts), maps.Clone(b.ByteMax)
	if b.ByteCounts == nil {
		b.ByteCounts = map[uint8]uint64{}
		b.ByteMax = map[uint8]float64{}
	}
	r.Buckets[index] = b
	b.End = now
	v := o.UsedGB - o.MyceliumGB
	// One bin per bit of an addressable byte count. Values beyond the physical
	// uint64 address range saturate in the final bin; exact observed maxima stay.
	bytes := uint64(min(float64(^uint64(0)>>1), math.Ceil(v*(1<<30))))
	bin := uint8(bits.Len64(bytes))
	b.ByteCounts[bin]++
	b.ByteMax[bin] = max(b.ByteMax[bin], v)
	if b.PeakAt.IsZero() || v > b.MaxGB {
		b.MaxGB, b.PeakAt = v, now
	}
}
func (r *externalMemoryRing) window(now time.Time, duration time.Duration) ExternalMemoryWindow {
	var w ExternalMemoryWindow
	values := map[float64]uint64{}
	// Include only actual sample intervals intersecting this public query window.
	cutoff := now.Add(-duration)
	for _, b := range r.Buckets {
		if b.PeakAt.IsZero() || maxTime(b.Start, b.End).Before(cutoff) || b.Start.After(now) {
			continue
		}
		w.Buckets++
		if w.FirstAt.IsZero() || b.Start.Before(w.FirstAt) {
			w.FirstAt = b.Start
		}
		if b.Start.After(w.LastAt) {
			w.LastAt = b.Start
		}
		for i, n := range b.Counts {
			values[externalBinUpper(int(i))] += uint64(n)
			w.Samples += uint64(n)
		}
		for bin, n := range b.ByteCounts {
			values[b.ByteMax[bin]] += n
			w.Samples += n
		}
		for value, n := range b.Values {
			values[math.Float64frombits(value)] += n
			w.Samples += n
		}
		if w.PeakAt.IsZero() || b.MaxGB > w.MaxGB || b.MaxGB == w.MaxGB && b.PeakAt.Before(w.PeakAt) {
			w.MaxGB, w.PeakAt = b.MaxGB, b.PeakAt
		}
	}
	ordered := make([]float64, 0, len(values))
	for value := range values {
		ordered = append(ordered, value)
	}
	sort.Float64s(ordered)
	quantile := func(p float64) float64 {
		target := uint64(math.Ceil(float64(w.Samples) * p))
		var n uint64
		for _, value := range ordered {
			n += values[value]
			if n >= target {
				return min(w.MaxGB, value)
			}
		}
		return w.MaxGB
	}
	if w.Samples > 0 {
		w.P50GB, w.P95GB = quantile(.5), quantile(.95)
	}
	return w
}
func (q *Queue) externalMemoryStatus(host string) *ExternalMemoryHistory {
	r := q.s.ExternalMemory[host]
	if r == nil {
		return nil
	}
	now := q.clock.Now()
	return &ExternalMemoryHistory{Day: r.window(now, 24*time.Hour), Week: r.window(now, 7*24*time.Hour)}
}

type HostBudgetSuggestion struct {
	TotalGB         float64              `json:"total_gb"`
	ExternalMaxGB   float64              `json:"external_max_gb"`
	HeadroomGB      float64              `json:"headroom_gb"`
	QueueOverheadGB float64              `json:"queue_overhead_gb"`
	SuggestedGB     float64              `json:"suggested_gb"`
	History         ExternalMemoryWindow `json:"history_7d"`
	DryRun          bool                 `json:"dry_run"`
}

// SuggestHostBudget never changes policy. The max is exact among valid samples;
// it cannot bound future peaks or memory missed by the heartbeat interval.
func SuggestHostBudget(total, headroom, queue float64, h *ExternalMemoryHistory) (HostBudgetSuggestion, error) {
	if total <= 0 || !validMemoryValue(total) || !validMemoryValue(headroom) || !validMemoryValue(queue) {
		return HostBudgetSuggestion{}, errors.New("total must be positive; headroom and queue overhead must be finite and nonnegative")
	}
	if h == nil || h.Week.Samples == 0 {
		return HostBudgetSuggestion{}, errors.New("no measured external memory history")
	}
	w := h.Week
	// Keep authored headroom; observed variability, not a universal quarter,
	// accounts for the difference between typical and peak external use.
	if w.Samples > 1 && w.P50GB > 0 {
		headroom = max(headroom, w.MaxGB-w.P50GB)
	}
	return HostBudgetSuggestion{TotalGB: total, ExternalMaxGB: w.MaxGB, HeadroomGB: headroom, QueueOverheadGB: queue, SuggestedGB: max(0, total-w.MaxGB-headroom-queue), History: w, DryRun: true}, nil
}

// The public longest query window supplies the coverage horizon. The number
// of subdivisions is measured from storage/memory and serialization cost.
func externalBucketSpan(slots int) time.Duration {
	return max(time.Nanosecond, 7*24*time.Hour/time.Duration(max(1, slots)))
}
