package myce

import (
	"errors"
	"fmt"
	"math"
	"time"
)

const externalBucketWidth = 15 * time.Minute
const externalBucketCount = 7*24*4 + 1

// HostMemoryObservation is simultaneous best-effort host used RAM and current
// worker/descendant RSS. It never uses reservations or historical attempt peaks.
type HostMemoryObservation struct {
	TotalGB    float64 `json:"total_gb"`
	UsedGB     float64 `json:"used_gb"`
	MyceliumGB float64 `json:"mycelium_gb"`
}
type externalMemoryBucket struct {
	Start  time.Time        `json:"start"`
	Counts map[uint8]uint32 `json:"counts"`
	MaxGB  float64          `json:"max_gb"`
	PeakAt time.Time        `json:"peak_at"`
}
type externalMemoryRing struct {
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
	if o == nil || !validMemoryValue(o.TotalGB) || o.TotalGB <= 0 || !validMemoryValue(o.UsedGB) || o.UsedGB > o.TotalGB || !validMemoryValue(o.MyceliumGB) || o.MyceliumGB > o.UsedGB {
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
	now := q.clock.Now()
	start := now.Truncate(externalBucketWidth)
	index := (start.Unix() / int64(externalBucketWidth/time.Second)) % externalBucketCount
	if index < 0 {
		index += externalBucketCount
	}
	b := r.Buckets[int(index)]
	if b == nil || !b.Start.Equal(start) {
		b = &externalMemoryBucket{Start: start, Counts: map[uint8]uint32{}}
		r.Buckets[int(index)] = b
	}
	v := o.UsedGB - o.MyceliumGB
	bin := uint8(externalBin(v))
	if b.Counts[bin] < math.MaxUint32 {
		b.Counts[bin]++
	}
	if b.PeakAt.IsZero() || v > b.MaxGB {
		b.MaxGB, b.PeakAt = v, now
	}
}
func (r *externalMemoryRing) window(now time.Time, duration time.Duration) ExternalMemoryWindow {
	var w ExternalMemoryWindow
	var counts [64]uint64
	// Include the boundary bucket: conservatively at most 15 minutes extra.
	cutoff := now.Add(-duration).Truncate(externalBucketWidth)
	for _, b := range r.Buckets {
		if b.PeakAt.IsZero() || b.Start.Before(cutoff) || b.Start.After(now) {
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
			counts[i] += uint64(n)
			w.Samples += uint64(n)
		}
		if w.PeakAt.IsZero() || b.MaxGB > w.MaxGB || b.MaxGB == w.MaxGB && b.PeakAt.Before(w.PeakAt) {
			w.MaxGB, w.PeakAt = b.MaxGB, b.PeakAt
		}
	}
	quantile := func(p float64) float64 {
		target := uint64(math.Ceil(float64(w.Samples) * p))
		var n uint64
		for i, c := range counts {
			n += c
			if n >= target {
				if i == 63 {
					return w.MaxGB
				}
				return min(w.MaxGB, externalBinUpper(i))
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
	headroom = max(headroom, .25*w.MaxGB)
	return HostBudgetSuggestion{TotalGB: total, ExternalMaxGB: w.MaxGB, HeadroomGB: headroom, QueueOverheadGB: queue, SuggestedGB: math.Floor(max(0, total-w.MaxGB-headroom-queue)*4) / 4, History: w, DryRun: true}, nil
}
