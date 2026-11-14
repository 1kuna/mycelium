package myce

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

type stagingPhase int

const (
	stagePreflight stagingPhase = iota
	stageRoots
	stageExtensionEnvironment
	stageSharedInputs
	stageCompiler
	stageFiles
	stageDiskBudget
	stageCacheLock
	stageCollectionLock
	stageCacheLookup
	stageCacheEviction
	stageEvictionCheck
	stageBlobHead
	stageBlobHeaders
	stageDownload
	stageVerify
	stageCachePublish
	stagePlacement
	stageNasInputs
	stageWorkPublish
	stagingPhaseCount
)

var stagingPhaseNames = [stagingPhaseCount]string{"preflight", "roots", "extension_environment", "shared_inputs", "compiler", "files", "disk_budget", "cache_lock", "collection_lock", "cache_lookup", "cache_eviction", "eviction_check", "blob_head", "blob_headers", "download", "verify", "cache_publish", "placement", "nas_inputs", "work_publish"}

// StagingPhaseTiming is a cumulative worker-clock measurement, including failed
// calls. Fixed counters avoid per-file, per-victim or per-request trace growth.
type StagingPhaseTiming struct {
	Name     string  `json:"name"`
	Calls    uint64  `json:"calls"`
	Seconds  float64 `json:"seconds"`
	Failures uint64  `json:"failures"`
	HTTP4xx  uint64  `json:"http_4xx,omitempty"`
	HTTP5xx  uint64  `json:"http_5xx,omitempty"`
	Expired  uint64  `json:"expired,omitempty"`
}

// StagingTimings is diagnostic only. Phases nest and concurrent work may overlap;
// summing them does not give elapsed time. Missing legacy evidence stays nil.
// StartedAt is first worker preparation, not the queue's assignment clock.
type StagingTimings struct {
	StartedAt      time.Time                             `json:"started_at"`
	CompletedAt    *time.Time                            `json:"completed_at,omitempty"`
	ElapsedSeconds float64                               `json:"elapsed_seconds"`
	CacheHits      uint64                                `json:"cache_hits"`
	CacheMisses    uint64                                `json:"cache_misses"`
	Evicted        uint64                                `json:"evicted"`
	Phases         [stagingPhaseCount]StagingPhaseTiming `json:"phases"`
}

type stagingRecorder struct {
	mu    sync.Mutex
	clock Clock
	value StagingTimings
}
type stagingRecorderKey struct{}

func stagingMeasure(ctx context.Context, phase stagingPhase) func(error) {
	r, _ := ctx.Value(stagingRecorderKey{}).(*stagingRecorder)
	if r == nil {
		return func(error) {}
	}
	r.mu.Lock()
	completed := r.value.CompletedAt != nil
	r.mu.Unlock()
	if completed {
		return func(error) {}
	}
	start := r.clock.Now()
	return func(err error) {
		elapsed := max(0, r.clock.Now().Sub(start).Seconds())
		r.mu.Lock()
		p := &r.value.Phases[phase]
		p.Calls++
		p.Seconds += elapsed
		if err != nil {
			p.Failures++
			var response *HTTPError
			if errors.As(err, &response) {
				if response.Status >= 400 && response.Status < 500 {
					p.HTTP4xx++
				}
				if response.Status >= 500 && response.Status < 600 {
					p.HTTP5xx++
				}
				if response.Status == 410 {
					p.Expired++
				}
			}
		}
		r.mu.Unlock()
	}
}
func stagingCount(ctx context.Context, kind string) {
	r, _ := ctx.Value(stagingRecorderKey{}).(*stagingRecorder)
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch kind {
	case "hit":
		r.value.CacheHits++
	case "miss":
		r.value.CacheMisses++
	case "evicted":
		r.value.Evicted++
	}
}
func (r *stagingRecorder) snapshot() *StagingTimings {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.value
	if v.CompletedAt == nil {
		v.ElapsedSeconds = max(0, r.clock.Now().Sub(v.StartedAt).Seconds())
	}
	for i := range v.Phases {
		v.Phases[i].Name = stagingPhaseNames[i]
	}
	return &v
}
func (r *stagingRecorder) complete(at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.value.CompletedAt == nil {
		r.value.CompletedAt = &at
		r.value.ElapsedSeconds = max(0, at.Sub(r.value.StartedAt).Seconds())
	}
}

// Call after the existing execution/session fence. Copy fixed-size facts only;
// never time, hash, do I/O or acquire a recorder lock under q.mu.
func retainStagingTimings(j *Job, value *StagingTimings) {
	if value == nil || value.StartedAt.IsZero() || value.ElapsedSeconds < 0 || math.IsNaN(value.ElapsedSeconds) || math.IsInf(value.ElapsedSeconds, 0) {
		return
	}
	v := *value
	if value.CompletedAt != nil {
		at := *value.CompletedAt
		v.CompletedAt = &at
	}
	for i, p := range v.Phases {
		v.Phases[i].Name = stagingPhaseNames[i]
		if p.Seconds < 0 || math.IsNaN(p.Seconds) || math.IsInf(p.Seconds, 0) {
			return
		}
	}
	if j.StagingTimings != nil && j.StagingTimings.CompletedAt != nil {
		return
	}
	j.StagingTimings = &v
}

func (s *StagingTimings) String() string {
	if s == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "staging worker_started=%s", s.StartedAt.UTC().Format(time.RFC3339Nano))
	if s.CompletedAt != nil {
		fmt.Fprintf(&b, " completed=%s elapsed=%.3fs", s.CompletedAt.UTC().Format(time.RFC3339Nano), s.ElapsedSeconds)
	}
	fmt.Fprintf(&b, " cache_hits=%d cache_misses=%d evicted=%d (nested/overlapping phases; unmeasured remainder includes preparation retries)", s.CacheHits, s.CacheMisses, s.Evicted)
	for i, p := range s.Phases {
		if p.Calls > 0 {
			fmt.Fprintf(&b, "\n    %s=%.3fs calls=%d failures=%d", stagingPhaseNames[i], p.Seconds, p.Calls, p.Failures)
			if p.HTTP4xx+p.HTTP5xx > 0 {
				fmt.Fprintf(&b, " http_4xx=%d http_5xx=%d expired=%d", p.HTTP4xx, p.HTTP5xx, p.Expired)
			}
		}
	}
	return b.String()
}
