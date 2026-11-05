package myce

import (
	"encoding/json"
	"log"
	"maps"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Bound untrusted version cardinality: 64 distinct identities plus overflow
// buckets per kind. No raw User-Agent, URL, job ID or credential is retained.
const jobGetIdentityLimit = 64

type jobGetIdentity struct{ Kind, Version string }
type jobGetTotal struct {
	Kind        string            `json:"kind"`
	Version     string            `json:"version"`
	Full        uint64            `json:"full"`
	Wait        uint64            `json:"wait"`
	RateLimited map[string]uint64 `json:"rate_limited,omitempty"`
}
type jobGetVolume struct {
	mu      sync.Mutex
	clock   Clock
	lastLog time.Time
	totals  map[jobGetIdentity]jobGetTotal
	observe func(time.Time, string)
}

func newJobGetVolume(clock Clock) *jobGetVolume {
	return &jobGetVolume{clock: clock, lastLog: clock.Now(), totals: map[jobGetIdentity]jobGetTotal{{"unknown", "unknown"}: {Kind: "unknown", Version: "unknown"}}}
}

func jobGetClient(agent string) jobGetIdentity {
	for _, product := range []struct{ prefix, kind string }{{"myce/", "cli"}, {"mycelium-python/", "python"}} {
		if version, ok := strings.CutPrefix(agent, product.prefix); ok && releaseName.MatchString(version) {
			return jobGetIdentity{product.kind, version}
		}
	}
	return jobGetIdentity{"unknown", "unknown"}
}

// Called before job observation, outside q.mu. Summaries are traffic-triggered
// at most once per minute; totals last for this handler's lifetime, not in SQLite.
func (v *jobGetVolume) record(agent string, wait bool) {
	v.recordObservation(agent, wait, "")
}

func (v *jobGetVolume) recordRejection(r *http.Request) {
	if route := observationRouteClass(r); route != "" {
		v.recordObservation(r.UserAgent(), false, route)
	}
}

func (v *jobGetVolume) recordObservation(agent string, wait bool, rejectedRoute string) {
	id := jobGetClient(agent)
	v.mu.Lock()
	if _, ok := v.totals[id]; !ok && len(v.totals) >= jobGetIdentityLimit {
		id.Version = "other"
	}
	total := v.totals[id]
	total.Kind, total.Version = id.Kind, id.Version
	if rejectedRoute != "" {
		if total.RateLimited == nil {
			total.RateLimited = map[string]uint64{}
		}
		total.RateLimited[rejectedRoute]++
	} else if wait {
		total.Wait++
	} else {
		total.Full++
	}
	v.totals[id] = total
	now := v.clock.Now()
	var summary []jobGetTotal
	if now.Sub(v.lastLog) >= time.Minute {
		v.lastLog = now
		summary = make([]jobGetTotal, 0, len(v.totals))
		for _, total := range v.totals {
			total.RateLimited = maps.Clone(total.RateLimited)
			summary = append(summary, total)
		}
	}
	v.mu.Unlock()
	if v.observe != nil && rejectedRoute == "" {
		v.observe(now, id.Kind)
	}
	if summary != nil {
		sort.Slice(summary, func(i, j int) bool {
			if summary[i].Kind != summary[j].Kind {
				return summary[i].Kind < summary[j].Kind
			}
			return summary[i].Version < summary[j].Version
		})
		data, _ := json.Marshal(summary)
		log.Printf("event=job_get_totals totals=%s", data)
	}
}
