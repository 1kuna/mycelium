// Package view serves `myce view`: a read-only live picture of a running
// installation. It is an ordinary HTTP client of the queue's status API and
// never runs inside the queue process.
package view

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"time"
)

// statusDoc decodes only what the view draws from the default GET /api/status.
// Absent fields stay zero; a field whose type changes is skipped (see decode).
type statusDoc struct {
	Now      time.Time    `json:"now"`
	Machines []docMachine `json:"machines"`
	Jobs     []docJob     `json:"jobs"`
}

type docMachine struct {
	ID                    string          `json:"id"`
	Gone                  bool            `json:"gone"`
	Leaving               bool            `json:"leaving"`
	Disabled              string          `json:"disabled"`
	LastSeen              time.Time       `json:"last_seen"`
	SharedHost            json.RawMessage `json:"shared_host"`
	EffectiveCPUCores     float64         `json:"effective_cpu_cores"`
	CPUInUseCores         float64         `json:"cpu_in_use_cores"`
	TotalMemoryGB         float64         `json:"total_memory_gb"`
	HostAvailableMemoryGB *float64        `json:"host_available_memory_gb"`
	HostMemoryInUseGB     float64         `json:"host_memory_in_use_gb"`
	UnifiedMemory         bool            `json:"unified_memory"`
	Devices               []docDevice     `json:"devices"`
	HostHealth            *struct {
		History []struct {
			Load *float64 `json:"load"`
		} `json:"history"`
	} `json:"host_health"`
}

type docDevice struct {
	ID            string  `json:"id"`
	Platform      string  `json:"platform"`
	TotalMemoryGB float64 `json:"total_memory_gb"`
	FreeMemoryGB  float64 `json:"free_memory_gb"`
	UnifiedMemory bool    `json:"unified_memory"`
}

type docJob struct {
	ID              string     `json:"id"`
	State           string     `json:"state"`
	Machine         string     `json:"machine"`
	Attempt         int        `json:"attempt"`
	Losses          int        `json:"losses"`
	PreemptionCount int        `json:"preemption_count"`
	QueueOrder      int        `json:"queue_order"`
	WaitingSince    *time.Time `json:"waiting_since"`
	StartedAt       *time.Time `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
	PriorityPolicy  struct {
		Tier  string `json:"tier"`
		Index int    `json:"index"`
	} `json:"priority_policy"`
	ApplicationProgress *struct {
		Completed float64 `json:"completed"`
		Total     float64 `json:"total"`
	} `json:"application_progress"`
	Runtime *struct {
		ElapsedSeconds   *float64 `json:"elapsed_seconds"`
		RemainingSeconds *float64 `json:"remaining_seconds"`
	} `json:"runtime"`
	Spec struct {
		Name      string `json:"name"`
		Project   string `json:"project"`
		Class     string `json:"class"`
		Priority  string `json:"priority"`
		Resumable bool   `json:"resumable"`
		Needs     struct {
			Platform     json.RawMessage `json:"platform"`
			MemoryGB     float64         `json:"memory_gb"`
			CPUCores     float64         `json:"cpu_cores"`
			HostMemoryGB float64         `json:"host_memory_gb"`
		} `json:"needs"`
	} `json:"spec"`
}

// decodeStatus tolerates fields whose JSON type differs from what the view
// expects: encoding/json skips such a field and decodes the rest, so a view
// built for one release keeps drawing a newer or older queue.
func decodeStatus(data []byte) (statusDoc, error) {
	var doc statusDoc
	err := json.NewDecoder(bytes.NewReader(data)).Decode(&doc)
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		err = nil
	}
	return doc, err
}

// Frame is everything the page receives. Field names follow the status API
// where one exists, so the page reads like the API documentation.
type Frame struct {
	Now        time.Time `json:"now"`
	Machines   []Machine `json:"machines"`
	Jobs       []Job     `json:"jobs"`
	Upstream   Upstream  `json:"upstream"`
	Anonymized bool      `json:"anonymized"`
}

type Machine struct {
	ID                    string    `json:"id"`
	Gone                  bool      `json:"gone"`
	Fresh                 bool      `json:"fresh"`
	Leaving               bool      `json:"leaving"`
	Disabled              bool      `json:"disabled"`
	SharedHost            bool      `json:"shared_host"`
	UnifiedMemory         bool      `json:"unified_memory"`
	EffectiveCPUCores     float64   `json:"effective_cpu_cores"`
	CPUInUseCores         float64   `json:"cpu_in_use_cores"`
	Load                  *float64  `json:"load,omitempty"`
	TotalMemoryGB         float64   `json:"total_memory_gb"`
	HostAvailableMemoryGB *float64  `json:"host_available_memory_gb,omitempty"`
	HostMemoryInUseGB     float64   `json:"host_memory_in_use_gb"`
	Devices               []Device  `json:"devices"`
	LastSeen              time.Time `json:"last_seen"`
}

// Device memory is memory held, not utilisation: status has no SM usage.
type Device struct {
	ID            string  `json:"id"`
	Platform      string  `json:"platform"`
	TotalMemoryGB float64 `json:"total_memory_gb"`
	FreeMemoryGB  float64 `json:"free_memory_gb"`
	UnifiedMemory bool    `json:"unified_memory"`
}

type Job struct {
	ID              string     `json:"id"`
	Name            string     `json:"name"`
	Project         string     `json:"project"`
	State           string     `json:"state"`
	Machine         string     `json:"machine,omitempty"`
	Production      bool       `json:"production"`
	Tier            string     `json:"tier,omitempty"`
	TierIndex       int        `json:"tier_index"`
	QueueOrder      int        `json:"queue_order"`
	Attempt         int        `json:"attempt"`
	Losses          int        `json:"losses"`
	PreemptionCount int        `json:"preemption_count"`
	Resumable       bool       `json:"resumable"`
	Progress        *float64   `json:"progress,omitempty"`
	WaitingSince    *time.Time `json:"waiting_since,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	Needs           Needs      `json:"needs"`
}

type Needs struct {
	Platform     string  `json:"platform,omitempty"`
	MemoryGB     float64 `json:"memory_gb,omitempty"`
	CPUCores     float64 `json:"cpu_cores,omitempty"`
	HostMemoryGB float64 `json:"host_memory_gb,omitempty"`
}

// Upstream tells the page how its data source is doing. State is a fixed
// code; raw errors (which can name addresses) never reach the page.
type Upstream struct {
	State        string     `json:"state"` // connecting, ok, rate_limited, unreachable, error
	LatencyMS    float64    `json:"latency_ms,omitempty"`
	NextPollMS   float64    `json:"next_poll_ms,omitempty"`
	RetryAfterMS float64    `json:"retry_after_ms,omitempty"`
	PolledAt     *time.Time `json:"polled_at,omitempty"`
}

func terminal(state string) bool {
	return state == "succeeded" || state == "failed" || state == "cancelled"
}

// projector turns status documents into frames and keeps the little session
// memory the picture needs: which machines were seen alive, each machine's
// observed heartbeat cadence, and which jobs were seen before they finished.
type projector struct {
	started time.Time // queue time of the first document
	alive   map[string]bool
	beat    map[string]*heartbeat
	active  map[string]bool
}

type heartbeat struct {
	last     time.Time
	interval time.Duration // smoothed observed advance of last_seen
	stalled  int           // consecutive documents without an advance
}

func newProjector() *projector {
	return &projector{alive: map[string]bool{}, beat: map[string]*heartbeat{}, active: map[string]bool{}}
}

// project builds a frame. pollGap is the time since the previous document,
// used so a slow poll never makes a healthy machine look silent.
func (p *projector) project(doc statusDoc, pollGap time.Duration) Frame {
	if p.started.IsZero() {
		p.started = doc.Now
	}
	f := Frame{Now: doc.Now, Machines: []Machine{}, Jobs: []Job{}}
	for _, m := range doc.Machines {
		if m.ID == "" {
			continue
		}
		fresh := !m.Gone && p.fresh(m, doc.Now, pollGap)
		if !m.Gone {
			p.alive[m.ID] = true
		}
		// A machine already gone when the view opened is history, not fleet.
		if m.Gone && !p.alive[m.ID] {
			continue
		}
		out := Machine{ID: m.ID, Gone: m.Gone, Fresh: fresh, Leaving: m.Leaving, Disabled: m.Disabled != "",
			SharedHost: len(m.SharedHost) > 0 && string(m.SharedHost) != "null", UnifiedMemory: m.UnifiedMemory,
			EffectiveCPUCores: m.EffectiveCPUCores, CPUInUseCores: m.CPUInUseCores, TotalMemoryGB: m.TotalMemoryGB,
			HostAvailableMemoryGB: finite(m.HostAvailableMemoryGB), HostMemoryInUseGB: m.HostMemoryInUseGB,
			Devices: []Device{}, LastSeen: m.LastSeen}
		if h := m.HostHealth; h != nil && len(h.History) > 0 {
			out.Load = finite(h.History[len(h.History)-1].Load)
		}
		for _, d := range m.Devices {
			out.Devices = append(out.Devices, Device{ID: d.ID, Platform: d.Platform, TotalMemoryGB: d.TotalMemoryGB, FreeMemoryGB: d.FreeMemoryGB, UnifiedMemory: d.UnifiedMemory})
		}
		f.Machines = append(f.Machines, out)
	}
	for _, j := range doc.Jobs {
		if j.ID == "" {
			continue
		}
		if !terminal(j.State) {
			p.active[j.ID] = true
		} else if !p.active[j.ID] && (j.FinishedAt == nil || j.FinishedAt.Before(p.started)) {
			continue // finished before the view opened
		}
		out := Job{ID: j.ID, Name: j.Spec.Name, Project: j.Spec.Project, State: j.State, Machine: j.Machine,
			Production: j.Spec.Class == "production" || j.Spec.Priority == "production" || j.PriorityPolicy.Tier == "production",
			Tier:       j.PriorityPolicy.Tier, TierIndex: j.PriorityPolicy.Index, QueueOrder: j.QueueOrder,
			Attempt: j.Attempt, Losses: j.Losses, PreemptionCount: j.PreemptionCount, Resumable: j.Spec.Resumable,
			Progress: progress(j), WaitingSince: j.WaitingSince, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt,
			Needs: Needs{Platform: platform(j.Spec.Needs.Platform), MemoryGB: j.Spec.Needs.MemoryGB, CPUCores: j.Spec.Needs.CPUCores, HostMemoryGB: j.Spec.Needs.HostMemoryGB}}
		if out.Name == "" {
			out.Name = out.ID
		}
		f.Jobs = append(f.Jobs, out)
	}
	sort.SliceStable(f.Machines, func(a, b int) bool { return f.Machines[a].ID < f.Machines[b].ID })
	return f
}

// fresh learns each machine's heartbeat cadence from how far last_seen moves
// between documents. A machine is silent only once last_seen has stopped for
// two documents and its age exceeds three learned intervals and two poll gaps.
// Without a learned interval there is no evidence of silence.
func (p *projector) fresh(m docMachine, now time.Time, pollGap time.Duration) bool {
	h := p.beat[m.ID]
	if h == nil {
		p.beat[m.ID] = &heartbeat{last: m.LastSeen}
		return true
	}
	if m.LastSeen.After(h.last) {
		step := m.LastSeen.Sub(h.last)
		if h.interval == 0 {
			h.interval = step
		} else {
			h.interval = (h.interval*7 + step*3) / 10
		}
		h.last, h.stalled = m.LastSeen, 0
		return true
	}
	h.stalled++
	if h.interval == 0 || h.stalled < 2 {
		return true
	}
	age := now.Sub(m.LastSeen)
	return age <= 3*h.interval || age <= 2*pollGap
}

// progress is application-reported completion, else elapsed over the queue's
// own runtime estimate; absent when neither exists.
func progress(j docJob) *float64 {
	if p := j.ApplicationProgress; p != nil && p.Total > 0 && p.Completed >= 0 {
		v := math.Min(1, p.Completed/p.Total)
		return &v
	}
	if r := j.Runtime; r != nil && r.ElapsedSeconds != nil && r.RemainingSeconds != nil && *r.ElapsedSeconds >= 0 && *r.RemainingSeconds >= 0 {
		if total := *r.ElapsedSeconds + *r.RemainingSeconds; total > 0 {
			v := *r.ElapsedSeconds / total
			return &v
		}
	}
	return nil
}

// platform reads the design's shorthand ([cuda] or "cuda") or {required|preferred: [...]}.
func platform(raw json.RawMessage) string {
	var list []string
	if json.Unmarshal(raw, &list) == nil && len(list) > 0 {
		return strings.ToLower(list[0])
	}
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return strings.ToLower(one)
	}
	var obj struct{ Required, Preferred []string }
	if json.Unmarshal(raw, &obj) == nil {
		if len(obj.Required) > 0 {
			return strings.ToLower(obj.Required[0])
		}
		if len(obj.Preferred) > 0 {
			return strings.ToLower(obj.Preferred[0])
		}
	}
	return ""
}

func finite(v *float64) *float64 {
	if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	return v
}
