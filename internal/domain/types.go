package domain

import "time"

type Accelerator struct {
	Index             int    `json:"index"`
	Vendor            string `json:"vendor"`
	Kind              string `json:"kind"`
	VRAMTotalMB       int    `json:"vram_total_mb"`
	VRAMUsedMB        int    `json:"vram_used_mb"`
	UnifiedMemory     bool   `json:"unified_memory"`
	ComputeCapability string `json:"compute_capability,omitempty"`
	ArchFamily        string `json:"arch_family,omitempty"`
}

type Node struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Address       string            `json:"address"`
	OS            string            `json:"os"`
	Labels        map[string]string `json:"labels,omitempty"`
	MaxUtil       float64           `json:"max_util"`
	OOMSeverity   OOMSeverity       `json:"oom_severity"`
	Accelerators  []Accelerator     `json:"accelerators"`
	UnifiedMemory bool              `json:"unified_memory"`
	SpeedClass    SpeedClass        `json:"speed_class"`
	Status        NodeStatus        `json:"status"`
	HeartbeatAt   time.Time         `json:"heartbeat_at"`
}

type SpeedClass struct {
	TokensPerSecRef float64   `json:"tokens_per_sec_ref"`
	Source          string    `json:"source"`
	ProbedAt        time.Time `json:"probed_at"`
}

type Preset struct {
	ID            string       `json:"id"`
	ModelRef      string       `json:"model_ref"`
	Backend       Backend      `json:"backend"`
	ContextLength int          `json:"context_length"`
	Quant         string       `json:"quant"`
	Capabilities  []Capability `json:"capabilities"`
	LaunchProfile string       `json:"launch_profile"`
	EstWeightsMB  int          `json:"est_weights_mb"`
	KVPerTokenMB  float64      `json:"kv_per_token_mb"`
	NodeID        string       `json:"node_id,omitempty"`
}

type ModelMetadata struct {
	ModelRef      string  `json:"model_ref"`
	Format        string  `json:"format"`
	WeightsMB     int     `json:"weights_mb"`
	KVPerTokenMB  float64 `json:"kv_per_token_mb"`
	ContextLength int     `json:"context_length"`
}

type Claim struct {
	WeightsMB    int `json:"weights_mb"`
	KVReservedMB int `json:"kv_reserved_mb"`
}

type ModelInstance struct {
	ID             string        `json:"id"`
	PresetID       string        `json:"preset_id"`
	NodeID         string        `json:"node_id"`
	AcceleratorSet []int         `json:"accelerator_set"`
	Claim          Claim         `json:"claim"`
	State          InstanceState `json:"state"`
	Addr           string        `json:"addr"`
	InFlight       int           `json:"in_flight"`
	Priority       Priority      `json:"priority"`
	Loading        bool          `json:"loading"`
}

type Job struct {
	ID             string     `json:"id"`
	TaskType       string     `json:"task_type"`
	Model          string     `json:"model"`
	PresetID       string     `json:"preset,omitempty"`
	Project        string     `json:"project"`
	Priority       Priority   `json:"priority"`
	SpeedPref      SpeedPref  `json:"speed_pref"`
	ContextRequest int        `json:"context_request,omitempty"`
	Preemption     Preemption `json:"preemption"`
	Streaming      bool       `json:"streaming"`
	DeadlineMS     int        `json:"deadline_ms,omitempty"`
	ParentID       string     `json:"parent_id,omitempty"`
	Status         JobStatus  `json:"status"`
}

type PlacementDecision struct {
	JobID            string          `json:"job_id"`
	InstanceID       string          `json:"instance_id,omitempty"`
	NodeID           string          `json:"node_id,omitempty"`
	AcceleratorSet   []int           `json:"accelerator_set,omitempty"`
	Claim            Claim           `json:"claim"`
	Action           PlacementAction `json:"action"`
	SpeedPrefApplied SpeedPref       `json:"speed_pref_applied"`
	Trace            []TraceStep     `json:"trace"`
	Preempted        []string        `json:"preempted,omitempty"`
	Requeued         []string        `json:"requeued,omitempty"`
}

type FleetSnapshot struct {
	Nodes     []Node          `json:"nodes"`
	Instances []ModelInstance `json:"instances"`
}

type NodeSnapshot struct {
	Node      Node            `json:"node"`
	Instances []ModelInstance `json:"instances"`
}

type RunMetric struct {
	JobID           string    `json:"job_id"`
	InstanceID      string    `json:"instance_id"`
	NodeID          string    `json:"node_id"`
	Project         string    `json:"project"`
	TokensPerSec    float64   `json:"tokens_per_sec"`
	TTFTms          int       `json:"ttft_ms"`
	LoadWallClockMS int       `json:"load_wall_clock_ms"`
	PeakVRAMMB      int       `json:"peak_vram_mb"`
	ContextUsed     int       `json:"context_used"`
	At              time.Time `json:"at"`
}

type Reservation struct {
	ID        string          `json:"id"`
	Kind      ReservationKind `json:"kind"`
	NodeID    string          `json:"node_id"`
	PresetID  string          `json:"preset_id,omitempty"`
	Headroom  Claim           `json:"headroom,omitempty"`
	ExpiresAt time.Time       `json:"expires_at,omitempty"`
}

type Lease struct {
	ID         string    `json:"id"`
	JobID      string    `json:"job_id"`
	InstanceID string    `json:"instance_id"`
	NodeID     string    `json:"node_id"`
	Claim      Claim     `json:"claim"`
	GrantedAt  time.Time `json:"granted_at"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`
}

type Project struct {
	ID             string     `json:"id"`
	Priority       Priority   `json:"priority"`
	SpeedPref      SpeedPref  `json:"speed_pref"`
	ContextCap     int        `json:"context_cap"`
	Preemption     Preemption `json:"preemption"`
	AutoApply      bool       `json:"auto_apply"`
	ReservedNodeID string     `json:"reserved_node_id,omitempty"`
}
