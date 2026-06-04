package domain

type Priority string

const (
	PriorityInteractive Priority = "interactive"
	PriorityNormal      Priority = "normal"
	PriorityBackground  Priority = "background"
)

type SpeedPref string

const (
	SpeedThroughput SpeedPref = "throughput"
	SpeedLatency    SpeedPref = "latency"
	SpeedAuto       SpeedPref = "auto"
)

type Preemption string

const (
	PreemptInherit            Preemption = "inherit"
	PreemptSoft               Preemption = "soft"
	PreemptHardForInteractive Preemption = "hard_for_interactive"
	PreemptHard               Preemption = "hard"
)

type HandlingClass string

const (
	HandlingStandard HandlingClass = ""
	HandlingPrivate  HandlingClass = "private"
)

type OOMSeverity string

const (
	OOMSoft         OOMSeverity = "soft"
	OOMCatastrophic OOMSeverity = "catastrophic"
)

type NodeStatus string

const (
	NodeReady       NodeStatus = "ready"
	NodeMaintenance NodeStatus = "maintenance"
	NodeDraining    NodeStatus = "draining"
	NodeUnreachable NodeStatus = "unreachable"
)

type Backend string

const (
	BackendLlamaCpp Backend = "llamacpp"
	BackendVLLM     Backend = "vllm"
	BackendMLX      Backend = "mlx"
	BackendCustom   Backend = "custom"
)

type Capability string

const (
	CapabilityChat        Capability = "chat"
	CapabilityCompletion  Capability = "completion"
	CapabilityEmbedding   Capability = "embedding"
	CapabilityVision      Capability = "vision"
	CapabilityImage       Capability = "image"
	CapabilityASR         Capability = "asr"
	CapabilityDiarization Capability = "diarization"
	CapabilityRerank      Capability = "rerank"
	CapabilityTTS         Capability = "tts"
)

type JobStatus string

const (
	JobQueued    JobStatus = "queued"
	JobPlacing   JobStatus = "placing"
	JobLoading   JobStatus = "loading"
	JobRunning   JobStatus = "running"
	JobDone      JobStatus = "done"
	JobPreempted JobStatus = "preempted"
	JobFailed    JobStatus = "failed"
)

type InstanceState string

const (
	InstLoading  InstanceState = "loading"
	InstReady    InstanceState = "ready"
	InstStopping InstanceState = "stopping"
	InstError    InstanceState = "error"
)

type PlacementAction string

const (
	ActionWarmInstance  PlacementAction = "placed_on_warm_instance"
	ActionLoadedNew     PlacementAction = "loaded_new"
	ActionQueued        PlacementAction = "queued"
	ActionHardPreempted PlacementAction = "hard_preempted_then_loaded"
	ActionDedicatedUnit PlacementAction = "dedicated_unit"
)

type ReservationKind string

const (
	ReservationHeadroom ReservationKind = "headroom"
	ReservationPinned   ReservationKind = "pinned"
)

type TelemetryPhase string

const (
	TelemetryPhasePlaced        TelemetryPhase = "placed"
	TelemetryPhaseLoadReady     TelemetryPhase = "load_ready"
	TelemetryPhaseUpstreamStart TelemetryPhase = "upstream_start"
	TelemetryPhaseFirstByte     TelemetryPhase = "first_byte"
	TelemetryPhaseStreamChunk   TelemetryPhase = "stream_chunk"
	TelemetryPhaseComplete      TelemetryPhase = "complete"
	TelemetryPhaseError         TelemetryPhase = "error"
)
