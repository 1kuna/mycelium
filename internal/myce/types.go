package myce

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"gopkg.in/yaml.v3"
)

// Clock keeps queue decisions deterministic. Only RealClock reads wall time.
type Clock interface{ Now() time.Time }
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// Platform accepts the design's shorthand [cuda], or {required: [cuda]},
// {preferred: [metal]}, or {any: true}.
type Platform struct {
	Required  []string `json:"required,omitempty" yaml:"required,omitempty"`
	Preferred []string `json:"preferred,omitempty" yaml:"preferred,omitempty"`
	Any       bool     `json:"any,omitempty" yaml:"any,omitempty"`
}

func (p *Platform) UnmarshalYAML(n *yaml.Node) error {
	*p = Platform{}
	if n.Kind == yaml.SequenceNode {
		return n.Decode(&p.Required)
	}
	if n.Kind == yaml.ScalarNode && n.Value == "any" {
		p.Any = true
		return nil
	}
	type plain Platform
	if n.Kind != yaml.MappingNode {
		return errors.New("platform must be a list, any, or a required/preferred/any mapping")
	}
	for i := 0; i < len(n.Content); i += 2 {
		if !slices.Contains([]string{"required", "preferred", "any"}, n.Content[i].Value) {
			return fmt.Errorf("unknown platform field %q", n.Content[i].Value)
		}
	}
	return n.Decode((*plain)(p))
}

type Environment struct {
	Name      string   `json:"name,omitempty" yaml:"name,omitempty"`
	LockFile  string   `json:"lock_file,omitempty" yaml:"lock_file,omitempty"`
	Python    string   `json:"python,omitempty" yaml:"python,omitempty"`
	Platforms []string `json:"platforms,omitempty" yaml:"platforms,omitempty"`
}

func (e *Environment) UnmarshalYAML(n *yaml.Node) error {
	*e = Environment{}
	if n.Kind == yaml.ScalarNode {
		return n.Decode(&e.Name)
	}
	type plain Environment
	for i := 0; i < len(n.Content); i += 2 {
		if !slices.Contains([]string{"name", "lock_file", "python", "platforms"}, n.Content[i].Value) {
			return fmt.Errorf("unknown environment field %q", n.Content[i].Value)
		}
	}
	return n.Decode((*plain)(e))
}
func (e Environment) Key() string {
	if e.Name != "" {
		return e.Name
	}
	return e.LockFile
}

type Spec struct {
	SubmissionKey string `json:"submission_key,omitempty" yaml:"submission_key,omitempty"`

	Files    FileManifest      `json:"files,omitempty" yaml:"files,omitempty"`
	Kind     string            `json:"kind,omitempty" yaml:"kind,omitempty"`
	Program  *Program          `json:"program,omitempty" yaml:"program,omitempty"`
	Args     []string          `json:"args,omitempty" yaml:"args,omitempty"`
	SizeHint SizeHint          `json:"size_hint,omitempty" yaml:"size_hint,omitempty"`
	Inputs   map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Outputs  map[string]string `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Name     string            `json:"name" yaml:"name"`
	Project  string            `json:"project" yaml:"project"`
	Priority string            `json:"priority" yaml:"priority"`
	Logs     string            `json:"logs,omitempty" yaml:"logs,omitempty"`
	Deadline *time.Time        `json:"deadline,omitempty" yaml:"deadline,omitempty"`
	Run      struct {
		Docker           bool              `json:"docker,omitempty" yaml:"docker,omitempty"`
		DockerNetwork    string            `json:"docker_network,omitempty" yaml:"docker_network,omitempty"`
		DockerMemoryGB   float64           `json:"docker_memory_gb,omitempty" yaml:"docker_memory_gb,omitempty"`
		DockerMemorySwap *float64          `json:"docker_memory_swap,omitempty" yaml:"docker_memory_swap,omitempty"`
		DockerSidecars   []DockerSidecar   `json:"docker_sidecars,omitempty" yaml:"docker_sidecars,omitempty"`
		Command          []string          `json:"command" yaml:"command"`
		Workdir          string            `json:"workdir,omitempty" yaml:"workdir,omitempty"`
		Env              map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	} `json:"run" yaml:"run"`
	Needs struct {
		MemoryGB     float64  `json:"memory_gb" yaml:"memory_gb"`
		HostMemoryGB float64  `json:"host_memory_gb,omitempty" yaml:"host_memory_gb,omitempty"`
		Platform     Platform `json:"platform" yaml:"platform"`
		Model        string   `json:"model,omitempty" yaml:"model,omitempty"`
	} `json:"needs" yaml:"needs"`
	Environment     Environment `json:"environment,omitempty" yaml:"environment,omitempty"`
	AllowedMachines []string    `json:"allowed_machines,omitempty" yaml:"allowed_machines,omitempty"`
	StartBy         *time.Time  `json:"start_by,omitempty" yaml:"start_by,omitempty"`
	GiveUpAt        *time.Time  `json:"give_up_at,omitempty" yaml:"give_up_at,omitempty"`
	ExpectedMinutes float64     `json:"expected_minutes,omitempty" yaml:"expected_minutes,omitempty"`
	Resumable       bool        `json:"resumable" yaml:"resumable"`
}

func (s Spec) Validate() error {
	if s.Logs != "" && s.Logs != "keep" && s.Logs != "none" {
		return errors.New("logs must be keep or none")
	}
	if s.Name == "" || s.Project == "" || (!s.extensionJob() && (len(s.Run.Command) == 0 || s.Run.Command[0] == "")) {
		return errors.New("name, project and run.command are required")
	}
	if rank(s.Priority) == 3 {
		return errors.New("priority must be production, deadline or experiment")
	}
	if s.StartBy != nil && s.Priority != "production" {
		return errors.New("start_by is only supported for production")
	}
	if s.Priority == "deadline" && s.Deadline == nil {
		return errors.New("deadline priority requires a deadline")
	}
	if err := validateJobFiles(s); err != nil {
		return err
	}
	if s.extensionJob() {
		return validateExtensionJob(s)
	}
	if s.Kind == "" && (len(s.Inputs)+len(s.Args) > 0 || (len(s.Outputs) > 0 && len(s.Files) == 0) || s.SizeHint.MemoryGB != 0 || s.SizeHint.HostMemoryGB != 0) {
		return errors.New("inputs, outputs, args and size_hint require a kind/program job")
	}
	if s.Needs.MemoryGB <= 0 || math.IsInf(s.Needs.MemoryGB, 0) || math.IsNaN(s.Needs.MemoryGB) {
		return errors.New("needs.memory_gb must be positive: CPU-only work stays outside Mycelium")
	}
	if !validMemoryValue(s.Needs.HostMemoryGB) {
		return errors.New("needs.host_memory_gb must be finite and nonnegative")
	}
	if err := s.validateDockerGroup(); err != nil {
		return err
	}
	if s.Run.Docker {
		for name := range s.Run.Env {
			if dockerDeviceEnvironment(name) {
				return fmt.Errorf("Docker run.env cannot override worker device selector %s", name)
			}
		}
		if _, err := dockerRunArgs(s.Run.Command); err != nil {
			return err
		}
	}
	p := s.Needs.Platform
	modes := 0
	if len(p.Required) > 0 {
		modes++
	}
	if len(p.Preferred) > 0 {
		modes++
	}
	if p.Any {
		modes++
	}
	if modes != 1 {
		return errors.New("needs.platform must specify exactly one of required, preferred or any")
	}
	for _, v := range append(slices.Clone(p.Required), p.Preferred...) {
		if !slices.Contains([]string{"cuda", "xpu", "metal", "fake"}, v) {
			return fmt.Errorf("unknown GPU platform %q", v)
		}
	}
	if s.Environment.Name != "" && s.Environment.LockFile != "" {
		return errors.New("environment must name an environment or a lock_file, not both")
	}
	if s.Environment.LockFile == "" && (s.Environment.Python != "" || len(s.Environment.Platforms) > 0) {
		return errors.New("python and platforms belong to a lock_file environment")
	}
	if s.Environment.LockFile != "" && s.Environment.Python == "" {
		return errors.New("lock_file environment requires python version")
	}
	for _, p := range s.Environment.Platforms {
		if !validGPU(p) {
			return fmt.Errorf("invalid environment platform %s", p)
		}
	}
	return nil
}
func rank(p string) int {
	switch p {
	case "production":
		return 0
	case "deadline":
		return 1
	case "experiment":
		return 2
	}
	return 3
}

type Copy struct {
	StopCause   string              `json:"stop_cause,omitempty"`
	Extension   *ExtensionSelection `json:"extension,omitempty"`
	LossCharged bool                `json:"loss_charged,omitempty"`
	Recoverable bool                `json:"recoverable"`
	Machine     string              `json:"machine"`
	Session     string              `json:"session"`
	Attempt     int                 `json:"attempt"`
	Progress    int64               `json:"progress"`

	// Exact cleanup evidence, retained even after another attempt is assigned.
	ReleaseConfirmed bool   `json:"release_confirmed,omitempty"`
	ReleaseReason    string `json:"release_reason,omitempty"`
}

type Job struct {
	// Durable cancellation fence, independent of diagnostic reason text.
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`

	// Derived status-only policy projection; a pointer preserves an empty host list.
	EligibleHosts           *[]string `json:"eligible_hosts,omitempty"`
	EligibleUnknownMachines []string  `json:"eligible_unknown_machines,omitempty"`
	EligibleUnrestricted    bool      `json:"eligible_unrestricted,omitempty"`

	FileTransport         bool                         `json:"file_transport,omitempty"`
	FileCheckpoints       map[int]FileCheckpoint       `json:"file_checkpoints,omitempty"`
	ResumeFiles           *FileCheckpoint              `json:"resume_files,omitempty"`
	OutputRefs            FileManifest                 `json:"output_refs,omitempty"`
	MeasuredNeeds         *SizeHint                    `json:"measured_needs,omitempty"`
	OOMCapacity           *SizeHint                    `json:"oom_capacity,omitempty"`
	OOMRetries            int                          `json:"oom_retries,omitempty"`
	Extensions            []Extension                  `json:"extensions,omitempty"`
	Extension             *ExtensionSelection          `json:"extension,omitempty"`
	OutputFiles           map[string]string            `json:"output_files,omitempty"`
	ReleaseConfirmed      bool                         `json:"release_confirmed,omitempty"`
	RequeuedByOperator    bool                         `json:"requeued_by_operator,omitempty"`
	LastStopOutcome       string                       `json:"last_stop_outcome,omitempty"`
	ProductionHost        string                       `json:"production_host,omitempty"`
	WaitingSince          time.Time                    `json:"waiting_since,omitempty"`
	AssignedAt            *time.Time                   `json:"assigned_at,omitempty"`
	FinishedAt            *time.Time                   `json:"finished_at,omitempty"`
	QueueOrder            int                          `json:"queue_order,omitempty"`
	EnvironmentFailureLog string                       `json:"environment_failure_log,omitempty"`
	EnvironmentRecipes    map[string]EnvironmentRecipe `json:"environment_recipes,omitempty"`
	Copies                []Copy                       `json:"copies,omitempty"`
	StartWindow           time.Duration                `json:"start_window,omitempty"`
	StartedAt             *time.Time                   `json:"started_at,omitempty"`
	StartLatencySeconds   float64                      `json:"start_latency_seconds"`
	Starts                int                          `json:"starts"`
	InfrastructureRetries int                          `json:"infrastructure_retries,omitempty"`
	Losses                int                          `json:"losses"`
	Progress              int64                        `json:"progress"`
	ID                    string                       `json:"id"`
	Spec                  Spec                         `json:"spec"`
	State                 string                       `json:"state"`
	Attempt               int                          `json:"attempt"`
	Machine               string                       `json:"machine,omitempty"`
	Session               string                       `json:"session,omitempty"`
	Queued                time.Time                    `json:"queued"`
	Front                 int64                        `json:"front"`
	StopAt                *time.Time                   `json:"stop_at,omitempty"`
	Reason                string                       `json:"reason,omitempty"`
	Checkpoint            string                       `json:"checkpoint"`
}
type Machine struct {
	queueMemoryPolicy bool // Worker-only: allowance came from the authenticated queue.

	HostRevision                HostRevision        `json:"host_revision"`
	HostBoot                    string              `json:"host_boot_id,omitempty"`
	Capabilities                []string            `json:"capabilities,omitempty"`
	MaxJobs                     int                 `json:"max_jobs,omitempty"`
	AdmissionReason             string              `json:"admission_reason,omitempty"`
	FileTransferError           string              `json:"file_transfer_error,omitempty"`
	CapabilityProfile           *CapabilityProfile  `json:"capability_profile,omitempty"`
	ExtensionReports            []ExtensionReport   `json:"extension_reports,omitempty"`
	HostMemoryAllowanceBytes    int64               `json:"memory_allowance_bytes,omitempty"`
	HostMemoryQualification     string              `json:"memory_allowance_qualification,omitempty"`
	HostMemoryPolicyMismatch    bool                `json:"host_memory_policy_mismatch,omitempty"`
	EffectiveHostMemoryBudgetGB float64             `json:"effective_host_memory_budget_gb,omitempty"`
	HostMemoryInUseGB           float64             `json:"host_memory_in_use_gb,omitempty"`
	hostBudgetGB                float64             // Effective shared-host queue allowance; compared against every device report.
	modelReservationDebtGB      float64             // Reservation deficit hidden by clamping scheduler free GPU memory to zero.
	hostReservationDebtGB       float64             // Same deficit for the shared host allowance.
	MemoryEnforcement           string              `json:"memory_enforcement,omitempty"`
	HostMemoryBudgetGB          float64             `json:"host_memory_budget_gb,omitempty"`
	MemoryCeilingSlackGB        float64             `json:"memory_ceiling_slack_gb,omitempty"`
	DockerCleanup               string              `json:"docker_cleanup,omitempty"`
	Disabled                    string              `json:"disabled,omitempty"`
	HostID                      string              `json:"host_id,omitempty"`
	HostAvailableMemoryGB       float64             `json:"host_available_memory_gb"`
	FreeHostMemoryGB            float64             `json:"free_host_memory_gb"`
	HostMemoryMeasured          bool                `json:"host_memory_measured,omitempty"`
	HostMemoryError             string              `json:"host_memory_error,omitempty"`
	UnifiedMemory               bool                `json:"unified_memory,omitempty"`
	MemoryUsedGB                float64             `json:"memory_used_gb"`
	TotalMemoryGB               float64             `json:"total_memory_gb"`
	ReservedMemoryGB            float64             `json:"reserved_memory_gb"`
	Device                      string              `json:"device,omitempty"`
	EnvironmentBuilder          bool                `json:"environment_builder,omitempty"`
	EnvironmentReports          []EnvironmentReport `json:"environment_reports,omitempty"`
	EnvironmentBuildSeconds     map[string]float64  `json:"environment_build_seconds,omitempty"`
	ModelCopySeconds            map[string]float64  `json:"model_copy_seconds,omitempty"`
	CopyReports                 []CopyReport        `json:"copy_reports,omitempty"`
	CopyBytesPerSecond          float64             `json:"copy_bytes_per_second,omitempty"`
	ApprovedModels              map[string]string   `json:"approved_models,omitempty"`
	CachedModels                []string            `json:"cached_models,omitempty"`
	CacheFreeBytes              int64               `json:"cache_free_bytes"`
	CacheError                  string              `json:"cache_error,omitempty"`
	MemoryError                 string              `json:"memory_error,omitempty"`
	MemoryMeasured              bool                `json:"memory_measured,omitempty"`
	ModelConfigHash             string              `json:"model_config_hash,omitempty"`
	ModelURL                    string              `json:"model_url,omitempty"`
	CheckpointUnavailable       bool                `json:"checkpoint_unavailable"`
	ID                          string              `json:"id"`
	Session                     string              `json:"session"`
	GPU                         string              `json:"gpu"`
	FreeMemoryGB                float64             `json:"free_memory_gb"`
	Models                      []string            `json:"models,omitempty"`
	Environments                []string            `json:"environments,omitempty"`
	LastSeen                    time.Time           `json:"last_seen"`
	Leaving                     bool                `json:"leaving"`
	Gone                        bool                `json:"gone"`

	// Explicit per-local_source tri-state; omission never authorizes use.
	ModelApprovalStates map[string]ModelApprovalState `json:"model_approval_states,omitempty"`
}
type Execution struct {
	Progress int64  `json:"progress,omitempty"`
	ID       string `json:"id"`
	Attempt  int    `json:"attempt"`
}
type Checkin struct {
	Started         bool          `json:"started,omitempty"`
	ModelReports    []ModelReport `json:"model_reports,omitempty"`
	PreviousSession string        `json:"previous_session,omitempty"`
	Fresh           bool          `json:"fresh,omitempty"`
	Machine         Machine       `json:"machine"`
	Running         *Execution    `json:"running,omitempty"`
}
type Reply struct {
	HostMaintenance  string              `json:"host_maintenance,omitempty"`
	HostMemoryBudget *HostMemoryBudget   `json:"host_memory_budget,omitempty"`
	FileCheckpoints  bool                `json:"file_checkpoints,omitempty"`
	AdmissionBlocked bool                `json:"admission_blocked,omitempty"`
	EnvironmentPlan  []EnvironmentRecipe `json:"environment_plan,omitempty"`
	CopyPlan         []CopyTask          `json:"copy_plan,omitempty"`
	CacheUsage       []CopyTask          `json:"cache_usage,omitempty"`
	CachePins        []string            `json:"cache_pins,omitempty"`
	ModelPlan        []ModelSlot         `json:"model_plan,omitempty"`
	Hold             bool                `json:"hold"`
	CheckpointRoot   string              `json:"checkpoint_root"`
	Job              *Job                `json:"job,omitempty"`
	Stop             bool                `json:"stop"`
	Kill             bool                `json:"kill"`
	Leave            bool                `json:"leave"`
	GraceSeconds     float64             `json:"grace_seconds"`
}
type Result struct {
	PoliteStop           bool         `json:"polite_stop,omitempty"`
	OutputRefs           FileManifest `json:"output_refs,omitempty"`
	Usage                *SizeHint    `json:"usage,omitempty"`
	OutOfMemory          bool         `json:"out_of_memory,omitempty"`
	ExtensionUnavailable bool         `json:"extension_unavailable,omitempty"`
	OperatorStop         bool         `json:"operator_stop,omitempty"`
	ReleaseOnly          bool         `json:"release_only,omitempty"`
	Forced               bool         `json:"forced,omitempty"`
	ReleasePending       bool         `json:"release_pending,omitempty"`
	Machine              string       `json:"machine"`
	Session              string       `json:"session"`
	Execution
	Interrupted bool   `json:"interrupted,omitempty"`
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
}
type LogChunk struct {
	Result
	Offset int64  `json:"offset,omitempty"`
	Text   string `json:"text"`
}

// HoldDetail describes a host fence independently of its compatibility text.
// A missing ClearsAt is not a promise of automatic recovery.
type HoldDetail struct {
	Host                   string     `json:"host"`
	Kind                   string     `json:"kind"`
	OperatorActionRequired bool       `json:"operator_action_required"`
	ClearsAt               *time.Time `json:"clears_at,omitempty"`
	Text                   string     `json:"text"`
}

type Snapshot struct {
	HoldsDetail  []HoldDetail  `json:"holds_detail"`
	HostHolds    []string      `json:"host_holds,omitempty"`
	Now          time.Time     `json:"now"`
	Usage        UsageReport   `json:"usage"`
	SyncRequests []SyncRequest `json:"sync_requests"`
	Pins         []ModelPin    `json:"pins"`
	LoadedModels []ModelSlot   `json:"loaded_models"`
	Warning      string        `json:"warning,omitempty"`
	Jobs         []*Job        `json:"jobs"`
	Machines     []*Machine    `json:"machines"`
}

func terminal(s string) bool { return s == "succeeded" || s == "failed" || s == "cancelled" }
