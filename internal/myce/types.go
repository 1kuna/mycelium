package myce

import (
	"errors"
	"fmt"
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

// JSON is a subset of YAML. Both entry points use the same typed node grammar;
// node tags prevent YAML's scalar coercions from widening the accepted forms.
func (p *Platform) UnmarshalJSON(data []byte) error {
	var n yaml.Node
	if err := yaml.Unmarshal(data, &n); err != nil {
		return err
	}
	if len(n.Content) != 1 {
		return errors.New("needs.platform is required")
	}
	return p.UnmarshalYAML(n.Content[0])
}

func (p *Platform) UnmarshalYAML(n *yaml.Node) error {
	var value Platform
	invalid := func() error {
		return reject("job_platform_invalid", nil)
	}
	list := func(n *yaml.Node) ([]string, error) {
		if n.Kind != yaml.SequenceNode || len(n.Content) == 0 {
			return nil, invalid()
		}
		var out []string
		for _, item := range n.Content {
			if item.Kind != yaml.ScalarNode || item.Tag != "!!str" || !validGPU(item.Value) {
				return nil, invalid()
			}
			out = append(out, item.Value)
		}
		return out, nil
	}
	switch n.Kind {
	case yaml.ScalarNode:
		// YAML treats null as the unset value without calling UnmarshalYAML.
		if n.Tag == "!!null" {
			*p = value
			return nil
		}
		if n.Tag != "!!str" {
			return invalid()
		}
		if n.Value == "any" {
			value.Any = true
		} else if validGPU(n.Value) {
			value.Required = []string{n.Value}
		} else {
			return invalid()
		}
	case yaml.SequenceNode:
		var err error
		value.Required, err = list(n)
		if err != nil {
			return err
		}
	case yaml.MappingNode:
		// The empty canonical value also occurs in stored extension jobs.
		if len(n.Content) == 0 {
			*p = value
			return nil
		}
		if len(n.Content) != 2 || n.Content[0].Tag != "!!str" {
			return invalid()
		}
		key, item := n.Content[0].Value, n.Content[1]
		var err error
		switch key {
		case "required":
			value.Required, err = list(item)
		case "preferred":
			value.Preferred, err = list(item)
		case "any":
			if item.Kind != yaml.ScalarNode || item.Tag != "!!bool" || item.Value != "true" {
				return invalid()
			}
			value.Any = true
		default:
			return invalid()
		}
		if err != nil {
			return err
		}
	default:
		return invalid()
	}
	*p = value
	return nil
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
	Isolation    string                 `json:"isolation,omitempty" yaml:"isolation,omitempty"`
	Reason       string                 `json:"reason,omitempty" yaml:"reason,omitempty"`
	SharedInputs map[string]SharedInput `json:"shared_inputs,omitempty" yaml:"shared_inputs,omitempty"`
	// PrepareSource extracts the validated CLI snapshot before member launch,
	// then stages explicit inputs over it. It is persisted, never an author field.
	PrepareSource bool `json:"prepare_source,omitempty" yaml:"-"`
	// ProfileMemory binds the CLI's base command/check and common input bytes.
	// Per-item arguments and files remain in the submitted execution spec.
	ProfileMemory string            `json:"profile_memory,omitempty" yaml:"-"`
	Diagnostics   *DiagnosticSpec   `json:"diagnostics,omitempty" yaml:"diagnostics,omitempty"`
	Disk          *DiskNeeds        `json:"disk,omitempty" yaml:"disk,omitempty"`
	After         *PieceDependency  `json:"after,omitempty" yaml:"after,omitempty"`
	Targets       []ExecutionTarget `json:"targets,omitempty" yaml:"targets,omitempty"`
	RequireTarget bool              `json:"require_target,omitempty" yaml:"require_target,omitempty"`
	FanOut        *FanOut           `json:"fan_out,omitempty" yaml:"fan_out,omitempty"`
	Workload      *Workload         `json:"workload,omitempty" yaml:"workload,omitempty"`
	SubmissionKey string            `json:"submission_key,omitempty" yaml:"submission_key,omitempty"`

	Files           FileManifest           `json:"files,omitempty" yaml:"files,omitempty"`
	Kind            string                 `json:"kind,omitempty" yaml:"kind,omitempty"`
	Program         *Program               `json:"program,omitempty" yaml:"program,omitempty"`
	Args            []string               `json:"args,omitempty" yaml:"args,omitempty"`
	SizeHint        SizeHint               `json:"size_hint,omitempty" yaml:"size_hint,omitempty"`
	Inputs          map[string]string      `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Outputs         map[string]string      `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Name            string                 `json:"name" yaml:"name"`
	Project         string                 `json:"project" yaml:"project"`
	Priority        string                 `json:"priority" yaml:"priority"`
	Class           string                 `json:"class,omitempty" yaml:"class,omitempty"`
	Logs            string                 `json:"logs,omitempty" yaml:"logs,omitempty"`
	Deadline        *time.Time             `json:"deadline,omitempty" yaml:"deadline,omitempty"`
	Run             RunSpec                `json:"run" yaml:"run"`
	Needs           ResourceNeeds          `json:"needs" yaml:"needs"`
	Execution       *ExecutionSpec         `json:"execution,omitempty" yaml:"execution,omitempty"`
	Extension       string                 `json:"extension,omitempty" yaml:"extension,omitempty"`
	ExtensionDigest string                 `json:"extension_digest,omitempty" yaml:"extension_digest,omitempty"`
	ExtensionCheck  *ExtensionCheckRequest `json:"extension_check,omitempty" yaml:"extension_check,omitempty"`
	Environment     Environment            `json:"environment,omitempty" yaml:"environment,omitempty"`
	AllowedMachines []string               `json:"allowed_machines,omitempty" yaml:"allowed_machines,omitempty"`
	StartBy         *time.Time             `json:"start_by,omitempty" yaml:"start_by,omitempty"`
	StartWindow     string                 `json:"start_window,omitempty" yaml:"start_window,omitempty"`
	GiveUpAt        *time.Time             `json:"give_up_at,omitempty" yaml:"give_up_at,omitempty"`
	ExpectedMinutes float64                `json:"expected_minutes,omitempty" yaml:"expected_minutes,omitempty"`
	Resumable       bool                   `json:"resumable" yaml:"resumable"`
}

func (s Spec) Validate() error {
	if err := ValidateNativeIsolation(s.Isolation, s.Reason); err != nil {
		return reject("job_execution_invalid", err)
	}
	if s.extensionJob() && (s.Isolation != "" || s.Reason != "") {
		return reject("job_execution_invalid", errors.New("extension registration owns isolation; job overrides are not allowed"))
	}
	if s.PrepareSource {
		if _, ok := s.Files[".myce-source.tar"]; !ok {
			return reject("job_files_invalid", errors.New("prepared source requires a frozen source archive"))
		}
	}
	if s.ProfileMemory != "" && (!profileMemoryWork(s) || (FileRef{SHA256: s.ProfileMemory}).validate() != nil) {
		return reject("job_profile_invalid", errors.New("profile_memory requires a profile and SHA-256 identity"))
	}
	if err := validateDiagnosticSpec(s); err != nil {
		return reject("job_diagnostics_invalid", err)
	}
	for _, target := range s.Targets {
		if !extensionName.MatchString(target.OS) || !extensionName.MatchString(target.Arch) || !validGPU(target.Platform) {
			return reject("job_placement_invalid", errors.New("targets require explicit OS, arch and platform"))
		}
	}
	if err := validatePieceDependency(s); err != nil {
		return reject("job_files_invalid", err)
	}
	if err := validateFanOut(s); err != nil {
		return err
	}
	if s.Workload != nil && (!s.extensionJob() || !s.Workload.valid()) {
		return errors.New("workload requires extension work, input_shape and positive units")
	}
	if err := normalizeResources(&s); err != nil {
		return err
	}
	if !s.extensionJob() {
		if err := lowerExecution(&s); err != nil {
			return reject("job_execution_invalid", err)
		}
	}
	if err := normalizeJobClass(&s); err != nil {
		return err
	}
	if s.Logs != "" && s.Logs != "keep" && s.Logs != "none" {
		return reject("job_logs_invalid", errors.New("logs must be keep or none"))
	}
	if s.Name == "" || s.Project == "" || (!s.extensionJob() && (len(s.Run.Command) == 0 || s.Run.Command[0] == "")) {
		return reject("job_fields_required", errors.New("name, project and run.command are required"))
	}
	if s.Priority != "" && legacyRank(s.Priority) == 3 {
		return reject("job_priority_invalid", errors.New("priority must be production, deadline or experiment"))
	}
	if s.StartBy != nil && s.Priority != "production" {
		return reject("job_deadline_invalid", errors.New("start_by is only supported for production"))
	}
	if s.StartWindow != "" {
		window, err := time.ParseDuration(s.StartWindow)
		if s.Priority != "production" || err != nil || window <= 0 || window > MaxProductionStartWindow {
			return reject("job_deadline_invalid", errors.New("start_window requires production and a duration greater than 0 and at most 1h"))
		}
	}
	if s.Priority == "deadline" && s.Deadline == nil {
		return reject("job_deadline_invalid", errors.New("deadline priority requires a deadline"))
	}
	if err := validateJobFiles(s); err != nil {
		return reject("job_files_invalid", err)
	}
	if s.extensionJob() {
		return rejectIf("job_program_invalid", validateExtensionJob(s))
	}
	if s.Kind == "" && (len(s.Inputs)+len(s.Args) > 0 || (len(s.Outputs) > 0 && len(s.Files) == 0 && len(s.SharedInputs) == 0 && s.After == nil) || s.SizeHint.MemoryGB != 0 || s.SizeHint.HostMemoryGB != 0) {
		return reject("job_program_invalid", errors.New("inputs, outputs, args and size_hint require a kind/program job"))
	}
	if !validMemoryValue(s.Needs.MemoryGB) || (s.Needs.Memory != "auto" && ((!s.Needs.cpuOnly() && s.Needs.MemoryGB <= 0) || (s.Needs.cpuOnly() && s.Needs.HostMemoryGB <= 0))) {
		return reject("job_memory_invalid", errors.New("declare positive memory_gb (host RAM for CPU work, GPU memory otherwise)"))
	}
	if !validMemoryValue(s.Needs.HostMemoryGB) {
		return reject("job_memory_invalid", errors.New("needs.host_memory_gb must be finite and nonnegative"))
	}
	if err := s.validateDockerGroup(); err != nil {
		return reject("job_docker_invalid", err)
	}
	if s.Run.Docker {
		for name := range s.Run.Env {
			if dockerDeviceEnvironment(name) {
				return reject("job_docker_invalid", fmt.Errorf("Docker run.env cannot override worker device selector %s", name))
			}
		}
		if _, err := dockerRunArgs(s.Run.Command); err != nil {
			return reject("job_docker_invalid", err)
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
		return reject("job_platform_invalid", errors.New("needs.platform must specify exactly one of required, preferred or any"))
	}
	for _, v := range append(slices.Clone(p.Required), p.Preferred...) {
		if !slices.Contains([]string{"cuda", "xpu", "metal", "fake", "cpu"}, v) {
			return reject("job_platform_invalid", fmt.Errorf("unknown GPU platform %q", v))
		}
	}
	if s.Environment.Name != "" && s.Environment.LockFile != "" {
		return reject("job_environment_invalid", errors.New("environment must name an environment or a lock_file, not both"))
	}
	if s.Environment.LockFile == "" && (s.Environment.Python != "" || len(s.Environment.Platforms) > 0) {
		return reject("job_environment_invalid", errors.New("python and platforms belong to a lock_file environment"))
	}
	if s.Environment.LockFile != "" && s.Environment.Python == "" {
		return reject("job_environment_invalid", errors.New("lock_file environment requires python version"))
	}
	for _, p := range s.Environment.Platforms {
		if !validGPU(p) {
			return reject("job_environment_invalid", fmt.Errorf("invalid environment platform %s", p))
		}
	}
	return nil
}
func legacyRank(p string) int {
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
	ProcessID        string              `json:"process_id,omitempty"`
	AllocatedDevices []string            `json:"allocated_devices,omitempty"`
	PriorityStop     *PriorityPreemption `json:"priority_stop,omitempty"`
	StopCause        string              `json:"stop_cause,omitempty"`
	Extension        *ExtensionSelection `json:"extension,omitempty"`
	LossCharged      bool                `json:"loss_charged,omitempty"`
	Recoverable      bool                `json:"recoverable"`
	Machine          string              `json:"machine"`
	Session          string              `json:"session"`
	Attempt          int                 `json:"attempt"`
	Progress         int64               `json:"progress"`

	// Exact cleanup evidence, retained even after another attempt is assigned.
	ReleaseConfirmed bool   `json:"release_confirmed,omitempty"`
	ReleaseReason    string `json:"release_reason,omitempty"`
}

type Job struct {
	Ledger        *JobLedgerInfo   `json:"ledger,omitempty"`
	LedgerEntries []JobLedgerEntry `json:"ledger_entries,omitempty"`
	// Read-only diagnostic retained when execution payloads are redacted.
	Isolation string `json:"isolation,omitempty"`
	// SubmissionReused is set only on a keyed submit reply, never on stored jobs.
	SubmissionReused       bool             `json:"submission_reused,omitempty"`
	LastHealthAlertAttempt int              `json:"last_health_alert_attempt,omitempty"`
	AttemptHistory         []AttemptOutcome `json:"attempt_history,omitempty"`
	// Read-only observations; never physical-release evidence for another attempt.
	Cancellation      *Cancellation      `json:"cancellation,omitempty"`
	CancellationScope *CancellationScope `json:"cancellation_scope,omitempty"`

	// Simulator-only baseline barrier; never persisted or exposed as an API.
	simGatherWait    bool
	Dependency       *PieceBinding   `json:"dependency,omitempty"`
	FanOut           *FanOutProgress `json:"fan_out,omitempty"`
	Batch            *BatchWork      `json:"batch,omitempty"`
	Timing           *WorkTiming     `json:"timing,omitempty"`
	DiagnosticCode   string          `json:"diagnostic_code,omitempty"`
	AllocatedDevices []string        `json:"allocated_devices,omitempty"`
	PreemptionCount  int             `json:"preemption_count,omitempty"`
	PriorityPolicy   PriorityMatch   `json:"priority_policy"`
	ExecutionCause   *RuntimeCause   `json:"execution_cause,omitempty"`
	CleanupCause     *RuntimeCause   `json:"cleanup_cause,omitempty"`
	// Durable cancellation fence, independent of diagnostic reason text.
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`

	// Derived status-only policy projection; a pointer preserves an empty host list.
	EligibleHosts           *[]string `json:"eligible_hosts,omitempty"`
	EligibleUnknownMachines []string  `json:"eligible_unknown_machines,omitempty"`
	EligibleUnrestricted    bool      `json:"eligible_unrestricted,omitempty"`

	SidecarEnvironmentRecipes map[string]map[string]EnvironmentRecipe `json:"sidecar_environment_recipes,omitempty"`

	FileTransport         bool                         `json:"file_transport,omitempty"`
	FileCheckpoints       map[int]FileCheckpoint       `json:"file_checkpoints,omitempty"`
	ResumeFiles           *FileCheckpoint              `json:"resume_files,omitempty"`
	FilesExpired          bool                         `json:"files_expired,omitempty"`
	FileReclamation       *FileReclamation             `json:"file_reclamation,omitempty"`
	OutputRefs            FileManifest                 `json:"output_refs,omitempty"`
	MemoryEnvelope        *JobMemoryEnvelope           `json:"memory_envelope,omitempty"`
	MemoryAttempts        []AttemptMemoryFacts         `json:"memory_attempts,omitempty"`
	MemoryAttemptsDropped int                          `json:"memory_attempts_dropped,omitempty"`
	MemoryFallback        *SizeHint                    `json:"memory_fallback,omitempty"`
	MemoryOOMFloor        *SizeHint                    `json:"memory_oom_floor,omitempty"`
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
	InputsStagedAt        *time.Time                   `json:"inputs_staged_at,omitempty"` // Worker clock; diagnostic only, scoped to this attempt.
	FinishedAt            *time.Time                   `json:"finished_at,omitempty"`
	QueueOrder            int                          `json:"queue_order,omitempty"`
	EnvironmentFailureLog string                       `json:"environment_failure_log,omitempty"`
	EnvironmentRecipes    map[string]EnvironmentRecipe `json:"environment_recipes,omitempty"`
	Copies                []Copy                       `json:"copies,omitempty"`
	StartWindow           time.Duration                `json:"start_window,omitempty"`
	StartByExplicit       *bool                        `json:"start_by_explicit,omitempty"`
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
	ExternalMemory         *ExternalMemoryHistory `json:"external_memory,omitempty"`
	MemoryObservation      *HostMemoryObservation `json:"memory_observation,omitempty"`
	Release                *RunningRelease        `json:"release,omitempty"`
	Fleet                  *FleetRuntimeProof     `json:"fleet,omitempty"`
	DiskReceivedAt         time.Time              `json:"disk_received_at,omitempty"` // Queue-owned receipt time; only disk check-ins renew it.
	DiskVolumes            []DiskVolume           `json:"disk_volumes,omitempty"`
	Enrollment             *EnrollmentProof       `json:"enrollment,omitempty"`
	Recovering             bool                   `json:"recovering,omitempty"`
	RecoveringSince        *time.Time             `json:"recovering_since,omitempty"`
	RecoveryNeedsAttention bool                   `json:"recovery_needs_attention,omitempty"` // Status-only projection.
	Replacement            *MachineReplacement    `json:"replacement,omitempty"`
	Devices                []MachineDevice        `json:"devices"`
	InventoryReason        string                 `json:"inventory_reason,omitempty"`
	CPUIsolation           string                 `json:"cpu_isolation,omitempty"`
	cpuLedger              bool
	CPUReserveCores        int           `json:"cpu_reserve_cores,omitempty"`
	CPUBudgetCores         int           `json:"cpu_budget_cores,omitempty"`
	EffectiveCPUCores      int           `json:"effective_cpu_cores"`
	CPUInUseCores          int           `json:"cpu_in_use_cores"`
	FreeCPUCores           int           `json:"free_cpu_cores"`
	CPUCapacityReason      string        `json:"cpu_capacity_reason,omitempty"`
	DockerCause            *RuntimeCause `json:"docker_cause,omitempty"`
	BenchmarkPending       bool          `json:"benchmark_pending,omitempty"`

	CachedSharedInputs  []string             `json:"cached_shared_inputs,omitempty"`
	SharedInputEvidence *SharedInputEvidence `json:"shared_input_evidence,omitempty"`

	Capabilities                []string            `json:"capabilities,omitempty"`
	NativeIsolationWarning      string              `json:"native_isolation_warning,omitempty"`
	MaxJobs                     int                 `json:"max_jobs,omitempty"`
	AdmissionReason             string              `json:"admission_reason,omitempty"`
	FileTransferError           string              `json:"file_transfer_error,omitempty"`
	CapabilityProfile           *CapabilityProfile  `json:"capability_profile,omitempty"`
	CPUExtensionFingerprint     string              `json:"cpu_extension_fingerprint,omitempty"`
	ExtensionFingerprint        string              `json:"extension_fingerprint,omitempty"`
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
	Disabled                    string              `json:"disabled,omitempty"`    // Status-only queue maintenance projection.
	LastResort                  bool                `json:"last_resort,omitempty"` // Status-only queue placement policy.
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
	AttemptProtocol string            `json:"attempt_protocol,omitempty"`
	Attempts        []AttemptReport   `json:"attempts,omitempty"`
	Recovering      bool              `json:"recovering,omitempty"` // Reconcile custody without offering new work.
	CurveReports    []WorkObservation `json:"curve_reports,omitempty"`
	BenchmarkDone   string            `json:"benchmark_done,omitempty"` // Exact preparation attempt, after process cleanup.
	Started         bool              `json:"started,omitempty"`
	ModelReports    []ModelReport     `json:"model_reports,omitempty"`
	PreviousSession string            `json:"previous_session,omitempty"`
	Fresh           bool              `json:"fresh,omitempty"`
	Machine         Machine           `json:"machine"`
	Running         *Execution        `json:"running,omitempty"`
}
type Reply struct {
	ExtensionQualificationPlan []ExtensionQualificationPlan `json:"extension_qualification_plan,omitempty"`

	AttemptProtocol   string              `json:"attempt_protocol,omitempty"`
	Attempts          []AttemptDirective  `json:"attempts,omitempty"`
	PriorityStop      *PriorityPreemption `json:"priority_stop,omitempty"`
	CPUBudgetCores    *int                `json:"cpu_budget_cores,omitempty"`
	BenchmarkCurves   []WorkObservation   `json:"benchmark_curves,omitempty"`
	BenchmarkMachine  *Machine            `json:"benchmark_machine,omitempty"`
	BenchmarkPlan     *HostLaunch         `json:"benchmark_plan,omitempty"`
	PreparationStopAt *time.Time          `json:"preparation_stop_at,omitempty"`
	HostMaintenance   string              `json:"host_maintenance,omitempty"`
	HostMemoryBudget  *HostMemoryBudget   `json:"host_memory_budget,omitempty"`
	FileCheckpoints   bool                `json:"file_checkpoints,omitempty"`
	AdmissionBlocked  bool                `json:"admission_blocked,omitempty"`
	EnvironmentPlan   []EnvironmentRecipe `json:"environment_plan,omitempty"`
	CopyPlan          []CopyTask          `json:"copy_plan,omitempty"`
	CacheUsage        []CopyTask          `json:"cache_usage,omitempty"`
	CachePins         []string            `json:"cache_pins,omitempty"`
	ModelPlan         []ModelSlot         `json:"model_plan,omitempty"`
	Hold              bool                `json:"hold"`
	CheckpointRoot    string              `json:"checkpoint_root"`
	Job               *Job                `json:"job,omitempty"`
	Stop              bool                `json:"stop"`
	Kill              bool                `json:"kill"`
	Leave             bool                `json:"leave"`
	UserCancel        bool                `json:"user_cancel,omitempty"`
	GraceSeconds      float64             `json:"grace_seconds"`
}

// AttemptReport binds heartbeat, progress and Started to an exact execution.
// Collections have no wire cardinality limit; admission remains in the planner.
type AttemptReport struct {
	DockerCause *RuntimeCause `json:"docker_cause,omitempty"`
	Execution
	InputsStagedAt *time.Time `json:"inputs_staged_at,omitempty"`
	Session        string     `json:"session"`
	Started        bool       `json:"started,omitempty"`
}

// AttemptDirective carries one assignment or control decision for an exact key.
// Job retains the ordinary job-v1 assignment shape and must match that key.
type AttemptDirective struct {
	Execution
	Session      string              `json:"session"`
	Job          *Job                `json:"job,omitempty"`
	Hold         bool                `json:"hold,omitempty"`
	Stop         bool                `json:"stop,omitempty"`
	Kill         bool                `json:"kill,omitempty"`
	UserCancel   bool                `json:"user_cancel,omitempty"`
	GraceSeconds float64             `json:"grace_seconds,omitempty"`
	PriorityStop *PriorityPreemption `json:"priority_stop,omitempty"`
}
type Result struct {
	InputsStagedAt       *time.Time          `json:"inputs_staged_at,omitempty"`
	Diagnostics          *AttemptDiagnostics `json:"diagnostics,omitempty"`
	PriorityStop         *PriorityPreemption `json:"priority_stop,omitempty"`
	ExecutionCause       *RuntimeCause       `json:"execution_cause,omitempty"`
	CleanupCause         *RuntimeCause       `json:"cleanup_cause,omitempty"`
	PoliteStop           bool                `json:"polite_stop,omitempty"`
	OutputRefs           FileManifest        `json:"output_refs,omitempty"`
	MemoryPeak           *AttemptMemoryPeak  `json:"memory_peak,omitempty"`
	Usage                *SizeHint           `json:"usage,omitempty"`
	OutOfMemory          bool                `json:"out_of_memory,omitempty"`
	ExtensionUnavailable bool                `json:"extension_unavailable,omitempty"`
	OperatorStop         bool                `json:"operator_stop,omitempty"`
	ReleaseOnly          bool                `json:"release_only,omitempty"`
	Forced               bool                `json:"forced,omitempty"`
	ReleasePending       bool                `json:"release_pending,omitempty"`
	Machine              string              `json:"machine"`
	Session              string              `json:"session"`
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
	RequestLatencies    []RequestLatency     `json:"request_latencies,omitempty"`
	ComponentExclusions []ComponentExclusion `json:"component_exclusions,omitempty"`
	DeviceMaintenance   []DeviceMaintenance  `json:"device_maintenance,omitempty"`

	HealthAlerts   []HealthAlert        `json:"health_alerts,omitempty"`
	QueueRelease   RunningRelease       `json:"queue_release"`
	FleetUpgrade   *FleetUpgradeStatus  `json:"fleet_upgrade,omitempty"`
	FileStore      *FileStoreStatus     `json:"file_store,omitempty"`
	HostStates     map[string]string    `json:"host_states,omitempty"`
	WorkCurves     []WorkObservation    `json:"work_curves"`
	PriorityPolicy PriorityPolicyRecord `json:"priority_policy"`
	HoldsDetail    []HoldDetail         `json:"holds_detail"`
	HostHolds      []string             `json:"host_holds,omitempty"`
	Now            time.Time            `json:"now"`
	Usage          UsageReport          `json:"usage"`
	SyncRequests   []SyncRequest        `json:"sync_requests"`
	Pins           []ModelPin           `json:"pins"`
	LoadedModels   []ModelSlot          `json:"loaded_models"`
	Warning        string               `json:"warning,omitempty"`
	Jobs           []*Job               `json:"jobs"`
	Machines       []*Machine           `json:"machines"`
}

func terminal(s string) bool { return s == "succeeded" || s == "failed" || s == "cancelled" }
