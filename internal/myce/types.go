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
	Name     string     `json:"name" yaml:"name"`
	Project  string     `json:"project" yaml:"project"`
	Priority string     `json:"priority" yaml:"priority"`
	Deadline *time.Time `json:"deadline,omitempty" yaml:"deadline,omitempty"`
	Run      struct {
		Command []string          `json:"command" yaml:"command"`
		Workdir string            `json:"workdir,omitempty" yaml:"workdir,omitempty"`
		Env     map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	} `json:"run" yaml:"run"`
	Needs struct {
		MemoryGB float64  `json:"memory_gb" yaml:"memory_gb"`
		Platform Platform `json:"platform" yaml:"platform"`
		Model    string   `json:"model,omitempty" yaml:"model,omitempty"`
	} `json:"needs" yaml:"needs"`
	Environment     Environment `json:"environment,omitempty" yaml:"environment,omitempty"`
	AllowedMachines []string    `json:"allowed_machines,omitempty" yaml:"allowed_machines,omitempty"`
	GiveUpAt        *time.Time  `json:"give_up_at,omitempty" yaml:"give_up_at,omitempty"`
	ExpectedMinutes float64     `json:"expected_minutes,omitempty" yaml:"expected_minutes,omitempty"`
	Resumable       bool        `json:"resumable" yaml:"resumable"`
}

func (s Spec) Validate() error {
	if s.Name == "" || s.Project == "" || len(s.Run.Command) == 0 || s.Run.Command[0] == "" {
		return errors.New("name, project and run.command are required")
	}
	if rank(s.Priority) == 3 {
		return errors.New("priority must be production, deadline or experiment")
	}
	if s.Priority == "deadline" && s.Deadline == nil {
		return errors.New("deadline priority requires a deadline")
	}
	if s.Needs.MemoryGB <= 0 || math.IsInf(s.Needs.MemoryGB, 0) || math.IsNaN(s.Needs.MemoryGB) {
		return errors.New("needs.memory_gb must be positive: CPU-only work stays outside Mycelium")
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
	Recoverable bool   `json:"recoverable"`
	Machine     string `json:"machine"`
	Session     string `json:"session"`
	Attempt     int    `json:"attempt"`
	Progress    int64  `json:"progress"`
}

type Job struct {
	EnvironmentFailureLog string                       `json:"environment_failure_log,omitempty"`
	EnvironmentRecipes    map[string]EnvironmentRecipe `json:"environment_recipes,omitempty"`
	Copies                []Copy                       `json:"copies,omitempty"`
	StartedAt             *time.Time                   `json:"started_at,omitempty"`
	StartLatencySeconds   float64                      `json:"start_latency_seconds"`
	Starts                int                          `json:"starts"`
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
	Device                  string              `json:"device,omitempty"`
	EnvironmentBuilder      bool                `json:"environment_builder,omitempty"`
	EnvironmentReports      []EnvironmentReport `json:"environment_reports,omitempty"`
	EnvironmentBuildSeconds map[string]float64  `json:"environment_build_seconds,omitempty"`
	ModelCopySeconds        map[string]float64  `json:"model_copy_seconds,omitempty"`
	CopyReports             []CopyReport        `json:"copy_reports,omitempty"`
	CopyBytesPerSecond      float64             `json:"copy_bytes_per_second,omitempty"`
	CachedModels            []string            `json:"cached_models,omitempty"`
	CacheFreeBytes          int64               `json:"cache_free_bytes"`
	CacheError              string              `json:"cache_error,omitempty"`
	MemoryError             string              `json:"memory_error,omitempty"`
	MemoryMeasured          bool                `json:"memory_measured,omitempty"`
	ModelConfigHash         string              `json:"model_config_hash,omitempty"`
	ModelURL                string              `json:"model_url,omitempty"`
	CheckpointUnavailable   bool                `json:"checkpoint_unavailable"`
	ID                      string              `json:"id"`
	Session                 string              `json:"session"`
	GPU                     string              `json:"gpu"`
	FreeMemoryGB            float64             `json:"free_memory_gb"`
	Models                  []string            `json:"models,omitempty"`
	Environments            []string            `json:"environments,omitempty"`
	LastSeen                time.Time           `json:"last_seen"`
	Leaving                 bool                `json:"leaving"`
	Gone                    bool                `json:"gone"`
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
	EnvironmentPlan []EnvironmentRecipe `json:"environment_plan,omitempty"`
	CopyPlan        []CopyTask          `json:"copy_plan,omitempty"`
	CacheUsage      []CopyTask          `json:"cache_usage,omitempty"`
	CachePins       []string            `json:"cache_pins,omitempty"`
	ModelPlan       []ModelSlot         `json:"model_plan,omitempty"`
	Hold            bool                `json:"hold"`
	CheckpointRoot  string              `json:"checkpoint_root"`
	Job             *Job                `json:"job,omitempty"`
	Stop            bool                `json:"stop"`
	Kill            bool                `json:"kill"`
	Leave           bool                `json:"leave"`
	GraceSeconds    float64             `json:"grace_seconds"`
}
type Result struct {
	Machine string `json:"machine"`
	Session string `json:"session"`
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
type Snapshot struct {
	SyncRequests []SyncRequest `json:"sync_requests"`
	Pins         []ModelPin    `json:"pins"`
	LoadedModels []ModelSlot   `json:"loaded_models"`
	Warning      string        `json:"warning,omitempty"`
	Jobs         []*Job        `json:"jobs"`
	Machines     []*Machine    `json:"machines"`
}

func terminal(s string) bool { return s == "succeeded" || s == "failed" || s == "cancelled" }
