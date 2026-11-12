package myce

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
)

// Advanced holds the job settings a typical agent never changes (MY-54). Each
// has a good default; set one only when the work needs it. Authors write them
// under `advanced:`. The stored and wire spec keeps these values in their flat
// fields so the previous release reads them during a rolling upgrade (MY-42);
// FoldAdvanced moves authored values there.
type Advanced struct {
	// AllowCPUFallback lets GPU work run on a CPU target when no GPU target
	// fits it at placement time. Default false: GPU work waits for a GPU.
	AllowCPUFallback bool `json:"allow_cpu_fallback,omitempty" yaml:"allow_cpu_fallback,omitempty"`

	// Resume and preemption.
	Resumable           bool                 `json:"resumable,omitempty" yaml:"resumable,omitempty"`
	ResumeCompatibility *ResumeCompatibility `json:"resume_compatibility,omitempty" yaml:"resume_compatibility,omitempty"`
	Preemptible         bool                 `json:"preemptible,omitempty" yaml:"preemptible,omitempty"`

	// Isolation.
	Isolation string `json:"isolation,omitempty" yaml:"isolation,omitempty"`
	Reason    string `json:"reason,omitempty" yaml:"reason,omitempty"`

	// Time limits and start windows.
	WallTime        string     `json:"wall_time,omitempty" yaml:"wall_time,omitempty"`
	GiveUpAt        *time.Time `json:"give_up_at,omitempty" yaml:"give_up_at,omitempty"`
	ExpectedMinutes float64    `json:"expected_minutes,omitempty" yaml:"expected_minutes,omitempty"`
	StartBy         *time.Time `json:"start_by,omitempty" yaml:"start_by,omitempty"`
	StartWindow     string     `json:"start_window,omitempty" yaml:"start_window,omitempty"`

	// Disk and memory hints.
	Disk     *DiskNeeds `json:"disk,omitempty" yaml:"disk,omitempty"`
	SizeHint SizeHint   `json:"size_hint,omitempty" yaml:"size_hint,omitempty"`

	// Placement exceptions. Mycelium places work; pins are situational (MY-31).
	AllowedMachines    []string          `json:"allowed_machines,omitempty" yaml:"allowed_machines,omitempty"`
	ExcludedMachines   []string          `json:"excluded_machines,omitempty" yaml:"excluded_machines,omitempty"`
	RequirementReasons map[string]string `json:"requirement_reasons,omitempty" yaml:"requirement_reasons,omitempty"`
	Portable           bool              `json:"portable,omitempty" yaml:"portable,omitempty"`
	RequireTarget      bool              `json:"require_target,omitempty" yaml:"require_target,omitempty"`

	// Results, logs and failure evidence.
	OutputDelivery   string             `json:"output_delivery,omitempty" yaml:"output_delivery,omitempty"`
	QualityTolerance map[string]float64 `json:"quality_tolerance,omitempty" yaml:"quality_tolerance,omitempty"`
	Logs             string             `json:"logs,omitempty" yaml:"logs,omitempty"`
	KeepOnFailure    []KeepFile         `json:"keep_on_failure,omitempty" yaml:"keep_on_failure,omitempty"`
	Diagnostics      *DiagnosticSpec    `json:"diagnostics,omitempty" yaml:"diagnostics,omitempty"`
}

// Fields whose old top-level placement is accepted for one release (MY-54).
// allow_cpu_fallback is new and exists only under advanced.
func advancedKeys() []string {
	t := reflect.TypeOf(Advanced{})
	keys := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		keys = append(keys, strings.Split(t.Field(i).Tag.Get("json"), ",")[0])
	}
	return keys
}

// AdvancedKeys lists the advanced field names, for authoring tools.
func AdvancedKeys() []string { return advancedKeys() }

// FoldAdvanced moves authored `advanced:` values onto the flat spec fields and
// clears Advanced. It returns the advanced fields this spec still sets at top
// level, which authoring tools report as deprecated. A field set in both
// places with different values is refused. Folding twice is harmless.
func (s *Spec) FoldAdvanced() ([]string, error) {
	flat := reflect.ValueOf(s).Elem()
	var nested reflect.Value
	if s.Advanced != nil {
		nested = reflect.ValueOf(s.Advanced).Elem()
	}
	t := reflect.TypeOf(Advanced{})
	var deprecated []string
	for i := range t.NumField() {
		field := t.Field(i)
		key := strings.Split(field.Tag.Get("json"), ",")[0]
		target := flat.FieldByName(field.Name)
		if field.Name != "AllowCPUFallback" && !target.IsZero() {
			deprecated = append(deprecated, key)
		}
		if !nested.IsValid() {
			continue
		}
		value := nested.Field(i)
		if value.IsZero() {
			continue
		}
		if !target.IsZero() && !reflect.DeepEqual(target.Interface(), value.Interface()) {
			return nil, rejectFieldRule("job_spec_invalid", fmt.Errorf("%s is set at top level and under advanced with different values; keep only advanced.%s", key, key))
		}
		target.Set(value)
	}
	s.Advanced = nil
	return deprecated, nil
}

// AdvancedStatus is the read-side view: the advanced values this job sets,
// or nil when every one keeps its default. Bulky diagnostic payloads and raw
// requirement text keep their existing status fields and redaction.
func AdvancedStatus(s Spec) *Advanced {
	a := &Advanced{}
	value, flat := reflect.ValueOf(a).Elem(), reflect.ValueOf(s)
	t := value.Type()
	for i := range t.NumField() {
		switch t.Field(i).Name {
		case "RequirementReasons", "KeepOnFailure", "Diagnostics", "ResumeCompatibility", "Disk", "SizeHint":
			continue
		}
		value.Field(i).Set(flat.FieldByName(t.Field(i).Name))
	}
	if a.Logs == "keep" {
		a.Logs = "" // The default.
	}
	if value.IsZero() {
		return nil
	}
	return a
}

// cpuFallback reports the MY-54 opt-in for plain GPU work. Extension work runs
// only on its declared variants; a CPU variant is how it runs on CPU.
func (s Spec) cpuFallback() bool {
	return s.AllowCPUFallback && !s.Needs.cpuOnly() && !s.extensionJob()
}

// platformFits is the structural backend test for one target. GPU work never
// runs on a CPU target unless it opted in with advanced.allow_cpu_fallback.
func (s Spec) platformFits(gpu string) bool {
	if s.Needs.cpuOnly() {
		return true
	}
	if gpu == "cpu" {
		return s.cpuFallback()
	}
	return len(s.Needs.Platform.Required) == 0 || slices.Contains(s.Needs.Platform.Required, gpu)
}

// backendAllows is the required-backend list test used by wait diagnostics,
// which also admits the CPU target of opted-in fallback work.
func (s Spec) backendAllows(gpu string) bool {
	return len(s.Needs.Platform.Required) == 0 || slices.Contains(s.Needs.Platform.Required, gpu) || gpu == "cpu" && s.cpuFallback()
}

func validateCPUFallback(s Spec) error {
	if !s.AllowCPUFallback {
		return nil
	}
	switch {
	case s.extensionJob():
		return errors.New("advanced.allow_cpu_fallback applies to plain commands; extension work runs on CPU through a declared cpu variant")
	case s.Needs.Model != "":
		return errors.New("advanced.allow_cpu_fallback cannot accompany needs.model, which runs on a GPU model server")
	case s.Needs.GPU != nil && s.Needs.GPU.Devices != nil:
		return errors.New("advanced.allow_cpu_fallback cannot accompany a needs.gpu.devices pin")
	}
	return nil
}

// cpuFallbackCandidate lowers GPU work to the CPU reservation vector for one
// CPU target, keeping the authored needs so a requeue tries a GPU again.
func cpuFallbackCandidate(j *Job, m *Machine) *Job {
	copy := *j
	authored := j.Spec.Needs
	copy.CPUFallbackFrom = &authored
	copy.Spec.Needs = ResourceNeeds{Tools: slices.Clone(authored.Tools), Network: authored.Network, Memory: authored.Memory, CPUCores: authored.CPUCores, MemoryGB: authored.MemoryGB, HostMemoryGB: authored.HostMemoryGB, Platform: Platform{Required: []string{"cpu"}}}
	if normalizeResources(&copy.Spec) != nil {
		return nil
	}
	return &copy
}

// restoreCPUFallback returns a requeued fallback job to its authored GPU needs.
func restoreCPUFallback(j *Job) {
	if j.CPUFallbackFrom != nil {
		j.Spec.Needs = *j.CPUFallbackFrom
		j.CPUFallbackFrom = nil
	}
}

// A CPU target takes opted-in GPU work only while no GPU target fits it now.
func (q *Queue) gpuTargetFits(j *Job, scans ...*placementScan) bool {
	for _, m := range q.planningMachines(scans...) {
		if m.GPU == "cpu" || m.Gone || m.Leaving || !q.jobAllowsMachine(j, m.ID) {
			continue
		}
		if fitsResources(j, m, m.FreeMemoryGB) {
			return true
		}
	}
	return false
}
