package myce

import (
	"fmt"
	"math"
	"time"
)

// WorkReference describes the amount in the variant's bundled correctness
// baseline. The frozen check/recipe owns semantics; these fields are generic.
// Ordinary accepted work supports one independent item and batch size one.
type WorkReference struct {
	InputShape string  `json:"input_shape" yaml:"input_shape"`
	Unit       string  `json:"unit" yaml:"unit"`
	Units      float64 `json:"units" yaml:"units"`
}

type Workload struct {
	InputShape string  `json:"input_shape" yaml:"input_shape"`
	Units      float64 `json:"units" yaml:"units"`
}

func (w Workload) valid() bool { return extensionName.MatchString(w.InputShape) && validRate(w.Units) }
func (r WorkReference) valid() bool {
	return (Workload{InputShape: r.InputShape, Units: r.Units}).valid() && extensionName.MatchString(r.Unit)
}

// ObservationKey contains no private input paths or application-specific data.
// Configuration is a digest of resource reservations, allowances and power mode.
type ObservationKey struct {
	// RepeatedWork binds exact plain-command bytes, independently of the broader
	// memory Work identity. Memory readers deliberately group across this field.
	RepeatedWork    string          `json:"repeated_work,omitempty"`
	ServerDigest    string          `json:"server_digest,omitempty"`
	Allowance       string          `json:"allowance,omitempty"`
	Work            string          `json:"work"`
	ExtensionDigest string          `json:"extension_digest"`
	Target          ExecutionTarget `json:"target"`
	Reference       WorkReference   `json:"reference"`
	Host            string          `json:"host"`
	Component       string          `json:"component"`
	Hardware        string          `json:"hardware"`
	Fingerprint     string          `json:"fingerprint"`
	Environment     string          `json:"environment"`
	Configuration   string          `json:"configuration"`
	Concurrency     int             `json:"concurrency"`
	BatchSize       int             `json:"batch_size"`
	// Check marks a qualification check's memory evidence (#618). A check runs
	// its small reference fixture, not the workload, so neither is sized from
	// the other's peaks; see memoryEstimate for the one-way seed. Omitted when
	// false, so existing keys keep their identity. Rows recorded before this
	// field stay shared.
	Check bool `json:"check,omitempty"`
}

// Invalidated accepted evidence remains a prior for exploration, never a learner.
type MemoryEscalationPrior struct {
	Peak       *AttemptMemoryPeak `json:"peak"`
	RecordedAt time.Time          `json:"recorded_at"`
}

// One record per kind of work (MY-49): its last accepted duration and, through
// memory learning, its accepted peak. Persisted as its own SQLite row.
// Last execution is provenance, not the duplicate fence: Queue.complete owns it.
type WorkObservation struct {
	MemoryEscalation    *MemoryEscalationPrior `json:"memory_escalation,omitempty"`
	GPUHostRange        *MemoryPeakRange       `json:"gpu_host_range,omitempty"`
	MemoryRange         *MemoryPeakRange       `json:"memory_range,omitempty"`
	MemoryLayout        string                 `json:"memory_layout,omitempty"`
	MemoryConfiguration string                 `json:"memory_configuration,omitempty"`
	// Family and accepted memory sample count since the last invalidation
	// (#585); zero on rows written before them.
	MemoryFamily     string             `json:"memory_family,omitempty"`
	MemorySamples    int                `json:"memory_samples,omitempty"`
	MemoryRecordedAt time.Time          `json:"memory_recorded_at,omitempty"`
	MemoryPeak       *AttemptMemoryPeak `json:"memory_peak,omitempty"`
	SampleID         string             `json:"sample_id,omitempty"`
	// LastSeconds is the last accepted attempt's duration, Started to completion.
	LastSeconds float64        `json:"last_seconds,omitempty"`
	Forbidden   bool           `json:"forbidden,omitempty"`
	StaleReason string         `json:"stale_reason,omitempty"`
	Source      string         `json:"source,omitempty"`
	Key         ObservationKey `json:"key"`
	Samples     int            `json:"samples"`
	RecordedAt  time.Time      `json:"recorded_at"`
	Job         string         `json:"job"`
	Machine     string         `json:"machine"`
	Session     string         `json:"session"`
	Attempt     int            `json:"attempt"`
}

type WorkTiming struct {
	HistoryReason   string          `json:"history_reason,omitempty"`
	PlacementReason string          `json:"placement_reason,omitempty"`
	Execution       Execution       `json:"execution"`
	Machine         string          `json:"machine"`
	Session         string          `json:"session"`
	Key             *ObservationKey `json:"key,omitempty"`
	// LastAcceptedSeconds is this kind of work's last accepted duration when
	// the attempt began. It is history, not a forecast or deadline.
	LastAcceptedSeconds *float64 `json:"last_accepted_seconds,omitempty"`
	// Deprecated: mirrors LastAcceptedSeconds for the adjacent release (MY-42).
	PredictedSeconds *float64 `json:"predicted_seconds,omitempty"`
	ActualSeconds    *float64 `json:"actual_seconds,omitempty"`
	Reason           string   `json:"reason,omitempty"`
}

func validRate(v float64) bool { return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }

func plainCommandWork(s Spec) bool {
	return !s.extensionJob() && s.Workload == nil && len(s.Run.Command) > 0
}

func (q *Queue) workObservationKey(j *Job) (*ObservationKey, string) {
	if j.Batch != nil {
		if j.Batch.Concurrency != 1 || len(j.Batch.Pieces) != 1 {
			return nil, "declared wave; occupancy is not comparable"
		}
		// A piece runs the ordinary recipe with its piece arguments: that is
		// its kind of work, for memory and duration alike.
		serial := *j
		serial.Batch = nil
		return q.workObservationKey(&serial)
	}
	if j.Spec.ExtensionCheck != nil && j.Spec.Needs.Memory != "auto" || j.Spec.ExtensionCheck == nil && j.Spec.Workload == nil && !plainCommandWork(j.Spec) {
		return nil, "workload/reference metadata unavailable"
	}
	if j.Spec.Resumable && j.Attempt > 1 {
		return nil, "resumed work is not comparable"
	}
	owner := q.s.Machines[j.Machine]
	if owner == nil || owner.Session != j.Session || q.unreported[owner.ID] || owner.Gone {
		return nil, "component evidence unavailable"
	}
	m := q.memoryObservationMachine(j, owner)
	component, hardware := "cpu", m.ExtensionFingerprint
	if m.GPU != "cpu" {
		component = m.Device
		if owner.Devices == nil || len(j.AllocatedDevices) != 1 || j.AllocatedDevices[0] != component {
			return nil, "physical component inventory unavailable"
		}
		hardware = ""
		for _, d := range owner.Devices {
			if d.ID == component {
				hardware = d.Identity
			}
		}
		if hardware == "" {
			return nil, "physical component identity unavailable"
		}
	}
	if a := machineAvailability(m); a != nil && !a.Available {
		return nil, "owner availability withdrawn"
	}
	if plainCommandWork(j.Spec) {
		key, reason := referenceObservationKey(Extension{}, m, WorkReference{}, profileMemoryIdentity(j), j.Spec.Needs, j.Spec.Args, j.Spec.Program, component, hardware)
		key.Environment = environmentKey(j, m)
		return key, reason
	}
	e := qualifiedExtension(j, m)
	if e == nil || m.ExtensionFingerprint == "" {
		return nil, "qualified reference unavailable"
	}
	v := e.variant(machineTarget(m))
	if v == nil {
		return nil, "variant reference unavailable"
	}
	ref, ok := v.References[j.Spec.workType()]
	if !ok || j.Spec.ExtensionCheck == nil && ref.InputShape != j.Spec.Workload.InputShape {
		return nil, "input shape has no matching reference"
	}
	key, reason := referenceObservationKey(*e, m, ref, j.Spec.workType(), j.Spec.Needs, j.Spec.Args, j.Spec.Program, component, hardware)
	if key != nil {
		key.Check = j.Spec.ExtensionCheck != nil
	}
	return key, reason
}

// Plain auto-memory commands are comparable by project, name, run, environment
// and inputs, so an omitted memory figure can be learned (#585).
func plainMemoryWork(s Spec) bool {
	return profileMemoryWork(s) || !s.extensionJob() && s.Needs.Memory == "auto" && s.Name != "" && s.Project != ""
}

// run --profile already supplies its YAML name, project, target requirement and
// reserved source manifest entry.
func profileMemoryWork(s Spec) bool {
	_, source := s.Files[".myce-source.tar"]
	return !s.extensionJob() && s.RequireTarget && source && s.Name != "" && s.Project != ""
}

func profileMemoryIdentity(j *Job) string {
	if j.planningMemoryIdentity != "" {
		return j.planningMemoryIdentity
	}
	s := j.Spec
	if s.ProfileMemory != "" {
		return "profile:" + identityHash(struct {
			Project, Name, Configuration, Profile string
			Environment                           Environment
			Recipes                               map[string]EnvironmentRecipe
		}{s.Project, s.Name, memoryConfiguration(s.Needs, s.Args, s.Program), s.ProfileMemory, s.Environment, j.EnvironmentRecipes})
	}
	inputs := FileManifest{}
	for name, ref := range s.Files {
		if name != ".myce-source.tar" {
			inputs[name] = ref
		}
	}
	return "profile:" + identityHash(struct {
		Project, Name, Configuration string
		Run                          RunSpec
		Environment                  Environment
		Recipes                      map[string]EnvironmentRecipe
		Inputs                       FileManifest
	}{s.Project, s.Name, memoryConfiguration(s.Needs, s.Args, s.Program), s.Run, s.Environment, j.EnvironmentRecipes, inputs})
}

func referenceObservationKey(e Extension, m *Machine, ref WorkReference, work string, needs ResourceNeeds, args []string, program *Program, component, hardware string) (*ObservationKey, string) {
	power, mode := "", ""
	if p := m.CapabilityProfile; p != nil && p.Availability != nil {
		power, mode = p.Availability.Signals.Power, p.Availability.Mode
	}
	// Exploratory reservations depend on live free room, not the work recipe.
	if needs.Memory == "auto" {
		needs.MemoryGB, needs.HostMemoryGB = 0, 0
	}
	configuration := identityHash(struct {
		Needs                                         ResourceNeeds
		CPUReserve, CPUAllowance                      int
		HostAllowance, DeviceAllowance, DeviceReserve float64
		Power, Mode                                   string
		Arguments                                     string
	}{needs, m.CPUReserveCores, m.EffectiveCPUCores, m.EffectiveHostMemoryBudgetGB, m.TotalMemoryGB, m.ReservedMemoryGB, power, mode, identityHash(struct {
		Args    []string
		Program *Program
	}{args, program})})
	return &ObservationKey{Work: work, ExtensionDigest: e.Digest, Target: machineTarget(m), Reference: ref,
		Host: hostID(m), Component: component, Hardware: hardware, Fingerprint: m.ExtensionFingerprint,
		Environment: extensionEnvironmentIdentity(e, m), Allowance: observationAllowance(m), Configuration: configuration, Concurrency: 1, BatchSize: 1}, ""
}

func (q *Queue) beginWorkTiming(j *Job) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	if j.Spec.Workload == nil && !plainCommandWork(j.Spec) && !(j.Spec.ExtensionCheck != nil && j.Spec.Needs.Memory == "auto") {
		return
	}
	placement := ""
	if j.Timing != nil {
		placement = j.Timing.PlacementReason
	}
	key, reason := q.workObservationKey(j)
	j.Timing = &WorkTiming{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Key: key, Reason: reason, PlacementReason: placement}
	if key == nil || reason != "" || j.Spec.ExtensionCheck != nil {
		return
	}
	if seconds, ok := q.lastAcceptedSeconds(key); ok {
		last, mirror := seconds, seconds
		j.Timing.LastAcceptedSeconds, j.Timing.PredictedSeconds = &last, &mirror
		return
	}
	j.Timing.HistoryReason = "no accepted matching measurement"
	if old, ok := q.s.WorkObservations[identityHash(key)]; ok {
		switch {
		case old.Forbidden:
			j.Timing.HistoryReason = "measurement forbidden after resource failure"
		case old.StaleReason != "":
			j.Timing.HistoryReason = "measurement invalidated"
		}
	}
}

// lastAcceptedSeconds is the kind of work's last accepted duration, unless its
// record is invalidated or forbidden after a resource failure.
func (q *Queue) lastAcceptedSeconds(key *ObservationKey) (float64, bool) {
	p, ok := q.s.WorkObservations[identityHash(key)]
	if !ok || p.Key != *key || p.Forbidden || p.StaleReason != "" || !validRate(p.LastSeconds) {
		return 0, false
	}
	return p.LastSeconds, true
}

// A changed attempt or identity cannot teach this key a duration.
func (q *Queue) validateWorkTiming(j *Job) {
	t := j.Timing
	if t == nil || t.Key == nil || t.Reason != "" {
		return
	}
	if t.Execution.ID != j.ID || t.Execution.Attempt != j.Attempt || t.Machine != j.Machine || t.Session != j.Session {
		q.touchJob(j.ID)
		t.Reason = "attempt changed; timing unknown"
		return
	}
	key, reason := q.workObservationKey(j)
	if reason != "" || key == nil || *key != *t.Key {
		q.touchJob(j.ID)
		t.Reason = "reference, qualification or configuration changed"
	}
}

// recordWorkObservation records an accepted attempt's duration and hands its
// eligible memory sample to recordWorkMemory. Accepted whole extension workload
// runs refresh their record's provenance and identity, as they always have, and so
// does a memory sample on a new or invalidated record; any other work adds
// only its duration and leaves memory evidence untouched.
func (q *Queue) recordWorkObservation(j *Job, r Result, sample *eligibleMemorySample) {
	t := j.Timing
	if t == nil || t.Key == nil || j.StartedAt == nil || r.ReleasePending || j.State != "running" || r.Interrupted || r.OutOfMemory || r.Forced {
		return
	}
	q.validateWorkTiming(j)
	seconds := q.clock.Now().Sub(*j.StartedAt).Seconds()
	// Whole extension workload runs refresh provenance; pieces never have.
	workload := t.Reason == "" && j.Spec.Workload != nil && j.Spec.ExtensionCheck == nil && j.Batch == nil
	if workload && !validRate(seconds) {
		return // An unmeasurable workload run teaches nothing, memory included.
	}
	// A qualification check learns memory, never a duration (#618).
	duration := t.Reason == "" && j.Spec.ExtensionCheck == nil && validRate(seconds)
	memory := sample != nil && sample.work
	if !duration && !memory {
		return
	}
	if duration {
		q.touchJob(j.ID)
		t.ActualSeconds = &seconds
	}
	key := identityHash(t.Key)
	old, exists := q.s.WorkObservations[key]
	point := old
	if !workload && !memory {
		if !exists {
			point = WorkObservation{Key: *t.Key, Source: "accepted work", RecordedAt: q.clock.Now(), Job: j.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt}
		}
		point.LastSeconds = seconds
		q.putWorkObservation(key, point)
		q.recordCurveLearning(j, old, exists, point)
		return
	}
	if workload || !exists || old.StaleReason != "" {
		if old.StaleReason != "" {
			point.LastSeconds = 0
		}
		point.Key, point.StaleReason, point.Source = *t.Key, "", "accepted work"
		point.RecordedAt, point.Job, point.Machine, point.Session, point.Attempt = q.clock.Now(), j.ID, r.Machine, r.Session, r.Attempt
	}
	point.Samples = old.Samples + 1
	if duration {
		point.LastSeconds = seconds
	}
	if memory {
		q.recordWorkMemory(*sample, old, &point)
	}
	q.putWorkObservation(key, point)
	q.recordCurveLearning(j, old, exists, point)
}

// Shared by the CLI and HTML status. A missing old field is explicitly unknown.
func JobTiming(j *Job) string {
	envelope := ""
	if j.StorageEnvelope != nil {
		envelope += "; " + j.StorageEnvelope.Summary()
	}
	if j.MemoryEnvelope != nil {
		envelope += fmt.Sprintf("; memory %.2f GiB (%s; %s)", j.MemoryEnvelope.HostGB, j.MemoryEnvelope.Origin(), j.MemoryEnvelope.Enforcement)
	}
	t := j.Timing
	if t == nil {
		return "last accepted duration unknown; actual unknown" + envelope + runtimeText(j)
	}
	last, actual := "unknown", "unknown"
	if t.LastAcceptedSeconds != nil {
		last = fmt.Sprintf("%.2fs", *t.LastAcceptedSeconds)
	}
	if t.ActualSeconds != nil {
		actual = fmt.Sprintf("%.2fs", *t.ActualSeconds)
	}
	text := "last accepted duration " + last + "; actual " + actual
	if t.Reason != "" {
		text += "; " + t.Reason
	}
	if t.LastAcceptedSeconds == nil && t.HistoryReason != "" {
		text += "; " + t.HistoryReason
	}
	if t.PlacementReason != "" {
		text += "; " + t.PlacementReason
	}
	return text + envelope + runtimeText(j)
}
