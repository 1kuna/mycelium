package myce

import (
	"fmt"
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

// One bounded aggregate per comparable key, persisted as its own SQLite row.
// Last execution is provenance, not the duplicate fence: Queue.complete owns it.
type WorkObservation struct {
	Repeated            *RepeatedWorkTiming    `json:"repeated,omitempty"`
	MemoryEscalation    *MemoryEscalationPrior `json:"memory_escalation,omitempty"`
	GPUHostRange        *MemoryPeakRange       `json:"gpu_host_range,omitempty"`
	MemoryRange         *MemoryPeakRange       `json:"memory_range,omitempty"`
	MemoryLayout        string                 `json:"memory_layout,omitempty"`
	MemoryConfiguration string                 `json:"memory_configuration,omitempty"`
	// Family and accepted memory sample count since the last invalidation
	// (#585); zero on rows written before them.
	MemoryFamily              string             `json:"memory_family,omitempty"`
	MemorySamples             int                `json:"memory_samples,omitempty"`
	MemoryRecordedAt          time.Time          `json:"memory_recorded_at,omitempty"`
	MemoryPeak                *AttemptMemoryPeak `json:"memory_peak,omitempty"`
	SampleID                  string             `json:"sample_id,omitempty"`
	Rate                      float64            `json:"rate,omitempty"`
	LatencySeconds            float64            `json:"latency_seconds,omitempty"`
	HostMemoryGB              float64            `json:"host_memory_gb,omitempty"`
	DeviceMemoryGB            float64            `json:"device_memory_gb,omitempty"`
	IncrementalHostMemoryGB   float64            `json:"incremental_host_memory_gb,omitempty"`
	IncrementalDeviceMemoryGB float64            `json:"incremental_device_memory_gb,omitempty"`
	Forbidden                 bool               `json:"forbidden,omitempty"`
	StaleReason               string             `json:"stale_reason,omitempty"`
	Source                    string             `json:"source,omitempty"`
	Key                       ObservationKey     `json:"key"`
	Samples                   int                `json:"samples"`
	Units                     float64            `json:"units"`
	Seconds                   float64            `json:"seconds"`
	RecordedAt                time.Time          `json:"recorded_at"`
	Job                       string             `json:"job"`
	Machine                   string             `json:"machine"`
	Session                   string             `json:"session"`
	Attempt                   int                `json:"attempt"`
}

type WorkTiming struct {
	RepeatedKey       *ObservationKey `json:"repeated_key,omitempty"`
	Comparability     string          `json:"comparability,omitempty"`
	Uncertainty       string          `json:"uncertainty,omitempty"`
	HistoryReason     string          `json:"history_reason,omitempty"`
	HistoryRecordedAt *time.Time      `json:"history_recorded_at,omitempty"`
	HistorySource     string          `json:"history_source,omitempty"`

	PlacementReason  string          `json:"placement_reason,omitempty"`
	Execution        Execution       `json:"execution"`
	Machine          string          `json:"machine"`
	Session          string          `json:"session"`
	Key              *ObservationKey `json:"key,omitempty"`
	PredictedSeconds *float64        `json:"predicted_seconds,omitempty"`
	ActualSeconds    *float64        `json:"actual_seconds,omitempty"`
	Samples          int             `json:"samples"`
	Reason           string          `json:"reason,omitempty"`
}

func (q *Queue) workObservationKey(j *Job) (*ObservationKey, string) {
	if j.Batch != nil {
		if j.Batch.Run {
			return q.waveObservationKey(j)
		}
		if j.Batch.Concurrency != 1 || len(j.Batch.Pieces) != 1 {
			return nil, "declared wave; serial occupancy is not comparable"
		}
		// A serial child runs the ordinary recipe with its piece arguments.
		// Keep its identity for memory, without asserting comparable occupancy.
		serial := *j
		serial.Batch = nil
		key, reason := q.workObservationKey(&serial)
		if key == nil {
			return nil, reason
		}
		return key, "declared wave; serial occupancy is not comparable"
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
	m := allocationMachine(owner, j.AllocatedDevices)
	if j.Spec.Needs.cpuOnly() && owner.Devices != nil {
		m = cpuMachineView(owner)
	}
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
	serverDigest := ""
	if v := e.variant(machineTarget(m)); v != nil {
		serverDigest = v.Sweeps[work].ServerDigest
	}
	return &ObservationKey{ServerDigest: serverDigest, Work: work, ExtensionDigest: e.Digest, Target: machineTarget(m), Reference: ref,
		Host: hostID(m), Component: component, Hardware: hardware, Fingerprint: m.ExtensionFingerprint,
		Environment: extensionEnvironmentIdentity(e, m), Allowance: observationAllowance(m), Configuration: configuration, Concurrency: 1, BatchSize: 1}, ""
}

func (q *Queue) beginWorkTiming(j *Job) {
	if j.Spec.Workload == nil && !plainCommandWork(j.Spec) && !(j.Spec.ExtensionCheck != nil && j.Spec.Needs.Memory == "auto") {
		return
	}
	placement := ""
	if j.Timing != nil {
		placement = j.Timing.PlacementReason
	}
	key, reason := q.workObservationKey(j)
	j.Timing = &WorkTiming{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Key: key, Reason: reason}
	j.Timing.PlacementReason = placement
	if plainCommandWork(j.Spec) {
		j.Timing.RepeatedKey, j.Timing.Reason = q.repeatedObservationKey(j)
		q.beginRepeatedTiming(j)
		return
	}
	j.Timing.HistoryReason = "no accepted matching measurement"
	if key != nil {
		if old, ok := q.s.WorkObservations[identityHash(key)]; ok {
			switch {
			case old.Forbidden:
				j.Timing.HistoryReason = "measurement forbidden after resource failure"
			case old.StaleReason != "":
				j.Timing.HistoryReason = "measurement invalidated"
			case q.clock.Now().Sub(old.RecordedAt) > workCurveMaxAge:
				j.Timing.HistoryReason = "measurement older than 30 days"
			default:
				j.Timing.HistoryReason = "matching measurement has no qualified throughput"
			}
		}
	}
	if key != nil && reason == "" && j.Spec.Workload != nil && j.Spec.ExtensionCheck == nil {
		if old, ok := q.s.WorkObservations[identityHash(key)]; ok && old.StaleReason == "" && !old.Forbidden && q.clock.Now().Sub(old.RecordedAt) <= 30*24*time.Hour && validRate(old.Units) && validRate(old.Seconds) {
			rate := old.Rate
			if !validRate(rate) {
				rate = old.Units / old.Seconds
			}
			predicted := j.Spec.Workload.Units / rate
			if validRate(predicted) {
				j.Timing.HistoryReason = ""
				j.Timing.PredictedSeconds, j.Timing.Samples = &predicted, old.Samples
				j.Timing.HistoryRecordedAt, j.Timing.HistorySource = &old.RecordedAt, "matching measured work curve"
			}
		}
	}
}

// Only called on mutation paths. Preserve a mixed/unknown interval even when
// the competing work finishes before this attempt. Reuse exact host custody.
func (q *Queue) invalidateWorkTimings() {
	obligations := map[string][]HostObligation{}
	for _, j := range q.liveJobs() {
		if j.Timing == nil || j.Timing.Key == nil || j.Timing.Reason != "" || j.State != "running" {
			continue
		}
		host := j.Timing.Key.Host
		if _, ok := obligations[host]; !ok {
			obligations[host] = q.hostObligations(host)
		}
		q.validateWorkTiming(j, obligations[host])
	}
}

func (q *Queue) validateWorkTiming(j *Job, obligations []HostObligation) {
	t := j.Timing
	if t == nil || t.Key == nil || t.Reason != "" {
		return
	}
	if t.Execution.ID != j.ID || t.Execution.Attempt != j.Attempt || t.Machine != j.Machine || t.Session != j.Session {
		t.Reason = "attempt changed; timing unknown"
		return
	}
	key, reason := q.workObservationKey(j)
	if reason != "" || key == nil || *key != *t.Key {
		t.Reason = "reference, qualification or configuration changed"
		return
	}
	if t.RepeatedKey != nil {
		current, reason := q.repeatedObservationKey(j)
		if reason != "" || current == nil || *current != *t.RepeatedKey {
			t.Reason = "exact command/source/input configuration changed"
			return
		}
		if m := q.s.Machines[j.Machine]; q.s.HostMaintenance[hostID(m)].SharedHost != nil {
			if reason := q.hostPressureReason(m); reason != "" {
				t.Reason = "host pressure unknown or contended"
				return
			}
		}
	}
	for _, p := range obligations {
		r := p.Result
		if p.Launch != nil || p.Engine != nil || p.Call != nil || p.Preparation || r.ID != j.ID || r.Attempt != j.Attempt || r.Machine != j.Machine || r.Session != j.Session {
			t.Reason = "mixed or unknown host occupancy"
			return
		}
	}
}

func (q *Queue) recordWorkObservation(j *Job, r Result, sample *eligibleMemorySample) {
	t := j.Timing
	if t == nil || t.Key == nil || j.StartedAt == nil || r.ReleasePending || j.State != "running" || r.Interrupted || r.OutOfMemory || r.Forced {
		return
	}
	q.validateWorkTiming(j, q.hostObligations(t.Key.Host))
	if t.RepeatedKey != nil && t.Reason == "" {
		t.Reason = repeatedContentionReason(r)
		if t.Reason == "" {
			q.recordRepeatedObservation(j, r)
		}
	}
	throughput := t.Reason == "" && !plainCommandWork(j.Spec)
	memory := sample != nil && sample.work
	if !throughput && !memory {
		return
	}
	key := identityHash(t.Key)
	old, exists := q.s.WorkObservations[key]
	original := old
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	point := WorkObservation{Key: *t.Key, Samples: old.Samples + 1, Source: "accepted work",
		RecordedAt: q.clock.Now(), Job: j.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt}
	if old.StaleReason != "" || q.clock.Now().Sub(old.RecordedAt) > workCurveMaxAge {
		old.Rate, old.LatencySeconds = 0, 0
	}
	if throughput && j.Spec.Workload != nil && j.Spec.ExtensionCheck == nil {
		seconds := q.clock.Now().Sub(*j.StartedAt).Seconds()
		point.Units, point.Seconds = old.Units+j.Spec.Workload.Units, old.Seconds+seconds
		if !validRate(seconds) || !validRate(point.Units) || !validRate(point.Seconds) {
			return
		}
		t.ActualSeconds = &seconds
		point.Rate = ewma(old.Rate, j.Spec.Workload.Units/seconds)
		point.LatencySeconds = ewma(old.LatencySeconds, seconds)
	} else if !memory {
		return
	} else if exists && old.StaleReason == "" {
		// Memory-only samples leave the existing throughput and provenance
		// untouched, including the clock that ages the throughput out.
		point = old
		point.Samples++
	}
	point.HostMemoryGB, point.DeviceMemoryGB = old.HostMemoryGB, old.DeviceMemoryGB
	point.IncrementalHostMemoryGB, point.IncrementalDeviceMemoryGB = old.IncrementalHostMemoryGB, old.IncrementalDeviceMemoryGB
	point.Forbidden = old.Forbidden
	point.SampleID = old.SampleID
	point.MemoryEscalation = old.MemoryEscalation
	point.MemoryPeak = old.MemoryPeak
	point.MemoryRange = old.MemoryRange
	point.GPUHostRange = old.GPUHostRange
	point.MemoryRecordedAt = old.MemoryRecordedAt
	point.MemoryConfiguration, point.MemoryLayout = old.MemoryConfiguration, old.MemoryLayout
	point.MemoryFamily, point.MemorySamples = old.MemoryFamily, old.MemorySamples
	if memory {
		q.recordWorkMemory(*sample, old, &point)
	}
	q.putWorkObservation(key, point)
	q.recordCurveLearning(j, original, exists, point)
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
		return "prediction unknown; actual unknown" + envelope + runtimeText(j)
	}
	prediction, actual := "unknown", "unknown"
	if t.PredictedSeconds != nil {
		prediction = fmt.Sprintf("%.2fs (%d samples)", *t.PredictedSeconds, t.Samples)
	}
	if t.ActualSeconds != nil {
		actual = fmt.Sprintf("%.2fs", *t.ActualSeconds)
	}
	text := "prediction " + prediction + "; actual " + actual
	if t.Reason != "" {
		text += "; " + t.Reason
	}
	if t.Comparability != "" {
		text += "; " + t.Comparability
	}
	if t.Uncertainty != "" {
		text += "; " + t.Uncertainty
	}
	if t.PredictedSeconds == nil && t.HistoryReason != "" {
		text += "; " + t.HistoryReason
	}
	if t.PlacementReason != "" {
		text += "; " + t.PlacementReason
	}
	return text + envelope + runtimeText(j)
}
