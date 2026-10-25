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
}

// One bounded aggregate per comparable key, persisted as its own SQLite row.
// Last execution is provenance, not the duplicate fence: Queue.complete owns it.
type WorkObservation struct {
	MemoryLayout              string             `json:"memory_layout,omitempty"`
	MemoryConfiguration       string             `json:"memory_configuration,omitempty"`
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
		return nil, "declared wave; serial occupancy is not comparable"
	}
	if j.Spec.ExtensionCheck != nil && j.Spec.Needs.Memory != "auto" || j.Spec.ExtensionCheck == nil && j.Spec.Workload == nil && !profileMemoryWork(j.Spec) {
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
	if profileMemoryWork(j.Spec) {
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
	return referenceObservationKey(*e, m, ref, j.Spec.workType(), j.Spec.Needs, j.Spec.Args, j.Spec.Program, component, hardware)
}

// run --profile already supplies its YAML name, project, target requirement and
// reserved source manifest entry. Ordinary commands have no comparable identity.
func profileMemoryWork(s Spec) bool {
	_, source := s.Files[".myce-source.tar"]
	return !s.extensionJob() && s.RequireTarget && source && s.Name != "" && s.Project != ""
}

func profileMemoryIdentity(j *Job) string {
	s := j.Spec
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
	if j.Spec.Workload == nil && !profileMemoryWork(j.Spec) && !(j.Spec.ExtensionCheck != nil && j.Spec.Needs.Memory == "auto") {
		return
	}
	placement := ""
	if j.Timing != nil {
		placement = j.Timing.PlacementReason
	}
	key, reason := q.workObservationKey(j)
	j.Timing = &WorkTiming{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Key: key, Reason: reason}
	j.Timing.PlacementReason = placement
	if key != nil && j.Spec.Workload != nil && j.Spec.ExtensionCheck == nil {
		if old, ok := q.s.WorkObservations[identityHash(key)]; ok && old.StaleReason == "" && !old.Forbidden && q.clock.Now().Sub(old.RecordedAt) <= 30*24*time.Hour && validRate(old.Units) && validRate(old.Seconds) {
			rate := old.Rate
			if !validRate(rate) {
				rate = old.Units / old.Seconds
			}
			predicted := j.Spec.Workload.Units / rate
			if validRate(predicted) {
				j.Timing.PredictedSeconds, j.Timing.Samples = &predicted, old.Samples
			}
		}
	}
}

// Only called on mutation paths. Preserve a mixed/unknown interval even when
// the competing work finishes before this attempt. Reuse exact host custody.
func (q *Queue) invalidateWorkTimings() {
	obligations := map[string][]HostObligation{}
	for _, j := range q.s.Jobs {
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
	for _, p := range obligations {
		r := p.Result
		if p.Launch != nil || p.Engine != nil || p.Call != nil || p.Preparation || r.ID != j.ID || r.Attempt != j.Attempt || r.Machine != j.Machine || r.Session != j.Session {
			t.Reason = "mixed or unknown host occupancy"
			return
		}
	}
}

func (q *Queue) recordWorkObservation(j *Job, r Result) {
	t := j.Timing
	if t == nil || t.Key == nil || j.StartedAt == nil || r.ReleasePending || j.State != "running" || r.Interrupted || r.OutOfMemory || r.Forced {
		return
	}
	q.validateWorkTiming(j, q.hostObligations(t.Key.Host))
	if t.Reason != "" {
		return
	}
	key := identityHash(t.Key)
	old, exists := q.s.WorkObservations[key]
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	point := WorkObservation{Key: *t.Key, Samples: old.Samples + 1, Source: "accepted work",
		RecordedAt: q.clock.Now(), Job: j.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt}
	if old.StaleReason != "" || q.clock.Now().Sub(old.RecordedAt) > workCurveMaxAge {
		old.Rate, old.LatencySeconds = 0, 0
	}
	if old.StaleReason != "" || !old.MemoryRecordedAt.IsZero() && q.clock.Now().Sub(old.MemoryRecordedAt) > workCurveMaxAge {
		old.MemoryPeak = nil
	}
	if j.Spec.Workload != nil && j.Spec.ExtensionCheck == nil {
		seconds := q.clock.Now().Sub(*j.StartedAt).Seconds()
		point.Units, point.Seconds = old.Units+j.Spec.Workload.Units, old.Seconds+seconds
		if !validRate(seconds) || !validRate(point.Units) || !validRate(point.Seconds) {
			return
		}
		t.ActualSeconds = &seconds
		point.Rate = ewma(old.Rate, j.Spec.Workload.Units/seconds)
		point.LatencySeconds = ewma(old.LatencySeconds, seconds)
	} else if r.MemoryPeak == nil || r.MemoryPeak.Constrained || r.MemoryPeak.HostGB == nil || !validRate(*r.MemoryPeak.HostGB) {
		return // Profiles and auto checks learn memory only, under the same solo custody fence.
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
	point.MemoryPeak = mergeMemoryPeaks(old.MemoryPeak, r.MemoryPeak)
	point.MemoryRecordedAt = old.MemoryRecordedAt
	point.MemoryConfiguration = memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
	point.MemoryLayout = "separate"
	if owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices); owner != nil && owner.UnifiedMemory && !j.Spec.Needs.cpuOnly() {
		point.MemoryLayout = "unified"
	}
	if r.MemoryPeak != nil && !r.MemoryPeak.Constrained && r.MemoryPeak.HostGB != nil && validRate(*r.MemoryPeak.HostGB) {
		point.MemoryRecordedAt = q.clock.Now()
	}
	if r.Usage != nil && r.Usage.valid() {
		point.HostMemoryGB = max(point.HostMemoryGB, r.Usage.HostMemoryGB)
		point.DeviceMemoryGB = max(point.DeviceMemoryGB, r.Usage.MemoryGB)
	}
	q.s.WorkObservations[key] = point
}

// Shared by the CLI and HTML status. A missing old field is explicitly unknown.
func JobTiming(j *Job) string {
	envelope := ""
	if j.MemoryEnvelope != nil && j.Spec.Needs.Memory == "auto" {
		envelope = fmt.Sprintf("; memory %.2f GiB (%s; %s)", j.MemoryEnvelope.HostGB, j.MemoryEnvelope.Source, j.MemoryEnvelope.Enforcement)
	}
	t := j.Timing
	if t == nil {
		return "prediction unknown; actual unknown" + envelope
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
	if t.PlacementReason != "" {
		text += "; " + t.PlacementReason
	}
	return text + envelope
}
