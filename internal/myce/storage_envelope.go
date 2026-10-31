package myce

import (
	"fmt"
	"slices"
	"time"
)

// StorageKey deliberately has no host identity. A variant must independently be
// qualified before its evidence can be used; storage evidence never qualifies it.
type StorageKey struct {
	Program       string          `json:"program"`
	Recipe        string          `json:"recipe"`
	Source        string          `json:"source"`
	Variant       ExecutionTarget `json:"variant"`
	Configuration string          `json:"configuration"`
	Environment   string          `json:"environment"`
	Shape         string          `json:"shape"`
	InputShape    string          `json:"input_shape"`
}

// Nil means unavailable, including on old workers. Complete means every managed
// root was observed throughout this execution, not native write enforcement.
// Scratch includes mutable workspace/output but excludes the staged input baseline.
// ManagedBytes is a simultaneous peak, not the sum of independently timed maxima.
type AttemptStoragePeak struct {
	ScratchBytes     uint64 `json:"scratch_bytes"`
	SpoolBytes       uint64 `json:"spool_bytes"`
	CheckpointBytes  uint64 `json:"checkpoint_bytes"`
	MetadataBytes    uint64 `json:"metadata_bytes"`
	ManagedBytes     uint64 `json:"managed_bytes"`
	Files            uint64 `json:"files"`
	StagedInputFiles uint64 `json:"staged_input_files"`
	StagedInputBytes uint64 `json:"staged_input_bytes"`
	AllocationUnit   uint64 `json:"allocation_unit"`
	Samples          uint64 `json:"samples"`
	Complete         bool   `json:"complete"`
	Constrained      bool   `json:"constrained,omitempty"`
	Source           string `json:"source"`
}

type StorageRange struct {
	Samples       uint64 `json:"samples"`
	Min           uint64 `json:"min"`
	Peak          uint64 `json:"peak"`
	PositiveError uint64 `json:"positive_error"`
}

func (r StorageRange) margin() uint64 { return max(r.Peak-r.Min, r.PositiveError) }
func (r StorageRange) need(unit uint64) uint64 {
	return roundStorage(addDiskBytes(r.Peak, r.margin()), unit)
}
func roundStorage(n, unit uint64) uint64 {
	if unit <= 1 || n == 0 {
		return n
	}
	return addDiskBytes(n, (unit-n%unit)%unit)
}
func (r StorageRange) add(n uint64, prediction *uint64) StorageRange {
	if r.Samples == 0 {
		r.Min = n
	} else {
		r.Min = min(r.Min, n)
	}
	r.Peak = max(r.Peak, n)
	r.Samples = addDiskBytes(r.Samples, 1)
	if prediction != nil && n > *prediction {
		r.PositiveError = max(r.PositiveError, n-*prediction)
	}
	return r
}

type StorageObservation struct {
	Key              StorageKey   `json:"key"`
	Staging          StorageRange `json:"staging"`
	Output           StorageRange `json:"output"`
	Scratch          StorageRange `json:"scratch"`
	Spool            StorageRange `json:"spool"`
	Checkpoint       StorageRange `json:"checkpoint"`
	Metadata         StorageRange `json:"metadata"`
	Managed          StorageRange `json:"managed"`
	Files            StorageRange `json:"files"`
	AllocationUnit   uint64       `json:"allocation_unit"`
	RecordedAt       time.Time    `json:"recorded_at"`
	OutputExecution  Execution    `json:"output_execution"`
	OutputRecordedAt time.Time    `json:"output_recorded_at"`
	Execution        Execution    `json:"execution"`
	// Negative evidence stays separate from the accepted ranges and their clock.
	LowerBound          *AttemptStoragePeak `json:"lower_bound,omitempty"`
	LowerBoundExecution Execution           `json:"lower_bound_execution,omitempty"`
	LowerBoundReason    string              `json:"lower_bound_reason,omitempty"`
	LowerBoundAt        time.Time           `json:"lower_bound_at,omitempty"`
}

type JobStorageEnvelope struct {
	Key             *StorageKey         `json:"key,omitempty"`
	Evidence        *StorageObservation `json:"evidence,omitempty"`
	Source          string              `json:"source"`
	Samples         uint64              `json:"samples"`
	OutputBytes     *uint64             `json:"output_bytes,omitempty"`
	ScratchBytes    *uint64             `json:"scratch_bytes,omitempty"`
	SpoolBytes      *uint64             `json:"spool_bytes,omitempty"`
	CheckpointBytes *uint64             `json:"checkpoint_bytes,omitempty"`
	MetadataBytes   *uint64             `json:"metadata_bytes,omitempty"`
	ManagedBytes    *uint64             `json:"managed_bytes,omitempty"`
	Files           *uint64             `json:"files,omitempty"`
	InputBytes      uint64              `json:"input_bytes"`
	PredictedNeed   *uint64             `json:"predicted_need,omitempty"`
	Margin          uint64              `json:"margin"`
	PositiveError   uint64              `json:"positive_error"`
	Multiplier      float64             `json:"multiplier"`
	// #506/#507 install limits. A prediction is never an installed hard limit.
	HardLimit   *uint64 `json:"hard_limit,omitempty"`
	Enforcement string  `json:"enforcement"`
	Exploration string  `json:"exploration,omitempty"`
}

type AttemptStorageFacts struct {
	Execution      Execution           `json:"execution"`
	Machine        string              `json:"machine"`
	Session        string              `json:"session"`
	Peak           *AttemptStoragePeak `json:"peak,omitempty"`
	Envelope       *JobStorageEnvelope `json:"envelope,omitempty"`
	LearningReason string              `json:"learning_reason"`
	OutputLearned  bool                `json:"output_learned,omitempty"`
}

func (q *Queue) storageKey(j *Job, m *Machine) *StorageKey {
	if m == nil || j.Spec.Resumable && j.Attempt > 1 || j.Batch != nil && (j.Batch.Run || j.Batch.Concurrency != 1 || len(j.Batch.Pieces) != 1) {
		return nil
	}
	if j.Spec.Needs.cpuOnly() && m.Devices != nil {
		m = cpuMachineView(m)
	}
	s := j.Spec
	key := StorageKey{Program: s.workType(), Variant: machineTarget(m), Shape: "manifest"}
	if s.Workload != nil {
		key.Shape = s.Workload.InputShape
	}
	if s.extensionJob() {
		e := qualifiedExtension(j, m)
		if e == nil || e.variant(machineTarget(m)) == nil {
			return nil
		}
		key.Recipe, key.Source = e.Digest, e.Digest
		key.Environment = extensionEnvironmentIdentity(*e, m)
	} else {
		source, ok := s.Files[".myce-source.tar"]
		if !ok || source.validate() != nil || s.Name == "" || s.Project == "" {
			return nil
		}
		key.Program = "profile:" + s.Name
		key.Environment = environmentKey(j, m)
		key.Source = source.SHA256
		run := s.Run
		run.Workdir = "" // Worker paths are not software identity.
		key.Recipe = identityHash(struct {
			Run         RunSpec
			Environment Environment
			Recipes     map[string]EnvironmentRecipe
		}{run, s.Environment, j.EnvironmentRecipes})
	}
	// Program bytes must not be confused with similarly sized input data.
	programSource := FileRef{}
	if s.Program != nil {
		programSource = s.Files[s.Program.File]
		if programSource.validate() != nil {
			return nil
		}
	}
	key.Configuration = identityHash(struct {
		Project                  string
		Args                     []string
		Program                  *Program
		ProgramSource            FileRef
		Configuration            string
		Env                      map[string]string
		Outputs                  map[string]string
		Inputs                   map[string]string
		Shared                   map[string]SharedInput
		StorageProtocol          string `json:"storage_protocol,omitempty"`
		Delivery                 string
		Resumable, FileTransport bool
		Diagnostics              *DiagnosticSpec
	}{s.Project, s.Args, s.Program, programSource, memoryConfiguration(s.Needs, nil, nil), s.Run.Env, s.Outputs, s.Inputs, s.SharedInputs, s.StorageProtocol, s.OutputDelivery, s.Resumable, j.FileTransport, s.Diagnostics})

	shape := map[string]int64{}
	for name, ref := range jobInputFiles(j) {
		if name != ".myce-source.tar" {
			shape[name] = ref.SizeBytes
		}
	}
	key.InputShape = identityHash(struct {
		Workload *Workload
		Files    map[string]int64
	}{s.Workload, shape})
	return &key
}

func (q *Queue) storageEnvelope(j *Job, m *Machine) *JobStorageEnvelope {
	e := &JobStorageEnvelope{Key: q.storageKey(j, m), Source: "unknown", Enforcement: "unenforced; observation only", Exploration: "bounded exploration required; fresh storage admission required"}
	e.InputBytes = manifestDiskBytes(jobInputFiles(j))
	if j.Spec.Disk != nil {
		e.InputBytes = max(e.InputBytes, j.Spec.Disk.InputBytes)
	}
	if e.Key != nil {
		point, ok := q.s.StorageObservations[identityHash(e.Key)]
		if ok {
			e.Evidence = &point
			outputAge := q.clock.Now().Sub(point.OutputRecordedAt)
			if point.Output.Samples > 0 && outputAge >= 0 && outputAge <= workCurveMaxAge {
				e.OutputBytes = storageValue(point.Output.need(1))
				e.Source = "accepted logical output; scratch unknown"
				e.Samples = point.Output.Samples
			}
			age := q.clock.Now().Sub(point.RecordedAt)
			if point.Managed.Samples > 0 && age >= 0 && age <= workCurveMaxAge {
				e.Source, e.Samples = "accepted managed storage peaks", point.Managed.Samples
				e.InputBytes = max(e.InputBytes, point.Staging.need(point.AllocationUnit))
				e.OutputBytes = storageValue(point.Output.need(1))
				e.ScratchBytes, e.SpoolBytes = storageValue(point.Scratch.need(point.AllocationUnit)), storageValue(point.Spool.need(point.AllocationUnit))
				e.CheckpointBytes, e.MetadataBytes = storageValue(point.Checkpoint.need(point.AllocationUnit)), storageValue(point.Metadata.need(point.AllocationUnit))
				e.ManagedBytes, e.Files = storageValue(point.Managed.need(point.AllocationUnit)), storageValue(point.Files.need(1))
				e.Margin, e.PositiveError = point.Managed.margin(), point.Managed.PositiveError
				e.Multiplier = 1
				if point.Managed.Peak > 0 {
					e.Multiplier += float64(e.Margin) / float64(point.Managed.Peak)
				}
				e.PredictedNeed = storageValue(addDiskBytes(e.InputBytes, *e.ManagedBytes))
				e.Exploration = ""
				if point.LowerBound != nil && (point.LowerBound.ManagedBytes > *e.ManagedBytes || point.LowerBound.Constrained && point.LowerBound.ManagedBytes >= point.Managed.Peak) {
					e.Exploration = "larger lower bound retained; fresh storage admission required"
				}
				return e
			}
		}
	}
	if e.OutputBytes == nil && j.Spec.Disk != nil && j.Spec.Disk.OutputBytes > 0 {
		e.OutputBytes = storageValue(j.Spec.Disk.OutputBytes)
		e.Source = "output-only cold prior; scratch unknown"
	}
	return e
}
func storageValue(n uint64) *uint64 { return &n }
func storageOutputEligible(reason string) bool {
	return reason == "eligible" || reason == "storage_incomplete" || reason == "storage_peak_unavailable"
}

func storageLearningReason(sample *eligibleMemorySample, reason string, j *Job, r Result) string {
	if sample == nil {
		return reason
	}
	if sample.storageKey == nil || j.StorageEnvelope == nil || j.StorageEnvelope.Key == nil || *j.StorageEnvelope.Key != *sample.storageKey {
		return "storage_identity_changed"
	}
	if !j.storedFileResults() {
		return "storage_manifest_unverified"
	}
	if r.StoragePeak == nil {
		return "storage_peak_unavailable"
	}
	if r.StoragePeak.Constrained {
		return "storage_constrained"
	}
	if !r.StoragePeak.Complete || r.StoragePeak.Samples == 0 || r.StoragePeak.AllocationUnit == 0 || r.StoragePeak.ManagedBytes < max(r.StoragePeak.ScratchBytes, r.StoragePeak.SpoolBytes, r.StoragePeak.CheckpointBytes, r.StoragePeak.MetadataBytes) {
		return "storage_incomplete"
	}

	return "eligible"
}

// Diagnostics/lower bounds do not grant a learning token or change clean ranges.
func (q *Queue) recordAttemptStorage(j *Job, r Result) {
	if r.ReleaseOnly || j.StartedAt == nil && r.StoragePeak == nil {
		return
	}
	sample, reason := q.memoryLearningSample(j, r)
	reason = storageLearningReason(sample, reason, j, r)
	facts := AttemptStorageFacts{Execution: r.Execution, Machine: r.Machine, Session: r.Session, Peak: r.StoragePeak, Envelope: j.StorageEnvelope, LearningReason: reason, OutputLearned: storageOutputEligible(reason)}
	j.StorageAttempts = append(slices.Clone(j.StorageAttempts), facts)
	if len(j.StorageAttempts) > memoryAttemptsLimit {
		j.StorageAttempts = append(j.StorageAttempts[:1:1], j.StorageAttempts[len(j.StorageAttempts)-memoryAttemptsLimit+1:]...)
		j.StorageAttemptsDropped++
	}
	if reason == "eligible" || r.StoragePeak == nil || j.StorageEnvelope == nil || j.StorageEnvelope.Key == nil {
		return
	}
	key := *j.StorageEnvelope.Key
	point := q.s.StorageObservations[identityHash(key)]
	point.Key = key
	peak := *r.StoragePeak
	if point.LowerBound != nil && point.LowerBound.ManagedBytes > peak.ManagedBytes {
		return
	}
	point.LowerBound = &peak
	point.LowerBoundReason = reason
	point.LowerBoundExecution = r.Execution
	point.LowerBoundAt = q.clock.Now()
	if q.s.StorageObservations == nil {
		q.s.StorageObservations = map[string]StorageObservation{}
	}
	q.s.StorageObservations[identityHash(key)] = point
}

func (e JobStorageEnvelope) Summary() string {
	return fmt.Sprintf("storage %s; samples %d; logical output %s; predicted %s; error/margin %d/%d bytes; hard limit %s; %s", e.Source, e.Samples, storageBytesText(e.OutputBytes), storageBytesText(e.PredictedNeed), e.PositiveError, e.Margin, storageBytesText(e.HardLimit), e.Enforcement)
}
func storageBytesText(n *uint64) string {
	if n == nil {
		return "unknown"
	}
	return fmt.Sprintf("%d bytes", *n)
}
func (p StorageObservation) Summary() string {
	e := JobStorageEnvelope{Source: "lower-bound evidence only", Enforcement: "unenforced; observation only"}
	if p.Output.Samples > 0 {
		e.Source = "accepted logical output; scratch unknown"
		e.OutputBytes = storageValue(p.Output.need(1))
		e.Samples = p.Output.Samples
	}
	if p.Managed.Samples > 0 {
		e.Source = "accepted managed storage peaks"
		e.Samples = p.Managed.Samples
		e.PredictedNeed = storageValue(p.Managed.need(p.AllocationUnit))
		e.Margin = p.Managed.margin()
		e.PositiveError = p.Managed.PositiveError
	}
	return fmt.Sprintf("%s · shape %s (%s) · %s/%s/%s · %s", p.Key.Program, p.Key.Shape, p.Key.InputShape[:min(12, len(p.Key.InputShape))], p.Key.Variant.OS, p.Key.Variant.Arch, p.Key.Variant.Platform, e.Summary())
}
