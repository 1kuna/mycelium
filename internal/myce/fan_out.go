package myce

import (
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
)

type FanOutPiece struct {
	Key  string   `json:"key" yaml:"key"`
	Args []string `json:"args,omitempty" yaml:"args,omitempty"`
}

// Common Inputs/Files are immutable inputs; Args name each independent piece.
// Applications own splitting and semantic reduction, never scheduler code.
type FanOut struct {
	Pieces         []FanOutPiece `json:"pieces" yaml:"pieces"`
	MaxConcurrency int           `json:"max_concurrency" yaml:"max_concurrency"`
}
type PieceResult struct {
	Execution   Execution         `json:"execution"`
	OutputRefs  FileManifest      `json:"output_refs,omitempty"`
	OutputFiles map[string]string `json:"output_files,omitempty"`
}
type FanOutProgress struct {
	Next     int                    `json:"next"`
	Children []string               `json:"children,omitempty"`
	Results  map[string]PieceResult `json:"results,omitempty"`
}
type BatchWork struct {
	Parent      string        `json:"parent"`
	Pieces      []FanOutPiece `json:"pieces"`
	Concurrency int           `json:"concurrency"`
	Run         bool          `json:"run,omitempty"`
	Seconds     float64       `json:"seconds,omitempty"`
}

func validateFanOut(s Spec) error {
	f := s.FanOut
	if f == nil {
		return nil
	}
	if !s.extensionJob() || s.ExtensionCheck != nil || s.Program != nil || s.Workload == nil || !s.Workload.valid() || len(f.Pieces) == 0 || len(f.Pieces) > 256 || f.MaxConcurrency < 1 || f.MaxConcurrency > 64 {
		return errors.New("fan_out requires extension work, a homogeneous workload, 1..256 pieces and max_concurrency in 1..64; programs and checks are unsupported")
	}
	seen := map[string]bool{}
	names := []string{".myce-batch.json"}
	for name := range s.Files {
		names = append(names, name)
	}
	for name := range s.Inputs {
		names = append(names, name)
	}
	if s.After != nil {
		for name := range s.After.Outputs {
			names = append(names, name)
		}
	}
	for _, p := range f.Pieces {
		if !extensionName.MatchString(p.Key) || p.Key == "." || p.Key == ".." || seen[p.Key] {
			return errors.New("fan_out piece keys must be unique clean names")
		}
		seen[p.Key] = true
		for name := range s.Outputs {
			names = append(names, filepath.Join(p.Key, name))
		}
	}
	for i, a := range names {
		for k, b := range names {
			if i != k && (a == b || strings.HasPrefix(a, b+"/")) {
				return errors.New("fan_out manifest, inputs and keyed outputs must not overlap")
			}
		}
	}
	return nil
}

func (q *Queue) fanOutTemplate(j *Job) *Job {
	copy := *j
	copy.Spec.FanOut = nil
	copy.FanOut = nil
	return &copy
}

func scaledBatchNeeds(n ResourceNeeds, p WorkObservation, count int) ResourceNeeds {
	n.CPUCores = max(1, n.CPUCores) * count
	n.MemoryGB *= float64(count)
	n.HostMemoryGB *= float64(count)
	if n.cpuOnly() {
		n.HostMemoryGB = max(n.HostMemoryGB, p.HostMemoryGB)
	} else {
		n.HostMemoryGB = max(n.HostMemoryGB, p.HostMemoryGB)
		n.MemoryGB = max(n.MemoryGB, p.DeviceMemoryGB)
	}
	return n
}

// A retry keeps membership and count, but uses the destination's own measured
// resource envelope. A fast variant's per-piece floor is not a whole-wave floor.
func (q *Queue) retryWaveCandidate(j *Job, m *Machine) *Job {
	e := qualifiedExtension(j, m)
	if e == nil {
		return nil
	}
	v := e.variant(machineTarget(m))
	if v == nil {
		return nil
	}
	sweep := v.Sweeps[j.Spec.workType()]
	if len(sweep.Run) == 0 || !slices.Contains(sweep.Counts, j.Batch.Concurrency) {
		return nil
	}
	key, _ := curveKey(*e, m, j.Spec.workType())
	if key == nil || key.Reference.InputShape != j.Spec.Workload.InputShape {
		return nil
	}
	key.Concurrency = j.Batch.Concurrency
	p := q.s.WorkObservations[identityHash(key)]
	if !q.usableCurve(p, *key) {
		return nil
	}
	copy := *j
	n := v.Needs
	n.Network = n.Network || j.Spec.Needs.Network
	n.Platform = Platform{Required: []string{m.GPU}}
	s := Spec{Needs: n}
	if normalizeResources(&s) != nil {
		return nil
	}
	n = s.Needs
	n.MemoryGB = max(n.MemoryGB, j.Spec.SizeHint.MemoryGB)
	n.HostMemoryGB = max(n.HostMemoryGB, j.Spec.SizeHint.HostMemoryGB)
	n = scaledBatchNeeds(n, p, j.Batch.Concurrency)
	n.GPU = j.Spec.Needs.GPU
	n.CPUCores = max(n.CPUCores, j.Spec.Needs.CPUCores)
	n.HostMemoryGB = max(n.HostMemoryGB, j.Spec.Needs.HostMemoryGB)
	if !n.cpuOnly() {
		n.MemoryGB = max(n.MemoryGB, j.Spec.Needs.MemoryGB)
	}
	copy.Spec.Needs = n
	batch := *j.Batch
	batch.Seconds = j.Spec.Workload.Units * float64(batch.Concurrency) / p.Rate
	copy.Batch = &batch
	return &copy
}

func (q *Queue) usableCurve(p WorkObservation, key ObservationKey) bool {
	return p.Key == key && p.Samples > 0 && !p.Forbidden && p.StaleReason == "" && validRate(p.Rate) && !q.clock.Now().Before(p.RecordedAt) && q.clock.Now().Sub(p.RecordedAt) <= workCurveMaxAge
}

// Keep exact occupancy for OOM invalidation, without treating a whole wave as
// a serial timing sample. Accepted wave throughput refinement remains deferred.
func (q *Queue) waveObservationKey(j *Job) (*ObservationKey, string) {
	owner := q.s.Machines[j.Machine]
	if owner != nil && owner.Session == j.Session && !owner.Gone && !q.unreported[owner.ID] {
		for _, m := range q.machineTargets(owner) {
			if j.Spec.Needs.cpuOnly() && m.GPU != "cpu" || !j.Spec.Needs.cpuOnly() && !batchOnTarget(j, m) {
				continue
			}
			if e := qualifiedExtension(j, m); e != nil {
				if key, _ := curveKey(*e, m, j.Spec.workType()); key != nil {
					key.Concurrency = j.Batch.Concurrency
					return key, "declared wave; throughput refinement disabled"
				}
			}
		}
	}
	return nil, "declared wave; component evidence unavailable"
}

func (q *Queue) fanOutComponents(j *Job) ([]batchComponent, map[string]*Job) {
	var components []batchComponent
	candidates := map[string]*Job{}
	for _, m := range q.plannerMachines() {
		if !q.workerIdle(m, j) || !q.placementAllowed(j, m) || !q.checkpointReady(j, m) {
			continue
		}
		for _, candidate := range extensionJobs(q.fanOutTemplate(j), m, true) {
			q.applyLearnedNeeds(candidate)
			if q.capReasonFor(candidate, m) != "" || !fitsResources(candidate, m, m.FreeMemoryGB) {
				continue
			}
			e := qualifiedExtension(candidate, m)
			if e == nil {
				continue
			}
			v := e.variant(machineTarget(m))
			if v == nil {
				continue
			}
			sweep, ok := v.Sweeps[j.Spec.workType()]
			if !ok || len(sweep.Run) == 0 || sweep.ServerDigest != "" {
				continue
			}
			key, _ := curveKey(*e, m, j.Spec.workType())
			if key == nil || key.Reference.InputShape != j.Spec.Workload.InputShape {
				continue
			}
			transfer, known := inputTransferSeconds(candidate, m)
			if !known {
				continue
			}
			component := batchComponent{id: targetKey(m), host: hostID(m), ready: environmentSeconds(candidate, m) + transfer, cpuBudget: float64(m.FreeCPUCores), hostBudget: m.FreeHostMemoryGB}
			for _, count := range sweep.Counts {
				if count > j.Spec.FanOut.MaxConcurrency {
					continue
				}
				k := *key
				k.Concurrency = count
				p := q.s.WorkObservations[identityHash(k)]
				if !q.usableCurve(p, k) {
					continue
				}
				copy := *candidate
				copy.Spec.Needs = scaledBatchNeeds(copy.Spec.Needs, p, count)
				if !fitsResources(&copy, m, m.FreeMemoryGB) || q.capReasonFor(&copy, m) != "" {
					continue
				}
				component.points = append(component.points, batchPoint{count: count, rate: p.Rate, cpu: float64(copy.Spec.Needs.CPUCores), host: hostNeed(m, copy.Spec.Needs.MemoryGB, copy.Spec.Needs.HostMemoryGB)})
			}
			if len(component.points) > 0 {
				components = append(components, component)
				candidates[component.id] = candidate
				break
			}
		}
	}
	return components, candidates
}

func (q *Queue) fanOutCandidate(j *Job, m *Machine) *Job {
	if j.State != "queued" || !dependencyReady(j) || cancelled(j) || j.FanOut == nil || j.FanOut.Next >= len(j.Spec.FanOut.Pieces) || !q.placementAllowed(j, m) || !q.checkpointReady(j, m) {
		return nil
	}
	components, candidates := q.fanOutComponents(j)
	remaining := len(j.Spec.FanOut.Pieces) - j.FanOut.Next
	plan := planBatch(components, remaining, j.Spec.Workload.Units)
	for _, a := range plan {
		if a.component != targetKey(m) {
			continue
		}
		copy := *candidates[a.component]
		e := qualifiedExtension(&copy, m)
		if e == nil {
			continue
		}
		key, _ := curveKey(*e, m, j.Spec.workType())
		if key == nil {
			continue
		}
		key.Concurrency = a.count
		copy.Spec.Needs = scaledBatchNeeds(copy.Spec.Needs, q.s.WorkObservations[identityHash(key)], a.count)
		copy.Spec.FanOut = j.Spec.FanOut
		copy.FanOut = j.FanOut
		copy.Batch = &BatchWork{Parent: j.ID, Pieces: slices.Clone(j.Spec.FanOut.Pieces[j.FanOut.Next : j.FanOut.Next+a.count]), Concurrency: a.count, Run: true, Seconds: float64(a.count) * j.Spec.Workload.Units / q.s.WorkObservations[identityHash(key)].Rate}
		return &copy
	}
	if len(plan) > 0 {
		return nil
	}
	// Unknown/ineligible curves grant no slots. Reuse ordinary serial eligibility
	// and release planning, rather than waiting for a measured but busy machine.
	for _, candidate := range extensionJobs(q.fanOutTemplate(j), m, true) {
		q.applyLearnedNeeds(candidate)
		candidate = q.memoryCandidate(candidate, m)
		if q.capReasonFor(candidate, m) == "" && q.batchResourcesFit(candidate, m) {
			candidate.Spec.FanOut = j.Spec.FanOut
			candidate.FanOut = j.FanOut
			candidate.Batch = &BatchWork{Parent: j.ID, Pieces: []FanOutPiece{j.Spec.FanOut.Pieces[j.FanOut.Next]}, Concurrency: 1}
			return candidate
		}
	}
	return nil
}

// Called inside assignment's existing transaction, after the current planner
// has admitted this wave. No forecast is persisted as a machine reservation.
func (q *Queue) assignFanOut(selected *Job, m *Machine) *Job {
	parent := q.touchJob(selected.ID)
	copy := cloneJob(selected)
	copy.Spec.FanOut = nil
	copy.Spec.SubmissionKey = ""
	copy.FanOut = nil
	copy.Spec.Outputs = map[string]string{}
	for _, piece := range copy.Batch.Pieces {
		for name, prefix := range parent.Spec.Outputs {
			local := name
			if copy.Batch.Run {
				local = filepath.Join(piece.Key, name)
			}
			copy.Spec.Outputs[local] = prefix
		}
	}
	if !copy.Batch.Run {
		copy.Spec.Args = append(slices.Clone(copy.Spec.Args), copy.Batch.Pieces[0].Args...)
	}
	if q.simulation {
		q.simID++
		copy.ID = fmt.Sprintf("sim-%012d", q.simID)
	} else {
		copy.ID = ID()
	}
	copy.Checkpoint = filepath.Join(q.root, copy.ID)
	copy.Queued = q.clock.Now()
	copy.WaitingSince = parent.WaitingSince
	if copy.Spec.Priority == "production" {
		until := q.clock.Now().Add(ProductionStartTimeout)
		copy.Spec.StartBy = &until
	}
	parent.FanOut.Next += len(copy.Batch.Pieces)
	parent.FanOut.Children = append(parent.FanOut.Children, copy.ID)
	q.putJob(copy.ID, &copy)
	return &copy
}

func (q *Queue) maintainFanOut() {
	for _, parent := range q.liveJobs() {
		if parent.Spec.FanOut == nil || parent.FanOut == nil {
			continue
		}
		if terminal(parent.State) {
			if parent.State != "succeeded" {
				for _, id := range parent.FanOut.Children {
					if child := q.s.Jobs[id]; child != nil && !terminal(child.State) {
						q.cancelJob(child)
					}
				}
			}
			continue
		}
		if parent.FanOut.Results == nil {
			parent.FanOut.Results = map[string]PieceResult{}
		}
		failed := false
		for _, id := range parent.FanOut.Children {
			child := q.s.Jobs[id]
			if child == nil {
				failed = true
				continue
			}
			if child.State == "failed" || child.State == "cancelled" {
				failed = true
				continue
			}
			if child.State != "succeeded" {
				continue
			}
			for _, piece := range child.Batch.Pieces {
				r := PieceResult{Execution: Execution{ID: child.ID, Attempt: child.Attempt}, OutputRefs: FileManifest{}, OutputFiles: map[string]string{}}
				for name := range parent.Spec.Outputs {
					local := name
					if child.Batch.Run {
						local = filepath.Join(piece.Key, name)
					}
					if ref, ok := child.OutputRefs[local]; ok {
						r.OutputRefs[name] = ref
					}
					if path, ok := child.OutputFiles[local]; ok {
						r.OutputFiles[name] = path
					}
				}
				parent.FanOut.Results[piece.Key] = r
			}
		}
		if failed {
			parent.State, parent.Reason = "failed", "declared piece failed"
		} else if len(parent.FanOut.Results) == len(parent.Spec.FanOut.Pieces) {
			parent.State, parent.Reason = "succeeded", ""
			parent.OutputRefs = FileManifest{}
			parent.OutputFiles = map[string]string{}
			for key, r := range parent.FanOut.Results {
				for name, ref := range r.OutputRefs {
					parent.OutputRefs[filepath.Join(key, name)] = ref
				}
				for name, path := range r.OutputFiles {
					parent.OutputFiles[filepath.Join(key, name)] = path
				}
			}
		}
		if terminal(parent.State) {
			now := q.clock.Now()
			parent.FinishedAt = &now
		} else {
			parent.Reason = fmt.Sprintf("declared pieces: %d/%d accepted", len(parent.FanOut.Results), len(parent.Spec.FanOut.Pieces))
		}
	}
}

// A public snapshot can omit nil collections without losing keyed membership.
func batchManifest(j *Job) map[string]any {
	return map[string]any{"parent": j.Batch.Parent, "pieces": j.Batch.Pieces, "concurrency": j.Batch.Concurrency, "args": j.Spec.Args, "inputs": slices.Sorted(maps.Keys(jobInputPaths(j))), "files": slices.Sorted(maps.Keys(jobInputFiles(j))), "outputs": slices.Sorted(maps.Keys(j.Spec.Outputs))}
}
