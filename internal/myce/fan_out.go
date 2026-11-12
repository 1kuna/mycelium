package myce

import (
	"errors"
	"fmt"
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
	// ShareXPU permits measured same-card admission only beside other opted-in pieces.
	ShareXPU bool `json:"share_xpu,omitempty" yaml:"share_xpu,omitempty"`
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
	// Frozen from the parent declaration; survives stripping Spec.FanOut on assignment.
	ShareXPU bool `json:"share_xpu,omitempty"`
}

func validateFanOut(s Spec) error {
	f := s.FanOut
	if f == nil {
		return nil
	}
	if !s.extensionJob() {
		return validatePlainFanOut(s)
	}
	if !s.extensionJob() || s.ExtensionCheck != nil || s.Program != nil || s.Workload == nil || !s.Workload.valid() || len(f.Pieces) == 0 || len(f.Pieces) > 256 || f.MaxConcurrency < 1 || f.MaxConcurrency > 64 {
		return errors.New("fan_out requires extension work, a homogeneous workload, 1..256 pieces and max_concurrency in 1..64; programs and checks are unsupported")
	}
	seen := map[string]bool{}
	names := []string{}
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

// Plain-command pieces carry their own stored files under this prefix in the
// parent manifest, so existing upload, claim, quota and retention paths see
// them as ordinary inputs. Each child receives only its own, renamed to dest.
const PlainPiecePrefix = ".myce-pieces/"

func validatePlainFanOut(s Spec) error {
	f := s.FanOut
	if len(f.Pieces) == 0 || len(f.Pieces) > 256 {
		return errors.New("plain-command pieces require 1..256 pieces")
	}
	if f.MaxConcurrency != 0 || f.ShareXPU {
		return errors.New("plain-command pieces are ordinary jobs; omit max_concurrency and share_xpu")
	}
	if s.After != nil || s.Run.Docker || s.Run.Workdir != "" || s.Kind != "" || s.Program != nil || s.Workload != nil || s.ExtensionCheck != nil || len(s.Inputs) > 0 {
		return errors.New("plain-command pieces cannot combine with after, Docker, workdir, kind/program, workload, checks or NAS inputs")
	}
	keys := map[string]bool{}
	for _, p := range f.Pieces {
		if !extensionName.MatchString(p.Key) || p.Key == "." || p.Key == ".." || keys[p.Key] {
			return errors.New("piece keys must be unique clean names")
		}
		keys[p.Key] = true
	}
	common := []string{}
	own := map[string][]string{}
	for name := range s.Files {
		rest, ok := strings.CutPrefix(name, PlainPiecePrefix)
		if !ok {
			common = append(common, name)
			continue
		}
		key, dest, ok := strings.Cut(rest, "/")
		if !ok || !keys[key] || dest == "" {
			return fmt.Errorf("stored piece file %q must be %s<declared key>/<dest>", name, PlainPiecePrefix)
		}
		own[key] = append(own[key], dest)
	}
	for name := range s.Outputs {
		common = append(common, name)
	}
	for key, dests := range own {
		names := append(slices.Clone(common), dests...)
		for i, a := range names {
			for k, b := range names {
				if i != k && (a == b || strings.HasPrefix(a, b+"/")) {
					return fmt.Errorf("piece %s inputs must not overlap common inputs, outputs or each other", key)
				}
			}
		}
	}
	return nil
}

func plainPieceFiles(files FileManifest, key string) FileManifest {
	out := FileManifest{}
	own := PlainPiecePrefix + key + "/"
	for name, ref := range files {
		if dest, ok := strings.CutPrefix(name, own); ok {
			out[dest] = ref
		} else if !strings.HasPrefix(name, PlainPiecePrefix) {
			out[name] = ref
		}
	}
	return out
}

// Plain pieces have no measured-batch planning: every piece becomes an
// ordinary job in the submit transaction, so the parent is fully dispatched
// (Next == len) and never holds a slot, like an admitted extension fan-out.
func (q *Queue) materializePlainPieces(parent *Job) {
	if parent.Spec.FanOut == nil || parent.Spec.extensionJob() || parent.FanOut == nil || parent.FanOut.Next > 0 {
		return
	}
	q.touchJob(parent.ID)
	for _, piece := range parent.Spec.FanOut.Pieces {
		child := cloneJob(parent)
		child.Spec.FanOut = nil
		child.Spec.SubmissionKey = ""
		child.Spec.AuthoringOrigin = OriginFanOutChild
		child.FanOut = nil
		child.Spec.Run.Command = append(slices.Clone(parent.Spec.Run.Command), piece.Args...)
		child.Spec.Files = plainPieceFiles(parent.Spec.Files, piece.Key)
		child.Batch = &BatchWork{Parent: parent.ID, Pieces: []FanOutPiece{piece}, Concurrency: 1}
		child.AdmissionWait = nil
		if q.simulation {
			q.simID++
			child.ID = fmt.Sprintf("sim-%012d", q.simID)
		} else {
			child.ID = ID()
		}
		child.Checkpoint = q.checkpointPath(child.ID)
		parent.FanOut.Children = append(parent.FanOut.Children, child.ID)
		q.putJob(child.ID, &child)
	}
	parent.FanOut.Next = len(parent.Spec.FanOut.Pieces)
}

func (q *Queue) fanOutTemplate(j *Job) *Job {
	copy := *j
	copy.Spec.FanOut = nil
	copy.FanOut = nil
	return &copy
}

func (q *Queue) fanOutCandidate(j *Job, m *Machine) *Job {
	return q.fanOutCandidateWithResources(j, m, nil)
}

func (q *Queue) fanOutCandidateWithResources(j *Job, m *Machine, resources map[batchResourceKey]batchResourceRoom, inventories ...[]*Machine) *Job {
	if j.State != "queued" || absoluteStartBy(j) && expired(j.Spec.StartBy, q.clock.Now()) || !dependencyReady(j) || cancelled(j) || j.FanOut == nil || j.FanOut.Next >= len(j.Spec.FanOut.Pieces) || !q.placementAllowed(j, m) || !q.checkpointReady(j, m) {
		return nil
	}
	// Each piece is an ordinary serial job: reuse ordinary eligibility and
	// release planning.
	for _, candidate := range extensionJobs(q.fanOutTemplate(j), m, true) {
		candidate.Spec.Args = append(slices.Clone(candidate.Spec.Args), j.Spec.FanOut.Pieces[j.FanOut.Next].Args...)
		q.applyLearnedNeeds(candidate)
		candidate = q.memoryCandidate(candidate, m)
		if q.capReasonFor(candidate, m) == "" && q.batchResourcesFitWithScan(candidate, m, resources) {
			candidate.Spec.FanOut = j.Spec.FanOut
			candidate.FanOut = j.FanOut
			candidate.Batch = &BatchWork{Parent: j.ID, Pieces: []FanOutPiece{j.Spec.FanOut.Pieces[j.FanOut.Next]}, Concurrency: 1, ShareXPU: j.Spec.FanOut.ShareXPU}
			return candidate
		}
	}
	return nil
}

// Called inside assignment's existing transaction, after the current planner
// has admitted this piece.
func (q *Queue) assignFanOut(selected *Job, m *Machine) *Job {
	parent := q.touchJob(selected.ID)
	copy := cloneJob(selected)
	copy.Spec.FanOut = nil
	copy.Spec.SubmissionKey = ""
	copy.Spec.AuthoringOrigin = OriginFanOutChild
	copy.FanOut = nil
	copy.Spec.Outputs = map[string]string{}
	for name, prefix := range parent.Spec.Outputs {
		copy.Spec.Outputs[name] = prefix
	}
	if q.simulation {
		q.simID++
		copy.ID = fmt.Sprintf("sim-%012d", q.simID)
	} else {
		copy.ID = ID()
	}
	copy.Checkpoint = q.checkpointPath(copy.ID)
	copy.Queued = q.clock.Now()
	copy.WaitingSince = parent.WaitingSince
	if copy.Spec.Priority == "production" {
		if !absoluteStartBy(&copy) {
			copy.Spec.StartBy = nil
		}
		initializeStartDeadline(&copy, copy.Queued)
	}
	// Parent evidence belongs to the demand before child materialization.
	q.closeAdmissionWait(parent, "wave_admitted")
	copy.AdmissionWait = nil
	parent.FanOut.Next += len(copy.Batch.Pieces)
	parent.FanOut.Children = append(parent.FanOut.Children, copy.ID)
	q.putJob(copy.ID, &copy)
	return &copy
}

func (q *Queue) maintainFanOutJob(parent *Job) {
	if parent.Spec.FanOut == nil || parent.FanOut == nil {
		return
	}
	if terminal(parent.State) {
		if parent.State != "succeeded" {
			for _, id := range parent.FanOut.Children {
				if child := q.s.Jobs[id]; child != nil && !terminal(child.State) {
					q.cancelJob(child)
				}
			}
		}
		return
	}
	if parent.FanOut.Results == nil {
		q.touchJob(parent.ID)
		parent.FanOut.Results = map[string]PieceResult{}
	}
	// Plain pieces are independent inputs: one failure lets the others settle,
	// then the parent fails naming them. Extension waves keep failing fast.
	plain := !parent.Spec.extensionJob()
	failed, pending := false, false
	var failedKeys []string
	for _, id := range parent.FanOut.Children {
		child := q.s.Jobs[id]
		if child == nil {
			failed = true
			continue
		}
		if child.State == "failed" || child.State == "cancelled" {
			failed = true
			for _, piece := range child.Batch.Pieces {
				failedKeys = append(failedKeys, piece.Key)
			}
			continue
		}
		if child.State != "succeeded" {
			pending = true
			continue
		}
		for _, piece := range child.Batch.Pieces {
			if old, ok := parent.FanOut.Results[piece.Key]; ok && old.Execution == (Execution{ID: child.ID, Attempt: child.Attempt}) {
				continue
			}
			q.touchJob(parent.ID)
			r := PieceResult{Execution: Execution{ID: child.ID, Attempt: child.Attempt}, OutputRefs: FileManifest{}, OutputFiles: map[string]string{}}
			for name := range parent.Spec.Outputs {
				if ref, ok := child.OutputRefs[name]; ok {
					r.OutputRefs[name] = ref
				}
				if path, ok := child.OutputFiles[name]; ok {
					r.OutputFiles[name] = path
				}
			}
			parent.FanOut.Results[piece.Key] = r
		}
	}
	if failed && plain && !pending {
		q.touchJob(parent.ID)
		parent.State, parent.Reason = "failed", plainPieceFailure(failedKeys, len(parent.Spec.FanOut.Pieces))
	} else if failed && !plain {
		q.touchJob(parent.ID)
		parent.State, parent.Reason = "failed", "declared piece failed"
	} else if !failed && len(parent.FanOut.Results) == len(parent.Spec.FanOut.Pieces) {
		q.touchJob(parent.ID)
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
		reason := fmt.Sprintf("declared pieces: %d/%d accepted", len(parent.FanOut.Results), len(parent.Spec.FanOut.Pieces))
		if parent.Reason != reason {
			q.touchJob(parent.ID)
			parent.Reason = reason
		}
	}
}

func plainPieceFailure(keys []string, total int) string {
	slices.Sort(keys)
	shown := keys
	if len(shown) > 10 {
		shown = shown[:10]
	}
	reason := fmt.Sprintf("%d/%d pieces failed: %s", len(keys), total, strings.Join(shown, ", "))
	if len(keys) > len(shown) {
		reason += fmt.Sprintf(" and %d more", len(keys)-len(shown))
	}
	return reason
}
