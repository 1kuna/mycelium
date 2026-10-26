package myce

import (
	"errors"
	"maps"
	"strings"
	"time"
)

// One edge to a declared piece, never to a planner-created wave.
type PieceDependency struct {
	Job     string            `json:"job" yaml:"job"`
	Piece   string            `json:"piece" yaml:"piece"`
	Outputs map[string]string `json:"outputs" yaml:"outputs"`
}

// Separate from Spec so keyed intent is unchanged by acceptance and retries.
type PieceBinding struct {
	Job       string            `json:"job"`
	Piece     string            `json:"piece"`
	State     string            `json:"state"`
	Execution Execution         `json:"execution"`
	ReadyAt   *time.Time        `json:"ready_at,omitempty"`
	Files     FileManifest      `json:"files,omitempty"`
	Inputs    map[string]string `json:"inputs,omitempty"`
}

func dependencyReady(j *Job) bool {
	return !j.simGatherWait && (j.Spec.After == nil || j.Dependency != nil && j.Dependency.State == "ready" && j.Dependency.ReadyAt != nil)
}

func validatePieceDependency(s Spec) error {
	a := s.After
	if a == nil {
		return nil
	}
	if a.Job == "" || a.Piece == "" || len(a.Outputs) == 0 || s.FanOut != nil || s.ExtensionCheck != nil || s.Program != nil || s.Run.Workdir != "" || s.Run.Docker {
		return errors.New("after requires one parent job, piece and output mapping on an ordinary batch/extension job without fan_out, check, program, workdir or Docker")
	}
	names := []string{}
	for name := range s.Files {
		names = append(names, name)
	}
	for name := range s.Inputs {
		names = append(names, name)
	}
	for name := range s.Outputs {
		names = append(names, name)
	}
	for name, output := range a.Outputs {
		if !safeRelative(name) || !safeRelative(output) {
			return errors.New("after outputs require clean relative names")
		}
		names = append(names, name)
	}
	for i, a := range names {
		for k, b := range names {
			if i != k && (a == b || strings.HasPrefix(a, b+"/")) {
				return errors.New("after input and job file names overlap")
			}
		}
	}
	return nil
}

// q.mu is held. Existing full IDs, same project and declared outputs rule out
// forward references, cycles and accidental references to internal waves.
func (q *Queue) validatePieceSource(s Spec) error {
	if s.After == nil {
		return nil
	}
	p := q.retainedJob(s.After.Job)
	if p == nil || p.Spec.Project != s.Project || p.Spec.FanOut == nil || p.FanOut == nil {
		return errors.New("after.job must be a full fan-out parent ID in the same project")
	}
	found := false
	for _, piece := range p.Spec.FanOut.Pieces {
		found = found || piece.Key == s.After.Piece
	}
	if !found {
		return errors.New("after.piece is not declared by the parent")
	}
	for _, name := range s.After.Outputs {
		if _, ok := p.Spec.Outputs[name]; !ok {
			return errors.New("after output is not declared by the parent")
		}
	}
	if !cancelled(p) && p.FilesExpired {
		return resultExpired()
	}
	// Stored inputs and NAS inputs already have mutually exclusive staging rules.
	if p.storedFileResults() && len(s.Inputs) > 0 || !p.storedFileResults() && len(s.Files) > 0 {
		return errors.New("after cannot mix stored and NAS inputs")
	}
	return nil
}

func (q *Queue) maintainPieceDependencies() {
	for _, j := range q.s.Jobs {
		if j.Spec.After == nil {
			continue
		}
		a := j.Spec.After
		p := q.retainedJob(a.Job)
		if p != nil && cancelled(p) {
			q.cancelJob(j)
			continue
		}
		if terminal(j.State) || cancelled(j) || j.Dependency != nil && j.Dependency.ReadyAt != nil {
			continue
		}
		if j.Dependency == nil {
			j.Dependency = &PieceBinding{Job: a.Job, Piece: a.Piece, State: "blocked"}
		}
		if p != nil && p.FanOut != nil {
			if r, ok := p.FanOut.Results[a.Piece]; ok && !p.FilesExpired {
				b := PieceBinding{Job: p.ID, Piece: a.Piece, State: "ready", Execution: r.Execution, Files: FileManifest{}, Inputs: map[string]string{}}
				complete := true
				for local, source := range a.Outputs {
					if ref, ok := r.OutputRefs[source]; ok {
						b.Files[local] = ref
					} else if path, ok := r.OutputFiles[source]; ok {
						b.Inputs[local] = path
					} else {
						complete = false
					}
				}
				if complete {
					now := q.clock.Now()
					b.ReadyAt = &now
					j.Dependency = &b
					j.WaitingSince = now
					j.Reason = ""
					if j.Spec.Priority == "production" && j.Spec.StartBy == nil {
						deadline := now.Add(ProductionStartTimeout)
						j.Spec.StartBy = &deadline
						j.StartWindow = ProductionStartTimeout
					}
					continue
				}
			}
		}
		if p == nil || terminal(p.State) {
			now := q.clock.Now()
			j.State, j.DiagnosticCode, j.Reason = "failed", "upstream_failed", "producer piece has no accepted outputs"
			if p != nil && p.FilesExpired {
				j.DiagnosticCode, j.Reason = "result_expired", "producer outputs expired"
			}
			j.FinishedAt = &now
		} else {
			j.Reason = "waiting for accepted producer piece"
		}
	}
}

func jobInputFiles(j *Job) FileManifest {
	if j.Dependency == nil || len(j.Dependency.Files) == 0 {
		return j.Spec.Files
	}
	files := maps.Clone(j.Spec.Files)
	if files == nil {
		files = FileManifest{}
	}
	maps.Copy(files, j.Dependency.Files)
	return files
}
func jobInputPaths(j *Job) map[string]string {
	if j.Dependency == nil || len(j.Dependency.Inputs) == 0 {
		return j.Spec.Inputs
	}
	paths := maps.Clone(j.Spec.Inputs)
	if paths == nil {
		paths = map[string]string{}
	}
	maps.Copy(paths, j.Dependency.Inputs)
	return paths
}

// All accepted references use the existing live/terminal retention window.
func jobFileManifests(j *Job) []FileManifest {
	files := []FileManifest{j.Spec.Files, j.OutputRefs}
	for _, a := range j.AttemptHistory {
		if a.Diagnostics != nil {
			files = append(files, a.Diagnostics.Files)
		}
	}
	if j.Dependency != nil {
		files = append(files, j.Dependency.Files)
	}
	if j.FanOut != nil {
		for _, r := range j.FanOut.Results {
			files = append(files, r.OutputRefs)
		}
	}
	return files
}
