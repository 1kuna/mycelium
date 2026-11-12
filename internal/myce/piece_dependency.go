package myce

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

// One edge to a declared piece or whole fan-out parent, never an internal wave.
type PieceDependency struct {
	Job     string            `json:"job" yaml:"job"`
	Piece   string            `json:"piece,omitempty" yaml:"piece,omitempty"`
	Outputs map[string]string `json:"outputs" yaml:"outputs"`
}

// Separate from Spec so keyed intent is unchanged by acceptance and retries.
type PieceBinding struct {
	Job        string               `json:"job"`
	Piece      string               `json:"piece"`
	State      string               `json:"state"`
	Execution  Execution            `json:"execution"`
	Executions map[string]Execution `json:"executions,omitempty"`
	ReadyAt    *time.Time           `json:"ready_at,omitempty"`
	Files      FileManifest         `json:"files,omitempty"`
	Inputs     map[string]string    `json:"inputs,omitempty"`
}

func dependencyReady(j *Job) bool {
	return !j.simGatherWait && (j.Spec.After == nil || j.Dependency != nil && j.Dependency.State == "ready" && j.Dependency.ReadyAt != nil)
}

func validatePieceDependency(s Spec) error {
	a := s.After
	if a == nil {
		return nil
	}
	if a.Job == "" || len(a.Outputs) == 0 || s.FanOut != nil && a.Piece != "" || s.ExtensionCheck != nil || s.Program != nil || s.Run.Workdir != "" {
		return errors.New("after requires one parent job and output mapping without check, program or workdir; fan_out consumers require the whole parent (omit piece)")
	}
	names := []string{}
	for name := range s.Files {
		names = append(names, name)
	}
	for name := range s.Inputs {
		names = append(names, name)
	}
	// Serial fallback writes unkeyed output names; measured waves write keyed
	// names (checked by validateFanOut). Neither may overwrite a bound input.
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
	declared := map[string]bool{}
	for _, piece := range p.Spec.FanOut.Pieces {
		declared[piece.Key] = true
	}
	if s.After.Piece != "" && !declared[s.After.Piece] {
		return errors.New("after.piece is not declared by the parent")
	}
	for _, name := range s.After.Outputs {
		if s.After.Piece == "" {
			key, output, ok := strings.Cut(name, "/")
			if !ok || !declared[key] {
				return errors.New("whole-parent after outputs require declared KEY/OUTPUT_NAME names")
			}
			name = output
		}
		if kind, _ := outputKind(name); kind != outputFile {
			return errors.New("after outputs name individual parent files, not folders or globs")
		}
		if _, ok := p.Spec.Outputs[name]; !ok {
			return errors.New("after output is not declared by the parent")
		}
	}
	if p.FilesExpired && (p.State == "succeeded" || !cancelled(p)) {
		return resultExpired()
	}
	// Stored inputs and NAS inputs already have mutually exclusive staging rules.
	if p.storedFileResults() && len(s.Inputs) > 0 || !p.storedFileResults() && len(s.Files) > 0 {
		return errors.New("after cannot mix stored and NAS inputs")
	}
	return nil
}

func (q *Queue) maintainPieceDependency(j *Job) {
	if j.Spec.After == nil {
		return
	}
	a := j.Spec.After
	p := q.retainedJob(a.Job)
	if j.DiagnosticCode == "upstream_cancelled" && j.Reason == "cancel requested" {
		q.touchJob(j.ID)
		j.Reason = "upstream parent cancelled"
	}
	// A later cancellation intent excludes new producer attempts, but
	// does not invalidate outputs from a succeeded producer.
	if p != nil && p.State != "succeeded" && cancelled(p) {
		q.cancelJob(j)
		if j.Reason == "cancel requested" {
			j.DiagnosticCode, j.Reason = "upstream_cancelled", "upstream parent cancelled"
		}
		return
	}
	if terminal(j.State) || cancelled(j) || j.Dependency != nil && j.Dependency.ReadyAt != nil {
		return
	}
	if j.Dependency == nil {
		q.touchJob(j.ID)
		j.Dependency = &PieceBinding{Job: a.Job, Piece: a.Piece, State: "blocked"}
	}
	if b := bindPieceDependency(p, a); b != nil {
		q.touchJob(j.ID)
		now := q.clock.Now()
		b.ReadyAt = &now
		j.Dependency = b
		j.WaitingSince = now
		j.Reason = ""
		if j.Spec.Priority == "production" && j.Spec.StartBy == nil {
			initializeStartDeadline(j, now)
		}
		return
	}
	if p == nil || terminal(p.State) {
		q.touchJob(j.ID)
		now := q.clock.Now()
		j.State, j.DiagnosticCode, j.Reason = "failed", "upstream_failed", "producer piece has no accepted outputs"
		if p != nil && p.FilesExpired {
			j.DiagnosticCode, j.Reason = "result_expired", "producer outputs expired"
		}
		if a.Piece == "" && j.DiagnosticCode == "upstream_failed" {
			j.Reason = fmt.Sprintf("upstream fan-out parent %s did not succeed", a.Job)
		}
		j.FinishedAt = &now
	} else {
		reason := "waiting for accepted producer piece"
		if a.Piece == "" {
			reason = "waiting for upstream fan-out parent success"
		}
		if j.Reason != reason {
			q.touchJob(j.ID)
			j.Reason = reason
		}
	}
}

// Metadata-only binding: completion has already verified the exact accepted
// outputs outside q.mu. A whole-parent edge never releases on partial results.
func bindPieceDependency(p *Job, a *PieceDependency) *PieceBinding {
	if p == nil || p.FanOut == nil || p.FilesExpired || a.Piece == "" && p.State != "succeeded" {
		return nil
	}
	b := &PieceBinding{Job: p.ID, Piece: a.Piece, State: "ready", Files: FileManifest{}, Inputs: map[string]string{}}
	if a.Piece == "" {
		b.Executions = map[string]Execution{}
	}
	for local, source := range a.Outputs {
		key := a.Piece
		if key == "" {
			key, source, _ = strings.Cut(source, "/")
		}
		r, ok := p.FanOut.Results[key]
		if !ok {
			return nil
		}
		if ref, ok := r.OutputRefs[source]; ok {
			b.Files[local] = ref
		} else if path, ok := r.OutputFiles[source]; ok {
			b.Inputs[local] = path
		} else {
			return nil
		}
		if a.Piece == "" {
			b.Executions[key] = r.Execution
		} else {
			b.Execution = r.Execution
		}
	}
	return b
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
	files := []FileManifest{j.Spec.Files, sharedStoredFiles(j.Spec), j.OutputRefs}
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
