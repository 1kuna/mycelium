package myce

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// One edge to an ordinary producer, declared piece or whole fan-out parent,
// never an internal wave.
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
	if j.simGatherWait {
		return false
	}
	if j.Spec.After != nil {
		return j.Dependency != nil && j.Dependency.State == "ready" && j.Dependency.ReadyAt != nil
	}
	return !warmPieceDependencies(j.Spec) || warmPieceAvailable(j)
}

// dependencyParents names every producer a job's declaration binds, once.
func dependencyParents(s Spec) []string {
	var out []string
	if s.After != nil {
		out = append(out, s.After.Job)
	}
	if s.FanOut != nil {
		for _, p := range s.FanOut.Pieces {
			if p.After != nil && !slices.Contains(out, p.After.Job) {
				out = append(out, p.After.Job)
			}
		}
	}
	return out
}

func freshPipelineRoot(j *Job) bool {
	return j.Spec.After == nil && j.Attempt == 0 && j.PreemptionPlacement == nil
}

func (q *Queue) pipelineStagePrecedes(stage, root *Job) bool {
	return freshPipelineRoot(root) && stage.Spec.After != nil && dependencyReady(stage) &&
		stage.Spec.Project == root.Spec.Project && q.comparePriority(stage.Spec, root.Spec) == 0
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
	if s.FanOut != nil {
		for _, piece := range s.FanOut.Pieces {
			if piece.After == nil {
				continue
			}
			p, err := q.validateProducer(s.Project, piece.After)
			if err != nil {
				return err
			}
			// Warm items stage only verified file-store references.
			if !p.storedFileResults() {
				return errors.New("per-piece after requires a producer with stored-file results")
			}
		}
	}
	if s.After == nil {
		return nil
	}
	p, err := q.validateProducer(s.Project, s.After)
	if err != nil {
		return err
	}
	// Stored inputs and NAS inputs already have mutually exclusive staging rules.
	if p.storedFileResults() && len(s.Inputs) > 0 || !p.storedFileResults() && len(s.Files) > 0 {
		return errors.New("after cannot mix stored and NAS inputs")
	}
	return nil
}

func (q *Queue) validateProducer(project string, a *PieceDependency) (*Job, error) {
	p := q.retainedJob(a.Job)
	if p == nil || p.Spec.Project != project || p.Batch != nil || p.Spec.ExtensionCheck != nil || p.Spec.Program != nil {
		return nil, errors.New("after.job must be a full producer job ID in the same project, not an internal piece, check or program")
	}
	declared := map[string]bool{}
	if p.Spec.FanOut != nil {
		for _, piece := range p.Spec.FanOut.Pieces {
			declared[piece.Key] = true
		}
	}
	if a.Piece != "" && !declared[a.Piece] {
		return nil, errors.New("after.piece is not declared by the parent")
	}
	for _, name := range a.Outputs {
		if a.Piece == "" && p.Spec.FanOut != nil {
			key, output, ok := strings.Cut(name, "/")
			if !ok || !declared[key] {
				return nil, errors.New("whole-parent after outputs require declared KEY/OUTPUT_NAME names")
			}
			name = output
		}
		if kind, _ := outputKind(name); kind != outputFile {
			return nil, errors.New("after outputs name individual parent files, not folders or globs")
		}
		if _, ok := p.Spec.Outputs[name]; !ok {
			return nil, errors.New("after output is not declared by the parent")
		}
	}
	if p.FilesExpired && (p.State == "succeeded" || !cancelled(p)) {
		return nil, resultExpired()
	}
	return p, nil
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
			kind := "producer"
			if p != nil && p.FanOut != nil {
				kind = "fan-out parent"
			}
			j.Reason = fmt.Sprintf("upstream %s %s did not succeed", kind, a.Job)
		}
		j.FinishedAt = &now
	} else {
		reason := "waiting for accepted producer piece"
		if a.Piece == "" {
			reason = "waiting for upstream producer success"
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
	if p == nil || p.FilesExpired || a.Piece == "" && p.State != "succeeded" {
		return nil
	}
	b := &PieceBinding{Job: p.ID, Piece: a.Piece, State: "ready", Files: FileManifest{}, Inputs: map[string]string{}}
	if p.FanOut == nil {
		if a.Piece != "" || p.State != "succeeded" {
			return nil
		}
		b.Execution = Execution{ID: p.ID, Attempt: p.Attempt}
		for local, source := range a.Outputs {
			if ref, ok := p.OutputRefs[source]; ok {
				b.Files[local] = ref
			} else if path, ok := p.OutputFiles[source]; ok {
				b.Inputs[local] = path
			} else {
				return nil
			}
		}
		return b
	}
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
	var files []FileManifest
	visitJobFileManifests(j, func(_ string, manifest FileManifest) {
		files = append(files, manifest)
	})
	return files
}

func visitJobFileManifests(j *Job, visit func(string, FileManifest)) {
	visit("input", j.Spec.Files)
	visit("shared_input", sharedStoredFiles(j.Spec))
	visit("result", j.OutputRefs)
	if j.Spec.localFirst() || j.Spec.warmPieces() {
		for _, save := range j.FileCheckpoints {
			visit("checkpoint", save.Files)
		}
		for _, m := range j.CheckpointMirrors {
			visit("checkpoint_mirror", m.Checkpoint.Files)
		}
	}
	for _, a := range j.AttemptHistory {
		if a.Diagnostics != nil {
			visit("diagnostic", a.Diagnostics.Files)
		}
	}
	if j.Dependency != nil {
		visit("dependency_input", j.Dependency.Files)
	}
	if j.FanOut != nil && j.FanOut.Warm != nil {
		for _, b := range j.FanOut.Warm.Dependencies {
			visit("dependency_input", b.Files)
		}
	}
	if j.FanOut != nil {
		for _, r := range j.FanOut.Results {
			visit("fan_out_result", r.OutputRefs)
		}
	}
}
