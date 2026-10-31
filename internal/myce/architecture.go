package myce

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
)

const architectureMismatch = "arch_mismatch"

type JobArchitecture struct {
	Target   ExecutionTarget   `json:"target"` // exact assigned attempt, never live inventory
	Excluded []ExecutionTarget `json:"excluded,omitempty"`
	Moved    bool              `json:"moved,omitempty"` // at most one architecture recovery per job
}

// Only explicit OS/exec diagnostics count. An exit code, missing dependency,
// signal, timeout or ordinary test failure alone is never portability evidence.
func architectureErrorText(text string) bool {
	for _, line := range strings.Split(strings.ToLower(text), "\n") {
		line = strings.TrimSpace(line)
		for _, signal := range []string{"exec format error", "bad cpu type in executable", "wrong architecture"} {
			if line == signal || strings.HasSuffix(line, ": "+signal) || strings.HasSuffix(line, ", but "+signal) {
				return true
			}
		}
		if strings.Contains(line, "mach-o file, but is an incompatible architecture") {
			return true
		}
	}
	return false
}

func architectureExecError(err error) bool {
	return err != nil && (errors.Is(err, syscall.ENOEXEC) || architectureErrorText(err.Error()))
}

// Capture stderr separately from stdout, including when retained logs are off.
// An explicit pipe keeps cmd.Wait independent of descendants holding stderr.
// Retain only one bounded line; the match is sticky and stores no raw output.
type architectureStderr struct {
	reader, writer *os.File
	done           chan struct{}
	once           sync.Once
	matched        bool
	line           []byte
}

func newArchitectureStderr(output *os.File) (*architectureStderr, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	c := &architectureStderr{reader: reader, writer: writer, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		defer reader.Close()
		_, _ = io.Copy(captureArchitectureOutput{c, output}, reader)
		c.matched = c.matched || architectureErrorText(string(c.line))
	}()
	return c, nil
}

type captureArchitectureOutput struct {
	capture *architectureStderr
	output  *os.File
}

func (w captureArchitectureOutput) Write(p []byte) (int, error) {
	c := w.capture
	for _, b := range p {
		if b == '\n' {
			c.matched = c.matched || architectureErrorText(string(c.line))
			c.line = c.line[:0]
		} else {
			if len(c.line) == 4096 {
				c.line = c.line[2048:]
			}
			c.line = append(c.line, b)
		}
	}
	return w.output.Write(p)
}

func (c *architectureStderr) finish(released bool) bool {
	c.once.Do(func() {
		c.writer.Close()
		if !released {
			c.reader.Close()
		}
		<-c.done
	})
	return c.matched
}

func classifyArchitectureFailure(r *Result, stderrMismatch bool) {
	if r.Success || r.OutOfMemory || r.PoliteStop || r.PriorityStop != nil || r.Forced {
		return
	}
	cause := r.ExecutionCause
	if cause == nil {
		return
	}
	known := cause.Class == architectureMismatch
	if !known && !r.Interrupted && cause.Class == "command_exit" {
		known = stderrMismatch
	}
	if known {
		r.Interrupted = false
		r.ExecutionCause = &RuntimeCause{Operation: cause.Operation, Class: architectureMismatch}
		r.Error = "architecture mismatch: correct targets.arch or the portable declaration"
	}
}

func architectureProgramKey(j *Job) string {
	// Freeze the executable contract, not its name, target, placement or retry.
	// Changed commands, source/artifacts, recipes or extension digest relearn.
	return identityHash(struct {
		Project                 string
		Run                     RunSpec
		Execution               *ExecutionSpec
		Program                 *Program
		Args                    []string `json:",omitempty"`
		Kind, Extension, Digest string
		Files                   FileManifest           `json:",omitempty"`
		Inputs                  map[string]string      `json:",omitempty"`
		Shared                  map[string]SharedInput `json:",omitempty"`
		Environment             Environment
		Recipes                 map[string]EnvironmentRecipe            `json:",omitempty"`
		Sidecars                map[string]map[string]EnvironmentRecipe `json:",omitempty"`
	}{j.Spec.Project, j.Spec.Run, j.Spec.Execution, j.Spec.Program, j.Spec.Args, j.Spec.Kind, j.Spec.Extension, j.Spec.ExtensionDigest, j.Spec.Files, j.Spec.Inputs, j.Spec.SharedInputs, j.Spec.Environment, j.EnvironmentRecipes, j.SidecarEnvironmentRecipes})
}

func (q *Queue) applyLearnedArchitecture(j *Job) {
	if exclusions := q.s.ArchitectureConstraints[architectureProgramKey(j)]; len(exclusions) > 0 {
		j.Architecture = &JobArchitecture{Excluded: slices.Clone(exclusions)}
	}
}

func executionJobTargetMatches(j *Job, m *Machine) bool {
	if j.Architecture != nil {
		t := machineTarget(m)
		if len(j.Architecture.Excluded) > 0 && (t.OS == "" || t.Arch == "") {
			return false
		}
		for _, bad := range j.Architecture.Excluded {
			if t.OS == bad.OS && t.Arch == bad.Arch {
				return false
			}
		}
	}
	return executionTargetMatches(j.Spec, m)
}

func (q *Queue) recordArchitectureMismatch(j *Job) {
	j.DiagnosticCode = architectureMismatch
	reason := "architecture mismatch; correct targets.arch or the portable declaration"
	a := j.Architecture
	if a == nil || !extensionName.MatchString(a.Target.OS) || !extensionName.MatchString(a.Target.Arch) {
		j.State, j.Reason = "failed", reason+" (attempt architecture unknown)"
		return
	}
	// Other attempts of this program may have learned since submission.
	for _, excluded := range q.s.ArchitectureConstraints[architectureProgramKey(j)] {
		if !slices.Contains(a.Excluded, excluded) {
			a.Excluded = append(a.Excluded, excluded)
		}
	}
	bad := ExecutionTarget{OS: a.Target.OS, Arch: a.Target.Arch}
	if !slices.Contains(a.Excluded, bad) {
		a.Excluded = append(a.Excluded, bad)
	}
	if q.s.ArchitectureConstraints == nil {
		q.s.ArchitectureConstraints = map[string][]ExecutionTarget{}
	}
	q.s.ArchitectureConstraints[architectureProgramKey(j)] = slices.Clone(a.Excluded)
	canMove := len(j.Spec.Targets) == 0 || slices.ContainsFunc(j.Spec.Targets, func(t ExecutionTarget) bool { return t.OS != bad.OS || t.Arch == "" || t.Arch != bad.Arch })
	if a.Moved || !canMove || automaticExtensionCheck(j) != nil {
		j.State, j.Reason = "failed", reason+" (no further architecture moves)"
		return
	}
	a.Moved = true
	q.requeue(j, fmt.Sprintf("architecture mismatch on %s/%s; excluded that architecture, waiting for a compatible target (one move only)", bad.OS, bad.Arch), false)
}
