package myce

import (
	"errors"
	"log"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

const legacyIsolationReason = "legacy extension registration; author a new version with isolation: sandboxed to opt in"

// ValidateNativeIsolation is shared by authoring and submission boundaries.
func ValidateNativeIsolation(mode, reason string) error {
	if mode != "" && mode != "sandboxed" && mode != "unsandboxed" {
		return errors.New("isolation must be sandboxed or unsandboxed")
	}
	if mode == "unsandboxed" && strings.TrimSpace(reason) == "" {
		return errors.New("isolation: unsandboxed requires a nonempty reason")
	}
	if mode == "" && reason != "" {
		return errors.New("reason requires an explicit isolation mode")
	}
	return nil
}

func (e Extension) unsandboxedReason() string {
	if e.Isolation == "unsandboxed" {
		return e.Reason
	}
	if e.Isolation == "" && !e.SandboxDefault {
		return legacyIsolationReason
	}
	return ""
}

// Frozen registrations, never a caller's job override, own extension policy.
// Placement evaluates one candidate; queued status may describe alternatives.
func nativeUnsandboxedReasons(j *Job) []string {
	if j.Spec.Run.Docker {
		return nil
	}
	var reasons []string
	if j.Spec.extensionJob() {
		for _, e := range j.Extensions {
			if j.State != "queued" && j.Extension != nil && j.Extension.Digest != e.Digest {
				continue
			}
			if reason := e.unsandboxedReason(); reason != "" {
				reasons = append(reasons, reason)
			}
		}
	} else if j.Spec.Isolation == "unsandboxed" {
		reasons = append(reasons, j.Spec.Reason)
	}
	slices.Sort(reasons)
	return slices.Compact(reasons)
}

func nativeUnsandboxed(j *Job) bool { return len(nativeUnsandboxedReasons(j)) > 0 }

func jobNativeIsolationStatus(j *Job) string {
	if reasons := nativeUnsandboxedReasons(j); len(reasons) > 0 {
		return "unsandboxed: " + strings.Join(reasons, "; ")
	}
	// Observation captures may already have removed the frozen payload.
	return j.Isolation
}

// Reuse the durable attempt store to avoid a warning for every sidecar,
// launch retry or recovery. This observation never authorizes execution.
func (s *workerStore) warnNativeIsolation(j *Job) error {
	status := jobNativeIsolationStatus(j)
	if status == "" || s.state.IsolationWarning != nil && *s.state.IsolationWarning == (Execution{ID: j.ID, Attempt: j.Attempt}) {
		return nil
	}
	previous := s.state.IsolationWarning
	s.state.IsolationWarning = &Execution{ID: j.ID, Attempt: j.Attempt}
	if err := s.save(); err != nil {
		s.state.IsolationWarning = previous
		return err
	}
	log.Printf("WARNING job %s attempt %d %q", j.ID, j.Attempt, status)
	return nil
}

// Bypass only the kernel wrapper. Managed workdir containment remains a launch
// invariant even when the payload has ordinary worker-user host permissions.
func (w Worker) isolateNativeAttempt(j *Job, namespace *nativeNamespace, cmd *exec.Cmd, spool, work string, env map[string]string, ports []string, main bool) error {
	if !nativeUnsandboxed(j) {
		return namespace.isolate(w, cmd, spool, work, env, ports, j.Spec.Needs.Network, main)
	}
	root, err := filepath.EvalSymlinks(work)
	if err != nil {
		return err
	}
	dir, err := filepath.EvalSymlinks(cmd.Dir)
	if err != nil {
		return err
	}
	if rel, err := filepath.Rel(root, dir); err != nil || rel != "." && !filepath.IsLocal(rel) {
		return errors.New("native isolation requires workdir beneath {work}; stage source with inputs or a profile")
	}
	return nil
}
