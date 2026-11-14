package myce

import (
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"
	"time"
)

// CheckpointStorageEvidence is captured at command exit, before cleanup or
// publication can outlive the bounded host-health history. No paths or OS error
// strings cross this boundary.
type CheckpointStorageEvidence struct {
	At                  time.Time `json:"at"`
	Reason              string    `json:"reason"`
	OperationAgeSeconds float64   `json:"operation_age_seconds,omitempty"`
	Operation           string    `json:"operation,omitempty"`
	Errno               int       `json:"errno,omitempty"`
}

func checkpointProbeFailure(err error, at time.Time) *CheckpointStorageEvidence {
	e := &CheckpointStorageEvidence{At: at, Reason: "mount_probe_failed"}
	var path *os.PathError
	var number syscall.Errno
	if errors.As(err, &path) {
		e.Operation = path.Op
		if errors.As(path.Err, &number) {
			e.Errno = int(number)
		}
	}
	return safeCheckpointStorage(e)
}

func safeCheckpointStorage(e *CheckpointStorageEvidence) *CheckpointStorageEvidence {
	if e == nil || e.At.IsZero() || (e.Reason != "mount_probe_stalled" && e.Reason != "mount_probe_failed") {
		return nil
	}
	out := *e
	if math.IsNaN(out.OperationAgeSeconds) || math.IsInf(out.OperationAgeSeconds, 0) || out.OperationAgeSeconds < 0 {
		out.OperationAgeSeconds = 0
	}
	switch out.Operation {
	case "stat", "lstat", "open", "read", "write", "close", "sync", "rename", "remove", "mkdir", "readdir", "readdirent":
	default:
		out.Operation = ""
	}
	if out.Operation == "" || out.Errno < 0 || out.Errno > 4095 {
		out.Errno = 0
	}
	return &out
}

func (e CheckpointStorageEvidence) String() string {
	text := fmt.Sprintf("checkpoint root %s at %s", e.Reason, e.At.UTC().Format(time.RFC3339Nano))
	if e.OperationAgeSeconds > 0 {
		text += fmt.Sprintf("; no operation progress for %.3fs", e.OperationAgeSeconds)
	}
	if e.Operation != "" {
		text += fmt.Sprintf("; operation=%s errno=%d", e.Operation, e.Errno)
	}
	return text
}

// Read only evidence already owned by the health sampler. Never initiate or
// wait for filesystem I/O on the exit path. The latest sample must overlap the
// command, and a healthy sample supersedes an earlier fault. Pending-operation
// progress can establish a stall even before the next periodic health sample.
func (w Worker) checkpointFailure(checkpoint string, started, exited time.Time) *CheckpointStorageEvidence {
	m := w.storageHealth
	if m == nil || w.CheckpointRoot == "" || !withinNAS(w.CheckpointRoot, checkpoint) || exited.Before(started) {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var evidence *CheckpointStorageEvidence
	for i := len(m.health.History) - 1; i >= 0; i-- {
		s := m.health.History[i]
		if s.At.After(exited) {
			continue
		}
		if s.At.Before(started) || exited.Sub(s.At) > m.health.Probe.freshness() {
			break
		}
		for _, mount := range s.Mounts {
			if mount.Root == "checkpoint" {
				evidence = safeCheckpointStorage(&CheckpointStorageEvidence{At: s.At, Reason: mount.Reason, OperationAgeSeconds: mount.OperationAgeSeconds})
				break
			}
		}
		break
	}
	if p := m.mounts["checkpoint"]; p != nil {
		if evidence != nil && evidence.Reason == "mount_probe_failed" && p.failure != nil && !p.failure.At.After(exited) {
			evidence.Operation, evidence.Errno = p.failure.Operation, p.failure.Errno
		}
		if p.pending != nil && len(p.pending) == 0 && !p.started.IsZero() {
			progressed := p.started
			if at := p.progressed.Load(); at != nil {
				progressed = *at
			}
			if age := exited.Sub(progressed); age >= checkpointAvailabilityStallBudget {
				evidence = &CheckpointStorageEvidence{At: exited, Reason: "mount_probe_stalled", OperationAgeSeconds: age.Seconds()}
			}
		}
	}
	root := m.storageRoots["checkpoint"]
	if state := m.storageBackings[root.backing]; state != nil && state.fault.Active && !state.fault.At.Before(started) && !state.fault.At.After(exited) {
		f := state.fault
		evidence = safeCheckpointStorage(&CheckpointStorageEvidence{At: f.At, Reason: "mount_probe_failed", Operation: f.Operation, Errno: f.Errno})
	}
	return evidence
}

func checkpointStorageInterruption(c *RuntimeCause) bool {
	return c != nil && c.Operation == "checkpoint" && c.Class == "storage_interrupted" && safeCheckpointStorage(c.CheckpointStorage) != nil
}
