package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// A preparation observation can expire while its uncancellable filesystem
// writer still runs. Join that owner before repeating preflight or discovery:
// probing during our own bulk copy can refuse the attempt and abandon the copy.
// Do not consume its result here; the original preparation call consumes it.
func (s *workerStore) waitCheckpointPreparation(ctx context.Context) error {
	if s == nil {
		return nil
	}
	n := &s.nasPrepare
	n.mu.Lock()
	pending := n.pending
	n.mu.Unlock()
	if pending == nil {
		return nil
	}
	budget := n.timeout
	if budget <= 0 {
		budget = checkpointPreparationBudget
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	select {
	case <-pending.done:
		return ctx.Err()
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%w: %v (checkpoint copy pending)", errWorkerCheckpoint, errCheckpointIOPending)
		}
		return ctx.Err()
	}
}

func (w Worker) startAssignedAttempt(ctx context.Context, s *workerStore, j Job) (*process, error) {
	if err := s.waitCheckpointPreparation(ctx); err != nil {
		return nil, err
	}
	w.launchCtx = ctx
	bound, err := w.forAttempt(j)
	if err != nil {
		return nil, &executionEvidenceError{s.startFailure(j, launchRefused, err), safeExecutionEvidence(s.startExecutionEvidence(j), j.Attempt)}
	}
	return bound.startContext(ctx, s, j)
}

// Only a positive exact-attempt no-execution receipt authorizes removing a
// refused copy. Unknown/restored execution and started payloads keep ordinary
// settled-job retention. The source lives in an earlier attempt, never here.
func (w Worker) cleanupRefusedCheckpoint(ctx context.Context, s *workerStore) error {
	j, r := s.state.Job, s.state.Result
	if j == nil || j.Spec.ephemeral() || r == nil || r.ReleasePending || r.ID != j.ID || r.Attempt != j.Attempt || r.Session != j.Session {
		return nil
	}
	e := safeExecutionEvidence(r.ExecutionEvidence, j.Attempt)
	if e == nil || e.Main != executionNotStarted || e.Sidecars != executionNotStarted {
		return nil
	}
	if s.nasPrepare.inFlight() {
		return errCheckpointIOPending
	}
	if !filepath.IsAbs(j.Checkpoint) || j.Attempt < 1 {
		return errors.New("invalid refused checkpoint path")
	}
	_, err := s.nasCleanup.runBounded(ctx, attemptCheckpoint(*j), func() (bool, error) {
		info, err := os.Lstat(j.Checkpoint)
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if !info.IsDir() {
			return false, errors.New("unexpected refused checkpoint parent")
		}
		remove := w.checkpointRemove
		if remove == nil {
			remove = os.RemoveAll
		}
		return true, remove(attemptCheckpoint(*j))
	}, false)
	return err
}
