package myce

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

func warmCheckpointPath(j Job, key string) string {
	return filepath.Join(attemptCheckpoint(j), key)
}

// The marker lives with worker-owned publication metadata. Only the worker
// writes it, and the queue rechecks the named delivery after verification.
func warmCheckpointDelivery(j Job, state string) (key string, sequence int, err error) {
	data, err := os.ReadFile(filepath.Join(attemptPublicationRoot(j, state), "warm-checkpoint.json"))
	if errors.Is(err, os.ErrNotExist) {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, err
	}
	if json.Unmarshal(data, &sequence) != nil || sequence < 1 || sequence > len(j.Spec.FanOut.Pieces) {
		return "", 0, errors.New("invalid saved warm checkpoint delivery")
	}
	return j.Spec.FanOut.Pieces[sequence-1].Key, sequence, nil
}

func (q *Queue) warmCheckpointAllowed(j *Job, p checkpointPublication) bool {
	if !j.Spec.warmPieces() {
		return p.Piece == "" && p.Sequence == 0
	}
	r := warmPieceRequest{Execution: p.Execution, Machine: p.Machine, Session: p.Session, Key: p.Piece, Sequence: p.Sequence}
	current, err := q.warmPieceJob(r)
	return err == nil && !current.ReleaseConfirmed && (current.State == "running" || current.State == "stopping") &&
		warmPieceMatches(current, r) && current.FanOut.Warm.Delivery.StartedAt != nil
}
