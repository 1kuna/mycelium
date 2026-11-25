package myce

import (
	"context"
	"database/sql"
	"errors"
	"maps"
)

// Reference publication and the pending/claimed transition use the same
// ordered queue writer. A successor can cancel an unclaimed intent as part
// of publishing a keeper. Once sealed, keepers must wait for the exact
// unlink actor to finish, even if a newer coordinator is already serving.
func (q *Queue) reconcileSharedBlobReferences() error {
	if q.sharedWriter == nil || len(q.s.SharedBlobReclaims) == 0 {
		return nil
	}
	blocked := false
	q.visitFileReferences(false, nil, func(ref FileRef, flags fileReferenceFlags, _ FileKeepingReference) {
		if claim, exists := q.s.SharedBlobReclaims[ref.SHA256]; exists && flags&fileKept != 0 {
			if claim == "" {
				delete(q.s.SharedBlobReclaims, ref.SHA256)
			} else {
				blocked = true
			}
		}
	})
	if blocked {
		return queueStopping()
	}
	return nil
}

// The caller holds the process-local lifecycle fence and has just checked
// the file age outside q.mu. No path can be unlinked until both writes commit.
func (q *Queue) claimSharedBlobUnlink(ctx context.Context, hash string, revision uint64) (string, error) {
	eligible := false
	err := q.updateRequest(ctx, func() error {
		if _, exists := q.s.SharedBlobReclaims[hash]; !exists && q.fileReferenceRevision == revision {
			if q.s.SharedBlobReclaims == nil {
				q.s.SharedBlobReclaims = map[string]string{}
			}
			q.s.SharedBlobReclaims[hash] = ""
		}
		claim, exists := q.s.SharedBlobReclaims[hash]
		eligible = exists && claim == ""
		return nil
	}, nil)
	if err != nil || !eligible {
		return "", err
	}
	claim := ID()
	claimed := false
	err = q.updateRequest(ctx, func() error {
		if previous, exists := q.s.SharedBlobReclaims[hash]; exists && previous == "" {
			q.s.SharedBlobReclaims[hash] = claim
			claimed = true
		}
		return nil
	}, nil)
	if err != nil && claimed {
		// An ambiguous seal can have committed even though this call will
		// never unlink. The exact actor can safely release that claim.
		err = errors.Join(err, q.finishSharedBlobUnlink(ctx, claim))
	}
	if err != nil || !claimed {
		return "", err
	}
	return claim, nil
}

func (q *Queue) finishSharedBlobUnlink(ctx context.Context, claim string) error {
	w := q.sharedWriter
	call, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.timeout)
	defer cancel()
	_, err := w.authority.db.ExecContext(call, `INSERT INTO myce_reclaim_finished_v1(claim)
		VALUES($1) ON CONFLICT DO NOTHING`, claim)
	return err
}

func (q *Queue) clearFinishedSharedReclaims(ctx context.Context) error {
	w := q.sharedWriter
	if w == nil {
		return nil
	}
	q.mu.RLock()
	claims := maps.Clone(q.s.SharedBlobReclaims)
	q.mu.RUnlock()
	finished := make(map[string]string)
	for hash, claim := range claims {
		if claim == "" {
			continue
		}
		call, cancel := context.WithTimeout(ctx, w.timeout)
		var done string
		err := w.authority.db.QueryRowContext(call,
			"SELECT claim FROM myce_reclaim_finished_v1 WHERE claim=$1", claim).Scan(&done)
		cancel()
		if err == nil {
			finished[hash] = claim
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if len(finished) == 0 {
		return nil
	}
	return q.updateRequest(ctx, func() error {
		for hash, claim := range finished {
			if q.s.SharedBlobReclaims[hash] == claim {
				delete(q.s.SharedBlobReclaims, hash)
			}
		}
		return nil
	}, nil)
}
