package myce

import (
	"context"
	"maps"
)

// Reference publication and sealing use the same ordered queue writer. A
// sealed digest waits for the exact unlink actor, including across handover.
func (q *Queue) reconcileSharedBlobReferences() error {
	if q.sharedWriter == nil || len(q.s.SharedBlobReclaims) == 0 {
		return nil
	}
	check := func(ref FileRef, flags fileReferenceFlags, _ FileKeepingReference) {
		if claim, exists := q.s.SharedBlobReclaims[ref.SHA256]; exists && claim == "" && flags&fileKept != 0 {
			delete(q.s.SharedBlobReclaims, ref.SHA256)
		}
	}
	for _, claim := range q.s.SharedBlobReclaims {
		if claim != "" {
			blocked := false
			q.visitFileReferences(false, nil, func(ref FileRef, flags fileReferenceFlags, owner FileKeepingReference) {
				if q.s.SharedBlobReclaims[ref.SHA256] != "" && flags&fileKept != 0 {
					blocked = true
				}
				check(ref, flags, owner)
			})
			if blocked {
				return queueStopping()
			}
			return nil
		}
	}
	// Older proofs could leave unsealed intents. They authorize no unlink.
	// Ordinary writes need only look at changed jobs and catalog references
	// to cancel them. The collector also discards leftovers.
	q.visitDurableFileReferences(false, nil, check, q.touchedJobs())
	if q.records.editingInputClaims {
		q.visitFileInputClaimKeepers(func(claim FileInputClaim, owner FileKeepingReference) {
			check(claim.File, fileKept, owner)
		})
	}
	return nil
}

func (q *Queue) claimSharedBlobUnlink(ctx context.Context, hash string, revision uint64) (string, error) {
	claims, err := q.claimSharedBlobUnlinks(ctx, []string{hash}, revision)
	return claims[hash], err
}

// Holding the local lifecycle fence and the observed revision through the
// writer decision lets the batch seal directly. A takeover before this commit
// prevents every unlink; a takeover afterwards observes all the sealed claims.
func (q *Queue) claimSharedBlobUnlinks(ctx context.Context, hashes []string, revision uint64) (map[string]string, error) {
	claims := make(map[string]string, len(hashes))
	err := q.updateRequest(ctx, func() error {
		if q.fileReferenceRevision != revision {
			return nil
		}
		if q.s.SharedBlobReclaims == nil {
			q.s.SharedBlobReclaims = map[string]string{}
		}
		for _, hash := range hashes {
			if q.s.SharedBlobReclaims[hash] == "" && claims[hash] == "" {
				claim := ID()
				q.s.SharedBlobReclaims[hash] = claim
				claims[hash] = claim
			}
		}
		return nil
	}, nil)
	if err != nil {
		// A seal may have committed despite the error. No unlink will run,
		// so any of these exact claims can now report completion.
		q.sharedWriter.authority.recordFinishedReclaims(maps.Values(claims))
		return nil, err
	}
	return claims, nil
}

func (q *Queue) finishSharedBlobUnlink(ctx context.Context, claim string) error {
	w := q.sharedWriter
	w.authority.recordFinishedReclaims(func(yield func(string) bool) { yield(claim) })
	return w.flushFinishedReclaims(ctx)
}

func (w *sharedQueueWriter) flushFinishedReclaims(ctx context.Context) error {
	call, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.timeout)
	defer cancel()
	return w.authority.flushFinishedReclaims(call)
}

func (q *Queue) clearFinishedSharedReclaims(ctx context.Context) error {
	w := q.sharedWriter
	if w == nil {
		return nil
	}
	if err := w.flushFinishedReclaims(ctx); err != nil {
		return err
	}
	q.mu.RLock()
	claims := maps.Clone(q.s.SharedBlobReclaims)
	q.mu.RUnlock()
	if len(claims) == 0 {
		return nil
	}
	var sealed []string
	unsealed := false
	for _, claim := range claims {
		if claim == "" {
			unsealed = true
		} else {
			sealed = append(sealed, claim)
		}
	}
	done := make(map[string]bool, len(sealed))
	if len(sealed) != 0 {
		call, cancel := context.WithTimeout(ctx, w.timeout)
		rows, err := w.authority.db.QueryContext(call, "SELECT claim FROM myce_reclaim_finished_v1 WHERE claim=ANY($1)", sealed)
		if err != nil {
			cancel()
			return err
		}
		for rows.Next() {
			var claim string
			if err := rows.Scan(&claim); err != nil {
				rows.Close()
				cancel()
				return err
			}
			done[claim] = true
		}
		err = rows.Err()
		rows.Close()
		cancel()
		if err != nil {
			return err
		}
	}
	if !unsealed && len(done) == 0 {
		return nil
	}
	return q.updateRequest(ctx, func() error {
		for hash, observed := range claims {
			if current, exists := q.s.SharedBlobReclaims[hash]; exists && current == observed && (current == "" || done[current]) {
				delete(q.s.SharedBlobReclaims, hash)
			}
		}
		return nil
	}, nil)
}
