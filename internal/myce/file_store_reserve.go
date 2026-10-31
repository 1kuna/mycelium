package myce

import (
	"context"
	"fmt"
	"net/http"
)

type fileStoreSubmissionKey struct{}

// WithFileStoreSubmission supplies pre-submission input attribution. The queue
// resolves the effective tier; shared-token project/class hints are not auth.
func WithFileStoreSubmission(ctx context.Context, s Spec) context.Context {
	return context.WithValue(ctx, fileStoreSubmissionKey{}, Spec{Project: s.Project, Class: jobClass(s)})
}
func fileStoreSubmissionHeaders(ctx context.Context, r *http.Request) {
	if s, ok := ctx.Value(fileStoreSubmissionKey{}).(Spec); ok {
		r.Header.Set("X-Myce-Project", s.Project)
		r.Header.Set("X-Myce-Class", s.Class)
	}
}
func fileStoreSubmissionRequest(r *http.Request) (*http.Request, error) {
	if r.Header.Get("X-Myce-Project") == "" && r.Header.Get("X-Myce-Class") == "" {
		return r, nil
	}
	s := Spec{Project: r.Header.Get("X-Myce-Project"), Class: r.Header.Get("X-Myce-Class")}
	if err := normalizeJobClass(&s); err != nil {
		return nil, err
	}
	return r.WithContext(WithFileStoreSubmission(r.Context(), s)), nil
}
func WithFileStoreProductionReserve(bytes int64) QueueOption {
	return func(q *Queue) { q.fileStoreProductionReserve = bytes }
}

// Call under q.mu, after inventory outside the lock. Existing production output
// commitments back the reserve; only its uncommitted remainder is withheld.
func (q *Queue) fileStoreReserveRefusal(status FileStoreStatus, need, growth, productionReserved uint64, production bool) error {
	if err := fileStoreRefusal(status, need); err != nil {
		return err
	}
	if growth == 0 || production {
		return nil
	}
	reserve := uint64(q.fileStoreProductionReserve)
	uncommitted := reserve - min(reserve, productionReserved)
	available := status.FreeBytes - min(status.FreeBytes, uncommitted)
	if need > available {
		return &HTTPError{Status: http.StatusInsufficientStorage, Code: "file_store_capacity", Message: fmt.Sprintf("queue-file-store: need=%d bytes non_production_free=%d bytes; production reserve=%d bytes (--file-store-production-reserve), available only to production-tier submissions", need, available, reserve)}
	}
	return nil
}
func (q *Queue) admitFileUpload(ctx context.Context, status FileStoreStatus, projected map[string]storedPayload, files FileManifest, need, growth uint64) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	s, scoped := ctx.Value(fileStoreSubmissionKey{}).(Spec)
	production := scoped && q.priorityMatch(s).Tier == "production"
	// Existing workers already declare exact async result identities before tus
	// creation. Recognize those production bytes without a worker upgrade.
	if !scoped && len(files) > 0 {
		production = true
		for _, ref := range files {
			owned := false
			for _, j := range q.liveJobs() {
				if terminal(j.State) || cancelled(j) || j.OutputAdmissionPending || q.priorityMatch(j.Spec).Tier != "production" {
					continue
				}
				for _, pending := range j.PendingOutputRefs {
					if pending == ref {
						owned = true
						break
					}
				}
				if owned {
					break
				}
			}
			if !owned {
				production = false
				break
			}
		}
	}
	return q.fileStoreReserveRefusal(status, need, growth, q.outputReservationsFor(projected, "", true), production)
}
