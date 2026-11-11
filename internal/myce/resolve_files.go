package myce

import (
	"context"
	"slices"
	"strings"
)

// ResolveFileInput separates placement eligibility from immutable input bytes.
// A verified sample is still advisory: submission checks the bytes again.
type ResolveFileInput struct {
	Field string  `json:"field"`
	Ref   FileRef `json:"ref"`
	State string  `json:"state"` // ready, needs-staging, unknown
}

func (q *Queue) resolveFileInputs(parent context.Context, req ResolveRequest, report *ResolveReport) {
	manifest := submissionFiles(req.Spec)
	if len(manifest) == 0 {
		return
	}
	report.FileInputs = make([]ResolveFileInput, 0, len(manifest))
	var verify []int
	for _, name := range sortedPortabilityKeys(manifest) {
		field := "files." + strings.TrimPrefix(name, "files/")
		if strings.HasPrefix(name, "shared/") {
			field = "shared_inputs." + strings.TrimPrefix(name, "shared/")
		}
		input := ResolveFileInput{Field: field, Ref: manifest[name], State: "unknown"}
		if slices.Contains(req.UnstagedInputs, name) {
			input.State = "needs-staging"
		} else {
			verify = append(verify, len(report.FileInputs))
		}
		report.FileInputs = append(report.FileInputs, input)
	}
	if len(verify) == 0 {
		return
	}
	q.mu.Lock()
	store := q.files
	if q.submitVerificationSlot == nil {
		q.submitVerificationSlot = make(chan struct{}, 1)
	}
	slot := q.submitVerificationSlot
	q.mu.Unlock()
	var check func(context.Context, FileRef) error
	if store != nil {
		check = store.verify // read-only; never the manifest publication fsync
	}
	resolveFileChecks(parent, report.FileInputs, verify, check, slot)
	for _, input := range report.FileInputs {
		if input.State == "needs-staging" && !slices.Contains(req.UnstagedInputs, resolveFileName(input.Field)) {
			report.Findings = append(report.Findings, ResolveFinding{Field: input.Field, Side: "queue", Problem: "declared file-store bytes are unavailable or do not match their SHA-256 and size", Fix: "Upload the exact bytes with myce file put, or correct the immutable hash and size; submit revalidates."})
		}
	}
}
func resolveFileName(field string) string {
	if name, ok := strings.CutPrefix(field, "shared_inputs."); ok {
		return "shared/" + name
	}
	return "files/" + strings.TrimPrefix(field, "files.")
}

// Resolve follows its caller's lifetime instead of a hash-size-blind deadline:
// under submission contention a fixed budget expired before any input was
// checked (#963). Each input takes submission's one charged I/O slot on its own,
// so a waiting submission interleaves after at most one resolve hash. Hashing
// stays outside q.mu. Completed checks survive caller cancellation; unfinished
// ones stay unknown. A syscall that ignores cancellation keeps its slot until
// it actually returns, and only this caller writes inputs.
func resolveFileChecks(ctx context.Context, inputs []ResolveFileInput, indexes []int, check func(context.Context, FileRef) error, slot chan struct{}) {
	if check == nil {
		for _, index := range indexes {
			inputs[index].State = "needs-staging"
		}
		return
	}
	for _, index := range indexes {
		select {
		case slot <- struct{}{}:
		case <-ctx.Done():
			return
		}
		ref := inputs[index].Ref
		done := make(chan error, 1)
		go func() {
			defer func() { <-slot }()
			done <- check(ctx, ref)
		}()
		select {
		case err := <-done:
			if ctx.Err() != nil {
				return // A cancelled read is not evidence about the bytes.
			}
			inputs[index].State = "ready"
			if err != nil {
				inputs[index].State = "needs-staging"
			}
		case <-ctx.Done():
			return
		}
	}
}
