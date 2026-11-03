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
	// Reuse submission's one charged I/O slot. Resolve is an observation: its
	// wait uses the existing local observation budget, then reports unknown.
	// Cancellation bounds hash reads; a stuck syscall keeps its slot until return.
	ctx, cancel := context.WithTimeout(parent, localObservationBudget)
	defer cancel()
	q.mu.Lock()
	store := q.files
	if q.submitVerificationSlot == nil {
		q.submitVerificationSlot = make(chan struct{}, 1)
	}
	slot := q.submitVerificationSlot
	q.mu.Unlock()
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return
	}
	inputs := slices.Clone(report.FileInputs)
	done := make(chan []ResolveFileInput, 1)
	go func() {
		defer func() { <-slot }()
		for _, index := range verify {
			if ctx.Err() != nil {
				break
			}
			input := &inputs[index]
			if store == nil {
				input.State = "needs-staging"
				continue
			}
			err := store.verify(ctx, input.Ref) // read-only; never the manifest publication fsync
			if ctx.Err() != nil {
				break
			}
			if err == nil {
				input.State = "ready"
			} else {
				input.State = "needs-staging"
			}
		}
		done <- inputs
	}()
	select {
	case report.FileInputs = <-done:
	case <-ctx.Done():
		return
	}
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
