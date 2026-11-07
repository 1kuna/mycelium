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
	State string  `json:"state"` // ready, needs-staging, verifying
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
		input := ResolveFileInput{Field: field, Ref: manifest[name], State: "verifying"}
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
		check = store.verify
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

// Resolve follows the caller's lifetime rather than imposing a hash-size-blind
// observation deadline. The shared submission slot bounds outstanding syscalls;
// cancellation returns without releasing a syscall that has not actually exited.
// Only the caller mutates inputs, preserving each completed result on cancellation.
func resolveFileChecks(ctx context.Context, inputs []ResolveFileInput, indexes []int, check func(context.Context, FileRef) error, slot chan struct{}) {
	if check == nil {
		for _, index := range indexes {
			inputs[index].State = "needs-staging"
		}
		return
	}
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return
	}
	type result struct {
		index int
		state string
	}
	// One message per input lets a completed check survive caller cancellation
	// without a background goroutine retaining or modifying the response slice.
	results := make(chan result, len(indexes))
	refs := make([]FileRef, len(indexes))
	for i, index := range indexes {
		refs[i] = inputs[index].Ref
	}
	go func() {
		defer func() { <-slot }()
		defer close(results)
		for i, index := range indexes {
			if ctx.Err() != nil {
				return
			}
			err := check(ctx, refs[i])
			state := "ready"
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				state = "needs-staging"
			}
			results <- result{index, state}
		}
	}()
	for {
		select {
		case r, ok := <-results:
			if !ok {
				return
			}
			inputs[r.index].State = r.state
		case <-ctx.Done():
			for {
				select {
				case r, ok := <-results:
					if !ok {
						return
					}
					inputs[r.index].State = r.state
				default:
					return
				}
			}
		}
	}
}
