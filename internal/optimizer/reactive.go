package optimizer

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"mycelium/internal/domain"
)

type ReactivePolicy struct {
	Buffer         float64
	SharedContexts []int
}

type RequeuePlan struct {
	Job       domain.Job
	Preset    domain.Preset
	TraceStep domain.TraceStep
}

func PlanReactiveRequeue(job domain.Job, preset domain.Preset, backendErr error, observedTokens int, policy ReactivePolicy) (RequeuePlan, error) {
	if !IsContextOverflow(preset.Backend, backendErr) {
		return RequeuePlan{}, fmt.Errorf("backend error is not context overflow: %w", backendErr)
	}
	if observedTokens <= 0 {
		return RequeuePlan{}, fmt.Errorf("observed token count must be positive: %d", observedTokens)
	}
	buffer := policy.Buffer
	if buffer == 0 {
		buffer = 1.20
	}
	if buffer < 1 {
		return RequeuePlan{}, fmt.Errorf("overflow buffer must be >= 1: %f", buffer)
	}

	required := int(math.Ceil(float64(observedTokens) * buffer))
	next := snapContext(required, policy.SharedContexts)
	newPreset := preset
	newPreset.ContextLength = next
	newPreset.ID = fmt.Sprintf("%s_ctx%d", preset.ID, next)
	newJob := job
	newJob.PresetID = newPreset.ID
	newJob.ContextRequest = next
	newJob.Status = domain.JobQueued
	return RequeuePlan{
		Job:    newJob,
		Preset: newPreset,
		TraceStep: domain.TraceStep{
			Step:   "reactive_requeue",
			Result: fmt.Sprintf("overflow observed=%d required=%d next_context=%d", observedTokens, required, next),
		},
	}, nil
}

func IsContextOverflow(backend domain.Backend, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, domain.ErrContextOverflow) {
		return true
	}
	message := strings.ToLower(err.Error())
	for _, pattern := range overflowPatterns(backend) {
		if strings.Contains(message, pattern) {
			return true
		}
	}
	return false
}

func overflowPatterns(backend domain.Backend) []string {
	switch backend {
	case domain.BackendLlamaCpp:
		return []string{"context overflow", "exceeds context", "too many tokens", "context window"}
	case domain.BackendVLLM:
		return []string{"maximum context length", "prompt is too long", "sequence length"}
	case domain.BackendMLX:
		return []string{"context length", "too many tokens"}
	default:
		return nil
	}
}

func snapContext(required int, shared []int) int {
	candidates := append([]int(nil), shared...)
	sort.Ints(candidates)
	for _, candidate := range candidates {
		if candidate >= required {
			return candidate
		}
	}
	return required
}
