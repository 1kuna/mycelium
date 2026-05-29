package optimizer

import (
	"errors"
	"strings"
	"testing"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
)

func TestPlanReactiveRequeueSnapsToSharedContext(t *testing.T) {
	job := fixtures.MakeJob(fixtures.WithJobID("job1"))
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset_qwen"), fixtures.WithContextLength(8000))

	plan, err := PlanReactiveRequeue(job, preset, domain.ErrContextOverflow, 9000, ReactivePolicy{
		Buffer:         1.2,
		SharedContexts: []int{6000, 12000, 16000},
	})
	if err != nil {
		t.Fatalf("PlanReactiveRequeue: %v", err)
	}
	if plan.Preset.ContextLength != 12000 || plan.Preset.ID != "preset_qwen_ctx12000" {
		t.Fatalf("preset = %+v", plan.Preset)
	}
	if plan.Job.PresetID != plan.Preset.ID || plan.Job.ContextRequest != 12000 || plan.Job.Status != domain.JobQueued {
		t.Fatalf("job = %+v", plan.Job)
	}
	if !strings.Contains(plan.TraceStep.Result, "next_context=12000") {
		t.Fatalf("trace = %+v", plan.TraceStep)
	}
}

func TestPlanReactiveRequeueUsesDefaultBufferAndRequiredContext(t *testing.T) {
	preset := fixtures.MakePreset()
	preset.Backend = domain.BackendVLLM
	plan, err := PlanReactiveRequeue(fixtures.MakeJob(), preset, errors.New("prompt is too long for maximum context length"), 1000, ReactivePolicy{})
	if err != nil {
		t.Fatalf("PlanReactiveRequeue: %v", err)
	}
	if plan.Preset.ContextLength != 1200 {
		t.Fatalf("context = %d", plan.Preset.ContextLength)
	}
}

func TestPlanReactiveRequeueFailsLoudOnNonOverflowAndBadInputs(t *testing.T) {
	_, err := PlanReactiveRequeue(fixtures.MakeJob(), fixtures.MakePreset(), errors.New("connection refused"), 1000, ReactivePolicy{})
	if err == nil || !strings.Contains(err.Error(), "not context overflow") {
		t.Fatalf("non-overflow err = %v", err)
	}
	_, err = PlanReactiveRequeue(fixtures.MakeJob(), fixtures.MakePreset(), domain.ErrContextOverflow, 0, ReactivePolicy{})
	if err == nil || !strings.Contains(err.Error(), "observed token") {
		t.Fatalf("observed err = %v", err)
	}
	_, err = PlanReactiveRequeue(fixtures.MakeJob(), fixtures.MakePreset(), domain.ErrContextOverflow, 1000, ReactivePolicy{Buffer: 0.5})
	if err == nil || !strings.Contains(err.Error(), "buffer") {
		t.Fatalf("buffer err = %v", err)
	}
}

func TestIsContextOverflowByBackend(t *testing.T) {
	if IsContextOverflow(domain.BackendLlamaCpp, nil) {
		t.Fatal("nil err should not overflow")
	}
	if !IsContextOverflow(domain.BackendLlamaCpp, errors.New("request exceeds context window")) {
		t.Fatal("llama context window should classify")
	}
	if !IsContextOverflow(domain.BackendVLLM, errors.New("prompt is too long")) {
		t.Fatal("vllm prompt too long should classify")
	}
	if !IsContextOverflow(domain.BackendMLX, errors.New("context length exceeded")) {
		t.Fatal("mlx context length should classify")
	}
	if IsContextOverflow(domain.BackendCustom, errors.New("context window")) {
		t.Fatal("unknown backend should not pattern-classify")
	}
}
