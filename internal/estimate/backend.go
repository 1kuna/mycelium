package estimate

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type BackendAware struct {
	LlamaCpp ports.ResourceEstimator
	Explicit ports.ResourceEstimator
}

func NewBackendAware(llamaCpp, explicit ports.ResourceEstimator) *BackendAware {
	if explicit == nil {
		explicit = NewInMemory()
	}
	return &BackendAware{LlamaCpp: llamaCpp, Explicit: explicit}
}

func (e *BackendAware) Estimate(ctx context.Context, p domain.Preset, contextLen, concurrency int) (domain.Claim, error) {
	switch p.Backend {
	case domain.BackendLlamaCpp:
		if e.LlamaCpp != nil {
			return e.LlamaCpp.Estimate(ctx, p, contextLen, concurrency)
		}
		return domain.Claim{}, fmt.Errorf("llamacpp preset %q requires GGUF preflight estimator", p.ID)
	case domain.BackendVLLM:
		return domain.Claim{}, fmt.Errorf("vllm preset %q requires unit-aware reservation estimation", p.ID)
	case domain.BackendMLX, domain.BackendCustom:
		return e.Explicit.Estimate(ctx, p, contextLen, concurrency)
	default:
		return domain.Claim{}, fmt.Errorf("unsupported backend %q for preset %q estimation", p.Backend, p.ID)
	}
}

func (e *BackendAware) EstimateForUnit(ctx context.Context, p domain.Preset, contextLen, concurrency int, node domain.Node, acceleratorSet []int) (domain.Claim, error) {
	switch p.Backend {
	case domain.BackendVLLM:
		return estimateVLLMReservation(ctx, p, node, acceleratorSet)
	case domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendCustom:
		return e.Estimate(ctx, p, contextLen, concurrency)
	default:
		return domain.Claim{}, fmt.Errorf("unsupported backend %q for preset %q estimation", p.Backend, p.ID)
	}
}

func estimateVLLMReservation(ctx context.Context, p domain.Preset, node domain.Node, acceleratorSet []int) (domain.Claim, error) {
	if err := ctx.Err(); err != nil {
		return domain.Claim{}, err
	}
	util, err := gpuMemoryUtilization(p.LaunchArgs)
	if err != nil {
		return domain.Claim{}, fmt.Errorf("vllm preset %q requires --gpu-memory-utilization: %w", p.ID, err)
	}
	total := 0
	for _, idx := range acceleratorSet {
		found := false
		for _, acc := range node.Accelerators {
			if acc.Index == idx {
				found = true
				total += acc.VRAMTotalMB
				break
			}
		}
		if !found {
			return domain.Claim{}, fmt.Errorf("vllm preset %q selected missing accelerator %d on node %q", p.ID, idx, node.ID)
		}
	}
	if total <= 0 {
		return domain.Claim{}, fmt.Errorf("vllm preset %q selected unit has no VRAM on node %q", p.ID, node.ID)
	}
	return domain.Claim{WeightsMB: int(math.Ceil(float64(total) * util))}, nil
}

func gpuMemoryUtilization(args []string) (float64, error) {
	for i, arg := range args {
		if arg == "--gpu-memory-utilization" {
			if i+1 >= len(args) {
				return 0, fmt.Errorf("missing value")
			}
			return parseUtil(args[i+1])
		}
		if strings.HasPrefix(arg, "--gpu-memory-utilization=") {
			return parseUtil(strings.TrimPrefix(arg, "--gpu-memory-utilization="))
		}
	}
	return 0, fmt.Errorf("argument not found")
}

func parseUtil(raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, err
	}
	if value <= 0 || value > 1 {
		return 0, fmt.Errorf("value must be > 0 and <= 1")
	}
	return value, nil
}

var _ ports.ResourceEstimator = (*BackendAware)(nil)
var _ ports.UnitResourceEstimator = (*BackendAware)(nil)
