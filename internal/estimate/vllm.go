package estimate

import (
	"context"
	"fmt"
	"math"

	"mycelium/internal/backends/vllmargs"
	"mycelium/internal/domain"
)

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
	// vLLM reserves one backend-managed memory pool; Claim has no separate
	// reservation field, so capacity math carries that pool in WeightsMB.
	return domain.Claim{WeightsMB: int(math.Ceil(float64(total) * util))}, nil
}

func gpuMemoryUtilization(args []string) (float64, error) {
	value, ok, err := vllmargs.GPUMemoryUtilization(args)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("argument not found")
	}
	return value, nil
}
