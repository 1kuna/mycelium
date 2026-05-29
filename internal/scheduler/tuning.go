package scheduler

import (
	"fmt"
	"strconv"
	"strings"

	"mycelium/internal/domain"
)

func tuneLaunchForPlacement(preset domain.Preset, decision domain.PlacementDecision, node domain.Node) (domain.Preset, error) {
	if preset.Backend != domain.BackendLlamaCpp {
		return preset, nil
	}
	args := append([]string(nil), preset.LaunchArgs...)
	if len(decision.AcceleratorSet) > 0 && !hasLaunchArg(args, "--n-gpu-layers", "-ngl") {
		args = append(args, "--n-gpu-layers", "999")
	}
	if len(decision.AcceleratorSet) > 1 && !hasLaunchArg(args, "--tensor-split", "-ts") {
		split, err := tensorSplit(node, decision.AcceleratorSet)
		if err != nil {
			return domain.Preset{}, err
		}
		args = append(args, "--tensor-split", split)
	}
	preset.LaunchArgs = args
	return preset, nil
}

func hasLaunchArg(args []string, names ...string) bool {
	for _, arg := range args {
		for _, name := range names {
			if arg == name || strings.HasPrefix(arg, name+"=") {
				return true
			}
		}
	}
	return false
}

func tensorSplit(node domain.Node, acceleratorSet []int) (string, error) {
	parts := make([]string, 0, len(acceleratorSet))
	for _, index := range acceleratorSet {
		acc, ok := acceleratorByIndex(node, index)
		if !ok {
			return "", fmt.Errorf("selected accelerator %d is missing from node %q", index, node.ID)
		}
		if acc.VRAMTotalMB <= 0 {
			return "", fmt.Errorf("selected accelerator %d on node %q has invalid vram_total_mb %d", index, node.ID, acc.VRAMTotalMB)
		}
		parts = append(parts, strconv.Itoa(acc.VRAMTotalMB))
	}
	return strings.Join(parts, ","), nil
}

func acceleratorByIndex(node domain.Node, index int) (domain.Accelerator, bool) {
	for _, acc := range node.Accelerators {
		if acc.Index == index {
			return acc, true
		}
	}
	return domain.Accelerator{}, false
}
