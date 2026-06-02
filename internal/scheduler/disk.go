package scheduler

import (
	"math"

	"mycelium/internal/domain"
)

func nodeDiskDropReason(preset domain.Preset, node domain.Node, fleet domain.FleetSnapshot) (string, bool) {
	if node.DiskTotalMB <= 0 {
		return "disk.unknown", true
	}
	minFreeRatio := node.DiskMinFreeRatio
	if minFreeRatio == 0 {
		minFreeRatio = domain.DefaultDiskMinFreeRatio
	}
	if minFreeRatio <= 0 || minFreeRatio >= 1 {
		return "disk.limit", true
	}
	floorMB := int(math.Ceil(float64(node.DiskTotalMB) * minFreeRatio))
	if node.DiskFreeMB <= floorMB {
		return "disk.free", true
	}
	requiredMB := artifactRequiredForNode(preset, node, fleet)
	if requiredMB < 0 {
		return "disk.required", true
	}
	if node.DiskFreeMB-requiredMB <= floorMB {
		return "disk.free_after_model", true
	}
	return "", false
}

func artifactRequiredForNode(preset domain.Preset, node domain.Node, fleet domain.FleetSnapshot) int {
	if modelAlreadyPresentOnNode(preset, node, fleet) {
		return 0
	}
	if preset.ArtifactSizeMB > 0 {
		return preset.ArtifactSizeMB
	}
	return preset.EstWeightsMB
}

func modelAlreadyPresentOnNode(preset domain.Preset, node domain.Node, fleet domain.FleetSnapshot) bool {
	if preset.NodeID != "" && preset.NodeID == node.ID {
		return true
	}
	for _, inst := range fleet.Instances {
		if inst.NodeID == node.ID && inst.PresetID == preset.ID {
			return true
		}
	}
	return false
}
