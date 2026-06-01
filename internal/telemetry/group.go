package telemetry

import (
	"fmt"
	"sort"
	"time"

	"mycelium/internal/domain"
)

func SelectGroupAnalysisNode(nodes []domain.Node, at time.Time, interval time.Duration) (domain.Node, bool) {
	ready := make([]domain.Node, 0, len(nodes))
	for _, node := range nodes {
		if node.ID == "" || node.Status != domain.NodeReady || !nodeCanCompute(node) {
			continue
		}
		ready = append(ready, node)
	}
	if len(ready) == 0 {
		return domain.Node{}, false
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].ID < ready[j].ID })
	if interval <= 0 {
		interval = time.Minute
	}
	slot := groupSlot(at, interval)
	return ready[int(slot%int64(len(ready)))], true
}

func AnalysisSlotID(at time.Time, interval time.Duration) string {
	if interval <= 0 {
		interval = time.Minute
	}
	return fmt.Sprintf("optimizer-slot-%d", groupSlot(at, interval))
}

func groupSlot(at time.Time, interval time.Duration) int64 {
	slot := at.UTC().UnixNano() / int64(interval)
	if slot < 0 {
		slot = -slot
	}
	return slot
}

func nodeCanCompute(node domain.Node) bool {
	if node.UnifiedMemory {
		return true
	}
	for _, accelerator := range node.Accelerators {
		if accelerator.VRAMTotalMB > 0 || accelerator.UnifiedMemory {
			return true
		}
	}
	return false
}
