package telemetry

import (
	"sort"
	"time"

	"mycelium/internal/domain"
)

func SelectGroupAnalysisNode(nodes []domain.Node, at time.Time, interval time.Duration) (domain.Node, bool) {
	ready := make([]domain.Node, 0, len(nodes))
	for _, node := range nodes {
		if node.ID == "" || node.Status != domain.NodeReady {
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
	slot := at.UTC().UnixNano() / int64(interval)
	if slot < 0 {
		slot = -slot
	}
	return ready[int(slot%int64(len(ready)))], true
}
