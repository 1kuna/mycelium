package scheduler

import (
	"sort"

	"mycelium/internal/domain"
)

type candidate struct {
	node domain.Node
	acc  []int
}

func (c candidate) name() string {
	return c.node.ID
}

func (p *Placer) filterCandidates(fleet domain.FleetSnapshot, claim domain.Claim) ([]candidate, domain.TraceStep) {
	var candidates []candidate
	dropped := map[string]string{}
	for _, node := range fleet.Nodes {
		if node.Status != domain.NodeReady {
			dropped[node.ID] = string(node.Status)
			continue
		}
		nodeFit := false
		for _, acc := range node.Accelerators {
			accSet := []int{acc.Index}
			if !p.allocator.CanStackLoad(node, accSet, fleet.Instances) {
				dropped[node.ID] = "load_in_flight"
				continue
			}
			if p.allocator.Fits(node, accSet, fleet.Instances, claim) {
				candidates = append(candidates, candidate{node: node, acc: accSet})
				nodeFit = true
			}
		}
		if !nodeFit {
			if _, exists := dropped[node.ID]; !exists {
				dropped[node.ID] = "fit"
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].node.ID != candidates[j].node.ID {
			return candidates[i].node.ID < candidates[j].node.ID
		}
		return candidates[i].acc[0] < candidates[j].acc[0]
	})
	return candidates, domain.TraceStep{
		Step: "filter",
		Data: map[string]any{"kept": candidateNames(candidates), "dropped": dropped},
	}
}

func candidateNames(candidates []candidate) []string {
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.name())
	}
	return out
}
