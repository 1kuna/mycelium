package telemetry

import (
	"testing"
	"time"

	"mycelium/internal/domain"
)

func TestSelectGroupAnalysisNodeRotatesReadyComputeNodes(t *testing.T) {
	nodes := []domain.Node{
		{ID: "node-c", Status: domain.NodeReady},
		{ID: "node-a", Status: domain.NodeReady},
		{ID: "node-b", Status: domain.NodeMaintenance},
	}
	start := time.Unix(0, 0).UTC()

	first, ok := SelectGroupAnalysisNode(nodes, start, time.Minute)
	if !ok || first.ID != "node-a" {
		t.Fatalf("first = %+v ok=%v", first, ok)
	}
	second, ok := SelectGroupAnalysisNode(nodes, start.Add(time.Minute), time.Minute)
	if !ok || second.ID != "node-c" {
		t.Fatalf("second = %+v ok=%v", second, ok)
	}
	third, ok := SelectGroupAnalysisNode(nodes, start.Add(2*time.Minute), time.Minute)
	if !ok || third.ID != "node-a" {
		t.Fatalf("third = %+v ok=%v", third, ok)
	}
}

func TestSelectGroupAnalysisNodeRejectsNoReadyNodes(t *testing.T) {
	if node, ok := SelectGroupAnalysisNode([]domain.Node{{ID: "node-a", Status: domain.NodeUnreachable}}, time.Now(), time.Second); ok || node.ID != "" {
		t.Fatalf("node = %+v ok=%v", node, ok)
	}
	if node, ok := SelectGroupAnalysisNode([]domain.Node{{Status: domain.NodeReady}}, time.Now(), 0); ok || node.ID != "" {
		t.Fatalf("missing id node = %+v ok=%v", node, ok)
	}
}
