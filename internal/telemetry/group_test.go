package telemetry

import (
	"testing"
	"time"

	"mycelium/internal/domain"
)

func TestSelectGroupAnalysisNodeRotatesReadyComputeNodes(t *testing.T) {
	nodes := []domain.Node{
		computeNode("node-c", domain.NodeReady),
		computeNode("node-a", domain.NodeReady),
		computeNode("node-b", domain.NodeMaintenance),
		{ID: "thin-peer", Status: domain.NodeReady},
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
	if slot := AnalysisSlotID(start.Add(2*time.Minute), time.Minute); slot != "optimizer-slot-2" {
		t.Fatalf("slot id = %s", slot)
	}
}

func TestSelectGroupAnalysisNodeRejectsNoReadyNodes(t *testing.T) {
	if node, ok := SelectGroupAnalysisNode([]domain.Node{computeNode("node-a", domain.NodeUnreachable)}, time.Now(), time.Second); ok || node.ID != "" {
		t.Fatalf("node = %+v ok=%v", node, ok)
	}
	if node, ok := SelectGroupAnalysisNode([]domain.Node{{Status: domain.NodeReady}}, time.Now(), 0); ok || node.ID != "" {
		t.Fatalf("missing id node = %+v ok=%v", node, ok)
	}
	if node, ok := SelectGroupAnalysisNode([]domain.Node{{ID: "thin-peer", Status: domain.NodeReady}}, time.Now(), 0); ok || node.ID != "" {
		t.Fatalf("thin node = %+v ok=%v", node, ok)
	}
}

func computeNode(id string, status domain.NodeStatus) domain.Node {
	return domain.Node{ID: id, Status: status, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 1024}}}
}
