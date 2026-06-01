package node

import (
	"context"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestAgentHeartbeatUsesInjectedClock(t *testing.T) {
	start := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	clock := mocks.NewFakeClock(start)
	agent := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), clock)

	clock.Advance(2 * time.Second)
	node, err := agent.Heartbeat(context.Background())
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !node.HeartbeatAt.Equal(start.Add(2 * time.Second)) {
		t.Fatalf("heartbeat_at = %s", node.HeartbeatAt)
	}
}

func TestAgentHeartbeatRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewAgent(fixtures.MakeNode(), mocks.NewBackendAdapter(), mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))).Heartbeat(ctx)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestHeartbeatTrackerMarksUnreachableAfterMissedBeats(t *testing.T) {
	start := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	clock := mocks.NewFakeClock(start)
	tracker := NewHeartbeatTracker(clock, HeartbeatPolicy{Interval: time.Second, MaxMisses: 2})
	node := fixtures.MakeNode()
	node.HeartbeatAt = start
	node.Status = domain.NodeReady
	tracker.Observe(node)

	got, err := tracker.Node(node.ID)
	if err != nil {
		t.Fatalf("Node: %v", err)
	}
	if got.Status != domain.NodeReady {
		t.Fatalf("initial status = %s", got.Status)
	}

	clock.Advance(2*time.Second + time.Nanosecond)
	got, err = tracker.Node(node.ID)
	if err != nil {
		t.Fatalf("Node after advance: %v", err)
	}
	if got.Status != domain.NodeUnreachable {
		t.Fatalf("status = %s", got.Status)
	}
}

func TestHeartbeatTrackerPreservesIntentionalNodeStates(t *testing.T) {
	start := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	clock := mocks.NewFakeClock(start)
	tracker := NewHeartbeatTracker(clock, HeartbeatPolicy{Interval: time.Second, MaxMisses: 1})
	for _, status := range []domain.NodeStatus{domain.NodeMaintenance, domain.NodeDraining} {
		node := fixtures.MakeNode(fixtures.WithNodeID(string(status)))
		node.Status = status
		node.HeartbeatAt = start
		tracker.Observe(node)
	}

	clock.Advance(time.Hour)
	fleet := tracker.Fleet()
	if len(fleet) != 2 {
		t.Fatalf("fleet = %+v", fleet)
	}
	for _, node := range fleet {
		if node.Status != domain.NodeMaintenance && node.Status != domain.NodeDraining {
			t.Fatalf("status changed: %+v", node)
		}
	}
}

func TestHeartbeatTrackerUnknownNodeAndDefaultPolicy(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	tracker := NewHeartbeatTracker(clock, HeartbeatPolicy{})
	if _, err := tracker.Node("missing"); err == nil || !strings.Contains(err.Error(), "unknown node") {
		t.Fatalf("unknown err = %v", err)
	}

	node := fixtures.MakeNode()
	node.HeartbeatAt = clock.Now()
	tracker.Observe(node)
	clock.Advance(defaultHeartbeatInterval * defaultHeartbeatMaxMisses)
	got, err := tracker.Node(node.ID)
	if err != nil {
		t.Fatalf("Node: %v", err)
	}
	if got.Status != domain.NodeReady {
		t.Fatalf("boundary should still be ready, got %s", got.Status)
	}
	clock.Advance(time.Nanosecond)
	got, err = tracker.Node(node.ID)
	if err != nil {
		t.Fatalf("Node after miss: %v", err)
	}
	if got.Status != domain.NodeUnreachable {
		t.Fatalf("after miss = %s", got.Status)
	}
}
