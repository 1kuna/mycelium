package scheduler

import (
	"testing"
	"time"

	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestQueueOrdersByPriorityThenFIFO(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	queue := NewQueue(clock)
	queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("normal-1")))
	queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("interactive"), fixtures.Interactive))
	queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("normal-2")))

	got, ok := queue.Dequeue()
	if !ok || got.ID != "interactive" {
		t.Fatalf("first = %+v ok=%v", got, ok)
	}
	got, _ = queue.Dequeue()
	if got.ID != "normal-1" {
		t.Fatalf("second = %s", got.ID)
	}
}

func TestQueueAgingPreventsStarvation(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	queue := NewQueue(clock)
	queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("background"), fixtures.Background))
	clock.Advance(201 * time.Minute)
	queue.Enqueue(fixtures.MakeJob(fixtures.WithJobID("interactive"), fixtures.Interactive))

	got, ok := queue.Dequeue()
	if !ok || got.ID != "background" {
		t.Fatalf("aged background should win, got %+v ok=%v", got, ok)
	}
}

func TestQueueEmpty(t *testing.T) {
	queue := NewQueue(mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)))
	if queue.Len() != 0 {
		t.Fatalf("Len = %d", queue.Len())
	}
	if got, ok := queue.Dequeue(); ok || got.ID != "" {
		t.Fatalf("Dequeue = %+v %v", got, ok)
	}
}
