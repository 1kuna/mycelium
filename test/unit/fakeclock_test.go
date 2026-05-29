package unit

import (
	"testing"
	"time"

	"mycelium/test/mocks"
)

func TestFakeClockAdvanceFiresDueTimer(t *testing.T) {
	start := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	clock := mocks.NewFakeClock(start)
	timer := clock.NewTimer(time.Second)

	clock.Advance(999 * time.Millisecond)
	select {
	case <-timer.C():
		t.Fatal("timer fired early")
	default:
	}

	clock.Advance(time.Millisecond)
	select {
	case got := <-timer.C():
		if !got.Equal(start.Add(time.Second)) {
			t.Fatalf("fired at %s", got)
		}
	default:
		t.Fatal("timer did not fire")
	}
}
