package myce

import (
	"context"
	"testing"
	"time"
)

// The burst's attribution must cover ordinary and request locks outside queue
// updates, and overlapping readers must not inflate the measured occupancy.
func TestQueueMutexHoldObservation(t *testing.T) {
	var holds []time.Duration
	mu := queueMutex{observeHold: func(d time.Duration) { holds = append(holds, d) }}
	unlock := requestLock(context.Background(), &mu)
	if mu.TryLock() || mu.TryRLock() {
		t.Fatal("writer did not exclude another lock")
	}
	unlock()
	if len(holds) != 1 || holds[0] <= 0 {
		t.Fatal("request lock outside updateRequest was not measured", holds)
	}

	mu.RLock()
	readerEntered, releaseReader, readerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readerDone)
		unlock := requestReadLock(context.Background(), &mu)
		close(readerEntered)
		<-releaseReader
		unlock()
	}()
	<-readerEntered
	if mu.TryLock() {
		t.Error("readers did not exclude writer")
		mu.Unlock()
	}
	mu.RUnlock()
	// Another reader still owns q.mu; occupancy must remain open.
	if len(holds) != 1 {
		t.Error("overlapping reader ended occupancy early", holds)
	}
	close(releaseReader)
	<-readerDone
	if len(holds) != 2 || holds[1] <= 0 {
		t.Fatal("read occupancy was missed or counted more than once", holds)
	}
	if !mu.TryLock() {
		t.Fatal("readers did not release writer")
	}
	mu.Unlock()
	if !mu.TryRLock() {
		t.Fatal("writer did not release reader")
	}
	mu.RUnlock()
	if len(holds) != 4 {
		t.Fatal("successful try-lock holds were not measured", holds)
	}
}
