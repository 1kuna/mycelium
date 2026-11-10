package myce

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
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

// Model a fully occupied control path without wall sleeps. Pending check-ins
// cannot bury a submit, and pending submits cannot starve worker reports. The
// same-class FIFO order survives both handoffs and failed TryLock attempts.
func TestQueueMutexSubmissionTurns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu queueMutex
		mu.Lock()
		order := make(chan int, 7)
		for i := range 7 {
			go func() {
				if i >= 4 {
					submissionMutex{&mu}.Lock()
				} else {
					mu.Lock()
				}
				order <- i
				mu.Unlock()
			}()
			synctest.Wait()
		}
		if mu.TryLock() {
			t.Fatal("TryLock bypassed an active writer")
		}
		mu.Unlock()
		synctest.Wait()
		var got []int
		for range 7 {
			got = append(got, <-order)
		}
		if want := []int{4, 0, 5, 1, 6, 2, 3}; !slices.Equal(got, want) {
			t.Fatalf("writer order = %v, want %v", got, want)
		}
		mu.RLock()
		if mu.TryLock() {
			t.Fatal("writer bypassed reader")
		}
		mu.RUnlock()
		if !mu.TryLock() {
			t.Fatal("failed TryLock retained writer turn")
		}
		mu.Unlock()
		if !mu.TryRLock() {
			t.Fatal("writer retained underlying lock")
		}
		mu.RUnlock()
	})
}
