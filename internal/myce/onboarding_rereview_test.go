package myce

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRereviewCancelledNAS(t *testing.T) {
	c, command := onboardingNAS(t, "linux")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := PrepareOnboardingStorage(ctx, c, false, command)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Ctrl-C must retain cancellation: %v", err)
	}
	if strings.Contains(err.Error(), "nas_unresponsive") {
		t.Fatal("cancelled check classified as NAS failure", err)
	}
}

func TestRereviewTimedOutIOIsNotReused(t *testing.T) {
	n := checkpointIO[bool]{timeout: 10 * time.Millisecond}
	release := make(chan struct{})
	_, err := n.runContext(context.Background(), "same", func() (bool, error) { <-release; return true, nil })
	if !errors.Is(err, errCheckpointIOTimeout) {
		t.Fatal(err)
	}
	n.mu.Lock()
	done := n.pending.done
	n.mu.Unlock()
	close(release)
	<-done
	want := errors.New("fresh check")
	value, err := n.run("same", func() (bool, error) { return false, want })
	if value || !errors.Is(err, want) {
		t.Fatalf("served stale successful result: %v %v", value, err)
	}
}

func TestRereviewCancelledIOIsNotReused(t *testing.T) {
	n := checkpointIO[bool]{timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := n.runContext(ctx, "same", func() (bool, error) { close(entered); <-release; return true, nil })
		result <- err
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	n.mu.Lock()
	done := n.pending.done
	n.mu.Unlock()
	close(release)
	<-done
	want := errors.New("fresh cancelled check")
	value, err := n.run("same", func() (bool, error) { return false, want })
	if value || !errors.Is(err, want) {
		t.Fatalf("cancelled result reused: %v %v", value, err)
	}
}

func TestRereviewCancellationDuringMountInventory(t *testing.T) {
	c, _ := onboardingNAS(t, "linux")
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	result := make(chan error, 1)
	command := func(ctx context.Context, _ string, _ ...string) ([]byte, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	go func() { _, err := PrepareOnboardingStorage(ctx, c, false, command); result <- err }()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("mount-inventory cancellation misclassified: %v", err)
	}
}
