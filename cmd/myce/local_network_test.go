package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestWorkerRetriesLocalNetworkConsentWithoutExiting(t *testing.T) {
	calls, pauses := 0, 0
	client, err := resolveWorkerJoining(context.Background(), func(context.Context) (myce.Client, error) {
		calls++
		if calls <= 20 {
			return myce.Client{}, myce.ErrLocalNetworkAccess
		}
		return myce.Client{URL: "https://test.invalid"}, nil
	}, func(_ context.Context, d time.Duration) error {
		pauses++
		if d != 2*time.Second {
			t.Fatal("unbounded consent retry rate", d)
		}
		return nil
	})
	if err != nil || client.URL != "https://test.invalid" || calls != 21 || pauses != 20 {
		t.Fatalf("consent recovery: calls=%d pauses=%d err=%v", calls, pauses, err)
	}
}

func TestWorkerConsentRetryCancellationAndTrustRefusal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, err := resolveWorkerJoining(ctx, func(context.Context) (myce.Client, error) {
		return myce.Client{}, myce.ErrLocalNetworkAccess
	}, func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("consent wait ignored cancellation", err)
	}
	refused := errors.New("credential or identity rejected")
	_, err = resolveWorkerJoining(context.Background(), func(context.Context) (myce.Client, error) {
		return myce.Client{}, refused
	}, func(context.Context, time.Duration) error {
		t.Fatal("trust refusal must not retry")
		return nil
	})
	if !errors.Is(err, refused) {
		t.Fatal("trust refusal changed", err)
	}
}
