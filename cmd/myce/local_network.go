package main

import (
	"context"
	"errors"
	"log"
	"time"

	"mycelium/internal/myce"
)

func resolveWorkerJoining(ctx context.Context, resolve func(context.Context) (myce.Client, error), pause func(context.Context, time.Duration) error) (myce.Client, error) {
	announced := false
	for {
		if err := ctx.Err(); err != nil {
			return myce.Client{}, err
		}
		client, err := resolve(ctx)
		if !errors.Is(err, myce.ErrLocalNetworkAccess) {
			return client, err
		}
		if !announced {
			log.Print("Mycelium is waiting for local network access. Allow Mycelium in System Settings > Privacy & Security > Local Network; the agent will retry automatically.")
			announced = true
		}
		// A LaunchAgent must remain alive for macOS to present its consent alert.
		// Keep each retry bounded; cancellation still stops the owned service.
		if err := pause(ctx, 2*time.Second); err != nil {
			return myce.Client{}, err
		}
	}
}
