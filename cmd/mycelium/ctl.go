package main

import (
	"context"

	"mycelium/cmd/internal/controlcli"
)

func runControl(ctx context.Context, args []string) error {
	return controlcli.Run(ctx, args)
}
