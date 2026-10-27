package main

import (
	"context"
	"errors"
	"flag"
	"io"

	"mycelium/internal/myce"
)

// runFleetApply is hidden: only a private local receipt and installed config
// select authority. There are no token, publisher key, or artifact URL flags.
func runFleetApply(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("fleet-apply", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	config := f.String("config", "", "installed config")
	receipt := f.String("receipt", "", "private operation receipt")
	if err := f.Parse(args); err != nil || f.NArg() != 0 || *config == "" || *receipt == "" {
		return errors.New("fleet_apply_arguments_invalid")
	}
	return myce.ApplyFleetUpdate(ctx, *config, *receipt)
}
