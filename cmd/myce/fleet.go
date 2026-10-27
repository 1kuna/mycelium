package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"

	"mycelium/internal/myce"
)

func fleetCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "upgrade" {
		return errors.New("usage: myce fleet upgrade [--release VERSION]")
	}
	f := flag.NewFlagSet("fleet upgrade", flag.ContinueOnError)
	authority := installedClientFlags(f)
	release := f.String("release", "", "approved signed release (default: current approval)")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("usage: myce fleet upgrade [--release VERSION]")
	}
	client, err := authority.client()
	if err != nil {
		return err
	}
	var status myce.FleetUpgradeStatus
	if err = client.Call(ctx, "POST", "/api/fleet/upgrade", myce.FleetUpgradeRequest{Release: *release}, &status); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(status)
}
