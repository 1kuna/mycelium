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
		return errors.New("usage: myce fleet upgrade [--release VERSION] [--queue-only] [--wait-production-idle]")
	}
	f := flag.NewFlagSet("fleet upgrade", flag.ContinueOnError)
	authority := installedClientFlags(f)
	release := f.String("release", "", "approved signed release (default: current approval)")
	queueOnly := f.Bool("queue-only", false, "upgrade only the queue service, preserving worker versions")
	waitIdle := f.Bool("wait-production-idle", false, "pause new production assignments and wait for fleet-wide production custody before queue restart")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("usage: myce fleet upgrade [--release VERSION] [--queue-only] [--wait-production-idle]")
	}
	client, err := authority.client()
	if err != nil {
		return err
	}
	var status myce.FleetUpgradeStatus
	if err = client.Call(ctx, "POST", "/api/fleet/upgrade", myce.FleetUpgradeRequest{Release: *release, QueueOnly: *queueOnly, WaitProductionIdle: *waitIdle}, &status); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(status)
}
