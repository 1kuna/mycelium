package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"

	"mycelium/internal/myce"
)

const fleetUsage = "usage: myce fleet upgrade [--release VERSION] [--queue-only] [--wait-production-idle]\n       myce fleet supersede --operation ID --reason TEXT"

func fleetCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) > 0 && args[0] == "supersede" {
		return fleetSupersedeCommand(ctx, args[1:], out)
	}
	if len(args) == 0 || args[0] != "upgrade" {
		return errors.New(fleetUsage)
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
		return errors.New(fleetUsage)
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

// Retire a stopped fleet operation with an audit record. Drains stay in place.
func fleetSupersedeCommand(ctx context.Context, args []string, out io.Writer) error {
	f := flag.NewFlagSet("fleet supersede", flag.ContinueOnError)
	authority := installedClientFlags(f)
	operation := f.String("operation", "", "stopped fleet operation ID shown by myce status")
	reason := f.String("reason", "", "why the operation is being retired (kept in the audit record)")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *operation == "" || *reason == "" {
		return errors.New(fleetUsage)
	}
	client, err := authority.client()
	if err != nil {
		return err
	}
	var record myce.FleetSupersession
	if err = client.Call(ctx, "POST", "/api/fleet/supersede", myce.FleetSupersedeRequest{Operation: *operation, Reason: *reason}, &record); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(record)
}
