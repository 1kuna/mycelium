package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/url"
	"strings"

	"mycelium/internal/myce"
)

const fleetUsage = "usage: myce fleet upgrade [--release VERSION] [--urgency normal|critical]\n       " + fleetLeaseUsage

const fleetLeaseUsage = "myce fleet lease open --project P --key K --for DURATION [--platform LIST] [--memory-gb N] [--stages JSON]\n       myce fleet lease close --project P --key K\n       myce fleet lease list"

func fleetCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) > 0 && args[0] == "lease" {
		return fleetLeaseCommand(ctx, args[1:], out)
	}
	if len(args) == 0 || args[0] != "upgrade" {
		return errors.New(fleetUsage)
	}
	f := flag.NewFlagSet("fleet upgrade", flag.ContinueOnError)
	authority := installedClientFlags(f)
	release := f.String("release", "", "approved signed release (default: current approval)")
	urgency := f.String("urgency", "normal", "normal: wait for work, then checkpoint-stop resumable non-production attempts after a bounded wait; critical: stop work immediately")
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
	if err = client.Call(ctx, "POST", "/api/fleet/upgrade", myce.FleetUpgradeRequest{Urgency: *urgency, Release: *release}, &status); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(status)
}

// A project declares open production work, including stages it has not
// submitted yet, so planned drains and fleet upgrades keep a host able to
// start them. The lease ends on its own after --for; renew by reopening.
func fleetLeaseCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: " + fleetLeaseUsage)
	}
	f := flag.NewFlagSet("fleet lease "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	project := f.String("project", "", "project that owns the production work")
	key := f.String("key", "", "the project's name for this run (reopen with the same key to renew)")
	ttl := f.String("for", "", "how long the lease stays open unless renewed or closed, e.g. 2h")
	platform := f.String("platform", "", "comma-separated accelerator platforms a stage requires")
	memory := f.Float64("memory-gb", 0, "device memory a stage needs")
	stages := f.String("stages", "", "JSON array of stages ({needs, allowed_machines, excluded_machines}) instead of --platform/--memory-gb")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("usage: " + fleetLeaseUsage)
	}
	open := args[0] == "open"
	if args[0] != "list" && (*project == "" || *key == "") || open && *ttl == "" || !open && (*ttl != "" || *platform != "" || *memory != 0 || *stages != "") {
		return errors.New("usage: " + fleetLeaseUsage)
	}
	client, err := authority.client()
	if err != nil {
		return err
	}
	var reply any
	switch args[0] {
	case "open":
		request := myce.ProductionLeaseRequest{Project: *project, Key: *key, TTL: *ttl}
		if *stages != "" {
			if *platform != "" || *memory != 0 {
				return errors.New("use --stages or --platform/--memory-gb, not both")
			}
			if err = json.Unmarshal([]byte(*stages), &request.Stages); err != nil {
				return errors.New("--stages must be a JSON array of stages")
			}
		} else if *platform != "" || *memory != 0 {
			stage := myce.ProductionStage{Needs: myce.ResourceNeeds{MemoryGB: *memory, Platform: myce.Platform{Any: *platform == ""}}}
			if *platform != "" {
				stage.Needs.Platform.Required = strings.Split(*platform, ",")
			}
			request.Stages = []myce.ProductionStage{stage}
		}
		var lease myce.ProductionLease
		err, reply = client.Call(ctx, "POST", "/api/production-leases", request, &lease), &lease
	case "close":
		var closed map[string]any
		err, reply = client.Call(ctx, "DELETE", "/api/production-leases/"+url.PathEscape(*project)+"/"+url.PathEscape(*key), nil, &closed), &closed
	case "list":
		var leases map[string]any
		err, reply = client.Call(ctx, "GET", "/api/production-leases", nil, &leases), &leases
	default:
		return errors.New("usage: " + fleetLeaseUsage)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(reply)
}
