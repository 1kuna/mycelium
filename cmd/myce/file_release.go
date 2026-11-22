package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"

	"mycelium/internal/myce"
)

func fileReleaseCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: myce file release plan --project PROJECT [--include-checkpoints] JOB... | apply --accept-expiry PLAN.json | claim --project PROJECT --accept-expiry CLAIM_ID")
	}
	f := flag.NewFlagSet("file release "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	project := f.String("project", "", "project attribution (not an authorization boundary)")
	checkpoints := f.Bool("include-checkpoints", false, "include legacy queue checkpoint generations of the selected finished jobs")
	accept := f.Bool("accept-expiry", false, "accept irreversible expiry after all downstream use and required preservation are complete")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "plan":
		if *project == "" || f.NArg() == 0 || *accept {
			return errors.New("plan requires --project and explicit job IDs")
		}
	case "apply":
		if f.NArg() != 1 || !*accept || *project != "" || *checkpoints {
			return errors.New("apply requires --accept-expiry and one reviewed plan file")
		}
	case "claim":
		if f.NArg() != 1 || !*accept || *project == "" || *checkpoints {
			return errors.New("claim release requires --project, --accept-expiry and one claim ID")
		}
	default:
		return errors.New("release requires plan, apply or claim")
	}
	c, err := authority.client()
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if args[0] == "plan" {
		var plan myce.FileReleasePlan
		err = c.Call(ctx, "POST", "/api/files/release/plan", myce.FileReleaseSelection{Project: *project, Jobs: f.Args(), IncludeCheckpoints: *checkpoints}, &plan)
		if err != nil {
			return err
		}
		return enc.Encode(plan)
	}
	if args[0] == "claim" {
		var result myce.FileInputClaimReleaseResult
		err = c.Call(ctx, "POST", "/api/files/release/claim", myce.FileInputClaimRelease{Project: *project, ClaimID: f.Arg(0), AcceptExpiry: *accept}, &result)
		if err != nil {
			return err
		}
		return enc.Encode(result)
	}
	file, err := os.Open(f.Arg(0))
	if err != nil {
		return err
	}
	defer file.Close()
	var plan myce.FileReleasePlan
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&plan); err != nil {
		return err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return errors.New("expected one release plan")
	}
	var result myce.FileReleaseResult
	err = c.Call(ctx, "POST", "/api/files/release/apply", myce.FileReleaseApply{
		Selection: plan.Selection, PlanID: plan.ID, AcceptExpiry: *accept, CollectionCandidates: plan.CollectionCandidates,
	}, &result)
	if err != nil {
		return err
	}
	return enc.Encode(result)
}
